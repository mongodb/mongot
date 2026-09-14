package com.xgen.mongot.replication.mongodb.autoembedding;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.util.mongodb.MongoDbReplSetStatus;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observes the generators installed in a {@link MaterializedViewManager} and writes per-generator
 * index status and replication-optime metrics. It only reads a snapshot of the installed generators
 * and never installs, drains or replaces one. It does not hold the manager lock across its own
 * work; the injected gate and generator snapshot each synchronize on the manager only for the
 * duration of that call.
 *
 * <p>Two write paths: {@link #copyFollowerStatuses} mirrors the lease table's published status and
 * optime onto each follower generator's index, and {@link #updateMaxReplicationOpTime()} keeps the
 * max-possible replication optime current for every queryable materialized view (leaders and
 * followers alike). The owning manager schedules the latter and feeds the former from its own
 * status-refresh tick.
 */
class MaterializedViewStatusUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewStatusUpdater.class);

  static final String OPTIME_UPDATER_ERROR_COUNTER_NAME = "matViewOptimeUpdaterError";

  private final AutoEmbeddingMongoClient autoEmbeddingMongoClient;
  private final MeterRegistry meterRegistry;

  /** Snapshot of the installed generators keyed by materialized view collection UUID. */
  private final Supplier<Map<UUID, MaterializedViewGenerator>> generators;

  /**
   * True for a generation the manager retired because its materialized view collection is missing.
   * Retired generations keep their committed status and are never overwritten from the lease.
   */
  private final Predicate<MaterializedViewGenerationId> isRetired;

  /** False while the owning manager is shut down or replication is unsupported. */
  private final BooleanSupplier updatesAllowed;

  MaterializedViewStatusUpdater(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MeterRegistry meterRegistry,
      Supplier<Map<UUID, MaterializedViewGenerator>> generators,
      Predicate<MaterializedViewGenerationId> isRetired,
      BooleanSupplier updatesAllowed) {
    this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
    this.meterRegistry = meterRegistry;
    this.generators = generators;
    this.isRetired = isRetired;
    this.updatesAllowed = updatesAllowed;
  }

  /**
   * Updates the local index status and replication optime of every follower generator from the
   * statuses the lease table published for it in {@code pollResult}. Leaders and retired
   * generations are left untouched.
   */
  void copyFollowerStatuses(LeaseManager.FollowerPollResult pollResult) {
    this.generators
        .get()
        .values()
        .forEach(
            generator -> {
              var generationId = generator.getIndexGeneration().getGenerationId();
              // A retired generation's lease still carries the status this host published before
              // its collection went missing. Copying that back would overwrite the committed
              // COLLECTION_NOT_FOUND and report the index queryable, and no recreated generator
              // will ever restore it.
              if (this.isRetired.test(generationId)) {
                return;
              }
              // Check generator.isLeader again before setting.
              if (pollResult.statuses().containsKey(generationId) && !generator.isLeader()) {
                var matViewIndex = generator.getIndexGeneration().getIndex();
                var status = pollResult.statuses().get(generationId);
                matViewIndex.setStatus(status);
                var replicationOpTime = pollResult.replicationOpTimes().get(generationId);
                if (replicationOpTime != null) {
                  // Preserve an existing max optime. If this is the first observation, the periodic
                  // max optime updater will advance it on its next run.
                  matViewIndex
                      .getMetricsUpdater()
                      .getIndexingMetricsUpdater()
                      .getReplicationOpTimeInfo()
                      .update(replicationOpTime.getValue(), replicationOpTime.getValue());
                }
              }
            });
  }

  /**
   * Periodically clears replication optime for non-queryable materialized view indexes and updates
   * maxPossibleReplicationOpTime for queryable indexes. This needs to happen separately since this
   * metric is updated only for indexes in the IndexCatalog in ReplicationOptimeUpdater.
   *
   * <p>This applies to both leaders and followers. Leaders update replication optime while
   * processing; followers refresh replication optime from the lease table in {@link
   * #copyFollowerStatuses}.
   */
  @VisibleForTesting
  void updateMaxReplicationOpTime() {
    if (!this.updatesAllowed.getAsBoolean()) {
      return;
    }
    try {
      var generators = this.generators.get().values();
      generators.forEach(
          generator -> {
            var matViewIndex = generator.getIndexGeneration().getIndex();
            matViewIndex
                .getMetricsUpdater()
                .getIndexingMetricsUpdater()
                .getReplicationOpTimeInfo()
                .unset(() -> !matViewIndex.getStatus().canServiceQueries());
          });

      // Use collection resolver client here for querying mongod cluster time to reduce impacts in
      // leaseManager and writer mongoClient
      var mongoClientOpt = this.autoEmbeddingMongoClient.getMaterializedViewResolverMongoClient();
      if (mongoClientOpt.isEmpty()) {
        LOG.warn("No MongotClient is present, skipping max optime update");
        return;
      }
      var opTime = MongoDbReplSetStatus.getReadConcernMajorityOpTime(mongoClientOpt.get());
      generators.forEach(
          generator -> {
            var matViewIndex = generator.getIndexGeneration().getIndex();
            if (matViewIndex.isClosed()) {
              return;
            }
            var status = matViewIndex.getStatus();
            if (!status.canServiceQueries()) {
              return;
            }
            var opTimeInfo =
                matViewIndex
                    .getMetricsUpdater()
                    .getIndexingMetricsUpdater()
                    .getReplicationOpTimeInfo();
            status
                .getOptime()
                .ifPresentOrElse(
                    replicationOptime ->
                        opTimeInfo.update(replicationOptime.getValue(), opTime.getValue()),
                    () -> opTimeInfo.update(opTime.getValue()));
          });
    } catch (Exception e) {
      LOG.error("Failed to update max optime for materialized views", e);
      this.meterRegistry.counter(OPTIME_UPDATER_ERROR_COUNTER_NAME).increment();
    }
  }
}
