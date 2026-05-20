package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.util.VerboseRunnable;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.MongoClientBuilder;
import com.xgen.mongot.util.mongodb.MongoDbReplSetStatus;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically fetches the last committed optime and sets it to indexing metrics updaters of the
 * stale indexes.
 *
 * <p>Instance of this class uses MongoClient which is connected to a specific DB. If connection
 * string changes then new instance of this class should be created with the updated MongotClient.
 * Currently, this class is being created by MongoDbReplicationManager which provides a fresh
 * instance of MongoClient during each initialization. The initialization happen after each
 * connection string change.
 *
 * <p>See <a
 * href="https://www.mongodb.com/docs/manual/reference/command/replSetGetStatus/#mongodb-data-replSetGetStatus.optimes.readConcernMajorityOpTime">replSetGetStatus.optimes.readConcernMajorityOpTime</a>
 */
public class ReplicationOptimeUpdater implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationOptimeUpdater.class);

  public static final Duration DEFAULT_UPDATE_INTERVAL = Duration.ofSeconds(10);

  @VisibleForTesting
  static final String ERROR_COUNTER_METRIC_NAME = "replicationOptimeUpdaterError";

  private final IndexCatalog indexCatalog;
  private final InitializedIndexCatalog initializedIndexCatalog;
  private final Optional<MongoClient> mongoClient;
  private final NamedScheduledExecutorService executor;
  private final Counter errorCounter;

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public ReplicationOptimeUpdater(
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      Optional<MongoClient> mongoClient,
      NamedScheduledExecutorService executor,
      Duration interval,
      MeterRegistry meterRegistry) {
    this.indexCatalog = indexCatalog;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.mongoClient = mongoClient;
    this.executor = executor;
    this.errorCounter = meterRegistry.counter(ERROR_COUNTER_METRIC_NAME);

    if (this.mongoClient.isPresent()) {
      LOG.info("Starting ReplicationOptimeUpdater.");
      executor.scheduleWithFixedDelay(
          new VerboseRunnable() {
            @Override
            public void verboseRun() {
              update();
            }

            @Override
            public Logger getLogger() {
              return LOG;
            }
          },
          0,
          interval.toMillis(),
          TimeUnit.MILLISECONDS);
    } else {
      LOG.info("Not starting ReplicationOptimeUpdater because mongoClient is not present.");
    }
  }

  // TODO(CLOUDP-405327): remove test-only overload once LIFECYCLE_ATTRIBUTION_METRICS rolls out.
  public static ReplicationOptimeUpdater create(
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      Optional<SyncSourceConfig> syncSource,
      Duration updateInterval,
      MeterRegistry meterRegistry) {
    return create(
        indexCatalog, initializedIndexCatalog, syncSource, updateInterval, meterRegistry, false);
  }

  public static ReplicationOptimeUpdater create(
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      Optional<SyncSourceConfig> syncSource,
      Duration updateInterval,
      MeterRegistry meterRegistry,
      boolean enableLifecycleAttributionMetrics) {
    // mongodSingleHostReplicationUri may be absent when this updater is created alongside a
    // NoOpReplicationManager (i.e. no healthy sync-source host has been selected yet).
    Optional<MongoClient> mongoClient =
        syncSource.flatMap(
            syncSourceConfig ->
                syncSourceConfig.mongodSingleHostReplicationUri.map(
                    uri ->
                        MongoClientBuilder.buildNonReplicationWithDefaults(
                            uri, "periodic optime fetcher", meterRegistry)));
    var executor =
        Executors.singleThreadScheduledExecutor("replicationOptimeUpdater", meterRegistry);
    if (enableLifecycleAttributionMetrics) {
      ThreadPoolResourceMetrics.create("replication").register(executor, meterRegistry);
    }
    return new ReplicationOptimeUpdater(
        indexCatalog,
        initializedIndexCatalog,
        mongoClient,
        executor,
        updateInterval,
        meterRegistry);
  }

  @VisibleForTesting
  static ReplicationOptimeUpdater create(
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      MongoClient mongoClient,
      NamedScheduledExecutorService executor,
      Duration interval,
      MeterRegistry meterRegistry) {
    return new ReplicationOptimeUpdater(
        indexCatalog,
        initializedIndexCatalog,
        Optional.of(mongoClient),
        executor,
        interval,
        meterRegistry);
  }

  @VisibleForTesting
  void update() {
    if (this.mongoClient.isEmpty()) {
      return;
    }
    try {
      var optime = MongoDbReplSetStatus.getReadConcernMajorityOpTime(this.mongoClient.get());
      Collection<IndexGeneration> indexes = this.indexCatalog.getIndexes();
      updateMaxReplicationOpTimeForQueryableIndexes(indexes, optime);
      unsetReplicationOpTimeInfo(indexes);
    } catch (Exception e) {
      LOG.error("Failed to update max optime", e);
      this.errorCounter.increment();
    }
  }

  /**
   * Unsets replication optime info for indexes that can not serve queries because mongot does not
   * run replication for such indexes. We should not report replication lag for them.
   */
  private void unsetReplicationOpTimeInfo(Collection<IndexGeneration> indexes) {
    // In the future (CLOUDP-219777), when index initialization happens asynchronously, it's ok
    // to not report optime till index is fully initialized.
    indexes.stream()
        .map(
            indexGeneration ->
                this.initializedIndexCatalog.getIndex(indexGeneration.getGenerationId()))
        .flatMap(Optional::stream)
        .filter(
            initializedIndex ->
                !initializedIndex.isClosed() && !initializedIndex.getStatus().canServiceQueries())
        .forEach(
            initializedIndex ->
                ignoringClosed(
                    initializedIndex,
                    index -> {
                      // stop reporting replication optime related metrics to InTel and Prom
                      // check canServiceQueries again here in case the value changed.
                      boolean didUnset =
                          initializedIndex
                              .getMetricsUpdater()
                              .getIndexingMetricsUpdater()
                              .getReplicationOpTimeInfo()
                              .unset(() -> !initializedIndex.getStatus().canServiceQueries());

                      if (!didUnset) {
                        LOG.atInfo()
                            .addKeyValue("indexId", initializedIndex.getGenerationId().indexId)
                            .addKeyValue("generationId", initializedIndex.getGenerationId())
                            .log(
                                "Failed to unset replication optime info, "
                                    + "possible concurrent modification?");
                      }
                    }));
  }

  private void updateMaxReplicationOpTimeForQueryableIndexes(
      Collection<IndexGeneration> indexes, BsonTimestamp lastCommittedOptime) {
    // In the future (CLOUDP-219777), when index initialization happens asynchronously, it's ok
    // to not report optime till index is fully initialized.
    indexes.stream()
        .map(
            indexGeneration ->
                this.initializedIndexCatalog.getIndex(indexGeneration.getGenerationId()))
        .flatMap(Optional::stream)
        .filter(initializedIndex -> initializedIndex.getStatus().canServiceQueries())
        .forEach(
            initializedIndex ->
                initializedIndex
                    .getStatus()
                    .getOptime()
                    .ifPresentOrElse(
                        replicationOptime ->
                            updateWithReplicationOpTime(
                                replicationOptime, lastCommittedOptime, initializedIndex),
                        () ->
                            updateWithoutReplicationOpTime(lastCommittedOptime, initializedIndex)));
  }

  // Used for STALE and RECOVERING_TRANSIENT indexes that store replication optime in status
  private void updateWithReplicationOpTime(
      BsonTimestamp replicationOptime,
      BsonTimestamp lastCommittedOptime,
      InitializedIndex initializedIndex) {
    ignoringClosed(
        initializedIndex,
        index -> {
          initializedIndex
              .getMetricsUpdater()
              .getIndexingMetricsUpdater()
              .getReplicationOpTimeInfo()
              .update(replicationOptime.getValue(), lastCommittedOptime.getValue());
        });
  }

  // Used for indexes that don't store replication optime in status or missing replication optime
  private void updateWithoutReplicationOpTime(
      BsonTimestamp lastCommittedOptime, InitializedIndex initializedIndex) {
    ignoringClosed(
        initializedIndex,
        index -> {
          initializedIndex
              .getMetricsUpdater()
              .getIndexingMetricsUpdater()
              .getReplicationOpTimeInfo()
              .update(lastCommittedOptime.getValue());
        });

    if (initializedIndex.getStatus().getStatusCode() == IndexStatus.StatusCode.STALE
        || initializedIndex.getStatus().getStatusCode()
            == IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT) {
      GenerationId generationId = initializedIndex.getGenerationId();
      LOG.atWarn()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .log("Stale/Recovering index status does not contain optime.");
    }
  }

  @Override
  public void close() {
    LOG.info("Shutting down.");
    Executors.shutdownOrFail(this.executor);
    this.mongoClient.ifPresent(MongoClient::close);
  }

  private void ignoringClosed(
      InitializedIndex initializedIndex, Consumer<InitializedIndex> action) {
    try {
      action.accept(initializedIndex);
    } catch (IllegalStateException e) {
      if (!initializedIndex.isClosed()) {
        throw e;
      }
      GenerationId generationId = initializedIndex.getGenerationId();
      LOG.atInfo()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .addKeyValue("exceptionMessage", e.getMessage())
          .log("Index has been closed, we can ignore the exception");
    }
  }
}
