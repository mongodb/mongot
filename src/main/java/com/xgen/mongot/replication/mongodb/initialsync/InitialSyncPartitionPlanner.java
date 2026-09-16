package com.xgen.mongot.replication.mongodb.initialsync;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.CollectionStatsMongoClient;
import com.xgen.mongot.replication.mongodb.common.PartitionPlanningMongoClient;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.SequentialFallback;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.ShortCircuitReason;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Decides whether an initial sync collection scan can be split across several concurrent {@code
 * $natural} cursors.
 *
 * <p>Planning is the best effort and never fails an initial sync: anything that makes partitioning
 * unsafe or not worthwhile produces a {@link SequentialFallback} and the caller runs the existing
 * single cursor scan.
 */
class InitialSyncPartitionPlanner {

  /** The smallest collection worth partitioning. */
  static final long MIN_DOCUMENT_COUNT = 30_000;

  private static final String OUTCOME_METRIC = "partitionPlanningOutcome";
  private static final String OUTCOME_TAG = "outcome";
  private static final String PARTITIONED_OUTCOME = "partitioned";

  private static final double[] PLANNING_DURATION_BUCKETS_SECONDS = {0.5, 1.0, 2.5, 5.0};

  /**
   * Wall clock budget for the whole stage. The replication client sets no socket timeout, so
   * without this a stuck read would block the sync indefinitely.
   *
   * <p>TODO(CLOUDP-417359): five seconds is a placeholder, not a measured value.
   */
  private static final Duration STAGE_BUDGET = Duration.ofSeconds(5);

  private final DefaultKeyValueLogger logger;
  private final Clock clock;
  private final InitialSyncContext context;
  private final CollectionStatsMongoClient statsClient;
  private final PartitionPlanningMongoClient planningClient;
  private final Map<ShortCircuitReason, Counter> fallbackCounters;
  private final Counter partitionedCounter;
  private final DistributionSummary planningDurationHistogram;
  private final Duration stageBudget;

  InitialSyncPartitionPlanner(
      Clock clock,
      InitialSyncContext context,
      CollectionStatsMongoClient statsClient,
      PartitionPlanningMongoClient planningClient,
      MetricsFactory metricsFactory) {
    this(clock, context, statsClient, planningClient, metricsFactory, STAGE_BUDGET);
  }

  @VisibleForTesting
  InitialSyncPartitionPlanner(
      Clock clock,
      InitialSyncContext context,
      CollectionStatsMongoClient statsClient,
      PartitionPlanningMongoClient planningClient,
      MetricsFactory metricsFactory,
      Duration stageBudget) {
    this.stageBudget = stageBudget;
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", context.getIndexId());
    defaultKeyValues.put("generationId", context.getGenerationId());
    this.logger =
        DefaultKeyValueLogger.getLogger(InitialSyncPartitionPlanner.class, defaultKeyValues);

    this.clock = clock;
    this.context = context;
    this.statsClient = statsClient;
    this.planningClient = planningClient;
    this.fallbackCounters = fallbackCounters(metricsFactory);
    this.partitionedCounter =
        metricsFactory.counter(OUTCOME_METRIC, Tags.of(OUTCOME_TAG, PARTITIONED_OUTCOME));
    this.planningDurationHistogram =
        metricsFactory.histogram(
            "partitionPlanningDurationSeconds", PLANNING_DURATION_BUCKETS_SECONDS);
  }

  private static Map<ShortCircuitReason, Counter> fallbackCounters(MetricsFactory metricsFactory) {
    Map<ShortCircuitReason, Counter> counters = new EnumMap<>(ShortCircuitReason.class);
    for (ShortCircuitReason reason : ShortCircuitReason.values()) {
      counters.put(
          reason, metricsFactory.counter(OUTCOME_METRIC, Tags.of(OUTCOME_TAG, reason.name())));
    }
    return counters;
  }

  /**
   * Produces this run's partition plan.
   *
   * <p>Takes the collection info that namespace resolution already fetched, so the clustered check
   * costs no extra round trip. A view cannot reach here: namespace resolution matches on {@code
   * info.uuid}, which only collections report.
   *
   * @param collectionInfo the resolved sync source collection
   * @return a plan to scan in parallel, or a fallback to the single cursor scan. Never fails
   */
  InitialSyncPartitionPlan plan(MongoDbCollectionInfo.Collection collectionInfo) {
    Instant startedAt = Instant.now(this.clock);
    return record(startedAt, choosePlanWithBudget(collectionInfo));
  }

  /** Runs the checks on their own thread, so that a hung read cannot outlast the stage budget. */
  private InitialSyncPartitionPlan choosePlanWithBudget(
      MongoDbCollectionInfo.Collection collectionInfo) {
    CompletableFuture<InitialSyncPartitionPlan> planning =
        InitialSyncManager.supplyAsync(
            () -> choosePlan(collectionInfo),
            String.format(
                "%s %s",
                this.context.uniqueString(), InitialSyncPartitionPlanner.class.getSimpleName()));

    try {
      return planning.get(this.stageBudget.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      // The worker is abandoned rather than canceled: a read blocked on the socket is not
      // interruptible, and the sequential fallback is correct whatever it eventually returns.
      this.logger
          .atWarn()
          .addKeyValue("stageBudgetMs", this.stageBudget.toMillis())
          .log("Partition planning exceeded its budget, falling back to a single cursor scan.");
      return new SequentialFallback(ShortCircuitReason.PLANNING_BUDGET_EXCEEDED);
    } catch (InterruptedException e) {
      // Reinstated for the scan to act on, since planning never fails a sync itself.
      Thread.currentThread().interrupt();
      return new SequentialFallback(ShortCircuitReason.PLANNING_FAILED);
    } catch (ExecutionException e) {
      // A backstop: choosePlan converts its own failures, so reaching here means something it
      // could not catch, such as an Error.
      this.logger
          .atWarn()
          .setCause(e.getCause())
          .log("Partition planning failed, falling back to a single cursor scan.");
      return new SequentialFallback(ShortCircuitReason.PLANNING_FAILED);
    }
  }

  private InitialSyncPartitionPlan choosePlan(MongoDbCollectionInfo.Collection collectionInfo) {
    try {
      // Step 1: pre-flight. A clustered collection reports $recordId as BinData rather than an
      // int64, which neither the boundaries nor the partition stop predicate can compare today.
      if (collectionInfo.options().isClustered()) {
        return new SequentialFallback(ShortCircuitReason.CLUSTERED_COLLECTION);
      }

      // Step 2: size estimate. collStats reports the count held in collection metadata, so this
      // does not walk the collection the way a countDocuments would.
      long documentCount = this.statsClient.getStats(namespace()).getCount();
      if (documentCount < MIN_DOCUMENT_COUNT) {
        this.logger
            .atInfo()
            .addKeyValue("documentCount", documentCount)
            .addKeyValue("minDocumentCount", MIN_DOCUMENT_COUNT)
            .log("Collection is too small to partition.");
        return new SequentialFallback(ShortCircuitReason.TOO_FEW_DOCUMENTS);
      }

      // Step 3: read the $initialSyncId before sampling. It identifies the sync source node's copy
      // of the data and changes when that node is resynced, which rewrites every recordId. Reading
      // it first means a resync during planning is caught, rather than silently pairing a new id
      // with boundaries sampled from the old copy.
      Optional<UUID> initialSyncId =
          this.planningClient.captureInitialSyncId(
              namespace(), this.context.getChangeStreamResumeOperationTime());
      if (initialSyncId.isEmpty()) {
        return new SequentialFallback(ShortCircuitReason.NO_INITIAL_SYNC_ID);
      }

      // TODO(CLOUDP-417359): sample, resolve the sampled ids to recordIds, and emit the boundaries.
      // Until that lands there is nothing to partition on, so planning declines.
      return new SequentialFallback(ShortCircuitReason.TOO_FEW_DOCUMENTS);
    } catch (RuntimeException e) {
      // Broad on purpose: this stage is an optimization, so nothing it does may fail the sync. A
      // failed read and a broken assumption both mean the same thing here, scan with one cursor.
      this.logger
          .atWarn()
          .setCause(e)
          .log("Partition planning failed, falling back to a single cursor scan.");
      return new SequentialFallback(ShortCircuitReason.PLANNING_FAILED);
    }
  }

  /** Reports the stage's outcome and how long it took, then hands the plan back unchanged. */
  private InitialSyncPartitionPlan record(Instant startedAt, InitialSyncPartitionPlan plan) {
    Duration planningDuration = Duration.between(startedAt, Instant.now(this.clock));

    Optional<ShortCircuitReason> reason =
        switch (plan) {
          case InitialSyncPartitionPlan.PartitionedResult ignored -> Optional.empty();
          case SequentialFallback fallback -> Optional.of(fallback.reason());
        };
    String outcome = reason.map(ShortCircuitReason::name).orElse(PARTITIONED_OUTCOME);

    reason.map(this.fallbackCounters::get).orElse(this.partitionedCounter).increment();
    this.planningDurationHistogram.record(planningDuration.toNanos() / 1_000_000_000.0);

    this.logger
        .atInfo()
        .addKeyValue("outcome", outcome)
        .addKeyValue("planningDurationMs", planningDuration.toMillis())
        .log("Finished partition planning.");

    return plan;
  }

  private MongoNamespace namespace() {
    return this.context.getIndexDefinition().getLastObservedNamespace();
  }
}
