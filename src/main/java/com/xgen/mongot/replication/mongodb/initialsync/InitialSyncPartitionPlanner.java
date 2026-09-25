package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.CollectionSamplingMongoClient;
import com.xgen.mongot.replication.mongodb.common.CollectionStatsMongoClient;
import com.xgen.mongot.replication.mongodb.common.PartitionPlanningMongoClient;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.PartitionRange;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.PartitionedResult;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.SequentialFallback;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.ShortCircuitReason;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

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

  static final int SAMPLE_SIZE = 1000;

  /** Below this many resolved, documents were deleted in between and the sample is redrawn. */
  static final int MIN_RESOLVED_SAMPLES = 800;

  /**
   * Draws to take before giving up on a collection that keeps resolving short. The stage budget
   * cuts planning off before this many sample and resolve round trips can run on a large
   * collection, so it bounds a churning collection rather than setting the usual cost.
   */
  static final int MAX_SAMPLE_ATTEMPTS = 5;

  /** A plan needs at least two ranges to be worth running. */
  static final int MIN_PARTITION_COUNT = 2;

  /**
   * TODO(CLOUDP-417359): 32 is unvalidated. Partition imbalance grows with K and nothing rebalances
   * yet, so this likely wants to track the indexer thread count.
   */
  static final int MAX_PARTITION_COUNT = 32;

  private static final String ID_FIELD = "_id";
  private static final BsonDocument ID_PROJECTION = new BsonDocument(ID_FIELD, new BsonInt32(1));

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
  private final CollectionSamplingMongoClient samplingClient;
  private final Map<ShortCircuitReason, Counter> fallbackCounters;
  private final Counter partitionedCounter;
  private final DistributionSummary planningDurationHistogram;
  private final Duration stageBudget;

  InitialSyncPartitionPlanner(
      Clock clock,
      InitialSyncContext context,
      CollectionStatsMongoClient statsClient,
      PartitionPlanningMongoClient planningClient,
      CollectionSamplingMongoClient samplingClient,
      MetricsFactory metricsFactory) {
    this(clock, context, statsClient, planningClient, samplingClient, metricsFactory, STAGE_BUDGET);
  }

  @VisibleForTesting
  InitialSyncPartitionPlanner(
      Clock clock,
      InitialSyncContext context,
      CollectionStatsMongoClient statsClient,
      PartitionPlanningMongoClient planningClient,
      CollectionSamplingMongoClient samplingClient,
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
    this.samplingClient = samplingClient;
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

  /**
   * Cuts the sorted recordIds into {@code partitionCount} ranges at evenly spaced quantiles. The
   * first and last ranges are open ended, so they cover the collection whatever the sample hit.
   */
  @VisibleForTesting
  static ImmutableList<PartitionRange> partitionsFrom(List<Long> sorted, int partitionCount) {
    List<Long> cuts =
        IntStream.range(1, partitionCount)
            .mapToObj(i -> sorted.get(i * sorted.size() / partitionCount))
            .toList();

    // recordIds are unique, and there are far more of them than partitions, so cuts cannot
    // repeat. The consumer treats a repeat as a breach of this contract and alerts.
    checkState(
        cuts.get(0) > 0 && Comparators.isInStrictOrder(cuts, Comparator.naturalOrder()),
        "partition boundaries must be positive and strictly increasing");

    ImmutableList.Builder<PartitionRange> partitions = ImmutableList.builder();
    for (int i = 0; i <= cuts.size(); i++) {
      partitions.add(
          new PartitionRange(
              i == 0 ? Optional.empty() : Optional.of(cuts.get(i - 1)),
              i == cuts.size() ? Optional.empty() : Optional.of(cuts.get(i))));
    }
    return partitions.build();
  }

  /**
   * Draws samples until enough of them resolve, or the attempt budget runs out.
   *
   * @return the resolved recordIds, or empty if no draw resolved {@link #MIN_RESOLVED_SAMPLES}
   */
  private Optional<List<Long>> sampleAndResolveWithRedraw() {
    for (int attempt = 1; attempt <= MAX_SAMPLE_ATTEMPTS; attempt++) {
      Optional<List<Long>> resolved = sampleAndResolveRecordIds();
      // A short $sample means the collection is smaller than the estimate, which redrawing
      // cannot change. Too few resolving means documents went away in between, which it can.
      if (resolved.isEmpty()) {
        return Optional.empty();
      }
      if (resolved.get().size() >= MIN_RESOLVED_SAMPLES) {
        return resolved;
      }
      this.logger
          .atInfo()
          .addKeyValue("resolved", resolved.get().size())
          .addKeyValue("attempt", attempt)
          .log("Too few sampled documents resolved, redrawing the sample.");
    }
    return Optional.empty();
  }

  /**
   * Samples {@code _id}s and resolves them to the recordIds the partition boundaries are cut on.
   *
   * @return the resolved recordIds, or empty if {@code $sample} itself came back short, which means
   *     the collection shrank since the size estimate
   */
  private Optional<List<Long>> sampleAndResolveRecordIds() {
    List<RawBsonDocument> sampled =
        this.samplingClient.getSamples(
            namespace(),
            ReadConcern.MAJORITY,
            SAMPLE_SIZE,
            Optional.empty(),
            Optional.of(ID_PROJECTION));
    if (sampled.size() < SAMPLE_SIZE) {
      this.logger
          .atInfo()
          .addKeyValue("sampled", sampled.size())
          .addKeyValue("sampleSize", SAMPLE_SIZE)
          .log("Sample came back short, collection is too small to partition.");
      return Optional.empty();
    }

    List<BsonValue> ids = sampled.stream().map(document -> document.get(ID_FIELD)).toList();
    return Optional.of(
        this.planningClient.resolveRecordIds(
            namespace(), ids, this.context.getChangeStreamResumeOperationTime()));
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

      // Steps 4 and 5: sample _ids, then resolve them to the recordIds the cuts are made on.
      Optional<List<Long>> resolved = sampleAndResolveWithRedraw();
      if (resolved.isEmpty()) {
        return new SequentialFallback(ShortCircuitReason.TOO_FEW_DOCUMENTS);
      }
      List<Long> recordIds = resolved.get();

      // Step 6: cut at K-1 evenly spaced quantiles of the sorted recordIds.
      int partitionCount =
          Math.clamp(
              this.context.getParallelPartitionCount(), MIN_PARTITION_COUNT, MAX_PARTITION_COUNT);

      return new PartitionedResult(
          partitionsFrom(recordIds.stream().sorted().toList(), partitionCount),
          initialSyncId.get());
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
