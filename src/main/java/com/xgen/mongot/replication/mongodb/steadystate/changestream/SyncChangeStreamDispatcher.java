package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.xgen.mongot.index.IndexTypeData.INDEX_TYPE_TAG_NAME;
import static com.xgen.mongot.index.IndexTypeData.getIndexTypeTag;
import static com.xgen.mongot.index.IndexTypeData.getNumGauge;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor.ReplicationMeterData.ChangeStreamMeterData;
import com.xgen.mongot.metrics.ServerStatusDataExtractor.Scope;
import com.xgen.mongot.metrics.Timed;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.DocumentEventBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.MongoExceptionUtils;
import com.xgen.mongot.replication.mongodb.common.OverloadRejectionMetrics;
import com.xgen.mongot.replication.mongodb.common.PartialEmbedMetricCounters;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.common.ViewFieldDependenciesMetricCounters;
import com.xgen.mongot.util.concurrent.Sleeper;
import com.xgen.mongot.util.mongodb.Errors;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ChangeStreamDispatcher} that dispatches getMore tasks through a {@link
 * ChangeStreamTaskExecutor}. The executor implementation determines scheduling behavior (immediate,
 * priority-aware, in-flight-limited, etc.); this class only owns the getMore lifecycle and metrics.
 */
final class SyncChangeStreamDispatcher implements ChangeStreamDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(SyncChangeStreamDispatcher.class);

  private static final double NANOS_PER_SECOND = 1_000_000_000.0;

  // Explicit histogram buckets (seconds) for getMore latency, so fleet-wide percentiles can be
  // computed with histogram_quantile (CLOUDP-425971). getMore latency is bimodal: batches with
  // events waiting return quickly, while an idle change stream waits out our own maxTimeMS
  // (changeStreamMaxTimeMs, 1s by default) and returns empty. The buckets are therefore dense
  // around 1s to separate those two modes, and extend past it because anything materially beyond
  // maxTimeMS means the getMore was delayed before mongod started awaiting -- which is how
  // execution-control deprioritization shows up, since it adds ticket wait rather than returning
  // an error.
  private static final double[] GET_MORE_DURATION_BUCKETS_SECONDS = {
    0.01, 0.05, 0.1, 0.25, 0.5, 0.7, 0.8, 0.9, 0.95, 1.0, 1.1, 1.5, 2.0, 3.0, 5.0, 10.0
  };

  /**
   * A getMore rejected at admission by Mongotune ingress rate limiting (462) never runs, so the
   * change stream cursor is still valid and we retry the same cursor on a short backoff instead of
   * failing the index's lifecycle.
   */
  @VisibleForTesting static final int MAX_THROTTLING_RETRIES = 5;

  /** Base delay for throttling retries; the actual delay is uniform over [0, base). */
  @VisibleForTesting static final Duration THROTTLING_RETRY_BACKOFF = Duration.ofMillis(100);

  private static final EnumSet<IndexTypeData.IndexTypeTag> AUTO_EMBEDDING_INDEX_TYPE_TAGS =
      EnumSet.of(
          IndexTypeData.IndexTypeTag.TAG_SEARCH_AUTO_EMBEDDING,
          IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH_AUTO_EMBEDDING);

  private final ChangeStreamMongoClientFactory syncMongoClientFactory;
  private final ChangeStreamTaskExecutor taskExecutor;
  private final boolean retryThrottledGetMoreOnSameCursor;
  private final Sleeper sleeper;

  private final Counter applicableUpdatesCounter;
  private final Counter witnessedUpdatesCounter;
  private final Counter skippedDocumentsWithoutMetadataNamespaceCounter;
  private final ImmutableMap<IndexTypeData.IndexTypeTag, PartialEmbedMetricCounters>
      partialEmbedMetricCounters;
  private final ImmutableMap<IndexTypeData.IndexTypeTag, ViewFieldDependenciesMetricCounters>
      viewFieldDependenciesMetricCounters;
  private final AtomicLong syncClientUp;
  /* Tracks count and duration of in progress getMore operations (server calls only) */
  private final AtomicLong getMoresInFlight;
  private final Timer getMoresInFlightTimer;
  /* Mergeable histogram of getMore latency (seconds) with explicit buckets around the 1s
  load-shedding threshold; supports fleet-wide histogram_quantile() unlike getMoresInFlightTimer. */
  private final DistributionSummary getMoreDurationHistogram;
  /* Tracks count and duration of queued getMore operations */
  private final ImmutableMap<IndexTypeData.IndexTypeTag, AtomicLong> getMoresScheduled;
  private final Timer getMoresSchedulingTimer;
  /* Tracks the overall count and duration of change-stream batches processing
  (includes: server call, preprocessing and indexing)  */
  private final ImmutableMap<IndexTypeData.IndexTypeTag, AtomicLong> batchesInProgressTotal;
  private final Timer batchesInProgressTotalTimer;
  /* Same total batch duration as batchesInProgressTotalTimer, but tagged by batch fullness
  (empty/partial/full). The per-fullness sample count also gives the empty/partial/full batch
  counts, surfacing empty (idle) and full (backlog) batches (CLOUDP-425971). */
  private final ImmutableMap<ChangeStreamIndexManager.BatchFullness, Timer>
      batchDurationsByFullness;
  /* Tracks the total number of change-stream ended exceptionally */
  private final Counter unexpectedBatchFailures;
  /* Tracks getMores retried on the same cursor after being throttled by mongod */
  private final Counter throttlingRetries;
  /* Tracks getMores that exhausted MAX_THROTTLING_RETRIES and failed the batch. Distinct from
  throttlingRetries, which counts attempts and so cannot distinguish "succeeded on the last retry"
  from "gave up". */
  private final Counter throttlingRetriesExhausted;
  /* Counts every overload rejection, independent of the retry flag and of the retry's outcome */
  private final OverloadRejectionMetrics overloadRejections;
  /* Tracks duration of preprocessing a batch */
  private final Timer preprocessingBatchTimer;

  private volatile boolean shutdown;

  /**
   * Convenience constructor with {@link Feature#RETRY_THROTTLED_GETMORE_ON_SAME_CURSOR} off, for
   * tests that do not exercise the flag-gated retry.
   */
  @VisibleForTesting
  SyncChangeStreamDispatcher(
      MeterRegistry meterRegistry,
      ChangeStreamMongoClientFactory syncMongoClientFactory,
      ChangeStreamTaskExecutor taskExecutor,
      CommonReplicationConfig.Type type) {
    this(meterRegistry, syncMongoClientFactory, taskExecutor, type, false);
  }

  /** Convenience constructor that sleeps for real between throttling retries. */
  public SyncChangeStreamDispatcher(
      MeterRegistry meterRegistry,
      ChangeStreamMongoClientFactory syncMongoClientFactory,
      ChangeStreamTaskExecutor taskExecutor,
      CommonReplicationConfig.Type type,
      boolean retryThrottledGetMoreOnSameCursor) {
    this(
        meterRegistry,
        syncMongoClientFactory,
        taskExecutor,
        type,
        retryThrottledGetMoreOnSameCursor,
        Sleeper.DEFAULT);
  }

  public SyncChangeStreamDispatcher(
      MeterRegistry meterRegistry,
      ChangeStreamMongoClientFactory syncMongoClientFactory,
      ChangeStreamTaskExecutor taskExecutor,
      CommonReplicationConfig.Type type,
      boolean retryThrottledGetMoreOnSameCursor,
      Sleeper sleeper) {
    this.syncMongoClientFactory = syncMongoClientFactory;
    this.taskExecutor = taskExecutor;
    this.sleeper = sleeper;
    this.retryThrottledGetMoreOnSameCursor = retryThrottledGetMoreOnSameCursor;
    this.shutdown = false;

    MetricsFactory metricsFactory =
        new MetricsFactory("indexing.steadyStateChangeStream", meterRegistry);
    Tag replicationTypeTag = Tag.of("replicationType", type.name());
    Tags replicationTypeTags = Tags.of(replicationTypeTag);
    this.getMoresInFlight = metricsFactory.numGauge("getMoresInFlight", replicationTypeTags);
    // TODO(CLOUDP-426135): remove this timer (and the per-index getPerIndexGetMoreTimer) in favor
    // of the mergeable getMoreDurationSeconds histogram below, and migrate dashboards to it. Kept
    // for now so this metrics-add stays additive. Subsumes CLOUDP-289914.
    this.getMoresInFlightTimer = metricsFactory.timerSec("getMoreDurations");
    this.getMoreDurationHistogram =
        metricsFactory.histogram(
            "getMoreDurationSeconds", replicationTypeTags, GET_MORE_DURATION_BUCKETS_SECONDS);
    // Both maps are keyed over every IndexTypeTag: looked up unchecked by getIndexTypeTag, so a
    // missing tag would NPE the first batch of that index type.
    String getMoresScheduledName = "getMoresScheduled";
    this.getMoresScheduled =
        Maps.toMap(
            EnumSet.allOf(IndexTypeData.IndexTypeTag.class),
            tag -> getNumGauge(metricsFactory, getMoresScheduledName, tag, replicationTypeTag));
    this.getMoresSchedulingTimer = metricsFactory.timerSec("getMoresSchedulingDurations");
    String batchesInProgressTotalName = "batchesInProgressTotal";
    this.batchesInProgressTotal =
        Maps.toMap(
            EnumSet.allOf(IndexTypeData.IndexTypeTag.class),
            tag ->
                getNumGauge(metricsFactory, batchesInProgressTotalName, tag, replicationTypeTag));
    this.batchesInProgressTotalTimer = metricsFactory.timerSec("batchesInProgressTotalDurations");
    this.batchDurationsByFullness =
        Maps.toMap(
            EnumSet.allOf(ChangeStreamIndexManager.BatchFullness.class),
            fullness ->
                metricsFactory.timer(
                    "batchDurationsByFullness",
                    replicationTypeTags.and("fullness", fullness.tagValue()),
                    TimeUnit.SECONDS));
    this.unexpectedBatchFailures = metricsFactory.counter("unexpectedBatchFailures");
    this.throttlingRetries =
        metricsFactory.counter("getMoreThrottlingRetries", replicationTypeTags);
    this.throttlingRetriesExhausted =
        metricsFactory.counter("getMoreThrottlingRetriesExhausted", replicationTypeTags);
    this.overloadRejections =
        OverloadRejectionMetrics.create(meterRegistry, OverloadRejectionMetrics.Phase.STEADY_STATE);
    this.preprocessingBatchTimer = metricsFactory.timerSec("preprocessingBatchDurations");

    Tags replicationTag = Tags.of(Scope.REPLICATION.getTag());
    this.witnessedUpdatesCounter =
        metricsFactory.counter(ChangeStreamMeterData.WITNESSED_UPDATES, replicationTag);
    this.applicableUpdatesCounter =
        metricsFactory.counter(ChangeStreamMeterData.APPLICABLE_UPDATES, replicationTag);
    this.skippedDocumentsWithoutMetadataNamespaceCounter =
        metricsFactory.counter("skippedChangeStreamDocumentsWithoutMetadataNamespace");
    this.partialEmbedMetricCounters =
        Maps.toMap(
            AUTO_EMBEDDING_INDEX_TYPE_TAGS,
            tag ->
                new PartialEmbedMetricCounters(
                    metricsFactory, replicationTag.and(INDEX_TYPE_TAG_NAME, tag.tagValue)));
    this.viewFieldDependenciesMetricCounters =
        Maps.toMap(
            AUTO_EMBEDDING_INDEX_TYPE_TAGS,
            tag ->
                new ViewFieldDependenciesMetricCounters(
                    metricsFactory, replicationTag.and(INDEX_TYPE_TAG_NAME, tag.tagValue)));
    this.syncClientUp =
        metricsFactory.numGauge(
            "dispatcher",
            replicationTag.and("client", "synchronous-batch").and(replicationTypeTag));
    this.syncClientUp.incrementAndGet();
  }

  @Override
  public void add(
      IndexDefinition indexDefinition,
      GenerationId generationId,
      ChangeStreamResumeInfo resumeInfo,
      ChangeStreamIndexManager indexManager,
      boolean removeMatchCollectionUuid)
      throws SteadyStateException {

    ChangeStreamMongoClient<SteadyStateException> mongoClient =
        this.syncMongoClientFactory.resumeTimedModeAwareChangeStream(
            generationId, resumeInfo, indexDefinition, removeMatchCollectionUuid);

    this.taskExecutor.addIndex(indexDefinition.getIndexId());

    this.enqueue(new ChangeStreamIndexingInfo(indexManager, mongoClient), Optional.empty());
  }

  @Override
  public void removeIndex(ObjectId indexId) {
    this.taskExecutor.removeIndex(indexId);
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down.");

    this.shutdown = true;
    this.taskExecutor.shutdown();
    this.syncClientUp.decrementAndGet();
  }

  private void enqueue(ChangeStreamIndexingInfo info, Optional<ChangeStreamBatch> previousBatch) {
    info.schedulingTimer.reset().start();
    AtomicLong scheduledGauge =
        this.getMoresScheduled.get(getIndexTypeTag(info.indexManager.indexDefinition));
    // Increment before enqueue because an accepted task may begin running immediately on another
    // thread. Either doGetMore or onDropped performs the matching decrement.
    scheduledGauge.incrementAndGet();

    EnqueueContext context =
        new EnqueueContext(
            info.indexManager.indexDefinition,
            previousBatch,
            () -> {
              scheduledGauge.decrementAndGet();
              closeMongoClient(
                  "Closing client after task was dropped by executor", info, Optional.empty());
            });

    this.taskExecutor.enqueue(() -> doGetMore(info), context);
  }

  private void doGetMore(ChangeStreamIndexingInfo info) {
    this.getMoresScheduled
        .get(getIndexTypeTag(info.indexManager.indexDefinition))
        .decrementAndGet();
    Duration schedulingElapsed = info.schedulingTimer.stop().elapsed();
    Timed.recordSampleIgnoringMetricsFailure(this.getMoresSchedulingTimer, schedulingElapsed);
    var steadyStateMetrics =
        info.indexManager
            .indexMetricsUpdater
            .getReplicationMetricsUpdater()
            .getSteadyStateMetrics();
    steadyStateMetrics.getBatchSchedulingTimer().record(schedulingElapsed);

    if (this.shutdown) {
      // Gracefully close open resources on shutdown and skip further processing.
      // ExecutorService guarantees orderly shutdown in which previously submitted
      // tasks are drained, but no new tasks will be accepted.
      closeMongoClient("Closing client on shutdown for index", info, Optional.empty());
      return;
    }

    if (info.indexManager.isShutdown()) {
      // Close the client cleaning open resources and skip batch fetching for stopped indexes.
      closeMongoClient("Closing client for index", info, Optional.empty());
      return;
    }

    try {
      this.batchesInProgressTotal
          .get(getIndexTypeTag(info.indexManager.indexDefinition))
          .incrementAndGet();

      var getNextTimer = Stopwatch.createStarted();

      var batch = getNextRetryingThrottled(info);

      var batchInfo =
          info.indexManager.indexBatch(
              batch,
              counters -> updateBatchCounters(counters, info.indexManager.indexDefinition),
              this.preprocessingBatchTimer);

      if (batchInfo.lifecycleEvent.isEmpty()) {
        enqueue(info, Optional.of(batch));
      } else {
        // On lifecycle event `lifecycleFuture` will get completed exceptionally and its index
        // manager will get closed. We need to close the client here.
        closeMongoClient(
            "Closing client for index", info, Optional.of(batchInfo.lifecycleEvent.get()));
      }

      // To limit concurrent queries against a mongod server we cap the number of threads calling
      // doGetMore(). While we could send additional getMore() operations, by processing indexing
      // results asynchronously, wait for indexing to complete to avoid OOM'ing mongot in case
      // getMore() are faster than indexing.
      batchInfo.indexingFuture.get();

      // Record the total duration, excluding failures.
      Duration batchDuration = getNextTimer.stop().elapsed();
      Timed.recordSampleIgnoringMetricsFailure(this.batchesInProgressTotalTimer, batchDuration);
      // Also record it tagged by batch fullness. The fullness is classified on the decoding thread
      // and is set by the time indexingFuture completes; guard against null defensively.
      ChangeStreamIndexManager.BatchFullness fullness = batchInfo.fullness.get();
      if (fullness != null) {
        Timed.recordSampleIgnoringMetricsFailure(
            this.batchDurationsByFullness.get(fullness), batchDuration);
      }
    } catch (Exception ex) {
      GenerationId generationId = info.indexManager.getGenerationId();
      LOG.atError()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .setCause(ex)
          .log("Failed fetching or processing a change stream batch for index");

      if (this.shutdown || info.indexManager.isShutdown()) {
        // A failure occurred during initiated shutdown, we don't have to worry about
        // failing the index (it's done at a higher level). This also covers the case where the
        // replication manager closed the shared mongo client to abort an in-flight getMore: the
        // index manager is already marked shut down even though the dispatcher's own shutdown
        // flag has not been set yet, so we must not fail its lifecycle for the expected failure.
        LOG.atInfo()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .log(
                "batch failure during shutdown, we do not need to close the index manager for "
                    + "the index");
        // Either batch fetching from upstream or indexing failed with an exception. In both
        // cases close the client and let higher level logic handle the index.
        closeMongoClient("Closing client for {}", info, Optional.empty());
        return;
      }

      // Cancel any remaining work and fail the lifecycle future. Note, lifecycleEvents from
      // the batch will be handled by the index manager and the indexingFuture will complete
      // successfully.
      this.unexpectedBatchFailures.increment();
      // failLifecycle must be called before closing mongo client to make sure the real lifecycle
      // exception is persisted. Otherwise, the index might be failed/staled due to "Cursor has
      // been
      // closed" exception.
      info.indexManager.failLifecycle(unwrapExecutionException(ex));

      // Either batch fetching from upstream or indexing failed with an exception. In both
      // cases close the client and let higher level logic handle the index.
      closeMongoClient("Closing client for {}", info, Optional.empty());
    } finally {
      this.batchesInProgressTotal
          .get(getIndexTypeTag(info.indexManager.indexDefinition))
          .decrementAndGet();
    }
  }

  /**
   * Fetches the next batch, retrying the <em>same</em> cursor when mongod rejects the getMore due
   * to load shedding (see {@link Errors#THROTTLING_ERROR_CODES}).
   *
   * <p>Rethrows the last exception once {@link #MAX_THROTTLING_RETRIES} is exhausted, incrementing
   * {@code getMoreThrottlingRetriesExhausted} so an exhausted budget is distinguishable from a
   * retry that succeeded.
   */
  private ChangeStreamBatch getNextRetryingThrottled(ChangeStreamIndexingInfo info)
      throws SteadyStateException {
    var steadyStateMetrics =
        info.indexManager
            .indexMetricsUpdater
            .getReplicationMetricsUpdater()
            .getSteadyStateMetrics();

    for (int retries = 0; ; retries++) {
      try {
        return withMeasurement(
            info.mongoClient, ChangeStreamMongoClient::getNext, steadyStateMetrics);
      } catch (SteadyStateException ex) {
        // Counted before any of the guards below, so the metric reflects how often mongod shed a
        // replication request regardless of the feature flag or what we do about it.
        this.overloadRejections.recordIfOverload(ex);

        boolean shuttingDown = this.shutdown || info.indexManager.isShutdown();
        boolean retryable =
            this.retryThrottledGetMoreOnSameCursor
                && MongoExceptionUtils.isThrottling(ex)
                && !shuttingDown;

        if (!retryable || retries >= MAX_THROTTLING_RETRIES) {
          // Only an exhausted budget counts: the other paths never intended to retry.
          if (retryable) {
            this.throttlingRetriesExhausted.increment();
            LOG.atWarn()
                .addKeyValue("indexId", info.indexManager.getGenerationId().indexId)
                .addKeyValue("retries", retries)
                .setCause(ex)
                .log("exhausted throttling retries for getMore, failing the batch");
          }
          throw ex;
        }

        GenerationId generationId = info.indexManager.getGenerationId();
        LOG.atInfo()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .addKeyValue("retry", retries + 1)
            .setCause(ex)
            .log("getMore was throttled by mongod, retrying on the same cursor");

        this.throttlingRetries.increment();
        sleepWithFullJitter(THROTTLING_RETRY_BACKOFF);
      }
    }
  }

  /** Sleeps for a duration drawn uniformly from [0, backoff). */
  private void sleepWithFullJitter(Duration backoff) throws SteadyStateException {
    try {
      this.sleeper.sleep(
          Duration.ofMillis(ThreadLocalRandom.current().nextLong(backoff.toMillis())));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw SteadyStateException.createShutDown();
    }
  }

  private Throwable unwrapExecutionException(Throwable throwable) {
    return (throwable instanceof ExecutionException) && (throwable.getCause() != null)
        ? throwable.getCause()
        : throwable;
  }

  private void updateBatchCounters(DocumentEventBatch batch, IndexDefinition indexDefinition) {
    this.applicableUpdatesCounter.increment(batch.updatesApplicable);
    this.witnessedUpdatesCounter.increment(batch.updatesWitnessed);
    this.skippedDocumentsWithoutMetadataNamespaceCounter.increment(
        batch.skippedDocumentsWithoutMetadataNamespace);
    if (MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDefinition)) {
      IndexTypeData.IndexTypeTag indexTypeTag = getIndexTypeTag(indexDefinition);
      this.partialEmbedMetricCounters.get(indexTypeTag).recordBatch(batch);
      this.viewFieldDependenciesMetricCounters.get(indexTypeTag).recordBatch(batch);
    }
  }

  private ChangeStreamBatch withMeasurement(
      ChangeStreamMongoClient<SteadyStateException> mongoClient,
      GetMoreProducer getMoreProducer,
      IndexMetricsUpdater.ReplicationMetricsUpdater.SteadyStateMetrics metricsUpdater)
      throws SteadyStateException {
    this.getMoresInFlight.incrementAndGet();
    var timer = Stopwatch.createStarted();

    try {
      var batch = getMoreProducer.apply(mongoClient);
      Duration duration = timer.stop().elapsed();
      Timed.recordSampleIgnoringMetricsFailure(this.getMoresInFlightTimer, duration);
      this.getMoreDurationHistogram.record(duration.toNanos() / NANOS_PER_SECOND);
      metricsUpdater.getBatchGetMoreTimer().record(duration);
      metricsUpdater.getPerIndexGetMoreTimer().record(duration);
      return batch;
    } finally {
      this.getMoresInFlight.decrementAndGet();
    }
  }

  private void closeMongoClient(
      String logMessage, ChangeStreamIndexingInfo info, Optional<Throwable> throwable) {
    GenerationId generationId = info.indexManager.getGenerationId();
    if (throwable.isPresent()) {
      LOG.atInfo()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .setCause(throwable.get())
          .log(logMessage);
    } else {
      LOG.atInfo()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .log(logMessage);
    }

    info.mongoClient.close();
  }

  @TestOnly
  @VisibleForTesting
  Optional<Integer> getEmbeddingAvailablePermits() {
    if (this.taskExecutor instanceof AutoEmbeddingExecutor autoEmbedding) {
      return autoEmbedding.getEmbeddingAvailablePermits();
    }
    return Optional.empty();
  }

  @TestOnly
  @VisibleForTesting
  Class<? extends ChangeStreamTaskExecutor> getTaskExecutorClass() {
    return this.taskExecutor.getClass();
  }

  @FunctionalInterface
  private interface GetMoreProducer {

    ChangeStreamBatch apply(ChangeStreamMongoClient<SteadyStateException> client)
        throws SteadyStateException;
  }

  private static class ChangeStreamIndexingInfo {

    final ChangeStreamIndexManager indexManager;
    final ChangeStreamMongoClient<SteadyStateException> mongoClient;
    final Stopwatch schedulingTimer;

    ChangeStreamIndexingInfo(
        ChangeStreamIndexManager indexManager,
        ChangeStreamMongoClient<SteadyStateException> mongoClient) {
      this.indexManager = indexManager;
      this.mongoClient = mongoClient;
      this.schedulingTimer = Stopwatch.createUnstarted();
    }
  }
}
