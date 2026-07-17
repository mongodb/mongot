package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.xgen.mongot.index.IndexTypeData.getIndexTypeTag;
import static com.xgen.mongot.index.IndexTypeData.getNumGauge;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor.ReplicationMeterData.ChangeStreamMeterData;
import com.xgen.mongot.metrics.ServerStatusDataExtractor.Scope;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.DocumentEventBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.PartialEmbedMetricCounters;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SyncChangeStreamDispatcher implements ChangeStreamDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(SyncChangeStreamDispatcher.class);

  private final ChangeStreamMongoClientFactory syncMongoClientFactory;
  private final NamedScheduledExecutorService executorService;

  private final Counter applicableUpdatesCounter;
  private final Counter witnessedUpdatesCounter;
  private final Counter skippedDocumentsWithoutMetadataNamespaceCounter;
  private final PartialEmbedMetricCounters partialEmbedMetricCounters;
  private final AtomicLong syncClientUp;
  /* Tracks count and duration of in progress getMore operations (server calls only) */
  private final AtomicLong getMoresInFlight;
  private final Timer getMoresInFlightTimer;
  /* Tracks count and duration of queued getMore operations */
  private final ImmutableMap<IndexTypeData.IndexTypeTag, AtomicLong> getMoresScheduled;
  private final Timer getMoresSchedulingTimer;
  /* Tracks the overall count and duration of change-stream batches processing
  (includes: server call, preprocessing and indexing)  */
  private final ImmutableMap<IndexTypeData.IndexTypeTag, AtomicLong> batchesInProgressTotal;
  private final Timer batchesInProgressTotalTimer;
  /* Tracks the total number of change-stream ended exceptionally */
  private final Counter unexpectedBatchFailures;
  /* Tracks duration of preprocessing a batch */
  private final Timer preprocessingBatchTimer;
  /* Tracks the total number of embedding getMore batches rescheduled due to the
  maxInFlightEmbeddingGetMores limit */
  private final Counter rescheduledEmbeddingGetMores;
  /* Max number of concurrent embedding getMores in-flight.
  Used as the initial number of permits for the concurrentEmbeddingGetMores semaphore. */
  private final Optional<Integer> maxInFlightEmbeddingGetMores;
  /* Optionally limits the number of concurrent embedding getMore batches in flight */
  private final Optional<Semaphore> concurrentEmbeddingGetMores;

  private volatile boolean shutdown;

  public SyncChangeStreamDispatcher(
      MeterRegistry meterRegistry,
      ChangeStreamMongoClientFactory syncMongoClientFactory,
      NamedScheduledExecutorService executorService,
      Optional<Integer> maxInFlightEmbeddingGetMores,
      CommonReplicationConfig.Type type) {
    this.syncMongoClientFactory = syncMongoClientFactory;
    this.executorService = executorService;
    this.shutdown = false;

    MetricsFactory metricsFactory =
        new MetricsFactory("indexing.steadyStateChangeStream", meterRegistry);
    Tag replicationTypeTag = Tag.of("replicationType", type.name());
    this.getMoresInFlight =
        metricsFactory.numGauge("getMoresInFlight", Tags.of(replicationTypeTag));
    // TODO(CLOUDP-289914): Remove this getMoresInFlightTimer after switching to new one.
    this.getMoresInFlightTimer = metricsFactory.timerSec("getMoreDurations");
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
    this.unexpectedBatchFailures = metricsFactory.counter("unexpectedBatchFailures");
    this.preprocessingBatchTimer = metricsFactory.timerSec("preprocessingBatchDurations");
    this.rescheduledEmbeddingGetMores = metricsFactory.counter("rescheduledEmbeddingGetMores");

    Tags replicationTag = Tags.of(Scope.REPLICATION.getTag());
    this.witnessedUpdatesCounter =
        metricsFactory.counter(ChangeStreamMeterData.WITNESSED_UPDATES, replicationTag);
    this.applicableUpdatesCounter =
        metricsFactory.counter(ChangeStreamMeterData.APPLICABLE_UPDATES, replicationTag);
    this.skippedDocumentsWithoutMetadataNamespaceCounter =
        metricsFactory.counter("skippedChangeStreamDocumentsWithoutMetadataNamespace");
    this.partialEmbedMetricCounters =
        new PartialEmbedMetricCounters(metricsFactory, replicationTag);
    this.syncClientUp =
        metricsFactory.numGauge(
            "dispatcher",
            replicationTag.and("client", "synchronous-batch").and(replicationTypeTag));
    this.syncClientUp.incrementAndGet();

    this.maxInFlightEmbeddingGetMores = maxInFlightEmbeddingGetMores;
    this.concurrentEmbeddingGetMores = maxInFlightEmbeddingGetMores.map(Semaphore::new);
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

    this.enqueue(new ChangeStreamIndexingInfo(indexManager, mongoClient));
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down.");

    this.shutdown = true;
    Executors.shutdownOrFail(this.executorService);
    this.syncClientUp.decrementAndGet();
  }

  private void enqueue(ChangeStreamIndexingInfo info) throws SteadyStateException {
    try {
      info.schedulingTimer.reset().start();

      this.executorService.submit(() -> doGetMore(info));

      // Update metrics only on successful submission
      this.getMoresScheduled
          .get(getIndexTypeTag(info.indexManager.indexDefinition))
          .incrementAndGet();
    } catch (RejectedExecutionException ex) {
      if (this.shutdown) {
        // Task submissions to executor will fail once ExecutorService::shutdown()
        // is called. In such case gracefully close open resources and exist.
        // Higher level logic will handle closing index managers and resume.
        closeMongoClient("Closing client on shutdown for index", info, Optional.empty());
        return;
      }

      GenerationId generationId = info.indexManager.getGenerationId();
      LOG.atError()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .log("Failed to submit index");
      throw SteadyStateException.createTransient(ex, "Submit failure");
    }
  }

  private void doGetMore(ChangeStreamIndexingInfo info) {
    if (info.indexManager.indexDefinition.isAutoEmbeddingIndex()
        && this.concurrentEmbeddingGetMores.isPresent()) {
      if (!this.concurrentEmbeddingGetMores.get().tryAcquire()) {
        // If we cannot acquire the semaphore, reschedule the batch for later.
        GenerationId generationId = info.indexManager.getGenerationId();
        LOG.atDebug()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .log(
                "Max embedding getMores already in progress: {}. "
                    + "Attempting to reschedule batch in 1 second",
                this.maxInFlightEmbeddingGetMores.get());
        try {
          this.executorService.schedule(() -> doGetMore(info), 1, TimeUnit.SECONDS);
          this.rescheduledEmbeddingGetMores.increment();
        } catch (RejectedExecutionException ex) {
          // If we cannot reschedule the batch, close the client and exit. This exception may only
          // happen during shutdown because the executor service uses an unbounded queue.
          if (this.shutdown) {
            closeMongoClient(
                "Failed to reschedule auto-embedding batch on executor service due to shutdown.",
                info,
                Optional.empty());
          } else {
            LOG.atError()
                .addKeyValue("indexId", generationId.indexId)
                .addKeyValue("generationId", generationId)
                .log(
                    "Failed to reschedule auto-embedding batch despite dispatcher being active. "
                        + "This should never happen.");
          }
        }
        return;
      }
    }

    try {
      this.getMoresScheduled
          .get(getIndexTypeTag(info.indexManager.indexDefinition))
          .decrementAndGet();
      this.getMoresSchedulingTimer.record(info.schedulingTimer.stop().elapsed());

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

        var batch =
            withMeasurement(
                info.mongoClient,
                ChangeStreamMongoClient::getNext,
                info.indexManager
                    .indexMetricsUpdater
                    .getReplicationMetricsUpdater()
                    .getSteadyStateMetrics());

        var batchInfo =
            info.indexManager.indexBatch(
                batch, this::updateBatchCounters, this.preprocessingBatchTimer);

        if (batchInfo.lifecycleEvent.isEmpty()) {
          enqueue(info);
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
        this.batchesInProgressTotalTimer.record(getNextTimer.stop().elapsed());
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
    } finally {
      // Release the concurrentEmbeddingGetMores semaphore in case of early return due to shutdown.
      if (info.indexManager.indexDefinition.isAutoEmbeddingIndex()
          && this.concurrentEmbeddingGetMores.isPresent()) {
        this.concurrentEmbeddingGetMores.get().release();
      }
    }
  }

  private Throwable unwrapExecutionException(Throwable throwable) {
    return (throwable instanceof ExecutionException) && (throwable.getCause() != null)
        ? throwable.getCause()
        : throwable;
  }

  private void updateBatchCounters(DocumentEventBatch batch) {
    this.applicableUpdatesCounter.increment(batch.updatesApplicable);
    this.witnessedUpdatesCounter.increment(batch.updatesWitnessed);
    this.skippedDocumentsWithoutMetadataNamespaceCounter.increment(
        batch.skippedDocumentsWithoutMetadataNamespace);
    this.partialEmbedMetricCounters.recordBatch(batch);
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
      this.getMoresInFlightTimer.record(duration);
      metricsUpdater.getBatchGetMoreTimer().record(duration);
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
    return this.concurrentEmbeddingGetMores.map(Semaphore::availablePermits);
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
