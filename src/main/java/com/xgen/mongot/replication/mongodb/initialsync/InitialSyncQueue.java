package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_SEARCH;
import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH;
import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH_AUTO_EMBEDDING;
import static com.xgen.mongot.index.IndexTypeData.getIndexTypeTag;
import static com.xgen.mongot.index.IndexTypeData.getNumGauge;
import static com.xgen.mongot.replication.mongodb.initialsync.InitialSyncManager.getFactory;
import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ClientSessionRecord;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.PauseInitialSyncException;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.concurrent.LockGuard;
import com.xgen.mongot.util.concurrent.OneShotSingleThreadExecutor;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InitialSyncQueue is responsible for the lifecycle of initial sync requests. Consumers can enqueue
 * indexes to be synced and cancel existing initial sync requests.
 *
 * <p>One instance is created at startup.
 */
@SuppressWarnings("GuardedBy") // Uses LockGuard instead
public class InitialSyncQueue {

  private static final Logger LOG = LoggerFactory.getLogger(InitialSyncDispatcher.class);

  private final ReentrantLock queueLock;

  private final Map<String, InitialSyncMongoClient> mongoClients;

  /** Queue for initial sync requests that is read from by the InitialSyncDispatcher. */
  private final String syncSourceHost;

  private final BlockingQueue<GenerationId> requestQueue;

  /** Map of pending initial syncs that is updated by the InitialSyncDispatcher. */
  @GuardedBy("queueLock")
  private final Map<GenerationId, InitialSyncRequest> queued;

  /** Set of index ids that are still in the requestQueue but have been cancelled. */
  @GuardedBy("queueLock")
  private final Set<GenerationId> cancelled;

  /** Map of in progress initial syncs that is updated by the InitialSyncDispatcher. */
  @GuardedBy("queueLock")
  private final Map<GenerationId, InProgressInitialSyncInfo> inProgress;

  private final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;

  /** Executes the initial syncs. */
  private final InitialSyncDispatcher dispatcher;

  /** A flag indicating whether the InitialSyncQueue has been shut down. */
  @GuardedBy("queueLock")
  private boolean shutdown;

  /**
   * Tracks the total number of times embedding indexes were re-queued due to the
   * concurrentEmbeddingSyncs semaphore.
   */
  private final Counter requeuedEmbeddingInitialSyncs;

  /**
   * The maximum number of concurrent embedding initial syncs allowed. Used as the initial number of
   * permits for the concurrentEmbeddingSyncs semaphore.
   */
  private final Optional<Integer> maxConcurrentEmbeddingInitialSyncs;

  /** A semaphore to limit the number of concurrent embedding initial syncs. */
  private final Optional<Semaphore> concurrentEmbeddingSyncs;

  /** We will return a shutdown exception for following index ids. */
  private final Set<ObjectId> pauseInitialSyncOnIndexIds;

  /** Map of queuedSyncs gauge for each {@link IndexTypeData.IndexTypeTag}. */
  protected final Map<IndexTypeData.IndexTypeTag, AtomicLong> queuedSyncs;

  /** Gate for pausing initial syncs. */
  private final Gate initialSyncGate;

  private final InitialSyncConfig initialSyncConfig;

  @VisibleForTesting
  InitialSyncQueue(
      MeterRegistry meterRegistry,
      Map<String, InitialSyncMongoClient> mongoClients,
      String syncSourceHost,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      BlockingQueue<GenerationId> requestQueue,
      Map<GenerationId, InitialSyncRequest> queued,
      Set<GenerationId> cancelled,
      Map<GenerationId, InProgressInitialSyncInfo> inProgress,
      MongoDbReplicationConfig replicationConfig,
      InitialSyncConfig initialSyncConfig,
      InitialSyncManagerFactory managerFactory,
      Path dataPath,
      Gate initialSyncGate) {
    this.mongoClients = mongoClients;
    this.syncSourceHost = syncSourceHost;
    this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
    this.requestQueue = requestQueue;
    this.queued = queued;
    this.cancelled = cancelled;
    this.inProgress = inProgress;

    this.queueLock = new ReentrantLock();
    this.shutdown = false;

    this.initialSyncConfig = initialSyncConfig;

    MetricsFactory metricsFactory = new MetricsFactory("initialsync.queue", meterRegistry);
    this.requeuedEmbeddingInitialSyncs = metricsFactory.counter("requeuedEmbeddingInitialSyncs");
    String queuedSyncsName = "queuedSyncs";
    this.queuedSyncs =
        Map.of(
            TAG_SEARCH,
            getNumGauge(metricsFactory, queuedSyncsName, TAG_SEARCH),
            TAG_VECTOR_SEARCH,
            getNumGauge(metricsFactory, queuedSyncsName, TAG_VECTOR_SEARCH),
            TAG_VECTOR_SEARCH_AUTO_EMBEDDING,
            getNumGauge(metricsFactory, queuedSyncsName, TAG_VECTOR_SEARCH_AUTO_EMBEDDING));

    boolean shouldLimitConcurrentEmbeddingInitialSyncs =
        replicationConfig.maxConcurrentEmbeddingInitialSyncs
            < replicationConfig.numConcurrentInitialSyncs;
    this.maxConcurrentEmbeddingInitialSyncs =
        shouldLimitConcurrentEmbeddingInitialSyncs
            ? Optional.of(replicationConfig.maxConcurrentEmbeddingInitialSyncs)
            : Optional.empty();
    this.concurrentEmbeddingSyncs = this.maxConcurrentEmbeddingInitialSyncs.map(Semaphore::new);

    this.pauseInitialSyncOnIndexIds = new HashSet<>(replicationConfig.pauseInitialSyncOnIndexIds);
    this.initialSyncGate = initialSyncGate;
    this.dispatcher =
        new InitialSyncDispatcher(meterRegistry, managerFactory, replicationConfig, dataPath);
    this.dispatcher.start();
  }

  /** Creates a new InitialSyncQueue, while also spawning a corresponding InitialSyncDispatcher. */
  public static InitialSyncQueue create(
      MeterRegistry meterRegistry,
      Map<String, ClientSessionRecord> mongoClientSessions,
      String syncSourceHost,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      MongoDbReplicationConfig replicationConfig,
      InitialSyncConfig initialSyncConfig,
      Path dataPath,
      Gate initialSyncGate) {
    Check.argIsPositive(replicationConfig.numConcurrentInitialSyncs, "numConcurrentInitialSyncs");
    Check.argIsPositive(
        replicationConfig.maxConcurrentEmbeddingInitialSyncs, "maxConcurrentEmbeddingInitialSyncs");

    Map<String, InitialSyncMongoClient> initialSyncMongoClients = new HashMap<>();
    mongoClientSessions.forEach(
        (host, clientSessionRecord) ->
            initialSyncMongoClients.put(
                host,
                DefaultInitialSyncMongoClient.create(
                    clientSessionRecord.syncMongoClient(),
                    clientSessionRecord.sessionRefresher(),
                    meterRegistry,
                    syncSourceHost)));

    MetricsFactory metricsFactory = new MetricsFactory("initialSyncManager", meterRegistry);
    InitialSyncManagerFactory factory =
        getFactory(initialSyncConfig, replicationConfig, metricsFactory);

    return create(
        meterRegistry,
        initialSyncMongoClients,
        syncSourceHost,
        indexingWorkSchedulerFactory,
        replicationConfig,
        initialSyncConfig,
        factory,
        dataPath,
        initialSyncGate);
  }

  @VisibleForTesting
  static InitialSyncQueue create(
      MeterRegistry meterRegistry,
      Map<String, InitialSyncMongoClient> mongoClients,
      String syncSourceHost,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      MongoDbReplicationConfig replicationConfig,
      InitialSyncConfig initialSyncConfig,
      InitialSyncManagerFactory managerFactory,
      Path dataPath,
      Gate initialSyncGate) {
    Check.argIsPositive(replicationConfig.numConcurrentInitialSyncs, "numConcurrentInitialSyncs");
    Check.argIsPositive(
        replicationConfig.maxConcurrentEmbeddingInitialSyncs, "maxConcurrentEmbeddingInitialSyncs");

    BlockingQueue<GenerationId> requestQueue = new LinkedBlockingQueue<>();
    Map<GenerationId, InitialSyncRequest> queued = new HashMap<>();
    Set<GenerationId> cancelled = new HashSet<>();
    Map<GenerationId, InProgressInitialSyncInfo> inProgress = new HashMap<>();

    return new InitialSyncQueue(
        meterRegistry,
        mongoClients,
        syncSourceHost,
        indexingWorkSchedulerFactory,
        requestQueue,
        queued,
        cancelled,
        inProgress,
        replicationConfig,
        initialSyncConfig,
        managerFactory,
        dataPath,
        initialSyncGate);
  }

  /**
   * Signals the InitialSyncQueue to halt all work scheduling.
   *
   * @return A future that completes when all currently scheduled work terminates.
   */
  public CompletableFuture<Void> shutdown() {
    try (LockGuard ignored = LockGuard.with(this.queueLock)) {
      LOG.info("Shutting down.");

      this.shutdown = true;

      // First shut down the dispatcher thread so no more work gets scheduled.
      this.dispatcher.interrupt();
      Crash.ifDoesNotJoin(this.dispatcher, Duration.ofSeconds(10));

      // Complete the queued initial syncs without removing them from the queued map so cancel() can
      // still be called on the initial sync if necessary.
      this.queued
          .values()
          .forEach(
              request ->
                  request
                      .getOnCompleteFuture()
                      .completeExceptionally(InitialSyncException.createShutDown()));

      this.inProgress.values().forEach(InProgressInitialSyncInfo::cancel);
      // We purposefully do not wait for the in progress futures to complete as they may need to
      // hold queueLock to do so.
      return FutureUtils.allOf(
          this.inProgress.values().stream()
              .map(info -> FutureUtils.swallowedFuture(info.getFuture()))
              .collect(Collectors.toList()));
    }
  }

  /**
   * Adds the supplied Index to the InitialSyncQueue. Once the Index has had its initial sync run,
   * either successHandler or exceptionHandler will be called depending on the result of the initial
   * sync.
   *
   * @param documentIndexer The DocumentIndexer used to index documents during the initial sync.
   * @param indexDefinition The IndexDefinition of the Index that is being synced.
   * @param initialSyncStartingHandler A callback to run when the initial sync has been picked up
   *     off the queue and is about to be run.
   * @param indexMetricsUpdater A metric updater class to handle both per-index and per-process
   *     metrics.
   * @param isNaturalOrderScanSupported indicates whether natural order scan is supported by current
   *     mongodb version and feature flag
   * @param removeMatchCollectionUuid A flag indicating if the matchCollectionUuidForUpdateLookup
   *     change stream parameter should be removed because it is unsupported.
   * @return a CompletableFuture that completes when the initial sync completes, potentially
   *     exceptionally
   * @throws InitialSyncException if the InitialSyncQueue has been shut down, or the corresponding
   *     index pauses initial sync.
   */
  public CompletableFuture<ChangeStreamResumeInfo> enqueue(
      DocumentIndexer documentIndexer,
      IndexDefinitionGeneration indexDefinition,
      Runnable initialSyncStartingHandler,
      IndexMetricsUpdater indexMetricsUpdater,
      boolean removeMatchCollectionUuid,
      boolean isNaturalOrderScanSupported)
      throws InitialSyncException {
    try (LockGuard ignored = LockGuard.with(this.queueLock)) {
      return doEnqueue(
          documentIndexer,
          indexDefinition,
          initialSyncStartingHandler,
          Optional.empty(),
          indexMetricsUpdater,
          removeMatchCollectionUuid,
          isNaturalOrderScanSupported);
    }
  }

  /**
   * Adds the supplied Index to the InitialSyncQueue for resume. Once the Index has had its initial
   * sync run, either successHandler or exceptionHandler will be called depending on the result of
   * the initial sync.
   *
   * @param documentIndexer The DocumentIndexer used to index documents during the initial sync.
   * @param indexDefinition The IndexDefinition of the Index that is being synced.
   * @param initialSyncStartingHandler A callback to run when the initial sync has been picked up
   *     off the queue and is about to be run.
   * @param resumeInfo An InitialSyncResumeInfo describing the point to resume initial sync from.
   * @param indexMetricsUpdater A metric updater class to handle both per-index and per-process
   *     metrics.
   * @param isNaturalOrderScanSupported indicates whether natural order scan is supported by current
   *     mongodb version and feature flag
   * @param removeMatchCollectionUuid A flag indicating if the matchCollectionUuidForUpdateLookup
   *     change stream parameter should be removed because it is unsupported.
   * @return a CompletableFuture that completes when the initial sync completes, potentially
   *     exceptionally
   * @throws InitialSyncException if the InitialSyncQueue has been shut down, or the corresponding
   *     index pauses initial sync.
   */
  public CompletableFuture<ChangeStreamResumeInfo> enqueueResume(
      DocumentIndexer documentIndexer,
      IndexDefinitionGeneration indexDefinition,
      Runnable initialSyncStartingHandler,
      InitialSyncResumeInfo resumeInfo,
      IndexMetricsUpdater indexMetricsUpdater,
      boolean removeMatchCollectionUuid,
      boolean isNaturalOrderScanSupported)
      throws InitialSyncException {
    return doEnqueue(
        documentIndexer,
        indexDefinition,
        initialSyncStartingHandler,
        Optional.of(resumeInfo),
        indexMetricsUpdater,
        removeMatchCollectionUuid,
        isNaturalOrderScanSupported);
  }

  private CompletableFuture<ChangeStreamResumeInfo> doEnqueue(
      DocumentIndexer documentIndexer,
      IndexDefinitionGeneration indexDefinition,
      Runnable initialSyncStartingHandler,
      @Var Optional<InitialSyncResumeInfo> resumeInfo,
      IndexMetricsUpdater indexMetricsUpdater,
      boolean removeMatchCollectionUuid,
      boolean isNaturalOrderScanSupported)
      throws InitialSyncException {
    try (LockGuard ignored = LockGuard.with(this.queueLock)) {

      if (this.shutdown) {
        throw InitialSyncException.createShutDown();
      }

      if (this.initialSyncGate.isClosed()) {
        throw InitialSyncException.createResumableTransient(
            new PauseInitialSyncException("Initial syncs are paused"));
      }
      GenerationId generationId = indexDefinition.getGenerationId();

      if (this.pauseInitialSyncOnIndexIds.contains(indexDefinition.getIndexId())) {
        LOG.atWarn()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .log("Initial sync for index is paused");
        throw InitialSyncException.createShutDown();
      }

      checkState(
          !this.cancelled.contains(generationId), "index %s has been cancelled", generationId);
      checkState(
          !this.queued.containsKey(generationId) && !this.inProgress.containsKey(generationId),
          "index %s is already enqueued",
          generationId);

      LOG.atInfo()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .log("Enqueueing initial sync for index");

      CompletableFuture<ChangeStreamResumeInfo> future = new CompletableFuture<>();

      @Var boolean useNaturalOrderScan = isNaturalOrderScanSupported;

      if (resumeInfo.isPresent()) {
        // Natural Order Scan won't be applied when resuming from _id scan
        if (resumeInfo.get().isBufferlessIdOrderInitialSyncResumeInfo()) {
          useNaturalOrderScan = false;
        }
        // If FF is set as false, we ensure _id scan will be applied
        if (resumeInfo.get().isBufferlessNaturalOrderInitialSyncResumeInfo()
            && !useNaturalOrderScan) {
          resumeInfo = Optional.empty();
        }
      }

      // TODO(CLOUDP-364699): Auto-embedding indexes do not support natural order scan for now.
      if (indexDefinition.getIndexDefinition().isAutoEmbeddingIndex()) {
        useNaturalOrderScan = false;
      }

      InitialSyncRequest request =
          new InitialSyncRequest(
              documentIndexer,
              indexDefinition,
              initialSyncStartingHandler,
              future,
              resumeInfo,
              indexMetricsUpdater,
              removeMatchCollectionUuid,
              useNaturalOrderScan);

      if (indexDefinition.getIndexDefinition().isAutoEmbeddingIndex()
          && this.concurrentEmbeddingSyncs.isPresent()) {
        if (!this.concurrentEmbeddingSyncs.get().tryAcquire()) {
          // If we are at the limit for concurrent embedding initial syncs, throw a transient
          // exception to requeue the index for initial sync using the default resync backoff.
          LOG.atDebug()
              .addKeyValue("indexId", generationId.indexId)
              .addKeyValue("generationId", generationId)
              .log(
                  "Max embedding indexes already running initial sync: {}. "
                      + "Re-queueing the index for initial sync.",
                  this.maxConcurrentEmbeddingInitialSyncs);
          request
              .getOnCompleteFuture()
              .completeExceptionally(
                  InitialSyncException.createResumableTransient(
                      new Throwable(
                          "Concurrent initial sync limit for embedding indexes reached: "
                              + this.maxConcurrentEmbeddingInitialSyncs
                              + " Re-queueing the index for initial sync.")));
          this.requeuedEmbeddingInitialSyncs.increment();
          return future;
        } else {
          future.whenComplete((result, throwable) -> this.concurrentEmbeddingSyncs.get().release());
        }
      }

      this.queued.put(generationId, request);

      boolean enqueued = this.requestQueue.offer(generationId);
      this.queuedSyncs
          .get(getIndexTypeTag(request.getIndexDefinitionGeneration().getIndexDefinition()))
          .incrementAndGet();
      Check.checkState(
          enqueued,
          "initial sync request queue should be unbounded, "
              + "but did not have capacity to queue request (size: %s, capacity: %s)",
          this.requestQueue.size(),
          this.requestQueue.remainingCapacity());

      return future;
    }
  }

  /**
   * Cancels a previously enqueued Index's initial sync.
   *
   * @param generationId The id of the index whose initial sync should be canceled.
   * @return A future that completes once all work for the initial sync has terminated.
   */
  public CompletableFuture<Void> cancel(GenerationId generationId) {
    try (LockGuard ignored = LockGuard.with(this.queueLock)) {

      checkState(
          !this.cancelled.contains(generationId),
          "index %s has already been cancelled",
          generationId);

      boolean inQueue = this.queued.containsKey(generationId);
      boolean inProgress = this.inProgress.containsKey(generationId);

      checkState(
          !(inQueue && inProgress), "index %s is both in queue and in progress", generationId);

      CompletableFuture<Void> cancelCompletedFuture;

      // If the index is still queued, add it to the cancelled set. The InitialSyncDispatcher will
      // then check this set when the corresponding request is de-queued.
      if (inQueue) {
        this.cancelled.add(generationId);
        InitialSyncRequest request = this.queued.get(generationId);
        request.getOnCompleteFuture().completeExceptionally(InitialSyncException.createShutDown());
        cancelCompletedFuture = FutureUtils.COMPLETED_FUTURE;
      } else if (inProgress) {
        InProgressInitialSyncInfo info = this.inProgress.get(generationId);
        info.cancel();

        // We swallow the result of the future since we don't care about if it completes
        // successfully
        // or not.
        cancelCompletedFuture =
            FutureUtils.swallowedFuture(info.getRequest().getOnCompleteFuture());
      } else {
        // Can happen if:
        // 1. Index is in INITIAL_SYNC, see when cancel() is called.
        // 2. We have removed the index from "inProgress", but haven't
        //    transitioned out of INITIAL_SYNC.
        //
        // This is a very unlikely race condition as we usually transition out of
        // INITIAL_SYNC immediately after we remove from "inProgress" and it's very
        // rare that a shutdown comes in between. See CLOUDP-117773 for more info on
        // what problem we're solving here.
        //
        // Unlike the above case, we don't have anything to wait on, we are done with
        // initial sync if we have removed the index from "inProgress" and we have
        // completed the future that's returned above.
        LOG.atInfo()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .log("cancel() for an index which is not in queue or in progress");
        cancelCompletedFuture = FutureUtils.COMPLETED_FUTURE;
      }

      return cancelCompletedFuture;
    }
  }

  @TestOnly
  @VisibleForTesting
  Optional<Integer> getEmbeddingAvailablePermits() {
    return this.concurrentEmbeddingSyncs.map(Semaphore::availablePermits);
  }

  /**
   * InitialSyncDispatcher accepts initial sync requests on the InitialSyncQueue's requestQueue,
   * runs them via an InitialSyncManager, and updates the InitialSyncQueue's queued and inProgress
   * maps.
   *
   * <p>The InitialSyncDispatcher will acquire permits from the supplied semaphore to run initial
   * syncs, limiting the number of concurrent initial syncs, and it can be shut down by interrupting
   * the thread it is running on.
   */
  class InitialSyncDispatcher extends Thread {

    private final InitialSyncManagerFactory initialSyncManagerFactory;
    private final Semaphore concurrentSyncs;
    private final Map<IndexTypeData.IndexTypeTag, AtomicLong> inProgressSyncs;
    private final AtomicLong inProgressResumedSyncs;
    private final Map<Boolean, LongTaskTimer> initialSyncLongTimerFeatureFlagMapping;
    private final Map<Boolean, Timer> initialSyncTimerFeatureFlagMapping;
    private final Boolean pauseAllInitialSync;
    private final Optional<Integer> embeddingGetMoreBatchSize;

    private final Map<Boolean, AtomicLong> collectionScansFeatureFlagMapping;

    /** A helper for persisting metrics data */
    @SuppressWarnings("unused")
    private Optional<IndexDirectoryHelper> indexDirectoryHelper;

    private final Map<GenerationId, Long> initialSyncStartTimeMap = new HashMap<>();

    InitialSyncDispatcher(
        MeterRegistry meterRegistry,
        InitialSyncManagerFactory initialSyncManagerFactory,
        MongoDbReplicationConfig replicationConfig,
        Path dataPath) {
      super("InitialSyncDispatcher");
      Check.argIsPositive(replicationConfig.numConcurrentInitialSyncs, "numConcurrentSyncs");

      this.initialSyncManagerFactory = initialSyncManagerFactory;
      this.concurrentSyncs = new Semaphore(replicationConfig.numConcurrentInitialSyncs);
      this.pauseAllInitialSync = replicationConfig.pauseAllInitialSyncs;
      this.embeddingGetMoreBatchSize = replicationConfig.embeddingGetMoreBatchSize;

      MetricsFactory metricsFactory = new MetricsFactory("initialsync.dispatcher", meterRegistry);
      String inProgressSyncsName = "inProgressSyncs";
      this.inProgressSyncs =
          Map.of(
              TAG_SEARCH,
              getNumGauge(metricsFactory, inProgressSyncsName, TAG_SEARCH),
              TAG_VECTOR_SEARCH,
              getNumGauge(metricsFactory, inProgressSyncsName, TAG_VECTOR_SEARCH),
              TAG_VECTOR_SEARCH_AUTO_EMBEDDING,
              getNumGauge(metricsFactory, inProgressSyncsName, TAG_VECTOR_SEARCH_AUTO_EMBEDDING));

      Tags naturalOrderScanTag = Tags.of("scan_type", "natural_order");
      Tags idOrderScanTag = Tags.of("scan_type", "id_order");

      String collectionScanName = "collectionScan";
      this.collectionScansFeatureFlagMapping =
          Map.of(
              true,
              metricsFactory.numGauge(collectionScanName, naturalOrderScanTag),
              false,
              metricsFactory.numGauge(collectionScanName, idOrderScanTag));
      this.inProgressResumedSyncs = metricsFactory.numGauge("inProgressResumedSyncs");
      String initialSyncLongTimerName = "syncDuration";
      this.initialSyncLongTimerFeatureFlagMapping =
          Map.of(
              true,
              metricsFactory.longTaskTimer(initialSyncLongTimerName, naturalOrderScanTag),
              false,
              metricsFactory.longTaskTimer(initialSyncLongTimerName, idOrderScanTag));
      String initialSyncTimerName = "completedSyncDuration";

      this.initialSyncTimerFeatureFlagMapping =
          Map.of(
              true,
              metricsFactory.timer(initialSyncTimerName, naturalOrderScanTag),
              false,
              metricsFactory.timer(initialSyncTimerName, idOrderScanTag));
      // TODO(CLOUDP-335647): remove queuedSyncs after switching to the new one in InitialSyncQueue.
      metricsFactory.collectionSizeGauge("queuedSyncs", InitialSyncQueue.this.requestQueue);
      metricsFactory.timeGauge(
          "inProgressInitialSyncDurationMax",
          this.initialSyncStartTimeMap,
          map -> {
            var now = Clock.systemUTC().millis();
            return map.values().stream().mapToLong(ts -> now - ts).max().orElse(0L);
          });
      metricsFactory.timeGauge(
          "inProgressInitialSyncDurationMin",
          this.initialSyncStartTimeMap,
          map -> {
            var now = Clock.systemUTC().millis();
            return map.values().stream().mapToLong(ts -> now - ts).min().orElse(0L);
          });
      metricsFactory.timeGauge(
          "inProgressInitialSyncDurationSum",
          this.initialSyncStartTimeMap,
          map -> {
            var now = Clock.systemUTC().millis();
            return map.values().stream().mapToLong(ts -> now - ts).sum();
          });
      try {
        this.indexDirectoryHelper =
            Optional.of(IndexDirectoryHelper.create(dataPath, metricsFactory));
      } catch (IOException e) {
        LOG.error("Failed to create IndexDirectoryHelper", e);
        this.indexDirectoryHelper = Optional.empty();
      }
    }

    @Override
    public void run() {
      if (this.pauseAllInitialSync) {
        LOG.info(
            "Initial sync paused due to pauseAllInitialSync flag being set to true. "
                + "Dispatcher will not start.");
        return;
      }

      while (true) {
        LOG.atInfo()
            .addKeyValue("numQueued", InitialSyncQueue.this.requestQueue.size())
            .log("Queued initial syncs.");

        GenerationId generationId;
        try {
          // Wait until another sync is allowed to run.
          this.concurrentSyncs.acquire();

          // Wait for another sync to be requested.
          generationId = InitialSyncQueue.this.requestQueue.take();

          // We need to hold the lock on the InitialSyncQueue while we create the future. Otherwise,
          // the InitialSyncQueue may attempt to cancel an initial sync by checking to see if it is
          // in the map after we've scheduled the work but prior to adding it to the map. If this
          // happened the InitialSyncQueue would return that the initial sync had been cancelled
          // while in reality it had been scheduled to run.
          //
          // We need to hold the lock interruptibly, as the InitialSyncDispatcher is shutdown
          // through an interrupt and it may be blocked on "queueLock" when it needs to shutdown.
          try (LockGuard ignored = LockGuard.withInterruptibly(InitialSyncQueue.this.queueLock)) {
            checkState(
                InitialSyncQueue.this.queued.containsKey(generationId),
                "index %s was in request queue but not in queued",
                generationId);

            InitialSyncRequest request = InitialSyncQueue.this.queued.remove(generationId);
            InitialSyncQueue.this
                .queuedSyncs
                .get(getIndexTypeTag(request.getIndexDefinitionGeneration().getIndexDefinition()))
                .decrementAndGet();

            if (InitialSyncQueue.this.cancelled.remove(generationId)) {
              this.concurrentSyncs.release();
              continue;
            }

            // Run the InitialSyncManager on its own explicit thread so we can interrupt it if we
            // need to.
            OneShotSingleThreadExecutor syncExecutor =
                new OneShotSingleThreadExecutor(
                    String.format(
                        "%s InitialSyncManager",
                        request.getIndexDefinitionGeneration().getGenerationId()));

            IndexDefinition indexDefinition =
                request.getIndexDefinitionGeneration().getIndexDefinition();
            this.inProgressSyncs.get(getIndexTypeTag(indexDefinition)).incrementAndGet();
            this.collectionScansFeatureFlagMapping
                .get(request.getUseNaturalOrderScan())
                .incrementAndGet();
            request
                .getResumeInfo()
                .ifPresent(unused -> this.inProgressResumedSyncs.incrementAndGet());
            Crash.because("failed running initial sync")
                .ifCompletesExceptionally(
                    CompletableFuture.runAsync(() -> runInitialSync(request), syncExecutor))
                .thenRun(
                    () -> {
                      this.inProgressSyncs.get(getIndexTypeTag(indexDefinition)).decrementAndGet();
                      this.collectionScansFeatureFlagMapping
                          .get(request.getUseNaturalOrderScan())
                          .decrementAndGet();
                      request
                          .getResumeInfo()
                          .ifPresent(unused -> this.inProgressResumedSyncs.decrementAndGet());
                    });

            Thread syncThread = syncExecutor.getThread();
            InProgressInitialSyncInfo info = new InProgressInitialSyncInfo(request, syncThread);
            InitialSyncQueue.this.inProgress.put(
                request.getIndexDefinitionGeneration().getGenerationId(), info);
          }
        } catch (InterruptedException e) {
          LOG.info("InitialSyncDispatcher thread interrupted, shutting down.");
          return;
        }
      }
    }

    /** Start timing initialSync() if it isn't a resumed initial sync. */
    private void runInitialSync(InitialSyncRequest request) {
      // TODO(CLOUDP-329867): re-enable persisting initial sync metrics after crash is fixed
      Optional<Path> metadataPathOptional = Optional.empty();

      if (request.getResumeInfo().isEmpty()) {
        // Save the start time of the initial sync so we can calculate the duration
        metadataPathOptional.ifPresent(
            path -> {
              var startTime = persistInitialSyncMetadata(path);
              this.initialSyncStartTimeMap.put(
                  request.getIndexDefinitionGeneration().getGenerationId(), startTime);
            });

        Stopwatch stopwatch = Stopwatch.createStarted();

        this.initialSyncLongTimerFeatureFlagMapping
            .get(request.getUseNaturalOrderScan())
            .record(() -> initialSync(request));
        // Note that this timer will only be recorded if no exception was thrown.
        this.initialSyncTimerFeatureFlagMapping
            .get(request.getUseNaturalOrderScan())
            .record(stopwatch.elapsed());
      } else {
        // Load the previously saved start time (if it exists) and use that to calculate initial
        // sync duration
        metadataPathOptional.ifPresent(
            path -> {
              try {
                var metadata = InitialSyncMetricsMetadata.fromFileIfExists(path);
                var maybeStartTime = metadata.flatMap(InitialSyncMetricsMetadata::startTime);
                maybeStartTime.ifPresent(
                    startTime ->
                        this.initialSyncStartTimeMap.put(
                            request.getIndexDefinitionGeneration().getGenerationId(), startTime));
              } catch (Exception e) {
                LOG.warn("Failed to load initial sync metrics", e);
              }
            });
        initialSync(request);
      }

      metadataPathOptional.ifPresent(
          metadataPath -> {
            this.initialSyncStartTimeMap.remove(
                request.getIndexDefinitionGeneration().getGenerationId());
            metadataPath.toFile().delete();
          });
    }

    private long persistInitialSyncMetadata(Path metadataPath) {
      var initialSyncStartTime = Clock.systemUTC().millis();

      try {
        new InitialSyncMetricsMetadata(Optional.of(initialSyncStartTime)).persist(metadataPath);
      } catch (IOException e) {
        LOG.warn("Failed to persist initial sync metrics", e);
      }

      return initialSyncStartTime;
    }

    /**
     * Creates and runs the {@link InitialSyncManager} for the given {@link InitialSyncRequest},
     * completing the InitialSyncRequest's future with the ChangeStreamResumeInfo from the sync if
     * it was successful, or completing it exceptionally if an exception is thrown while syncing.
     *
     * <p>Note that initialSync() is run asynchronously on its own thread, so interrupts can be used
     * to signal InitialSyncManager to cancel.
     */
    @SuppressWarnings("GuardedBy")
    private void initialSync(InitialSyncRequest request) {
      @Var Optional<Runnable> completeFuture = Optional.empty();
      try {
        // The default mongoClient should be constructed from syncSource.mongodURI, it is mapped to
        // the host InitialSyncQueue.this.syncSourceHost
        @Var
        InitialSyncMongoClient mongoClient =
            InitialSyncQueue.this.mongoClients.get(InitialSyncQueue.this.syncSourceHost);
        LOG.atInfo()
            .addKeyValue("host", InitialSyncQueue.this.syncSourceHost)
            .log("using default mongoClient");

        // Checking whether natural order scan is being resumed
        if (InitialSyncQueue.this.initialSyncConfig.avoidNaturalOrderScanSyncSourceChangeResync()
            && request.getResumeInfo().isPresent()
            && request.getResumeInfo().get().isBufferlessNaturalOrderInitialSyncResumeInfo()) {
          var resumeInfo = request.getResumeInfo().get();
          // make sure getSyncSourceHost() is in connection string mappings
          if (resumeInfo.getSyncSourceHost().isPresent()
              && InitialSyncQueue.this.mongoClients.containsKey(
                  resumeInfo.getSyncSourceHost().get())
              && !resumeInfo
                  .getSyncSourceHost()
                  .get()
                  .equals(InitialSyncQueue.this.syncSourceHost)) {
            // Replace mongoClient if according sync source could be found./
            mongoClient =
                InitialSyncQueue.this.mongoClients.get(resumeInfo.getSyncSourceHost().get());
            LOG.atInfo()
                .addKeyValue("host", resumeInfo.getSyncSourceHost().get())
                .addKeyValue("indexId", request.getIndexDefinitionGeneration().getIndexId())
                .addKeyValue(
                    "generationId", request.getIndexDefinitionGeneration().getGenerationId())
                .log("switching to original mongoClient from initial sync resume info");
          }
        }

        // Here namespace resolution is first performed, right before the actual sync occurs below
        // in syncManager.sync(). This method will throw an exception if the collection
        // does not exist, and reschedule this initial sync in case the collection exists in the
        // future (if the cluster is sharded.)
        String collectionName =
            mongoClient.resolveAndUpdateCollectionName(
                request.getIndexDefinitionGeneration().getIndexDefinition());

        InitialSyncContext syncContext =
            InitialSyncContext.create(
                request.getIndexDefinitionGeneration(),
                InitialSyncQueue.this.indexingWorkSchedulerFactory.getIndexingWorkScheduler(
                    request.getIndexDefinitionGeneration().getIndexDefinition()),
                request.getDocumentIndexer(),
                request.getIndexMetricsUpdater(),
                this.embeddingGetMoreBatchSize,
                request.isRemoveMatchCollectionUuid(),
                request.getUseNaturalOrderScan(),
                InitialSyncQueue.this.initialSyncGate);

        InitialSyncManager syncManager =
            this.initialSyncManagerFactory.create(
                syncContext,
                mongoClient,
                new MongoNamespace(syncContext.getIndexDefinition().getDatabase(), collectionName),
                request.getResumeInfo());

        // Here the index's status is updated to INITIAL_SYNC, and only executes if namespace
        // resolution above succeeds. Otherwise, so long as namespace resolution keeps failing when
        // we're retrying it, the index's status will repeatedly be set to DOES_NOT_EXIST.
        request.getInitialSyncStartingHandler().run();

        ChangeStreamResumeInfo resumeInfo = syncManager.sync();

        completeFuture = Optional.of(() -> request.getOnCompleteFuture().complete(resumeInfo));
      } catch (Throwable t) {
        completeFuture = Optional.of(() -> request.getOnCompleteFuture().completeExceptionally(t));
      } finally {
        checkState(completeFuture.isPresent(), "completeFuture must be set");

        // Regardless of whether the initial sync finished exceptionally or not, our work is done,
        // so remove it from the in progress map and allow another initial sync to run.
        // We also want to complete the future after we've removed it from the inProgress map so
        // that whatever handles the future can immediately enqueue it back onto the
        // InitialSyncQueue if necessary.
        //
        // We specifically do not allow interruptions to prevent us from performing cleanup here.
        // Otherwise, interrupt (due to cancellations in initial sync) may prevent this future
        // from ever completing.
        // Note that in InitialSyncQueue::shutdown we do not wait for the future to complete
        // while holding queueLock. So we can not deadlock.
        try (LockGuard ignored = LockGuard.with(InitialSyncQueue.this.queueLock)) {
          InitialSyncQueue.this.inProgress.remove(
              request.getIndexDefinitionGeneration().getGenerationId());

          completeFuture.get().run();
        } finally {
          this.concurrentSyncs.release();
        }
      }
    }
  }
}
