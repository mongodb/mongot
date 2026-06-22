package com.xgen.mongot.replication.mongodb.synonyms;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.version.SynonymMappingId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.concurrent.LockGuard;
import com.xgen.mongot.util.concurrent.OneShotSingleThreadExecutor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SynonymManager} is responsible for the lifecycle of {@link SynonymMappingManager}
 * requests. Synonym mapping managers can enqueue requests for synonym artifacts and cancel existing
 * requests through SynonymManager.
 *
 * <p>Architecturally similar to {@link
 * com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue}.
 */
@SuppressWarnings("GuardedBy") // Uses LockGuard instead
public class SynonymManager {

  private static final Logger LOG = LoggerFactory.getLogger(SynonymManager.class);

  private final ReentrantLock queueLock;

  // Client to replicate with.
  private final SynonymSyncMongoClient mongoClient;

  // Queue of synonym mappings with requests to serve.
  private final BlockingQueue<SynonymMappingId> requestQueue;

  // Sync requests for synonym mappings that have scheduled work in the queue.
  @GuardedBy("queueLock")
  private final Map<SynonymMappingId, SynonymSyncRequest> queued;

  // Synonym mappings that have been cancelled.
  @GuardedBy("queueLock")
  private final Set<SynonymMappingId> cancelled;

  // In-progress sync info for requests that are currently being served.
  @GuardedBy("queueLock")
  private final Map<SynonymMappingId, InProgressSynonymSyncInfo> syncInProgress;

  private final SynonymCollectionScanner.Factory collectionScannerFactory;

  // Dispatcher pulls requests off the queue and creates threads to service requests.
  private final SynonymSyncDispatcher dispatcher;

  // If this SynonymManager has been shut down.
  @GuardedBy("queueLock")
  private boolean shutdown;

  // Metrics for synonym sync
  private final SynonymSyncMetrics metrics;

  private SynonymManager(
      SynonymSyncMongoClient mongoClient,
      BlockingQueue<SynonymMappingId> requestQueue,
      Map<SynonymMappingId, SynonymSyncRequest> queued,
      Set<SynonymMappingId> cancelled,
      Map<SynonymMappingId, InProgressSynonymSyncInfo> syncInProgress,
      int numConcurrentSynonymSyncs,
      SynonymCollectionScanner.Factory collectionScannerFactory,
      MetricsFactory metricsFactory) {
    this.mongoClient = mongoClient;
    this.requestQueue = requestQueue;
    this.queued = queued;
    this.cancelled = cancelled;
    this.syncInProgress = syncInProgress;
    this.collectionScannerFactory = collectionScannerFactory;

    this.queueLock = new ReentrantLock();
    this.shutdown = false;

    metricsFactory.collectionSizeGauge("queueDepth", this.requestQueue);
    this.metrics = new SynonymSyncMetrics(metricsFactory);

    this.dispatcher = new SynonymSyncDispatcher(numConcurrentSynonymSyncs, this.metrics);
    this.dispatcher.start();
  }

  /**
   * Create a SynonymManager configured to perform numConcurrentSynonymSyncs synonym collection
   * syncs at a time.
   */
  public static SynonymManager create(
      boolean isSharded,
      MongoClient mongoClient,
      SessionRefresher sessionRefresher,
      MeterRegistry meterRegistry,
      int numConcurrentSynonymSyncs) {
    Check.argIsPositive(numConcurrentSynonymSyncs, "numConcurrentSynonymSyncs");

    BlockingQueue<SynonymMappingId> requestQueue = new LinkedBlockingQueue<>();
    Map<SynonymMappingId, SynonymSyncRequest> queued = new HashMap<>();
    Set<SynonymMappingId> cancelled = new HashSet<>();
    Map<SynonymMappingId, InProgressSynonymSyncInfo> syncInProgress = new HashMap<>();

    SynonymSyncMongoClient synonymSyncMongoClient =
        new SynonymSyncMongoClient(isSharded, mongoClient, sessionRefresher, meterRegistry);
    MetricsFactory metricsFactory = new MetricsFactory("synonymSync", meterRegistry);
    return create(
        synonymSyncMongoClient,
        requestQueue,
        queued,
        cancelled,
        syncInProgress,
        numConcurrentSynonymSyncs,
        SynonymCollectionScanner::new,
        metricsFactory);
  }

  @VisibleForTesting
  static SynonymManager create(
      SynonymSyncMongoClient synonymSyncMongoClient,
      BlockingQueue<SynonymMappingId> requestQueue,
      Map<SynonymMappingId, SynonymSyncRequest> queued,
      Set<SynonymMappingId> cancelled,
      Map<SynonymMappingId, InProgressSynonymSyncInfo> syncInProgress,
      int numConcurrentSyncs,
      SynonymCollectionScanner.Factory collectionScannerFactory,
      MetricsFactory metricsFactory) {
    return new SynonymManager(
        synonymSyncMongoClient,
        requestQueue,
        queued,
        cancelled,
        syncInProgress,
        numConcurrentSyncs,
        collectionScannerFactory,
        metricsFactory);
  }

  /**
   * Shutdown this SynonymManager. Remove all pending sync requests from the queue and interrupt all
   * in-progress syncs.
   *
   * <p>Returned future waits for all futures to complete.
   */
  public CompletableFuture<Void> shutdown() {
    try (LockGuard ignored = LockGuard.with(this.queueLock)) {
      LOG.info("Shutting down.");

      this.shutdown = true;

      // Shut down the dispatcher - don't schedule any more work.
      this.dispatcher.interrupt();
      Crash.ifDoesNotJoin(this.dispatcher, Duration.ofSeconds(10));

      this.queued
          .values()
          .forEach(
              request ->
                  request.getFuture().completeExceptionally(SynonymSyncException.createShutDown()));

      this.syncInProgress.values().forEach(InProgressSynonymSyncInfo::cancel);

      return FutureUtils.allOf(
          this.syncInProgress.values().stream()
              .map(info -> FutureUtils.swallowedFuture(info.getRequest().getFuture()))
              .collect(Collectors.toList()));
    }
  }

  /**
   * Add the supplied {@link SynonymMappingDefinition} to the sync queue for a collection scan.
   *
   * @param documentIndexer The indexer to use to process {@link
   *     com.xgen.mongot.index.synonym.SynonymDocument}s.
   * @param indexGeneration The index generation that this mapping is a part of.
   * @param synonymDefinition The synonym mapping definition to begin managing.
   * @param synonymSyncStartingHandler A callback to run once the synonym sync is picked up off the
   *     queue.
   * @return A future containing the operationTime at the beginning of a successful sync, or the
   *     postBatchResumeToken from an empty change stream batch. Completes once synonym artifacts
   *     are created and the respective synonym registry is updated. May complete exceptionally on
   *     an unsuccessful sync.
   * @throws SynonymSyncException Only throws if shut down.
   */
  public CompletableFuture<SynonymMappingHighWaterMark> enqueueCollectionScan(
      SynonymDocumentIndexer documentIndexer,
      IndexGeneration indexGeneration,
      SynonymMappingDefinition synonymDefinition,
      Runnable synonymSyncStartingHandler)
      throws SynonymSyncException {
    this.metrics.getCollScansCounter().increment();
    try (LockGuard ignored = LockGuard.with(this.queueLock)) {
      return enqueue(
          SynonymCollectionScanRequest.create(
              documentIndexer,
              indexGeneration,
              synonymDefinition,
              synonymSyncStartingHandler,
              this.collectionScannerFactory,
              this.metrics));
    }
  }

  /**
   * Add the supplied {@link SynonymMappingDefinition} to the sync queue to perform a "change stream
   * sync". A "change stream sync" opens a changes stream against the synonym source collection, and
   * determines if there have been changes since the last successful collection scan.
   *
   * @param resettableChangeStreamClient A change stream client with a watermark pointing to the
   *     last seen by a collection scan.
   * @param indexGeneration The index generation that this mapping is a part of.
   * @param synonymDefinition The synonym mapping definition to begin managing.
   * @return A future containing the operationTime at the beginning of a successful sync, or the
   *     postBatchResumeToken from an empty change stream batch. Completes once synonym artifacts
   *     are created and the respective synonym registry is updated. May complete exceptionally on
   *     an unsuccessful sync.
   * @throws SynonymSyncException Only throws if shut down.
   */
  public CompletableFuture<SynonymMappingHighWaterMark> enqueueChangeStream(
      ResettableChangeStreamClient resettableChangeStreamClient,
      IndexGeneration indexGeneration,
      SynonymMappingDefinition synonymDefinition)
      throws SynonymSyncException {
    try (LockGuard ignored = LockGuard.with(this.queueLock)) {
      return enqueue(
          SynonymChangeStreamRequest.create(
              resettableChangeStreamClient, indexGeneration, synonymDefinition, this.metrics));
    }
  }

  @VisibleForTesting
  @GuardedBy("queueLock")
  CompletableFuture<SynonymMappingHighWaterMark> enqueue(SynonymSyncRequest request)
      throws SynonymSyncException {
    if (this.shutdown) {
      throw SynonymSyncException.createShutDown();
    }

    SynonymMappingId synonymId = request.getMappingId();
    checkState(
        !this.cancelled.contains(synonymId), "synonym mapping %s has been cancelled", synonymId);
    checkState(
        !this.queued.containsKey(synonymId) && !this.syncInProgress.containsKey(synonymId),
        "synonym mapping %s is already enqueued",
        synonymId);

    this.queued.put(synonymId, request);

    boolean enqueued = this.requestQueue.offer(synonymId);
    Check.checkState(
        enqueued,
        "synonym request queue should be unbounded, but did not have capacity to queue request");

    return request.getFuture();
  }

  /**
   * Cancels a previously enqueued synonym sync. {@code synonymMappingId} must be queued for sync,
   * have a sync in progress, or be listening for changes (e.g. synonymMappingId must be in one of
   * queued or syncInProgress maps).
   *
   * <p>Completes once work is finished. Always completes successfully, regardless of if the request
   * future completes successfully or exceptionally.
   */
  public CompletableFuture<Void> cancel(SynonymMappingId synonymMappingId) {
    try (LockGuard ignored = LockGuard.with(this.queueLock)) {
      checkState(
          !this.cancelled.contains(synonymMappingId),
          "synonym mapping %s has already been cancelled",
          synonymMappingId);

      if (!(this.queued.containsKey(synonymMappingId)
          || this.syncInProgress.containsKey(synonymMappingId))) {
        // No work scheduled for this synonym mapping, nothing to "cancel".
        return FutureUtils.COMPLETED_FUTURE;
      }

      // If the synonym sync is in the sync queue
      if (this.queued.containsKey(synonymMappingId)) {
        this.cancelled.add(synonymMappingId);
        SynonymSyncRequest request = this.queued.get(synonymMappingId);
        request.getFuture().completeExceptionally(SynonymSyncException.createShutDown());
        return FutureUtils.COMPLETED_FUTURE;
      }

      InProgressSynonymSyncInfo info = this.syncInProgress.get(synonymMappingId);
      info.cancel();

      // Swallow the result - SynonymManager doesn't care if this was successful or unsuccessful,
      // only
      // that it has completed.
      return FutureUtils.swallowedFuture(info.getRequest().getFuture());
    }
  }

  public SynonymSyncMongoClient getClient() {
    return this.mongoClient;
  }

  /**
   * SynonymSyncDispatcher accepts synonym sync requests on its requestQueue, and runs them via
   * SynonymCollectionScanner and SynonymDocumentIndexer.
   *
   * <p>The SynonymSyncDispatcher will acquire permits from its internal semaphore to run synonym
   * syncs, limiting the number of concurrent syncs. SynonymSyncDispatcher can be shut down by
   * interrupting the thread it is running on.
   *
   * <p>Architecturally, SynonymSyncDispatcher is quite similar to InitialSyncQueue.Dispatcher.
   */
  class SynonymSyncDispatcher extends Thread {
    private final Semaphore concurrentSyncs;
    private final SynonymSyncMetrics metrics;

    SynonymSyncDispatcher(int numConcurrentSyncs, SynonymSyncMetrics metrics) {
      super("SynonymSyncDispatcher");
      Check.argIsPositive(numConcurrentSyncs, "numConcurrentSyncs");

      this.concurrentSyncs = new Semaphore(numConcurrentSyncs);
      this.metrics = metrics;
    }

    @Override
    public void run() {
      while (true) {
        if (!SynonymManager.this.requestQueue.isEmpty()) {
          LOG.info("{} queued synonym syncs.", SynonymManager.this.requestQueue.size());
        }

        SynonymMappingId mappingId;
        try {
          this.concurrentSyncs.acquire();

          mappingId = SynonymManager.this.requestQueue.take();
        } catch (InterruptedException e) {
          LOG.info("SynonymManager thread interrupted, shutting down.");
          return;
        }

        // We need to hold the lock interruptibly, as the SynonymSyncDispatcher is shutdown
        // through an interrupt and it may be blocked on "queueLock" when it needs to shutdown.
        //
        // We need to hold the lock interruptibly, as the sync thread is shutdown
        // through an interrupt and it may be blocked on "queueLock" when it needs to shutdown.
        try (LockGuard ignored = LockGuard.withInterruptibly(SynonymManager.this.queueLock)) {
          checkState(
              SynonymManager.this.queued.containsKey(mappingId),
              "synonym mapping %s was in request queue but not in queued",
              mappingId);

          SynonymSyncRequest request = SynonymManager.this.queued.remove(mappingId);

          if (SynonymManager.this.cancelled.remove(mappingId)) {
            this.concurrentSyncs.release();
            continue;
          }

          OneShotSingleThreadExecutor syncExecutor =
              new OneShotSingleThreadExecutor(
                  String.format("%s SynonymManager", request.getMappingId().uniqueString()));
          Crash.because("failed to run synonym sync")
              .ifCompletesExceptionally(
                  CompletableFuture.runAsync(() -> runSynonymSync(request), syncExecutor));

          Thread syncThread = syncExecutor.getThread();
          InProgressSynonymSyncInfo info = new InProgressSynonymSyncInfo(request, syncThread);
          SynonymManager.this.syncInProgress.put(request.getMappingId(), info);
        } catch (InterruptedException e) {
          LOG.info("InitialSyncDispatcher thread interrupted, shutting down.");
          return;
        }
      }
    }

    /**
     * Create and run the task for the given SynonymSyncRequest. Interrupts can be used to signal
     * SynonymSyncDispatcher to cancel.
     */
    private void runSynonymSync(SynonymSyncRequest request) {
      @Var Optional<Runnable> completeFuture = Optional.empty();
      Timer.Sample syncSample = Timer.start();
      try {
        SynonymMappingHighWaterMark highWaterMark = request.doWork(SynonymManager.this.mongoClient);

        completeFuture = Optional.of(() -> request.getFuture().complete(highWaterMark));
      } catch (Throwable e) {
        this.metrics.getExceptionCounter().increment();
        completeFuture = Optional.of(() -> request.getFuture().completeExceptionally(e));
      } finally {
        //noinspection ConstantConditions
        checkState(completeFuture.isPresent(), "completeFuture must be set");

        // Remove this sync request from the queue and complete it, regardless of whether or not it
        // completed successfully or exceptionally. Either way - it is done here, make room for
        // another sync to start (and potentially for the handler to enqueue an update).
        try (LockGuard ignored = LockGuard.withInterruptibly(SynonymManager.this.queueLock)) {
          SynonymManager.this.syncInProgress.remove(request.getMappingId());
          completeFuture.get().run();
        } catch (InterruptedException e) {
          LOG.info("InitialSyncDispatcher thread interrupted, shutting down.");
        } finally {
          this.concurrentSyncs.release();
          syncSample.stop(this.metrics.getSyncDurationTimer());
        }
      }
    }
  }
}
