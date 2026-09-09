package com.xgen.mongot.replication.mongodb.synonyms;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.SearchIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.version.SynonymMappingId;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FutureUtils;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A {@link SynonymMappingManager} is responsible for enqueueing work to keep a desired synonym
 * mapping up-to-date in a {@link com.xgen.mongot.index.synonym.SynonymRegistry}.
 *
 * <p>SynonymMappingManager schedules work via {@link SynonymManager}. SynonymMappingManagers
 * maintain no persistent state after shutdown.
 */
public class SynonymMappingManager {

  private static final Duration DEFAULT_CHANGE_STREAM_BACKOFF = Duration.ofSeconds(10);
  private static final Duration DEFAULT_TRANSIENT_BACKOFF = Duration.ofSeconds(30);

  /** Backoff used Mongotune ingress rate limiting. */
  @VisibleForTesting static final Duration THROTTLED_BACKOFF = Duration.ofMillis(100);

  private final SynonymManager synonymManager;
  private final DefaultKeyValueLogger logger;
  private final Executor lifecycleExecutor;
  private final SynonymDocumentIndexerFactory documentIndexerFactory;
  private final SynonymMappingDefinition definition;
  private final IndexGeneration indexGeneration;
  private final SearchIndex index;

  private final Duration schedulingDelay;
  private final Duration transientBackoff;
  private final ResettableChangeStreamClient cachedChangedStreamClient;

  @GuardedBy("this")
  private CompletableFuture<Void> initFuture;

  @GuardedBy("this")
  private State state;

  // The "high water mark" of how up-to-date this synonym mapping is. Holds either the operationTime
  // of the last synonym sync or the postBatchResumeToken of the last empty change stream scan.
  private volatile SynonymMappingHighWaterMark synonymMappingHighWaterMark;

  enum State {
    INITIALIZING, // INITIALIZING should be reported to users as SYNC_ENQUEUED
    SYNC_ENQUEUED,
    INITIAL_SYNC,
    READY,
    READY_UPDATING,
    INVALID,
    SHUTDOWN,
    FAILED
  }

  private SynonymMappingManager(
      SynonymManager synonymManager,
      Executor lifecycleExecutor,
      SynonymDocumentIndexerFactory documentIndexerFactory,
      SynonymMappingDefinition definition,
      IndexGeneration indexGeneration,
      Duration schedulingDelay,
      Duration transientBackoff) {
    var synonymMappingId =
        SynonymMappingId.from(indexGeneration.getGenerationId(), definition.name());
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", synonymMappingId.indexGenerationId.indexId);
    defaultKeyValues.put("generationId", synonymMappingId.indexGenerationId);
    defaultKeyValues.put("synonymMappingName", synonymMappingId.name);
    this.logger = DefaultKeyValueLogger.getLogger(SynonymMappingManager.class, defaultKeyValues);
    this.synonymManager = synonymManager;
    this.lifecycleExecutor = lifecycleExecutor;
    this.documentIndexerFactory = documentIndexerFactory;
    this.definition = definition;
    this.indexGeneration = indexGeneration;
    this.schedulingDelay = schedulingDelay;
    this.transientBackoff = transientBackoff;
    this.index = indexGeneration.getIndex().asSearchIndex();

    this.state = State.INITIALIZING;
    this.initFuture = FutureUtils.COMPLETED_FUTURE;
    this.synonymMappingHighWaterMark = SynonymMappingHighWaterMark.createEmpty();
    MongoNamespace namespace =
        new MongoNamespace(
            indexGeneration.getDefinition().getDatabase(), definition.source().collection());
    this.cachedChangedStreamClient =
        new ResettableChangeStreamClient(
            namespace, this.synonymManager.getClient(), this.definition);
  }

  /**
   * Create and initialize {@link SynonymMappingManager}. Schedule initialization on the lifecycle
   * executor.
   *
   * <p>At the completion of this method, {@link SynonymMappingManager} will have successfully
   * enqueued a {@link SynonymCollectionScanRequest} via the provided {@link SynonymManager}.
   *
   * @return An initialized {@link SynonymMappingManager}.
   */
  @VisibleForTesting
  static SynonymMappingManager create(
      SynonymManager synonymManager,
      Executor lifecycleExecutor,
      SynonymDocumentIndexerFactory documentIndexerFactory,
      SynonymMappingDefinition definition,
      IndexGeneration indexGeneration,
      Duration changeStreamBackoff,
      Duration transientBackoff) {
    SynonymMappingManager mappingManager =
        new SynonymMappingManager(
            synonymManager,
            lifecycleExecutor,
            documentIndexerFactory,
            definition,
            indexGeneration,
            changeStreamBackoff,
            transientBackoff);

    synchronized (mappingManager) {
      mappingManager.initFuture =
          Crash.because("failed to initialize mapping manager")
              .ifCompletesExceptionally(
                  CompletableFuture.runAsync(mappingManager::init, mappingManager.lifecycleExecutor)
                      .handleAsync(
                          (ignored, throwable) -> {
                            if (throwable != null) {
                              mappingManager.failMapping(throwable);
                            }

                            return null;
                          },
                          mappingManager.lifecycleExecutor));
    }

    return mappingManager;
  }

  /**
   * Create SynonymMappingManagers for an {@link IndexGeneration}. SynonymMappingManagers are
   * responsible for creating and updating a single synonym mapping specified in an index.
   */
  public static List<SynonymMappingManager> create(
      SynonymManager synonymManager, Executor executor, IndexGeneration indexGeneration) {

    if (indexGeneration.getType() == IndexDefinitionGeneration.Type.VECTOR) {
      return Collections.emptyList();
    }

    Check.expectedType(IndexDefinition.Type.SEARCH, indexGeneration.getDefinition().getType());
    var searchDefinition = indexGeneration.getDefinition().asSearchDefinition();

    return searchDefinition.getSynonyms().orElseGet(Collections::emptyList).stream()
        .map(
            synonymMappingDefinition ->
                SynonymMappingManager.create(
                    synonymManager,
                    executor,
                    SynonymDocumentIndexer.factory(indexGeneration.getGenerationId()),
                    synonymMappingDefinition,
                    indexGeneration,
                    SynonymMappingManager.DEFAULT_CHANGE_STREAM_BACKOFF,
                    SynonymMappingManager.DEFAULT_TRANSIENT_BACKOFF))
        .collect(Collectors.toList());
  }

  /**
   * Initialize this SynonymMappingManager and start work to create and maintain a SynonymMapping in
   * the SynonymRegistry.
   */
  private synchronized void init() {
    if (isTerminated()) {
      return;
    }

    checkState(
        this.state.equals(State.INITIALIZING), "cannot call init() while not in INITIALIZING");
    this.transitionState(State.SYNC_ENQUEUED);
    enqueueCollectionScan();
  }

  /**
   * Initialize this SynonymMappingManager. Enqueues a {@link SynonymCollectionScanRequest} via the
   * configured {@link SynonymManager}. The enqueued request will transition the state of this
   * SynonymMappingManager to either INITIAL_SYNC or READY_UPDATING when the request is pulled off
   * the queue.
   */
  private synchronized void enqueueCollectionScan() {
    if (isTerminated()) {
      return;
    }

    this.synonymMappingHighWaterMark = SynonymMappingHighWaterMark.createEmpty();
    this.logger.info("Enqueueing synonym collection scan.");
    try {
      Crash.because("failed to enqueue collection scan")
          .ifCompletesExceptionally(
              this.synonymManager
                  .enqueueCollectionScan(
                      this.documentIndexerFactory.create(
                          this.index.getSynonymRegistry(), this.definition),
                      this.indexGeneration,
                      this.definition,
                      this::transitionStateOnBeginCollectionScan)
                  .handleAsync(this::handleSynonymScanResult, this.lifecycleExecutor));
    } catch (SynonymSyncException e) {
      handleSynonymSyncException(e);
    }
  }

  private synchronized void transitionStateOnBeginCollectionScan() {
    if (isTerminated()) {
      return;
    }

    this.index.getSynonymRegistry().beginUpdate(this.definition.name());
    switch (this.state) {
      case INVALID, SYNC_ENQUEUED, INITIAL_SYNC -> {
        transitionState(State.INITIAL_SYNC);
      }
      case READY, READY_UPDATING -> {
        transitionState(State.READY_UPDATING);
      }
      case INITIALIZING, SHUTDOWN, FAILED ->
          throw new AssertionError(
              "cannot start sync from INITIALIZING, SHUTDOWN, or FAILED states");
    }
  }

  private synchronized void enqueueChangeStream() {
    if (isTerminated()) {
      return;
    }

    this.logger.trace("Enqueueing synonym change stream.");
    this.cachedChangedStreamClient.reset(this.synonymMappingHighWaterMark);
    try {
      Crash.because("failed to enqueue change stream")
          .ifCompletesExceptionally(
              this.synonymManager
                  .enqueueChangeStream(
                      this.cachedChangedStreamClient, this.indexGeneration, this.definition)
                  .handleAsync(this::handleSynonymScanResult, this.lifecycleExecutor));
    } catch (SynonymSyncException e) {
      handleSynonymSyncException(e);
    }
  }

  /**
   * Stop any in-progress work and return. A SynonymMappingManager maintains no durable state after
   * shutdown.
   */
  public synchronized CompletableFuture<Void> shutdown() {
    State currentStatus = this.state;
    transitionState(State.SHUTDOWN);

    this.logger.info("Shutting down.");
    return switch (currentStatus) {
      case INITIALIZING -> this.initFuture;

      // Remove sync from the queue, if it hasn't been pulled off yet.
      // Stop the in-progress synonym sync in INITIAL_SYNC or READY_UPDATING.
      // These cases have change streams that they're listening on to cancel.
      case SYNC_ENQUEUED, INITIAL_SYNC, READY_UPDATING, READY, INVALID ->
          this.synonymManager.cancel(
              SynonymMappingId.from(
                  this.indexGeneration.getGenerationId(), this.definition.name()));

      // If we've already failed, we're done shutting down.
      // If we're already shut down, we're done.
      case FAILED, SHUTDOWN -> FutureUtils.COMPLETED_FUTURE;
    };
  }

  /**
   * The current mapping is up-to-date, as of this mark. Holds either the operationTime (start) of
   * the last collection scan or the postBatchResumeToken of the last empty change stream batch.
   * Change events after this mark trigger an update.
   */
  public SynonymMappingHighWaterMark getSynonymMappingHighWaterMark() {
    return this.synonymMappingHighWaterMark;
  }

  private synchronized Void handleSynonymScanResult(
      SynonymMappingHighWaterMark highWaterMark, Throwable throwable) {
    // If synonym mapping manager is already shutdown, stop handling this synonym scan and return.
    if (isTerminated()) {
      return null;
    }

    if (throwable != null) {
      handleSynonymSyncException(throwable);
      return null;
    }

    this.logger.trace(
        "successful completion of request, advancing high water mark from {} to {}",
        this.synonymMappingHighWaterMark,
        highWaterMark);
    this.synonymMappingHighWaterMark = highWaterMark;

    // A high water mark's presence, and the absence of an exception, indicate that the last
    // operation was successful. This synonym mapping is up-to-date to at least the point of the
    // high water mark.
    if (this.synonymMappingHighWaterMark.isPresent()) {
      // A synonym mapping should only ever reach this point in one of these states.
      Check.expectedType(
          EnumSet.of(State.INITIAL_SYNC, State.READY, State.READY_UPDATING, State.INVALID),
          this.state);

      // All valid states, aside from INVALID, transition to READY in this scenario. An INVALID
      // state indicates that the synonym source collection associated with this mapping still
      // contains an invalid document - it has advanced its high water mark without seeing a change
      // event.
      if (this.state != State.INVALID) {
        this.transitionState(State.READY);
      }

      // All states enqueue a change stream on a successful sync with a new high water mark.
      scheduleOrFail(this::enqueueChangeStream, this.schedulingDelay, this.lifecycleExecutor);
      return null;
    }

    // The absence of a high water mark indicates that the last sync saw a change - and there is no
    // use keeping track of how up-to-date the result of the scan was. It enqueues a collection
    // scan, and sets the high water mark to the operation time of that sync.

    // A synonym mapping should only ever successfully complete a scan without a new high water mark
    // in one of these states.
    Check.expectedType(EnumSet.of(State.READY, State.READY_UPDATING, State.INVALID), this.state);

    // All valid states, aside from INVALID, transition to READY_UPDATING in this case.
    if (this.state != State.INVALID) {
      this.transitionState(State.READY_UPDATING);
    }

    // All states enqueue a collection scan on a successful sync without a new high water mark.
    enqueueCollectionScan();
    return null;
  }

  /** Returns true if no more work can be scheduled. */
  private synchronized boolean isTerminated() {
    return this.state.equals(State.SHUTDOWN) || this.state.equals(State.FAILED);
  }

  @VisibleForTesting
  synchronized State getState() {
    return this.state;
  }

  private synchronized void failMapping(Throwable throwable) {
    this.logger.error("Failing due to unexpected error.", throwable);
    this.index.getSynonymRegistry().fail(this.definition.name(), throwable.getMessage());
    transitionState(State.FAILED);
  }

  private synchronized void transitionState(State state) {
    this.state = state;
    if (isTerminated()) {
      this.cachedChangedStreamClient.close();
    }
  }

  private synchronized void handleSynonymSyncException(Throwable throwable) {
    if (isTerminated()) {
      if (!SynonymSyncException.isShutDown(throwable)) {
        this.logger.info(
            "Experienced error during synonym sync. Not scheduling more work.", throwable);

        // Otherwise return; encountered shutdown exception in State.SHUTDOWN.
        return;
      }
    }

    if (!(throwable instanceof SynonymSyncException exception)) {
      failMapping(throwable);
      return;
    }
    this.logger.trace(
        "exceptional completion of request with operation time {}", exception.getOperationTime());
    this.synonymMappingHighWaterMark =
        exception
            .getOperationTime()
            .map(SynonymMappingHighWaterMark::create)
            .orElseGet(SynonymMappingHighWaterMark::createEmpty);

    switch (exception.getType()) {
      case FIELD_EXCEEDED, INVALID -> {
        transitionState(State.INVALID);
        if (this.synonymMappingHighWaterMark.isPresent()) {
          // If we have a high water mark, we can watch for changes that occur after the invalid or
          // exceeded event - maybe those changes have corrected a mistake that invalidated a
          // synonym document.
          scheduleOrFail(this::enqueueChangeStream, this.schedulingDelay, this.lifecycleExecutor);
        } else {
          // If we don't have an operation time or resume token to start a change stream from, we'll
          // have to try again.
          scheduleOrFail(this::enqueueCollectionScan, this.schedulingDelay, this.lifecycleExecutor);
        }
        return;
      }
      case DROPPED -> {
        // A dropped collection is the same as a synonym source collection that defines zero synonym
        // mappings; it doesn't define any synonyms, but is not invalid either. This is a legal
        // state for a synonym mapping to be in.
        //
        // Transition the synonym mapping to READY because we have already indexed all synonym
        // documents in the collection.
        transitionState(State.READY);
        if (this.synonymMappingHighWaterMark.isPresent()) {
          // If we have a high water mark, enqueue a change stream and start listening for changes.
          enqueueChangeStream();
        } else {
          // If we don't have a high water mark, enqueue a collection scan so that we can have a
          // valid operation time to start changes streams listening for new documents from.
          enqueueCollectionScan();
        }
        return;
      }
      case FAILED -> {
        failMapping(exception);
        return;
      }
      case SHUTDOWN -> {
        transitionState(State.SHUTDOWN);
        return;
      }
      case TRANSIENT -> {
        // remain in previous state
        scheduleOrFail(this::enqueueCollectionScan, backoffFor(exception), this.lifecycleExecutor);
        return;
      }
    }
    this.logger.error("unknown exception type {}", exception.getType());
    Check.unreachable("Unknown exception type");
  }

  /**
   * Returns the delay before retrying a transient failure: a short jittered backoff when mongod
   * shed the request, otherwise the flat transient backoff.
   */
  @VisibleForTesting
  Duration backoffFor(SynonymSyncException exception) {
    if (!exception.isThrottled()) {
      return this.transientBackoff;
    }
    return Duration.ofMillis(ThreadLocalRandom.current().nextLong(THROTTLED_BACKOFF.toMillis()));
  }

  private static void scheduleOrFail(Runnable runnable, Duration duration, Executor executor) {
    Crash.because("failed to schedule runnable")
        .ifCompletesExceptionally(
            CompletableFuture.runAsync(
                runnable,
                CompletableFuture.delayedExecutor(
                    duration.toMillis(), TimeUnit.MILLISECONDS, executor)));
  }
}
