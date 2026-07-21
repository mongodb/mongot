package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.bsonDocumentToChangeStreamDocument;
import static com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.indexOfLifecycleEvent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.DocumentEventBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ChangeStreams;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.Priority;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.common.ViewFieldDependencies;
import io.micrometer.core.instrument.Timer;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.bson.RawBsonDocument;

class DecodingExecutorChangeStreamIndexManager extends ChangeStreamIndexManager {

  /* In-order decoding per generation id. */
  private final DecodingWorkScheduler decodingScheduler;
  private final Optional<AutoEmbeddingMaterializedViewConfig> autoEmbeddingMaterializedViewConfig;

  /* View-pipeline field dependencies are a pure function of the immutable index definition, so
   * they are derived once and reused across change-stream batches. */
  private final Supplier<Optional<ViewFieldDependencies>> viewFieldDependencies;

  @GuardedBy("this")
  private Optional<Throwable> lifecycleException;

  private DecodingExecutorChangeStreamIndexManager(
      IndexDefinition indexDefinition,
      DefaultKeyValueLogger logger,
      IndexingWorkScheduler indexingWorkScheduler,
      DocumentIndexer documentIndexer,
      MongoNamespace namespace,
      Consumer<ChangeStreamResumeInfo> resumeInfoUpdater,
      IndexMetricsUpdater indexMetricsUpdater,
      CompletableFuture<Void> lifecycleFuture,
      GenerationId generationId,
      DecodingWorkScheduler decodingScheduler,
      Optional<AutoEmbeddingMaterializedViewConfig> autoEmbeddingMaterializedViewConfig) {
    super(
        indexDefinition,
        logger,
        indexingWorkScheduler,
        documentIndexer,
        namespace,
        resumeInfoUpdater,
        indexMetricsUpdater,
        lifecycleFuture,
        generationId);
    this.decodingScheduler = decodingScheduler;
    this.autoEmbeddingMaterializedViewConfig = autoEmbeddingMaterializedViewConfig;
    this.lifecycleException = Optional.empty();
    this.viewFieldDependencies =
        Suppliers.memoize(
            () -> ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDefinition));
  }

  static DecodingExecutorChangeStreamIndexManager createWithDecodingScheduler(
      IndexDefinition indexDefinition,
      IndexingWorkScheduler indexingWorkScheduler,
      DocumentIndexer documentIndexer,
      MongoNamespace namespace,
      Consumer<ChangeStreamResumeInfo> resumeInfoUpdater,
      IndexMetricsUpdater indexMetricsUpdater,
      CompletableFuture<Void> lifecycleFuture,
      GenerationId generationId,
      DecodingWorkScheduler decodingScheduler) {
    return createWithDecodingScheduler(
        indexDefinition,
        indexingWorkScheduler,
        documentIndexer,
        namespace,
        resumeInfoUpdater,
        indexMetricsUpdater,
        lifecycleFuture,
        generationId,
        decodingScheduler,
        Optional.empty());
  }

  static DecodingExecutorChangeStreamIndexManager createWithDecodingScheduler(
      IndexDefinition indexDefinition,
      IndexingWorkScheduler indexingWorkScheduler,
      DocumentIndexer documentIndexer,
      MongoNamespace namespace,
      Consumer<ChangeStreamResumeInfo> resumeInfoUpdater,
      IndexMetricsUpdater indexMetricsUpdater,
      CompletableFuture<Void> lifecycleFuture,
      GenerationId generationId,
      DecodingWorkScheduler decodingScheduler,
      Optional<AutoEmbeddingMaterializedViewConfig> autoEmbeddingMaterializedViewConfig) {

    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", generationId.indexId);
    defaultKeyValues.put("generationId", generationId);
    var logger =
        DefaultKeyValueLogger.getLogger(
            DecodingExecutorChangeStreamIndexManager.class, defaultKeyValues);

    logger.info("Created decoding executor manager for index");

    return new DecodingExecutorChangeStreamIndexManager(
        indexDefinition,
        logger,
        indexingWorkScheduler,
        documentIndexer,
        namespace,
        resumeInfoUpdater,
        indexMetricsUpdater,
        lifecycleFuture,
        generationId,
        decodingScheduler,
        autoEmbeddingMaterializedViewConfig);
  }

  @Override
  BatchInfo indexBatch(
      ChangeStreamBatch batch,
      DocumentMetricsUpdater metricsUpdater,
      Timer preprocessingBatchTimer) {
    synchronized (this) {
      // If we're given a batch after we've been shut down, return a failed future with a SHUT_DOWN
      // SteadyStateException to signal that we've been shut down.
      if (this.isShutdown()) {
        return new BatchInfo(CompletableFuture.failedFuture(SteadyStateException.createShutDown()));
      }

      // Otherwise the external future should not have completed yet.
      if (this.lifecycleException.isPresent()) {
        return new BatchInfo(CompletableFuture.failedFuture(this.lifecycleException.get()));
      }

      var timer = Stopwatch.createStarted();

      // Schedule the batch to be indexed, and update the resumeInfo only after the batch has been
      // indexed.
      Optional<ChangeStreamResumeInfo> resumeInfo = getResumeInfo(batch);

      // Look at the last two events in batch for a lifecycle event (DROP, RENAME, INVALIDATE). If
      // found, no new batches will be submitted by upper logic and `lifecycleFuture` will get
      // completed exceptionally after indexing completes.
      Optional<SteadyStateException> lifecycleEvent = getMaybeLifecycleEventException(batch);

      preprocessingBatchTimer.record(timer.stop().elapsed());

      // Whether this batch includes a lifecycleEvent. Note: on RENAME events, a lifecycle event is
      // triggered when processing the following event. To successfully catch and remove RENAME we
      // look if `renameNamespace` is present.
      boolean hasLifecycleEvent = lifecycleEvent.isPresent() || this.renameNamespace.isPresent();

      CompletableFuture<Void> schedulingFuture = new CompletableFuture<>();

      // Schedule the given batch for decoding. Decoding will be done in-order (FIFO) per index
      // (generation id). In case a batch decoding ended exceptionally, no further decoding will get
      // scheduled for that index and schedulingFuture will end exceptionally. Resulting with
      // cancellation of all pending decoding and indexing work for that index.
      this.decodingScheduler
          .schedule(
              this.generationId,
              Optional.of(this.attemptId),
              batch.getRawEvents(),
              Priority.STEADY_STATE_CHANGE_STREAM,
              events -> {
                // Record the size of every raw event in the batch. This runs on the decoding
                // thread rather than the IO/dispatch thread so the (batch-length) traversal doesn't
                // block the thread that fetches the next change-stream batch (CLOUDP-353521).
                ChangeStreamDocumentUtils.recordChangeStreamEventSizes(
                    events,
                    this.indexMetricsUpdater.getIndexingMetricsUpdater()::recordDocumentSizeBytes);

                // A list view of change-stream documents based on given raw documents. The view
                // excludes non-CRUD events from the end of the list. Note: decoding from raw
                // document to change-stream document happens when calling List#get.
                List<ChangeStreamDocument<RawBsonDocument>> indexableChangeStreamEvents =
                    getIndexableChangeStreamEvents(batch.getRawEvents(), hasLifecycleEvent);

                // Asynchronously decode and dedupe events. If an unexpected document type or
                // lifecycle event is detected during traversal throws a SteadyStateException.
                DocumentEventBatch documentEventBatch =
                    ChangeStreamDocumentUtils.handleDocumentEvents(
                        // Inapplicable updates are not filtered out because decoding happens lazily
                        // and the batch has not been traversed up to this point.
                        indexableChangeStreamEvents,
                        this.indexDefinition,
                        this.indexDefinition.createFieldDefinitionResolver(
                            this.generationId.generation.indexFormatVersion),
                        false /* areUpdateEventsPrefiltered */,
                        this.viewFieldDependencies.get(),
                        this.autoEmbeddingMaterializedViewConfig);

                metricsUpdater.accept(documentEventBatch);

                // Schedule the given batch for indexing if not shut down or lifecycle future is
                // not completed. this.scheduleIndexing, ChangeStreamIndexManager#failLifecycle,
                // ChangeStreamIndexManager#indexBatch and ChangeStreamIndexManager#shutdown
                // calls are serialized (synchronized) to ensure threads see the latest state.
                scheduleIndexingAsync(documentEventBatch.finalChangeEvents, resumeInfo)
                    .whenComplete(
                        (unused, indexingFailure) -> {
                          if (indexingFailure != null) {
                            schedulingFuture.completeExceptionally(indexingFailure);
                          } else {
                            schedulingFuture.complete(null);
                            recordPerBatchMetrics(documentEventBatch);
                          }
                        });
              },
              this.indexMetricsUpdater.getReplicationMetricsUpdater())
          .exceptionally(
              decodingFailure -> {
                schedulingFuture.completeExceptionally(decodingFailure);
                return null;
              });

      // Attach a future stage pipeline to handle: post indexing resume info updates, and indexing
      // future completed exceptionally.
      CompletableFuture<Void> indexingFuture = new CompletableFuture<>();

      this.latestIndexingFuture =
          postIndexingPipeline(
              schedulingFuture,
              indexingFuture,
              resumeInfo,
              lifecycleEvent,
              batch.getCommandOperationTime());

      return new BatchInfo(indexingFuture, lifecycleEvent);
    }
  }

  /**
   * Cancels any work that is queued for this index, either decoding or indexing, and completes the
   * lifecycle future after the in-flight indexing batches are processed.
   *
   * <p>`getMore()` operations run concurrently to decoding and indexing work. Work cancellation
   * removes currently pending task and waits for in-flight to complete, but new work could be
   * scheduled right after. To eliminate races in calling `decodingScheduler.schedule()` after
   * `decodingScheduler.cancel()` and `indexingScheduler.cancel()` were called but before
   * `lifecycleFuture` is completed exceptionally, possibly leaving the index in inconsistent state
   * if a later batch got scheduled. We capture the lifecycle exception to fail any incoming batch
   * after `failLifecycle` is called with passed reason.
   */
  @Override
  void failLifecycle(Throwable reason) {
    synchronized (this) { // https://github.com/mockito/mockito/issues/2970
      // Ensures this method is executed only once per index generation lifecycle.
      // If a lifecycleException has already been detected,
      // exit early to prevent the following:
      // 1. Inconsistent error messages across hosts.
      // 2. Premature cancellation of indexing jobs tied to subsequent attempts for the same index
      // generation lifecycle.
      if (this.lifecycleException.isPresent()) {
        this.logger
            .atWarn()
            .addKeyValue("generationId", this.generationId)
            .addKeyValue("reason", reason)
            .log("Skipping failLifecycle because it's called once already");
        return;
      }
      this.setLifeCycleException(reason);
      this.decodingScheduler
          .cancel(this.generationId, Optional.of(this.attemptId), reason)
          .thenCompose(
              unused ->
                  this.indexingWorkScheduler.cancel(
                      this.generationId, Optional.of(this.attemptId), reason))
          .handle(
              (result, throwable) -> {
                this.logger.warn(
                    "failing generation lifecycle after waiting for any in-flight "
                        + "batches to complete: {}",
                    this.generationId);
                return this.lifecycleFuture.completeExceptionally(reason);
              });
    }
  }

  @VisibleForTesting
  synchronized void setLifeCycleException(Throwable reason) {
    this.lifecycleException = Optional.of(reason);
  }

  /**
   * Schedule a decoded batch for indexing. In case we saw a lifecycle event skip scheduling as
   * pending work is canceled anyway. In case of shutdown schedule the decoded batch as we wait for
   * latestIndexingFuture to complete. Note: new batches passed to `indexBatch` after shutdown will
   * be rejected.
   */
  private CompletableFuture<Void> scheduleIndexingAsync(
      List<DocumentEvent> finalChangeEvents, Optional<ChangeStreamResumeInfo> resumeInfo) {
    synchronized (this) { // https://github.com/mockito/mockito/issues/2970

      // Nothing to do if lifecycle event was detected.
      if (this.lifecycleException.isPresent()) {
        return CompletableFuture.failedFuture(this.lifecycleException.get());
      }

      return this.indexingWorkScheduler.schedule(
          finalChangeEvents,
          SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
          this.documentIndexer,
          this.generationId,
          Optional.of(this.attemptId),
          resumeInfo.map(
              info ->
                  IndexCommitUserData.createChangeStreamResume(
                      info, this.generationId.generation.indexFormatVersion)),
          this.indexMetricsUpdater.getIndexingMetricsUpdater());
    }
  }

  /** Returns a list which lazily decodes raw bson events as the list is traversed. */
  @VisibleForTesting
  protected List<ChangeStreamDocument<RawBsonDocument>> getIndexableChangeStreamEvents(
      List<RawBsonDocument> events, boolean hasLifecycleEvent) {

    // If a lifecycle event was found, create a sublist view excluding the lifecycle event(s).
    var eventsToIndex =
        hasLifecycleEvent ? events.subList(0, indexOfLifecycleEvent(events)) : events;

    return ChangeStreamDocumentUtils.asLazyDecodableChangeStreamDocumentsWithEventValidation(
        eventsToIndex, this::checkNonIndexableEvent);
  }

  /**
   * Checks whether the given event is a lifecycle or unexpected. If so returns a matching
   * exception.
   *
   * <p>In case of DROP or DROP_DATABASE event, the index is dropped. In case of RENAME, INVALIDATE
   * and OTHER in the middle of the batch, require an initial sync.
   *
   * <p>This behavior matches for OTHER event and a RENAME without a following INVALIDATE, as if
   * detected during pre-processing lifecycle check. For RENAME with following INVALIDATE or
   * orphaned INVALIDATE in middle of batch, we require initial-sync instead of resume, but those
   * are unexpected scenarios as RENAME should always be followed with an INVALIDATE, and INVALIDATE
   * is expected to be the last event in change-stream.
   */
  private Optional<SteadyStateException> checkNonIndexableEvent(
      ChangeStreamDocument<RawBsonDocument> event) {
    return switch (event.getOperationType()) {
      case INSERT, UPDATE, REPLACE, DELETE -> Optional.empty();
      case DROP, DROP_DATABASE -> {
        logChangeStreamEventDetails(event);
        yield Optional.of(
            SteadyStateException.createDropped(
                String.format(
                    "Unexpected operation type %s, clusterTime=%s, resumeToken=%s",
                    event.getOperationType(), event.getClusterTime(), event.getResumeToken())));
      }
      case RENAME, INVALIDATE, OTHER -> {
        logChangeStreamEventDetails(event);
        yield Optional.of(
            SteadyStateException.createRequiresResync(
                String.format(
                    "Unexpected operation type %s, clusterTime=%s, resumeToken=%s",
                    event.getOperationType(), event.getClusterTime(), event.getResumeToken())));
      }
    };
  }

  /**
   * Invalidating events (DROP, DROP_DATABASE and RENAME) are guaranteed to be the last two events
   * in stream, if they happen. Look at the last two events to search for a lifecycleEvent. If
   * found, return an exception to trigger lifecycle changes.
   *
   * @return An {@link Optional} of SteadyStateException describing the change-stream
   *     lifecycleEvents. Otherwise {@link Optional#empty()}.
   */
  private Optional<SteadyStateException> getMaybeLifecycleEventException(ChangeStreamBatch batch) {
    int numEvents = batch.getRawEvents().size();
    if (numEvents == 0) {
      return Optional.empty();
    }

    if (numEvents > 1) {
      Optional<SteadyStateException> lifecycleEvent =
          checkForInvalidatingEvent(batch.getRawEvents().get(numEvents - 2));

      if (lifecycleEvent.isPresent()) {
        return lifecycleEvent;
      }
    }

    return checkForInvalidatingEvent(batch.getRawEvents().get(numEvents - 1));
  }

  private Optional<SteadyStateException> checkForInvalidatingEvent(
      RawBsonDocument rawChangeStreamEvent) {
    ChangeStreamDocument<RawBsonDocument> event =
        bsonDocumentToChangeStreamDocument(rawChangeStreamEvent);

    // If the last event was a rename, this should be an INVALIDATE.
    if (this.renameNamespace.isPresent()) {
      return Optional.of(handleRenameEvent(event));
    }

    switch (event.getOperationType()) {
      case DROP, DROP_DATABASE -> {
        logChangeStreamEventDetails(event);
        return Optional.of(SteadyStateException.createDropped());
      }
      case RENAME -> {
        logChangeStreamEventDetails(event);
        if (Objects.isNull(event.getDestinationNamespace())) {
          return Optional.of(
              SteadyStateException.createNonInvalidatingResync(
                  new MongoChangeStreamException(
                      "RENAME event did not have destination namespace")));
        }
        if (ChangeStreams.renameCausedCollectionDrop(event, this.namespace)) {
          this.logger.info(
              "rename event caused collection drop. Renamed: {} to: {} (current namespace: {})",
              event.getNamespace(),
              event.getDestinationNamespace(),
              this.namespace);

          return Optional.of(
              SteadyStateException.createDropped(
                  String.format("collection overwritten by %s", event.getNamespace())));
        }

        // collection was renamed but not dropped:
        MongoNamespace destinationNamespace =
            Objects.requireNonNull(
                event.getDestinationNamespace(),
                "RENAME event did not have a destination namespace");

        // If this event is a RENAME, flag that so that the next event (which should be an
        // INVALIDATE) can be handled.
        this.renameNamespace = Optional.of(destinationNamespace);
      }
      case INVALIDATE -> {
        // If we saw an INVALIDATE event without a preceding RENAME event, get the resumeToken to
        // be used in startAfter.
        logChangeStreamEventDetails(event);
        return Optional.of(
            SteadyStateException.createInvalidated(
                ChangeStreamResumeInfo.create(this.namespace, event.getResumeToken())));
      }
      case OTHER -> {
        // If we saw an event we can't process, then there's no use in trying to restart
        // the change stream. Resync and hope for the best.
        logChangeStreamEventDetails(event);
        return Optional.of(
            SteadyStateException.createRequiresResync(
                String.format("witnessed unknown change stream event: %s", event)));
      }
      case INSERT, UPDATE, REPLACE, DELETE -> {
        // No-op
      }
    }

    return Optional.empty();
  }
}
