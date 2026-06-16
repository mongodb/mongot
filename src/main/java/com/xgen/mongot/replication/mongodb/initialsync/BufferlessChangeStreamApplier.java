package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.opTimeFromBatch;
import static com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector.ChangeStreamMode;
import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.Var;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateCommandFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.DocumentEventBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ChangeStreams;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

public class BufferlessChangeStreamApplier implements AutoCloseable {
  /**
   * Max amount of time to spend trying to get within maxLagTime. Defaults to 5 minutes, as defined
   * in {@link InitialSyncConfig}.
   */
  private final Duration timeout;

  /**
   * Max amount of time to lag behind the end of the change stream. Defaults to 1 minute, as defined
   * in {@link InitialSyncConfig}.
   */
  private final Duration maxLagTime;

  private final List<String> excludedChangestreamFields;
  private final boolean matchCollectionUuidForUpdateLookup;

  private final DefaultKeyValueLogger logger;
  private final Clock clock;
  private final InitialSyncContext context;
  private final InitialSyncMongoClient mongoClient;
  private final ServerClusterTimeProvider clusterTimeProvider;
  private final MongoNamespace namespace;

  private final Counter applicableUpdatesCounter;
  private final Counter witnessedUpdatesCounter;
  private final Counter skippedDocumentsWithoutMetadataNamespaceCounter;

  /* Tracks duration of preprocessing a batch */
  private final Timer preprocessingBatchTimer;

  private final Counter totalDocumentsCounter;

  private BsonTimestamp highWaterMark;
  private Optional<ChangeStreamMongoClient<InitialSyncException>> changeStreamMongoClient;
  private Optional<ChangeStreamResumeInfo> resumeInfo;

  private volatile boolean shutdown;

  private final boolean avoidNaturalOrderScanSyncSourceChangeResync;
  private final boolean splitLargeChangeStreamEvents;

  // If true, this is a fresh initial sync (not resuming from a crash). The change stream will
  // start at highWaterMark + 1 since the collection scan already captured everything at
  // highWaterMark. If false, we're resuming from a crash and must use highWaterMark (inclusive)
  // to avoid missing events from multi-document transactions where multiple events share the
  // same optime.
  private final boolean isFreshStart;

  public static final String SKIPPED_DOCUMENTS_WITHOUT_METADATA_NAMESPACE =
      "skippedInitialSyncDocumentsWithoutMetadataNamespace";

  public static final String PREPROCESSING_BATCH_DURATIONS =
      "changeStreamPreprocessingBatchDurations";

  @VisibleForTesting
  BufferlessChangeStreamApplier(
      Clock clock,
      Duration timeout,
      Duration maxLagTime,
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup,
      boolean splitLargeChangeStreamEvents,
      InitialSyncContext context,
      InitialSyncMongoClient mongoClient,
      MongoNamespace namespace,
      BsonTimestamp highWaterMark,
      MetricsFactory metricsFactory,
      boolean avoidNaturalOrderScanSyncSourceChangeResync,
      boolean isFreshStart) {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", context.getIndexId());
    defaultKeyValues.put("generationId", context.getGenerationId());
    this.logger =
        DefaultKeyValueLogger.getLogger(BufferlessChangeStreamApplier.class, defaultKeyValues);

    this.clock = clock;
    this.timeout = timeout;
    this.maxLagTime = maxLagTime;
    this.excludedChangestreamFields = excludedChangestreamFields;
    this.matchCollectionUuidForUpdateLookup = matchCollectionUuidForUpdateLookup;
    this.splitLargeChangeStreamEvents = splitLargeChangeStreamEvents;
    this.context = context;
    this.mongoClient = mongoClient;
    this.clusterTimeProvider = mongoClient::getMaxValidMajorityReadOptime;
    this.namespace = namespace;

    this.highWaterMark = highWaterMark;
    this.changeStreamMongoClient = Optional.empty();
    this.resumeInfo = Optional.empty();

    this.shutdown = false;
    this.isFreshStart = isFreshStart;

    Tags replicationTag = Tags.of(ServerStatusDataExtractor.Scope.REPLICATION.getTag());
    this.witnessedUpdatesCounter =
        metricsFactory.counter(
            ServerStatusDataExtractor.ReplicationMeterData.InitialSyncMeterData.WITNESSED_UPDATES,
            replicationTag);
    this.applicableUpdatesCounter =
        metricsFactory.counter(
            ServerStatusDataExtractor.ReplicationMeterData.InitialSyncMeterData.APPLICABLE_UPDATES,
            replicationTag);
    this.skippedDocumentsWithoutMetadataNamespaceCounter =
        metricsFactory.counter(SKIPPED_DOCUMENTS_WITHOUT_METADATA_NAMESPACE);
    this.preprocessingBatchTimer = metricsFactory.timer(PREPROCESSING_BATCH_DURATIONS);
    this.totalDocumentsCounter = metricsFactory.counter("changeStreamTotalDocuments");
    this.avoidNaturalOrderScanSyncSourceChangeResync = avoidNaturalOrderScanSyncSourceChangeResync;
  }

  @Override
  public void close() throws InitialSyncException {
    this.changeStreamMongoClient.ifPresent(ChangeStreamMongoClient::close);
  }

  BsonTimestamp applyEvents(
      BsonValue lastScannedToken, BsonTimestamp stopAfterOpTime, boolean continueSync)
      throws InitialSyncException {
    this.logger.info("Starting an event application phase.");
    openChangeStreamMongoClient();

    // Continue applying events while the position of the change stream cursor is before
    // stopAfterOpTime.
    @Var
    CompletableFuture<Void> indexingFuture =
        processEventsUpToScan(lastScannedToken, stopAfterOpTime);

    // Try to get within maxLagTime of the end of the change stream. Skip this if we're done
    // scanning the collection as we already have a valid snapshot and are ready to transition the
    // index to STEADY_STATE.
    if (continueSync) {
      indexingFuture = processEventsUpToLag(indexingFuture, lastScannedToken);
    }

    try {
      // Wait for the last batch to finish indexing, whichever phase it is from.
      InitialSyncException.getOrWrapThrowable(
          indexingFuture, InitialSyncException.Phase.CHANGE_STREAM);
    } catch (Exception e) {
      cancelIndexing(e);
      throw e;
    }

    return this.highWaterMark;
  }

  void signalShutdown() {
    this.shutdown = true;
  }

  /**
   * Return the ChangeStreamResumeInfo indicating the position in the change stream where steady
   * state replication can resume from.
   */
  Optional<ChangeStreamResumeInfo> getResumeInfo() {
    return this.resumeInfo;
  }

  /**
   * Opens a change stream if one has not been opened yet. We have to instantiate the
   * ChangeStreamMongoClient in the worker thread rather than the BufferlessInitialSyncManager
   * thread since InitialSyncMongoClient will fail if interrupted, and we have to support
   * interruption in the BufferlessInitialSyncManager thread.
   */
  private void openChangeStreamMongoClient() throws InitialSyncException {
    if (this.changeStreamMongoClient.isEmpty()) {
      // Validate that we opened a cursor against the correct collection.
      this.mongoClient.ensureCollectionNameUnchanged(
          this.context.getIndexDefinition(), this.namespace.getCollectionName());

      this.changeStreamMongoClient =
          Optional.of(
              this.mongoClient.getChangeStreamMongoClient(
                  getAggregateCommand(),
                  this.namespace,
                  this.context.getInitialSyncMetricsUpdater(),
                  this.context.getEmbeddingGetMoreBatchSize(),
                  this.context.getGenerationId()));
    }
  }

  // Builds the MongoDB aggregation command used to open or resume a change stream.
  //
  // The logic distinguishes between two cases:
  // 1. Fresh start (isFreshStart=true): Use highWaterMark + 1. This is safe because the collection
  //    scan snapshot already includes everything at the highWaterMark tick. The +1 avoids
  //    reprocessing events that were already captured by the collection scan.
  // 2. Resuming from crash (isFreshStart=false): Use highWaterMark (inclusive) to ensure we don't
  //    miss any events from multi-document transactions where many events share the same optime.
  //    Reprocessing is safe because the replication logic is idempotent.
  private ChangeStreamAggregateCommand getAggregateCommand() {
    // this.context.removeMatchCollectionUuid will be true if change stream commands are failing
    // because matchCollectionUuidForUpdateLookup is unrecognized.
    ChangeStreamAggregateCommandFactory factory =
        new ChangeStreamAggregateCommandFactory(
            this.context.getIndexDefinition(),
            this.namespace,
            this.excludedChangestreamFields,
            !this.context.isRemoveMatchCollectionUuid() && this.matchCollectionUuidForUpdateLookup,
            this.splitLargeChangeStreamEvents);
    ChangeStreamMode mode = ChangeStreamMode.getDefault();

    if (this.isFreshStart) {
      // For a fresh start, use highWaterMark + 1. The collection scan already captured everything
      // at highWaterMark, so we start after it to avoid reprocessing.
      BsonTimestamp startOpTime =
          new BsonTimestamp(this.highWaterMark.getTime(), this.highWaterMark.getInc() + 1);
      this.logger
          .atInfo()
          .addKeyValue("opTime", startOpTime)
          .addKeyValue("isFreshStart", true)
          .log("Starting change stream from opTime (+1 tick for fresh start)");
      return factory.fromOperationTime(
          startOpTime, this.context.getIndexDefinition().getIndexId(), mode);
    } else {
      // For resuming from a crash, use highWaterMark (inclusive) to ensure we don't miss events
      // from multi-document transactions. Reprocessing is safe because replication is idempotent.
      this.logger
          .atInfo()
          .addKeyValue("opTime", this.highWaterMark)
          .addKeyValue("isFreshStart", false)
          .log("Starting change stream from opTime (inclusive for resume)");
      return factory.fromOperationTime(
          this.highWaterMark, this.context.getIndexDefinition().getIndexId(), mode);
    }
  }

  private CompletableFuture<Void> processEventsUpToScan(
      BsonValue lastScannedToken, BsonTimestamp stopAfterOpTime) throws InitialSyncException {
    // Synchronize on prior indexing, and get a batch to be indexed after. To begin, there is no
    // indexing to wait on, so this future is set as completed.
    @Var CompletableFuture<Void> indexingFuture = CompletableFuture.completedFuture(null);

    try {
      // Handle the initial batch since we need some ChangeStreamResumeInfo to return.
      indexingFuture = scheduleNextWhenCompletes(indexingFuture, lastScannedToken);

      // Keep applying events while the opTime of the last applied batch is behind the opTime at
      // which we finished the last collection scan.
      while (stopAfterOpTime.compareTo(this.highWaterMark) > 0) {
        if (this.shutdown) {
          handleShutdown();
          return Check.unreachable("handleShutdown() should have thrown InitialSyncException");
        }

        indexingFuture = scheduleNextWhenCompletes(indexingFuture, lastScannedToken);
      }

      // Continue with the next sub-phase of event application.
      return indexingFuture;
    } catch (Exception e) {
      cancelIndexing(e);
      throw e;
    }
  }

  private CompletableFuture<Void> processEventsUpToLag(
      @Var CompletableFuture<Void> indexingFuture, BsonValue lastScannedToken)
      throws InitialSyncException {
    Instant stopTime = Instant.now(this.clock).plus(this.timeout);

    try {
      while (!caughtUp(stopTime)) {
        if (this.shutdown) {
          handleShutdown();
          return Check.unreachable("handleShutdown() should have thrown InitialSyncException");
        }

        indexingFuture = scheduleNextWhenCompletes(indexingFuture, lastScannedToken);
      }

      return indexingFuture;
    } catch (Exception e) {
      cancelIndexing(e);
      throw e;
    }
  }

  /**
   * Prepares the next batch, waits for the indexingFuture to complete, then schedules the batch and
   * returns its indexing future. If we fail to get the next batch, we wait until the indexing
   * future completes to throw the exception.
   */
  private CompletableFuture<Void> scheduleNextWhenCompletes(
      CompletableFuture<Void> indexingFuture, BsonValue lastScannedToken)
      throws InitialSyncException {
    DocumentEventBatch batch;
    try {
      batch = getNextBatch();
    } catch (InitialSyncException e) {
      // Wait for the last indexing to complete before rethrowing.
      InitialSyncException.getOrWrapThrowable(
          indexingFuture, InitialSyncException.Phase.CHANGE_STREAM);
      throw e;
    }

    // Wait for the previous batch to finish indexing.
    InitialSyncException.getOrWrapThrowable(
        indexingFuture, InitialSyncException.Phase.CHANGE_STREAM);

    // When previous indexing finishes, schedule the current batch.
    return this.context
        .schedule(
            batch.finalChangeEvents,
            SchedulerQueue.Priority.INITIAL_SYNC_CHANGE_STREAM,
            getCommitUserData(lastScannedToken))
        .thenRun(() -> updateBatchCounters(batch));
  }

  /**
   * Gets the next batch and preprocesses it through ChangeStreamDocumentUtils.handleDocumentEvents.
   */
  private DocumentEventBatch getNextBatch() throws InitialSyncException {
    checkState(
        this.changeStreamMongoClient.isPresent(),
        "ChangeStreamMongoClient has not been instantiated");
    ChangeStreamBatch batch = this.changeStreamMongoClient.get().getNext();
    ChangeStreamDocumentUtils.recordChangeStreamEventSizes(
        batch.getRawEvents(),
        this.context.getIndexMetricsUpdater().getIndexingMetricsUpdater()::recordDocumentSizeBytes);

    var preprocessingBatchTimer = Stopwatch.createStarted();
    // Update the ChangeStreamResumeInfo and the highWaterMark so that we have something to return
    // even if we have an empty batch.
    this.resumeInfo =
        Optional.of(ChangeStreamResumeInfo.create(this.namespace, batch.getPostBatchResumeToken()));
    this.highWaterMark = opTimeFromBatch(batch);

    ChangeStreamDocumentUtils.DocumentEventBatch documentEventBatch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            // Inapplicable updates are already filtered out in processBatch.
            processBatch(batch),
            this.context.getIndexDefinition(),
            this.context
                .getIndexDefinition()
                .createFieldDefinitionResolver(this.context.getIndexFormatVersion()),
            true /* areUpdateEventsPrefiltered */);

    this.totalDocumentsCounter.increment(batch.getRawEvents().size());
    this.preprocessingBatchTimer.record(preprocessingBatchTimer.stop().elapsed());

    return documentEventBatch;
  }

  private void updateBatchCounters(DocumentEventBatch documentEventBatch) {
    this.applicableUpdatesCounter.increment(documentEventBatch.updatesApplicable);
    this.witnessedUpdatesCounter.increment(documentEventBatch.updatesWitnessed);
    this.skippedDocumentsWithoutMetadataNamespaceCounter.increment(
        documentEventBatch.skippedDocumentsWithoutMetadataNamespace);

    if (documentEventBatch.applicableDocumentsTotal > 0) {
      this.context
          .getInitialSyncMetricsUpdater()
          .getChangeStreamBatchTotalApplicableDocuments()
          .record(documentEventBatch.applicableDocumentsTotal);

      this.context
          .getInitialSyncMetricsUpdater()
          .getChangeStreamBatchTotalApplicableBytes()
          .record(documentEventBatch.applicableDocumentsTotalBytes);

      this.context
          .getInitialSyncMetricsUpdater()
          .getTotalApplicableBytes()
          .increment(documentEventBatch.applicableDocumentsTotalBytes);
    }
  }

  private List<ChangeStreamDocument<RawBsonDocument>> processBatch(
      ChangeStreamBatch batch) throws InitialSyncException {
    List<ChangeStreamDocument<RawBsonDocument>> documentEventsToApply = new ArrayList<>();

    for (ChangeStreamDocument<RawBsonDocument> event :
        ChangeStreamDocumentUtils.asLazyDecodableChangeStreamDocuments(batch.getRawEvents())) {
      switch (event.getOperationType()) {
        case UPDATE -> {
          // Only buffer updates that actually impact fields that we're indexing.
          UpdateDescription updateDescription = event.getUpdateDescription();
          Check.argNotNull(updateDescription, "updateDescription");
          if (!ChangeStreams.updateDescriptionAppliesToIndex(
              updateDescription,
              this.context
                  .getIndexDefinition()
                  .createFieldDefinitionResolver(this.context.getIndexFormatVersion()),
              this.context.getIndexDefinition().getView())) {
            continue;
          }
        }
        case DROP, DROP_DATABASE ->
            throw InitialSyncException.createDropped(
                String.format("from change stream event: %s", event.getOperationType().getValue()));
        case RENAME -> {
          if (ChangeStreams.renameCausedCollectionDrop(event, this.namespace)) {
            this.logger
                .atInfo()
                .addKeyValue("renamedFrom", event.getNamespace())
                .addKeyValue("renamedTo", event.getDestinationNamespace())
                .addKeyValue("currentNamespace", this.namespace)
                .log("rename event caused collection drop");
            throw InitialSyncException.createDropped(
                String.format("collection overwritten by %s", event.getNamespace()));
          }

          // If the collection scan was going on still, that cursor will have been invalidated.
          // If the scan was done, then we're still applying events because we haven't reached
          // minValidOpTime. We could potentially try to follow the rename in the future, but for
          // now we'll restart the sync.
          throw InitialSyncException.createRequiresResync("collection was renamed");

          // If the collection scan was going on still, that cursor will have been invalidated.
          // If the scan was done, then we're still applying events because we haven't reached
          // minValidOpTime. We could potentially try to follow the rename in the future, but for
          // now we'll restart the sync.
        }
        case OTHER ->
            throw InitialSyncException.createRequiresResync(
                String.format(
                    "witnessed unknown change stream event: %s",
                    event.getOperationType().getValue()));
        case INVALIDATE -> {
          this.logger
              .atInfo()
              .addKeyValue("namespace", event.getNamespace())
              .addKeyValue("database", event.getDatabaseName())
              .addKeyValue("clusterTime", event.getClusterTime())
              .log("Witnessed invalidate event. Performing a full resync.");
          // For an invalidate event, for simplicity, perform a full resync of the index build
          // Resume is not possible when we use highwater mark inclusively as that would result in
          // an infinite loop. If we don't issue a full re-sync, then there is a chance we are
          // struck in a loop because we process the same event again as we are not advancing the
          // high water mark while we resume after a crash from processing change stream update.
          throw InitialSyncException.createRequiresResync("witnessed invalidate event");
        }
        default -> {
          // Apply all other operation types.
        }
      }

      documentEventsToApply.add(event);
    }

    return documentEventsToApply;
  }

  /**
   * The change stream event application phase ends when at least one of the following conditions
   * are met.
   *
   * <ol>
   *   <li>We've reached the timeout.
   *   <li>The last event applied is within maxLagTime of the end of the change stream.
   * </ol>
   */
  private boolean caughtUp(Instant stopTime) throws InitialSyncException {
    Duration lag =
        Duration.between(
            Instant.ofEpochSecond(this.highWaterMark.getTime()),
            Instant.ofEpochSecond(this.clusterTimeProvider.getCurrentClusterTime().getTime()));
    return Instant.now(this.clock).isAfter(stopTime) || lag.compareTo(this.maxLagTime) <= 0;
  }

  private IndexCommitUserData getCommitUserData(BsonValue lastScannedToken) {
    return IndexCommitUserData.createInitialSyncResume(
        this.context.getIndexFormatVersion(),
        InitialSyncResumeInfo.create(
            this.context.useNaturalOrderScan(),
            this.highWaterMark,
            lastScannedToken,
            this.avoidNaturalOrderScanSyncSourceChangeResync
                ? Optional.of(this.mongoClient.getSyncSourceHost())
                : Optional.empty()));
  }

  /**
   * On shutdown signal, processing is cancelled and an exception is thrown. We need to do the /*
   * same in the case of an unexpected error during processing.
   */
  private void cancelIndexing(Exception e) {
    if (!this.shutdown) {
      FutureUtils.getAndSwallow(
          this.context.cancel(e),
          error -> this.logger.error("Failure during indexing cancellation", error));
    }
  }

  /**
   * Cancels any batches in the scheduler, waits for any in-progress batches to finish, then throws
   * a shutdown InitialSyncException.
   */
  private void handleShutdown() throws InitialSyncException {
    InitialSyncException shutdownException = InitialSyncException.createShutDown();
    // Cancel processing for this index, to remove any work in the scheduler's queue.
    CompletableFuture<Void> cancelFuture = this.context.cancel(shutdownException);
    // Wait for all work to be finished.
    InitialSyncException.getOrWrapThrowable(cancelFuture, InitialSyncException.Phase.CHANGE_STREAM);

    throw shutdownException;
  }
}
