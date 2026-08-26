package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FutureUtils;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/** Responsible for scanning a collection during a bufferless initial sync. */
public class BufferlessCollectionScanner {

  protected static final String ID_KEY = "_id";

  private final DefaultKeyValueLogger logger;
  private final Clock clock;

  private final CollectionScanBatchPreprocessor batchPreprocessor;

  private long docsQueuedForIndexing = 0;
  private long docsIndexed = 0;

  private volatile boolean shutdown;

  private final boolean avoidNaturalOrderScanSyncSourceChangeResync;

  protected final InitialSyncContext context;
  protected final InitialSyncMongoClient mongoClient;

  /**
   * lastScannedToken stores the last scanned document where the cursor could be resumed from.
   *
   * <p>lastScannedToken will be the last document _id(@BsonValue) for _id scan and
   * postBatchResumeToken(@BsonDocument) for natural order scan, which is coupled with
   * context.useNaturalOrderScan() upon class construction.
   */
  protected BsonValue lastScannedToken;

  @VisibleForTesting
  protected BufferlessCollectionScanner(
      Clock clock,
      InitialSyncContext context,
      InitialSyncMongoClient mongoClient,
      BsonValue lastScannedToken,
      MetricsFactory metricsFactory,
      boolean avoidNaturalOrderScanSyncSourceChangeResync) {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", context.getIndexId());
    defaultKeyValues.put("generationId", context.getGenerationId());
    this.logger =
        DefaultKeyValueLogger.getLogger(BufferlessCollectionScanner.class, defaultKeyValues);

    this.clock = clock;
    this.context = context;
    this.mongoClient = mongoClient;
    this.lastScannedToken = lastScannedToken;
    this.batchPreprocessor = new CollectionScanBatchPreprocessor(context, metricsFactory);
    this.avoidNaturalOrderScanSyncSourceChangeResync = avoidNaturalOrderScanSyncSourceChangeResync;
    this.shutdown = false;
  }

  /**
   * Scans the collection and indexes the documents into the Index. Returns a boolean indicating
   * whether the scan needs to continue.
   */
  Result scanWithTimeLimit(Duration collectionScanTime) throws InitialSyncException {
    this.logger.info("Starting a collection scan phase.");

    // Synchronize on prior indexing, and buffer a batch to be indexed after. To begin, there is no
    // indexing to wait on, so this future is set as complete.
    @Var CompletableFuture<Void> indexingFuture = CompletableFuture.completedFuture(null);
    CollectionScanMongoClient<InitialSyncException> mongoClient = getClient();

    try (mongoClient) {
      Instant timeLimit = Instant.now(this.clock).plus(collectionScanTime);

      while (mongoClient.hasNext() && Instant.now(this.clock).isBefore(timeLimit)) {
        if (this.shutdown) {
          handleShutdown();
          return Check.unreachable("handleShutdown() should have thrown InitialSyncException");
        }

        indexingFuture = scheduleNextWhenCompletes(indexingFuture, mongoClient);
      }

      // Wait for the last indexing to complete
      InitialSyncException.getOrWrapThrowable(
          indexingFuture, InitialSyncException.Phase.COLLECTION_SCAN);
      this.logger
          .atInfo()
          .addKeyValue("numDocumentsIndexed", this.docsIndexed + this.docsQueuedForIndexing)
          .log("Finished a collection scan phase.");

      return new Result(mongoClient.hasNext(), this.lastScannedToken);
    } catch (Exception e) {
      // On shutdown signal, processing is cancelled and a shutdown exception is thrown, but we also
      // need to cancel it in case of unexpected error during processing.
      boolean isShutdownException =
          e instanceof InitialSyncException && ((InitialSyncException) e).isShutdown();
      if (!isShutdownException) {
        this.logger
            .atInfo()
            .addKeyValue("numDocumentsIndexed", this.docsIndexed)
            .addKeyValue("numDocumentsInQueue", this.docsQueuedForIndexing)
            .log(
                "Collection scan ended with exception. "
                    + "Documents in scheduling queue to be cancelled.");
        FutureUtils.getAndSwallow(
            cancelProcessing(e),
            error -> this.logger.error("Failure during indexing cancellation", error));
      }

      throw e;
    }
  }

  /**
   * Inform this scanner to shut down if it's being run on a different thread.
   *
   * <p>Note that signalShutdown() does not guarantee that this scanner will have stopped when it
   * returns.
   */
  void signalShutdown() {
    this.logger.info("Signalling BufferlessCollectionScanner shutdown.");
    this.shutdown = true;
  }

  /**
   * Issues a new collection scan cursor with the change stream high watermark as the
   * readConcern.afterClusterTime, ensuring the scan represents a view of the collection from an
   * opTime after the last event applied. Note that min is an inclusive lower bound and may cause
   * the doc with the lastScannedToken to be scanned twice.
   */
  private CollectionScanMongoClient<InitialSyncException> getClient() throws InitialSyncException {
    return CollectionScanCursorFactory.open(
        this.context,
        this.mongoClient,
        builder -> {
          if (this.context.useNaturalOrderScan()) {
            builder
                .hint(CollectionScanCursorFactory.NATURAL_ASC) // $natural in ascending order,
                .requestResumeToken(BsonBoolean.TRUE); // Always request for resume token
            if (!this.lastScannedToken.asDocument().isEmpty()) {
              // Start at last post batch resume token, or later record id if given record was
              // deleted.
              builder.startAt(this.lastScannedToken.asDocument());
            }
          } else {
            builder
                .sort(Sorts.ascending(ID_KEY)) // Scan by _id to provide a consistent scan ordering,
                .hint(Indexes.ascending(ID_KEY)) // Ensure we use the _id index for the scan,
                .lastScannedId(this.lastScannedToken); // Resume from last id scanned,
          }
        });
  }

  /**
   * Buffers the next batch, then waits for the indexingFuture to complete, schedules the batch, and
   * returns the batch's indexing future. If we fail to buffer the next batch, we wait until the
   * indexing future completes to throw the exception. Also updates the lastScannedToken.
   */
  private CompletableFuture<Void> scheduleNextWhenCompletes(
      CompletableFuture<Void> indexingFuture,
      CollectionScanMongoClient<InitialSyncException> mongoClient)
      throws InitialSyncException {
    List<DocumentEvent> batch;
    try {
      batch = bufferNextBatch(mongoClient);
    } catch (Exception ex) {
      // Wait for the last indexing to complete before rethrowing.
      InitialSyncException.getOrWrapThrowable(
          indexingFuture, InitialSyncException.Phase.COLLECTION_SCAN);
      throw ex;
    }

    // Wait for indexingFuture to complete, then schedule the next batch.
    return scheduleBatchWhenCompletes(indexingFuture, batch, mongoClient.getPostBatchResumeToken());
  }

  /** Gets the next batch. */
  protected List<DocumentEvent> bufferNextBatch(
      CollectionScanMongoClient<InitialSyncException> mongoClient) throws InitialSyncException {
    return this.batchPreprocessor.preprocess(mongoClient.getNext());
  }

  /**
   * Wait for indexingFuture to complete, then schedule the given batch and return it's indexing
   * future.
   */
  private CompletableFuture<Void> scheduleBatchWhenCompletes(
      CompletableFuture<Void> indexingFuture,
      List<DocumentEvent> batch,
      Optional<BsonDocument> postBatchResumeToken)
      throws InitialSyncException {
    // Wait for the previous indexing to complete.
    InitialSyncException.getOrWrapThrowable(
        indexingFuture, InitialSyncException.Phase.COLLECTION_SCAN);
    // Now that the previous indexing has finished, we can schedule this batch and update
    // lastScannedToken.
    return scheduleWork(batch, postBatchResumeToken)
        .thenRun(() -> CollectionScanBatchMetrics.record(this.context, batch));
  }

  private CompletableFuture<Void> scheduleWork(
      List<DocumentEvent> batch, Optional<BsonDocument> postBatchResumeToken) {
    // Empty batch is possible when we scan an empty collection.
    if (batch.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    // Update the lastScannedToken for a non-empty batch and schedule it for indexing.
    updateLastScannedToken(batch, postBatchResumeToken);

    this.docsIndexed = this.docsIndexed + this.docsQueuedForIndexing;
    this.docsQueuedForIndexing = batch.size();

    return this.context.schedule(
        batch, SchedulerQueue.Priority.INITIAL_SYNC_COLLECTION_SCAN, getCommitUserData(batch));
  }

  protected void updateLastScannedToken(
      List<DocumentEvent> batch, Optional<BsonDocument> postBatchResumeToken) {
    if (this.context.useNaturalOrderScan()) {
      // A null $recordId marks the end of the collection scan (mongod 8.2.1+), and mongod refuses
      // to resume from such a token with KeyNotFound. Keep the previous resumable position instead:
      // on resume this costs one batch of rescan rather than a full resync. This token also flows
      // into the change stream applier via the scan Result, so guarding it here keeps the whole
      // end-of-scan window resumable.
      BsonDocument token = postBatchResumeToken.get();
      if (!token.isNull("$recordId")) {
        this.lastScannedToken = token;
      }
      return;
    }
    this.lastScannedToken = batch.getLast().getDocumentId();
  }

  private IndexCommitUserData getCommitUserData(List<DocumentEvent> batch) {
    checkArg(!batch.isEmpty(), "empty collection scan batch is not expected");

    // Use the _id from the last document in the batch and create resume info with that id.
    return IndexCommitUserData.createInitialSyncResume(
        this.context.getIndexFormatVersion(),
        InitialSyncResumeInfo.create(
            this.context.useNaturalOrderScan(),
            this.context.getChangeStreamResumeOperationTime(),
            this.lastScannedToken,
            this.avoidNaturalOrderScanSyncSourceChangeResync
                ? Optional.of(this.mongoClient.getSyncSourceHost())
                : Optional.empty()));
  }

  private CompletableFuture<Void> cancelProcessing(Throwable reason) {
    return this.context.cancel(reason);
  }

  private void handleShutdown() throws InitialSyncException {
    this.logger
        .atInfo()
        .addKeyValue("numDocumentsIndexed", this.docsIndexed)
        .addKeyValue("numDocumentsInQueue", this.docsQueuedForIndexing)
        .log(
            "Shutting down the BufferlessCollectionScanner. "
                + "Documents in scheduling queue to be cancelled.");
    InitialSyncException shutdownException = InitialSyncException.createShutDown();
    // Cancel processing for this index, to remove any work in the scheduler's queue.
    CompletableFuture<Void> cancelFuture = cancelProcessing(shutdownException);
    // Wait for all work to be finished.
    InitialSyncException.getOrWrapThrowable(
        cancelFuture, InitialSyncException.Phase.COLLECTION_SCAN);
    throw shutdownException;
  }

  public static class Result {
    private final boolean continueSync;
    private final BsonValue lastScannedToken;

    private Result(boolean continueSync, BsonValue lastScannedToken) {
      this.continueSync = continueSync;
      this.lastScannedToken = lastScannedToken;
    }

    public boolean getContinueSync() {
      return this.continueSync;
    }

    public BsonValue getLastScannedToken() {
      return this.lastScannedToken;
    }
  }
}
