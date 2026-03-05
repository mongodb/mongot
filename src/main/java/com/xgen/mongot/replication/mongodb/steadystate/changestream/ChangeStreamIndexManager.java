package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.bsonDocumentToChangeStreamDocument;
import static com.xgen.mongot.util.Check.checkState;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.xgen.mongot.index.DocsExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.ChangeStreamEventCheckException;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.DocumentEventBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FutureUtils;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;

/** One instance per generation id. */
abstract class ChangeStreamIndexManager {

  /**
   * Encapsulates the results of an indexBatch call. Holds a future representing when the indexing
   * finishes. Also holds whether or not the batch had a lifecycle event.
   */
  static final class BatchInfo {
    final CompletableFuture<Void> indexingFuture;
    final Optional<SteadyStateException> lifecycleEvent;

    BatchInfo(
        CompletableFuture<Void> indexingFuture, Optional<SteadyStateException> lifecycleEvent) {

      this.indexingFuture = indexingFuture;
      this.lifecycleEvent = lifecycleEvent;
    }

    BatchInfo(CompletableFuture<Void> indexingFuture) {
      this(indexingFuture, Optional.empty());
    }
  }

  protected final DefaultKeyValueLogger logger;
  protected final IndexDefinition indexDefinition;
  protected final IndexingWorkScheduler indexingWorkScheduler;
  protected final DocumentIndexer documentIndexer;
  protected final GenerationId generationId;
  protected final ObjectId attemptId;
  protected final MongoNamespace namespace;
  protected final IndexMetricsUpdater indexMetricsUpdater;
  private final Consumer<ChangeStreamResumeInfo> resumeInfoUpdater;

  // use these to limit how often we write the optime to the log
  private volatile Instant optimeLastUpdate;
  private static final Duration OPTIME_LOG_PERIOD = Duration.ofMinutes(1);

  private static final Set<OperationType> INVALIDATING_OPERATIONS =
      Set.of(
          OperationType.RENAME,
          OperationType.DROP,
          OperationType.DROP_DATABASE,
          OperationType.INVALIDATE);

  protected Optional<MongoNamespace> renameNamespace;

  /**
   * lifecycleFuture is the future that should be completed when an exception occurs while indexing
   * or shutdown() is invoked.
   *
   * <p>lifecycleFuture thus can be used by higher-level components to monitor the health of the
   * ChangeStreamIndexManager and react to events.
   */
  protected final CompletableFuture<Void> lifecycleFuture;

  /** indexingFuture is a future that represents asynchronous work being done to index a batch. */
  @GuardedBy("this")
  protected CompletableFuture<Void> latestIndexingFuture;

  @GuardedBy("this")
  private boolean shutdown;

  protected ChangeStreamIndexManager(
      IndexDefinition indexDefinition,
      DefaultKeyValueLogger logger,
      IndexingWorkScheduler indexingWorkScheduler,
      DocumentIndexer documentIndexer,
      MongoNamespace namespace,
      Consumer<ChangeStreamResumeInfo> resumeInfoUpdater,
      IndexMetricsUpdater indexMetricsUpdater,
      CompletableFuture<Void> lifecycleFuture,
      GenerationId generationId) {

    this.logger = logger;
    this.indexDefinition = indexDefinition;
    this.indexingWorkScheduler = indexingWorkScheduler;
    this.documentIndexer = documentIndexer;
    this.namespace = namespace;
    this.resumeInfoUpdater = resumeInfoUpdater;
    this.indexMetricsUpdater = indexMetricsUpdater;
    this.lifecycleFuture = lifecycleFuture;
    this.generationId = generationId;

    this.latestIndexingFuture = FutureUtils.COMPLETED_FUTURE;
    this.shutdown = false;
    this.optimeLastUpdate = Instant.MIN;
    this.renameNamespace = Optional.empty();
    this.attemptId = new ObjectId();
  }

  /**
   * Gracefully shuts down the change stream indexing.
   *
   * @return a future that completes when the scheduled work for the index has completed. The future
   *     will only ever complete successfully.
   */
  public synchronized CompletableFuture<Void> shutdown() {
    // Flipping this flag should prevent future work from being scheduled.
    this.shutdown = true;

    // Complete the external future with a SHUT_DOWN SteadyStateException when the current indexing
    // task completes no matter how it completes.
    Crash.because("failed indexing batch")
        .ifCompletesExceptionally(
            FutureUtils.swallowedFuture(this.latestIndexingFuture)
                .handle(
                    (result, throwable) -> {
                      if (!this.lifecycleFuture.isDone()) {
                        failLifecycle(SteadyStateException.createShutDown());
                      }
                      return null;
                    }));

    // Return a future that returns when the external future is completed, but doesn't complete
    // exceptionally.
    return FutureUtils.swallowedFuture(this.lifecycleFuture);
  }

  public synchronized boolean isShutdown() {
    return this.shutdown;
  }

  abstract BatchInfo indexBatch(
      ChangeStreamBatch batch,
      DocumentMetricsUpdater metricsUpdater,
      Timer preprocessingBatchTimer);

  protected CompletableFuture<Void> postIndexingPipeline(
      CompletableFuture<Void> schedulingFuture,
      CompletableFuture<Void> indexingFuture,
      Optional<ChangeStreamResumeInfo> resumeInfo,
      Optional<SteadyStateException> lifecycleEvent,
      BsonTimestamp commandOperationTime) {
    return schedulingFuture
        .thenRun(
            () -> {
              resumeInfo.ifPresent(this.resumeInfoUpdater);
              if (resumeInfo.isPresent()) {
                try {
                  BsonTimestamp opTime =
                      ResumeTokenUtils.opTimeFromResumeToken(resumeInfo.get().getResumeToken());
                  this.indexMetricsUpdater
                      .getIndexingMetricsUpdater()
                      .getReplicationOpTimeInfo()
                      .update(opTime.getValue(), commandOperationTime.getValue());

                  if (Instant.now().isAfter(this.optimeLastUpdate.plus(OPTIME_LOG_PERIOD))) {
                    this.optimeLastUpdate = Instant.now();
                    this.logger.debug("Processed change stream batch with optime={}", opTime);
                  }
                } catch (DecoderException ex) {
                  this.logger.warn("Error reading optime from resume token.", ex);
                }
              }

              // When a lifecycle event is found in the batch, it will not be re-enqueued by the
              // ChangeStreamManager, so we don't need to cancel here.
              lifecycleEvent.ifPresent(this.lifecycleFuture::completeExceptionally);

              // Indexing completed successfully, even if there was a lifecycleEvent
              indexingFuture.complete(null);
            })
        .exceptionally(
            throwable -> {
              // Complete the indexing future with the failure. Do not complete the
              // lifecycleFuture. The ChangeStreamManager will see the indexing
              // failure and handle it.
              Throwable failure = unwrapIndexingException(throwable);

              indexingFuture.completeExceptionally(failure);

              return null;
            });
  }

  protected void recordPerBatchMetrics(DocumentEventBatch documentEventBatch) {
    if (documentEventBatch.applicableDocumentsTotal > 0) {
      var steadyStateMetrics =
          this.indexMetricsUpdater.getReplicationMetricsUpdater().getSteadyStateMetrics();

      steadyStateMetrics
          .getBatchTotalApplicableDocuments()
          .record(documentEventBatch.applicableDocumentsTotal);

      steadyStateMetrics
          .getBatchTotalApplicableBytes()
          .record(documentEventBatch.applicableDocumentsTotalBytes);
    }
  }

  /**
   * Cancels any work that is queued for this index and schedules completion of the lifecycle future
   * after the in-flight batches are processed.
   */
  abstract void failLifecycle(Throwable reason);

  protected Optional<ChangeStreamResumeInfo> getResumeInfo(ChangeStreamBatch batch) {
    int numEvents = batch.getRawEvents().size();
    if (numEvents == 0) {
      return Optional.of(
          ChangeStreamResumeInfo.create(this.namespace, batch.getPostBatchResumeToken()));
    }

    // In general, we want to resume the changestream after the current batch. However, there are
    // two cases involving invalidating events. Invalidating events involve an event which causes
    // invalidation (i.e. DROP, DROP_DATABASE,or RENAME), as well as the actual INVALIDATE event.

    // First, it is possible for the DROP/DROP_DATABASE/RENAME event to be in a different batch than
    // the INVALIDATE event if the batch is already full.

    // If we were to commit the resumeToken associated with the DROP/DROP_DATABASE/RENAME event, we
    // would resume the change stream and immediately see an INVALIDATE event, without knowing if it
    // was due to a drop vs a rename.

    // Secondly, it is possible for mongot to crash after a RENAME event, but before the new
    // changestream has caused any index commits to occur. In this case, we want the index to see
    // the rename event after mongot restarts, so we want to commit resume info from before the
    // RENAME event.

    // To avoid these cases, we find the last non-invalidating (i.e. not DROP, DROP_DATABASE,
    // RENAME, or INVALIDATE) event, and use that as the resume info. In most cases, this resume
    // info will not be used again.

    OperationType lastEventType =
        bsonDocumentToChangeStreamDocument(batch.getRawEvents().get(numEvents - 1))
            .getOperationType();
    boolean lastEventInvalidates = INVALIDATING_OPERATIONS.contains(lastEventType);

    if (!lastEventInvalidates) {
      return Optional.of(
          ChangeStreamResumeInfo.create(this.namespace, batch.getPostBatchResumeToken()));
    }

    // Return resume info of last non-invalidating operation
    for (RawBsonDocument rawEvent : batch.getRawEvents().reversed()) {
      ChangeStreamDocument<RawBsonDocument> changeStreamDoc =
          bsonDocumentToChangeStreamDocument(rawEvent);
      if (!INVALIDATING_OPERATIONS.contains(changeStreamDoc.getOperationType())) {
        return Optional.of(
            ChangeStreamResumeInfo.create(this.namespace, changeStreamDoc.getResumeToken()));
      }
    }

    // All events are invalidating; do not change resume info
    return Optional.empty();
  }

  /** Handles the event after a RENAME by returning the lifeCycleException. */
  protected SteadyStateException handleRenameEvent(ChangeStreamDocument<RawBsonDocument> event) {
    checkState(this.renameNamespace.isPresent(), "renameNamespace must be present");

    // We expect that the event following a rename is an INVALIDATE event.
    OperationType eventOperationType = Objects.requireNonNull(event.getOperationType());
    if (!eventOperationType.equals(OperationType.INVALIDATE)) {
      return SteadyStateException.createRequiresResync(
          String.format(
              "witnessed event of type %s after RENAME, expected INVALIDATE", eventOperationType));
    }

    // Get the resumeToken from the INVALIDATE event so it can be used in startAfter.
    return SteadyStateException.createRenamed(
        ChangeStreamResumeInfo.create(this.renameNamespace.get(), event.getResumeToken()));
  }

  protected void logChangeStreamEventDetails(ChangeStreamDocument<RawBsonDocument> event) {
    this.logger.info(
        "Received a {} change-stream event for index."
            + " Details: [clusterTime:{}, resumeToken:{}, namespace:{}, "
            + "destinationNamespace:{}, database:{}, documentKey:{}]",
        event.getOperationType(),
        event.getClusterTime(),
        event.getResumeToken(),
        event.getNamespace(),
        event.getDestinationNamespaceDocument(),
        event.getDatabaseName(),
        event.getDocumentKey());
  }

  public GenerationId getGenerationId() {
    return this.generationId;
  }

  private static Throwable unwrapIndexingException(Throwable throwable) {
    Throwable unwrapped =
        throwable instanceof CompletionException && throwable.getCause() != null
            ? throwable.getCause()
            : throwable;

    if (unwrapped instanceof FieldExceededLimitsException f) {
      return SteadyStateException.createFieldExceeded(f.getMessage());
    }

    if (unwrapped instanceof ChangeStreamEventCheckException && unwrapped.getCause() != null) {
      return unwrapped.getCause();
    }

    if (unwrapped instanceof DocsExceededLimitsException) {
      return SteadyStateException.createDocsExceeded(unwrapped);
    }

    return unwrapped;
  }

  @FunctionalInterface
  public interface DocumentMetricsUpdater {

    void accept(
        int updatesWitnessed, int updatesApplicable, int skippedDocumentsWithoutMetadataNamespace);
  }
}
