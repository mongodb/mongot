package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.mongot.util.Check.checkState;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.PartitionResumeInfo;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;

/**
 * Scans one partition of a collection during a parallel natural-order bufferless initial sync: a
 * single {@code $natural} cursor bounded to that partition's {@code $recordId} range, run for at
 * most one scan window.
 *
 * <p>K of these run concurrently against one index, so a scanner touches only its own {@link
 * ParallelCollectionScanPartitionState}. It schedules batches without commit data because the
 * session installs one supplier covering every partition; a per-batch blob here would publish this
 * partition's progress while reverting its siblings'.
 *
 * <p>Stopping belongs to the cycle that owns the K workers: it supplies the deadline and the abort
 * flag, and a scanner that stops for either returns normally with the reason.
 */
class ParallelCollectionScanner {

  private static final String RECORD_ID_FIELD = "$recordId";
  private static final String INITIAL_SYNC_ID_FIELD = "$initialSyncId";

  /** Why a scanner stopped, which is what tells the cycle whether the partition still has work. */
  enum StopReason {
    /** The partition consumed its entire range and will not be scanned again. */
    RANGE_CONSUMED,
    /** The scan window ran out; the partition resumes in a later window. */
    WINDOW_DEADLINE,
    /** The cycle asked every worker to stop, e.g. on shutdown or a sibling's failure. */
    ABORTED
  }

  record Result(StopReason stopReason, long documentsIndexed) {}

  /**
   * A batch handed to the index and the {@code $recordId} the cursor reached scanning it, which
   * becomes a durable resume point only once the future completes. The position is absent when the
   * batch exhausted the cursor, leaving nothing to resume from.
   */
  private record ScheduledBatch(
      CompletableFuture<Void> indexingFuture, Optional<Long> endRecordId) {}

  private final DefaultKeyValueLogger logger;
  private final Clock clock;
  private final InitialSyncContext context;
  private final InitialSyncMongoClient mongoClient;
  private final ParallelCollectionScanPartitionState partitionState;
  private final UUID initialSyncId;
  private final BooleanSupplier abortRequested;
  private final CollectionScanBatchPreprocessor batchPreprocessor;

  /**
   * Whether this cursor's incarnation still has to be confirmed against the plan, which is so only
   * when it opens with no {@code $_startAt} for the server to confirm.
   */
  private boolean incarnationUnconfirmed;

  /** Counts only what has landed, so a failure reports how far the partition actually got. */
  private long documentsIndexed = 0;

  private long documentsInFlight = 0;

  ParallelCollectionScanner(
      Clock clock,
      InitialSyncContext context,
      InitialSyncMongoClient mongoClient,
      ParallelCollectionScanPartitionState partitionState,
      UUID initialSyncId,
      BooleanSupplier abortRequested,
      MetricsFactory metricsFactory) {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", context.getIndexId());
    defaultKeyValues.put("generationId", context.getGenerationId());
    defaultKeyValues.put("partitionId", partitionState.getPartitionId());
    this.logger =
        DefaultKeyValueLogger.getLogger(ParallelCollectionScanner.class, defaultKeyValues);

    this.clock = clock;
    this.context = context;
    this.mongoClient = mongoClient;
    this.partitionState = partitionState;
    this.initialSyncId = initialSyncId;
    this.abortRequested = abortRequested;
    this.incarnationUnconfirmed = getStartAt().isEmpty();
    this.batchPreprocessor = new CollectionScanBatchPreprocessor(context, metricsFactory);
  }

  /**
   * Scans this partition's remaining range until it is consumed, the deadline passes, or the cycle
   * aborts, indexing every document scanned.
   *
   * <p>The budget is an instant rather than a duration because the cycle owns it: workers start
   * staggered, so a per-worker duration would stretch the window to whichever started last.
   */
  Result scanUntil(Instant deadline) throws InitialSyncException {

    checkState(
        !this.partitionState.isDone(), "scanning a partition that already consumed its range");

    this.logger.info("Starting a partition scan.");

    @Var Optional<Long> lastScannedRecordId = Optional.empty();
    @Var CompletableFuture<Void> indexingFuture = CompletableFuture.completedFuture(null);
    StopReason stopReason;

    try (CollectionScanMongoClient<InitialSyncException> cursor = openCursor()) {
      while (true) {
        // Stopping is checked ahead of hasNext because that call resolves the namespace against the
        // source: a scanner that is already stopping should not spend a round trip to find out. A
        // cursor exhausted at that same moment reports ABORTED instead of RANGE_CONSUMED, leaving
        // the partition unfinished, and the next window reopens it to consume nothing.
        if (this.abortRequested.getAsBoolean()) {
          stopReason = StopReason.ABORTED;
          break;
        }
        if (!Instant.now(this.clock).isBefore(deadline)) {
          stopReason = StopReason.WINDOW_DEADLINE;
          break;
        }
        if (!cursor.hasNext()) {
          stopReason = StopReason.RANGE_CONSUMED;
          break;
        }

        ScheduledBatch scheduled =
            scheduleNextWhenCompletes(indexingFuture, cursor, lastScannedRecordId);
        indexingFuture = scheduled.indexingFuture();
        lastScannedRecordId = scheduled.endRecordId();

        // No position means the batch exhausted the cursor, which consumes the range as surely as
        // reaching its upper bound does.
        if (lastScannedRecordId.map(this::isAtOrPastUpperBound).orElse(true)) {
          stopReason = StopReason.RANGE_CONSUMED;
          break;
        }
      }

      // Wait for the last indexing to complete
      awaitIndexing(indexingFuture);
      onBatchLanded(lastScannedRecordId);

      this.logger
          .atInfo()
          .addKeyValue("stopReason", stopReason)
          .addKeyValue("numDocumentsIndexed", this.documentsIndexed)
          .addKeyValue("resumeRecordId", this.partitionState.getResumeRecordId())
          .log("Finished a partition scan phase.");

      return new Result(stopReason, this.documentsIndexed);
    } catch (Exception e) {
      // Cancelling queued work is left to the cycle, which cancels the whole generation at once: a
      // failing worker doing it would tear down its siblings.
      this.logger
          .atWarn()
          .addKeyValue("numDocumentsIndexed", this.documentsIndexed)
          .addKeyValue("numDocumentsInFlight", this.documentsInFlight)
          .setCause(e)
          .log("Partition scan ended with exception.");
      throw e;
    }
  }

  /**
   * Records the previously scheduled batch reaching the index: its position becomes durable and its
   * documents count as indexed.
   */
  private void onBatchLanded(Optional<Long> recordId) {
    recordId.ifPresent(this.partitionState::advanceResumeRecordId);
    this.documentsIndexed += this.documentsInFlight;
    this.documentsInFlight = 0;
  }

  /** Opens this partition's cursor, bounded below by where the partition left off. */
  private CollectionScanMongoClient<InitialSyncException> openCursor() throws InitialSyncException {
    return CollectionScanCursorFactory.open(
        this.context,
        this.mongoClient,
        builder -> {
          builder
              .hint(CollectionScanCursorFactory.NATURAL_ASC)
              .requestResumeToken(BsonBoolean.TRUE);
          getStartAt().ifPresent(builder::startAt);
        });
  }

  /**
   * Builds the {@code $_startAt} token: where this partition resumed from, or its lower bound, or
   * nothing for the first partition on its first window.
   */
  private Optional<BsonDocument> getStartAt() {
    return this.partitionState
        .getResumeRecordId()
        .or(this.partitionState::getLowerRecordId)
        .map(
            recordId ->
                new BsonDocument()
                    .append(RECORD_ID_FIELD, new BsonInt64(recordId))
                    .append(INITIAL_SYNC_ID_FIELD, new BsonBinary(this.initialSyncId)));
  }

  /**
   * Fetches the next batch and where it left the cursor, then schedules it once the previous batch
   * has landed.
   */
  private ScheduledBatch scheduleNextWhenCompletes(
      CompletableFuture<Void> indexingFuture,
      CollectionScanMongoClient<InitialSyncException> cursor,
      Optional<Long> landedRecordId)
      throws InitialSyncException {
    List<DocumentEvent> batch;
    Optional<Long> endRecordId;
    try {
      batch = this.batchPreprocessor.preprocess(cursor.getNext());
      endRecordId = recordIdAfterBatch(cursor);
    } catch (Exception e) {
      awaitIndexing(indexingFuture);
      throw e;
    }

    return new ScheduledBatch(
        scheduleBatchWhenCompletes(indexingFuture, batch, landedRecordId), endRecordId);
  }

  private CompletableFuture<Void> scheduleBatchWhenCompletes(
      CompletableFuture<Void> indexingFuture,
      List<DocumentEvent> batch,
      Optional<Long> landedRecordId)
      throws InitialSyncException {
    awaitIndexing(indexingFuture);
    onBatchLanded(landedRecordId);

    return scheduleBatch(batch);
  }

  /**
   * Returns the {@code $recordId} the cursor has scanned up to, taken from the post-batch resume
   * token, or empty once the cursor is exhausted. This is the partition's position rather than the
   * last document's: it advances over skipped documents too, so a partition whose remaining range
   * is all deletes still progresses.
   *
   * <p>An exhausted cursor reports a null recordId on mongod 8.1 and later, which resets its scan
   * position at end-of-scan. Nothing in the range is left to resume from, so there is no position
   * to report.
   */
  private Optional<Long> recordIdAfterBatch(CollectionScanMongoClient<InitialSyncException> cursor)
      throws InitialSyncException {
    BsonDocument resumeToken =
        cursor
            .getPostBatchResumeToken()
            .orElseThrow(
                () ->
                    InitialSyncException.createFailed(
                        new IllegalStateException(
                            "natural order collection scan returned no postBatchResumeToken")));

    // Only a cursor that opened without a token needs this, and only once: the client checks every
    // later token against the one before it, so anchoring the first anchors the whole cursor.
    if (this.incarnationUnconfirmed) {
      verifyPlannedIncarnation(resumeToken);
      this.incarnationUnconfirmed = false;
    }

    if (resumeToken.isNull(RECORD_ID_FIELD)) {
      return Optional.empty();
    }

    if (!resumeToken.isNumber(RECORD_ID_FIELD)) {
      // Partitioning is only planned for numeric recordIds, so anything else means the plan is
      // being applied to a collection it was not built for.
      throw InitialSyncException.createFailed(
          new IllegalStateException(
              String.format(
                  "expected a numeric %s in postBatchResumeToken, got %s",
                  RECORD_ID_FIELD, resumeToken.get(RECORD_ID_FIELD))));
    }
    return Optional.of(resumeToken.getNumber(RECORD_ID_FIELD).longValue());
  }

  /**
   * Fails the sync if the cursor reads a different incarnation of the collection than the partition
   * boundaries were computed against.
   */
  private void verifyPlannedIncarnation(BsonDocument resumeToken) throws InitialSyncException {
    BsonBinary plannedId = new BsonBinary(this.initialSyncId);
    if (plannedId.equals(resumeToken.get(INITIAL_SYNC_ID_FIELD))) {
      return;
    }

    throw InitialSyncException.createInitialSyncIdMismatch(
        String.format(
            "collection scan is reading %s %s, but the partitions were planned against %s",
            INITIAL_SYNC_ID_FIELD, resumeToken.get(INITIAL_SYNC_ID_FIELD), this.initialSyncId));
  }

  private CompletableFuture<Void> scheduleBatch(List<DocumentEvent> batch) {
    if (batch.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    this.documentsInFlight = batch.size();

    return this.context
        .scheduleWithoutCommitUserData(batch, SchedulerQueue.Priority.INITIAL_SYNC_COLLECTION_SCAN)
        .thenRun(() -> CollectionScanBatchMetrics.record(this.context, batch));
  }

  private void awaitIndexing(CompletableFuture<Void> indexingFuture) throws InitialSyncException {
    InitialSyncException.getOrWrapThrowable(
        indexingFuture, InitialSyncException.Phase.COLLECTION_SCAN);
  }

  /**
   * Returns whether the cursor reached this partition's upper bound, which {@link
   * PartitionResumeInfo} defines as inclusive. The last partition has no bound and runs to
   * end-of-cursor.
   */
  private boolean isAtOrPastUpperBound(long recordId) {
    return this.partitionState.getUpperRecordId().filter(upper -> recordId >= upper).isPresent();
  }
}
