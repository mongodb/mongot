package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.testing.mongot.mock.index.SearchIndex.IGNORE_METRICS;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer.mockDocumentIndexer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoCommandException;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.mongodb.common.AggregateCommandCollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.mongodb.Errors;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class ParallelCollectionScannerTest {

  private static final BsonTimestamp HIGH_WATER_MARK = new BsonTimestamp(10, 1);

  /** Only shapes the context: the scanner takes the partition it scans directly. */
  private static final int PARTITION_COUNT = 4;

  private static final UUID INITIAL_SYNC_ID = UUID.randomUUID();
  private static final String AFTER_CLUSTER_TIME_FIELD = "afterClusterTime";

  /** No test here should stop on the deadline unless it asks to. */
  private static final Instant NO_DEADLINE = Instant.MAX;

  /** Advances a second per reading, so deadlines are crossed by iteration, not wall-clock wait. */
  private static final class TickingClock extends Clock {
    private int ticks = 0;

    @Override
    public Instant instant() {
      return Instant.EPOCH.plusSeconds(this.ticks++);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }
  }

  private static ParallelCollectionScanPartitionState partition(
      Optional<Long> lowerRecordId, Optional<Long> upperRecordId, Optional<Long> resumeRecordId) {
    return new ParallelCollectionScanPartitionState(
        0, lowerRecordId, upperRecordId, resumeRecordId, false);
  }

  private static ParallelCollectionScanPartitionState unboundedPartition() {
    return partition(Optional.empty(), Optional.empty(), Optional.empty());
  }

  private static List<RawBsonDocument> documents(int... ids) {
    return Arrays.stream(ids)
        .mapToObj(id -> BsonUtils.documentToRaw(new BsonDocument("_id", new BsonInt32(id))))
        .toList();
  }

  /** A token carrying a scan position, which mongod 8.0 reports even at end-of-scan. */
  private static BsonDocument resumeToken(long recordId) {
    return new BsonDocument()
        .append("$recordId", new BsonInt64(recordId))
        .append("$initialSyncId", new BsonBinary(INITIAL_SYNC_ID));
  }

  /** mongod 8.1 and later reset the scan position at end-of-scan, nulling out the recordId. */
  private static BsonDocument exhaustedResumeToken() {
    return new BsonDocument()
        .append("$recordId", BsonNull.VALUE)
        .append("$initialSyncId", new BsonBinary(INITIAL_SYNC_ID));
  }

  /**
   * A range can hold no documents. Nothing is scheduled or published, but the partition is still
   * consumed so the cycle stops scanning it.
   */
  @Test
  public void scanCompletesARangeThatHoldsNoDocuments() throws Exception {
    Fixture fixture =
        Fixture.withNoBatches(partition(Optional.of(100L), Optional.of(900L), Optional.empty()));

    ParallelCollectionScanner.Result result = fixture.scanner.scanUntil(NO_DEADLINE);

    Assert.assertEquals(ParallelCollectionScanner.StopReason.RANGE_CONSUMED, result.stopReason());
    Assert.assertEquals(0, result.documentsIndexed());
    Assert.assertEquals(Optional.empty(), fixture.partition.getResumeRecordId());
    verify(fixture.cursor).getNext();
    // An empty batch is never handed to the scheduler: it would cost a queue slot to index nothing.
    verify(fixture.indexingWorkScheduler, never())
        .schedule(any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void scanIndexesEveryBatchAndReportsTheRangeConsumed() throws Exception {
    Fixture fixture =
        Fixture.withBatches(
            unboundedPartition(), new Batch(documents(1, 2), 2), new Batch(documents(3), 3));

    ParallelCollectionScanner.Result result = fixture.scanner.scanUntil(NO_DEADLINE);

    Assert.assertEquals(ParallelCollectionScanner.StopReason.RANGE_CONSUMED, result.stopReason());
    Assert.assertEquals(3, result.documentsIndexed());
    verify(fixture.documentIndexer, times(3)).indexDocumentEvent(any());
    Assert.assertEquals(Optional.of(3L), fixture.partition.getResumeRecordId());
  }

  /** The documents a batch carries and the {@code $recordId} the cursor reaches by its end. */
  private record Batch(List<RawBsonDocument> documents, long endRecordId) {}

  private static final class Fixture {

    private final InitialSyncMongoClient mongoClient;
    private final CollectionScanMongoClient<InitialSyncException> cursor;
    private final IndexingWorkScheduler indexingWorkScheduler;
    private final DocumentIndexer documentIndexer;
    private final InitialSyncContext context;
    private final ParallelCollectionScanPartitionState partition;
    private final AtomicBoolean abortRequested = new AtomicBoolean(false);
    private final ParallelCollectionScanner scanner;

    private Fixture(ParallelCollectionScanPartitionState partition, Clock clock) throws Exception {
      this.partition = partition;
      this.cursor = mock(AggregateCommandCollectionScanMongoClient.class);
      this.mongoClient = mock(InitialSyncMongoClient.class);
      when(this.mongoClient.getCollectionAggregateCommandMongoClient(any(), any(), any(), any()))
          .thenReturn(this.cursor);

      this.documentIndexer = mockDocumentIndexer();
      this.indexingWorkScheduler = mock(IndexingWorkScheduler.class);

      when(this.indexingWorkScheduler.schedule(any(), any(), any(), any(), any(), any(), any()))
          .thenAnswer(
              invocation -> {
                List<DocumentEvent> batch = invocation.getArgument(0);
                for (DocumentEvent event : batch) {
                  this.documentIndexer.indexDocumentEvent(event);
                }
                Optional<IndexCommitUserData> commitUserData = invocation.getArgument(5);
                commitUserData.ifPresent(this.documentIndexer::updateCommitUserData);
                return CompletableFuture.completedFuture(null);
              });
      when(this.indexingWorkScheduler.cancel(any(), any(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));

      this.context =
          InitialSyncContext.create(
                  MOCK_INDEX_DEFINITION_GENERATION,
                  this.indexingWorkScheduler,
                  this.documentIndexer,
                  IGNORE_METRICS,
                  Optional.empty(),
                  false,
                  true,
                  true,
                  PARTITION_COUNT,
                  ToggleGate.opened())
              .withProgress(HIGH_WATER_MARK);

      this.scanner =
          new ParallelCollectionScanner(
              clock,
              this.context,
              this.mongoClient,
              this.partition,
              INITIAL_SYNC_ID,
              this.abortRequested::get,
              new MetricsFactory("test", new SimpleMeterRegistry()));
    }

    /** A partition whose cursor yields one batch and is then exhausted. */
    static Fixture withBatch(ParallelCollectionScanPartitionState partition, Batch batch)
        throws Exception {
      Fixture fixture = new Fixture(partition, Clock.systemUTC());

      when(fixture.cursor.hasNext()).thenReturn(true).thenReturn(false);
      when(fixture.cursor.getNext()).thenReturn(batch.documents());
      when(fixture.cursor.getPostBatchResumeToken())
          .thenReturn(Optional.of(resumeToken(batch.endRecordId())));

      return fixture;
    }

    /** A partition whose cursor yields two batches and is then exhausted. */
    static Fixture withBatches(
        ParallelCollectionScanPartitionState partition, Batch first, Batch second)
        throws Exception {
      Fixture fixture = new Fixture(partition, Clock.systemUTC());

      when(fixture.cursor.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
      when(fixture.cursor.getNext()).thenReturn(first.documents()).thenReturn(second.documents());
      when(fixture.cursor.getPostBatchResumeToken())
          .thenReturn(Optional.of(resumeToken(first.endRecordId())))
          .thenReturn(Optional.of(resumeToken(second.endRecordId())));

      return fixture;
    }

    /**
     * A partition whose range holds no documents. The cursor still reports a batch to fetch,
     * because the client only sends the aggregate on the first {@code getNext}; the empty range
     * shows up as that batch coming back empty and exhausted.
     */
    static Fixture withNoBatches(ParallelCollectionScanPartitionState partition) throws Exception {
      Fixture fixture = new Fixture(partition, Clock.systemUTC());

      when(fixture.cursor.hasNext()).thenReturn(true).thenReturn(false);
      when(fixture.cursor.getNext()).thenReturn(List.of());
      when(fixture.cursor.getPostBatchResumeToken())
          .thenReturn(Optional.of(exhaustedResumeToken()));

      return fixture;
    }

    /** A partition whose cursor never runs out, for deadline and abort tests. */
    static Fixture withEndlessBatches(ParallelCollectionScanPartitionState partition, Clock clock)
        throws Exception {
      Fixture fixture = new Fixture(partition, clock);

      when(fixture.cursor.hasNext()).thenReturn(true);
      when(fixture.cursor.getNext()).thenReturn(documents(1));
      when(fixture.cursor.getPostBatchResumeToken()).thenReturn(Optional.of(resumeToken(1)));

      return fixture;
    }
  }

  @Test
  public void scanReadsAtTheRunsHighWaterMark() throws Exception {
    Fixture fixture = Fixture.withBatch(unboundedPartition(), new Batch(documents(1), 1));

    fixture.scanner.scanUntil(NO_DEADLINE);

    verify(fixture.mongoClient)
        .getCollectionAggregateCommandMongoClient(
            argThat(
                command ->
                    command
                        .getReadConcernDocument()
                        .filter(
                            readConcern ->
                                HIGH_WATER_MARK.equals(
                                    readConcern.getTimestamp(AFTER_CLUSTER_TIME_FIELD)))
                        .isPresent()),
            any(),
            any(),
            any());
  }

  @Test
  public void scanSchedulesBatchesWithoutCommitUserData() throws Exception {
    Fixture fixture = Fixture.withBatch(unboundedPartition(), new Batch(documents(1), 1));

    fixture.scanner.scanUntil(NO_DEADLINE);

    verify(fixture.indexingWorkScheduler)
        .schedule(any(), any(), any(), any(), any(), eq(Optional.empty()), any());
    verify(fixture.documentIndexer, never()).updateCommitUserData(any());
  }

  @Test
  public void scanStartsAtTheResumePositionWhenThePartitionHasProgress() throws Exception {
    Fixture fixture =
        Fixture.withBatch(
            partition(Optional.of(100L), Optional.of(900L), Optional.of(500L)),
            new Batch(documents(1), 501));

    fixture.scanner.scanUntil(NO_DEADLINE);

    verifyStartsAt(fixture, Optional.of(500L));
  }

  @Test
  public void scanStartsAtTheLowerBoundBeforeThePartitionHasProgress() throws Exception {
    Fixture fixture =
        Fixture.withBatch(
            partition(Optional.of(100L), Optional.of(900L), Optional.empty()),
            new Batch(documents(1), 101));

    fixture.scanner.scanUntil(NO_DEADLINE);

    verifyStartsAt(fixture, Optional.of(100L));
  }

  @Test
  public void scanOmitsStartAtForTheFirstPartitionsFirstWindow() throws Exception {
    Fixture fixture =
        Fixture.withBatch(
            partition(Optional.empty(), Optional.of(900L), Optional.empty()),
            new Batch(documents(1), 1));

    fixture.scanner.scanUntil(NO_DEADLINE);

    verifyStartsAt(fixture, Optional.empty());
  }

  private void verifyStartsAt(Fixture fixture, Optional<Long> expectedRecordId) throws Exception {
    Optional<BsonDocument> expectedStartAt =
        expectedRecordId.map(ParallelCollectionScannerTest::resumeToken);

    verify(fixture.mongoClient)
        .getCollectionAggregateCommandMongoClient(
            argThat(command -> command.getStartAt().equals(expectedStartAt)), any(), any(), any());
  }

  /**
   * Checked per batch, so the crossing batch is indexed whole and overshoots into the neighbour.
   */
  @Test
  public void scanStopsOnceItReachesItsUpperBound() throws Exception {
    Fixture fixture =
        Fixture.withBatches(
            partition(Optional.empty(), Optional.of(2L), Optional.empty()),
            new Batch(documents(1, 2, 3), 3),
            new Batch(documents(4), 4));

    ParallelCollectionScanner.Result result = fixture.scanner.scanUntil(NO_DEADLINE);

    Assert.assertEquals(ParallelCollectionScanner.StopReason.RANGE_CONSUMED, result.stopReason());
    Assert.assertEquals(3, result.documentsIndexed());
    verify(fixture.indexingWorkScheduler, times(1))
        .schedule(any(), any(), any(), any(), any(), any(), any());
    Assert.assertEquals(Optional.of(3L), fixture.partition.getResumeRecordId());
  }

  @Test
  public void scanStopsAtTheWindowDeadlineWithTheRangeUnfinished() throws Exception {
    Fixture fixture = Fixture.withEndlessBatches(unboundedPartition(), new TickingClock());

    ParallelCollectionScanner.Result result =
        fixture.scanner.scanUntil(Instant.EPOCH.plusSeconds(1));

    Assert.assertEquals(ParallelCollectionScanner.StopReason.WINDOW_DEADLINE, result.stopReason());
    Assert.assertEquals(1, result.documentsIndexed());
    Assert.assertEquals(Optional.of(1L), fixture.partition.getResumeRecordId());
  }

  /**
   * An aborting worker returns instead of throwing, and leaves cancelling queued work to whoever
   * aborted the run, since it covers every partition at once.
   */
  @Test
  public void scanStopsWhenTheRunAbortsWithoutCancellingQueuedWork() throws Exception {
    Fixture fixture = Fixture.withEndlessBatches(unboundedPartition(), Clock.systemUTC());
    fixture.abortRequested.set(true);

    ParallelCollectionScanner.Result result = fixture.scanner.scanUntil(NO_DEADLINE);

    Assert.assertEquals(ParallelCollectionScanner.StopReason.ABORTED, result.stopReason());
    Assert.assertEquals(0, result.documentsIndexed());
    Assert.assertEquals(Optional.empty(), fixture.partition.getResumeRecordId());
    verify(fixture.indexingWorkScheduler, never()).cancel(any(), any(), any());
  }

  /** A position that outran the index would let a resume skip documents no commit contains. */
  @Test
  public void scanLeavesTheResumePositionAloneWhenIndexingFails() throws Exception {
    Fixture fixture = Fixture.withBatch(unboundedPartition(), new Batch(documents(1), 1));
    doReturn(CompletableFuture.failedFuture(new IllegalStateException("indexing failed")))
        .when(fixture.indexingWorkScheduler)
        .schedule(any(), any(), any(), any(), any(), any(), any());

    Assert.assertThrows(IllegalStateException.class, () -> fixture.scanner.scanUntil(NO_DEADLINE));

    Assert.assertEquals(Optional.empty(), fixture.partition.getResumeRecordId());
  }

  /** Same invariant, in the ordering the scan relies on: publish only once the batch has landed. */
  @Test
  public void scanPublishesABatchsPositionOnlyAfterItHasLanded() throws Exception {
    Fixture fixture =
        Fixture.withBatches(
            unboundedPartition(), new Batch(documents(1), 1), new Batch(documents(2), 2));

    CompletableFuture<Void> firstBatchIndexed = new CompletableFuture<>();
    doReturn(firstBatchIndexed)
        .doReturn(CompletableFuture.completedFuture(null))
        .when(fixture.indexingWorkScheduler)
        .schedule(any(), any(), any(), any(), any(), any(), any());

    CountDownLatch scanFinished = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread scannerThread =
        new Thread(
            () -> {
              try {
                fixture.scanner.scanUntil(NO_DEADLINE);
              } catch (Throwable t) {
                failure.set(t);
              }
              scanFinished.countDown();
            });
    scannerThread.start();

    // The scan cannot finish while the first batch is in flight, nor have published its position.
    Assert.assertFalse(scanFinished.await(500, TimeUnit.MILLISECONDS));
    Assert.assertEquals(Optional.empty(), fixture.partition.getResumeRecordId());

    firstBatchIndexed.complete(null);
    Assert.assertTrue(scanFinished.await(5, TimeUnit.SECONDS));

    Assert.assertNull(failure.get());
    Assert.assertEquals(Optional.of(2L), fixture.partition.getResumeRecordId());
  }

  @Test
  public void scanFailsWhenTheServerReturnsNoResumeToken() throws Exception {
    Fixture fixture = Fixture.withBatch(unboundedPartition(), new Batch(documents(1), 1));
    when(fixture.cursor.getPostBatchResumeToken()).thenReturn(Optional.empty());

    InitialSyncException exception =
        Assert.assertThrows(
            InitialSyncException.class, () -> fixture.scanner.scanUntil(NO_DEADLINE));

    Assert.assertTrue(exception.isFailed());
    Assert.assertEquals(Optional.empty(), fixture.partition.getResumeRecordId());
  }

  /**
   * Reading the next position happens while a batch indexes, so a failure there must await that
   * batch rather than leave its future unobserved. Here that batch is itself failing, so awaiting
   * it surfaces its error instead of the resume token's.
   */
  @Test
  public void scanAwaitsTheInFlightBatchWhenReadingTheNextPositionFails() throws Exception {
    Fixture fixture =
        Fixture.withBatches(
            unboundedPartition(), new Batch(documents(1), 1), new Batch(documents(2), 2));
    doReturn(CompletableFuture.failedFuture(new IllegalStateException("indexing failed")))
        .when(fixture.indexingWorkScheduler)
        .schedule(any(), any(), any(), any(), any(), any(), any());
    when(fixture.cursor.getPostBatchResumeToken())
        .thenReturn(Optional.of(resumeToken(1)))
        .thenReturn(Optional.empty());

    IllegalStateException exception =
        Assert.assertThrows(
            IllegalStateException.class, () -> fixture.scanner.scanUntil(NO_DEADLINE));

    Assert.assertEquals("indexing failed", exception.getMessage());
  }

  /** A non-numeric recordId means the plan was built for a different collection layout. */
  @Test
  public void scanFailsWhenTheResumeTokensRecordIdIsNotNumeric() throws Exception {
    Fixture fixture = Fixture.withBatch(unboundedPartition(), new Batch(documents(1), 1));
    when(fixture.cursor.getPostBatchResumeToken())
        .thenReturn(
            Optional.of(
                new BsonDocument()
                    .append("$recordId", new BsonString("clustered"))
                    .append("$initialSyncId", new BsonBinary(INITIAL_SYNC_ID))));

    InitialSyncException exception =
        Assert.assertThrows(
            InitialSyncException.class, () -> fixture.scanner.scanUntil(NO_DEADLINE));

    Assert.assertTrue(exception.isFailed());
  }

  /**
   * The batch that exhausts the cursor carries no position on mongod 8.1 and later, which consumes
   * the range rather than failing the scan.
   */
  @Test
  public void scanConsumesTheRangeWhenTheExhaustedCursorReportsNoPosition() throws Exception {
    Fixture fixture = Fixture.withBatch(unboundedPartition(), new Batch(documents(1), 1));
    when(fixture.cursor.getPostBatchResumeToken()).thenReturn(Optional.of(exhaustedResumeToken()));

    ParallelCollectionScanner.Result result = fixture.scanner.scanUntil(NO_DEADLINE);

    Assert.assertEquals(ParallelCollectionScanner.StopReason.RANGE_CONSUMED, result.stopReason());
    Assert.assertEquals(1, result.documentsIndexed());
    verify(fixture.documentIndexer).indexDocumentEvent(any());
    Assert.assertEquals(Optional.empty(), fixture.partition.getResumeRecordId());
  }

  /**
   * Without a position from the final batch, the partition keeps the last one it had, so a crash
   * before the range is recorded as consumed rescans that tail instead of skipping it.
   */
  @Test
  public void scanKeepsTheLastKnownPositionWhenTheCursorIsExhausted() throws Exception {
    Fixture fixture =
        Fixture.withBatches(
            unboundedPartition(), new Batch(documents(1), 1), new Batch(documents(2), 2));
    doReturn(Optional.of(resumeToken(1)))
        .doReturn(Optional.of(exhaustedResumeToken()))
        .when(fixture.cursor)
        .getPostBatchResumeToken();

    ParallelCollectionScanner.Result result = fixture.scanner.scanUntil(NO_DEADLINE);

    Assert.assertEquals(ParallelCollectionScanner.StopReason.RANGE_CONSUMED, result.stopReason());
    Assert.assertEquals(2, result.documentsIndexed());
    Assert.assertEquals(Optional.of(1L), fixture.partition.getResumeRecordId());
  }

  /**
   * The first partition's first window sends no {@code $_startAt}, so the server cannot reject a
   * cursor on a collection recreated since the plan was built. Unchecked, the planned bounds would
   * address an unrelated recordId space.
   */
  @Test
  public void scanFailsWhenTheCollectionIsNotTheIncarnationThePlanWasBuiltFor() throws Exception {
    Fixture fixture =
        Fixture.withBatch(
            partition(Optional.empty(), Optional.of(900L), Optional.empty()),
            new Batch(documents(1), 1));
    when(fixture.cursor.getPostBatchResumeToken())
        .thenReturn(
            Optional.of(
                new BsonDocument()
                    .append("$recordId", new BsonInt64(1))
                    .append("$initialSyncId", new BsonBinary(UUID.randomUUID()))));

    InitialSyncException exception =
        Assert.assertThrows(
            InitialSyncException.class, () -> fixture.scanner.scanUntil(NO_DEADLINE));

    // Reported as the server reports it, which is what retains the index and resyncs.
    Assert.assertTrue(exception.isRequiresResync());
    Assert.assertEquals(
        Errors.INITIAL_SYNC_ID_MISMATCH.code,
        ((MongoCommandException) exception.getCause()).getErrorCode());
    Assert.assertEquals(Optional.empty(), fixture.partition.getResumeRecordId());
  }

  /**
   * The client checks each token against the one before it, so one confirmation covers a whole
   * cursor. The drift fed in here is caught by the client in production, not by the scanner.
   */
  @Test
  public void scanConfirmsTheIncarnationOnceRatherThanOnEveryBatch() throws Exception {
    Fixture fixture =
        Fixture.withBatches(
            unboundedPartition(), new Batch(documents(1), 1), new Batch(documents(2), 2));
    doReturn(Optional.of(resumeToken(1)))
        .doReturn(
            Optional.of(
                new BsonDocument()
                    .append("$recordId", new BsonInt64(2))
                    .append("$initialSyncId", new BsonBinary(UUID.randomUUID()))))
        .when(fixture.cursor)
        .getPostBatchResumeToken();

    ParallelCollectionScanner.Result result = fixture.scanner.scanUntil(NO_DEADLINE);

    Assert.assertEquals(2, result.documentsIndexed());
    Assert.assertEquals(Optional.of(2L), fixture.partition.getResumeRecordId());
  }

  /**
   * A cursor that sends {@code $_startAt} is rejected at open unless the incarnation matches, so
   * the scanner leaves it to the server. Only a mock can produce the mismatch fed in here.
   */
  @Test
  public void scanLeavesTheIncarnationToTheServerWhenItOpensAtAPlannedPosition() throws Exception {
    Fixture fixture =
        Fixture.withBatch(
            partition(Optional.of(100L), Optional.of(900L), Optional.empty()),
            new Batch(documents(1), 101));
    when(fixture.cursor.getPostBatchResumeToken())
        .thenReturn(
            Optional.of(
                new BsonDocument()
                    .append("$recordId", new BsonInt64(101))
                    .append("$initialSyncId", new BsonBinary(UUID.randomUUID()))));

    ParallelCollectionScanner.Result result = fixture.scanner.scanUntil(NO_DEADLINE);

    Assert.assertEquals(1, result.documentsIndexed());
    Assert.assertEquals(Optional.of(101L), fixture.partition.getResumeRecordId());
  }

  /** A done partition resumes past its upper bound, so scanning it would index a neighbour's. */
  @Test
  public void scanRefusesAPartitionThatAlreadyConsumedItsRange() throws Exception {
    Fixture fixture =
        Fixture.withBatch(
            new ParallelCollectionScanPartitionState(
                0, Optional.empty(), Optional.of(900L), Optional.of(900L), true),
            new Batch(documents(1), 901));

    Assert.assertThrows(IllegalStateException.class, () -> fixture.scanner.scanUntil(NO_DEADLINE));

    verify(fixture.mongoClient, never())
        .getCollectionAggregateCommandMongoClient(any(), any(), any(), any());
  }

  @Test
  public void scanPropagatesCursorFailures() throws Exception {
    Fixture fixture = Fixture.withBatch(unboundedPartition(), new Batch(documents(1), 1));
    when(fixture.cursor.getNext())
        .thenThrow(InitialSyncException.createRequiresResync("mocked error"));

    InitialSyncException exception =
        Assert.assertThrows(
            InitialSyncException.class, () -> fixture.scanner.scanUntil(NO_DEADLINE));

    Assert.assertTrue(exception.isRequiresResync());
    verify(fixture.documentIndexer, never()).indexDocumentEvent(any());
  }
}
