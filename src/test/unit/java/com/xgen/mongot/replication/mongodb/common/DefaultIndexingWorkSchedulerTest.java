package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.base.CaseFormat;
import com.google.errorprone.annotations.Var;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.ServerStatusDataExtractor;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Enums;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.LockGuard;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

public class DefaultIndexingWorkSchedulerTest {

  private static final IndexCommitUserData COMMIT_USER_DATA =
      getCommitUserData(new MongoNamespace("db", "collection"), 0);

  private static final double NO_EPSILON = 0.0;

  private static final IndexMetricsUpdater.IndexingMetricsUpdater IGNORE_METRICS =
      SearchIndex.mockIndexingMetricsUpdater(IndexDefinition.Type.SEARCH);

  @Test
  public void testSingleBatch() throws Exception {
    DefaultIndexingWorkScheduler scheduler = scheduler();
    DocumentIndexer indexer = indexer();

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));

    DocumentEvent insertDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
    DocumentEvent updateDocument =
        DocumentEvent.createUpdate(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
    DocumentEvent deleteDocument = DocumentEvent.createDelete(new BsonInt32(1));

    // batch with 3 events for different documents
    List<DocumentEvent> batch = List.of(insertDocument, updateDocument, deleteDocument);
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    indexingFuture.get(5, TimeUnit.SECONDS);

    // since each event is on a different document, there should be 3 indexing events.
    verify(indexer, times(3)).indexDocumentEvent(any());
  }

  @Test
  public void testSingleBatchExceedsFieldLimits() {
    DefaultIndexingWorkScheduler scheduler = scheduler();
    DocumentIndexer indexer =
        com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
            .mockFieldLimitsExceeded();

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));

    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);

    List<DocumentEvent> batch = List.of(event);
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    var e = assertThrows(ExecutionException.class, indexingFuture::get);
    Throwable limitExceeded = e.getCause();
    assertThat(limitExceeded).isInstanceOf(FieldExceededLimitsException.class);
    assertEquals("exceeded", limitExceeded.getMessage());
  }

  @Test
  public void testSingleDocumentExceedsLimit() {
    DefaultIndexingWorkScheduler scheduler = scheduler();
    DocumentIndexer indexer =
        com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
            .mockDocumentExceedsFieldLimit("exceeded when indexing document");

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));

    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);

    List<DocumentEvent> batch = List.of(event);
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    var e = assertThrows(ExecutionException.class, indexingFuture::get);
    Throwable limitExceeded = e.getCause();
    assertThat(limitExceeded).isInstanceOf(FieldExceededLimitsException.class);
    assertEquals("exceeded when indexing document", limitExceeded.getMessage());
  }

  @Test
  public void testSingleDocumentExceedsLimitButIndexAsAWholeDoesNot() {
    // this shouldn't happen in practice for field limits, but it tests that error handling in IWS
    // is robust to different order of errors coming from index writer.
    DefaultIndexingWorkScheduler scheduler = scheduler();
    DocumentIndexer indexer =
        com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
            .mockDocumentExceedsLimitButNotIndex("exceeded when indexing document");

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));

    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);

    List<DocumentEvent> batch = List.of(event);
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    var e = assertThrows(ExecutionException.class, indexingFuture::get);
    Throwable limitExceeded = e.getCause();
    assertThat(limitExceeded).isInstanceOf(FieldExceededLimitsException.class);
    assertEquals("exceeded when indexing document", limitExceeded.getMessage());
  }

  @Test
  public void testExceedsFieldLimitsDoesNotUpdateCommitUserDataAfterBatch() {
    DefaultIndexingWorkScheduler scheduler = scheduler();
    DocumentIndexer indexer =
        com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
            .mockFieldLimitsExceeded();

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));

    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);

    List<DocumentEvent> batch = List.of(event);
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    assertThrows(ExecutionException.class, indexingFuture::get);
    // make sure commit user data is not updated,
    // so this batch won't be committed in the background
    verify(indexer, times(0)).updateCommitUserData(any());
  }

  @Test
  public void testMultipleBatchesSameGeneration() throws FieldExceededLimitsException {
    DefaultIndexingWorkScheduler scheduler = scheduler();

    CountDownLatch finishIndexing = new CountDownLatch(1);
    Answer<Void> hang =
        invocation -> {
          finishIndexing.await();
          return null;
        };
    DocumentIndexer indexer = indexer();
    doAnswer(hang).when(indexer).indexDocumentEvent(any());

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));

    DocumentEvent insertDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);

    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    List<DocumentEvent> batch = List.of(insertDocument);
    scheduler.schedule(
        batch,
        SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
        indexer,
        genId,
        Optional.of(attemptId),
        Optional.of(COMMIT_USER_DATA),
        IGNORE_METRICS);
    scheduler.schedule(
        batch,
        SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
        indexer,
        genId,
        Optional.of(attemptId),
        Optional.of(COMMIT_USER_DATA),
        IGNORE_METRICS);

    // first batch should index immediately.  second batch should wait for the first batch to index,
    // since they are both for the same index
    verify(indexer, timeout(500).times(1)).indexDocumentEvent(any());

    finishIndexing.countDown();

    // after the first batch is completed, indexing for the second batch can begin
    verify(indexer, timeout(500).times(2)).indexDocumentEvent(any());
  }

  @Test
  public void testMaxTwoBatches() throws Exception {
    DefaultIndexingWorkScheduler scheduler = scheduler();

    CountDownLatch finishIndexing = new CountDownLatch(1);
    Answer<Void> hang =
        invocation -> {
          finishIndexing.await();
          return null;
        };
    DocumentIndexer indexer = indexer();
    doAnswer(hang).when(indexer).indexDocumentEvent(any());

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));

    DocumentEvent insertDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);

    GenerationId firstGenId = new GenerationId(new ObjectId(), Generation.CURRENT);
    GenerationId secondGenId = new GenerationId(new ObjectId(), Generation.CURRENT);
    GenerationId thirdGenId = new GenerationId(new ObjectId(), Generation.CURRENT);

    List<DocumentEvent> batch = List.of(insertDocument);
    CompletableFuture<Void> firstIndexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            firstGenId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    CompletableFuture<Void> secondIndexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            secondGenId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    scheduler.schedule(
        batch,
        SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
        indexer,
        thirdGenId,
        Optional.of(new ObjectId()),
        Optional.of(COMMIT_USER_DATA),
        IGNORE_METRICS);

    // the first and second indexes can both be enqueued at the same time
    verify(indexer, timeout(500).times(2)).indexDocumentEvent(any());

    // first and second indexes will now finish
    finishIndexing.countDown();
    firstIndexingFuture.get(5, TimeUnit.SECONDS);
    secondIndexingFuture.get(5, TimeUnit.SECONDS);

    // also, now that two indexes are completed, the third batch can begin.
    verify(indexer, timeout(500).times(3)).indexDocumentEvent(any());
  }

  @Test
  public void testTwoBatchesAtATime() throws Exception {
    DefaultIndexingWorkScheduler scheduler = scheduler();

    MutableLatch finishIndexing = new MutableLatch(3);
    Answer<Optional<FieldExceededLimitsException>> hang =
        invocation -> {
          finishIndexing.awaitAndReset(2);
          return Optional.empty();
        };
    DocumentIndexer indexer = indexer();
    doAnswer(hang).when(indexer).exceededLimits();

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));

    DocumentEvent insertDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);

    GenerationId firstGenId = new GenerationId(new ObjectId(), Generation.CURRENT);
    GenerationId secondGenId = new GenerationId(new ObjectId(), Generation.CURRENT);

    List<DocumentEvent> batch = List.of(insertDocument);
    CompletableFuture<Void> firstIndexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            firstGenId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    CompletableFuture<Void> secondIndexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            firstGenId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    CompletableFuture<Void> thirdIndexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            secondGenId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    // the first and third batches can both index at the same time, as they are for different
    // indexes
    verify(indexer, timeout(500).times(2)).indexDocumentEvent(any());

    // first and third indexing futures will now finish
    finishIndexing.countDown();
    verify(indexer, timeout(500).times(2)).updateCommitUserData(COMMIT_USER_DATA);
    firstIndexingFuture.get(5, TimeUnit.SECONDS);
    thirdIndexingFuture.get(5, TimeUnit.SECONDS);

    // also, now that the first batch is completed, the second batch can begin.
    // it had to wait because it was for the same index.
    verify(indexer, timeout(500).times(3)).indexDocumentEvent(any());

    finishIndexing.countDown();
    verify(indexer, timeout(500).times(3)).updateCommitUserData(COMMIT_USER_DATA);

    secondIndexingFuture.get(5, TimeUnit.SECONDS);
    verify(indexer, timeout(500).times(3)).updateCommitUserData(COMMIT_USER_DATA);
  }

  @Test
  public void testCancelBatchInExecutor() throws Exception {
    DefaultIndexingWorkScheduler scheduler = scheduler();

    // Create DocumentIndexers that hang while indexing until told to complete.
    BiFunction<CyclicBarrier, CyclicBarrier, Answer<CompletableFuture<Void>>> hangForBarriers =
        (startedBarrier, doneBarrier) ->
            invocation -> {
              try {
                startedBarrier.await();
                doneBarrier.await();
                return null;
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            };

    CyclicBarrier firstIndexingStarted = new CyclicBarrier(2);
    CyclicBarrier firstIndexingDone = new CyclicBarrier(2);
    DocumentIndexer indexer = indexer();
    doAnswer(hangForBarriers.apply(firstIndexingStarted, firstIndexingDone))
        .when(indexer)
        .indexDocumentEvent(any());

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));

    DocumentEvent insertDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);

    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    List<DocumentEvent> batch = List.of(insertDocument);
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            genId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    firstIndexingStarted.await(5, TimeUnit.SECONDS);

    CompletableFuture<Void> cancelFuture =
        scheduler.cancel(genId, Optional.of(attemptId), new Exception("error"));

    // cancel future should not complete until the in-flight batch is completed
    Assert.assertThrows(TimeoutException.class, () -> cancelFuture.get(500, TimeUnit.MILLISECONDS));

    // allow the in-flight batch to complete
    firstIndexingDone.await(5, TimeUnit.SECONDS);

    // the cancel future should now be able to complete
    cancelFuture.get(500, TimeUnit.MILLISECONDS);

    // verify that the in-flight batch updated commit user data and the future
    // is completed normally
    verify(indexer, timeout(500).times(1)).updateCommitUserData(COMMIT_USER_DATA);
    indexingFuture.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testCancelBatchWaiting() throws Exception {
    DefaultIndexingWorkScheduler scheduler = singleIndexingThreadScheduler();

    DocumentIndexer indexer = indexer();

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));

    DocumentEvent insertDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);

    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    List<DocumentEvent> batch = List.of(insertDocument);
    CompletableFuture<Void> firstIndexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            genId,
            Optional.of(attemptId),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    CompletableFuture<Void> secondIndexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            genId,
            Optional.of(attemptId),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    CompletableFuture<Void> cancelFuture =
        scheduler.cancel(genId, Optional.of(attemptId), new Exception("error"));
    assertTrue(cancelFuture.isDone());

    scheduler.start();

    // verify the cancelled batches were not indexed and there futures were completed exceptionally
    TestUtils.assertThrows(
        "error", Exception.class, () -> firstIndexingFuture.get(5, TimeUnit.SECONDS));
    TestUtils.assertThrows(
        "error", Exception.class, () -> secondIndexingFuture.get(5, TimeUnit.SECONDS));
    verify(indexer, times(0)).indexDocumentEvent(any());

    CompletableFuture<Void> thirdIndexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            genId,
            Optional.of(attemptId),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    // verify that batches submitted after the cancel with the same attemptId are not processed
    TestUtils.assertThrows(
        "error", Exception.class, () -> thirdIndexingFuture.get(5, TimeUnit.SECONDS));
    verify(indexer, times(0)).indexDocumentEvent(any());

    CompletableFuture<Void> fourthIndexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            genId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    // verify that batches submitted after the cancel with a different attemptId are still processed
    fourthIndexingFuture.get(5, TimeUnit.SECONDS);
    verify(indexer, times(1)).indexDocumentEvent(any());
  }

  @Test
  public void testCancelOnlyOne() throws Exception {
    DefaultIndexingWorkScheduler scheduler = singleIndexingThreadScheduler();

    DocumentIndexer indexer = indexer();
    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));

    DocumentEvent insertDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);

    GenerationId firstGenId = new GenerationId(new ObjectId(), Generation.CURRENT);
    GenerationId secondGenId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    List<DocumentEvent> batch = List.of(insertDocument);
    CompletableFuture<Void> firstIndexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            firstGenId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    CompletableFuture<Void> secondIndexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            secondGenId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    CompletableFuture<Void> cancelFuture =
        scheduler.cancel(firstGenId, Optional.of(attemptId), new Exception("error"));
    assertTrue(cancelFuture.isDone());

    scheduler.start();

    // verify the indexing for the first index fails, because indexing for that index was cancelled,
    // but indexing for the second index succeeds.
    TestUtils.assertThrows(
        "error", Exception.class, () -> firstIndexingFuture.get(5, TimeUnit.SECONDS));
    secondIndexingFuture.get(5, TimeUnit.SECONDS);
    verify(indexer, times(1)).indexDocumentEvent(any());
  }

  @Test
  @SuppressWarnings("unused")
  public void testMetricSanity() throws Exception {
    MeterRegistry registry = new SimpleMeterRegistry();
    DefaultIndexingWorkScheduler scheduler =
        new DefaultIndexingWorkScheduler(Executors.fixedSizeThreadPool("indexing", 1, registry));
    DocumentIndexer indexer = indexer();
    ObjectId indexId = new ObjectId();
    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));
    DocumentEvent insertDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
    DocumentEvent updateDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
    DocumentEvent deleteDocument = DocumentEvent.createDelete(new BsonInt32(1));

    List<DocumentEvent> batch = List.of(insertDocument, updateDocument, deleteDocument);
    CompletableFuture<Void> indexingFutureToCancel =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            genId,
            Optional.of(attemptId),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(attemptId),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    CompletableFuture<Void> secondIndexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(attemptId),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    // 3 batches are scheduled and enqueued
    assertEquals(
        3.0, registry.find("indexingWorkScheduler.queuedBatchesTotal").gauge().value(), NO_EPSILON);
    // 3 * 3 events are scheduled and enqueued
    assertEquals(
        9.0, registry.find("indexingWorkScheduler.queuedEventsTotal").gauge().value(), NO_EPSILON);

    CompletableFuture<Void> cancelFuture =
        scheduler.cancel(genId, Optional.of(attemptId), new Exception("error"));
    cancelFuture.get(500, TimeUnit.MILLISECONDS);

    // the first batch is cancelled
    assertEquals(
        2.0, registry.find("indexingWorkScheduler.queuedBatchesTotal").gauge().value(), NO_EPSILON);
    assertEquals(
        6.0, registry.find("indexingWorkScheduler.queuedEventsTotal").gauge().value(), NO_EPSILON);

    scheduler.start();
    indexingFuture.get(5, TimeUnit.SECONDS);
    secondIndexingFuture.get(5, TimeUnit.SECONDS);

    var defaultTag =
        Tags.of("timeUnit", Enums.convertNameTo(CaseFormat.LOWER_CAMEL, TimeUnit.SECONDS));

    assertEquals(3, registry.counter("indexingWorkScheduler.enqueueCalls").count(), NO_EPSILON);
    assertEquals(2, registry.counter("indexingWorkScheduler.dequeueCalls").count(), NO_EPSILON);
    assertEquals(
        2,
        registry
            .timer(
                "indexingWorkScheduler.indexingBatchDurations",
                Tags.of(ServerStatusDataExtractor.Scope.REPLICATION.getTag()).and(defaultTag))
            .count(),
        NO_EPSILON);

    // all batches and events are executed.
    // the queue is empty
    assertEquals(
        0.0, registry.find("indexingWorkScheduler.queuedBatchesTotal").gauge().value(), NO_EPSILON);
    assertEquals(
        0.0, registry.find("indexingWorkScheduler.queuedEventsTotal").gauge().value(), NO_EPSILON);
  }

  @Test
  public void testUserDataIsUpdatedStrictlyAfterBatchIsIndexed() {

    class Batch {
      final DocumentEvent document;
      final IndexCommitUserData commitData;

      public Batch(RawBsonDocument document, IndexCommitUserData commitData, ObjectId indexId) {
        this.document =
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
        this.commitData = commitData;
      }
    }

    GenerationId generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    MongoNamespace namespace = new MongoNamespace("db", "collection");
    DefaultIndexingWorkScheduler scheduler = scheduler();
    DocumentIndexer indexer = indexer();

    List<Batch> batches =
        IntStream.range(0, 10)
            .mapToObj(
                id -> {
                  RawBsonDocument document =
                      BsonUtils.documentToRaw(
                          new BsonDocument(
                              generationId.indexId.toString(),
                              new BsonDocument("_id", new BsonInt32(id))));
                  return new Batch(
                      document, getCommitUserData(namespace, id), generationId.indexId);
                })
            .collect(Collectors.toList());

    batches.forEach(
        batch ->
            scheduler.schedule(
                List.of(batch.document),
                SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
                indexer,
                generationId,
                Optional.of(new ObjectId()),
                Optional.of(batch.commitData),
                IGNORE_METRICS));

    batches.forEach(
        batch -> {
          InOrder inOrder = inOrder(indexer);
          try {
            inOrder.verify(indexer, timeout(500).times(1)).indexDocumentEvent(batch.document);
          } catch (FieldExceededLimitsException e) {
            Assert.fail();
          }
          inOrder.verify(indexer, timeout(500).times(1)).updateCommitUserData(batch.commitData);
        });
  }

  @Test
  public void testUserDataIsNotUpdatedWhenCommitDataIsEmpty() throws Exception {
    GenerationId generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    DefaultIndexingWorkScheduler scheduler = scheduler();
    DocumentIndexer indexer = indexer();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(
                generationId.indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));
    DocumentEvent documentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), generationId.indexId),
            document);

    scheduler.schedule(
        List.of(documentEvent),
        SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
        indexer,
        generationId,
        Optional.of(new ObjectId()),
        Optional.empty(),
        IGNORE_METRICS);
    verify(indexer, timeout(500).times(1)).indexDocumentEvent(documentEvent);
    verify(indexer, timeout(500).times(0)).updateCommitUserData(any());
  }

  private DefaultIndexingWorkScheduler singleIndexingThreadScheduler() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    return new DefaultIndexingWorkScheduler(
        Executors.fixedSizeThreadPool("indexing", 1, meterRegistry));
  }

  private DefaultIndexingWorkScheduler scheduler() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    NamedExecutorService executor = Executors.fixedSizeThreadPool("indexing", 2, meterRegistry);
    return DefaultIndexingWorkScheduler.create(executor);
  }

  private DocumentIndexer indexer() {
    return com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
        .mockDocumentIndexer();
  }

  private static IndexCommitUserData getCommitUserData(MongoNamespace namespace, int token) {
    return IndexCommitUserData.createChangeStreamResume(
        ChangeStreamResumeInfo.create(namespace, new BsonDocument("token", new BsonInt32(token))),
        IndexFormatVersion.CURRENT.versionNumber);
  }

  private static class MutableLatch {
    CountDownLatch latch;
    private final Lock resetLock;
    private final Condition resetCondition;

    MutableLatch(int size) {
      this.latch = new CountDownLatch(size);
      this.resetLock = new ReentrantLock();
      this.resetCondition = this.resetLock.newCondition();
    }

    void awaitAndReset(int newSize) throws InterruptedException {
      @Var CountDownLatch currentLatch = null;
      try (var ignored = LockGuard.with(this.resetLock)) {
        while (this.latch.getCount() == 0) {
          this.resetCondition.await();
        }

        currentLatch = this.latch;
      }

      currentLatch.countDown();
      currentLatch.await();

      try (var ignored = LockGuard.with(this.resetLock)) {
        if (this.latch.getCount() == 0) {
          this.latch = new CountDownLatch(newSize);
          this.resetCondition.signalAll();
        }
      }
    }

    public void countDown() {
      try (var ignored = LockGuard.with(this.resetLock)) {
        this.latch.countDown();
      }
    }
  }
}
