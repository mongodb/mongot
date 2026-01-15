package com.xgen.mongot.replication.mongodb.initialsync;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.IndexTypeData.INDEX_TYPE_TAG_NAME;
import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_SEARCH;
import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH;
import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH_AUTO_EMBEDDING;
import static com.xgen.mongot.index.IndexTypeData.getIndexTypeTag;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.uniqueMockGenerationDefinition;
import static com.xgen.testing.mongot.mock.index.SearchIndex.IGNORE_METRICS;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockSearchDefinition;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_VECTOR_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.mockAutoEmbeddingVectorDefinition;
import static com.xgen.testing.mongot.mock.index.VectorIndex.mockVectorDefinition;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer.mockDocumentIndexer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import com.google.errorprone.annotations.Keep;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.mongodb.common.BufferlessIdOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.BufferlessNaturalOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.util.Condition;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import com.xgen.testing.ControlledBlockingQueue;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class InitialSyncQueueTest {

  private static final ChangeStreamResumeInfo RESUME_INFO =
      ChangeStreamResumeInfo.create(
          new MongoNamespace("InitialSyncQueueTest", "InitialSyncQueueTest"),
          new BsonDocument("_data", new BsonBinary("post batch resume token".getBytes())));

  private static final InitialSyncResumeInfo INITIAL_SYNC_RESUME_INFO =
      new BufferlessIdOrderInitialSyncResumeInfo(new BsonTimestamp(), new BsonInt64(10000));

  private static final InitialSyncResumeInfo NATURAL_ORDER_INITIAL_SYNC_RESUME_INFO =
      new BufferlessNaturalOrderInitialSyncResumeInfo(
          new BsonTimestamp(), new BsonDocument(), Optional.empty());

  private static final DocumentIndexer DOCUMENT_INDEXER = mockDocumentIndexer();

  @Test
  public void testSuccessfulBufferlessInitialSync() throws Exception {
    Mocks mocks = Mocks.create(Map.of(MOCK_INDEX_GENERATION_ID, () -> RESUME_INFO));

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable initialSyncStartedCallback = startedLatch::countDown;

    CompletableFuture<ChangeStreamResumeInfo> future =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            MOCK_INDEX_DEFINITION_GENERATION,
            initialSyncStartedCallback,
            IGNORE_METRICS,
            false,
            false);

    startedLatch.await(5, TimeUnit.SECONDS);
    Assert.assertFalse(future.isDone());
    mocks.mockInitialSyncManagerFactory.completeSync(MOCK_INDEX_GENERATION_ID);

    ChangeStreamResumeInfo result = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(RESUME_INFO, result);
  }

  @Test
  public void testInitialSyncExceptionBufferlessInitialSync() throws Exception {
    Mocks mocks =
        Mocks.create(
            Map.of(
                MOCK_INDEX_GENERATION_ID,
                () -> {
                  throw InitialSyncException.createDropped();
                }));

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable initialSyncStartedCallback = startedLatch::countDown;

    CompletableFuture<ChangeStreamResumeInfo> future =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            MOCK_INDEX_DEFINITION_GENERATION,
            initialSyncStartedCallback,
            IGNORE_METRICS,
            false,
            false);

    startedLatch.await(5, TimeUnit.SECONDS);
    Assert.assertFalse(future.isDone());
    mocks.mockInitialSyncManagerFactory.completeSync(MOCK_INDEX_GENERATION_ID);

    assertCompletesInitialSyncException(future, InitialSyncException.Type.DROPPED);
  }

  @Test
  public void testDoesNotExistExceptionBufferlessInitialSync() throws Exception {
    Mocks mocks = Mocks.create(Map.of(MOCK_INDEX_GENERATION_ID, () -> RESUME_INFO));

    when(mocks.mongoClient.resolveAndUpdateCollectionName(any()))
        .thenThrow(InitialSyncException.createDoesNotExist("collection does not exist locally"));

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable initialSyncStartedCallback = startedLatch::countDown;

    CompletableFuture<ChangeStreamResumeInfo> future =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            MOCK_INDEX_DEFINITION_GENERATION,
            initialSyncStartedCallback,
            IGNORE_METRICS,
            false,
            false);

    assertCompletesInitialSyncException(future, InitialSyncException.Type.DOES_NOT_EXIST);
    // The initialSyncStartedCallback should not be run if the collection does not exist
    Assert.assertEquals(1, startedLatch.getCount());
  }

  @Test
  public void testRuntimeExceptionBufferlessInitialSync() throws Exception {
    Mocks mocks =
        Mocks.create(
            Map.of(
                MOCK_INDEX_GENERATION_ID,
                () -> {
                  throw new RuntimeException();
                }));

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable initialSyncStartedCallback = startedLatch::countDown;

    CompletableFuture<ChangeStreamResumeInfo> future =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            MOCK_INDEX_DEFINITION_GENERATION,
            initialSyncStartedCallback,
            IGNORE_METRICS,
            false,
            false);

    startedLatch.await(5, TimeUnit.SECONDS);
    Assert.assertFalse(future.isDone());
    mocks.mockInitialSyncManagerFactory.completeSync(MOCK_INDEX_GENERATION_ID);

    future
        .handle(
            (ignored, throwable) -> {
              Assert.assertNull(ignored);
              Assert.assertNotNull(throwable);
              assertThat(throwable).isInstanceOf(RuntimeException.class);
              return null;
            })
        .get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testBufferlessInitialSyncsQueue() throws Exception {
    // Enqueue 4 initial syncs while only 2 should be allowed to run concurrently.
    List<IndexDefinitionGeneration> definitions =
        IntStream.range(0, 6)
            .mapToObj(i -> uniqueMockGenerationDefinition())
            .collect(Collectors.toList());

    GenerationId index0 = definitions.get(0).getGenerationId();
    GenerationId index1 = definitions.get(1).getGenerationId();
    GenerationId index2 = definitions.get(2).getGenerationId();
    GenerationId index3 = definitions.get(3).getGenerationId();
    GenerationId index4 = definitions.get(4).getGenerationId();
    GenerationId index5 = definitions.get(5).getGenerationId();

    Mocks mocks =
        Mocks.create(
            Map.of(
                index0,
                () -> RESUME_INFO,
                index1,
                () -> RESUME_INFO,
                index2,
                () -> {
                  throw InitialSyncException.createDropped();
                },
                index3,
                () -> {
                  throw InitialSyncException.createDropped();
                },
                index4,
                () -> RESUME_INFO,
                index5,
                () -> RESUME_INFO));

    CountDownLatch startedLatch0 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback0 = startedLatch0::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future0 =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            definitions.get(0),
            initialSyncStartedCallback0,
            IGNORE_METRICS,
            false,
            false);

    CountDownLatch startedLatch1 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback1 = startedLatch1::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future1 =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            definitions.get(1),
            initialSyncStartedCallback1,
            IGNORE_METRICS,
            false,
            false);

    CountDownLatch startedLatch2 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback2 = startedLatch2::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future2 =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            definitions.get(2),
            initialSyncStartedCallback2,
            IGNORE_METRICS,
            false,
            false);

    CountDownLatch startedLatch3 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback3 = startedLatch3::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future3 =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            definitions.get(3),
            initialSyncStartedCallback3,
            IGNORE_METRICS,
            false,
            true);

    CountDownLatch startedLatch4 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback4 = startedLatch4::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future4 =
        mocks.initialSyncQueue.enqueueResume(
            DOCUMENT_INDEXER,
            definitions.get(4),
            initialSyncStartedCallback4,
            INITIAL_SYNC_RESUME_INFO,
            IGNORE_METRICS,
            false,
            true);

    // isNaturalOrderScanSupported should be passed correctly through initial context
    Assert.assertFalse(mocks.queued.get(index2).getUseNaturalOrderScan());
    Assert.assertTrue(mocks.queued.get(index3).getUseNaturalOrderScan());

    // isNaturalOrderScanSupported should be false when resuming from _id scan
    Assert.assertFalse(mocks.queued.get(index4).getUseNaturalOrderScan());

    CountDownLatch startedLatch5 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback5 = startedLatch5::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future5 =
        mocks.initialSyncQueue.enqueueResume(
            DOCUMENT_INDEXER,
            definitions.get(5),
            initialSyncStartedCallback5,
            NATURAL_ORDER_INITIAL_SYNC_RESUME_INFO,
            IGNORE_METRICS,
            false,
            false);

    // isNaturalOrderScanSupported should be false and resumeInfo should be set as empty even with a
    // natural order resume info
    Assert.assertFalse(mocks.queued.get(index5).getUseNaturalOrderScan());
    Assert.assertFalse(mocks.queued.get(index5).getResumeInfo().isPresent());

    // The first two initial syncs should have started.
    startedLatch0.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(index0), timeout(1000)).sync();

    startedLatch1.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(index1), timeout(1000)).sync();

    // The second two initial syncs should not start.
    Thread.sleep(500);
    Assert.assertEquals(1, startedLatch2.getCount());
    verify(mocks.mockInitialSyncManagerFactory.getManager(index2), times(0)).sync();
    Assert.assertEquals(1, startedLatch3.getCount());
    verify(mocks.mockInitialSyncManagerFactory.getManager(index3), times(0)).sync();

    // Complete the first initial sync.
    mocks.mockInitialSyncManagerFactory.completeSync(index0);
    FutureUtils.swallowedFuture(future0).get(5, TimeUnit.SECONDS);

    // The third initial sync should start.
    startedLatch2.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(index2), timeout(1000)).sync();

    // The fourth initial sync should not have started.
    Assert.assertEquals(1, startedLatch3.getCount());
    verify(mocks.mockInitialSyncManagerFactory.getManager(index3), times(0)).sync();

    // Complete the third initial sync.
    mocks.mockInitialSyncManagerFactory.completeSync(index2);
    FutureUtils.swallowedFuture(future2).get(5, TimeUnit.SECONDS);

    // The fourth initial sync should start.
    startedLatch3.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(index3), timeout(1000)).sync();

    // Complete the remaining initial syncs.
    mocks.mockInitialSyncManagerFactory.completeSync(index1);
    mocks.mockInitialSyncManagerFactory.completeSync(index3);
    FutureUtils.swallowedFuture(future1).get(5, TimeUnit.SECONDS);
    FutureUtils.swallowedFuture(future3).get(5, TimeUnit.SECONDS);
    mocks.mockInitialSyncManagerFactory.completeSync(index4);
    FutureUtils.swallowedFuture(future4).get(5, TimeUnit.SECONDS);
    mocks.mockInitialSyncManagerFactory.completeSync(index5);
    FutureUtils.swallowedFuture(future5).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testBufferlessInitialSyncsQueue_ChangeSyncSource() throws Exception {
    List<IndexDefinitionGeneration> definitions =
        IntStream.range(0, 2)
            .mapToObj(i -> uniqueMockGenerationDefinition())
            .collect(Collectors.toList());

    GenerationId index0 = definitions.get(0).getGenerationId();
    GenerationId index1 = definitions.get(1).getGenerationId();

    String syncSourceHost0 = "host0";
    InitialSyncMongoClient mongoClient0 = mock(InitialSyncMongoClient.class);
    when(mongoClient0.resolveAndUpdateCollectionName(any()))
        .thenReturn(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME);

    String syncSourceHost1 = "host1";
    InitialSyncMongoClient mongoClient1 = mock(InitialSyncMongoClient.class);
    when(mongoClient1.resolveAndUpdateCollectionName(any()))
        .thenReturn(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME);

    Mocks mocks =
        Mocks.create(
            Map.of(index0, () -> RESUME_INFO, index1, () -> RESUME_INFO),
            new InitialSyncConfig(true),
            syncSourceHost0,
            Map.of(syncSourceHost0, mongoClient0, syncSourceHost1, mongoClient1));

    CountDownLatch startedLatch0 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback0 = startedLatch0::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future0 =
        mocks.initialSyncQueue.enqueueResume(
            DOCUMENT_INDEXER,
            definitions.get(0),
            initialSyncStartedCallback0,
            new BufferlessNaturalOrderInitialSyncResumeInfo(
                new BsonTimestamp(), new BsonDocument(), Optional.of(syncSourceHost0)),
            IGNORE_METRICS,
            false,
            true);

    CountDownLatch startedLatch1 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback1 = startedLatch1::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future1 =
        mocks.initialSyncQueue.enqueueResume(
            DOCUMENT_INDEXER,
            definitions.get(1),
            initialSyncStartedCallback1,
            new BufferlessNaturalOrderInitialSyncResumeInfo(
                new BsonTimestamp(), new BsonDocument(), Optional.of(syncSourceHost1)),
            IGNORE_METRICS,
            false,
            true);

    // The first two initial syncs should have started.
    startedLatch0.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(index0), timeout(1000)).sync();

    assertEquals(mongoClient0, mocks.mockInitialSyncManagerFactory.getClient(index0));

    startedLatch1.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(index1), timeout(1000)).sync();
    assertEquals(mongoClient1, mocks.mockInitialSyncManagerFactory.getClient(index1));

    // Complete the first initial sync.
    mocks.mockInitialSyncManagerFactory.completeSync(index0);
    FutureUtils.swallowedFuture(future0).get(5, TimeUnit.SECONDS);

    // Complete the remaining initial syncs.
    mocks.mockInitialSyncManagerFactory.completeSync(index1);
    FutureUtils.swallowedFuture(future1).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testCancelInProgressBufferlessSync() throws Exception {
    Mocks mocks = Mocks.create(Map.of(MOCK_INDEX_GENERATION_ID, () -> RESUME_INFO));

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable initialSyncStartedCallback = startedLatch::countDown;

    CompletableFuture<ChangeStreamResumeInfo> future =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            MOCK_INDEX_DEFINITION_GENERATION,
            initialSyncStartedCallback,
            IGNORE_METRICS,
            false,
            false);

    startedLatch.await(5, TimeUnit.SECONDS);

    CompletableFuture<Void> cancelledFuture =
        mocks.initialSyncQueue.cancel(MOCK_INDEX_GENERATION_ID);
    cancelledFuture.get(5, TimeUnit.SECONDS);
    assertCompletesInitialSyncException(future, InitialSyncException.Type.SHUT_DOWN);
    Assert.assertEquals(0, mocks.inProgress.size());
  }

  @Test
  public void testInitialSyncIsCancelledRightAfterCompletionStillPerformsCleanup()
      throws Exception {
    Mocks mocks = Mocks.create(Map.of(MOCK_INDEX_GENERATION_ID, () -> RESUME_INFO));

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable initialSyncStartedCallback = startedLatch::countDown;

    CompletableFuture<ChangeStreamResumeInfo> syncFuture =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            MOCK_INDEX_DEFINITION_GENERATION,
            initialSyncStartedCallback,
            IGNORE_METRICS,
            false,
            false);

    startedLatch.await(5, TimeUnit.SECONDS);

    mocks.mockInitialSyncManagerFactory.completeSync(MOCK_INDEX_GENERATION_ID);
    CompletableFuture<Void> cancelledFuture =
        mocks.initialSyncQueue.cancel(MOCK_INDEX_GENERATION_ID);

    // Both these future should complete
    cancelledFuture.get(5, TimeUnit.SECONDS);

    // We only care that syncFuture completes, it doesn't matter if it completes successfully or
    // not, depending on how events are interleaved.
    FutureUtils.swallowedFuture(syncFuture).get(5, TimeUnit.SECONDS);

    // Make sure the IS thread cleaned up after itself even if it was interrupted during cleanup
    // time
    Assert.assertEquals(0, mocks.inProgress.size());
  }

  @Test
  public void testCancelQueuedBufferlessSync() throws Exception {
    // Enqueue 3 initial syncs and wait until the first two are started.
    List<IndexDefinitionGeneration> definitions =
        IntStream.range(0, 3)
            .mapToObj(i -> uniqueMockGenerationDefinition())
            .collect(Collectors.toList());

    GenerationId index0 = definitions.get(0).getGenerationId();
    GenerationId index1 = definitions.get(1).getGenerationId();
    GenerationId index2 = definitions.get(2).getGenerationId();

    Mocks mocks =
        Mocks.create(
            Map.of(
                index0, () -> RESUME_INFO, index1, () -> RESUME_INFO, index2, () -> RESUME_INFO));

    CountDownLatch startedLatch0 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback0 = startedLatch0::countDown;
    mocks.initialSyncQueue.enqueue(
        DOCUMENT_INDEXER,
        definitions.get(0),
        initialSyncStartedCallback0,
        IGNORE_METRICS,
        false,
        false);

    CountDownLatch startedLatch1 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback1 = startedLatch1::countDown;
    mocks.initialSyncQueue.enqueue(
        DOCUMENT_INDEXER,
        definitions.get(1),
        initialSyncStartedCallback1,
        IGNORE_METRICS,
        false,
        false);

    CountDownLatch startedLatch2 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback2 = startedLatch2::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future2 =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            definitions.get(2),
            initialSyncStartedCallback2,
            IGNORE_METRICS,
            false,
            false);

    // The first two initial syncs should have started.
    startedLatch0.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(index0), timeout(1000)).sync();

    startedLatch1.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(index1), timeout(1000)).sync();

    // The third initial sync shouldn't be started.
    Assert.assertEquals(1, startedLatch2.getCount());
    verify(mocks.mockInitialSyncManagerFactory.getManager(index2), times(0)).sync();

    // Cancel the third initial sync.
    CompletableFuture<Void> cancelledFuture =
        mocks.initialSyncQueue.cancel(definitions.get(2).getGenerationId());
    cancelledFuture.get(5, TimeUnit.SECONDS);
    future2
        .handle(
            (ignored, throwable) -> {
              Assert.assertNull(ignored);
              Assert.assertNotNull(throwable);
              assertThat(throwable).isInstanceOf(InitialSyncException.class);
              Assert.assertTrue(((InitialSyncException) throwable).isShutdown());
              return null;
            })
        .get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testBufferlessSyncOfTwoIndexesWithSameIndexId() throws Exception {
    // Enqueue 2 initial syncs with the same indexId at the same time, cancel one but not the other
    IndexDefinitionGeneration index0 = uniqueMockGenerationDefinition();
    IndexDefinitionGeneration index1 =
        SearchIndexDefinitionGenerationBuilder.create(
            index0.getIndexDefinition().asSearchDefinition(),
            index0.generation().incrementUser(),
            Collections.emptyList());

    GenerationId indexId0 = index0.getGenerationId();
    GenerationId indexId1 = index1.getGenerationId();

    Mocks mocks = Mocks.create(Map.of(indexId0, () -> RESUME_INFO, indexId1, () -> RESUME_INFO));

    CountDownLatch startedLatch0 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback0 = startedLatch0::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future0 =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER, index0, initialSyncStartedCallback0, IGNORE_METRICS, false, false);

    CountDownLatch startedLatch1 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback1 = startedLatch1::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future1 =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER, index1, initialSyncStartedCallback1, IGNORE_METRICS, false, false);

    // The first two initial syncs should have started.
    startedLatch0.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(indexId0), timeout(1000)).sync();

    startedLatch1.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(indexId1), timeout(1000)).sync();

    // Cancel sync 0
    CompletableFuture<Void> cancelledFuture = mocks.initialSyncQueue.cancel(indexId0);
    cancelledFuture.get(5, TimeUnit.SECONDS);

    // sync 0 should be off, 1 still on
    assertCompletesInitialSyncException(future0, InitialSyncException.Type.SHUT_DOWN);
    Assert.assertFalse(future1.isCompletedExceptionally());
    Assert.assertFalse(future1.isCancelled());
    // finish initial sync 1
    mocks.mockInitialSyncManagerFactory.completeSync(indexId1);
    assertCompletesInitialSyncSuccess(future1);
  }

  @Test
  public void testShutdownNoBufferlessSyncs() throws Exception {
    Mocks mocks = Mocks.create(Collections.emptyMap());
    CompletableFuture<Void> future = mocks.initialSyncQueue.shutdown();
    Assert.assertTrue(future.isDone());
    future.get();
  }

  @Test
  public void testShutdownWithBufferlessSyncs() throws Exception {
    // Enqueue 4 initial syncs and wait until the first two are started.
    List<IndexDefinitionGeneration> definitions =
        IntStream.range(0, 4)
            .mapToObj(i -> uniqueMockGenerationDefinition())
            .collect(Collectors.toList());

    GenerationId index0 = definitions.get(0).getGenerationId();
    GenerationId index1 = definitions.get(1).getGenerationId();
    GenerationId index2 = definitions.get(2).getGenerationId();
    GenerationId index3 = definitions.get(3).getGenerationId();

    Mocks mocks =
        Mocks.create(
            Map.of(
                index0,
                () -> RESUME_INFO,
                index1,
                () -> {
                  throw InitialSyncException.createDropped();
                },
                index2,
                () -> RESUME_INFO,
                index3,
                () -> {
                  throw InitialSyncException.createDropped();
                }));

    CountDownLatch startedLatch0 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback0 = startedLatch0::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future0 =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            definitions.get(0),
            initialSyncStartedCallback0,
            IGNORE_METRICS,
            false,
            false);

    CountDownLatch startedLatch1 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback1 = startedLatch1::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future1 =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            definitions.get(1),
            initialSyncStartedCallback1,
            IGNORE_METRICS,
            false,
            false);

    CountDownLatch startedLatch2 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback2 = startedLatch2::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future2 =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            definitions.get(2),
            initialSyncStartedCallback2,
            IGNORE_METRICS,
            false,
            false);

    // The first two initial syncs should have started.
    startedLatch0.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(index0), timeout(1000)).sync();

    startedLatch1.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(index1), timeout(1000)).sync();

    CompletableFuture<Void> shutdownFuture = mocks.initialSyncQueue.shutdown();

    // All initial syncs should complete with a SHUT_DOWN InitialSyncException.
    assertCompletesInitialSyncException(future0, InitialSyncException.Type.SHUT_DOWN);
    assertCompletesInitialSyncException(future1, InitialSyncException.Type.SHUT_DOWN);
    assertCompletesInitialSyncException(future2, InitialSyncException.Type.SHUT_DOWN);

    // The shutdown future should complete successfully.
    shutdownFuture.get();
  }

  @Test
  public void testCannotEnqueueBufferlessAfterShutdown() throws Exception {
    Mocks mocks = Mocks.create(Map.of(MOCK_INDEX_GENERATION_ID, () -> RESUME_INFO));
    mocks.initialSyncQueue.shutdown();

    try {
      mocks.initialSyncQueue.enqueue(
          DOCUMENT_INDEXER,
          MOCK_INDEX_DEFINITION_GENERATION,
          () -> {
            // ignored
          },
          IGNORE_METRICS,
          false,
          false);

      Assert.fail("InitialSyncQueue didn't throw exception");
    } catch (InitialSyncException e) {
      Assert.assertTrue(e.isShutdown());
    }
  }

  @Test
  public void testCannotEnqueueBufferlessExistingIndex() throws Exception {
    Mocks mocks = Mocks.create(Map.of(MOCK_INDEX_GENERATION_ID, () -> RESUME_INFO));

    mocks.initialSyncQueue.enqueue(
        DOCUMENT_INDEXER,
        MOCK_INDEX_DEFINITION_GENERATION,
        () -> {
          // ignored
        },
        IGNORE_METRICS,
        false,
        false);

    try {
      mocks.initialSyncQueue.enqueue(
          DOCUMENT_INDEXER,
          MOCK_INDEX_DEFINITION_GENERATION,
          () -> {
            // ignored
          },
          IGNORE_METRICS,
          false,
          false);

      Assert.fail("InitialSyncQueue didn't throw exception");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testShutdownBufferlessAfterTakenOffRequestQueue() throws Exception {
    ControlledBlockingQueue<GenerationId> blockingQueue = ControlledBlockingQueue.paused();
    Mocks mocks = Mocks.create(Map.of(MOCK_INDEX_GENERATION_ID, () -> RESUME_INFO), blockingQueue);

    mocks.initialSyncQueue.enqueue(
        DOCUMENT_INDEXER, MOCK_INDEX_DEFINITION_GENERATION, () -> {}, IGNORE_METRICS, false, false);
    CompletableFuture<Void> shutdownFuture =
        CompletableFuture.runAsync(mocks.initialSyncQueue::shutdown);
    Thread.sleep(500); // Wait for shutdown() to interrupt the dispatcher thread
    blockingQueue.resume();

    shutdownFuture.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testCancelBufferlessNonExistentIndexPasses() throws Exception {
    Mocks mocks = Mocks.create(Collections.emptyMap());
    var future = mocks.initialSyncQueue.cancel(MOCK_INDEX_GENERATION_ID);
    Assert.assertTrue(future.isDone() && !future.isCompletedExceptionally());
  }

  @Test
  public void testLimitEmbeddingInitialSyncs() throws Exception {
    VectorIndexDefinitionGeneration firstIndexGeneration =
        mockDefinitionGeneration(mockAutoEmbeddingVectorDefinition(new ObjectId()));
    VectorIndexDefinitionGeneration secondIndexGeneration =
        mockDefinitionGeneration(mockAutoEmbeddingVectorDefinition(new ObjectId()));

    Mocks mocks =
        Mocks.create(
            Map.of(
                firstIndexGeneration.getGenerationId(), () -> RESUME_INFO,
                secondIndexGeneration.getGenerationId(), () -> RESUME_INFO),
            ControlledBlockingQueue.ready(),
            Optional.of(2), // numConcurrentInitialSyncs
            Optional.of(1)); // maxConcurrentEmbeddingInitialSyncs

    CountDownLatch startedLatch0 = new CountDownLatch(1);
    CountDownLatch startedLatch1 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback0 = startedLatch0::countDown;
    Runnable initialSyncStartedCallback1 = startedLatch1::countDown;

    mocks.initialSyncQueue.enqueue(
        DOCUMENT_INDEXER,
        firstIndexGeneration,
        initialSyncStartedCallback0,
        IGNORE_METRICS,
        false,
        false);
    mocks.initialSyncQueue.enqueue(
        DOCUMENT_INDEXER,
        secondIndexGeneration,
        initialSyncStartedCallback1,
        IGNORE_METRICS,
        false,
        false);

    // The first initial syncs should have started.
    startedLatch0.await(5, TimeUnit.SECONDS);
    verify(
            mocks.mockInitialSyncManagerFactory.getManager(firstIndexGeneration.getGenerationId()),
            timeout(1000))
        .sync();

    // The second initial should not start.
    Thread.sleep(500);
    Assert.assertEquals(1, startedLatch1.getCount());
  }

  @Test
  public void testEmbeddingInitialSyncLimitExceedsNumConcurrentInitialSyncs() throws Exception {
    VectorIndexDefinitionGeneration firstIndexGeneration =
        mockDefinitionGeneration(mockAutoEmbeddingVectorDefinition(new ObjectId()));
    VectorIndexDefinitionGeneration secondIndexGeneration =
        mockDefinitionGeneration(mockAutoEmbeddingVectorDefinition(new ObjectId()));

    Mocks mocks =
        Mocks.create(
            Map.of(
                firstIndexGeneration.getGenerationId(), () -> RESUME_INFO,
                secondIndexGeneration.getGenerationId(), () -> RESUME_INFO),
            ControlledBlockingQueue.ready(),
            Optional.of(2), // numConcurrentInitialSyncs
            Optional.of(3)); // maxConcurrentEmbeddingInitialSyncs

    CountDownLatch startedLatch0 = new CountDownLatch(1);
    CountDownLatch startedLatch1 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback0 = startedLatch0::countDown;
    Runnable initialSyncStartedCallback1 = startedLatch1::countDown;

    mocks.initialSyncQueue.enqueue(
        DOCUMENT_INDEXER,
        firstIndexGeneration,
        initialSyncStartedCallback0,
        IGNORE_METRICS,
        false,
        false);
    mocks.initialSyncQueue.enqueue(
        DOCUMENT_INDEXER,
        secondIndexGeneration,
        initialSyncStartedCallback1,
        IGNORE_METRICS,
        false,
        false);

    // Both initial syncs should have started.
    startedLatch0.await(5, TimeUnit.SECONDS);
    verify(
            mocks.mockInitialSyncManagerFactory.getManager(firstIndexGeneration.getGenerationId()),
            timeout(1000))
        .sync();
    startedLatch1.await(5, TimeUnit.SECONDS);
    verify(
            mocks.mockInitialSyncManagerFactory.getManager(secondIndexGeneration.getGenerationId()),
            timeout(1000))
        .sync();
  }

  @Test
  public void testConcurrentEmbeddingAndSearchInitialSyncs() throws Exception {
    VectorIndexDefinitionGeneration embeddingIndexGeneration =
        mockDefinitionGeneration(mockAutoEmbeddingVectorDefinition(new ObjectId()));

    Mocks mocks =
        Mocks.create(
            Map.of(
                embeddingIndexGeneration.getGenerationId(),
                () -> RESUME_INFO,
                MOCK_INDEX_GENERATION_ID,
                () -> RESUME_INFO),
            ControlledBlockingQueue.ready(),
            Optional.of(2), // numConcurrentInitialSyncs
            Optional.of(1)); // maxConcurrentEmbeddingInitialSyncs

    CountDownLatch startedLatch0 = new CountDownLatch(1);
    CountDownLatch startedLatch1 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback0 = startedLatch0::countDown;
    Runnable initialSyncStartedCallback1 = startedLatch1::countDown;

    mocks.initialSyncQueue.enqueue(
        DOCUMENT_INDEXER,
        embeddingIndexGeneration,
        initialSyncStartedCallback0,
        IGNORE_METRICS,
        false,
        false);
    mocks.initialSyncQueue.enqueue(
        DOCUMENT_INDEXER,
        MOCK_INDEX_DEFINITION_GENERATION,
        initialSyncStartedCallback1,
        IGNORE_METRICS,
        false,
        false);

    // Both initial syncs should have started.
    startedLatch0.await(5, TimeUnit.SECONDS);
    verify(
            mocks.mockInitialSyncManagerFactory.getManager(
                embeddingIndexGeneration.getGenerationId()),
            timeout(1000))
        .sync();
    startedLatch1.await(5, TimeUnit.SECONDS);
    verify(mocks.mockInitialSyncManagerFactory.getManager(MOCK_INDEX_GENERATION_ID), timeout(1000))
        .sync();
  }

  @Test
  public void testConcurrentEmbeddingSyncsSemaphoreReleased() throws Exception {
    VectorIndexDefinitionGeneration indexGeneration =
        mockDefinitionGeneration(mockAutoEmbeddingVectorDefinition(new ObjectId()));

    Mocks mocks =
        Mocks.create(
            Map.of(indexGeneration.getGenerationId(), () -> RESUME_INFO),
            ControlledBlockingQueue.ready(),
            Optional.of(2), // numConcurrentInitialSyncs
            Optional.of(1)); // maxConcurrentEmbeddingInitialSyncs

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable initialSyncStartedCallback = startedLatch::countDown;

    CompletableFuture<ChangeStreamResumeInfo> future =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            indexGeneration,
            initialSyncStartedCallback,
            IGNORE_METRICS,
            false,
            false);

    startedLatch.await(5, TimeUnit.SECONDS);
    Assert.assertFalse(future.isDone());

    assertNumberOfEmbeddingPermits(mocks, 0);
    mocks.mockInitialSyncManagerFactory.completeSync(indexGeneration.getGenerationId());
    assertCompletesInitialSyncSuccess(future);
    assertNumberOfEmbeddingPermits(mocks, 1);
  }

  @Test
  public void testConcurrentEmbeddingSyncsSemaphoreReleasedAfterException() throws Exception {
    VectorIndexDefinitionGeneration indexGeneration =
        mockDefinitionGeneration(mockAutoEmbeddingVectorDefinition(new ObjectId()));

    Mocks mocks =
        Mocks.create(
            Map.of(
                indexGeneration.getGenerationId(),
                () -> {
                  throw InitialSyncException.createDropped();
                }),
            ControlledBlockingQueue.ready(),
            Optional.of(2), // numConcurrentInitialSyncs
            Optional.of(1)); // maxConcurrentEmbeddingInitialSyncs

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable initialSyncStartedCallback = startedLatch::countDown;

    CompletableFuture<ChangeStreamResumeInfo> future =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            indexGeneration,
            initialSyncStartedCallback,
            IGNORE_METRICS,
            false,
            false);

    startedLatch.await(5, TimeUnit.SECONDS);
    Assert.assertFalse(future.isDone());

    assertNumberOfEmbeddingPermits(mocks, 0);
    mocks.mockInitialSyncManagerFactory.completeSync(indexGeneration.getGenerationId());
    assertCompletesInitialSyncException(future, InitialSyncException.Type.DROPPED);
    assertNumberOfEmbeddingPermits(mocks, 1);
  }

  @Test
  public void testConcurrentEmbeddingSyncsSemaphoreReleasedAfterShutdown() throws Exception {
    VectorIndexDefinitionGeneration indexGeneration =
        mockDefinitionGeneration(mockAutoEmbeddingVectorDefinition(new ObjectId()));

    Mocks mocks =
        Mocks.create(
            Map.of(indexGeneration.getGenerationId(), () -> RESUME_INFO),
            ControlledBlockingQueue.ready(),
            Optional.of(2), // numConcurrentInitialSyncs
            Optional.of(1)); // maxConcurrentEmbeddingInitialSyncs

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable initialSyncStartedCallback = startedLatch::countDown;

    CompletableFuture<ChangeStreamResumeInfo> future =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            indexGeneration,
            initialSyncStartedCallback,
            IGNORE_METRICS,
            false,
            false);

    startedLatch.await(5, TimeUnit.SECONDS);
    Assert.assertFalse(future.isDone());

    assertNumberOfEmbeddingPermits(mocks, 0);
    mocks.initialSyncQueue.shutdown().get(5, TimeUnit.SECONDS);
    assertCompletesInitialSyncException(future, InitialSyncException.Type.SHUT_DOWN);
    assertNumberOfEmbeddingPermits(mocks, 1);
  }

  @Test
  public void testRemoveMatchCollectionUuid() throws Exception {
    Mocks mocks =
        Mocks.create(
            Map.of(MOCK_INDEX_GENERATION_ID, () -> RESUME_INFO),
            ControlledBlockingQueue.ready(),
            new SimpleMeterRegistry(),
            Optional.of(2),
            Optional.empty());

    CompletableFuture<ChangeStreamResumeInfo> future =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            MOCK_INDEX_DEFINITION_GENERATION,
            () -> {},
            IGNORE_METRICS,
            true,
            false);
    mocks.mockInitialSyncManagerFactory.completeSync(MOCK_INDEX_GENERATION_ID);
    future.get(5, TimeUnit.SECONDS);

    assertThat(mocks.mockInitialSyncManagerFactory.context).isNotNull();
    assertThat(mocks.mockInitialSyncManagerFactory.context.isRemoveMatchCollectionUuid()).isTrue();
  }

  @Test
  public void testInProgressSearchGauge() throws Exception {
    testInProgressMetricTags(MOCK_INDEX_DEFINITION_GENERATION);
  }

  @Test
  public void testInProgressVectorSearchGauge() throws Exception {
    testInProgressMetricTags(MOCK_VECTOR_INDEX_DEFINITION_GENERATION);
  }

  @Test
  public void testInProgressAutoEmbeddingGauge() throws Exception {
    VectorIndexDefinitionGeneration indexGeneration =
        mockDefinitionGeneration(mockAutoEmbeddingVectorDefinition(new ObjectId()));
    testInProgressMetricTags(indexGeneration);
  }

  @Test
  public void testQueuedSearchGauge() throws Exception {
    SearchIndexDefinitionGeneration indexGeneration1 =
        mockDefinitionGeneration(mockSearchDefinition(new ObjectId()));
    SearchIndexDefinitionGeneration indexGeneration2 =
        mockDefinitionGeneration(mockSearchDefinition(new ObjectId()));
    testQueuedMetricTags(indexGeneration1, indexGeneration2);
  }

  @Test
  public void testQueuedVectorSearchGauge() throws Exception {
    VectorIndexDefinitionGeneration indexGeneration1 =
        mockDefinitionGeneration(mockVectorDefinition(new ObjectId()));
    VectorIndexDefinitionGeneration indexGeneration2 =
        mockDefinitionGeneration(mockVectorDefinition(new ObjectId()));
    testQueuedMetricTags(indexGeneration1, indexGeneration2);
  }

  @Test
  public void testQueuedAutoEmbeddingGauge() throws Exception {
    VectorIndexDefinitionGeneration indexGeneration1 =
        mockDefinitionGeneration(mockAutoEmbeddingVectorDefinition(new ObjectId()));
    VectorIndexDefinitionGeneration indexGeneration2 =
        mockDefinitionGeneration(mockAutoEmbeddingVectorDefinition(new ObjectId()));
    testQueuedMetricTags(indexGeneration1, indexGeneration2);
  }

  @Test
  public void testEnqueueInitialSyncsPaused() throws Exception {
    var initialSyncGate = ToggleGate.closed();

    VectorIndexDefinitionGeneration indexGeneration =
        mockDefinitionGeneration(mockAutoEmbeddingVectorDefinition(new ObjectId()));
    Mocks mocks =
        Mocks.create(Map.of(indexGeneration.getGenerationId(), () -> RESUME_INFO), initialSyncGate);
    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable initialSyncStartedCallback = startedLatch::countDown;

    assertThrows(
        InitialSyncException.class,
        () ->
            mocks.initialSyncQueue.enqueue(
                DOCUMENT_INDEXER,
                indexGeneration,
                initialSyncStartedCallback,
                IGNORE_METRICS,
                false,
                false));
    Assert.assertEquals(0, mocks.queued.size());

    initialSyncGate.open();

    CompletableFuture<ChangeStreamResumeInfo> future =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            indexGeneration,
            initialSyncStartedCallback,
            IGNORE_METRICS,
            false,
            false);

    // Wait for the task to be picked up by the dispatcher and moved to in-progress.
    startedLatch.await(5, TimeUnit.SECONDS);
    mocks.mockInitialSyncManagerFactory.completeSync(indexGeneration.getGenerationId());

    // The future should now complete successfully.
    future.get(5, TimeUnit.SECONDS);
  }

  private void testInProgressMetricTags(IndexDefinitionGeneration indexGeneration)
      throws Exception {
    String name = "initialsync.dispatcher.inProgressSyncs";
    MeterRegistry registry = new SimpleMeterRegistry();

    Mocks mocks =
        Mocks.create(
            Map.of(indexGeneration.getGenerationId(), () -> RESUME_INFO),
            ControlledBlockingQueue.ready(),
            registry,
            Optional.of(2), // numConcurrentInitialSyncs
            Optional.empty()); // maxConcurrentEmbeddingInitialSyncs

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable initialSyncStartedCallback = startedLatch::countDown;
    CompletableFuture<ChangeStreamResumeInfo> future =
        mocks.initialSyncQueue.enqueue(
            DOCUMENT_INDEXER,
            indexGeneration,
            initialSyncStartedCallback,
            IGNORE_METRICS,
            false,
            false);

    startedLatch.await(5, TimeUnit.SECONDS);
    IndexTypeData.IndexTypeTag indexType = getIndexTypeTag(indexGeneration.getIndexDefinition());
    assertIndexTypeTagsGauge(
        registry,
        name,
        indexType == TAG_SEARCH ? 1D : 0D,
        indexType == TAG_VECTOR_SEARCH ? 1D : 0D,
        indexType == TAG_VECTOR_SEARCH_AUTO_EMBEDDING ? 1D : 0D);
    mocks.mockInitialSyncManagerFactory.completeSync(indexGeneration.getGenerationId());
    future.get(5, TimeUnit.SECONDS);
    // Wait for .thenRun() clause to complete to decrement gauges
    Thread.sleep(500);
    assertIndexTypeTagsGauge(registry, name, 0D, 0D, 0D);
  }

  private void testQueuedMetricTags(
      IndexDefinitionGeneration indexGeneration1, IndexDefinitionGeneration indexGeneration2)
      throws Exception {
    String name = "initialsync.queue.queuedSyncs";
    MeterRegistry registry = new SimpleMeterRegistry();

    Mocks mocks =
        Mocks.create(
            Map.of(
                indexGeneration1.getGenerationId(),
                () -> RESUME_INFO,
                indexGeneration2.getGenerationId(),
                () -> RESUME_INFO),
            ControlledBlockingQueue.ready(),
            registry,
            Optional.of(1), // numConcurrentInitialSyncs
            Optional.empty()); // maxConcurrentEmbeddingInitialSyncs

    CountDownLatch startedLatch1 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback1 = startedLatch1::countDown;
    mocks.initialSyncQueue.enqueue(
        DOCUMENT_INDEXER,
        indexGeneration1,
        initialSyncStartedCallback1,
        IGNORE_METRICS,
        false,
        false);

    CountDownLatch startedLatch2 = new CountDownLatch(1);
    Runnable initialSyncStartedCallback2 = startedLatch2::countDown;
    mocks.initialSyncQueue.enqueue(
        DOCUMENT_INDEXER,
        indexGeneration2,
        initialSyncStartedCallback2,
        IGNORE_METRICS,
        false,
        false);

    // Attempt to start both initial syncs. One initial sync should remain queued.
    startedLatch1.await(5, TimeUnit.SECONDS);
    IndexTypeData.IndexTypeTag indexType = getIndexTypeTag(indexGeneration1.getIndexDefinition());
    assertIndexTypeTagsGauge(
        registry,
        name,
        indexType == TAG_SEARCH ? 1D : 0D,
        indexType == TAG_VECTOR_SEARCH ? 1D : 0D,
        indexType == TAG_VECTOR_SEARCH_AUTO_EMBEDDING ? 1D : 0D);
    mocks.mockInitialSyncManagerFactory.completeSync(indexGeneration1.getGenerationId());
    // Start the second initial sync. No more initial syncs should be queued.
    startedLatch2.await(5, TimeUnit.SECONDS);
    assertIndexTypeTagsGauge(registry, name, 0D, 0D, 0D);
  }

  private static void assertIndexTypeTagsGauge(
      MeterRegistry registry,
      String meterName,
      double expectedSearchValue,
      double expectedVectorSearchValue,
      double expectedAutoEmbeddingValue) {
    assertThat(
            registry.get(meterName).tag(INDEX_TYPE_TAG_NAME, TAG_SEARCH.tagValue).gauge().value())
        .isEqualTo(expectedSearchValue);
    assertThat(
            registry
                .get(meterName)
                .tag(INDEX_TYPE_TAG_NAME, TAG_VECTOR_SEARCH.tagValue)
                .gauge()
                .value())
        .isEqualTo(expectedVectorSearchValue);
    assertThat(
            registry
                .get(meterName)
                .tag(INDEX_TYPE_TAG_NAME, TAG_VECTOR_SEARCH_AUTO_EMBEDDING.tagValue)
                .gauge()
                .value())
        .isEqualTo(expectedAutoEmbeddingValue);
  }

  private static void assertCompletesInitialSyncException(
      CompletableFuture<?> future, InitialSyncException.Type type) throws Exception {
    future
        .handle(
            (ignored, throwable) -> {
              Assert.assertNull(ignored);
              Assert.assertNotNull(throwable);
              assertThat(throwable).isInstanceOf(InitialSyncException.class);
              Assert.assertEquals(type, ((InitialSyncException) throwable).getType());
              return null;
            })
        .get(5, TimeUnit.SECONDS);
  }

  private static void assertCompletesInitialSyncSuccess(CompletableFuture<?> future)
      throws Exception {
    future
        .handle(
            (ignored, throwable) -> {
              Assert.assertNull(throwable);
              return null;
            })
        .get(5, TimeUnit.SECONDS);
  }

  private static void assertNumberOfEmbeddingPermits(Mocks mocks, int expectedPermits) {
    assertThat(mocks.initialSyncQueue.getEmbeddingAvailablePermits()).isPresent();
    // The concurrentEmbeddingSyncs semaphore's permit is released in the initial sync future's
    // async whenComplete clause. It's possible that this executes after calling future.get(), so we
    // wait up to 1 second for the permit to be released.
    try {
      Condition.await()
          .atMost(Duration.ofSeconds(1))
          .until(
              () -> mocks.initialSyncQueue.getEmbeddingAvailablePermits().get() == expectedPermits);
    } catch (Exception e) {
      Assert.fail(
          "Expected "
              + expectedPermits
              + " embedding permits, but got "
              + mocks.initialSyncQueue.getEmbeddingAvailablePermits().get());
    }
  }

  private static class Mocks {

    final InitialSyncMongoClient mongoClient;
    final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;
    @Keep final BlockingQueue<GenerationId> requestQueue;
    @Keep final Map<GenerationId, InitialSyncRequest> queued;
    @Keep final Set<GenerationId> cancelled;
    final Map<GenerationId, InProgressInitialSyncInfo> inProgress;

    final MockInitialSyncManagerFactory mockInitialSyncManagerFactory;

    final InitialSyncQueue initialSyncQueue;
    final TemporaryFolder tempFolder;

    private Mocks(
        Map<String, InitialSyncMongoClient> mongoClients,
        String syncSourceHost,
        IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
        BlockingQueue<GenerationId> requestQueue,
        Map<GenerationId, InitialSyncRequest> queued,
        Set<GenerationId> cancelled,
        Map<GenerationId, InProgressInitialSyncInfo> inProgress,
        MongoDbReplicationConfig replicationConfig,
        InitialSyncConfig initialSyncConfig,
        MockInitialSyncManagerFactory mockInitialSyncManagerFactory,
        MeterRegistry meterRegistry,
        Gate initialSyncGate)
        throws IOException {
      this.mongoClient = mongoClients.get(syncSourceHost);
      this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
      this.requestQueue = requestQueue;
      this.queued = queued;
      this.cancelled = cancelled;
      this.inProgress = inProgress;

      this.mockInitialSyncManagerFactory = mockInitialSyncManagerFactory;
      this.tempFolder = TestUtils.getTempFolder();
      this.tempFolder.create();
      var rootPath = this.tempFolder.newFolder("indexes").toPath();
      this.initialSyncQueue =
          new InitialSyncQueue(
              meterRegistry,
              mongoClients,
              syncSourceHost,
              this.indexingWorkSchedulerFactory,
              requestQueue,
              queued,
              cancelled,
              inProgress,
              replicationConfig,
              initialSyncConfig,
              mockInitialSyncManagerFactory,
              rootPath,
              initialSyncGate);
    }

    static Mocks create(
        Map<GenerationId, CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>>
            resultSuppliers,
        BlockingQueue<GenerationId> blockingQueue,
        MeterRegistry meterRegistry,
        Optional<Integer> numConcurrentInitialSyncs,
        Optional<Integer> maxConcurrentEmbeddingInitialSyncs)
        throws Exception {
      return create(
          resultSuppliers,
          blockingQueue,
          meterRegistry,
          numConcurrentInitialSyncs,
          maxConcurrentEmbeddingInitialSyncs,
          ToggleGate.opened());
    }

    static Mocks create(
        Map<GenerationId, CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>>
            resultSuppliers,
        BlockingQueue<GenerationId> blockingQueue,
        MeterRegistry meterRegistry,
        Optional<Integer> numConcurrentInitialSyncs,
        Optional<Integer> maxConcurrentEmbeddingInitialSyncs,
        Gate initialSyncGate)
        throws Exception {
      InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
      when(mongoClient.resolveAndUpdateCollectionName(any()))
          .thenReturn(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME);
      return create(
          Map.of("test", mongoClient),
          "test",
          resultSuppliers,
          blockingQueue,
          meterRegistry,
          numConcurrentInitialSyncs,
          maxConcurrentEmbeddingInitialSyncs,
          initialSyncGate,
          new InitialSyncConfig(false));
    }

    static Mocks create(
        Map<String, InitialSyncMongoClient> mongoClients,
        String syncSourceHost,
        Map<GenerationId, CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>>
            resultSuppliers,
        BlockingQueue<GenerationId> blockingQueue,
        MeterRegistry meterRegistry,
        Optional<Integer> numConcurrentInitialSyncs,
        Optional<Integer> maxConcurrentEmbeddingInitialSyncs,
        Gate initialSyncGate,
        InitialSyncConfig initialSyncConfig)
        throws Exception {

      return new Mocks(
          mongoClients,
          syncSourceHost,
          spy(
              IndexingWorkSchedulerFactory.create(
                  2, mock(Supplier.class), new SimpleMeterRegistry())),
          blockingQueue,
          new HashMap<>(),
          new HashSet<>(),
          new HashMap<>(),
          MongoDbReplicationConfig.create(
              numConcurrentInitialSyncs,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(false),
              Optional.empty(),
              Optional.empty(),
              maxConcurrentEmbeddingInitialSyncs,
              Optional.empty(),
              List.of(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty()),
          initialSyncConfig,
          new MockInitialSyncManagerFactory(resultSuppliers),
          meterRegistry,
          initialSyncGate);
    }

    static Mocks create(
        Map<GenerationId, CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>>
            resultSuppliers,
        BlockingQueue<GenerationId> blockingQueue,
        Optional<Integer> numConcurrentInitialSyncs,
        Optional<Integer> maxConcurrentEmbeddingInitialSyncs)
        throws Exception {
      return Mocks.create(
          resultSuppliers,
          blockingQueue,
          new SimpleMeterRegistry(),
          numConcurrentInitialSyncs,
          maxConcurrentEmbeddingInitialSyncs);
    }

    static Mocks create(
        Map<GenerationId, CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>>
            resultSuppliers,
        BlockingQueue<GenerationId> blockingQueue,
        Optional<Integer> numConcurrentInitialSyncs,
        Optional<Integer> maxConcurrentEmbeddingInitialSyncs,
        Gate initialSyncGate)
        throws Exception {
      return Mocks.create(
          resultSuppliers,
          blockingQueue,
          new SimpleMeterRegistry(),
          numConcurrentInitialSyncs,
          maxConcurrentEmbeddingInitialSyncs,
          initialSyncGate);
    }

    static Mocks create(
        Map<GenerationId, CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>>
            resultSuppliers,
        BlockingQueue<GenerationId> blockingQueue)
        throws Exception {
      return Mocks.create(resultSuppliers, blockingQueue, Optional.of(2), Optional.empty());
    }

    static Mocks create(
        Map<GenerationId, CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>>
            resultSuppliers,
        BlockingQueue<GenerationId> blockingQueue,
        Gate initialSyncGate)
        throws Exception {
      return Mocks.create(
          resultSuppliers, blockingQueue, Optional.of(2), Optional.empty(), initialSyncGate);
    }

    static Mocks create(
        Map<GenerationId, CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>>
            resultSuppliers,
        Gate initialSyncGate)
        throws Exception {
      return Mocks.create(resultSuppliers, ControlledBlockingQueue.ready(), initialSyncGate);
    }

    static Mocks create(
        Map<GenerationId, CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>>
            resultSuppliers)
        throws Exception {
      return create(resultSuppliers, ControlledBlockingQueue.ready(), ToggleGate.opened());
    }

    static Mocks create(
        Map<GenerationId, CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>>
            resultSuppliers,
        InitialSyncConfig initialSyncConfig,
        String syncSourceHost,
        Map<String, InitialSyncMongoClient> mongoClients)
        throws Exception {
      return Mocks.create(
          mongoClients,
          syncSourceHost,
          resultSuppliers,
          ControlledBlockingQueue.ready(),
          new SimpleMeterRegistry(),
          Optional.of(2),
          Optional.empty(),
          ToggleGate.opened(),
          initialSyncConfig);
    }

    private static class MockInitialSyncManagerFactory implements InitialSyncManagerFactory {

      private final Map<GenerationId, CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>>
          resultSuppliers;
      private final Map<GenerationId, CountDownLatch> latches;
      private final Map<GenerationId, InitialSyncManager> managers;
      private InitialSyncContext context;

      private final Map<GenerationId, InitialSyncMongoClient> mongoClients;

      MockInitialSyncManagerFactory(
          Map<GenerationId, CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>>
              resultSuppliers) {
        this.resultSuppliers = resultSuppliers;

        this.latches =
            resultSuppliers.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new CountDownLatch(1)));

        this.managers =
            resultSuppliers.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                          InitialSyncManager manager = createInitialSyncManager();
                          CountDownLatch latch = this.latches.get(entry.getKey());
                          CheckedSupplier<ChangeStreamResumeInfo, InitialSyncException>
                              resultSupplier = this.resultSuppliers.get(entry.getKey());

                          try {
                            when(manager.sync())
                                .then(
                                    ignored -> {
                                      try {
                                        latch.await();
                                      } catch (InterruptedException e) {
                                        throw InitialSyncException.createShutDown();
                                      }
                                      return resultSupplier.get();
                                    });
                          } catch (InitialSyncException e) {
                            throw new RuntimeException(e);
                          }
                          return manager;
                        }));
        this.mongoClients = new HashMap<>();
      }

      private InitialSyncManager createInitialSyncManager() {
        return mock(BufferlessInitialSyncManager.class);
      }

      /**
       * Creates an InitialSyncManager that waits until completeSync() has been called on its index,
       * then returns the value produced by the resultSupplier at its index.
       */
      @Override
      public InitialSyncManager create(
          InitialSyncContext context,
          InitialSyncMongoClient mongoClient,
          MongoNamespace namespace,
          Optional<InitialSyncResumeInfo> resumeInfo) {
        this.context = context;
        this.mongoClients.put(context.getGenerationId(), mongoClient);
        return this.managers.get(context.getGenerationId());
      }

      void completeSync(GenerationId generationId) {
        this.latches.get(generationId).countDown();
      }

      InitialSyncManager getManager(GenerationId generationId) {
        return this.managers.get(generationId);
      }

      InitialSyncMongoClient getClient(GenerationId generationId) {
        return this.mongoClients.get(generationId);
      }
    }
  }
}
