package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DATABASE_NAME;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME;
import static com.xgen.testing.mongot.mock.index.VectorIndex.mockAutoEmbeddingVectorDefinition;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer.mockDocumentIndexer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.util.Condition;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class SyncChangeStreamManagerTest {

  public static final IndexMetricsUpdater IGNORE_METRICS =
      IndexMetricsUpdaterBuilder.builder()
          .metricsFactory(SearchIndex.mockMetricsFactory())
          .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
          .build();
  private static final ChangeStreamResumeInfo INITIAL_RESUME_INFO =
      ChangeStreamResumeInfo.create(
          new MongoNamespace(MOCK_INDEX_DATABASE_NAME, MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME),
          new BsonDocument("_data", new BsonBinary("initial resume token".getBytes())));

  @Test
  public void testAssertionErrorOnUnexpectedChangeStreamEvent() throws Exception {
    var indexingWorkSchedulerFactory = mock(IndexingWorkSchedulerFactory.class);
    var indexingWorkScheduler = mock(IndexingWorkScheduler.class);
    var changeStreamMongoClient = mock(ChangeStreamMongoClient.class);
    var mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    var changeStreamIndexManagerFactory = mock(ChangeStreamIndexManagerFactory.class);

    when(changeStreamMongoClient.getNext())
        .thenReturn(
            new ChangeStreamBatch(
                ChangeStreamUtils.toRawBsonDocuments(
                    List.of(ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION))),
                ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                new BsonTimestamp()));

    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(changeStreamMongoClient);

    when(indexingWorkSchedulerFactory.getIndexingWorkScheduler(eq(MOCK_INDEX_DEFINITION)))
        .thenReturn(indexingWorkScheduler);
    when(indexingWorkScheduler.cancel(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();
    DecodingWorkScheduler decodingScheduler = DecodingWorkScheduler.create(4, simpleMeterRegistry);

    when(changeStreamIndexManagerFactory.create(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              DecodingExecutorChangeStreamIndexManager manager =
                  spy(
                      DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
                          invocation.getArgument(0),
                          invocation.getArgument(1),
                          invocation.getArgument(2),
                          invocation.getArgument(3),
                          invocation.getArgument(4),
                          invocation.getArgument(5),
                          invocation.getArgument(6),
                          invocation.getArgument(7),
                          decodingScheduler));

              when(manager.getIndexableChangeStreamEvents(any(), anyBoolean()))
                  .thenThrow(new AssertionError("unexpected!"));

              return manager;
            });

    ChangeStreamManager changeStreamManager =
        ChangeStreamManager.createSync(
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            mongoClientFactory,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory,
            SteadyStateReplicationConfig.builder()
                .setChangeStreamQueryMaxTimeMs(1)
                .setNumConcurrentChangeStreams(1)
                .setChangeStreamCursorMaxTimeSec(1)
                .setMaxInFlightEmbeddingGetMores(1)
                .build(),
            changeStreamIndexManagerFactory);

    var lifecycleFuture =
        changeStreamManager.add(
            MOCK_INDEX_GENERATION_ID,
            mockDocumentIndexer(),
            MOCK_INDEX_DEFINITION,
            INITIAL_RESUME_INFO,
            (ignoreResumeInfo) -> {},
            IGNORE_METRICS,
            false);

    ExecutionException executionException =
        Assert.assertThrows(
            ExecutionException.class, () -> lifecycleFuture.get(30, TimeUnit.SECONDS));
    assertThat(executionException.getCause()).isInstanceOf(AssertionError.class);

    Assert.assertEquals(1, decodingScheduler.getSchedulerQueue().getFailedIndexingAttemptsSize());
    decodingScheduler.shutdown().get();
    // ReplicationIndexManager would stop the index generation once the lifecycle exception is
    // surfaced. The attempt id would be removed from the WeakHashMap once it is no longer
    // referenced, after changeStreamIndexManager is removed. Since WeakHashMap cleanup relies
    // on garbage collection, which might not happen immediately, we retry the cleanup check
    // over the course of one minute.
    changeStreamManager.stop(MOCK_INDEX_GENERATION_ID).get();
    ensureAttemptIdInvalidatesWithinOneMinute(
        indexingWorkScheduler,
        ignored -> decodingScheduler.getSchedulerQueue().getFailedIndexingAttemptsSize() == 0);
  }

  public void ensureAttemptIdInvalidatesWithinOneMinute(
      IndexingWorkScheduler indexingWorkScheduler, Predicate<Void> condition)
      throws InterruptedException {
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTime < 60000) {
      // Retry resetting and invoking garbage collection multiple times to mitigate flakiness.
      Thread.sleep(500);
      Mockito.reset(indexingWorkScheduler);
      System.gc();

      if (condition.test(null)) {
        return;
      }
    }
    Assert.fail("Attempt id is not removed from the map within 1 minute.");
  }

  @Test
  public void testLifecycleExceptionAndMongoClientClosure() throws Exception {
    var indexingWorkSchedulerFactory = mock(IndexingWorkSchedulerFactory.class);
    var indexingWorkScheduler = mock(IndexingWorkScheduler.class);
    var changeStreamMongoClient = mock(ChangeStreamMongoClient.class);
    var mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    var changeStreamIndexManagerFactory = mock(ChangeStreamIndexManagerFactory.class);

    when(changeStreamMongoClient.getNext())
        .thenReturn(
            new ChangeStreamBatch(
                ChangeStreamUtils.toRawBsonDocuments(
                    List.of(ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION))),
                ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                new BsonTimestamp()));

    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(changeStreamMongoClient);

    when(indexingWorkSchedulerFactory.getIndexingWorkScheduler(eq(MOCK_INDEX_DEFINITION)))
        .thenReturn(indexingWorkScheduler);
    when(indexingWorkScheduler.cancel(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();
    DecodingWorkScheduler decodingScheduler = DecodingWorkScheduler.create(4, simpleMeterRegistry);

    var capturedDecodingExecutorChangeStreamIndexManagerRef =
        new AtomicReference<DecodingExecutorChangeStreamIndexManager>();
    when(changeStreamIndexManagerFactory.create(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              DecodingExecutorChangeStreamIndexManager manager =
                  spy(
                      DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
                          invocation.getArgument(0),
                          invocation.getArgument(1),
                          invocation.getArgument(2),
                          invocation.getArgument(3),
                          invocation.getArgument(4),
                          invocation.getArgument(5),
                          invocation.getArgument(6),
                          invocation.getArgument(7),
                          decodingScheduler));

              when(manager.getIndexableChangeStreamEvents(any(), anyBoolean()))
                  .thenThrow(new AssertionError("unexpected!"));
              capturedDecodingExecutorChangeStreamIndexManagerRef.set(manager);
              return manager;
            });

    ChangeStreamManager changeStreamManager =
        ChangeStreamManager.createSync(
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            mongoClientFactory,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory,
            SteadyStateReplicationConfig.builder()
                .setChangeStreamQueryMaxTimeMs(1)
                .setNumConcurrentChangeStreams(1)
                .setChangeStreamCursorMaxTimeSec(1)
                .setMaxInFlightEmbeddingGetMores(1)
                .build(),
            changeStreamIndexManagerFactory);

    var lifecycleFuture =
        changeStreamManager.add(
            MOCK_INDEX_GENERATION_ID,
            mockDocumentIndexer(),
            MOCK_INDEX_DEFINITION,
            INITIAL_RESUME_INFO,
            (ignoreResumeInfo) -> {},
            IGNORE_METRICS,
            false);

    ExecutionException executionException =
        Assert.assertThrows(
            ExecutionException.class, () -> lifecycleFuture.get(30, TimeUnit.SECONDS));
    assertThat(executionException.getCause()).isInstanceOf(AssertionError.class);

    DecodingExecutorChangeStreamIndexManager decodingExecutorChangeStreamIndexManager =
        capturedDecodingExecutorChangeStreamIndexManagerRef.get();
    Assert.assertNotNull(decodingExecutorChangeStreamIndexManager);
    InOrder inOrder =
        Mockito.inOrder(decodingExecutorChangeStreamIndexManager, changeStreamMongoClient);
    // Verify lifecycle exception is called only once and
    // MongoClient is closed after lifecycle failure is set
    inOrder.verify(decodingExecutorChangeStreamIndexManager, times(1)).setLifeCycleException(any());
    inOrder.verify(changeStreamMongoClient, timeout(1000)).close();
    decodingScheduler.shutdown();
  }

  @Test
  public void testRemoveMatchCollectionUuid() throws Exception {
    var mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    var changeStreamIndexManagerFactory = mock(ChangeStreamIndexManagerFactory.class);

    when(changeStreamIndexManagerFactory.create(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                spy(
                    DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
                        invocation.getArgument(0),
                        invocation.getArgument(1),
                        invocation.getArgument(2),
                        invocation.getArgument(3),
                        invocation.getArgument(4),
                        invocation.getArgument(5),
                        invocation.getArgument(6),
                        invocation.getArgument(7),
                        mock(DecodingWorkScheduler.class))));

    ChangeStreamManager changeStreamManager =
        ChangeStreamManager.createSync(
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            mongoClientFactory,
            mock(ChangeStreamModeSelector.class),
            mock(IndexingWorkSchedulerFactory.class),
            SteadyStateReplicationConfig.builder()
                .setChangeStreamQueryMaxTimeMs(1)
                .setNumConcurrentChangeStreams(1)
                .setChangeStreamCursorMaxTimeSec(1)
                .setMaxInFlightEmbeddingGetMores(1)
                .build(),
            changeStreamIndexManagerFactory);

    // Add an index with removeMatchCollectionUuid set to true. Verify that this is properly passed
    // to the ChangeStreamMongoClientFactory.
    changeStreamManager.add(
        MOCK_INDEX_GENERATION_ID,
        mockDocumentIndexer(),
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        (ignoreResumeInfo) -> {},
        IGNORE_METRICS,
        true);

    verify(mongoClientFactory, timeout(500))
        .resumeTimedModeAwareChangeStream(any(), any(), any(), eq(true));
  }

  @Test
  public void testConcurrentEmbeddingGetMoresSemaphoreReleased_batchSuccess() throws Exception {
    testConcurrentEmbeddingGetMoresSemaphore(GetMoreScenario.SUCCESS);
  }

  @Test
  public void testConcurrentEmbeddingGetMoresSemaphoreReleased_batchException() throws Exception {
    testConcurrentEmbeddingGetMoresSemaphore(GetMoreScenario.INDEXING_EXCEPTION);
  }

  @Test
  public void testConcurrentEmbeddingGetMoresSemaphoreReleased_shutdown() throws Exception {
    testConcurrentEmbeddingGetMoresSemaphore(GetMoreScenario.SHUTDOWN);
  }

  // Asserts that the concurrentEmbeddingGetMores semaphore is released when the getMore batch
  // completes successfully, throws an exception, or a shutdown is triggered.
  private void testConcurrentEmbeddingGetMoresSemaphore(GetMoreScenario scenario) throws Exception {
    VectorIndexDefinition embeddingDefinition = mockAutoEmbeddingVectorDefinition(new ObjectId());
    IndexDefinitionGeneration embeddingGeneration =
        mockIndexGeneration(embeddingDefinition).getDefinitionGeneration();

    var indexingWorkSchedulerFactory = mock(IndexingWorkSchedulerFactory.class);
    var indexingWorkScheduler = mock(IndexingWorkScheduler.class);
    var changeStreamMongoClient = mock(ChangeStreamMongoClient.class);
    var mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    var changeStreamIndexManagerFactory = mock(ChangeStreamIndexManagerFactory.class);

    ChangeStreamBatch batch =
        new ChangeStreamBatch(
            ChangeStreamUtils.toRawBsonDocuments(
                List.of(ChangeStreamUtils.insertEvent(0, embeddingDefinition))),
            ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
            new BsonTimestamp());

    CountDownLatch getMoreStarted = new CountDownLatch(1);
    CountDownLatch getMoreDone = new CountDownLatch(1);
    when(changeStreamMongoClient.getNext())
        .thenAnswer(
            ignored -> {
              getMoreStarted.countDown();
              getMoreDone.await();
              return batch;
            });

    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(changeStreamMongoClient);

    DecodingWorkScheduler decodingScheduler =
        DecodingWorkScheduler.create(4, new SimpleMeterRegistry());
    when(changeStreamIndexManagerFactory.create(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                spy(
                    DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
                        invocation.getArgument(0),
                        invocation.getArgument(1),
                        invocation.getArgument(2),
                        invocation.getArgument(3),
                        invocation.getArgument(4),
                        invocation.getArgument(5),
                        invocation.getArgument(6),
                        invocation.getArgument(7),
                        decodingScheduler)));

    ChangeStreamManager changeStreamManager =
        ChangeStreamManager.createSync(
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            mongoClientFactory,
            mock(ChangeStreamModeSelector.class),
            mock(IndexingWorkSchedulerFactory.class),
            SteadyStateReplicationConfig.builder()
                .setChangeStreamQueryMaxTimeMs(1)
                .setNumConcurrentChangeStreams(2)
                .setChangeStreamCursorMaxTimeSec(1)
                .setMaxInFlightEmbeddingGetMores(1)
                .setReplicationType(CommonReplicationConfig.Type.AUTO_EMBEDDING)
                .build(),
            changeStreamIndexManagerFactory);

    CompletableFuture<Void> indexingFuture = new CompletableFuture<>();
    when(indexingWorkSchedulerFactory.getIndexingWorkScheduler(eq(embeddingDefinition)))
        .thenReturn(indexingWorkScheduler);
    when(indexingWorkScheduler.schedule(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(indexingFuture);

    changeStreamManager.add(
        embeddingGeneration.getGenerationId(),
        mockDocumentIndexer(),
        embeddingDefinition,
        INITIAL_RESUME_INFO,
        (ignoreResumeInfo) -> {},
        IGNORE_METRICS,
        false);

    getMoreStarted.await();
    assertNumberOfEmbeddingPermits(changeStreamManager, 0);
    getMoreDone.countDown();
    switch (scenario) {
      case SUCCESS:
        indexingFuture.complete(null);
        break;
      case INDEXING_EXCEPTION:
        indexingFuture.completeExceptionally(
            SteadyStateException.createTransient(new Throwable("Indexing failed")));
        break;
      case SHUTDOWN:
        indexingFuture.completeExceptionally(
            SteadyStateException.createTransient(new Throwable("Shutdown requested")));
        changeStreamManager.shutdown();
        break;
    }
    assertNumberOfEmbeddingPermits(changeStreamManager, 1);
  }

  private static void assertNumberOfEmbeddingPermits(
      ChangeStreamManager manager, int expectedPermits) {
    assertThat(manager.getEmbeddingAvailablePermits()).isPresent();
    // The concurrentEmbeddingGetMores semaphore's permit is released in "finally" block of the call
    // to doGetMore(). It's possible that this executes after the indexingFuture completes, so we
    // wait up to 1 second for the permit to be released.
    try {
      Condition.await()
          .atMost(Duration.ofSeconds(1))
          .until(() -> manager.getEmbeddingAvailablePermits().get() == expectedPermits);
    } catch (Exception e) {
      Assert.fail(
          "Expected "
              + expectedPermits
              + " embedding permits, but got "
              + manager.getEmbeddingAvailablePermits().get());
    }
  }

  private enum GetMoreScenario {
    SUCCESS,
    INDEXING_EXCEPTION,
    SHUTDOWN
  }

  // ---------------------------------------------------------------------------
  // Activity observer wiring (the static-flag-gated observer lifecycle).
  // ---------------------------------------------------------------------------

  @Test
  public void activityObserverCreatedAndIndexRegisteredOnAdd() throws Exception {
    RecordingObserverFactory observerFactory = new RecordingObserverFactory();
    CountDownLatch getMoreDone = new CountDownLatch(1);
    ChangeStreamManager manager = managerWithObserverFactory(observerFactory, getMoreDone);

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        mockDocumentIndexer(),
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        (ignoreResumeInfo) -> {},
        IGNORE_METRICS,
        false);

    // The observer for the index's database is created once and the index registers with it.
    assertThat(observerFactory.createdDatabases).containsExactly(MOCK_INDEX_DATABASE_NAME);
    verify(observerFactory.observerFor(MOCK_INDEX_DATABASE_NAME))
        .register(eq(MOCK_INDEX_GENERATION_ID), any(), any());

    getMoreDone.countDown();
    manager.shutdown();
  }

  @Test
  public void activityObserverClosedWhenItsLastIndexIsRemoved() throws Exception {
    RecordingObserverFactory observerFactory = new RecordingObserverFactory();
    CountDownLatch getMoreDone = new CountDownLatch(1);
    ChangeStreamManager manager = managerWithObserverFactory(observerFactory, getMoreDone);

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        mockDocumentIndexer(),
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        (ignoreResumeInfo) -> {},
        IGNORE_METRICS,
        false);
    ChangeStreamActivityObserver observer = observerFactory.observerFor(MOCK_INDEX_DATABASE_NAME);
    // deregister returns true => that was the database's last managed index.
    when(observer.deregister(MOCK_INDEX_GENERATION_ID)).thenReturn(true);

    manager.stop(MOCK_INDEX_GENERATION_ID);

    verify(observer).deregister(MOCK_INDEX_GENERATION_ID);
    verify(observer).close();

    getMoreDone.countDown();
    manager.shutdown();
  }

  @Test
  public void activityObserverNotClosedWhileIndexesRemain() throws Exception {
    RecordingObserverFactory observerFactory = new RecordingObserverFactory();
    CountDownLatch getMoreDone = new CountDownLatch(1);
    ChangeStreamManager manager = managerWithObserverFactory(observerFactory, getMoreDone);

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        mockDocumentIndexer(),
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        (ignoreResumeInfo) -> {},
        IGNORE_METRICS,
        false);
    ChangeStreamActivityObserver observer = observerFactory.observerFor(MOCK_INDEX_DATABASE_NAME);
    // deregister returns false => the database still has other managed indexes.
    when(observer.deregister(MOCK_INDEX_GENERATION_ID)).thenReturn(false);

    manager.stop(MOCK_INDEX_GENERATION_ID);

    verify(observer).deregister(MOCK_INDEX_GENERATION_ID);
    verify(observer, never()).close();

    getMoreDone.countDown();
    manager.shutdown();
  }

  @Test
  public void allActivityObserversClosedOnShutdown() throws Exception {
    RecordingObserverFactory observerFactory = new RecordingObserverFactory();
    CountDownLatch getMoreDone = new CountDownLatch(1);
    ChangeStreamManager manager = managerWithObserverFactory(observerFactory, getMoreDone);

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        mockDocumentIndexer(),
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        (ignoreResumeInfo) -> {},
        IGNORE_METRICS,
        false);
    ChangeStreamActivityObserver observer = observerFactory.observerFor(MOCK_INDEX_DATABASE_NAME);

    getMoreDone.countDown();
    manager.shutdown();

    verify(observer).close();
  }

  // Builds a manager wired with the supplied observer factory. The change-stream client's getNext
  // blocks on getMoreDone so the dispatcher does no real work until the test releases it at
  // teardown; the observer registration under test happens synchronously in
  // add()/stop()/shutdown().
  private ChangeStreamManager managerWithObserverFactory(
      ChangeStreamManager.ActivityObserverFactory observerFactory, CountDownLatch getMoreDone)
      throws Exception {
    var changeStreamMongoClient = mock(ChangeStreamMongoClient.class);
    var mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    var changeStreamIndexManagerFactory = mock(ChangeStreamIndexManagerFactory.class);

    when(changeStreamMongoClient.getNext())
        .thenAnswer(
            ignored -> {
              getMoreDone.await();
              return new ChangeStreamBatch(
                  ChangeStreamUtils.toRawBsonDocuments(
                      List.of(ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION))),
                  ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                  new BsonTimestamp());
            });
    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(changeStreamMongoClient);

    DecodingWorkScheduler decodingScheduler =
        DecodingWorkScheduler.create(4, new SimpleMeterRegistry());
    when(changeStreamIndexManagerFactory.create(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                spy(
                    DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
                        invocation.getArgument(0),
                        invocation.getArgument(1),
                        invocation.getArgument(2),
                        invocation.getArgument(3),
                        invocation.getArgument(4),
                        invocation.getArgument(5),
                        invocation.getArgument(6),
                        invocation.getArgument(7),
                        decodingScheduler)));

    return ChangeStreamManager.createSync(
        MeterAndFtdcRegistry.createWithSimpleRegistries(),
        mongoClientFactory,
        mock(ChangeStreamModeSelector.class),
        mock(IndexingWorkSchedulerFactory.class),
        SteadyStateReplicationConfig.builder()
            .setChangeStreamQueryMaxTimeMs(1)
            .setNumConcurrentChangeStreams(1)
            .setChangeStreamCursorMaxTimeSec(1)
            .setMaxInFlightEmbeddingGetMores(1)
            .build(),
        changeStreamIndexManagerFactory,
        Optional.of(observerFactory));
  }

  // Records each database an observer was created for and hands back a mock observer per
  // database so the test can verify register/deregister/close on it.
  private static final class RecordingObserverFactory
      implements ChangeStreamManager.ActivityObserverFactory {
    private final Map<String, ChangeStreamActivityObserver> observers = new HashMap<>();
    private final List<String> createdDatabases = new ArrayList<>();

    @Override
    public ChangeStreamActivityObserver create(String databaseName) {
      this.createdDatabases.add(databaseName);
      return this.observers.computeIfAbsent(
          databaseName, ignored -> mock(ChangeStreamActivityObserver.class));
    }

    ChangeStreamActivityObserver observerFor(String databaseName) {
      return this.observers.get(databaseName);
    }
  }
}
