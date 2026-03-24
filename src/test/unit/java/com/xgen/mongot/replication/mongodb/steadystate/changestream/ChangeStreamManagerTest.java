package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DATABASE_NAME;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockDefinitionBuilder;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockSearchDefinition;
import static com.xgen.testing.mongot.mock.index.VectorIndex.mockAutoEmbeddingVectorDefinition;
import static com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils.toRawBsonDocuments;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.xgen.mongot.embedding.providers.EmbeddingServiceRegistry;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException.Type;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.stubbing.Answer;

@RunWith(Parameterized.class)
public class ChangeStreamManagerTest {

  private static final ChangeStreamResumeInfo INITIAL_RESUME_INFO =
      ChangeStreamResumeInfo.create(
          new MongoNamespace(MOCK_INDEX_DATABASE_NAME, MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME),
          new BsonDocument("_data", new BsonBinary("initial resume token".getBytes())));

  public static final IndexMetricsUpdater IGNORE_METRICS =
      IndexMetricsUpdaterBuilder.builder()
          .metricsFactory(SearchIndex.mockMetricsFactory())
          .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
          .build();

  @Parameter() public TestContext context;

  @Parameters(name = "{0}")
  public static List<TestContext> params() {
    return List.of(
        new TestContext("sync-client", ChangeStreamManagerTestSyncHelpers::getSyncParameters));
  }

  @Test
  public void testAddBeginsTailingChangeStream() throws Exception {
    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.SINGLE_EVENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory());

    ChangeStreamManager manager = instance.changeStreamManager;

    CountDownLatch indexedDocuments = new CountDownLatch(5);

    DocumentIndexer indexer = indexer();

    Answer<Long> countDocument =
        invocation -> {
          indexedDocuments.countDown();
          return null;
        };
    doAnswer(countDocument).when(indexer).indexDocumentEvent(any());

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        indexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    if (!indexedDocuments.await(5, TimeUnit.SECONDS)) {
      Assert.fail();
    }
  }

  @Test
  public void testAddRegistersGenerationInModeSelector() throws Exception {
    ChangeStreamModeSelector modeSelector = mock(ChangeStreamModeSelector.class);

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.SINGLE_EVENT,
            modeSelector,
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory());

    ChangeStreamManager manager = instance.changeStreamManager;

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        indexer(),
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    verify(modeSelector)
        .register(
            MOCK_INDEX_GENERATION_ID, MOCK_INDEX_DEFINITION, INITIAL_RESUME_INFO.getNamespace());
  }

  @Test
  public void testHandleCollectionRename() throws Exception {
    ChangeStreamModeSelector modeSelector = mock(ChangeStreamModeSelector.class);

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.SINGLE_EVENT,
            modeSelector,
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory());

    ChangeStreamManager manager = instance.changeStreamManager;
    IndexDefinition indexDefinition =
        mockDefinitionBuilder().lastObservedCollectionName("oldCollectionName").build();

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        indexer(),
        indexDefinition,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);
    Assert.assertEquals(
        INITIAL_RESUME_INFO.getNamespace().getCollectionName(),
        indexDefinition.getLastObservedCollectionName());
  }

  @Test
  public void testStopShutsDownIndexManager() throws Exception {
    CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
    ChangeStreamIndexManagerFactory indexManagerFactory =
        mock(ChangeStreamIndexManagerFactory.class);

    when(indexManagerFactory.create(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              ChangeStreamIndexManager manager =
                  spy(
                      DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
                          MOCK_INDEX_DEFINITION,
                          invocation.getArgument(1),
                          invocation.getArgument(2),
                          invocation.getArgument(3),
                          invocation.getArgument(4),
                          invocation.getArgument(5),
                          invocation.getArgument(6),
                          invocation.getArgument(7),
                          mock(DecodingWorkScheduler.class)));
              when(manager.shutdown()).thenReturn(shutdownFuture);
              return manager;
            });

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.SINGLE_EVENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            indexManagerFactory);

    ChangeStreamManager manager = instance.changeStreamManager;

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        indexer(),
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    CompletableFuture<Void> stopFuture = manager.stop(MOCK_INDEX_GENERATION_ID);
    assertFalse(shutdownFuture.isDone());
    assertFalse(stopFuture.isDone());

    shutdownFuture.complete(null);
    Assert.assertTrue(stopFuture.isDone());
  }

  @Test
  public void testStopRemovesGenerationFromModeSelector() throws Exception {
    ChangeStreamModeSelector modeSelector = mock(ChangeStreamModeSelector.class);
    CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
    ChangeStreamIndexManagerFactory indexManagerFactory =
        mock(ChangeStreamIndexManagerFactory.class);

    when(indexManagerFactory.create(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              ChangeStreamIndexManager manager =
                  spy(
                      DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
                          MOCK_INDEX_DEFINITION,
                          invocation.getArgument(1),
                          invocation.getArgument(2),
                          invocation.getArgument(3),
                          invocation.getArgument(4),
                          invocation.getArgument(5),
                          invocation.getArgument(6),
                          invocation.getArgument(7),
                          mock(DecodingWorkScheduler.class)));
              when(manager.shutdown()).thenReturn(shutdownFuture);
              return manager;
            });

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.SINGLE_EVENT,
            modeSelector,
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            indexManagerFactory);

    ChangeStreamManager manager = instance.changeStreamManager;

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        indexer(),
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    manager.stop(MOCK_INDEX_GENERATION_ID);
    verify(modeSelector).remove(MOCK_INDEX_GENERATION_ID);
  }

  @Test
  public void testCannotAddSameIndexTwiceWithoutStop() throws Exception {
    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.SINGLE_EVENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory());

    ChangeStreamManager manager = instance.changeStreamManager;

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        indexer(),
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    Assert.assertThrows(
        IllegalStateException.class,
        () ->
            manager.add(
                MOCK_INDEX_GENERATION_ID,
                indexer(),
                MOCK_INDEX_DEFINITION,
                INITIAL_RESUME_INFO,
                ignoreResumeInfo(),
                IGNORE_METRICS,
                false));
  }

  @Test
  public void testCanAddSameIndexTwiceAfterStop() throws Exception {
    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.SINGLE_EVENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory());

    ChangeStreamManager manager = instance.changeStreamManager;

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        indexer(),
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    manager.stop(MOCK_INDEX_GENERATION_ID);

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        indexer(),
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);
  }

  @Test
  public void testCanAddTwoIndexesWithSameIndexId() throws Exception {
    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.SINGLE_EVENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(2)
                .setMaxInFlightEmbeddingGetMores(2),
            this.context.getIndexManagerFactory());

    ChangeStreamManager manager = instance.changeStreamManager;

    CompletableFuture<Void> first =
        manager.add(
            MOCK_INDEX_GENERATION_ID,
            indexer(),
            MOCK_INDEX_DEFINITION,
            INITIAL_RESUME_INFO,
            ignoreResumeInfo(),
            IGNORE_METRICS,
            false);

    GenerationId otherSwapId = GenerationIdBuilder.incrementUser(MOCK_INDEX_GENERATION_ID);
    CompletableFuture<Void> second =
        manager.add(
            otherSwapId,
            indexer(),
            MOCK_INDEX_DEFINITION,
            INITIAL_RESUME_INFO,
            ignoreResumeInfo(),
            IGNORE_METRICS,
            false);

    // Make sure the manager keeps track of both by canceling one and not the other
    assertFalse(first.isDone());
    assertFalse(second.isDone());
    manager.stop(MOCK_INDEX_GENERATION_ID).get();
    Assert.assertTrue(first.isDone());
    assertFalse(second.isDone());
  }

  @Test
  public void testFutureFailsOnMongoException() throws Exception {
    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.EXCEPTION,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory());

    ChangeStreamManager manager = instance.changeStreamManager;

    CompletableFuture<Void> future =
        manager.add(
            MOCK_INDEX_GENERATION_ID,
            indexer(),
            MOCK_INDEX_DEFINITION,
            INITIAL_RESUME_INFO,
            ignoreResumeInfo(),
            IGNORE_METRICS,
            false);

    ExecutionException executionException =
        Assert.assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(executionException.getCause()).isInstanceOf(SteadyStateException.class);

    var expectedException = (SteadyStateException) executionException.getCause();
    Assert.assertTrue(expectedException.isTransient());
    assertThat(expectedException.getCause()).isInstanceOf(MongoException.class);
  }

  @Test
  public void testLimitsExceededPropagatesSteadyStateException() throws Exception {
    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.SINGLE_EVENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory());

    ChangeStreamManager manager = instance.changeStreamManager;

    DocumentIndexer indexer =
        com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
            .mockFieldLimitsExceeded();
    CompletableFuture<Void> future =
        manager.add(
            MOCK_INDEX_GENERATION_ID,
            indexer,
            MOCK_INDEX_DEFINITION,
            INITIAL_RESUME_INFO,
            ignoreResumeInfo(),
            IGNORE_METRICS,
            false);

    ExecutionException executionException =
        Assert.assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(executionException.getCause()).isInstanceOf(SteadyStateException.class);
    Assert.assertSame(
        SteadyStateException.Type.FIELD_EXCEEDED,
        ((SteadyStateException) executionException.getCause()).getType());
  }

  @Test
  public void testInvalidateEventPropagatesSteadyStateException() throws Exception {
    List<ChangeStreamDocument<RawBsonDocument>> invalidateEvent =
        List.of(
            ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
            ChangeStreamUtils.invalidateEvent(0));

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.MULTIPLE_EVENTS,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory(),
            invalidateEvent);

    ChangeStreamManager manager = instance.changeStreamManager;

    CompletableFuture<Void> future =
        manager.add(
            MOCK_INDEX_GENERATION_ID,
            indexer(),
            MOCK_INDEX_DEFINITION,
            INITIAL_RESUME_INFO,
            ignoreResumeInfo(),
            IGNORE_METRICS,
            false);

    ExecutionException executionException =
        Assert.assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(executionException.getCause()).isInstanceOf(SteadyStateException.class);
    Assert.assertSame(
        SteadyStateException.Type.INVALIDATED,
        ((SteadyStateException) executionException.getCause()).getType());
  }

  @Test
  public void testInvalidateOtherEventSteadyPropagatesStateException() throws Exception {
    List<ChangeStreamDocument<RawBsonDocument>> events =
        List.of(
            ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
            ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION),
            ChangeStreamUtils.otherEvent(2),
            ChangeStreamUtils.insertEvent(3, MOCK_INDEX_DEFINITION));

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.MULTIPLE_EVENTS,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory(),
            events);

    ChangeStreamManager manager = instance.changeStreamManager;

    CompletableFuture<Void> future =
        manager.add(
            MOCK_INDEX_GENERATION_ID,
            indexer(),
            MOCK_INDEX_DEFINITION,
            INITIAL_RESUME_INFO,
            ignoreResumeInfo(),
            IGNORE_METRICS,
            false);

    ExecutionException executionException =
        Assert.assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(executionException.getCause()).isInstanceOf(SteadyStateException.class);
    Assert.assertSame(
        Type.REQUIRES_RESYNC, ((SteadyStateException) executionException.getCause()).getType());
  }

  @Test
  public void testInvalidateRenameEventSteadyPropagatesStateException() throws Exception {
    List<ChangeStreamDocument<RawBsonDocument>> events =
        List.of(
            ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
            ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION),
            ChangeStreamUtils.renameEvent(2),
            ChangeStreamUtils.insertEvent(3, MOCK_INDEX_DEFINITION));

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.MULTIPLE_EVENTS,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory(),
            events);

    ChangeStreamManager manager = instance.changeStreamManager;

    CompletableFuture<Void> future =
        manager.add(
            MOCK_INDEX_GENERATION_ID,
            indexer(),
            MOCK_INDEX_DEFINITION,
            INITIAL_RESUME_INFO,
            ignoreResumeInfo(),
            IGNORE_METRICS,
            false);

    ExecutionException executionException =
        Assert.assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(executionException.getCause()).isInstanceOf(SteadyStateException.class);
    Assert.assertSame(
        Type.REQUIRES_RESYNC, ((SteadyStateException) executionException.getCause()).getType());
  }

  @Test
  public void testRenameEventNoDestinationSteadyPropagatesStateException() throws Exception {
    List<ChangeStreamDocument<RawBsonDocument>> events =
        List.of(
            ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
            ChangeStreamUtils.renameEvent(1, ChangeStreamUtils.OTHER_NAMESPACE, null));

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.MULTIPLE_EVENTS,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory(),
            events);

    ChangeStreamManager manager = instance.changeStreamManager;

    CompletableFuture<Void> future =
        manager.add(
            MOCK_INDEX_GENERATION_ID,
            indexer(),
            MOCK_INDEX_DEFINITION,
            INITIAL_RESUME_INFO,
            ignoreResumeInfo(),
            IGNORE_METRICS,
            false);

    ExecutionException executionException =
        Assert.assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(executionException.getCause()).isInstanceOf(SteadyStateException.class);
    Assert.assertSame(
        Type.NON_INVALIDATING_RESYNC,
        ((SteadyStateException) executionException.getCause()).getType());
  }

  @Test
  public void testCancelStopsScheduling() throws Exception {
    IndexingWorkSchedulerFactory indexingWorkSchedulerFactory = spy(indexingWorkSchedulerFactory());
    IndexingWorkScheduler scheduler = mock(IndexingWorkScheduler.class);
    when(indexingWorkSchedulerFactory.getIndexingWorkScheduler(MOCK_INDEX_DEFINITION))
        .thenReturn(scheduler);

    // use this future to signal that the first batch failed to index in the scheduler
    CompletableFuture<Void> failFuture = new CompletableFuture<>();

    CyclicBarrier firstBatchScheduled = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreDone = new CyclicBarrier(2);

    // return failFuture on the first call, then call the real method each time after
    Answer<CompletableFuture<Void>> answer =
        ignored -> {
          doCallRealMethod()
              .when(scheduler)
              .schedule(any(), any(), any(), any(), any(), any(), any());
          firstBatchScheduled.await();
          return failFuture;
        };
    doAnswer(answer).when(scheduler).schedule(any(), any(), any(), any(), any(), any(), any());

    // when cancel is called, wait until signalled
    CountDownLatch hangAfterCancel = new CountDownLatch(1);
    Answer<CompletableFuture<Void>> cancelAnswer =
        ignored -> {
          hangAfterCancel.await();
          return CompletableFuture.completedFuture(null);
        };
    doAnswer(cancelAnswer).when(scheduler).cancel(any(), any(), any());

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.SINGLE_EVENT_THEN_HANGING_CLIENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory,
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(2)
                .setMaxInFlightEmbeddingGetMores(2),
            this.context.getIndexManagerFactory(),
            secondGetMoreStarted,
            secondGetMoreDone);

    ChangeStreamManager manager = instance.changeStreamManager;

    DocumentIndexer indexer = indexer();

    CompletableFuture<Void> future =
        manager.add(
            MOCK_INDEX_GENERATION_ID,
            indexer,
            MOCK_INDEX_DEFINITION,
            INITIAL_RESUME_INFO,
            ignoreResumeInfo(),
            IGNORE_METRICS,
            false);

    // when we get here, the first batch should be waiting on us to complete the failedFuture
    firstBatchScheduled.await();
    secondGetMoreStarted.await();

    // run this asynchronously, as it will trigger the cancel that we want to block on
    CompletableFuture.runAsync(
        () -> failFuture.completeExceptionally(new Exception("weird error")));

    verify(scheduler, timeout(5000).times(1)).cancel(any(), any(), any());

    secondGetMoreDone.await();

    hangAfterCancel.countDown();

    ExecutionException executionException =
        Assert.assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(executionException.getCause()).isInstanceOf(Exception.class);

    verify(indexer, never()).indexDocumentEvent(any());
  }

  @Test
  public void testIndexingWorkSchedulerFactoryBlocksOnFullCapacity() throws Exception {
    // Create a ChangeStreamMongoClientFactory that hangs until signaled before publishing its
    // result.
    IndexDefinitionGeneration firstDefinition = mockDefinitionGeneration(new ObjectId());
    IndexDefinitionGeneration secondDefinition = mockDefinitionGeneration(new ObjectId());
    IndexDefinitionGeneration thirdDefinition = mockDefinitionGeneration(new ObjectId());
    IndexDefinitionGeneration fourthDefinition = mockDefinitionGeneration(new ObjectId());

    CyclicBarrier firstGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier firstGetMoreDone = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreDone = new CyclicBarrier(2);
    CyclicBarrier thirdGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier thirdGetMoreDone = new CyclicBarrier(2);
    CyclicBarrier fourthGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier fourthGetMoreDone = new CyclicBarrier(2);

    Map<GenerationId, GetMoreBarrier> barriers =
        Map.of(
            firstDefinition.getGenerationId(),
            new GetMoreBarrier(firstGetMoreStarted, firstGetMoreDone),
            secondDefinition.getGenerationId(),
            new GetMoreBarrier(secondGetMoreStarted, secondGetMoreDone),
            thirdDefinition.getGenerationId(),
            new GetMoreBarrier(thirdGetMoreStarted, thirdGetMoreDone),
            fourthDefinition.getGenerationId(),
            new GetMoreBarrier(fourthGetMoreStarted, fourthGetMoreDone));

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.HANGING_CLIENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(2)
                .setMaxInFlightEmbeddingGetMores(2),
            this.context.getIndexManagerFactory(),
            barriers);

    ChangeStreamManager manager = instance.changeStreamManager;

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
    DocumentIndexer firstIndexer = indexer();
    doAnswer(hangForBarriers.apply(firstIndexingStarted, firstIndexingDone))
        .when(firstIndexer)
        .indexDocumentEvent(any());

    CyclicBarrier secondIndexingStarted = new CyclicBarrier(2);
    CyclicBarrier secondIndexingDone = new CyclicBarrier(2);
    DocumentIndexer secondIndexer = indexer();
    doAnswer(hangForBarriers.apply(secondIndexingStarted, secondIndexingDone))
        .when(secondIndexer)
        .indexDocumentEvent(any());

    CyclicBarrier thirdIndexingStarted = new CyclicBarrier(2);
    CyclicBarrier thirdIndexingDone = new CyclicBarrier(2);
    DocumentIndexer thirdIndexer = indexer();
    doAnswer(hangForBarriers.apply(thirdIndexingStarted, thirdIndexingDone))
        .when(thirdIndexer)
        .indexDocumentEvent(any());

    CyclicBarrier fourthIndexingStarted = new CyclicBarrier(2);
    CyclicBarrier fourthIndexingDone = new CyclicBarrier(2);
    DocumentIndexer fourthIndexer = indexer();
    doAnswer(hangForBarriers.apply(fourthIndexingStarted, fourthIndexingDone))
        .when(fourthIndexer)
        .indexDocumentEvent(any());

    // Add all of the indexers to the manager.
    manager.add(
        firstDefinition.getGenerationId(),
        firstIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);
    manager.add(
        secondDefinition.getGenerationId(),
        secondIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);
    manager.add(
        thirdDefinition.getGenerationId(),
        thirdIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);
    manager.add(
        fourthDefinition.getGenerationId(),
        fourthIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    // The first and second indexes should have getMores dispatched on their behalf, but the
    // third and fourth should not.
    // Desired state:
    //  1: getMore
    //  2: getMore
    //  3: blocked on getMore (first in queue)
    //  4: blocked on getMore (second in queue)
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);
    firstGetMoreStarted.reset();
    secondGetMoreStarted.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> thirdGetMoreStarted.await(500, TimeUnit.MILLISECONDS));
    thirdGetMoreStarted.reset();
    Assert.assertThrows(
        TimeoutException.class, () -> fourthGetMoreStarted.await(500, TimeUnit.MILLISECONDS));
    fourthGetMoreStarted.reset();

    // Complete the first and second indexes' getMores, and see both begin indexing.
    // The third and fourth should still be waiting for a batch to finish indexing.
    //
    // Desired state:
    //  1: indexing
    //  2: indexing
    //  3: blocked on getMore (first in queue)
    //  4: blocked on getMore (second in queue)
    firstGetMoreDone.await(5, TimeUnit.SECONDS);
    firstIndexingStarted.await(5, TimeUnit.SECONDS);
    secondGetMoreDone.await(5, TimeUnit.SECONDS);
    secondIndexingStarted.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> thirdGetMoreStarted.await(500, TimeUnit.MILLISECONDS));
    thirdGetMoreStarted.reset();
    Assert.assertThrows(
        TimeoutException.class, () -> fourthGetMoreStarted.await(500, TimeUnit.MILLISECONDS));
    fourthGetMoreStarted.reset();

    // Complete the first index's indexing so it should now be blocked awaiting a getMore.  The
    // getMore for the third index should begin.  The fourth index should still be awaiting a
    // getMore.
    //
    // Desired state:
    //  1: blocked on getMore (second in queue)
    //  2: indexing
    //  3: getMore
    //  4: blocked on getMore (first in queue)
    firstIndexingDone.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> firstGetMoreStarted.await(500, TimeUnit.MILLISECONDS));
    firstGetMoreStarted.reset();
    thirdGetMoreStarted.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> fourthGetMoreStarted.await(500, TimeUnit.MILLISECONDS));
    fourthGetMoreStarted.reset();

    // Complete the second index's indexing and see that it is now awaiting a getMore.
    // the fourth index's getMore can now begin.
    //
    // Desired state:
    //  1: blocked on getMore (in queue)
    //  2: blocked on getMore (in queue)
    //  3: getMore
    //  4: getMore
    secondIndexingDone.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> secondGetMoreStarted.await(500, TimeUnit.MILLISECONDS));
    secondGetMoreStarted.reset();
    fourthGetMoreStarted.await(5, TimeUnit.SECONDS);

    // Complete the fourth index's getMore and see that it is enqueued to index.  The third
    // index should still be in it's getMore.
    //
    // Desired state:
    //  1: blocked on getMore (in queue)
    //  2: blocked on getMore (in queue)
    //  3: getMore
    //  4: indexing
    fourthGetMoreDone.await(5, TimeUnit.SECONDS);
    fourthIndexingStarted.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> thirdIndexingStarted.await(500, TimeUnit.MILLISECONDS));
    thirdIndexingStarted.reset();

    // Complete the third index's getMore and see that it is now indexing.
    //
    // Desired state:
    //  1: blocked on getMore (in queue)
    //  2: blocked on getMore (in queue)
    //  3: indexing
    //  4: indexing
    thirdGetMoreDone.await(5, TimeUnit.SECONDS);
    thirdIndexingStarted.await(5, TimeUnit.SECONDS);

    // Complete the fourth index's indexing and see that it is now blocked awaiting a getMore.
    //
    // Desired state:
    //  1: blocked on getMore (in queue)
    //  2: blocked on getMore (in queue)
    //  3: indexing
    //  4: blocked on getMore (third in queue)

    fourthIndexingDone.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> fourthGetMoreStarted.await(500, TimeUnit.MILLISECONDS));
    fourthGetMoreStarted.reset();

    // When the first and second indexes complete their first indexing and are re-added to the
    // ChangeStreamManager's queue, the order is nondeterministic. Since we're not sure which index
    // is up next, we need to check both possibilities.

    try {
      // Arbitrarily assume the first index is next in the queue.
      // If that's not the case, we'll fall through to the catch clause.
      firstGetMoreStarted.await(5, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // We couldn't start a getMore on the first index, so now try with the second index.
      // If this can't start either, the test will fail.
      secondGetMoreStarted.await(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testMongoClientErrorReleasesCapacity() throws Exception {
    // Create a ChangeStreamMongoClientFactory that hangs until signaled before publishing its
    // result, and
    // publishes a batch for the first index and an error for the second.
    IndexDefinitionGeneration firstDefinition = mockDefinitionGeneration(new ObjectId());
    IndexDefinitionGeneration secondDefinition = mockDefinitionGeneration(new ObjectId());
    IndexDefinitionGeneration thirdDefinition = mockDefinitionGeneration(new ObjectId());

    CyclicBarrier firstGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier firstGetMoreDone = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreDone = new CyclicBarrier(2);
    CyclicBarrier thirdGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier thirdGetMoreDone = new CyclicBarrier(2);

    Map<GenerationId, GetMoreBarrier> successBarriers =
        Map.of(
            firstDefinition.getGenerationId(),
            new GetMoreBarrier(firstGetMoreStarted, firstGetMoreDone),
            thirdDefinition.getGenerationId(),
            new GetMoreBarrier(thirdGetMoreStarted, thirdGetMoreDone));

    Map<GenerationId, GetMoreBarrier> errorBarriers =
        Map.of(
            secondDefinition.getGenerationId(),
            new GetMoreBarrier(secondGetMoreStarted, secondGetMoreDone));

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
    DocumentIndexer firstIndexer = indexer();
    doAnswer(hangForBarriers.apply(firstIndexingStarted, firstIndexingDone))
        .when(firstIndexer)
        .indexDocumentEvent(any());

    CyclicBarrier secondIndexingStarted = new CyclicBarrier(2);
    CyclicBarrier secondIndexingDone = new CyclicBarrier(2);
    DocumentIndexer secondIndexer = indexer();
    doAnswer(hangForBarriers.apply(secondIndexingStarted, secondIndexingDone))
        .when(secondIndexer)
        .indexDocumentEvent(any());

    CyclicBarrier thirdIndexingStarted = new CyclicBarrier(2);
    CyclicBarrier thirdIndexingDone = new CyclicBarrier(2);
    DocumentIndexer thirdIndexer = indexer();
    doAnswer(hangForBarriers.apply(thirdIndexingStarted, thirdIndexingDone))
        .when(thirdIndexer)
        .indexDocumentEvent(any());

    // Create a manager with numConcurrentChangeStreams of 2.
    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.SUCCESS_AND_ERROR_HANGING_CLIENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(2)
                .setMaxInFlightEmbeddingGetMores(2),
            this.context.getIndexManagerFactory(),
            successBarriers,
            errorBarriers);

    ChangeStreamManager manager = instance.changeStreamManager;

    // Add the indexes to the manager.
    manager.add(
        firstDefinition.getGenerationId(),
        firstIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);
    manager.add(
        secondDefinition.getGenerationId(),
        secondIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);
    manager.add(
        thirdDefinition.getGenerationId(),
        thirdIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    // GetMores should happen for the first 2 indexes.
    // Desired state:
    // 1: getMore
    // 2: getMore
    // 3: waiting for capacity
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);
    secondGetMoreStarted.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> thirdGetMoreStarted.await(1, TimeUnit.SECONDS));
    thirdGetMoreStarted.reset();

    // After the GetMores complete, only the first should be indexing (since the 2nd
    // errored) and free space for the third index to issue a getMore should open up.
    // Desired state:
    // 1: indexing
    // 2: errored
    // 3: getMore
    firstGetMoreDone.await(5, TimeUnit.SECONDS);
    firstIndexingStarted.await(5, TimeUnit.SECONDS);

    secondGetMoreDone.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> secondIndexingStarted.await(1, TimeUnit.SECONDS));
    secondIndexingStarted.reset();

    thirdGetMoreStarted.await(5, TimeUnit.SECONDS);
  }

  @Test
  public void testQueuedRequestForAlreadyClosedIndexIsSkipped() throws Exception {
    // Create a ChangeStreamMongoClientFactory that hangs until signaled before publishing its
    // result, and
    // publishes a batch for the first index and an error for the second.
    IndexDefinitionGeneration firstDefinition = mockDefinitionGeneration(new ObjectId());
    IndexDefinitionGeneration secondDefinition = mockDefinitionGeneration(new ObjectId());
    IndexDefinitionGeneration thirdDefinition = mockDefinitionGeneration(new ObjectId());

    CyclicBarrier firstGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier firstGetMoreDone = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreDone = new CyclicBarrier(2);
    CyclicBarrier thirdGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier thirdGetMoreDone = new CyclicBarrier(2);

    Map<GenerationId, GetMoreBarrier> successBarriers =
        Map.of(
            firstDefinition.getGenerationId(),
            new GetMoreBarrier(firstGetMoreStarted, firstGetMoreDone),
            secondDefinition.getGenerationId(),
            new GetMoreBarrier(secondGetMoreStarted, secondGetMoreDone),
            thirdDefinition.getGenerationId(),
            new GetMoreBarrier(thirdGetMoreStarted, thirdGetMoreDone));

    Map<GenerationId, GetMoreBarrier> errorBarriers = Map.of();

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
    DocumentIndexer firstIndexer = indexer();
    doAnswer(hangForBarriers.apply(firstIndexingStarted, firstIndexingDone))
        .when(firstIndexer)
        .indexDocumentEvent(any());

    CyclicBarrier thirdIndexingStarted = new CyclicBarrier(2);
    CyclicBarrier thirdIndexingDone = new CyclicBarrier(2);
    DocumentIndexer thirdIndexer = indexer();
    doAnswer(hangForBarriers.apply(thirdIndexingStarted, thirdIndexingDone))
        .when(thirdIndexer)
        .indexDocumentEvent(any());

    ChangeStreamIndexManagerFactory indexManagerFactory =
        (indexDefinition,
            scheduler,
            indexer,
            namespace,
            resumeInfoConsumer,
            indexMetricsUpdater,
            externalFuture,
            generationId) -> {
          var manager =
              this.context
                  .getIndexManagerFactory()
                  .create(
                      indexDefinition,
                      scheduler,
                      indexer,
                      namespace,
                      resumeInfoConsumer,
                      indexMetricsUpdater,
                      externalFuture,
                      generationId);

          if (generationId.equals(secondDefinition.getGenerationId())) {
            // shutdown manager for the second index, so we can
            // test the corresponding request is skipped
            manager.shutdown();
          }

          return manager;
        };

    // Create a manager with numConcurrentChangeStreams of 2.
    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.SUCCESS_AND_ERROR_HANGING_CLIENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(2)
                .setMaxInFlightEmbeddingGetMores(2),
            indexManagerFactory,
            successBarriers,
            errorBarriers);

    ChangeStreamManager manager = instance.changeStreamManager;

    // Add the indexes to the manager.
    manager.add(
        firstDefinition.getGenerationId(),
        firstIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);
    manager.add(
        secondDefinition.getGenerationId(),
        indexer(),
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);
    manager.add(
        thirdDefinition.getGenerationId(),
        thirdIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    // we expect both first and third getMore to be in-flight, while
    // the second one should never be executed
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);
    thirdGetMoreStarted.await(5, TimeUnit.SECONDS);

    // both should complete successfully
    firstGetMoreDone.await(5, TimeUnit.SECONDS);
    thirdGetMoreDone.await(5, TimeUnit.SECONDS);

    // check that only the second mongo client is closed
    ResumeableClientFactory clientFactory = instance.clientFactory;
    verify(extractMockedMongoClient(clientFactory, firstDefinition.getGenerationId()), never())
        .close();
    verify(extractMockedMongoClient(clientFactory, secondDefinition.getGenerationId()), times(1))
        .close();
    verify(extractMockedMongoClient(clientFactory, thirdDefinition.getGenerationId()), never())
        .close();
  }

  @Test
  public void testQueuesMultipleBatchesForSameIndex() throws Exception {
    // Create a ChangeStreamMongoClientFactory that hangs until signaled before publishing its
    // result, and
    // publishes a batch for the first index and an error for the second.
    IndexDefinitionGeneration firstDefinition =
        mockIndexGeneration(new ObjectId()).getDefinitionGeneration();
    IndexDefinitionGeneration secondDefinition =
        mockIndexGeneration(new ObjectId()).getDefinitionGeneration();

    CyclicBarrier firstGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier firstGetMoreDone = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreDone = new CyclicBarrier(2);

    Map<GenerationId, GetMoreBarrier> barriers =
        Map.of(
            firstDefinition.getGenerationId(),
            new GetMoreBarrier(firstGetMoreStarted, firstGetMoreDone),
            secondDefinition.getGenerationId(),
            new GetMoreBarrier(secondGetMoreStarted, secondGetMoreDone));

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
    DocumentIndexer firstIndexer = indexer();
    doAnswer(hangForBarriers.apply(firstIndexingStarted, firstIndexingDone))
        .when(firstIndexer)
        .indexDocumentEvent(any());

    CyclicBarrier secondIndexingStarted = new CyclicBarrier(2);
    CyclicBarrier secondIndexingDone = new CyclicBarrier(2);
    DocumentIndexer secondIndexer = indexer();
    doAnswer(hangForBarriers.apply(secondIndexingStarted, secondIndexingDone))
        .when(secondIndexer)
        .indexDocumentEvent(any());

    // Create a manager with numConcurrentChangeStreams of 2.
    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.HANGING_CLIENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(2)
                .setMaxInFlightEmbeddingGetMores(2),
            this.context.getIndexManagerFactory(),
            barriers);

    ChangeStreamManager manager = instance.changeStreamManager;

    // Add the indexes to the manager.
    manager.add(
        firstDefinition.getGenerationId(),
        firstIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    // getMore should happen for the first index.
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);

    // As soon as the next getMore finishes, indexing of that batch should begin
    // along with the next getMore, as there is already a batch in
    // the scheduler.
    firstGetMoreDone.await(5, TimeUnit.SECONDS);

    firstIndexingStarted.await(5, TimeUnit.SECONDS);
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> firstIndexingStarted.await(1, TimeUnit.SECONDS));
    firstIndexingStarted.reset();

    // Add a manager for the second index, which should start getMores
    // once the queue drains of existing batches for the first index
    // and grabs the next batch for the first index.
    manager.add(
        secondDefinition.getGenerationId(),
        secondIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    // When the second getMore finishes, the next one should not begin, as
    // there are two batches waiting to be indexed.
    firstGetMoreDone.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> firstGetMoreStarted.await(1, TimeUnit.SECONDS));
    firstGetMoreStarted.reset();

    // When indexing of the first batch completes, indexing of the second batch should begin,
    // and a getMore for the second index should begin.
    firstIndexingDone.await(5, TimeUnit.SECONDS);
    firstIndexingStarted.await(5, TimeUnit.SECONDS);

    Assert.assertThrows(
        TimeoutException.class, () -> firstGetMoreStarted.await(1, TimeUnit.SECONDS));
    firstGetMoreStarted.reset();
    secondGetMoreStarted.await(5, TimeUnit.SECONDS);
  }

  @Test
  public void testEmptyBatchWaitsForIndexingBatch() throws Exception {
    // Create a ChangeStreamMongoClientFactory that hangs until signaled before publishing its
    // result, alternating between a batch with an insert and an empty batch
    IndexDefinitionGeneration definition =
        mockIndexGeneration(new ObjectId()).getDefinitionGeneration();

    CyclicBarrier getMoreStarted = new CyclicBarrier(2);
    CyclicBarrier getMoreDone = new CyclicBarrier(2);

    Map<GenerationId, GetMoreBarrier> barriers =
        Map.of(
            definition.getGenerationId(),
            new GetMoreBarrier(
                getMoreStarted,
                getMoreDone,
                List.of(
                    new ChangeStreamBatch(
                        toRawBsonDocuments(
                            Collections.singletonList(
                                ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION))),
                        ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                        new BsonTimestamp()),
                    new ChangeStreamBatch(
                        List.of(),
                        ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                        new BsonTimestamp()))));

    // Create DocumentIndexers that hang while indexing until told to complete.
    BiFunction<CyclicBarrier, CyclicBarrier, Answer<CompletableFuture<Void>>> hangForBarriers =
        (startedBarrier, doneBarrier) ->
            invocation -> {
              synchronized (this) {
                try {
                  startedBarrier.await();
                  startedBarrier.reset();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }

                try {
                  doneBarrier.await();
                  doneBarrier.reset();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }

                return CompletableFuture.completedFuture(null);
              }
            };

    CyclicBarrier indexingStarted = new CyclicBarrier(2);
    CyclicBarrier indexingDone = new CyclicBarrier(2);
    DocumentIndexer indexer = indexer();

    doAnswer(hangForBarriers.apply(indexingStarted, indexingDone))
        .when(indexer)
        .indexDocumentEvent(any());

    ChangeStreamIndexManagerFactory indexManagerFactory =
        mock(ChangeStreamIndexManagerFactory.class);
    when(indexManagerFactory.create(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                this.context
                    .getIndexManagerFactory()
                    .create(
                        invocation.getArgument(0),
                        invocation.getArgument(1),
                        invocation.getArgument(2),
                        invocation.getArgument(3),
                        invocation.getArgument(4),
                        invocation.getArgument(5),
                        invocation.getArgument(6),
                        invocation.getArgument(7)));

    // Create a manager with numConcurrentChangeStreams of 2.
    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.HANGING_CLIENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(2)
                .setMaxInFlightEmbeddingGetMores(2),
            indexManagerFactory,
            barriers);

    ChangeStreamManager manager = instance.changeStreamManager;

    // Add the index to the manager.
    manager.add(
        definition.getGenerationId(),
        indexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    // getMore should happen for the index.
    getMoreStarted.await(5, TimeUnit.SECONDS);

    // As soon as the next getMore finishes, indexing of that batch should begin
    // along with the next getMore, as there is already a batch in
    // the scheduler.
    getMoreDone.await(5, TimeUnit.SECONDS);

    indexingStarted.await(5, TimeUnit.SECONDS);
    getMoreStarted.await(5, TimeUnit.SECONDS);

    // When the second getMore finishes, it should wait for the first batch
    // to finish indexing, before it handles the empty batch
    // and sees that the first indexing is completed, so it doesn't have to.
    getMoreDone.await(5, TimeUnit.SECONDS);

    // once the first indexing finishes, another get more should start
    indexingDone.await(5, TimeUnit.SECONDS);
    getMoreStarted.await(5, TimeUnit.SECONDS);

    // indexing of the second batch should not block, as it is empty.
    Assert.assertThrows(TimeoutException.class, () -> indexingStarted.await(1, TimeUnit.SECONDS));
    indexingStarted.reset();

    // once the third getMore finishes, indexing of that batch should begin
    getMoreDone.await(5, TimeUnit.SECONDS);
    indexingStarted.await(5, TimeUnit.SECONDS);
  }

  @Test
  public void testMultipleBatchesSameIndexFirstBatchFails() throws Exception {
    // Create a ChangeStreamMongoClientFactory that hangs until signaled before publishing its
    // result, and publishes a batch for the first index and an error for the second.
    IndexDefinitionGeneration firstDefinition =
        mockIndexGeneration(new ObjectId()).getDefinitionGeneration();
    IndexDefinitionGeneration secondDefinition =
        mockIndexGeneration(new ObjectId()).getDefinitionGeneration();

    CyclicBarrier firstGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier firstGetMoreDone = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreDone = new CyclicBarrier(2);

    Map<GenerationId, GetMoreBarrier> barriers =
        Map.of(
            firstDefinition.getGenerationId(),
                new GetMoreBarrier(firstGetMoreStarted, firstGetMoreDone),
            secondDefinition.getGenerationId(),
                new GetMoreBarrier(secondGetMoreStarted, secondGetMoreDone));

    CountDownLatch failed = new CountDownLatch(1);
    // Create DocumentIndexers that hang while indexing until told to complete.
    BiFunction<CyclicBarrier, CyclicBarrier, Answer<CompletableFuture<Void>>> hangForBarriers =
        (startedBarrier, doneBarrier) ->
            invocation -> {
              try {
                startedBarrier.await();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }

              try {
                doneBarrier.await();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }

              if (failed.getCount() == 1) {
                failed.countDown();
                throw new RuntimeException();
              }

              return null;
            };

    CyclicBarrier firstIndexingStarted = new CyclicBarrier(2);
    CyclicBarrier firstIndexingDone = new CyclicBarrier(2);
    DocumentIndexer firstIndexer = indexer();
    doAnswer(hangForBarriers.apply(firstIndexingStarted, firstIndexingDone))
        .when(firstIndexer)
        .indexDocumentEvent(any());

    CyclicBarrier secondIndexingStarted = new CyclicBarrier(2);
    CyclicBarrier secondIndexingDone = new CyclicBarrier(2);
    DocumentIndexer secondIndexer = indexer();
    doAnswer(hangForBarriers.apply(secondIndexingStarted, secondIndexingDone))
        .when(secondIndexer)
        .indexDocumentEvent(any());

    // Create a manager with numConcurrentChangeStreams of 2.
    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.HANGING_CLIENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(2)
                .setMaxInFlightEmbeddingGetMores(2),
            this.context.getIndexManagerFactory(),
            barriers);

    ChangeStreamManager manager = instance.changeStreamManager;

    // Add the indexes to the manager.
    manager.add(
        firstDefinition.getGenerationId(),
        firstIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    // getMore should happen for the first index.
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);

    // As soon as the next getMore finishes, indexing of that batch should begin
    // along with the next getMore, as there is already a batch in
    // the scheduler.
    firstGetMoreDone.await(5, TimeUnit.SECONDS);

    firstIndexingStarted.await(5, TimeUnit.SECONDS);
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> firstIndexingStarted.await(1, TimeUnit.SECONDS));
    firstIndexingStarted.reset();

    // When the second getMore finishes, the next one should not begin, as
    // there are two batches waiting to be indexed.
    firstGetMoreDone.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> firstGetMoreStarted.await(1, TimeUnit.SECONDS));
    firstGetMoreStarted.reset();

    // When indexing of the first batch completes, indexing of the second batch should not begin,
    // because indexing of the first batch failed.
    firstIndexingDone.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> firstIndexingStarted.await(1, TimeUnit.SECONDS));
  }

  @Test
  public void testMultipleBatchesSameIndexSecondBatchFails() throws Exception {
    // Create a ChangeStreamMongoClientFactory that hangs until signaled before publishing its
    // result, and
    // publishes a batch for the first index and an error for the second.
    IndexDefinitionGeneration firstDefinition =
        mockIndexGeneration(new ObjectId()).getDefinitionGeneration();

    CyclicBarrier firstGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier firstGetMoreDone = new CyclicBarrier(2);

    Map<GenerationId, GetMoreBarrier> barriers =
        Map.of(
            firstDefinition.getGenerationId(),
            new GetMoreBarrier(firstGetMoreStarted, firstGetMoreDone));

    AtomicBoolean signaled = new AtomicBoolean(false);
    // Create DocumentIndexers that hang while indexing until told to complete.
    BiFunction<CyclicBarrier, CyclicBarrier, Answer<CompletableFuture<Void>>> hangForBarriers =
        (startedBarrier, doneBarrier) ->
            invocation -> {
              try {
                startedBarrier.await();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }

              try {
                doneBarrier.await();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }

              if (signaled.get()) {
                throw new RuntimeException();
              }

              return null;
            };

    CyclicBarrier firstIndexingStarted = new CyclicBarrier(2);
    CyclicBarrier firstIndexingDone = new CyclicBarrier(2);
    DocumentIndexer firstIndexer = indexer();
    doAnswer(hangForBarriers.apply(firstIndexingStarted, firstIndexingDone))
        .when(firstIndexer)
        .indexDocumentEvent(any());

    // Create a manager with numConcurrentChangeStreams of 3.
    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.HANGING_CLIENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(3)
                .setMaxInFlightEmbeddingGetMores(3),
            this.context.getIndexManagerFactory(),
            barriers);

    ChangeStreamManager manager = instance.changeStreamManager;

    // Add the indexes to the manager.
    manager.add(
        firstDefinition.getGenerationId(),
        firstIndexer,
        MOCK_INDEX_DEFINITION,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    // getMore should happen for the first index.
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);

    // As soon as the next getMore finishes, indexing of that batch should begin
    // along with the next getMore, as there is already a batch in
    // the scheduler.
    firstGetMoreDone.await(5, TimeUnit.SECONDS);

    firstIndexingStarted.await(5, TimeUnit.SECONDS);

    // let the next getMore finish, indexing should not start, as the scheduler
    // has a capacity of 1.
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);
    firstGetMoreDone.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> firstIndexingStarted.await(1, TimeUnit.SECONDS));
    firstIndexingStarted.reset();

    // signal the client to start producing errors
    signaled.set(true);

    // this get more should fail
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);
    firstGetMoreDone.await(5, TimeUnit.SECONDS);

    // let the indexing finish
    firstIndexingDone.await(5, TimeUnit.SECONDS);

    // the rest of the indexing should be cancelled
    Assert.assertThrows(
        TimeoutException.class, () -> firstIndexingStarted.await(1, TimeUnit.SECONDS));
    firstIndexingStarted.reset();
  }

  @Test
  public void testLimitInflightEmbeddingGetMores() throws Exception {
    // This test is only relevant for sync change streams. In-flight embedding getMores are not
    // limited for async change streams, as async change streams are deprecated.
    Assume.assumeTrue(this.context.name.equals("sync-client"));
    // Set up registry for embedding model config mapping
    setupRegistry();

    VectorIndexDefinition firstDefinition = mockAutoEmbeddingVectorDefinition(new ObjectId());
    IndexDefinitionGeneration firstGeneration =
        mockIndexGeneration(firstDefinition).getDefinitionGeneration();
    VectorIndexDefinition secondDefinition = mockAutoEmbeddingVectorDefinition(new ObjectId());
    IndexDefinitionGeneration secondGeneration =
        mockIndexGeneration(secondDefinition).getDefinitionGeneration();

    DocumentIndexer firstIndexer = indexer();
    when(firstIndexer.getIndexDefinition()).thenReturn(firstDefinition);
    DocumentIndexer secondIndexer = indexer();
    when(secondIndexer.getIndexDefinition()).thenReturn(secondDefinition);

    CyclicBarrier firstGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier firstGetMoreDone = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreDone = new CyclicBarrier(2);

    Map<GenerationId, GetMoreBarrier> barriers =
        Map.of(
            firstGeneration.getGenerationId(),
            new GetMoreBarrier(firstGetMoreStarted, firstGetMoreDone),
            secondGeneration.getGenerationId(),
            new GetMoreBarrier(secondGetMoreStarted, secondGetMoreDone));

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.HANGING_CLIENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(2)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory(),
            barriers);

    ChangeStreamManager manager = instance.changeStreamManager;

    manager.add(
        firstGeneration.getGenerationId(),
        firstIndexer,
        firstDefinition,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);
    // Ensure that getMore is called on the first index before the second.
    Thread.sleep(100);
    manager.add(
        secondGeneration.getGenerationId(),
        secondIndexer,
        secondDefinition,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    // The first index should have getMores dispatched and begin indexing, but the second index
    // should be blocked until the first index batch is done.
    //
    // Desired state:
    //  1. getMore
    //  2. blocked on getMore (enqueued but rescheduling until first index finishes)
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);
    Assert.assertThrows(
        TimeoutException.class, () -> secondGetMoreStarted.await(500, TimeUnit.MILLISECONDS));
    secondGetMoreStarted.reset();

    // Complete the first index's getMore. Only one getMore should be started, but either index may
    // be next in queue (second batch from first index or rescheduled batch from second index)
    firstGetMoreDone.await(5, TimeUnit.SECONDS);
    try {
      secondGetMoreStarted.await(5, TimeUnit.SECONDS);
      Assert.assertThrows(
          TimeoutException.class, () -> firstGetMoreStarted.await(500, TimeUnit.MILLISECONDS));
    } catch (TimeoutException e1) {
      try {
        firstGetMoreStarted.await(5, TimeUnit.SECONDS);
      } catch (TimeoutException e2) {
        // If neither getMore is started, then the test is failing.
        Assert.fail("Neither getMore was started after first index finished indexing.");
      }
    }
    EmbeddingServiceRegistry.clearRegistry();
  }

  @Test
  public void testEmbeddingGetMoreLimitExceedsNumConcurrentChangeStreams() throws Exception {
    // This test is only relevant for sync change streams. In-flight embedding getMores are not
    // limited for async change streams, as async change streams are deprecated.
    Assume.assumeTrue(this.context.name.equals("sync-client"));

    VectorIndexDefinition firstDefinition = mockAutoEmbeddingVectorDefinition(new ObjectId());
    IndexDefinitionGeneration firstGeneration =
        mockIndexGeneration(firstDefinition).getDefinitionGeneration();
    VectorIndexDefinition secondDefinition = mockAutoEmbeddingVectorDefinition(new ObjectId());
    IndexDefinitionGeneration secondGeneration =
        mockIndexGeneration(secondDefinition).getDefinitionGeneration();

    CyclicBarrier firstGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier firstGetMoreDone = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreDone = new CyclicBarrier(2);

    Map<GenerationId, GetMoreBarrier> barriers =
        Map.of(
            firstGeneration.getGenerationId(),
            new GetMoreBarrier(firstGetMoreStarted, firstGetMoreDone),
            secondGeneration.getGenerationId(),
            new GetMoreBarrier(secondGetMoreStarted, secondGetMoreDone));

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.HANGING_CLIENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(2)
                .setMaxInFlightEmbeddingGetMores(3),
            this.context.getIndexManagerFactory(),
            barriers);

    ChangeStreamManager manager = instance.changeStreamManager;

    manager.add(
        firstGeneration.getGenerationId(),
        indexer(),
        firstDefinition,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);
    manager.add(
        secondGeneration.getGenerationId(),
        indexer(),
        secondDefinition,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    // Verify that both getMores may start and run concurrently.
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);
    secondGetMoreStarted.await(5, TimeUnit.SECONDS);
    firstGetMoreDone.await(5, TimeUnit.SECONDS);
    secondGetMoreDone.await(5, TimeUnit.SECONDS);
  }

  @Test
  public void testConcurrentWithEmbeddingAndSearchIndex() throws Exception {
    // This test is only relevant for sync change streams. In-flight embedding getMores are not
    // limited for async change streams, as async change streams are deprecated.
    Assume.assumeTrue(this.context.name.equals("sync-client"));

    SearchIndexDefinition firstDefinition = mockSearchDefinition(new ObjectId());
    IndexDefinitionGeneration firstGeneration =
        mockIndexGeneration(firstDefinition).getDefinitionGeneration();
    VectorIndexDefinition secondDefinition = mockAutoEmbeddingVectorDefinition(new ObjectId());
    IndexDefinitionGeneration secondGeneration =
        mockIndexGeneration(secondDefinition).getDefinitionGeneration();

    CyclicBarrier firstGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier firstGetMoreDone = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreStarted = new CyclicBarrier(2);
    CyclicBarrier secondGetMoreDone = new CyclicBarrier(2);

    Map<GenerationId, GetMoreBarrier> barriers =
        Map.of(
            firstGeneration.getGenerationId(),
            new GetMoreBarrier(firstGetMoreStarted, firstGetMoreDone),
            secondGeneration.getGenerationId(),
            new GetMoreBarrier(secondGetMoreStarted, secondGetMoreDone));

    var instance =
        this.context.instanceFactory.create(
            ClientFactoryType.HANGING_CLIENT,
            mock(ChangeStreamModeSelector.class),
            indexingWorkSchedulerFactory(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(2)
                .setMaxInFlightEmbeddingGetMores(1),
            this.context.getIndexManagerFactory(),
            barriers);

    ChangeStreamManager manager = instance.changeStreamManager;

    manager.add(
        firstGeneration.getGenerationId(),
        indexer(),
        firstDefinition,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);
    manager.add(
        secondGeneration.getGenerationId(),
        indexer(),
        secondDefinition,
        INITIAL_RESUME_INFO,
        ignoreResumeInfo(),
        IGNORE_METRICS,
        false);

    // Verify that both getMores may start and run concurrently.
    firstGetMoreStarted.await(5, TimeUnit.SECONDS);
    secondGetMoreStarted.await(5, TimeUnit.SECONDS);
    firstGetMoreDone.await(5, TimeUnit.SECONDS);
    secondGetMoreDone.await(5, TimeUnit.SECONDS);
  }

  private static void setupRegistry() {
    EmbeddingModelCatalog.registerModelConfig(
        "voyage-3-large",
        EmbeddingModelConfig.create(
            "voyage-3-large",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.of("us-east-1"),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(512),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.START),
                    Optional.of(100),
                    Optional.of(1000)),
                new EmbeddingServiceConfig.ErrorHandlingConfig(50, 50L, 10L, 0.1),
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "token123", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty())));
  }

  private Consumer<ChangeStreamResumeInfo> ignoreResumeInfo() {
    return new Consumer<>() {
      @Override
      public void accept(ChangeStreamResumeInfo changeStreamResumeInfo) {}
    };
  }

  private DocumentIndexer indexer() {
    return com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
        .mockDocumentIndexer();
  }

  private IndexingWorkSchedulerFactory indexingWorkSchedulerFactory() {
    return IndexingWorkSchedulerFactory.create(2, mock(Supplier.class), new SimpleMeterRegistry());
  }

  private AutoCloseable extractMockedMongoClient(
      ResumeableClientFactory factory, GenerationId generationId) {
    return factory.resumeModeAwareChangeStream(
        generationId,
        mock(ChangeStreamResumeInfo.class),
        SearchIndexDefinitionBuilder.VALID_INDEX,
        false);
  }

  private enum ClientFactoryType {
    SINGLE_EVENT,
    MULTIPLE_EVENTS,
    EXCEPTION,
    HANGING_CLIENT,
    SINGLE_EVENT_THEN_HANGING_CLIENT,
    SUCCESS_AND_ERROR_HANGING_CLIENT,
  }

  @FunctionalInterface
  private interface ResumeableClientFactory {

    AutoCloseable resumeModeAwareChangeStream(
        GenerationId generationId,
        ChangeStreamResumeInfo resumeInfo,
        IndexDefinition indexDefinition,
        boolean removeMatchCollectionUuid);
  }

  @FunctionalInterface
  private interface TestInstanceFactory {

    ParameterInstance create(
        ClientFactoryType clientFactoryType,
        ChangeStreamModeSelector modeSelector,
        IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
        SteadyStateReplicationConfig.Builder partialReplicationConfig,
        ChangeStreamIndexManagerFactory indexManagerFactory,
        Object... args)
        throws Exception;
  }

  private static class ParameterInstance {

    final ChangeStreamManager changeStreamManager;
    final ResumeableClientFactory clientFactory;

    public ParameterInstance(
        ChangeStreamManager changeStreamManager, ResumeableClientFactory clientFactory) {
      this.changeStreamManager = changeStreamManager;
      this.clientFactory = clientFactory;
    }
  }

  private static class TestContext {
    final String name;
    final TestInstanceFactory instanceFactory;

    TestContext(String name, TestInstanceFactory instanceFactory) {
      this.name = name;
      this.instanceFactory = instanceFactory;
    }

    @Override
    public String toString() {
      return this.name;
    }

    public ChangeStreamIndexManagerFactory getIndexManagerFactory() {
      return ChangeStreamManager.indexManagerFactoryWithDecodingScheduler(
          DecodingWorkScheduler.create(2, new SimpleMeterRegistry()));
    }
  }

  private static class ChangeStreamManagerTestSyncHelpers {

    @SuppressWarnings("unchecked")
    static ChangeStreamMongoClientFactory getSyncClientFactoryForType(
        ClientFactoryType type, Object... args) throws Exception {
      return switch (type) {
        case EXCEPTION -> syncExceptionProducingClient();
        case SINGLE_EVENT -> syncSingleEventProducingClient();
        case MULTIPLE_EVENTS -> {
          Assert.assertEquals(1, args.length);
          yield syncEventsProducingClient((List<ChangeStreamDocument<RawBsonDocument>>) args[0]);
        }
        case SINGLE_EVENT_THEN_HANGING_CLIENT -> {
          Assert.assertEquals(2, args.length);
          yield syncEventThenHangingClient((CyclicBarrier) args[0], (CyclicBarrier) args[1]);
        }
        case HANGING_CLIENT -> {
          Assert.assertEquals(1, args.length);
          yield syncHangingClient((Map<GenerationId, GetMoreBarrier>) args[0]);
        }
        case SUCCESS_AND_ERROR_HANGING_CLIENT -> {
          Assert.assertEquals(2, args.length);
          yield syncSuccessAndErrorProducingHangingClient(
              (Map<GenerationId, GetMoreBarrier>) args[0],
              (Map<GenerationId, GetMoreBarrier>) args[1]);
        }
      };
    }

    static ParameterInstance getSyncParameters(
        ClientFactoryType clientFactoryType,
        ChangeStreamModeSelector modeSelector,
        IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
        SteadyStateReplicationConfig.Builder partialReplicationConfig,
        ChangeStreamIndexManagerFactory indexManagerFactory,
        Object... args)
        throws Exception {

      ChangeStreamMongoClientFactory mongoClientFactory =
          getSyncClientFactoryForType(clientFactoryType, args);

      SteadyStateReplicationConfig replicationConfig =
          partialReplicationConfig
              .setChangeStreamQueryMaxTimeMs(1000)
              .setChangeStreamCursorMaxTimeSec(1000)
              .build();

      ChangeStreamManager manager =
          ChangeStreamManager.createSync(
              MeterAndFtdcRegistry.createWithSimpleRegistries(),
              mongoClientFactory,
              modeSelector,
              indexingWorkSchedulerFactory,
              replicationConfig,
              indexManagerFactory);

      return new ParameterInstance(manager, mongoClientFactory::resumeTimedModeAwareChangeStream);
    }

    static ChangeStreamMongoClientFactory syncSingleEventProducingClient() throws Exception {

      var client = mock(ChangeStreamMongoClient.class);
      var clientFactory = mock(ChangeStreamMongoClientFactory.class);

      when(client.getNext())
          .thenReturn(
              new ChangeStreamBatch(
                  toRawBsonDocuments(
                      Collections.singletonList(
                          ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION))),
                  ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                  new BsonTimestamp()));

      when(clientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
          .thenReturn(client);
      return clientFactory;
    }

    static ChangeStreamMongoClientFactory syncEventsProducingClient(
        List<ChangeStreamDocument<RawBsonDocument>> events) throws Exception {
      var client = mock(ChangeStreamMongoClient.class);
      var clientFactory = mock(ChangeStreamMongoClientFactory.class);

      when(client.getNext())
          .thenReturn(
              new ChangeStreamBatch(
                  toRawBsonDocuments(events),
                  ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                  new BsonTimestamp()));

      when(clientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
          .thenReturn(client);
      return clientFactory;
    }

    static ChangeStreamMongoClientFactory syncExceptionProducingClient() throws Exception {
      var client = mock(ChangeStreamMongoClient.class);
      when(client.getNext())
          .thenThrow(SteadyStateException.createTransient(new MongoException("exception")));

      var clientFactory = mock(ChangeStreamMongoClientFactory.class);
      when(clientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
          .thenReturn(client);
      return clientFactory;
    }

    static ChangeStreamMongoClientFactory syncEventThenHangingClient(
        CyclicBarrier started, CyclicBarrier done) throws Exception {
      var clientFactory = mock(ChangeStreamMongoClientFactory.class);
      var client = mock(ChangeStreamMongoClient.class);

      when(client.getNext())
          .thenReturn(
              new ChangeStreamBatch(
                  toRawBsonDocuments(
                      Collections.singletonList(
                          ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION))),
                  ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                  new BsonTimestamp()))
          .thenAnswer(
              ignored -> {
                started.await();
                done.await();
                return new ChangeStreamBatch(
                    toRawBsonDocuments(
                        Collections.singletonList(
                            ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION))),
                    ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                    new BsonTimestamp());
              });

      when(clientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
          .thenReturn(client);
      return clientFactory;
    }

    static ChangeStreamMongoClientFactory syncHangingClient(
        Map<GenerationId, GetMoreBarrier> barriers) {
      var clientFactory = mock(ChangeStreamMongoClientFactory.class);

      AtomicInteger count = new AtomicInteger(0);
      when(clientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
          .then(
              invocation -> {
                GenerationId generationId = invocation.getArgument(0);
                GetMoreBarrier indexBarrier = barriers.get(generationId);

                var client = mock(ChangeStreamMongoClient.class);
                when(client.getNext())
                    .thenAnswer(
                        ignored -> {
                          indexBarrier.getMoreStarted.await();
                          indexBarrier.getMoreDone.await();
                          int resultIndex = count.get();
                          count.updateAndGet(
                              (number) -> (number + 1) % indexBarrier.responses.size());
                          return indexBarrier.responses.get(resultIndex);
                        });

                return client;
              });

      return clientFactory;
    }

    /**
     * Returns a ChangeStreamMongoClientFactory that hangs before returning a batch or error
     * depending on the indexId by waiting on a barrier specific to the index.
     */
    static ChangeStreamMongoClientFactory syncSuccessAndErrorProducingHangingClient(
        Map<GenerationId, GetMoreBarrier> successBarriers,
        Map<GenerationId, GetMoreBarrier> errorBarriers) {
      var clientFactory = mock(ChangeStreamMongoClientFactory.class);

      AtomicInteger count = new AtomicInteger(0);
      Map<GenerationId, ChangeStreamMongoClient<SteadyStateException>> successClients =
          new HashMap<>();

      successBarriers.forEach(
          (generationId, barrier) -> {
            var successClient = mock(ChangeStreamMongoClient.class);
            try {
              when(successClient.getNext())
                  .thenAnswer(
                      invocation -> {
                        barrier.getMoreStarted.await();
                        barrier.getMoreDone.await();
                        count.updateAndGet((number) -> (number + 1) % barrier.responses.size());
                        return barrier.responses.get(count.get());
                      });
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            successClients.put(generationId, successClient);
          });

      Map<GenerationId, ChangeStreamMongoClient<SteadyStateException>> errorClients =
          new HashMap<>();

      errorBarriers.forEach(
          (generationId, barrier) -> {
            var errorClient = mock(ChangeStreamMongoClient.class);
            try {
              when(errorClient.getNext())
                  .then(
                      invocation -> {
                        barrier.getMoreStarted.await();
                        barrier.getMoreDone.await();
                        return new MongoException(
                            Errors.CHANGE_STREAM_FATAL_ERROR.code,
                            Errors.CHANGE_STREAM_FATAL_ERROR.name);
                      });
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            errorClients.put(generationId, errorClient);
          });

      when(clientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
          .then(
              invocation -> {
                GenerationId generationId = invocation.getArgument(0);
                if (successClients.containsKey(generationId)) {
                  return successClients.get(generationId);
                }

                if (errorClients.containsKey(generationId)) {
                  return errorClients.get(generationId);
                }

                throw new RuntimeException("unexpected generationId " + generationId);
              });
      return clientFactory;
    }
  }

  private static class GetMoreBarrier {
    public final CyclicBarrier getMoreStarted;
    public final CyclicBarrier getMoreDone;
    public final List<ChangeStreamBatch> responses;

    public GetMoreBarrier(CyclicBarrier getMoreStarted, CyclicBarrier getMoreDone) {
      this.getMoreStarted = getMoreStarted;
      this.getMoreDone = getMoreDone;
      this.responses =
          Collections.singletonList(
              new ChangeStreamBatch(
                  toRawBsonDocuments(
                      Collections.singletonList(
                          ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION))),
                  ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                  new BsonTimestamp()));
    }

    public GetMoreBarrier(
        CyclicBarrier getMoreStarted,
        CyclicBarrier getMoreDone,
        List<ChangeStreamBatch> responses) {
      assertFalse(responses.isEmpty());

      this.getMoreStarted = getMoreStarted;
      this.getMoreDone = getMoreDone;
      this.responses = responses;
    }
  }
}
