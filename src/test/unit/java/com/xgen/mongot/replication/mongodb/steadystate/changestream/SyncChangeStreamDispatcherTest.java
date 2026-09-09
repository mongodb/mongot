package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.IndexTypeData.INDEX_TYPE_TAG_NAME;
import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_SEARCH;
import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_SEARCH_AUTO_EMBEDDING;
import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH;
import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH_AUTO_EMBEDDING;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.VectorIndex.mockAutoEmbeddingVectorDefinition;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer.mockDocumentIndexer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.common.SteadyStateSchedulingMethod;
import com.xgen.mongot.util.Condition;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.Sleeper;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class SyncChangeStreamDispatcherTest {

  private static final IndexMetricsUpdater IGNORE_METRICS =
      IndexMetricsUpdaterBuilder.builder()
          .metricsFactory(SearchIndex.mockMetricsFactory())
          .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
          .build();

  @Test
  public void partialEmbedMetrics_onlyIncludeAutoEmbeddingIndexTypeTags() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    SyncChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            meterRegistry,
            mock(ChangeStreamMongoClientFactory.class),
            mock(ChangeStreamTaskExecutor.class),
            CommonReplicationConfig.Type.DEFAULT);

    try {
      assertThat(
              meterRegistry
                  .find("indexing.steadyStateChangeStream.partialEmbedApplied")
                  .tag(INDEX_TYPE_TAG_NAME, TAG_SEARCH_AUTO_EMBEDDING.tagValue)
                  .counter())
          .isNotNull();
      assertThat(
              meterRegistry
                  .find("indexing.steadyStateChangeStream.partialEmbedApplied")
                  .tag(INDEX_TYPE_TAG_NAME, TAG_VECTOR_SEARCH_AUTO_EMBEDDING.tagValue)
                  .counter())
          .isNotNull();
      assertThat(
              meterRegistry
                  .find("indexing.steadyStateChangeStream.partialEmbedApplied")
                  .tag(INDEX_TYPE_TAG_NAME, TAG_SEARCH.tagValue)
                  .counter())
          .isNull();
      assertThat(
              meterRegistry
                  .find("indexing.steadyStateChangeStream.partialEmbedApplied")
                  .tag(INDEX_TYPE_TAG_NAME, TAG_VECTOR_SEARCH.tagValue)
                  .counter())
          .isNull();
      assertThat(
              meterRegistry
                  .find("indexing.steadyStateChangeStream.viewFieldDependenciesDecision")
                  .tag(INDEX_TYPE_TAG_NAME, TAG_SEARCH_AUTO_EMBEDDING.tagValue)
                  .meters())
          .isNotEmpty();
      assertThat(
              meterRegistry
                  .find("indexing.steadyStateChangeStream.viewFieldDependenciesDecision")
                  .tag(INDEX_TYPE_TAG_NAME, TAG_SEARCH.tagValue)
                  .meters())
          .isEmpty();
    } finally {
      syncDispatcher.shutdown();
    }
  }

  @Test
  public void partialEmbedMetrics_searchAutoEmbeddingBatch_routesIncrementToSearchTag()
      throws Exception {
    ObjectId indexId = new ObjectId();
    FieldPath sourcePath = FieldPath.parse("title");
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "titleVector",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition("voyage-3-large", sourcePath))
                    .build())
            .build();
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .defaultMetadata()
            .mappings(mappings)
            .build();
    UpdateDescription updateDescription =
        new UpdateDescription(
            List.of(), new BsonDocument(sourcePath.toString(), new BsonString("changed title")));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append(sourcePath.toString(), new BsonString("changed title"));
    var event =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDescription, fullDocument);
    ChangeStreamDocumentUtils.DocumentEventBatch documentEventBatch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(event),
            indexDefinition,
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            true,
            Optional.empty(),
            Optional.empty());
    assertThat(documentEventBatch.partialEmbedApplied).isEqualTo(1);

    IndexDefinitionGeneration generation =
        mockIndexGeneration(indexDefinition).getDefinitionGeneration();
    BsonTimestamp now = new BsonTimestamp(1_700_000_000, 0);
    ChangeStreamBatch changeStreamBatch =
        new ChangeStreamBatch(Collections.emptyList(), ChangeStreamUtils.resumeToken(now), now);
    ChangeStreamMongoClient<SteadyStateException> mongoClient = mock(ChangeStreamMongoClient.class);
    when(mongoClient.getNext()).thenReturn(changeStreamBatch);
    ChangeStreamMongoClientFactory mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(mongoClient);
    ChangeStreamIndexManager indexManager =
        spy(
            DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
                indexDefinition,
                mock(IndexingWorkScheduler.class),
                mockDocumentIndexer(),
                ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                resumeInfo -> {},
                IGNORE_METRICS,
                new CompletableFuture<>(),
                generation.getGenerationId(),
                DecodingWorkScheduler.create(2, new SimpleMeterRegistry())));
    doAnswer(
            invocation -> {
              ChangeStreamIndexManager.DocumentMetricsUpdater metricsUpdater =
                  invocation.getArgument(1);
              metricsUpdater.accept(documentEventBatch);
              return new ChangeStreamIndexManager.BatchInfo(
                  CompletableFuture.completedFuture(null),
                  Optional.empty(),
                  new AtomicReference<>(ChangeStreamIndexManager.BatchFullness.EMPTY));
            })
        .when(indexManager)
        .indexBatch(any(), any(), any());

    AtomicReference<Runnable> acceptedTask = new AtomicReference<>();
    AtomicBoolean acceptedFirstTask = new AtomicBoolean();
    ChangeStreamTaskExecutor taskExecutor =
        new ChangeStreamTaskExecutor() {
          @Override
          public boolean enqueue(Runnable task, EnqueueContext context) {
            if (acceptedFirstTask.compareAndSet(false, true)) {
              acceptedTask.set(task);
              return true;
            }
            context.onDropped().run();
            return false;
          }

          @Override
          public void shutdown() {}
        };
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    SyncChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            meterRegistry, mongoClientFactory, taskExecutor, CommonReplicationConfig.Type.DEFAULT);

    try {
      syncDispatcher.add(
          indexDefinition,
          generation.getGenerationId(),
          mock(ChangeStreamResumeInfo.class),
          indexManager,
          false);
      acceptedTask.get().run();

      assertThat(
              meterRegistry
                  .get("indexing.steadyStateChangeStream.partialEmbedApplied")
                  .tag(INDEX_TYPE_TAG_NAME, TAG_SEARCH_AUTO_EMBEDDING.tagValue)
                  .counter()
                  .count())
          .isEqualTo(1.0);
      assertThat(
              meterRegistry
                  .get("indexing.steadyStateChangeStream.partialEmbedApplied")
                  .tag(INDEX_TYPE_TAG_NAME, TAG_VECTOR_SEARCH_AUTO_EMBEDDING.tagValue)
                  .counter()
                  .count())
          .isEqualTo(0.0);
    } finally {
      syncDispatcher.shutdown();
    }
  }

  @Test
  public void testConcurrentEmbeddingGetMoresSemaphoreReleased_indexManagerShutdown()
      throws SteadyStateException {
    VectorIndexDefinition embeddingDefinition = mockAutoEmbeddingVectorDefinition(new ObjectId());
    IndexDefinitionGeneration embeddingGeneration =
        mockIndexGeneration(embeddingDefinition).getDefinitionGeneration();

    ChangeStreamMongoClientFactory mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(mock(ChangeStreamMongoClient.class));

    AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();
    ChangeStreamIndexManager indexManager =
        spy(
            DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
                embeddingDefinition,
                mock(IndexingWorkScheduler.class),
                mockDocumentIndexer(),
                ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                resumeInfoReference::set,
                IGNORE_METRICS,
                new CompletableFuture<>(),
                embeddingGeneration.getGenerationId(),
                DecodingWorkScheduler.create(2, new SimpleMeterRegistry())));
    // Simulate index manager shutdown.
    when(indexManager.isShutdown()).thenReturn(true);

    // Create a dispatcher with a maximum of 1 concurrent embedding getMore.
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    AutoEmbeddingExecutor taskExecutor =
        new AutoEmbeddingExecutor(
            meterRegistry,
            Executors.fixedSizeThreadScheduledExecutor("executor", 1, meterRegistry),
            Optional.of(1));
    SyncChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            meterRegistry, mongoClientFactory, taskExecutor, CommonReplicationConfig.Type.DEFAULT);

    try {
      syncDispatcher.add(
          embeddingDefinition,
          embeddingGeneration.getGenerationId(),
          mock(ChangeStreamResumeInfo.class),
          indexManager,
          false);

      // The semaphore should be released when we return early due to index manager shutdown.
      assertNumberOfEmbeddingPermits(syncDispatcher, 1);
    } finally {
      // Shut down the dispatcher so its scheduled executor threads do not leak across the suite.
      syncDispatcher.shutdown();
    }
  }

  @Test
  public void testGetNextFailureWhileIndexManagerShutDownDoesNotFailLifecycle() throws Exception {
    var definition = SearchIndex.mockSearchDefinition(new ObjectId());
    IndexDefinitionGeneration generation =
        mockIndexGeneration(definition).getDefinitionGeneration();

    // getNext() fails as it would when the replication manager closes the shared client to abort an
    // in-flight read on shutdown.
    ChangeStreamMongoClient<SteadyStateException> mongoClient = mock(ChangeStreamMongoClient.class);
    when(mongoClient.getNext())
        .thenThrow(SteadyStateException.createTransient(new RuntimeException("closed"), "closed"));

    ChangeStreamMongoClientFactory mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(mongoClient);

    ChangeStreamIndexManager indexManager =
        spy(
            DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
                definition,
                mock(IndexingWorkScheduler.class),
                mockDocumentIndexer(),
                ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                resumeInfo -> {},
                IGNORE_METRICS,
                new CompletableFuture<>(),
                generation.getGenerationId(),
                DecodingWorkScheduler.create(2, new SimpleMeterRegistry())));
    // Not shut down when doGetMore checks before the read, but shut down by the time getNext()
    // fails and we reach the catch block (client was closed out from under the in-flight read).
    when(indexManager.isShutdown()).thenReturn(false, true);

    SyncChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            new SimpleMeterRegistry(),
            mongoClientFactory,
            new FifoExecutor(
                Executors.fixedSizeThreadScheduledExecutor(
                    "executor", 1, new SimpleMeterRegistry())),
            CommonReplicationConfig.Type.DEFAULT);

    try {
      syncDispatcher.add(
          definition,
          generation.getGenerationId(),
          mock(ChangeStreamResumeInfo.class),
          indexManager,
          false);

      // The client is closed gracefully and the index lifecycle is not failed for the expected read
      // failure during shutdown.
      verify(mongoClient, timeout(2000)).close();
      verify(indexManager, never()).failLifecycle(any());
    } finally {
      // Shut down the dispatcher so its scheduled executor threads do not leak across the suite.
      syncDispatcher.shutdown();
    }
  }

  @Test
  public void testGetMoresScheduledTracksAcceptedTask() throws Exception {
    var definition = SearchIndex.mockSearchDefinition(new ObjectId());
    IndexDefinitionGeneration generation =
        mockIndexGeneration(definition).getDefinitionGeneration();

    ChangeStreamMongoClient<SteadyStateException> mongoClient = mock(ChangeStreamMongoClient.class);
    ChangeStreamMongoClientFactory mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(mongoClient);

    ChangeStreamIndexManager indexManager =
        spy(
            DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
                definition,
                mock(IndexingWorkScheduler.class),
                mockDocumentIndexer(),
                ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                resumeInfo -> {},
                IGNORE_METRICS,
                new CompletableFuture<>(),
                generation.getGenerationId(),
                DecodingWorkScheduler.create(2, new SimpleMeterRegistry())));
    when(indexManager.isShutdown()).thenReturn(true);

    AtomicReference<Runnable> acceptedTask = new AtomicReference<>();
    ChangeStreamTaskExecutor taskExecutor =
        new ChangeStreamTaskExecutor() {
          @Override
          public boolean enqueue(Runnable task, EnqueueContext context) {
            acceptedTask.set(task);
            return true;
          }

          @Override
          public void shutdown() {}
        };
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    SyncChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            meterRegistry, mongoClientFactory, taskExecutor, CommonReplicationConfig.Type.DEFAULT);

    try {
      syncDispatcher.add(
          definition,
          generation.getGenerationId(),
          mock(ChangeStreamResumeInfo.class),
          indexManager,
          false);

      assertThat(getMoresScheduledTotal(meterRegistry)).isEqualTo(1.0);
      acceptedTask.get().run();
      assertThat(getMoresScheduledTotal(meterRegistry)).isEqualTo(0.0);
    } finally {
      syncDispatcher.shutdown();
    }
  }

  @Test
  public void testPreviousBatchFromRealGetMoreDrivesNextGetMoresRouting() throws Exception {
    var definition = SearchIndex.mockSearchDefinition(new ObjectId());
    IndexDefinitionGeneration generation =
        mockIndexGeneration(definition).getDefinitionGeneration();

    BsonTimestamp now = new BsonTimestamp(1_700_000_000, 0);
    ChangeStreamBatch caughtUpBatch =
        new ChangeStreamBatch(Collections.emptyList(), ChangeStreamUtils.resumeToken(now), now);
    BsonTimestamp laggingResumeToken = new BsonTimestamp(now.getTime() - 60, 0);
    ChangeStreamBatch highLagBatch =
        new ChangeStreamBatch(
            Collections.emptyList(), ChangeStreamUtils.resumeToken(laggingResumeToken), now);

    ChangeStreamMongoClient<SteadyStateException> mongoClient = mock(ChangeStreamMongoClient.class);
    when(mongoClient.getNext())
        .thenReturn(caughtUpBatch)
        .thenReturn(highLagBatch)
        .thenThrow(
            SteadyStateException.createTransient(
                new RuntimeException("stop after 3rd getMore"), "test-terminate"));

    ChangeStreamMongoClientFactory mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(mongoClient);

    ChangeStreamIndexManager indexManager =
        DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
            definition,
            mock(IndexingWorkScheduler.class),
            mockDocumentIndexer(),
            ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
            resumeInfo -> {},
            IGNORE_METRICS,
            new CompletableFuture<>(),
            generation.getGenerationId(),
            DecodingWorkScheduler.create(2, new SimpleMeterRegistry()));

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    // Threshold of 10s: caughtUpBatch (0s lag) is below it (regular), highLagBatch (60s lag) is
    // above it (preferred).
    ChangeStreamDualQueueExecutor taskExecutor =
        new ChangeStreamDualQueueExecutor(
            "test-dispatcher",
            1,
            meterRegistry,
            new OptimeLagPriorityStrategy(10),
            SteadyStateSchedulingMethod.OPTIME_LAG,
            4);
    SyncChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            meterRegistry, mongoClientFactory, taskExecutor, CommonReplicationConfig.Type.DEFAULT);

    try {
      CountDownLatch warmedUp = new CountDownLatch(1);
      taskExecutor.enqueue(
          warmedUp::countDown, new EnqueueContext(definition, Optional.empty(), () -> {}));
      assertThat(warmedUp.await(2, TimeUnit.SECONDS)).isTrue();

      syncDispatcher.add(
          definition,
          generation.getGenerationId(),
          mock(ChangeStreamResumeInfo.class),
          indexManager,
          false);

      verify(mongoClient, timeout(2000).times(3)).getNext();

      double prefCount =
          meterRegistry.get("test-dispatcher.tasksRoutedToPreferredQueue").counter().count();
      double regCount =
          meterRegistry.get("test-dispatcher.tasksRoutedToRegularQueue").counter().count();
      // getMore 1: no previousBatch, defaults to preferred.
      // getMore 2: routed off caughtUpBatch -> regular.
      // getMore 3: routed off highLagBatch -> preferred.
      assertThat(List.of(prefCount, regCount)).containsExactly(2.0, 1.0).inOrder();
    } finally {
      syncDispatcher.shutdown();
    }
  }

  @Test
  public void testPerIndexGetMoreDurationRecordedPerGetMore() throws Exception {
    var definition = SearchIndex.mockSearchDefinition(new ObjectId());
    IndexDefinitionGeneration generation =
        mockIndexGeneration(definition).getDefinitionGeneration();

    BsonTimestamp now = new BsonTimestamp(1_700_000_000, 0);
    ChangeStreamBatch emptyBatch =
        new ChangeStreamBatch(Collections.emptyList(), ChangeStreamUtils.resumeToken(now), now);

    ChangeStreamMongoClient<SteadyStateException> mongoClient = mock(ChangeStreamMongoClient.class);
    when(mongoClient.getNext())
        .thenReturn(emptyBatch)
        .thenReturn(emptyBatch)
        .thenThrow(
            SteadyStateException.createTransient(
                new RuntimeException("stop after 3rd getMore"), "test-terminate"));

    ChangeStreamMongoClientFactory mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(mongoClient);

    IndexMetricsUpdater metrics =
        IndexMetricsUpdaterBuilder.builder()
            .metricsFactory(SearchIndex.mockMetricsFactory())
            .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
            .build();

    ChangeStreamIndexManager indexManager =
        DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
            definition,
            mock(IndexingWorkScheduler.class),
            mockDocumentIndexer(),
            ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
            resumeInfo -> {},
            metrics,
            new CompletableFuture<>(),
            generation.getGenerationId(),
            DecodingWorkScheduler.create(2, new SimpleMeterRegistry()));

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    ChangeStreamDualQueueExecutor taskExecutor =
        new ChangeStreamDualQueueExecutor(
            "test-dispatcher",
            1,
            meterRegistry,
            new OptimeLagPriorityStrategy(10),
            SteadyStateSchedulingMethod.OPTIME_LAG,
            4);
    SyncChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            meterRegistry, mongoClientFactory, taskExecutor, CommonReplicationConfig.Type.DEFAULT);

    try {
      syncDispatcher.add(
          definition,
          generation.getGenerationId(),
          mock(ChangeStreamResumeInfo.class),
          indexManager,
          false);

      verify(mongoClient, timeout(2000).times(3)).getNext();

      // Each successful getMore records one per-index duration sample. The first two getNext calls
      // return; the third throws before the duration is recorded, so the count is 2.
      assertThat(
              metrics
                  .getReplicationMetricsUpdater()
                  .getSteadyStateMetrics()
                  .getPerIndexGetMoreTimer()
                  .count())
          .isEqualTo(2L);
    } finally {
      syncDispatcher.shutdown();
    }
  }

  @Test
  public void getNext_throttled_retriesSameCursorBeforeFailingLifecycle() throws Exception {
    var definition = SearchIndex.mockSearchDefinition(new ObjectId());
    IndexDefinitionGeneration generation =
        mockIndexGeneration(definition).getDefinitionGeneration();

    // Two throttled getMores, then an unrelated transient failure that is not retried in place.
    ChangeStreamMongoClient<SteadyStateException> mongoClient = mock(ChangeStreamMongoClient.class);
    when(mongoClient.getNext())
        .thenThrow(throttled(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code))
        .thenThrow(throttled(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code))
        .thenThrow(
            SteadyStateException.createTransient(
                new RuntimeException("not throttling"), "test-terminate"));

    ChangeStreamMongoClientFactory mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(mongoClient);

    ChangeStreamIndexManager indexManager =
        spy(indexManager(definition, generation, new CompletableFuture<>()));

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    SyncChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            meterRegistry,
            mongoClientFactory,
            new FifoExecutor(
                Executors.fixedSizeThreadScheduledExecutor(
                    "executor", 1, new SimpleMeterRegistry())),
            CommonReplicationConfig.Type.DEFAULT,
            /* retryThrottledGetMoreOnSameCursor= */ true,
            Sleeper.NO_OP);

    try {
      syncDispatcher.add(
          definition,
          generation.getGenerationId(),
          mock(ChangeStreamResumeInfo.class),
          indexManager,
          false);

      // Both rate-limited getMores are retried on the same cursor, so only the third,
      // non-throttling failure reaches the lifecycle.
      verify(indexManager, timeout(5000)).failLifecycle(any());
      verify(mongoClient, timeout(5000).times(3)).getNext();
      assertThat(throttlingRetries(meterRegistry)).isEqualTo(2.0);
    } finally {
      syncDispatcher.shutdown();
    }
  }

  @Test
  public void getNext_throttledWithFeatureDisabled_failsLifecycleImmediately() throws Exception {
    var definition = SearchIndex.mockSearchDefinition(new ObjectId());
    IndexDefinitionGeneration generation =
        mockIndexGeneration(definition).getDefinitionGeneration();

    ChangeStreamMongoClient<SteadyStateException> mongoClient = mock(ChangeStreamMongoClient.class);
    when(mongoClient.getNext())
        .thenThrow(throttled(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code));

    ChangeStreamMongoClientFactory mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(mongoClient);

    ChangeStreamIndexManager indexManager =
        spy(indexManager(definition, generation, new CompletableFuture<>()));

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    // Flag off (the convenience constructor defaults it to false).
    SyncChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            meterRegistry,
            mongoClientFactory,
            new FifoExecutor(
                Executors.fixedSizeThreadScheduledExecutor(
                    "executor", 1, new SimpleMeterRegistry())),
            CommonReplicationConfig.Type.DEFAULT);

    try {
      syncDispatcher.add(
          definition,
          generation.getGenerationId(),
          mock(ChangeStreamResumeInfo.class),
          indexManager,
          false);

      // With RETRY_THROTTLED_GETMORE_ON_SAME_CURSOR off, the throttled getMore takes the
      // pre-existing path straight away: one call, no retries, lifecycle failed. This is what
      // makes disabling the flag a behavioural revert to the state before CLOUDP-427401.
      verify(indexManager, timeout(5000)).failLifecycle(any());
      verify(mongoClient, timeout(5000).times(1)).getNext();
      assertThat(throttlingRetries(meterRegistry)).isEqualTo(0.0);
    } finally {
      syncDispatcher.shutdown();
    }
  }

  @Test
  public void getNext_throttlingPersists_failsLifecycleAfterRetriesExhausted() throws Exception {
    var definition = SearchIndex.mockSearchDefinition(new ObjectId());
    IndexDefinitionGeneration generation =
        mockIndexGeneration(definition).getDefinitionGeneration();

    ChangeStreamMongoClient<SteadyStateException> mongoClient = mock(ChangeStreamMongoClient.class);
    when(mongoClient.getNext())
        .thenThrow(throttled(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code));

    ChangeStreamMongoClientFactory mongoClientFactory = mock(ChangeStreamMongoClientFactory.class);
    when(mongoClientFactory.resumeTimedModeAwareChangeStream(any(), any(), any(), anyBoolean()))
        .thenReturn(mongoClient);

    ChangeStreamIndexManager indexManager =
        spy(indexManager(definition, generation, new CompletableFuture<>()));

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    SyncChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            meterRegistry,
            mongoClientFactory,
            new FifoExecutor(
                Executors.fixedSizeThreadScheduledExecutor(
                    "executor", 1, new SimpleMeterRegistry())),
            CommonReplicationConfig.Type.DEFAULT,
            /* retryThrottledGetMoreOnSameCursor= */ true,
            Sleeper.NO_OP);

    try {
      syncDispatcher.add(
          definition,
          generation.getGenerationId(),
          mock(ChangeStreamResumeInfo.class),
          indexManager,
          false);

      // The initial attempt plus MAX_THROTTLING_RETRIES retries, then we fall through to the
      // existing shutdown/retry path.
      verify(indexManager, timeout(5000)).failLifecycle(any());
      int expectedGetNextCalls = SyncChangeStreamDispatcher.MAX_THROTTLING_RETRIES + 1;
      verify(mongoClient, timeout(5000).times(expectedGetNextCalls)).getNext();
      verify(mongoClient, timeout(5000)).close();
      assertThat(throttlingRetries(meterRegistry))
          .isEqualTo((double) SyncChangeStreamDispatcher.MAX_THROTTLING_RETRIES);
    } finally {
      syncDispatcher.shutdown();
    }
  }

  private static SteadyStateException throttled(int code) {
    return SteadyStateException.wrapMongoException(
        new MongoCommandException(
            new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(code))
                .append("errmsg", new BsonString("throttled")),
            new ServerAddress()));
  }

  private static ChangeStreamIndexManager indexManager(
      SearchIndexDefinition definition,
      IndexDefinitionGeneration generation,
      CompletableFuture<Void> lifecycleFuture) {
    return DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
        definition,
        mock(IndexingWorkScheduler.class),
        mockDocumentIndexer(),
        ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
        resumeInfo -> {},
        IGNORE_METRICS,
        lifecycleFuture,
        generation.getGenerationId(),
        DecodingWorkScheduler.create(2, new SimpleMeterRegistry()));
  }

  private static double throttlingRetries(SimpleMeterRegistry meterRegistry) {
    return meterRegistry
        .get("indexing.steadyStateChangeStream.getMoreThrottlingRetries")
        .counter()
        .count();
  }

  private static void assertNumberOfEmbeddingPermits(
      SyncChangeStreamDispatcher dispatcher, int expectedPermits) {
    assertThat(dispatcher.getEmbeddingAvailablePermits()).isPresent();
    // The concurrentEmbeddingGetMores semaphore's permit is released in "finally" block of the call
    // to doGetMore(). It's possible that this executes after the indexingFuture completes, so we
    // wait up to 1 second for the permit to be released.
    try {
      Condition.await()
          .atMost(Duration.ofSeconds(1))
          .until(() -> dispatcher.getEmbeddingAvailablePermits().get() == expectedPermits);
    } catch (Exception e) {
      Assert.fail(
          "Expected "
              + expectedPermits
              + " embedding permits, but got "
              + dispatcher.getEmbeddingAvailablePermits().get());
    }
  }

  private static double getMoresScheduledTotal(SimpleMeterRegistry meterRegistry) {
    return meterRegistry
        .find("indexing.steadyStateChangeStream.getMoresScheduled")
        .gauges()
        .stream()
        .mapToDouble(Gauge::value)
        .sum();
  }
}
