package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.SessionRefresher.mockSessionRefresher;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.errorprone.annotations.Keep;
import com.mongodb.ConnectionString;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.replication.mongodb.common.ClientSessionRecord;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.metrics.SimpleMetricsFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

public class MaterializedViewManagerTest {
  private static final ObjectId INDEX_ID = new ObjectId();
  private static final MaterializedViewIndexDefinitionGeneration MOCK_INDEX_DEFINITION_GENERATION =
      mockMatViewDefinitionGeneration(INDEX_ID);
  private static final MaterializedViewGenerationId MOCK_MAT_VIEW_GENERATION_ID =
      MOCK_INDEX_DEFINITION_GENERATION.getGenerationId();
  private static final GenerationId MOCK_GENERATION_ID =
      new GenerationId(INDEX_ID, Generation.CURRENT);

  @Test
  public void testAddIndex() throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.addIndexForReplication(materializedViewindexGeneration);

    verify(mocks.leaseManager).add(materializedViewindexGeneration);
  }

  @Test
  public void testUpdateExistingIndexNewDefinitionVersion() throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var materializedViewindexGeneration =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    MaterializedViewGenerator materializedViewGenerator =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration);
    mocks.addIndexForReplication(materializedViewindexGeneration);

    // Add a new version of the same index.
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var newIndexGeneration = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2, 1));
    mocks.addIndexForReplication(newIndexGeneration);

    // Verify that the old MaterializedViewGenerator was shut down and the new one was added to the
    // lease manager.
    verify(materializedViewGenerator).shutdown();
    verify(mocks.leaseManager).add(materializedViewindexGeneration);
    verify(mocks.leaseManager).add(newIndexGeneration);
  }

  @Test
  public void testUpdateExistingIndexSameDefinitionVersion() throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var materializedViewindexGeneration =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    mocks.mockMaterializedViewGenerator(materializedViewindexGeneration);
    mocks.addIndexForReplication(materializedViewindexGeneration);
    verify(mocks.materializedViewGeneratorFactory).create(any(), any());

    // Add a new version of the same index.
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var newIndexGeneration = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2));
    mocks.mockMaterializedViewGenerator(newIndexGeneration);
    Mockito.clearInvocations(mocks.materializedViewGeneratorFactory);
    mocks.addIndexForReplication(newIndexGeneration);

    // Verify that only one MaterializedViewGenerator was created.
    verify(mocks.materializedViewGeneratorFactory, never()).create(any(), any());
    // Verify that both index generations point to the same index.
    assertEquals(materializedViewindexGeneration.getIndex(), newIndexGeneration.getIndex());
  }

  @Test
  public void testDropIndex() throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator materializedViewGenerator =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration);
    mocks.addIndexForReplication(materializedViewindexGeneration);

    // Drop it, ensure that MaterializedViewGenerator::drop was called.
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);
    verify(materializedViewGenerator).shutdown();
    Mockito.clearInvocations(materializedViewGenerator);
    // Drop again, verify does not get dropped again.
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);
    verify(materializedViewGenerator, never()).shutdown();
  }

  @Test
  public void testSameIndexCanNotBeAddedTwice() {
    Mocks mocks = Mocks.create();
    var index = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(index);
    mocks.addIndexForReplication(index);
    verify(mocks.materializedViewGeneratorFactory).create(any(), any());
    Mockito.clearInvocations(mocks.materializedViewGeneratorFactory);
    mocks.addIndexForReplication(index);
    verify(mocks.materializedViewGeneratorFactory, never()).create(any(), any());
  }

  @Test
  public void testCanNotInteractAfterShutDown() {
    Mocks mocks = Mocks.create();

    var index = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    mocks.mockMaterializedViewGenerator(index);

    mocks.addIndexForReplication(index);
    mocks.manager.shutdown();

    // can not add a new index or cancel existing
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            mocks.addIndexForReplication(
                mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION)));
    Assert.assertThrows(
        RuntimeException.class, () -> mocks.manager.dropIndex(index.getGenerationId()));
  }

  @Test
  public void testShutdownNoIndexes() throws Exception {
    Mocks mocks = Mocks.create();

    mocks.manager.shutdown().get(5, TimeUnit.SECONDS);

    verify(mocks.initialSyncQueue).shutdown();
    verify(mocks.initialSyncQueue).shutdown();
    verify(mocks.steadyStateManager).shutdown();
    verify(mocks.executorService).shutdown();
    verify(mocks.clientSessionRecordMap.get("test").sessionRefresher()).shutdown();
  }

  @Test
  public void testShutdownWithIndexes() throws Exception {
    Mocks mocks = Mocks.create();

    MaterializedViewIndexGeneration materializedViewindexGeneration1 =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    // Add two different indexes to the AutoEmbeddingMaterializedViewManager.
    MaterializedViewGenerator materializedViewGenerator1 =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration1);
    mocks.addIndexForReplication(materializedViewindexGeneration1);

    MaterializedViewIndexGeneration materializedViewindexGeneration2 =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator materializedViewGenerator2 =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration2);
    mocks.addIndexForReplication(materializedViewindexGeneration2);

    // Ensure that shutdown() shuts down both of the MaterializedViewGenerator.
    mocks.manager.shutdown().get(5, TimeUnit.SECONDS);
    verify(materializedViewGenerator1).shutdown();
    verify(materializedViewGenerator2).shutdown();
    verify(mocks.initialSyncQueue).shutdown();
    verify(mocks.steadyStateManager).shutdown();
    verify(mocks.commitExecutor).shutdown();
    verify(mocks.executorService).shutdown();
    verify(mocks.clientSessionRecordMap.get("test").sessionRefresher()).shutdown();
  }

  @Test
  public void testShutdownTwoIndexesWithSameIdShutsBothDown() throws Exception {
    Mocks mocks = Mocks.create();
    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var index1 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    var index2 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2, 1));
    assertEquals(gen1.indexId, gen2.indexId);

    // Add two different indexes to the AutoEmbeddingMaterializedViewManager.
    MaterializedViewGenerator materializedViewGenerator1 =
        mocks.mockMaterializedViewGenerator(index1);
    MaterializedViewGenerator materializedViewGenerator2 =
        mocks.mockMaterializedViewGenerator(index2);

    mocks.addIndexForReplication(index1);
    mocks.addIndexForReplication(index2);

    // Ensure that shutdown() shuts down both of the MaterializedViewGenerator.
    mocks.manager.shutdown().get(5, TimeUnit.SECONDS);
    verify(materializedViewGenerator1).shutdown();
    verify(materializedViewGenerator2).shutdown();
  }

  @Test
  // This test verifies that task shutdown and metrics deregistration work together correctly.
  //
  // Multiple indexes are actively replicating and each has its own manager with background tasks.
  // When mongot shuts down, all these components must shut down gracefully.
  public void testExecutorShutdown() throws Exception {
    Mocks mocks = Mocks.create();

    MaterializedViewIndexGeneration materializedViewindexGeneration1 =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator materializedViewGenerator1 =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration1);
    mocks.addIndexForReplication(materializedViewindexGeneration1);

    MaterializedViewIndexGeneration materializedViewindexGeneration2 =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator materializedViewGenerator2 =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration2);
    mocks.addIndexForReplication(materializedViewindexGeneration2);

    MaterializedViewIndexGeneration materializedViewindexGeneration3 =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator materializedViewGenerator3 =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration3);
    mocks.addIndexForReplication(materializedViewindexGeneration3);

    // Trigger shutdown. ExecutorService could shut down before the shutdown tasks complete,
    // causing metrics to be deregistered while tasks are still running. So, wait for the shutdown
    // future to complete and expect it to complete successfully, i.e., no exceptions.
    CompletableFuture<Void> shutdownFuture = mocks.manager.shutdown();
    shutdownFuture.get(5, TimeUnit.SECONDS);

    // Verify all index managers were properly shut down
    verify(materializedViewGenerator1).shutdown();
    for (MaterializedViewGenerator materializedViewGenerator :
        Arrays.asList(materializedViewGenerator2, materializedViewGenerator3)) {
      verify(materializedViewGenerator).shutdown();
    }

    // Verify all components were shut down
    verify(mocks.initialSyncQueue).shutdown();
    verify(mocks.steadyStateManager).shutdown();
    verify(mocks.commitExecutor).shutdown();
    verify(mocks.executorService).shutdown();
    verify(mocks.clientSessionRecordMap.get("test").sessionRefresher()).shutdown();
  }

  @Test
  public void testAutoEmbeddingManagerLaterInitialized() {
    Mocks mocks = Mocks.create();
    CompletableFuture<Void> future = new CompletableFuture<>();
    mocks.addMockMaterializedViewGenerator(future);
    assertFalse(mocks.manager.isInitialized());
    future.complete(null);
    assertTrue(mocks.manager.isInitialized());
  }

  @Test
  public void testGetAutoEmbeddingInitStateNoIndexes() {
    Mocks mocks = Mocks.create();
    assertTrue(mocks.manager.isInitialized());
  }

  @Test
  public void testGetAutoEmbeddingInitStateAllInitialized() {
    Mocks mocks = Mocks.create();
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    assertTrue(mocks.manager.isInitialized());
  }

  @Test
  public void testGetAutoEmbeddingInitStateSomeNotInitialized() {
    Mocks mocks = Mocks.create();
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    mocks.addMockMaterializedViewGenerator(new CompletableFuture<>());
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testGetAutoEmbeddingInitStateSomeNotInitialized2() {
    Mocks mocks = Mocks.create();
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    mocks.addMockMaterializedViewGenerator(new CompletableFuture<>());
    mocks.addMockMaterializedViewGenerator(new CompletableFuture<>());
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testGetAutoEmbeddingInitStateSomeCompleteSomeFailed() {
    Mocks mocks = Mocks.create();
    mocks.addMockMaterializedViewGenerator(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    mocks.addMockMaterializedViewGenerator(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testGetAutoEmbeddingInitStateAllFailed() {
    Mocks mocks = Mocks.create();
    mocks.addMockMaterializedViewGenerator(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    mocks.addMockMaterializedViewGenerator(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    mocks.addMockMaterializedViewGenerator(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testHeartbeatLogIsEmitted() throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(MaterializedViewManager.class);
    List<ILoggingEvent> logEvents = TestUtils.getLogEvents(logger);

    Mocks mocks = Mocks.create();

    // Verify the startup log is emitted
    boolean foundStartupLog =
        new ArrayList<>(logEvents).stream()
            .anyMatch(
                event ->
                    event
                        .getFormattedMessage()
                        .contains("Starting auto-embedding leader heartbeat"));
    assertTrue("Expected heartbeat startup log to be emitted", foundStartupLog);

    // Capture the scheduled heartbeat task and execute it to verify the actual heartbeat log
    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(
            runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

    // Execute the captured heartbeat task
    runnableCaptor.getValue().run();

    // Verify the actual heartbeat log is emitted
    boolean foundHeartbeatLog =
        logEvents.stream()
            .anyMatch(
                event -> event.getFormattedMessage().equals("Auto-embedding leader heartbeat"));
    assertTrue("Expected heartbeat log to be emitted", foundHeartbeatLog);
  }

  static class Mocks {
    final NamedExecutorService executorService;
    @Keep final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;
    @Keep final MongotCursorManager cursorManager;
    final Map<String, ClientSessionRecord> clientSessionRecordMap;
    final InitialSyncQueue initialSyncQueue;
    final SteadyStateManager steadyStateManager;
    final MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory;
    final NamedScheduledExecutorService commitExecutor;
    final NamedScheduledExecutorService heartbeatExecutor;
    final NamedScheduledExecutorService optimeUpdaterExecutor;
    final Supplier<MaterializedViewManager> managerSupplier;
    final DecodingWorkScheduler decodingScheduler;
    final MeterRegistry meterRegistry;
    final LeaseManager leaseManager;

    MaterializedViewManager manager;

    private Mocks(
        NamedExecutorService executorService,
        IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
        DecodingWorkScheduler decodingScheduler,
        MongotCursorManager cursorManager,
        Map<String, ClientSessionRecord> clientSessionRecordMap,
        InitialSyncQueue initialSyncQueue,
        SteadyStateManager steadyStateManager,
        BatchMongoClient syncBatchMongoClient,
        MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory,
        NamedScheduledExecutorService commitExecutor,
        NamedScheduledExecutorService heartbeatExecutor,
        NamedScheduledExecutorService optimeUpdaterExecutor,
        LeaseManager leaseManager) {
      this.executorService = executorService;
      this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
      this.cursorManager = cursorManager;
      this.clientSessionRecordMap = clientSessionRecordMap;
      this.initialSyncQueue = initialSyncQueue;
      this.steadyStateManager = steadyStateManager;
      this.materializedViewGeneratorFactory = materializedViewGeneratorFactory;
      this.commitExecutor = commitExecutor;
      this.heartbeatExecutor = heartbeatExecutor;
      this.optimeUpdaterExecutor = optimeUpdaterExecutor;
      this.decodingScheduler = decodingScheduler;
      this.meterRegistry = new SimpleMeterRegistry();
      this.managerSupplier =
          () ->
              new MaterializedViewManager(
                  executorService,
                  indexingWorkSchedulerFactory,
                  clientSessionRecordMap,
                  new SyncSourceConfig(
                      new ConnectionString("mongodb://newString"),
                      Optional.empty(),
                      new ConnectionString("mongodb://newString")),
                  initialSyncQueue,
                  steadyStateManager,
                  syncBatchMongoClient,
                  decodingScheduler,
                  materializedViewGeneratorFactory,
                  commitExecutor,
                  heartbeatExecutor,
                  optimeUpdaterExecutor,
                  this.meterRegistry,
                  leaseManager);
      this.manager = this.managerSupplier.get();
      this.leaseManager = leaseManager;
    }

    public void recreateManager() {
      this.manager = this.managerSupplier.get();
    }

    private static Mocks create() {
      NamedExecutorService executorService =
          spy(Executors.fixedSizeThreadPool("indexing", 1, new SimpleMeterRegistry()));

      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
          mock(IndexingWorkSchedulerFactory.class);

      DecodingWorkScheduler decodingWorkScheduler = mock(DecodingWorkScheduler.class);

      MongotCursorManager cursorManager = mock(MongotCursorManager.class);
      SessionRefresher sessionRefresher = mockSessionRefresher();

      InitialSyncQueue initialSyncQueue = mock(InitialSyncQueue.class);
      when(initialSyncQueue.shutdown()).thenReturn(COMPLETED_FUTURE);

      SteadyStateManager steadyStateManager = mock(SteadyStateManager.class);
      when(steadyStateManager.shutdown()).thenReturn(COMPLETED_FUTURE);

      com.mongodb.client.MongoClient syncMongoClient = mock(com.mongodb.client.MongoClient.class);

      BatchMongoClient syncBatchMongoClient = mock(BatchMongoClient.class);

      MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory =
          mock(MaterializedViewManager.MaterializedViewGeneratorFactory.class);

      NamedScheduledExecutorService commitExecutor =
          spy(
              Executors.fixedSizeThreadScheduledExecutor(
                  "index-commit", 1, new SimpleMeterRegistry()));

      NamedScheduledExecutorService heartbeatExecutor =
          spy(
              Executors.singleThreadScheduledExecutor(
                  "mat-view-leader-heartbeat", new SimpleMeterRegistry()));

      NamedScheduledExecutorService optimeUpdaterExecutor =
          spy(
              Executors.singleThreadScheduledExecutor(
                  "mat-view-optime-updater", new SimpleMeterRegistry()));

      InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
      when(initializedIndexCatalog.getIndex(any()))
          .thenReturn(Optional.of(mock(InitializedIndex.class)));

      LeaseManager leaseManager = mock(LeaseManager.class);
      when(leaseManager.drop(any())).thenReturn(COMPLETED_FUTURE);

      return new Mocks(
          executorService,
          indexingWorkSchedulerFactory,
          decodingWorkScheduler,
          cursorManager,
          Map.of("test", new ClientSessionRecord(syncMongoClient, sessionRefresher)),
          initialSyncQueue,
          steadyStateManager,
          syncBatchMongoClient,
          materializedViewGeneratorFactory,
          commitExecutor,
          heartbeatExecutor,
          optimeUpdaterExecutor,
          leaseManager);
    }

    private MaterializedViewGenerator mockMaterializedViewGenerator(
        MaterializedViewIndexGeneration materializedViewIndexGeneration) {
      var materializedViewGenerator =
          spy(
              new MaterializedViewGenerator(
                  this.executorService,
                  this.cursorManager,
                  this.initialSyncQueue,
                  this.steadyStateManager,
                  materializedViewIndexGeneration,
                  mock(InitializedIndex.class),
                  mock(DocumentIndexer.class),
                  mock(PeriodicIndexCommitter.class),
                  new SimpleMetricsFactory(),
                  mock(FeatureFlags.class),
                  Duration.ofSeconds(1),
                  Duration.ofSeconds(1),
                  Duration.ofSeconds(1),
                  false));
      when(this.materializedViewGeneratorFactory.create(eq(materializedViewIndexGeneration), any()))
          .thenReturn(materializedViewGenerator);
      return materializedViewGenerator;
    }

    private void addMockMaterializedViewGenerator(CompletableFuture<Void> initFuture) {
      var mockMatViewIndexGeneration =
          mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
      when(mockMaterializedViewGenerator(mockMatViewIndexGeneration).getInitFuture())
          .thenReturn(initFuture);
      addIndexForReplication(mockMatViewIndexGeneration);
    }

    private void addIndexForReplication(
        MaterializedViewIndexGeneration materializedViewindexGeneration) {
      AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
          mock(AutoEmbeddingIndexGeneration.class);
      when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
          .thenReturn(materializedViewindexGeneration);
      when(autoEmbeddingIndexGeneration.getGenerationId())
          .thenReturn(
              new GenerationId(
                  materializedViewindexGeneration.getGenerationId().indexId,
                  new Generation(
                      materializedViewindexGeneration.getGenerationId().generation.userIndexVersion,
                      IndexFormatVersion.CURRENT)));
      this.manager.add(autoEmbeddingIndexGeneration);
    }
  }
}
