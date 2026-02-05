package com.xgen.mongot.replication.mongodb;

import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockInitializedIndex;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.SessionRefresher.mockSessionRefresher;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Keep;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ClientSessionRecord;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.ReplicationOptimeUpdater;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymManager;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class MongoDbReplicationManagerTest {

  @Test
  public void testDropIndex() throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    ReplicationIndexManager replicationIndexManager = mocks.mockReplicationIndexManager();
    IndexGeneration indexGeneration = mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);

    mocks.addIndexForReplication(indexGeneration);

    // Drop it, ensure that ReplicationIndexManager::drop was called.
    mocks.manager.dropIndex(MOCK_INDEX_GENERATION_ID).get(5, TimeUnit.SECONDS);
    verify(replicationIndexManager).drop();
    Mockito.clearInvocations(replicationIndexManager);
    // Drop again, verify does not get dropped again.
    mocks.manager.dropIndex(MOCK_INDEX_GENERATION_ID).get(5, TimeUnit.SECONDS);
    verify(replicationIndexManager, never()).drop();
  }

  @Test
  public void testSameIndexCanNotBeAddedTwice() {
    Mocks mocks = Mocks.create();
    var index = mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockReplicationIndexManager();
    mocks.addIndexForReplication(index);
    verify(mocks.replicationIndexManagerFactory)
        .create(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            anyBoolean());
    Mockito.clearInvocations(mocks.replicationIndexManagerFactory);
    verify(mocks.replicationIndexManagerFactory, never())
        .create(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            anyBoolean());
  }

  @Test
  public void testCanNotInteractAfterShutDown() {
    Mocks mocks = Mocks.create();
    mocks.mockReplicationIndexManager();

    var index =
        mockIndexGeneration(
            mockDefinitionGeneration(SearchIndex.mockSearchDefinition(new ObjectId())));

    mocks.addIndexForReplication(index);
    mocks.manager.shutdown();

    // can not add a new index or cancel existing
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            mocks.addIndexForReplication(
                mockIndexGeneration(
                    mockDefinitionGeneration(SearchIndex.mockSearchDefinition(new ObjectId())))));
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
    verify(mocks.synonymManager).shutdown();
    verify(mocks.executorService).shutdown();
    verify(mocks.clientSessionRecordMap.get("test").sessionRefresher()).shutdown();
  }

  @Test
  public void testShutdownWithIndexes() throws Exception {
    Mocks mocks = Mocks.create();

    // Add two different indexes to the MongoDbReplicationManager.
    ReplicationIndexManager replicationIndexManager1 = mocks.mockReplicationIndexManager();

    SearchIndexDefinition indexDefinition1 =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("1")
            .database("foo")
            .lastObservedCollectionName("bar")
            .collectionUuid(UUID.randomUUID())
            .mappings(DocumentFieldDefinitionBuilder.builder().build())
            .build();
    IndexGeneration indexGeneration =
        mockIndexGeneration(mockDefinitionGeneration(indexDefinition1));

    mocks.addIndexForReplication(indexGeneration);

    ReplicationIndexManager replicationIndexManager2 = mocks.mockReplicationIndexManager();

    SearchIndexDefinition indexDefinition2 =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("2")
            .database("foo")
            .lastObservedCollectionName("bar")
            .collectionUuid(UUID.randomUUID())
            .mappings(DocumentFieldDefinitionBuilder.builder().build())
            .build();
    mocks.addIndexForReplication(mockIndexGeneration(mockDefinitionGeneration(indexDefinition2)));

    // Ensure that shutdown() shuts down both of the IndexLifecycleManagers.
    mocks.manager.shutdown().get(5, TimeUnit.SECONDS);
    verify(replicationIndexManager1).shutdown();
    verify(replicationIndexManager2).shutdown();
    verify(mocks.initialSyncQueue).shutdown();
    verify(mocks.steadyStateManager).shutdown();
    verify(mocks.synonymManager).shutdown();
    verify(mocks.commitExecutor).shutdown();
    verify(mocks.executorService).shutdown();
    verify(mocks.clientSessionRecordMap.get("test").sessionRefresher()).shutdown();
  }

  @Test
  public void testShutdownTwoIndexesWithSameIdShutsBothDown() throws Exception {
    Mocks mocks = Mocks.create();
    var gen1 = MOCK_INDEX_GENERATION_ID;
    var gen2 = GenerationIdBuilder.incrementUser(gen1);
    var index1 = mockIndexGeneration(mockDefinitionGeneration(gen1));
    var index2 = mockIndexGeneration(mockDefinitionGeneration(gen2));
    Assert.assertEquals(gen1.indexId, gen2.indexId);

    // Add two different indexes to the MongoDbReplicationManager.
    ReplicationIndexManager replicationIndexManager1 = mocks.mockReplicationIndexManager(index1);
    ReplicationIndexManager replicationIndexManager2 = mocks.mockReplicationIndexManager(index2);

    mocks.addIndexForReplication(index1);
    mocks.addIndexForReplication(index2);

    // Ensure that shutdown() shuts down both of the IndexLifecycleManagers.
    mocks.manager.shutdown().get(5, TimeUnit.SECONDS);
    verify(replicationIndexManager1).shutdown();
    verify(replicationIndexManager2).shutdown();
  }

  @Test
  public void testShardedShutdownClosesMongosClients() throws Exception {
    Mocks shardedMocks = Mocks.createSharded();
    shardedMocks.manager.shutdown().get(5, TimeUnit.SECONDS);

    verify(shardedMocks.initialSyncQueue).shutdown();
    verify(shardedMocks.steadyStateManager).shutdown();
    verify(shardedMocks.synonymManager).shutdown();
    verify(shardedMocks.executorService).shutdown();
    verify(shardedMocks.clientSessionRecordMap.get("test").sessionRefresher()).shutdown();
    Check.isPresent(shardedMocks.synonymsSessionRefresher, "synonymsSessionRefresher");
    verify(shardedMocks.synonymsSessionRefresher.get()).shutdown();
    shardedMocks
        .indexingWorkSchedulerFactory
        .getIndexingWorkSchedulers()
        .forEach((strategy, scheduler) -> verify(scheduler).shutdown());
    verify(shardedMocks.decodingScheduler).shutdown();
  }

  @Test
  // This test verifies that task shutdown and metrics deregistration work together correctly.
  //
  // Multiple indexes are actively replicating and each has its own manager with background tasks.
  // When mongot shuts down, all these components must shut down gracefully.
  public void testExecutorShutdown() throws Exception {
    Mocks mocks = Mocks.create();
    MeterRegistry meterRegistry = mocks.meterRegistry;

    // Add multiple indexes to the MongoDbReplicationManager.
    SearchIndexDefinition indexDefinition1 =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("index1")
            .database("foo")
            .lastObservedCollectionName("bar")
            .collectionUuid(UUID.randomUUID())
            .mappings(DocumentFieldDefinitionBuilder.builder().build())
            .build();
    IndexGeneration indexGeneration1 =
        mockIndexGeneration(mockDefinitionGeneration(indexDefinition1));
    ReplicationIndexManager replicationIndexManager1 =
        mocks.mockReplicationIndexManager(indexGeneration1);
    mocks.addIndexForReplication(indexGeneration1);

    SearchIndexDefinition indexDefinition2 =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("index2")
            .database("foo")
            .lastObservedCollectionName("bar")
            .collectionUuid(UUID.randomUUID())
            .mappings(DocumentFieldDefinitionBuilder.builder().build())
            .build();
    IndexGeneration indexGeneration2 =
        mockIndexGeneration(mockDefinitionGeneration(indexDefinition2));
    ReplicationIndexManager replicationIndexManager2 =
        mocks.mockReplicationIndexManager(indexGeneration2);
    mocks.addIndexForReplication(indexGeneration2);

    SearchIndexDefinition indexDefinition3 =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("index3")
            .database("foo")
            .lastObservedCollectionName("bar")
            .collectionUuid(UUID.randomUUID())
            .mappings(DocumentFieldDefinitionBuilder.builder().build())
            .build();
    IndexGeneration indexGeneration3 =
        mockIndexGeneration(mockDefinitionGeneration(indexDefinition3));
    ReplicationIndexManager replicationIndexManager3 =
        mocks.mockReplicationIndexManager(indexGeneration3);
    mocks.addIndexForReplication(indexGeneration3);

    // Verify metrics exist before shutdown
    assertTrue(
        "Metrics should exist before shutdown", mocks.indexReplicationManagerGaugeMetricsExist());

    // Count metrics before shutdown before the actual verification.
    int metricsBeforeShutdown = meterRegistry.getMeters().size();
    assertTrue("Should have metrics registered", metricsBeforeShutdown > 0);

    // Trigger shutdown. ExecutorService could shut down before the shutdown tasks complete,
    // causing metrics to be deregistered while tasks are still running. So, wait for the shutdown
    // future to complete and expect it to complete successfully, i.e., no exceptions.
    CompletableFuture<Void> shutdownFuture = mocks.manager.shutdown();
    shutdownFuture.get(5, TimeUnit.SECONDS);

    // Verify all index managers were properly shut down
    verify(replicationIndexManager1).shutdown();
    for (ReplicationIndexManager replicationIndexManager :
        Arrays.asList(replicationIndexManager2, replicationIndexManager3)) {
      verify(replicationIndexManager).shutdown();
    }

    // Verify all components were shut down
    verify(mocks.initialSyncQueue).shutdown();
    verify(mocks.steadyStateManager).shutdown();
    verify(mocks.synonymManager).shutdown();
    verify(mocks.commitExecutor).shutdown();
    verify(mocks.executorService).shutdown();
    verify(mocks.clientSessionRecordMap.get("test").sessionRefresher()).shutdown();
  }

  @Test
  // This test verifies that task shutdown and metrics deregistration do not race and this test
  // is more of a stress test while testExecutorShutdown verifies the correctness of the shutdown
  // in isolation.
  //
  // The race condition occurred when:
  // 1. Shutdown tasks were running on executors
  // 2. The executors themselves were shut down before tasks completed
  // 3. Metrics were deregistered while tasks were still trying to update them
  public void testShutdownMetricsCompletesSuccessfully() throws Exception {
    for (int iteration = 0; iteration < 10; iteration++) {
      Mocks mocks = Mocks.create();

      // Verify metrics before.
      assertFalse("Should have metrics registered", mocks.meterRegistry.getMeters().isEmpty());

      // Add an index
      SearchIndexDefinition indexDefinition =
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId())
              .name("stress-test-" + iteration)
              .database("foo")
              .lastObservedCollectionName("bar")
              .collectionUuid(UUID.randomUUID())
              .mappings(DocumentFieldDefinitionBuilder.builder().build())
              .build();
      IndexGeneration indexGeneration =
          mockIndexGeneration(mockDefinitionGeneration(indexDefinition));
      ReplicationIndexManager replicationIndexManager =
          mocks.mockReplicationIndexManager(indexGeneration);
      mocks.addIndexForReplication(indexGeneration);

      // Trigger shutdown. ExecutorService could shut down before the shutdown tasks complete,
      // causing metrics to be deregistered while tasks are still running. So, wait for the shutdown
      // future to complete and expect it to complete successfully, i.e., no exceptions.
      CompletableFuture<Void> shutdownFuture = mocks.manager.shutdown();
      shutdownFuture.get(5, TimeUnit.SECONDS);
      verify(replicationIndexManager).shutdown();

      // Verify all components were shut down
      verify(mocks.initialSyncQueue).shutdown();
      verify(mocks.steadyStateManager).shutdown();
      verify(mocks.synonymManager).shutdown();
      verify(mocks.commitExecutor).shutdown();
      verify(mocks.executorService).shutdown();
      verify(mocks.clientSessionRecordMap.get("test").sessionRefresher()).shutdown();

      // Verify metrics are deregister
      assertTrue(
          "All metrics should be deregistered after shutdown",
          mocks.meterRegistry.getMeters().isEmpty());
    }
  }

  @Test
  public void testRemoveOneOfTwoIndexesWithSameIndexId() throws Exception {
    Mocks mocks = Mocks.create();
    var gen1 = MOCK_INDEX_GENERATION_ID;
    var gen2 = GenerationIdBuilder.incrementUser(gen1);
    var index1 = mockIndexGeneration(mockDefinitionGeneration(gen1));
    var index2 = mockIndexGeneration(mockDefinitionGeneration(gen2));

    // Add two different indexes to the MongoDbReplicationManager.
    // Add two different indexes to the MongoDbReplicationManager.
    ReplicationIndexManager replicationIndexManager1 = mocks.mockReplicationIndexManager(index1);
    ReplicationIndexManager replicationIndexManager2 = mocks.mockReplicationIndexManager(index2);

    mocks.addIndexForReplication(index1);
    mocks.addIndexForReplication(index2);
    clearInvocations(replicationIndexManager2);

    mocks.manager.dropIndex(gen1).get();

    // only the first should drop but not the second
    verify(replicationIndexManager1).drop();
    verifyNoMoreInteractions(replicationIndexManager2);
  }

  @Test
  public void testReplicationManagerLaterInitialized() {
    Mocks mocks = Mocks.create();
    CompletableFuture<Void> future = new CompletableFuture<>();
    mocks.addMockReplicationIndexManager(future);
    assertFalse(mocks.manager.isInitialized());
    future.complete(null);
    assertTrue(mocks.manager.isInitialized());
  }

  @Test
  public void testGetReplicationInitStateNoIndexes() {
    Mocks mocks = Mocks.create();
    assertTrue(mocks.manager.isInitialized());
  }

  @Test
  public void testGetReplicationInitStateAllInitialized() {
    Mocks mocks = Mocks.create();
    mocks.addMockReplicationIndexManager(CompletableFuture.completedFuture(null));
    mocks.addMockReplicationIndexManager(CompletableFuture.completedFuture(null));
    mocks.addMockReplicationIndexManager(CompletableFuture.completedFuture(null));
    assertTrue(mocks.manager.isInitialized());
  }

  @Test
  public void testGetReplicationInitStateSomeNotInitialized() {
    Mocks mocks = Mocks.create();
    mocks.addMockReplicationIndexManager(new CompletableFuture<>());
    mocks.addMockReplicationIndexManager(CompletableFuture.completedFuture(null));
    mocks.addMockReplicationIndexManager(new CompletableFuture<>());
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testGetReplicationInitStateSomeNotInitialized2() {
    Mocks mocks = Mocks.create();
    mocks.addMockReplicationIndexManager(CompletableFuture.completedFuture(null));
    mocks.addMockReplicationIndexManager(new CompletableFuture<>());
    mocks.addMockReplicationIndexManager(new CompletableFuture<>());
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testGetReplicationInitStateSomeCompleteSomeFailed() {
    Mocks mocks = Mocks.create();
    mocks.addMockReplicationIndexManager(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    mocks.addMockReplicationIndexManager(CompletableFuture.completedFuture(null));
    mocks.addMockReplicationIndexManager(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testGetReplicationInitStateAllFailed() {
    Mocks mocks = Mocks.create();
    mocks.addMockReplicationIndexManager(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    mocks.addMockReplicationIndexManager(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    mocks.addMockReplicationIndexManager(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testGetReplicationInitStateMixed() {
    Mocks mocks = Mocks.create();
    mocks.addMockReplicationIndexManager(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    mocks.addMockReplicationIndexManager(CompletableFuture.completedFuture(null));
    mocks.addMockReplicationIndexManager(new CompletableFuture<>());
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testIndexReplicationMetric() {
    Mocks mocks = Mocks.create();

    assertTrue(mocks.indexReplicationManagerGaugeMetricsExist());

    // check that all gauges have 0 by default
    for (ReplicationIndexManager.State state : ReplicationIndexManager.State.values()) {
      mocks.waitForIndexReplicationManagerGaugeMetric(state.name(), 0);
    }
  }

  @Test
  public void testShutdownAfterNewManagerCreated() {
    Mocks mocks = Mocks.create();

    assertTrue(mocks.indexReplicationManagerGaugeMetricsExist());

    // There could be a race condition where a new replication manager is created
    // before the previous was fully shut down
    mocks.recreateManager();
    assertTrue(mocks.indexReplicationManagerGaugeMetricsExist());

    mocks.manager.shutdown().join();
    assertFalse(mocks.indexReplicationManagerGaugeMetricsExist());
  }

  @Test
  public void testGetClientSessionRecords() {
    SyncSourceConfig syncSourceConfig1 =
        new SyncSourceConfig(
            new ConnectionString("mongodb://localhost1:27017"),
            Optional.of(
                Map.of(
                    "localhost1",
                    new ConnectionString("mongodb://localhost1:27017"),
                    "localhost2",
                    new ConnectionString("mongodb://localhost2:27018"))),
            Optional.empty(),
            new ConnectionString("mongodb://localhost1:27017"));

    var result1 =
        MongoDbReplicationManager.getClientSessionRecords(
            syncSourceConfig1,
            MongoDbReplicationManager.getSyncMaxConnections(
                syncSourceConfig1, MongoDbReplicationConfig.getDefault()),
            new SimpleMeterRegistry(),
            spy(Executors.fixedSizeThreadScheduledExecutor("test", 1, new SimpleMeterRegistry())),
            "localhost1");
    Assert.assertEquals(2, result1.size());
    Assert.assertTrue(result1.containsKey("localhost1"));
    Assert.assertTrue(result1.containsKey("localhost2"));

    SyncSourceConfig syncSourceConfig2 =
        new SyncSourceConfig(
            new ConnectionString("mongodb://localhost1:27017"),
            Optional.of(
                Map.of(
                    "localhost2",
                    new ConnectionString("mongodb://localhost2:27017"),
                    "localhost3",
                    new ConnectionString("mongodb://localhost3:27018"))),
            Optional.empty(),
            new ConnectionString("mongodb://localhost1:27017"));

    var result2 =
        MongoDbReplicationManager.getClientSessionRecords(
            syncSourceConfig2,
            MongoDbReplicationManager.getSyncMaxConnections(
                syncSourceConfig2, MongoDbReplicationConfig.getDefault()),
            new SimpleMeterRegistry(),
            spy(Executors.fixedSizeThreadScheduledExecutor("test", 1, new SimpleMeterRegistry())),
            MongoDbReplicationManager.getSyncSourceHost(syncSourceConfig2));
    Assert.assertEquals(3, result2.size());
    Assert.assertTrue(result2.containsKey("localhost1"));
    Assert.assertTrue(result2.containsKey("localhost2"));
    Assert.assertTrue(result2.containsKey("localhost3"));
  }

  @Test
  public void testGetSyncSourceHost() {
    // when there is a match
    Assert.assertEquals(
        "localhost1",
        MongoDbReplicationManager.getSyncSourceHost(
            new SyncSourceConfig(
                new ConnectionString("mongodb://localhost1:27017"),
                Optional.of(
                    Map.of(
                        "localhost1",
                        new ConnectionString("mongodb://localhost1:27017"),
                        "localhost2",
                        new ConnectionString("mongodb://localhost2:27018"))),
                Optional.empty(),
                new ConnectionString("mongodb://localhost1:27017"))));

    // when connecting string mapping is empty
    Assert.assertEquals(
        "localhost1",
        MongoDbReplicationManager.getSyncSourceHost(
            new SyncSourceConfig(
                new ConnectionString("mongodb://localhost1:27017"),
                Optional.empty(),
                Optional.empty(),
                new ConnectionString("mongodb://localhost1:27017"))));

    // when there is no match
    SyncSourceConfig syncSourceConfig3 =
        new SyncSourceConfig(
            new ConnectionString("mongodb://localhost1:27017"),
            Optional.of(
                Map.of(
                    "localhost2",
                    new ConnectionString("mongodb://localhost2:27017"),
                    "localhost3",
                    new ConnectionString("mongodb://localhost3:27018"))),
            Optional.empty(),
            new ConnectionString("mongodb://localhost1:27017"));

    Assert.assertEquals(
        "localhost1", MongoDbReplicationManager.getSyncSourceHost(syncSourceConfig3));
  }

  private static class Mocks {
    final NamedExecutorService executorService;
    @Keep final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;
    @Keep final MongotCursorManager cursorManager;
    final Map<String, ClientSessionRecord> clientSessionRecordMap;
    final Optional<SessionRefresher> synonymsSessionRefresher;
    final InitialSyncQueue initialSyncQueue;
    final SteadyStateManager steadyStateManager;
    final SynonymManager synonymManager;
    final ReplicationIndexManagerFactory replicationIndexManagerFactory;
    @Keep final Map<GenerationId, ReplicationIndexManager> lifecycleManagers;
    final NamedScheduledExecutorService commitExecutor;
    final Supplier<MongoDbReplicationManager> managerSupplier;
    MongoDbReplicationManager manager;
    final DecodingWorkScheduler decodingScheduler;
    final InitializedIndexCatalog initializedIndexCatalog;
    final MeterRegistry meterRegistry;

    private Mocks(
        NamedExecutorService executorService,
        IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
        DecodingWorkScheduler decodingScheduler,
        MongotCursorManager cursorManager,
        Map<String, ClientSessionRecord> clientSessionRecordMap,
        InitialSyncQueue initialSyncQueue,
        SteadyStateManager steadyStateManager,
        SynonymManager synonymManager,
        BatchMongoClient syncBatchMongoClient,
        Optional<MongoClient> synonymsSyncMongoClient,
        Optional<SessionRefresher> synonymsSessionRefresher,
        ReplicationIndexManagerFactory replicationIndexManagerFactory,
        Map<GenerationId, ReplicationIndexManager> lifecycleManagers,
        NamedScheduledExecutorService commitExecutor,
        InitializedIndexCatalog initializedIndexCatalog,
        Duration commitInterval,
        Duration requestRateLimitBackoffMs) {
      this.executorService = executorService;
      this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
      this.cursorManager = cursorManager;
      this.clientSessionRecordMap = clientSessionRecordMap;
      this.synonymsSessionRefresher = synonymsSessionRefresher;
      this.initialSyncQueue = initialSyncQueue;
      this.steadyStateManager = steadyStateManager;
      this.synonymManager = synonymManager;
      this.replicationIndexManagerFactory = replicationIndexManagerFactory;
      this.lifecycleManagers = lifecycleManagers;
      this.commitExecutor = commitExecutor;
      this.decodingScheduler = decodingScheduler;
      this.initializedIndexCatalog = initializedIndexCatalog;
      this.meterRegistry = new SimpleMeterRegistry();
      this.managerSupplier =
          () ->
              new MongoDbReplicationManager(
                  executorService,
                  indexingWorkSchedulerFactory,
                  cursorManager,
                  clientSessionRecordMap,
                  Optional.of(
                      new SyncSourceConfig(
                          new ConnectionString("mongodb://newString"),
                          Optional.empty(),
                          new ConnectionString("mongodb://newString"))),
                  FeatureFlags.getDefault(),
                  initialSyncQueue,
                  steadyStateManager,
                  synonymManager,
                  syncBatchMongoClient,
                  decodingScheduler,
                  synonymsSyncMongoClient,
                  synonymsSessionRefresher,
                  replicationIndexManagerFactory,
                  this.meterRegistry,
                  lifecycleManagers,
                  commitExecutor,
                  mock(ReplicationOptimeUpdater.class),
                  initializedIndexCatalog,
                  commitInterval,
                  requestRateLimitBackoffMs,
                  false);
      this.manager = this.managerSupplier.get();
    }

    public void recreateManager() {
      this.manager = this.managerSupplier.get();
    }

    static Mocks createSharded() {
      return create(
          Optional.of(mock(com.mongodb.client.MongoClient.class)),
          Optional.of(mockSessionRefresher()));
    }

    static Mocks create() {
      return create(Optional.empty(), Optional.empty());
    }

    private static Mocks create(
        Optional<com.mongodb.client.MongoClient> synonymsSyncMongoClient,
        Optional<SessionRefresher> synonymsSessionRefresher) {
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

      SynonymManager synonymManager = mock(SynonymManager.class);
      when(synonymManager.shutdown()).thenReturn(COMPLETED_FUTURE);

      com.mongodb.client.MongoClient syncMongoClient = mock(com.mongodb.client.MongoClient.class);

      BatchMongoClient syncBatchMongoClient = mock(BatchMongoClient.class);

      ReplicationIndexManagerFactory replicationIndexManagerFactory =
          mock(ReplicationIndexManagerFactory.class);
      Map<GenerationId, ReplicationIndexManager> lifecycleManagers = new ConcurrentHashMap<>();

      NamedScheduledExecutorService commitExecutor =
          spy(
              Executors.fixedSizeThreadScheduledExecutor(
                  "index-commit", 1, new SimpleMeterRegistry()));
      InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
      when(initializedIndexCatalog.getIndex(any()))
          .thenReturn(Optional.of(mock(InitializedIndex.class)));
      return new Mocks(
          executorService,
          indexingWorkSchedulerFactory,
          decodingWorkScheduler,
          cursorManager,
          Map.of("test", new ClientSessionRecord(syncMongoClient, sessionRefresher)),
          initialSyncQueue,
          steadyStateManager,
          synonymManager,
          syncBatchMongoClient,
          synonymsSyncMongoClient,
          synonymsSessionRefresher,
          replicationIndexManagerFactory,
          lifecycleManagers,
          commitExecutor,
          initializedIndexCatalog,
          Duration.ofMinutes(1),
          Duration.ofMillis(100));
    }

    /** mock an index manager, but only for the given index. */
    private ReplicationIndexManager mockReplicationIndexManager(
        com.xgen.mongot.index.IndexGeneration indexGeneration) {
      ReplicationIndexManager replicationIndexManager = mock(ReplicationIndexManager.class);
      when(replicationIndexManager.drop()).thenReturn(COMPLETED_FUTURE);
      var shutdownFuture =
          CompletableFuture.runAsync(
              () -> Crash.because("interrupted sleeping").ifThrows(() -> Thread.sleep(25)),
              this.executorService);
      when(replicationIndexManager.shutdown()).thenReturn(shutdownFuture);
      var synonymShutdownFuture =
          CompletableFuture.runAsync(
              () -> Crash.because("interrupted sleeping").ifThrows(() -> Thread.sleep(25)),
              this.executorService);
      when(this.synonymManager.cancel(any())).thenReturn(synonymShutdownFuture);
      when(this.replicationIndexManagerFactory.create(
              any(),
              any(),
              any(),
              any(),
              any(),
              same(indexGeneration),
              any(),
              any(),
              any(),
              any(),
              any(),
              any(),
              anyBoolean()))
          .thenReturn(replicationIndexManager);
      return replicationIndexManager;
    }

    private ReplicationIndexManager mockReplicationIndexManager() {
      ReplicationIndexManager replicationIndexManager = mock(ReplicationIndexManager.class);
      when(replicationIndexManager.drop()).thenReturn(COMPLETED_FUTURE);
      when(replicationIndexManager.shutdown()).thenReturn(COMPLETED_FUTURE);
      when(this.replicationIndexManagerFactory.create(
              any(),
              any(),
              any(),
              any(),
              any(),
              any(),
              any(),
              any(),
              any(),
              any(),
              any(),
              any(),
              anyBoolean()))
          .thenReturn(replicationIndexManager);
      return replicationIndexManager;
    }

    private void addMockReplicationIndexManager(CompletableFuture<Void> initFuture) {
      when(mockReplicationIndexManager().getInitFuture()).thenReturn(initFuture);
      addIndexForReplication(mockIndexGeneration(mockDefinitionGeneration(new ObjectId())));
    }

    private void addIndexForReplication(IndexGeneration indexGeneration) {
      InitializedSearchIndex initializedSearchIndex =
          mockInitializedIndex(
              indexGeneration.getIndex().asSearchIndex(), indexGeneration.getGenerationId());
      when(this.initializedIndexCatalog.getIndex(indexGeneration.getGenerationId()))
          .thenReturn(Optional.of(initializedSearchIndex));
      this.manager.add(indexGeneration);
    }

    private void waitForIndexReplicationManagerGaugeMetric(String state, int expectedValue) {
      assertEquals(
          this.meterRegistry
              .get(
                  "replication.mongodb."
                      + MongoDbReplicationManager.REPLICATION_INDEX_MANAGER_STATE)
              .tag(MongoDbReplicationManager.STATE_LABEL, state)
              .gauge()
              .value(),
          expectedValue,
          0.1);
    }

    private boolean indexReplicationManagerGaugeMetricsExist() {
      return !this.meterRegistry
          .find("replication.mongodb." + MongoDbReplicationManager.REPLICATION_INDEX_MANAGER_STATE)
          .gauges()
          .isEmpty();
    }
  }
}
