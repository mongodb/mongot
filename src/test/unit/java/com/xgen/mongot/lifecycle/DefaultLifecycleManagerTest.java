package com.xgen.mongot.lifecycle;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.uniqueMockGenerationDefinition;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.blobstore.BlobstoreException;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.blobstore.BlobstoreSnapshotterManager;
import com.xgen.mongot.index.blobstore.IndexBlobstoreSnapshotter;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.replication.ReplicationManagerFactory;
import com.xgen.mongot.replication.mongodb.MongoDbNoOpReplicationManager;
import com.xgen.mongot.replication.mongodb.MongoDbReplicationManager;
import com.xgen.mongot.replication.mongodb.autoembedding.MaterializedViewManager;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.mongodb.ConnectionInfo;
import com.xgen.mongot.util.mongodb.ConnectionStringUtil;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import com.xgen.testing.mongot.mock.index.IndexFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class DefaultLifecycleManagerTest {
  public static final SyncSourceConfig MOCK_SYNC_SOURCE_CONFIG = createMockSyncSourceConfig();

  private static SyncSourceConfig createMockSyncSourceConfig() {
    ConnectionInfo c = ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://localhost");
    return new SyncSourceConfig(c, c, Optional.empty(), Optional.empty());
  }

  private static class Mocks {
    public final ReplicationManagerFactory replicationManagerFactory;
    final BlobstoreSnapshotterManager snapshotterManager;
    final IndexBlobstoreSnapshotter snapshotter;
    final DefaultLifecycleManager lifecycleManager;
    final NamedExecutorService initExecutor;
    final NamedExecutorService lifecycleExecutor;
    final NamedExecutorService blobstoreExecutor;
    final InitializedIndexCatalog initializedIndexCatalog;
    final Gate replicationGate;

    private Mocks(MeterRegistry meterRegistry, Gate replicationGate) {
      try {
        this.replicationManagerFactory = this.getMockReplicationManagerFactory(replicationGate);
        this.initExecutor =
            Executors.fixedSizeThreadPool(new ObjectId().toString(), 1, meterRegistry);
        this.lifecycleExecutor =
            Executors.fixedSizeThreadPool(new ObjectId().toString(), 1, meterRegistry);
        this.blobstoreExecutor =
            Executors.fixedSizeThreadPool(new ObjectId().toString(), 1, meterRegistry);

        this.initializedIndexCatalog = new InitializedIndexCatalog();
        ArrayList<IndexGeneration> createdIndexes = new ArrayList<>();
        AtomicReference<IndexStatus> statusForCreatedIndexes =
            new AtomicReference<>(IndexStatus.steady());
        this.snapshotterManager = mock(BlobstoreSnapshotterManager.class);
        this.snapshotter = mock(IndexBlobstoreSnapshotter.class);
        this.replicationGate = replicationGate;

        doReturn(Optional.of(this.snapshotter))
            .when(this.snapshotterManager)
            .get(any(GenerationId.class));
        when(this.snapshotterManager.areDownloadsEnabled()).thenReturn(true);

        this.lifecycleManager =
            new DefaultLifecycleManager(
                this.replicationManagerFactory,
                Optional.of(MOCK_SYNC_SOURCE_CONFIG),
                this.initializedIndexCatalog,
                IndexFactory.mockIndexFactory(createdIndexes::add, statusForCreatedIndexes::get),
                Optional.of(this.snapshotterManager),
                (syncConfig) -> Optional.empty(),
                meterRegistry,
                this.replicationGate,
                this.initExecutor,
                this.lifecycleExecutor,
                this.blobstoreExecutor);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public static Mocks create() {
      return new Builder().build();
    }

    public static Mocks create(Gate replicationGate) {
      return new Builder().setReplicationGate(replicationGate).build();
    }

    private ReplicationManagerFactory getMockReplicationManagerFactory(Gate replicationGate) {
      return (Optional<SyncSourceConfig> syncSourceConfig) -> {
        if (replicationGate.isClosed()) {
          return mock(MongoDbNoOpReplicationManager.class);
        }

        ReplicationManager replicationManager = mock(MongoDbReplicationManager.class);

        when(replicationManager.dropIndex(any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(replicationManager.shutdown()).thenReturn(CompletableFuture.completedFuture(null));
        when(replicationManager.getSyncSourceConfig()).thenReturn(syncSourceConfig);
        when(replicationManager.isReplicationSupported()).thenReturn(true);
        return replicationManager;
      };
    }

    private static class Builder {
      private Gate replicationGate = ToggleGate.opened();

      public Builder setReplicationGate(Gate replicationGate) {
        this.replicationGate = replicationGate;
        return this;
      }

      public Mocks build() {
        return new Mocks(new SimpleMeterRegistry(), this.replicationGate);
      }
    }
  }

  @Test
  public void testCreateDefaultManager() {
    @Var var mocks = Mocks.create();
    Assert.assertTrue(mocks.replicationGate.isOpen());
    Assert.assertTrue(
        mocks.lifecycleManager.getReplicationManager() instanceof MongoDbReplicationManager);

    ToggleGate closedGate = ToggleGate.closed();
    mocks = Mocks.create(closedGate);
    Assert.assertTrue(mocks.replicationGate.isClosed());
    Assert.assertTrue(
        mocks.lifecycleManager.getReplicationManager() instanceof MongoDbNoOpReplicationManager);
  }

  @Test
  public void testDropIndex() throws Exception {
    Mocks mocks = Mocks.create();
    IndexGeneration generation = mockIndexGeneration(new ObjectId());
    var generationId = generation.getGenerationId();
    // Add an index.
    mocks.lifecycleManager.add(generation);
    IndexLifecycleManager indexLifecycleManager =
        mocks.lifecycleManager.getIndexLifecycleManager(generationId);
    assertNotNull(indexLifecycleManager);
    waitForState(indexLifecycleManager, IndexLifecycleManager.State.RUNNING);
    verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(generation);

    // Drop it, ensure that ReplicationManager::dropIndex was called.
    CompletableFuture<Void> future = mocks.lifecycleManager.dropIndex(generationId);
    assertNull(mocks.lifecycleManager.getIndexLifecycleManager(generationId));
    assertEquals(IndexLifecycleManager.State.DROPPED, indexLifecycleManager.getState());

    future.get();
    verify(mocks.lifecycleManager.getReplicationManager()).dropIndex(generationId);
    verify(mocks.snapshotterManager).drop(generationId);
  }

  @Test
  public void testSameIndexCanNotBeAddedTwice() {
    Mocks mocks = Mocks.create();

    var index = mockIndexGeneration();

    mocks.lifecycleManager.add(index);

    Assert.assertThrows(RuntimeException.class, () -> mocks.lifecycleManager.add(index));
  }

  @Test
  public void testCanNotInteractAfterShutDown() {
    Mocks mocks = Mocks.create();

    var index = mockIndexGeneration(new ObjectId());
    mocks.lifecycleManager.add(index);
    mocks.lifecycleManager.shutdown();

    // can not add a new index or cancel existing
    Assert.assertThrows(
        RuntimeException.class,
        () -> mocks.lifecycleManager.add(mockIndexGeneration(new ObjectId())));
    Assert.assertThrows(
        RuntimeException.class, () -> mocks.lifecycleManager.dropIndex(index.getGenerationId()));
  }

  @Test
  public void testShutdownNoIndexes() throws Exception {
    Mocks mocks = Mocks.create();

    mocks.lifecycleManager.shutdown().get(5, TimeUnit.SECONDS);
    verify(mocks.lifecycleManager.getReplicationManager()).shutdown();
    verify(mocks.snapshotterManager).shutdown();
    assertTrue(mocks.lifecycleManager.isReplicationShutdown());
    assertTrue(mocks.initExecutor.isShutdown());
    assertTrue(mocks.lifecycleExecutor.isShutdown());
  }

  @Test
  public void testShutdownWithIndexes() throws Exception {
    Mocks mocks = Mocks.create();

    // Add two different indexes to the LifecycleManager.
    SearchIndexDefinition indexDefinition1 =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("1")
            .database("foo")
            .lastObservedCollectionName("bar")
            .collectionUuid(UUID.randomUUID())
            .mappings(DocumentFieldDefinitionBuilder.builder().build())
            .build();
    IndexGeneration indexGeneration1 = mockIndexGeneration(indexDefinition1);
    mocks.lifecycleManager.add(indexGeneration1);

    SearchIndexDefinition indexDefinition2 =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("2")
            .database("foo")
            .lastObservedCollectionName("bar")
            .collectionUuid(UUID.randomUUID())
            .mappings(DocumentFieldDefinitionBuilder.builder().build())
            .build();
    IndexGeneration indexGeneration2 = mockIndexGeneration(indexDefinition2);
    mocks.lifecycleManager.add(indexGeneration2);

    IndexLifecycleManager indexLifecycleManager =
        mocks.lifecycleManager.getIndexLifecycleManager(indexGeneration1.getGenerationId());
    IndexLifecycleManager indexLifecycleManager2 =
        mocks.lifecycleManager.getIndexLifecycleManager(indexGeneration2.getGenerationId());
    assertNotNull(indexLifecycleManager);
    assertNotNull(indexLifecycleManager2);
    waitForState(indexLifecycleManager, IndexLifecycleManager.State.RUNNING);
    waitForState(indexLifecycleManager2, IndexLifecycleManager.State.RUNNING);
    verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(indexGeneration1);
    verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(indexGeneration2);

    // Ensure that shutdown() shuts down both of the IndexLifecycleManagers.
    mocks.lifecycleManager.shutdown().get(5, TimeUnit.SECONDS);
    verify(mocks.lifecycleManager.getReplicationManager()).shutdown();
    verify(mocks.snapshotterManager).shutdown();
    assertNotNull(indexLifecycleManager);
    assertNotNull(indexLifecycleManager2);
    assertTrue(mocks.initExecutor.isShutdown());
    assertTrue(mocks.lifecycleExecutor.isShutdown());
  }

  @Test
  public void testShutdownTwoIndexesWithSameIdShutsBothDown() throws Exception {
    Mocks mocks = Mocks.create();
    var gen1 = MOCK_INDEX_GENERATION_ID;
    var gen2 = GenerationIdBuilder.incrementUser(gen1);
    var index1 = mockIndexGeneration(gen1);
    var index2 = mockIndexGeneration(gen2);
    Assert.assertEquals(gen1.indexId, gen2.indexId);

    // Add two different indexes to the MongoDbReplicationManager.
    mocks.lifecycleManager.add(index1);
    mocks.lifecycleManager.add(index2);

    IndexLifecycleManager indexLifecycleManager =
        mocks.lifecycleManager.getIndexLifecycleManager(gen1);
    IndexLifecycleManager indexLifecycleManager2 =
        mocks.lifecycleManager.getIndexLifecycleManager(gen2);
    assertNotNull(mocks.lifecycleManager.getIndexLifecycleManager(gen1));
    assertNotNull(mocks.lifecycleManager.getIndexLifecycleManager(gen2));
    assertNotEquals(
        mocks.lifecycleManager.getIndexLifecycleManager(gen1),
        mocks.lifecycleManager.getIndexLifecycleManager(gen2));
    waitForState(indexLifecycleManager, IndexLifecycleManager.State.RUNNING);
    waitForState(indexLifecycleManager2, IndexLifecycleManager.State.RUNNING);
    verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(index1);
    verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(index2);

    // Ensure that shutdown() shuts down both of the IndexLifecycleManagers.
    mocks.lifecycleManager.shutdown().get(5, TimeUnit.SECONDS);
    verify(mocks.lifecycleManager.getReplicationManager(), timeout(5000)).shutdown();
    verify(mocks.snapshotterManager).shutdown();
  }

  @Test
  public void testRemoveOneOfTwoIndexesWithSameIndexId() throws Exception {
    Mocks mocks = Mocks.create();
    var gen1 = MOCK_INDEX_GENERATION_ID;
    var gen2 = GenerationIdBuilder.incrementUser(gen1);
    var index1 = mockIndexGeneration(gen1);
    var index2 = mockIndexGeneration(gen2);

    // Add two different indexes to the MongoDbReplicationManager.
    mocks.lifecycleManager.add(index1);
    mocks.lifecycleManager.add(index2);
    IndexLifecycleManager indexLifecycleManager =
        mocks.lifecycleManager.getIndexLifecycleManager(gen1);
    IndexLifecycleManager indexLifecycleManager2 =
        mocks.lifecycleManager.getIndexLifecycleManager(gen2);
    assertNotNull(mocks.lifecycleManager.getIndexLifecycleManager(gen1));
    assertNotNull(mocks.lifecycleManager.getIndexLifecycleManager(gen2));
    waitForState(indexLifecycleManager, IndexLifecycleManager.State.RUNNING);
    verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(index1);

    mocks.lifecycleManager.dropIndex(gen1).get();

    assertNull(mocks.lifecycleManager.getIndexLifecycleManager(gen1));
    assertNotNull(mocks.lifecycleManager.getIndexLifecycleManager(gen2));
    assertEquals(IndexLifecycleManager.State.DROPPED, indexLifecycleManager.getState());
    waitForState(indexLifecycleManager2, IndexLifecycleManager.State.RUNNING);
    verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(index2);
    // only the first should drop but not the second
    verify(mocks.lifecycleManager.getReplicationManager()).dropIndex(gen1);
    verify(mocks.snapshotterManager).drop(gen1);
    verify(mocks.snapshotterManager, times(0)).drop(gen2);
  }

  @Test
  public void testLifecycleManagerInitialized() {
    Mocks mocks = Mocks.create();
    var index1 = mockIndexGeneration(MOCK_INDEX_GENERATION_ID);
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(
            x -> {
              latch.await();
              return doCallRealMethod();
            })
        .when(mocks.lifecycleManager.getReplicationManager())
        .add(index1);
    mocks.lifecycleManager.add(index1);
    IndexLifecycleManager indexLifecycleManager =
        mocks.lifecycleManager.getIndexLifecycleManager(MOCK_INDEX_GENERATION_ID);
    assertFalse(mocks.lifecycleManager.isInitialized());
    when(mocks.lifecycleManager.getReplicationManager().isInitialized()).thenReturn(true);
    assertFalse(mocks.lifecycleManager.isInitialized());
    latch.countDown();
    waitForState(indexLifecycleManager, IndexLifecycleManager.State.RUNNING);
    assertTrue(mocks.lifecycleManager.isInitialized());
    verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(index1);
  }

  @Test
  public void testGetReplicationInitStateNoIndexes() {
    Mocks mocks = Mocks.create();
    assertFalse(mocks.lifecycleManager.isInitialized());
    when(mocks.lifecycleManager.getReplicationManager().isInitialized()).thenReturn(true);
    assertTrue(mocks.lifecycleManager.isInitialized());
  }

  @Test
  public void testLifecycleManagerAllInitialized() {
    Mocks mocks = Mocks.create();
    var index1 = mockIndexGeneration(MOCK_INDEX_GENERATION_ID);
    var index2 = mockIndexGeneration(uniqueMockGenerationDefinition());
    mocks.lifecycleManager.add(index1);
    mocks.lifecycleManager.add(index2);
    IndexLifecycleManager indexLifecycleManager =
        mocks.lifecycleManager.getIndexLifecycleManager(MOCK_INDEX_GENERATION_ID);
    waitForState(indexLifecycleManager, IndexLifecycleManager.State.RUNNING);
    waitForState(
        mocks.lifecycleManager.getIndexLifecycleManager(index2.getGenerationId()),
        IndexLifecycleManager.State.RUNNING);
    assertFalse(mocks.lifecycleManager.isInitialized());
    when(mocks.lifecycleManager.getReplicationManager().isInitialized()).thenReturn(true);
    assertTrue(mocks.lifecycleManager.isInitialized());
    // Once initialized, should be considered initialized after new indexes are added.
    mocks.lifecycleManager.add(mockIndexGeneration(uniqueMockGenerationDefinition()));
    assertTrue(mocks.lifecycleManager.isInitialized());
    verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(index1);
  }

  @Test
  public void testLifecycleManagerInitializedWithIndexDownloading() throws BlobstoreException {
    Mocks mocks = Mocks.create();
    var index1 = mockIndexGeneration(MOCK_INDEX_GENERATION_ID);
    var index2 = mockIndexGeneration(uniqueMockGenerationDefinition());
    CountDownLatch latch = new CountDownLatch(1);
    IndexBlobstoreSnapshotter snapshotter = mock(IndexBlobstoreSnapshotter.class);
    when(snapshotter.shouldDownloadIndex()).thenReturn(true);

    when(mocks.snapshotterManager.get(index2.getGenerationId()))
        .thenAnswer(x -> Optional.of(snapshotter));
    doAnswer(
            x -> {
              latch.await();
              return null;
            })
        .when(snapshotter)
        .downloadIndex();

    mocks.lifecycleManager.add(index1);
    mocks.lifecycleManager.add(index2);
    IndexLifecycleManager indexLifecycleManager =
        mocks.lifecycleManager.getIndexLifecycleManager(MOCK_INDEX_GENERATION_ID);
    waitForState(indexLifecycleManager, IndexLifecycleManager.State.RUNNING);
    waitForState(
        mocks.lifecycleManager.getIndexLifecycleManager(index2.getGenerationId()),
        IndexLifecycleManager.State.DOWNLOADING);
    assertFalse(mocks.lifecycleManager.isInitialized());
    when(mocks.lifecycleManager.getReplicationManager().isInitialized()).thenReturn(true);
    assertTrue(mocks.lifecycleManager.isInitialized());
    latch.countDown();
    verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(index1);
  }

  // ==================== MaterializedViewManager Propagation Tests ====================

  private DefaultLifecycleManager createLifecycleManagerWithMatViewManager(
      MaterializedViewManager matViewManager) throws Exception {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    ReplicationManagerFactory replicationManagerFactory =
        (Optional<SyncSourceConfig> syncSourceConfig) -> {
          ReplicationManager rm = mock(MongoDbReplicationManager.class);
          when(rm.dropIndex(any())).thenReturn(CompletableFuture.completedFuture(null));
          when(rm.shutdown()).thenReturn(CompletableFuture.completedFuture(null));
          when(rm.getSyncSourceConfig()).thenReturn(syncSourceConfig);
          when(rm.isReplicationSupported()).thenReturn(true);
          return rm;
        };

    return new DefaultLifecycleManager(
        replicationManagerFactory,
        Optional.of(MOCK_SYNC_SOURCE_CONFIG),
        new InitializedIndexCatalog(),
        IndexFactory.mockIndexFactory(ig -> {}, () -> IndexStatus.steady()),
        Optional.empty(),
        (syncConfig) -> Optional.of(matViewManager),
        meterRegistry,
        ToggleGate.opened(),
        Executors.fixedSizeThreadPool("init", 1, meterRegistry),
        Executors.fixedSizeThreadPool("lifecycle", 1, meterRegistry),
        Executors.fixedSizeThreadPool("blobstore", 1, meterRegistry));
  }

  @Test
  public void testShutdownReplication_propagatesToMaterializedViewManager() throws Exception {
    MaterializedViewManager matViewManager = mock(MaterializedViewManager.class);
    when(matViewManager.shutdownReplication()).thenReturn(CompletableFuture.completedFuture(null));
    DefaultLifecycleManager lifecycleManager =
        createLifecycleManagerWithMatViewManager(matViewManager);

    lifecycleManager.shutdownReplication().get(5, TimeUnit.SECONDS);

    verify(matViewManager).shutdownReplication();
  }

  @Test
  public void testRestartReplication_propagatesToMaterializedViewManager() throws Exception {
    MaterializedViewManager matViewManager = mock(MaterializedViewManager.class);
    when(matViewManager.shutdownReplication()).thenReturn(CompletableFuture.completedFuture(null));
    DefaultLifecycleManager lifecycleManager =
        createLifecycleManagerWithMatViewManager(matViewManager);

    lifecycleManager.restartReplication();

    verify(matViewManager).restartReplication();
  }

  @Test
  public void testUpdateSyncSource_propagatesToMaterializedViewManager() throws Exception {
    MaterializedViewManager matViewManager = mock(MaterializedViewManager.class);
    when(matViewManager.shutdownReplication()).thenReturn(CompletableFuture.completedFuture(null));
    DefaultLifecycleManager lifecycleManager =
        createLifecycleManagerWithMatViewManager(matViewManager);

    SyncSourceConfig newConfig =
        new SyncSourceConfig(
            ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newHost"),
            ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newHost"),
            Optional.empty(),
            Optional.empty());
    lifecycleManager.updateSyncSource(newConfig);

    verify(matViewManager).updateSyncSource(newConfig);
  }

  @Test
  public void testShutdown_propagatesToMaterializedViewManager() throws Exception {
    MaterializedViewManager matViewManager = mock(MaterializedViewManager.class);
    when(matViewManager.shutdown()).thenReturn(CompletableFuture.completedFuture(null));
    when(matViewManager.shutdownReplication()).thenReturn(CompletableFuture.completedFuture(null));
    DefaultLifecycleManager lifecycleManager =
        createLifecycleManagerWithMatViewManager(matViewManager);

    lifecycleManager.shutdown().get(5, TimeUnit.SECONDS);

    verify(matViewManager).shutdown();
  }

  // Wait for up to 5s for the actual state to match the desired state.
  void waitForState(IndexLifecycleManager manager, IndexLifecycleManager.State state) {
    try {
      int maxAttempts = 50;
      @Var int attemptCount = 0;
      while (attemptCount < maxAttempts) {
        if (manager.getState() == state) {
          return;
        } else {
          Thread.sleep(100);
          attemptCount += 1;
        }
      }
      fail();
    } catch (InterruptedException e) {
      fail();
    }
  }
}
