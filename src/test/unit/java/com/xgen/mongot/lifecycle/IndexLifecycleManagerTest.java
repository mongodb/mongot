package com.xgen.mongot.lifecycle;

import static com.xgen.mongot.util.FunctionalUtils.nopConsumer;
import static com.xgen.testing.mongot.mock.index.IndexFactory.mockIndexFactory;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.blobstore.BlobstoreException;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.blobstore.BlobstoreSnapshotterManager;
import com.xgen.mongot.index.blobstore.IndexBlobstoreSnapshotter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.replication.mongodb.MongoDbReplicationManager;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.types.ObjectId;
import org.junit.Test;

public class IndexLifecycleManagerTest {
  static final Executor EXECUTOR = Executors.newFixedThreadPool(3);

  static class Mocks {
    IndexLifecycleManager indexLifecycleManager;
    ReplicationManager replicationManager;
    DefaultLifecycleManager.ReplicationManagerWrapper replicationManagerWrapper;
    IndexGeneration indexGeneration;
    InitializedIndexCatalog initializedIndexCatalog;
    IndexFactory indexFactory;
    BlobstoreSnapshotterManager snapshotterManager;
    IndexLifecycleManager.Metrics metrics;

    public Mocks(
        ReplicationManager replicationManager,
        DefaultLifecycleManager.ReplicationManagerWrapper replicationManagerWrapper,
        IndexFactory indexFactory,
        BlobstoreSnapshotterManager snapshotterManager,
        IndexGeneration indexGeneration,
        InitializedIndexCatalog initializedIndexCatalog,
        IndexLifecycleManager.Metrics metrics,
        IndexLifecycleManager indexLifecycleManager) {
      this.replicationManager = replicationManager;
      this.replicationManagerWrapper = replicationManagerWrapper;
      this.indexGeneration = indexGeneration;
      this.initializedIndexCatalog = initializedIndexCatalog;
      this.indexFactory = indexFactory;
      this.metrics = metrics;
      this.indexLifecycleManager = indexLifecycleManager;
      this.snapshotterManager = snapshotterManager;
    }

    static Mocks create() {
      return create(true);
    }

    static Mocks create(boolean startLifecycle) {
      ReplicationManager replicationManager = mock(MongoDbReplicationManager.class);
      var replicationManagerWrapper =
          new DefaultLifecycleManager.ReplicationManagerWrapper(replicationManager);
      AtomicReference<IndexStatus> statusForCreatedIndexes =
          new AtomicReference<>(IndexStatus.steady());
      MetricsFactory metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());

      try {
        IndexGeneration indexGeneration = mockIndexGeneration(new ObjectId());
        InitializedIndexCatalog initializedIndexCatalog = new InitializedIndexCatalog();
        IndexFactory indexFactory = mockIndexFactory(nopConsumer(), statusForCreatedIndexes::get);
        IndexLifecycleManager.Metrics metrics =
            IndexLifecycleManager.Metrics.create(metricsFactory);
        BlobstoreSnapshotterManager snapshotterManager = mock(BlobstoreSnapshotterManager.class);
        IndexLifecycleManager lifecycleManager =
            startLifecycle
                ? IndexLifecycleManager.create(
                    replicationManagerWrapper,
                    indexGeneration,
                    initializedIndexCatalog,
                    indexFactory,
                    Optional.of(snapshotterManager),
                    EXECUTOR,
                    EXECUTOR,
                    EXECUTOR,
                    metrics)
                : spy(
                    new IndexLifecycleManager(
                        replicationManagerWrapper,
                        indexGeneration,
                        initializedIndexCatalog,
                        indexFactory,
                        Optional.of(snapshotterManager),
                        true,
                        metrics));
        return new Mocks(
            replicationManager,
            replicationManagerWrapper,
            indexFactory,
            snapshotterManager,
            indexGeneration,
            initializedIndexCatalog,
            metrics,
            lifecycleManager);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testCreation() throws IOException {
    Mocks mocks = Mocks.create();
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING));
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    try (InitializedIndex initializedIndex =
        mocks
            .initializedIndexCatalog
            .getIndex(mocks.indexGeneration.getGenerationId())
            .orElseThrow()) {
      assertEquals(IndexStatus.StatusCode.STEADY, initializedIndex.getStatus().getStatusCode());
    }
  }

  @Test
  public void testStuckInInitialization() throws InterruptedException {
    Mocks mocks = Mocks.create(false);
    CountDownLatch countDownLatch = new CountDownLatch(1);

    doAnswer(
            x -> {
              countDownLatch.await();
              x.callRealMethod();
              return null;
            })
        .when(mocks.indexLifecycleManager)
        .initialize();
    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);
    Thread.sleep(100);
    assertEquals(IndexLifecycleManager.State.NOT_STARTED, mocks.indexLifecycleManager.getState());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    countDownLatch.countDown();
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING));
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    verify(mocks.snapshotterManager, never()).scheduleUpload(mocks.indexGeneration);
  }

  @Test
  public void testStuckBeforeReplicationStart() throws InterruptedException {
    Mocks mocks = Mocks.create(false);
    CountDownLatch countDownLatch = new CountDownLatch(1);

    doAnswer(
            x -> {
              countDownLatch.await();
              x.callRealMethod();
              return null;
            })
        .when(mocks.indexLifecycleManager)
        .startReplication();
    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);
    Thread.sleep(100);
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.INITIALIZED));
    assertEquals(1, mocks.metrics.indexesInInitializedState.get());
    countDownLatch.countDown();
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING));
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    verify(mocks.snapshotterManager, never()).scheduleUpload(mocks.indexGeneration);
  }

  @Test
  public void testReplicationRestart() throws InterruptedException {
    Mocks mocks = Mocks.create(false);
    IndexBlobstoreSnapshotter snapshotter = mock(IndexBlobstoreSnapshotter.class);
    when(mocks.snapshotterManager.get(mocks.indexGeneration.getGenerationId()))
        .thenAnswer(x -> Optional.of(snapshotter));

    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);

    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING));
    verify(mocks.replicationManager, times(1)).add(any());

    mocks.replicationManagerWrapper.setReplicationEnabled(false);
    assertEquals(IndexLifecycleManager.State.RUNNING, mocks.indexLifecycleManager.getState());
    mocks.replicationManagerWrapper.setReplicationEnabled(true);
    mocks.indexLifecycleManager.startReplication();
    assertEquals(IndexLifecycleManager.State.RUNNING, mocks.indexLifecycleManager.getState());
    verify(mocks.replicationManager, times(2)).add(any());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(mocks.indexGeneration);
  }

  @Test
  public void testReplicationRestartBeforeInitializing() throws InterruptedException {
    Mocks mocks = Mocks.create(false);
    IndexBlobstoreSnapshotter snapshotter = mock(IndexBlobstoreSnapshotter.class);
    when(mocks.snapshotterManager.get(mocks.indexGeneration.getGenerationId()))
        .thenAnswer(x -> Optional.of(snapshotter));

    CountDownLatch countDownLatch = new CountDownLatch(1);

    doAnswer(
            x -> {
              countDownLatch.await();
              x.callRealMethod();
              return null;
            })
        .when(mocks.indexLifecycleManager)
        .initialize();
    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);

    mocks.replicationManagerWrapper.setReplicationEnabled(false);
    assertEquals(IndexLifecycleManager.State.NOT_STARTED, mocks.indexLifecycleManager.getState());
    countDownLatch.countDown();
    Thread.sleep(100);
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.INITIALIZED));
    mocks.replicationManagerWrapper.setReplicationEnabled(true);
    mocks.indexLifecycleManager.startReplication();
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING));

    verify(mocks.replicationManager, atLeast(1)).add(any());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(mocks.indexGeneration);
  }

  @Test
  public void testReplicationRestartBeforeReplicating() throws InterruptedException {
    Mocks mocks = Mocks.create(false);

    mocks.replicationManagerWrapper.setReplicationEnabled(false);
    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);
    Thread.sleep(100);
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.INITIALIZED));
    assertEquals(1, mocks.metrics.indexesInInitializedState.get());
    mocks.replicationManagerWrapper.setReplicationEnabled(true);
    mocks.indexLifecycleManager.startReplication();
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING));
    verify(mocks.replicationManager, atLeast(1)).add(any());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    verify(mocks.snapshotterManager, never()).scheduleUpload(mocks.indexGeneration);
  }

  @Test
  public void testReplicationRestartWhileReplicating() throws InterruptedException {
    Mocks mocks = Mocks.create(false);
    CountDownLatch countDownLatch = new CountDownLatch(1);

    doAnswer(
            x -> {
              countDownLatch.await();
              x.callRealMethod();
              return null;
            })
        .doCallRealMethod()
        .when(mocks.indexLifecycleManager)
        .startReplication();
    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);

    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.INITIALIZED));
    mocks.replicationManagerWrapper.setReplicationEnabled(false);

    assertEquals(IndexLifecycleManager.State.INITIALIZED, mocks.indexLifecycleManager.getState());
    assertEquals(1, mocks.metrics.indexesInInitializedState.get());
    countDownLatch.countDown();
    Thread.sleep(50);
    assertEquals(IndexLifecycleManager.State.INITIALIZED, mocks.indexLifecycleManager.getState());
    verify(mocks.replicationManager, timeout(100).times(0)).add(any());
    assertEquals(1, mocks.metrics.indexesInInitializedState.get());
    mocks.replicationManagerWrapper.setReplicationEnabled(true);
    mocks.indexLifecycleManager.startReplication();
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING));
    verify(mocks.replicationManager, times(1)).add(any());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    verify(mocks.snapshotterManager, never()).scheduleUpload(mocks.indexGeneration);
  }

  @Test
  public void testDrop() {
    Mocks mocks = Mocks.create(false);
    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);

    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING));
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    mocks.indexLifecycleManager.drop();
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.DROPPED));

    verify(mocks.replicationManager, times(1)).dropIndex(any());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
  }

  @Test
  public void startLifecycle_snapshotterAbsent_initializesAndReplicatesWithoutFcis() {
    Mocks mocks = Mocks.create(false);
    when(mocks.snapshotterManager.get(mocks.indexGeneration.getGenerationId()))
        .thenReturn(Optional.empty());

    mocks.indexLifecycleManager.startLifecycle(Runnable::run, Runnable::run, Runnable::run);

    assertEquals(IndexLifecycleManager.State.RUNNING, mocks.indexLifecycleManager.getState());
    assertTrue(
        mocks
            .initializedIndexCatalog
            .getIndex(mocks.indexGeneration.getGenerationId())
            .isPresent());
    verify(mocks.snapshotterManager).add(mocks.indexGeneration);
    verify(mocks.replicationManager).add(mocks.indexGeneration);
    verify(mocks.snapshotterManager, never()).scheduleUpload(any());
    assertEquals(0, mocks.metrics.failedDownloadIndexes.count(), 0);
  }

  @Test
  public void testBlobstoreSnapshotter() throws BlobstoreException {
    {
      Mocks mocks = Mocks.create(false);
      mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);
      verify(mocks.snapshotterManager, timeout(10000)).add(mocks.indexGeneration);
      verify(mocks.snapshotterManager, never()).scheduleUpload(mocks.indexGeneration);
    }
    {
      Mocks mocks = Mocks.create(false);
      IndexBlobstoreSnapshotter snapshotter = mock(IndexBlobstoreSnapshotter.class);
      when(snapshotter.shouldDownloadIndex()).thenReturn(false);
      when(mocks.snapshotterManager.get(mocks.indexGeneration.getGenerationId()))
          .thenAnswer(x -> Optional.of(snapshotter));

      mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);
      verify(mocks.snapshotterManager, timeout(10000)).add(mocks.indexGeneration);
      waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING);
      verify(snapshotter, never()).downloadIndex();
    }
    {
      Mocks mocks = Mocks.create(false);
      IndexBlobstoreSnapshotter snapshotter = mock(IndexBlobstoreSnapshotter.class);
      when(snapshotter.shouldDownloadIndex()).thenReturn(true);
      when(mocks.snapshotterManager.get(mocks.indexGeneration.getGenerationId()))
          .thenAnswer(x -> Optional.of(snapshotter));
      CountDownLatch latch = new CountDownLatch(1);
      doAnswer(
              x -> {
                latch.await();
                return null;
              })
          .when(snapshotter)
          .downloadIndex();

      mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);
      waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.DOWNLOADING);
      latch.countDown();
      waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING);
      verify(snapshotter).downloadIndex();
      verify(mocks.snapshotterManager, timeout(10000)).scheduleUpload(mocks.indexGeneration);
    }
    {
      Mocks mocks = Mocks.create(false);
      IndexBlobstoreSnapshotter snapshotter = mock(IndexBlobstoreSnapshotter.class);
      when(snapshotter.shouldDownloadIndex()).thenReturn(true);
      when(mocks.snapshotterManager.get(mocks.indexGeneration.getGenerationId()))
          .thenAnswer(x -> Optional.of(snapshotter));
      doThrow(new BlobstoreException("Test exception") {}).when(snapshotter).downloadIndex();

      mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);
      waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING);
      verify(snapshotter).downloadIndex();
      assertEquals(1, mocks.metrics.failedDownloadIndexes.count(), 0);
    }
  }

  @Test
  public void testDropBeforeInitializing() throws InterruptedException {
    Mocks mocks = Mocks.create(false);
    CountDownLatch countDownLatch = new CountDownLatch(1);

    doAnswer(
            x -> {
              countDownLatch.await();
              x.callRealMethod();
              return null;
            })
        .doCallRealMethod()
        .when(mocks.indexLifecycleManager)
        .initialize();
    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);

    mocks.indexLifecycleManager.drop();
    assertEquals(IndexLifecycleManager.State.DROPPED, mocks.indexLifecycleManager.getState());

    countDownLatch.countDown();
    verify(mocks.replicationManager, never()).add(any());
    verify(mocks.replicationManager, times(1)).dropIndex(any());
    assertEquals(IndexLifecycleManager.State.DROPPED, mocks.indexLifecycleManager.getState());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
  }

  @Test
  public void testDropBeforeReplicating() throws IOException {
    Mocks mocks = Mocks.create(false);
    CountDownLatch countDownLatch = new CountDownLatch(1);

    doAnswer(
            x -> {
              countDownLatch.await();
              x.callRealMethod();
              return null;
            })
        .doCallRealMethod()
        .when(mocks.indexLifecycleManager)
        .startReplication();
    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);

    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.INITIALIZED));
    assertEquals(1, mocks.metrics.indexesInInitializedState.get());
    mocks.indexLifecycleManager.drop();
    countDownLatch.countDown();
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.DROPPED));
    verify(mocks.replicationManager, never()).add(any());
    verify(mocks.replicationManager, times(1)).dropIndex(any());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
  }

  @Test
  public void testDropWhileReplicationShutdown() throws InterruptedException {
    Mocks mocks = Mocks.create(false);
    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);

    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING));
    // Drop index while replication shutdown.
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    mocks.replicationManagerWrapper.setReplicationEnabled(false);
    mocks.indexLifecycleManager.drop();
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.DROPPED));
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    mocks.replicationManagerWrapper.setReplicationEnabled(true);
    mocks.indexLifecycleManager.startReplication();
    verify(mocks.replicationManager, times(1)).dropIndex(any());
    verify(mocks.replicationManager, times(1)).add(any());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
  }

  @Test
  public void testDropAfterReplicationRestart() throws InterruptedException {
    Mocks mocks = Mocks.create(false);

    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);

    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING));
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    mocks.replicationManagerWrapper.setReplicationEnabled(false);
    mocks.indexLifecycleManager.drop();
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());

    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.DROPPED));
    mocks.indexLifecycleManager.startReplication();
    verify(mocks.replicationManager, times(1)).dropIndex(any());
    verify(mocks.replicationManager, times(1)).add(any());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
  }

  @Test
  public void testShutdownBeforeReplicating() throws IOException {
    Mocks mocks = Mocks.create(false);
    CountDownLatch countDownLatch = new CountDownLatch(1);

    doAnswer(
            x -> {
              countDownLatch.await();
              x.callRealMethod();
              return null;
            })
        .doCallRealMethod()
        .when(mocks.indexLifecycleManager)
        .startReplication();
    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);

    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.INITIALIZED));
    assertEquals(1, mocks.metrics.indexesInInitializedState.get());
    mocks.indexLifecycleManager.shutdown();
    countDownLatch.countDown();
    assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.SHUTDOWN));
    verify(mocks.replicationManager, never()).add(any());
    verify(mocks.replicationManager, never()).dropIndex(any());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    verify(mocks.indexGeneration.getIndex(), never()).drop();
  }

  @Test
  public void testInitializeIndexFailed() throws IOException {
    Mocks mocks = Mocks.create(false);
    doThrow(IOException.class).when(mocks.indexFactory).getInitializedIndex(any(), any(), any());

    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);
    waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.SHUTDOWN);
    assertEquals(
        IndexStatus.StatusCode.FAILED,
        mocks.indexGeneration.getIndex().getStatus().getStatusCode());
    assertEquals(
        IndexStatus.Reason.INITIALIZATION_FAILED,
        mocks.indexGeneration.getIndex().getStatus().getReason().orElseThrow());
    assertEquals(1.0, mocks.metrics.failedInitializationIndexes.count(), 0.1);
    assertEquals(0, mocks.metrics.failedDropIndexes.count(), 0.1);
    verify(mocks.snapshotterManager).add(mocks.indexGeneration);
    verify(mocks.snapshotterManager, never()).scheduleUpload(mocks.indexGeneration);
    verify(mocks.replicationManager, never()).dropIndex(any());
  }

  @Test
  public void testInitializeIndexFailedAfterDrop() throws Exception {
    Mocks mocks = Mocks.create(false);
    CountDownLatch blockInitializeCompletion = new CountDownLatch(1);
    CountDownLatch initializeCalled = new CountDownLatch(1);
    doAnswer(
            x -> {
              initializeCalled.countDown();
              blockInitializeCompletion.await();
              throw new RuntimeException("test");
            })
        .when(mocks.indexFactory)
        .getInitializedIndex(any(), any(), any());

    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);
    initializeCalled.await();
    mocks.indexLifecycleManager.drop();
    verify(mocks.replicationManager, never()).add(any());
    verify(mocks.replicationManager, times(1)).dropIndex(any());
    verify(mocks.indexGeneration.getIndex(), never()).drop();
    blockInitializeCompletion.countDown();
    waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.DROPPED);

    assertNotEquals(
        IndexStatus.StatusCode.FAILED,
        mocks.indexGeneration.getIndex().getStatus().getStatusCode());
    assertEquals(
        Optional.of(IndexStatus.Reason.INDEX_DROPPED),
        mocks.indexGeneration.getIndex().getStatus().getReason());
    assertEquals(0.0, mocks.metrics.failedInitializationIndexes.count(), 0.1);
    assertEquals(0, mocks.metrics.failedDropIndexes.count(), 0.1);

    verify(mocks.replicationManager, never()).add(any());
    verify(mocks.replicationManager, times(1)).dropIndex(any());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    verify(mocks.indexGeneration.getIndex(), timeout(5000).times(1)).drop();
  }

  @Test
  public void testInitializeIndexFailedAfterShutdown() throws Exception {
    Mocks mocks = Mocks.create(false);
    CountDownLatch blockInitializeCompletion = new CountDownLatch(1);
    CountDownLatch initializeCalled = new CountDownLatch(1);
    doAnswer(
            x -> {
              initializeCalled.countDown();
              blockInitializeCompletion.await();
              throw new RuntimeException();
            })
        .when(mocks.indexFactory)
        .getInitializedIndex(any(), any(), any());

    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);
    initializeCalled.await();
    mocks.indexLifecycleManager.shutdown();
    blockInitializeCompletion.countDown();
    waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.SHUTDOWN);

    assertNotEquals(
        IndexStatus.StatusCode.FAILED,
        mocks.indexGeneration.getIndex().getStatus().getStatusCode());
    assertEquals(Optional.empty(), mocks.indexGeneration.getIndex().getStatus().getReason());
    assertEquals(0.0, mocks.metrics.failedInitializationIndexes.count(), 0.1);
    assertEquals(0, mocks.metrics.failedDropIndexes.count(), 0.1);

    verify(mocks.replicationManager, never()).add(any());
    verify(mocks.replicationManager, never()).dropIndex(any());
    assertEquals(0, mocks.metrics.indexesInInitializedState.get());
    verify(mocks.indexGeneration.getIndex(), never()).drop();
  }

  @Test
  public void testInitializeIndexFailedAndDropFailed() throws IOException, InterruptedException {
    Mocks mocks = Mocks.create(false);
    doThrow(IOException.class).when(mocks.indexFactory).getInitializedIndex(any(), any(), any());
    doThrow(IOException.class).when(mocks.indexGeneration.getIndex()).drop();
    CountDownLatch initializeCalled = new CountDownLatch(1);
    CountDownLatch blockInitializeCompletion = new CountDownLatch(1);
    doAnswer(
            x -> {
              initializeCalled.countDown();
              blockInitializeCompletion.await();
              throw new RuntimeException("test");
            })
        .when(mocks.indexFactory)
        .getInitializedIndex(any(), any(), any());

    mocks.indexLifecycleManager.startLifecycle(EXECUTOR, EXECUTOR, EXECUTOR);
    initializeCalled.await();
    mocks.indexLifecycleManager.drop();
    blockInitializeCompletion.countDown();
    waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.DROPPED);
    assertEquals(
        IndexStatus.StatusCode.DOES_NOT_EXIST,
        mocks.indexGeneration.getIndex().getStatus().getStatusCode());
    assertEquals(0.0, mocks.metrics.failedInitializationIndexes.count(), 0.1);
    assertEquals(1.0, mocks.metrics.failedDropIndexes.count(), 0.1);
  }

  @Test
  public void testMultipleInitializeIndexFailures() throws IOException {
    ExecutorService executorService = Executors.newFixedThreadPool(1);
    // Start multiple indexes on same executor.
    {
      Mocks mocks = Mocks.create(false);
      doThrow(IOException.class).when(mocks.indexFactory).getInitializedIndex(any(), any(), any());
      mocks.indexLifecycleManager.startLifecycle(executorService, executorService, executorService);
      waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.SHUTDOWN);
      assertEquals(
          IndexStatus.StatusCode.FAILED,
          mocks.indexGeneration.getIndex().getStatus().getStatusCode());
      assertEquals(
          IndexStatus.Reason.INITIALIZATION_FAILED,
          mocks.indexGeneration.getIndex().getStatus().getReason().orElseThrow());
    }
    {
      Mocks mocks = Mocks.create(false);
      doThrow(IOException.class).when(mocks.indexFactory).getInitializedIndex(any(), any(), any());
      mocks.indexLifecycleManager.startLifecycle(executorService, executorService, executorService);
      waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.SHUTDOWN);
      assertEquals(
          IndexStatus.StatusCode.FAILED,
          mocks.indexGeneration.getIndex().getStatus().getStatusCode());
      assertEquals(
          IndexStatus.Reason.INITIALIZATION_FAILED,
          mocks.indexGeneration.getIndex().getStatus().getReason().orElseThrow());
      assertEquals(1.0, mocks.metrics.failedInitializationIndexes.count(), 0.1);
    }
    {
      Mocks mocks = Mocks.create(false);
      mocks.indexLifecycleManager.startLifecycle(executorService, executorService, executorService);
      assertTrue(waitForState(mocks.indexLifecycleManager, IndexLifecycleManager.State.RUNNING));
      try (InitializedIndex initializedIndex =
          mocks
              .initializedIndexCatalog
              .getIndex(mocks.indexGeneration.getGenerationId())
              .orElseThrow()) {
        assertEquals(IndexStatus.StatusCode.STEADY, initializedIndex.getStatus().getStatusCode());
      }
    }
  }

  // Wait up to 1 minute for the actual state to match the desired state.
  boolean waitForState(
      IndexLifecycleManager indexLifecycleManager, IndexLifecycleManager.State state) {
    try {
      int maxAttempts = 600;
      @Var int attemptCount = 0;
      while (attemptCount < maxAttempts) {
        if (indexLifecycleManager.getState() == state) {
          Thread.sleep(100);
          return true;
        } else {
          Thread.sleep(100);
          attemptCount += 1;
        }
      }
    } catch (InterruptedException e) {
      fail();
    }
    return false;
  }
}
