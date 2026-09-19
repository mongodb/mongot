package com.xgen.mongot.replication.mongodb;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.RecyclableReplicationIndexManager.LeaderStatus;
import com.xgen.mongot.replication.mongodb.common.BufferlessIdOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexStateInfo;
import com.xgen.mongot.replication.mongodb.common.StaleStateInfo;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.Test;

public class RecyclableReplicationIndexManagerTest {

  private static final BsonTimestamp STALE_OPTIME = new BsonTimestamp(39, 975);

  // ── isLeader ────────────────────────────────────────────────────────────────

  @Test
  public void isLeader_newlyCreated_returnsFalse() {
    var rrim = createManager(mockWrappedFactory());

    assertFalse(rrim.isLeader());
  }

  @Test
  public void leaderSignal_setsIsLeaderTrue() {
    var factory = mockWrappedFactory();
    when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));
    var rrim = createManager(factory);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    assertTrue(rrim.isLeader());
  }

  // ── onLeaderStatusChange (LEADER) ───────────────────────────────────────────

  @Test
  public void leaderSignal_createsWrapped() {
    var factory = mockWrappedFactory();
    when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));
    var rrim = createManager(factory);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    verify(factory).get();
  }

  @Test
  public void leaderSignal_idempotent() {
    var factory = mockWrappedFactory();
    when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));
    var rrim = createManager(factory);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    verify(factory, times(1)).get();
    assertTrue(rrim.isLeader());
  }

  @Test
  public void leaderFollowerLeader_createsNewWrapped() {
    var factory = mockWrappedFactory();
    var innerRim1 = stubShutdown(mock(ReplicationIndexManager.class));
    var innerRim2 = mock(ReplicationIndexManager.class);
    when(factory.get()).thenReturn(innerRim1, innerRim2);
    var rrim = createManager(factory);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    verify(factory, times(2)).get();
    assertTrue(rrim.isLeader());
  }

  // ── onLeaderStatusChange (FOLLOWER) ─────────────────────────────────────────

  @Test
  public void followerSignal_clearsIsLeader() {
    var factory = mockWrappedFactory();
    var innerRim = stubShutdown(mock(ReplicationIndexManager.class));
    when(factory.get()).thenReturn(innerRim);
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);

    assertFalse(rrim.isLeader());
  }

  @Test
  public void followerSignal_shutsDownWrapped() {
    var factory = mockWrappedFactory();
    var innerRim = stubShutdown(mock(ReplicationIndexManager.class));
    when(factory.get()).thenReturn(innerRim);
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);

    verify(innerRim).shutdown();
  }

  @Test
  public void followerSignal_whenNotLeader_doesNotCreateWrapped() {
    var factory = mockWrappedFactory();
    var rrim = createManager(factory);

    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);

    verify(factory, never()).get();
    assertFalse(rrim.isLeader());
  }

  @Test
  public void followerSignal_whenNotLeader_idempotent() {
    var factory = mockWrappedFactory();
    var rrim = createManager(factory);

    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);

    verify(factory, never()).get();
    assertFalse(rrim.isLeader());
  }

  // ── getInitFuture ───────────────────────────────────────────────────────────

  @Test
  public void getInitFuture_whenFollower_returnsCompletedFuture() {
    var rrim = createManager(mockWrappedFactory());

    var future = rrim.getInitFuture();

    assertTrue(future.isDone());
    assertFalse(future.isCompletedExceptionally());
  }

  @Test
  public void getInitFuture_whenLeader_delegatesToWrapped() {
    var factory = mockWrappedFactory();
    var innerRim = mock(ReplicationIndexManager.class);
    var innerFuture = new CompletableFuture<Void>();
    when(innerRim.getInitFuture()).thenReturn(innerFuture);
    when(factory.get()).thenReturn(innerRim);
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    assertThat(rrim.getInitFuture()).isSameInstanceAs(innerFuture);
  }

  // ── getState ────────────────────────────────────────────────────────────────

  @Test
  public void getState_initiallyReturnsInitializing() {
    var rrim = createManager(mockWrappedFactory());

    assertThat(rrim.getState()).isEqualTo(IndexManager.State.INITIALIZING);
  }

  @Test
  public void getState_whenLeader_delegatesToWrapped() {
    var factory = mockWrappedFactory();
    var innerRim = mock(ReplicationIndexManager.class);
    when(innerRim.getState()).thenReturn(IndexManager.State.STEADY_STATE);
    when(factory.get()).thenReturn(innerRim);
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    assertThat(rrim.getState()).isEqualTo(IndexManager.State.STEADY_STATE);
  }

  @Test
  public void getState_afterShutdown_whenFollower_returnsShutDown() {
    var rrim = createManager(mockWrappedFactory());

    rrim.shutdown();

    assertThat(rrim.getState()).isEqualTo(IndexManager.State.SHUT_DOWN);
  }

  @Test
  public void getState_afterDrop_whenFollower_returnsShutDown() {
    var rrim = createManager(mockWrappedFactory());

    rrim.drop();

    assertThat(rrim.getState()).isEqualTo(IndexManager.State.SHUT_DOWN);
  }

  // ── shutdown ────────────────────────────────────────────────────────────────

  @Test
  public void shutdown_whenLeader_shutsDownWrapped() {
    var factory = mockWrappedFactory();
    var innerRim = stubShutdown(mock(ReplicationIndexManager.class));
    when(factory.get()).thenReturn(innerRim);
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    rrim.shutdown();

    verify(innerRim).shutdown();
  }

  @Test
  public void shutdown_whenFollower_noShutdownFuture_returnsCompletedFuture() {
    var rrim = createManager(mockWrappedFactory());

    var future = rrim.shutdown();

    assertTrue(future.isDone());
    assertFalse(future.isCompletedExceptionally());
  }

  @Test
  public void shutdown_whenFollower_withPendingShutdownFuture_returnsPendingFuture() {
    var factory = mockWrappedFactory();
    var pendingFuture = new CompletableFuture<Void>();
    var innerRim = mock(ReplicationIndexManager.class);
    when(innerRim.shutdown()).thenReturn(pendingFuture);
    when(factory.get()).thenReturn(innerRim);
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER); // stores pendingFuture as shutdownFuture

    var result = rrim.shutdown();

    assertThat(result).isSameInstanceAs(pendingFuture);
  }

  @Test
  public void shutdown_whenFollower_setsStateToShutDown() {
    var rrim = createManager(mockWrappedFactory());

    rrim.shutdown();

    assertThat(rrim.getState()).isEqualTo(IndexManager.State.SHUT_DOWN);
  }

  // ── drop ────────────────────────────────────────────────────────────────────

  @Test
  public void drop_whenLeader_dropsWrapped() {
    var factory = mockWrappedFactory();
    var innerRim = mock(ReplicationIndexManager.class);
    when(innerRim.drop()).thenReturn(CompletableFuture.completedFuture(null));
    when(factory.get()).thenReturn(innerRim);
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    rrim.drop();

    verify(innerRim).drop();
  }

  @Test
  public void drop_whenFollower_returnsCompletedFuture() {
    var rrim = createManager(mockWrappedFactory());

    var future = rrim.drop();

    assertTrue(future.isDone());
    assertFalse(future.isCompletedExceptionally());
  }

  @Test
  public void drop_whenFollower_withPendingShutdownFuture_returnsPendingFuture() {
    var factory = mockWrappedFactory();
    var pendingFuture = new CompletableFuture<Void>();
    var innerRim = mock(ReplicationIndexManager.class);
    when(innerRim.shutdown()).thenReturn(pendingFuture);
    when(factory.get()).thenReturn(innerRim);
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER); // stores pendingFuture as shutdownFuture

    var result = rrim.drop();

    assertThat(result).isSameInstanceAs(pendingFuture);
  }

  @Test
  public void drop_whenFollower_setsStateToShutDown() {
    var rrim = createManager(mockWrappedFactory());

    rrim.drop();

    assertThat(rrim.getState()).isEqualTo(IndexManager.State.SHUT_DOWN);
  }

  // ── async transitions & coalescing ──────────────────────────────────────────

  @Test
  public void leaderFollowerLeader_withPendingShutdown_awaitsShutdownBeforeNewWrapped() {
    var factory = mockWrappedFactory();
    var innerRim1 = mock(ReplicationIndexManager.class);
    var pendingShutdown = new CompletableFuture<Void>();
    when(innerRim1.shutdown()).thenReturn(pendingShutdown);
    var innerRim2 = mock(ReplicationIndexManager.class);
    when(factory.get()).thenReturn(innerRim1, innerRim2);
    var rrim = RecyclableReplicationIndexManager.createForTesting(factory, Runnable::run);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    // The new LEADER must await full completion of the prior shutdown: no second inner manager yet.
    verify(factory, times(1)).get();
    assertFalse(rrim.isLeader());

    pendingShutdown.complete(null);

    verify(factory, times(2)).get();
    assertTrue(rrim.isLeader());
  }

  @Test
  public void rapidFlapping_endingFollower_convergesToFollower() {
    var factory = mockWrappedFactory();
    var inner1 = stubShutdown(mock(ReplicationIndexManager.class));
    var inner2 = stubShutdown(mock(ReplicationIndexManager.class));
    when(factory.get()).thenReturn(inner1, inner2);
    var rrim = createManager(factory);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);

    assertFalse(rrim.isLeader());
  }

  @Test
  public void rapidFlapping_endingLeader_convergesToLeader() {
    var factory = mockWrappedFactory();
    var inner1 = stubShutdown(mock(ReplicationIndexManager.class));
    var inner2 = stubShutdown(mock(ReplicationIndexManager.class));
    when(factory.get()).thenReturn(inner1, inner2);
    var rrim = createManager(factory);

    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    assertTrue(rrim.isLeader());
  }

  @Test
  public void transitions_onSingleThreadExecutor_doNotDeadlock() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      var factory = mockWrappedFactory();
      var innerRim1 = mock(ReplicationIndexManager.class);
      var pendingShutdown = new CompletableFuture<Void>();
      when(innerRim1.shutdown()).thenReturn(pendingShutdown);
      var innerRim2 = mock(ReplicationIndexManager.class);
      when(factory.get()).thenReturn(innerRim1, innerRim2);
      var rrim = RecyclableReplicationIndexManager.createForTesting(factory, executor);

      rrim.onLeaderStatusChange(LeaderStatus.LEADER);
      verify(factory, timeout(5000).times(1)).get();

      // Wait for the demotion to actually run and park the chain on the (still pending) shutdown
      // before issuing the new LEADER, so the transition is not coalesced away.
      rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);
      verify(innerRim1, timeout(5000)).shutdown();

      rrim.onLeaderStatusChange(LeaderStatus.LEADER);

      // The new LEADER's reconcile is chained after the pending shutdown. Completing the shutdown
      // must let that reconcile run on the single pool thread -- proving reconcile never parked
      // that thread on the shutdown future (a blocking join would deadlock here).
      pendingShutdown.complete(null);

      verify(factory, timeout(5000).times(2)).get();
      executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertTrue(rrim.isLeader());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void demoteDrain_doesNotHoldMonitorAgainstUpcallThread() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      var factory = mockWrappedFactory();
      var innerRim1 = mock(ReplicationIndexManager.class);
      var innerRim2 = mock(ReplicationIndexManager.class);
      when(factory.get()).thenReturn(innerRim1, innerRim2);

      var drainEntered = new CountDownLatch(1);
      var releaseDrain = new CountDownLatch(1);
      var innerShutdown = new CompletableFuture<Void>();
      when(innerRim1.shutdown())
          .thenAnswer(
              invocation -> {
                drainEntered.countDown();
                releaseDrain.await(); // simulate the change-stream read loop draining
                return innerShutdown;
              });

      var rrim = RecyclableReplicationIndexManager.createForTesting(factory, executor);
      rrim.onLeaderStatusChange(LeaderStatus.LEADER);
      verify(factory, timeout(5000)).get();

      // Demote: its reconcile blocks the executor thread inside shutdown() (the simulated drain).
      rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);
      assertTrue(drainEntered.await(5, TimeUnit.SECONDS));

      // While the drain blocks, an FFM upcall must still be able to enter onLeaderStatusChange --
      // i.e. reconcile did not hold this monitor across the blocking shutdown. This would time out
      // if the monitor were held during the drain.
      CompletableFuture<Void> upcall =
          CompletableFuture.runAsync(() -> rrim.onLeaderStatusChange(LeaderStatus.LEADER));
      upcall.get(5, TimeUnit.SECONDS);

      releaseDrain.countDown();
      innerShutdown.complete(null);
    } finally {
      executor.shutdownNow();
    }
  }

  // ── failed-transition handling & periodic reconcile ─────────────────────────

  @Test
  public void leaderTransition_whenFactoryThrows_recordsFailureAndStaysFollower() {
    var factory = mockWrappedFactory();
    when(factory.get()).thenThrow(new RuntimeException("boom"));
    var rrim = createManager(factory);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    assertFalse(rrim.isLeader());
    assertThat(rrim.leadershipTransitionFailureCount()).isEqualTo(1.0);
  }

  @Test
  public void periodicReconcile_afterFailedPromote_createsWrappedOnRetry() {
    var factory = mockWrappedFactory();
    when(factory.get())
        .thenThrow(new RuntimeException("boom"))
        .thenReturn(mock(ReplicationIndexManager.class));
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    assertFalse(rrim.isLeader());

    rrim.triggerReconcileTick();

    assertTrue(rrim.isLeader());
    verify(factory, times(2)).get();
  }

  @Test
  public void periodicReconcile_whenAlreadyConverged_isNoOp() {
    var factory = mockWrappedFactory();
    when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    rrim.triggerReconcileTick();
    rrim.triggerReconcileTick();

    verify(factory, times(1)).get();
    assertTrue(rrim.isLeader());
  }

  // ── follower status recovery (init) ─────────────────────────────────────────

  @Test
  public void followerInit_noCommitData_setsNotStarted() {
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY);

    createFollowerManager(index, FeatureFlags.getDefault());

    assertThat(index.status().getStatusCode()).isEqualTo(IndexStatus.StatusCode.NOT_STARTED);
  }

  @Test
  public void followerInit_exceededLimit_setsFailed() {
    FollowerIndex index = followerIndex(IndexCommitUserData.createExceeded("too big"));

    createFollowerManager(index, FeatureFlags.getDefault());

    assertThat(index.status().getStatusCode()).isEqualTo(IndexStatus.StatusCode.FAILED);
  }

  @Test
  public void followerInit_staleInfo_setsStaleAndUpdatesOptime() {
    ChangeStreamResumeInfo staleResumeInfo =
        ChangeStreamResumeInfo.create(
            new MongoNamespace("db", "coll"), ChangeStreamUtils.resumeToken(STALE_OPTIME));
    StaleStateInfo staleStateInfo =
        StaleStateInfo.create(
            StaleStatusReason.DOCS_EXCEEDED,
            StaleStatusReason.DOCS_EXCEEDED.formatMessage(),
            IndexCommitUserData.createChangeStreamResume(
                staleResumeInfo, IndexFormatVersion.CURRENT.versionNumber));
    FollowerIndex index = followerIndex(IndexCommitUserData.createStale(staleStateInfo));

    createFollowerManager(index, FeatureFlags.getDefault());

    assertThat(index.status().getStatusCode()).isEqualTo(IndexStatus.StatusCode.STALE);
    verify(index.opTimeInfo).update(STALE_OPTIME.getValue(), STALE_OPTIME.getValue());
  }

  @Test
  public void followerInit_collectionNotFound_setsDoesNotExist() {
    FeatureFlags featureFlags =
        FeatureFlags.withDefaults()
            .enable(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)
            .build();
    FollowerIndex index =
        followerIndex(
            IndexCommitUserData.createFromIndexStateInfo(
                IndexStateInfo.create(
                    IndexStatus.StatusCode.DOES_NOT_EXIST,
                    IndexStatus.Reason.COLLECTION_NOT_FOUND)));

    createFollowerManager(index, featureFlags);

    assertThat(index.status().getStatusCode()).isEqualTo(IndexStatus.StatusCode.DOES_NOT_EXIST);
    assertThat(index.status().getReason()).hasValue(IndexStatus.Reason.COLLECTION_NOT_FOUND);
  }

  @Test
  public void followerInit_initialSyncResumeInfo_setsInitialSync() {
    FollowerIndex index =
        followerIndex(
            IndexCommitUserData.createInitialSyncResume(
                IndexFormatVersion.CURRENT.versionNumber,
                new BufferlessIdOrderInitialSyncResumeInfo(
                    new BsonTimestamp(1234567890L), new BsonString("a"))));

    createFollowerManager(index, FeatureFlags.getDefault());

    assertThat(index.status().getStatusCode()).isEqualTo(IndexStatus.StatusCode.INITIAL_SYNC);
  }

  @Test
  public void followerInit_steadyStateResumeInfo_setsSteadyAndUpdatesOptime() {
    FollowerIndex index =
        followerIndex(
            IndexCommitUserData.createChangeStreamResume(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace("database", "collection"),
                    new BsonDocument("_data", new BsonString("820000000000000000"))),
                IndexFormatVersion.CURRENT.versionNumber));

    createFollowerManager(index, FeatureFlags.getDefault());

    assertThat(index.status().getStatusCode()).isEqualTo(IndexStatus.StatusCode.STEADY);
    verify(index.opTimeInfo).update(anyLong(), anyLong());
  }

  @Test
  public void followerInit_whenShutDownBeforeInitRuns_doesNotSetStatus() {
    ManualExecutor executor = new ManualExecutor();
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY);
    var rrim =
        RecyclableReplicationIndexManager.createForTesting(
            mockWrappedFactory(),
            executor,
            index.index,
            FeatureFlags.getDefault(),
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION));

    rrim.shutdown(); // SHUT_DOWN before the deferred follower init runs
    executor.runAll();

    verify(index.index, never()).setStatus(any());
  }

  @Test
  public void getInitFuture_whenFollowerInitPending_reflectsFollowerInit() {
    ManualExecutor executor = new ManualExecutor();
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY);
    var rrim =
        RecyclableReplicationIndexManager.createForTesting(
            mockWrappedFactory(),
            executor,
            index.index,
            FeatureFlags.getDefault(),
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION));

    assertFalse(rrim.getInitFuture().isDone()); // deferred on the manual executor

    executor.runAll();

    assertTrue(rrim.getInitFuture().isDone());
  }

  @Test
  public void promotion_waitsForFollowerInitToComplete() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      CountDownLatch initEntered = new CountDownLatch(1);
      CountDownLatch releaseInit = new CountDownLatch(1);

      var factory = mockWrappedFactory();
      when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));

      // A follower index whose SLS read blocks, so the init stays pending until released.
      InitializedSearchIndex index = mock(InitializedSearchIndex.class);
      IndexWriter writer = mock(IndexWriter.class);
      when(writer.getCommitUserData())
          .thenAnswer(
              invocation -> {
                initEntered.countDown();
                releaseInit.await();
                return IndexCommitUserData.EMPTY.toEncodedData();
              });
      when(index.getWriter()).thenReturn(writer);

      var rrim =
          RecyclableReplicationIndexManager.createForTesting(
              factory,
              executor,
              index,
              FeatureFlags.getDefault(),
              mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION));

      assertTrue(initEntered.await(5, TimeUnit.SECONDS)); // init is running and blocked

      rrim.onLeaderStatusChange(LeaderStatus.LEADER);

      // Promotion composes after the init future, so it must not run while init is blocked -- even
      // though a second pool thread is free.
      Thread.sleep(100);
      verify(factory, never()).get();

      releaseInit.countDown(); // let the init finish

      verify(factory, timeout(5000)).get(); // now promotion proceeds
      assertTrue(rrim.isLeader());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void failedFollowerInit_stillPromotesAndIsCountedWithoutBlockingReadiness() {
    var factory = mockWrappedFactory();
    when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));

    // A follower index whose SLS read fails.
    InitializedSearchIndex index = mock(InitializedSearchIndex.class);
    IndexWriter writer = mock(IndexWriter.class);
    when(writer.getCommitUserData())
        .thenThrow(new UncheckedIOException("SLS unavailable", new IOException("boom")));
    when(index.getWriter()).thenReturn(writer);

    var rrim =
        RecyclableReplicationIndexManager.createForTesting(
            factory,
            index,
            FeatureFlags.getDefault(),
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION)); // runs init synchronously

    // A failed follower init must NOT fail the node-wide readiness gate: the init future completes
    // normally, and the failure is recorded on the counter instead.
    assertTrue(rrim.getInitFuture().isDone());
    assertFalse(rrim.getInitFuture().isCompletedExceptionally());
    assertThat(rrim.followerInitFailureCount()).isEqualTo(1.0);

    // And it must not block promotion.
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    verify(factory).get();
    assertTrue(rrim.isLeader());
  }

  // ── periodic follower status refresh (poll) ─────────────────────────────────

  @Test
  public void periodicRefresh_whenCommittedDataChanged_republishesStatus() {
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY, steadyCommitData());
    var rrim = createFollowerManager(index, FeatureFlags.getDefault());
    assertThat(index.status().getStatusCode()).isEqualTo(IndexStatus.StatusCode.NOT_STARTED);

    rrim.triggerReconcileTick();

    assertThat(index.status().getStatusCode()).isEqualTo(IndexStatus.StatusCode.STEADY);
    verify(index.opTimeInfo).update(anyLong(), anyLong());
  }

  @Test
  public void periodicRefresh_whenCommittedDataUnchanged_doesNotRepublish() {
    FollowerIndex index = followerIndex(steadyCommitData());
    var rrim = createFollowerManager(index, FeatureFlags.getDefault());

    rrim.triggerReconcileTick();

    // The one-time init publishes once; an unchanged poll must not republish.
    verify(index.index, times(1)).setStatus(any());
  }

  @Test
  public void periodicRefresh_whenLeader_doesNotRead() {
    var factory = mockWrappedFactory();
    when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY);
    var rrim =
        RecyclableReplicationIndexManager.createForTesting(
            factory,
            index.index,
            FeatureFlags.getDefault(),
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION));
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    rrim.triggerReconcileTick();

    // Only the one-time init read; once leader, the inner manager owns status.
    verify(index.writer, times(1)).getCommitUserData();
  }

  @Test
  public void periodicRefresh_afterDemotion_republishesEvenWhenDataUnchanged() {
    var factory = mockWrappedFactory();
    var inner = stubShutdown(mock(ReplicationIndexManager.class));
    when(factory.get()).thenReturn(inner);
    FollowerIndex index = followerIndex(steadyCommitData());
    var rrim =
        RecyclableReplicationIndexManager.createForTesting(
            factory,
            index.index,
            FeatureFlags.getDefault(),
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION));
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);

    rrim.triggerReconcileTick();

    // init publish + post-demotion republish, even though the committed data never changed.
    verify(index.index, times(2)).setStatus(any());
  }

  @Test
  public void periodicRefresh_whenReadThrows_incrementsCounterAndKeepsInitFutureCompleted() {
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY);
    when(index.writer.getCommitUserData())
        .thenReturn(IndexCommitUserData.EMPTY.toEncodedData())
        .thenThrow(new UncheckedIOException("SLS unavailable", new IOException("boom")));
    var rrim = createFollowerManager(index, FeatureFlags.getDefault());

    rrim.triggerReconcileTick();

    assertTrue(rrim.getInitFuture().isDone());
    assertFalse(rrim.getInitFuture().isCompletedExceptionally());
    assertThat(rrim.followerRefreshFailureCount()).isEqualTo(1.0);
  }

  @Test
  public void periodicRefresh_whenReadThrows_retriesOnNextTick() {
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY);
    when(index.writer.getCommitUserData())
        .thenReturn(IndexCommitUserData.EMPTY.toEncodedData())
        .thenThrow(new UncheckedIOException("SLS unavailable", new IOException("boom")))
        .thenReturn(IndexCommitUserData.createExceeded("too big").toEncodedData());
    var rrim = createFollowerManager(index, FeatureFlags.getDefault());

    rrim.triggerReconcileTick(); // read throws: counted, no publish
    rrim.triggerReconcileTick(); // read succeeds: republishes

    assertThat(index.status().getStatusCode()).isEqualTo(IndexStatus.StatusCode.FAILED);
    assertThat(rrim.followerRefreshFailureCount()).isEqualTo(1.0);
  }

  @Test
  public void periodicRefresh_whilePreviousRefreshInFlight_skips() {
    ManualExecutor executor = new ManualExecutor();
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY, steadyCommitData());
    var rrim =
        RecyclableReplicationIndexManager.createForTesting(
            mockWrappedFactory(),
            executor,
            index.index,
            FeatureFlags.getDefault(),
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION));
    executor.runAll(); // one-time init read

    rrim.triggerReconcileTick(); // dispatches a poll (now in flight, still queued)
    rrim.triggerReconcileTick(); // skipped: a poll is already in flight
    executor.runAll(); // runs the single queued poll

    verify(index.writer, times(2)).getCommitUserData(); // init + one poll, not three
  }

  @Test
  public void periodicRefresh_beforeFollowerInitCompletes_skips() {
    ManualExecutor executor = new ManualExecutor();
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY);
    var rrim =
        RecyclableReplicationIndexManager.createForTesting(
            mockWrappedFactory(),
            executor,
            index.index,
            FeatureFlags.getDefault(),
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION));

    rrim.triggerReconcileTick(); // init not done yet: skip

    executor.runAll(); // only the one-time init runs
    verify(index.writer, times(1)).getCommitUserData();
  }

  @Test
  public void shutdown_whilePeriodicRefreshDeferred_doesNotSetStatus() {
    ManualExecutor executor = new ManualExecutor();
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY, steadyCommitData());
    var rrim =
        RecyclableReplicationIndexManager.createForTesting(
            mockWrappedFactory(),
            executor,
            index.index,
            FeatureFlags.getDefault(),
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION));
    executor.runAll(); // init publishes NOT_STARTED
    rrim.triggerReconcileTick(); // dispatches a poll, deferred on the manual executor

    rrim.shutdown(); // SHUT_DOWN before the deferred poll runs
    executor.runAll(); // the poll runs but must skip publishing

    assertThat(index.status().getStatusCode()).isEqualTo(IndexStatus.StatusCode.NOT_STARTED);
    verify(index.index, times(1)).setStatus(any());
  }

  @Test
  public void promotionDuringDeferredRefresh_leaderStatusWins() {
    var factory = mockWrappedFactory();
    when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY);
    var rrim =
        RecyclableReplicationIndexManager.createForTesting(
            factory,
            index.index,
            FeatureFlags.getDefault(),
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION));
    // The next read promotes this node mid-read; the post-read leader-wins guard must suppress the
    // follower publish so the inner manager owns status.
    when(index.writer.getCommitUserData())
        .thenAnswer(
            invocation -> {
              rrim.onLeaderStatusChange(LeaderStatus.LEADER);
              return steadyCommitData().toEncodedData();
            });

    rrim.triggerReconcileTick();

    assertTrue(rrim.isLeader());
    assertThat(index.status().getStatusCode()).isEqualTo(IndexStatus.StatusCode.NOT_STARTED);
  }

  @Test
  public void periodicRefresh_promotionBeforeOptimeUpdate_doesNotRegressLeaderOptime() {
    var factory = mockWrappedFactory();
    when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));
    // Init reads EMPTY (NOT_STARTED, no optime update); the poll reads steady data, which would
    // otherwise take the optime-updating branch.
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY, steadyCommitData());
    var rrim =
        RecyclableReplicationIndexManager.createForTesting(
            factory,
            index.index,
            FeatureFlags.getDefault(),
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION));

    // Land a promotion in the narrow window after the poll's follower re-check but before its
    // optime update: fetching the unset lock is the step immediately preceding update(). The guard
    // around the update must suppress it, so a stale follower poll cannot regress the newly
    // promoted leader's optime metric even though its status write is dropped.
    when(index.opTimeInfo.getUnsetLock())
        .thenAnswer(
            invocation -> {
              rrim.onLeaderStatusChange(LeaderStatus.LEADER);
              return new Object();
            });

    rrim.triggerReconcileTick();

    assertTrue(rrim.isLeader());
    verify(index.opTimeInfo, never()).update(anyLong(), anyLong());
  }

  @Test
  public void periodicRefresh_whenPromotionPending_skips() {
    ManualExecutor executor = new ManualExecutor();
    var factory = mockWrappedFactory();
    when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));
    FollowerIndex index = followerIndex(IndexCommitUserData.EMPTY, steadyCommitData());
    var rrim =
        RecyclableReplicationIndexManager.createForTesting(
            factory,
            executor,
            index.index,
            FeatureFlags.getDefault(),
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION));
    executor.runAll(); // one-time init completes (reads EMPTY)

    // A promotion is signaled but its reconcile is still queued on the manual executor. The tick
    // must not dispatch a blocking poll while a promotion is pending, so a slow read cannot occupy
    // the shared transition executor ahead of that promotion.
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    rrim.triggerReconcileTick();
    executor.runAll(); // runs the queued reconcile -> promotes

    assertTrue(rrim.isLeader());
    verify(index.writer, times(1)).getCommitUserData(); // only the init read; the poll was skipped
  }

  // ── helpers ─────────────────────────────────────────────────────────────────

  @SuppressWarnings("unchecked")
  private static Supplier<ReplicationIndexManager> mockWrappedFactory() {
    return mock(Supplier.class);
  }

  private static RecyclableReplicationIndexManager createFollowerManager(
      FollowerIndex index, FeatureFlags featureFlags) {
    return RecyclableReplicationIndexManager.createForTesting(
        mockWrappedFactory(),
        index.index,
        featureFlags,
        mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION));
  }

  private static IndexCommitUserData steadyCommitData() {
    return IndexCommitUserData.createChangeStreamResume(
        ChangeStreamResumeInfo.create(
            new MongoNamespace("database", "collection"),
            new BsonDocument("_data", new BsonString("820000000000000000"))),
        IndexFormatVersion.CURRENT.versionNumber);
  }

  private static FollowerIndex followerIndex(IndexCommitUserData... commitData) {
    // InitializedIndex is a sealed interface; mock the concrete permitted type instead.
    InitializedSearchIndex index = mock(InitializedSearchIndex.class);

    // Stub getCommitUserData to return each commit datum in order (the last repeats), so a test can
    // drive the one-time init read followed by successive periodic-poll reads.
    IndexWriter writer = mock(IndexWriter.class);
    EncodedUserData first = commitData[0].toEncodedData();
    EncodedUserData[] rest =
        Arrays.stream(commitData, 1, commitData.length)
            .map(IndexCommitUserData::toEncodedData)
            .toArray(EncodedUserData[]::new);
    when(writer.getCommitUserData()).thenReturn(first, rest);
    when(index.getWriter()).thenReturn(writer);

    ReplicationOpTimeInfo opTimeInfo = mock(ReplicationOpTimeInfo.class);
    when(opTimeInfo.getUnsetLock()).thenReturn(new Object());
    IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater =
        mock(IndexMetricsUpdater.IndexingMetricsUpdater.class);
    when(indexingMetricsUpdater.getReplicationOpTimeInfo()).thenReturn(opTimeInfo);
    IndexMetricsUpdater metricsUpdater = mock(IndexMetricsUpdater.class);
    when(metricsUpdater.getIndexingMetricsUpdater()).thenReturn(indexingMetricsUpdater);
    when(index.getMetricsUpdater()).thenReturn(metricsUpdater);

    AtomicReference<IndexStatus> statusRef = new AtomicReference<>(IndexStatus.unknown());
    doAnswer(
            invocation -> {
              statusRef.set(invocation.getArgument(0));
              return null;
            })
        .when(index)
        .setStatus(any());
    when(index.getStatus()).thenAnswer(invocation -> statusRef.get());

    return new FollowerIndex(index, writer, opTimeInfo, statusRef);
  }

  /** Bundles a mock follower index with the captured status and optime metric for assertions. */
  private static final class FollowerIndex {
    final InitializedIndex index;
    final IndexWriter writer;
    final ReplicationOpTimeInfo opTimeInfo;
    private final AtomicReference<IndexStatus> statusRef;

    FollowerIndex(
        InitializedIndex index,
        IndexWriter writer,
        ReplicationOpTimeInfo opTimeInfo,
        AtomicReference<IndexStatus> statusRef) {
      this.index = index;
      this.writer = writer;
      this.opTimeInfo = opTimeInfo;
      this.statusRef = statusRef;
    }

    IndexStatus status() {
      return this.statusRef.get();
    }
  }

  /**
   * Executor that queues tasks until {@link #runAll()} so tests can control follower-init timing.
   */
  private static final class ManualExecutor implements Executor {
    private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();

    @Override
    public void execute(Runnable command) {
      this.tasks.add(command);
    }

    void runAll() {
      while (!this.tasks.isEmpty()) {
        this.tasks.poll().run();
      }
    }
  }

  private static ReplicationIndexManager stubShutdown(ReplicationIndexManager rim) {
    when(rim.shutdown()).thenReturn(CompletableFuture.completedFuture(null));
    return rim;
  }

  private RecyclableReplicationIndexManager createManager(
      Supplier<ReplicationIndexManager> wrappedFactory) {
    return RecyclableReplicationIndexManager.createForTesting(wrappedFactory);
  }
}
