package com.xgen.mongot.replication.mongodb;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.replication.mongodb.RecyclableReplicationIndexManager.LeaderStatus;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.Test;

public class RecyclableReplicationIndexManagerTest {

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

  // ── helpers ─────────────────────────────────────────────────────────────────

  @SuppressWarnings("unchecked")
  private static Supplier<ReplicationIndexManager> mockWrappedFactory() {
    return mock(Supplier.class);
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
