package com.xgen.mongot.replication.mongodb;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
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
import static org.mockito.Mockito.withSettings;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.CommitDataStagingWriter;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.PreparableWriter;
import com.xgen.mongot.index.PrepareResult;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.RecyclableReplicationIndexManager.LeaderStatus;
import com.xgen.mongot.replication.mongodb.common.BufferlessIdOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexStateInfo;
import com.xgen.mongot.replication.mongodb.common.StaleStateInfo;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
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
  public void duplicateLeaderSignal_recyclesSession() {
    // A duplicate LEADER bumps the leadership epoch, so the session created under
    // the older epoch is stopped and re-prepared rather than coalesced away
    // Leadership edges are not trusted to be deduplicated.
    var factory = mockWrappedFactory();
    var innerRim1 = stubShutdown(mock(ReplicationIndexManager.class));
    var innerRim2 = mock(ReplicationIndexManager.class);
    when(factory.get()).thenReturn(innerRim1, innerRim2);
    var rrim = createManager(factory);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    verify(factory, times(2)).get();
    verify(innerRim1).shutdown();
    assertTrue(rrim.isLeader());
  }

  // ── prepare-before-create ──────────────────────────────────────────────────

  @Test
  public void prepare_ready_createsWrapped() {
    var factory = mockWrappedFactory();
    when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));
    var rrim =
        createManagerWithWriter(factory, () -> new PrepareResult.Ready(EncodedUserData.EMPTY));

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    verify(factory).get();
    assertTrue(rrim.isLeader());
  }

  @Test
  public void prepare_runsBeforeFactory() throws Exception {
    // The native writer must be prepared before the inner manager is created, so
    // RIM.init reads the prepared token.
    var factory = mockWrappedFactory();
    when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));
    var prepared = new AtomicBoolean(false);
    PreparableWriter writer =
        () -> {
          prepared.set(true);
          return new PrepareResult.Ready(EncodedUserData.EMPTY);
        };
    when(factory.get())
        .thenAnswer(
            invocation -> {
              assertTrue("factory.get() ran before prepareWriter()", prepared.get());
              return mock(ReplicationIndexManager.class);
            });
    var rrim = createManagerWithWriter(factory, writer);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    verify(factory).get();
    assertTrue(rrim.isLeader());
  }

  @Test
  public void prepare_superseded_doesNotCreateWrapped() {
    var factory = mockWrappedFactory();
    var rrim = createManagerWithWriter(factory, PrepareResult.Superseded::new);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    verify(factory, never()).get();
    assertFalse(rrim.isLeader());
    assertThat(rrim.prepareSupersededCount()).isEqualTo(1.0);
  }

  @Test
  public void prepare_error_doesNotCreateWrapped_countsFailure() {
    var factory = mockWrappedFactory();
    PreparableWriter writer =
        () -> {
          throw new IOException("prepare boom");
        };
    var rrim = createManagerWithWriter(factory, writer);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    verify(factory, never()).get();
    assertFalse(rrim.isLeader());
    assertThat(rrim.leadershipTransitionFailureCount()).isEqualTo(1.0);
  }

  // ── prepare currency & executor ─────────────────────────────────────────────

  @Test
  public void prepare_runsOnPrepareExecutor() throws Exception {
    // Native prepare is I/O-bound: it must run on the dedicated prepare executor,
    // never the caller (FFM upcall) or the transition-chain thread.
    ThreadFactory named =
        runnable -> {
          Thread thread = new Thread(runnable, "rrim-prepare-test");
          thread.setDaemon(true);
          return thread;
        };
    ExecutorService prepares = Executors.newSingleThreadExecutor(named);
    try {
      var factory = mockWrappedFactory();
      when(factory.get()).thenReturn(mock(ReplicationIndexManager.class));
      AtomicReference<String> prepareThread = new AtomicReference<>();
      String callerThread = Thread.currentThread().getName();
      PreparableWriter writer =
          () -> {
            prepareThread.set(Thread.currentThread().getName());
            return new PrepareResult.Ready(EncodedUserData.EMPTY);
          };
      // Transitions run synchronously on the caller; only prepare should hop threads.
      var rrim =
          RecyclableReplicationIndexManager.createForTesting(
              factory, Runnable::run, writer, prepares);

      rrim.onLeaderStatusChange(LeaderStatus.LEADER);
      fence(prepares); // prepareAndPromote (Ready) runs and creates the wrapped manager

      assertThat(prepareThread.get()).isEqualTo("rrim-prepare-test");
      assertThat(prepareThread.get()).isNotEqualTo(callerThread);
      verify(factory).get();
      assertTrue(rrim.isLeader());
    } finally {
      prepares.shutdownNow();
    }
  }

  @Test
  public void followerDuringPrepare_doesNotCreateWrapped() throws Exception {
    // A FOLLOWER signal that arrives while prepare is in flight must win: the now
    // stale Ready result is discarded and no inner manager is created.
    ExecutorService transitions = Executors.newSingleThreadExecutor();
    ExecutorService prepares = Executors.newSingleThreadExecutor();
    try {
      var factory = mockWrappedFactory();
      CountDownLatch prepareEntered = new CountDownLatch(1);
      CountDownLatch releasePrepare = new CountDownLatch(1);
      var rrim =
          RecyclableReplicationIndexManager.createForTesting(
              factory, transitions, gatedReadyWriter(prepareEntered, releasePrepare), prepares);

      rrim.onLeaderStatusChange(LeaderStatus.LEADER);
      assertTrue(prepareEntered.await(5, TimeUnit.SECONDS)); // prepare running, blocked

      rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER); // leadership moves away mid-prepare
      releasePrepare.countDown(); // prepare returns Ready, but currency has changed

      fence(prepares); // prepareAndPromote's post-prepare currency re-check runs
      fence(transitions); // and any reconcile it re-drives settles

      verify(factory, never()).get();
      assertFalse(rrim.isLeader());
    } finally {
      transitions.shutdownNow();
      prepares.shutdownNow();
    }
  }

  @Test
  public void shutdownDuringPrepare_doesNotCreateWrappedAndSettlesShutDown() throws Exception {
    // A shutdown while prepare is in flight must settle it: the in-flight prepare
    // observes SHUT_DOWN on completion and installs nothing.
    ExecutorService transitions = Executors.newSingleThreadExecutor();
    ExecutorService prepares = Executors.newSingleThreadExecutor();
    try {
      var factory = mockWrappedFactory();
      CountDownLatch prepareEntered = new CountDownLatch(1);
      CountDownLatch releasePrepare = new CountDownLatch(1);
      var rrim =
          RecyclableReplicationIndexManager.createForTesting(
              factory, transitions, gatedReadyWriter(prepareEntered, releasePrepare), prepares);

      rrim.onLeaderStatusChange(LeaderStatus.LEADER);
      assertTrue(prepareEntered.await(5, TimeUnit.SECONDS));

      CompletableFuture<Void> shutdown = rrim.shutdown(); // SHUT_DOWN while prepare is in flight
      releasePrepare.countDown();

      fence(prepares); // prepare completes, observes SHUT_DOWN, and creates nothing

      assertTrue(shutdown.isDone());
      verify(factory, never()).get();
      assertFalse(rrim.isLeader());
      assertThat(rrim.getState()).isEqualTo(IndexManager.State.SHUT_DOWN);
    } finally {
      transitions.shutdownNow();
      prepares.shutdownNow();
    }
  }

  @Test
  public void dropDuringPrepare_doesNotCreateWrappedAndSettlesShutDown() throws Exception {
    // Same as the shutdown case, via drop().
    ExecutorService transitions = Executors.newSingleThreadExecutor();
    ExecutorService prepares = Executors.newSingleThreadExecutor();
    try {
      var factory = mockWrappedFactory();
      CountDownLatch prepareEntered = new CountDownLatch(1);
      CountDownLatch releasePrepare = new CountDownLatch(1);
      var rrim =
          RecyclableReplicationIndexManager.createForTesting(
              factory, transitions, gatedReadyWriter(prepareEntered, releasePrepare), prepares);

      rrim.onLeaderStatusChange(LeaderStatus.LEADER);
      assertTrue(prepareEntered.await(5, TimeUnit.SECONDS));

      CompletableFuture<Void> drop = rrim.drop(); // dropped while prepare is in flight
      releasePrepare.countDown();

      fence(prepares);

      assertTrue(drop.isDone());
      verify(factory, never()).get();
      assertFalse(rrim.isLeader());
      assertThat(rrim.getState()).isEqualTo(IndexManager.State.SHUT_DOWN);
    } finally {
      transitions.shutdownNow();
      prepares.shutdownNow();
    }
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

  // ── getStateWithoutLock ─────────────────────────────────────────────────────

  @Test
  public void getStateWithoutLock_initiallyReturnsInitializing() {
    var rrim = createManager(mockWrappedFactory());

    assertThat(rrim.getStateWithoutLock()).isEqualTo(IndexManager.State.INITIALIZING);
  }

  @Test
  public void getStateWithoutLock_whenLeader_delegatesToWrappedWithoutLock() {
    var factory = mockWrappedFactory();
    var innerRim = mock(ReplicationIndexManager.class);
    when(innerRim.getStateWithoutLock()).thenReturn(IndexManager.State.STEADY_STATE);
    when(factory.get()).thenReturn(innerRim);
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    assertThat(rrim.getStateWithoutLock()).isEqualTo(IndexManager.State.STEADY_STATE);
    verify(innerRim, never()).getState();
  }

  @Test
  public void getStateWithoutLock_afterShutdown_whenLeader_returnsShutDown() {
    var factory = mockWrappedFactory();
    var innerRim = stubShutdown(mock(ReplicationIndexManager.class));
    when(factory.get()).thenReturn(innerRim);
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    rrim.shutdown();

    assertThat(rrim.getStateWithoutLock()).isEqualTo(IndexManager.State.SHUT_DOWN);
  }

  @Test
  public void getStateWithoutLock_whileMonitorHeld_doesNotBlock() throws Exception {
    var rrim = createManager(mockWrappedFactory());
    CountDownLatch monitorHeld = new CountDownLatch(1);
    CountDownLatch releaseMonitor = new CountDownLatch(1);
    Thread holder =
        new Thread(
            () -> {
              synchronized (rrim) {
                monitorHeld.countDown();
                try {
                  releaseMonitor.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
            });
    holder.start();
    try {
      assertTrue(monitorHeld.await(5, TimeUnit.SECONDS));

      IndexManager.State state =
          CompletableFuture.supplyAsync(rrim::getStateWithoutLock).get(5, TimeUnit.SECONDS);

      assertThat(state).isEqualTo(IndexManager.State.INITIALIZING);
    } finally {
      releaseMonitor.countDown();
      holder.join();
    }
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
  public void shutdown_whileInnerDraining_completesOnlyAfterTheDrain() throws Exception {
    // N9 regression: a shutdown() racing a reconcile-owned drain must return a future completed
    // by that drain, never a stale completed future that lies about the inner manager still
    // draining.
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      var factory = mockWrappedFactory();
      var innerRim = mock(ReplicationIndexManager.class);
      var drainEntered = new CountDownLatch(1);
      var releaseDrain = new CountDownLatch(1);
      var innerShutdown = new CompletableFuture<Void>();
      when(innerRim.shutdown())
          .thenAnswer(
              invocation -> {
                drainEntered.countDown();
                releaseDrain.await(5, TimeUnit.SECONDS);
                return innerShutdown;
              });
      when(factory.get()).thenReturn(innerRim);
      var rrim = RecyclableReplicationIndexManager.createForTesting(factory, executor);
      rrim.onLeaderStatusChange(LeaderStatus.LEADER);
      verify(factory, timeout(5000)).get();

      // Demote: the reconcile parks the executor thread inside the inner shutdown().
      rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER);
      assertTrue(drainEntered.await(5, TimeUnit.SECONDS));

      CompletableFuture<Void> terminal = rrim.shutdown(); // during the drain
      assertFalse(terminal.isDone());

      releaseDrain.countDown();
      innerShutdown.complete(null);

      // The settle runs on the drain's thread, so await it rather than assert synchronously.
      terminal.get(5, TimeUnit.SECONDS);
      assertFalse(terminal.isCompletedExceptionally());
      verify(innerRim, times(1)).shutdown(); // exactly one stop
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void shutdown_whileATerminateOwnedDrainIsInFlight_completesOnlyAfterTheDrain() {
    // The first shutdown() owns the stop; a second shutdown() during that drain must see the
    // session's future still pending (it used to complete it immediately, letting callers tear
    // down executors mid-drain). The inner mock returns a pending future immediately -- the real
    // RIM shape: the drain runs on the inner manager's executors and its future completes later.
    var factory = mockWrappedFactory();
    var innerRim = mock(ReplicationIndexManager.class);
    var innerShutdown = new CompletableFuture<Void>();
    when(innerRim.shutdown()).thenReturn(innerShutdown);
    when(factory.get()).thenReturn(innerRim);
    var rrim = RecyclableReplicationIndexManager.createForTesting(factory, Runnable::run);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    verify(factory, timeout(5000)).get();

    var first = rrim.shutdown(); // owns the stop
    CompletableFuture<Void> second = rrim.shutdown(); // during the terminate-owned drain
    assertThat(second).isSameInstanceAs(first);
    assertFalse(second.isDone());

    innerShutdown.complete(null);

    assertTrue(second.isDone());
    assertFalse(second.isCompletedExceptionally());
    verify(innerRim, times(1)).shutdown(); // the second terminal owned no stop
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
  public void drop_whenFollower_setsStateToShutDown() {
    var rrim = createManager(mockWrappedFactory());

    rrim.drop();

    assertThat(rrim.getState()).isEqualTo(IndexManager.State.SHUT_DOWN);
  }

  @Test
  public void drop_afterSettledShutdown_returnsSettledFutureAndDoesNotDropInner() {
    // A drop() after a settled shutdown has nothing left to stop: it returns the settled terminal
    // future and does not touch the inner manager again. The physical close and drop of the index
    // is performed by the caller above this manager (IndexActions.dropIndex, after this future
    // settles), so a chained inner drop would be redundant and race that cleanup.
    var factory = mockWrappedFactory();
    var innerRim = stubShutdown(mock(ReplicationIndexManager.class));
    when(factory.get()).thenReturn(innerRim);
    var rrim = createManager(factory);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    rrim.shutdown();
    var terminal = rrim.drop();

    assertTrue(terminal.isDone());
    assertFalse(terminal.isCompletedExceptionally());
    verify(innerRim, never()).drop();
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

  // ── status reads never hold the monitor across inner calls ─────────────────

  @Test
  public void getState_blockedInInnerState_doesNotBlockTheUpcallThread() throws Exception {
    // A metrics scrape calls getState() on every manager; the inner manager's monitor is held
    // across native commits and drains. getState() must therefore read the inner state OFF the
    // RRIM monitor: a scrape parked in the inner accessor must not stall a concurrent leadership
    // upcall (and the Rust leadership task behind it).
    var factory = mockWrappedFactory();
    var innerRim = mock(ReplicationIndexManager.class);
    var stateEntered = new CountDownLatch(1);
    var releaseState = new CountDownLatch(1);
    when(innerRim.getState())
        .thenAnswer(
            invocation -> {
              stateEntered.countDown();
              releaseState.await(
                  5, TimeUnit.SECONDS); // simulate the scrape parked in the inner read
              return IndexManager.State.STEADY_STATE;
            });
    when(innerRim.shutdown()).thenReturn(CompletableFuture.completedFuture(null));
    when(factory.get()).thenReturn(innerRim);
    var rrim = RecyclableReplicationIndexManager.createForTesting(factory, Runnable::run);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // a session is running, so getState() delegates

    CompletableFuture<IndexManager.State> scrape =
        CompletableFuture.supplyAsync(rrim::getState); // parks in the inner getState()
    assertTrue(stateEntered.await(5, TimeUnit.SECONDS));

    // The upcall must complete while the scrape is STILL parked in the inner read (it stays
    // parked until released below, or up to its own 5s self-release). A short deadline is what
    // makes this discriminating: with the old monitor-holding accessors the upcall waits behind
    // the scrape's inner read and times out here.
    CompletableFuture<Void> upcall =
        CompletableFuture.runAsync(() -> rrim.onLeaderStatusChange(LeaderStatus.LEADER));
    upcall.get(1, TimeUnit.SECONDS);

    releaseState.countDown();
    assertThat(scrape.get(5, TimeUnit.SECONDS)).isEqualTo(IndexManager.State.STEADY_STATE);
  }

  @Test
  public void getInitFuture_blockedInInnerInit_doesNotBlockTheUpcallThread() throws Exception {
    // Same lock rule as getState(), for the init future.
    var factory = mockWrappedFactory();
    var innerRim = mock(ReplicationIndexManager.class);
    var innerFuture = new CompletableFuture<Void>();
    var readEntered = new CountDownLatch(1);
    var releaseRead = new CountDownLatch(1);
    when(innerRim.getInitFuture())
        .thenAnswer(
            invocation -> {
              readEntered.countDown();
              releaseRead.await(5, TimeUnit.SECONDS);
              return innerFuture;
            });
    when(factory.get()).thenReturn(innerRim);
    var rrim = RecyclableReplicationIndexManager.createForTesting(factory, Runnable::run);
    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    CompletableFuture<CompletableFuture<Void>> scrape =
        CompletableFuture.supplyAsync(rrim::getInitFuture);
    assertTrue(readEntered.await(5, TimeUnit.SECONDS));

    // Same discrimination as the getState variant: the upcall must complete while the read is
    // still parked (until released below, or up to its own 5s self-release); the old
    // monitor-holding accessor times out here.
    CompletableFuture<Void> upcall =
        CompletableFuture.runAsync(() -> rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER));
    upcall.get(1, TimeUnit.SECONDS);

    releaseRead.countDown();
    assertThat(scrape.get(5, TimeUnit.SECONDS)).isSameInstanceAs(innerFuture);
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

  // ── promotion deferred on a queued initial-sync entry ───────────────────────

  @Test
  public void promote_whileInitialSyncEntryQueued_defersUntilTheEntryIsCleared() throws Exception {
    // The previous session's queued initial sync (cancelled but not yet dequeued) must gate the
    // promotion: the next manager's enqueue would trip the queue's already-cancelled check and
    // fail the index. The promotion is deferred and re-checked, not dropped.
    var entryPresent = new AtomicBoolean(true);
    AtomicReference<Runnable> recheck = new AtomicReference<>();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    when(scheduler.schedule(any(Runnable.class), anyLong(), any()))
        .thenAnswer(
            invocation -> {
              recheck.set(invocation.getArgument(0));
              return null;
            });

    var fixture = new SessionFencingFixture();
    AtomicInteger prepares = new AtomicInteger();
    PreparableWriter countingWriter =
        () -> {
          prepares.incrementAndGet();
          return new PrepareResult.Ready(EncodedUserData.EMPTY);
        };
    @SuppressWarnings("unchecked")
    Function<DocumentIndexer, ReplicationIndexManager> factory = mock(Function.class);
    when(factory.apply(any())).thenReturn(mock(ReplicationIndexManager.class));
    var rrim = fixture.rrim(factory, countingWriter, entryPresent::get, scheduler);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    // Deferred: no prepare, no manager, one counted wait, and a re-check scheduled 1s out.
    verify(factory, never()).apply(any());
    assertThat(prepares.get()).isEqualTo(0);
    assertThat(rrim.initialSyncEntryWaitCount()).isEqualTo(1.0);
    assertThat(recheck.get()).isNotNull();

    entryPresent.set(false);
    recheck.get().run(); // the re-check fires and re-drives the promotion

    verify(factory, timeout(5000)).apply(any());
    assertThat(prepares.get()).isEqualTo(1);
    assertTrue(rrim.isLeader());
  }

  @Test
  public void promote_whileInitialSyncEntryStaysQueued_defersAgain() {
    // Each reconcile taken while the entry is queued defers again and counts the wait; nothing
    // is prepared.
    var entryPresent = new AtomicBoolean(true);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    when(scheduler.schedule(any(Runnable.class), anyLong(), any()))
        .thenAnswer(invocation -> invocation.<Runnable>getArgument(0));

    var fixture = new SessionFencingFixture();
    @SuppressWarnings("unchecked")
    Function<DocumentIndexer, ReplicationIndexManager> factory = mock(Function.class);
    when(factory.apply(any())).thenReturn(mock(ReplicationIndexManager.class));
    var rrim =
        fixture.rrim(
            factory,
            () -> new PrepareResult.Ready(EncodedUserData.EMPTY),
            entryPresent::get,
            scheduler);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);
    rrim.triggerReconcileTick();

    verify(factory, never()).apply(any());
    assertThat(rrim.initialSyncEntryWaitCount()).isEqualTo(2.0);
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

  // ── per-session document indexer ─────────────

  @Test
  public void promote_handsSessionIndexerToFactory() {
    var fixture = new SessionFencingFixture();
    AtomicReference<DocumentIndexer> handed = new AtomicReference<>();
    var rrim =
        fixture.rrim(
            indexer -> {
              handed.set(indexer);
              return mock(ReplicationIndexManager.class);
            });

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    assertThat(handed.get()).isInstanceOf(SessionFencedDocumentIndexer.class);
  }

  @Test
  public void promote_beginsTenureBeforeFactory() throws Exception {
    // RIM.init may write immediately, so the session indexer must already be tenured when the
    // factory runs: a stage from within the factory reaches the writer instead of being dropped.
    var fixture = new SessionFencingFixture();
    var rrim =
        fixture.rrim(
            indexer -> {
              try {
                indexer.stageCommitUserData();
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
              return mock(ReplicationIndexManager.class);
            });

    rrim.onLeaderStatusChange(LeaderStatus.LEADER);

    verify(fixture.stagingWriter).stageUserData(any());
    assertThat(fixture.staleWrites()).isEqualTo(0.0);
  }

  @Test
  public void staleStageAfterShutdownFuture_isDropped_nextSessionUnaffected() throws Exception {
    // Scenario 10 / C6: an old session's stage that lands after its RIM's shutdown future completed
    // and the next session started is dropped by the ended tenure; the next session is unaffected.
    var fixture = new SessionFencingFixture();
    ArrayDeque<ReplicationIndexManager> rims =
        new ArrayDeque<>(
            List.of(
                stubShutdown(mock(ReplicationIndexManager.class)),
                mock(ReplicationIndexManager.class)));
    var rrim = fixture.rrim(indexer -> rims.poll());

    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // session 1 + RIM 1
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER); // RIM 1 shutdown completes -> session 1 ends
    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // session 2 + RIM 2

    SessionFencedDocumentIndexer session1 = fixture.created.get(0);
    SessionFencedDocumentIndexer session2 = fixture.created.get(1);
    session1.stageCommitUserData(); // stale: lands on the dead session -> dropped
    session2.stageCommitUserData(); // the live session's own stage still works

    verify(fixture.stagingWriter, times(1)).stageUserData(any()); // only session 2's
    assertThat(fixture.staleWrites()).isEqualTo(1.0); // only session 1's dropped stage
  }

  @Test
  public void tenureEnds_whenShutdownFutureCompletesExceptionally() throws Exception {
    var fixture = new SessionFencingFixture();
    CompletableFuture<Void> rim1Shutdown = new CompletableFuture<>();
    var rim1 = mock(ReplicationIndexManager.class);
    when(rim1.shutdown()).thenReturn(rim1Shutdown);
    ArrayDeque<ReplicationIndexManager> rims = new ArrayDeque<>(List.of(rim1));
    var rrim = fixture.rrim(indexer -> rims.poll());
    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // session 1 + rim 1
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER); // demote: rim 1 shutdown is pending
    SessionFencedDocumentIndexer session1 = fixture.created.get(0);

    rim1Shutdown.completeExceptionally(new RuntimeException("drain failed"));

    // The tenure ends on exceptional completion too, so stale work is dropped.
    session1.stageCommitUserData();
    verify(fixture.stagingWriter, never()).stageUserData(any());
    assertThat(fixture.staleWrites()).isEqualTo(1.0);
  }

  @Test
  public void tenureEnds_whenRrimShutdownThrowsSynchronously() throws Exception {
    var fixture = new SessionFencingFixture();
    var rim1 = mock(ReplicationIndexManager.class);
    when(rim1.shutdown()).thenThrow(new RuntimeException("boom"));
    ArrayDeque<ReplicationIndexManager> rims = new ArrayDeque<>(List.of(rim1));
    var rrim = fixture.rrim(indexer -> rims.poll());
    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // session 1 + rim 1
    SessionFencedDocumentIndexer session1 = fixture.created.get(0);

    assertThrows(RuntimeException.class, rrim::shutdown);

    // The tenure was ended before the synchronous throw escaped.
    session1.stageCommitUserData();
    verify(fixture.stagingWriter, never()).stageUserData(any());
    assertThat(fixture.staleWrites()).isEqualTo(1.0);
  }

  @Test
  public void tenureEnds_whenSessionRecycledForStaleEpoch() throws Exception {
    var fixture = new SessionFencingFixture();
    ArrayDeque<ReplicationIndexManager> rims =
        new ArrayDeque<>(
            List.of(
                stubShutdown(mock(ReplicationIndexManager.class)),
                mock(ReplicationIndexManager.class)));
    var rrim = fixture.rrim(indexer -> rims.poll());
    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // session 1 + rim 1
    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // duplicate LEADER recycles session 1

    SessionFencedDocumentIndexer session1 = fixture.created.get(0);
    session1.stageCommitUserData(); // stale: session 1 was recycled -> dropped

    verify(fixture.stagingWriter, never()).stageUserData(any());
    assertThat(fixture.staleWrites()).isEqualTo(1.0);
  }

  @Test
  public void tenureEnds_whenRrimShutdownRuns() throws Exception {
    var fixture = new SessionFencingFixture();
    var rrim = fixture.rrim(indexer -> stubShutdown(mock(ReplicationIndexManager.class)));
    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // session 1
    SessionFencedDocumentIndexer session1 = fixture.created.get(0);

    rrim.shutdown();

    session1.stageCommitUserData();
    verify(fixture.stagingWriter, never()).stageUserData(any());
    assertThat(fixture.staleWrites()).isEqualTo(1.0);
  }

  @Test
  public void tenureEnds_whenRrimDropRuns() throws Exception {
    var fixture = new SessionFencingFixture();
    var rrim =
        fixture.rrim(
            indexer -> {
              var rim = mock(ReplicationIndexManager.class);
              when(rim.drop()).thenReturn(CompletableFuture.completedFuture(null));
              return rim;
            });
    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // session 1
    SessionFencedDocumentIndexer session1 = fixture.created.get(0);

    rrim.drop();

    session1.stageCommitUserData();
    verify(fixture.stagingWriter, never()).stageUserData(any());
    assertThat(fixture.staleWrites()).isEqualTo(1.0);
  }

  @Test
  public void recycle_endsOldTenureBeforeNextPrepare() throws Exception {
    // Finding 1: session N's tenure must end BEFORE prepare for N+1 runs. Observe the recycled
    // session from INSIDE the re-prepare: the writer probes session 1 and records whether a stage
    // through it was dropped at that moment. On the fixed code the tenure has already ended, so the
    // probe sees a drop; a version that ran prepare first (tenure ending a moment later) would see
    // the stage reach the writer, failing this test.
    var fixture = new SessionFencingFixture();
    CompletableFuture<Void> rim1Shutdown = new CompletableFuture<>();
    var rim1 = mock(ReplicationIndexManager.class);
    when(rim1.shutdown()).thenReturn(rim1Shutdown);
    ArrayDeque<ReplicationIndexManager> rims =
        new ArrayDeque<>(List.of(rim1, mock(ReplicationIndexManager.class)));
    AtomicBoolean recycledTenureEndedAtRePrepare = new AtomicBoolean();
    PreparableWriter prepareWriter =
        () -> {
          // The indexer is added to created after prepareWriter() returns, so a non-empty list
          // means
          // this is the re-prepare for the next session. Probe the recycled (first) session now.
          if (!fixture.created.isEmpty()) {
            SessionFencedDocumentIndexer recycled = fixture.created.get(0);
            double droppedBefore = fixture.staleWrites();
            try {
              recycled.stageCommitUserData();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
            recycledTenureEndedAtRePrepare.set(fixture.staleWrites() > droppedBefore);
          }
          return new PrepareResult.Ready(EncodedUserData.EMPTY);
        };
    var rrim = fixture.rrim(indexer -> rims.poll(), prepareWriter);

    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // session 1 + rim 1
    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // recycle, but rim 1 shutdown is pending

    // The next session is not prepared while the old shutdown is pending.
    assertThat(fixture.created).hasSize(1);

    rim1Shutdown.complete(null);

    // The next session is prepared, and the recycled tenure had ended before that prepare ran.
    assertThat(fixture.created).hasSize(2);
    assertThat(recycledTenureEndedAtRePrepare.get()).isTrue();
  }

  @Test
  public void tenureEnds_whenDemotionInnerShutdownThrows() throws Exception {
    // Finding 2: reconcile()'s demote path runs the inner shutdown through safeShutdown, which
    // catches a synchronous throw and returns a completed future. The tenure must still end.
    var fixture = new SessionFencingFixture();
    var rim1 = mock(ReplicationIndexManager.class);
    when(rim1.shutdown()).thenThrow(new RuntimeException("drain boom"));
    ArrayDeque<ReplicationIndexManager> rims = new ArrayDeque<>(List.of(rim1));
    var rrim = fixture.rrim(indexer -> rims.poll());
    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // session 1 + rim 1
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER); // demote: inner shutdown throws, swallowed
    SessionFencedDocumentIndexer session1 = fixture.created.get(0);

    session1.stageCommitUserData(); // tenure ended despite the swallowed throw -> dropped

    verify(fixture.stagingWriter, never()).stageUserData(any());
    assertThat(fixture.staleWrites()).isEqualTo(1.0);
  }

  @Test
  public void tenureStaysActive_untilShutdownFutureCompletes() throws Exception {
    // Finding 3: the tenure must NOT end while the inner shutdown is still draining -- RIM issues
    // its shutdown-time status commits through this indexer during the drain, and they must reach
    // the writer. Only once the shutdown future completes does the tenure end.
    var fixture = new SessionFencingFixture();
    CompletableFuture<Void> rim1Shutdown = new CompletableFuture<>();
    var rim1 = mock(ReplicationIndexManager.class);
    when(rim1.shutdown()).thenReturn(rim1Shutdown);
    ArrayDeque<ReplicationIndexManager> rims = new ArrayDeque<>(List.of(rim1));
    var rrim = fixture.rrim(indexer -> rims.poll());
    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // session 1 + rim 1
    rrim.onLeaderStatusChange(LeaderStatus.FOLLOWER); // demote: rim 1 shutdown is pending
    SessionFencedDocumentIndexer session1 = fixture.created.get(0);

    // While the shutdown drains, the tenure is still active: a stage reaches the writer, no drop.
    session1.stageCommitUserData();
    verify(fixture.stagingWriter, times(1)).stageUserData(any());
    assertThat(fixture.staleWrites()).isEqualTo(0.0);

    rim1Shutdown.complete(null);

    // Once the shutdown completes the tenure ends, so further work is dropped.
    session1.stageCommitUserData();
    verify(fixture.stagingWriter, times(1)).stageUserData(any()); // no new stage reached the writer
    assertThat(fixture.staleWrites()).isEqualTo(1.0);
  }

  @Test
  public void factoryThrows_endsOrphanTenureAndCountsFailure() throws Exception {
    // Finding 4: when the RIM factory throws after beginTenure, the catch must end the orphan
    // tenure (so the unpublished indexer accepts no work) and count the promotion failure.
    var fixture = new SessionFencingFixture();
    var rrim =
        fixture.rrim(
            indexer -> {
              throw new RuntimeException("factory boom");
            });

    rrim.onLeaderStatusChange(LeaderStatus.LEADER); // prepare tenures session 1; factory throws

    SessionFencedDocumentIndexer session1 = fixture.created.get(0);
    session1.stageCommitUserData(); // orphan tenure was ended -> dropped

    verify(fixture.stagingWriter, never()).stageUserData(any());
    assertThat(fixture.staleWrites()).isEqualTo(1.0);
    assertThat(rrim.leadershipTransitionFailureCount()).isEqualTo(1.0);
    assertFalse(rrim.isLeader());
  }

  // ── helpers ─────────────────────────────────────────────────────────────────

  @SuppressWarnings("unchecked")
  private static Supplier<ReplicationIndexManager> mockWrappedFactory() {
    return mock(Supplier.class);
  }

  /**
   * Builds an {@link RecyclableReplicationIndexManager} whose promotions create real {@link
   * SessionFencedDocumentIndexer}s against a mock VectorLite index, so tests can assert the
   * per-session tenure wiring. Transitions and prepares run synchronously.
   */
  private static final class SessionFencingFixture {
    final InitializedVectorIndex index = mock(InitializedVectorIndex.class);
    final IndexWriter writer =
        mock(IndexWriter.class, withSettings().extraInterfaces(CommitDataStagingWriter.class));
    final CommitDataStagingWriter stagingWriter = (CommitDataStagingWriter) this.writer;
    final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    final List<SessionFencedDocumentIndexer> created = new ArrayList<>();
    private final IndexGeneration generation;

    SessionFencingFixture() {
      var definition =
          VectorIndexDefinitionBuilder.builder().withCustomEngineVectorField("vector", 3).build();
      this.generation = mockIndexGeneration(definition);
      when(this.index.getDefinition()).thenReturn(definition);
      when(this.index.getGenerationId()).thenReturn(this.generation.getGenerationId());
      when(this.index.getWriter()).thenReturn(this.writer);
      when(this.writer.getCommitUserData()).thenReturn(IndexCommitUserData.EMPTY.toEncodedData());
    }

    RecyclableReplicationIndexManager rrim(
        Function<DocumentIndexer, ReplicationIndexManager> factory) {
      return rrim(factory, () -> new PrepareResult.Ready(EncodedUserData.EMPTY));
    }

    RecyclableReplicationIndexManager rrim(
        Function<DocumentIndexer, ReplicationIndexManager> factory,
        PreparableWriter preparableWriter) {
      return rrim(factory, preparableWriter, () -> false, /* reconcileScheduler= */ null);
    }

    RecyclableReplicationIndexManager rrim(
        Function<DocumentIndexer, ReplicationIndexManager> factory,
        PreparableWriter preparableWriter,
        BooleanSupplier initialSyncEntryPresent,
        ScheduledExecutorService reconcileScheduler) {
      Function<EncodedUserData, SessionFencedDocumentIndexer> sessionIndexerFactory =
          token -> {
            var indexer =
                new SessionFencedDocumentIndexer(
                    this.index,
                    IndexCommitUserData.fromEncodedData(
                        token, Optional.of(this.generation.getGenerationId())),
                    this.meterRegistry);
            this.created.add(indexer);
            return indexer;
          };
      return RecyclableReplicationIndexManager.createForTesting(
          factory,
          sessionIndexerFactory,
          Runnable::run,
          preparableWriter,
          Runnable::run,
          this.meterRegistry,
          initialSyncEntryPresent,
          reconcileScheduler);
    }

    double staleWrites() {
      return this.meterRegistry
          .find(SessionFencedDocumentIndexer.STALE_SESSION_WRITE_COUNTER_NAME)
          .counters()
          .stream()
          .mapToDouble(Counter::count)
          .sum();
    }
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

  /** Submits a no-op and waits for it, so all prior tasks on {@code executor} have run. */
  private static void fence(ExecutorService executor) throws Exception {
    executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
  }

  /**
   * A {@link PreparableWriter} that signals {@code entered} on entry, blocks until {@code release}
   * is counted down, then returns {@link PrepareResult.Ready}. Lets a test land a leadership signal
   * (or shutdown/drop) while prepare is in flight.
   */
  private static PreparableWriter gatedReadyWriter(CountDownLatch entered, CountDownLatch release) {
    return () -> {
      entered.countDown();
      try {
        if (!release.await(5, TimeUnit.SECONDS)) {
          throw new IOException("gated prepare was not released within timeout");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted while gated in prepare", e);
      }
      return new PrepareResult.Ready(EncodedUserData.EMPTY);
    };
  }

  private RecyclableReplicationIndexManager createManager(
      Supplier<ReplicationIndexManager> wrappedFactory) {
    return RecyclableReplicationIndexManager.createForTesting(wrappedFactory);
  }

  private RecyclableReplicationIndexManager createManagerWithWriter(
      Supplier<ReplicationIndexManager> wrappedFactory, PreparableWriter preparableWriter) {
    return RecyclableReplicationIndexManager.createForTesting(
        wrappedFactory, Runnable::run, preparableWriter, Runnable::run);
  }
}
