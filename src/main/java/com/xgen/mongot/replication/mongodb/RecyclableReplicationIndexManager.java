package com.xgen.mongot.replication.mongodb;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link IndexManager} for VectorLite indexes that defers replication until this node is elected
 * leader. Created in follower mode with no active inner manager. On {@link LeaderStatus#LEADER} a
 * fresh inner {@link ReplicationIndexManager} is created and starts replication; on {@link
 * LeaderStatus#FOLLOWER} the inner manager is shut down.
 *
 * <p>Leadership transitions are applied asynchronously so the native FFM upcall thread is never
 * blocked. Each signal only records the desired state and appends an idempotent {@code reconcile}
 * step to a single serialized {@link #transitionChain}; the reconcile step runs on {@link
 * #transitionExecutor} and drives the actual state toward {@link #desiredStatus}. A {@code
 * FOLLOWER} step returns the inner manager's shutdown future so a subsequent {@code LEADER} step
 * only starts once the prior shutdown has fully completed -- awaiting completion without blocking
 * any thread.
 *
 * <p>Because SLS leadership is edge-triggered, a failed transition would otherwise leave the node
 * permanently diverged from its assigned role. Failures are therefore never fatal: they increment
 * {@link #LEADERSHIP_TRANSITION_FAILURE_COUNTER_NAME} and are retried by a low-frequency periodic
 * reconcile tick (mirroring the auto-embedding {@code MaterializedViewManager} self-heal).
 *
 * <p>TODO(CLOUDP-439803): Add more detailed documentation
 */
public final class RecyclableReplicationIndexManager implements IndexManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(RecyclableReplicationIndexManager.class);

  /**
   * Counts leadership transitions that failed to apply. Tagged by {@code direction} (PROMOTE when a
   * LEADER transition failed, DEMOTE when a FOLLOWER transition failed).
   */
  @VisibleForTesting
  static final String LEADERSHIP_TRANSITION_FAILURE_COUNTER_NAME =
      "recyclableLeadershipTransitionFailures";

  /**
   * How often the safety-net reconcile tick re-drives actual state toward {@link #desiredStatus}.
   */
  private static final Duration DEFAULT_RECONCILE_INTERVAL = Duration.ofSeconds(30);

  /** Leadership states signaled by the native vector engine via FFM upcall. */
  public enum LeaderStatus {
    LEADER,
    FOLLOWER
  }

  private final Supplier<ReplicationIndexManager> wrappedFactory;

  /** Runs reconcile steps. Never blocked on a shutdown future (see {@link #reconcile}). */
  private final Executor transitionExecutor;

  private final MeterRegistry meterRegistry;

  @GuardedBy("this")
  private Optional<ReplicationIndexManager> wrapped;

  /**
   * The most recently signaled desired role; the single source of truth reconcile drives toward.
   */
  @GuardedBy("this")
  private LeaderStatus desiredStatus;

  /** Tail of the serialized transition pipeline; every reconcile is appended here. */
  @GuardedBy("this")
  private CompletableFuture<Void> transitionChain;

  @GuardedBy("this")
  private Optional<CompletableFuture<Void>> shutdownFuture;

  /** Handle to the periodic reconcile tick, cancelled on shutdown/drop. */
  @GuardedBy("this")
  private Optional<ScheduledFuture<?>> reconcileTickFuture;

  @GuardedBy("this")
  private State state;

  private RecyclableReplicationIndexManager(
      Supplier<ReplicationIndexManager> wrappedFactory,
      Executor transitionExecutor,
      MeterRegistry meterRegistry) {
    this.wrappedFactory = wrappedFactory;
    this.transitionExecutor = transitionExecutor;
    this.meterRegistry = meterRegistry;
    this.wrapped = Optional.empty();
    this.desiredStatus = LeaderStatus.FOLLOWER;
    this.transitionChain = CompletableFuture.completedFuture(null);
    this.shutdownFuture = Optional.empty();
    this.reconcileTickFuture = Optional.empty();
    this.state = State.INITIALIZING;
  }

  /** Returns whether this manager is currently acting as leader. */
  public synchronized boolean isLeader() {
    return this.wrapped.isPresent();
  }

  @Override
  public synchronized CompletableFuture<Void> getInitFuture() {
    return this.wrapped
        .map(ReplicationIndexManager::getInitFuture)
        .orElse(CompletableFuture.completedFuture(null));
  }

  @Override
  public synchronized IndexManager.State getState() {
    return this.wrapped.map(ReplicationIndexManager::getState).orElse(this.state);
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    ReplicationIndexManager toShutdown;
    synchronized (this) {
      this.desiredStatus = LeaderStatus.FOLLOWER;
      cancelReconcileTick();
      // Mark SHUT_DOWN even while leader so a late upcall or in-flight reconcile cannot re-promote.
      this.state = State.SHUT_DOWN;
      if (this.wrapped.isEmpty()) {
        return this.shutdownFuture.orElse(CompletableFuture.completedFuture(null));
      }
      toShutdown = this.wrapped.get();
      this.wrapped = Optional.empty();
    }
    // Off monitor: shutdown() can block while the change-stream read loop drains, and holding this
    // monitor across it would stall a concurrent onLeaderStatusChange() on the FFM upcall thread.
    return recordShutdownFuture(toShutdown.shutdown());
  }

  @Override
  public CompletableFuture<Void> drop() {
    ReplicationIndexManager toDrop;
    synchronized (this) {
      this.desiredStatus = LeaderStatus.FOLLOWER;
      cancelReconcileTick();
      this.state = State.SHUT_DOWN;
      if (this.wrapped.isEmpty()) {
        return this.shutdownFuture.orElse(CompletableFuture.completedFuture(null));
      }
      toDrop = this.wrapped.get();
      this.wrapped = Optional.empty();
    }
    // Off monitor: drop() -> shutdown() can block while the change-stream read loop drains.
    return recordShutdownFuture(toDrop.drop());
  }

  /**
   * Called when this node's leadership status changes. Thread-safe; may be invoked from an FFM
   * upcall thread. Non-blocking: it only records {@code newStatus} as the desired state and
   * enqueues an asynchronous reconcile, so the upcall thread is never blocked (no {@code join()}).
   *
   * <p>Idempotent and coalescing: reconcile always drives the actual state toward the latest {@link
   * #desiredStatus}, so redundant signals are no-ops and bursts collapse to the final state.
   */
  public synchronized void onLeaderStatusChange(LeaderStatus newStatus) {
    if (this.state == State.SHUT_DOWN) {
      return;
    }
    this.desiredStatus = newStatus;
    enqueueReconcile();
  }

  /**
   * Appends one reconcile step to the single serialized chain. The reconcile body runs on {@link
   * #transitionExecutor}, never on the caller's thread. No try/catch is needed: {@code
   * thenComposeAsync} delivers an executor rejection to the returned future (the JDK wraps {@code
   * executor.execute} and completes exceptionally), so it lands in {@code exceptionally} rather
   * than throwing synchronously here.
   */
  @GuardedBy("this")
  private void enqueueReconcile() {
    this.transitionChain =
        this.transitionChain
            .thenComposeAsync(ignored -> reconcile(), this.transitionExecutor)
            .exceptionally(
                throwable -> {
                  handleTransitionFailure(throwable);
                  return null;
                });
  }

  /**
   * Idempotent reconcile toward {@link #desiredStatus}. Creates the inner manager only when we
   * should be leader and are not; shuts it down (returning the shutdown future so the chain awaits
   * full completion before the next step) only when we should be follower and are leader; otherwise
   * a no-op. Never crashes: unexpected failures are recorded and retried by the periodic tick.
   */
  private CompletableFuture<Void> reconcile() {
    ReplicationIndexManager toShutdown;
    synchronized (this) {
      if (this.state == State.SHUT_DOWN) {
        return CompletableFuture.completedFuture(null);
      }
      boolean leaderNow = this.wrapped.isPresent();
      if (this.desiredStatus == LeaderStatus.LEADER && !leaderNow) {
        // Promote. create() only schedules async init (VectorLite indexes have no synonym
        // managers), so it does not block while this monitor is held.
        try {
          this.wrapped = Optional.of(this.wrappedFactory.get());
        } catch (Throwable t) {
          handleTransitionFailure(t);
        }
        return CompletableFuture.completedFuture(null);
      }
      if (this.desiredStatus != LeaderStatus.FOLLOWER || !leaderNow) {
        return CompletableFuture.completedFuture(null); // converged / coalesced no-op
      }
      // Demote. Reflect follower immediately, but shut the inner manager down OUTSIDE this monitor
      // (below): shutdown() can block synchronously while the read loop drains, and holding the
      // monitor across it would stall a concurrent onLeaderStatusChange() on the FFM upcall thread.
      // Running that drain on transitionExecutor (the shared lifecycleExecutor, possibly a single
      // thread) is acceptable: the inner ReplicationIndexManager already runs its whole lifecycle
      // (build/init and steady-state orchestration) on that pool, so tearing it down there is
      // consistent with where this index's blocking work already lives. The pool already carries
      // blocking build work for every index, the drain is bounded (~5-10s), and transitions are
      // infrequent.
      toShutdown = this.wrapped.get();
      this.wrapped = Optional.empty();
    }
    // The transition chain awaits this future, so a subsequent LEADER only starts once the prior
    // shutdown has fully completed.
    return recordShutdownFuture(safeShutdown(toShutdown));
  }

  /** Invokes {@code inner.shutdown()} off the monitor, recording a failure instead of throwing. */
  private CompletableFuture<Void> safeShutdown(ReplicationIndexManager inner) {
    try {
      return inner.shutdown();
    } catch (Throwable t) {
      handleTransitionFailure(t);
      return CompletableFuture.completedFuture(null);
    }
  }

  private CompletableFuture<Void> recordShutdownFuture(CompletableFuture<Void> future) {
    synchronized (this) {
      this.shutdownFuture = Optional.of(future);
    }
    return future;
  }

  /**
   * Records a failed transition without crashing. Every failure -- including the rare shutdown-time
   * {@link RejectedExecutionException} -- increments the counter; the metric is enough to observe
   * it. Unwraps {@link CompletionException} so the logged cause is the real one.
   */
  private synchronized void handleTransitionFailure(Throwable t) {
    Throwable cause = (t instanceof CompletionException && t.getCause() != null) ? t.getCause() : t;
    String direction = this.desiredStatus == LeaderStatus.LEADER ? "PROMOTE" : "DEMOTE";
    this.meterRegistry
        .counter(LEADERSHIP_TRANSITION_FAILURE_COUNTER_NAME, "direction", direction)
        .increment();
    LOG.atWarn()
        .setCause(cause)
        .log("leadership transition failed; will retry on periodic reconcile");
  }

  /**
   * Starts the safety-net reconcile tick on {@code reconcileExecutor}. Timer only: each tick merely
   * enqueues a reconcile (when the actual and desired states diverge); the reconcile body still
   * runs on {@link #transitionExecutor}.
   */
  @GuardedBy("this")
  private void startPeriodicReconcile(
      ScheduledExecutorService reconcileExecutor, Duration interval) {
    // ofNullable so a mocked ScheduledExecutorService (which returns null) is tolerated in tests; a
    // real executor always returns a ScheduledFuture.
    this.reconcileTickFuture =
        Optional.ofNullable(
            reconcileExecutor.scheduleWithFixedDelay(
                this::runReconcileTick,
                interval.toMillis(),
                interval.toMillis(),
                TimeUnit.MILLISECONDS));
  }

  /**
   * Body of the periodic tick. Runs on the scheduled executor and must never throw, or {@code
   * scheduleWithFixedDelay} would cancel the repeating task.
   */
  private synchronized void runReconcileTick() {
    try {
      if (needsReconcile()) {
        enqueueReconcile();
      }
    } catch (Throwable t) {
      LOG.atWarn().setCause(t).log("leadership reconcile tick failed");
    }
  }

  @GuardedBy("this")
  private boolean needsReconcile() {
    boolean leaderNow = this.wrapped.isPresent();
    return this.state != State.SHUT_DOWN
        && ((this.desiredStatus == LeaderStatus.LEADER && !leaderNow)
            || (this.desiredStatus == LeaderStatus.FOLLOWER && leaderNow));
  }

  @GuardedBy("this")
  private void cancelReconcileTick() {
    this.reconcileTickFuture.ifPresent(future -> future.cancel(false));
    this.reconcileTickFuture = Optional.empty();
  }

  /**
   * Creates a {@link RecyclableReplicationIndexManager} in follower mode. Replication will not
   * start until {@link #onLeaderStatusChange(LeaderStatus)} is called with {@link
   * LeaderStatus#LEADER}. Leadership transitions run on {@code lifecycleExecutor}; the periodic
   * safety-net reconcile is scheduled on {@code commitExecutor}.
   */
  public static RecyclableReplicationIndexManager create(
      Executor lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      Optional<SynonymManager> synonymManager,
      IndexGeneration indexGeneration,
      InitializedIndex initializedIndex,
      DocumentIndexer documentIndexer,
      ScheduledExecutorService commitExecutor,
      Duration commitInterval,
      Duration requestRateLimitBackoff,
      MeterRegistry meterRegistry,
      FeatureFlags featureFlags,
      boolean enableNaturalOrderScan) {
    Supplier<ReplicationIndexManager> wrappedFactory =
        () ->
            ReplicationIndexManager.create(
                lifecycleExecutor,
                cursorManager,
                initialSyncQueue,
                steadyStateManager,
                synonymManager,
                indexGeneration,
                initializedIndex,
                documentIndexer,
                commitExecutor,
                commitInterval,
                requestRateLimitBackoff,
                meterRegistry,
                featureFlags,
                enableNaturalOrderScan);
    RecyclableReplicationIndexManager manager =
        new RecyclableReplicationIndexManager(wrappedFactory, lifecycleExecutor, meterRegistry);
    synchronized (manager) {
      manager.startPeriodicReconcile(commitExecutor, DEFAULT_RECONCILE_INTERVAL);
    }
    return manager;
  }

  /**
   * Creates a manager whose transitions run synchronously on the calling thread and without the
   * periodic reconcile tick, so tests are deterministic. Use {@link #triggerReconcileTick()} to
   * exercise the periodic path explicitly.
   */
  @VisibleForTesting
  public static RecyclableReplicationIndexManager createForTesting(
      Supplier<ReplicationIndexManager> wrappedFactory) {
    return new RecyclableReplicationIndexManager(
        wrappedFactory, Runnable::run, new SimpleMeterRegistry());
  }

  /**
   * Like {@link #createForTesting(Supplier)} but with an injected transition executor, so tests can
   * drive the asynchronous coalescing/await-shutdown behavior. Does not start the periodic tick.
   */
  @VisibleForTesting
  public static RecyclableReplicationIndexManager createForTesting(
      Supplier<ReplicationIndexManager> wrappedFactory, Executor transitionExecutor) {
    return new RecyclableReplicationIndexManager(
        wrappedFactory, transitionExecutor, new SimpleMeterRegistry());
  }

  /** Runs one reconcile tick synchronously, as the periodic scheduler would. */
  @VisibleForTesting
  synchronized void triggerReconcileTick() {
    runReconcileTick();
  }

  /** Total count recorded on {@link #LEADERSHIP_TRANSITION_FAILURE_COUNTER_NAME}. */
  @VisibleForTesting
  double leadershipTransitionFailureCount() {
    return this.meterRegistry.find(LEADERSHIP_TRANSITION_FAILURE_COUNTER_NAME).counters().stream()
        .mapToDouble(Counter::count)
        .sum();
  }
}
