package com.xgen.mongot.replication.mongodb;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.PreparableWriter;
import com.xgen.mongot.index.PrepareResult;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexStateInfo;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import com.xgen.mongot.replication.mongodb.common.StaleStateInfo;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymManager;
import com.xgen.mongot.util.Crash;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import org.bson.BsonTimestamp;
import org.jetbrains.annotations.Nullable;
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
 * any thread. Every stop (terminal request, demotion, session recycle) settles {@code lastStop}
 * only when the drain completes, so every {@code shutdown()}/{@code drop()} caller gets a future
 * that reflects the drain actually in flight; the physical index drop stays above this manager
 * (IndexActions.dropIndex).
 *
 * <p>Because SLS leadership is edge-triggered, a failed transition would otherwise leave the node
 * permanently diverged from its assigned role. Failures are therefore never fatal: they increment
 * {@link #LEADERSHIP_TRANSITION_FAILURE_COUNTER_NAME} and are retried by a low-frequency periodic
 * reconcile tick (mirroring the auto-embedding {@code MaterializedViewManager} self-heal).
 *
 * <p>Lock rule: never block while holding this manager's monitor. No inner-manager calls, no native
 * calls, and no indexer calls except {@code beginTenure} on an unpublished indexer. The inner
 * manager's monitor is held across native commits, index clears, force merges, and the
 * change-stream drain, and metrics scrapes call {@link #getState()} on every manager, so a call
 * that can block on the inner manager would stall a concurrent {@link #onLeaderStatusChange} on the
 * FFM upcall thread (and the Rust leadership task behind it). Status accessors therefore snapshot
 * this manager's fields under its monitor, release it, and only then read the inner manager; a
 * status read is eventually consistent, so a snapshot one recycle stale is fine.
 *
 * <p>While a follower, the index status is recovered once at creation (this one-time init gates
 * readiness via {@link #getInitFuture()}) and then kept fresh by the same periodic tick: each pass
 * re-reads the committed user data and republishes the index status when it changed. This manager
 * only reads -- it never advances the read pin. The pin is kept current by the log-tailing in the
 * storage layer (the follower tracks the latest materialized checkpoint), so a promotion also
 * resumes from that latest token without any refresh here. Poll failures only increment {@link
 * #FOLLOWER_REFRESH_FAILURE_COUNTER_NAME} and are retried on the next tick; they never affect the
 * init future or node readiness.
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

  /** Counts follower status-recovery (init) failures. */
  @VisibleForTesting
  static final String FOLLOWER_INIT_FAILURE_COUNTER_NAME = "recyclableFollowerInitFailures";

  /** Counts periodic follower status-refresh (poll) failures. */
  @VisibleForTesting
  static final String FOLLOWER_REFRESH_FAILURE_COUNTER_NAME = "recyclableFollowerRefreshFailures";

  /**
   * Counts prepares that returned {@link PrepareResult.Superseded} (leadership moved away during
   * prepare). Benign: the node waits for the next leadership signal.
   */
  @VisibleForTesting
  static final String PREPARE_SUPERSEDED_COUNTER_NAME = "recyclablePrepareSuperseded";

  /**
   * Counts promotions deferred because the previous session's initial-sync queue entry (queued or
   * in progress) has not been dequeued yet. Each deferral schedules a re-check; the count tracks
   * how long the wait lasted.
   */
  @VisibleForTesting
  static final String INITIAL_SYNC_ENTRY_WAIT_COUNTER_NAME = "recyclableInitialSyncEntryWait";

  /** How long to wait between re-checks of a still-queued initial-sync entry. */
  private static final Duration INITIAL_SYNC_ENTRY_RECHECK_DELAY = Duration.ofSeconds(1);

  /**
   * Default fake writer for the wrapped-factory-only test factories, which have no real index:
   * always Ready with no token, so promotion proceeds straight to creating the wrapped manager.
   */
  private static final PreparableWriter TEST_ALWAYS_READY_WRITER =
      () -> new PrepareResult.Ready(EncodedUserData.EMPTY);

  /**
   * Placeholder document indexer handed to the wrapped-factory-only test factories, whose inner
   * manager ignores its document indexer. Never reachable in production, where {@link
   * #sessionIndexerFactory} is always present and a real {@link SessionFencedDocumentIndexer} is
   * created per session; it throws on every call to keep that invariant loud.
   */
  private static final DocumentIndexer UNUSABLE_DOCUMENT_INDEXER = new UnusableDocumentIndexer();

  /**
   * How often the safety-net reconcile tick re-drives actual state toward {@link #desiredStatus}.
   */
  private static final Duration DEFAULT_RECONCILE_INTERVAL = Duration.ofSeconds(30);

  /** Leadership states signaled by the native vector engine via FFM upcall. */
  public enum LeaderStatus {
    LEADER,
    FOLLOWER
  }

  /**
   * What follower status recovery/refresh needs. Present only when this manager was created against
   * an index (production and the index-taking test factories); the wrapped-factory-only test
   * factories leave it empty, making the periodic refresh a no-op there.
   */
  private record FollowerContext(
      InitializedIndex index, FeatureFlags featureFlags, IndexGeneration indexGeneration) {}

  private final Function<DocumentIndexer, ReplicationIndexManager> wrappedFactory;

  /**
   * Builds a fresh per-session document indexer seeded from the prepared token, one per leadership
   * session. Present for production and the index-taking test factories; absent for the
   * wrapped-factory-only test factories, which have no index to build one from and whose inner
   * manager ignores the indexer. Mirrors {@link #followerContext}'s present-in-production
   * convention.
   */
  private final Optional<Function<EncodedUserData, SessionFencedDocumentIndexer>>
      sessionIndexerFactory;

  /** Runs reconcile steps. Never blocked on a shutdown future (see {@link #reconcile}). */
  private final Executor transitionExecutor;

  /**
   * The index's native writer, prepared before each leadership session. Present only for production
   * and index-taking test factories; the wrapped-factory-only test factories inject a fake.
   */
  private final PreparableWriter preparableWriter;

  /**
   * Dedicated, bounded executor for {@link #prepareAndPromote}: native prepare is I/O-bound and
   * must not occupy the shared lifecycle threads. See {@link VectorlitePrepareExecutor}.
   */
  private final Executor prepareExecutor;

  /**
   * Whether the initial-sync queue still holds this index generation's entry (queued or in
   * progress) from the previous session. Promotion is deferred while it does: enqueuing the next
   * session's initial sync behind a cancelled-but-still-queued entry trips the queue's
   * already-cancelled check and fails the index.
   */
  private final BooleanSupplier initialSyncEntryPresent;

  /**
   * Scheduler for the one-second initial-sync-entry re-check; the same executor the periodic
   * reconcile tick runs on. Absent in the wrapped-factory-only test seams, which re-drive the wait
   * through {@link #triggerReconcileTick()} instead.
   */
  private final Optional<ScheduledExecutorService> reconcileScheduler;

  private final MeterRegistry meterRegistry;

  /** Volatile so {@link #getStateWithoutLock()} can read it without the monitor. */
  @GuardedBy("this")
  private volatile Optional<ReplicationIndexManager> wrapped;

  /**
   * The current leadership session's document indexer, present while {@link #wrapped} is
   * (production and index-taking test factories only). Its tenure begins before the session's
   * manager and ends when that manager's shutdown future completes, so stale work from a prior
   * session is dropped rather than reaching this session's write path.
   */
  @GuardedBy("this")
  private Optional<SessionFencedDocumentIndexer> currentSessionIndexer;

  /**
   * Monotonic leadership epoch, bumped on EVERY upcall (LEADER or FOLLOWER). Each running session
   * records the epoch it was created under in {@link #sessionEpoch}; a session older than this is
   * stopped and re-prepared even when the desired role is still LEADER, so a leadership flap faster
   * than a reconcile no longer collapses into a stale session.
   */
  @GuardedBy("this")
  private long leadershipEpoch;

  /** The {@link #leadershipEpoch} the current {@link #wrapped} session was created under. */
  @GuardedBy("this")
  private long sessionEpoch;

  /** True while a {@link #prepareAndPromote} is in flight, so at most one runs per index. */
  @GuardedBy("this")
  private boolean preparing;

  /**
   * The most recently signaled desired role; the single source of truth reconcile drives toward.
   */
  @GuardedBy("this")
  private LeaderStatus desiredStatus;

  /** Tail of the serialized transition pipeline; every reconcile is appended here. */
  @GuardedBy("this")
  private CompletableFuture<Void> transitionChain;

  /**
   * The current session's stop future: created pending whenever a stop takes the inner manager (a
   * terminal request, a demotion, a session recycle) and completed only when that stop settles, so
   * a {@code shutdown()}/{@code drop()} that finds no session returns a future that reflects the
   * drain actually in flight rather than one that lies about it. Completed (never pending) when no
   * session was ever stopped.
   */
  @GuardedBy("this")
  private CompletableFuture<Void> lastStop;

  /**
   * Future for the one-time follower status recovery. Surfaced by {@link #getInitFuture()} while
   * this node is a follower (before any {@link LeaderStatus#LEADER} promotion). Never reassigned or
   * failed by the periodic refresh, so node readiness can never be blocked by refresh failures.
   */
  @GuardedBy("this")
  private CompletableFuture<Void> followerInitFuture;

  @GuardedBy("this")
  private Optional<FollowerContext> followerContext;

  /**
   * Raw committed user data last published as follower, for change detection. Cleared on demotion
   * so a re-follower republishes even if the committed data equals its last publish (the inner
   * manager owned the status in between).
   */
  @GuardedBy("this")
  private Optional<EncodedUserData> lastPublishedUserData;

  /** Non-overlap guard: at most one periodic follower refresh in flight at a time. */
  @GuardedBy("this")
  private boolean followerRefreshInFlight;

  /** Handle to the periodic reconcile tick, cancelled on shutdown/drop. */
  @GuardedBy("this")
  private Optional<ScheduledFuture<?>> reconcileTickFuture;

  /** Volatile so {@link #getStateWithoutLock()} can read it without the monitor. */
  @GuardedBy("this")
  private volatile State state;

  private RecyclableReplicationIndexManager(
      Function<DocumentIndexer, ReplicationIndexManager> wrappedFactory,
      Optional<Function<EncodedUserData, SessionFencedDocumentIndexer>> sessionIndexerFactory,
      Executor transitionExecutor,
      PreparableWriter preparableWriter,
      Executor prepareExecutor,
      MeterRegistry meterRegistry,
      BooleanSupplier initialSyncEntryPresent,
      @Nullable ScheduledExecutorService reconcileScheduler) {
    this.wrappedFactory = wrappedFactory;
    this.sessionIndexerFactory = sessionIndexerFactory;
    this.transitionExecutor = transitionExecutor;
    this.preparableWriter = preparableWriter;
    this.prepareExecutor = prepareExecutor;
    this.meterRegistry = meterRegistry;
    this.initialSyncEntryPresent = initialSyncEntryPresent;
    this.reconcileScheduler = Optional.ofNullable(reconcileScheduler);
    this.wrapped = Optional.empty();
    this.currentSessionIndexer = Optional.empty();
    this.leadershipEpoch = 0;
    this.sessionEpoch = 0;
    this.preparing = false;
    this.desiredStatus = LeaderStatus.FOLLOWER;
    this.transitionChain = CompletableFuture.completedFuture(null);
    // Completed: no session has ever been stopped.
    this.lastStop = CompletableFuture.completedFuture(null);
    this.followerInitFuture = CompletableFuture.completedFuture(null);
    this.followerContext = Optional.empty();
    this.lastPublishedUserData = Optional.empty();
    this.followerRefreshInFlight = false;
    this.reconcileTickFuture = Optional.empty();
    this.state = State.INITIALIZING;
  }

  /** Returns whether this manager is currently acting as leader. */
  public synchronized boolean isLeader() {
    return this.wrapped.isPresent();
  }

  /**
   * Snapshot-then-read, per the lock rule in the class comment: the inner {@code getInitFuture()}
   * takes the inner manager's monitor, which can be held across a native commit or a change-stream
   * drain, so it must never be called while holding this manager's monitor.
   */
  @Override
  public CompletableFuture<Void> getInitFuture() {
    // While a follower, readiness reflects the status-recovery init; once leader, the inner
    // manager's init (which starts replication) is authoritative.
    Optional<ReplicationIndexManager> wrapped;
    CompletableFuture<Void> followerInit;
    synchronized (this) {
      wrapped = this.wrapped;
      followerInit = this.followerInitFuture;
    }
    return wrapped.map(ReplicationIndexManager::getInitFuture).orElse(followerInit);
  }

  /** Snapshot-then-read, per the lock rule in the class comment. */
  @Override
  public IndexManager.State getState() {
    Optional<ReplicationIndexManager> wrapped;
    IndexManager.State state;
    synchronized (this) {
      wrapped = this.wrapped;
      state = this.state;
    }
    return wrapped.map(ReplicationIndexManager::getState).orElse(state);
  }

  @Override
  @SuppressWarnings("GuardedBy") // volatile reads; writes still happen under this monitor
  public IndexManager.State getStateWithoutLock() {
    // Read wrapped first: shutdown() and drop() set state to SHUT_DOWN before clearing wrapped, so
    // a reader that sees wrapped cleared by them also sees SHUT_DOWN.
    return this.wrapped.map(ReplicationIndexManager::getStateWithoutLock).orElse(this.state);
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    return terminate(false);
  }

  @Override
  public CompletableFuture<Void> drop() {
    return terminate(true);
  }

  /**
   * Runs the terminal request and returns the stable {@link #lastStop}. Whoever finds the manager
   * present owns its stop; a caller that races a reconcile-owned drain gets a future completed by
   * that drain, never a completed future that lies about it. A drop on a running session stops it
   * with {@code RIM.drop()} so the inner index drop runs; a drop after a settled stop just returns
   * the settled future -- the physical close and drop of the index is performed by the caller above
   * this manager (IndexActions.dropIndex) after this future settles.
   */
  private CompletableFuture<Void> terminate(boolean drop) {
    ReplicationIndexManager toStop;
    Optional<SessionFencedDocumentIndexer> endedSession;
    CompletableFuture<Void> lastStop;
    synchronized (this) {
      this.desiredStatus = LeaderStatus.FOLLOWER;
      cancelReconcileTick();
      // Mark SHUT_DOWN even while leader so a late upcall or in-flight reconcile cannot re-promote.
      this.state = State.SHUT_DOWN;
      if (this.wrapped.isEmpty()) {
        // Nothing to stop: no manager ever, already stopped, or a reconcile-owned drain in flight
        // (which settles {@code lastStop} when it completes). The physical close and drop of the
        // index is performed by the caller above this manager (IndexActions.dropIndex, after this
        // future settles).
        return this.lastStop;
      }
      // This call owns the stop, so it replaces {@code lastStop}: every terminal call that lands
      // while this drain runs (the manager is now absent) returns this same future, completed only
      // when the drain settles.
      toStop = this.wrapped.get();
      this.wrapped = Optional.empty();
      endedSession = this.currentSessionIndexer;
      this.currentSessionIndexer = Optional.empty();
      lastStop = new CompletableFuture<>();
      this.lastStop = lastStop;
    }
    // Off monitor: shutdown() can block while the change-stream read loop drains, and holding this
    // monitor across it would stall a concurrent onLeaderStatusChange() on the FFM upcall thread.
    CompletableFuture<Void> future;
    try {
      future = drop ? toStop.drop() : toStop.shutdown();
    } catch (Throwable t) {
      // End the tenure even on a synchronous throw, before it escapes, so every stop path is
      // uniform; and settle the future so a concurrent waiter is not left pending.
      endedSession.ifPresent(SessionFencedDocumentIndexer::endTenure);
      lastStop.completeExceptionally(t);
      throw t;
    }
    // The stop this call owned settles the terminal future.
    settleFrom(lastStop, endTenureWhenComplete(endedSession, future));
    return lastStop;
  }

  /** Completes {@code stop} with {@code source}'s outcome, exceptional or not. */
  private static void settleFrom(CompletableFuture<Void> stop, CompletableFuture<Void> source) {
    source.whenComplete(
        (ignored, throwable) -> {
          if (throwable != null) {
            stop.completeExceptionally(throwable);
          } else {
            stop.complete(null);
          }
        });
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
    // Treat every upcall as a session boundary: bump the epoch so a running
    // session created under an older epoch is recycled even when the desired role
    // is LEADER again (a leadership flap faster than a reconcile).
    this.leadershipEpoch++;
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
   * Idempotent reconcile toward {@link #desiredStatus}. On promotion it prepares the native writer
   * (off the chain, see {@link #prepareAndPromote}) before creating the inner manager; on demotion
   * it shuts the inner manager down (returning the shutdown future so the chain awaits full
   * completion before the next step); a running session from an older leadership epoch is recycled
   * (stopped, then re-prepared) even when the desired role is still LEADER. Never crashes:
   * unexpected failures are recorded and retried by the periodic tick.
   */
  private CompletableFuture<Void> reconcile() {
    // The initial-sync-queue read runs OFF this monitor (the lock rule):
    // hasEntry takes the queue's lock. The value is monotone here -- the
    // previous session's entry can only leave the queue, and a new one cannot
    // be enqueued before the next manager exists -- so a stale read at worst
    // defers one re-check.
    boolean initialSyncEntryPresent = this.initialSyncEntryPresent.getAsBoolean();
    ReplicationIndexManager toShutdown;
    Optional<SessionFencedDocumentIndexer> endedSession;
    @Var boolean rePrepareAfterShutdown = false;
    @Var CompletableFuture<Void> lastStop = null;
    synchronized (this) {
      if (this.state == State.SHUT_DOWN) {
        return CompletableFuture.completedFuture(null);
      }
      boolean leaderNow = this.wrapped.isPresent();
      if (this.desiredStatus == LeaderStatus.LEADER) {
        if (leaderNow) {
          if (this.sessionEpoch == this.leadershipEpoch) {
            return CompletableFuture.completedFuture(null); // converged
          }
          // A running session from an older epoch (a leadership flap): stop it,
          // then re-prepare the new term (below, off the monitor).
          toShutdown = this.wrapped.get();
          this.wrapped = Optional.empty();
          endedSession = this.currentSessionIndexer;
          this.currentSessionIndexer = Optional.empty();
          this.lastPublishedUserData = Optional.empty();
          lastStop = new CompletableFuture<>();
          this.lastStop = lastStop;
          rePrepareAfterShutdown = true;
        } else {
          // Finish follower status recovery before promoting, so the leader init does not race the
          // follower init. Non-blocking: re-drive reconcile once the init completes.
          // followerInitFuture always completes normally (see scheduleFollowerInit).
          if (!this.followerInitFuture.isDone()) {
            return this.followerInitFuture.thenComposeAsync(
                ignored -> reconcile(), this.transitionExecutor);
          }
          // The previous session's queued (or in-progress) initial sync must be dequeued before
          // the next manager enqueues its own: enqueuing behind a cancelled-but-still-queued
          // entry trips the queue's already-cancelled check and fails the index. Defer the
          // promotion and re-check shortly (the periodic tick re-drives too).
          if (initialSyncEntryPresent) {
            this.meterRegistry.counter(INITIAL_SYNC_ENTRY_WAIT_COUNTER_NAME).increment();
            scheduleInitialSyncEntryRecheck();
            return CompletableFuture.completedFuture(null);
          }
          if (this.preparing) {
            // A prepare is already in flight for this index; its completion re-drives.
            return CompletableFuture.completedFuture(null);
          }
          // Prepare the native write path BEFORE creating the RIM, so a promoted
          // follower rebuilds its write base from the latest durable checkpoint
          // and RIM.init resumes from the prepared token. Prepare
          // runs OFF this chain on the dedicated prepare executor, so a FOLLOWER/
          // terminal signal arriving mid-prepare is seen immediately by the next
          // chain step.
          this.preparing = true;
          long epoch = this.leadershipEpoch;
          try {
            this.prepareExecutor.execute(() -> prepareAndPromote(epoch));
          } catch (RuntimeException e) {
            // Executor rejected (e.g. shutting down): retry on the periodic tick.
            this.preparing = false;
            handleTransitionFailure(e);
          }
          return CompletableFuture.completedFuture(null);
        }
      } else {
        // desired == FOLLOWER
        if (!leaderNow) {
          return CompletableFuture.completedFuture(null); // converged
        }
        // Demote. Reflect follower immediately, but shut the inner manager down OUTSIDE this
        // monitor
        // (below): shutdown() can block synchronously while the read loop drains, and holding the
        // monitor across it would stall a concurrent onLeaderStatusChange() on the FFM upcall
        // thread. Running that drain on transitionExecutor is acceptable: the inner manager already
        // runs its whole lifecycle there, the drain is bounded (~5-10s), and transitions are rare.
        toShutdown = this.wrapped.get();
        this.wrapped = Optional.empty();
        endedSession = this.currentSessionIndexer;
        this.currentSessionIndexer = Optional.empty();
        lastStop = new CompletableFuture<>();
        this.lastStop = lastStop;
        // Force the first post-demotion poll to republish even if the committed data equals what
        // this node last published as a follower: the inner manager owned (and may have
        // overwritten)
        // the index status in between.
        this.lastPublishedUserData = Optional.empty();
      }
    }
    // The transition chain awaits this future, so a subsequent LEADER only starts once the prior
    // shutdown has fully completed. The tenure-end dependent future (not the raw shutdown) is
    // what the re-prepare below and the next chained reconcile wait on, so both run only after
    // the old tenure has ended. When the stop settles, the session's terminal future settles from
    // it: a terminal request that landed while this drain was running returns that future, and
    // only now learns the drain is done. The drained manager is retained for a late drop.
    CompletableFuture<Void> shutdown =
        endTenureWhenComplete(endedSession, safeShutdown(toShutdown));
    settleFrom(lastStop, shutdown);
    if (rePrepareAfterShutdown) {
      // Once the stale session has fully drained, re-drive to prepare the new term.
      return shutdown.thenComposeAsync(ignored -> reconcile(), this.transitionExecutor);
    }
    return shutdown;
  }

  /**
   * Schedules the one-second re-check that re-drives a promotion deferred on a still-queued
   * initial-sync entry ({@link #initialSyncEntryPresent}). A no-op without a scheduler (the
   * wrapped-factory-only test seams re-drive through {@link #triggerReconcileTick()}); a rejected
   * schedule (executor shutting down) falls back to the periodic tick the same way.
   */
  @GuardedBy("this")
  private void scheduleInitialSyncEntryRecheck() {
    try {
      this.reconcileScheduler.ifPresent(
          scheduler ->
              scheduler.schedule(
                  () -> {
                    synchronized (this) {
                      if (this.state != State.SHUT_DOWN) {
                        enqueueReconcile();
                      }
                    }
                  },
                  INITIAL_SYNC_ENTRY_RECHECK_DELAY.toMillis(),
                  TimeUnit.MILLISECONDS));
    } catch (RuntimeException e) {
      // Executor rejected (e.g. shutting down): the periodic tick retries.
    }
  }

  /**
   * Prepare the native write path for leadership {@code epoch}, then create the inner manager on a
   * Ready result. Runs on {@link #prepareExecutor} (never the reconcile chain or an FFM upcall
   * thread): native prepare is I/O-bound and the RRIM monitor is not held across it. On completion
   * it re-checks currency under the monitor, so a newer signal that arrived during prepare wins.
   */
  private void prepareAndPromote(long epoch) {
    PrepareResult result;
    try {
      result = this.preparableWriter.prepareWriter();
    } catch (Throwable t) {
      clearPreparingAndCountPromoteFailure(
          t, "native writer prepare failed; will retry on periodic reconcile");
      // needsReconcile() re-drives on the periodic tick (Idle && desired LEADER).
      return;
    }

    // Build the per-session document indexer OUTSIDE the monitor: its delegate
    // constructor does a native commit-user-data read, and holding the RRIM monitor across it would
    // block a concurrent FFM leadership upcall (and the Rust leadership task behind it). If the
    // session turns out not to be current below, this indexer is discarded unused -- its tenure is
    // never begun. Empty for a non-Ready result or the wrapped-factory-only test factories.
    Optional<SessionFencedDocumentIndexer> sessionIndexer;
    try {
      sessionIndexer =
          result instanceof PrepareResult.Ready ready
              ? this.sessionIndexerFactory.map(factory -> factory.apply(ready.userData()))
              : Optional.empty();
    } catch (Throwable t) {
      clearPreparingAndCountPromoteFailure(
          t, "building the per-session document indexer failed; will retry on periodic reconcile");
      return;
    }

    synchronized (this) {
      this.preparing = false;
      // A newer signal (or shutdown) may have arrived while prepare ran: let the latest state win
      // instead of installing a stale session. Any indexer built above is discarded here; its
      // tenure was never begun.
      if (this.state == State.SHUT_DOWN
          || this.desiredStatus != LeaderStatus.LEADER
          || epoch != this.leadershipEpoch
          || this.wrapped.isPresent()) {
        if (this.state != State.SHUT_DOWN) {
          enqueueReconcile();
        }
        return;
      }
      if (result instanceof PrepareResult.Ready) {
        // Promote. Begin the session's tenure BEFORE creating the RIM (RIM.init may write
        // immediately); the shared per-generation indexer is ignored for VectorLite
        // Stale work from a prior session is dropped once that
        // session's tenure ends, so it never reaches this session's write path. create() only
        // schedules async init, so it does not block under the monitor; the new indexer is
        // unpublished, so taking its monitor here cannot deadlock.
        try {
          sessionIndexer.ifPresent(indexer -> indexer.beginTenure(epoch));
          DocumentIndexer documentIndexer =
              sessionIndexer
                  .map(indexer -> (DocumentIndexer) indexer)
                  .orElse(UNUSABLE_DOCUMENT_INDEXER);
          this.wrapped = Optional.of(this.wrappedFactory.apply(documentIndexer));
          this.currentSessionIndexer = sessionIndexer;
          this.sessionEpoch = epoch;
          LOG.atInfo().log("native writer prepared; replication resumes from the prepared token");
        } catch (Throwable t) {
          // The inner manager failed before a session was installed: end the just-begun tenure so
          // the orphaned indexer accepts no work, and retry on the periodic tick.
          sessionIndexer.ifPresent(SessionFencedDocumentIndexer::endTenure);
          handleTransitionFailure(t);
        }
      } else if (result instanceof PrepareResult.Superseded) {
        // Leadership moved away during prepare; wait for the next signal.
        this.meterRegistry.counter(PREPARE_SUPERSEDED_COUNTER_NAME).increment();
      } else if (result instanceof PrepareResult.Failed failed) {
        // Not produced by any writer today; reported as a transition failure so an unexpected
        // terminal verdict cannot pass silently.
        handleTransitionFailure(
            new IllegalStateException("native writer terminal: " + failed.message()));
      }
    }
  }

  /**
   * Clears the in-flight {@link #preparing} flag and records a promotion failure (counter + log),
   * so a failed prepare or indexer build is retried on the periodic reconcile tick.
   */
  private void clearPreparingAndCountPromoteFailure(Throwable t, String message) {
    synchronized (this) {
      this.preparing = false;
    }
    this.meterRegistry
        .counter(LEADERSHIP_TRANSITION_FAILURE_COUNTER_NAME, "direction", "PROMOTE")
        .increment();
    LOG.atWarn().setCause(t).log(message);
  }

  /** Invokes {@code inner.shutdown()} off the monitor, recording a failure instead of throwing. */
  private CompletableFuture<Void> safeShutdown(ReplicationIndexManager inner) {
    try {
      CompletableFuture<Void> future = inner.shutdown();
      // A null (never a real ReplicationIndexManager) is tolerated so callers can compose on the
      // result unconditionally.
      return future != null ? future : CompletableFuture.completedFuture(null);
    } catch (Throwable t) {
      handleTransitionFailure(t);
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Ends {@code endedSession}'s tenure once its manager's {@code shutdownFuture} completes (success
   * or failure), off any monitor, and returns the dependent future that completes only after the
   * tenure has ended. Callers must record and chain on the returned future, not the input: the next
   * session's reconcile/prepare is composed on the recorded shutdown future, and this makes "the
   * old tenure ends before the next prepare" structural rather than a race between two independent
   * callbacks on the same future. {@code endTenure()} is a non-blocking flag flip that preserves
   * the original completion, so a session-less caller gets the future unchanged.
   */
  private static CompletableFuture<Void> endTenureWhenComplete(
      Optional<SessionFencedDocumentIndexer> endedSession, CompletableFuture<Void> shutdownFuture) {
    return endedSession
        .map(indexer -> shutdownFuture.whenComplete((ignored, throwable) -> indexer.endTenure()))
        .orElse(shutdownFuture);
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
   * scheduleWithFixedDelay} would cancel the repeating task. Dispatch-only: the leadership
   * reconcile body and the follower status refresh both run elsewhere ({@link
   * #transitionExecutor}), never on the scheduler thread. NOT synchronized: this tick runs on the
   * shared periodic scheduler, so it takes this manager's monitor only briefly (the snapshot in
   * {@link #needsReconcile()}, the enqueue in {@link #enqueueReconcile()}) rather than across both
   * calls -- holding it here would stall a concurrent {@link #onLeaderStatusChange} on the FFM
   * upcall thread.
   */
  private void runReconcileTick() {
    try {
      if (needsReconcile()) {
        synchronized (this) {
          enqueueReconcile();
        }
      }
      maybeScheduleFollowerRefresh();
    } catch (Throwable t) {
      LOG.atWarn().setCause(t).log("leadership reconcile tick failed");
    }
  }

  /**
   * Schedules one follower status refresh (poll) on {@link #transitionExecutor} unless this node is
   * leader or a promotion is already pending ({@link #desiredStatus} is LEADER), shut down, the
   * one-time init has not finished publishing yet, a previous poll is still in flight, or no index
   * context exists (wrapped-factory-only tests). Failures only increment {@link
   * #FOLLOWER_REFRESH_FAILURE_COUNTER_NAME} and log -- they never touch {@link #followerInitFuture}
   * (so node readiness is unaffected) and the next tick retries, which also retries a failed
   * one-time init publish.
   *
   * <p>Kept outside {@link #transitionChain} so a slow SLS read is never serialized into the
   * chain's ordering. It still shares {@link #transitionExecutor} with reconcile, though, so on a
   * single-thread pool an in-flight blocking read can delay a promotion queued behind it -- bounded
   * by one committed-state read, and avoided in the common case by skipping the poll once a
   * promotion is pending (the {@code desiredStatus == LEADER} guard below). Races with a concurrent
   * promotion are otherwise benign -- {@link #setFollowerStatus} re-checks under the monitor and
   * lets the leader win.
   *
   * <p>Synchronized itself (not called under a caller's monitor): reads several fields and flips
   * {@link #followerRefreshInFlight} atomically without holding any caller's lock.
   */
  private synchronized void maybeScheduleFollowerRefresh() {
    if (this.state == State.SHUT_DOWN
        || this.wrapped.isPresent()
        || this.desiredStatus == LeaderStatus.LEADER
        || !this.followerInitFuture.isDone()
        || this.followerRefreshInFlight
        || this.followerContext.isEmpty()) {
      return;
    }
    FollowerContext ctx = this.followerContext.get();
    this.followerRefreshInFlight = true;
    try {
      CompletableFuture.runAsync(
              () -> refreshAndPublishFollowerStatus(ctx), this.transitionExecutor)
          .whenComplete(
              (ignored, throwable) -> {
                synchronized (this) {
                  this.followerRefreshInFlight = false;
                }
                if (throwable != null) {
                  this.meterRegistry.counter(FOLLOWER_REFRESH_FAILURE_COUNTER_NAME).increment();
                  LOG.atWarn().setCause(throwable).log("periodic follower status refresh failed");
                }
              });
    } catch (RuntimeException e) {
      // runAsync delivers an executor rejection synchronously; never leave the guard stuck.
      this.followerRefreshInFlight = false;
      throw e;
    }
  }

  /**
   * Whether actual state diverges from {@link #desiredStatus} and a reconcile is needed. Snapshots
   * this manager's fields under its monitor and reads nothing else, so it never blocks (the lock
   * rule in the class comment). A dead inner manager is not detected here: it is exported by the
   * per-state manager gauge and the native fail-stop gauge, and recovery is master's halt + restart
   * + replay -- recycling a terminal inner manager would just re-terminate it.
   */
  private boolean needsReconcile() {
    Optional<ReplicationIndexManager> wrapped;
    State state;
    LeaderStatus desiredStatus;
    boolean preparing;
    long sessionEpoch;
    long leadershipEpoch;
    synchronized (this) {
      wrapped = this.wrapped;
      state = this.state;
      desiredStatus = this.desiredStatus;
      preparing = this.preparing;
      sessionEpoch = this.sessionEpoch;
      leadershipEpoch = this.leadershipEpoch;
    }
    if (state == State.SHUT_DOWN) {
      return false;
    }
    boolean leaderNow = wrapped.isPresent();
    if (desiredStatus == LeaderStatus.LEADER) {
      // Re-drive when not leader yet, or the running session is from an older
      // epoch; skip while a prepare is already in flight (it will re-drive).
      return !preparing && (!leaderNow || sessionEpoch != leadershipEpoch);
    }
    return leaderNow; // desired FOLLOWER but still leader
  }

  @GuardedBy("this")
  private void cancelReconcileTick() {
    this.reconcileTickFuture.ifPresent(future -> future.cancel(false));
    this.reconcileTickFuture = Optional.empty();
  }

  /**
   * Schedules the one-time follower status recovery on {@link #transitionExecutor} (never the FFM
   * upcall thread, because the read is a blocking native SLS call). Surfaced through {@link
   * #getInitFuture()} while this node remains a follower.
   *
   * <p>{@code handle} makes {@link #followerInitFuture} complete <em>normally</em> even when the
   * status read fails, so one index's recovery error never fails the node-wide {@code
   * isInitialized()} gate (mirroring {@link ReplicationIndexManager}, which fails the index rather
   * than propagating a failed init). The failure stays observable via {@link
   * #FOLLOWER_INIT_FAILURE_COUNTER_NAME} and a log. Promotion is ordered after this future by
   * {@link #reconcile()}.
   */
  @GuardedBy("this")
  private void scheduleFollowerInit(
      InitializedIndex index, FeatureFlags featureFlags, IndexGeneration indexGeneration) {
    FollowerContext ctx = new FollowerContext(index, featureFlags, indexGeneration);
    this.followerContext = Optional.of(ctx);
    this.followerInitFuture =
        CompletableFuture.runAsync(
                () -> refreshAndPublishFollowerStatus(ctx), this.transitionExecutor)
            .handle(
                (ignored, throwable) -> {
                  if (throwable != null) {
                    this.meterRegistry.counter(FOLLOWER_INIT_FAILURE_COUNTER_NAME).increment();
                    LOG.atWarn().setCause(throwable).log("follower status recovery failed");
                  }
                  return null;
                });
  }

  /**
   * One follower status refresh: reads the committed index user data (for VectorLite, from SLS --
   * kept fresh by the storage layer's log tailing) and republishes the index status from it when it
   * changed since this node last published. Blocking (native SLS read); runs on {@link
   * #transitionExecutor} only. Used by both the one-time init ({@link #scheduleFollowerInit}) and
   * the periodic tick ({@link #maybeScheduleFollowerRefresh}).
   *
   * <p>Reads only -- it never advances the read pin (the storage layer keeps it within the latest
   * materialized frontier; force-advancing past that could read unmaterialized pages).
   */
  private void refreshAndPublishFollowerStatus(FollowerContext ctx) {
    synchronized (this) {
      // Skip the blocking native read when the result could not be published anyway.
      if (this.wrapped.isPresent() || this.state == State.SHUT_DOWN) {
        return;
      }
    }
    EncodedUserData encodedUserData = ctx.index().getWriter().getCommitUserData();
    synchronized (this) {
      if (this.wrapped.isPresent() || this.state == State.SHUT_DOWN) {
        return;
      }
      if (this.lastPublishedUserData.map(encodedUserData::equals).orElse(false)) {
        return; // unchanged since last publish; avoid status/metrics churn
      }
    }
    publishFollowerStatus(ctx, encodedUserData);
    synchronized (this) {
      // Only record when still publishing as follower: if a promotion or shutdown raced the publish
      // (setFollowerStatus dropped it), a later refresh must retry.
      if (this.wrapped.isEmpty() && this.state != State.SHUT_DOWN) {
        this.lastPublishedUserData = Optional.of(encodedUserData);
      }
    }
  }

  /**
   * Publishes the index status corresponding to {@code encodedUserData} without starting
   * replication -- this node is a follower. The index is opened before this manager is created, so
   * its writer is readable here. Mirrors {@link MongoDbNoOpReplicationManager}'s {@code
   * resetStatus} and the status half of {@link ReplicationIndexManager#init}: indexes are set as
   * failed(exceededLimit), stale, doesNotExist(collectionNotFound), initialSync, steady, or
   * notStarted.
   *
   * <p>On promotion the inner {@link ReplicationIndexManager} re-derives status via {@code
   * index.getWriter().getCommitUserData()} and resumes replication from the latest durable token
   * (kept current by the storage layer's takeover gate), so the leader path stays consistent with
   * this one.
   *
   * <p>TODO(CLOUDP-280897): Make the status recovery logic in a single place; any change here must
   * be reflected in the two callers above.
   */
  private void publishFollowerStatus(FollowerContext ctx, EncodedUserData encodedUserData) {
    InitializedIndex index = ctx.index();
    FeatureFlags featureFlags = ctx.featureFlags();
    IndexCommitUserData userData =
        IndexCommitUserData.fromEncodedData(
            encodedUserData, Optional.of(ctx.indexGeneration().getGenerationId()));

    Optional<String> exceededReason = userData.exceededLimitsReason();
    if (exceededReason.isPresent()) {
      setFollowerStatus(
          index,
          IndexStatus.failed(
              ReplicationIndexManager.EXCEEDED_LIMIT_REASON_PREFIX + exceededReason.get()));
      return;
    }

    Optional<StaleStateInfo> staleStateInfo = userData.staleStateInfo();
    if (staleStateInfo.isPresent()) {
      // Update optime info metric before setting index to STALE to ensure metrics consistency.
      // Acquire unset lock to ensure another thread does not unset the metric while we update it.
      ReplicationOpTimeInfo opTimeInfo =
          index.getMetricsUpdater().getIndexingMetricsUpdater().getReplicationOpTimeInfo();
      BsonTimestamp lastOptime = staleStateInfo.get().getLastOptime();
      synchronized (opTimeInfo.getUnsetLock()) {
        // Hold this monitor across BOTH the optime update and the status write, re-checking
        // leadership/shutdown first. A promotion that raced the reads above must suppress the
        // optime update too, not just the status write -- otherwise this (older) follower poll
        // could regress the newly promoted leader's reported optime even while its status write is
        // dropped. Preserves the unsetLock -> this ordering setFollowerStatus already established.
        synchronized (this) {
          if (this.wrapped.isPresent() || this.state == State.SHUT_DOWN) {
            return;
          }
          opTimeInfo.update(lastOptime.getValue(), lastOptime.getValue());
          index.setStatus(
              IndexStatus.stale(
                  staleStateInfo.get().getMessage(), staleStateInfo.get().getLastOptime()));
        }
      }
      return;
    }

    if (featureFlags.isEnabled(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)) {
      Optional<IndexStateInfo> indexStateInfo = userData.indexStateInfo();
      if (indexStateInfo.isPresent() && indexStateInfo.get().isCollectionNotFound()) {
        setFollowerStatus(index, IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
        return;
      }
    }

    if (userData.initialSyncResumeInfo().isPresent()) {
      setFollowerStatus(index, IndexStatus.initialSync());
      return;
    }

    if (userData.resumeInfo().isPresent()) {
      ChangeStreamResumeInfo resumeInfo = userData.resumeInfo().get();
      // A resume token optime will have an inc that is one greater than the "actual" optime of the
      // last event in the change stream. (see: https://tinyurl.com/ydbjkv8k)
      // We subtract one from the inc value of the resume token to get the operation time of the
      // last indexed event, to match the optime that would be set by the change stream batch
      // application process.
      BsonTimestamp resumeTokenOpTime =
          Crash.because("failed to parse resume token")
              .ifThrows(() -> ResumeTokenUtils.opTimeFromResumeToken(resumeInfo.getResumeToken()));
      BsonTimestamp lastIndexedOpTime =
          new BsonTimestamp(
              resumeTokenOpTime.getTime(), Math.max(0, resumeTokenOpTime.getInc() - 1));
      ReplicationOpTimeInfo opTimeInfo =
          index.getMetricsUpdater().getIndexingMetricsUpdater().getReplicationOpTimeInfo();

      // Update optime info metric before setting index to STEADY to ensure metrics consistency.
      // Acquire unset lock to ensure another thread does not unset the metric while we update it.
      synchronized (opTimeInfo.getUnsetLock()) {
        // See the STALE branch above: guard the optime update and status write together under this
        // monitor so a racing promotion cannot leave the leader's optime metric regressed to this
        // follower poll's older checkpoint.
        synchronized (this) {
          if (this.wrapped.isPresent() || this.state == State.SHUT_DOWN) {
            return;
          }
          opTimeInfo.update(lastIndexedOpTime.getValue(), lastIndexedOpTime.getValue());
          index.setStatus(IndexStatus.steady());
        }
      }
      return;
    }

    setFollowerStatus(index, IndexStatus.notStarted());
  }

  /**
   * Publishes {@code status} on the index unless this manager has since been promoted to leader
   * (the inner {@link ReplicationIndexManager} then owns status) or shut down. The check-and-set is
   * guarded by this monitor so a concurrent promotion or shutdown wins.
   */
  private synchronized void setFollowerStatus(InitializedIndex index, IndexStatus status) {
    if (this.wrapped.isPresent() || this.state == State.SHUT_DOWN) {
      return;
    }
    index.setStatus(status);
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
      ScheduledExecutorService commitExecutor,
      Duration commitInterval,
      Duration requestRateLimitBackoff,
      MeterRegistry meterRegistry,
      FeatureFlags featureFlags,
      boolean enableNaturalOrderScan,
      PreparableWriter preparableWriter,
      Executor prepareExecutor) {
    // Each leadership session gets its own document indexer, so a late token update or a stale
    // stage
    // from an old session cannot poison the next session. The shared
    // per-generation DocumentIndexer that MongoDbReplicationManager passes into the factory is
    // therefore ignored for VectorLite: the factory takes this session's indexer instead.
    Function<DocumentIndexer, ReplicationIndexManager> wrappedFactory =
        sessionIndexer ->
            ReplicationIndexManager.create(
                lifecycleExecutor,
                cursorManager,
                initialSyncQueue,
                steadyStateManager,
                synonymManager,
                indexGeneration,
                initializedIndex,
                sessionIndexer,
                commitExecutor,
                commitInterval,
                requestRateLimitBackoff,
                meterRegistry,
                featureFlags,
                enableNaturalOrderScan);
    Function<EncodedUserData, SessionFencedDocumentIndexer> sessionIndexerFactory =
        preparedToken ->
            new SessionFencedDocumentIndexer(
                initializedIndex,
                IndexCommitUserData.fromEncodedData(
                    preparedToken, Optional.of(indexGeneration.getGenerationId())),
                meterRegistry);
    RecyclableReplicationIndexManager manager =
        new RecyclableReplicationIndexManager(
            wrappedFactory,
            Optional.of(sessionIndexerFactory),
            lifecycleExecutor,
            preparableWriter,
            prepareExecutor,
            meterRegistry,
            // Defer promotion while the previous session's initial-sync queue entry (queued or
            // in progress) has not been dequeued yet.
            () -> initialSyncQueue.hasEntry(indexGeneration.getGenerationId()),
            // The 1s re-check runs on the same executor as the periodic tick.
            commitExecutor);
    synchronized (manager) {
      // Recover and publish this follower's index status from the committed checkpoint (for
      // VectorLite, read from SLS) so queries see the correct status without replicating. Runs on
      // the lifecycle executor, never the FFM upcall thread.
      manager.scheduleFollowerInit(initializedIndex, featureFlags, indexGeneration);
      manager.startPeriodicReconcile(commitExecutor, DEFAULT_RECONCILE_INTERVAL);
    }
    return manager;
  }

  /**
   * Adapts a document-indexer-agnostic {@link Supplier} factory to the {@link Function} the manager
   * now takes, for the wrapped-factory-only test seams: their inner manager is a mock that ignores
   * its document indexer, so the argument is discarded. Paired with an absent {@link
   * #sessionIndexerFactory}, so no real per-session indexer is built.
   */
  private static Function<DocumentIndexer, ReplicationIndexManager> ignoringIndexer(
      Supplier<ReplicationIndexManager> wrappedFactory) {
    return documentIndexer -> wrappedFactory.get();
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
        ignoringIndexer(wrappedFactory),
        Optional.empty(),
        Runnable::run,
        TEST_ALWAYS_READY_WRITER,
        Runnable::run,
        new SimpleMeterRegistry(),
        () -> false,
        null);
  }

  /**
   * Like {@link #createForTesting(Supplier)} but with an injected transition executor, so tests can
   * drive the asynchronous coalescing/await-shutdown behavior. Does not start the periodic tick.
   */
  @VisibleForTesting
  public static RecyclableReplicationIndexManager createForTesting(
      Supplier<ReplicationIndexManager> wrappedFactory, Executor transitionExecutor) {
    return new RecyclableReplicationIndexManager(
        ignoringIndexer(wrappedFactory),
        Optional.empty(),
        transitionExecutor,
        TEST_ALWAYS_READY_WRITER,
        Runnable::run,
        new SimpleMeterRegistry(),
        () -> false,
        null);
  }

  /**
   * Like {@link #createForTesting(Supplier, Executor)} but with an injected preparable writer and
   * prepare executor, so tests can drive prepare outcomes (Ready / Superseded / error) and the
   * epoch/recycle behavior.
   */
  @VisibleForTesting
  public static RecyclableReplicationIndexManager createForTesting(
      Supplier<ReplicationIndexManager> wrappedFactory,
      Executor transitionExecutor,
      PreparableWriter preparableWriter,
      Executor prepareExecutor) {
    return new RecyclableReplicationIndexManager(
        ignoringIndexer(wrappedFactory),
        Optional.empty(),
        transitionExecutor,
        preparableWriter,
        prepareExecutor,
        new SimpleMeterRegistry(),
        () -> false,
        null);
  }

  /**
   * Like {@link #createForTesting(Supplier, Executor, PreparableWriter, Executor)} but with a
   * document-indexer-taking factory and a real per-session indexer factory, so tests can assert the
   * session indexer is created, tenured before the factory runs, handed to it, and ended when the
   * session's shutdown future completes. The {@code meterRegistry} is shared with the session
   * indexers so a test can read {@code recyclableStaleSessionWrites}.
   */
  @VisibleForTesting
  static RecyclableReplicationIndexManager createForTesting(
      Function<DocumentIndexer, ReplicationIndexManager> wrappedFactory,
      Function<EncodedUserData, SessionFencedDocumentIndexer> sessionIndexerFactory,
      Executor transitionExecutor,
      PreparableWriter preparableWriter,
      Executor prepareExecutor,
      MeterRegistry meterRegistry) {
    return createForTesting(
        wrappedFactory,
        sessionIndexerFactory,
        transitionExecutor,
        preparableWriter,
        prepareExecutor,
        meterRegistry,
        () -> false,
        null);
  }

  /**
   * Like {@link #createForTesting(Function, Function, Executor, PreparableWriter, Executor,
   * MeterRegistry)} but with an injectable initial-sync-entry gate and re-check scheduler, so tests
   * can defer a promotion on a still-queued initial-sync entry and drive the re-check.
   */
  @VisibleForTesting
  static RecyclableReplicationIndexManager createForTesting(
      Function<DocumentIndexer, ReplicationIndexManager> wrappedFactory,
      Function<EncodedUserData, SessionFencedDocumentIndexer> sessionIndexerFactory,
      Executor transitionExecutor,
      PreparableWriter preparableWriter,
      Executor prepareExecutor,
      MeterRegistry meterRegistry,
      BooleanSupplier initialSyncEntryPresent,
      @Nullable ScheduledExecutorService reconcileScheduler) {
    return new RecyclableReplicationIndexManager(
        wrappedFactory,
        Optional.of(sessionIndexerFactory),
        transitionExecutor,
        preparableWriter,
        prepareExecutor,
        meterRegistry,
        initialSyncEntryPresent,
        reconcileScheduler);
  }

  /**
   * Like {@link #createForTesting(Supplier)} but wires the follower status recovery against {@code
   * index}, running it synchronously on the calling thread so tests can assert the published index
   * status. Does not start the periodic tick.
   */
  @VisibleForTesting
  public static RecyclableReplicationIndexManager createForTesting(
      Supplier<ReplicationIndexManager> wrappedFactory,
      InitializedIndex index,
      FeatureFlags featureFlags,
      IndexGeneration indexGeneration) {
    return createForTesting(wrappedFactory, Runnable::run, index, featureFlags, indexGeneration);
  }

  /**
   * Like {@link #createForTesting(Supplier, InitializedIndex, FeatureFlags, IndexGeneration)} but
   * with an injected transition executor, so tests can defer the follower status recovery (e.g. to
   * exercise the promote/shutdown guard in {@link #setFollowerStatus}). Does not start the periodic
   * tick.
   */
  @VisibleForTesting
  public static RecyclableReplicationIndexManager createForTesting(
      Supplier<ReplicationIndexManager> wrappedFactory,
      Executor transitionExecutor,
      InitializedIndex index,
      FeatureFlags featureFlags,
      IndexGeneration indexGeneration) {
    RecyclableReplicationIndexManager manager =
        new RecyclableReplicationIndexManager(
            ignoringIndexer(wrappedFactory),
            Optional.empty(),
            transitionExecutor,
            TEST_ALWAYS_READY_WRITER,
            Runnable::run,
            new SimpleMeterRegistry(),
            () -> false,
            null);
    synchronized (manager) {
      manager.scheduleFollowerInit(index, featureFlags, indexGeneration);
    }
    return manager;
  }

  /** Runs one reconcile tick synchronously, as the periodic scheduler would. */
  @VisibleForTesting
  void triggerReconcileTick() {
    runReconcileTick();
  }

  /** Total count recorded on {@link #LEADERSHIP_TRANSITION_FAILURE_COUNTER_NAME}. */
  @VisibleForTesting
  double leadershipTransitionFailureCount() {
    return this.meterRegistry.find(LEADERSHIP_TRANSITION_FAILURE_COUNTER_NAME).counters().stream()
        .mapToDouble(Counter::count)
        .sum();
  }

  /** Total count recorded on {@link #FOLLOWER_INIT_FAILURE_COUNTER_NAME}. */
  @VisibleForTesting
  double followerInitFailureCount() {
    return this.meterRegistry.find(FOLLOWER_INIT_FAILURE_COUNTER_NAME).counters().stream()
        .mapToDouble(Counter::count)
        .sum();
  }

  /** Total count recorded on {@link #FOLLOWER_REFRESH_FAILURE_COUNTER_NAME}. */
  @VisibleForTesting
  double followerRefreshFailureCount() {
    return this.meterRegistry.find(FOLLOWER_REFRESH_FAILURE_COUNTER_NAME).counters().stream()
        .mapToDouble(Counter::count)
        .sum();
  }

  /** Total count recorded on {@link #PREPARE_SUPERSEDED_COUNTER_NAME}. */
  @VisibleForTesting
  double prepareSupersededCount() {
    return this.meterRegistry.find(PREPARE_SUPERSEDED_COUNTER_NAME).counters().stream()
        .mapToDouble(Counter::count)
        .sum();
  }

  /** Total count recorded on {@link #INITIAL_SYNC_ENTRY_WAIT_COUNTER_NAME}. */
  @VisibleForTesting
  double initialSyncEntryWaitCount() {
    return this.meterRegistry.find(INITIAL_SYNC_ENTRY_WAIT_COUNTER_NAME).counters().stream()
        .mapToDouble(Counter::count)
        .sum();
  }

  /**
   * A {@link DocumentIndexer} that throws on every call: handed to the wrapped-factory-only test
   * factories as a placeholder (their inner manager ignores it) and never reachable in production,
   * where a real {@link SessionFencedDocumentIndexer} is always built per session.
   */
  private static final class UnusableDocumentIndexer implements DocumentIndexer {
    private static UnsupportedOperationException unusable() {
      return new UnsupportedOperationException(
          "wrapped-factory-only test document indexer must not be used");
    }

    @Override
    public void indexDocumentEvent(DocumentEvent event) {
      throw unusable();
    }

    @Override
    public List<DocumentEvent> prepareBatch(List<DocumentEvent> events) {
      throw unusable();
    }

    @Override
    public void updateCommitUserData(IndexCommitUserData commitUserData) {
      throw unusable();
    }

    @Override
    public void updateCommitUserDataSupplier(Supplier<IndexCommitUserData> commitUserDataSupplier) {
      throw unusable();
    }

    @Override
    public void stageCommitUserData() {
      throw unusable();
    }

    @Override
    public void commit() {
      throw unusable();
    }

    @Override
    public void clearIndex(IndexCommitUserData commitUserData) {
      throw unusable();
    }

    @Override
    public Optional<ExceededLimitsException> exceededLimits() {
      throw unusable();
    }

    @Override
    public IndexDefinition getIndexDefinition() {
      throw unusable();
    }
  }
}
