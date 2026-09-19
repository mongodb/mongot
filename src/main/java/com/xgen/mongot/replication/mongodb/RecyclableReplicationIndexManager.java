package com.xgen.mongot.replication.mongodb;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.bson.BsonTimestamp;
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

  @Override
  public synchronized CompletableFuture<Void> getInitFuture() {
    // While a follower, readiness reflects the status-recovery init; once leader, the inner
    // manager's init (which starts replication) is authoritative.
    return this.wrapped.map(ReplicationIndexManager::getInitFuture).orElse(this.followerInitFuture);
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
        // Finish follower status recovery before promoting, so the leader init does not race the
        // follower init. Non-blocking: re-drive reconcile once the init completes.
        // followerInitFuture always completes normally (see scheduleFollowerInit), so this
        // continuation never skips.
        if (!this.followerInitFuture.isDone()) {
          return this.followerInitFuture.thenComposeAsync(
              ignored -> reconcile(), this.transitionExecutor);
        }
        // Promote. create() only schedules async init (VectorLite indexes have no synonym
        // managers), so it does not block while this monitor is held.
        //
        // No pin refresh here. The storage layer's leadership takeover is materialization-frontier
        // gated: it publishes LEADER only once this node's read pin already covers the latest
        // durable checkpoint root, so the inner manager's getCommitUserData() init read is already
        // the latest durable token and resumes replication from it. This manager must NEVER
        // force-advance the pin -- jumping it past the materialized frontier could read
        // unmaterialized pages. TODO(CLOUDP-446295): the promoted node's native worker write base
        // is still frozen at index-open, so its first commit can orphan docs the old leader indexed
        // during this process's uptime (leadership-scoped worker reseed).
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
      // Force the first post-demotion poll to republish even if the committed data equals what this
      // node last published as a follower: the inner manager owned (and may have overwritten) the
      // index status in between.
      this.lastPublishedUserData = Optional.empty();
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
   * scheduleWithFixedDelay} would cancel the repeating task. Dispatch-only: the leadership
   * reconcile body and the follower status refresh both run elsewhere ({@link
   * #transitionExecutor}), never on the scheduler thread.
   */
  private synchronized void runReconcileTick() {
    try {
      if (needsReconcile()) {
        enqueueReconcile();
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
   */
  @GuardedBy("this")
  private void maybeScheduleFollowerRefresh() {
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

    Optional<String> exceededReason = userData.getExceededLimitsReason();
    if (exceededReason.isPresent()) {
      setFollowerStatus(
          index,
          IndexStatus.failed(
              ReplicationIndexManager.EXCEEDED_LIMIT_REASON_PREFIX + exceededReason.get()));
      return;
    }

    Optional<StaleStateInfo> staleStateInfo = userData.getStaleStateInfo();
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
      Optional<IndexStateInfo> indexStateInfo = userData.getIndexStateInfo();
      if (indexStateInfo.isPresent() && indexStateInfo.get().isCollectionNotFound()) {
        setFollowerStatus(index, IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
        return;
      }
    }

    if (userData.getInitialSyncResumeInfo().isPresent()) {
      setFollowerStatus(index, IndexStatus.initialSync());
      return;
    }

    if (userData.getResumeInfo().isPresent()) {
      ChangeStreamResumeInfo resumeInfo = userData.getResumeInfo().get();
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
      // Recover and publish this follower's index status from the committed checkpoint (for
      // VectorLite, read from SLS) so queries see the correct status without replicating. Runs on
      // the lifecycle executor, never the FFM upcall thread.
      manager.scheduleFollowerInit(initializedIndex, featureFlags, indexGeneration);
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
            wrappedFactory, transitionExecutor, new SimpleMeterRegistry());
    synchronized (manager) {
      manager.scheduleFollowerInit(index, featureFlags, indexGeneration);
    }
    return manager;
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
}
