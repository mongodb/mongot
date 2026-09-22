package com.xgen.mongot.replication.mongodb.autoembedding;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.ReplicationIndexManager;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.mongot.util.mongodb.Errors;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.jetbrains.annotations.Nullable;

/**
 * MaterializedViewGenerator manages one materialized view collection for auto-embedding indexes.
 * Each generator corresponds to a single materialized view (not an index - one index may have
 * multiple materialized views across generations). Supports both leader and follower roles:
 *
 * <ul>
 *   <li>Leader mode: Runs the full replication lifecycle (initial sync, steady state) and writes to
 *       the materialized view collection.
 *   <li>Follower mode: Remains idle and does not run replication. Status is polled by the
 *       MaterializedViewManager from the LeaseManager.
 * </ul>
 *
 * <p>All generators are created as followers. To activate leader mode, call {@link #becomeLeader()}
 * which starts the replication loop. This design naturally supports both static leadership (call
 * becomeLeader() immediately after creation) and dynamic materialized-view-level leader election
 * (CLOUDP-373432).
 *
 * <p>Note: Generators follow a one-way lifecycle from follower to leader. To transition back to
 * follower mode (e.g., when leadership is lost), the generator must be shut down and replaced with
 * a new generator. This respects the {@link ReplicationIndexManager} design that generators are not
 * restarted once stopped.
 *
 * <p>Leader-status metrics are emitted in MaterializedViewGenerator rather than in {@link
 * com.xgen.mongot.embedding.mongodb.leasing.LeaseManager} because the generator is the component
 * that actually controls the replication workflow. Owning a lease only means this process has been
 * granted leadership in the lease store; it does not by itself determine whether this generator is
 * currently acting as the replication leader. The generator transitions to leader only when {@link
 * #becomeLeader()} runs and starts the replication loop, and it clears the leader status when it
 * shuts down. Emitting in the generator therefore reflects the real replication role (who is
 * running initial sync and steady state), which is what we want to alert on (split-brain = multiple
 * generators running replication; orphaned = no generator running replication).
 */
public class MaterializedViewGenerator extends ReplicationIndexManager {

  /**
   * Whether this generator is currently acting as the leader for this materialized view. When true,
   * the generator runs the replication loop. When false, the generator remains idle and does not
   * run replication. All generators start as followers (isLeader = false) and must call {@link
   * #becomeLeader()} to activate leader mode.
   */
  @GuardedBy("this")
  private boolean isLeader;

  /**
   * Cached shutdown future so that every caller of {@link #shutdown()} gets the same future from
   * the first real shutdown. Without this, a second {@code shutdown()} call would see the generator
   * already in {@code SHUT_DOWN} state and return {@code COMPLETED_FUTURE} immediately, which can
   * cause {@code whenComplete} callbacks to fire before the real shutdown finishes.
   */
  @GuardedBy("this")
  @Nullable
  private CompletableFuture<Void> shutdownFuture = null;

  /** Executor for scheduling lifecycle tasks. Stored here since parent's field is private. */
  private final Executor lifecycleExecutor;

  /**
   * The materialized view index this generator is managing. Used to update the leader-status gauge
   * via {@link InitializedMaterializedViewIndex#setLeaderMode(boolean)}.
   */
  private final InitializedMaterializedViewIndex matViewIndex;

  private final Optional<TenureFencedMaterializedViewDocumentIndexer> tenureIndexer;

  private final Counter oplogFalloffResyncCounter;

  /** Retained so {@link #runIndexerOperation} can tag its skipped-rejection counter by reason. */
  private final MetricsFactory metricsFactory;

  /**
   * Manager-owned recovery for a writer that was fenced by a newer leader. Written once by the
   * manager thread in {@link #setFencingRejectionHandler}, before this generator's replication
   * threads are ever started, and read from those threads thereafter -- {@code volatile} so that
   * ordering holds even if a future change starts them earlier than today's code does.
   */
  private volatile Optional<FencingRejectionHandler> fencingRejectionHandler;

  @FunctionalInterface
  interface FencingRejectionHandler {
    void handle(MaterializedViewGenerator generator);
  }

  /**
   * Invoked when the source collection this generator tails is dropped, before the generator
   * transitions to a terminal state. See {@link #handleCollectionDropped()}.
   */
  private final Runnable onSourceCollectionDropped;

  MaterializedViewGenerator(
      Executor lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      MaterializedViewIndexGeneration indexGeneration,
      InitializedMaterializedViewIndex matViewIndex,
      DocumentIndexer documentIndexer,
      PeriodicIndexCommitter periodicCommitter,
      MetricsFactory metricsFactory,
      FeatureFlags featureFlags,
      Duration resyncBackoff,
      Duration transientBackoff,
      Duration requestRateLimitBackoffMs,
      boolean enableNaturalOrderScan,
      Runnable onSourceCollectionDropped) {
    super(
        lifecycleExecutor,
        cursorManager,
        initialSyncQueue,
        steadyStateManager,
        Collections.emptyList(),
        indexGeneration,
        matViewIndex,
        documentIndexer,
        periodicCommitter,
        metricsFactory,
        featureFlags,
        resyncBackoff,
        transientBackoff,
        requestRateLimitBackoffMs,
        enableNaturalOrderScan);
    this.lifecycleExecutor = lifecycleExecutor;
    this.isLeader = false; // All generators start as followers
    this.matViewIndex = matViewIndex;
    this.tenureIndexer =
        documentIndexer instanceof TenureFencedMaterializedViewDocumentIndexer fencedIndexer
            ? Optional.of(fencedIndexer)
            : Optional.empty();
    this.oplogFalloffResyncCounter = metricsFactory.counter("oplogFalloffResyncEvents");
    this.metricsFactory = metricsFactory;
    this.fencingRejectionHandler = Optional.empty();
    this.onSourceCollectionDropped = onSourceCollectionDropped;
  }

  void setFencingRejectionHandler(FencingRejectionHandler fencingRejectionHandler) {
    this.fencingRejectionHandler = Optional.of(fencingRejectionHandler);
  }

  /**
   * The source collection is gone for good — replication binds to its uuid, which never returns —
   * so the materialized view and its lease can never ingest again no matter what the config later
   * says. Clean them up now, while this generator still holds the lease: the teardown below makes
   * the state terminal, the heartbeat then releases the lease, and cleanup would otherwise wait out
   * the stale-lease GC graces. The callback is best-effort; the local teardown must run regardless.
   */
  @Override
  protected void handleCollectionDropped() {
    try {
      this.onSourceCollectionDropped.run();
    } catch (RuntimeException e) {
      this.logger
          .atWarn()
          .setCause(e)
          .log(
              "Shared resource cleanup on source collection drop failed; the stale-lease GC "
                  + "cleans up after the graces");
    }
    super.handleCollectionDropped();
  }

  /** Returns whether this generator is currently acting as the leader. */
  public synchronized boolean isLeader() {
    return this.isLeader;
  }

  /**
   * Self-heals this generation's {@code leaderStatus} gauge if it was evicted from the registry.
   * See {@link InitializedMaterializedViewIndex#ensureLeaderStatusGaugeRegistered()}.
   */
  void ensureLeaderStatusGaugeRegistered() {
    this.matViewIndex.ensureLeaderStatusGaugeRegistered();
  }

  /**
   * Clears the cached leader flag and the leader-status gauge in lockstep. Idempotent. Lock order:
   * manager → generator (caller in {@link MaterializedViewManager} holds the manager monitor; this
   * method takes the generator monitor briefly), matching the existing {@link
   * MaterializedViewManager#transitionToFollower} → {@link #shutdown()} path.
   */
  synchronized void clearLeaderState() {
    this.isLeader = false;
    this.matViewIndex.setLeaderMode(false);
  }

  /**
   * True if replication shut down because this generation's materialized view collection is missing
   * on this shard. {@link ReplicationIndexManager} commits that reason to the index user data and
   * replays it at every init, so no recreated generator can clear it.
   */
  synchronized boolean isCollectionMissing() {
    // SHUT_DOWN rather than any terminal state: it is the only state the collection-missing path
    // produces, and the status is shared with the follower refresh that copies the leader's.
    return getState() == State.SHUT_DOWN && this.matViewIndex.getStatus().isCollectionNotFound();
  }

  /**
   * Transitions this generator to leader mode and starts the replication loop. In leader mode, the
   * generator runs the full replication lifecycle (initial sync, steady state) and writes to the
   * materialized view collection.
   *
   * <p>If already in leader mode, this is a no-op.
   *
   * <p>This method schedules the replication initialization on the lifecycle executor. If
   * initialization fails, the index is failed with INITIALIZATION_FAILED reason.
   */
  public synchronized void becomeLeader() {
    if (this.isLeader) {
      return;
    }
    this.tenureIndexer.ifPresent(TenureFencedMaterializedViewDocumentIndexer::beginTenure);
    this.logger.info("Transitioning to leader mode, starting replication loop");
    this.isLeader = true;
    this.matViewIndex.setLeaderMode(true);

    // Schedule replication initialization on the lifecycle executor.
    // Note: For dynamic leader election, when a generator loses leadership, it is shut down and
    // replaced with a new follower generator (see MaterializedViewManager.transitionToFollower).
    // When this new generator later acquires leadership, becomeLeader() is called on a fresh
    // generator in the initial follower state, so no special handling is needed here.
    this.initFuture =
        CompletableFuture.runAsync(this::initReplication, this.lifecycleExecutor)
            .handleAsync(
                (ignored, throwable) -> {
                  if (throwable != null) {
                    this.logger
                        .atWarn()
                        .setCause(throwable)
                        .log("Leader initialization failed, dropping index for resync");
                    // For materialized views, data is stored in MongoDB. Always drop on failure,
                    // because the data can be resynced from the source collection.
                    this.failAndDropIndex(throwable, IndexStatus.Reason.INITIALIZATION_FAILED);
                  }
                  return null;
                },
                this.lifecycleExecutor);
  }

  /** Initializes the replication loop. Called by becomeLeader() to start replication. */
  private synchronized void initReplication() {
    super.init();
    // init() may transition to a terminal state (stale index → SHUT_DOWN, exceededIndex →
    // FAILED_EXCEEDED); a terminal generator cannot act as leader.
    if (MaterializedViewManager.TERMINAL_STATES.contains(this.getState())) {
      clearLeaderState();
    }
  }

  @Override
  public synchronized CompletableFuture<Void> shutdown() {
    if (this.shutdownFuture != null) {
      this.logger.atInfo().log("Shutdown already in progress");
      return this.shutdownFuture;
    }
    this.logger.atInfo().log("Shutting down materialized view generator");
    // End the tenure here, and only here: it must happen atomically with the transition to
    // SHUT_DOWN below (both under this monitor). Every guard in ReplicationIndexManager that
    // stops further work from being scheduled is keyed on isShutDown(), so a generator left live
    // but tenure-less would still enter paths like clearIndex(), where the tenure-fenced
    // indexer's rejection would surface inside a Crash.ifThrows wrapper and halt the JVM.
    this.tenureIndexer.ifPresent(TenureFencedMaterializedViewDocumentIndexer::endTenure);
    this.shutdownFuture = super.shutdown();
    this.matViewIndex.setLeaderMode(false);
    // Once shutdown, this generator is no longer the leader, as becomeLeader will be no-op because
    // of shutdown checks in ReplicationIndexManager
    this.isLeader = false;
    return this.shutdownFuture;
  }

  /**
   * Creates a MaterializedViewGenerator for the supplied materialized view generation. The
   * generator is created in follower mode. Call {@link #becomeLeader()} to activate leader mode and
   * start the replication loop.
   *
   * @param matViewIndex the materialized view index to manage. The generator will call {@link
   *     InitializedMaterializedViewIndex#setLeaderMode(boolean)} to update the leader-status gauge.
   */
  public static MaterializedViewGenerator create(
      Executor lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      MaterializedViewIndexGeneration indexGeneration,
      InitializedMaterializedViewIndex matViewIndex,
      DocumentIndexer documentIndexer,
      PeriodicIndexCommitter periodicCommitter,
      Duration resyncBackoff,
      Duration transientBackoff,
      Duration requestRateLimitBackoffMs,
      MeterRegistry meterRegistry,
      FeatureFlags featureFlags,
      boolean enableNaturalOrderScan,
      Runnable onSourceCollectionDropped) {
    return new MaterializedViewGenerator(
        lifecycleExecutor,
        cursorManager,
        initialSyncQueue,
        steadyStateManager,
        indexGeneration,
        matViewIndex,
        documentIndexer,
        periodicCommitter,
        new MetricsFactory("materializedViewGenerator", meterRegistry),
        featureFlags,
        resyncBackoff,
        transientBackoff,
        requestRateLimitBackoffMs,
        enableNaturalOrderScan,
        onSourceCollectionDropped);
  }

  public MaterializedViewIndexGeneration getIndexGeneration() {
    return (MaterializedViewIndexGeneration) this.indexGeneration;
  }

  @Override
  // For auto-embedding index, we always resync instead of leaving the index in
  // RECOVERING_NON_TRANSIENT state.
  protected void handleSteadyStateNonInvalidatingResync(SteadyStateException steadyStateException) {
    if (handleFencingRejectionOrResyncInPlace(
        steadyStateException.getCause(), steadyStateException)) {
      return;
    }
    this.logger
        .atInfo()
        .setCause(steadyStateException)
        .log("Resync triggered during steady state replication for auto-embedding index");
    if (isOplogFalloff(steadyStateException)) {
      this.oplogFalloffResyncCounter.increment();
    }
    enqueueInitialSync(IndexStatus.initialSync());
  }

  @Override
  // A fencing rejection during initial sync would otherwise fall into the generic FAILED path
  // (failAndDropIndex/failAndCloseIndex with INITIAL_SYNC_REPLICATION_FAILED), a reason
  // IndexStatus.canBeRecovered() does not cover, so IndexRecoveryStager never restages it and this
  // generator's generation is stuck until another host or an external config change intervenes.
  // Route it through the same handling as the steady-state case instead.
  protected void handleInitialSyncFailed(InitialSyncException initialSyncException) {
    if (handleFencingRejectionOrResyncInPlace(
        initialSyncException.getCause(), initialSyncException)) {
      return;
    }
    super.handleInitialSyncFailed(initialSyncException);
  }

  /**
   * Handles a fencing rejection the same way regardless of which path (steady-state resync or
   * initial-sync failure) detected it: retires and replaces the generator via the manager-owned
   * handler when one is installed (dynamic leader election), or resyncs in place when it isn't.
   *
   * <p>Static leader election has no retire-and-replace path -- its {@code LeaseManager}'s {@code
   * releaseLeadership()}/{@code tryAcquireLeadership()} are no-ops -- so on initial sync
   * specifically, falling through to the generic {@code FAILED} path would strand the generation on
   * a reason {@code IndexStatus.canBeRecovered()} doesn't cover, with nothing left to ever restage
   * it. Resyncing in place instead matches what the steady-state path already does for a static
   * leader.
   *
   * @return true if {@code cause} was a fencing rejection and has been fully handled
   */
  private boolean handleFencingRejectionOrResyncInPlace(
      @Nullable Throwable cause, Exception fullException) {
    if (!isFencingRejection(cause)) {
      return false;
    }
    if (this.fencingRejectionHandler.isPresent()) {
      this.logger.atWarn().setCause(fullException).log("Fenced by a newer leader");
      this.fencingRejectionHandler.get().handle(this);
    } else {
      this.logger
          .atInfo()
          .setCause(fullException)
          .log("Fenced by a newer leader; resyncing in place (static leader election)");
      enqueueInitialSync(IndexStatus.initialSync());
    }
    return true;
  }

  @Override
  // Reached from shutdownReplicationAndCommitStatus()/staleIndex()/clearIndex()'s commitIndexer()/
  // clearIndexer() call, whose write can be rejected for an ended tenure (INVALID_LEASE) or a
  // fencing rejection (FENCING_REJECTION). All three already guard against writing once this
  // generator is SHUT_DOWN (see ReplicationIndexManager), so a rejection reaching here despite
  // that guard means either a narrow window between the guard and the write (this generator's own
  // shutdown() raced in) or, if not yet SHUT_DOWN, a genuine unexpected failure -- rethrow for that
  // case so it still halts. maybeForceMergeAfterInitialSync() is overridden below to never call
  // commit() for this generator, and handleSteadyStateNonInvalidatingResync()/
  // handleInitialSyncFailed() detect FENCING_REJECTION themselves rather than letting it reach the
  // indexer, so those aren't sources of this either.
  protected void runIndexerOperation(CheckedRunnable<IOException> operation) throws IOException {
    try {
      operation.run();
    } catch (MaterializedViewNonTransientException e) {
      boolean isTerminalWriteRejection =
          switch (e.getReason()) {
            case INVALID_LEASE, FENCING_REJECTION -> true;
            default -> false;
          };
      if (!isTerminalWriteRejection || getState() != State.SHUT_DOWN) {
        throw e;
      }
      this.logger
          .atWarn()
          .setCause(e)
          .log(
              "Commit rejected ({}) while already shutting down for another reason; skipping",
              e.getReason());
      this.metricsFactory
          .counter("skippedTerminalCommitRejections", Tags.of("reason", e.getReason().name()))
          .increment();
    }
  }

  @Override
  // Force-merge exists to pin Lucene segment count for query-latency perf testing
  // (FORCE_MERGE_MAX_NUM_SEGMENTS_PROPERTY). The auto-embed index that benchmark targets is built
  // by a separate, non-MV ReplicationIndexManager reading from this generator's MV collection via
  // change streams -- not this generator. This generator's own Lucene index is not that query
  // path (VectorSearchCommand's explicit-queryVector bypass is its only direct reader, and nothing
  // perf-tests that), so force-merge has nothing to do here.
  protected void maybeForceMergeAfterInitialSync() {}

  private static boolean isOplogFalloff(SteadyStateException steadyStateException) {
    Throwable cause = steadyStateException.getCause();
    if (cause instanceof com.mongodb.MongoException mongoException) {
      int errorCode = mongoException.getCode();
      return errorCode == Errors.CHANGE_STREAM_HISTORY_LOST.code
          || errorCode == Errors.CAPPED_POSITION_LOST.code;
    }
    return false;
  }

  private static boolean isFencingRejection(@Nullable Throwable cause) {
    return cause instanceof MaterializedViewNonTransientException exception
        && exception.getReason() == MaterializedViewNonTransientException.Reason.FENCING_REJECTION;
  }
}
