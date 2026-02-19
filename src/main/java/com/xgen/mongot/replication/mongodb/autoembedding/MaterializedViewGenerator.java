package com.xgen.mongot.replication.mongodb.autoembedding;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.ReplicationIndexManager;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

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
 * restarted once stopped. See {@link MaterializedViewManager#transitionToFollower}.
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

  /** Executor for scheduling lifecycle tasks. Stored here since parent's field is private. */
  private final Executor lifecycleExecutor;

  MaterializedViewGenerator(
      Executor lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      IndexGeneration indexGeneration,
      InitializedIndex initializedIndex,
      DocumentIndexer documentIndexer,
      PeriodicIndexCommitter periodicCommitter,
      MetricsFactory metricsFactory,
      FeatureFlags featureFlags,
      Duration resyncBackoff,
      Duration transientBackoff,
      Duration requestRateLimitBackoffMs,
      boolean enableNaturalOrderScan) {
    super(
        lifecycleExecutor,
        cursorManager,
        initialSyncQueue,
        steadyStateManager,
        Collections.emptyList(),
        indexGeneration,
        initializedIndex,
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
  }

  /** Returns whether this generator is currently acting as the leader. */
  public synchronized boolean isLeader() {
    return this.isLeader;
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
    this.logger.info("Transitioning to leader mode, starting replication loop");
    this.isLeader = true;

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
  }

  /**
   * Creates a MaterializedViewGenerator for the supplied materialized view generation. The
   * generator is created in follower mode. Call {@link #becomeLeader()} to activate leader mode and
   * start the replication loop.
   */
  public static MaterializedViewGenerator create(
      Executor lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      IndexGeneration indexGeneration,
      InitializedIndex initializedIndex,
      DocumentIndexer documentIndexer,
      PeriodicIndexCommitter periodicCommitter,
      Duration requestRateLimitBackoffMs,
      MeterRegistry meterRegistry,
      FeatureFlags featureFlags,
      boolean enableNaturalOrderScan) {
    return create(
        lifecycleExecutor,
        cursorManager,
        initialSyncQueue,
        steadyStateManager,
        indexGeneration,
        initializedIndex,
        documentIndexer,
        periodicCommitter,
        meterRegistry,
        featureFlags,
        DEFAULT_RESYNC_BACKOFF,
        DEFAULT_TRANSIENT_BACKOFF,
        requestRateLimitBackoffMs,
        enableNaturalOrderScan);
  }

  @VisibleForTesting
  static MaterializedViewGenerator create(
      Executor lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      IndexGeneration indexGeneration,
      InitializedIndex initializedIndex,
      DocumentIndexer documentIndexer,
      PeriodicIndexCommitter periodicCommitter,
      MeterRegistry meterRegistry,
      FeatureFlags featureFlags,
      Duration resyncBackoff,
      Duration transientBackoff,
      Duration requestRateLimitBackoffMs,
      boolean enableNaturalScan) {

    return new MaterializedViewGenerator(
        lifecycleExecutor,
        cursorManager,
        initialSyncQueue,
        steadyStateManager,
        indexGeneration,
        initializedIndex,
        documentIndexer,
        periodicCommitter,
        new MetricsFactory("materializedViewGenerator", meterRegistry),
        featureFlags,
        resyncBackoff,
        transientBackoff,
        requestRateLimitBackoffMs,
        enableNaturalScan);
  }

  public MaterializedViewIndexGeneration getIndexGeneration() {
    return (MaterializedViewIndexGeneration) this.indexGeneration;
  }

  @Override
  // For auto-embedding index, we always resync instead of leaving the index in
  // RECOVERING_NON_TRANSIENT state.
  protected void handleSteadyStateNonInvalidatingResync(SteadyStateException steadyStateException) {
    this.logger.info(
        "Exception requiring resync occurred during steady state replication.",
        steadyStateException);
    enqueueInitialSync(IndexStatus.initialSync());
  }
}
