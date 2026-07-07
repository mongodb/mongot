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
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/**
 * An {@link IndexManager} for VectorLite indexes that defers replication until this node is elected
 * leader. Created in follower mode with no active inner manager. On {@link LeaderStatus#LEADER}, a
 * fresh inner {@link ReplicationIndexManager} is created and starts replication. On {@link
 * LeaderStatus#FOLLOWER}, the inner manager is shut down.
 */
public final class RecyclableReplicationIndexManager implements IndexManager {

  /** Leadership states signaled by the native vector engine via FFM upcall. */
  public enum LeaderStatus {
    LEADER,
    FOLLOWER
  }

  private final Supplier<ReplicationIndexManager> wrappedFactory;

  @GuardedBy("this")
  private Optional<ReplicationIndexManager> wrapped;

  @GuardedBy("this")
  private State state;

  @GuardedBy("this")
  private Optional<CompletableFuture<Void>> shutdownFuture;

  private RecyclableReplicationIndexManager(Supplier<ReplicationIndexManager> wrappedFactory) {
    this.wrappedFactory = wrappedFactory;
    this.wrapped = Optional.empty();
    this.state = State.INITIALIZING;
    this.shutdownFuture = Optional.empty();
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
  public synchronized CompletableFuture<Void> shutdown() {
    if (this.isLeader()) {
      return this.wrapped.get().shutdown();
    }
    this.state = State.SHUT_DOWN;
    return this.shutdownFuture.orElse(CompletableFuture.completedFuture(null));
  }

  @Override
  public synchronized CompletableFuture<Void> drop() {
    if (this.isLeader()) {
      return this.wrapped.get().drop();
    }
    this.state = State.SHUT_DOWN;
    return this.shutdownFuture.orElse(CompletableFuture.completedFuture(null));
  }

  /**
   * Called when this node's leadership status changes. Thread-safe; may be invoked from an FFM
   * upcall thread.
   *
   * <p>On {@link LeaderStatus#LEADER}: creates a fresh inner {@link ReplicationIndexManager} which
   * starts replication asynchronously. Idempotent — subsequent LEADER signals while already leader
   * are no-ops.
   *
   * <p>On {@link LeaderStatus#FOLLOWER} : shutdown replication and reset wrapped to empty
   */
  public synchronized void onLeaderStatusChange(LeaderStatus newStatus) {
    if (newStatus == LeaderStatus.LEADER) {
      if (this.isLeader()) {
        return;
      }
      // wait for previous shutdown finishes before start a new wrapper
      if (this.shutdownFuture.isPresent()) {
        this.shutdownFuture.get().join();
      }
      this.wrapped = Optional.of(this.wrappedFactory.get());
    } else {
      if (this.isLeader()) {
        this.shutdownFuture = Optional.of(this.wrapped.get().shutdown());
        this.wrapped = Optional.empty();
      }
    }
  }

  /**
   * Creates a {@link RecyclableReplicationIndexManager} in follower mode. Replication will not
   * start until {@link #onLeaderStatusChange(LeaderStatus)} is called with {@link
   * LeaderStatus#LEADER}.
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
    return new RecyclableReplicationIndexManager(wrappedFactory);
  }

  @VisibleForTesting
  public static RecyclableReplicationIndexManager createForTesting(
      Supplier<ReplicationIndexManager> wrappedFactory) {
    return new RecyclableReplicationIndexManager(wrappedFactory);
  }
}
