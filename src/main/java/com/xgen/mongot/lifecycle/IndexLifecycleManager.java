package com.xgen.mongot.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.blobstore.BlobstoreException;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.blobstore.BlobstoreSnapshotterManager;
import com.xgen.mongot.index.blobstore.IndexBlobstoreSnapshotter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.lifecycle.DefaultLifecycleManager.ReplicationManagerWrapper;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.FutureUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.LoggerFactory;

/**
 * Manage the lifecycle of a single index. Only one IndexLifecycleManager should be created per
 * index generation. An index lifecycle can be in the following states:
 *
 * <p>1. NOT_STARTED -> DOWNLOADING (optional) -> INITIALIZED
 *
 * <p>2. INITIALIZED -> RUNNING
 *
 * <p>3. NOT_STARTED / INITIALIZED / RUNNING -> SHUTDOWN (shutting down lifecyclemanger).
 *
 * <p>4. NOT_STARTED / INITIALIZED / RUNNING -> DROPPED (dropping the index).
 *
 * <p>Except initialization (which can take a long time), all other methods and state transitions
 * are synchronized as they can be accessed by multiple threads.
 */
public class IndexLifecycleManager {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(IndexLifecycleManager.class);
  private static final String INITIALIZATION_FAILED_PREFIX = "Initialization failed: ";

  enum State {
    NOT_STARTED,
    DOWNLOADING,
    INITIALIZED,
    RUNNING,
    SHUTDOWN,
    DROPPED
  }

  // volatile is correct here: state is read outside synchronized blocks (e.g. getState()) but
  // written only inside synchronized blocks, so volatile ensures readers see the latest value
  // without blocking the conf call.
  private volatile State state;
  private final ReplicationManagerWrapper replicationManagerWrapper;
  private final IndexGeneration indexGeneration;
  private final IndexFactory indexFactory;

  @GuardedBy("this")
  private final Optional<? extends BlobstoreSnapshotterManager> snapshotterManager;

  private final boolean blobstoreDownloadsEnabled;

  private final InitializedIndexCatalog initializedIndexCatalog;
  private final Metrics metrics;
  @GuardedBy("this")
  private CompletableFuture<Void> initFuture;

  static class Metrics {
    /** Track the number of indexes that are initialized but not replicating. */
    final AtomicLong indexesInInitializedState;

    final Counter failedInitializationIndexes;
    final Counter failedDropIndexes;
    final Counter failedDownloadIndexes;
    final Timer indexInitializationDurations;

    public Metrics(
        AtomicLong indexesInInitializedState,
        Counter failedInitializationIndexes,
        Counter failedDropIndexes,
        Counter failedDownloadIndexes,
        Timer indexInitializationDurations) {
      this.indexesInInitializedState = indexesInInitializedState;
      this.failedInitializationIndexes = failedInitializationIndexes;
      this.failedDropIndexes = failedDropIndexes;
      this.failedDownloadIndexes = failedDownloadIndexes;
      this.indexInitializationDurations = indexInitializationDurations;
    }

    void decrementIndexesInInitializedState(State state) {
      if (state == State.INITIALIZED) {
        this.indexesInInitializedState.decrementAndGet();
      }
    }

    public static Metrics create(MetricsFactory metricsFactory) {
      return new Metrics(
          metricsFactory.numGauge("indexesInInitializedState"),
          metricsFactory.counter("failedInitializationIndexes"),
          metricsFactory.counter("failedDropIndexes"),
          metricsFactory.counter("failedDownloadIndexes"),
          metricsFactory.timer("indexInitializationDuration"));
    }
  }

  @VisibleForTesting
  IndexLifecycleManager(
      ReplicationManagerWrapper replicationManagerWrapper,
      IndexGeneration indexGeneration,
      InitializedIndexCatalog initializedIndexCatalog,
      IndexFactory indexFactory,
      Optional<? extends BlobstoreSnapshotterManager> snapshotterManager,
      boolean blobstoreDownloadsEnabled,
      Metrics metrics) {
    this.replicationManagerWrapper = replicationManagerWrapper;
    this.indexGeneration = indexGeneration;
    this.snapshotterManager = snapshotterManager;
    this.state = State.NOT_STARTED;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.indexFactory = indexFactory;
    this.metrics = metrics;
    this.blobstoreDownloadsEnabled = blobstoreDownloadsEnabled;
    this.initFuture = FutureUtils.COMPLETED_FUTURE;
  }

  static IndexLifecycleManager create(
      ReplicationManagerWrapper replicationManagerWrapper,
      IndexGeneration indexGeneration,
      InitializedIndexCatalog initializedIndexCatalog,
      IndexFactory indexFactory,
      Optional<? extends BlobstoreSnapshotterManager> snapshotterManager,
      Executor initializationExecutor,
      Executor replicationExecutor,
      Executor blobstoreExecutor,
      Metrics metrics) {
    IndexLifecycleManager indexLifecycleManager =
        new IndexLifecycleManager(
            replicationManagerWrapper,
            indexGeneration,
            initializedIndexCatalog,
            indexFactory,
            snapshotterManager,
            snapshotterManager.isPresent() && snapshotterManager.get().areDownloadsEnabled(),
            metrics);
    indexLifecycleManager.startLifecycle(
        initializationExecutor, replicationExecutor, blobstoreExecutor);
    return indexLifecycleManager;
  }

  public synchronized void startLifecycle(
      Executor initializationExecutor, Executor replicationExecutor, Executor blobstoreExecutor) {
    this.initFuture = CompletableFuture.runAsync(this::initialize, initializationExecutor);
    this.initFuture.thenRunAsync(this::startReplication, replicationExecutor);
    this.initFuture.thenRunAsync(this::scheduleUpload, blobstoreExecutor);
  }

  /**
   * Initialize the index (for now a no-op). Initialization can be canceled due to shutdown, which
   * should be gracefully handled.
   */
  @VisibleForTesting
  void initialize() {
    if (this.state == State.SHUTDOWN || this.state == State.DROPPED) {
      return;
    }
    GenerationId generationId = this.indexGeneration.getGenerationId();
    try {
      var timer = Timer.start();
      getSnapshotter()
          .ifPresent(
              snapshotter -> {
                try {
                  if (this.blobstoreDownloadsEnabled && snapshotter.shouldDownloadIndex()) {
                    // Since drop may be called in another thread, we introduce a synchronized block
                    // to transition from UNKNOWN to NOT_STARTED in an atomic way.
                    synchronized (this) {
                      if (this.indexGeneration.getIndex().getStatus().isUnknown()) {
                        this.indexGeneration.getIndex().setStatus(IndexStatus.notStarted());
                      }
                    }
                    transitionState(State.DOWNLOADING);
                    snapshotter.downloadIndex();
                  }
                } catch (BlobstoreException e) {
                  this.metrics.failedDownloadIndexes.increment();
                  LOG.atError()
                      .addKeyValue("indexId", generationId.indexId)
                      .addKeyValue("generationId", generationId)
                      .setCause(e)
                      .log("Failed to download index");
                }
              });

      var initializedIndex =
          this.indexFactory.getInitializedIndex(
              this.indexGeneration.getIndex(), this.indexGeneration.getDefinitionGeneration());
      long elapsed = timer.stop(this.metrics.indexInitializationDurations);
      LOG.atInfo()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .addKeyValue("duration", Duration.ofNanos(elapsed))
          .log("Initialized index");
      synchronized (this) {
        if (this.state == State.DROPPED) {
          // getInitializedIndex() has already run, so the index holds open resources that must be
          // explicitly closed before deletion. drop() called before getInitializedIndex() (e.g. the
          // early return at the top of this method) has no open resources to clean up.
          try {
            initializedIndex.close();
            initializedIndex.drop();
          } catch (Exception e) {
            LOG.atError()
                .addKeyValue("indexId", generationId.indexId)
                .addKeyValue("generationId", generationId)
                .setCause(e)
                .log("Unable to drop index");
            this.metrics.failedDropIndexes.increment();
          }
          return;
        }
        this.initializedIndexCatalog.addIndex(initializedIndex);
        this.metrics.indexesInInitializedState.incrementAndGet();
        transitionState(State.INITIALIZED);
      }

    } catch (Exception e) {
      LOG.atWarn()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .addKeyValue("state", this.state)
          .setCause(e)
          .log("Initialization failed for index");
      handleInitializationError(e);
    }
  }

  public CompletableFuture<Void> drop() {
    synchronized (this) { // https://github.com/mockito/mockito/issues/2970
      this.metrics.decrementIndexesInInitializedState(this.state);
      transitionState(State.DROPPED);
      this.indexGeneration
          .getIndex()
          .setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.INDEX_DROPPED));
      this.initFuture.cancel(true);

      return this.replicationManagerWrapper.dropIndex(this.indexGeneration.getGenerationId());
    }
  }

  /** Starts replication, this will be triggered after index initialization. */
  public void startReplication() {
    synchronized (this) { // https://github.com/mockito/mockito/issues/2970
      if (this.state != State.INITIALIZED && this.state != State.RUNNING) {
        return;
      }
      if (!this.replicationManagerWrapper.add(this.indexGeneration)) {
        GenerationId generationId = this.indexGeneration.getGenerationId();
        // Can happen if replication is shutdown. Do nothing as we would expect the index to be
        // replicated after replication is restarted.
        LOG.atWarn()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .log("Did not add index for replication");
      } else {
        this.metrics.decrementIndexesInInitializedState(this.state);
        transitionState(State.RUNNING);
      }
    }
  }

  private synchronized Optional<? extends IndexBlobstoreSnapshotter> getSnapshotter() {
    return this.snapshotterManager.flatMap(
        manager -> {
          manager.add(this.indexGeneration);
          return manager.get(this.indexGeneration.getGenerationId());
        });
  }

  private synchronized void scheduleUpload() {
    GenerationId generationId = this.indexGeneration.getGenerationId();
    if ((this.state == State.INITIALIZED || this.state == State.RUNNING)
        && this.snapshotterManager.flatMap(manager -> manager.get(generationId)).isPresent()) {
      this.snapshotterManager.get().scheduleUpload(this.indexGeneration);
    }
  }

  public synchronized void shutdown() {
    this.metrics.decrementIndexesInInitializedState(this.state);
    transitionState(State.SHUTDOWN);
    this.initFuture.cancel(true);
  }

  private synchronized void transitionState(State state) {
    if (this.state != State.SHUTDOWN && this.state != State.DROPPED) {
      this.state = state;
    }
  }

  private void handleInitializationError(Throwable throwable) {
    synchronized (this) {
      Index index = this.indexGeneration.getIndex();
      GenerationId generationId = this.indexGeneration.getGenerationId();

      if (this.state == State.DROPPED) {
        dropIndex();
      } else if (this.state != State.SHUTDOWN) {
        LOG.atError()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .log("Initialization failed for index due to unexpected error.");
        this.metrics.failedInitializationIndexes.increment();
        this.metrics.indexesInInitializedState.decrementAndGet();
        index.setStatus(
            IndexStatus.failed(
                INITIALIZATION_FAILED_PREFIX + throwable.getMessage(),
                IndexStatus.Reason.INITIALIZATION_FAILED));
        transitionState(State.SHUTDOWN);
      }
    }
  }

  private synchronized void dropIndex() {
    try {
      this.indexGeneration.getIndex().close();
      this.indexGeneration.getIndex().drop();
    } catch (Exception e) {
      GenerationId generationId = this.indexGeneration.getGenerationId();
      LOG.atError()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .setCause(e)
          .log("Unable to drop index");
      this.metrics.failedDropIndexes.increment();
    }
  }

  public State getState() {
    return this.state;
  }
}
