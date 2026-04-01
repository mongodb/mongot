package com.xgen.mongot.lifecycle;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.blobstore.BlobstoreSnapshotterManager;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.replication.ReplicationManagerFactory;
import com.xgen.mongot.replication.mongodb.autoembedding.AutoEmbeddingMaterializedViewManagerFactory;
import com.xgen.mongot.replication.mongodb.autoembedding.MaterializedViewManager;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.Runtime;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.LoggerFactory;

/**
 * DefaultLifecycleManager implements a LifecycleManager that manages the lifecycle of Indexes, and
 * manages their replication and cursors.
 *
 * <p>This class is thread-safe.
 */
public class DefaultLifecycleManager implements LifecycleManager {
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(DefaultLifecycleManager.class);

  /** This is overridden by the corresponding lifecycle config value if defined. */
  private static final int DEFAULT_INITIALIZATION_THREADS =
      Math.max(1, Runtime.INSTANCE.getNumCpus() / 4);

  /** A mapping of existing IndexLifecycleManagers. */
  @GuardedBy("this")
  private final Map<GenerationId, IndexLifecycleManager> indexManagers;

  /** If the existing IndexLifecycleManagers have started processing the index lifecycles. */
  @GuardedBy("this")
  private boolean initialized;

  @GuardedBy("this")
  private boolean shutdown;

  /**
   * The Executor that is used for initializing the index.
   *
   * <p>The NamedExecutorService is owned by this LifecycleManager.
   */
  private final NamedExecutorService initExecutor;

  /**
   * The Executor that is used by steady state indexing, as well as the ReplicationIndexManager for
   * scheduling its lifecycle tasks.
   *
   * <p>The NamedExecutorService is owned by this LifecycleManager.
   */
  private final NamedExecutorService lifecycleExecutor;

  private final NamedExecutorService blobstoreExecutor;

  private final ReplicationManagerFactory replicationManagerFactory;
  private final AutoEmbeddingMaterializedViewManagerFactory autoEmbeddingMatViewManagerFactory;
  private final ReplicationManagerWrapper replicationManagerWrapper;
  private final Optional<MaterializedViewManager> materializedViewManager;
  private final InitializedIndexCatalog initializedIndexCatalog;
  private final IndexFactory indexFactory;
  private final Optional<? extends BlobstoreSnapshotterManager> snapshotterManager;
  // The MeterRegistry is required for instantiating executors.
  private final MeterRegistry meterRegistry;
  private Optional<SyncSourceConfig> syncSourceConfig;

  private final IndexLifecycleManager.Metrics metrics;

  /**
   * A wrapper over the ReplicationManager that can be used across different instantiations of the
   * ReplicationManager. Thread-safe.
   */
  static class ReplicationManagerWrapper {
    private volatile ReplicationManager currentReplicationManager;

    /** Track whether replication is currently shutdown or not. */
    private volatile boolean isReplicationEnabled;

    ReplicationManagerWrapper(ReplicationManager manager) {
      this.currentReplicationManager = manager;
      this.isReplicationEnabled = true;
    }

    synchronized void setCurrentReplicationManager(ReplicationManager lifecycleManager) {
      this.currentReplicationManager = lifecycleManager;
    }

    synchronized void setReplicationEnabled(boolean replicationEnabled) {
      this.isReplicationEnabled = replicationEnabled;
    }

    synchronized boolean isReplicationSupported() {
      return this.currentReplicationManager.isReplicationSupported();
    }

    synchronized boolean add(IndexGeneration indexGeneration) {
      if (indexGeneration.getIndex().isClosed() || !this.isReplicationEnabled) {
        return false;
      }
      this.currentReplicationManager.add(indexGeneration);
      return true;
    }

    synchronized CompletableFuture<Void> dropIndex(GenerationId generationId) {
      return this.currentReplicationManager.dropIndex(generationId);
    }
  }

  public DefaultLifecycleManager(
      ReplicationManagerFactory replicationManagerFactory,
      Optional<SyncSourceConfig> syncSourceConfig,
      InitializedIndexCatalog initializedIndexCatalog,
      IndexFactory indexFactory,
      Optional<? extends BlobstoreSnapshotterManager> snapshotterManager,
      AutoEmbeddingMaterializedViewManagerFactory autoEmbeddingMaterializedViewManagerFactory,
      MeterRegistry meterRegistry,
      Gate replicationGate,
      LifecycleConfig lifecycleConfig) {
    this(
        replicationManagerFactory,
        syncSourceConfig,
        initializedIndexCatalog,
        indexFactory,
        snapshotterManager,
        autoEmbeddingMaterializedViewManagerFactory,
        meterRegistry,
        replicationGate,
        Executors.fixedSizeThreadPool(
            "init-lifecycle",
            lifecycleConfig.initializationThreads.orElse(DEFAULT_INITIALIZATION_THREADS),
            meterRegistry),
        Executors.fixedSizeThreadPool(
            "index-lifecycle", Math.max(1, Runtime.INSTANCE.getNumCpus() / 4), meterRegistry),
        Executors.fixedSizeThreadPool("blobstore-lifecycle", 1, meterRegistry));
  }

  @VisibleForTesting
  DefaultLifecycleManager(
      ReplicationManagerFactory replicationManagerFactory,
      Optional<SyncSourceConfig> syncSourceConfig,
      InitializedIndexCatalog initializedIndexCatalog,
      IndexFactory indexFactory,
      Optional<? extends BlobstoreSnapshotterManager> snapshotterManager,
      AutoEmbeddingMaterializedViewManagerFactory autoEmbeddingMatViewManagerFactory,
      MeterRegistry meterRegistry,
      Gate replicationGate,
      NamedExecutorService initExecutor,
      NamedExecutorService lifecycleExecutor,
      NamedExecutorService blobstoreExecutor) {
    this.initExecutor = initExecutor;
    this.lifecycleExecutor = lifecycleExecutor;
    this.blobstoreExecutor = blobstoreExecutor;
    this.replicationManagerFactory = replicationManagerFactory;
    this.autoEmbeddingMatViewManagerFactory = autoEmbeddingMatViewManagerFactory;
    this.syncSourceConfig = syncSourceConfig;

    if (replicationGate.isClosed()) {
      LOG.atWarn()
          .addKeyValue("replicationGate", replicationGate.toString())
          .log("Disk usage exceeded pause threshold, pausing replication on initialization.");
    }
    this.replicationManagerWrapper =
        new ReplicationManagerWrapper(this.replicationManagerFactory.create(syncSourceConfig));

    this.initializedIndexCatalog = initializedIndexCatalog;
    this.indexFactory = indexFactory;
    this.snapshotterManager = snapshotterManager;
    // TODO(CLOUDP-361549): Provide a way to disable it
    this.materializedViewManager = this.autoEmbeddingMatViewManagerFactory.create(syncSourceConfig);
    this.meterRegistry = meterRegistry;
    MetricsFactory metricsFactory = new MetricsFactory("lifecycle", meterRegistry);
    this.indexManagers = new HashMap<>();
    this.initialized = false;
    this.shutdown = false;
    this.metrics = IndexLifecycleManager.Metrics.create(metricsFactory);
  }

  @Override
  public synchronized boolean isInitialized() {
    if (!this.initialized) {
      this.initialized =
          this.indexManagers.values().stream()
                  .allMatch(
                      indexManager ->
                          indexManager.getState() != IndexLifecycleManager.State.NOT_STARTED
                              && indexManager.getState() != IndexLifecycleManager.State.INITIALIZED)
              // TODO(CLOUDP-361594): Should we include MaterializedViewManager::initialized in
              // this method for HealthManager/ConfCall?
              && this.replicationManagerWrapper.currentReplicationManager.isInitialized();
    }
    return this.initialized;
  }

  @Override
  public synchronized void add(IndexGeneration indexGeneration) {
    checkState(!this.shutdown, "cannot call add() after shutdown()");
    @Var IndexGeneration indexGenerationForReplication = indexGeneration;
    if (indexGeneration.getType() == IndexDefinitionGeneration.Type.AUTO_EMBEDDING) {
      AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
          Check.instanceOf(indexGeneration, AutoEmbeddingIndexGeneration.class);
      this.materializedViewManager.ifPresent(
          matViewManager -> matViewManager.add(autoEmbeddingIndexGeneration));
      // Extract derived indexGeneration for normal processing.
      indexGenerationForReplication = autoEmbeddingIndexGeneration.getDerivedIndexGeneration();
    }
    GenerationId generationId = indexGenerationForReplication.getGenerationId();
    checkState(
        !this.indexManagers.containsKey(generationId),
        "index %s has already been added",
        generationId);

    IndexLifecycleManager indexLifecycleManager =
        IndexLifecycleManager.create(
            this.replicationManagerWrapper,
            indexGenerationForReplication,
            this.initializedIndexCatalog,
            this.indexFactory,
            this.snapshotterManager,
            this.initExecutor,
            this.lifecycleExecutor,
            this.blobstoreExecutor,
            this.metrics);

    this.indexManagers.put(generationId, indexLifecycleManager);
  }

  @Override
  public synchronized void restartReplication() {
    this.replicationManagerWrapper.setReplicationEnabled(true);
    this.indexManagers.values().forEach(IndexLifecycleManager::startReplication);
    this.materializedViewManager.ifPresent(MaterializedViewManager::restartReplication);
  }

  @Override
  public synchronized CompletableFuture<Void> dropIndex(GenerationId generationId) {
    checkState(!this.shutdown, "cannot call dropIndex() after shutdown()");
    if (!this.indexManagers.containsKey(generationId)) {
      LOG.atWarn()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .log("Cannot drop index because it is not already added.");
      return CompletableFuture.completedFuture(null);
    }
    this.snapshotterManager.ifPresent(manager -> manager.drop(generationId));
    IndexLifecycleManager indexManager = this.indexManagers.remove(generationId);
    return FutureUtils.allOf(
        List.of(
            indexManager.drop(),
            this.materializedViewManager
                .map(matViewManager -> matViewManager.dropIndex(generationId))
                .orElse(FutureUtils.COMPLETED_FUTURE)));
  }

  @Override
  public Optional<SyncSourceConfig> getSyncSourceConfig() {
    return this.syncSourceConfig;
  }

  @Override
  public synchronized void updateSyncSource(SyncSourceConfig syncSourceConfig) {
    this.replicationManagerWrapper.setCurrentReplicationManager(
        this.replicationManagerFactory.create(Optional.of(syncSourceConfig)));
    this.syncSourceConfig = Optional.of(syncSourceConfig);
    this.materializedViewManager.ifPresent(
        matViewManager -> matViewManager.updateSyncSource(syncSourceConfig));
  }

  @Override
  public synchronized boolean isReplicationSupported() {
    return this.replicationManagerWrapper.isReplicationSupported();
  }

  @Override
  public synchronized CompletableFuture<Void> shutdown() {
    // Need to create a separate executor to run the shutdown tasks, otherwise it may end up running
    // on the indexing executor. As one of the shutdown tasks is shutting down that executor, this
    // will hang forever.
    this.shutdown = true;
    this.replicationManagerWrapper.setReplicationEnabled(false);
    var shutdownExecutor =
        Executors.fixedSizeThreadPool("lifecycle-manager-shutdown", 1, this.meterRegistry);

    this.snapshotterManager.ifPresent(BlobstoreSnapshotterManager::shutdown);
    this.indexManagers.values().forEach(IndexLifecycleManager::shutdown);

    // Only shutdown the executor service after all the tasks complete to avoid race condition
    // where metrics are deregistered before all tasks finish.
    return FutureUtils.allOf(
            List.of(
                CompletableFuture.runAsync(
                    () -> Executors.shutdownOrFail(this.initExecutor), shutdownExecutor),
                CompletableFuture.runAsync(
                    () -> Executors.shutdownOrFail(this.lifecycleExecutor), shutdownExecutor),
                CompletableFuture.runAsync(
                    () -> Executors.shutdownOrFail(this.blobstoreExecutor), shutdownExecutor)))
        .thenComposeAsync(
            ignored ->
                FutureUtils.allOf(
                    List.of(
                        this.replicationManagerWrapper.currentReplicationManager.shutdown(),
                        this.materializedViewManager
                            .map(MaterializedViewManager::shutdown)
                            .orElse(FutureUtils.COMPLETED_FUTURE))),
            shutdownExecutor)
        .whenComplete((result, throwable) -> shutdownExecutor.shutdown());
  }

  @Override
  public CompletableFuture<Void> shutdownReplication() {
    this.replicationManagerWrapper.setReplicationEnabled(false);
    return FutureUtils.allOf(
        List.of(
            this.replicationManagerWrapper.currentReplicationManager.shutdown(),
            this.materializedViewManager
                .map(MaterializedViewManager::shutdownReplication)
                .orElse(FutureUtils.COMPLETED_FUTURE)));
  }

  @TestOnly
  @Override
  public ReplicationManager getReplicationManager() {
    return this.replicationManagerWrapper.currentReplicationManager;
  }

  @TestOnly
  @VisibleForTesting
  boolean isReplicationShutdown() {
    return !this.replicationManagerWrapper.isReplicationEnabled;
  }

  @TestOnly
  @VisibleForTesting
  synchronized IndexLifecycleManager getIndexLifecycleManager(GenerationId generationId) {
    return this.indexManagers.get(generationId);
  }

  @TestOnly
  @VisibleForTesting
  public Optional<? extends BlobstoreSnapshotterManager> getSnapshotterManager() {
    return this.snapshotterManager;
  }
}
