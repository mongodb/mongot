package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.mongot.replication.mongodb.MongoDbReplicationManager.getClientSessionRecords;
import static com.xgen.mongot.replication.mongodb.MongoDbReplicationManager.getSyncBatchMongoClient;
import static com.xgen.mongot.replication.mongodb.MongoDbReplicationManager.getSyncSourceHost;
import static com.xgen.mongot.util.Check.checkState;
import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.replication.mongodb.ReplicationIndexManager;
import com.xgen.mongot.replication.mongodb.common.ClientSessionRecord;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DefaultDocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.SteadyStateReplicationConfig;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.Runtime;
import com.xgen.mongot.util.VerboseRunnable;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import com.xgen.mongot.util.mongodb.MongoDbReplSetStatus;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton instance created at startup, manages view generators for all auto-embedding
 * indexes/indexGenerations.
 */
public class MaterializedViewManager implements ReplicationManager {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewManager.class);

  public static final String MAT_VIEW_MANAGER_STATE = "matViewGeneratorState";

  // TODO(CLOUDP-356241): Make this parameter part of durabilityConfig
  private static final int NUM_COMMITTING_THREADS = 1;
  // TODO(CLOUDP-356241): Make this parameter part of durabilityConfig
  private static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(30);

  /** Interval for emitting leader heartbeat log lines for monitoring purposes. */
  private static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofSeconds(30);

  private static final Duration DEFAULT_OPTIME_UPDATE_INTERVAL = Duration.ofSeconds(10);

  public static final String OPTIME_UPDATER_ERROR_COUNTER_NAME = "matViewOptimeUpdaterError";

  public static final String STATE_LABEL = "state";

  /**
   * The Executor that is used by steady state indexing, as well as the ReplicationIndexManager for
   * scheduling its lifecycle tasks.
   *
   * <p>The NamedExecutorService is owned by this AutoEmbeddingMatViewManager.
   */
  private final NamedExecutorService lifecycleExecutor;

  private final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;

  private final MaterializedViewGeneratorFactory matViewGeneratorFactory;

  /**
   * Holds (Host, ClientSessionRecord) mapping
   *
   * <p>The synchronous MongoClient used by the initial sync and session refresh systems, but owned
   * by this AutoEmbeddingMatViewManager.
   */
  private final Map<String, ClientSessionRecord> clientSessionRecordMap;

  private final SyncSourceConfig syncSourceConfig;

  /** This InitialSyncQueue is used for auto-embedding replication workload only. */
  private final InitialSyncQueue initialSyncQueue;

  /** The SteadyStateManager is used for auto-embedding replication workload only. */
  private final SteadyStateManager steadyStateManager;

  private final BatchMongoClient syncBatchMongoClient;

  private final DecodingWorkScheduler decodingWorkScheduler;

  private final MeterRegistry meterRegistry;

  /** A mapping of all initialized materialized view generators by IndexID. */
  @GuardedBy("this")
  private final Map<UUID, MaterializedViewGenerator> managedMaterializedViewGenerators;

  /** A mapping of materialized view collection to active GenerationIds. */
  @GuardedBy("this")
  private final Map<UUID, Set<GenerationId>> activeGenerationIdByMatViewCollection;

  private final NamedScheduledExecutorService commitExecutor;

  private final MetricsFactory metricsFactory;

  private final LeaseManager leaseManager;

  /** Executor for the leader heartbeat task. */
  private final NamedScheduledExecutorService heartbeatExecutor;

  // Executor for periodic optime updates.
  private final NamedScheduledExecutorService optimeUpdaterExecutor;

  private final ScheduledFuture<?> optimeUpdaterFuture;

  private final Counter optimeUpdaterErrorCounter;

  @GuardedBy("this")
  private boolean shutdown;

  @VisibleForTesting
  MaterializedViewManager(
      NamedExecutorService lifecycleExecutor,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      Map<String, ClientSessionRecord> clientSessionRecordMap,
      SyncSourceConfig syncSourceConfig,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      BatchMongoClient syncBatchMongoClient,
      DecodingWorkScheduler decodingWorkScheduler,
      MaterializedViewGeneratorFactory matViewGeneratorFactory,
      NamedScheduledExecutorService commitExecutor,
      NamedScheduledExecutorService heartbeatExecutor,
      NamedScheduledExecutorService optimeUpdaterExecutor,
      MeterRegistry meterRegistry,
      LeaseManager leaseManager) {
    this.lifecycleExecutor = lifecycleExecutor;
    this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
    this.clientSessionRecordMap = clientSessionRecordMap;
    this.initialSyncQueue = initialSyncQueue;
    this.steadyStateManager = steadyStateManager;
    this.syncBatchMongoClient = syncBatchMongoClient;
    this.decodingWorkScheduler = decodingWorkScheduler;
    this.matViewGeneratorFactory = matViewGeneratorFactory;
    this.meterRegistry = meterRegistry;
    this.managedMaterializedViewGenerators = new ConcurrentHashMap<>();
    this.activeGenerationIdByMatViewCollection = new ConcurrentHashMap<>();
    this.commitExecutor = commitExecutor;
    this.heartbeatExecutor = heartbeatExecutor;
    this.optimeUpdaterExecutor = optimeUpdaterExecutor;
    this.syncSourceConfig = syncSourceConfig;
    this.shutdown = false;
    this.metricsFactory =
        new MetricsFactory("autoembedding.replication.mongodb", this.meterRegistry);
    this.leaseManager = leaseManager;
    this.optimeUpdaterErrorCounter = this.meterRegistry.counter(OPTIME_UPDATER_ERROR_COUNTER_NAME);
    createStateGauges(this, this.metricsFactory);
    startHeartbeat();

    // Periodic optime updates for materialized view indexes
    this.optimeUpdaterFuture =
        this.optimeUpdaterExecutor.scheduleWithFixedDelay(
            new VerboseRunnable() {
              @Override
              public void verboseRun() {
                updateMaxReplicationOpTime();
              }

              @Override
              public Logger getLogger() {
                return LOG;
              }
            },
            0,
            DEFAULT_OPTIME_UPDATE_INTERVAL.toMillis(),
            TimeUnit.MILLISECONDS);
  }

  // TODO(CLOUDP-360913): Investigate whether we need customized disk monitor
  /** Creates a new AutoEmbeddingMatViewManager. */
  @VisibleForTesting
  public static MaterializedViewManager create(
      Path rootPath,
      SyncSourceConfig syncSourceConfig,
      // TODO(CLOUDP-360914): Create a separate replication config for mat view.
      MongoDbReplicationConfig materializedViewConfig,
      InitialSyncConfig initialSyncConfig,
      FeatureFlags featureFlags,
      MongotCursorManager cursorManager,
      Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      LeaseManager leaseManager) {
    if (embeddingServiceManagerSupplier.isEmpty()) {
      throw new IllegalArgumentException("EmbeddingServiceManagerSupplier must be provided");
    }
    LOG.info("creating AutoEmbeddingMatViewManager");
    var meterRegistry = meterAndFtdcRegistry.meterRegistry();
    meterRegistry.gauge("materializedView.replication.manager", 1);
    var lifecycleExecutor =
        Executors.fixedSizeThreadPool(
            "materialized-view-lifecycle",
            Math.max(1, Runtime.INSTANCE.getNumCpus() / 4),
            meterRegistry);

    var indexingWorkSchedulerFactory =
        IndexingWorkSchedulerFactory.create(
            materializedViewConfig.numIndexingThreads,
            embeddingServiceManagerSupplier.get(),
            meterRegistry);

    var decodingWorkScheduler =
        DecodingWorkScheduler.create(
            materializedViewConfig.numChangeStreamDecodingThreads, meterRegistry);

    var sessionRefreshExecutor =
        Executors.singleThreadScheduledExecutor("session-refresh", meterRegistry);

    var syncSourceHost = getSyncSourceHost(syncSourceConfig);

    var clientSessionRecords =
        getClientSessionRecords(
            syncSourceConfig,
            materializedViewConfig,
            meterRegistry,
            sessionRefreshExecutor,
            syncSourceHost);

    var syncMongoClient = clientSessionRecords.get(syncSourceHost).syncMongoClient();
    var sessionRefresher = clientSessionRecords.get(syncSourceHost).sessionRefresher();

    var syncBatchMongoClient =
        getSyncBatchMongoClient(
            syncSourceConfig, materializedViewConfig.numConcurrentChangeStreams, meterRegistry);

    var steadyStateManager =
        SteadyStateManager.create(
            meterAndFtdcRegistry,
            sessionRefresher,
            indexingWorkSchedulerFactory,
            syncMongoClient,
            syncBatchMongoClient,
            decodingWorkScheduler,
            getAutoEmbeddingSteadyStateReplicationConfig(materializedViewConfig));

    var initialSyncQueue =
        InitialSyncQueue.create(
            meterRegistry,
            clientSessionRecords,
            syncSourceHost,
            indexingWorkSchedulerFactory,
            materializedViewConfig,
            initialSyncConfig,
            /* This path should be different from dataPath used in Lucene */
            rootPath.resolve("autoEmbedding"),
            ToggleGate.opened());

    var commitExecutor =
        Executors.fixedSizeThreadScheduledExecutor(
            "mat-view-commit", NUM_COMMITTING_THREADS, meterRegistry);

    var heartbeatExecutor =
        Executors.singleThreadScheduledExecutor("mat-view-leader-heartbeat", meterRegistry);

    var optimeUpdaterExecutor =
        Executors.singleThreadScheduledExecutor("mat-view-optime-updater", meterRegistry);

    return new MaterializedViewManager(
        lifecycleExecutor,
        indexingWorkSchedulerFactory,
        clientSessionRecords,
        syncSourceConfig,
        initialSyncQueue,
        steadyStateManager,
        syncBatchMongoClient,
        decodingWorkScheduler,
        new MaterializedViewGeneratorFactory(
            lifecycleExecutor,
            cursorManager,
            initialSyncQueue,
            steadyStateManager,
            meterRegistry,
            featureFlags,
            commitExecutor,
            DEFAULT_COMMIT_INTERVAL,
            Duration.ofMillis(materializedViewConfig.requestRateLimitBackoffMs),
            initialSyncConfig.enableNaturalOrderScan()),
        commitExecutor,
        heartbeatExecutor,
        optimeUpdaterExecutor,
        meterRegistry,
        leaseManager);
  }

  /** Creates gauges to track the number of view generators by state */
  private static void createStateGauges(
      MaterializedViewManager autoEmbeddingMatViewManager, MetricsFactory metricsFactory) {
    Arrays.stream(ReplicationIndexManager.State.values())
        .forEach(
            state ->
                metricsFactory.objectValueGauge(
                    MAT_VIEW_MANAGER_STATE,
                    autoEmbeddingMatViewManager,
                    manager -> manager.gaugeViewGenerators(state),
                    Tags.of(STATE_LABEL, state.name())));
  }

  /** helper function similar to MongoDbReplicationManager::gaugeReplicationManagers */
  private double gaugeViewGenerators(ReplicationIndexManager.State state) {
    return this.getMatViewGenerators().entrySet().stream()
        .filter(m -> m.getValue().getState() == state)
        .count();
  }

  @Override
  public Optional<SyncSourceConfig> getSyncSourceConfig() {
    return Optional.of(this.syncSourceConfig);
  }

  @Override
  public synchronized boolean isInitialized() {
    return this.managedMaterializedViewGenerators.values().stream()
        .map(MaterializedViewGenerator::getInitFuture)
        .allMatch(initFuture -> initFuture.isDone() && !initFuture.isCompletedExceptionally());
  }

  /**
   * Performs AutoEmbeddingMatViewGenerator live swapping if input indexGeneration has a higher
   * definition version (user version), or adds to managedMatViewGenerators directly with no
   * matching AutoEmbeddingMatViewGenerator. Otherwise, treat it as no op.
   *
   * <p>Only supports filter field modification in index redefinition use case
   */
  @Override
  public synchronized void add(IndexGeneration indexGeneration) {
    checkState(!this.shutdown, "cannot call add() after shutdown()");
    // Reference counting by all indexGenerations with all attempts
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        Check.instanceOf(indexGeneration, AutoEmbeddingIndexGeneration.class);
    UUID uuid = getCollectionUuid(autoEmbeddingIndexGeneration.getGenerationId());
    this.activeGenerationIdByMatViewCollection
        .computeIfAbsent(uuid, unused -> ConcurrentHashMap.newKeySet())
        .add(autoEmbeddingIndexGeneration.getGenerationId());

    // Process MaterializedView generation by MaterializedViewIndexGeneration
    MaterializedViewIndexGeneration matViewIndexGeneration =
        autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration();
    this.managedMaterializedViewGenerators.compute(
        uuid,
        (indexId, existingMaterializedViewGenerator) -> {
          if (existingMaterializedViewGenerator == null) {
            this.leaseManager.add(matViewIndexGeneration);
            return this.matViewGeneratorFactory.create(matViewIndexGeneration, COMPLETED_FUTURE);
          } else if (existingMaterializedViewGenerator
              .getIndexGeneration()
              .needsNewMatViewGenerator(matViewIndexGeneration)) {
            // create a new mat view generator, but shutdown the existing one first. add the new
            // index generation to lease manager after the existing one is shutdown
            return this.matViewGeneratorFactory.create(
                matViewIndexGeneration,
                existingMaterializedViewGenerator
                    .shutdown()
                    .thenRun(() -> this.leaseManager.add(matViewIndexGeneration)));
          } else {
            // TODO(CLOUDP-366953): Temporary approach to ensure the new index generation points
            // to the same underlying index when re-using the generator.
            matViewIndexGeneration.swapIndex(
                existingMaterializedViewGenerator.getIndexGeneration().getIndex());
            return existingMaterializedViewGenerator;
          }
        });
  }

  @Override
  public synchronized CompletableFuture<Void> dropIndex(GenerationId generationId) {
    checkState(!this.shutdown, "cannot call dropIndex() after shutdown()");
    UUID uuid = getCollectionUuid(generationId);
    if (this.activeGenerationIdByMatViewCollection.containsKey(uuid)) {
      this.activeGenerationIdByMatViewCollection.get(uuid).remove(generationId);
      if (this.activeGenerationIdByMatViewCollection.get(uuid).isEmpty()) {
        this.activeGenerationIdByMatViewCollection.remove(uuid);
        return dropMatView(uuid, generationId);
      }
    }
    return COMPLETED_FUTURE;
  }

  /**
   * dropMatView is only triggered when MatView collection is no longer needed, not always called
   * with dropIndex, which just shutdowns MatView replication.
   */
  private synchronized CompletableFuture<Void> dropMatView(UUID uuid, GenerationId generationId) {
    checkState(!this.shutdown, "cannot call dropMatView() after shutdown()");
    var matViewGenerator = this.managedMaterializedViewGenerators.remove(uuid);
    var matViewWriter =
        Check.instanceOf(
            matViewGenerator.getIndexGeneration().getIndex().getWriter(),
            MaterializedViewWriter.class);
    return matViewGenerator
        .shutdown()
        .thenComposeAsync(ignored -> matViewWriter.dropMaterializedViewCollection())
        .thenComposeAsync(ignored -> this.leaseManager.drop(generationId))
        .exceptionally(
            throwable -> {
              throw new MaterializedViewNonTransientException(throwable);
            });
  }

  @Override
  public synchronized CompletableFuture<Void> shutdown() {
    LOG.info("Shutting down.");
    this.shutdown = true;

    // Cancel the periodic optime update task
    this.optimeUpdaterFuture.cancel(false);

    List<CompletableFuture<?>> futures =
        this.managedMaterializedViewGenerators.values().stream()
            .map(MaterializedViewGenerator::shutdown)
            .collect(Collectors.toList());

    // Need to create a separate executor to run the shutdown tasks, otherwise it may end up running
    // on the indexing executor. As one of the shutdown tasks is shutting down that executor, this
    // will hang forever.
    var shutdownExecutor =
        Executors.fixedSizeThreadPool("mat-view-manager-shutdown", 1, this.meterRegistry);

    return FutureUtils.allOf(futures)
        .thenComposeAsync(
            ignored ->
                CompletableFuture.allOf(
                    this.initialSyncQueue.shutdown(), this.steadyStateManager.shutdown()),
            shutdownExecutor)
        .thenRunAsync(
            () ->
                this.clientSessionRecordMap
                    .values()
                    .forEach(
                        clientSessionRecord -> {
                          clientSessionRecord.sessionRefresher().shutdown();
                          clientSessionRecord.syncMongoClient().close();
                        }),
            shutdownExecutor)
        .thenRunAsync(this.syncBatchMongoClient::close, shutdownExecutor)
        .thenRunAsync(this.decodingWorkScheduler::shutdown, shutdownExecutor)
        .thenRunAsync(
            () ->
                this.indexingWorkSchedulerFactory
                    .getIndexingWorkSchedulers()
                    .forEach((strategy, scheduler) -> scheduler.shutdown()),
            shutdownExecutor)
        .thenRunAsync(() -> Executors.shutdownOrFail(this.commitExecutor), shutdownExecutor)
        .thenRunAsync(() -> Executors.shutdownOrFail(this.heartbeatExecutor), shutdownExecutor)
        .thenRunAsync(() -> Executors.shutdownOrFail(this.optimeUpdaterExecutor), shutdownExecutor)
        .thenRunAsync(() -> Executors.shutdownOrFail(this.lifecycleExecutor), shutdownExecutor)
        // Signal the shutdown executor to clean up, but don't block waiting for it to do so.
        .thenRunAsync(shutdownExecutor::shutdown, shutdownExecutor);
  }

  @Override
  public boolean isReplicationSupported() {
    return true;
  }

  /**
   * Starts the periodic heartbeat task that emits log lines for monitoring purposes. Customers can
   * use monitoring systems to watch for this log line. If the log line stops appearing for a
   * certain duration, an alert can be triggered indicating the leader may be down.
   */
  private void startHeartbeat() {
    LOG.atInfo()
        .addKeyValue("interval", DEFAULT_HEARTBEAT_INTERVAL)
        .log("Starting auto-embedding leader heartbeat");
    this.heartbeatExecutor.scheduleWithFixedDelay(
        new VerboseRunnable() {
          @Override
          public void verboseRun() {
            emitHeartbeat();
          }

          @Override
          public Logger getLogger() {
            return LOG;
          }
        },
        DEFAULT_HEARTBEAT_INTERVAL.toMillis(),
        DEFAULT_HEARTBEAT_INTERVAL.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  /** Emits a heartbeat log line for monitoring auto-embedding leader health. */
  private void emitHeartbeat() {
    LOG.info("Auto-embedding leader heartbeat");
  }

  /**
   * Periodically updates the maxPossibleReplicationOpTime for all queryable materialized view
   * indexes. This needs to happen separately since this metric is updated only for indexes in the
   * IndexCatalog in ReplicationOptimeUpdater.
   */
  @VisibleForTesting
  void updateMaxReplicationOpTime() {
    if (isShutdown()) {
      return;
    }
    try {
      var opTime =
          MongoDbReplSetStatus.getReadConcernMajorityOpTime(
              this.clientSessionRecordMap
                  .get(getSyncSourceHost(this.syncSourceConfig))
                  .syncMongoClient());
      getMatViewGenerators()
          .values()
          .forEach(
              generator -> {
                var matViewIndex = generator.getIndexGeneration().getIndex();
                if (matViewIndex.isClosed()) {
                  return;
                }
                var status = matViewIndex.getStatus();
                if (!status.canServiceQueries()) {
                  return;
                }
                var opTimeInfo =
                    matViewIndex
                        .getMetricsUpdater()
                        .getIndexingMetricsUpdater()
                        .getReplicationOpTimeInfo();
                status
                    .getOptime()
                    .ifPresentOrElse(
                        replicationOptime ->
                            opTimeInfo.update(replicationOptime.getValue(), opTime.getValue()),
                        () -> opTimeInfo.update(opTime.getValue()));
              });
    } catch (Exception e) {
      LOG.error("Failed to update max optime for materialized views", e);
      this.optimeUpdaterErrorCounter.increment();
    }
  }

  private synchronized boolean isShutdown() {
    return this.shutdown;
  }

  // TODO(CLOUDP-360195): Extract destination Materialized View collection UUID from
  // GenerationId, AutoEmbeddingIndexGenerationFactory should implement compatibility
  // check to decide whether to create a new Materialized View collection for new auto-embedding
  // index definition version
  public static UUID getCollectionUuid(GenerationId generationId) {
    return UUID.nameUUIDFromBytes(generationId.indexId.toByteArray());
  }

  static class MaterializedViewGeneratorFactory {
    private final NamedExecutorService lifecycleExecutor;
    private final MongotCursorManager cursorManager;
    private final InitialSyncQueue initialSyncQueue;
    private final SteadyStateManager steadyStateManager;
    private final MeterRegistry meterRegistry;
    private final FeatureFlags featureFlags;
    private final NamedScheduledExecutorService commitExecutor;
    private final Duration commitInterval;
    private final Duration requestRateLimitBackoffMs;
    private final boolean enableNaturalOrderScan;

    MaterializedViewGeneratorFactory(
        NamedExecutorService lifecycleExecutor,
        MongotCursorManager cursorManager,
        InitialSyncQueue initialSyncQueue,
        SteadyStateManager steadyStateManager,
        MeterRegistry meterRegistry,
        FeatureFlags featureFlags,
        NamedScheduledExecutorService commitExecutor,
        Duration commitInterval,
        Duration requestRateLimitBackoffMs,
        boolean enableNaturalOrderScan) {
      this.lifecycleExecutor = lifecycleExecutor;
      this.cursorManager = cursorManager;
      this.initialSyncQueue = initialSyncQueue;
      this.steadyStateManager = steadyStateManager;
      this.meterRegistry = meterRegistry;
      this.featureFlags = featureFlags;
      this.enableNaturalOrderScan = enableNaturalOrderScan;
      this.commitExecutor = commitExecutor;
      this.commitInterval = commitInterval;
      this.requestRateLimitBackoffMs = requestRateLimitBackoffMs;
    }

    MaterializedViewGenerator create(
        MaterializedViewIndexGeneration matViewIndexGeneration,
        CompletableFuture<Void> preCondition) {
      InitializedMaterializedViewIndex matViewIndex = matViewIndexGeneration.getIndex();
      DocumentIndexer indexer = DefaultDocumentIndexer.create(matViewIndex);
      // TODO(CLOUDP-361153): Remove this or replace this as our customized committer.
      PeriodicIndexCommitter committer =
          new PeriodicIndexCommitter(
              matViewIndex, indexer, this.commitExecutor, this.commitInterval);
      // Close it for now, since we manually commit it in IndexingWorkScheduler::finalizeBatch
      committer.close();
      return MaterializedViewGenerator.create(
          this.lifecycleExecutor,
          this.cursorManager,
          this.initialSyncQueue,
          this.steadyStateManager,
          matViewIndexGeneration,
          matViewIndex,
          indexer,
          committer,
          this.requestRateLimitBackoffMs,
          this.meterRegistry,
          this.featureFlags,
          this.enableNaturalOrderScan,
          preCondition);
    }
  }

  /**
   * Creates a copy of {@link MaterializedViewManager#managedMaterializedViewGenerators}. Thread
   * safe method.
   */
  @SuppressWarnings("GuardedBy") // iterations through ConcurrentHashMap (copying) are thread safe
  private Map<UUID, MaterializedViewGenerator> getMatViewGenerators() {
    return new HashMap<>(this.managedMaterializedViewGenerators);
  }

  /**
   * Creates a SteadyStateReplicationConfig for auto-embedding indexes with INDEXED_FIELDS mode
   * enforced. Auto-embedding indexes always have well-defined field mappings, so projection is
   * always applicable. This eliminates unnecessary IO from non-indexed field updates.
   */
  private static SteadyStateReplicationConfig getAutoEmbeddingSteadyStateReplicationConfig(
      MongoDbReplicationConfig replicationConfig) {
    return SteadyStateReplicationConfig.builder()
        .setNumConcurrentChangeStreams(replicationConfig.numConcurrentChangeStreams)
        .setChangeStreamQueryMaxTimeMs(replicationConfig.changeStreamMaxTimeMs)
        .setChangeStreamCursorMaxTimeSec(replicationConfig.changeStreamCursorMaxTimeSec)
        .setEnableChangeStreamProjection(Optional.of(true)) // Force INDEXED_FIELDS mode
        .setMaxInFlightEmbeddingGetMores(replicationConfig.maxInFlightEmbeddingGetMores)
        .setEmbeddingGetMoreBatchSize(replicationConfig.embeddingGetMoreBatchSize)
        .setExcludedChangestreamFields(replicationConfig.excludedChangestreamFields)
        .setMatchCollectionUuidForUpdateLookup(replicationConfig.matchCollectionUuidForUpdateLookup)
        .setEnableSplitLargeChangeStreamEvents(replicationConfig.enableSplitLargeChangeStreamEvents)
        .build();
  }
}
