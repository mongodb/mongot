package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.mongot.replication.mongodb.MongoDbReplicationManager.getClientSessionRecords;
import static com.xgen.mongot.replication.mongodb.MongoDbReplicationManager.getSyncBatchMongoClient;
import static com.xgen.mongot.replication.mongodb.MongoDbReplicationManager.getSyncSourceHost;
import static com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig.Type.AUTO_EMBEDDING;
import static com.xgen.mongot.util.Check.checkState;
import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;

import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.mongodb.ReplicationIndexManager;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.ClientSessionRecord;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.SteadyStateReplicationConfig;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * Factory for creating MaterializedViewGenerator instances. All generators are created as
 * followers. The caller is responsible for calling {@link MaterializedViewGenerator#becomeLeader()}
 * to activate leader mode when appropriate.
 */
class MaterializedViewGeneratorFactory {
  private final Path resolvedPath;
  private final NamedExecutorService lifecycleExecutor;
  private final MongotCursorManager cursorManager;
  private final MeterAndFtdcRegistry meterAndFtdcRegistry;
  private final FeatureFlags featureFlags;
  private final Duration resyncBackoff;
  private final Duration transientBackoff;
  private final Duration requestRateLimitBackoffMs;
  private final boolean enableNaturalOrderScan;
  private final AutoEmbeddingMaterializedViewConfig materializedViewConfig;
  private final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;
  private final DecodingWorkScheduler decodingWorkScheduler;
  private final InitialSyncConfig initialSyncConfig;
  private final MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog;
  private final LeaseManager leaseManager;

  // These fields are initialized in updateSyncSourceConfig(), owned by
  // MaterializedViewGeneratorFactory
  private boolean initialized;
  private Map<String, ClientSessionRecord> clientSessionRecordMap;
  private Optional<BatchMongoClient> syncBatchMongoClient;
  private Optional<InitialSyncQueue> initialSyncQueue;
  private Optional<SteadyStateManager> steadyStateManager;

  // Wired exactly once by the owning MaterializedViewManager's constructor (the factory is built
  // before the manager exists); every generator created afterwards reports its source-collection
  // drops through this handler. Empty until wired, and create() refuses to run without it.
  private Optional<Consumer<MaterializedViewIndexGeneration>> onSourceCollectionDropped =
      Optional.empty();

  void setOnSourceCollectionDropped(Consumer<MaterializedViewIndexGeneration> handler) {
    checkState(
        this.onSourceCollectionDropped.isEmpty(), "onSourceCollectionDropped is already wired");
    this.onSourceCollectionDropped = Optional.of(handler);
  }

  MaterializedViewGeneratorFactory(
      Path resolvedPath,
      NamedExecutorService lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncConfig initialSyncConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      FeatureFlags featureFlags,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      DecodingWorkScheduler decodingWorkScheduler,
      MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
      LeaseManager leaseManager) {
    this.resolvedPath = resolvedPath;
    this.lifecycleExecutor = lifecycleExecutor;
    this.cursorManager = cursorManager;
    this.meterAndFtdcRegistry = meterAndFtdcRegistry;
    this.featureFlags = featureFlags;
    this.enableNaturalOrderScan = initialSyncConfig.enableNaturalOrderScan();
    this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
    this.decodingWorkScheduler = decodingWorkScheduler;
    this.initialSyncConfig = initialSyncConfig;
    this.matViewMetadataCatalog = matViewMetadataCatalog;
    this.materializedViewConfig = materializedViewConfig;
    this.leaseManager = leaseManager;
    this.resyncBackoff =
        materializedViewConfig.resyncBackoff.orElse(ReplicationIndexManager.DEFAULT_RESYNC_BACKOFF);
    this.transientBackoff =
        materializedViewConfig.transientBackoff.orElse(
            ReplicationIndexManager.DEFAULT_TRANSIENT_BACKOFF);
    this.requestRateLimitBackoffMs =
        Duration.ofMillis(materializedViewConfig.requestRateLimitBackoffMs);
    // Sets to empty values. updateSyncSourceConfig() will be called later to initialize them.
    this.initialized = false;
    this.clientSessionRecordMap = new HashMap<>();
    this.syncBatchMongoClient = Optional.empty();
    this.initialSyncQueue = Optional.empty();
    this.steadyStateManager = Optional.empty();
  }

  /**
   * Creates a MaterializedViewGenerator in follower mode. Call {@link
   * MaterializedViewGenerator#becomeLeader()} on the returned generator to activate leader mode and
   * start the replication loop.
   */
  MaterializedViewGenerator create(MaterializedViewIndexGeneration matViewIndexGeneration) {
    InitializedMaterializedViewIndex matViewIndex = matViewIndexGeneration.getIndex();
    Consumer<MaterializedViewIndexGeneration> sourceCollectionDropped =
        Check.isPresent(this.onSourceCollectionDropped, "onSourceCollectionDropped");
    DocumentIndexer indexer =
        new TenureFencedMaterializedViewDocumentIndexer(
            matViewIndex, this.leaseManager, matViewIndexGeneration.getGenerationId());
    // TODO(CLOUDP-361153): Remove this or replace this as our customized committer.
    PeriodicIndexCommitter committer = PeriodicIndexCommitter.createInactive(matViewIndex, indexer);
    return MaterializedViewGenerator.create(
        this.lifecycleExecutor,
        this.cursorManager,
        Check.isPresent(this.initialSyncQueue, "initialSyncQueue"),
        Check.isPresent(this.steadyStateManager, "steadyStateManager"),
        matViewIndexGeneration,
        matViewIndex,
        indexer,
        committer,
        this.resyncBackoff,
        this.transientBackoff,
        this.requestRateLimitBackoffMs,
        this.meterAndFtdcRegistry.meterRegistry(),
        this.featureFlags,
        this.enableNaturalOrderScan,
        () -> sourceCollectionDropped.accept(matViewIndexGeneration));
  }

  boolean isInitialized() {
    return this.initialized;
  }

  void markUninitialized() {
    this.initialized = false;
  }

  /** Returns true if the InitialSyncQueue has an entry for this generationId. */
  boolean hasQueueEntry(GenerationId generationId) {
    return this.initialSyncQueue.map(q -> q.hasEntry(generationId)).orElse(false);
  }

  void updateSyncSourceConfig(SyncSourceConfig syncSourceConfig) {
    syncSourceConfig.validateReplicationUrisAvailable();

    var meterRegistry = this.meterAndFtdcRegistry.meterRegistry();
    var sessionRefreshExecutor =
        Executors.singleThreadScheduledExecutor("auto-embedding-session-refresh", meterRegistry);

    var syncSourceHost = getSyncSourceHost(syncSourceConfig);

    this.clientSessionRecordMap =
        getClientSessionRecords(
            syncSourceConfig,
            getSyncMaxConnections(this.materializedViewConfig),
            AUTO_EMBEDDING,
            meterRegistry,
            sessionRefreshExecutor,
            syncSourceHost);

    var syncMongoClient = this.clientSessionRecordMap.get(syncSourceHost).syncMongoClient();
    var sessionRefresher = this.clientSessionRecordMap.get(syncSourceHost).sessionRefresher();

    this.syncBatchMongoClient =
        Optional.of(
            getSyncBatchMongoClient(
                syncSourceConfig,
                this.materializedViewConfig.numConcurrentChangeStreams,
                AUTO_EMBEDDING.metricsNamespacePrefix,
                meterRegistry));

    this.steadyStateManager =
        Optional.of(
            SteadyStateManager.create(
                this.meterAndFtdcRegistry,
                sessionRefresher,
                this.indexingWorkSchedulerFactory,
                syncMongoClient,
                this.syncBatchMongoClient.get(),
                this.decodingWorkScheduler,
                getAutoEmbeddingSteadyStateReplicationConfig(this.materializedViewConfig),
                this.featureFlags.isEnabled(Feature.LIFECYCLE_ATTRIBUTION_METRICS),
                this.featureFlags.isEnabled(Feature.CHANGE_STREAM_PRIORITY_SCHEDULING),
                // The change-stream activity observer covers DEFAULT replication only.
                false));

    this.initialSyncQueue =
        Optional.of(
            InitialSyncQueue.create(
                meterRegistry,
                this.clientSessionRecordMap,
                syncSourceHost,
                this.indexingWorkSchedulerFactory,
                this.materializedViewConfig,
                this.initialSyncConfig,
                /* This path should be different from the dataPath used in Lucene */
                this.resolvedPath,
                ToggleGate.opened(),
                this.featureFlags,
                Optional.of(this.matViewMetadataCatalog)));
    this.initialized = true;
  }

  /**
   * Closes the current replication clients without waiting for generators to unwind. Closing a
   * client is the only reliable way to interrupt a synchronous driver read on an unhealthy sync
   * source, so this must happen before waiting for the generators that own those reads. This
   * follows the abort-before-wait convention in {@link
   * com.xgen.mongot.replication.mongodb.MongoDbReplicationManager}.
   */
  void abortClients() {
    checkState(
        !this.initialized, "abortClients must be called after shutdown marks the factory idle");
    this.clientSessionRecordMap
        .values()
        .forEach(
            clientSessionRecord -> {
              clientSessionRecord.sessionRefresher().markStopping();
              clientSessionRecord.syncMongoClient().close();
            });
    this.syncBatchMongoClient.ifPresent(BatchMongoClient::close);
  }

  public CompletionStage<Void> shutdown() {
    // Capture current state before clearing fields. shutdown() is invoked asynchronously (off
    // the MaterializedViewManager lock). Callers MUST block on the returned future before
    // calling updateSyncSourceConfig(); otherwise a concurrent updateSyncSourceConfig() could
    // repopulate these fields and this shutdown chain would capture and close the new resources.
    // Captured locals are private to this shutdown chain. Async callbacks MUST NOT reference
    // this.X for the same reason.
    var sessionRecordsToClose = this.clientSessionRecordMap;
    var batchClientToClose = this.syncBatchMongoClient;
    var initialSyncQueueFuture =
        this.initialSyncQueue.map(InitialSyncQueue::shutdown).orElse(COMPLETED_FUTURE);
    var steadyStateFuture =
        this.steadyStateManager.map(SteadyStateManager::shutdown).orElse(COMPLETED_FUTURE);

    // Clear fields synchronously so any re-initialization starts from a clean slate.
    this.clientSessionRecordMap = new HashMap<>();
    this.syncBatchMongoClient = Optional.empty();
    this.steadyStateManager = Optional.empty();
    this.initialSyncQueue = Optional.empty();

    return CompletableFuture.allOf(initialSyncQueueFuture, steadyStateFuture)
        .whenCompleteAsync(
            (result, throwable) ->
                sessionRecordsToClose
                    .values()
                    .forEach(
                        clientSessionRecord -> {
                          clientSessionRecord.sessionRefresher().shutdown();
                          clientSessionRecord.syncMongoClient().close();
                        }))
        .whenCompleteAsync(
            (result, throwable) -> batchClientToClose.ifPresent(BatchMongoClient::close));
  }

  /**
   * Creates a SteadyStateReplicationConfig for auto-embedding indexes with INDEXED_FIELDS mode
   * enforced. Auto-embedding indexes always have well-defined field mappings, so projection is
   * always applicable. This eliminates unnecessary IO from non-indexed field updates.
   */
  private static SteadyStateReplicationConfig getAutoEmbeddingSteadyStateReplicationConfig(
      AutoEmbeddingMaterializedViewConfig materializedViewConfig) {
    return SteadyStateReplicationConfig.builder()
        .setNumConcurrentChangeStreams(materializedViewConfig.numConcurrentChangeStreams)
        .setChangeStreamQueryMaxTimeMs(materializedViewConfig.changeStreamMaxTimeMs)
        .setChangeStreamCursorMaxTimeSec(materializedViewConfig.changeStreamCursorMaxTimeSec)
        .setEnableChangeStreamProjection(Optional.of(true)) // Force INDEXED_FIELDS mode
        .setMaxInFlightEmbeddingGetMores(materializedViewConfig.maxInFlightEmbeddingGetMores)
        .setEmbeddingGetMoreBatchSize(materializedViewConfig.embeddingGetMoreBatchSize)
        .setExcludedChangestreamFields(materializedViewConfig.getExcludedChangestreamFields())
        .setMatchCollectionUuidForUpdateLookup(
            materializedViewConfig.getMatchCollectionUuidForUpdateLookup())
        .setEnableSplitLargeChangeStreamEvents(
            materializedViewConfig.getEnableSplitLargeChangeStreamEvents())
        .setAutoEmbeddingMaterializedViewConfig(materializedViewConfig)
        .setReplicationType(AUTO_EMBEDDING)
        .build();
  }

  private static int getSyncMaxConnections(AutoEmbeddingMaterializedViewConfig replicationConfig) {
    int initialSyncConnections = (2 * replicationConfig.getNumConcurrentInitialSyncs());
    int sessionRefreshConnections = 1;
    int changeStreamModeSelectionConnections = 1;

    return initialSyncConnections
        + sessionRefreshConnections
        + changeStreamModeSelectionConnections;
  }
}
