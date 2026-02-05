package com.xgen.mongot.replication.mongodb;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.replication.mongodb.common.ClientSessionRecord;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DefaultDocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.DefaultSessionRefresher;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.common.ReplicationOptimeUpdater;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.SteadyStateReplicationConfig;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymManager;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.Runtime;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import com.xgen.mongot.util.mongodb.MongoClientBuilder;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One instance created at startup. */
public class MongoDbReplicationManager implements ReplicationManager {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbReplicationManager.class);

  public static final String REPLICATION_INDEX_MANAGER_STATE = "indexManagerState";

  public static final String STATE_LABEL = "state";

  /**
   * The Executor that is used by steady state indexing, as well as the ReplicationIndexManager for
   * scheduling its lifecycle tasks.
   *
   * <p>The NamedExecutorService is owned by this MongoDbReplicationManager.
   */
  private final NamedExecutorService lifecycleExecutor;

  private final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;

  private final MongotCursorManager cursorManager;

  /** Hostname, ClientSessionRecord mapping */
  private final Map<String, ClientSessionRecord> clientSessionRecordMap;

  private final Optional<SyncSourceConfig> syncSourceConfig;

  /**
   * The InitialSyncQueue to be used by IndexLifecycleManagers.
   *
   * <p>The InitialSyncQueue is owned by this MongoDbReplicationManager.
   */
  private final InitialSyncQueue initialSyncQueue;

  /**
   * The SteadyStateManager to be used by IndexLifecycleManagers.
   *
   * <p>The SteadyStateManager is owned by this MongoDbReplicationManager.
   */
  private final SteadyStateManager steadyStateManager;

  /**
   * The SynonymManager to be used by IndexLifecycleManagers.
   *
   * <p>The SynonymManager is owned by this MongoDbReplicationManager.
   */
  private final SynonymManager synonymManager;

  private final Optional<MongoClient> synonymsSyncClient;

  private final Optional<? extends SessionRefresher> synonymsSessionRefresher;

  private final BatchMongoClient syncBatchMongoClient;

  private final DecodingWorkScheduler decodingWorkScheduler;

  private final ReplicationIndexManagerFactory replicationIndexManagerFactory;

  private final MeterRegistry meterRegistry;

  /** A mapping of existing IndexLifecycleManagers. */
  @GuardedBy("this")
  private final Map<GenerationId, ReplicationIndexManager> indexManagers;

  private final NamedScheduledExecutorService commitExecutor;

  private final Duration commitInterval;

  private final ReplicationOptimeUpdater replicationOptimeUpdater;

  // TODO(CLOUDP-231027): Remove after separating from LifecycleManager.
  private final InitializedIndexCatalog initializedIndexCatalog;

  private final MetricsFactory metricsFactory;

  private final AtomicLong managerUp;

  @GuardedBy("this")
  private boolean shutdown;

  private final FeatureFlags featureFlags;

  private final Duration requestRateLimitBackoffDuration;

  /** A flag indicating whether natural order scan is enabled for initial sync */
  private final boolean enableNaturalOrderScan;

  @VisibleForTesting
  MongoDbReplicationManager(
      NamedExecutorService lifecycleExecutor,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      MongotCursorManager cursorManager,
      Map<String, ClientSessionRecord> clientSessionRecordMap,
      Optional<SyncSourceConfig> syncSourceConfig,
      FeatureFlags featureFlags,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      SynonymManager synonymManager,
      BatchMongoClient syncBatchMongoClient,
      DecodingWorkScheduler decodingWorkScheduler,
      Optional<MongoClient> synonymsSyncClient,
      Optional<? extends SessionRefresher> synonymsSessionRefresher,
      ReplicationIndexManagerFactory replicationIndexManagerFactory,
      MeterRegistry meterRegistry,
      Map<GenerationId, ReplicationIndexManager> indexManagers,
      NamedScheduledExecutorService commitExecutor,
      ReplicationOptimeUpdater replicationOptimeUpdater,
      InitializedIndexCatalog initializedIndexCatalog,
      Duration commitInterval,
      Duration requestRateLimitBackoffDuration,
      boolean enableNaturalOrderScan) {
    this.lifecycleExecutor = lifecycleExecutor;
    this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
    this.cursorManager = cursorManager;
    this.clientSessionRecordMap = clientSessionRecordMap;
    this.initialSyncQueue = initialSyncQueue;
    this.steadyStateManager = steadyStateManager;
    this.synonymManager = synonymManager;
    this.syncBatchMongoClient = syncBatchMongoClient;
    this.decodingWorkScheduler = decodingWorkScheduler;
    this.synonymsSyncClient = synonymsSyncClient;
    this.synonymsSessionRefresher = synonymsSessionRefresher;
    this.replicationIndexManagerFactory = replicationIndexManagerFactory;
    this.meterRegistry = meterRegistry;
    this.indexManagers = indexManagers;
    this.commitExecutor = commitExecutor;
    this.commitInterval = commitInterval;
    this.syncSourceConfig = syncSourceConfig;
    this.featureFlags = featureFlags;
    this.replicationOptimeUpdater = replicationOptimeUpdater;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.requestRateLimitBackoffDuration = requestRateLimitBackoffDuration;
    this.enableNaturalOrderScan = enableNaturalOrderScan;
    this.shutdown = false;
    this.metricsFactory = new MetricsFactory("replication.mongodb", meterRegistry);
    createStateGauges(this, this.metricsFactory);
    this.managerUp = this.metricsFactory.numGauge("manager", Tags.of("type", "normal"));
    this.managerUp.incrementAndGet();
  }

  /** Creates a new MongoDbReplicationManager. */
  @VisibleForTesting
  public static MongoDbReplicationManager create(
      Path dataPath,
      Gate initialSyncGate,
      Optional<SyncSourceConfig> syncSourceConfig,
      MongoDbReplicationConfig replicationConfig,
      DurabilityConfig durabilityConfig,
      InitialSyncConfig initialSyncConfig,
      FeatureFlags featureFlags,
      MongotCursorManager cursorManager,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      Duration replicationOptimeUpdaterInterval,
      BatchMongoClient syncBatchMongoClient,
      Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier) {
    if (syncSourceConfig.isEmpty()) {
      throw new IllegalArgumentException("syncSourceConfig must be provided");
    }
    LOG.info("creating MongoDbReplicationManager");
    var meterRegistry = meterAndFtdcRegistry.meterRegistry();
    meterRegistry.gauge("replication.manager", 1);
    var lifecycleExecutor =
        Executors.fixedSizeThreadPool(
            "indexing-lifecycle", Math.max(1, Runtime.INSTANCE.getNumCpus() / 4), meterRegistry);

    var indexingWorkSchedulerFactory =
        embeddingServiceManagerSupplier
            .map(
                supplier ->
                    IndexingWorkSchedulerFactory.create(
                        replicationConfig.numIndexingThreads, supplier, meterRegistry))
            .orElseGet(
                () ->
                    IndexingWorkSchedulerFactory.createWithoutEmbeddingStrategy(
                        replicationConfig.numIndexingThreads, meterRegistry));

    var decodingWorkScheduler =
        DecodingWorkScheduler.create(
            replicationConfig.numChangeStreamDecodingThreads, meterRegistry);

    var sessionRefreshExecutor =
        Executors.singleThreadScheduledExecutor("session-refresh", meterRegistry);

    // There should only be one sync source host
    var syncSourceHost = getSyncSourceHost(syncSourceConfig.get());

    var clientSessionRecords =
        getClientSessionRecords(
            syncSourceConfig.get(),
            getSyncMaxConnections(syncSourceConfig.get(), replicationConfig),
            meterRegistry,
            sessionRefreshExecutor,
            syncSourceHost);

    var syncMongoClient = clientSessionRecords.get(syncSourceHost).syncMongoClient();
    var sessionRefresher = clientSessionRecords.get(syncSourceHost).sessionRefresher();
    // create mongos client/session refresher if mongosUri is provided, otherwise use mongod client
    var synonymsMongoClient =
        syncSourceConfig
            .get()
            .mongosUri
            .map(
                connectionString ->
                    getSynonymsMongoClient(
                        connectionString,
                        syncSourceConfig.get().sslContext,
                        replicationConfig.numConcurrentSynonymSyncs,
                        meterRegistry));

    var synonymsSessionRefresher =
        synonymsMongoClient.map(
            client ->
                DefaultSessionRefresher.create(meterRegistry, sessionRefreshExecutor, client));

    var initialSyncQueue =
        InitialSyncQueue.create(
            meterRegistry,
            clientSessionRecords,
            syncSourceHost,
            indexingWorkSchedulerFactory,
            replicationConfig,
            initialSyncConfig,
            dataPath,
            initialSyncGate);

    SteadyStateReplicationConfig steadyStateReplicationConfig =
        getSteadyStateReplicationConfig(replicationConfig);

    var steadyStateManager =
        SteadyStateManager.create(
            meterAndFtdcRegistry,
            sessionRefresher,
            indexingWorkSchedulerFactory,
            syncMongoClient,
            syncBatchMongoClient,
            decodingWorkScheduler,
            steadyStateReplicationConfig);

    var synonymManager =
        SynonymManager.create(
            synonymsMongoClient.isPresent(),
            synonymsMongoClient.orElse(syncMongoClient),
            synonymsSessionRefresher.orElse((DefaultSessionRefresher) sessionRefresher),
            meterRegistry,
            replicationConfig.numConcurrentSynonymSyncs);

    var commitExecutor =
        Executors.fixedSizeThreadScheduledExecutor(
            "index-commit", durabilityConfig.numCommittingThreads, meterRegistry);

    var replicationOptimeMetricUpdater =
        ReplicationOptimeUpdater.create(
            indexCatalog,
            initializedIndexCatalog,
            syncSourceConfig,
            replicationOptimeUpdaterInterval,
            meterRegistry);

    return new MongoDbReplicationManager(
        lifecycleExecutor,
        indexingWorkSchedulerFactory,
        cursorManager,
        clientSessionRecords,
        syncSourceConfig,
        featureFlags,
        initialSyncQueue,
        steadyStateManager,
        synonymManager,
        syncBatchMongoClient,
        decodingWorkScheduler,
        synonymsMongoClient,
        synonymsSessionRefresher,
        ReplicationIndexManager::create,
        meterRegistry,
        new ConcurrentHashMap<>(), // ConcurrentHashMap to make copy without locks
        commitExecutor,
        replicationOptimeMetricUpdater,
        initializedIndexCatalog,
        durabilityConfig.commitInterval,
        Duration.ofMillis(replicationConfig.requestRateLimitBackoffMs),
        initialSyncConfig.enableNaturalOrderScan());
  }

  @VisibleForTesting
  public static MongoDbReplicationManager create(
      Path dataPath,
      Gate initialSyncGate,
      Optional<SyncSourceConfig> syncSourceConfig,
      MongoDbReplicationConfig replicationConfig,
      DurabilityConfig durabilityConfig,
      InitialSyncConfig initialSyncConfig,
      FeatureFlags featureFlags,
      MongotCursorManager cursorManager,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      Duration replicationOptimeUpdaterInterval,
      Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier) {
    if (syncSourceConfig.isEmpty()) {
      throw new IllegalArgumentException("syncSourceConfig must be provided");
    }

    var meterRegistry = meterAndFtdcRegistry.meterRegistry();

    var syncBatchMongoClient =
        getSyncBatchMongoClient(
            syncSourceConfig.get(), replicationConfig.numConcurrentChangeStreams, meterRegistry);

    return create(
        dataPath,
        initialSyncGate,
        syncSourceConfig,
        replicationConfig,
        durabilityConfig,
        initialSyncConfig,
        featureFlags,
        cursorManager,
        indexCatalog,
        initializedIndexCatalog,
        meterAndFtdcRegistry,
        replicationOptimeUpdaterInterval,
        syncBatchMongoClient,
        embeddingServiceManagerSupplier);
  }

  public static MongoDbReplicationManager create(
      Path dataPath,
      Optional<SyncSourceConfig> syncSourceConfig,
      MongoDbReplicationConfig replicationConfig,
      DurabilityConfig durabilityConfig,
      InitialSyncConfig initialSyncConfig,
      FeatureFlags featureFlags,
      MongotCursorManager cursorManager,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      Gate initialSyncGate,
      Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier) {
    return create(
        dataPath,
        initialSyncGate,
        syncSourceConfig,
        replicationConfig,
        durabilityConfig,
        initialSyncConfig,
        featureFlags,
        cursorManager,
        indexCatalog,
        initializedIndexCatalog,
        meterAndFtdcRegistry,
        ReplicationOptimeUpdater.DEFAULT_UPDATE_INTERVAL,
        embeddingServiceManagerSupplier);
  }

  /** Creates gauges to track the number of index replication managers by state */
  private static void createStateGauges(
      MongoDbReplicationManager mongoDbReplicationManager, MetricsFactory metricsFactory) {
    Arrays.stream(ReplicationIndexManager.State.values())
        .forEach(
            state ->
                metricsFactory.objectValueGauge(
                    REPLICATION_INDEX_MANAGER_STATE,
                    mongoDbReplicationManager,
                    manager -> manager.gaugeReplicationManagers(state),
                    Tags.of(STATE_LABEL, state.name())));
  }

  // lock is not needed because we iterate through a copy of managers
  private double gaugeReplicationManagers(ReplicationIndexManager.State state) {
    return this.getIndexManagers().entrySet().stream()
        .filter(m -> m.getValue().getState() == state)
        .count();
  }

  public static com.mongodb.client.MongoClient getSyncMongoClient(
      SyncSourceConfig syncSourceConfig,
      MeterRegistry meterRegistry,
      ConnectionString uri,
      int maxConnections) {
    return MongoClientBuilder.builder(uri, meterRegistry)
        .sslContext(syncSourceConfig.sslContext)
        .description("initial sync and session refresh")
        .maxConnections(maxConnections)
        .buildSyncClient();
  }

  /** Creates (host, ClientSessionRecord) mapping from syncSourceConfig */
  public static Map<String, ClientSessionRecord> getClientSessionRecords(
      SyncSourceConfig syncSourceConfig,
      int maxConnections,
      MeterRegistry meterRegistry,
      NamedScheduledExecutorService sessionRefreshExecutor,
      String syncSourceHost) {
    LOG.atInfo().addKeyValue("defaultHost", syncSourceHost).log("start constructing mongoClients");

    // make sure syncClient and session refresher connecting mongodUri is included
    var syncMongoClient =
        getSyncMongoClient(
            syncSourceConfig, meterRegistry, syncSourceConfig.mongodUri, maxConnections);
    var sessionRefresher =
        DefaultSessionRefresher.create(meterRegistry, sessionRefreshExecutor, syncMongoClient);
    Map<String, ClientSessionRecord> clientSessionHostMap = new HashMap<>();
    clientSessionHostMap.put(
        syncSourceHost, new ClientSessionRecord(syncMongoClient, sessionRefresher));

    // construct other mongoClients and session refreshers
    syncSourceConfig.mongodUris.ifPresent(
        uris ->
            uris.forEach(
                (host, connectionString) -> {
                  if (!clientSessionHostMap.containsKey(host)) {
                    var client =
                        getSyncMongoClient(
                            syncSourceConfig, meterRegistry, connectionString, maxConnections);
                    var refresher =
                        DefaultSessionRefresher.create(
                            meterRegistry, sessionRefreshExecutor, client);
                    clientSessionHostMap.put(host, new ClientSessionRecord(client, refresher));
                  }
                }));
    return clientSessionHostMap;
  }

  static int getSyncMaxConnections(
      SyncSourceConfig syncSourceConfig, MongoDbReplicationConfig replicationConfig) {
    int initialSyncConnections = (2 * replicationConfig.numConcurrentInitialSyncs);
    // synonym syncs do not use this client when mongosUri exists
    int synonymSyncConnections =
        syncSourceConfig.mongosUri.isPresent() ? 0 : replicationConfig.numConcurrentSynonymSyncs;
    int sessionRefreshConnections = 1;
    int changeStreamModeSelectionConnections = 1;

    return initialSyncConnections
        + synonymSyncConnections
        + sessionRefreshConnections
        + changeStreamModeSelectionConnections;
  }

  public static String getSyncSourceHost(SyncSourceConfig syncSourceConfig) {
    Optional<String> hostName =
        syncSourceConfig.mongodUris.flatMap(
            map ->
                map.entrySet().stream()
                    .filter(e -> syncSourceConfig.mongodUri.equals(e.getValue()))
                    .map(Map.Entry::getKey)
                    .findFirst());

    if (hostName.isEmpty()) {
      // There should only be one host from mongodUri for Atlas that's using a direct connection for
      // initial sync
      String host = syncSourceConfig.mongodUri.getHosts().getFirst();
      // return the host name excluding port.
      return host.split(":")[0];
    }

    return hostName.get();
  }

  private static com.mongodb.client.MongoClient getSynonymsMongoClient(
      ConnectionString connectionString,
      Optional<SSLContext> sslContext,
      int numConcurrentSynonymSyncs,
      MeterRegistry meterRegistry) {
    return MongoClientBuilder.builder(connectionString, meterRegistry)
        .sslContext(sslContext)
        .description("mongos synonym sync")
        .maxConnections(numConcurrentSynonymSyncs)
        .buildSyncClient();
  }

  public static BatchMongoClient getSyncBatchMongoClient(
      SyncSourceConfig syncSourceConfig,
      int numConcurrentChangeStreams,
      MeterRegistry meterRegistry) {
    return MongoClientBuilder.builder(syncSourceConfig.mongodClusterUri, meterRegistry)
        .sslContext(syncSourceConfig.sslContext)
        .description("steady state sync")
        .maxConnections(numConcurrentChangeStreams)
        .buildSyncBatchClient();
  }

  public static SteadyStateReplicationConfig getSteadyStateReplicationConfig(
      MongoDbReplicationConfig replicationConfig) {
    return SteadyStateReplicationConfig.builder()
        .setNumConcurrentChangeStreams(replicationConfig.numConcurrentChangeStreams)
        .setChangeStreamQueryMaxTimeMs(replicationConfig.changeStreamMaxTimeMs)
        .setChangeStreamCursorMaxTimeSec(replicationConfig.changeStreamCursorMaxTimeSec)
        .setEnableChangeStreamProjection(replicationConfig.enableSteadyStateChangeStreamProjection)
        .setMaxInFlightEmbeddingGetMores(replicationConfig.maxInFlightEmbeddingGetMores)
        .setEmbeddingGetMoreBatchSize(replicationConfig.embeddingGetMoreBatchSize)
        .setExcludedChangestreamFields(replicationConfig.getExcludedChangestreamFields())
        .setMatchCollectionUuidForUpdateLookup(
            replicationConfig.getMatchCollectionUuidForUpdateLookup())
        .setEnableSplitLargeChangeStreamEvents(
            replicationConfig.getEnableSplitLargeChangeStreamEvents())
        .build();
  }

  @Override
  public Optional<SyncSourceConfig> getSyncSourceConfig() {
    return this.syncSourceConfig;
  }

  @Override
  public synchronized boolean isInitialized() {
    return this.indexManagers.values().stream()
        .map(ReplicationIndexManager::getInitFuture)
        .allMatch(initFuture -> initFuture.isDone() && !initFuture.isCompletedExceptionally());
  }

  @Override
  public synchronized void add(IndexGeneration indexGeneration) {
    checkState(!this.shutdown, "cannot call add() after shutdown()");
    Optional<InitializedIndex> initializedIndex =
        this.initializedIndexCatalog.getIndex(indexGeneration.getGenerationId());
    checkState(
        initializedIndex.isPresent(),
        "Index: %s not initialized, cannot replicate",
        indexGeneration.getGenerationId());
    GenerationId generationId = indexGeneration.getGenerationId();
    if (this.indexManagers.containsKey(generationId)) {
      LOG.warn("Cannot add index {} as it has already been added", generationId);
      return;
    }

    Index index = indexGeneration.getIndex();
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(initializedIndex.get());
    PeriodicIndexCommitter committer =
        new PeriodicIndexCommitter(index, indexer, this.commitExecutor, this.commitInterval);

    ReplicationIndexManager indexManager =
        this.replicationIndexManagerFactory.create(
            this.lifecycleExecutor,
            this.cursorManager,
            this.initialSyncQueue,
            this.steadyStateManager,
            Optional.of(this.synonymManager),
            indexGeneration,
            initializedIndex.get(),
            indexer,
            committer,
            this.requestRateLimitBackoffDuration,
            this.meterRegistry,
            this.featureFlags,
            this.enableNaturalOrderScan);

    this.indexManagers.put(generationId, indexManager);
  }

  @Override
  public synchronized CompletableFuture<Void> dropIndex(GenerationId generationId) {
    checkState(!this.shutdown, "cannot call stopReplication() after shutdown()");
    if (!this.indexManagers.containsKey(generationId)) {
      LOG.warn("Cannot drop index {} because it is not already added.", generationId);
      return CompletableFuture.completedFuture(null);
    }

    ReplicationIndexManager indexManager = this.indexManagers.remove(generationId);
    return indexManager.drop();
  }

  @Override
  public synchronized CompletableFuture<Void> shutdown() {
    LOG.info("Shutting down.");

    this.shutdown = true;

    List<CompletableFuture<?>> futures =
        this.indexManagers.values().stream()
            .map(ReplicationIndexManager::shutdown)
            .collect(Collectors.toList());

    // Need to create a separate executor to run the shutdown tasks, otherwise it may end up running
    // on the indexing executor. As one of the shutdown tasks is shutting down that executor, this
    // will hang forever.
    var shutdownExecutor =
        Executors.fixedSizeThreadPool("replication-manager-shutdown", 1, this.meterRegistry);

    // Only shutdown the executor service after all the tasks complete to avoid race condition
    // where metrics are deregistered before all tasks finish.
    return FutureUtils.allOf(futures)
        .thenComposeAsync(
            ignored ->
                CompletableFuture.allOf(
                    this.initialSyncQueue.shutdown(),
                    this.steadyStateManager.shutdown(),
                    this.synonymManager.shutdown()),
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
        .thenRunAsync(
            () -> this.synonymsSessionRefresher.ifPresent(SessionRefresher::shutdown),
            shutdownExecutor)
        .thenRunAsync(() -> this.synonymsSyncClient.ifPresent(MongoClient::close), shutdownExecutor)
        .thenRunAsync(this.decodingWorkScheduler::shutdown, shutdownExecutor)
        .thenRunAsync(
            () ->
                this.indexingWorkSchedulerFactory
                    .getIndexingWorkSchedulers()
                    .forEach((strategy, scheduler) -> scheduler.shutdown()),
            shutdownExecutor)
        .thenRunAsync(() -> Executors.shutdownOrFail(this.commitExecutor), shutdownExecutor)
        .thenRunAsync(() -> Executors.shutdownOrFail(this.lifecycleExecutor), shutdownExecutor)
        .thenRunAsync(this.replicationOptimeUpdater::close, shutdownExecutor)
        .thenRunAsync(this::deregisterGauges, shutdownExecutor)
        .whenComplete((result, throwable) -> shutdownExecutor.shutdown());
  }

  @Override
  public boolean isReplicationSupported() {
    return true;
  }

  @VisibleForTesting
  synchronized ReplicationIndexManager getReplicationIndexManager(IndexGeneration indexGeneration) {
    return this.indexManagers.get(indexGeneration.getGenerationId());
  }

  /** Creates a copy of {@link MongoDbReplicationManager#indexManagers}. Thread safe method. */
  @SuppressWarnings("GuardedBy") // iterations through ConcurrentHashMap (copying) are thread safe
  private Map<GenerationId, ReplicationIndexManager> getIndexManagers() {
    return new HashMap<>(this.indexManagers);
  }

  private void deregisterGauges() {
    this.managerUp.decrementAndGet();
    this.metricsFactory.close();
  }
}
