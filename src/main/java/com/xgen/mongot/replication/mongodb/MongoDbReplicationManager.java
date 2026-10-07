package com.xgen.mongot.replication.mongodb;

import static com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig.Type.DEFAULT;
import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.autoembedding.DerivedLocalIndexGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.replication.mongodb.common.ClientSessionRecord;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DefaultDocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.DefaultSessionRefresher;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.ReplicationOptimeUpdater;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.SteadyStateReplicationConfig;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymManager;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.Optionals;
import com.xgen.mongot.util.Runtime;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import com.xgen.mongot.util.mongodb.ConnectionInfo;
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
  private final Map<GenerationId, IndexManager> indexManagers;

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

  /**
   * Wires a {@link RecyclableReplicationIndexManager}'s leadership sink to its native engine. Empty
   * in builds without a custom vector engine (e.g. community).
   */
  private final Optional<LeaderStatusRegistrar> leaderStatusRegistrar;

  /** private constructor - Use static factory methods to construct instances. */
  private MongoDbReplicationManager(
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
      Map<GenerationId, IndexManager> indexManagers,
      NamedScheduledExecutorService commitExecutor,
      ReplicationOptimeUpdater replicationOptimeUpdater,
      InitializedIndexCatalog initializedIndexCatalog,
      Duration commitInterval,
      Duration requestRateLimitBackoffDuration,
      boolean enableNaturalOrderScan,
      Optional<LeaderStatusRegistrar> leaderStatusRegistrar) {
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
    this.leaderStatusRegistrar = leaderStatusRegistrar;
    this.shutdown = false;
    this.metricsFactory = new MetricsFactory("replication.mongodb", meterRegistry);
    this.managerUp = this.metricsFactory.numGauge("manager", Tags.of("type", "normal"));
    this.managerUp.incrementAndGet();
  }

  @VisibleForTesting
  static MongoDbReplicationManager create(
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
      Map<GenerationId, IndexManager> indexManagers,
      NamedScheduledExecutorService commitExecutor,
      ReplicationOptimeUpdater replicationOptimeUpdater,
      InitializedIndexCatalog initializedIndexCatalog,
      Duration commitInterval,
      Duration requestRateLimitBackoffDuration,
      boolean enableNaturalOrderScan,
      Optional<LeaderStatusRegistrar> leaderStatusRegistrar) {
    MongoDbReplicationManager manager =
        new MongoDbReplicationManager(
            lifecycleExecutor,
            indexingWorkSchedulerFactory,
            cursorManager,
            clientSessionRecordMap,
            syncSourceConfig,
            featureFlags,
            initialSyncQueue,
            steadyStateManager,
            synonymManager,
            syncBatchMongoClient,
            decodingWorkScheduler,
            synonymsSyncClient,
            synonymsSessionRefresher,
            replicationIndexManagerFactory,
            meterRegistry,
            indexManagers,
            commitExecutor,
            replicationOptimeUpdater,
            initializedIndexCatalog,
            commitInterval,
            requestRateLimitBackoffDuration,
            enableNaturalOrderScan,
            leaderStatusRegistrar);

    // Register gauges after construction is complete
    createStateGauges(manager, manager.metricsFactory);

    return manager;
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
      Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier,
      Optional<LeaderStatusRegistrar> leaderStatusRegistrar) {
    if (syncSourceConfig.isEmpty()) {
      throw new IllegalArgumentException("syncSourceConfig must be provided");
    }
    syncSourceConfig.get().validateReplicationUrisAvailable();

    LOG.info("creating MongoDbReplicationManager");
    var meterRegistry = meterAndFtdcRegistry.meterRegistry();
    meterRegistry.gauge("replication.manager", 1);
    boolean enableLifecycleAttributionMetrics =
        featureFlags.isEnabled(Feature.LIFECYCLE_ATTRIBUTION_METRICS);
    Optional<ThreadPoolResourceMetrics.Bindings> attributionBindings =
        enableLifecycleAttributionMetrics
            ? Optional.of(meterAndFtdcRegistry.threadPoolResourceBindings())
            : Optional.empty();
    boolean enableChangeStreamPriorityScheduling =
        featureFlags.isEnabled(Feature.CHANGE_STREAM_PRIORITY_SCHEDULING);
    boolean enableChangeStreamActivityObserver =
        featureFlags.isEnabled(Feature.CHANGE_STREAM_ACTIVITY_OBSERVER);

    var lifecycleExecutor =
        Executors.fixedSizeThreadPool(
            "indexing-lifecycle", Math.max(1, Runtime.INSTANCE.getNumCpus() / 4), meterRegistry);
    attributionBindings.ifPresent(
        bindings ->
            bindings.forSubsystem("replication").register(lifecycleExecutor, meterRegistry));

    var indexingWorkSchedulerFactory =
        embeddingServiceManagerSupplier
            .map(
                supplier ->
                    IndexingWorkSchedulerFactory.create(
                        replicationConfig.numIndexingThreads,
                        supplier,
                        meterRegistry,
                        attributionBindings))
            .orElseGet(
                () ->
                    IndexingWorkSchedulerFactory.createWithoutEmbeddingStrategy(
                        replicationConfig.numIndexingThreads, meterRegistry, attributionBindings));

    var decodingWorkScheduler =
        DecodingWorkScheduler.create(
            replicationConfig.numChangeStreamDecodingThreads,
            CommonReplicationConfig.Type.DEFAULT,
            meterRegistry,
            attributionBindings);

    var sessionRefreshExecutor =
        Executors.singleThreadScheduledExecutor("session-refresh", meterRegistry);
    attributionBindings.ifPresent(
        bindings ->
            bindings.forSubsystem("replication").register(sessionRefreshExecutor, meterRegistry));

    // There should only be one sync source host
    var syncSourceHost = getSyncSourceHost(syncSourceConfig.get());

    var clientSessionRecords =
        getClientSessionRecords(
            syncSourceConfig.get(),
            getSyncMaxConnections(syncSourceConfig.get(), replicationConfig),
            DEFAULT,
            meterRegistry,
            sessionRefreshExecutor,
            syncSourceHost);

    var syncMongoClient = clientSessionRecords.get(syncSourceHost).syncMongoClient();
    var sessionRefresher = clientSessionRecords.get(syncSourceHost).sessionRefresher();

    // create mongos client/session refresher if we're in a sharded environment,
    // otherwise use mongod client
    Optional<MongoClient> synonymsMongoClient;
    if (syncSourceConfig.get().isSharded) {
      ConnectionInfo mongosSingleHostUri =
          Optionals.orElseThrow(
              syncSourceConfig.get().mongosSingleHostReplicationUri,
              "mongosSingleHostReplicationUri must be set before starting up the "
                  + "MongoDbReplicationManager if we're in a sharded environment");
      synonymsMongoClient =
          Optional.of(
              getSynonymsMongoClient(
                  mongosSingleHostUri, replicationConfig.numConcurrentSynonymSyncs, meterRegistry));
    } else {
      synonymsMongoClient = Optional.empty();
    }

    var synonymsSessionRefresher =
        synonymsMongoClient.map(
            client ->
                DefaultSessionRefresher.create(
                    new MetricsFactory("replication.synonyms.sessionRefresher", meterRegistry),
                    DEFAULT,
                    sessionRefreshExecutor,
                    client));

    var initialSyncQueue =
        InitialSyncQueue.create(
            meterRegistry,
            clientSessionRecords,
            syncSourceHost,
            indexingWorkSchedulerFactory,
            replicationConfig,
            initialSyncConfig,
            dataPath,
            initialSyncGate,
            featureFlags,
            Optional.empty());

    SteadyStateReplicationConfig steadyStateReplicationConfig =
        getSteadyStateReplicationConfig(replicationConfig, featureFlags);

    var steadyStateManager =
        SteadyStateManager.create(
            meterAndFtdcRegistry,
            sessionRefresher,
            indexingWorkSchedulerFactory,
            syncMongoClient,
            syncBatchMongoClient,
            decodingWorkScheduler,
            steadyStateReplicationConfig,
            enableLifecycleAttributionMetrics,
            enableChangeStreamPriorityScheduling,
            enableChangeStreamActivityObserver);

    var synonymManager =
        SynonymManager.create(
            syncSourceConfig.get().isSharded,
            synonymsMongoClient.orElse(syncMongoClient),
            synonymsSessionRefresher.orElse((DefaultSessionRefresher) sessionRefresher),
            meterRegistry,
            replicationConfig.numConcurrentSynonymSyncs);

    var commitExecutor =
        Executors.fixedSizeThreadScheduledExecutor(
            "index-commit", durabilityConfig.numCommittingThreads, meterRegistry);
    attributionBindings.ifPresent(
        bindings -> bindings.forSubsystem("replication").register(commitExecutor, meterRegistry));

    var replicationOptimeMetricUpdater =
        ReplicationOptimeUpdater.create(
            indexCatalog,
            initializedIndexCatalog,
            syncSourceConfig,
            replicationOptimeUpdaterInterval,
            meterRegistry,
            attributionBindings);

    return MongoDbReplicationManager.create(
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
        initialSyncConfig.enableNaturalOrderScan(),
        leaderStatusRegistrar);
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
      Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier,
      Optional<LeaderStatusRegistrar> leaderStatusRegistrar) {
    if (syncSourceConfig.isEmpty()) {
      throw new IllegalArgumentException("syncSourceConfig must be provided");
    }

    var meterRegistry = meterAndFtdcRegistry.meterRegistry();

    var syncBatchMongoClient =
        getSyncBatchMongoClient(
            syncSourceConfig.get(),
            replicationConfig.numConcurrentChangeStreams,
            DEFAULT.metricsNamespacePrefix,
            meterRegistry);

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
        embeddingServiceManagerSupplier,
        leaderStatusRegistrar);
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
      Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier,
      Optional<LeaderStatusRegistrar> leaderStatusRegistrar) {
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
        embeddingServiceManagerSupplier,
        leaderStatusRegistrar);
  }

  /** Creates gauges to track the number of index replication managers by state */
  private static void createStateGauges(
      MongoDbReplicationManager mongoDbReplicationManager, MetricsFactory metricsFactory) {
    Arrays.stream(IndexManager.State.values())
        .forEach(
            state ->
                metricsFactory.objectValueGauge(
                    REPLICATION_INDEX_MANAGER_STATE,
                    mongoDbReplicationManager,
                    manager -> manager.gaugeReplicationManagers(state),
                    Tags.of(STATE_LABEL, state.name())));
  }

  // lock is not needed because we iterate through a copy of managers
  private double gaugeReplicationManagers(IndexManager.State state) {
    return this.getIndexManagers().entrySet().stream()
        .filter(m -> m.getValue().getState() == state)
        .count();
  }

  public static com.mongodb.client.MongoClient getSyncMongoClient(
      ConnectionInfo syncSource,
      String metricsNamespacePrefix,
      MeterRegistry meterRegistry,
      int maxConnections) {
    return MongoClientBuilder.builder(syncSource, metricsNamespacePrefix, meterRegistry)
        .description("initial sync and session refresh")
        .maxConnections(maxConnections)
        .buildSyncClient();
  }

  /** Creates (host, ClientSessionRecord) mapping from syncSourceConfig */
  public static Map<String, ClientSessionRecord> getClientSessionRecords(
      SyncSourceConfig syncSourceConfig,
      int maxConnections,
      CommonReplicationConfig.Type type,
      MeterRegistry meterRegistry,
      NamedScheduledExecutorService sessionRefreshExecutor,
      String syncSourceHost) {

    // validateReplicationUrisAvailable() should have caught an absent URI before we get here;
    // the orElseThrow is a defensive assertion.
    ConnectionInfo mongodUri =
        Optionals.orElseThrow(
            syncSourceConfig.mongodSingleHostReplicationUri,
            "syncSourceConfig.mongodSingleHostReplicationUri must be set before starting up"
                + " the MongoDbReplicationManager");

    LOG.atInfo().addKeyValue("defaultHost", syncSourceHost).log("start constructing mongoClients");

    var sessionRefresherMetricsFactory =
        new MetricsFactory("replication.sessionRefresher", meterRegistry);
    // make sure syncClient and session refresher connecting mongodSingleHostReplicationUri is
    // included
    var syncMongoClient =
        getSyncMongoClient(mongodUri, type.metricsNamespacePrefix, meterRegistry, maxConnections);
    var sessionRefresher =
        DefaultSessionRefresher.create(
            sessionRefresherMetricsFactory, type, sessionRefreshExecutor, syncMongoClient);
    Map<String, ClientSessionRecord> clientSessionHostMap = new HashMap<>();
    clientSessionHostMap.put(
        syncSourceHost, new ClientSessionRecord(syncMongoClient, sessionRefresher));

    // construct other mongoClients and session refreshers
    syncSourceConfig.mongodUris.ifPresent(
        uris ->
            uris.forEach(
                (host, syncSource) -> {
                  if (!clientSessionHostMap.containsKey(host)) {
                    var client =
                        getSyncMongoClient(
                            syncSource, type.metricsNamespacePrefix, meterRegistry, maxConnections);
                    var refresher =
                        DefaultSessionRefresher.create(
                            sessionRefresherMetricsFactory, type, sessionRefreshExecutor, client);
                    clientSessionHostMap.put(host, new ClientSessionRecord(client, refresher));
                  }
                }));
    return clientSessionHostMap;
  }

  static int getSyncMaxConnections(
      SyncSourceConfig syncSourceConfig, MongoDbReplicationConfig replicationConfig) {
    int initialSyncConnections = (2 * replicationConfig.numConcurrentInitialSyncs);
    // synonym syncs do not use this client when cluster is sharded
    int synonymSyncConnections =
        syncSourceConfig.isSharded ? 0 : replicationConfig.numConcurrentSynonymSyncs;
    int sessionRefreshConnections = 1;
    int changeStreamModeSelectionConnections = 1;

    return initialSyncConnections
        + synonymSyncConnections
        + sessionRefreshConnections
        + changeStreamModeSelectionConnections;
  }

  /**
   * Returns the hostname (without port) of the single mongod instance used for initial sync.
   *
   * <p>Prefers a match from {@link SyncSourceConfig#mongodUris} when available. Falls back to
   * extracting the first host from {@link SyncSourceConfig#mongodSingleHostReplicationUri}.
   */
  public static String getSyncSourceHost(SyncSourceConfig syncSourceConfig) {

    // validateReplicationUrisAvailable() should have caught an absent URI before we get here;
    // the orElseThrow is a defensive assertion.
    ConnectionInfo mongodUri =
        Optionals.orElseThrow(
            syncSourceConfig.mongodSingleHostReplicationUri,
            "syncSourceConfig.mongodSingleHostReplicationUri must be set before starting up"
                + " the MongoDbReplicationManager");

    Optional<String> hostName =
        syncSourceConfig.mongodUris.flatMap(
            map ->
                map.entrySet().stream()
                    .filter(e -> mongodUri.equals(e.getValue()))
                    .map(Map.Entry::getKey)
                    .findFirst());

    if (hostName.isEmpty()) {
      // There should only be one host from mongodSingleHostReplicationUri for Atlas that's using a
      // direct connection for initial sync
      String host = mongodUri.uri().getHosts().getFirst();
      // return the host name excluding port.
      return host.split(":")[0];
    }

    return hostName.get();
  }

  private static com.mongodb.client.MongoClient getSynonymsMongoClient(
      ConnectionInfo connectionInfo, int numConcurrentSynonymSyncs, MeterRegistry meterRegistry) {
    return MongoClientBuilder.builder(connectionInfo, meterRegistry)
        .description("mongos synonym sync")
        .maxConnections(numConcurrentSynonymSyncs)
        .buildSyncClient();
  }

  public static BatchMongoClient getSyncBatchMongoClient(
      SyncSourceConfig syncSourceConfig,
      int numConcurrentChangeStreams,
      String metricsNamespacePrefix,
      MeterRegistry meterRegistry) {
    var connectionInfo = syncSourceConfig.mongodClusterReplicationUri;
    return MongoClientBuilder.builder(connectionInfo, metricsNamespacePrefix, meterRegistry)
        .description("steady state sync")
        .maxConnections(numConcurrentChangeStreams)
        .buildSyncBatchClient();
  }

  /**
   * Derives the steady-state replication config from the given {@link MongoDbReplicationConfig}.
   */
  public static SteadyStateReplicationConfig getSteadyStateReplicationConfig(
      MongoDbReplicationConfig replicationConfig) {
    return getSteadyStateReplicationConfig(replicationConfig, FeatureFlags.getDefault());
  }

  /**
   * Derives the steady-state replication config, resolving the feature flags the steady-state path
   * needs so the change stream managers do not each have to take {@link FeatureFlags}.
   */
  public static SteadyStateReplicationConfig getSteadyStateReplicationConfig(
      MongoDbReplicationConfig replicationConfig, FeatureFlags featureFlags) {
    return SteadyStateReplicationConfig.builder()
        .setRetryThrottledGetMoreOnSameCursor(
            featureFlags.isEnabled(Feature.RETRY_THROTTLED_GETMORE_ON_SAME_CURSOR))
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
        .setSteadyStateSchedulingMethod(replicationConfig.getSteadyStateSchedulingMethod())
        .setSteadyStatePriorityRatio(replicationConfig.getSteadyStatePriorityRatio())
        .setSteadyStatePriorityOptimeThresholdSeconds(
            replicationConfig.getSteadyStatePriorityOptimeThresholdSeconds())
        .setChangeStreamInactivityThresholdSec(
            replicationConfig.getChangeStreamInactivityThresholdSec())
        .setChangeStreamKeepAliveIntervalSec(
            replicationConfig.getChangeStreamKeepAliveIntervalSec())
        .build();
  }

  @Override
  public Optional<SyncSourceConfig> getSyncSourceConfig() {
    return this.syncSourceConfig;
  }

  @Override
  public synchronized boolean isInitialized() {
    return this.indexManagers.values().stream()
        .map(IndexManager::getInitFuture)
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

    DefaultDocumentIndexer indexer =
        indexGeneration instanceof DerivedLocalIndexGeneration derivedGeneration
            ? DefaultDocumentIndexer.create(
                initializedIndex.get(), derivedGeneration.getCommitMetadataSupplier())
            : DefaultDocumentIndexer.create(initializedIndex.get());

    IndexManager indexManager =
        ReplicationIndexManagerFactory.forIndexGeneration(
                indexGeneration, this.replicationIndexManagerFactory, this.leaderStatusRegistrar)
            .create(
                this.lifecycleExecutor,
                this.cursorManager,
                this.initialSyncQueue,
                this.steadyStateManager,
                Optional.of(this.synonymManager),
                indexGeneration,
                initializedIndex.get(),
                indexer,
                this.commitExecutor,
                this.commitInterval,
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

    IndexManager indexManager = this.indexManagers.remove(generationId);
    return indexManager.drop();
  }

  @Override
  public synchronized CompletableFuture<Void> shutdown() {
    LOG.info("Shutting down.");

    this.shutdown = true;

    List<CompletableFuture<?>> futures =
        this.indexManagers.values().stream()
            .map(IndexManager::shutdown)
            .collect(Collectors.toList());

    // Abort any in-flight replication reads before we start awaiting the shutdown futures above.
    // The synchronous mongo driver ignores Thread.interrupt() during a blocking socket read, so the
    // only way to unblock a hung getMore/find (e.g. when the sync source is unhealthy) is to close
    // the client, which closes the socket. Doing this early lets the initial-sync, steady-state and
    // synonym worker threads unwind so their executors terminate before Executors.shutdownOrFail
    // hits its hard timeout and crashes mongot. The later .close() calls in the chain below become
    // idempotent no-ops.
    abortReplicationMongoClients();

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

  /**
   * Closes all replication mongo clients to abort any in-flight reads. Closing a client closes its
   * sockets, which is the only reliable way to unblock a synchronous-driver read that is hung on an
   * unhealthy sync source. Every client's {@code close()} is idempotent, so it is safe for this to
   * run before the later close stages of {@link #shutdown()}.
   */
  private void abortReplicationMongoClients() {
    LOG.info("Aborting replication mongo clients to unblock in-flight reads.");

    // Mark the session refreshers as stopping before closing the clients so that a refresh racing
    // with the close is recognized as an expected shutdown failure rather than being counted.
    this.clientSessionRecordMap
        .values()
        .forEach(clientSessionRecord -> clientSessionRecord.sessionRefresher().markStopping());
    this.synonymsSessionRefresher.ifPresent(SessionRefresher::markStopping);

    this.clientSessionRecordMap
        .values()
        .forEach(clientSessionRecord -> clientSessionRecord.syncMongoClient().close());
    this.syncBatchMongoClient.close();
    this.synonymsSyncClient.ifPresent(MongoClient::close);
  }

  @Override
  public boolean isReplicationSupported() {
    return true;
  }

  @VisibleForTesting
  synchronized IndexManager getReplicationIndexManager(IndexGeneration indexGeneration) {
    return this.indexManagers.get(indexGeneration.getGenerationId());
  }

  /** Creates a copy of {@link MongoDbReplicationManager#indexManagers}. Thread safe method. */
  @SuppressWarnings("GuardedBy") // iterations through ConcurrentHashMap (copying) are thread safe
  private Map<GenerationId, IndexManager> getIndexManagers() {
    return new HashMap<>(this.indexManagers);
  }

  private void deregisterGauges() {
    this.managerUp.decrementAndGet();
    this.metricsFactory.close();
  }
}
