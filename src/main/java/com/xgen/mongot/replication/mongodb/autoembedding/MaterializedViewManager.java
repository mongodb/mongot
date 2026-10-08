package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog.canonicalKey;
import static com.xgen.mongot.replication.mongodb.IndexManager.State.FAILED;
import static com.xgen.mongot.replication.mongodb.IndexManager.State.FAILED_EXCEEDED;
import static com.xgen.mongot.replication.mongodb.IndexManager.State.SHUT_DOWN;
import static com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig.Type.AUTO_EMBEDDING;
import static com.xgen.mongot.util.Check.checkState;
import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.cache.EmbeddingRetryCache;
import com.xgen.mongot.embedding.cache.EmbeddingRetryCache.ReleaseTrigger;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.leasing.DynamicLeaderLeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.Lease;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.StaticLeaderLeaseManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.utils.MongoClientOperationExecutor;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGenerationUtil;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.replication.mongodb.IndexManager;
import com.xgen.mongot.replication.mongodb.ReplicationIndexManager;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.Runtime;
import com.xgen.mongot.util.VerboseRunnable;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton instance created at startup, manages view generators for all auto-embedding
 * indexes/indexGenerations. Supports both leader and follower roles.
 *
 * <p>Leader mode: Populates auto-embedding materialized views by running generators, initial sync,
 * and steady state replication.
 *
 * <p>Follower mode: Tracks materialized view status by polling the lease manager without populating
 * the materialized view.
 */
public class MaterializedViewManager implements ReplicationManager {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewManager.class);

  public static final String MAT_VIEW_MANAGER_STATE = "matViewGeneratorState";

  public static final String GENERATOR_SHUTDOWN_FAILURE_COUNTER_NAME =
      "matViewGeneratorShutdownFailures";

  public static final String GENERATOR_CREATION_FAILURE_COUNTER_NAME =
      "matViewGeneratorCreationFailures";

  public static final String GENERATOR_CREATION_RETRY_COUNTER_NAME =
      "matViewGeneratorCreationRetries";

  /** Counts lease reacquisitions granted while the prior local generator still leads. */
  public static final String UNSAFE_LEADERSHIP_REACQUISITION_COUNTER_NAME =
      "matViewUnsafeLeadershipReacquisitions";

  /**
   * Epoch-seconds timestamp of the most recent unsafe leadership reacquisition, alongside {@link
   * #UNSAFE_LEADERSHIP_REACQUISITION_COUNTER_NAME}. An absolute timestamp lets alerting detect a
   * new event with a single time() comparison, unaffected by scrape gaps or process restarts; an
   * increase over the counter's window cannot distinguish a new event from an old value replayed
   * after a reporting gap (CLOUDP-451834).
   */
  public static final String UNSAFE_LEADERSHIP_REACQUISITION_LAST_EVENT_GAUGE_NAME =
      "matViewUnsafeLeadershipReacquisitionsLastEventEpochSeconds";

  /** Counts generations retired because their materialized-view collection is missing. */
  public static final String RETIRED_GENERATION_COUNTER_NAME = "matViewRetiredGenerations";

  public static final String STATE_LABEL = "state";

  public static final String MAT_VIEW_GC_SCANNER_ALIVE_GAUGE = "matViewGcScannerAlive";

  // Per-host materialized-view replication lag (ms).
  // Process-level gauge intended for MaaS/Atlas export.
  public static final String MAT_VIEW_REPLICATION_LAG_MS_GAUGE = "materializedViewReplicationLagMs";
  public static final String STAT_LABEL = "stat";
  public static final String STAT_MAX = "max";
  public static final String STAT_AVG = "avg";
  public static final String MAT_VIEW_COLLECTION_DROP_COUNTER_NAME = "matViewCollectionDrop";

  public static final String DROP_SCENARIO_TAG = "scenario";
  public static final String DROP_OUTCOME_TAG = "outcome";
  public static final String DROP_SCENARIO_GC = "gc";
  public static final String DROP_SCENARIO_INDEX_DELETION = "indexDeletion";
  public static final String DROP_SCENARIO_SOURCE_COLLECTION_DROP = "sourceCollectionDrop";

  // Sticky terminal states that disqualify a generator from holding a matview lease. Shared
  // by emitHeartbeat (release these leases), refreshStatus (skip acquisition for these), and
  // MaterializedViewGenerator.initReplication (clear leader state on terminal init).
  // STEADY_STATE_SHUT_DOWN is intentionally excluded — it is a transient pre-classification
  // state in handleSteadyStateException and the classifier can transition back to non-terminal
  // for recoverable cases (transient errors, renames, invalidates, resyncs).
  static final Set<IndexManager.State> TERMINAL_STATES = Set.of(SHUT_DOWN, FAILED, FAILED_EXCEEDED);

  // ==================== Common Fields ====================

  private final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;

  private final AutoEmbeddingMongoClient autoEmbeddingMongoClient;

  private final DecodingWorkScheduler decodingWorkScheduler;

  private final MeterRegistry meterRegistry;

  /** State for {@link #UNSAFE_LEADERSHIP_REACQUISITION_LAST_EVENT_GAUGE_NAME}; set on event. */
  private final AtomicLong unsafeLeadershipReacquisitionLastEventEpochSeconds;

  /** A mapping of materialized view collections to active GenerationIds. */
  @GuardedBy("this")
  @VisibleForTesting
  final ActiveGenerationIdCatalog activeGenerationIdCatalog;

  private final MetricsFactory metricsFactory;
  private final MongoClientOperationExecutor matViewWriterOperationExecutor;

  @GuardedBy("this")
  private boolean shutdown;

  @GuardedBy("this")
  private boolean isReplicationEnabled;

  /** A mapping of all initialized materialized view generators by IndexID. */
  @GuardedBy("this")
  private final Map<UUID, MaterializedViewGenerator> managedMaterializedViewGenerators;

  /** The LeaseManager for status polling (follower) and lease management (leader). */
  private final LeaseManager leaseManager;

  /**
   * The live index config, consulted by the GC sweep: an index absent from the catalog has been
   * deleted, so none of its leases is a current version worth protecting.
   */
  private final IndexCatalog indexCatalog;

  /** Factory for creating MaterializedViewGenerator instances. */
  private final MaterializedViewGeneratorFactory matViewGeneratorFactory;

  private final MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog;

  /**
   * Same instance wired into the embedding scheduler. Held here so the drop path can release a
   * dropped generation's claims, evicting its retained embeddings.
   */
  private final EmbeddingRetryCache embeddingRetryCache;

  // ==================== Index Leader Fields ====================

  /**
   * The Executor that is used by steady state indexing, as well as the MaterializedViewGenerator
   * for scheduling its lifecycle tasks. Used when acting as the leader for indexes.
   *
   * <p>The NamedExecutorService is owned by this MaterializedViewManager.
   */
  private final NamedExecutorService lifecycleExecutor;

  /** Executor for periodic max optime updates. */
  private final NamedScheduledExecutorService optimeUpdaterExecutor;

  private final ScheduledFuture<?> optimeUpdaterFuture;

  /** Executor for the leader heartbeat task. Used when acting as leader. */
  private final NamedScheduledExecutorService heartbeatExecutor;

  private final ScheduledFuture<?> heartbeatFuture;

  // ==================== Status Refresh Fields ====================

  /** Executor for periodic follower status and replication optime refresh. */
  private final NamedScheduledExecutorService statusRefreshExecutor;

  private final ScheduledFuture<?> statusRefreshFuture;

  // ==================== Stale-lease GC scanner fields ====================

  // The scan interval and the mark/delete grace periods are configurable via
  // AutoEmbeddingMaterializedViewConfig (staleLeaseScanIntervalMs / staleLeaseMarkGracePeriodMs /
  // staleLeaseDeleteGracePeriodMs), set per cluster through the conf call.

  /** Executor for the periodic stale-lease GC scan. Separate from heartbeat and status refresh. */
  private final NamedScheduledExecutorService staleLeaseScannerExecutor;

  private final ScheduledFuture<?> staleLeaseScannerFuture;

  private final StaleLeaseGarbageCollector staleLeaseGarbageCollector;
  private final MaterializedViewStatusUpdater statusUpdater;

  private final AutoEmbeddingMaterializedViewConfig materializedViewConfig;

  /**
   * Gates the stale-lease GC sweep at runtime. The scanner thread is always scheduled; each tick
   * evaluates {@link DynamicFeatureFlags#MV_GC_ENABLED} (cluster-invariant) and does nothing while
   * the flag is off, so cleanup can be enabled or disabled per cluster without a restart.
   */
  private final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;

  /**
   * Tracks generators with a pending async shutdown from {@link #transitionToFollower}. {@link
   * #refreshStatus()} skips leadership acquisition for these ids to avoid a race where a new
   * generator tries to enqueue init-sync before the old generator finishes cleanup. Removed in
   * {@link #refreshStatus()} once the shutdown future completes and the InitialSyncQueue confirms
   * the stale entry is gone.
   */
  @VisibleForTesting
  final Map<MaterializedViewGenerationId, CompletableFuture<Void>> pendingShutdowns =
      new ConcurrentHashMap<>();

  /**
   * Generations that can never make progress on this shard because their materialized-view
   * collection is missing, so they must be neither recreated nor re-elected (CLOUDP-437172).
   * Lock-free: read and written both inside and outside the manager monitor.
   */
  @VisibleForTesting
  final Set<MaterializedViewGenerationId> retiredGenerations = ConcurrentHashMap.newKeySet();

  /**
   * Package-private constructor for testing - registers gauges and starts periodic tasks. This
   * constructor maintains backward compatibility with existing tests. Production code should use
   * {@link #create} static factory method which uses the private constructor to avoid this-escape.
   */
  @VisibleForTesting
  MaterializedViewManager(
      NamedExecutorService lifecycleExecutor,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      DecodingWorkScheduler decodingWorkScheduler,
      MaterializedViewGeneratorFactory matViewGeneratorFactory,
      NamedScheduledExecutorService heartbeatExecutor,
      NamedScheduledExecutorService statusRefreshExecutor,
      NamedScheduledExecutorService optimeUpdaterExecutor,
      NamedScheduledExecutorService staleLeaseScannerExecutor,
      MeterRegistry meterRegistry,
      LeaseManager leaseManager,
      IndexCatalog indexCatalog,
      MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      EmbeddingRetryCache embeddingRetryCache) {
    this(
        lifecycleExecutor,
        indexingWorkSchedulerFactory,
        autoEmbeddingMongoClient,
        decodingWorkScheduler,
        matViewGeneratorFactory,
        heartbeatExecutor,
        statusRefreshExecutor,
        optimeUpdaterExecutor,
        staleLeaseScannerExecutor,
        meterRegistry,
        leaseManager,
        indexCatalog,
        matViewMetadataCatalog,
        materializedViewConfig,
        dynamicFeatureFlagRegistry,
        embeddingRetryCache,
        true); // registerGauges = true for backward compatibility
  }

  /**
   * Private constructor - optionally registers gauges based on the registerGauges parameter. Use
   * static factory methods to construct instances.
   */
  private MaterializedViewManager(
      NamedExecutorService lifecycleExecutor,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      DecodingWorkScheduler decodingWorkScheduler,
      MaterializedViewGeneratorFactory matViewGeneratorFactory,
      NamedScheduledExecutorService heartbeatExecutor,
      NamedScheduledExecutorService statusRefreshExecutor,
      NamedScheduledExecutorService optimeUpdaterExecutor,
      NamedScheduledExecutorService staleLeaseScannerExecutor,
      MeterRegistry meterRegistry,
      LeaseManager leaseManager,
      IndexCatalog indexCatalog,
      MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      EmbeddingRetryCache embeddingRetryCache,
      boolean registerGauges) {
    this.lifecycleExecutor = lifecycleExecutor;
    this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
    this.decodingWorkScheduler = decodingWorkScheduler;
    this.matViewGeneratorFactory = matViewGeneratorFactory;
    matViewGeneratorFactory.setOnSourceCollectionDropped(
        this::cleanUpMatViewResourcesForDroppedSourceCollection);
    this.meterRegistry = meterRegistry;
    this.unsafeLeadershipReacquisitionLastEventEpochSeconds = new AtomicLong();
    // Registered against the raw registry (like its counter) rather than this.metricsFactory, so
    // the exported name sits beside mongot_matViewUnsafeLeadershipReacquisitions_total instead of
    // under the replication.mongodb namespace.
    Gauge.builder(
            UNSAFE_LEADERSHIP_REACQUISITION_LAST_EVENT_GAUGE_NAME,
            this.unsafeLeadershipReacquisitionLastEventEpochSeconds,
            Number::doubleValue)
        // strong reference so the state object is not garbage collected
        .strongReference(true)
        .register(this.meterRegistry);
    this.managedMaterializedViewGenerators = new ConcurrentHashMap<>();
    this.activeGenerationIdCatalog = new ActiveGenerationIdCatalog(matViewMetadataCatalog);
    this.heartbeatExecutor = heartbeatExecutor;
    this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
    this.shutdown = false;
    this.metricsFactory = new MetricsFactory("replication.mongodb", this.meterRegistry);
    this.matViewWriterOperationExecutor =
        new MongoClientOperationExecutor(this.metricsFactory, "matViewCollection");
    this.statusRefreshExecutor = statusRefreshExecutor;
    this.optimeUpdaterExecutor = optimeUpdaterExecutor;
    this.staleLeaseScannerExecutor = staleLeaseScannerExecutor;
    this.leaseManager = leaseManager;
    this.indexCatalog = indexCatalog;
    this.matViewMetadataCatalog = matViewMetadataCatalog;
    this.embeddingRetryCache = embeddingRetryCache;
    this.isReplicationEnabled = false;
    this.materializedViewConfig = materializedViewConfig;
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
    this.statusUpdater =
        new MaterializedViewStatusUpdater(
            autoEmbeddingMongoClient,
            this.meterRegistry,
            this::getMatViewGenerators,
            this::publishIfCurrentGenerator,
            this::isRetired,
            () -> !isShutdown() && isReplicationSupported());
    this.staleLeaseGarbageCollector =
        new StaleLeaseGarbageCollector(
            leaseManager,
            materializedViewConfig,
            dynamicFeatureFlagRegistry,
            indexCatalog,
            this.meterRegistry,
            this.metricsFactory,
            () -> !isShutdown() && isReplicationSupported(),
            this::hasConfiguredAutoEmbeddingIndex,
            this::usedByLocalGeneration,
            (leaseKey, namespace) -> dropMatViewCollection(leaseKey, namespace, DROP_SCENARIO_GC));

    // Always start heartbeat - it emits heartbeat only for indexes where this instance is leader
    LOG.atInfo()
        .addKeyValue("intervalMs", materializedViewConfig.leaseManagerHeartbeatIntervalMs)
        .log("Starting auto-embedding heartbeat");
    this.heartbeatFuture =
        heartbeatExecutor.scheduleWithFixedDelay(
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
            0,
            this.materializedViewConfig.leaseManagerHeartbeatIntervalMs,
            TimeUnit.MILLISECONDS);

    // Periodic status and replication optime refresh for followers.
    this.statusRefreshFuture =
        statusRefreshExecutor.scheduleWithFixedDelay(
            new VerboseRunnable() {
              @Override
              public void verboseRun() {
                refreshStatus();
              }

              @Override
              public Logger getLogger() {
                return LOG;
              }
            },
            0,
            this.materializedViewConfig.materializedViewStatusRefreshIntervalMs,
            TimeUnit.MILLISECONDS);

    // Periodic replication optime cleanup and max optime updates for materialized view indexes.
    this.optimeUpdaterFuture =
        optimeUpdaterExecutor.scheduleWithFixedDelay(
            new VerboseRunnable() {
              @Override
              public void verboseRun() {
                MaterializedViewManager.this.statusUpdater.updateMaxReplicationOpTime();
              }

              @Override
              public Logger getLogger() {
                return LOG;
              }
            },
            0,
            this.materializedViewConfig.materializedViewOptimeUpdateIntervalMs,
            TimeUnit.MILLISECONDS);

    // Periodic stale-lease GC scan. Always scheduled; each tick is a no-op unless the
    // MV_GC_ENABLED dynamic feature flag is on, so cleanup toggles at runtime without a restart.
    // The first run is deferred by StaleLeaseGarbageCollector.firstScanDelayMs (see its javadoc).
    this.staleLeaseScannerFuture =
        staleLeaseScannerExecutor.scheduleWithFixedDelay(
            new VerboseRunnable() {
              @Override
              public void verboseRun() {
                MaterializedViewManager.this.staleLeaseGarbageCollector.scanStaleLeasesForCleanup();
              }

              @Override
              public Logger getLogger() {
                return LOG;
              }
            },
            StaleLeaseGarbageCollector.firstScanDelayMs(this.materializedViewConfig),
            this.materializedViewConfig.staleLeaseScanIntervalMs,
            TimeUnit.MILLISECONDS);

    if (registerGauges) {
      createStateGauges(this, this.metricsFactory);
    }
  }

  /** Creates a new MaterializedViewManager. */
  @VisibleForTesting
  public static MaterializedViewManager create(
      Path rootPath,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
      InitialSyncConfig initialSyncConfig,
      FeatureFlags featureFlags,
      MongotCursorManager cursorManager,
      Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      LeaseManager leaseManager,
      IndexCatalog indexCatalog,
      MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    if (embeddingServiceManagerSupplier.isEmpty()) {
      throw new IllegalArgumentException("EmbeddingServiceManagerSupplier must be provided");
    }
    LOG.info("creating AutoEmbeddingMatViewManager");
    var meterRegistry = meterAndFtdcRegistry.meterRegistry();
    meterRegistry.gauge("materializedView.replication.manager", 1);
    boolean enableLifecycleAttributionMetrics =
        featureFlags.isEnabled(Feature.LIFECYCLE_ATTRIBUTION_METRICS);
    Optional<ThreadPoolResourceMetrics.Bindings> attributionBindings =
        enableLifecycleAttributionMetrics
            ? Optional.of(meterAndFtdcRegistry.threadPoolResourceBindings())
            : Optional.empty();

    var lifecycleExecutor =
        Executors.fixedSizeThreadPool(
            "materialized-view-lifecycle",
            Math.max(1, Runtime.INSTANCE.getNumCpus() / 4),
            meterRegistry);
    attributionBindings.ifPresent(
        bindings ->
            bindings.forSubsystem("autoembedding").register(lifecycleExecutor, meterRegistry));

    // Constructed here, not in the factory, so the drop path below can release claims on the same
    // instance the scheduler caches into. The cache is always created, but the scheduler only
    // consults it when the EMBEDDING_RETRY_CACHE flag is enabled — evaluated per batch (not once
    // here) so the flag toggles at runtime without recreating the manager. Disabled (default)
    // leaves
    // master behavior unchanged: nothing is cached or claimed.
    var embeddingRetryCache = EmbeddingRetryCache.create(meterRegistry);
    BooleanSupplier embeddingRetryCacheEnabled =
        () ->
            dynamicFeatureFlagRegistry.evaluateClusterInvariant(
                DynamicFeatureFlags.EMBEDDING_RETRY_CACHE);
    // Evaluated per index (not once here) so the flag toggles at runtime without recreating the
    // manager, and per-index scoping lets a customer opt individual indexes into SKIP.
    Predicate<ObjectId> skipInvalidRequestEnabledForIndex =
        indexId ->
            dynamicFeatureFlagRegistry.evaluate(
                DynamicFeatureFlags.AUTO_EMBEDDING_400_SKIP.getName(),
                indexId,
                DynamicFeatureFlags.AUTO_EMBEDDING_400_SKIP.getFallback());

    var indexingWorkSchedulerFactory =
        IndexingWorkSchedulerFactory.createEmbeddingIndexingSchedulerOnly(
            materializedViewConfig.numIndexingThreads,
            embeddingServiceManagerSupplier.get(),
            matViewMetadataCatalog,
            meterRegistry,
            materializedViewConfig.globalMemoryBudgetHeapPercent,
            materializedViewConfig.perBatchMemoryBudgetHeapPercent,
            attributionBindings,
            embeddingRetryCache,
            embeddingRetryCacheEnabled,
            skipInvalidRequestEnabledForIndex,
            generationId ->
                LeaseManager.isUsableLeaseVersion(
                    leaseManager.getOwnedTenure(generationId).leaseVersion()));

    var decodingWorkScheduler =
        DecodingWorkScheduler.create(
            materializedViewConfig.numChangeStreamDecodingThreads,
            AUTO_EMBEDDING,
            meterRegistry,
            attributionBindings);

    var materializedViewGeneratorFactory =
        new MaterializedViewGeneratorFactory(
            rootPath.resolve("autoEmbedding"),
            lifecycleExecutor,
            cursorManager,
            initialSyncConfig,
            meterAndFtdcRegistry,
            featureFlags,
            indexingWorkSchedulerFactory,
            decodingWorkScheduler,
            matViewMetadataCatalog,
            materializedViewConfig,
            leaseManager);

    var heartbeatExecutor =
        Executors.singleThreadScheduledExecutor("mat-view-leader-heartbeat", meterRegistry);

    var statusRefreshExecutor =
        Executors.singleThreadScheduledExecutor("mat-view-status-refresh", meterRegistry);

    var optimeUpdaterExecutor =
        Executors.singleThreadScheduledExecutor("mat-view-optime-updater", meterRegistry);

    var staleLeaseScannerExecutor =
        Executors.singleThreadScheduledExecutor("mat-view-stale-lease-scanner", meterRegistry);

    MaterializedViewManager manager =
        new MaterializedViewManager(
            lifecycleExecutor,
            indexingWorkSchedulerFactory,
            autoEmbeddingMongoClient,
            decodingWorkScheduler,
            materializedViewGeneratorFactory,
            heartbeatExecutor,
            statusRefreshExecutor,
            optimeUpdaterExecutor,
            staleLeaseScannerExecutor,
            meterRegistry,
            leaseManager,
            indexCatalog,
            matViewMetadataCatalog,
            materializedViewConfig,
            dynamicFeatureFlagRegistry,
            embeddingRetryCache,
            false); // Don't register gauges/tasks in constructor to avoid this-escape

    // Register gauges after construction is complete
    createStateGauges(manager, manager.metricsFactory);

    return manager;
  }

  /** Creates gauges to track the number of view generators by state */
  private static void createStateGauges(
      MaterializedViewManager autoEmbeddingMatViewManager, MetricsFactory metricsFactory) {
    Arrays.stream(IndexManager.State.values())
        .forEach(
            state ->
                metricsFactory.objectValueGauge(
                    MAT_VIEW_MANAGER_STATE,
                    autoEmbeddingMatViewManager,
                    manager -> manager.gaugeViewGenerators(state),
                    Tags.of(STATE_LABEL, state.name())));
    // matViewGcScannerAlive: 1 while GC is actually running for this cluster — scanner scheduled &
    // alive AND MV_GC_ENABLED on — 0 otherwise. Bound gauge re-evaluated on scrape, so it tracks
    // the dynamic flag with no restart. Registered here (post-construction) because it binds
    // `this`.
    metricsFactory.objectValueGauge(
        MAT_VIEW_GC_SCANNER_ALIVE_GAUGE,
        autoEmbeddingMatViewManager,
        MaterializedViewManager::gcScannerAliveValue);
    // Per-host max/avg replication lag across queryable materialized views, re-evaluated on scrape.
    // Registered here (post-construction) because it binds `this`.
    metricsFactory.objectValueGauge(
        MAT_VIEW_REPLICATION_LAG_MS_GAUGE,
        autoEmbeddingMatViewManager,
        manager -> manager.replicationLagMs(stats -> (double) stats.getMax()),
        Tags.of(STAT_LABEL, STAT_MAX));
    metricsFactory.objectValueGauge(
        MAT_VIEW_REPLICATION_LAG_MS_GAUGE,
        autoEmbeddingMatViewManager,
        manager -> manager.replicationLagMs(LongSummaryStatistics::getAverage),
        Tags.of(STAT_LABEL, STAT_AVG));
  }

  /**
   * Aggregates replication lag (ms) across all materialized views that can currently service
   * queries, applying {@code selector} (max or average) to the resulting summary. Returns {@link
   * Double#NaN} when no such MV has reported an optime snapshot yet, so an idle host emits nothing.
   * Never throws: a transient index-lifecycle race during a scrape degrades to NaN.
   */
  private double replicationLagMs(ToDoubleFunction<LongSummaryStatistics> selector) {
    try {
      return summarizeReplicationLagMs(queryableMaterializedViewLagsMs(), selector);
    } catch (RuntimeException e) {
      return Double.NaN;
    }
  }

  @VisibleForTesting
  LongStream queryableMaterializedViewLagsMs() {
    Set<MaterializedViewGenerationId> followerIds = this.leaseManager.getFollowerGenerationIds();
    return this.getMatViewGenerators().values().stream()
        // A generator that is shut down, or whose generation the lease manager no longer tracks,
        // has stopped advancing its replication optime; counting its frozen snapshot would report
        // lag that grows with wall-clock time until restart.
        .filter(generator -> !TERMINAL_STATES.contains(generator.getState()))
        .filter(
            generator -> {
              MaterializedViewGenerationId generationId =
                  generator.getIndexGeneration().getGenerationId();
              return this.leaseManager.isLeader(generationId) || followerIds.contains(generationId);
            })
        .map(generator -> generator.getIndexGeneration().getIndex())
        .filter(matViewIndex -> !matViewIndex.isClosed())
        .filter(matViewIndex -> matViewIndex.getStatus().canServiceQueries())
        .map(
            matViewIndex ->
                matViewIndex
                    .getMetricsUpdater()
                    .getIndexingMetricsUpdater()
                    .getReplicationOpTimeInfo()
                    .snapshot())
        .flatMap(Optional::stream)
        .mapToLong(ReplicationOpTimeInfo.Snapshot::replicationLagMs);
  }

  static double summarizeReplicationLagMs(
      LongStream lagsMs, ToDoubleFunction<LongSummaryStatistics> selector) {
    LongSummaryStatistics stats = lagsMs.summaryStatistics();
    return stats.getCount() == 0 ? Double.NaN : selector.applyAsDouble(stats);
  }

  /** helper function similar to MongoDbReplicationManager::gaugeReplicationManagers */
  private double gaugeViewGenerators(IndexManager.State state) {
    return this.getMatViewGenerators().entrySet().stream()
        .filter(m -> m.getValue().getStateWithoutLock() == state)
        .count();
  }

  @Override
  public Optional<SyncSourceConfig> getSyncSourceConfig() {
    return this.autoEmbeddingMongoClient.getSyncSourceConfig();
  }

  @Override
  public synchronized boolean isInitialized() {
    return this.managedMaterializedViewGenerators.values().stream()
        .map(MaterializedViewGenerator::getInitFuture)
        .allMatch(initFuture -> initFuture.isDone() && !initFuture.isCompletedExceptionally());
  }

  /**
   * Adds an index generation to be managed.
   *
   * <p>If no generator exists for this collection, creates a new one. If an existing generator is
   * found, always replaces it — the {@code skipInitialSync} flag in {@link LeaseManager#add}
   * determines whether the new generator resumes from the last checkpoint or does initial sync.
   *
   * <p>Follower mode: Tracks the materialized view generation for status polling.
   */
  @Override
  public synchronized void add(IndexGeneration indexGeneration) {
    checkState(!this.shutdown, "cannot call add() after shutdown()");
    LOG.atInfo()
        .addKeyValue("generationId", indexGeneration.getGenerationId())
        .log("Adding index generation to materialized view manager");
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        Check.instanceOf(indexGeneration, AutoEmbeddingIndexGeneration.class);
    MaterializedViewIndexGeneration matViewIndexGeneration =
        autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration();
    Optional<UUID> staleUuid = this.activeGenerationIdCatalog.add(autoEmbeddingIndexGeneration);
    // A generator keyed by a stale UUID would never be found again by drop or status refresh.
    // Take it out so it can be handed over under the current UUID below.
    Optional<MaterializedViewGenerator> displaced =
        staleUuid.isPresent()
            ? Optional.ofNullable(this.managedMaterializedViewGenerators.remove(staleUuid.get()))
            : Optional.empty();
    if (!this.isReplicationEnabled) {
      // No generator will be installed, so nothing inherits the displaced one; stop it here.
      displaced.ifPresent(this::shutDownDisplacedGenerator);
      LOG.atInfo().log(
          "Skipping creating materialized view generator because replication is disabled.");
      return;
    }

    // Always create generators for both leader and follower modes.
    // Leader mode: generator runs replication loop and writes to materialized view.
    // Follower mode: generator is passive, status is polled from LeaseManager.
    UUID uuid = getCollectionUuid(matViewIndexGeneration.getGenerationId());
    try {
      this.managedMaterializedViewGenerators.compute(
          uuid,
          (ignored, existingGenerator) -> {
            // Nothing under the current UUID yet: replace the displaced generator through the
            // normal path, which shuts it down once its successor exists.
            if (existingGenerator == null && displaced.isPresent()) {
              return computeGenerator(displaced.get(), matViewIndexGeneration);
            }
            // A generator already lives under the current UUID; the displaced one is redundant.
            displaced.ifPresent(this::shutDownDisplacedGenerator);
            return computeGenerator(existingGenerator, matViewIndexGeneration);
          });
    } catch (RuntimeException e) {
      // A failed replacement leaves the displaced generator running. Stop it rather than keep it,
      // so the current UUID's slot stays empty and the status refresh retries the replacement.
      displaced.ifPresent(this::shutDownDisplacedGenerator);
      throw e;
    }
  }

  private void shutDownDisplacedGenerator(MaterializedViewGenerator generator) {
    LOG.atInfo()
        .addKeyValue("generationId", generator.getIndexGeneration().getGenerationId())
        .log("Shutting down generator displaced by an MV collection UUID change");
    generator
        .shutdown()
        .exceptionally(
            throwable -> {
              LOG.atWarn()
                  .addKeyValue("generationId", generator.getIndexGeneration().getGenerationId())
                  .setCause(throwable)
                  .log("Failed to shut down displaced generator");
              return null;
            });
  }

  /**
   * Stops materialized-view replication without stopping the manager's periodic tasks.
   *
   * <p>This follows the regular replication shutdown convention: signal generators to stop, abort
   * their clients to unblock in-flight reads, and only then wait for shutdown futures.
   */
  public synchronized CompletableFuture<Void> shutdownReplication() {
    prepareForShutdown();

    // Shutdown all generators first. Each generator handles its own role-specific cleanup and
    // synchronously transitions to SHUT_DOWN, so later read failures are treated as shutdown noise.
    // For follower mode, the map is empty, so this completes immediately.
    List<CompletableFuture<?>> futures =
        this.managedMaterializedViewGenerators.values().stream()
            .map(MaterializedViewGenerator::shutdown)
            .collect(Collectors.toList());
    this.managedMaterializedViewGenerators.clear();

    // Mirror MongoDbReplicationManager.abortReplicationMongoClients(): close the clients after
    // signaling shutdown but before waiting on the generator futures, so a blocked synchronous
    // driver read can unwind.
    this.matViewGeneratorFactory.abortClients();

    // Need to create a separate executor to run the shutdown tasks, otherwise it may end up running
    // on the indexing executor. As one of the shutdown tasks is shutting down that executor, this
    // will hang forever.
    var shutdownExecutor =
        Executors.fixedSizeThreadPool("mat-view-manager-shutdown", 1, this.meterRegistry);
    return FutureUtils.allOf(futures)
        .thenComposeAsync(ignored -> this.matViewGeneratorFactory.shutdown(), shutdownExecutor)
        // Signal the shutdown executor to clean up, but don't block waiting for it to do so.
        .thenRunAsync(shutdownExecutor::shutdown, shutdownExecutor);
  }

  /**
   * Computes the generator for the given materialized view index generation. This method is called
   * by {@link ConcurrentHashMap#compute} to determine the generator to use.
   *
   * <p>If no existing generator exists, creates a new one. If the incoming generation has a lower
   * definition version than the existing generator, the existing generator is kept (guards against
   * out-of-order updates). Otherwise, replaces the existing generator. The {@code skipInitialSync}
   * flag in {@link LeaseManager#add} ensures the new generator resumes correctly: Lucene-only or
   * same-version changes preserve the existing commit (RESUME_STEADY_STATE), while MV schema
   * changes trigger initial sync from the high water mark (RESUME_INITIAL_SYNC).
   *
   * @param existingGenerator the existing generator, or null if none exists
   * @param matViewIndexGeneration the materialized view index generation
   * @return the generator to use
   */
  private MaterializedViewGenerator computeGenerator(
      MaterializedViewGenerator existingGenerator,
      MaterializedViewIndexGeneration matViewIndexGeneration) {
    // A recovery attempt bumps the search attempt but keeps this MV generation id, so it arrives
    // here and would hand the retained lease to a fresh generator, restarting the flap.
    if (isRetired(matViewIndexGeneration.getGenerationId())) {
      LOG.atInfo()
          .addKeyValue("generationId", matViewIndexGeneration.getGenerationId())
          .log(
              "Skipping generator for retired generation, its materialized view collection is "
                  + "missing");
      return existingGenerator;
    }

    if (existingGenerator == null) {
      LOG.atInfo()
          .addKeyValue("generationId", matViewIndexGeneration.getGenerationId())
          .log("Creating new generator for generation");
      return createNewGenerator(matViewIndexGeneration);
    }

    // Guard against out-of-order updates: do not replace if the incoming generation has a
    // lower definition version than the existing generator (would revert MV schema/mapping).
    long existingVersion =
        existingGenerator.getIndexGeneration().getDefinition().getDefinitionVersion().orElse(0L);
    long newVersion = matViewIndexGeneration.getDefinition().getDefinitionVersion().orElse(0L);
    if (newVersion < existingVersion) {
      LOG.atWarn()
          .addKeyValue("generationId", matViewIndexGeneration.getGenerationId())
          .addKeyValue("existingVersion", existingVersion)
          .addKeyValue("newVersion", newVersion)
          .log("Ignoring generation with lower definition version than existing generator");
      return existingGenerator;
    }

    LOG.atInfo()
        .addKeyValue("generationId", matViewIndexGeneration.getGenerationId())
        .log("Replacing existing generator for generation");
    return replaceGenerator(existingGenerator, matViewIndexGeneration);
  }

  /** Creates a new generator for a new index. */
  private MaterializedViewGenerator createNewGenerator(
      MaterializedViewIndexGeneration matViewIndexGeneration) {
    this.leaseManager.add(matViewIndexGeneration, false);
    MaterializedViewGenerator generator;
    try {
      generator = createManagedGenerator(matViewIndexGeneration);
    } catch (RuntimeException e) {
      // Transient failures are retried by refreshStatus's periodic reconciliation.
      recordGeneratorCreationFailure(
          matViewIndexGeneration.getGenerationId(), e, "CREATE_NEW_GENERATOR");
      throw e;
    }
    // For static leader election only - activates leader mode immediately since leadership is
    // determined at startup. For dynamic leader election, generators start as followers and
    // leadership is acquired later via refreshStatus() when expired leases are detected.
    if (this.leaseManager instanceof StaticLeaderLeaseManager) {
      activateStaticLeadership(generator, matViewIndexGeneration.getGenerationId());
    } else if (this.leaseManager.isLeader(matViewIndexGeneration.getGenerationId())) {
      // For dynamic leader election: if we already own the lease (e.g., after restart with
      // unexpired lease), activate leadership immediately. The add() method already added
      // this generation to leaderGenerationIds, so we just need to start the generator.
      // Note: Must use matViewIndexGeneration.getGenerationId() (MaterializedViewGenerationId)
      // because that's what DynamicLeaderLeaseManager.add() uses when adding to
      // leaderGenerationIds.
      LOG.atInfo()
          .addKeyValue("generationId", matViewIndexGeneration.getGenerationId())
          .log("Activating leadership for generator - we own the existing lease after restart");
      becomeLeaderSafely(generator, matViewIndexGeneration.getGenerationId());
    }
    return generator;
  }

  /**
   * Updates the sync source configuration used for auto-embedding replication.
   *
   * <p>If the provided config has valid replication URIs, the underlying {@link
   * MaterializedViewGeneratorFactory} is re-initialized with the new connection details. The caller
   * is responsible for enabling replication (e.g. via {@link #setIsReplicationEnabled}) if
   * appropriate.
   *
   * <p>If the URIs are not yet available, the update is skipped and {@code false} is returned.
   *
   * @param syncSourceConfig the new sync source configuration
   * @return {@code true} if the factory was updated; {@code false} if URIs were unavailable and the
   *     update was skipped
   */
  public synchronized boolean updateSyncSource(SyncSourceConfig syncSourceConfig) {
    if (!syncSourceConfig.hasReplicationUrisAvailable()) {
      LOG.atInfo()
          .addKeyValue(
              "mongodSingleHostReplicationUri",
              syncSourceConfig
                  .mongodSingleHostReplicationUri
                  .map(info -> info.uri().getHosts())
                  .orElse(null))
          .addKeyValue(
              "mongosSingleHostReplicationUri",
              syncSourceConfig
                  .mongosSingleHostReplicationUri
                  .map(info -> info.uri().getHosts())
                  .orElse(null))
          .log("Sync source URIs not yet available; skipping auto-embedding sync source update.");
      return false;
    }
    LOG.info("Update AutoEmbeddingMatViewManager by new sync source.");
    this.matViewGeneratorFactory.updateSyncSourceConfig(syncSourceConfig);
    return true;
  }

  public synchronized void setIsReplicationEnabled(boolean isReplicationEnabled) {
    this.isReplicationEnabled = isReplicationEnabled;
  }

  /**
   * Disables replication and marks the generator factory as uninitialized.
   *
   * <p>Must be called under the {@code MaterializedViewManager} lock before any async teardown so
   * that concurrent callers of {@link #restartReplication()} observe the uninitialized state
   * immediately, without waiting for the async factory shutdown chain to complete.
   */
  public synchronized void prepareForShutdown() {
    this.isReplicationEnabled = false;
    this.matViewGeneratorFactory.markUninitialized();
  }

  /**
   * Replaces an existing generator with a new one for index redefinition or same-version retry.
   *
   * <p>Note on timing: The new generator is returned immediately (in follower mode) while
   * leaseManager.add() runs asynchronously after the old generator shuts down. Leadership
   * activation is deferred to {@link #refreshStatus()} via the {@link #pendingShutdowns} guard to
   * avoid a race where the new generator enqueues to InitialSyncQueue before the old generator's
   * cancelled entry is cleaned up by the dispatcher (same race as CLOUDP-393734).
   *
   * <p>Note: shutdown() is guaranteed to complete successfully (never exceptionally) per its
   * contract, so thenRun() will always execute.
   *
   * <p>Resolves skipInitialSync from definition diff: Lucene-only change (e.g. hnswOptions) →
   * preserve existing commit (RESUME_STEADY_STATE); MV schema change (e.g. filter) → do initial
   * sync from high water mark (RESUME_INITIAL_SYNC).
   */
  private MaterializedViewGenerator replaceGenerator(
      MaterializedViewGenerator existingGenerator,
      MaterializedViewIndexGeneration matViewIndexGeneration) {
    // Lucene-only change: preserve commit → RESUME_STEADY_STATE. MV schema change: do initial sync
    // from high watermark → RESUME_INITIAL_SYNC.
    boolean skipInitialSync =
        MaterializedViewIndexGenerationUtil.skipInitialSync(
            existingGenerator.getIndexGeneration().getDefinition(),
            matViewIndexGeneration.getDefinition());
    LOG.atInfo()
        .addKeyValue("generationId", matViewIndexGeneration.getGenerationId())
        .addKeyValue("skipInitialSync", skipInitialSync)
        .log("Determined index action when replacing existing generator for generation");
    MaterializedViewGenerationId oldGenerationId =
        existingGenerator.getIndexGeneration().getGenerationId();
    MaterializedViewGenerationId newGenerationId = matViewIndexGeneration.getGenerationId();

    // Block leadership acquisition early, before the slow factory.create() call.
    // This prevents the same race as CLOUDP-393734: if shutdown() completes synchronously
    // (e.g., cancel() returns COMPLETED_FUTURE for QUEUED entries), the old generator's
    // InitialSyncQueue entry may still be present when the new generator tries to enqueue.
    this.pendingShutdowns.put(newGenerationId, new CompletableFuture<>());
    try {
      MaterializedViewGenerator newGenerator;
      try {
        newGenerator = createManagedGenerator(matViewIndexGeneration);
      } catch (RuntimeException e) {
        // Existing generator is left in place (still replicating the prior definition).
        recordGeneratorCreationFailure(
            matViewIndexGeneration.getGenerationId(), e, "REPLACE_GENERATOR");
        throw e;
      }
      CompletableFuture<Void> shutdownFuture =
          existingGenerator
              .shutdown()
              // Swap the lease even when the old generator's drain fails: skipping it leaves the
              // old generation leader forever while the new one is never registered.
              .exceptionally(
                  throwable -> {
                    LOG.atWarn()
                        .addKeyValue("oldGenerationId", oldGenerationId)
                        .setCause(throwable)
                        .log("Replaced generator failed to shut down; swapping the lease anyway");
                    return null;
                  })
              .thenRun(
                  () -> {
                    if (oldGenerationId.equals(newGenerationId)) {
                      // Same-version replacement (e.g., fell off oplog, Lucene rebuild):
                      // same MaterializedViewGenerationId means the old generator's
                      // InitialSyncQueue entry could still be present (CLOUDP-393734).
                      // Skip lease drop+add (no-op for same genId) and defer
                      // becomeLeader to refreshStatus for both static and dynamic.
                      LOG.atInfo()
                          .addKeyValue("generationId", newGenerationId)
                          .log(
                              "Same generationId replacement, deferring "
                                  + "leadership to refreshStatus");
                    } else {
                      // Different-version replacement: different genIds mean no
                      // InitialSyncQueue collision. Swap lease tracking and activate
                      // leadership immediately.
                      this.leaseManager.drop(oldGenerationId);
                      this.leaseManager.add(matViewIndexGeneration, skipInitialSync);
                      if (this.leaseManager instanceof StaticLeaderLeaseManager) {
                        activateStaticLeadership(
                            newGenerator, matViewIndexGeneration.getGenerationId());
                      } else if (this.leaseManager.isLeader(newGenerationId)) {
                        LOG.atInfo()
                            .addKeyValue("oldGenerationId", oldGenerationId)
                            .addKeyValue("newGenerationId", newGenerationId)
                            .log(
                                "Activating leadership for new generator after lease registration");
                        becomeLeaderSafely(newGenerator, newGenerationId);
                      }
                      // Unregister metrics for the now-replaced generation so the registry
                      // doesn't retain a stale series for a defunct materialized view.
                      try {
                        existingGenerator.getIndexGeneration().getIndex().close();
                      } catch (IOException e) {
                        LOG.atWarn()
                            .addKeyValue("oldGenerationId", oldGenerationId)
                            .setCause(e)
                            .log("Failed to close replaced materialized view index");
                      }
                    }
                  });
      this.pendingShutdowns.put(newGenerationId, shutdownFuture);
      return newGenerator;
    } catch (RuntimeException e) {
      this.pendingShutdowns.remove(newGenerationId);
      throw e;
    }
  }

  /**
   * Transitions a generator from leader to follower mode by shutting down the old generator and
   * replacing it with a new follower generator. This is called when leadership is lost (e.g., due
   * to OCC failure in lease renewal).
   *
   * <p>This respects the ReplicationIndexManager design that generators are not restarted once
   * stopped - instead, we create a new generator in follower mode.
   *
   * <p>Note: Unlike {@link #replaceGenerator}, this does not call leaseManager.add() since the
   * lease already exists, and does not activate leadership since we just lost it.
   *
   * <p>{@code existingGenerator.shutdown()} runs before the slow {@code factory.create()} rather
   * than after it (CLOUDP-434413). {@link ReplicationIndexManager#shutdown()} transitions the
   * generator to {@code SHUT_DOWN} and ends its tenure synchronously, returning a future for the
   * asynchronous drain, so calling it first costs nothing but ends the demoted generator's tenure
   * immediately instead of leaving it able to commit under its now-obsolete fencing token for the
   * duration of the factory call. It must be {@code shutdown()} that ends the tenure and not a
   * separate "end tenure now" step: every guard in {@link ReplicationIndexManager} that stops work
   * from being scheduled is keyed on {@code isShutDown()}, so a generator left live but tenure-less
   * would still enter paths like {@code clearIndex()}, whose {@code
   * TenureFencedMaterializedViewDocumentIndexer} call would then throw inside a {@code
   * Crash.ifThrows} wrapper and halt the JVM.
   *
   * @param uuid the UUID of the materialized view collection
   * @param existingGenerator the existing leader generator to replace
   */
  private synchronized void transitionToFollower(
      UUID uuid, MaterializedViewGenerator existingGenerator) {
    MaterializedViewIndexGeneration matViewIndexGeneration = existingGenerator.getIndexGeneration();
    MaterializedViewGenerationId generationId = matViewIndexGeneration.getGenerationId();
    // Skip if a previous transitionToFollower is still shutting down for this generationId.
    // This can happen when two heartbeat threads both snapshot the same leader generator
    // before either enters this synchronized block.
    if (this.pendingShutdowns.containsKey(generationId)) {
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .log("Shutdown already pending, skipping duplicate transitionToFollower");
      return;
    }
    // Third path that builds a generator, alongside computeGenerator and createAndStoreGenerator.
    // Heartbeat pass 2 already excludes terminal generators, so a retired one should never reach
    // here; the check keeps that from depending on pass 2's guard.
    if (isRetired(generationId)) {
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .log("Skipping follower transition for retired generation");
      return;
    }
    // Block leadership acquisition early, before the slow factory.create() call.
    this.pendingShutdowns.put(generationId, new CompletableFuture<>());
    // Shut down the existing generator first: this ends its tenure synchronously (see javadoc), so
    // it stops committing under its obsolete fencing token before the slow create() below rather
    // than after it. Has to happen whether or not the new generator is created.
    try {
      CompletableFuture<Void> shutdownFuture = existingGenerator.shutdown();
      this.pendingShutdowns.put(generationId, shutdownFuture);
      LOG.atDebug()
          .addKeyValue("generationId", generationId)
          .log("Old generator shutdown initiated (async completion tracked via pendingShutdowns)");
    } catch (Throwable throwable) {
      // Will be retried in the next heartbeat cycle
      this.meterRegistry.counter(GENERATOR_SHUTDOWN_FAILURE_COUNTER_NAME).increment();
      this.pendingShutdowns.remove(generationId);
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .setCause(throwable)
          .log("Old generator shutdown completed exceptionally");
    }
    try {
      MaterializedViewGenerator newGenerator = createManagedGenerator(matViewIndexGeneration);
      // Replace in the map immediately so subsequent operations use the new follower generator.
      this.managedMaterializedViewGenerators.put(uuid, newGenerator);
    } catch (Throwable e) {
      // Do not rethrow: this runs on the leader heartbeat thread, which must keep scheduling. The
      // old generator is left shut down in the map; refreshStatus recreates the follower.
      recordGeneratorCreationFailure(generationId, e, "FACTORY_CREATE_AFTER_LEADERSHIP_LOSS");
    }
  }

  /**
   * Retires a fenced generator instead of allowing it to resync with its obsolete lease version.
   * This releases dynamic leadership before the replacement can be activated.
   *
   * <p>Runs on {@link #lifecycleExecutor}, asynchronously with respect to whatever reported the
   * fencing rejection, so both the manager and the generation's metadata may have moved on by the
   * time this runs: bail out instead of assuming either is still around.
   */
  synchronized void handleFencingRejection(MaterializedViewGenerator fencedGenerator) {
    if (isShutdown()) {
      return;
    }
    MaterializedViewGenerationId generationId =
        fencedGenerator.getIndexGeneration().getGenerationId();
    Optional<UUID> uuid =
        this.matViewMetadataCatalog
            .getMetadataIfPresent(generationId)
            .map(MaterializedViewCollectionMetadata::collectionUuid);
    if (uuid.isEmpty()
        || this.managedMaterializedViewGenerators.get(uuid.get()) != fencedGenerator) {
      return;
    }

    LOG.atWarn()
        .addKeyValue("generationId", generationId)
        .log("Retiring fenced materialized view generator");
    try {
      this.leaseManager.releaseLeadership(generationId);
    } catch (Throwable e) {
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .setCause(e)
          .log("Failed to release fenced materialized view leadership");
    }
    transitionToFollower(uuid.get(), fencedGenerator);
  }

  void scheduleFencingRejection(MaterializedViewGenerator fencedGenerator) {
    // The generator reports this while holding its monitor. Defer acquiring the manager monitor to
    // preserve the manager-before-generator lock order used by transitionToFollower().
    try {
      this.lifecycleExecutor.execute(() -> handleFencingRejection(fencedGenerator));
    } catch (RejectedExecutionException e) {
      // The manager is already shutting down (shutdown() stops lifecycleExecutor before it gets to
      // this generator's own shutdown() call): shutdown() shuts down every managed generator
      // regardless, so there is nothing left here to retire.
      LOG.atInfo()
          .addKeyValue("generationId", fencedGenerator.getIndexGeneration().getGenerationId())
          .log("Manager is shutting down; skipping fencing-rejection retirement");
    }
  }

  private MaterializedViewGenerator createManagedGenerator(
      MaterializedViewIndexGeneration matViewIndexGeneration) {
    MaterializedViewGenerator generator =
        this.matViewGeneratorFactory.create(matViewIndexGeneration);
    if (this.leaseManager.supportsFencingRecovery()) {
      generator.setFencingRejectionHandler(this::scheduleFencingRejection);
    }
    return generator;
  }

  private void recordGeneratorCreationFailure(
      MaterializedViewGenerationId generationId, Throwable failure, String errorType) {
    // Tag the counter so alerts can distinguish call site (errorType) and transient cause
    // (reason). Cardinality is bounded: 3 errorTypes × ~10 MVTE Reasons + NON_TRANSIENT.
    String reason =
        (failure instanceof MaterializedViewTransientException mvte)
            ? mvte.getReason().name()
            : "NON_TRANSIENT";
    this.meterRegistry
        .counter(GENERATOR_CREATION_FAILURE_COUNTER_NAME, "errorType", errorType, "reason", reason)
        .increment();
    LOG.atWarn()
        .addKeyValue("generationId", generationId)
        .addKeyValue("indexId", generationId.indexId)
        .addKeyValue("errorType", errorType)
        .addKeyValue("reason", reason)
        .setCause(failure)
        .log("Materialized view generator creation failed.");
  }

  /**
   * Activates leader mode on the generator for static leader election. This method should only be
   * called when using {@link StaticLeaderLeaseManager}.
   *
   * <p>For static leader election, leadership is determined at startup and never changes, so we
   * activate immediately if this instance is the leader.
   *
   * @deprecated Static leader election is being replaced by the dynamic leader election mechanism
   *     (CLOUDP-373432). This method will be removed when StaticLeaderLeaseManager is deleted.
   */
  @Deprecated
  private void activateStaticLeadership(
      MaterializedViewGenerator generator, MaterializedViewGenerationId generationId) {
    boolean isLeader = this.leaseManager.isLeader(generationId);
    LOG.atInfo()
        .addKeyValue("generationId", generationId)
        .addKeyValue("isLeader", isLeader)
        .log(
            "Creating auto-embedding generator for static leader election (leader mode = {})",
            isLeader);
    if (isLeader) {
      becomeLeaderSafely(generator, generationId);
    }
  }

  /**
   * Calls {@link MaterializedViewGenerator#becomeLeader()}, logging and swallowing any failure
   * (e.g. the generator's tenure indexer rejecting an invalid lease) instead of letting it
   * propagate to the caller.
   *
   * <p>Also releases leadership on failure: {@code becomeLeader()} never sets the generator's
   * {@code isLeader} flag when it throws, but the lease manager can still show this generation as
   * leader-owned (e.g. {@code add()} already added it before this call). Without releasing here,
   * the generation would be stuck marked as owned-but-not-actually-leading forever, since
   * refreshStatus() only attempts acquisition for generations the lease manager reports as
   * acquirable. {@code releaseLeadership()} is a no-op for {@link StaticLeaderLeaseManager}, so
   * this is safe to call from either lease manager.
   */
  private void becomeLeaderSafely(
      MaterializedViewGenerator generator, MaterializedViewGenerationId generationId) {
    try {
      generator.becomeLeader();
    } catch (Throwable becomeLeaderFailure) {
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .setCause(becomeLeaderFailure)
          .log("becomeLeader failed while activating leadership");
      this.leaseManager.releaseLeadership(generationId);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> dropIndex(GenerationId generationId) {
    checkState(!this.shutdown, "cannot call dropIndex() after shutdown()");
    return cleanUpMatViewResources(generationId)
        .thenRun(() -> cleanUpGenerationIdStates(generationId));
  }

  /**
   * Cleans up internal GenerationId'S internal states including states in leaseManager and
   * collection metadata for the given generation id with format "indexID1-f6-u1-a0" with indexID1
   * as indexId.
   *
   * <p>For example: we may have indexID1-f6-u1-a0 and indexID1-f6-u1-a1 for
   * canonicalKey=matview-indexID1-u1, so we keep removing generationIDs in
   * this.activeGenerationIdCatalog.genIdByMatViewGenId until indexID1-f6-u1-a0 and
   * indexID1-f6-u1-a1 are all dropped, then triggers this.matViewMetadataCatalog.removeMetadata for
   * matview-indexID1-u1
   */
  private synchronized void cleanUpGenerationIdStates(GenerationId generationId) {
    // generationId format: indexID-f6-u1-a0, indexID-f6-u1-a1, ....
    // matViewGeneratorId format: matview-indexID1-u1
    String matViewGeneratorId = canonicalKey(generationId);
    // Reference counting GenerationId by matViewGeneratorId, calls leaseManager.drop at the last
    // GenerationId by matViewGeneratorId, and the de-register metadata for matViewGeneratorId in
    // matViewMetadataCatalog
    Set<GenerationId> genIdsByMatViewGeneratorId =
        this.activeGenerationIdCatalog.genIdByMatViewGenId.get(matViewGeneratorId);
    if (genIdsByMatViewGeneratorId == null) {
      return;
    }
    genIdsByMatViewGeneratorId.remove(generationId);
    // No need to clean up lease and metadata when other attempts still reference this generator
    if (!genIdsByMatViewGeneratorId.isEmpty()) {
      return;
    }
    // There is no active generationIds (indexID-f6-u1-a0, indexID-f6-u1-a1) attached to this
    // MaterializedViewGenerator (assuming 1:1 mapping between MaterializedViewGenerationId and
    // MaterializedViewGenerator), ready to clean up MaterializedViewGenerationId in LeaseManager
    // and Catalog.
    var matViewGenerationId = MaterializedViewGenerationId.from(generationId);
    // Don't call leaseManager.dropLease() here, as lease may manage multiple
    // MaterializedViewGenerationIds.
    this.leaseManager.drop(matViewGenerationId);
    this.pendingShutdowns.remove(matViewGenerationId);
    this.retiredGenerations.remove(matViewGenerationId);
    this.activeGenerationIdCatalog.genIdByMatViewGenId.remove(matViewGeneratorId);
    this.matViewMetadataCatalog.removeMetadata(matViewGenerationId);
    // The generator is fully gone (last attempt dropped): release any embeddings its generation
    // still held. matViewGeneratorId is the same canonicalKey the claims are keyed on.
    this.embeddingRetryCache.releaseClaims(matViewGeneratorId, ReleaseTrigger.DROP);
  }

  /**
   * Returns the MV collection UUID {@code generationId} was registered under in {@link
   * ActiveGenerationIdCatalog#genIdByMatViewCollection}, falling back to {@code currentUuid}.
   *
   * <p>The metadata catalog resolves every attempt of a generation to the collection's current
   * UUID. When a stale MV UUID is repaired between attempts, the older attempt and its generator
   * stay keyed by the UUID they were added with; tearing that attempt down under the current UUID
   * would find nothing and leave the old generator in the map until restart.
   */
  private synchronized UUID registeredCollectionUuid(GenerationId generationId, UUID currentUuid) {
    Set<GenerationId> current =
        this.activeGenerationIdCatalog.genIdByMatViewCollection.get(currentUuid);
    // Common case: registered under the current UUID, same as the behavior before this lookup.
    if (current != null && current.contains(generationId)) {
      return currentUuid;
    }
    // Otherwise it was added before the MV UUID changed; find the UUID it is still listed under.
    return this.activeGenerationIdCatalog.genIdByMatViewCollection.entrySet().stream()
        .filter(entry -> entry.getValue().contains(generationId))
        .map(Map.Entry::getKey)
        .findFirst()
        .orElse(currentUuid);
  }

  /**
   * Tears down this mongot's local state for a dropped generation. When this was the last local
   * user of the MV collection (UUID ref-count hits 0), it decides whether to clean up the shared MV
   * collection + lease now, or leave them for the leaderless stale-lease GC. {@code dropIndex} is
   * only reached via the config-driven removal path, which means either:
   *
   * <ul>
   *   <li><b>generation retired</b> (autoEmbed-field update, initial-sync retry, format upgrade) --
   *       the index lives on. Leave the shared collection + lease; cleaning them up is gated on
   *       every mongot flipping off the old version, which only the GC can safely observe.
   *   <li><b>index deletion</b> -- the leader cleans up the index's MV collections and leases now,
   *       including leftovers earlier supersessions left behind for the GC.
   * </ul>
   */
  private synchronized CompletableFuture<Void> cleanUpMatViewResources(GenerationId generationId) {
    var metadata = this.matViewMetadataCatalog.getMetadataIfPresent(generationId);
    if (metadata.isEmpty()) {
      // Not a materialized-view index (e.g. search or plain vector); nothing to drop here.
      return COMPLETED_FUTURE;
    }
    UUID currentUuid = metadata.get().collectionUuid();
    UUID uuid = registeredCollectionUuid(generationId, currentUuid);
    if (!uuid.equals(currentUuid)) {
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("registeredUuid", uuid)
          .addKeyValue("currentUuid", currentUuid)
          .log("Dropping generation under the MV collection UUID it was registered with");
    }
    Set<GenerationId> genIdsByUuid =
        this.activeGenerationIdCatalog.genIdByMatViewCollection.get(uuid);
    if (genIdsByUuid == null) {
      // generationId is dropped twice.
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .addKeyValue("collectionName", metadata.get().collectionName())
          .log("MatView collection already dropped, possible duplicate drop for generationId");
      return COMPLETED_FUTURE;
    }
    genIdsByUuid.remove(generationId);
    if (!genIdsByUuid.isEmpty()) {
      // Other generationIds are still using this matview UUID, no need to drop the collection.
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("collectionName", metadata.get().collectionName())
          .addKeyValue("remainingGenerationIds", new ArrayList<>(genIdsByUuid))
          .log("Other generations still active on this MatView collection, skipping drop");
      return COMPLETED_FUTURE;
    }
    // Last local user of this MV collection: tear down local state below, then decide whether to
    // clean up the shared MV collection + lease now or leave them for the stale-lease GC.
    String leaseKey = metadata.get().collectionName();
    this.activeGenerationIdCatalog.genIdByMatViewCollection.remove(uuid);
    var generator = this.managedMaterializedViewGenerators.remove(uuid);
    var lastMaterializedViewIndexGeneration =
        this.activeGenerationIdCatalog.latestMatViewIndexGenerationByCollection.remove(uuid);
    if (generator == null && lastMaterializedViewIndexGeneration == null) {
      // same UUID should already be dropped.
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .addKeyValue("collectionName", leaseKey)
          .log("Generator and last generation both null, MatView collection may be dropped");
      return COMPLETED_FUTURE;
    }
    var materializedViewIndexGeneration =
        generator != null ? generator.getIndexGeneration() : lastMaterializedViewIndexGeneration;
    // Re-derive leadership from the lease, not the generator: the generator may already be shut
    // down (e.g. a prior sync-source transition) when this config-driven drop arrives.
    boolean wasLeader =
        this.leaseManager.isLeader(materializedViewIndexGeneration.getGenerationId());
    // Stop this mongot's heartbeat now (remove from leaderGenerationIds) so the old lease can
    // expire for GC. Idempotent -- cleanUpGenerationIdStates calls drop() again.
    this.leaseManager.drop(materializedViewIndexGeneration.getGenerationId());
    CompletableFuture<Void> generatorShutdownFuture =
        (generator != null ? generator.shutdown() : COMPLETED_FUTURE)
            .exceptionally(
                throwable -> {
                  LOG.error(
                      "Failed to shutdown generator for {}, ignore it for now.",
                      generationId,
                      throwable);
                  return null;
                });

    // Deletion vs retirement comes from the IndexCatalog: config removes a deleted index from
    // the catalog before dropping its generations, while retirement paths (staged swap,
    // phasing-out, initial-sync retry) leave the entry in place. (Format upgrades remove-then-
    // readd, but only for unloadable-format indexes, which never register MV metadata here.)
    // Retired MVs must stay for the stale-lease GC: retiring is safe only once every mongot has
    // flipped off the old version, which no single mongot can observe.
    boolean indexDeleted = this.indexCatalog.getIndexById(generationId.indexId).isEmpty();

    if (!indexDeleted) {
      // Retiring a generation does not clear checkpoint risk: the index lives on and the
      // definition-update and retry paths keep the MV collection. The one case where we know the
      // collection is gone clears in retireIfCollectionMissing().
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("collectionName", leaseKey)
          .addKeyValue("wasLeader", wasLeader)
          .log(
              "Generation retired while the index lives on; leaving this MV collection and "
                  + "lease for the stale-lease GC instead of dropping now");
      // The lease document stays for the GC, but this node is done with the lease: evict the
      // local copy after the generator finishes shutting down, so a later re-initialization of
      // the same definition re-reads the database instead of stale metadata (a recreated MV
      // collection keeps its name but gets a new UUID).
      return generatorShutdownFuture.thenRun(() -> this.leaseManager.evictCachedLease(leaseKey));
    }

    // The index is gone, so MV documents without a checkpoint no longer matter.
    //
    // Wait for the generator to stop: drop() above already took leadership away, so a commit
    // still running its bulk write (up to 311s in prod) would find isLeader false and re-flag an
    // index that no longer exists. Run it before the leader check below, since the host holding
    // the flag usually lost leadership in the same failure and never reaches the leader-only path.
    CompletableFuture<Void> riskClearedFuture =
        generatorShutdownFuture.thenRun(
            () -> this.leaseManager.clearCheckpointCommitRiskForIndex(generationId.indexId));

    // Index deleted: clean up the shared MV collection and lease now (leader only), plus any
    // leftover sibling leases of this index — a supersession that completed before the deletion
    // left its MV + lease behind for the GC, and cleaning those up now spares the GC's graces.
    if (!wasLeader) {
      // Cache eviction only — never the lease document. The lease document is the GC's only
      // handle on the MV collection, so only a path that just dropped the collection may delete
      // it. Leadership is per-generation and dies with the generation that acquired it: when
      // sibling generations share the lease (a definition revert), the last one out reaches this
      // branch on the very node the document still names as owner, and an owner-side delete here
      // would strand the never-dropped MV. That owner's pair is left for the stale-lease GC,
      // which cleans a deleted index's leases in drop-then-delete order after the graces.
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("collectionName", leaseKey)
          .log(
              "Index deleted; not the leader, leaving shared resource cleanup to the leader "
                  + "or, if no leader remains, to the stale-lease GC");
      return riskClearedFuture.thenRun(() -> this.leaseManager.evictCachedLease(leaseKey));
    }
    LOG.atInfo()
        .addKeyValue("generationId", generationId)
        .addKeyValue("collectionName", leaseKey)
        .log("Index deleted; dropping its materialized view collections and leases");
    // Detached from the returned future: the config monitor blocks on every drop it applies
    // (IndexActions awaits them), and this cleanup retries against the source cluster -- with
    // the mongod unreachable it would stall all config processing behind it. Nothing needs to
    // wait for it: any failure just leaves the lease for the stale-lease GC.
    riskClearedFuture
        .thenComposeAsync(
            ignored ->
                materializedViewIndexGeneration
                    .getIndex()
                    .getWriter()
                    .dropMaterializedViewCollection()
                    .handle(
                        (unusedResult, dropError) -> {
                          boolean dropped = dropError == null;
                          recordCollectionDrop(DROP_SCENARIO_INDEX_DELETION, dropped);
                          return dropped;
                        }))
        .thenAccept(
            mvDropped -> {
              // Leftovers first: the own-lease delete below evicts the leaseKey -> database
              // mapping the leftover scan resolves the lease table through.
              cleanUpLeftoverLeases(generationId.indexId, leaseKey);
              // Drop-before-delete, as in the GC: a surviving MV's lease must stay visible to the
              // lease-driven scanner. No mark either — it wouldn't shorten the delete grace, and
              // the GC marks a deleted index's leases itself.
              if (mvDropped) {
                deleteLeaseOrLeaveForGc(leaseKey);
              } else {
                LOG.atWarn()
                    .addKeyValue("generationId", generationId)
                    .addKeyValue("collectionName", leaseKey)
                    .log(
                        "Failed to drop the deleted index's materialized view collection; "
                            + "leaving its lease for the stale-lease GC");
              }
            })
        .exceptionally(
            throwable ->
                logDetachedCleanupFailure(
                    throwable, generationId, leaseKey, DROP_SCENARIO_INDEX_DELETION));
    return generatorShutdownFuture;
  }

  private Void logDetachedCleanupFailure(
      Throwable throwable, GenerationId generationId, String leaseKey, String scenario) {
    LOG.atWarn()
        .addKeyValue("generationId", generationId)
        .addKeyValue("collectionName", leaseKey)
        .addKeyValue("scenario", scenario)
        .setCause(throwable)
        .log("Materialized view cleanup failed; leaving the rest for the stale-lease GC");
    return null;
  }

  /**
   * Cleans up leases that earlier supersessions of the deleted index left behind for the GC: drops
   * each one's MV collection, then deletes the lease. Any failure just leaves the lease where the
   * GC already finds it. Leases still backing a locally active generation are skipped — their own
   * {@code dropIndex} in this same config update cleans them up. Leader-only; the by-id deletes are
   * idempotent.
   */
  private void cleanUpLeftoverLeases(ObjectId indexId, String droppedLeaseKey) {
    for (LeaseManager.DeletedIndexLease leftover :
        this.leaseManager.findLeasesForDeletedIndex(indexId, droppedLeaseKey)) {
      Lease lease = leftover.lease();
      if (lease.id().equals(droppedLeaseKey) || usedByLocalGeneration(lease)) {
        continue;
      }
      if (leftover.matViewNamespace().isPresent()
          && dropMatViewCollection(
              lease.id(), leftover.matViewNamespace().get(), DROP_SCENARIO_INDEX_DELETION)) {
        deleteLeaseOrLeaveForGc(lease.id());
      }
    }
  }

  /**
   * Deletes a deleted index's lease; a failed delete is left alone — the GC scanner finds the lease
   * on its own and, with the safeguard no longer protecting a deleted index's leases, marks and
   * deletes it after the graces.
   */
  private void deleteLeaseOrLeaveForGc(String leaseKey) {
    if (!this.leaseManager.cleanupLeaseForDeletedIndex(leaseKey)) {
      LOG.atWarn()
          .addKeyValue("leaseKey", leaseKey)
          .log("Could not delete a deleted index's lease; leaving it for the stale-lease GC");
    }
  }

  /**
   * Cleans up a generation's MV collection and lease when the source collection it tails was
   * dropped. The change stream binds to the source collection's uuid, which never returns, so the
   * pair can never ingest again no matter what the config later says — and the config-driven
   * deletion that follows arrives only after this generator's shutdown has released the lease,
   * leaving nobody entitled to delete it (see the non-leader branch of {@code
   * cleanUpMatViewResources}).
   *
   * <p>Runs from the generator's own drop handling, while this node still holds the lease. That
   * ordering is what makes the eager cleanup safe: the lease holder is the only MV writer, and the
   * drop closes its writer first, so the dropped collection cannot be re-created behind us.
   * Best-effort — without leadership, or on any failure, the pair stays where the stale-lease GC
   * finds it after the graces.
   *
   * <p>Deregistering the generation is not best-effort and not leader-only: it must happen on every
   * node that witnesses the drop, before the teardown closes the index. See {@link
   * #deregisterGenerationOfDroppedSourceCollection}.
   */
  private void cleanUpMatViewResourcesForDroppedSourceCollection(
      MaterializedViewIndexGeneration generation) {
    MaterializedViewGenerationId generationId = generation.getGenerationId();
    // Leadership decides who cleans up the shared resources, but nobody should be replicating a
    // collection that no longer exists, so this runs on every node that witnesses the drop.
    deregisterGenerationOfDroppedSourceCollection(generation);
    // The source collection is gone, so no generation of this index can ever index again --
    // clear them all, whether or not we hold the lease or the cleanup below works.
    // TODO(CLOUDP-XXXXX): this can run while a commit is still finishing, which re-flags a
    // collection that is gone. Unlike the index-deletion path there is no shutdown future to
    // wait on here, so it needs its own fix.
    this.leaseManager.clearCheckpointCommitRiskForIndex(generationId.indexId);
    if (!this.leaseManager.isLeader(generationId)) {
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .log(
              "Source collection dropped; not the lease holder, leaving cleanup to the "
                  + "stale-lease GC");
      return;
    }
    var metadata = this.matViewMetadataCatalog.getMetadataIfPresent(generationId);
    if (metadata.isEmpty()) {
      return;
    }
    String leaseKey = metadata.get().collectionName();
    LOG.atInfo()
        .addKeyValue("generationId", generationId)
        .addKeyValue("collectionName", leaseKey)
        .log("Source collection dropped; cleaning up its materialized view collection and lease");
    // Detached: the caller is the generator's lifecycle handling and must not block on mongod
    // I/O. Drop-before-delete, as in the deletion path: a surviving MV's lease must stay
    // visible to the lease-driven GC scanner.
    generation
        .getIndex()
        .getWriter()
        .dropMaterializedViewCollection()
        .handle(
            (unusedResult, dropError) -> {
              boolean dropped = dropError == null;
              recordCollectionDrop(DROP_SCENARIO_SOURCE_COLLECTION_DROP, dropped);
              return dropped;
            })
        .thenAccept(
            mvDropped -> {
              if (mvDropped) {
                deleteLeaseOrLeaveForGc(leaseKey);
              } else {
                LOG.atWarn()
                    .addKeyValue("generationId", generationId)
                    .addKeyValue("collectionName", leaseKey)
                    .log(
                        "Failed to drop the materialized view collection of a dropped source "
                            + "collection; leaving its lease for the stale-lease GC");
              }
            })
        .exceptionally(
            throwable ->
                logDetachedCleanupFailure(
                    throwable, generationId, leaseKey, DROP_SCENARIO_SOURCE_COLLECTION_DROP));
  }

  /**
   * Takes a generation whose source collection is gone out of the buffer that {@link
   * #retryFailedGeneratorCreations()} and {@link #restartReplication()} rebuild generators from.
   *
   * <p>Both rebuild a generator for every buffered generation whose own generator is shut down --
   * exactly the state the teardown after this callback leaves behind. The rebuilt generator would
   * reuse the same, by then closed, index, and would find no checkpoint either, because the
   * checkpoint lives in the lease document this cleanup deletes. It would therefore start an
   * initial sync instead of resuming, fail to resolve the dropped collection, and take the process
   * down committing the resulting status. The rest of this generation's bookkeeping is left alone:
   * the config-driven deletion still finds its generator and unwinds normally.
   */
  private void deregisterGenerationOfDroppedSourceCollection(
      MaterializedViewIndexGeneration generation) {
    // Resolve the key the same way ActiveGenerationIdCatalog#add resolved it when it buffered the
    // generation -- through the metadata catalog, not the index's own accessor -- so that a
    // divergence between the two can never turn this removal into a silent no-op.
    var metadata = this.matViewMetadataCatalog.getMetadataIfPresent(generation.getGenerationId());
    if (metadata.isEmpty()) {
      return;
    }
    UUID matViewCollectionUuid = metadata.get().collectionUuid();
    // Value-matched, and deliberately not under the manager monitor: this runs on the generator's
    // thread while it holds the generator's own lock, and the config-driven cleanup takes those
    // two locks in the opposite order. A newer generation of the same index may already own the
    // entry -- a definition update racing the drop -- and has to keep its place in the buffer.
    @SuppressWarnings("GuardedBy") // ConcurrentHashMap: a single keyed removal is atomic on its own
    boolean deregistered =
        this.activeGenerationIdCatalog.latestMatViewIndexGenerationByCollection.remove(
            matViewCollectionUuid, generation);
    if (deregistered) {
      LOG.atInfo()
          .addKeyValue("generationId", generation.getGenerationId())
          .addKeyValue("collectionUuid", matViewCollectionUuid)
          .log(
              "Source collection dropped; deregistered its generation so the status refresh "
                  + "cannot rebuild a generator on the closed index");
    }
  }

  @Override
  public synchronized CompletableFuture<Void> shutdown() {
    LOG.info("Shutting down.");
    this.shutdown = true;
    prepareForShutdown();

    // Cancel the periodic status refresh task
    this.statusRefreshFuture.cancel(false);

    // Cancel the periodic optime update task
    this.optimeUpdaterFuture.cancel(false);

    // Cancel the periodic heartbeat task
    this.heartbeatFuture.cancel(false);

    // Cancel the periodic stale-lease GC scan
    this.staleLeaseScannerFuture.cancel(false);

    // Shutdown all generators first. Each generator handles its own role-specific cleanup and
    // synchronously transitions to SHUT_DOWN, so later read failures are treated as shutdown noise.
    // For follower mode, the map is empty, so this completes immediately.
    List<CompletableFuture<?>> futures =
        this.managedMaterializedViewGenerators.values().stream()
            .map(MaterializedViewGenerator::shutdown)
            .collect(Collectors.toList());

    // Mirror MongoDbReplicationManager.abortReplicationMongoClients(): close the clients after
    // signaling shutdown but before waiting on the generator futures, so a blocked synchronous
    // driver read can unwind.
    this.matViewGeneratorFactory.abortClients();

    // Need to create a separate executor to run the shutdown tasks, otherwise it may end up running
    // on the indexing executor. As one of the shutdown tasks is shutting down that executor, this
    // will hang forever.
    var shutdownExecutor =
        Executors.fixedSizeThreadPool("mat-view-manager-shutdown", 1, this.meterRegistry);

    return FutureUtils.allOf(futures)
        // Fire and forget, no need to wait for it to complete.
        .thenRunAsync(this.matViewGeneratorFactory::shutdown, shutdownExecutor)
        .thenRunAsync(this.decodingWorkScheduler::shutdown, shutdownExecutor)
        // Several IndexingStrategy keys can map to the same scheduler instance, so shut each
        // distinct one down once rather than once per key.
        .thenRunAsync(
            () ->
                this.indexingWorkSchedulerFactory.getIndexingWorkSchedulers().values().stream()
                    .distinct()
                    .forEach(IndexingWorkScheduler::shutdown),
            shutdownExecutor)
        .thenRunAsync(() -> Executors.shutdownOrFail(this.heartbeatExecutor), shutdownExecutor)
        .thenRunAsync(() -> Executors.shutdownOrFail(this.optimeUpdaterExecutor), shutdownExecutor)
        .thenRunAsync(() -> Executors.shutdownOrFail(this.lifecycleExecutor), shutdownExecutor)
        .thenRunAsync(() -> Executors.shutdownOrFail(this.statusRefreshExecutor), shutdownExecutor)
        .thenRunAsync(
            () -> Executors.shutdownOrFail(this.staleLeaseScannerExecutor), shutdownExecutor)
        // Signal the shutdown executor to clean up, but don't block waiting for it to do so.
        .thenRunAsync(shutdownExecutor::shutdown, shutdownExecutor);
  }

  @Override
  public synchronized boolean isReplicationSupported() {
    return this.isReplicationEnabled;
  }

  /**
   * Refreshes status for all managed indexes where this instance is a follower. Polls status from
   * LeaseManager for each follower index and updates the index status. Also attempts to acquire
   * leadership for any expired leases and transitions generators to follower mode if leadership was
   * lost.
   */
  private void refreshStatus() {
    if (isShutdown() || !isReplicationSupported()) {
      return;
    }
    // Recreate any generator a transient failure left missing or shut down, before the election
    // loop below reads the managed map.
    retryFailedGeneratorCreations();
    // Poll all follower statuses from LeaseManager.
    var pollResult = this.leaseManager.pollFollowerStatuses();

    // Update the local index status and replication optime for all followers.
    this.statusUpdater.copyFollowerStatuses(pollResult);

    // Two-phase pendingShutdowns cleanup: collect entries that are ready (shutdown done + queue
    // clean), then activate deferred leadership and remove. Using two phases instead of removeIf
    // keeps becomeLeader() out of the predicate — if it threw inside removeIf, the entry would
    // never be removed, causing a tight retry loop on every refreshStatus cycle.
    List<MaterializedViewGenerationId> readyForCleanup = new ArrayList<>();
    this.pendingShutdowns.forEach(
        (genId, future) -> {
          if (future.isDone() && !this.matViewGeneratorFactory.hasQueueEntry(genId)) {
            readyForCleanup.add(genId);
          }
        });
    for (MaterializedViewGenerationId genId : readyForCleanup) {
      // Activate deferred leadership from replaceGenerator: the generator is in
      // leaderGenerationIds but was never activated because the queue was dirty.
      try {
        // Retirement releases leadership, so isLeader is normally already false here; the explicit
        // check keeps a retired generation from being re-activated if that ever stops holding.
        if (this.leaseManager.isLeader(genId) && !isRetired(genId)) {
          var gen = getMatViewGenerator(genId);
          if (gen.isPresent() && !gen.get().isLeader()) {
            LOG.atInfo()
                .addKeyValue("generationId", genId)
                .log("Activating deferred leadership from replaceGenerator");
            becomeLeaderSafely(gen.get(), genId);
          }
        }
      } catch (Throwable e) {
        LOG.atError()
            .addKeyValue("generationId", genId)
            .setCause(e)
            .log("Failed to activate deferred leadership, removing from pendingShutdowns");
      }
      this.pendingShutdowns.remove(genId);
      LOG.atInfo()
          .addKeyValue("generationId", genId)
          .log("Shutdown and queue cleanup complete, leadership acquisition unblocked");
    }

    // Dynamic leader election only: attempt to acquire leadership for acquirable leases.
    if (this.leaseManager instanceof DynamicLeaderLeaseManager) {
      List<MaterializedViewGenerationId> skippedForShutdown = new ArrayList<>();
      for (MaterializedViewGenerationId generationId : pollResult.acquirableLeases()) {
        if (this.pendingShutdowns.containsKey(generationId)) {
          skippedForShutdown.add(generationId);
          continue;
        }
        // This host still owns the retired generation's lease row, so it stays acquirable on every
        // poll. Skip before the disqualification branch below, which would warn once per tick.
        if (isRetired(generationId)) {
          continue;
        }
        // Get a fresh snapshot from managedMaterializedViewGenerators instead of the snapshot
        // taken at the start of refreshStatus(). The generator may still be missing due to a
        // race condition (leaseManager.add() called but generator not yet stored), which is
        // handled below - createNewGenerator() will call becomeLeader() when it completes.
        var generator = getMatViewGenerator(generationId);
        if (generator.isEmpty() || TERMINAL_STATES.contains(generator.get().getState())) {
          // For generator is empty, this is a transient race condition: leaseManager.add() was
          // called (adding to followerGenerationIds), but the generator hasn't been stored in the
          // map yet. This is safe because createNewGenerator() checks isLeader() after creating the
          // generator and will call becomeLeader() when it completes.
          LOG.atWarn()
              .addKeyValue("generationId", generationId)
              .addKeyValue("generatorState", generator.map(ReplicationIndexManager::getState))
              .addKeyValue("generatorCreated", generator.isPresent())
              .log("Generator disqualified for leadership acquisition");
          // Terminated generators should be shut down to release resources, should never become
          // leader again. Explicitly shutting down here as fail-safe.
          generator.ifPresent(MaterializedViewGenerator::shutdown);
          continue;
        }
        // The lease manager demoted this generation (that is what made the lease acquirable),
        // but its generator still leads the prior tenure. Acquiring here would re-activate that
        // generator under a fresh lease while its in-flight work holds the prior tenure's pin —
        // the lose-and-reacquire ABA (CLOUDP-437680), reachable only because this acquisition can
        // beat heartbeat pass 2, whose generator.isLeader() && !leaseManager.isLeader() check
        // drains exactly this state. Skip acquisition for the cycle and leave the drain to pass
        // 2; the replacement follower it installs acquires on a later cycle, pinning the new
        // tenure's leaseVersion at beginTenure.
        //
        // Gated on LEASE_HEARTBEAT_PARTIAL_UPDATE (evaluated per tick, so a flip needs no
        // restart). With the flag off, most reacquisitions-while-leading are benign
        // CLOUDP-397647 OCC self-heals: the whole-document renewal demoted a live leader while
        // the lease still named this host unexpired, so no interim leader was possible and
        // re-acquiring is safe — skipping those would convert daily fleet self-heals into full
        // generator replacements for no safety gain. The flag removes that demotion source, so
        // the skip activates exactly when its residual triggers are real takeovers and
        // expiries, where the drain is the correct behavior.
        if (this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
                DynamicFeatureFlags.LEASE_HEARTBEAT_PARTIAL_UPDATE)
            && generator.get().isLeader()) {
          LOG.atInfo()
              .addKeyValue("generationId", generationId)
              .log(
                  "Lease is acquirable but the local generator still leads the prior tenure;"
                      + " skipping acquisition until the heartbeat drains it");
          continue;
        }
        MaterializedViewGenerator generatorBeforeAcquisition = generator.get();
        if (this.leaseManager.tryAcquireLeadership(generationId)) {
          // Synchronize on the manager monitor (same lock as transitionToFollower) to
          // prevent the generator from being replaced between re-read and becomeLeader().
          synchronized (this) {
            var currentGenerator = getMatViewGenerator(generationId);
            if (currentGenerator.isEmpty()
                || TERMINAL_STATES.contains(currentGenerator.get().getState())
                || this.pendingShutdowns.containsKey(generationId)) {
              // Last case: a concurrent transitionToFollower() replaced this generator (and
              // registered its predecessor's shutdown in pendingShutdowns) while we were
              // blocked waiting on this monitor. Do not activate while a predecessor is still
              // draining.
              LOG.atWarn()
                  .addKeyValue("generationId", generationId)
                  .log(
                      "Generator gone, in terminal state, or shutting down after leadership "
                          + "acquisition, releasing lease");
              this.leaseManager.releaseLeadership(generationId);
              continue;
            }
            if (currentGenerator.get() == generatorBeforeAcquisition
                && currentGenerator.get().isLeader()) {
              this.meterRegistry.counter(UNSAFE_LEADERSHIP_REACQUISITION_COUNTER_NAME).increment();
              this.unsafeLeadershipReacquisitionLastEventEpochSeconds.set(
                  System.currentTimeMillis() / 1000);
              LOG.atError()
                  .addKeyValue("generationId", generationId)
                  .log(
                      "Reacquired materialized-view leadership while the prior local generator"
                          + " still leads; in-flight work from the prior tenure may be unsafe");
            }
            LOG.atInfo()
                .addKeyValue("indexId", generationId.indexId)
                .addKeyValue("generationId", generationId)
                .log("Acquired leadership for materialized view, transitioning to leader mode");
            try {
              currentGenerator.get().becomeLeader();
            } catch (Throwable becomeLeaderFailure) {
              LOG.atWarn()
                  .addKeyValue("generationId", generationId)
                  .addKeyValue("indexId", generationId.indexId)
                  .setCause(becomeLeaderFailure)
                  .log(
                      "becomeLeader failed after acquiring lease in refreshStatus; releasing "
                          + "leadership");
              this.leaseManager.releaseLeadership(generationId);
            }
          }
        }
      }
      if (!skippedForShutdown.isEmpty()) {
        LOG.atInfo()
            .addKeyValue("skippedGenerationIds", skippedForShutdown)
            .log("Skipping leadership acquisition — old generators still shutting down");
      }
    }
  }

  private synchronized boolean isShutdown() {
    return this.shutdown;
  }

  /**
   * Records one MV-collection drop attempt on the shared {@code matViewCollectionDrop} counter,
   * tagged by scenario ({@code gc} / {@code indexDeletion}) and outcome. Replaces the GC-only
   * failure counter so drop success/failure is comparable across paths. Each tag combination is
   * registered lazily on first emission, and the dropped collection/lease identity stays in the
   * structured drop logs rather than a (unbounded) label.
   */
  private void recordCollectionDrop(String scenario, boolean success) {
    this.metricsFactory
        .counter(
            MAT_VIEW_COLLECTION_DROP_COUNTER_NAME,
            Tags.of(DROP_SCENARIO_TAG, scenario, DROP_OUTCOME_TAG, success ? "success" : "failure"))
        .increment();
  }

  /**
   * Whether any auto-embedding index generation is currently configured on this node. Synchronized
   * for the catalog's guard; the GC sweep takes the manager lock only for this read.
   */
  private synchronized boolean hasConfiguredAutoEmbeddingIndex() {
    return !this.activeGenerationIdCatalog.genIdByMatViewCollection.isEmpty();
  }

  /**
   * 1 only while GC is actually running: the scanner thread is scheduled and alive, the manager is
   * up, AND {@link DynamicFeatureFlags#MV_GC_ENABLED} is on. The thread is always scheduled (gating
   * is per tick), so the flag has to be part of the value — re-evaluated on every scrape, this
   * tracks the flag with no restart: when the control plane turns it off (propagated by the next
   * conf call), the gauge drops to 0 in lockstep.
   */
  private double gcScannerAliveValue() {
    return !isShutdown()
            && this.staleLeaseScannerFuture != null
            && !this.staleLeaseScannerFuture.isCancelled()
            && !this.staleLeaseScannerFuture.isDone()
            && this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
                DynamicFeatureFlags.MV_GC_ENABLED)
        ? 1.0
        : 0.0;
  }

  /** True if a generation on this mongot still reads {@code lease}'s materialized view. */
  private boolean usedByLocalGeneration(Lease lease) {
    UUID matViewUuid = lease.materializedViewCollectionMetadata().collectionUuid();
    synchronized (this) {
      return this.activeGenerationIdCatalog.genIdByMatViewCollection.containsKey(matViewUuid);
    }
  }

  /**
   * Emits a heartbeat log line for monitoring auto-embedding leader health. Lists all indexes where
   * this instance is the leader.
   */
  private void emitHeartbeat() {
    if (isShutdown() || !isReplicationSupported()) {
      return;
    }

    boolean isDynamicLeader = this.leaseManager instanceof DynamicLeaderLeaseManager;

    // Pass 1 (pre-renewal): release leases for terminal-state generators so this tick does
    // not waste a renewal on them. State-based — independent of the cached isLeader flag,
    // which can stay stale (e.g., async failAndDropIndex does not clear the subclass field).
    // Terminal states are sticky, so the outer state read is observation-safe.
    if (isDynamicLeader) {
      getMatViewGenerators()
          .forEach(
              (uuid, generator) -> {
                var generationId = generator.getIndexGeneration().getGenerationId();

                if (TERMINAL_STATES.contains(generator.getState())
                    && this.leaseManager.isLeader(generationId)) {
                  // Re-read under the manager monitor and verify object identity. Guards
                  // against the same-genId replaceGenerator race (Lucene-only change /
                  // oplog falloff resync swaps the generator instance at this genId):
                  // without this, we could release the new generator's lease.
                  synchronized (this) {
                    var current = getMatViewGenerator(generationId);
                    if (current.isEmpty() || current.get() != generator) {
                      LOG.atWarn()
                          .addKeyValue("generationId", generationId)
                          .log(
                              "Skipped lease release: snapshot generator no longer matches "
                                  + "live map (same-genId replaceGenerator race)");
                    } else if (!retireIfCollectionMissing(generator)) {
                      // Retirement, when it applies, already released the lease and cleared
                      // leader state.
                      LOG.atWarn()
                          .addKeyValue("generationId", generationId)
                          .addKeyValue("state", generator.getState())
                          .log(
                              "Generator in terminal state but lease still held — "
                                  + "stopping local lease renewal so another node can "
                                  + "acquire after lease TTL expiry");
                      this.leaseManager.releaseLeadership(generationId);
                      // Clear cached flag + leader-status gauge in lockstep with the lease.
                      // failAndRetainIndex / shutDownReplicationOnStale* paths never reach
                      // index.close(), so without this the gauge stays at 1 indefinitely.
                      generator.clearLeaderState();
                    }
                  }
                }
              });
    }

    this.leaseManager.heartbeat();

    // Pass 2 (post-renewal): detect lost leadership and transition to follower. Runs AFTER
    // the renewal call so this tick's renewal failures (OCC contention, partition) are
    // visible immediately instead of next tick. Terminal generators were already handled
    // by pass 1 and are excluded here.
    if (isDynamicLeader) {
      getMatViewGenerators()
          .forEach(
              (uuid, generator) -> {
                var generationId = generator.getIndexGeneration().getGenerationId();
                if (!TERMINAL_STATES.contains(generator.getState())
                    && generator.isLeader()
                    && !this.leaseManager.isLeader(generationId)) {
                  // Inside the manager monitor: re-verify object identity AND non-terminal
                  // state. Identity check guards against a same-genId replaceGenerator
                  // landing between snapshot and here (transitionToFollower's unconditional
                  // map put would otherwise overwrite the new generator). State re-check
                  // guards against an async terminal transition between the outer guard and
                  // this point — without it we'd shut down a now-terminal generator that
                  // pass 1 of the next tick will handle correctly anyway.
                  synchronized (this) {
                    var current = getMatViewGenerator(generationId);
                    if (current.isPresent()
                        && current.get() == generator
                        && !TERMINAL_STATES.contains(generator.getState())) {
                      LOG.atInfo()
                          .addKeyValue("indexId", generationId.indexId)
                          .addKeyValue("generationId", generationId)
                          .log("Detected leadership loss, replacing with follower generator");
                      transitionToFollower(uuid, generator);
                    } else {
                      LOG.atWarn()
                          .addKeyValue("generationId", generationId)
                          .addKeyValue("state", generator.getState())
                          .log(
                              "Skipped transitionToFollower: same-genId replaceGenerator "
                                  + "race, or state flipped terminal under us");
                    }
                  }
                }
              });
    }

    // Self-heal the leaderStatus gauge if it was evicted from the registry while the generation is
    // still live (e.g. a sibling matview generation sharing the same Meter.Id tore it down on
    // close). The generator map is keyed by source-collection UUID, so it holds at most one attempt
    // per index; re-registering under the manager monitor and only when the snapshot generator is
    // still the mapped one guarantees a concurrently-replaced (soon-to-be-dropped) attempt can
    // never own the re-registered gauge — same guard as the passes above. No-op when the gauge is
    // present; only a missing gauge is re-registered, against the current owner's state object.
    getMatViewGenerators()
        .forEach(
            (uuid, generator) -> {
              synchronized (this) {
                var current = getMatViewGenerator(generator.getIndexGeneration().getGenerationId());
                if (current.isPresent() && current.get() == generator) {
                  generator.ensureLeaderStatusGaugeRegistered();
                }
              }
            });

    // Log heartbeat for monitoring
    var leaderIndexIds =
        this.leaseManager.getLeaderGenerationIds().stream()
            .map(generationId -> generationId.indexId.toHexString())
            .collect(Collectors.toList());
    if (!leaderIndexIds.isEmpty()) {
      LOG.atInfo()
          .addKeyValue("leaderIndexCount", leaderIndexIds.size())
          .addKeyValue("leaderIndexIds", leaderIndexIds)
          .log("Auto-embedding leader heartbeat");
    }
  }

  /**
   * Drops the MV collection at {@code namespace} via the writer client, recording the attempt on
   * the matViewCollectionDrop counter under {@code scenario}. Missing collections drop as a no-op,
   * keeping retries and concurrent cleanups idempotent.
   */
  private boolean dropMatViewCollection(
      String leaseKey, MongoNamespace namespace, String scenario) {
    try {
      var mongoClient =
          Check.isPresent(
              this.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient(),
              "materializedViewWriterMongoClient");
      // Retries + request metrics like every lease-table op; failure below means failed retries.
      this.matViewWriterOperationExecutor.execute(
          "dropMatViewCollection",
          () ->
              mongoClient
                  .getDatabase(namespace.getDatabaseName())
                  .getCollection(namespace.getCollectionName())
                  .drop());
      recordCollectionDrop(scenario, true);
      LOG.atInfo()
          .addKeyValue("leaseKey", leaseKey)
          .addKeyValue("collectionName", namespace.getCollectionName())
          .addKeyValue("database", namespace.getDatabaseName())
          .addKeyValue("scenario", scenario)
          .log("Dropped materialized view collection");
      return true;
    } catch (Exception | AssertionError e) {
      recordCollectionDrop(scenario, false);
      LOG.atWarn()
          .addKeyValue("leaseKey", leaseKey)
          .addKeyValue("collectionName", namespace.getCollectionName())
          .addKeyValue("scenario", scenario)
          .setCause(e)
          .log("Failed to drop materialized view collection");
      return false;
    }
  }

  public UUID getCollectionUuid(MaterializedViewGenerationId generationId) {
    return this.matViewMetadataCatalog.getMetadata(generationId).collectionUuid();
  }

  public synchronized Optional<MaterializedViewGenerator> getMatViewGenerator(
      MaterializedViewGenerationId generationId) {
    return this.matViewMetadataCatalog
        .getMetadataIfPresent(generationId)
        .map(MaterializedViewCollectionMetadata::collectionUuid)
        .map(this.managedMaterializedViewGenerators::get);
  }

  /**
   * Enables replication and (re)creates a materialized view generator for every active generation.
   *
   * <p>Per-generation generator creation can throw {@link MaterializedViewTransientException} —
   * e.g. a mongod primary stepdown returning {@code 10107 NotPrimary} during the lease read in
   * {@link com.xgen.mongot.index.mongodb.MaterializedViewWriter#getCommitUserData}. Such failures
   * are caught and the affected generation is skipped; {@link #refreshStatus} periodically retries
   * any active generation still missing a generator. Non-transient exceptions still propagate so
   * genuine bugs remain fatal.
   *
   * <p>No-op if the generator factory's sync source URIs are not yet available.
   */
  public synchronized void restartReplication() {
    if (!this.matViewGeneratorFactory.isInitialized()) {
      LOG.atInfo().log(
          "Skipping materialized view replication restart - sync source URIs not yet available.");
      return;
    }
    this.isReplicationEnabled = true;
    this.activeGenerationIdCatalog.latestMatViewIndexGenerationByCollection.forEach(
        this::createAndStoreGenerator);
  }

  /** Outcome of {@link #createAndStoreGenerator}. A deliberate skip is not a retry outcome. */
  private enum GeneratorCreation {
    CREATED,
    SKIPPED_RETIRED,
    SKIPPED_DEREGISTERED,
    FAILED
  }

  /**
   * Creates a generator for the collection and stores it, swallowing transient failures so they
   * don't crash mongot. Rolls back the lease registration on a transient failure and leaves the
   * retry to {@link #refreshStatus}; non-transient exceptions propagate.
   */
  private synchronized GeneratorCreation createAndStoreGenerator(
      UUID uuid, MaterializedViewIndexGeneration matViewIndexGeneration) {
    // restartReplication rebuilds every active generation without consulting
    // needsGeneratorRecreation, so the retirement check is repeated at this shared choke point.
    if (isRetired(matViewIndexGeneration.getGenerationId())) {
      LOG.atInfo()
          .addKeyValue("generationId", matViewIndexGeneration.getGenerationId())
          .log("Skipping generator creation for retired generation");
      return GeneratorCreation.SKIPPED_RETIRED;
    }
    // Presence in the buffer is the mandate to rebuild, so re-read it here: both callers iterate
    // that map, and a weakly consistent iteration can carry an entry that a source-collection drop
    // deregistered in the meantime. Rebuilding then would resurrect a generation whose index the
    // drop teardown has already closed. Ordering makes this read decisive rather than lucky: a
    // rebuild only follows the old generator reaching SHUT_DOWN, which follows the deregistration.
    if (this.activeGenerationIdCatalog.latestMatViewIndexGenerationByCollection.get(uuid)
        != matViewIndexGeneration) {
      LOG.atInfo()
          .addKeyValue("generationId", matViewIndexGeneration.getGenerationId())
          .log("Skipping generator creation for a generation that is no longer registered");
      return GeneratorCreation.SKIPPED_DEREGISTERED;
    }
    try {
      this.managedMaterializedViewGenerators.put(uuid, createNewGenerator(matViewIndexGeneration));
      return GeneratorCreation.CREATED;
    } catch (MaterializedViewTransientException e) {
      this.leaseManager.drop(matViewIndexGeneration.getGenerationId());
      LOG.atWarn()
          .addKeyValue("generationId", matViewIndexGeneration.getGenerationId())
          .addKeyValue("reason", e.getReason())
          .setCause(e)
          .log("Skipping generator creation for index; will retry on next refresh.");
      return GeneratorCreation.FAILED;
    }
  }

  /**
   * Recreates generators for active generations that are missing or were left shut down, so an
   * unreplicated index recovers without operator intervention. No-op unless replication is enabled
   * and the generator factory is initialized.
   */
  private synchronized void retryFailedGeneratorCreations() {
    if (isShutdown()
        || !this.isReplicationEnabled
        || !this.matViewGeneratorFactory.isInitialized()) {
      return;
    }
    this.activeGenerationIdCatalog.latestMatViewIndexGenerationByCollection.forEach(
        (uuid, matViewIndexGeneration) -> {
          if (!needsGeneratorRecreation(uuid, matViewIndexGeneration)) {
            return;
          }
          try {
            MaterializedViewGenerator shutDownGenerator =
                this.managedMaterializedViewGenerators.get(uuid);
            if (shutDownGenerator != null) {
              // The replacement reuses the same materialized-view index, including its gauge.
              // Clear stale leadership before installing the fresh follower.
              shutDownGenerator.clearLeaderState();
            }
            recordGeneratorRetry(createAndStoreGenerator(uuid, matViewIndexGeneration));
          } catch (Exception e) {
            // Last line of defense: a throw out of the status-refresh runnable would permanently
            // cancel scheduleWithFixedDelay. Log and move on; the next tick retries.
            recordGeneratorRetry(GeneratorCreation.FAILED);
            LOG.atError()
                .addKeyValue("collectionUuid", uuid)
                .setCause(e)
                .log("Generator creation retry failed; will retry on next refresh.");
          }
        });
  }

  /** Retirement is intentional, so it is neither a successful retry nor a failed one. */
  private void recordGeneratorRetry(GeneratorCreation outcome) {
    if (outcome == GeneratorCreation.SKIPPED_RETIRED
        || outcome == GeneratorCreation.SKIPPED_DEREGISTERED) {
      return;
    }
    this.meterRegistry
        .counter(
            GENERATOR_CREATION_RETRY_COUNTER_NAME,
            "outcome",
            outcome == GeneratorCreation.CREATED ? "success" : "failure")
        .increment();
  }

  /**
   * True when a collection's active generation has no usable generator: none is stored, or the
   * stored one was left {@code SHUT_DOWN} (e.g. a failed transition to follower) with no shutdown
   * still in flight for it. Runtime-error states (FAILED/FAILED_EXCEEDED) are left alone —
   * recreating them would churn without resolving the underlying failure. Retired generations never
   * need one.
   */
  private synchronized boolean needsGeneratorRecreation(
      UUID uuid, MaterializedViewIndexGeneration matViewIndexGeneration) {
    // Tested by generation id before the generator is read, because shutdownReplication clears the
    // generator: otherwise a retired generation looks like a missing one and every later refresh
    // records a creation-retry failure.
    if (isRetired(matViewIndexGeneration.getGenerationId())) {
      return false;
    }
    var generator = this.managedMaterializedViewGenerators.get(uuid);
    if (generator == null) {
      return true;
    }
    if (retireIfCollectionMissing(generator)) {
      return false;
    }
    return generator.getState() == SHUT_DOWN
        && !this.pendingShutdowns.containsKey(generator.getIndexGeneration().getGenerationId());
  }

  /**
   * Whether this generation has been retired. Membership is the authority: the condition retirement
   * is derived from is not stable, because {@link #refreshStatus()} overwrites the index status
   * from the lease on every tick a generator is not leader.
   */
  private boolean isRetired(MaterializedViewGenerationId generationId) {
    return this.retiredGenerations.contains(generationId);
  }

  /**
   * Retires {@code generator}'s generation if its materialized view collection is missing, so that
   * only a new generation can recover it — never a recreated generator. Idempotent.
   *
   * <p>Releases leadership here rather than leaving it to the heartbeat, because a shutdown between
   * retiring and the next heartbeat would leave the lease manager renewing a lease no generator
   * will ever claim.
   */
  private boolean retireIfCollectionMissing(MaterializedViewGenerator generator) {
    if (!generator.isCollectionMissing()) {
      return false;
    }
    var generationId = generator.getIndexGeneration().getGenerationId();
    if (this.retiredGenerations.add(generationId)) {
      this.meterRegistry.counter(RETIRED_GENERATION_COUNTER_NAME).increment();
      LOG.atWarn()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .log(
              "Materialized view collection is missing on this shard; retiring generation until "
                  + "the index is recreated");
      this.leaseManager.releaseLeadership(generationId);
      generator.clearLeaderState();
      this.leaseManager.clearCheckpointCommitRisk(generationId);
    }
    return true;
  }

  @VisibleForTesting
  synchronized ActiveGenerationIdCatalog getActiveGenerationIdCatalog() {
    return this.activeGenerationIdCatalog;
  }

  @VisibleForTesting
  StaleLeaseGarbageCollector staleLeaseGarbageCollector() {
    return this.staleLeaseGarbageCollector;
  }

  @VisibleForTesting
  MaterializedViewStatusUpdater statusUpdater() {
    return this.statusUpdater;
  }

  /**
   * GenerationId Catalog categorizes all GenerationIDs by two GroupBy keys, MaterializedView
   * Collection UUID and MaterializedViewGenerationId.
   *
   * <p>For example: Given the indexId = 123abc, we have GenerationIds =
   * ['123abc-f6-u0-a0','123abc-f6-u0-a1','123abc-f6-u1-a0','123abc-f6-u1-a1', '123abc-f6-u2-a0']
   * <br>
   * '123abc-f6-u0-a0': initial definition<br>
   * '123abc-f6-u0-a1': retried generation<br>
   * '123abc-f6-u1-a0': definition update with new filter but uses same UUID<br>
   * '123abc-f6-u1-a1': retried generation<br>
   * '123abc-f6-u2-a0': definition update with new UUID<br>
   *
   * <p>genIdByMatViewCollection = {uuid1:
   * ['123abc-f6-u0-a0','123abc-f6-u0-a1','123abc-f6-u1-a0','123abc-f6-u1-a1'], uuid2:
   * ['123abc-f6-u2-a0']}<br>
   * genIdByMatViewGenId = {'matview-123abc-u0': ['123abc-f6-u0-a0','123abc-f6-u0-a1'],
   * 'matview-123abc-u1': ['123abc-f6-u1-a0','123abc-f6-u1-a1'], 'matview-123abc-u2':
   * ['123abc-f6-u2-a0']} <br>
   * latestMatViewIndexGenerationByCollection is just a simpler map for genIdByMatViewCollection,
   * used as MatViewIndexGeneration buffer for RestartReplication.
   */
  static class ActiveGenerationIdCatalog {
    private final MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog;
    final Map<UUID, Set<GenerationId>> genIdByMatViewCollection;
    // String MaterializedViewGenerationId key is also unique ID for MaterializedViewGenerator.
    final Map<String, Set<GenerationId>> genIdByMatViewGenId;
    // Used as buffer for RestartReplication, we can't use this.managedMaterializedViewGenerators to
    // buffer MatViewIndexGeneration since GeneratorFactory is being updated.
    final Map<UUID, MaterializedViewIndexGeneration> latestMatViewIndexGenerationByCollection;

    @VisibleForTesting
    ActiveGenerationIdCatalog(MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog) {
      this.matViewMetadataCatalog = matViewMetadataCatalog;
      this.genIdByMatViewCollection = new ConcurrentHashMap<>();
      this.genIdByMatViewGenId = new ConcurrentHashMap<>();
      this.latestMatViewIndexGenerationByCollection = new ConcurrentHashMap<>();
    }

    /**
     * Registers the generation. Returns the stale MV collection UUID whose generator serves this MV
     * generation, if any; the caller must hand that generator over to the current UUID.
     */
    Optional<UUID> add(AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration) {
      MaterializedViewIndexGeneration matViewIndexGeneration =
          autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration();
      GenerationId generationId = autoEmbeddingIndexGeneration.getGenerationId();
      UUID uuid = this.matViewMetadataCatalog.getMetadata(generationId).collectionUuid();
      // Reference counting by all indexGenerations with all attempts
      String matViewGeneratorId = canonicalKey(generationId);
      Optional<UUID> staleUuid = moveOffStaleUuids(uuid, matViewGeneratorId);
      this.genIdByMatViewCollection
          .computeIfAbsent(uuid, unused -> ConcurrentHashMap.newKeySet())
          .add(generationId);
      this.genIdByMatViewGenId
          .computeIfAbsent(matViewGeneratorId, unused -> ConcurrentHashMap.newKeySet())
          .add(generationId);
      // Track the latest matViewIndexGeneration per collection for restartReplication().
      this.latestMatViewIndexGenerationByCollection.compute(
          uuid,
          (unused, existing) -> {
            if (existing == null) {
              return matViewIndexGeneration;
            }
            long newVersion =
                matViewIndexGeneration.getDefinition().getDefinitionVersion().orElse(0L);
            long existingVersion = existing.getDefinition().getDefinitionVersion().orElse(0L);
            if (newVersion >= existingVersion) {
              return matViewIndexGeneration;
            }
            LOG.atWarn()
                .log(
                    "Received index generation with lower definition"
                        + " version than existing, likely a bug");
            return existing;
          });
      return staleUuid;
    }

    /**
     * Moves this MV generation's attempts off any other MV collection UUID onto {@code uuid}. A
     * repaired stale MV UUID re-keys the collection between attempts; without the move, drop
     * ref-counting and generator retries would keep following the stale UUID. Attempts of other MV
     * generations sharing the stale UUID stay where they are.
     *
     * <p>Returns the stale UUID whose generator serves this MV generation, if any. At most one is
     * expected, since every add moves all of this MV generation's attempts onto the current UUID.
     */
    @VisibleForTesting
    Optional<UUID> moveOffStaleUuids(UUID uuid, String matViewGeneratorId) {
      Map<UUID, Set<GenerationId>> attemptsByStaleUuid = new HashMap<>();
      this.genIdByMatViewCollection.forEach(
          (otherUuid, genIds) -> {
            Set<GenerationId> ours =
                genIds.stream()
                    .filter(id -> canonicalKey(id).equals(matViewGeneratorId))
                    .collect(Collectors.toSet());
            if (!otherUuid.equals(uuid) && !ours.isEmpty()) {
              attemptsByStaleUuid.put(otherUuid, ours);
            }
          });
      List<UUID> handOverFrom = new ArrayList<>();
      attemptsByStaleUuid.forEach(
          (staleUuid, ours) -> {
            moveAttempts(staleUuid, uuid, ours);
            // The generator under a UUID serves its latest generation. Only if that is this MV
            // generation does the generator move with us; otherwise it keeps serving the other one.
            var latest = this.latestMatViewIndexGenerationByCollection.get(staleUuid);
            if (latest != null
                && canonicalKey(latest.getGenerationId()).equals(matViewGeneratorId)) {
              // Drop the retry buffer entry too, or the status refresh would recreate a generator.
              this.latestMatViewIndexGenerationByCollection.remove(staleUuid);
              handOverFrom.add(staleUuid);
            }
            LOG.atInfo()
                .addKeyValue("matViewGeneratorId", matViewGeneratorId)
                .addKeyValue("staleUuid", staleUuid)
                .addKeyValue("currentUuid", uuid)
                .addKeyValue("movedGenerationIds", new ArrayList<>(ours))
                .log("MV collection UUID changed; moved earlier attempts off the stale UUID");
          });
      if (handOverFrom.size() > 1) {
        LOG.atWarn()
            .addKeyValue("matViewGeneratorId", matViewGeneratorId)
            .addKeyValue("staleUuids", handOverFrom)
            .log("Several stale MV collection UUIDs hold this MV generation's generator");
      }
      return handOverFrom.stream().findFirst();
    }

    private void moveAttempts(UUID fromUuid, UUID toUuid, Set<GenerationId> attempts) {
      Set<GenerationId> remaining = this.genIdByMatViewCollection.get(fromUuid);
      if (remaining != null) {
        remaining.removeAll(attempts);
        if (remaining.isEmpty()) {
          this.genIdByMatViewCollection.remove(fromUuid);
        }
      }
      this.genIdByMatViewCollection
          .computeIfAbsent(toUuid, unused -> ConcurrentHashMap.newKeySet())
          .addAll(attempts);
    }
  }

  /**
   * Runs {@code publish} only while {@code generator} is still the generator installed for {@code
   * uuid}. The comparison is by identity, not generation id: a replacement can keep the generation
   * id (Lucene rebuild, attempt replay). Running inside {@code computeIfPresent} makes the check
   * and the write atomic against installation, which {@link #add} and {@link #transitionToFollower}
   * both perform on this map. Without it a generator displaced after the caller's snapshot would
   * still pass its own checks -- {@link MaterializedViewGenerator#shutdown} clears isLeader -- and
   * overwrite the status its successor shares with it.
   *
   * <p>{@code publish} must only touch memory. It runs under this key's map lock, which generator
   * installation also holds while creating and shutting down generators.
   */
  @SuppressWarnings("GuardedBy") // ConcurrentHashMap compute is thread safe
  private void publishIfCurrentGenerator(
      UUID uuid, MaterializedViewGenerator generator, Runnable publish) {
    this.managedMaterializedViewGenerators.computeIfPresent(
        uuid,
        (ignored, current) -> {
          if (current == generator) {
            publish.run();
          }
          return current;
        });
  }

  /**
   * Creates a copy of {@link MaterializedViewManager#managedMaterializedViewGenerators}. Thread
   * safe method.
   */
  @SuppressWarnings("GuardedBy") // iterations through ConcurrentHashMap (copying) are thread safe
  private Map<UUID, MaterializedViewGenerator> getMatViewGenerators() {
    return new HashMap<>(this.managedMaterializedViewGenerators);
  }
}
