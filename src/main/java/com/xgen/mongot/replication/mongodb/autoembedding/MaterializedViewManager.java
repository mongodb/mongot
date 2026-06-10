package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog.canonicalKey;
import static com.xgen.mongot.replication.mongodb.MongoDbReplicationManager.getClientSessionRecords;
import static com.xgen.mongot.replication.mongodb.MongoDbReplicationManager.getSyncBatchMongoClient;
import static com.xgen.mongot.replication.mongodb.MongoDbReplicationManager.getSyncSourceHost;
import static com.xgen.mongot.replication.mongodb.ReplicationIndexManager.State.FAILED;
import static com.xgen.mongot.replication.mongodb.ReplicationIndexManager.State.FAILED_EXCEEDED;
import static com.xgen.mongot.replication.mongodb.ReplicationIndexManager.State.SHUT_DOWN;
import static com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig.Type.AUTO_EMBEDDING;
import static com.xgen.mongot.util.Check.checkState;
import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.leasing.DynamicLeaderLeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.StaticLeaderLeaseManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGenerationUtil;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.replication.mongodb.ReplicationIndexManager;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.ClientSessionRecord;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DefaultDocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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

  // TODO(CLOUDP-356241): Make this parameter part of durabilityConfig
  private static final int NUM_COMMITTING_THREADS = 1;
  // TODO(CLOUDP-356241): Make this parameter part of durabilityConfig
  private static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(30);

  public static final String OPTIME_UPDATER_ERROR_COUNTER_NAME = "matViewOptimeUpdaterError";

  public static final String GENERATOR_SHUTDOWN_FAILURE_COUNTER_NAME =
      "matViewGeneratorShutdownFailures";

  public static final String GENERATOR_CREATION_FAILURE_COUNTER_NAME =
      "matViewGeneratorCreationFailures";

  public static final String STATE_LABEL = "state";

  // Sticky terminal states that disqualify a generator from holding a matview lease. Shared
  // by emitHeartbeat (release these leases), refreshStatus (skip acquisition for these), and
  // MaterializedViewGenerator.initReplication (clear leader state on terminal init).
  // STEADY_STATE_SHUT_DOWN is intentionally excluded — it is a transient pre-classification
  // state in handleSteadyStateException and the classifier can transition back to non-terminal
  // for recoverable cases (transient errors, renames, invalidates, resyncs).
  static final Set<ReplicationIndexManager.State> TERMINAL_STATES =
      Set.of(SHUT_DOWN, FAILED, FAILED_EXCEEDED);

  // ==================== Common Fields ====================

  private final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;

  private final AutoEmbeddingMongoClient autoEmbeddingMongoClient;

  private final DecodingWorkScheduler decodingWorkScheduler;

  private final MeterRegistry meterRegistry;

  /** A mapping of materialized view collections to active GenerationIds. */
  @GuardedBy("this")
  private final ActiveGenerationIdCatalog activeGenerationIdCatalog;

  private final NamedScheduledExecutorService commitExecutor;

  private final MetricsFactory metricsFactory;

  @GuardedBy("this")
  private boolean shutdown;

  @GuardedBy("this")
  private boolean isReplicationEnabled;

  /** A mapping of all initialized materialized view generators by IndexID. */
  @GuardedBy("this")
  private final Map<UUID, MaterializedViewGenerator> managedMaterializedViewGenerators;

  /** The LeaseManager for status polling (follower) and lease management (leader). */
  private final LeaseManager leaseManager;

  /** Factory for creating MaterializedViewGenerator instances. */
  private final MaterializedViewGeneratorFactory matViewGeneratorFactory;

  private final MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog;

  // ==================== Index Leader Fields ====================

  /**
   * The Executor that is used by steady state indexing, as well as the MaterializedViewGenerator
   * for scheduling its lifecycle tasks. Used when acting as the leader for indexes.
   *
   * <p>The NamedExecutorService is owned by this MaterializedViewManager.
   */
  private final NamedExecutorService lifecycleExecutor;

  /** Executor for periodic optime updates. Used when acting as leader. */
  private final NamedScheduledExecutorService optimeUpdaterExecutor;

  private final ScheduledFuture<?> optimeUpdaterFuture;

  /** Executor for the leader heartbeat task. Used when acting as leader. */
  private final NamedScheduledExecutorService heartbeatExecutor;

  private final ScheduledFuture<?> heartbeatFuture;

  // ==================== Status Refresh Fields ====================

  /** Executor for periodic status refresh. Leader updates optime; follower polls status. */
  private final NamedScheduledExecutorService statusRefreshExecutor;

  private final ScheduledFuture<?> statusRefreshFuture;

  private final AutoEmbeddingMaterializedViewConfig materializedViewConfig;

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
      NamedScheduledExecutorService commitExecutor,
      NamedScheduledExecutorService heartbeatExecutor,
      NamedScheduledExecutorService statusRefreshExecutor,
      NamedScheduledExecutorService optimeUpdaterExecutor,
      MeterRegistry meterRegistry,
      LeaseManager leaseManager,
      MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig) {
    this(
        lifecycleExecutor,
        indexingWorkSchedulerFactory,
        autoEmbeddingMongoClient,
        decodingWorkScheduler,
        matViewGeneratorFactory,
        commitExecutor,
        heartbeatExecutor,
        statusRefreshExecutor,
        optimeUpdaterExecutor,
        meterRegistry,
        leaseManager,
        matViewMetadataCatalog,
        materializedViewConfig,
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
      NamedScheduledExecutorService commitExecutor,
      NamedScheduledExecutorService heartbeatExecutor,
      NamedScheduledExecutorService statusRefreshExecutor,
      NamedScheduledExecutorService optimeUpdaterExecutor,
      MeterRegistry meterRegistry,
      LeaseManager leaseManager,
      MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
      boolean registerGauges) {
    this.lifecycleExecutor = lifecycleExecutor;
    this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
    this.decodingWorkScheduler = decodingWorkScheduler;
    this.matViewGeneratorFactory = matViewGeneratorFactory;
    this.meterRegistry = meterRegistry;
    this.managedMaterializedViewGenerators = new ConcurrentHashMap<>();
    this.activeGenerationIdCatalog = new ActiveGenerationIdCatalog(matViewMetadataCatalog);
    this.commitExecutor = commitExecutor;
    this.heartbeatExecutor = heartbeatExecutor;
    this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
    this.shutdown = false;
    this.metricsFactory = new MetricsFactory("replication.mongodb", this.meterRegistry);
    this.statusRefreshExecutor = statusRefreshExecutor;
    this.optimeUpdaterExecutor = optimeUpdaterExecutor;
    this.leaseManager = leaseManager;
    this.matViewMetadataCatalog = matViewMetadataCatalog;
    this.isReplicationEnabled = false;
    this.materializedViewConfig = materializedViewConfig;

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

    // Periodic status refresh for all indexes (leader updates optime, follower polls status)
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

    // Periodic optime updates for materialized view indexes.
    // Always scheduled - the method checks each generator's leader status.
    this.optimeUpdaterFuture =
        optimeUpdaterExecutor.scheduleWithFixedDelay(
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
            this.materializedViewConfig.materializedViewOptimeUpdateIntervalMs,
            TimeUnit.MILLISECONDS);

    if (registerGauges) {
      createStateGauges(this, this.metricsFactory);
    }
  }

  // TODO(CLOUDP-360913): Investigate whether we need customized disk monitor
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
      MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient) {
    if (embeddingServiceManagerSupplier.isEmpty()) {
      throw new IllegalArgumentException("EmbeddingServiceManagerSupplier must be provided");
    }
    LOG.info("creating AutoEmbeddingMatViewManager");
    var meterRegistry = meterAndFtdcRegistry.meterRegistry();
    meterRegistry.gauge("materializedView.replication.manager", 1);
    boolean enableLifecycleAttributionMetrics =
        featureFlags.isEnabled(Feature.LIFECYCLE_ATTRIBUTION_METRICS);

    var lifecycleExecutor =
        Executors.fixedSizeThreadPool(
            "materialized-view-lifecycle",
            Math.max(1, Runtime.INSTANCE.getNumCpus() / 4),
            meterRegistry);
    if (enableLifecycleAttributionMetrics) {
      ThreadPoolResourceMetrics.create("autoembedding").register(lifecycleExecutor, meterRegistry);
    }

    var indexingWorkSchedulerFactory =
        IndexingWorkSchedulerFactory.createEmbeddingIndexingSchedulerOnly(
            materializedViewConfig.numIndexingThreads,
            embeddingServiceManagerSupplier.get(),
            matViewMetadataCatalog,
            meterRegistry,
            materializedViewConfig.globalMemoryBudgetHeapPercent,
            materializedViewConfig.perBatchMemoryBudgetHeapPercent,
            enableLifecycleAttributionMetrics);

    var decodingWorkScheduler =
        DecodingWorkScheduler.create(
            materializedViewConfig.numChangeStreamDecodingThreads,
            AUTO_EMBEDDING,
            meterRegistry,
            enableLifecycleAttributionMetrics);

    var commitExecutor =
        Executors.fixedSizeThreadScheduledExecutor(
            "mat-view-commit", NUM_COMMITTING_THREADS, meterRegistry);
    if (enableLifecycleAttributionMetrics) {
      ThreadPoolResourceMetrics.create("autoembedding").register(commitExecutor, meterRegistry);
    }

    var materializedViewGeneratorFactory =
        new MaterializedViewGeneratorFactory(
            rootPath.resolve("autoEmbedding"),
            lifecycleExecutor,
            cursorManager,
            initialSyncConfig,
            meterAndFtdcRegistry,
            featureFlags,
            commitExecutor,
            indexingWorkSchedulerFactory,
            decodingWorkScheduler,
            matViewMetadataCatalog,
            DEFAULT_COMMIT_INTERVAL,
            materializedViewConfig);

    var heartbeatExecutor =
        Executors.singleThreadScheduledExecutor("mat-view-leader-heartbeat", meterRegistry);

    var statusRefreshExecutor =
        Executors.singleThreadScheduledExecutor("mat-view-status-refresh", meterRegistry);

    var optimeUpdaterExecutor =
        Executors.singleThreadScheduledExecutor("mat-view-optime-updater", meterRegistry);

    MaterializedViewManager manager =
        new MaterializedViewManager(
            lifecycleExecutor,
            indexingWorkSchedulerFactory,
            autoEmbeddingMongoClient,
            decodingWorkScheduler,
            materializedViewGeneratorFactory,
            commitExecutor,
            heartbeatExecutor,
            statusRefreshExecutor,
            optimeUpdaterExecutor,
            meterRegistry,
            leaseManager,
            matViewMetadataCatalog,
            materializedViewConfig,
            false); // Don't register gauges/tasks in constructor to avoid this-escape

    // Register gauges after construction is complete
    createStateGauges(manager, manager.metricsFactory);

    return manager;
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
    this.activeGenerationIdCatalog.add(autoEmbeddingIndexGeneration);
    if (!this.isReplicationEnabled) {
      LOG.atInfo().log(
          "Skipping creating materialized view generator because replication is disabled.");
      return;
    }

    // Always create generators for both leader and follower modes.
    // Leader mode: generator runs replication loop and writes to materialized view.
    // Follower mode: generator is passive, status is polled from LeaseManager.
    this.managedMaterializedViewGenerators.compute(
        getCollectionUuid(matViewIndexGeneration.getGenerationId()),
        (ignored, existingGenerator) ->
            computeGenerator(existingGenerator, matViewIndexGeneration));
  }

  public synchronized CompletableFuture<Void> shutdownReplication() {
    prepareForShutdown();

    // Shutdown all generators. Each generator handles its own role-specific cleanup.
    // For follower mode, the map is empty, so this completes immediately.
    List<CompletableFuture<?>> futures =
        this.managedMaterializedViewGenerators.values().stream()
            .map(MaterializedViewGenerator::shutdown)
            .collect(Collectors.toList());
    this.managedMaterializedViewGenerators.clear();

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
      generator = this.matViewGeneratorFactory.create(matViewIndexGeneration);
    } catch (RuntimeException e) {
      // TODO(CLOUDP-398177): add retry mechanism for generator creation failure.
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
      generator.becomeLeader();
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
    boolean wasLeader = this.leaseManager.isLeader(oldGenerationId);
    this.pendingShutdowns.put(newGenerationId, new CompletableFuture<>());
    try {
      MaterializedViewGenerator newGenerator;
      try {
        newGenerator = this.matViewGeneratorFactory.create(matViewIndexGeneration);
      } catch (RuntimeException e) {
        // TODO(CLOUDP-398177): add retry mechanism for generator creation failure.
        recordGeneratorCreationFailure(
            matViewIndexGeneration.getGenerationId(), e, "REPLACE_GENERATOR");
        throw e;
      }
      CompletableFuture<Void> shutdownFuture =
          existingGenerator
              .shutdown()
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
                      } else if (wasLeader) {
                        LOG.atInfo()
                            .addKeyValue("oldGenerationId", oldGenerationId)
                            .addKeyValue("newGenerationId", newGenerationId)
                            .log("Activating leadership for new generator");
                        newGenerator.becomeLeader();
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

  // TODO(CLOUDP-408738): holds the manager monitor across existingGenerator.shutdown(), which
  // can be slow for STEADY_STATE generators and blocks refreshStatus. Defer the shutdown
  // future outside the monitor and track it via pendingShutdowns.
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
    // Block leadership acquisition early, before the slow factory.create() call.
    this.pendingShutdowns.put(generationId, new CompletableFuture<>());
    try {
      MaterializedViewGenerator newGenerator =
          this.matViewGeneratorFactory.create(matViewIndexGeneration);
      // Replace in the map immediately so subsequent operations use the new follower generator.
      this.managedMaterializedViewGenerators.put(uuid, newGenerator);
    } catch (Throwable e) {
      // Do not rethrow: this runs on the leader heartbeat thread, which must keep scheduling.
      // TODO(CLOUDP-398177): add retry mechanism for generator creation failure.
      recordGeneratorCreationFailure(generationId, e, "FACTORY_CREATE_AFTER_LEADERSHIP_LOSS");
    }
    // Have to shut down the existing generator no matter new generator is created or not
    CompletableFuture<Void> shutdownFuture;
    try {
      shutdownFuture = existingGenerator.shutdown();
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
      generator.becomeLeader();
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
    this.activeGenerationIdCatalog.genIdByMatViewGenId.remove(matViewGeneratorId);
    this.matViewMetadataCatalog.removeMetadata(matViewGenerationId);
  }

  /**
   * Handles the drop of a materialized view collection by reference counting UUID. Handles both
   * drop MaterializedView collection and corresponding Lease document
   *
   * <p>Leader mode: Shuts down the generator, drops the materialized view collection and cleans up
   * lease entry.
   *
   * <p>Follower mode: Shuts down the generator and clean up lease entry in memory.
   */
  private synchronized CompletableFuture<Void> cleanUpMatViewResources(GenerationId generationId) {
    var metadata = this.matViewMetadataCatalog.getMetadataIfPresent(generationId);
    if (metadata.isEmpty()) {
      // Not a materialized-view index (e.g. search or plain vector); nothing to drop here.
      return COMPLETED_FUTURE;
    }
    UUID uuid = metadata.get().collectionUuid();
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
    // No other generationIds are using this matview UUID, good to drop the collection.
    LOG.atInfo()
        .addKeyValue("generationId", generationId)
        .addKeyValue("collectionName", metadata.get().collectionName())
        .log("Dropping lease and materialized view collection");
    this.activeGenerationIdCatalog.genIdByMatViewCollection.remove(uuid);
    var generator = this.managedMaterializedViewGenerators.remove(uuid);
    var lastMaterializedViewIndexGeneration =
        this.activeGenerationIdCatalog.latestMatViewIndexGenerationByCollection.remove(uuid);
    if (generator == null && lastMaterializedViewIndexGeneration == null) {
      // same UUID should already be dropped.
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .addKeyValue("collectionName", metadata.get().collectionName())
          .log("Generator and last generation both null, MatView collection may be dropped");
      return COMPLETED_FUTURE;
    }
    var materializedViewIndexGeneration =
        generator != null ? generator.getIndexGeneration() : lastMaterializedViewIndexGeneration;
    // Checks both generator and leaseManager, since we may dropIndex when sync source changes.
    boolean wasLeader =
        this.leaseManager.isLeader(materializedViewIndexGeneration.getGenerationId());
    // Stop heartbeats immediately by removing from leaderGenerationIds. This must happen AFTER
    // capturing wasLeader (so we still know to drop the MV collection) but BEFORE the async
    // cleanup chain (so heartbeats stop while generator shutdown and MV drop are in progress).
    // The call is idempotent — cleanUpGenerationIdStates() will call leaseManager.drop() again,
    // which is a safe no-op on ConcurrentHashMap.
    this.leaseManager.drop(materializedViewIndexGeneration.getGenerationId());
    CompletableFuture<Void> generatorShutdownFuture =
        generator != null ? generator.shutdown() : COMPLETED_FUTURE;
    return generatorShutdownFuture
        .exceptionally(
            throwable -> {
              LOG.error(
                  "Failed to shutdown generator for {}, ignore it for now.",
                  generationId,
                  throwable);
              return null;
            })
        .thenComposeAsync(
            ignored -> {
              if (wasLeader) {
                return materializedViewIndexGeneration
                    .getIndex()
                    .getWriter()
                    .dropMaterializedViewCollection()
                    .exceptionally(
                        throwable -> {
                          LOG.error(
                              "Failed to drop materialized view collection for {}, "
                                  + "ignore it for now.",
                              generationId,
                              throwable);
                          return null;
                        });
              }
              return COMPLETED_FUTURE;
            })
        .thenComposeAsync(ignored -> this.leaseManager.dropLease(metadata.get().collectionName()));
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

    // Shutdown all generators. Each generator handles its own role-specific cleanup.
    // For follower mode, the map is empty, so this completes immediately.
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
        // Fire and forget, no need to wait for it to complete.
        .thenRunAsync(this.matViewGeneratorFactory::shutdown, shutdownExecutor)
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
        .thenRunAsync(() -> Executors.shutdownOrFail(this.statusRefreshExecutor), shutdownExecutor)
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
    // Poll all follower statuses from LeaseManager.
    var pollResult = this.leaseManager.pollFollowerStatuses();

    // Update the local index status for all followers.
    getMatViewGenerators()
        .values()
        .forEach(
            generator -> {
              var generationId = generator.getIndexGeneration().getGenerationId();
              // Check generator.isLeader again before setting.
              if (pollResult.statuses().containsKey(generationId) && !generator.isLeader()) {
                var status = pollResult.statuses().get(generationId);
                generator.getIndexGeneration().getIndex().setStatus(status);
              }
            });

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
        if (this.leaseManager.isLeader(genId)) {
          var gen = getMatViewGenerator(genId);
          if (gen.isPresent() && !gen.get().isLeader()) {
            LOG.atInfo()
                .addKeyValue("generationId", genId)
                .log("Activating deferred leadership from replaceGenerator");
            gen.get().becomeLeader();
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
        if (this.leaseManager.tryAcquireLeadership(generationId)) {
          // Synchronize on the manager monitor (same lock as transitionToFollower) to
          // prevent the generator from being replaced between re-read and becomeLeader().
          synchronized (this) {
            var currentGenerator = getMatViewGenerator(generationId);
            if (currentGenerator.isEmpty()
                || TERMINAL_STATES.contains(currentGenerator.get().getState())) {
              LOG.atWarn()
                  .addKeyValue("generationId", generationId)
                  .log(
                      "Generator gone or in terminal state after leadership acquisition, "
                          + "releasing lease");
              this.leaseManager.releaseLeadership(generationId);
              continue;
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

  /**
   * Periodically updates the maxPossibleReplicationOpTime for all queryable materialized view
   * indexes. This needs to happen separately since this metric is updated only for indexes in the
   * IndexCatalog in ReplicationOptimeUpdater.
   *
   * <p>This applies to both leaders and followers since both need accurate optime information for
   * replication lag reporting (materialized view lag + lucene lag).
   */
  @VisibleForTesting
  void updateMaxReplicationOpTime() {
    if (isShutdown() || !isReplicationSupported()) {
      return;
    }
    try {
      // Use collection resolver client here for querying mongod cluster time to reduce impacts in
      // leaseManager and writer mongoClient
      var mongoClientOpt = this.autoEmbeddingMongoClient.getMaterializedViewResolverMongoClient();
      if (mongoClientOpt.isEmpty()) {
        LOG.warn("No MongotClient is present, skipping max optime update");
        return;
      }
      var opTime = MongoDbReplSetStatus.getReadConcernMajorityOpTime(mongoClientOpt.get());
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
      this.meterRegistry.counter(OPTIME_UPDATER_ERROR_COUNTER_NAME).increment();
    }
  }

  private synchronized boolean isShutdown() {
    return this.shutdown;
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
                    if (current.isPresent() && current.get() == generator) {
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
                    } else {
                      LOG.atWarn()
                          .addKeyValue("generationId", generationId)
                          .log(
                              "Skipped lease release: snapshot generator no longer matches "
                                  + "live map (same-genId replaceGenerator race)");
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
   * are caught and the affected generation is skipped (and its lease-manager registration rolled
   * back so {@link #refreshStatus} doesn't see a follower with no generator). Retry happens on the
   * next conf push that drives another {@code restartReplication}; {@code refreshStatus} itself
   * does not re-attempt generator creation. Non-transient exceptions still propagate so genuine
   * bugs remain fatal.
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
        (uuid, matViewIndexGeneration) -> {
          try {
            this.managedMaterializedViewGenerators.put(
                uuid, createNewGenerator(matViewIndexGeneration));
          } catch (MaterializedViewTransientException e) {
            // Swallow transient failures (e.g. 10107 NotPrimary during mongod stepdown) so they
            // don't propagate to PeriodicConfigMonitor.Crash and halt the JVM. Retry happens on
            // the conf call changes; refreshStatus() does not re-attempt generator creation.
            // createNewGenerator() has already called leaseManager.add() before throwing, so
            // roll that back to avoid leaving a dangling follower entry that refreshStatus()
            // would log "Generator disqualified" against every heartbeat.
            this.leaseManager.drop(matViewIndexGeneration.getGenerationId());
            LOG.atWarn()
                .addKeyValue("generationId", matViewIndexGeneration.getGenerationId())
                .addKeyValue("reason", e.getReason())
                .setCause(e)
                .log("Skipping generator restart for index; will retry on next conf push.");
          }
        });
  }

  @VisibleForTesting
  synchronized ActiveGenerationIdCatalog getActiveGenerationIdCatalog() {
    return this.activeGenerationIdCatalog;
  }

  /**
   * Factory for creating MaterializedViewGenerator instances. All generators are created as
   * followers. The caller is responsible for calling {@link
   * MaterializedViewGenerator#becomeLeader()} to activate leader mode when appropriate.
   */
  static class MaterializedViewGeneratorFactory {
    private final Path resolvedPath;
    private final NamedExecutorService lifecycleExecutor;
    private final MongotCursorManager cursorManager;
    private final MeterAndFtdcRegistry meterAndFtdcRegistry;
    private final FeatureFlags featureFlags;
    private final NamedScheduledExecutorService commitExecutor;
    private final Duration commitInterval;
    private final Duration resyncBackoff;
    private final Duration transientBackoff;
    private final Duration requestRateLimitBackoffMs;
    private final boolean enableNaturalOrderScan;
    private final AutoEmbeddingMaterializedViewConfig materializedViewConfig;
    private final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;
    private final DecodingWorkScheduler decodingWorkScheduler;
    private final InitialSyncConfig initialSyncConfig;
    private final MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog;

    // These fields are initialized in updateSyncSourceConfig(), owned by
    // MaterializedViewGeneratorFactory
    private boolean initialized;
    private Map<String, ClientSessionRecord> clientSessionRecordMap;
    private Optional<BatchMongoClient> syncBatchMongoClient;
    private Optional<InitialSyncQueue> initialSyncQueue;
    private Optional<SteadyStateManager> steadyStateManager;

    MaterializedViewGeneratorFactory(
        Path resolvedPath,
        NamedExecutorService lifecycleExecutor,
        MongotCursorManager cursorManager,
        InitialSyncConfig initialSyncConfig,
        MeterAndFtdcRegistry meterAndFtdcRegistry,
        FeatureFlags featureFlags,
        NamedScheduledExecutorService commitExecutor,
        IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
        DecodingWorkScheduler decodingWorkScheduler,
        MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog,
        Duration commitInterval,
        AutoEmbeddingMaterializedViewConfig materializedViewConfig) {
      this.resolvedPath = resolvedPath;
      this.lifecycleExecutor = lifecycleExecutor;
      this.cursorManager = cursorManager;
      this.meterAndFtdcRegistry = meterAndFtdcRegistry;
      this.featureFlags = featureFlags;
      this.enableNaturalOrderScan = initialSyncConfig.enableNaturalOrderScan();
      this.commitExecutor = commitExecutor;
      this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
      this.decodingWorkScheduler = decodingWorkScheduler;
      this.initialSyncConfig = initialSyncConfig;
      this.matViewMetadataCatalog = matViewMetadataCatalog;
      this.commitInterval = commitInterval;
      this.materializedViewConfig = materializedViewConfig;
      this.resyncBackoff =
          materializedViewConfig.resyncBackoff.orElse(
              ReplicationIndexManager.DEFAULT_RESYNC_BACKOFF);
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
     * MaterializedViewGenerator#becomeLeader()} on the returned generator to activate leader mode
     * and start the replication loop.
     */
    MaterializedViewGenerator create(MaterializedViewIndexGeneration matViewIndexGeneration) {
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
          this.enableNaturalOrderScan);
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
                  this.featureFlags.isEnabled(Feature.LIFECYCLE_ATTRIBUTION_METRICS)));

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
                  Optional.of(this.matViewMetadataCatalog)));
      this.initialized = true;
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

    private ActiveGenerationIdCatalog(
        MaterializedViewCollectionMetadataCatalog matViewMetadataCatalog) {
      this.matViewMetadataCatalog = matViewMetadataCatalog;
      this.genIdByMatViewCollection = new ConcurrentHashMap<>();
      this.genIdByMatViewGenId = new ConcurrentHashMap<>();
      this.latestMatViewIndexGenerationByCollection = new ConcurrentHashMap<>();
    }

    void add(AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration) {
      MaterializedViewIndexGeneration matViewIndexGeneration =
          autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration();
      GenerationId generationId = autoEmbeddingIndexGeneration.getGenerationId();
      UUID uuid = this.matViewMetadataCatalog.getMetadata(generationId).collectionUuid();
      // Reference counting by all indexGenerations with all attempts
      String matViewGeneratorId = canonicalKey(generationId);
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
