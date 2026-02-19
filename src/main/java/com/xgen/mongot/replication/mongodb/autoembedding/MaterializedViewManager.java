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
import com.xgen.mongot.embedding.mongodb.leasing.DynamicLeaderLeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.StaticLeaderLeaseManager;
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

  /** Interval for emitting leader heartbeat log lines for monitoring purposes. */
  private static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofSeconds(30);

  // TODO(CLOUDP-356241): Make this parameter part of materializedViewManagerConfig
  private static final Duration DEFAULT_STATUS_TRACKING_INTERVAL = Duration.ofSeconds(30);

  /** Interval for periodic optime updates. Used when acting as leader. */
  private static final Duration DEFAULT_OPTIME_UPDATE_INTERVAL = Duration.ofSeconds(10);

  public static final String OPTIME_UPDATER_ERROR_COUNTER_NAME = "matViewOptimeUpdaterError";

  public static final String STATE_LABEL = "state";

  // ==================== Common Fields ====================

  private final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;

  private final SyncSourceConfig syncSourceConfig;

  private final BatchMongoClient syncBatchMongoClient;

  private final DecodingWorkScheduler decodingWorkScheduler;

  private final MeterRegistry meterRegistry;

  /** A mapping of materialized view collections to active GenerationIds. */
  @GuardedBy("this")
  private final Map<UUID, Set<GenerationId>> activeGenerationIdByMatViewCollection;

  private final NamedScheduledExecutorService commitExecutor;

  private final MetricsFactory metricsFactory;

  @GuardedBy("this")
  private boolean shutdown;

  /** A mapping of all initialized materialized view generators by IndexID. */
  @GuardedBy("this")
  private final Map<UUID, MaterializedViewGenerator> managedMaterializedViewGenerators;

  /** The LeaseManager for status polling (follower) and lease management (leader). */
  private final LeaseManager leaseManager;

  /** Factory for creating MaterializedViewGenerator instances. */
  private final MaterializedViewGeneratorFactory matViewGeneratorFactory;

  // ==================== Index Leader Fields ====================

  /**
   * The Executor that is used by steady state indexing, as well as the MaterializedViewGenerator
   * for scheduling its lifecycle tasks. Used when acting as the leader for indexes.
   *
   * <p>The NamedExecutorService is owned by this MaterializedViewManager.
   */
  private final NamedExecutorService lifecycleExecutor;

  /**
   * Mapping of sync source host to ClientSessionRecord. Contains the synchronous MongoClient used
   * by initial sync and session refresh systems. Owned by this MaterializedViewManager.
   */
  private final Map<String, ClientSessionRecord> clientSessionRecordMap;

  /**
   * This InitialSyncQueue is used for auto-embedding replication workload only. Used when acting as
   * the leader.
   */
  private final InitialSyncQueue initialSyncQueue;

  /**
   * The SteadyStateManager is used for auto-embedding replication workload only. Used when acting
   * as the leader.
   */
  private final SteadyStateManager steadyStateManager;

  /** Executor for periodic optime updates. Used when acting as leader. */
  private final NamedScheduledExecutorService optimeUpdaterExecutor;

  private final ScheduledFuture<?> optimeUpdaterFuture;

  private final Counter optimeUpdaterErrorCounter;

  /** Executor for the leader heartbeat task. Used when acting as leader. */
  private final NamedScheduledExecutorService heartbeatExecutor;

  private final ScheduledFuture<?> heartbeatFuture;

  // ==================== Status Refresh Fields ====================

  /** Executor for periodic status refresh. Leader updates optime; follower polls status. */
  private final NamedScheduledExecutorService statusRefreshExecutor;

  private final ScheduledFuture<?> statusRefreshFuture;

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
      NamedScheduledExecutorService statusRefreshExecutor,
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
    this.syncSourceConfig = syncSourceConfig;
    this.shutdown = false;
    this.metricsFactory =
        new MetricsFactory("autoembedding.replication.mongodb", this.meterRegistry);
    this.statusRefreshExecutor = statusRefreshExecutor;
    this.optimeUpdaterExecutor = optimeUpdaterExecutor;
    this.leaseManager = leaseManager;
    this.optimeUpdaterErrorCounter = this.meterRegistry.counter(OPTIME_UPDATER_ERROR_COUNTER_NAME);
    createStateGauges(this, this.metricsFactory);
    // Always start heartbeat - it emits heartbeat only for indexes where this instance is leader
    LOG.atInfo()
        .addKeyValue("interval", DEFAULT_HEARTBEAT_INTERVAL)
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
            DEFAULT_HEARTBEAT_INTERVAL.toMillis(),
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
            DEFAULT_STATUS_TRACKING_INTERVAL.toMillis(),
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
            DEFAULT_OPTIME_UPDATE_INTERVAL.toMillis(),
            TimeUnit.MILLISECONDS);
  }

  // TODO(CLOUDP-360913): Investigate whether we need customized disk monitor
  /** Creates a new MaterializedViewManager. */
  @VisibleForTesting
  public static MaterializedViewManager create(
      Path rootPath,
      SyncSourceConfig syncSourceConfig,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
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
            getSyncMaxConnections(materializedViewConfig),
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
            /* This path should be different from the dataPath used in Lucene */
            rootPath.resolve("autoEmbedding"),
            ToggleGate.opened());

    var commitExecutor =
        Executors.fixedSizeThreadScheduledExecutor(
            "mat-view-commit", NUM_COMMITTING_THREADS, meterRegistry);

    var materializedViewGeneratorFactory =
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
            initialSyncConfig.enableNaturalOrderScan());

    var heartbeatExecutor =
        Executors.singleThreadScheduledExecutor("mat-view-leader-heartbeat", meterRegistry);

    var statusRefreshExecutor =
        Executors.singleThreadScheduledExecutor("mat-view-status-refresh", meterRegistry);

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
        materializedViewGeneratorFactory,
        commitExecutor,
        heartbeatExecutor,
        statusRefreshExecutor,
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
   * Adds an index generation to be managed.
   *
   * <p>Leader mode: Performs AutoEmbeddingMatViewGenerator live swapping if input indexGeneration
   * has a higher definition version (user version), or adds to managedMatViewGenerators directly
   * with no matching AutoEmbeddingMatViewGenerator. Otherwise, treat it as no op. Only supports
   * filter field modification in index redefinition use case.
   *
   * <p>Follower mode: Tracks the materialized view generation for status polling.
   */
  @Override
  public synchronized void add(IndexGeneration indexGeneration) {
    checkState(!this.shutdown, "cannot call add() after shutdown()");
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        Check.instanceOf(indexGeneration, AutoEmbeddingIndexGeneration.class);
    UUID uuid = getCollectionUuid(autoEmbeddingIndexGeneration.getGenerationId());
    GenerationId generationId = autoEmbeddingIndexGeneration.getGenerationId();

    // Reference counting by all indexGenerations with all attempts
    this.activeGenerationIdByMatViewCollection
        .computeIfAbsent(uuid, unused -> ConcurrentHashMap.newKeySet())
        .add(generationId);

    MaterializedViewIndexGeneration matViewIndexGeneration =
        autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration();

    // Always create generators for both leader and follower modes.
    // Leader mode: generator runs replication loop and writes to materialized view.
    // Follower mode: generator is passive, status is polled from LeaseManager.
    this.managedMaterializedViewGenerators.compute(
        uuid,
        (ignored, existingGenerator) ->
            computeGenerator(existingGenerator, matViewIndexGeneration, generationId));
  }

  /**
   * Computes the generator for the given materialized view index generation. This method is called
   * by {@link ConcurrentHashMap#compute} to determine the generator to use.
   *
   * @param existingGenerator the existing generator, or null if none exists
   * @param matViewIndexGeneration the materialized view index generation
   * @param generationId the generation ID
   * @return the generator to use
   */
  private MaterializedViewGenerator computeGenerator(
      MaterializedViewGenerator existingGenerator,
      MaterializedViewIndexGeneration matViewIndexGeneration,
      GenerationId generationId) {
    if (existingGenerator == null) {
      return createNewGenerator(matViewIndexGeneration, generationId);
    }

    boolean needsNewGenerator =
        existingGenerator.getIndexGeneration().needsNewMatViewGenerator(matViewIndexGeneration);
    if (needsNewGenerator) {
      return replaceGenerator(existingGenerator, matViewIndexGeneration, generationId);
    } else {
      return reuseGenerator(existingGenerator, matViewIndexGeneration);
    }
  }

  /** Creates a new generator for a new index. */
  private MaterializedViewGenerator createNewGenerator(
      MaterializedViewIndexGeneration matViewIndexGeneration, GenerationId generationId) {
    this.leaseManager.add(matViewIndexGeneration);
    MaterializedViewGenerator generator =
        this.matViewGeneratorFactory.create(matViewIndexGeneration);
    // For static leader election only - activates leader mode immediately since leadership is
    // determined at startup. For dynamic leader election, generators start as followers and
    // leadership is acquired later via refreshStatus() when expired leases are detected.
    if (this.leaseManager instanceof StaticLeaderLeaseManager) {
      activateStaticLeadership(generator, generationId);
    }
    return generator;
  }

  /**
   * Replaces an existing generator with a new one for index redefinition.
   *
   * <p>Note on timing: The new generator is returned immediately (in follower mode) while
   * leaseManager.add() and leadership activation run asynchronously after the old generator shuts
   * down. This is intentional - the new generator is safe in follower mode (no writes), and we must
   * wait for the old generator to fully shutdown before activating leadership. If dropIndex() races
   * with this transition, the generator is removed from the map first, and leaseManager.drop()
   * handles cleanup.
   *
   * <p>Note: shutdown() is guaranteed to complete successfully (never exceptionally) per its
   * contract, so thenRun() will always execute.
   *
   * <p>For dynamic leader election: If the old generator was a leader, the new generator should
   * also become leader immediately since we still own the lease for this index. This ensures that
   * index definition updates (e.g., filter field changes) trigger a new initial sync with the
   * updated field mapping.
   */
  private MaterializedViewGenerator replaceGenerator(
      MaterializedViewGenerator existingGenerator,
      MaterializedViewIndexGeneration matViewIndexGeneration,
      GenerationId generationId) {
    MaterializedViewGenerator newGenerator =
        this.matViewGeneratorFactory.create(matViewIndexGeneration);
    // Capture whether the old generator was a leader BEFORE shutdown.
    // For dynamic leader election, if we were the leader for the old generation,
    // we should also be the leader for the new generation (same index, same lease).
    GenerationId oldGenerationId = existingGenerator.getIndexGeneration().getGenerationId();
    boolean wasLeader = this.leaseManager.isLeader(oldGenerationId);
    existingGenerator
        .shutdown()
        .thenRun(
            () -> {
              this.leaseManager.add(matViewIndexGeneration);
              if (this.leaseManager instanceof StaticLeaderLeaseManager) {
                // For static leader election - see the comments in createNewGenerator().
                activateStaticLeadership(newGenerator, generationId);
              } else if (wasLeader) {
                // For dynamic leader election: if the old generator was a leader, the new
                // generator should also become leader. This is safe because:
                // 1. We still own the lease (lease is per-index, not per-generation)
                // 2. The old generator has been shut down
                // 3. The new generator has the updated index definition with new field mapping
                LOG.atInfo()
                    .addKeyValue("oldGenerationId", oldGenerationId)
                    .addKeyValue("newGenerationId", generationId)
                    .log(
                        "Activating leadership for new generator - "
                            + "old generator was leader, transferring leadership");
                newGenerator.becomeLeader();
              }
            });
    return newGenerator;
  }

  /** Reuses an existing generator when the definition version is the same. */
  private MaterializedViewGenerator reuseGenerator(
      MaterializedViewGenerator existingGenerator,
      MaterializedViewIndexGeneration matViewIndexGeneration) {
    // Same definition version: reuse existing generator.
    // TODO(CLOUDP-366953): Temporary approach to ensure the new index generation points
    // to the same underlying index when re-using the generator.
    matViewIndexGeneration.swapIndex(existingGenerator.getIndexGeneration().getIndex());
    return existingGenerator;
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
   * @param uuid the UUID of the materialized view collection
   * @param existingGenerator the existing leader generator to replace
   */
  private synchronized void transitionToFollower(
      UUID uuid, MaterializedViewGenerator existingGenerator) {
    MaterializedViewIndexGeneration matViewIndexGeneration = existingGenerator.getIndexGeneration();
    MaterializedViewGenerator newGenerator =
        this.matViewGeneratorFactory.create(matViewIndexGeneration);
    // Replace in the map immediately so subsequent operations use the new follower generator.
    this.managedMaterializedViewGenerators.put(uuid, newGenerator);
    // Shutdown the old generator asynchronously. No need to wait or activate leadership.
    existingGenerator.shutdown();
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
      MaterializedViewGenerator generator, GenerationId generationId) {
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
    UUID uuid = getCollectionUuid(generationId);

    // Common logic: reference counting
    if (this.activeGenerationIdByMatViewCollection.containsKey(uuid)) {
      this.activeGenerationIdByMatViewCollection.get(uuid).remove(generationId);
      if (this.activeGenerationIdByMatViewCollection.get(uuid).isEmpty()) {
        this.activeGenerationIdByMatViewCollection.remove(uuid);
        return onDrop(uuid, generationId);
      }
    }
    return COMPLETED_FUTURE;
  }

  /**
   * Handles the drop of a materialized view collection.
   *
   * <p>Leader mode: Shuts down the generator, drops the materialized view collection, and removes
   * from lease manager.
   *
   * <p>Follower mode: Shuts down the generator and removes from lease manager.
   */
  private synchronized CompletableFuture<Void> onDrop(UUID uuid, GenerationId generationId) {
    var generator = this.managedMaterializedViewGenerators.remove(uuid);
    if (generator == null) {
      return this.leaseManager.drop(generationId);
    }

    // Use generator.isLeader() to check the generator's current role state.
    // This is more accurate than leaseManager.isLeader() for dynamic leader election
    // since the generator tracks its own leadership state.
    if (generator.isLeader()) {
      // Leader mode: shutdown generator, drop the materialized view collection, remove from lease
      var matViewWriter =
          Check.instanceOf(
              generator.getIndexGeneration().getIndex().getWriter(), MaterializedViewWriter.class);
      return generator
          .shutdown()
          .thenComposeAsync(ignored -> matViewWriter.dropMaterializedViewCollection())
          .thenComposeAsync(ignored -> this.leaseManager.drop(generationId))
          .exceptionally(
              throwable -> {
                throw new MaterializedViewNonTransientException(throwable);
              });
    } else {
      // Follower mode: shutdown generator and remove from lease
      return generator.shutdown().thenComposeAsync(ignored -> this.leaseManager.drop(generationId));
    }
  }

  @Override
  public synchronized CompletableFuture<Void> shutdown() {
    LOG.info("Shutting down.");
    this.shutdown = true;

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
        .thenRunAsync(() -> Executors.shutdownOrFail(this.statusRefreshExecutor), shutdownExecutor)
        // Signal the shutdown executor to clean up, but don't block waiting for it to do so.
        .thenRunAsync(shutdownExecutor::shutdown, shutdownExecutor);
  }

  @Override
  public boolean isReplicationSupported() {
    return true;
  }

  /**
   * Refreshes status for all managed indexes where this instance is a follower. Polls status from
   * LeaseManager for each follower index and updates the index status. Also attempts to acquire
   * leadership for any expired leases and transitions generators to follower mode if leadership was
   * lost.
   */
  private void refreshStatus() {
    if (isShutdown()) {
      return;
    }
    // Poll all follower statuses from LeaseManager.
    var pollResult = this.leaseManager.pollFollowerStatuses();
    var generators = getMatViewGenerators();

    // Update the local index status for all followers.
    generators
        .values()
        .forEach(
            generator -> {
              var generationId = generator.getIndexGeneration().getGenerationId();
              if (pollResult.statuses().containsKey(generationId)) {
                var status = pollResult.statuses().get(generationId);
                generator.getIndexGeneration().getIndex().setStatus(status);
              }
            });

    // Dynamic leader election only: attempt to acquire leadership for acquirable leases.
    if (this.leaseManager instanceof DynamicLeaderLeaseManager) {
      for (GenerationId generationId : pollResult.acquirableLeases()) {
        if (this.leaseManager.tryAcquireLeadership(generationId)) {
          // Successfully acquired leadership - transition generator to leader mode.
          UUID uuid = getCollectionUuid(generationId);
          var generator = generators.get(uuid);
          if (generator != null) {
            LOG.atInfo()
                .addKeyValue("indexId", generationId.indexId)
                .addKeyValue("generationId", generationId)
                .log("Acquired leadership for materialized view, transitioning to leader mode");
            generator.becomeLeader();
          }
        }
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
    if (isShutdown()) {
      return;
    }
    try {
      var clientSessionRecord =
          this.clientSessionRecordMap.get(getSyncSourceHost(this.syncSourceConfig));
      if (clientSessionRecord == null) {
        LOG.warn("No client session record for sync source, skipping optime update");
        return;
      }
      var opTime =
          MongoDbReplSetStatus.getReadConcernMajorityOpTime(clientSessionRecord.syncMongoClient());
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

  /**
   * Emits a heartbeat log line for monitoring auto-embedding leader health. Lists all indexes where
   * this instance is the leader.
   */
  private void emitHeartbeat() {
    // Delegate to lease manager for lease renewal (no-op for static, renews for dynamic)
    this.leaseManager.heartbeat();

    // Dynamic leader election only: detect generators that lost leadership and replace them
    // with new follower generators. This handles the case where leadership was lost in
    // DynamicLeaderLeaseManager (e.g., due to failed lease renewal).
    if (this.leaseManager instanceof DynamicLeaderLeaseManager) {
      getMatViewGenerators()
          .forEach(
              (uuid, generator) -> {
                var generationId = generator.getIndexGeneration().getGenerationId();
                if (generator.isLeader() && !this.leaseManager.isLeader(generationId)) {
                  LOG.atInfo()
                      .addKeyValue("indexId", generationId.indexId)
                      .addKeyValue("generationId", generationId)
                      .log("Detected leadership loss, replacing with follower generator");
                  transitionToFollower(uuid, generator);
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

  // TODO(CLOUDP-360195): Extract destination Materialized View collection UUID from
  // GenerationId, AutoEmbeddingIndexGenerationFactory should implement compatibility
  // check to decide whether to create a new Materialized View collection for new auto-embedding
  // index definition version
  public static UUID getCollectionUuid(GenerationId generationId) {
    return UUID.nameUUIDFromBytes(generationId.indexId.toByteArray());
  }

  /**
   * Factory for creating MaterializedViewGenerator instances. All generators are created as
   * followers. The caller is responsible for calling {@link
   * MaterializedViewGenerator#becomeLeader()} to activate leader mode when appropriate.
   */
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
          this.initialSyncQueue,
          this.steadyStateManager,
          matViewIndexGeneration,
          matViewIndex,
          indexer,
          committer,
          this.requestRateLimitBackoffMs,
          this.meterRegistry,
          this.featureFlags,
          this.enableNaturalOrderScan);
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
