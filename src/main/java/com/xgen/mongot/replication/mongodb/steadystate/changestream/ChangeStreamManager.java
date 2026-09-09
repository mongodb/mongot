package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector.ChangeStreamMode;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.CollectionSamplingMongoClient;
import com.xgen.mongot.replication.mongodb.common.CollectionStatsMongoClient;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.HeuristicChangeStreamModeSelector;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.common.SteadyStateSchedulingMethod;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ChangeStreamManager manages the lifecycle of the change stream tailing process for indexes in
 * steady state indexing.
 *
 * <p>At run time there is intended to only be a single ChangeStreamManager.
 */
public class ChangeStreamManager {

  private static final Logger LOG = LoggerFactory.getLogger(ChangeStreamManager.class);

  private final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;
  private final ChangeStreamIndexManagerFactory indexManagerFactory;
  private final ChangeStreamDispatcher dispatcher;
  private final ChangeStreamModeSelector modeSelector;
  private final Optional<ActivityObserverFactory> activityObserverFactory;

  @GuardedBy("this")
  private final Map<GenerationId, ChangeStreamIndexManager> indexManagers;

  /** The per-database activity observers, present only when the observer feature is enabled. */
  @GuardedBy("this")
  private final Map<String, ChangeStreamActivityObserver> observersByDatabase;

  /** The observer database each index registered with, so stop can find its observer. */
  @GuardedBy("this")
  private final Map<GenerationId, String> observerDatabaseByIndex;

  @GuardedBy("this")
  private boolean shutdown;

  /** Creates the activity observer for one database when its first index is added. */
  @FunctionalInterface
  interface ActivityObserverFactory {
    ChangeStreamActivityObserver create(String databaseName);
  }

  private ChangeStreamManager(
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      ChangeStreamIndexManagerFactory indexManagerFactory,
      ChangeStreamDispatcher dispatcher,
      ChangeStreamModeSelector modeSelector,
      Optional<ActivityObserverFactory> activityObserverFactory) {
    this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
    this.indexManagerFactory = indexManagerFactory;
    this.dispatcher = dispatcher;
    this.modeSelector = modeSelector;
    this.activityObserverFactory = activityObserverFactory;
    this.indexManagers = new HashMap<>();
    this.observersByDatabase = new HashMap<>();
    this.observerDatabaseByIndex = new HashMap<>();
    this.shutdown = false;
  }

  public static ChangeStreamManager create(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      SessionRefresher sessionRefresher,
      com.mongodb.client.MongoClient syncMongoClient,
      BatchMongoClient syncBatchMongoClient,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      DecodingWorkScheduler decodingScheduler,
      SteadyStateReplicationConfig replicationConfig,
      boolean enableLifecycleAttributionMetrics,
      boolean enableChangeStreamPriorityScheduling,
      boolean enableChangeStreamActivityObserver) {
    Check.argIsPositive(
        replicationConfig.getChangeStreamQueryMaxTimeMs(), "changeStreamQueryMaxTimeMs");
    Check.argIsPositive(
        replicationConfig.getChangeStreamCursorMaxTimeSec(), "changeStreamCursorMaxTimeSec");

    Optional<ChangeStreamMode> modeSelectorOverride =
        replicationConfig
            .getEnableChangeStreamProjection()
            .map(enable -> enable ? ChangeStreamMode.INDEXED_FIELDS : ChangeStreamMode.ALL_FIELDS);

    var meterRegistry = meterAndFtdcRegistry.meterRegistry();

    NamedScheduledExecutorService modeSelectorExecutor =
        Executors.singleThreadScheduledExecutor(
            replicationConfig.getReplicationType().metricsNamespacePrefix
                + "change-stream-mode-selector",
            meterRegistry);
    ChangeStreamModeSelector modeSelector =
        new HeuristicChangeStreamModeSelector(
            new CollectionStatsMongoClient(syncMongoClient),
            new CollectionSamplingMongoClient(syncMongoClient),
            modeSelectorExecutor,
            modeSelectorOverride,
            new MetricsFactory("indexing.changeStreamModeSelector", meterRegistry));

    if (enableLifecycleAttributionMetrics) {
      meterAndFtdcRegistry
          .threadPoolResourceBindings()
          .forSubsystem(replicationConfig.getReplicationType().resourceAttributionSubsystem)
          .register(modeSelectorExecutor, meterRegistry);
    }

    Optional<ActivityObserverFactory> activityObserverFactory =
        enableChangeStreamActivityObserver
            ? Optional.of(
                databaseName ->
                    ChangeStreamActivityObserver.create(
                        databaseName,
                        syncMongoClient,
                        sessionRefresher,
                        meterAndFtdcRegistry,
                        Duration.ofSeconds(
                            replicationConfig.getChangeStreamInactivityThresholdSec()),
                        replicationConfig.getReplicationType()))
            : Optional.empty();

    return getSyncChangeStreamManager(
        meterAndFtdcRegistry,
        sessionRefresher,
        syncBatchMongoClient,
        indexingWorkSchedulerFactory,
        decodingScheduler,
        modeSelector,
        replicationConfig,
        enableLifecycleAttributionMetrics,
        enableChangeStreamPriorityScheduling,
        activityObserverFactory);
  }

  private static ChangeStreamManager getSyncChangeStreamManager(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      SessionRefresher sessionRefresher,
      BatchMongoClient syncBatchMongoClient,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      DecodingWorkScheduler decodingScheduler,
      ChangeStreamModeSelector modeSelector,
      SteadyStateReplicationConfig replicationConfig,
      boolean enableLifecycleAttributionMetrics,
      boolean enableChangeStreamPriorityScheduling,
      Optional<ActivityObserverFactory> activityObserverFactory) {

    ChangeStreamMongoClientFactory syncMongoClientFactory =
        ChangeStreamMongoClientFactory.create(
            sessionRefresher,
            syncBatchMongoClient,
            modeSelector,
            meterAndFtdcRegistry,
            replicationConfig);

    return createSync(
        meterAndFtdcRegistry,
        syncMongoClientFactory,
        modeSelector,
        indexingWorkSchedulerFactory,
        replicationConfig,
        indexManagerFactoryWithDecodingScheduler(
            decodingScheduler, replicationConfig.getAutoEmbeddingMaterializedViewConfig()),
        enableLifecycleAttributionMetrics,
        enableChangeStreamPriorityScheduling,
        activityObserverFactory);
  }

  @VisibleForTesting
  static ChangeStreamManager createSync(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      ChangeStreamMongoClientFactory syncMongoClientFactory,
      ChangeStreamModeSelector modeSelector,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      SteadyStateReplicationConfig replicationConfig,
      ChangeStreamIndexManagerFactory indexManagerFactory) {
    return createSync(
        meterAndFtdcRegistry,
        syncMongoClientFactory,
        modeSelector,
        indexingWorkSchedulerFactory,
        replicationConfig,
        indexManagerFactory,
        true,
        false,
        Optional.empty());
  }

  @VisibleForTesting
  static ChangeStreamManager createSync(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      ChangeStreamMongoClientFactory syncMongoClientFactory,
      ChangeStreamModeSelector modeSelector,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      SteadyStateReplicationConfig replicationConfig,
      ChangeStreamIndexManagerFactory indexManagerFactory,
      Optional<ActivityObserverFactory> activityObserverFactory) {
    return createSync(
        meterAndFtdcRegistry,
        syncMongoClientFactory,
        modeSelector,
        indexingWorkSchedulerFactory,
        replicationConfig,
        indexManagerFactory,
        true,
        false,
        activityObserverFactory);
  }

  @VisibleForTesting
  static ChangeStreamManager createSync(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      ChangeStreamMongoClientFactory syncMongoClientFactory,
      ChangeStreamModeSelector modeSelector,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      SteadyStateReplicationConfig replicationConfig,
      ChangeStreamIndexManagerFactory indexManagerFactory,
      boolean enableChangeStreamPriorityScheduling) {
    return createSync(
        meterAndFtdcRegistry,
        syncMongoClientFactory,
        modeSelector,
        indexingWorkSchedulerFactory,
        replicationConfig,
        indexManagerFactory,
        true,
        enableChangeStreamPriorityScheduling,
        Optional.empty());
  }

  private static ChangeStreamManager createSync(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      ChangeStreamMongoClientFactory syncMongoClientFactory,
      ChangeStreamModeSelector modeSelector,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      SteadyStateReplicationConfig replicationConfig,
      ChangeStreamIndexManagerFactory indexManagerFactory,
      boolean enableLifecycleAttributionMetrics,
      boolean enableChangeStreamPriorityScheduling,
      Optional<ActivityObserverFactory> activityObserverFactory) {
    Check.argIsPositive(
        replicationConfig.getNumConcurrentChangeStreams(), "numConcurrentChangeStreams");
    Check.argIsPositive(
        replicationConfig.getMaxInFlightEmbeddingGetMores(), "maxInFlightEmbeddingGetMores");

    LOG.info("Creating with sync-batch client.");

    ChangeStreamTaskExecutor taskExecutor =
        createTaskExecutor(
            meterAndFtdcRegistry,
            replicationConfig,
            enableLifecycleAttributionMetrics,
            enableChangeStreamPriorityScheduling);

    ChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            meterAndFtdcRegistry.meterRegistry(),
            syncMongoClientFactory,
            taskExecutor,
            replicationConfig.getReplicationType(),
            replicationConfig.getRetryThrottledGetMoreOnSameCursor());

    return new ChangeStreamManager(
        indexingWorkSchedulerFactory,
        indexManagerFactory,
        syncDispatcher,
        modeSelector,
        activityObserverFactory);
  }

  /**
   * Builds the {@link ChangeStreamTaskExecutor} that determines how getMore tasks are scheduled.
   *
   * <p>Priority scheduling ({@code enableChangeStreamPriorityScheduling}) only changes behavior for
   * {@link CommonReplicationConfig.Type#DEFAULT} when {@link
   * SteadyStateReplicationConfig#getSteadyStateSchedulingMethod()} selects a variant other than
   * {@link SteadyStateSchedulingMethod#FIFO}, where it swaps the plain scheduled executor for a
   * {@link ChangeStreamDualQueueExecutor}. Otherwise (including for {@link
   * CommonReplicationConfig.Type#AUTO_EMBEDDING}) the scheduled-executor path below is used, which
   * yields an {@link AutoEmbeddingExecutor} when a max-in-flight-embedding-getMores cap applies and
   * a {@link FifoExecutor} when it does not.
   *
   * <p>Known gap: {@link ChangeStreamDualQueueExecutor} has no concept of the
   * max-in-flight-embedding-getMores cap. Legacy, non-materialized-view auto-embedding indexes
   * ({@code type:TEXT}) still run on this {@link CommonReplicationConfig.Type#DEFAULT} path today,
   * tracked for removal under CLOUDP-426030), so enabling {@code
   * enableChangeStreamPriorityScheduling} on a {@code DEFAULT} deployment that still has one of
   * these indexes will silently remove the concurrency cap on its embedding getMores.
   */
  private static ChangeStreamTaskExecutor createTaskExecutor(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      SteadyStateReplicationConfig replicationConfig,
      boolean enableLifecycleAttributionMetrics,
      boolean enableChangeStreamPriorityScheduling) {
    String executorName =
        replicationConfig.getReplicationType().metricsNamespacePrefix
            + "change-stream-sync-dispatcher";

    SteadyStateSchedulingMethod schedulingMethod =
        replicationConfig.getSteadyStateSchedulingMethod();
    if (enableChangeStreamPriorityScheduling
        && replicationConfig.getReplicationType() == CommonReplicationConfig.Type.DEFAULT
        && schedulingMethod != SteadyStateSchedulingMethod.FIFO) {
      registerSchedulingConfigGauge(
          meterAndFtdcRegistry.meterRegistry(),
          replicationConfig,
          schedulingMethod,
          enableChangeStreamPriorityScheduling);
      ChangeStreamDualQueueExecutor dualQueueExecutor =
          new ChangeStreamDualQueueExecutor(
              executorName,
              replicationConfig.getNumConcurrentChangeStreams(),
              meterAndFtdcRegistry.meterRegistry(),
              createPriorityStrategy(schedulingMethod, replicationConfig),
              schedulingMethod,
              replicationConfig.getSteadyStatePriorityRatio());
      if (enableLifecycleAttributionMetrics) {
        meterAndFtdcRegistry
            .threadPoolResourceBindings()
            .forSubsystem(replicationConfig.getReplicationType().resourceAttributionSubsystem)
            .register(dualQueueExecutor.getDelegate(), meterAndFtdcRegistry.meterRegistry());
      }
      return dualQueueExecutor;
    }

    // Report FIFO as the scheduling method for non-dualQueue tasks
    registerSchedulingConfigGauge(
        meterAndFtdcRegistry.meterRegistry(),
        replicationConfig,
        SteadyStateSchedulingMethod.FIFO,
        enableChangeStreamPriorityScheduling);

    NamedScheduledExecutorService scheduledExecutor =
        Executors.fixedSizeThreadScheduledExecutor(
            executorName,
            replicationConfig.getNumConcurrentChangeStreams(),
            meterAndFtdcRegistry.meterRegistry());
    if (enableLifecycleAttributionMetrics) {
      meterAndFtdcRegistry
          .threadPoolResourceBindings()
          .forSubsystem(replicationConfig.getReplicationType().resourceAttributionSubsystem)
          .register(scheduledExecutor, meterAndFtdcRegistry.meterRegistry());
    }

    boolean shouldLimitMaxInFlightEmbeddingGetMores =
        replicationConfig.getMaxInFlightEmbeddingGetMores()
            < replicationConfig.getNumConcurrentChangeStreams();
    return shouldLimitMaxInFlightEmbeddingGetMores
        ? new AutoEmbeddingExecutor(
            meterAndFtdcRegistry.meterRegistry(),
            scheduledExecutor,
            Optional.of(replicationConfig.getMaxInFlightEmbeddingGetMores()))
        : new FifoExecutor(scheduledExecutor);
  }

  /**
   * Registers an info gauge surfacing the steady-state scheduling settings this process is running.
   */
  private static void registerSchedulingConfigGauge(
      MeterRegistry meterRegistry,
      SteadyStateReplicationConfig replicationConfig,
      SteadyStateSchedulingMethod effectiveSchedulingMethod,
      boolean prioritySchedulingFeatureFlag) {
    String namespace =
        replicationConfig.getReplicationType().metricsNamespacePrefix + "replication.steadyState";
    new MetricsFactory(namespace, meterRegistry)
        .numGauge(
            "schedulingConfig",
            Tags.of(
                "steadyStateSchedulingMethod",
                effectiveSchedulingMethod.name(),
                "steadyStatePriorityRatio",
                String.valueOf(replicationConfig.getSteadyStatePriorityRatio()),
                "steadyStatePriorityOptimeThresholdSeconds",
                String.valueOf(replicationConfig.getSteadyStatePriorityOptimeThresholdSeconds()),
                "prioritySchedulingFeatureFlag",
                prioritySchedulingFeatureFlag ? "enabled" : "disabled"))
        .set(1);
  }

  /**
   * Builds the {@link ChangeStreamPriorityStrategy} for the given {@link
   * SteadyStateSchedulingMethod}. Only called when {@code schedulingMethod} is not {@link
   * SteadyStateSchedulingMethod#FIFO}.
   */
  private static ChangeStreamPriorityStrategy createPriorityStrategy(
      SteadyStateSchedulingMethod schedulingMethod,
      SteadyStateReplicationConfig replicationConfig) {
    switch (schedulingMethod) {
      case HAD_DOCUMENTS:
        return new HadDocumentsPriorityStrategy();
      case BATCH_DATA_SIZE:
        return new BatchDataSizePriorityStrategy(
            BatchDataSizePriorityStrategy.DEFAULT_HIGH_PRIORITY_BATCH_SIZE_BYTES);
      case OPTIME_LAG:
        return new OptimeLagPriorityStrategy(
            replicationConfig.getSteadyStatePriorityOptimeThresholdSeconds());
      case FIFO:
        throw new IllegalStateException(
            "createPriorityStrategy should not be called for scheduling method: "
                + schedulingMethod);
    }
    throw new AssertionError("unreachable: " + schedulingMethod);
  }

  /**
   * Gracefully shuts down the ChangeStreamManager.
   *
   * @return a future that completes when the ChangeStreamManager has completed shutting down. The
   *     future will only ever complete successfully.
   */
  public CompletableFuture<Void> shutdown() {
    LOG.info("Shutting down.");

    List<ChangeStreamActivityObserver> observersToClose;
    CompletableFuture<Void> result;
    synchronized (this) {
      this.shutdown = true;

      // First shut down the dispatcher thread so no more work gets scheduled.
      this.dispatcher.shutdown();

      List<CompletableFuture<?>> futures =
          this.indexManagers.values().stream()
              .map(ChangeStreamIndexManager::shutdown)
              .collect(Collectors.toList());

      this.modeSelector.shutdown();

      // Snapshot the observers so they can be closed after releasing the monitor: close() blocks
      // on the read loop draining, so it must not run while the manager lock is held.
      observersToClose = List.copyOf(this.observersByDatabase.values());
      this.observersByDatabase.clear();
      this.observerDatabaseByIndex.clear();

      result =
          Crash.because("failed shutting down change stream managers")
              .ifCompletesExceptionally(FutureUtils.allOf(futures));
    }
    observersToClose.forEach(ChangeStreamActivityObserver::close);
    return result;
  }

  /** Add an index to be managed by the ChangeStreamManager and begin indexing. */
  public synchronized CompletableFuture<Void> add(
      GenerationId generationId,
      DocumentIndexer documentIndexer,
      IndexDefinition indexDefinition,
      ChangeStreamResumeInfo resumeInfo,
      Consumer<ChangeStreamResumeInfo> resumeInfoUpdater,
      IndexMetricsUpdater indexMetricsUpdater,
      boolean removeMatchCollectionUuid)
      throws SteadyStateException {
    LOG.atInfo()
        .addKeyValue("indexId", generationId.indexId)
        .addKeyValue("generationId", generationId)
        .addKeyValue("namespace", resumeInfo.getNamespace())
        .log("Adding index to be managed by ChangeStreamManager");

    checkState(
        !this.indexManagers.containsKey(generationId),
        "ChangeStreamManager already contains index %s",
        generationId);

    if (this.shutdown) {
      throw SteadyStateException.createShutDown();
    }

    CompletableFuture<Void> lifecycleFuture = new CompletableFuture<>();
    MongoNamespace namespace = resumeInfo.getNamespace();
    if (!namespace.getCollectionName().equals(indexDefinition.getLastObservedCollectionName())) {
      // After a rename, it's possible that mongot is restarted before the definition is persisted.
      // So we also update the collection name in the definition here.
      LOG.atInfo()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .addKeyValue("renamedFrom", indexDefinition.getLastObservedCollectionName())
          .addKeyValue("renamedTo", namespace.getCollectionName())
          .log("Collection name in the definition of index is changed.");
      indexDefinition.setLastObservedCollectionName(namespace.getCollectionName());
    }

    ChangeStreamIndexManager indexManager =
        this.indexManagerFactory.create(
            indexDefinition,
            this.indexingWorkSchedulerFactory.getIndexingWorkScheduler(indexDefinition),
            documentIndexer,
            namespace,
            resumeInfoUpdater,
            indexMetricsUpdater,
            lifecycleFuture,
            generationId);

    this.indexManagers.put(generationId, indexManager);
    this.modeSelector.register(generationId, indexDefinition, namespace);
    this.dispatcher.add(
        indexDefinition, generationId, resumeInfo, indexManager, removeMatchCollectionUuid);

    if (this.activityObserverFactory.isPresent()) {
      try {
        String databaseName = namespace.getDatabaseName();
        ChangeStreamActivityObserver observer =
            this.observersByDatabase.computeIfAbsent(
                databaseName, this.activityObserverFactory.get()::create);
        observer.register(generationId, namespace, indexDefinition.getCollectionUuid());
        this.observerDatabaseByIndex.put(generationId, databaseName);
      } catch (RuntimeException e) {
        // The observer is purely observational; a failure to create or register it must never
        // fail the index add. The index simply runs unobserved.
        LOG.atWarn()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .setCause(e)
            .log("Failed to register index with the activity observer; continuing without it");
      }
    }

    return lifecycleFuture;
  }

  /**
   * Gracefully shuts down the change stream indexing of the index with the supplied id.
   *
   * @param generationId the id of the index to stop indexing
   * @return a future that completes when the scheduled work for the index has completed. The future
   *     will only ever complete successfully.
   */
  public CompletableFuture<Void> stop(GenerationId generationId) {
    @Var ChangeStreamActivityObserver observerToClose = null;
    CompletableFuture<Void> result;
    synchronized (this) {
      checkState(
          this.indexManagers.containsKey(generationId),
          "ChangeStreamManager does not contain index %s",
          generationId);
      ChangeStreamIndexManager manager = this.indexManagers.remove(generationId);
      this.modeSelector.remove(generationId);

      String observerDatabase = this.observerDatabaseByIndex.remove(generationId);
      if (observerDatabase != null) {
        ChangeStreamActivityObserver observer = this.observersByDatabase.get(observerDatabase);
        if (observer != null && observer.deregister(generationId)) {
          // The database has no managed indexes left, so its observer is no longer needed. It is
          // closed after releasing the monitor, because close blocks on the read loop draining.
          this.observersByDatabase.remove(observerDatabase);
          observerToClose = observer;
        }
      }
      result = manager.shutdown();
    }
    if (observerToClose != null) {
      observerToClose.close();
    }
    return result;
  }

  @TestOnly
  @VisibleForTesting
  Optional<Integer> getEmbeddingAvailablePermits() {
    if (this.dispatcher instanceof SyncChangeStreamDispatcher syncChangeStreamDispatcher) {
      return syncChangeStreamDispatcher.getEmbeddingAvailablePermits();
    } else {
      return Optional.empty();
    }
  }

  @TestOnly
  @VisibleForTesting
  Optional<Class<? extends ChangeStreamTaskExecutor>> getTaskExecutorClass() {
    if (this.dispatcher instanceof SyncChangeStreamDispatcher syncChangeStreamDispatcher) {
      return Optional.of(syncChangeStreamDispatcher.getTaskExecutorClass());
    } else {
      return Optional.empty();
    }
  }

  public static ChangeStreamIndexManagerFactory indexManagerFactoryWithDecodingScheduler(
      DecodingWorkScheduler decodingScheduler) {
    return indexManagerFactoryWithDecodingScheduler(decodingScheduler, Optional.empty());
  }

  private static ChangeStreamIndexManagerFactory indexManagerFactoryWithDecodingScheduler(
      DecodingWorkScheduler decodingScheduler,
      Optional<AutoEmbeddingMaterializedViewConfig> autoEmbeddingMaterializedViewConfig) {
    return (indexDefinition,
        indexingWorkScheduler,
        documentIndexer,
        namespace,
        resumeInfoUpdater,
        indexMetricsUpdater,
        externalFuture,
        generationId) ->
        DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
            indexDefinition,
            indexingWorkScheduler,
            documentIndexer,
            namespace,
            resumeInfoUpdater,
            indexMetricsUpdater,
            externalFuture,
            generationId,
            decodingScheduler,
            autoEmbeddingMaterializedViewConfig);
  }
}
