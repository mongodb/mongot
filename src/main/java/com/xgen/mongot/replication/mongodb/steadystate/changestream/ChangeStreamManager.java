package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector.ChangeStreamMode;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.CollectionSamplingMongoClient;
import com.xgen.mongot.replication.mongodb.common.CollectionStatsMongoClient;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.HeuristicChangeStreamModeSelector;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
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

  @GuardedBy("this")
  private final Map<GenerationId, ChangeStreamIndexManager> indexManagers;

  @GuardedBy("this")
  private boolean shutdown;

  private ChangeStreamManager(
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      ChangeStreamIndexManagerFactory indexManagerFactory,
      ChangeStreamDispatcher dispatcher,
      ChangeStreamModeSelector modeSelector) {
    this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
    this.indexManagerFactory = indexManagerFactory;
    this.dispatcher = dispatcher;
    this.modeSelector = modeSelector;
    this.indexManagers = new HashMap<>();
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
      boolean enableLifecycleAttributionMetrics) {
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
      ThreadPoolResourceMetrics.create(
              replicationConfig.getReplicationType().resourceAttributionSubsystem)
          .register(modeSelectorExecutor, meterRegistry);
    }

    return getSyncChangeStreamManager(
        meterAndFtdcRegistry,
        sessionRefresher,
        syncBatchMongoClient,
        indexingWorkSchedulerFactory,
        decodingScheduler,
        modeSelector,
        replicationConfig,
        enableLifecycleAttributionMetrics);
  }

  private static ChangeStreamManager getSyncChangeStreamManager(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      SessionRefresher sessionRefresher,
      BatchMongoClient syncBatchMongoClient,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      DecodingWorkScheduler decodingScheduler,
      ChangeStreamModeSelector modeSelector,
      SteadyStateReplicationConfig replicationConfig,
      boolean enableLifecycleAttributionMetrics) {

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
        indexManagerFactoryWithDecodingScheduler(decodingScheduler),
        enableLifecycleAttributionMetrics);
  }

  // TODO(CLOUDP-405327): remove test-only overload once LIFECYCLE_ATTRIBUTION_METRICS rolls out.
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
        false);
  }

  private static ChangeStreamManager createSync(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      ChangeStreamMongoClientFactory syncMongoClientFactory,
      ChangeStreamModeSelector modeSelector,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      SteadyStateReplicationConfig replicationConfig,
      ChangeStreamIndexManagerFactory indexManagerFactory,
      boolean enableLifecycleAttributionMetrics) {
    Check.argIsPositive(
        replicationConfig.getNumConcurrentChangeStreams(), "numConcurrentChangeStreams");
    Check.argIsPositive(
        replicationConfig.getMaxInFlightEmbeddingGetMores(), "maxInFlightEmbeddingGetMores");

    LOG.info("Creating with sync-batch client.");

    NamedScheduledExecutorService executorService =
        Executors.fixedSizeThreadScheduledExecutor(
            replicationConfig.getReplicationType().metricsNamespacePrefix
                + "change-stream-sync-dispatcher",
            replicationConfig.getNumConcurrentChangeStreams(),
            meterAndFtdcRegistry.meterRegistry());

    if (enableLifecycleAttributionMetrics) {
      ThreadPoolResourceMetrics.create(
              replicationConfig.getReplicationType().resourceAttributionSubsystem)
          .register(executorService, meterAndFtdcRegistry.meterRegistry());
    }

    boolean shouldLimitMaxInFlightEmbeddingGetMores =
        replicationConfig.getMaxInFlightEmbeddingGetMores()
            < replicationConfig.getNumConcurrentChangeStreams();

    SyncChangeStreamDispatcher syncDispatcher =
        new SyncChangeStreamDispatcher(
            meterAndFtdcRegistry.meterRegistry(),
            syncMongoClientFactory,
            executorService,
            // Pass in an empty optional if maxInFlightEmbeddingGetMores is greater than or equal to
            // numConcurrentChangeStreams, since this is the same as not limiting the number of
            // in-flight getMores.
            shouldLimitMaxInFlightEmbeddingGetMores
                ? Optional.of(replicationConfig.getMaxInFlightEmbeddingGetMores())
                : Optional.empty(),
            replicationConfig.getReplicationType());

    return new ChangeStreamManager(
        indexingWorkSchedulerFactory, indexManagerFactory, syncDispatcher, modeSelector);
  }

  /**
   * Gracefully shuts down the ChangeStreamManager.
   *
   * @return a future that completes when the ChangeStreamManager has completed shutting down. The
   *     future will only ever complete successfully.
   */
  public synchronized CompletableFuture<Void> shutdown() {
    LOG.info("Shutting down.");

    this.shutdown = true;

    // First shut down the dispatcher thread so no more work gets scheduled.
    this.dispatcher.shutdown();

    List<CompletableFuture<?>> futures =
        this.indexManagers.values().stream()
            .map(ChangeStreamIndexManager::shutdown)
            .collect(Collectors.toList());

    this.modeSelector.shutdown();

    return Crash.because("failed shutting down change stream managers")
        .ifCompletesExceptionally(FutureUtils.allOf(futures));
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

    return lifecycleFuture;
  }

  /**
   * Gracefully shuts down the change stream indexing of the index with the supplied id.
   *
   * @param generationId the id of the index to stop indexing
   * @return a future that completes when the scheduled work for the index has completed. The future
   *     will only ever complete successfully.
   */
  public synchronized CompletableFuture<Void> stop(GenerationId generationId) {
    checkState(
        this.indexManagers.containsKey(generationId),
        "ChangeStreamManager does not contain index %s",
        generationId);
    ChangeStreamIndexManager manager = this.indexManagers.remove(generationId);
    this.modeSelector.remove(generationId);
    return manager.shutdown();
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

  public static ChangeStreamIndexManagerFactory indexManagerFactoryWithDecodingScheduler(
      DecodingWorkScheduler decodingScheduler) {
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
            decodingScheduler);
  }
}
