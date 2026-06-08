package com.xgen.mongot.config.manager;

import static com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGenerationFactory.isAutoEmbeddingResolutionFailed;
import static com.xgen.mongot.util.Check.checkArg;
import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.DynamicFeatureFlagJournal;
import com.xgen.mongot.config.backup.DynamicFeatureFlagJournalWriter;
import com.xgen.mongot.config.backup.JournalWriter;
import com.xgen.mongot.config.manager.metrics.GroupedIndexGenerationMetrics;
import com.xgen.mongot.config.manager.metrics.IndexGenerationStateMetrics;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexFactory;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGenerationProducer;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.lifecycle.LifecycleManager;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.replication.InitialSyncStatus;
import com.xgen.mongot.replication.ReplicationStatus;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultConfigManager implements a ConfigManager that owns the lifecycle of Indexes, and manages
 * their replication and cursors.
 *
 * <p>Methods here need to be synchronized because {@link DefaultConfigManager#close()} might be
 * called from a shutdown hook thread.
 */
public class DefaultConfigManager implements ConfigManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultConfigManager.class);
  private static final Duration REPLICATION_MANAGER_SHUTDOWN_TIMEOUT = Duration.ofMinutes(3);

  /**
   * DefaultConfigManager owns the config state and may delegate actions on it to others.
   *
   * <p>configState should only be modified by one thread at a time (particularly shutting its
   * members down).
   */
  @GuardedBy("this")
  public final ConfigState configState;

  private volatile ImmutableList<IndexInformation> cachedIndexInfo;

  private volatile ImmutableMap<ObjectId, List<IndexGenerationStateMetrics>>
      cachedGroupedIndexGenerationStateMetrics;

  protected final MetricsFactory metricsFactory;

  protected final Gate initialSyncGate;
  protected final Gate replicationGate;

  final FeatureFlags featureFlags;

  @GuardedBy("this")
  private boolean closed;

  @GuardedBy("this")
  private boolean replicationInitialized;

  @GuardedBy("this")
  private Instant replicationStatusChangeTimestamp;

  protected DefaultConfigManager(
      ConfigState configState,
      MeterRegistry meterRegistry,
      ReplicationStateMonitor replicationStateMonitor,
      FeatureFlags featureFlags) {
    this.configState = configState;
    this.metricsFactory = new MetricsFactory("configManager", meterRegistry);

    this.closed = false;
    this.replicationInitialized = false;
    this.initialSyncGate = replicationStateMonitor.getInitialSyncGate();
    this.replicationGate = replicationStateMonitor.getReplicationGate();
    this.featureFlags = featureFlags;
    this.replicationStatusChangeTimestamp = Instant.now();

    updateIndexInfos();
  }

  /**
   * Constructs a new initialized DefaultConfigManager.
   *
   * <p>Initialization performs the following steps synchronously:
   *
   * <ol>
   *   <li>Creates and drops new indexes as required by an existing config journal
   *   <li>Stages upgrades for live indexes with outdated format versions
   * </ol>
   *
   * <p>DefaultConfigManager takes ownership of the IndexCatalog, MongotCursorManager and
   * LifecycleManager.
   *
   * <p>Though most initialization tasks are performed by {@code initialize}, under certain
   * scenarios involving outdated index format versions, certain indexes' definitions may "revert"
   * to their live version, causing them to differ from the latest control plane conf call. For this
   * reason, it is required to immediately follow up a call to {@code initialize} with one to {@code
   * update}.
   */
  public static DefaultConfigManager initialize(
      IndexCatalog indexCatalog,
      Optional<Set<ObjectId>> desiredIndexIds,
      InitializedIndexCatalog initializedIndexCatalog,
      MongotCursorManager cursorManager,
      IndexFactory indexFactory,
      Optional<MaterializedViewIndexFactory> materializedViewIndexFactory,
      Path configJournalPath,
      MeterRegistry meterRegistry,
      LifecycleManager lifecycleManager,
      ReplicationStateMonitor replicationStateMonitor,
      FeatureFlags featureFlags,
      Path dynamicFeatureFlagsJournalPath,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    ConfigState configState =
        ConfigState.create(
            new StagedIndexes(),
            indexCatalog,
            new PhasingOutIndexes(),
            initializedIndexCatalog,
            cursorManager,
            indexFactory,
            materializedViewIndexFactory,
            lifecycleManager,
            new JournalWriter(configJournalPath),
            meterRegistry,
            featureFlags,
            dynamicFeatureFlagRegistry);

    return initialize(
        configState,
        desiredIndexIds,
        configJournalPath,
        replicationStateMonitor,
        featureFlags,
        meterRegistry,
        dynamicFeatureFlagsJournalPath);
  }

  /** Only used for testing. Use the above method instead. */
  @VisibleForTesting
  public static DefaultConfigManager initialize(
      ConfigState configState,
      Optional<Set<ObjectId>> desiredIndexIds,
      Path configJournalPath,
      ReplicationStateMonitor replicationStateMonitor,
      FeatureFlags featureFlags,
      MeterRegistry meterRegistry,
      Path dynamicFeatureFlagsJournalPath) {

    Crash.because("failed to initialize from config journal")
        .ifThrows(
            () ->
                initializeFromExistingJournal(
                    configState, configJournalPath, desiredIndexIds, meterRegistry));

    Crash.because("failed to upgrade index format versions")
        .ifThrows(() -> IndexFormatVersionUpgrader.upgradeAndDrop(configState));

    DynamicFeatureFlagJournalWriter dynamicFeatureFlagJournalWriter =
        new DynamicFeatureFlagJournalWriter(dynamicFeatureFlagsJournalPath);
    try {
      initializeDynamicFeatureFlagsFromJournal(configState, dynamicFeatureFlagJournalWriter);
    } catch (Exception e) {
      LOG.error("failed to initialize feature flags from config journal", e);
    }

    startLifecycle(configState);

    return new DefaultConfigManager(
        configState, meterRegistry, replicationStateMonitor, featureFlags);
  }

  /**
   * Adds all the indexes in configState to the configState's lifecycle manager.
   *
   * <p>Prior to this point no indexes should have been added to the lifecycle manager.
   */
  protected static void startLifecycle(ConfigState configState) {
    checkArg(
        configState.phasingOut.getSize() == 0,
        "phasingOut indexes should have been dropped on startup, but were present");

    configState.indexCatalog.getIndexes().stream()
        .filter(indexGeneration -> !isAutoEmbeddingResolutionFailed(indexGeneration))
        .forEach(indexGeneration -> configState.getLifecycleManager().add(indexGeneration));

    configState.staged.getIndexes().stream()
        .filter(indexGeneration -> !isAutoEmbeddingResolutionFailed(indexGeneration))
        .forEach(indexGeneration -> configState.getLifecycleManager().add(indexGeneration));
  }

  /**
   * When replication is restarted after shutdown, adds all the indexes in configState to be
   * replicated via the configState's lifecycle manager.
   *
   * <p>Prior to this point no indexes should have been added to the replication manager.
   */
  private static void restartReplication(ConfigState configState) {
    configState.getLifecycleManager().restartReplication();
  }

  /**
   * Initialize feature flags with a feature flag journal when initial conf call fails. If initial
   * confcall succeeds it writes confcall's dynamic feature flags back to journal.
   */
  protected static void initializeDynamicFeatureFlagsFromJournal(
      ConfigState configState, DynamicFeatureFlagJournalWriter writer)
      throws IOException, BsonParseException {

    Optional<DynamicFeatureFlagJournal> optionalFeatureFlagJournal =
        DynamicFeatureFlagJournal.fromFileIfExists(writer.dynamicFeatureFlagJournalPath());
    if (!configState.dynamicFeatureFlagRegistry.isInitializedByInitialConfCall()) {
      LOG.info("Initializing configState featureflags from dynamicFeatureflagJournal");
      optionalFeatureFlagJournal.ifPresent(
          dynamicFeatureFlagJournal ->
              configState.dynamicFeatureFlagRegistry.updateDynamicFeatureFlags(
                  dynamicFeatureFlagJournal.dynamicFeatureFlags()));
    } else {
      LOG.info("Writing dynamicFeatureFlags from initialConfCall to dynamicFeatureflagJournal");
      writer.persist(
          new DynamicFeatureFlagJournal(configState.dynamicFeatureFlagRegistry.snapshot()));
    }
  }

  /** Initialize state with a config journal if one was written before. */
  protected static void initializeFromExistingJournal(
      ConfigState configState,
      Path configJournalPath,
      Optional<Set<ObjectId>> desiredIndexIds,
      MeterRegistry meterRegistry)
      throws IOException,
          BsonParseException,
          Invariants.InvariantException,
          InvalidAnalyzerDefinitionException {
    LOG.info("Initializing");

    Optional<ConfigJournalV1> optionalConfigJournal =
        ConfigJournalV1.fromFileIfExists(configJournalPath);

    // If no journal had been written before, there's nothing to initialize.
    if (optionalConfigJournal.isEmpty()) {
      LOG.info("No config journal found, nothing to initialize");
    } else {
      ConfigInitializer.initialize(
          configState, optionalConfigJournal.get(), desiredIndexIds, meterRegistry);
    }

    // Notify configState that all indexes are added from config journal.
    configState.postInitializationFromConfigJournal();
  }

  @Override
  public synchronized void update(
      List<VectorIndexDefinition> vectorDefinitions,
      List<SearchIndexDefinition> searchDefinitions,
      List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions,
      Set<UUID> directMongodCollectionSet) {
    ensureOpen("update");

    // no dynamic feature flag update is needed since this code path only serves community and
    // localdev.
    Crash.because("failed to update config")
        .ifThrows(
            () ->
                updateCycle(
                    vectorDefinitions,
                    searchDefinitions,
                    analyzerDefinitions,
                    directMongodCollectionSet));

    // Update our cache of index information.
    updateIndexInfos();
  }

  /**
   * Restarts replication if the sync source has changed; otherwise delegates to {@link
   * #handleDiskBasedReplicationRestart()}. Ensures replication is restarted at most once per cycle.
   *
   * <p>On a sync source change, a {@link
   * com.xgen.mongot.replication.mongodb.MongoDbNoOpReplicationManager} is installed when
   * single-host URIs are not yet available (see {@link
   * com.xgen.mongot.util.mongodb.SyncSourceConfig#hasReplicationUrisAvailable()}) or disk usage
   * exceeds the pause threshold. Replication stays disabled until the next sync source change.
   */
  @Override
  public synchronized void handleReplicationAndSyncSourceUpdate(
      SyncSourceConfig newSyncSourceConfig) {
    ensureOpen("handleReplicationAndSyncSourceUpdate");
    Optional<SyncSourceConfig> currentConfig =
        this.configState.getLifecycleManager().getSyncSourceConfig();
    boolean syncSourceUpdated =
        currentConfig.isEmpty() || !currentConfig.get().equals(newSyncSourceConfig);
    if (syncSourceUpdated) {
      shutdownAndRestartReplication(newSyncSourceConfig);
    } else if (!newSyncSourceConfig.hasReplicationUrisAvailable()) {
      // Sync source config is missing required replication URIs.
      // Don't try to restart replication and wait for the next update loop.
      LOG.info("Replication URIs unavailable, skipping disk-based restart check");
    } else {
      handleDiskBasedReplicationRestart();
    }
  }

  protected final synchronized void updateCycle(
      List<VectorIndexDefinition> vectorDefinitions,
      List<SearchIndexDefinition> searchDefinitions,
      List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions,
      Set<UUID> directMongodCollectionSet)
      throws IOException, InvalidAnalyzerDefinitionException, Invariants.InvariantException {
    List<IndexDefinitionGenerationProducer> producers =
        IndexDefinitionGenerationProducer.createProducers(
            vectorDefinitions, searchDefinitions, analyzerDefinitions);

    // we first modify our state to reflect the desired index definitions, then manage our staged
    // and phasing out indexes.
    DesiredConfigStateUpdater.update(
        this.configState,
        vectorDefinitions,
        searchDefinitions,
        analyzerDefinitions,
        producers,
        this.metricsFactory);

    if (this.featureFlags.isEnabled(
        Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)) {
      // Retries failed initial sync indexes.
      IndexInitialSyncRecovery.retryFailedInitialSyncIndexes(
          this.configState, IndexActions.withReplication(this.configState));

    }
    // hot swaps staged indexes that reached steady state.
    StagedIndexesSwapper.swapReady(this.configState, this.featureFlags, this.metricsFactory);

    // stage new index attempts for indexes that can be recovered.
    IndexRecoveryStager.stageRecoveryAttempts(
        this.configState, directMongodCollectionSet, this.featureFlags);

    // drops phasing out indexes that are no longer used.
    PhasingOutIndexesDropper.dropUnused(
        this.configState, IndexActions.withReplication(this.configState));
  }

  @Override
  public synchronized void close() {
    LOG.info("Shutting down.");

    this.closed = true;
    this.configState.cursorManager.close();

    shutdown();

    CollectionUtils.concat(
            this.configState.staged.getIndexes(),
            this.configState.indexCatalog.getIndexes(),
            this.configState.phasingOut.getIndexes())
        .forEach(
            indexGen ->
                Crash.because("failed to close index")
                    .ifThrows(
                        () -> {
                          synchronized (this) {
                            var initializedIndex =
                                this.configState.initializedIndexCatalog.getIndex(
                                    indexGen.getGenerationId());
                            if (initializedIndex.isPresent()) {
                              initializedIndex.get().close();
                            } else {
                              GenerationId generationId = indexGen.getGenerationId();
                              LOG.atWarn()
                                  .addKeyValue("indexId", generationId.indexId)
                                  .addKeyValue("generationId", generationId)
                                  .log("Not closing initialized index, not yet initialized");
                            }
                          }
                        }));

    this.configState.indexFactory.close();
    this.configState.materializedViewIndexFactory.ifPresent(MaterializedViewIndexFactory::close);
  }

  @Override
  public void refreshIndexInfos() {
    updateIndexInfos();
  }

  @Override
  public List<IndexInformation> getIndexInfos() {
    return this.cachedIndexInfo;
  }

  @Override
  public synchronized ReplicationStatus getReplicationStatus() {
    return new ReplicationStatus(
        Instant.now(),
        this.replicationStatusChangeTimestamp,
        !this.configState.getLifecycleManager().isReplicationSupported(),
        getInitialSyncStatus());
  }

  private synchronized InitialSyncStatus getInitialSyncStatus() {
    return new InitialSyncStatus(
        this.initialSyncGate.lastChanged(), this.initialSyncGate.isClosed());
  }

  @Override
  public List<GroupedIndexGenerationMetrics> getGroupedIndexGenerationMetrics() {
    return this.cachedGroupedIndexGenerationStateMetrics.entrySet().stream()
        .map(
            groupedIndexGenerations ->
                new GroupedIndexGenerationMetrics(
                    groupedIndexGenerations.getKey(), groupedIndexGenerations.getValue()))
        .collect(Collectors.toList());
  }

  @Override
  public int getCurrentIndexFormatVersion() {
    return IndexFormatVersion.CURRENT.versionNumber;
  }

  public final synchronized void updateIndexInfos() {
    ensureOpen("updateIndexInfos");

    AllIndexInformations allIndexInfos = AllPresentIndexes.allIndexInfos(this.configState);

    this.cachedIndexInfo = ImmutableList.copyOf(allIndexInfos.indexInformations());
    this.cachedGroupedIndexGenerationStateMetrics =
        ImmutableMap.copyOf(allIndexInfos.allIndexGenerationsGrouped());
  }

  @Override
  public synchronized List<IndexDefinition> getLiveIndexes() {
    return this.configState.indexCatalog.getIndexes().stream()
        .map(IndexGeneration::getDefinition)
        .collect(Collectors.toList());
  }

  @Override
  public synchronized List<IndexGeneration> getLiveIndexGenerations() {
    return new ArrayList<>(this.configState.indexCatalog.getIndexes());
  }

  @Override
  public synchronized boolean isReplicationInitialized() {
    if (!this.replicationInitialized) {
      this.replicationInitialized = this.configState.getLifecycleManager().isInitialized();
    }
    return this.replicationInitialized;
  }

  @GuardedBy("this")
  protected final void ensureOpen(String methodName) {
    checkState(
        !this.closed,
        "cannot call %s() when %s is closed",
        methodName,
        this.getClass().getSimpleName());
  }

  private void shutdownReplication() {
    Crash.because("failed waiting for lifecycle manager to shut down replication")
        .ifThrows(
            () -> {
              // Need to reassure the compiler that we are locking prior to accessing
              // this.configState since it cannot understand when the lambda is invoked.
              synchronized (this) {
                Crash.because("failed to shut down replication manager")
                    .ifCompletesExceptionally(
                        this.configState.getLifecycleManager().shutdownReplication())
                    .get(REPLICATION_MANAGER_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
              }
            });
  }

  private void shutdown() {
    Crash.because("failed waiting for lifecycle manager to shut down")
        .ifThrows(
            () -> {
              // Need to reassure the compiler that we are locking prior to accessing
              // this.configState since it cannot understand when the lambda is invoked.
              synchronized (this) {
                Crash.because("failed to shut down replication manager")
                    .ifCompletesExceptionally(this.configState.getLifecycleManager().shutdown())
                    .get(REPLICATION_MANAGER_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
              }
            });
  }

  /** Determines whether disk usage should trigger a replication restart. */
  protected final synchronized void handleDiskBasedReplicationRestart() {
    // Restart replication if intended pause value changes.
    var isReplicationPaused = !this.configState.getLifecycleManager().isReplicationSupported();
    var shouldRestartReplication = this.shouldReplicationBePaused() != isReplicationPaused;

    Optional<SyncSourceConfig> currentConfig =
        this.configState.getLifecycleManager().getSyncSourceConfig();
    if (currentConfig.isPresent() && shouldRestartReplication) {
      LOG.atInfo()
          .addKeyValue("replicationGate", this.replicationGate.toString())
          .log("Disk usage crossed pause or resume threshold, restarting replication");
      shutdownAndRestartReplication(currentConfig.get());
    }
  }

  protected final synchronized void shutdownAndRestartReplication(
      SyncSourceConfig newSyncSourceConfig) {
    LOG.info("Shutting down the old LifecycleManager.");
    this.replicationStatusChangeTimestamp = Instant.now();
    shutdownReplication();
    this.configState.updateSyncSource(newSyncSourceConfig);
    restartReplication(this.configState);
    LOG.atInfo()
        .addKeyValue(
            "elapsedTimeMs",
            Duration.between(this.replicationStatusChangeTimestamp, Instant.now()).toMillis())
        .log("Finished shutting down the old LifecycleManager and started the new.");
    this.replicationStatusChangeTimestamp = Instant.now();
  }

  protected final boolean shouldReplicationBePaused() {
    return this.replicationGate.isClosed();
  }

  /**
   * ReplicationMode is set by {@code DiskMonitorConfig}. Replication mode will be set to ENABLE if
   * the config is empty or both DISABLE and DISK_UTILIZATION_BASED flags are false. DISABLE
   * overrides DISK_UTILIZATION_BASED
   */
  public enum ReplicationMode {
    ENABLE,
    DISABLE,
    DISK_UTILIZATION_BASED
  }
}
