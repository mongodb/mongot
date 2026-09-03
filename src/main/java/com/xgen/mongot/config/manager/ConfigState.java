package com.xgen.mongot.config.manager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalEntry;
import com.xgen.mongot.config.backup.JournalWriter;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingFailureMessages;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGenerationFactory;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexFactory;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.lifecycle.LifecycleManager;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor.ReplicationMeterData;
import com.xgen.mongot.metrics.ServerStatusDataExtractor.Scope;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.types.ObjectId;
import org.slf4j.LoggerFactory;

/**
 * ConfigState is used across the config package to maintain the state of indexes and persist the
 * current state to disk when required.
 *
 * <p>ConfigManager is in charge of maintaining and modifying the members of ConfigState, including
 * ensuring that access to ConfigState is properly synchronized. ConfigState is not thread safe and
 * does not provide any consistency guarantees between the different members, nor between its
 * members and the persisted config journal.
 */
public class ConfigState {
  public static final String STAGED_INDEXES_FEATURE_VERSION_FOUR =
      "stagedIndexesFeatureVersionFour";
  public static final String LIVE_INDEXES_FEATURE_VERSION_FOUR =
      "indexesInCatalogFeatureVersionFour";
  public static final String PHASING_OUT_INDEXES_FEATURE_VERSION_FOUR =
      "indexesPhasingOutFeatureVersionFour";

  /**
   * Indexes staged to replace a corresponding index in the indexCatalog. A corresponding index must
   * be present in the catalog, this invariant is enforced by IndexActions and
   * Invariants::validateGenerationalInvariants.
   */
  public final StagedIndexes staged;

  /**
   * The indexes that are currently active and queryable. Indexes in the catalog may not necessarily
   * be in steady state.
   */
  public final IndexCatalog indexCatalog;

  /**
   * The indexes whose config journal entry could not be parsed, and which therefore cannot be
   * initialized or queried. Tracked so that mongot can report why they are unusable rather than
   * behaving as though they do not exist.
   */
  public final IncompatibleIndexes incompatibleIndexes;

  /**
   * Indexes on the way to be dropped, waiting for their cursors to be killed.
   *
   * <p>If mongot restarts at any point these can be dropped (cursors are not persisted). Thus,
   * these indexes are journal-ed as dropped.
   */
  public final PhasingOutIndexes phasingOut;

  /**
   * The collection of initialized indexes ({@link com.xgen.mongot.index.InitializedIndex}) for the
   * corresponding indexes managed in this ConfigState. The indexes themselves could be in various
   * states (staged, phasing out, catalog).
   *
   * <p>An index in the ConfigState, once initialized, must be added to this collection.
   */
  final InitializedIndexCatalog initializedIndexCatalog;

  /**
   * The collection of {@link DynamicFeatureFlagConfig}s. It's required to persist them within
   * journal to avoid inconsistent feature flag configs after bootstrapping.
   */
  public final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;

  /** Indicates whether {@link ConfigState#postInitializationFromConfigJournal} is called. */
  @GuardedBy("this")
  private @Var boolean isInitializedFromConfigJournal;

  /** For each index id, tracks the max user index version number in memory. */
  @GuardedBy("this")
  private final Map<ObjectId, Integer> maxUserIndexVersionNumberPerIndex;

  /** The last {@link ConfigJournalV1} passed to {@link ConfigState#persist}. */
  @GuardedBy("this")
  private @Var Optional<ConfigJournalV1> lastPersistedConfigJournal;

  public final IndexFactory indexFactory;
  final MongotCursorManager cursorManager;

  final Optional<MaterializedViewIndexFactory> materializedViewIndexFactory;

  /** LifecycleManager manages indexes in any of the 3 above states. */
  private final LifecycleManager lifecycleManager;

  private final MetricsFactory metricsFactory;
  private final JournalWriter journalWriter;

  private ConfigState(
      StagedIndexes staged,
      IndexCatalog indexCatalog,
      IncompatibleIndexes incompatibleIndexes,
      PhasingOutIndexes phasingOut,
      InitializedIndexCatalog initializedIndexCatalog,
      MongotCursorManager cursorManager,
      IndexFactory indexFactory,
      Optional<MaterializedViewIndexFactory> materializedViewIndexFactory,
      LifecycleManager lifecycleManager,
      MetricsFactory metricsFactory,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      JournalWriter journalWriter) {
    this.staged = staged;
    this.indexCatalog = indexCatalog;
    this.incompatibleIndexes = incompatibleIndexes;
    this.phasingOut = phasingOut;
    this.isInitializedFromConfigJournal = false;
    this.maxUserIndexVersionNumberPerIndex = new HashMap<>();
    this.lastPersistedConfigJournal = Optional.empty();
    this.initializedIndexCatalog = initializedIndexCatalog;

    this.cursorManager = cursorManager;
    this.lifecycleManager = lifecycleManager;
    this.indexFactory = indexFactory;
    this.materializedViewIndexFactory = materializedViewIndexFactory;
    this.metricsFactory = metricsFactory;
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;

    this.journalWriter = journalWriter;
  }

  public static ConfigState create(
      StagedIndexes staged,
      IndexCatalog indexCatalog,
      IncompatibleIndexes incompatibleIndexes,
      PhasingOutIndexes phasingOut,
      InitializedIndexCatalog initializedIndexCatalog,
      MongotCursorManager cursorManager,
      IndexFactory indexFactory,
      Optional<MaterializedViewIndexFactory> materializedViewIndexFactory,
      LifecycleManager lifecycleManager,
      JournalWriter journalWriter,
      MeterRegistry meterRegistry,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    Tags replicationTags = Tags.of(Scope.REPLICATION.getTag());
    MetricsFactory metricsFactory =
        new MetricsFactory("configState", meterRegistry, replicationTags);

    ConfigState configState =
        new ConfigState(
            staged,
            indexCatalog,
            incompatibleIndexes,
            phasingOut,
            initializedIndexCatalog,
            cursorManager,
            indexFactory,
            materializedViewIndexFactory,
            lifecycleManager,
            metricsFactory,
            dynamicFeatureFlagRegistry,
            journalWriter);

    registerMetrics(metricsFactory, staged, indexCatalog, phasingOut, featureFlags);

    return configState;
  }

  public void updateSyncSource(SyncSourceConfig syncSourceConfig) {
    this.lifecycleManager.updateSyncSource(syncSourceConfig);
    this.materializedViewIndexFactory.ifPresent(f -> f.updateSyncSource(syncSourceConfig));
    resetUnresolvedAutoEmbeddingIndexes();
  }

  /** This method is called when the {@link ConfigState} is initialized from the config journal. */
  public synchronized void postInitializationFromConfigJournal() {
    // Get max version number per indexId and put them into maxUserIndexVersionNumberPerIndex.
    this.maxUserIndexVersionNumberPerIndex.putAll(
        Streams.concat(
                this.indexCatalog.getIndexes().stream(),
                this.staged.getIndexes().stream(),
                this.phasingOut.getIndexes().stream())
            .collect(
                Collectors.toMap(
                    indexGeneration -> indexGeneration.getDefinition().getIndexId(),
                    indexGeneration ->
                        indexGeneration
                            .getDefinitionGeneration()
                            .generation()
                            .userIndexVersion
                            .versionNumber,
                    BinaryOperator.maxBy(Integer::compareTo))));
    this.isInitializedFromConfigJournal = true;
  }

  /** This method should be called after {@link ConfigState#postInitializationFromConfigJournal}. */
  @VisibleForTesting
  public UserIndexVersion getNewUserIndexVersion(ObjectId indexId) {
    // For conservative purposes, also check the version number from indexCatalog, staged and
    // phasingOut. This should be unnecessary.
    Optional<Integer> maxUserVersionFromManagedIndexes =
        Streams.concat(
                this.staged.getIndex(indexId).stream(),
                this.indexCatalog.getIndexById(indexId).stream(),
                this.phasingOut.getIndexesById(indexId).stream())
            .map(
                index ->
                    index.getDefinitionGeneration().generation().userIndexVersion.versionNumber)
            .max(Integer::compareTo);

    synchronized (this) {
      Check.checkState(
          this.isInitializedFromConfigJournal,
          "postInitializationFromConfigJournal should be called before getNewUserIndexVersion");

      // find the max version if present
      Optional<Integer> maxUserVersion =
          Streams.concat(
                  maxUserVersionFromManagedIndexes.stream(),
                  Optional.ofNullable(this.maxUserIndexVersionNumberPerIndex.get(indexId)).stream())
              .max(Integer::compareTo);

      // increment or start from 0
      int newUserVersionNumber = maxUserVersion.map(max -> max + 1).orElse(0);

      // Keep the map updated.
      this.maxUserIndexVersionNumberPerIndex.put(indexId, newUserVersionNumber);

      return new UserIndexVersion(newUserVersionNumber);
    }
  }

  public LifecycleManager getLifecycleManager() {
    return this.lifecycleManager;
  }

  static void registerMetrics(
      MetricsFactory metricsFactory,
      StagedIndexes staged,
      IndexCatalog indexCatalog,
      PhasingOutIndexes phasingOut,
      FeatureFlags featureFlags) {
    numberOfIndexesPerIndexFormatVersion(
        staged::getIndexes, metricsFactory, ReplicationMeterData.STAGED_INDEXES);
    numberOfIndexesPerIndexFormatVersion(
        indexCatalog::getIndexes, metricsFactory, ReplicationMeterData.LIVE_INDEXES);
    numberOfIndexesPerIndexFormatVersion(
        phasingOut::getIndexes, metricsFactory, ReplicationMeterData.PHASING_OUT_INDEXES);
    if (featureFlags.isEnabled(Feature.INDEX_FEATURE_VERSION_FOUR)) {
      numberOfIndexesAtIndexFeatureVersionFour(
          staged::getIndexes, metricsFactory, STAGED_INDEXES_FEATURE_VERSION_FOUR);
      numberOfIndexesAtIndexFeatureVersionFour(
          indexCatalog::getIndexes, metricsFactory, LIVE_INDEXES_FEATURE_VERSION_FOUR);
      numberOfIndexesAtIndexFeatureVersionFour(
          phasingOut::getIndexes, metricsFactory, PHASING_OUT_INDEXES_FEATURE_VERSION_FOUR);
    }
  }

  private static void numberOfIndexesPerIndexFormatVersion(
      Supplier<Collection<IndexGeneration>> getGenerations,
      MetricsFactory metricsFactory,
      String meterName) {
    List<IndexFormatVersion> allIndexFormatVersions =
        List.of(IndexFormatVersion.MIN_SUPPORTED_VERSION, IndexFormatVersion.CURRENT);

    allIndexFormatVersions.stream()
        .map(
            indexFormatVersion -> new IndexFormatVersionCounter(getGenerations, indexFormatVersion))
        .forEach(
            ifvCounter ->
                metricsFactory.objectValueGauge(
                    meterName,
                    ifvCounter,
                    IndexFormatVersionCounter::getNumIndexes,
                    Tags.of(
                        "indexFormatVersion",
                        String.valueOf(ifvCounter.indexFormatVersion().versionNumber))));
  }

  private static void numberOfIndexesAtIndexFeatureVersionFour(
      Supplier<Collection<IndexGeneration>> getGenerations,
      MetricsFactory metricsFactory,
      String meterName) {
    metricsFactory.objectValueGauge(
        meterName,
        new IndexFeatureVersionCounter(getGenerations, 4),
        IndexFeatureVersionCounter::getNumIndexes);
  }

  /**
   * Persist the config journal to disk. Throws Invariants.InvariantException if the journal is
   * semantically invalid.
   */
  public synchronized void persist(ConfigJournalV1 configJournal)
      throws IOException, Invariants.InvariantException {
    try {
      Invariants.validateGenerationalInvariants(
          configJournal.getStagedIndexes(),
          configJournal.getLiveIndexes(),
          configJournal.getDeletedIndexes());
    } catch (Invariants.InvariantException e) {
      LoggerFactory.getLogger(ConfigState.class)
          .atError()
          .addKeyValue("configJournal", JsonCodec.toJson(configJournal))
          .setCause(e)
          .log("Exception while validating config journal");
      throw e;
    }
    this.journalWriter.persist(configJournal);

    if (this.isInitializedFromConfigJournal) {
      // Update metrics if the config journal is changed.
      ConfigJournalV1 oldConfig =
          this.lastPersistedConfigJournal.orElse(
              new ConfigJournalV1(List.of(), List.of(), List.of()));
      updateMetricsForConfigStateChange(oldConfig, configJournal);
    }
    this.lastPersistedConfigJournal = Optional.of(configJournal);
  }

  protected enum State {
    NOT_EXIST,
    LIVE,
    STAGED,
    DELETED,
  }

  private void updateMetricsForConfigStateChange(
      ConfigJournalV1 oldConfig, ConfigJournalV1 newConfig) {
    HashMap<GenerationId, State> oldGenerationStateMap = getGenerationStateMap(oldConfig);
    HashMap<GenerationId, State> newGenerationStateMap = getGenerationStateMap(newConfig);
    HashSet<GenerationId> generationIds = new HashSet<>();
    generationIds.addAll(oldGenerationStateMap.keySet());
    generationIds.addAll(newGenerationStateMap.keySet());
    generationIds.forEach(
        generationId -> {
          State oldState = oldGenerationStateMap.getOrDefault(generationId, State.NOT_EXIST);
          State newState = newGenerationStateMap.getOrDefault(generationId, State.NOT_EXIST);
          if (oldState != newState) {
            this.metricsFactory
                .counter(
                    "stateTransition",
                    Tags.of("fromState", oldState.name(), "toState", newState.name()))
                .increment();
          }
        });
  }

  private HashMap<GenerationId, State> getGenerationStateMap(ConfigJournalV1 config) {
    HashMap<GenerationId, State> generationStateMap = new HashMap<>();
    config
        .getLiveIndexes()
        .forEach(
            indexDefinitionGeneration ->
                generationStateMap.put(indexDefinitionGeneration.getGenerationId(), State.LIVE));
    config
        .getStagedIndexes()
        .forEach(
            indexDefinitionGeneration ->
                generationStateMap.put(indexDefinitionGeneration.getGenerationId(), State.STAGED));
    config
        .getDeletedIndexes()
        .forEach(
            indexDefinitionGeneration ->
                generationStateMap.put(indexDefinitionGeneration.getGenerationId(), State.DELETED));
    return generationStateMap;
  }

  /**
   * Instantiate a new journal representing the current config state. It may differ from the journal
   * persisted to disk.
   *
   * <p>The quarantined entries in {@link #incompatibleIndexes} are folded back in alongside the
   * indexes mongot actually holds, so that they are preserved despite having no index to be derived
   * from. Quarantined deleted entries are not carried: their data is dropped at startup.
   */
  ConfigJournalV1 currentJournal() {
    return new ConfigJournalV1(
        entries(this.staged.getIndexes(), this.incompatibleIndexes.getStagedIndexes()),
        entries(this.indexCatalog.getIndexes(), this.incompatibleIndexes.getLiveIndexes()),
        // if we restart at any point, we can just drop the phasing out indexes.
        entries(this.phasingOut.getIndexes(), List.of()));
  }

  private static List<JournalEntry<? extends IndexDefinitionGeneration>> entries(
      Collection<IndexGeneration> indexes, Collection<QuarantinedIndex> quarantined) {
    return Stream.concat(
            indexes.stream()
                .map(IndexGeneration::getDefinitionGeneration)
                .map(JournalEntry::<IndexDefinitionGeneration>parsed),
            quarantined.stream().map(QuarantinedIndex::asJournalEntry))
        .collect(Collectors.toList());
  }

  private record IndexFormatVersionCounter(
      Supplier<Collection<IndexGeneration>> getGenerations, IndexFormatVersion indexFormatVersion) {

    private double getNumIndexes() {
      return (double)
          this.getGenerations.get().stream()
              .filter(
                  indexGeneration ->
                      indexGeneration
                          .getDefinitionGeneration()
                          .generation()
                          .indexFormatVersion
                          .equals(this.indexFormatVersion))
              .count();
    }
  }

  private record IndexFeatureVersionCounter(
      Supplier<Collection<IndexGeneration>> getGenerations, int indexFeatureVersion) {

    private double getNumIndexes() {
      return (double)
          this.getGenerations.get().stream()
              .filter(
                  indexGeneration ->
                      indexGeneration
                              .getDefinitionGeneration()
                              .getIndexDefinition()
                              .getParsedIndexFeatureVersion()
                          == this.indexFeatureVersion)
              .count();
    }
  }

  /**
   * Resets index status for unresolved auto embedding indexes so IndexRecoveryStager picks them up
   * and retries. Generations already reporting {@code DOES_NOT_EXIST} (source collection absent on
   * this shard) are left untouched -- they are not transient resolution failures and recover via
   * the collection-reappearance path, so resetting them would replace the intended quiet {@code
   * COLLECTION_NOT_FOUND} status with a spurious failure.
   */
  private void resetUnresolvedAutoEmbeddingIndexes() {
    Stream.concat(this.staged.getIndexes().stream(), this.indexCatalog.getIndexes().stream())
        .filter(AutoEmbeddingIndexGenerationFactory::isAutoEmbeddingResolutionFailed)
        // COLLECTION_NOT_FOUND (source collection absent on this shard) is unresolved by type
        // but is not a transient failure: keep it a quiet DOES_NOT_EXIST and let it recover via
        // the collection-reappearance path (IndexRecoveryStager), not this transient-retry reset.
        .filter(indexGeneration -> !indexGeneration.getIndex().getStatus().isCollectionNotFound())
        // A STALE index is serving offline from its reconstructed derived Lucene; leave it serving
        // (IndexRecoveryStager re-stages it by type). Reset the rest to a recoverable RETRY.
        .filter(
            indexGeneration ->
                indexGeneration.getIndex().getStatus().getStatusCode()
                    != IndexStatus.StatusCode.STALE)
        .forEach(
            unresolvedIndexGeneration ->
                unresolvedIndexGeneration
                    .getIndex()
                    .setStatus(
                        IndexStatus.failed(
                            AutoEmbeddingFailureMessages.withFailurePrefix(
                                "Failed to resolve auto embedding index. Retrying..."),
                            IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY)));
  }
}
