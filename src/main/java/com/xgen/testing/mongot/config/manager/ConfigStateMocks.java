package com.xgen.testing.mongot.config.manager;

import static com.xgen.testing.mongot.mock.index.IndexFactory.mockIndexFactory;
import static com.xgen.testing.mongot.mock.replication.ReplicationManager.mockReplicationManager;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalWriter;
import com.xgen.mongot.config.manager.ConfigState;
import com.xgen.mongot.config.manager.IndexActions;
import com.xgen.mongot.config.manager.PhasingOutIndexes;
import com.xgen.mongot.config.manager.StagedIndexes;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorManagerImpl;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexGenerationFactory;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.AnalyzerRegistryFactory;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexFactory;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.lifecycle.DefaultLifecycleManager;
import com.xgen.mongot.lifecycle.LifecycleConfig;
import com.xgen.mongot.lifecycle.LifecycleManager;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.replication.ReplicationManagerFactory;
import com.xgen.mongot.util.Condition;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.mongodb.ConnectionInfo;
import com.xgen.mongot.util.mongodb.ConnectionStringUtil;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.embedding.providers.FakeEmbeddingClientFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/** Spied versions of all the dependencies of the config.manager package. */
public class ConfigStateMocks {
  public static final String DEFAULT_MDB_URI = "mongodb://localhost";

  public static final SyncSourceConfig MOCK_SYNC_SOURCE_CONFIG = createMockSyncSourceConfig();

  private static SyncSourceConfig createMockSyncSourceConfig() {
    ConnectionInfo mongod = ConnectionStringUtil.toConnectionInfoUnchecked(DEFAULT_MDB_URI);
    return new SyncSourceConfig(mongod, mongod, mongod, Optional.empty(), Optional.empty());
  }

  public static final LifecycleConfig DEFAULT_LIFECYCLE_CONFIG = LifecycleConfig.getDefault();

  public enum State {
    STAGED,
    LIVE,
    PHASE_OUT
  }

  private static final EmbeddingServiceConfig.EmbeddingConfig VOYAGE_3_CONFIG =
      new EmbeddingServiceConfig.EmbeddingConfig(
          Optional.empty(),
          new EmbeddingServiceConfig.VoyageModelConfig(
              Optional.of(1024),
              Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
              Optional.of(100),
              Optional.of(120_000)),
          new EmbeddingServiceConfig.ErrorHandlingConfig(3, 100L, 200L, 0.1),
          new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
              "token123", "2024-10-15T22:32:20.925Z"),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          true,
          Optional.empty(),
          false,
          Optional.empty());

  private static final EmbeddingServiceConfig TEST_EMBEDDING_CONFIG_V3_LARGE =
      new EmbeddingServiceConfig(
          EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
          "voyage-3-large",
          EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
          VOYAGE_3_CONFIG);

  public final StagedIndexes staged;
  public final IndexCatalog indexCatalog;
  public final PhasingOutIndexes phasingOut;
  public final InitializedIndexCatalog initializedIndexCatalog;

  public final MongotCursorManager cursorManager;
  public final AnalyzerRegistryFactory analyzerRegistryFactory;
  public final IndexFactory indexFactory;
  public final ArrayList<IndexGeneration> createdIndexes;
  public final Path configJournalPath;
  public final ConfigState configState;
  public final JournalWriter journalWriter;
  public final MeterRegistry meterRegistry;
  public final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;
  public final ToggleGate replicationGate;

  public final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;

  private final AtomicReference<IndexStatus> statusForCreatedIndexes;

  public final LifecycleManager lifecycleManager;

  /** If true, wait until index is initialized before returning from {@link #addIndex} call. */
  public final boolean waitUntilInitialized;

  public final MetricsFactory metricsFactory;
  public final FeatureFlags featureFlags;

  private ConfigStateMocks(
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      MongotCursorManager mongotCursorManager,
      LifecycleManager lifecycleManager,
      AnalyzerRegistryFactory analyzerRegistryFactory,
      IndexFactory indexFactory,
      MaterializedViewIndexFactory materializedViewIndexFactory,
      ArrayList<IndexGeneration> createdIndexes,
      AtomicReference<IndexStatus> statusForCreatedIndexes,
      Path configJournalPath,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      boolean waitUntilInitialized,
      ToggleGate replicationGate) {
    this.indexCatalog = indexCatalog;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.cursorManager = mongotCursorManager;
    this.analyzerRegistryFactory = analyzerRegistryFactory;
    this.indexFactory = indexFactory;
    this.createdIndexes = createdIndexes;
    this.statusForCreatedIndexes = statusForCreatedIndexes;
    this.configJournalPath = configJournalPath;
    this.journalWriter = spy(new JournalWriter(this.configJournalPath));
    this.meterRegistry = new SimpleMeterRegistry();
    this.lifecycleManager = lifecycleManager;
    this.staged = spy(new StagedIndexes());
    this.phasingOut = spy(new PhasingOutIndexes());
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
    this.embeddingServiceManagerSupplier =
        Suppliers.ofInstance(
            new EmbeddingServiceManager(
                List.of(TEST_EMBEDDING_CONFIG_V3_LARGE),
                new FakeEmbeddingClientFactory(),
                Executors.singleThreadScheduledExecutor("indexing", this.meterRegistry),
                this.meterRegistry,
                Optional.empty()));
    this.featureFlags = spy(FeatureFlags.withQueryFeaturesEnabled());
    this.configState =
        spy(
            ConfigState.create(
                this.staged,
                this.indexCatalog,
                this.phasingOut,
                this.initializedIndexCatalog,
                this.cursorManager,
                this.indexFactory,
                Optional.of(materializedViewIndexFactory),
                this.lifecycleManager,
                this.journalWriter,
                this.meterRegistry,
                this.featureFlags,
                this.dynamicFeatureFlagRegistry));
    this.waitUntilInitialized = waitUntilInitialized;
    this.metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());
    this.replicationGate = replicationGate;
  }

  public static ConfigStateMocks create() throws Exception {
    ConfigStateMocks configStateMocks = create(getConfigJournalPath(), true);
    Condition.await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> configStateMocks.lifecycleManager.isInitialized());
    return configStateMocks;
  }

  public static ConfigStateMocks create(Path configJournalPath, boolean waitUntilInitialized)
      throws Exception {
    return create(
        configJournalPath,
        waitUntilInitialized,
        new DynamicFeatureFlagRegistry(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
  }

  /**
   * Create ConfigState and related members.
   *
   * @param configJournalPath config Journal Path
   * @param waitUntilInitialized If true, wait until index is initialized before returning from
   *     {@link #addIndex} call.
   * @param dynamicFeatureFlagRegistry dynamic configs
   */
  public static ConfigStateMocks create(
      Path configJournalPath,
      boolean waitUntilInitialized,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry)
      throws Exception {
    IndexCatalog indexCatalog = spy(new DefaultIndexCatalog());
    InitializedIndexCatalog initializedIndexCatalog = spy(new InitializedIndexCatalog());
    MongotCursorManager cursorManager =
        spy(
            MongotCursorManagerImpl.fromConfig(
                CursorConfig.getDefault(),
                new SimpleMeterRegistry(),
                indexCatalog,
                initializedIndexCatalog));

    ArrayList<IndexGeneration> createdIndexes = new ArrayList<>();
    AtomicReference<IndexStatus> statusForCreatedIndexes =
        new AtomicReference<>(IndexStatus.steady());
    IndexFactory indexFactory = mockIndexFactory(createdIndexes::add, statusForCreatedIndexes::get);
    MaterializedViewIndexFactory materializedViewFactory =
        mockIndexFactory(
            MaterializedViewIndexFactory.class, createdIndexes::add, statusForCreatedIndexes::get);
    AnalyzerRegistryFactory analyzerRegistryFactory = spy(AnalyzerRegistry.factory());
    ToggleGate replicationGate = ToggleGate.opened();
    LifecycleManager lifecycleManager =
        spy(
            new DefaultLifecycleManager(
                ConfigStateMocks.getMockReplicationManagerFactory(replicationGate),
                Optional.of(MOCK_SYNC_SOURCE_CONFIG),
                initializedIndexCatalog,
                indexFactory,
                Optional.empty(),
                (syncSourceConfig) -> Optional.empty(),
                new SimpleMeterRegistry(),
                replicationGate,
                DEFAULT_LIFECYCLE_CONFIG));

    return new ConfigStateMocks(
        indexCatalog,
        initializedIndexCatalog,
        cursorManager,
        lifecycleManager,
        analyzerRegistryFactory,
        indexFactory,
        materializedViewFactory,
        createdIndexes,
        statusForCreatedIndexes,
        configJournalPath,
        dynamicFeatureFlagRegistry,
        waitUntilInitialized,
        replicationGate);
  }

  public ReplicationManagerFactory getReplicationManagerFactory() {
    return (unused) -> {
      var replicationManager = mockReplicationManager();
      when(replicationManager.dropIndex(any())).thenReturn(CompletableFuture.completedFuture(null));
      return replicationManager;
    };
  }

  private static ReplicationManagerFactory getMockReplicationManagerFactory(Gate replicationGate) {
    return (Optional<SyncSourceConfig> syncSourceConfig) -> {
      ReplicationManager replicationManager = mockReplicationManager();
      when(replicationManager.dropIndex(any())).thenReturn(CompletableFuture.completedFuture(null));
      when(replicationManager.getSyncSourceConfig()).thenReturn(syncSourceConfig);
      when(replicationManager.isReplicationSupported()).thenReturn(replicationGate.isOpen());
      when(replicationManager.isInitialized()).thenReturn(true);
      return replicationManager;
    };
  }

  /** Next time index factory creates a new index, Use this status for it. */
  public void setStatusForCreatedIndexes(IndexStatus status) {
    this.statusForCreatedIndexes.set(status);
  }

  /** Stages an index given an existing index in the catalog with this id. */
  public IndexGeneration stageIndex(ObjectId indexId) throws Exception {
    var oldIndex = this.indexCatalog.getIndexById(indexId).orElseThrow();

    return switch (oldIndex.getType()) {
      case SEARCH ->
          addIndex(
              oldIndex
                  .getDefinitionGeneration()
                  .asSearch()
                  .incrementUser(oldIndex.getDefinitionGeneration().asSearch().definition()),
              State.STAGED);
      case VECTOR, AUTO_EMBEDDING ->
          addIndex(
              oldIndex
                  .getDefinitionGeneration()
                  .asVector()
                  .incrementUser(oldIndex.getDefinitionGeneration().asVector().definition()),
              State.STAGED);
    };
  }

  /** Stages an index with a new attempt, given an existing index in the catalog with this id. */
  public IndexGeneration stageIndexAttempt(ObjectId indexId) throws Exception {
    var oldIndex = this.indexCatalog.getIndexById(indexId).orElseThrow();

    return switch (oldIndex.getType()) {
      case SEARCH ->
          addIndex(
              oldIndex
                  .getDefinitionGeneration()
                  .asSearch()
                  .incrementUser(oldIndex.getDefinitionGeneration().asSearch().definition()),
              State.STAGED);
      case VECTOR, AUTO_EMBEDDING ->
          addIndex(
              oldIndex
                  .getDefinitionGeneration()
                  .asVector()
                  .incrementUser(oldIndex.getDefinitionGeneration().asVector().definition()),
              State.STAGED);
    };
  }

  public IndexGeneration addVectorIndex(ObjectId id, State state) throws Exception {
    return addIndex(
        com.xgen.testing.mongot.mock.index.IndexGeneration.mockVectorDefinitionGeneration(id),
        state);
  }

  public IndexGeneration addIndex(ObjectId id, State state) throws Exception {
    return addIndex(
        com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration(id), state);
  }

  /** Adds one phased out index. */
  public IndexGeneration addIndex(IndexDefinitionGeneration definitionGeneration, State state)
      throws Exception {

    switch (state) {
      case STAGED -> {
        IndexActions.withReplication(this.configState).addStagedIndex(definitionGeneration);
        if (this.waitUntilInitialized) {
          waitAndGetInitializedIndex(definitionGeneration.getGenerationId());
        }
        return this.staged
            .getIndex(definitionGeneration.getIndexDefinition().getIndexId())
            .orElseThrow();
      }
      case LIVE -> {
        IndexActions.withReplication(this.configState).addNewIndexes(List.of(definitionGeneration));
        if (this.waitUntilInitialized) {
          waitAndGetInitializedIndex(definitionGeneration.getGenerationId());
        }

        return this.indexCatalog
            .getIndexById(definitionGeneration.getIndexDefinition().getIndexId())
            .orElseThrow();
      }
      case PHASE_OUT -> {
        var index =
            IndexGenerationFactory.getIndexGeneration(this.indexFactory, definitionGeneration);
        this.lifecycleManager.add(index);
        phaseOutIndex(index);
        return index;
      }
    }
    throw new IllegalStateException("Unexpected value: " + state);
  }

  /** Add index to be phased out. */
  public void phaseOutIndex(IndexGeneration indexGeneration) {
    this.phasingOut.addIndex(indexGeneration);
  }

  /** Wait until index is initialized and return the index. */
  public InitializedIndex waitAndGetInitializedIndex(GenerationId generationId) {
    Condition.await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> this.initializedIndexCatalog.getIndex(generationId).isPresent());
    return this.initializedIndexCatalog.getIndex(generationId).orElseThrow();
  }

  /**
   * Verifies that no indexes were created via the factory and no indexes were added to the catalog
   * or replication.
   */
  public void assertNoIndexActivity() {
    assertNoIndexCatalogedAndReplicated();
    assertNoIndexDeleted();
    assertNoIndexCreated();
    assertNoIndexPhasedOut();
    assertNoIndexStagingActivity();
  }

  /** Asserts that no behavior associated with indexes being created occurred. */
  public void assertNoIndexCatalogedAndReplicated() {
    // Shouldn't have tried to add to the index catalog.
    verify(this.indexCatalog, times(0)).addIndex(any());

    // Shouldn't have tried to start replication.
    verify(this.lifecycleManager, times(0)).add(any());
  }

  /** Asserts that no behavior associated with indexes being phasedOut. */
  public void assertNoIndexPhasedOut() {
    // Shouldn't have tried to add to phasingOut indexes
    verify(this.phasingOut, times(0)).addIndex(any());
    verify(this.phasingOut, times(0)).removeIndex(any());
  }

  /** Asserts that no behavior associated with indexes being staged. */
  void assertNoIndexStagingActivity() {
    verify(this.staged, times(0)).addIndex(any());
    verify(this.staged, times(0)).removeIndex(any());
  }

  public void assertOneIndexCatalogedAndReplicated() {
    // Should have added our index.
    verify(this.indexCatalog).addIndex(any());

    // Should have started replication.
    verify(this.lifecycleManager).add(any());
  }

  public void assertAtLeastOneIndexCataloged() {
    // Should have added one or more indexes.
    verify(this.indexCatalog, atLeastOnce()).addIndex(any());
  }

  void assertAtLeastOneIndexCatalogedAndReplicated() {
    assertAtLeastOneIndexCataloged();

    // Should have started replication.
    verify(this.lifecycleManager, atLeastOnce()).add(any());
  }

  public void assertAtLeastOneIndexStaged() {
    // Should have added one or more indexes.
    verify(this.staged, atLeastOnce()).addIndex(any());
  }

  public void assertAtLeastOneIndexStagedAndReplicated() {
    assertAtLeastOneIndexStaged();

    // Should have started replication.
    verify(this.lifecycleManager, atLeastOnce()).add(any());
  }

  public void assertNoIndexCreated() {
    // As a pre-existing index could have been initialized asynchronously only verify no new index
    // has been created.
    try {
      verify(this.indexFactory, never()).getIndex(any());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void assertNoIndexReplicated() {
    verify(this.lifecycleManager, never()).add(any());
  }

  /** One index created in index factory. */
  public void assertOneIndexCreated() throws Exception {
    verify(this.indexFactory, times(1)).getIndex(any());
  }

  public void assertIndexCatalogSize(int size) {
    Assert.assertEquals(size, this.indexCatalog.getIndexes().size());
  }

  /**
   * Assert that one index was instantiated with this definition (there could be other indexes
   * instantiated besides this one).
   */
  public void assertIndexCreated(IndexDefinitionGeneration definitionGeneration) throws Exception {
    verify(this.indexFactory).getIndex(eq(definitionGeneration));
  }

  /**
   * Assert that one index was instantiated with this definition (there could be other indexes
   * instantiated besides this one).
   */
  public void assertIndexCreated(IndexDefinition indexDefinition) throws Exception {
    verify(this.indexFactory)
        .getIndex(argThat(definition -> definition.getIndexDefinition().equals(indexDefinition)));
  }

  /** Same as above, and the index should have been created with this analyzer. */
  public void assertIndexCreated(OverriddenBaseAnalyzerDefinition analyzerDefinition)
      throws Exception {
    verify(this.indexFactory, times(1))
        .getIndex(
            argThat(
                definition ->
                    definition
                        .asSearch()
                        .definition()
                        .analyzerDefinitions()
                        .equals(List.of(analyzerDefinition))));
  }

  /** Same as above, and the index should have been created with this analyzer. */
  public void assertIndexCreated(CustomAnalyzerDefinition analyzerDefinition) throws Exception {
    verify(this.indexFactory, times(1))
        .getIndex(
            argThat(
                definition ->
                    definition
                        .getIndexDefinition()
                        .asSearchDefinition()
                        .getAnalyzers()
                        .equals(List.of(analyzerDefinition))));
  }

  public void assertIndexCreatedAndDropped(IndexDefinitionGeneration deletedDefinition)
      throws Exception {
    assertIndexCreated(deletedDefinition);
    Index index =
        this.createdIndexes.stream()
            .filter(i -> deletedDefinition.equals(i.getDefinitionGeneration()))
            .findFirst()
            .orElseThrow(
                () ->
                    new AssertionError(
                        "Should have found an index with this definition generation"))
            .getIndex();
    verify(index).drop();
  }

  /**
   * Note, only works for the same IndexDefinition object, if it was de-serialized, it will yell.
   */
  public void assertIndexDropped(
      IndexDefinitionGeneration deletedDefinition, InitializedIndex initializedIndex)
      throws IOException {
    Index index =
        this.createdIndexes.stream()
            .filter(i -> deletedDefinition.equals(i.getDefinitionGeneration()))
            .findFirst()
            .orElseThrow(
                () ->
                    new AssertionError(
                        "Should have found an index with this definition generation"))
            .getIndex();
    verify(initializedIndex).close();
    verify(index, atLeastOnce()).drop();
  }

  /**
   * Asserts that no indexes were deleted, and no behavior associated with indexes being deleted
   * occurred.
   */
  private void assertNoIndexDeleted() {
    // Shouldn't have tried to drop from the index catalog.
    verify(this.indexCatalog, times(0)).removeIndex(any());

    // Shouldn't have tried to kill index cursors.
    verify(this.cursorManager, times(0)).killIndexCursors(any());

    // Shouldn't have tried to stop replication.
    verify(this.lifecycleManager, times(0)).dropIndex(any());
  }

  public void assertStagedIndexesAre(IndexGeneration... indexes) {
    HashSet<IndexGeneration> expected = Sets.newHashSet(indexes);
    Assert.assertEquals(expected, Sets.newHashSet(this.staged.getIndexes()));
  }

  public void assertLiveIndexesAre(IndexGeneration... indexes) {
    HashSet<IndexGeneration> expected = Sets.newHashSet(indexes);
    Assert.assertEquals(expected, Sets.newHashSet(this.indexCatalog.getIndexes()));
  }

  public void assertPhasingOutIndexesAre(IndexGeneration... indexes) {

    HashSet<IndexGeneration> expected = Sets.newHashSet(indexes);
    Assert.assertEquals(expected, Sets.newHashSet(this.phasingOut.getIndexes()));
  }

  public void assertJournalPersistedAtLeastOnce() throws Exception {
    verify(this.journalWriter, atLeastOnce()).persist(any());
  }

  /*Tests that the expected journal was the last to be persisted.*/
  public void assertPersistedJournalEquals(ConfigJournalV1 expected) throws Exception {
    assertJournaled(expected);

    var actualPersisted =
        ConfigJournalV1.fromFileIfExists(this.configJournalPath)
            .orElseThrow(
                () ->
                    new AssertionError("Should have written a config journal, but none was found"));
    Assert.assertEquals(expected, actualPersisted);
  }

  private void assertJournaled(ConfigJournalV1 expected) throws Exception {
    verify(this.configState, atLeastOnce()).persist(argThat(expected::equals));
  }

  public void assertPersistedJournalEmpty() throws Exception {
    var configJournal =
        ConfigJournalV1.fromFileIfExists(this.configJournalPath)
            .orElseThrow(
                () ->
                    new AssertionError("Should have written a config journal, but none was found"));
    Assert.assertEquals(
        "config journal has non-empty staged indexes", 0, configJournal.getStagedIndexes().size());
    Assert.assertEquals(
        "config journal has non-empty indexes", 0, configJournal.getLiveIndexes().size());
    Assert.assertEquals(
        "config journal has non-empty deleted indexes",
        0,
        configJournal.getDeletedIndexes().size());
  }

  /** Resets the mocks verification counters so they can be re-verified. */
  public void clearInvocations() {
    this.createdIndexes.clear();
    Mockito.clearInvocations(
        this.staged,
        this.indexCatalog,
        this.phasingOut,
        this.cursorManager,
        this.lifecycleManager,
        this.analyzerRegistryFactory,
        this.indexFactory);
  }

  /** Creates a temporary folder where config journals can be written. */
  private static Path getConfigJournalPath() throws Exception {
    TemporaryFolder folder = TestUtils.getTempFolder();
    return Paths.get(folder.getRoot().toPath().toString(), "configJournal.json");
  }
}
