package com.xgen.mongot.config.manager;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION_CURRENT;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_STATUS;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SYNONYM_DETAILED_STATUS;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SYNONYM_STATUS;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_VECTOR_DEFINITION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalEntryIdentity;
import com.xgen.mongot.config.backup.QuarantinePolicy;
import com.xgen.mongot.config.manager.metrics.GroupedIndexGenerationMetrics;
import com.xgen.mongot.config.manager.metrics.IndexConfigState;
import com.xgen.mongot.config.manager.metrics.IndexGenerationStateMetrics;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.AggregatedIndexMetrics;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexDetailedStatus;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexGenerationFactory;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSearchEngine;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.FileUtils;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.ConnectionStringUtil;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.testing.mongot.config.backup.ConfigJournalV1Builder;
import com.xgen.testing.mongot.config.backup.QuarantinedIndexBuilder;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.config.manager.DefaultConfigManagerMocks;
import com.xgen.testing.mongot.config.manager.metrics.IndexGenerationStateMetricsBuilder;
import com.xgen.testing.mongot.index.IndexGenerationMetricsBuilder;
import com.xgen.testing.mongot.index.IndexMetricsBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.metrics.micrometer.PercentilesBuilder;
import com.xgen.testing.mongot.metrics.micrometer.SerializableTimerBuilder;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import com.xgen.testing.mongot.mock.index.query.Query;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.json.JsonObject;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class DefaultConfigManagerTest {
  /**
   * The journal is read fail-fast until quarantineUnparseableIndex is turned on, so that enabling
   * the flag is the only thing that changes startup behaviour for a malformed definition.
   */
  @Test
  public void quarantinePolicy_flagDisabled_isFailFast() {
    assertEquals(
        QuarantinePolicy.FAIL_FAST,
        DefaultConfigManager.quarantinePolicy(FeatureFlags.getDefault()));
  }

  /**
   * The policy derived from the flag is the one the journal read actually uses: under QUARANTINE an
   * entry whose definition does not parse no longer fails initialization.
   */
  @Test
  public void initializeFromExistingJournal_quarantine_unparseableEntryDoesNotFailInit()
      throws Exception {
    ConfigStateMocks mocks = ConfigStateMocks.create();
    writeJournalWithUnparseableLiveIndex(mocks.configJournalPath);

    DefaultConfigManager.initializeFromExistingJournal(
        mocks.configState,
        mocks.configJournalPath,
        Optional.empty(),
        new SimpleMeterRegistry(),
        QuarantinePolicy.QUARANTINE);

    mocks.assertNoIndexActivity();
  }

  @Test
  public void initializeFromExistingJournal_failFast_unparseableEntryFailsInit() throws Exception {
    ConfigStateMocks mocks = ConfigStateMocks.create();
    writeJournalWithUnparseableLiveIndex(mocks.configJournalPath);

    Assert.assertThrows(
        BsonParseException.class,
        () ->
            DefaultConfigManager.initializeFromExistingJournal(
                mocks.configState,
                mocks.configJournalPath,
                Optional.empty(),
                new SimpleMeterRegistry(),
                QuarantinePolicy.FAIL_FAST));
  }

  /**
   * The two tests above call initializeFromExistingJournal directly. These two go through the
   * production DefaultConfigManager.initialize entry point instead, so that a regression which
   * stops forwarding quarantinePolicy(featureFlags) is caught rather than left green.
   */
  @Test
  public void initialize_quarantineFlagEnabled_unparseableEntryDoesNotCrash() throws Exception {
    Path configJournalPath = DefaultConfigManagerMocks.getConfigJournalPath();
    writeJournalWithUnparseableLiveIndex(configJournalPath);
    FeatureFlags featureFlags =
        FeatureFlags.withDefaults().enable(Feature.QUARANTINE_UNPARSEABLE_INDEX).build();

    DefaultConfigManagerMocks mocks =
        DefaultConfigManagerMocks.create(configJournalPath, featureFlags);

    mocks.assertNoIndexActivity();
  }

  @Test
  @Crash.TestOnlyHaltHandler
  public void initialize_quarantineFlagDisabled_unparseableEntryCrashes() throws Exception {
    Path configJournalPath = DefaultConfigManagerMocks.getConfigJournalPath();
    writeJournalWithUnparseableLiveIndex(configJournalPath);
    Path crashLogPath = configJournalPath.resolveSibling("crash.log");
    Crash.setCrashLogPath(crashLogPath);

    AtomicInteger exitCode = new AtomicInteger(-1);
    Crash.setHaltHandlerForTesting(
        code -> {
          exitCode.set(code);
          throw new HaltError();
        });

    try {
      Assert.assertThrows(
          HaltError.class,
          () -> DefaultConfigManagerMocks.create(configJournalPath, FeatureFlags.getDefault()));
      assertEquals("expected fail-exit code 1", 1, exitCode.get());
    } finally {
      Crash.clearHaltHandlerForTesting();
    }

    // The crash is reported under the config-journal category rather than inferred as OTHER, so a
    // journal this binary cannot read is distinguishable from an unrelated startup failure.
    BsonDocument structuredLogs =
        new JsonObject(Files.readAllLines(crashLogPath).get(0)).toBsonDocument();
    assertEquals(
        Crash.CrashCategory.CONFIG_JOURNAL_INITIALIZE_FAILURE.toString(),
        structuredLogs.getString("crashCategory").getValue());
  }

  /** Lets the halt handler above stop execution instead of letting initialization continue. */
  private static final class HaltError extends Error {
    private HaltError() {
      super("halt");
    }
  }

  /**
   * Writes a journal holding a single live index carrying a field no version of mongot knows about,
   * which makes the entry unparseable while leaving its identity readable.
   */
  private static void writeJournalWithUnparseableLiveIndex(Path configJournalPath)
      throws Exception {
    BsonDocument journal =
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_INDEX_DEFINITION_GENERATION)
            .build()
            .toBson();
    journal
        .getArray("indexes")
        .get(0)
        .asDocument()
        .append("fieldFromTheFuture", new BsonBoolean(true));

    FileUtils.atomicallyReplace(configJournalPath, JsonCodec.toJson(journal));
  }

  @Test
  public void quarantinePolicy_flagEnabled_isQuarantine() {
    FeatureFlags featureFlags =
        FeatureFlags.withDefaults().enable(Feature.QUARANTINE_UNPARSEABLE_INDEX).build();

    assertEquals(QuarantinePolicy.QUARANTINE, DefaultConfigManager.quarantinePolicy(featureFlags));
  }

  /**
   * A quarantined live index is the per-index unit the startup counter exists for: the node boots
   * quietly with a degraded index, and the live counter is what an alert can fire on.
   */
  @Test
  public void initialize_quarantinedLiveIndex_incrementsLiveCounter() throws Exception {
    Path configJournalPath = DefaultConfigManagerMocks.getConfigJournalPath();
    writeJournalWithUnparseableLiveIndex(configJournalPath);
    FeatureFlags featureFlags =
        FeatureFlags.withDefaults().enable(Feature.QUARANTINE_UNPARSEABLE_INDEX).build();

    DefaultConfigManagerMocks mocks =
        DefaultConfigManagerMocks.create(configJournalPath, featureFlags);

    var metrics = new MetricsFactory("ConfigInitializer", mocks.meterRegistry);
    Assert.assertEquals(
        1.0, metrics.counter("quarantinedIndexesAtStartup", Tags.of("type", "live")).count(), 0.01);
    Assert.assertEquals(
        0.0,
        metrics.counter("quarantinedIndexesAtStartup", Tags.of("type", "staged")).count(),
        0.01);
  }

  /**
   * A staged successor that cannot be parsed quarantines without touching the live index, so only
   * the staged counter records it.
   */
  @Test
  public void initialize_quarantinedStagedIndex_incrementsStagedCounter() throws Exception {
    QuarantinedIndex quarantinedStaged =
        QuarantinedIndexBuilder.quarantine(
            MOCK_INDEX_DEFINITION_GENERATION.incrementUser(
                MOCK_INDEX_DEFINITION_GENERATION.definition()));
    ConfigJournalV1 config =
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_INDEX_DEFINITION_GENERATION)
            .quarantinedStagedIndex(quarantinedStaged)
            .build();
    Path configJournalPath = DefaultConfigManagerMocks.getConfigJournalPath();
    FileUtils.atomicallyReplace(configJournalPath, JsonCodec.toJson(config.toBson()));
    FeatureFlags featureFlags =
        FeatureFlags.withDefaults().enable(Feature.QUARANTINE_UNPARSEABLE_INDEX).build();

    DefaultConfigManagerMocks mocks =
        DefaultConfigManagerMocks.create(configJournalPath, featureFlags);

    var metrics = new MetricsFactory("ConfigInitializer", mocks.meterRegistry);
    Assert.assertEquals(
        0.0, metrics.counter("quarantinedIndexesAtStartup", Tags.of("type", "live")).count(), 0.01);
    Assert.assertEquals(
        1.0,
        metrics.counter("quarantinedIndexesAtStartup", Tags.of("type", "staged")).count(),
        0.01);
  }

  /**
   * A quarantined live entry whose parseable staged successor auto-resolves it at boot is not a
   * degraded index, so it must not be counted: an alert for a self-healed condition is noise. This
   * is why the counter reads IncompatibleIndexes after moveSupersededQuarantinedIndexesToDropped
   * rather than the journal's quarantined entries.
   */
  @Test
  public void initialize_liveQuarantineSupersededByParseableStaged_leavesCountersAtZero()
      throws Exception {
    QuarantinedIndex quarantinedLive =
        QuarantinedIndexBuilder.quarantine(MOCK_INDEX_DEFINITION_GENERATION);
    IndexDefinitionGeneration stagedSuccessor =
        MOCK_INDEX_DEFINITION_GENERATION.incrementUser(
            MOCK_INDEX_DEFINITION_GENERATION.definition());
    ConfigJournalV1 config =
        ConfigJournalV1Builder.builder()
            .quarantinedLiveIndex(quarantinedLive)
            .stagedIndex(stagedSuccessor)
            .build();
    Path configJournalPath = DefaultConfigManagerMocks.getConfigJournalPath();
    FileUtils.atomicallyReplace(configJournalPath, JsonCodec.toJson(config.toBson()));
    FeatureFlags featureFlags =
        FeatureFlags.withDefaults().enable(Feature.QUARANTINE_UNPARSEABLE_INDEX).build();

    DefaultConfigManagerMocks mocks =
        DefaultConfigManagerMocks.create(configJournalPath, featureFlags);

    var metrics = new MetricsFactory("ConfigInitializer", mocks.meterRegistry);
    Assert.assertEquals(
        0.0, metrics.counter("quarantinedIndexesAtStartup", Tags.of("type", "live")).count(), 0.01);
    Assert.assertEquals(
        0.0,
        metrics.counter("quarantinedIndexesAtStartup", Tags.of("type", "staged")).count(),
        0.01);
  }

  /** A clean journal never reaches the quarantine path, so neither counter is touched. */
  @Test
  public void initialize_cleanJournal_leavesCountersAtZero() throws Exception {
    ConfigJournalV1 config =
        ConfigJournalV1Builder.builder().liveIndex(MOCK_INDEX_DEFINITION_GENERATION).build();
    Path configJournalPath = DefaultConfigManagerMocks.getConfigJournalPath();
    FileUtils.atomicallyReplace(configJournalPath, JsonCodec.toJson(config.toBson()));
    FeatureFlags featureFlags =
        FeatureFlags.withDefaults().enable(Feature.QUARANTINE_UNPARSEABLE_INDEX).build();

    DefaultConfigManagerMocks mocks =
        DefaultConfigManagerMocks.create(configJournalPath, featureFlags);

    var metrics = new MetricsFactory("ConfigInitializer", mocks.meterRegistry);
    Assert.assertEquals(
        0.0, metrics.counter("quarantinedIndexesAtStartup", Tags.of("type", "live")).count(), 0.01);
    Assert.assertEquals(
        0.0,
        metrics.counter("quarantinedIndexesAtStartup", Tags.of("type", "staged")).count(),
        0.01);
  }

  /**
   * Tests that the DefaultConfigManager does not try to do anything when running init() with no
   * config journal present.
   */
  @Test
  public void testInitNoConfigJournal() throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    mocks.assertEmptyOrNoConfigJournalInit();
  }

  /**
   * On clean shutdown an offline-serving unresolved auto-embedding index's reconstructed Lucene
   * reader is closed via its initialized-index catalog registration, so it does not leak.
   */
  @Test
  public void close_offlineServingAutoEmbeddingIndex_closesReconstructedReader() throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();

    InitializedVectorIndex reconstructed = mock(InitializedVectorIndex.class);
    when(reconstructed.getStatus()).thenReturn(IndexStatus.steady());
    IndexGeneration offlineServing =
        UnresolvedAutoEmbeddingIndexGeneration.createStale(
            MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT, reconstructed, Map.of());
    when(reconstructed.getGenerationId()).thenReturn(offlineServing.getGenerationId());
    // The generation is in the index catalog, and production registers its reconstructed derived
    // index in the initialized-index catalog under the same generation id, so shutdown closes the
    // reconstructed reader via that registration.
    mocks.indexCatalog.addIndex(offlineServing);
    mocks.mockedDependencies.initializedIndexCatalog.addIndex(reconstructed);

    mocks.configManager.close();

    verify(reconstructed).close();
  }

  /**
   * Tests that the DefaultConfigManager does not try to do anything when running init() with an
   * empty config journal present.
   */
  @Test
  public void testInitEmptyConfigJournal() throws Exception {
    // Test with an empty config journal
    var configJournal = ConfigJournalV1Builder.builder().build();
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create(configJournal);
    mocks.assertEmptyOrNoConfigJournalInit();
  }

  /**
   * Tests that a config journal with a deleted index properly cleans up the deleted index. Test for
   * both config versions.
   */
  @Test
  public void testInitDeletedIndexConfigJournal() throws Exception {
    ConfigJournalV1 configJournalV1 =
        ConfigJournalV1Builder.builder().deletedIndex(MOCK_INDEX_DEFINITION_GENERATION).build();
    assertInitDeleted(DefaultConfigManagerMocks.create(configJournalV1));
  }

  private void assertInitDeleted(DefaultConfigManagerMocks mocks) throws Exception {
    // Shouldn't have tried to add any indexes.
    mocks.assertNoIndexesAdded();

    // One index should have been dropped:
    mocks.assertOneIndexInstantiated();
    verify(mocks.mockedDependencies.createdIndexes.get(0).getIndex()).drop();

    // Should have written an updated config journal with no deleted indexes.
    mocks.assertEmptyConfigJournal();

    mocks.assertConfigUpdated(0, 0);
  }

  /**
   * Tests that a config journal with an existing index properly seeds the manager with the index.
   */
  @Test
  public void testInitExistingIndexConfigJournalV1() throws Exception {
    // Write a config journal with a deleted index.
    ConfigJournalV1 configJournal =
        ConfigJournalV1Builder.builder().liveIndex(MOCK_INDEX_DEFINITION_GENERATION).build();

    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create(configJournal);

    mocks.assertIndexCatalogedAndReplicated();
    mocks.waitForIndexInitialization(MOCK_INDEX_DEFINITION_GENERATION.getGenerationId());
    mocks.assertConfigUpdated(1, 1, configJournal);
  }

  /**
   * Verifies that a config journal removes indexes that are not present in the initial confcall
   * response before index initialization.
   */
  @Test
  public void testRemoveDeletedIndexesFromConfigJournalWithoutRequiredIndex() throws Exception {
    // Initialize the config journal with a mock index
    ConfigJournalV1 configJournal =
        ConfigJournalV1Builder.builder().liveIndex(MOCK_INDEX_DEFINITION_GENERATION).build();
    // Prepare an empty updated config journal for assertions
    ConfigJournalV1 updatedConfigJournal = ConfigJournalV1Builder.builder().build();
    // Simulate desired index set without the mock index
    Set<ObjectId> desiredIndexIds = new HashSet<>();
    desiredIndexIds.add(new ObjectId());

    DefaultConfigManagerMocks mocks =
        DefaultConfigManagerMocks.create(configJournal, desiredIndexIds);
    var metrics = new MetricsFactory("ConfigInitializer", mocks.meterRegistry);
    // Assert that the index catalog size is zero, indicating removal
    Assert.assertEquals(
        "Index catalog size should be zero when the mock index is not desired.",
        0,
        mocks.mockedDependencies.configState.indexCatalog.getSize());
    // Verify that the configuration was updated appropriately
    mocks.assertConfigUpdated(0, 0, updatedConfigJournal);
    Assert.assertEquals(
        1.0, metrics.counter("removedIndexesAtStartup", Tags.of("type", "live")).count(), 0.01);
  }

  /**
   * Verifies that an index can be initialized when it is included in the initial confcall response.
   */
  @Test
  public void testRemoveDeletedIndexesFromConfigJournalWithRequiredIndex() throws Exception {
    // Initialize the config journal with a mock index
    ConfigJournalV1 configJournal =
        ConfigJournalV1Builder.builder().liveIndex(MOCK_INDEX_DEFINITION_GENERATION).build();
    // Simulate desired index set including the mock index
    Set<ObjectId> desiredIndexIds = new HashSet<>();
    desiredIndexIds.add(MOCK_INDEX_DEFINITION_GENERATION.getIndexId());

    DefaultConfigManagerMocks mocks =
        DefaultConfigManagerMocks.create(configJournal, desiredIndexIds);
    var metrics = new MetricsFactory("ConfigInitializer", mocks.meterRegistry);
    // Assert that the index catalog size reflects the presence of the mock index
    mocks.assertIndexCatalogedAndReplicated();
    Assert.assertEquals(
        "Index catalog size should be one when the mock index is desired.",
        1,
        mocks.mockedDependencies.configState.indexCatalog.getSize());
    // Verify that the configuration was updated correctly after index inclusion
    mocks.waitForIndexInitialization(MOCK_INDEX_DEFINITION_GENERATION.getGenerationId());
    mocks.assertConfigUpdated(1, 1, configJournal);
    Assert.assertEquals(
        0, metrics.counter("removedIndexesAtStartup", Tags.of("type", "live")).count(), 0.01);
  }

  /** Tests that nothing should happen when no change is requested on update. */
  @Test
  public void testUpdateNoChange() throws Exception {
    // Create a DefaultConfigManager with no indexes.
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    mocks.clearInvocations();

    // Update the config manager with a desired state of no indexes.
    mocks.configManager.update(emptyList(), emptyList(), emptyList(), emptySet(), emptySet());

    mocks.assertNoIndexActivity();
    mocks.assertConfigUpdated(0, 0);

    // Should have written an empty updated config journal.
    mocks.assertEmptyConfigJournal();
  }

  /** Tests that adding an index in update() registers it and begins replication. */
  @Test
  public void testUpdateAddIndex() throws Exception {
    // Create a DefaultConfigManager with no indexes.
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();

    mocks.clearInvocations();

    // Update the config manager with our desired index.
    List<SearchIndexDefinition> indexDefinitions = Collections.singletonList(MOCK_INDEX_DEFINITION);

    mocks.configManager.update(emptyList(), indexDefinitions, emptyList(), emptySet(), emptySet());

    mocks.assertIndexCatalogedAndReplicated();
    mocks.waitForIndexInitialization(MOCK_INDEX_DEFINITION_GENERATION_CURRENT.getGenerationId());
    mocks.assertConfigUpdated(
        1,
        1,
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_INDEX_DEFINITION_GENERATION_CURRENT)
            .build());
  }

  /** Tests that adding a vector index in update() registers it and begins replication. */
  @Test
  public void testUpdateAddVectorIndex() throws Exception {
    // Create a DefaultConfigManager with no indexes.
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();

    mocks.clearInvocations();

    // Update the config manager with our desired index.
    List<VectorIndexDefinition> indexDefinitions =
        Collections.singletonList(MOCK_VECTOR_DEFINITION);

    mocks.configManager.update(indexDefinitions, emptyList(), emptyList(), emptySet(), emptySet());

    mocks.assertIndexCatalogedAndReplicated();
    mocks.waitForIndexInitialization(
        MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT.getGenerationId());
    mocks.assertConfigUpdated(
        1,
        1,
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT)
            .build());
  }

  /** Tests that removing an index in update() properly cleans it up. */
  @Test
  public void testUpdateRemoveIndex() throws Exception {
    // Create a DefaultConfigManager with no indexes.
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();

    // Update the config manager with our desired index.
    List<SearchIndexDefinition> indexDefinitions = Collections.singletonList(MOCK_INDEX_DEFINITION);
    mocks.configManager.update(emptyList(), indexDefinitions, emptyList(), emptySet(), emptySet());

    // Get the Index so we can verify it was dropped later.
    Optional<IndexGeneration> optionalIndex = mocks.indexCatalog.getIndexById(MOCK_INDEX_ID);
    Assert.assertTrue(optionalIndex.isPresent());
    Index index = optionalIndex.get().getIndex();
    mocks.waitForIndexInitialization(optionalIndex.get().getGenerationId());
    InitializedIndex initializedIndex = mocks.getInitializedIndex(optionalIndex.get());

    mocks.clearInvocations();

    // Now update the config manager again to remove the index.
    mocks.configManager.update(emptyList(), emptyList(), emptyList(), emptySet(), emptySet());

    // It's very important that the indexed dropped in the following specific order (see comment in
    // DesiredConfigStateUpdater::dropIndex), so we test for it explicitly.
    InOrder inOrder =
        Mockito.inOrder(
            mocks.indexCatalog,
            mocks.cursorManager,
            mocks.lifecycleManager,
            index,
            initializedIndex);
    inOrder.verify(mocks.indexCatalog).removeIndex(MOCK_INDEX_ID);
    inOrder.verify(mocks.lifecycleManager).dropIndex(MOCK_INDEX_GENERATION_ID);
    inOrder.verify(index, atLeastOnce()).close();
    inOrder.verify(index, atLeastOnce()).drop();

    mocks.assertConfigUpdated(0, 0);

    // Should have written an empty config journal.
    mocks.assertEmptyConfigJournal();
  }

  /** Tests that removing a vector index in update() properly cleans it up. */
  @Test
  public void testUpdateRemoveVectorIndex() throws Exception {
    // Create a DefaultConfigManager with no indexes.
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();

    // Update the config manager with our desired index.
    List<VectorIndexDefinition> indexDefinitions =
        Collections.singletonList(MOCK_VECTOR_DEFINITION);
    mocks.configManager.update(indexDefinitions, emptyList(), emptyList(), emptySet(), emptySet());

    // Get the Index so we can verify it was dropped later.
    Optional<IndexGeneration> optionalIndex =
        mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID);
    Assert.assertTrue(optionalIndex.isPresent());
    Index index = optionalIndex.get().getIndex();
    mocks.waitForIndexInitialization(optionalIndex.get().getGenerationId());
    InitializedIndex initializedIndex = mocks.getInitializedIndex(optionalIndex.get());
    mocks.clearInvocations();

    // Now update the config manager again to remove the index.
    mocks.configManager.update(emptyList(), emptyList(), emptyList(), emptySet(), emptySet());

    // It's very important that the indexed dropped in the following specific order (see comment in
    // DesiredConfigStateUpdater::dropIndex), so we test for it explicitly.
    InOrder inOrder =
        Mockito.inOrder(
            mocks.indexCatalog,
            mocks.cursorManager,
            mocks.lifecycleManager,
            index,
            initializedIndex);
    inOrder.verify(mocks.indexCatalog).removeIndex(VectorIndex.MOCK_INDEX_ID);
    inOrder
        .verify(mocks.lifecycleManager, atLeastOnce())
        .dropIndex(VectorIndex.MOCK_INDEX_GENERATION_ID);
    inOrder.verify(index, atLeastOnce()).close();
    inOrder.verify(index, atLeastOnce()).drop();

    mocks.assertConfigUpdated(0, 0);

    // Should have written an empty config journal.
    mocks.assertEmptyConfigJournal();
  }

  /** Tests the "happy path" of an index swap. */
  @Test
  public void testIndexSwappedInAndOldOnePhasesOut() throws Exception {
    var mocks = DefaultConfigManagerMocks.create();
    ConfigStateMocks configStateMocks = mocks.mockedDependencies;

    var newDef =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .analyzerName("lucene.keyword")
            .build();
    mocks.configManager.update(
        emptyList(), List.of(MOCK_INDEX_DEFINITION), emptyList(), emptySet(), emptySet());
    configStateMocks.assertIndexCatalogSize(1);

    // important for this test, otherwise the index will be created in steady state and be swapped
    // in the same cycle.
    configStateMocks.setStatusForCreatedIndexes(IndexStatus.initialSync());

    // index definition changed, we expect a swap to be staged:
    mocks.configManager.update(emptyList(), List.of(newDef), emptyList(), emptySet(), emptySet());
    var oldIndex = configStateMocks.indexCatalog.getIndexById(MOCK_INDEX_ID).orElseThrow();
    var newIndex = configStateMocks.staged.getIndex(MOCK_INDEX_ID).orElseThrow();
    mocks.waitForIndexInitialization(oldIndex.getGenerationId());
    // there was a query running on the old index. So it can't be dropped yet.
    var cursorId =
        mocks.cursorManager.newCursor(
                MOCK_INDEX_DEFINITION.getDatabase(),
                MOCK_INDEX_DEFINITION.getLastObservedCollectionName(),
                MOCK_INDEX_DEFINITION.getCollectionUuid(),
                Optional.empty(),
                Query.mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    newIndex.getIndex().setStatus(IndexStatus.steady());
    mocks.configManager.update(emptyList(), List.of(newDef), emptyList(), emptySet(), emptySet());

    // indexes should have been swapped at this point
    configStateMocks.assertStagedIndexesAre();
    configStateMocks.assertLiveIndexesAre(newIndex);
    configStateMocks.assertPhasingOutIndexesAre(oldIndex);

    mocks.cursorManager.killCursor(cursorId);
    // in this config cycle we have no cursors on the old index, so it is ready to be dropped.
    mocks.configManager.update(emptyList(), List.of(newDef), emptyList(), emptySet(), emptySet());
    configStateMocks.assertLiveIndexesAre(newIndex);
    configStateMocks.assertPhasingOutIndexesAre();
  }

  /**
   * Situation where we receive a changed index definition for a staged swap, that is, the new
   * definition is different than the current and staged definitions.
   */
  @Test
  public void testSwapsOnDefinitionChangeDuringSwapsReStagesSwap() throws Exception {
    var mocks = DefaultConfigManagerMocks.create();
    ConfigStateMocks configStateMocks = mocks.mockedDependencies;

    var def1 =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .analyzerName("lucene.keyword")
            .build();
    var def2 =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).analyzerName("lucene.cjk").build();

    // first create an index
    mocks.configManager.update(
        emptyList(), List.of(MOCK_INDEX_DEFINITION), emptyList(), emptySet(), emptySet());
    configStateMocks.assertIndexCatalogSize(1);

    // important for this test, otherwise the index will be created in steady state and be swapped
    // in the same cycle.
    configStateMocks.setStatusForCreatedIndexes(IndexStatus.initialSync());

    // index definition changed, we expect a swap to be staged:
    mocks.configManager.update(emptyList(), List.of(def1), emptyList(), emptySet(), emptySet());

    mocks.waitForIndexInitialization(Generation.CURRENT.generationId(MOCK_INDEX_ID));
    var oldIndex = configStateMocks.indexCatalog.getIndexById(MOCK_INDEX_ID).orElseThrow();
    InitializedIndex initializedIndex = mocks.getInitializedIndex(oldIndex);
    var newIndexDef1 = configStateMocks.staged.getIndex(MOCK_INDEX_ID).orElseThrow();

    mocks.configManager.update(emptyList(), List.of(def2), emptyList(), emptySet(), emptySet());

    // def1 no longer relevant and should have been dropped
    verify(newIndexDef1.getIndex(), atLeastOnce()).drop();
    verify(initializedIndex, times(0)).close();
    var newIndexDef2 = configStateMocks.staged.getIndex(MOCK_INDEX_ID).orElseThrow();
    Assert.assertSame(def2, newIndexDef2.getDefinition());

    configStateMocks.assertStagedIndexesAre(newIndexDef2);
    configStateMocks.assertLiveIndexesAre(oldIndex);
  }

  /**
   * user wants MOCK_INDEX_DEFINITION, then def1 then MOCK_INDEX_DEFINITION again. When we go back
   * to MOCK_INDEX_DEFINITION, we should realize that the staged index is no longer relevant and
   * drop it. This is possible as the index corresponding to MOCK_INDEX_DEFINITION is still in the
   * catalog.
   */
  @Test
  public void testSwapOnDefinitionRevertedDuringSwapDropSwap() throws Exception {
    var mocks = DefaultConfigManagerMocks.create();
    var configStateMocks = mocks.mockedDependencies;

    // first create an index and "wait" for steady
    mocks.configManager.update(
        emptyList(), List.of(MOCK_INDEX_DEFINITION), emptyList(), emptySet(), emptySet());
    var oldIndex =
        mocks.indexCatalog.getIndexById(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow();
    oldIndex.getIndex().setStatus(IndexStatus.steady());
    configStateMocks.setStatusForCreatedIndexes(IndexStatus.initialSync());
    // stage a swap
    var def1 =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .analyzerName("lucene.keyword")
            .build();
    mocks.configManager.update(emptyList(), List.of(def1), emptyList(), emptySet(), emptySet());
    // "regret" and revert to the first definition:
    mocks.configManager.update(
        emptyList(), List.of(MOCK_INDEX_DEFINITION), emptyList(), emptySet(), emptySet());
    configStateMocks.assertStagedIndexesAre();
    configStateMocks.assertLiveIndexesAre(oldIndex);
    configStateMocks.assertIndexCatalogSize(1);
  }

  @Test
  public void testImmediateIndexSwapIfItIsNotSteadyAndNewGenerationAdded() throws Exception {
    var mocks = DefaultConfigManagerMocks.create();
    var configStateMocks = mocks.mockedDependencies;
    var liveDefinition = MOCK_INDEX_DEFINITION;
    var stagedDefinition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .analyzerName("lucene.keyword")
            .build();

    // create an index and "wait" for initial sync
    mocks.configManager.update(
        emptyList(), List.of(liveDefinition), emptyList(), emptySet(), emptySet());
    var liveIndex =
        mocks.indexCatalog.getIndexById(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow();
    liveIndex.getIndex().setStatus(IndexStatus.initialSync());

    // stage a swap with the updated defintion
    mocks.configManager.update(
        emptyList(), List.of(stagedDefinition), emptyList(), emptySet(), emptySet());

    // check that previously live index is dropped
    verify(liveIndex.getIndex(), atLeastOnce()).drop();

    // check the staged index took it place
    configStateMocks.assertIndexCatalogSize(1);
    configStateMocks.assertPhasingOutIndexesAre();
    configStateMocks.assertStagedIndexesAre();
    Assert.assertSame(
        stagedDefinition,
        configStateMocks
            .indexCatalog
            .getIndexById(stagedDefinition.getIndexId())
            .orElseThrow()
            .getDefinition());
  }

  @Test
  public void testRecoveringIndexStagedWhenNotCurrentlyStaged() throws Exception {
    var mocks = DefaultConfigManagerMocks.create();
    var configStateMocks = mocks.mockedDependencies;
    var liveDefinition = MOCK_INDEX_DEFINITION;

    // Create an index and set recovering status
    mocks.configManager.update(
        emptyList(), List.of(liveDefinition), emptyList(), emptySet(), emptySet());
    var liveIndex =
        mocks.indexCatalog.getIndexById(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow();
    liveIndex.getIndex().setStatus(IndexStatus.recoveringNonTransient(new BsonTimestamp()));

    // Run another update cycle with the same definitions
    configStateMocks.clearInvocations();
    mocks.configManager.update(
        emptyList(), List.of(liveDefinition), emptyList(), emptySet(), emptySet());

    // Check that the live index still exists and a new one was staged with the same definition
    configStateMocks.assertIndexCatalogSize(1);
    configStateMocks.assertAtLeastOneIndexStagedAndReplicated();
    configStateMocks.assertJournalPersistedAtLeastOnce();
    Assert.assertSame(
        liveDefinition,
        configStateMocks
            .staged
            .getIndex(liveDefinition.getIndexId())
            .orElseThrow()
            .getDefinition());
  }

  @Test
  public void testRecoveringIndexDoesNotStageIfNewDefinitionStaged() throws Exception {
    var mocks = DefaultConfigManagerMocks.create();
    var configStateMocks = mocks.mockedDependencies;
    var liveDefinition = MOCK_INDEX_DEFINITION;
    var stagedDefinition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .analyzerName("lucene.keyword")
            .build();
    configStateMocks.setStatusForCreatedIndexes(IndexStatus.initialSync());

    // Create an index in steady state
    mocks.configManager.update(
        emptyList(), List.of(liveDefinition), emptyList(), emptySet(), emptySet());
    var liveIndex =
        mocks.indexCatalog.getIndexById(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow();
    liveIndex.getIndex().setStatus(IndexStatus.steady());

    // Stage a swap with updated definition
    mocks.configManager.update(
        emptyList(), List.of(stagedDefinition), emptyList(), emptySet(), emptySet());

    // Check that the live index still exists and a new one was staged with the same definition
    configStateMocks.assertIndexCatalogSize(1);
    configStateMocks.assertAtLeastOneIndexStagedAndReplicated();
    var stagedIndex =
        configStateMocks.staged.getIndex(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow();

    // Fail the original index and run another update cycle
    configStateMocks.clearInvocations();
    liveIndex.getIndex().setStatus(IndexStatus.recoveringNonTransient(new BsonTimestamp()));
    mocks.configManager.update(
        emptyList(), List.of(stagedDefinition), emptyList(), emptySet(), emptySet());

    // Check that no indexes were staged.
    configStateMocks.assertNoIndexActivity();
    Assert.assertSame(
        stagedIndex,
        configStateMocks.staged.getIndex(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow());
  }

  @Test
  public void testGetIndexInfos() throws Exception {
    // Create a DefaultConfigManager with two indexes, one in STEADY and one in DOES_NOT_EXIST.
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();

    // Once the DefaultConfigManager is initialized, it should return no index stats
    assertEquals(emptyList(), mocks.configManager.getIndexInfos());

    // Once an index is added, its stats should be available in the returned stats, as the
    // cache should be updated at the end of the update call.
    mocks.configManager.update(
        singletonList(MOCK_VECTOR_DEFINITION),
        singletonList(MOCK_INDEX_DEFINITION),
        emptyList(),
        emptySet(),
        emptySet());

    mocks.waitForIndexInitialization(
        MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT.getGenerationId());
    mocks.waitForIndexInitialization(MOCK_INDEX_DEFINITION_GENERATION_CURRENT.getGenerationId());

    List<IndexInformation> expected =
        List.of(
            new IndexInformation.Search(
                MOCK_INDEX_DEFINITION,
                MOCK_INDEX_STATUS,
                List.of(),
                new AggregatedIndexMetrics(0, 0, new BsonTimestamp(0), 0, 0L),
                Optional.of(
                    new IndexDetailedStatus.Search(
                        MOCK_SYNONYM_DETAILED_STATUS,
                        MOCK_INDEX_DEFINITION,
                        List.of(),
                        MOCK_INDEX_STATUS,
                        MOCK_INDEX_DEFINITION_GENERATION_CURRENT.getGenerationId(),
                        Optional.of(
                            new AggregatedIndexMetrics(0, 0, new BsonTimestamp(0), 0, 0L)))),
                Optional.empty(),
                MOCK_SYNONYM_STATUS),
            new IndexInformation.Vector(
                MOCK_VECTOR_DEFINITION,
                IndexStatus.steady(),
                List.of(),
                new AggregatedIndexMetrics(0, 0, new BsonTimestamp(0), 0, 0L),
                Optional.of(
                    new IndexDetailedStatus.Vector(
                        MOCK_VECTOR_DEFINITION,
                        IndexStatus.steady(),
                        MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT.getGenerationId(),
                        Optional.of(
                            new AggregatedIndexMetrics(0, 0, new BsonTimestamp(0), 0, 0L)))),
                Optional.empty()));

    var actual = new ArrayList<>(mocks.configManager.getIndexInfos());
    actual.sort(Comparator.comparing(info -> info.getDefinition().getType()));
    assertThat(actual).containsExactlyElementsIn(expected);
  }

  @Test
  public void getQuarantinedIndexInfos_reflectsBothJournalStatesAndTracksRemovals()
      throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    assertThat(mocks.configManager.getQuarantinedIndexInfos().isEmpty()).isTrue();

    QuarantinedIndex live = quarantined(new ObjectId(), "liveIndex");
    QuarantinedIndex staged = quarantined(new ObjectId(), "stagedIndex");
    mocks.mockedDependencies.incompatibleIndexes.addLiveIndex(live);
    mocks.mockedDependencies.incompatibleIndexes.addStagedIndex(staged);
    mocks.configManager.refreshIndexInfos();

    QuarantinedIndexInfos infos = mocks.configManager.getQuarantinedIndexInfos();
    assertThat(infos.liveIndexes()).containsExactly(live);
    assertThat(infos.stagedIndexes()).containsExactly(staged);

    mocks.mockedDependencies.incompatibleIndexes.removeLiveIndex(live.indexId());
    mocks.configManager.refreshIndexInfos();

    QuarantinedIndexInfos refreshed = mocks.configManager.getQuarantinedIndexInfos();
    assertThat(refreshed.liveIndexes()).isEmpty();
    assertThat(refreshed.stagedIndexes()).containsExactly(staged);
  }

  private static QuarantinedIndex quarantined(ObjectId indexId, String name) {
    return new QuarantinedIndex(
        new JournalEntryIdentity(
            indexId,
            "db",
            UUID.randomUUID(),
            name,
            new Generation(new UserIndexVersion(1), IndexFormatVersion.CURRENT)),
        IndexDefinitionGeneration.Type.SEARCH,
        "missing field 'fieldFromTheFuture'",
        new BsonDocument("fieldFromTheFuture", new BsonBoolean(true)));
  }

  @Test
  public void testGetLiveIndexes() throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    assertEquals(emptyList(), mocks.configManager.getLiveIndexes());

    mocks.configManager.update(
        singletonList(MOCK_VECTOR_DEFINITION),
        singletonList(MOCK_INDEX_DEFINITION),
        emptyList(),
        emptySet(),
        emptySet());

    List<IndexDefinition> expected = List.of(MOCK_INDEX_DEFINITION, MOCK_VECTOR_DEFINITION);
    var actual = new ArrayList<>(mocks.configManager.getLiveIndexes());
    actual.sort(Comparator.comparing(IndexDefinition::getType));
    Assert.assertEquals("expected index information did not match returned", expected, actual);
  }

  @Test
  public void testGetGroupedIndexStats() throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    var configStateMocks = mocks.mockedDependencies;
    var stagedDefinition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .analyzerName("lucene.keyword")
            .build();
    configStateMocks.setStatusForCreatedIndexes(IndexStatus.initialSync());

    // Once the DefaultConfigManager is initialized, it should return no stats
    assertEquals(emptyList(), mocks.configManager.getGroupedIndexGenerationMetrics());

    // Create an index in steady state
    mocks.configManager.update(
        emptyList(), singletonList(MOCK_INDEX_DEFINITION), emptyList(), emptySet(), emptySet());
    var liveIndex =
        mocks.indexCatalog.getIndexById(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow();
    liveIndex.getIndex().setStatus(IndexStatus.steady());

    // Stage a swap with updated definition
    mocks.configManager.update(
        emptyList(), List.of(stagedDefinition), emptyList(), emptySet(), emptySet());

    GenerationId generationId = new GenerationId(MOCK_INDEX_ID, Generation.CURRENT);
    IndexGenerationStateMetrics initialIndex =
        IndexGenerationStateMetricsBuilder.builder()
            .state(IndexConfigState.LIVE)
            .indexGenerationMetrics(
                IndexGenerationMetricsBuilder.builder()
                    .generationId(generationId)
                    .indexMetrics(
                        IndexMetricsBuilder.builder()
                            .indexDefinition(MOCK_INDEX_DEFINITION)
                            .queryingMetrics(
                                IndexMetricsBuilder.QueryingMetricsBuilder.builder()
                                    .searchResultBatchLatency(
                                        SerializableTimerBuilder.builder()
                                            .timeUnit(TimeUnit.MILLISECONDS)
                                            .percentiles(
                                                PercentilesBuilder.builder()
                                                    .zeroPercentiles()
                                                    .build())
                                            .build())
                                    .tokenFacetsStateRefresh(
                                        SerializableTimerBuilder.builder()
                                            .timeUnit(TimeUnit.MILLISECONDS)
                                            .percentiles(
                                                PercentilesBuilder.builder()
                                                    .zeroPercentiles()
                                                    .build())
                                            .build())
                                    .build())
                            .indexingMetrics(
                                IndexMetricsBuilder.IndexingMetricsBuilder.builder().build())
                            .indexStatus(IndexStatus.steady())
                            .build())
                    .build())
            .build();
    GenerationId stagedGenerationId =
        new GenerationId(MOCK_INDEX_ID, Generation.CURRENT.incrementUser());
    IndexGenerationStateMetrics stagedIndex =
        IndexGenerationStateMetricsBuilder.builder()
            .state(IndexConfigState.STAGED)
            .indexGenerationMetrics(
                IndexGenerationMetricsBuilder.builder()
                    .generationId(stagedGenerationId)
                    .indexMetrics(
                        IndexMetricsBuilder.builder()
                            .indexDefinition(stagedDefinition)
                            .queryingMetrics(
                                IndexMetricsBuilder.QueryingMetricsBuilder.builder()
                                    .searchResultBatchLatency(
                                        SerializableTimerBuilder.builder()
                                            .timeUnit(TimeUnit.MILLISECONDS)
                                            .percentiles(
                                                PercentilesBuilder.builder()
                                                    .zeroPercentiles()
                                                    .build())
                                            .build())
                                    .tokenFacetsStateRefresh(
                                        SerializableTimerBuilder.builder()
                                            .timeUnit(TimeUnit.MILLISECONDS)
                                            .percentiles(
                                                PercentilesBuilder.builder()
                                                    .zeroPercentiles()
                                                    .build())
                                            .build())
                                    .build())
                            .indexStatus(IndexStatus.initialSync())
                            .build())
                    .build())
            .build();
    mocks.waitForIndexInitialization(generationId);
    mocks.waitForIndexInitialization(stagedGenerationId);
    // stagedIndexGenerationStats needs to be first
    GroupedIndexGenerationMetrics groupedIndexGenerationMetrics =
        new GroupedIndexGenerationMetrics(MOCK_INDEX_ID, List.of(stagedIndex, initialIndex));

    List<GroupedIndexGenerationMetrics> expected = List.of(groupedIndexGenerationMetrics);
    Assert.assertEquals(
        "expected grouped index stats did not match returned",
        expected,
        mocks.configManager.getGroupedIndexGenerationMetrics());
  }

  @Test
  public void testConfigMayBeWrittenInNonExistingDirectory() throws Exception {
    var path =
        Paths.get(
            DefaultConfigManagerMocks.getConfigJournalPath().toString(),
            "a",
            "b",
            "c",
            "configJournal.json");
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create(path);
    mocks.assertEmptyOrNoConfigJournalInit();
    // updates will write an empty config, and create all the directories
    mocks.configManager.update(emptyList(), emptyList(), emptyList(), emptySet(), emptySet());
    Assert.assertTrue(path.toFile().exists());
  }

  @Test
  public void testCloseClosesCursorManagerAndReplicationManager() throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    mocks.configManager.close();
    InOrder inOrder = Mockito.inOrder(mocks.cursorManager, mocks.lifecycleManager);

    inOrder.verify(mocks.cursorManager).close();
    inOrder.verify(mocks.lifecycleManager).shutdown();
  }

  @Test
  public void testCloseClosesAllIndexesAndIndexFactory() throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    var liveGen = mocks.mockedDependencies.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    Index live = liveGen.getIndex();
    mocks.waitForIndexInitialization(liveGen.getGenerationId());
    InitializedIndex liveInitializedIndex = mocks.getInitializedIndex(liveGen);

    var stagedGen = mocks.mockedDependencies.stageIndex(live.getDefinition().getIndexId());
    Index staged = stagedGen.getIndex();
    mocks.waitForIndexInitialization(stagedGen.getGenerationId());
    InitializedIndex stagedInitializedIndex = mocks.getInitializedIndex(stagedGen);

    var phasedOutGen =
        mocks.mockedDependencies.addIndex(new ObjectId(), ConfigStateMocks.State.PHASE_OUT);
    Index phasedOut = phasedOutGen.getIndex();
    InitializedIndex phasedOutInitializedIndex =
        mocks.mockedDependencies.waitAndGetInitializedIndex(phasedOutGen.getGenerationId());

    mocks.configManager.close();

    var sequence =
        inOrder(
            live,
            staged,
            phasedOut,
            mocks.indexFactory,
            liveInitializedIndex,
            stagedInitializedIndex,
            phasedOutInitializedIndex);
    sequence.verify(stagedInitializedIndex).close();
    sequence.verify(liveInitializedIndex).close();
    sequence.verify(phasedOutInitializedIndex).close();
    sequence.verify(mocks.indexFactory).close();
  }

  @Test
  public void getInitialSyncStatus_closedGate_pausesInitialSync() throws Exception {
    // Create a closed gate to start.
    var initialSyncGate = ToggleGate.closed();

    var replicationStateMonitor =
        ReplicationStateMonitor.builder()
            .setInitialSyncGate(initialSyncGate)
            .setReplicationGate(ToggleGate.opened())
            .build();
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create(replicationStateMonitor);

    // Check that status is created with initial sync paused when gate is closed.
    assertThat(mocks.configManager.getReplicationStatus().initialSyncStatus());
    assertThat(mocks.configManager.getReplicationStatus().initialSyncStatus().isInitialSyncPaused())
        .isTrue();

    // Check that status is created with initial sync not paused when gate is open.
    initialSyncGate.open();
    assertThat(mocks.configManager.getReplicationStatus().initialSyncStatus());
    assertThat(mocks.configManager.getReplicationStatus().initialSyncStatus().isInitialSyncPaused())
        .isFalse();
  }

  @Test
  public void getReplicationStatus_reportsReplicationPathsIndependently() throws Exception {
    var mocks = DefaultConfigManagerMocks.create();

    Mockito.doReturn(true).when(mocks.lifecycleManager).isReplicationSupported();
    Mockito.doReturn(true).when(mocks.lifecycleManager).isAutoEmbeddingReplicationSupported();
    var replicationStatus = mocks.configManager.getReplicationStatus();
    assertThat(replicationStatus.isReplicationStopped()).isFalse();
    assertThat(replicationStatus.isAutoEmbeddingMaterializedViewStopped()).isFalse();

    Mockito.doReturn(false).when(mocks.lifecycleManager).isAutoEmbeddingReplicationSupported();
    var autoEmbeddingStoppedStatus = mocks.configManager.getReplicationStatus();
    assertThat(autoEmbeddingStoppedStatus.isReplicationStopped()).isFalse();
    assertThat(autoEmbeddingStoppedStatus.isAutoEmbeddingMaterializedViewStopped()).isTrue();

    Mockito.doReturn(false).when(mocks.lifecycleManager).isReplicationSupported();
    Mockito.doReturn(true).when(mocks.lifecycleManager).isAutoEmbeddingReplicationSupported();
    var replicationStoppedStatus = mocks.configManager.getReplicationStatus();
    assertThat(replicationStoppedStatus.isReplicationStopped()).isTrue();
    assertThat(replicationStoppedStatus.isAutoEmbeddingMaterializedViewStopped()).isFalse();

    Mockito.doReturn(false).when(mocks.lifecycleManager).isAutoEmbeddingReplicationSupported();
    var bothStoppedStatus = mocks.configManager.getReplicationStatus();
    assertThat(bothStoppedStatus.isReplicationStopped()).isTrue();
    assertThat(bothStoppedStatus.isAutoEmbeddingMaterializedViewStopped()).isTrue();
  }

  @Test
  public void testHandleReplicationAndSyncSourceUpdate_changedSyncSource_restartsReplication()
      throws Exception {
    var mocks = DefaultConfigManagerMocks.create();
    mocks.clearInvocations();

    var differentConfig =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://otherhost"))
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://otherhost"))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://otherhost"))
            .build();

    mocks.configManager.handleReplicationAndSyncSourceUpdate(differentConfig);

    verify(mocks.lifecycleManager).updateSyncSource(differentConfig);
  }

  @Test
  public void testHandleReplicationAndSyncSourceUpdate_missingMongodUri_restartsReplication()
      throws Exception {
    var mocks = DefaultConfigManagerMocks.create();
    mocks.clearInvocations();

    var configWithMissingUri =
        SyncSourceConfig.builder()
            // mongodSingleHostReplicationUri intentionally left empty — differs from
            // MOCK_SYNC_SOURCE_CONFIG, so a restart is expected.
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://host"))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://host"))
            .build();

    mocks.configManager.handleReplicationAndSyncSourceUpdate(configWithMissingUri);

    verify(mocks.lifecycleManager).updateSyncSource(configWithMissingUri);
  }

  @Test
  public void testHandleReplicationAndSyncSourceUpdate_unchangedSyncSource_doesNotRestart()
      throws Exception {
    var mocks = DefaultConfigManagerMocks.create();
    mocks.clearInvocations();

    mocks.configManager.handleReplicationAndSyncSourceUpdate(
        ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);

    verify(mocks.lifecycleManager, never()).updateSyncSource(any());
  }

  @Test
  public void
      testHandleReplicationAndSyncSourceUpdate_unchangedSyncSource_missingUris_skipsDiskCheck()
          throws Exception {
    var mocks = DefaultConfigManagerMocks.create();

    var noUriConfig =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://host"))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://host"))
            .build();

    // First call: config differs from MOCK_SYNC_SOURCE_CONFIG, triggering a restart.
    mocks.configManager.handleReplicationAndSyncSourceUpdate(noUriConfig);
    mocks.clearInvocations();

    // Second call: same no-URI config — sync source unchanged, URIs absent, disk check skipped.
    mocks.configManager.handleReplicationAndSyncSourceUpdate(noUriConfig);

    verify(mocks.lifecycleManager, never()).updateSyncSource(any());
  }

  @Test
  public void testHandleReplicationAndSyncSourceUpdate_unchangedSyncSourceAndDiskPressure_restarts()
      throws Exception {
    var replicationGate = ToggleGate.opened();
    var mocks = DefaultConfigManagerMocks.create(replicationStateMonitor(replicationGate));
    mocks.clearInvocations();

    // Simulate disk pressure crossing the pause threshold.
    replicationGate.close();

    // Same sync source with valid URIs — disk pressure changed, restart expected.
    mocks.configManager.handleReplicationAndSyncSourceUpdate(
        ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);

    verify(mocks.lifecycleManager).updateSyncSource(ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);
  }

  @Test
  public void
      testHandleReplicationAndSyncSourceUpdate_autoEmbeddingGateCloses_pausesOnlyMaterializedView()
          throws Exception {
    var replicationGate = ToggleGate.opened();
    var autoEmbeddingReplicationGate = ToggleGate.opened();
    var mocks =
        DefaultConfigManagerMocks.create(
            replicationStateMonitor(replicationGate), autoEmbeddingReplicationGate);
    Mockito.doReturn(true).when(mocks.lifecycleManager).hasAutoEmbeddingReplication();
    mocks.clearInvocations();

    autoEmbeddingReplicationGate.close();

    mocks.configManager.handleReplicationAndSyncSourceUpdate(
        ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);

    verify(mocks.lifecycleManager).shutdownAutoEmbeddingReplication();
    verify(mocks.lifecycleManager, never()).updateSyncSource(any());
    assertEquals(1.0, transitionCount(mocks, "pause"), 0.0);
    assertEquals(0.0, transitionCount(mocks, "resume"), 0.0);
    assertEquals(1.0, pausedGaugeValue(mocks), 0.0);
  }

  @Test
  public void
      testHandleReplicationAndSyncSourceUpdate_autoEmbeddingGateCloses_noManager_doesNothing()
          throws Exception {
    var replicationGate = ToggleGate.opened();
    var autoEmbeddingReplicationGate = ToggleGate.opened();
    var mocks =
        DefaultConfigManagerMocks.create(
            replicationStateMonitor(replicationGate), autoEmbeddingReplicationGate);
    // No auto-embedding replication on this node (hasAutoEmbeddingReplication() is false), so a
    // closed gate must not trigger a pause action or its logging on every cycle.
    mocks.clearInvocations();

    autoEmbeddingReplicationGate.close();

    mocks.configManager.handleReplicationAndSyncSourceUpdate(
        ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);

    verify(mocks.lifecycleManager, never()).shutdownAutoEmbeddingReplication();
    verify(mocks.lifecycleManager, never()).restartAutoEmbeddingReplication(any());
    assertEquals(0.0, transitionCount(mocks, "pause"), 0.0);
    assertEquals(0.0, transitionCount(mocks, "resume"), 0.0);
  }

  @Test
  public void
      testHandleReplicationAndSyncSourceUpdate_autoEmbeddingGateOpens_resumesOnlyMaterializedView()
          throws Exception {
    var replicationGate = ToggleGate.opened();
    var autoEmbeddingReplicationGate = ToggleGate.closed();
    var mocks =
        DefaultConfigManagerMocks.create(
            replicationStateMonitor(replicationGate), autoEmbeddingReplicationGate);
    Mockito.doReturn(true).when(mocks.lifecycleManager).hasAutoEmbeddingReplication();
    Mockito.doReturn(false).when(mocks.lifecycleManager).isAutoEmbeddingReplicationSupported();
    mocks.clearInvocations();

    autoEmbeddingReplicationGate.open();

    mocks.configManager.handleReplicationAndSyncSourceUpdate(
        ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);

    verify(mocks.lifecycleManager)
        .restartAutoEmbeddingReplication(ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);
    verify(mocks.lifecycleManager, never()).updateSyncSource(any());
    assertEquals(1.0, transitionCount(mocks, "resume"), 0.0);
    assertEquals(0.0, transitionCount(mocks, "pause"), 0.0);
    assertEquals(0.0, pausedGaugeValue(mocks), 0.0);
  }

  @Test
  public void
      testHandleReplicationAndSyncSourceUpdate_coupledGateCloses_usesFullReplicationRestart()
          throws Exception {
    var replicationGate = ToggleGate.opened();
    var mocks =
        DefaultConfigManagerMocks.create(replicationStateMonitor(replicationGate), replicationGate);
    mocks.clearInvocations();

    replicationGate.close();

    mocks.configManager.handleReplicationAndSyncSourceUpdate(
        ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);

    verify(mocks.lifecycleManager).updateSyncSource(ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);
    verify(mocks.lifecycleManager, never()).shutdownAutoEmbeddingReplication();
  }

  @Test
  public void testHandleReplicationAndSyncSourceUpdate_bothGatesClose_usesSingleFullRestart()
      throws Exception {
    var replicationGate = ToggleGate.opened();
    var autoEmbeddingReplicationGate = ToggleGate.opened();
    var mocks =
        DefaultConfigManagerMocks.create(
            replicationStateMonitor(replicationGate), autoEmbeddingReplicationGate);
    mocks.clearInvocations();

    replicationGate.close();
    autoEmbeddingReplicationGate.close();

    mocks.configManager.handleReplicationAndSyncSourceUpdate(
        ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);

    verify(mocks.lifecycleManager).updateSyncSource(ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);
    verify(mocks.lifecycleManager, never()).shutdownAutoEmbeddingReplication();
    verify(mocks.lifecycleManager, never()).restartAutoEmbeddingReplication(any());
  }

  /**
   * The lifecycle manager blocks inside shutdownReplication() before it returns its future
   * (CLOUDP-447488), so the timeout only bounds the shutdown when it runs off the calling thread. A
   * shutdown that never finishes has to crash at the timeout instead of holding the config manager
   * monitor that conf calls need; the caller thread terminating is what releases that monitor.
   */
  @Test
  @Crash.TestOnlyHaltHandler
  public void shutdownReplication_shutdownNeverFinishes_crashesAtTimeout() throws Exception {
    var mocks = DefaultConfigManagerMocks.create();
    CountDownLatch releaseShutdown = new CountDownLatch(1);
    Mockito.doAnswer(
            invocation -> {
              releaseShutdown.await();
              return FutureUtils.COMPLETED_FUTURE;
            })
        .when(mocks.lifecycleManager)
        .shutdownReplication();
    AtomicInteger exitCode = new AtomicInteger(-1);
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    Crash.setHaltHandlerForTesting(
        code -> {
          exitCode.set(code);
          throw new HaltError();
        });
    Thread caller =
        new Thread(() -> mocks.configManager.shutdownReplication(Duration.ofMillis(100)));
    caller.setUncaughtExceptionHandler((ignored, throwable) -> thrown.set(throwable));

    caller.start();
    caller.join(Duration.ofSeconds(30).toMillis());

    try {
      assertThat(caller.isAlive()).isFalse();
      assertThat(thrown.get()).isInstanceOf(HaltError.class);
      assertEquals("expected fail-exit code 1", 1, exitCode.get());
    } finally {
      Crash.clearHaltHandlerForTesting();
      releaseShutdown.countDown();
    }
  }

  private static ReplicationStateMonitor replicationStateMonitor(ToggleGate replicationGate) {
    return ReplicationStateMonitor.builder()
        .setReplicationGate(replicationGate)
        .setInitialSyncGate(ToggleGate.opened())
        .build();
  }

  private static double transitionCount(DefaultConfigManagerMocks mocks, String transition) {
    return mocks
        .meterRegistry
        .get("configManager.autoEmbeddingReplicationGateTransitions")
        .tag("transition", transition)
        .counter()
        .count();
  }

  private static double pausedGaugeValue(DefaultConfigManagerMocks mocks) {
    return mocks.meterRegistry.get("configManager.autoEmbeddingReplicationPaused").gauge().value();
  }

  /**
   * Validates that when a live index (v1) fails with INITIAL_SYNC_REPLICATION_FAILED_RETRY while a
   * staged replacement (v2) already exists, the recovery logic skips v1 instead of crashing.
   *
   * <p>This scenario occurs when a customer updates an index definition (staging v2) while v1's
   * initial sync replication fails in the background. Since IndexInitialSyncRecovery runs before
   * StagedIndexesSwapper in the update cycle, it must recognize that a staged replacement already
   * handles v1's index ID and avoid retrying it. StagedIndexesSwapper then swaps v2 into live
   * because v1 is non-serviceable (FAILED).
   */
  @Test
  public void testFailedInitialSyncIndexWithStagedReplacement_skipsRetry() throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults().enable(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK).build();
    var mocks = DefaultConfigManagerMocks.create(featureFlags);
    var configStateMocks = mocks.mockedDependencies;

    // Create v1 and let it reach steady state.
    mocks.configManager.update(
        emptyList(), List.of(MOCK_INDEX_DEFINITION), emptyList(), emptySet(), emptySet());
    var liveIndex = mocks.indexCatalog.getIndexById(MOCK_INDEX_ID).orElseThrow();
    liveIndex.getIndex().setStatus(IndexStatus.steady());

    // Future indexes created in initial sync (so v2 won't be immediately swapped in the
    // same cycle it is staged).
    configStateMocks.setStatusForCreatedIndexes(IndexStatus.initialSync());

    // Change the definition to stage v2.
    var newDef =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .analyzerName("lucene.keyword")
            .build();
    mocks.configManager.update(emptyList(), List.of(newDef), emptyList(), emptySet(), emptySet());

    var stagedIndex = configStateMocks.staged.getIndex(MOCK_INDEX_ID).orElseThrow();

    // Simulate v1 failing with a retryable initial sync failure in the background.
    liveIndex
        .getIndex()
        .setStatus(
            IndexStatus.failed(
                "initial sync failed", IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY));

    configStateMocks.clearInvocations();

    // Run another update cycle. IndexInitialSyncRecovery should skip v1 because v2 is staged.
    // Then StagedIndexesSwapper swaps v2 into live because v1 is non-serviceable (FAILED).
    mocks.configManager.update(emptyList(), List.of(newDef), emptyList(), emptySet(), emptySet());

    // v2 was promoted to live by StagedIndexesSwapper (v1 is FAILED and non-serviceable).
    configStateMocks.assertLiveIndexesAre(stagedIndex);
    // No staged indexes remain after the swap.
    configStateMocks.assertStagedIndexesAre();
    // v1 was phased out by StagedIndexesSwapper and then dropped by PhasingOutIndexesDropper
    // (no active cursors), so phasing out is now empty.
    configStateMocks.assertPhasingOutIndexesAre();
  }

  @Test
  public void startLifecycle_readyCustomVectorEngine_enqueuesCatalogAndStaged() throws Exception {
    ConfigStateMocks mocks = ConfigStateMocks.create();
    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(true);
    var liveDef = customVectorEngineDefinitionGeneration();
    var stagedDef = liveDef.incrementUser(liveDef.getIndexDefinition());
    IndexGeneration live = IndexGenerationFactory.getIndexGeneration(mocks.indexFactory, liveDef);
    IndexGeneration staged =
        IndexGenerationFactory.getIndexGeneration(mocks.indexFactory, stagedDef);
    mocks.indexCatalog.addIndex(live);
    mocks.staged.addIndex(staged);
    mocks.clearInvocations();

    DefaultConfigManager.startLifecycle(mocks.configState);

    verify(mocks.lifecycleManager).add(live);
    verify(mocks.lifecycleManager).add(staged);
  }

  @Test
  public void startLifecycle_unreadyCustomVectorEngine_skipsCatalogAndStaged() throws Exception {
    ConfigStateMocks mocks = ConfigStateMocks.create();
    // Mockito boolean defaults leave isReadyToInitialize false.
    var liveDef = customVectorEngineDefinitionGeneration();
    var stagedDef = liveDef.incrementUser(liveDef.getIndexDefinition());
    IndexGeneration live = IndexGenerationFactory.getIndexGeneration(mocks.indexFactory, liveDef);
    IndexGeneration staged =
        IndexGenerationFactory.getIndexGeneration(mocks.indexFactory, stagedDef);
    mocks.indexCatalog.addIndex(live);
    mocks.staged.addIndex(staged);
    mocks.clearInvocations();

    DefaultConfigManager.startLifecycle(mocks.configState);

    verify(mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void promoteReadyIndexes_readyDeferredCatalogIndex_enqueuesOnce() throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    var definitionGeneration = customVectorEngineDefinitionGeneration();
    IndexActions.withReplication(mocks.mockedDependencies.configState)
        .addNewIndexes(List.of(definitionGeneration));
    verify(mocks.lifecycleManager, never()).add(any());

    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(true);
    mocks.clearInvocations();

    mocks.configManager.promoteReadyIndexes();

    verify(mocks.lifecycleManager, times(1)).add(any());

    mocks.clearInvocations();
    mocks.configManager.promoteReadyIndexes();
    verify(mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void promoteReadyIndexes_readyDeferredStagedIndex_enqueues() throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(true);
    var definitionGeneration = customVectorEngineDefinitionGeneration();
    IndexActions.withReplication(mocks.mockedDependencies.configState)
        .addNewIndexes(List.of(definitionGeneration));
    mocks.waitForIndexInitialization(definitionGeneration.getGenerationId());
    mocks.clearInvocations();

    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(false);
    var stagedDefinitionGeneration =
        definitionGeneration.incrementUser(definitionGeneration.getIndexDefinition());
    IndexActions.withReplication(mocks.mockedDependencies.configState)
        .addStagedIndex(stagedDefinitionGeneration);
    verify(mocks.lifecycleManager, never()).add(any());

    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(true);
    mocks.clearInvocations();
    mocks.configManager.promoteReadyIndexes();

    verify(mocks.lifecycleManager, times(1)).add(any());
  }

  @Test
  public void promoteReadyIndexes_unready_isNoOp() throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    IndexActions.withReplication(mocks.mockedDependencies.configState)
        .addNewIndexes(List.of(customVectorEngineDefinitionGeneration()));
    mocks.clearInvocations();

    mocks.configManager.promoteReadyIndexes();

    verify(mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void promoteReadyIndexes_alreadyInLifecycle_isNoOp() throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(true);
    var definitionGeneration = customVectorEngineDefinitionGeneration();
    IndexActions.withReplication(mocks.mockedDependencies.configState)
        .addNewIndexes(List.of(definitionGeneration));
    mocks.clearInvocations();

    mocks.configManager.promoteReadyIndexes();

    verify(mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void promoteReadyIndexes_nonzeroAttempt_neverPromotesEvenAfterRepeatedSweeps()
      throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    var definitionGeneration = customVectorEngineDefinitionGeneration().incrementAttempt();
    IndexActions.withReplication(mocks.mockedDependencies.configState)
        .addNewIndexes(List.of(definitionGeneration));
    verify(mocks.lifecycleManager, never()).add(any());

    // Even if a naive stub returns true, the factory under test for attempt>0 must refuse. Drive
    // the real readiness contract: unready forever for nonzero attempt.
    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(false);
    mocks.clearInvocations();

    mocks.configManager.promoteReadyIndexes();
    mocks.configManager.promoteReadyIndexes();
    mocks.configManager.promoteReadyIndexes();

    verify(mocks.lifecycleManager, never()).add(any());
    Assert.assertFalse(mocks.lifecycleManager.contains(definitionGeneration.getGenerationId()));
  }

  @Test
  public void promoteReadyIndexes_stagedNonzeroAttempt_neverPromotesAfterRepeatedSweeps()
      throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(true);
    var live = customVectorEngineDefinitionGeneration();
    IndexActions.withReplication(mocks.mockedDependencies.configState).addNewIndexes(List.of(live));
    mocks.waitForIndexInitialization(live.getGenerationId());
    mocks.clearInvocations();

    var staged = live.incrementUser(live.getIndexDefinition()).incrementAttempt();
    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(false);
    IndexActions.withReplication(mocks.mockedDependencies.configState).addStagedIndex(staged);
    verify(mocks.lifecycleManager, never()).add(any());
    mocks.clearInvocations();

    mocks.configManager.promoteReadyIndexes();
    mocks.configManager.promoteReadyIndexes();

    verify(mocks.lifecycleManager, never()).add(any());
    Assert.assertFalse(mocks.lifecycleManager.contains(staged.getGenerationId()));
  }

  @Test
  public void promoteReadyIndexes_readinessLossDoesNotUnenqueueAlreadyAddedIndex()
      throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(true);
    var definitionGeneration = customVectorEngineDefinitionGeneration();
    IndexActions.withReplication(mocks.mockedDependencies.configState)
        .addNewIndexes(List.of(definitionGeneration));
    Assert.assertTrue(mocks.lifecycleManager.contains(definitionGeneration.getGenerationId()));
    mocks.waitForIndexInitialization(definitionGeneration.getGenerationId());

    // If readiness later flips false, an already-enqueued index must stay in the lifecycle.
    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(false);
    mocks.clearInvocations();
    mocks.configManager.promoteReadyIndexes();

    verify(mocks.lifecycleManager, never()).add(any());
    verify(mocks.lifecycleManager, never()).dropIndex(any());
    Assert.assertTrue(mocks.lifecycleManager.contains(definitionGeneration.getGenerationId()));
  }

  @Test
  public void update_promotesReadyIndexesAsBackstop() throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    var definitionGeneration = customVectorEngineDefinitionGeneration();
    IndexActions.withReplication(mocks.mockedDependencies.configState)
        .addNewIndexes(List.of(definitionGeneration));
    verify(mocks.lifecycleManager, never()).add(any());

    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(true);
    mocks.clearInvocations();

    mocks.configManager.update(
        List.of((VectorIndexDefinition) definitionGeneration.getIndexDefinition()),
        emptyList(),
        emptyList(),
        emptySet(),
        emptySet());

    verify(mocks.lifecycleManager, times(1)).add(any());
  }

  @Test
  public void isReplicationInitialized_noLiveIndexes_returnsTrue() throws Exception {
    var mocks = DefaultConfigManagerMocks.create();

    assertThat(mocks.configManager.isReplicationInitialized()).isTrue();
  }

  @Test
  public void isReplicationInitialized_liveIndexWithUnknownStatus_returnsFalse() throws Exception {
    var mocks = DefaultConfigManagerMocks.create();
    var liveIndex = mockIndexGeneration();
    liveIndex.getIndex().setStatus(IndexStatus.unknown());
    doReturn(List.of(liveIndex)).when(mocks.indexCatalog).getIndexes();

    assertThat(mocks.configManager.isReplicationInitialized()).isFalse();
  }

  @Test
  public void isReplicationInitialized_onceAllLiveIndexesKnown_latchesTrue() throws Exception {
    var mocks = DefaultConfigManagerMocks.create();
    var liveIndex = mockIndexGeneration();
    liveIndex.getIndex().setStatus(IndexStatus.steady());
    doReturn(List.of(liveIndex)).when(mocks.indexCatalog).getIndexes();
    assertThat(mocks.configManager.isReplicationInitialized()).isTrue();

    // Reverting a live index to an unknown status does not un-latch replication readiness.
    liveIndex.getIndex().setStatus(IndexStatus.unknown());

    assertThat(mocks.configManager.isReplicationInitialized()).isTrue();
  }

  private static VectorIndexDefinitionGeneration customVectorEngineDefinitionGeneration() {
    return mockDefinitionGeneration(
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("custom_vector_engine_index")
            .database("mock_database")
            .lastObservedCollectionName("mock_collection")
            .collectionUuid(UUID.randomUUID())
            .setFields(
                List.<VectorIndexFieldDefinition>of(
                    new VectorDataFieldDefinition(
                        FieldPath.parse("vector.path"),
                        new VectorFieldSpecification(
                            3,
                            VectorSimilarity.COSINE,
                            VectorQuantization.NONE,
                            new VectorIndexingAlgorithm.HnswIndexingAlgorithm(),
                            VectorSearchEngine.CUSTOM))))
            .build());
  }
}
