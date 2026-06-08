package com.xgen.mongot.config.manager;

import static com.google.common.truth.Truth.assertThat;
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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.config.backup.ConfigJournalV1;
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
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.util.mongodb.ConnectionStringUtil;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.testing.mongot.config.backup.ConfigJournalV1Builder;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.config.manager.DefaultConfigManagerMocks;
import com.xgen.testing.mongot.config.manager.metrics.IndexGenerationStateMetricsBuilder;
import com.xgen.testing.mongot.index.IndexGenerationMetricsBuilder;
import com.xgen.testing.mongot.index.IndexMetricsBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.metrics.micrometer.PercentilesBuilder;
import com.xgen.testing.mongot.metrics.micrometer.SerializableTimerBuilder;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import com.xgen.testing.mongot.mock.index.query.Query;
import io.micrometer.core.instrument.Tags;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class DefaultConfigManagerTest {
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
    mocks.configManager.update(emptyList(), emptyList(), emptyList(), emptySet());

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

    mocks.configManager.update(emptyList(), indexDefinitions, emptyList(), emptySet());

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

    mocks.configManager.update(indexDefinitions, emptyList(), emptyList(), emptySet());

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
    mocks.configManager.update(emptyList(), indexDefinitions, emptyList(), emptySet());

    // Get the Index so we can verify it was dropped later.
    Optional<IndexGeneration> optionalIndex = mocks.indexCatalog.getIndexById(MOCK_INDEX_ID);
    Assert.assertTrue(optionalIndex.isPresent());
    Index index = optionalIndex.get().getIndex();
    mocks.waitForIndexInitialization(optionalIndex.get().getGenerationId());
    InitializedIndex initializedIndex = mocks.getInitializedIndex(optionalIndex.get());

    mocks.clearInvocations();

    // Now update the config manager again to remove the index.
    mocks.configManager.update(emptyList(), emptyList(), emptyList(), emptySet());

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
    mocks.configManager.update(indexDefinitions, emptyList(), emptyList(), emptySet());

    // Get the Index so we can verify it was dropped later.
    Optional<IndexGeneration> optionalIndex =
        mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID);
    Assert.assertTrue(optionalIndex.isPresent());
    Index index = optionalIndex.get().getIndex();
    mocks.waitForIndexInitialization(optionalIndex.get().getGenerationId());
    InitializedIndex initializedIndex = mocks.getInitializedIndex(optionalIndex.get());
    mocks.clearInvocations();

    // Now update the config manager again to remove the index.
    mocks.configManager.update(emptyList(), emptyList(), emptyList(), emptySet());

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
        emptyList(), List.of(MOCK_INDEX_DEFINITION), emptyList(), emptySet());
    configStateMocks.assertIndexCatalogSize(1);

    // important for this test, otherwise the index will be created in steady state and be swapped
    // in the same cycle.
    configStateMocks.setStatusForCreatedIndexes(IndexStatus.initialSync());

    // index definition changed, we expect a swap to be staged:
    mocks.configManager.update(emptyList(), List.of(newDef), emptyList(), emptySet());
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
    mocks.configManager.update(emptyList(), List.of(newDef), emptyList(), emptySet());

    // indexes should have been swapped at this point
    configStateMocks.assertStagedIndexesAre();
    configStateMocks.assertLiveIndexesAre(newIndex);
    configStateMocks.assertPhasingOutIndexesAre(oldIndex);

    mocks.cursorManager.killCursor(cursorId);
    // in this config cycle we have no cursors on the old index, so it is ready to be dropped.
    mocks.configManager.update(emptyList(), List.of(newDef), emptyList(), emptySet());
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
        emptyList(), List.of(MOCK_INDEX_DEFINITION), emptyList(), emptySet());
    configStateMocks.assertIndexCatalogSize(1);

    // important for this test, otherwise the index will be created in steady state and be swapped
    // in the same cycle.
    configStateMocks.setStatusForCreatedIndexes(IndexStatus.initialSync());

    // index definition changed, we expect a swap to be staged:
    mocks.configManager.update(emptyList(), List.of(def1), emptyList(), emptySet());

    mocks.waitForIndexInitialization(Generation.CURRENT.generationId(MOCK_INDEX_ID));
    var oldIndex = configStateMocks.indexCatalog.getIndexById(MOCK_INDEX_ID).orElseThrow();
    InitializedIndex initializedIndex = mocks.getInitializedIndex(oldIndex);
    var newIndexDef1 = configStateMocks.staged.getIndex(MOCK_INDEX_ID).orElseThrow();

    mocks.configManager.update(emptyList(), List.of(def2), emptyList(), emptySet());

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
        emptyList(), List.of(MOCK_INDEX_DEFINITION), emptyList(), emptySet());
    var oldIndex =
        mocks.indexCatalog.getIndexById(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow();
    oldIndex.getIndex().setStatus(IndexStatus.steady());
    configStateMocks.setStatusForCreatedIndexes(IndexStatus.initialSync());
    // stage a swap
    var def1 =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .analyzerName("lucene.keyword")
            .build();
    mocks.configManager.update(emptyList(), List.of(def1), emptyList(), emptySet());
    // "regret" and revert to the first definition:
    mocks.configManager.update(
        emptyList(), List.of(MOCK_INDEX_DEFINITION), emptyList(), emptySet());
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
    mocks.configManager.update(emptyList(), List.of(liveDefinition), emptyList(), emptySet());
    var liveIndex =
        mocks.indexCatalog.getIndexById(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow();
    liveIndex.getIndex().setStatus(IndexStatus.initialSync());

    // stage a swap with the updated defintion
    mocks.configManager.update(emptyList(), List.of(stagedDefinition), emptyList(), emptySet());

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
    mocks.configManager.update(emptyList(), List.of(liveDefinition), emptyList(), emptySet());
    var liveIndex =
        mocks.indexCatalog.getIndexById(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow();
    liveIndex.getIndex().setStatus(IndexStatus.recoveringNonTransient(new BsonTimestamp()));

    // Run another update cycle with the same definitions
    configStateMocks.clearInvocations();
    mocks.configManager.update(emptyList(), List.of(liveDefinition), emptyList(), emptySet());

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
    mocks.configManager.update(emptyList(), List.of(liveDefinition), emptyList(), emptySet());
    var liveIndex =
        mocks.indexCatalog.getIndexById(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow();
    liveIndex.getIndex().setStatus(IndexStatus.steady());

    // Stage a swap with updated definition
    mocks.configManager.update(emptyList(), List.of(stagedDefinition), emptyList(), emptySet());

    // Check that the live index still exists and a new one was staged with the same definition
    configStateMocks.assertIndexCatalogSize(1);
    configStateMocks.assertAtLeastOneIndexStagedAndReplicated();
    var stagedIndex =
        configStateMocks.staged.getIndex(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow();

    // Fail the original index and run another update cycle
    configStateMocks.clearInvocations();
    liveIndex.getIndex().setStatus(IndexStatus.recoveringNonTransient(new BsonTimestamp()));
    mocks.configManager.update(emptyList(), List.of(stagedDefinition), emptyList(), emptySet());

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
                new AggregatedIndexMetrics(0, 0, new BsonTimestamp(0), 0),
                Optional.of(
                    new IndexDetailedStatus.Search(
                        MOCK_SYNONYM_DETAILED_STATUS,
                        MOCK_INDEX_DEFINITION,
                        MOCK_INDEX_STATUS,
                        MOCK_INDEX_DEFINITION_GENERATION_CURRENT.getGenerationId(),
                        Optional.of(new AggregatedIndexMetrics(0, 0, new BsonTimestamp(0), 0)))),
                Optional.empty(),
                MOCK_SYNONYM_STATUS),
            new IndexInformation.Vector(
                MOCK_VECTOR_DEFINITION,
                IndexStatus.steady(),
                List.of(),
                new AggregatedIndexMetrics(0, 0, new BsonTimestamp(0), 0),
                Optional.of(
                    new IndexDetailedStatus.Vector(
                        MOCK_VECTOR_DEFINITION,
                        IndexStatus.steady(),
                        MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT.getGenerationId(),
                        Optional.of(new AggregatedIndexMetrics(0, 0, new BsonTimestamp(0), 0)))),
                Optional.empty()));

    var actual = new ArrayList<>(mocks.configManager.getIndexInfos());
    actual.sort(Comparator.comparing(info -> info.getDefinition().getType()));
    assertThat(actual).containsExactlyElementsIn(expected);
  }

  @Test
  public void testGetLiveIndexes() throws Exception {
    DefaultConfigManagerMocks mocks = DefaultConfigManagerMocks.create();
    assertEquals(emptyList(), mocks.configManager.getLiveIndexes());

    mocks.configManager.update(
        singletonList(MOCK_VECTOR_DEFINITION),
        singletonList(MOCK_INDEX_DEFINITION),
        emptyList(),
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
        emptyList(), singletonList(MOCK_INDEX_DEFINITION), emptyList(), emptySet());
    var liveIndex =
        mocks.indexCatalog.getIndexById(MOCK_INDEX_DEFINITION.getIndexId()).orElseThrow();
    liveIndex.getIndex().setStatus(IndexStatus.steady());

    // Stage a swap with updated definition
    mocks.configManager.update(emptyList(), List.of(stagedDefinition), emptyList(), emptySet());

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
    mocks.configManager.update(emptyList(), emptyList(), emptyList(), emptySet());
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
    var replicationStateMonitor =
        ReplicationStateMonitor.builder()
            .setReplicationGate(replicationGate)
            .setInitialSyncGate(ToggleGate.opened())
            .build();
    var mocks = DefaultConfigManagerMocks.create(replicationStateMonitor);
    mocks.clearInvocations();

    // Simulate disk pressure crossing the pause threshold.
    replicationGate.close();

    // Same sync source with valid URIs — disk pressure changed, restart expected.
    mocks.configManager.handleReplicationAndSyncSourceUpdate(
        ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);

    verify(mocks.lifecycleManager).updateSyncSource(ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);
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
        FeatureFlags.withDefaults()
            .enable(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)
            .build();
    var mocks = DefaultConfigManagerMocks.create(featureFlags);
    var configStateMocks = mocks.mockedDependencies;

    // Create v1 and let it reach steady state.
    mocks.configManager.update(
        emptyList(), List.of(MOCK_INDEX_DEFINITION), emptyList(), emptySet());
    var liveIndex =
        mocks.indexCatalog.getIndexById(MOCK_INDEX_ID).orElseThrow();
    liveIndex.getIndex().setStatus(IndexStatus.steady());

    // Future indexes created in initial sync (so v2 won't be immediately swapped in the
    // same cycle it is staged).
    configStateMocks.setStatusForCreatedIndexes(IndexStatus.initialSync());

    // Change the definition to stage v2.
    var newDef =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .analyzerName("lucene.keyword")
            .build();
    mocks.configManager.update(emptyList(), List.of(newDef), emptyList(), emptySet());

    var stagedIndex = configStateMocks.staged.getIndex(MOCK_INDEX_ID).orElseThrow();

    // Simulate v1 failing with a retryable initial sync failure in the background.
    liveIndex.getIndex().setStatus(
        IndexStatus.failed(
            "initial sync failed",
            IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY));

    configStateMocks.clearInvocations();

    // Run another update cycle. IndexInitialSyncRecovery should skip v1 because v2 is staged.
    // Then StagedIndexesSwapper swaps v2 into live because v1 is non-serviceable (FAILED).
    mocks.configManager.update(emptyList(), List.of(newDef), emptyList(), emptySet());

    // v2 was promoted to live by StagedIndexesSwapper (v1 is FAILED and non-serviceable).
    configStateMocks.assertLiveIndexesAre(stagedIndex);
    // No staged indexes remain after the swap.
    configStateMocks.assertStagedIndexesAre();
    // v1 was phased out by StagedIndexesSwapper and then dropped by PhasingOutIndexesDropper
    // (no active cursors), so phasing out is now empty.
    configStateMocks.assertPhasingOutIndexesAre();
  }
}
