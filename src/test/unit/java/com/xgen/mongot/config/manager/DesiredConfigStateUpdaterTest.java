package com.xgen.mongot.config.manager;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockVectorDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION_CURRENT;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_WITH_ANALYZER;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SEARCH_ANALYZER_NAME;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_VECTOR_DEFINITION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT;
import static org.junit.Assume.assumeFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalEntryIdentity;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.IndexDefinitionGenerationProducer;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.testing.mongot.config.backup.ConfigJournalV1Builder;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.CustomAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class DesiredConfigStateUpdaterTest {
  private static final ObjectId QUARANTINED_INDEX_ID = new ObjectId("012345678901234567890123");

  private static final SearchIndexDefinition MOCK_INDEX_DEFINITION_CHANGED =
      SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).analyzerName("lucene.cjk").build();
  private static final VectorIndexDefinition MOCK_VECTOR_DEFINITION_CHANGED =
      VectorIndexDefinitionBuilder.from(MOCK_VECTOR_DEFINITION).withFilterPath("new.path").build();

  @Test
  public void testEmptyUpdatedToEmptyDoesNothing() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    swapUpdate(mocks, Collections.emptyList(), Collections.emptyList());

    mocks.assertNoIndexActivity();
    mocks.assertPersistedJournalEmpty();
  }

  @Test
  public void testEmptyUpdatedToSearchIndexAddsIndexAndPersists() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    swapUpdate(mocks, Collections.emptyList(), List.of(SearchIndex.MOCK_INDEX_DEFINITION));

    // created and added an index:
    mocks.assertOneIndexCreated();
    mocks.assertOneIndexCatalogedAndReplicated();

    // journal after update() reflects the added index
    var expectedJournal =
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_INDEX_DEFINITION_GENERATION_CURRENT)
            .build();
    mocks.assertPersistedJournalEquals(expectedJournal);
    mocks.assertIndexCatalogSize(1);

    // Let's make sure that we wrote ahead our desire to create the index before creating
    // it.
    InOrder inOrder = Mockito.inOrder(mocks.indexFactory, mocks.journalWriter);
    inOrder.verify(mocks.journalWriter).persist(argThat(expectedJournal::equals));
    inOrder.verify(mocks.indexFactory).getIndex(any());
  }

  @Test
  public void testEmptyUpdatedToVectorIndexAddsIndexAndPersists() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION), List.of());

    // created and added an index:
    mocks.assertOneIndexCreated();
    mocks.assertOneIndexCatalogedAndReplicated();

    // journal after update() reflects the added index
    var expectedJournal =
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT)
            .build();
    mocks.assertPersistedJournalEquals(expectedJournal);
    mocks.assertIndexCatalogSize(1);

    // Let's make sure that we wrote ahead our desire to create the index before creating
    // it.
    InOrder inOrder = Mockito.inOrder(mocks.indexFactory, mocks.journalWriter);
    inOrder.verify(mocks.journalWriter).persist(argThat(expectedJournal::equals));
    inOrder.verify(mocks.indexFactory).getIndex(any());
  }

  @Test
  public void testSearchUpdateNoChangeDoesNothing() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();

    swapUpdate(mocks, Collections.emptyList(), List.of(SearchIndex.MOCK_INDEX_DEFINITION));
    mocks.clearInvocations();
    mocks.assertIndexCatalogSize(1);

    // update with the same desired index, shouldn't do anything
    swapUpdate(mocks, Collections.emptyList(), List.of(SearchIndex.MOCK_INDEX_DEFINITION));
    mocks.assertNoIndexActivity();
  }

  @Test
  public void testVectorUpdateNoChangeDoesNothing() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();

    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION), List.of());
    mocks.clearInvocations();
    mocks.assertIndexCatalogSize(1);

    // update with the same desired index, shouldn't do anything
    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION), List.of());
    mocks.assertNoIndexActivity();
  }

  @Test
  public void testUpdateSearchIndexRemoved() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();

    swapUpdate(mocks, Collections.emptyList(), List.of(SearchIndex.MOCK_INDEX_DEFINITION));
    mocks.clearInvocations();
    IndexGeneration generation = mocks.indexCatalog.getIndexById(MOCK_INDEX_ID).orElseThrow();
    var index = generation.getIndex();
    InitializedIndex initializedIndex =
        mocks.waitAndGetInitializedIndex(generation.getGenerationId());
    // Should drop the index now its not desired.
    swapUpdate(mocks, Collections.emptyList(), Collections.emptyList());
    mocks.assertIndexCatalogSize(0);

    // It's very important that the indexed is dropped in the following specific order (see comment
    // in DesiredConfigStateUpdater::dropIndex), so we test for it explicitly.
    // We also assert that we write ahead our desire to have this index dropped.
    InOrder inOrder =
        Mockito.inOrder(
            mocks.journalWriter,
            mocks.indexCatalog,
            mocks.cursorManager,
            mocks.lifecycleManager,
            index,
            initializedIndex);
    inOrder
        .verify(mocks.journalWriter, atLeastOnce())
        .persist(
            argThat(
                actual ->
                    ConfigJournalV1Builder.builder()
                        .deletedIndex(MOCK_INDEX_DEFINITION_GENERATION_CURRENT)
                        .build()
                        .equals(actual)));
    inOrder.verify(mocks.indexCatalog).removeIndex(MOCK_INDEX_ID);
    inOrder.verify(mocks.lifecycleManager).dropIndex(MOCK_INDEX_GENERATION_ID);
    inOrder.verify(initializedIndex).close();
    inOrder.verify(index, atLeastOnce()).drop();
    // after we dropped the index, we assert that the desire to drop the index is no longer
    // recorded in the journal
    inOrder
        .verify(mocks.journalWriter, atLeastOnce())
        .persist(argThat(actual -> emptyConfigJournal().equals(actual)));
  }

  @Test
  public void testUpdateVectorIndexRemoved() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();

    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION), List.of());
    mocks.clearInvocations();
    var generation = mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    var index = generation.getIndex();
    InitializedIndex initializedIndex =
        mocks.waitAndGetInitializedIndex(generation.getGenerationId());
    // Should drop the index now its not desired.
    swapUpdate(mocks, Collections.emptyList(), Collections.emptyList());
    mocks.assertIndexCatalogSize(0);

    // It's very important that the indexed is dropped in the following specific order (see comment
    // in DesiredConfigStateUpdater::dropIndex), so we test for it explicitly.
    // We also assert that we write ahead our desire to have this index dropped.
    InOrder inOrder =
        Mockito.inOrder(
            mocks.journalWriter,
            mocks.indexCatalog,
            mocks.cursorManager,
            mocks.lifecycleManager,
            index,
            initializedIndex);
    inOrder
        .verify(mocks.journalWriter, atLeastOnce())
        .persist(
            argThat(
                actual ->
                    ConfigJournalV1Builder.builder()
                        .deletedIndex(MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT)
                        .build()
                        .equals(actual)));
    inOrder.verify(mocks.indexCatalog).removeIndex(VectorIndex.MOCK_INDEX_ID);
    inOrder.verify(mocks.lifecycleManager).dropIndex(VectorIndex.MOCK_INDEX_GENERATION_ID);
    inOrder.verify(initializedIndex).close();
    inOrder.verify(index, atLeastOnce()).drop();
    // after we dropped the index, we assert that the desire to drop the index is no longer
    // recorded in the journal
    inOrder
        .verify(mocks.journalWriter, atLeastOnce())
        .persist(argThat(actual -> emptyConfigJournal().equals(actual)));
  }

  @Test
  public void testSearchIndexModifiedStagesSwap() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    swapUpdate(mocks, Collections.emptyList(), List.of(SearchIndex.MOCK_INDEX_DEFINITION));
    var oldIndex = mocks.indexCatalog.getIndexById(SearchIndex.MOCK_INDEX_ID).orElseThrow();

    mocks.clearInvocations();
    swapUpdate(mocks, Collections.emptyList(), List.of(MOCK_INDEX_DEFINITION_CHANGED));

    mocks.assertOneIndexCreated();
    mocks.assertAtLeastOneIndexStagedAndReplicated();
    mocks.assertLiveIndexesAre(oldIndex);

    var newIndex = mocks.staged.getIndex(SearchIndex.MOCK_INDEX_ID).orElseThrow();
    mocks.assertStagedIndexesAre(newIndex);

    Assert.assertNotSame(oldIndex, newIndex);
    Assert.assertEquals(MOCK_INDEX_DEFINITION, oldIndex.getDefinition());
    Assert.assertEquals(MOCK_INDEX_DEFINITION_CHANGED, newIndex.getDefinition());
    // lets verify that we journaled the addition prior to creating the index:
    InOrder inOrder = Mockito.inOrder(mocks.journalWriter, mocks.indexFactory, mocks.staged);
    inOrder
        .verify(mocks.journalWriter)
        .persist(
            argThat(
                actual ->
                    actual.getStagedIndexes().size() == 1
                        && actual.getStagedIndexes().get(0).getIndexDefinition()
                            == MOCK_INDEX_DEFINITION_CHANGED));
    inOrder.verify(mocks.indexFactory).getIndex(any());
    inOrder.verify(mocks.staged).addIndex(newIndex);
  }

  @Test
  public void testVectorIndexModifiedStagesSwap() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION), List.of());
    var oldIndex = mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();

    mocks.clearInvocations();
    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION_CHANGED), List.of());

    mocks.assertOneIndexCreated();
    mocks.assertAtLeastOneIndexStagedAndReplicated();
    mocks.assertLiveIndexesAre(oldIndex);

    var newIndex = mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    mocks.assertStagedIndexesAre(newIndex);

    Assert.assertNotSame(oldIndex, newIndex);
    Assert.assertEquals(MOCK_VECTOR_DEFINITION, oldIndex.getDefinition());
    Assert.assertEquals(MOCK_VECTOR_DEFINITION_CHANGED, newIndex.getDefinition());
    // lets verify that we journaled the addition prior to creating the index:
    InOrder inOrder = Mockito.inOrder(mocks.journalWriter, mocks.indexFactory, mocks.staged);
    inOrder
        .verify(mocks.journalWriter)
        .persist(
            argThat(
                actual ->
                    actual.getStagedIndexes().size() == 1
                        && actual.getStagedIndexes().get(0).getIndexDefinition()
                            == MOCK_VECTOR_DEFINITION_CHANGED));
    inOrder.verify(mocks.indexFactory).getIndex(any());
    inOrder.verify(mocks.staged).addIndex(newIndex);
  }

  @Test
  public void testSearchIndexModifiedDifferentThanCurrentSwapSameAsCatalogRevertsSwap()
      throws Exception {
    // user wants definition X then definition Y then X again, when we go back to
    // X we simply undo the current running swap (that is, dropping Y).
    ConfigStateMocks mocks = getEmptyMocks();
    swapUpdate(mocks, Collections.emptyList(), List.of(SearchIndex.MOCK_INDEX_DEFINITION));
    var oldIndexGeneration = mocks.indexCatalog.getIndexById(MOCK_INDEX_ID).orElseThrow();
    InitializedIndex oldInitializedIndex =
        mocks.waitAndGetInitializedIndex(oldIndexGeneration.getGenerationId());

    swapUpdate(mocks, Collections.emptyList(), List.of(MOCK_INDEX_DEFINITION_CHANGED));
    var stagedIndexGeneration = mocks.staged.getIndex(MOCK_INDEX_ID).orElseThrow();
    var stagedIndex = stagedIndexGeneration.getIndex();
    InitializedIndex stagedInitializedIndex =
        mocks.waitAndGetInitializedIndex(stagedIndexGeneration.getGenerationId());

    // make sure our setup is correct:
    mocks.assertLiveIndexesAre(oldIndexGeneration);
    mocks.assertStagedIndexesAre(stagedIndexGeneration);
    mocks.clearInvocations();

    // at this point we have MOCK_INDEX_DEFINITION in the catalog and MOCK_INDEX_DEFINITION_CHANGED
    // as staged. reverting back to MOCK_INDEX_DEFINITION.
    swapUpdate(mocks, Collections.emptyList(), List.of(MOCK_INDEX_DEFINITION));
    mocks.assertStagedIndexesAre(); // no running swaps.

    mocks.assertLiveIndexesAre(oldIndexGeneration);
    verify(oldInitializedIndex, times(0)).close();

    // our final journal should only record the original index.
    mocks.assertPersistedJournalEquals(
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_INDEX_DEFINITION_GENERATION_CURRENT)
            .build());

    // let's assert that we journaled our intent to drop the swapped index before we did. And that
    // we dropped it in the correct order.
    InOrder inOrder =
        Mockito.inOrder(
            mocks.journalWriter,
            mocks.lifecycleManager,
            mocks.staged,
            stagedIndex,
            stagedInitializedIndex);
    inOrder
        .verify(mocks.journalWriter)
        .persist(
            argThat(
                actual ->
                    actual.getDeletedIndexes().size() == 1
                        && actual.getDeletedIndexes().get(0).getIndexDefinition()
                            == MOCK_INDEX_DEFINITION_CHANGED));

    inOrder.verify(mocks.staged).removeIndex(stagedIndexGeneration);
    inOrder.verify(mocks.lifecycleManager).dropIndex(any());
    inOrder.verify(stagedInitializedIndex).close();
    inOrder.verify(stagedIndexGeneration.getIndex()).drop();
  }

  @Test
  public void testVectorIndexModifiedDifferentThanCurrentSwapSameAsCatalogRevertsSwap()
      throws Exception {
    // user wants definition X then definition Y then X again, when we go back to
    // X we simply undo the current running swap (that is, dropping Y).
    ConfigStateMocks mocks = getEmptyMocks();
    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION), List.of());
    var oldIndexGeneration =
        mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    InitializedIndex oldInitializedIndex =
        mocks.waitAndGetInitializedIndex(oldIndexGeneration.getGenerationId());
    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION_CHANGED), List.of());
    var stagedIndexGeneration = mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    var stagedIndex = stagedIndexGeneration.getIndex();
    InitializedIndex stagedInitializedIndex =
        mocks.waitAndGetInitializedIndex(stagedIndexGeneration.getGenerationId());
    // make sure our setup is correct:
    mocks.assertLiveIndexesAre(oldIndexGeneration);
    mocks.assertStagedIndexesAre(stagedIndexGeneration);
    mocks.clearInvocations();

    // at this point we have MOCK_INDEX_DEFINITION in the catalog and MOCK_INDEX_DEFINITION_CHANGED
    // as staged. reverting back to MOCK_INDEX_DEFINITION.
    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION), List.of());
    mocks.assertStagedIndexesAre(); // no running swaps.

    mocks.assertLiveIndexesAre(oldIndexGeneration);
    verify(oldInitializedIndex, times(0)).close();

    // our final journal should only record the original index.
    mocks.assertPersistedJournalEquals(
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT)
            .build());

    // let's assert that we journaled our intent to drop the swapped index before we did. And that
    // we dropped it in the correct order.
    InOrder inOrder =
        Mockito.inOrder(
            mocks.journalWriter,
            mocks.lifecycleManager,
            mocks.staged,
            stagedIndex,
            stagedInitializedIndex);
    inOrder
        .verify(mocks.journalWriter)
        .persist(
            argThat(
                actual ->
                    actual.getDeletedIndexes().size() == 1
                        && actual.getDeletedIndexes().get(0).getIndexDefinition()
                            == MOCK_VECTOR_DEFINITION_CHANGED));

    inOrder.verify(mocks.staged).removeIndex(stagedIndexGeneration);
    inOrder.verify(mocks.lifecycleManager).dropIndex(any());
    inOrder.verify(stagedInitializedIndex).close();
    inOrder.verify(stagedIndexGeneration.getIndex()).drop();
  }

  @Test
  public void testSearchIndexModifiedDifferentThanCurrentSwapAndCatalogStagesNewSwap()
      throws Exception {
    // User wanted def1 then def2 then def3. When def3 comes we need to drop def2 and stage def3.
    // This corresponds to DIFFERENT_FROM_BOTH.
    var def1 = MOCK_INDEX_DEFINITION;
    var def2 = MOCK_INDEX_DEFINITION_CHANGED;
    var def3 =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .analyzerName("lucene.keyword")
            .build();

    ConfigStateMocks mocks = getEmptyMocks();
    swapUpdate(mocks, Collections.emptyList(), List.of(def1));
    var oldIndex = mocks.indexCatalog.getIndexById(MOCK_INDEX_ID).orElseThrow();
    InitializedIndex oldInitializedIndex =
        mocks.waitAndGetInitializedIndex(oldIndex.getGenerationId());
    swapUpdate(mocks, Collections.emptyList(), List.of(def2));
    var stagedIndexForDef2 = mocks.staged.getIndex(MOCK_INDEX_ID).orElseThrow();
    InitializedIndex staged2InitializedIndex =
        mocks.waitAndGetInitializedIndex(stagedIndexForDef2.getGenerationId());
    // make sure our setup is correct:
    mocks.assertLiveIndexesAre(oldIndex);
    mocks.assertStagedIndexesAre(stagedIndexForDef2);
    mocks.clearInvocations();

    // at this point we have def1 in the catalog and def2
    // as staged. changing to def3 should drop def2
    swapUpdate(mocks, Collections.emptyList(), List.of(def3));
    mocks.assertAtLeastOneIndexStagedAndReplicated();

    var stagedIndexForDef3 = mocks.staged.getIndex(MOCK_INDEX_ID).orElseThrow();
    Assert.assertNotSame(stagedIndexForDef2, stagedIndexForDef3);
    mocks.assertStagedIndexesAre(stagedIndexForDef3);

    mocks.assertLiveIndexesAre(oldIndex);
    verify(oldInitializedIndex, times(0)).close();

    // our final journal should only record the original index.
    mocks.assertPersistedJournalEquals(
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_INDEX_DEFINITION_GENERATION_CURRENT)
            .stagedIndex(
                SearchIndexDefinitionGenerationBuilder.create(
                    def3,
                    stagedIndexForDef3.getDefinitionGeneration().generation(),
                    Collections.emptyList()))
            .build());

    InOrder inOrder =
        Mockito.inOrder(
            mocks.journalWriter,
            mocks.lifecycleManager,
            mocks.indexFactory,
            mocks.staged,
            stagedIndexForDef2.getIndex(),
            stagedIndexForDef3.getIndex(),
            staged2InitializedIndex);
    // first we drop def2 in order
    inOrder
        .verify(mocks.journalWriter)
        .persist(
            argThat(
                actual ->
                    actual.getDeletedIndexes().size() == 1
                        && actual.getDeletedIndexes().getFirst().getIndexDefinition() == def2));
    inOrder.verify(mocks.staged).removeIndex(stagedIndexForDef2);
    inOrder.verify(staged2InitializedIndex).close();
    inOrder.verify(stagedIndexForDef2.getIndex()).drop();
    // now we add def3
    inOrder
        .verify(mocks.journalWriter)
        .persist(
            argThat(
                actual ->
                    actual.getStagedIndexes().size() == 1
                        && actual.getStagedIndexes().getFirst().getIndexDefinition() == def3));
    inOrder.verify(mocks.indexFactory).getIndex(any());
    inOrder.verify(mocks.staged).addIndex(stagedIndexForDef3);
    inOrder.verify(mocks.lifecycleManager).add(any());
  }

  @Test
  public void testVectorIndexModifiedDifferentThanCurrentSwapAndCatalogStagesNewSwap()
      throws Exception {
    // User wanted def1 then def2 then def3. When def3 comes we need to drop def2 and stage def3.
    // This corresponds to DIFFERENT_FROM_BOTH.
    var def1 = MOCK_VECTOR_DEFINITION;
    var def2 = MOCK_VECTOR_DEFINITION_CHANGED;
    var def3 =
        VectorIndexDefinitionBuilder.from(MOCK_VECTOR_DEFINITION)
            .withCosineVectorField("new-vector", 1024)
            .build();

    ConfigStateMocks mocks = getEmptyMocks();
    swapUpdate(mocks, List.of(def1), List.of());
    var oldIndex = mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    InitializedIndex oldInitializedIndex =
        mocks.waitAndGetInitializedIndex(oldIndex.getGenerationId());
    swapUpdate(mocks, List.of(def2), List.of());
    var stagedIndexForDef2 = mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    InitializedIndex staged2InitializedIndex =
        mocks.waitAndGetInitializedIndex(stagedIndexForDef2.getGenerationId());
    // make sure our setup is correct:
    mocks.assertLiveIndexesAre(oldIndex);
    mocks.assertStagedIndexesAre(stagedIndexForDef2);
    mocks.clearInvocations();

    // at this point we have def1 in the catalog and def2
    // as staged. changing to def3 should drop def2
    swapUpdate(mocks, List.of(def3), List.of());
    mocks.assertAtLeastOneIndexStagedAndReplicated();

    var stagedIndexForDef3 = mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    Assert.assertNotSame(stagedIndexForDef2, stagedIndexForDef3);
    mocks.assertStagedIndexesAre(stagedIndexForDef3);

    mocks.assertLiveIndexesAre(oldIndex);
    verify(oldInitializedIndex, times(0)).close();

    // our final journal should only record the original index.
    mocks.assertPersistedJournalEquals(
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT)
            .stagedIndex(
                new VectorIndexDefinitionGeneration(
                    def3, stagedIndexForDef3.getDefinitionGeneration().generation()))
            .build());

    InOrder inOrder =
        Mockito.inOrder(
            mocks.journalWriter,
            mocks.lifecycleManager,
            mocks.indexFactory,
            mocks.staged,
            stagedIndexForDef2.getIndex(),
            stagedIndexForDef3.getIndex(),
            staged2InitializedIndex);
    // first we drop def2 in order
    inOrder
        .verify(mocks.journalWriter)
        .persist(
            argThat(
                actual ->
                    actual.getDeletedIndexes().size() == 1
                        && actual.getDeletedIndexes().get(0).getIndexDefinition() == def2));
    inOrder.verify(mocks.staged).removeIndex(stagedIndexForDef2);
    inOrder.verify(staged2InitializedIndex).close();
    inOrder.verify(stagedIndexForDef2.getIndex()).drop();
    // now we add def3
    inOrder
        .verify(mocks.journalWriter)
        .persist(
            argThat(
                actual ->
                    actual.getStagedIndexes().size() == 1
                        && actual.getStagedIndexes().getFirst().getIndexDefinition() == def3));
    inOrder.verify(mocks.indexFactory).getIndex(any());
    inOrder.verify(mocks.staged).addIndex(stagedIndexForDef3);
    inOrder.verify(mocks.lifecycleManager).add(any());
  }

  @Test
  public void
      testSearchIndexModifiedDiffThanCurrentSwapSameAsCatalogWithOldFormatVersionStagesNewSwap()
          throws Exception {
    assumeFalse(IndexFormatVersion.MIN_SUPPORTED_VERSION.equals(IndexFormatVersion.CURRENT));

    // User wanted X then def Y, then back to def X but in between the index format version changed.
    // We should stage a new swap in that case.
    ConfigStateMocks mocks = getEmptyMocks();
    var oldDefinitionGeneration =
        new SearchIndexDefinitionGeneration(
            AnalyzerBoundSearchIndexDefinition.create(MOCK_INDEX_DEFINITION, List.of()),
            new Generation(UserIndexVersion.FIRST, IndexFormatVersion.MIN_SUPPORTED_VERSION));
    mocks.addIndex(oldDefinitionGeneration, ConfigStateMocks.State.LIVE);
    var oldIndexGeneration = mocks.indexCatalog.getIndexById(MOCK_INDEX_ID).orElseThrow();
    var oldIndex = oldIndexGeneration.getIndex();
    InitializedIndex oldInitializedIndex =
        mocks.waitAndGetInitializedIndex(oldIndexGeneration.getGenerationId());
    var stagedDefinitionGeneration =
        new SearchIndexDefinitionGeneration(
            AnalyzerBoundSearchIndexDefinition.create(MOCK_INDEX_DEFINITION_CHANGED, List.of()),
            new Generation(UserIndexVersion.FIRST, IndexFormatVersion.CURRENT));
    mocks.addIndex(stagedDefinitionGeneration, ConfigStateMocks.State.STAGED);
    var stagedIndexGeneration = mocks.staged.getIndex(MOCK_INDEX_ID).orElseThrow();
    var stagedIndex = stagedIndexGeneration.getIndex();
    InitializedIndex stagedInitializedIndex =
        mocks.waitAndGetInitializedIndex(stagedIndexGeneration.getGenerationId());
    // make sure our setup is correct:
    mocks.assertLiveIndexesAre(oldIndexGeneration);
    mocks.assertStagedIndexesAre(stagedIndexGeneration);
    mocks.clearInvocations();

    // at this point we have MOCK_INDEX_DEFINITION in the catalog and MOCK_INDEX_DEFINITION_CHANGED
    // as staged. reverting back to MOCK_INDEX_DEFINITION.
    swapUpdate(mocks, Collections.emptyList(), List.of(MOCK_INDEX_DEFINITION));
    mocks.assertAtLeastOneIndexStagedAndReplicated();

    var stagedUpgradedIndexForMockIndexDef = mocks.staged.getIndex(MOCK_INDEX_ID).orElseThrow();
    Assert.assertNotSame(stagedIndexGeneration, stagedUpgradedIndexForMockIndexDef);
    mocks.assertStagedIndexesAre(stagedUpgradedIndexForMockIndexDef);

    mocks.assertLiveIndexesAre(oldIndexGeneration);
    verify(oldInitializedIndex, never()).close();

    // Our final journal should record the outdated original index and the new staged index with the
    // same definition. The new staged index will have UserIndexVersion of 1, since FIRST would be
    // the version staged by IndexFormatVersionUpgrader.
    mocks.assertPersistedJournalEquals(
        ConfigJournalV1Builder.builder()
            .liveIndex(oldDefinitionGeneration)
            .stagedIndex(
                new SearchIndexDefinitionGeneration(
                    AnalyzerBoundSearchIndexDefinition.create(MOCK_INDEX_DEFINITION, List.of()),
                    new Generation(new UserIndexVersion(1), IndexFormatVersion.CURRENT)))
            .build());

    InOrder inOrder =
        Mockito.inOrder(
            mocks.journalWriter,
            mocks.lifecycleManager,
            mocks.indexFactory,
            mocks.staged,
            oldIndex,
            stagedIndex,
            stagedInitializedIndex);
    // first we drop stagedIndex in order
    inOrder
        .verify(mocks.journalWriter)
        .persist(
            argThat(
                actual ->
                    actual.getDeletedIndexes().size() == 1
                        && actual.getDeletedIndexes().get(0).getIndexDefinition()
                            == MOCK_INDEX_DEFINITION_CHANGED));
    inOrder.verify(mocks.staged).removeIndex(stagedIndexGeneration);
    inOrder.verify(stagedInitializedIndex).close();
    inOrder.verify(stagedIndexGeneration.getIndex(), atLeastOnce()).drop();
    // now we add the upgraded MOCK_INDEX_DEFINITION
    inOrder
        .verify(mocks.journalWriter)
        .persist(
            argThat(
                actual ->
                    actual.getStagedIndexes().size() == 1
                        && actual.getStagedIndexes().get(0).getIndexDefinition()
                            == MOCK_INDEX_DEFINITION));
    inOrder.verify(mocks.indexFactory).getIndex(any());
    inOrder.verify(mocks.staged).addIndex(stagedUpgradedIndexForMockIndexDef);
    inOrder.verify(mocks.lifecycleManager).add(any());
  }

  @Test
  public void
      testVectorIndexModifiedDiffThanCurrentSwapSameAsCatalogWithOldFormatVersionStagesNewSwap()
          throws Exception {
    assumeFalse(IndexFormatVersion.MIN_SUPPORTED_VERSION.equals(IndexFormatVersion.CURRENT));

    // User wanted X then def Y, then back to def X but in between the index format version changed.
    // We should stage a new swap in that case.
    ConfigStateMocks mocks = getEmptyMocks();
    var oldDefinitionGeneration =
        new VectorIndexDefinitionGeneration(
            MOCK_VECTOR_DEFINITION,
            new Generation(UserIndexVersion.FIRST, IndexFormatVersion.MIN_SUPPORTED_VERSION));
    mocks.addIndex(oldDefinitionGeneration, ConfigStateMocks.State.LIVE);
    var oldIndexGeneration =
        mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    var oldIndex = oldIndexGeneration.getIndex();
    InitializedIndex oldInitializedIndex =
        mocks.waitAndGetInitializedIndex(oldIndexGeneration.getGenerationId());
    var stagedDefinitionGeneration =
        new VectorIndexDefinitionGeneration(
            MOCK_VECTOR_DEFINITION_CHANGED,
            new Generation(UserIndexVersion.FIRST, IndexFormatVersion.CURRENT));
    mocks.addIndex(stagedDefinitionGeneration, ConfigStateMocks.State.STAGED);
    var stagedIndexGeneration = mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    var stagedIndex = stagedIndexGeneration.getIndex();
    InitializedIndex stagedInitializedIndex =
        mocks.waitAndGetInitializedIndex(stagedIndexGeneration.getGenerationId());
    // make sure our setup is correct:
    mocks.assertLiveIndexesAre(oldIndexGeneration);
    mocks.assertStagedIndexesAre(stagedIndexGeneration);
    mocks.clearInvocations();

    // at this point we have MOCK_INDEX_DEFINITION in the catalog and MOCK_INDEX_DEFINITION_CHANGED
    // as staged. reverting back to MOCK_INDEX_DEFINITION.
    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION), List.of());
    mocks.assertAtLeastOneIndexStagedAndReplicated();

    var stagedUpgradedIndexForMockIndexDef =
        mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    Assert.assertNotSame(stagedIndexGeneration, stagedUpgradedIndexForMockIndexDef);
    mocks.assertStagedIndexesAre(stagedUpgradedIndexForMockIndexDef);

    mocks.assertLiveIndexesAre(oldIndexGeneration);
    verify(oldInitializedIndex, never()).close();

    // Our final journal should record the outdated original index and the new staged index with the
    // same definition. The new staged index will have UserIndexVersion of 1, since FIRST would be
    // the version staged by IndexFormatVersionUpgrader.
    mocks.assertPersistedJournalEquals(
        ConfigJournalV1Builder.builder()
            .liveIndex(oldDefinitionGeneration)
            .stagedIndex(
                new VectorIndexDefinitionGeneration(
                    MOCK_VECTOR_DEFINITION,
                    new Generation(new UserIndexVersion(1), IndexFormatVersion.CURRENT)))
            .build());

    InOrder inOrder =
        Mockito.inOrder(
            mocks.journalWriter,
            mocks.lifecycleManager,
            mocks.indexFactory,
            mocks.staged,
            oldIndex,
            stagedIndex,
            stagedInitializedIndex);
    // first we drop stagedIndex in order
    inOrder
        .verify(mocks.journalWriter)
        .persist(
            argThat(
                actual ->
                    actual.getDeletedIndexes().size() == 1
                        && actual.getDeletedIndexes().get(0).getIndexDefinition()
                            == MOCK_VECTOR_DEFINITION_CHANGED));
    inOrder.verify(mocks.staged).removeIndex(stagedIndexGeneration);
    inOrder.verify(stagedInitializedIndex).close();
    inOrder.verify(stagedIndexGeneration.getIndex(), atLeastOnce()).drop();
    // now we add the upgraded MOCK_INDEX_DEFINITION
    inOrder
        .verify(mocks.journalWriter)
        .persist(
            argThat(
                actual ->
                    actual.getStagedIndexes().size() == 1
                        && actual.getStagedIndexes().get(0).getIndexDefinition()
                            == MOCK_VECTOR_DEFINITION));
    inOrder.verify(mocks.indexFactory).getIndex(any());
    inOrder.verify(mocks.staged).addIndex(stagedUpgradedIndexForMockIndexDef);
    inOrder.verify(mocks.lifecycleManager).add(any());
  }

  /**
   * When we add a new index, we might already have an index with the same id that we are phasing
   * out, in this case, we need to make sure we don't assign the same generation id to the new one.
   */
  @Test
  public void testSearchIndexAddedChoosesCorrectGenerationInPresenceOfPhaseOut() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    swapUpdate(mocks, Collections.emptyList(), List.of(SearchIndex.MOCK_INDEX_DEFINITION));
    // This index got moved to phase out by a successful swap then a drop.
    var oldIndex = mocks.indexCatalog.removeIndex(MOCK_INDEX_ID).orElseThrow();
    mocks.phasingOut.addIndex(oldIndex);
    mocks.clearInvocations();

    // the same desired index is now added again
    swapUpdate(mocks, Collections.emptyList(), List.of(SearchIndex.MOCK_INDEX_DEFINITION));
    mocks.assertOneIndexCatalogedAndReplicated();
    var newIndex = mocks.indexCatalog.getIndexById(MOCK_INDEX_ID).orElseThrow();
    Assert.assertNotSame(newIndex, oldIndex);
    Assert.assertEquals(
        oldIndex.getDefinitionGeneration().generation().userIndexVersion.versionNumber + 1,
        newIndex.getDefinitionGeneration().generation().userIndexVersion.versionNumber);
  }

  @Test
  public void testVectorIndexAddedChoosesCorrectGenerationInPresenceOfPhaseOut() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION), List.of());
    // This index got moved to phase out by a successful swap then a drop.
    var oldIndex = mocks.indexCatalog.removeIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    mocks.phasingOut.addIndex(oldIndex);
    mocks.clearInvocations();

    // the same desired index is now added again
    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION), List.of());
    mocks.assertOneIndexCatalogedAndReplicated();
    var newIndex = mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    Assert.assertNotSame(newIndex, oldIndex);
    Assert.assertEquals(
        oldIndex.getDefinitionGeneration().generation().userIndexVersion.versionNumber + 1,
        newIndex.getDefinitionGeneration().generation().userIndexVersion.versionNumber);
  }

  @Test
  public void testOneAddedOneModifiedWritesCorrectJournal() throws Exception {
    // Tests that there is no cross-talk between adding and modifying indexes.
    ConfigStateMocks mocks = getEmptyMocks();
    var oldDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .name("foo")
            .indexId(new ObjectId())
            .dynamicMapping()
            .build();
    var oldDefinitionGeneration =
        SearchIndexDefinitionGenerationBuilder.create(
            oldDefinition, Generation.CURRENT, Collections.emptyList());
    var newDefinition =
        SearchIndexDefinitionBuilder.builder()
            .database(oldDefinition.getDatabase())
            .collectionUuid(oldDefinition.getCollectionUuid())
            .lastObservedCollectionName(oldDefinition.getLastObservedCollectionName())
            .indexId(oldDefinition.getIndexId())
            .name("foo")
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(false).build())
            .build();
    var newDefinitionGeneration =
        SearchIndexDefinitionGenerationBuilder.create(
            newDefinition, Generation.CURRENT.incrementUser(), Collections.emptyList());

    swapUpdate(mocks, Collections.emptyList(), List.of(oldDefinition));
    var oldIndex = mocks.indexCatalog.getIndexById(oldDefinition.getIndexId()).orElseThrow();
    mocks.clearInvocations();

    // modify and add an index
    swapUpdate(mocks, Collections.emptyList(), List.of(newDefinition, MOCK_INDEX_DEFINITION));
    mocks.assertIndexCatalogSize(2);

    var finalStateJournal =
        ConfigJournalV1Builder.builder()
            .liveIndex(oldDefinitionGeneration)
            .liveIndex(MOCK_INDEX_DEFINITION_GENERATION)
            .stagedIndex(newDefinitionGeneration)
            .build();
    mocks.assertPersistedJournalEquals(finalStateJournal);

    // the next three in-order assertions are for the 3 indexes (one being added, one
    // modified being added as staged) being journaled before being added/removed.
    // the assertions are built in a way that only tests for the specific definition generation, so
    // the order of operations in the updater does not matter.
    var newAddedOrder =
        Mockito.inOrder(mocks.journalWriter, mocks.indexCatalog, mocks.lifecycleManager);
    // New index journaled before being added
    newAddedOrder
        .verify(mocks.journalWriter, atLeastOnce())
        .persist(
            argThat(
                actual ->
                    actual.getLiveIndexes().stream()
                        .anyMatch(
                            def ->
                                def.getGenerationId()
                                    .equals(MOCK_INDEX_DEFINITION_GENERATION.getGenerationId()))));
    newAddedOrder.verify(mocks.indexCatalog).addIndex(any());
    newAddedOrder.verify(mocks.lifecycleManager).add(any());

    // old should not have been dropped
    verify(mocks.indexCatalog, never()).removeIndex(any());
    verify(mocks.lifecycleManager, never()).dropIndex(any());
    verify(oldIndex.getIndex(), never()).drop();

    var modifiedAddedOrder =
        Mockito.inOrder(mocks.journalWriter, mocks.indexCatalog, mocks.lifecycleManager);
    // adds the new index
    modifiedAddedOrder
        .verify(mocks.journalWriter, atLeastOnce())
        .persist(
            argThat(
                actual ->
                    actual.getStagedIndexes().stream()
                        .anyMatch(
                            def ->
                                def.getGenerationId()
                                    .equals(newDefinitionGeneration.getGenerationId()))));
    modifiedAddedOrder.verify(mocks.indexCatalog).addIndex(any());
    modifiedAddedOrder.verify(mocks.lifecycleManager).add(any());
  }

  @Test
  public void testOneVectorIndexAddedOneVectorIndexModifiedWritesCorrectJournal() throws Exception {
    // Tests that there is no cross-talk between adding and modifying indexes.
    ConfigStateMocks mocks = getEmptyMocks();
    var oldDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("foo")
            .withCosineVectorField("vec", 1000)
            .build();
    var oldDefinitionGeneration =
        new VectorIndexDefinitionGeneration(oldDefinition, Generation.CURRENT);
    var newDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(oldDefinition.getIndexId())
            .collectionUuid(oldDefinition.getCollectionUuid())
            .database(oldDefinition.getDatabase())
            .lastObservedCollectionName(oldDefinition.getLastObservedCollectionName())
            .name("foo")
            .withCosineVectorField("vec", 2000)
            .build();
    var newDefinitionGeneration =
        new VectorIndexDefinitionGeneration(newDefinition, Generation.CURRENT.incrementUser());

    swapUpdate(mocks, List.of(oldDefinition), List.of());
    var oldIndex = mocks.indexCatalog.getIndexById(oldDefinition.getIndexId()).orElseThrow();
    mocks.clearInvocations();

    // modify and add an index
    swapUpdate(mocks, List.of(newDefinition, MOCK_VECTOR_DEFINITION), List.of());
    mocks.assertIndexCatalogSize(2);

    var finalStateJournal =
        ConfigJournalV1Builder.builder()
            .liveIndex(oldDefinitionGeneration)
            .liveIndex(MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT)
            .stagedIndex(newDefinitionGeneration)
            .build();
    mocks.assertPersistedJournalEquals(finalStateJournal);

    // the next three in-order assertions are for the 3 indexes (one being added, one
    // modified being added as staged) being journaled before being added/removed.
    // the assertions are built in a way that only tests for the specific definition generation, so
    // the order of operations in the updater does not matter.
    var newAddedOrder =
        Mockito.inOrder(mocks.journalWriter, mocks.indexCatalog, mocks.lifecycleManager);
    // New index journaled before being added
    newAddedOrder
        .verify(mocks.journalWriter, atLeastOnce())
        .persist(
            argThat(
                actual ->
                    actual.getLiveIndexes().stream()
                        .anyMatch(
                            def ->
                                def.getGenerationId()
                                    .equals(
                                        MOCK_VECTOR_INDEX_DEFINITION_GENERATION_CURRENT
                                            .getGenerationId()))));
    newAddedOrder.verify(mocks.indexCatalog).addIndex(any());
    newAddedOrder.verify(mocks.lifecycleManager).add(any());

    // old should not have been dropped
    verify(mocks.indexCatalog, never()).removeIndex(any());
    verify(mocks.lifecycleManager, never()).dropIndex(any());
    verify(oldIndex.getIndex(), never()).drop();

    var modifiedAddedOrder =
        Mockito.inOrder(mocks.journalWriter, mocks.indexCatalog, mocks.lifecycleManager);
    // adds the new index
    modifiedAddedOrder
        .verify(mocks.journalWriter, atLeastOnce())
        .persist(
            argThat(
                actual ->
                    actual.getStagedIndexes().stream()
                        .anyMatch(
                            def ->
                                def.getGenerationId()
                                    .equals(newDefinitionGeneration.getGenerationId()))));
    modifiedAddedOrder.verify(mocks.indexCatalog).addIndex(any());
    modifiedAddedOrder.verify(mocks.lifecycleManager).add(any());
  }

  @Test
  public void testRemovedDropsStagedIndex() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    ObjectId id = new ObjectId();
    GenerationId generation1 = GenerationIdBuilder.create(id);
    GenerationId generation2 = GenerationIdBuilder.incrementUser(generation1);

    mocks.addIndex(mockDefinitionGeneration(generation1), ConfigStateMocks.State.LIVE);
    mocks.addIndex(mockDefinitionGeneration(generation2), ConfigStateMocks.State.STAGED);
    mocks.clearInvocations();

    swapUpdate(mocks, Collections.emptyList(), List.of());

    mocks.assertLiveIndexesAre();
    mocks.assertStagedIndexesAre();
    mocks.assertPersistedJournalEquals(ConfigJournalV1Builder.builder().build());
  }

  @Test
  public void testRemovedVectorIndexDropsStagedIndex() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    ObjectId id = new ObjectId();
    GenerationId generation1 = GenerationIdBuilder.create(id);
    GenerationId generation2 = GenerationIdBuilder.incrementUser(generation1);

    mocks.addIndex(mockVectorDefinitionGeneration(generation1), ConfigStateMocks.State.LIVE);
    mocks.addIndex(mockVectorDefinitionGeneration(generation2), ConfigStateMocks.State.STAGED);
    mocks.clearInvocations();

    swapUpdate(mocks, Collections.emptyList(), List.of());

    mocks.assertLiveIndexesAre();
    mocks.assertStagedIndexesAre();
    mocks.assertPersistedJournalEquals(ConfigJournalV1Builder.builder().build());
  }

  @Test
  public void testUpdateDoesNotAffectPhaseOutIndexes() throws Exception {
    var phaseOutIndexDef = MOCK_INDEX_DEFINITION_GENERATION;
    ConfigStateMocks mocks = ConfigStateMocks.create();
    mocks.addIndex(phaseOutIndexDef, ConfigStateMocks.State.PHASE_OUT);

    var expectedJournal = ConfigJournalV1Builder.builder().deletedIndex(phaseOutIndexDef).build();
    mocks.clearInvocations();

    // when nothing is changed:
    swapUpdate(mocks, Collections.emptyList(), List.of());
    mocks.assertNoIndexActivity();
    Assert.assertEquals(1, mocks.phasingOut.getIndexes().size());

    mocks.assertPersistedJournalEquals(expectedJournal);
  }

  @Test
  public void testVectorIndexUpdateDoesNotAffectPhaseOutIndexes() throws Exception {
    var phaseOutIndexDef = VectorIndex.MOCK_VECTOR_INDEX_DEFINITION_GENERATION;
    ConfigStateMocks mocks = ConfigStateMocks.create();
    mocks.addIndex(phaseOutIndexDef, ConfigStateMocks.State.PHASE_OUT);

    var expectedJournal = ConfigJournalV1Builder.builder().deletedIndex(phaseOutIndexDef).build();
    mocks.clearInvocations();

    // when nothing is changed:
    swapUpdate(mocks, Collections.emptyList(), List.of());
    mocks.assertNoIndexActivity();
    Assert.assertEquals(1, mocks.phasingOut.getIndexes().size());

    mocks.assertPersistedJournalEquals(expectedJournal);
  }

  @Test
  public void testInvariantsAreEnforced() throws Exception {
    var mocks = getEmptyMocks();

    swapUpdate(
        mocks,
        Collections.emptyList(),
        List.of(MOCK_INDEX_DEFINITION, MOCK_INDEX_DEFINITION_WITH_ANALYZER));
    // Should recognize two indexes with the same id, then not take any action
    mocks.assertNoIndexActivity();
    verifyNoMoreInteractions(mocks.journalWriter);
  }

  @Test
  public void testVectorIdUniquenessInvariantIsEnforced() throws Exception {
    var mocks = getEmptyMocks();
    swapUpdate(mocks, List.of(MOCK_VECTOR_DEFINITION, MOCK_VECTOR_DEFINITION_CHANGED), List.of());
    // Should recognize two indexes with the same id, then not take any action
    mocks.assertNoIndexActivity();
    verifyNoMoreInteractions(mocks.journalWriter);
  }

  @Test
  public void testUniqueVectorIdsAcrossSearchAndVectorIndexesIsEnforced() throws Exception {
    var mocks = getEmptyMocks();
    swapUpdate(
        mocks,
        List.of(
            VectorIndexDefinitionBuilder.builder()
                .indexId(MOCK_INDEX_DEFINITION.getIndexId())
                .withCosineVectorField("vector", 1024)
                .build()),
        List.of(MOCK_INDEX_DEFINITION));
    // Should recognize two indexes with the same id, then not take any action
    mocks.assertNoIndexActivity();
    verifyNoMoreInteractions(mocks.journalWriter);
  }

  @Test
  public void testUniqueNamespacesAndNameAcrossSearchAndVectorIndexes() throws Exception {
    var mocks = getEmptyMocks();
    swapUpdate(
        mocks,
        List.of(
            VectorIndexDefinitionBuilder.builder()
                .indexId(new ObjectId())
                .withCosineVectorField("vector", 1024)
                .database(MOCK_INDEX_DEFINITION.getDatabase())
                .collectionUuid(MOCK_INDEX_DEFINITION.getCollectionUuid())
                .name(MOCK_INDEX_DEFINITION.getName())
                .build()),
        List.of(MOCK_INDEX_DEFINITION));
    // Should recognize two indexes with the same id, then not take any action
    mocks.assertNoIndexActivity();
    verifyNoMoreInteractions(mocks.journalWriter);
  }

  @Test
  public void testImmutableIndexType() throws Exception {
    var mocks = getEmptyMocks();
    var indexId = MOCK_INDEX_DEFINITION.getIndexId();

    // introduce search index
    swapUpdate(mocks, List.of(), List.of(MOCK_INDEX_DEFINITION));
    mocks.indexCatalog.getIndexById(indexId).orElseThrow();
    mocks.clearInvocations();

    // try to introduce vector index with the same id

    swapUpdate(
        mocks,
        List.of(
            VectorIndexDefinitionBuilder.builder()
                .indexId(indexId)
                .withCosineVectorField("vector", 1024)
                .build()),
        List.of());

    mocks.assertNoIndexActivity();
    mocks.assertPersistedJournalEquals(
        ConfigJournalV1Builder.builder().liveIndex(MOCK_INDEX_DEFINITION_GENERATION).build());
  }

  @Test
  public void testReindexMetrics() throws Exception {
    var mocks = getEmptyMocks();
    var meterRegistry = new SimpleMeterRegistry();
    var metricsFactory = new MetricsFactory("test", meterRegistry);

    swapUpdate(mocks, List.of(), List.of(MOCK_INDEX_DEFINITION), metricsFactory);

    swapUpdate(mocks, List.of(), List.of(MOCK_INDEX_DEFINITION_CHANGED), metricsFactory);

    Assert.assertEquals(
        1, meterRegistry.get("test.reindex").tag("reason", "ANALYZER").counter().count(), 0);
  }

  @Test
  public void testReindexMetrics_analyzers() throws Exception {
    var mocks = getEmptyMocks();
    var meterRegistry = new SimpleMeterRegistry();
    var metricsFactory = new MetricsFactory("test", meterRegistry);

    var mockIndexWithStandardTokenizer =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .analyzers(
                List.of(
                    CustomAnalyzerDefinitionBuilder.builder(
                            "foo", TokenizerDefinitionBuilder.StandardTokenizer.builder().build())
                        .build()))
            .build();

    var mockIndexWithKeywordTokenizer =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .analyzers(
                List.of(
                    CustomAnalyzerDefinitionBuilder.builder(
                            "foo", TokenizerDefinitionBuilder.KeywordTokenizer.build())
                        .build()))
            .build();

    swapUpdate(mocks, List.of(), List.of(mockIndexWithStandardTokenizer), metricsFactory);
    swapUpdate(mocks, List.of(), List.of(mockIndexWithKeywordTokenizer), metricsFactory);

    Assert.assertEquals(
        1, meterRegistry.get("test.reindex").tag("reason", "ANALYZERS").counter().count(), 0);
  }

  @Test
  public void testReindexMetrics_duplicate() throws Exception {
    var mocks = getEmptyMocks();
    var meterRegistry = new SimpleMeterRegistry();
    var metricsFactory = new MetricsFactory("test", meterRegistry);

    var duplicateIndexDefinition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .indexId(new ObjectId())
            .name("dupe")
            .build();
    var duplicateIndexDefinitionChanged =
        SearchIndexDefinitionBuilder.from(duplicateIndexDefinition)
            .analyzerName("lucene.cjk")
            .build();

    swapUpdate(
        mocks, List.of(), List.of(MOCK_INDEX_DEFINITION, duplicateIndexDefinition), metricsFactory);

    swapUpdate(
        mocks,
        List.of(),
        List.of(MOCK_INDEX_DEFINITION_CHANGED, duplicateIndexDefinitionChanged),
        metricsFactory);

    // Multiple reindexings caused by same reason -> one metric emitted
    Assert.assertEquals(
        1, meterRegistry.get("test.reindex").tag("reason", "ANALYZER").counter().count(), 0);
  }

  @Test
  public void testReindexMetrics_multiple() throws Exception {
    var mocks = getEmptyMocks();
    var meterRegistry = new SimpleMeterRegistry();
    var metricsFactory = new MetricsFactory("test", meterRegistry);

    swapUpdate(
        mocks, List.of(MOCK_VECTOR_DEFINITION), List.of(MOCK_INDEX_DEFINITION), metricsFactory);

    swapUpdate(
        mocks,
        List.of(MOCK_VECTOR_DEFINITION_CHANGED),
        List.of(MOCK_INDEX_DEFINITION_CHANGED),
        metricsFactory);

    Assert.assertEquals(
        1, meterRegistry.get("test.reindex").tag("reason", "MULTIPLE").counter().count(), 0);
  }

  /**
   * A quarantined index that is desired in no form at all - the user deleted it - is dropped from
   * the journal and from disk.
   */
  @Test
  public void undesiredQuarantinedLiveIndex_isDroppedFromJournalAndDisk() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    QuarantinedIndex quarantined = quarantinedIndex(QUARANTINED_INDEX_ID, "quarantined");
    mocks.configState.incompatibleIndexes.addLiveIndex(quarantined);

    swapUpdate(mocks, List.of(), List.of(), Set.of());

    verify(mocks.indexFactory).deleteGenerationData(quarantined.generationId());
    Assert.assertTrue(mocks.configState.incompatibleIndexes.isEmpty());
    mocks.assertPersistedJournalEmpty();
  }

  @Test
  public void undesiredQuarantinedStagedIndex_isDroppedFromJournalAndDisk() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    QuarantinedIndex quarantined = quarantinedIndex(QUARANTINED_INDEX_ID, "quarantined");
    mocks.configState.incompatibleIndexes.addStagedIndex(quarantined);

    swapUpdate(mocks, List.of(), List.of(), Set.of());

    verify(mocks.indexFactory).deleteGenerationData(quarantined.generationId());
    Assert.assertTrue(mocks.configState.incompatibleIndexes.isEmpty());
    mocks.assertPersistedJournalEmpty();
  }

  /** An index can be quarantined live and staged at once, and then both generations go. */
  @Test
  public void undesiredQuarantinedIndex_dropsBothItsGenerations() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    QuarantinedIndex live = quarantinedIndex(QUARANTINED_INDEX_ID, "quarantined", 1);
    QuarantinedIndex staged = quarantinedIndex(QUARANTINED_INDEX_ID, "quarantined", 2);
    mocks.configState.incompatibleIndexes.addLiveIndex(live);
    mocks.configState.incompatibleIndexes.addStagedIndex(staged);

    swapUpdate(mocks, List.of(), List.of(), Set.of());

    verify(mocks.indexFactory).deleteGenerationData(live.generationId());
    verify(mocks.indexFactory).deleteGenerationData(staged.generationId());
    Assert.assertTrue(mocks.configState.incompatibleIndexes.isEmpty());
    mocks.assertPersistedJournalEmpty();
  }

  /**
   * The whole point of quarantine: an index this binary cannot read but the control plane still
   * wants keeps its data and its journal entry, verbatim.
   */
  @Test
  public void desiredButStillUnreadableQuarantinedIndex_isLeftAlone() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    QuarantinedIndex quarantined = quarantinedIndex(QUARANTINED_INDEX_ID, "quarantined");
    mocks.configState.incompatibleIndexes.addLiveIndex(quarantined);

    swapUpdate(mocks, List.of(), List.of(), Set.of(QUARANTINED_INDEX_ID));

    verify(mocks.indexFactory, never()).deleteGenerationData(any());
    Assert.assertEquals(
        List.of(quarantined), List.copyOf(mocks.configState.incompatibleIndexes.getLiveIndexes()));
    Assert.assertEquals(
        List.of(quarantined.asJournalEntry()), mocks.configState.currentJournal().getLiveEntries());
  }

  /**
   * The escape hatch: the index was updated to a definition this binary can parse, so the
   * quarantined generation is superseded and the replacement is built fresh.
   */
  @Test
  public void quarantinedIndexSupersededByReadableDefinition_isDroppedAndRebuilt()
      throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    QuarantinedIndex quarantined = quarantinedIndex(MOCK_INDEX_ID, "quarantined");
    mocks.configState.incompatibleIndexes.addLiveIndex(quarantined);

    // A readable definition arrived, so the control plane no longer reports the id as unreadable.
    swapUpdate(mocks, List.of(), List.of(MOCK_INDEX_DEFINITION), Set.of());

    verify(mocks.indexFactory).deleteGenerationData(quarantined.generationId());
    Assert.assertTrue(mocks.configState.incompatibleIndexes.isEmpty());
    mocks.assertOneIndexCreated();
    mocks.assertIndexCatalogSize(1);
  }

  /** A quarantined staged generation goes without disturbing the live index serving queries. */
  @Test
  public void quarantinedStagedGeneration_isDroppedWhileLiveIndexIsPreserved() throws Exception {
    ConfigStateMocks mocks = ConfigStateMocks.create();
    mocks.addIndex(MOCK_INDEX_DEFINITION_GENERATION_CURRENT, ConfigStateMocks.State.LIVE);
    mocks.configState.postInitializationFromConfigJournal();
    QuarantinedIndex quarantined = quarantinedIndex(MOCK_INDEX_ID, "quarantined", 2);
    mocks.configState.incompatibleIndexes.addStagedIndex(quarantined);

    swapUpdate(mocks, List.of(), List.of(MOCK_INDEX_DEFINITION), Set.of());

    verify(mocks.indexFactory).deleteGenerationData(quarantined.generationId());
    Assert.assertTrue(mocks.configState.incompatibleIndexes.isEmpty());
    mocks.assertIndexCatalogSize(1);
    mocks.assertPersistedJournalEquals(
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_INDEX_DEFINITION_GENERATION_CURRENT)
            .build());
  }

  /**
   * The index has a parseable live generation and a quarantined staged one. MMS reports its
   * authoritative definition as unreadable, and {@code MmsConfigUpdater} sends the live definition
   * in its place - so this update carries both a readable definition and the id, as unreadable. The
   * staged generation stays quarantined: the definition says nothing about whether the generation
   * we could not parse is the one MMS wants.
   */
  @Test
  public void quarantinedStagedGeneration_survivesASubstitutedLiveDefinition() throws Exception {
    ConfigStateMocks mocks = ConfigStateMocks.create();
    mocks.addIndex(MOCK_INDEX_DEFINITION_GENERATION_CURRENT, ConfigStateMocks.State.LIVE);
    mocks.configState.postInitializationFromConfigJournal();
    QuarantinedIndex quarantined = quarantinedIndex(MOCK_INDEX_ID, "quarantined", 2);
    mocks.configState.incompatibleIndexes.addStagedIndex(quarantined);

    swapUpdate(mocks, List.of(), List.of(MOCK_INDEX_DEFINITION), Set.of(MOCK_INDEX_ID));

    verify(mocks.indexFactory, never()).deleteGenerationData(any());
    Assert.assertEquals(
        List.of(quarantined),
        List.copyOf(mocks.configState.incompatibleIndexes.getStagedIndexes()));
    mocks.assertIndexCatalogSize(1);
  }

  /** Nodes with nothing quarantined - every node, normally - do no extra work. */
  @Test
  public void withoutQuarantinedIndexes_nothingIsDeleted() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();

    swapUpdate(mocks, List.of(), List.of(MOCK_INDEX_DEFINITION), Set.of());

    verify(mocks.indexFactory, never()).deleteGenerationData(any());
  }

  private static QuarantinedIndex quarantinedIndex(ObjectId indexId, String name) {
    return quarantinedIndex(indexId, name, 1);
  }

  private static QuarantinedIndex quarantinedIndex(
      ObjectId indexId, String name, int userIndexVersion) {
    var identity =
        new JournalEntryIdentity(
            indexId,
            "db",
            UUID.fromString("00000000-1111-2222-3333-444444444444"),
            name,
            new Generation(new UserIndexVersion(userIndexVersion), IndexFormatVersion.CURRENT));

    return new QuarantinedIndex(
        identity,
        IndexDefinitionGeneration.Type.SEARCH,
        "field is required",
        new BsonDocument("fieldFromTheFuture", new BsonBoolean(true)));
  }

  private ConfigJournalV1 emptyConfigJournal() {
    return ConfigJournalV1Builder.builder().build();
  }

  private ConfigStateMocks getEmptyMocks() throws Exception {
    var mocks = ConfigStateMocks.create();
    mocks.configState.postInitializationFromConfigJournal();
    return mocks;
  }

  private void swapUpdate(
      ConfigStateMocks mocks,
      List<VectorIndexDefinition> vectorIndexes,
      List<SearchIndexDefinition> searchIndexes)
      throws Exception {
    swapUpdate(
        mocks, vectorIndexes, searchIndexes, new MetricsFactory("test", new SimpleMeterRegistry()));
  }

  private void swapUpdate(
      ConfigStateMocks mocks,
      List<VectorIndexDefinition> vectorIndexes,
      List<SearchIndexDefinition> searchIndexes,
      MetricsFactory metricsFactory)
      throws Exception {
    swapUpdate(mocks, vectorIndexes, searchIndexes, Set.of(), metricsFactory);
  }

  private void swapUpdate(
      ConfigStateMocks mocks,
      List<VectorIndexDefinition> vectorIndexes,
      List<SearchIndexDefinition> searchIndexes,
      Set<ObjectId> unreadableDesiredIds)
      throws Exception {
    swapUpdate(
        mocks,
        vectorIndexes,
        searchIndexes,
        unreadableDesiredIds,
        new MetricsFactory("test", new SimpleMeterRegistry()));
  }

  private void swapUpdate(
      ConfigStateMocks mocks,
      List<VectorIndexDefinition> vectorIndexes,
      List<SearchIndexDefinition> searchIndexes,
      Set<ObjectId> unreadableDesiredIds,
      MetricsFactory metricsFactory)
      throws Exception {
    List<IndexDefinitionGenerationProducer> producers =
        IndexDefinitionGenerationProducer.createProducers(
            vectorIndexes,
            searchIndexes,
            List.of(
                OverriddenBaseAnalyzerDefinition.stockAnalyzerWithName(MOCK_SEARCH_ANALYZER_NAME)));
    DesiredConfigStateUpdater.update(
        mocks.configState,
        vectorIndexes,
        searchIndexes,
        Collections.emptyList(),
        producers,
        unreadableDesiredIds,
        metricsFactory);
  }
}
