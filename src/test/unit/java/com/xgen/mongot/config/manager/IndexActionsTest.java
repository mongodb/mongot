package com.xgen.mongot.config.manager;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.mock.index.IndexGeneration;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexActionsTest {
  private static final IndexDefinitionGeneration STAGED_INDEX_DEFINITION_GENERATION =
      SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.incrementUser(
          SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.definition());

  private IndexActions actions;
  private ConfigStateMocks mocks;

  @Before
  public void setUp() throws Exception {
    this.mocks = ConfigStateMocks.create();
    this.actions = IndexActions.withReplication(this.mocks.configState);
  }

  @Test
  public void testAddIndex() throws Exception {
    this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
    Assert.assertTrue(this.mocks.indexCatalog.getIndexById(SearchIndex.MOCK_INDEX_ID).isPresent());
    this.mocks.waitAndGetInitializedIndex(
        SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.getGenerationId());
    verify(this.mocks.lifecycleManager).add(any());
  }

  @Test
  public void testAddStagedIndex() throws Exception {
    this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
    this.mocks.clearInvocations();

    this.actions.addStagedIndex(STAGED_INDEX_DEFINITION_GENERATION);
    var index = this.mocks.staged.getIndex(SearchIndex.MOCK_INDEX_ID).orElseThrow();
    verify(this.mocks.lifecycleManager).add(any());
    Assert.assertSame(
        STAGED_INDEX_DEFINITION_GENERATION.getIndexDefinition(), index.getDefinition());
    this.mocks.waitAndGetInitializedIndex(STAGED_INDEX_DEFINITION_GENERATION.getGenerationId());
  }

  @Test
  public void testDropFromCatalog() throws Exception {
    this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
    var indexes = List.copyOf(this.mocks.indexCatalog.getIndexes());
    InitializedIndex initializedIndex =
        this.mocks.waitAndGetInitializedIndex(indexes.get(0).getGenerationId());
    this.actions.dropFromCatalog(indexes);
    this.mocks.assertIndexCatalogSize(0);
    this.mocks.assertIndexDropped(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION, initializedIndex);
    verify(this.mocks.lifecycleManager).dropIndex(any());
    Assert.assertFalse(
        this.mocks
            .initializedIndexCatalog
            .getIndex(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.getGenerationId())
            .isPresent());
  }

  @Test
  public void testDropFromStaged() throws Exception {
    this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
    this.actions.addStagedIndex(STAGED_INDEX_DEFINITION_GENERATION);

    var indexes = List.copyOf(this.mocks.staged.getIndexes());
    InitializedIndex initializedIndex =
        this.mocks.waitAndGetInitializedIndex(indexes.get(0).getGenerationId());
    this.actions.dropFromStaged(indexes);
    this.mocks.assertStagedIndexesAre(); // no staged indexes.
    this.mocks.assertIndexDropped(STAGED_INDEX_DEFINITION_GENERATION, initializedIndex);
    verify(this.mocks.lifecycleManager, times(1)).dropIndex(any());
    Assert.assertFalse(
        this.mocks
            .initializedIndexCatalog
            .getIndex(STAGED_INDEX_DEFINITION_GENERATION.getGenerationId())
            .isPresent());
  }

  @Test
  public void testDropFromPhasingOut() throws Exception {
    var toDrop =
        this.mocks.addIndex(
            SearchIndex.MOCK_INDEX_DEFINITION_GENERATION, ConfigStateMocks.State.PHASE_OUT);
    InitializedIndex initializedIndex =
        this.mocks.waitAndGetInitializedIndex(toDrop.getGenerationId());
    var doNotDrop =
        this.mocks.addIndex(STAGED_INDEX_DEFINITION_GENERATION, ConfigStateMocks.State.PHASE_OUT);

    Assert.assertEquals(2, this.mocks.phasingOut.getIndexes().size());

    this.actions.dropFromPhasingOut(List.of(toDrop));

    this.mocks.assertIndexDropped(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION, initializedIndex);
    verify(this.mocks.lifecycleManager, times(1)).dropIndex(any());
    this.mocks.assertPhasingOutIndexesAre(doNotDrop); // one index should remain
    verify(doNotDrop.getIndex(), never()).drop();
    Assert.assertFalse(
        this.mocks.initializedIndexCatalog.getIndex(toDrop.getGenerationId()).isPresent());
    this.mocks.waitAndGetInitializedIndex(doNotDrop.getGenerationId());
    Assert.assertTrue(
        this.mocks.initializedIndexCatalog.getIndex(doNotDrop.getGenerationId()).isPresent());
  }

  @Test
  public void testAddStagedIndexWithoutCorrespondingLiveIndexThrows() {
    // can't add staged index without corresponding index in catalog.
    Assert.assertThrows(
        RuntimeException.class,
        () -> this.actions.addStagedIndex(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
  }

  @Test
  public void testAddDuplicateIndexToCatalogThrows()
      throws IOException, InvalidAnalyzerDefinitionException {
    this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));

    Assert.assertThrows(
        RuntimeException.class,
        () -> this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION)));
  }

  @Test
  public void testDuplicateIndexToStagedThrows()
      throws IOException, InvalidAnalyzerDefinitionException {
    this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
    this.actions.addStagedIndex(STAGED_INDEX_DEFINITION_GENERATION);

    Assert.assertThrows(
        RuntimeException.class,
        () -> this.actions.addStagedIndex(STAGED_INDEX_DEFINITION_GENERATION));
  }

  /**
   * We make sure that we don't allow indexes to be dropped if as a result, a staged index will not
   * have a corresponding index in the catalog.
   */
  @Test
  public void testRemovingIndexFromCatalogWithExistingCorrespondingStagedIndexThrows()
      throws IOException, InvalidAnalyzerDefinitionException {
    this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
    this.actions.addStagedIndex(STAGED_INDEX_DEFINITION_GENERATION);
    var index = this.mocks.indexCatalog.getIndexById(SearchIndex.MOCK_INDEX_ID).orElseThrow();

    Assert.assertThrows(RuntimeException.class, () -> this.actions.dropFromCatalog(List.of(index)));
  }

  @Test
  public void testRemovingIndexNotInCatalogThrows() {
    var indexGen = IndexGeneration.mockIndexGeneration();

    TestUtils.assertThrows(
        "attempting to remove index not present in catalog",
        RuntimeException.class,
        () -> this.actions.dropFromCatalog(List.of(indexGen)));
  }

  @Test
  public void testRemovingIndexNotInStagedThrows() {
    var indexGen = IndexGeneration.mockIndexGeneration();

    TestUtils.assertThrows(
        "attempting to remove index not present in staged",
        RuntimeException.class,
        () -> this.actions.dropFromStaged(List.of(indexGen)));
  }

  @Test
  public void testRemovingIndexNotInPhasingOutThrows() {
    var indexGen = IndexGeneration.mockIndexGeneration();

    TestUtils.assertThrows(
        "attempting to remove index not present in phasingOut",
        RuntimeException.class,
        () -> this.actions.dropFromPhasingOut(List.of(indexGen)));
  }

  @Test
  public void testAddIndexWithoutReplication() throws Exception {
    this.actions = IndexActions.withoutReplication(this.mocks.configState);
    this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
    Assert.assertTrue(this.mocks.indexCatalog.getIndexById(SearchIndex.MOCK_INDEX_ID).isPresent());
    verify(this.mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void testAddStagedIndexWithoutReplication() throws Exception {
    this.actions = IndexActions.withoutReplication(this.mocks.configState);

    this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
    this.mocks.clearInvocations();

    this.actions.addStagedIndex(STAGED_INDEX_DEFINITION_GENERATION);
    var index = this.mocks.staged.getIndex(SearchIndex.MOCK_INDEX_ID).orElseThrow();
    verify(this.mocks.lifecycleManager, never()).add(any());
    Assert.assertSame(
        STAGED_INDEX_DEFINITION_GENERATION.getIndexDefinition(), index.getDefinition());
  }

  @Test
  public void testDropFromCatalogWithoutReplication() throws Exception {
    this.actions = IndexActions.withoutReplication(this.mocks.configState);

    this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
    var indexes = List.copyOf(this.mocks.indexCatalog.getIndexes());
    Assert.assertTrue(
        this.mocks.initializedIndexCatalog.getIndex(indexes.get(0).getGenerationId()).isEmpty());
    this.actions.dropFromCatalog(indexes);
    this.mocks.assertIndexCatalogSize(0);
    this.mocks.assertIndexCreatedAndDropped(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION);
    verify(this.mocks.lifecycleManager, never()).dropIndex(any());
  }

  @Test
  public void testDropFromStagedWithoutReplication() throws Exception {
    this.actions = IndexActions.withoutReplication(this.mocks.configState);

    this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
    this.actions.addStagedIndex(STAGED_INDEX_DEFINITION_GENERATION);

    var indexes = List.copyOf(this.mocks.staged.getIndexes());
    Assert.assertTrue(
        this.mocks.initializedIndexCatalog.getIndex(indexes.get(0).getGenerationId()).isEmpty());
    this.actions.dropFromStaged(indexes);
    this.mocks.assertStagedIndexesAre(); // no staged indexes.
    this.mocks.assertIndexCreatedAndDropped(STAGED_INDEX_DEFINITION_GENERATION);
    verify(this.mocks.lifecycleManager, never()).dropIndex(any());
  }

  @Test
  public void testAddAutoEmbeddingIndex() throws Exception {
    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    this.actions.addNewIndexes(List.of(definitionGeneration));
    Assert.assertTrue(this.mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).isPresent());
    this.mocks.waitAndGetInitializedIndex(definitionGeneration.getGenerationId());
    verify(this.mocks.lifecycleManager).add(any(AutoEmbeddingIndexGeneration.class));
  }

  /**
   * Verifies that the index is present in the catalog when the replication flow drops the index.
   * Otherwise, the replication flow will fail to kill the cursors before the index is removed from
   * the catalog.
   */
  @Test
  public void testDropIndexKeepsIndexInCatalogDuringReplicationDrop() throws Exception {
    this.actions.addNewIndexes(List.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
    var indexes = List.copyOf(this.mocks.indexCatalog.getIndexes());
    GenerationId generationId = indexes.get(0).getGenerationId();
    this.mocks.waitAndGetInitializedIndex(generationId);

    AtomicReference<Boolean> isIndexInCatalog = new AtomicReference<>(false);

    doAnswer(
            invocation -> {
              GenerationId droppedGenerationId = invocation.getArgument(0);
              boolean isPresent =
                  this.mocks.initializedIndexCatalog.getIndex(droppedGenerationId).isPresent();
              isIndexInCatalog.set(isPresent);
              return CompletableFuture.completedFuture(null);
            })
        .when(this.mocks.lifecycleManager)
        .dropIndex(any());

    this.actions.dropFromCatalog(indexes);

    verify(this.mocks.lifecycleManager).dropIndex(generationId);

    Assert.assertTrue(
        "Index must be present in Catalog when lifecycleManager.dropIndex() runs",
        isIndexInCatalog.get());

    Assert.assertFalse(
        "Index must be removed from Catalog after drop completes",
        this.mocks.initializedIndexCatalog.getIndex(generationId).isPresent());
  }

  @Test
  public void
      addNewIndexes_withMongoClientNotAvailable_addsUnresolvedIndexWithResolutionFailedStatus()
          throws Exception {
    doThrow(
            new MaterializedViewTransientException(
                "No sync source available",
                MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE))
        .when(this.mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);

    this.actions.addNewIndexes(List.of(definitionGeneration));

    var catalogIndex =
        this.mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    Assert.assertTrue(catalogIndex instanceof UnresolvedAutoEmbeddingIndexGeneration);
    Assert.assertEquals(
        IndexStatus.StatusCode.FAILED, catalogIndex.getIndex().getStatus().getStatusCode());
    Assert.assertEquals(
        IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_FAILED,
        catalogIndex.getIndex().getStatus().getReason().get());
    Assert.assertFalse(
        "Index with MONGO_CLIENT_NOT_AVAILABLE should NOT be recoverable",
        catalogIndex.getIndex().getStatus().canBeRecovered());
    verify(this.mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void addNewIndexes_withUnknownReason_addsUnresolvedIndexWithResolutionRetryStatus()
      throws Exception {
    // Default constructor uses UNKNOWN reason
    doThrow(new MaterializedViewTransientException("Transient MongoDB error"))
        .when(this.mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);

    this.actions.addNewIndexes(List.of(definitionGeneration));

    var catalogIndex =
        this.mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    Assert.assertTrue(catalogIndex instanceof UnresolvedAutoEmbeddingIndexGeneration);
    Assert.assertEquals(
        IndexStatus.StatusCode.FAILED, catalogIndex.getIndex().getStatus().getStatusCode());
    Assert.assertEquals(
        IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY,
        catalogIndex.getIndex().getStatus().getReason().get());
    Assert.assertTrue(
        "Index with UNKNOWN reason should be recoverable immediately",
        catalogIndex.getIndex().getStatus().canBeRecovered());
    verify(this.mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void addStagedIndex_withNoSyncSource_addsUnresolvedStagedIndexWithResolutionFailedStatus()
      throws Exception {
    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    this.actions.addNewIndexes(List.of(definitionGeneration));
    this.mocks.clearInvocations();

    doThrow(
            new MaterializedViewTransientException(
                "No sync source available",
                MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE))
        .when(this.mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var stagedDefinitionGeneration =
        definitionGeneration.incrementUser(definitionGeneration.definition());
    this.actions.addStagedIndex(stagedDefinitionGeneration);

    var stagedIndex = this.mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    Assert.assertTrue(stagedIndex instanceof UnresolvedAutoEmbeddingIndexGeneration);
    Assert.assertEquals(
        IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_FAILED,
        stagedIndex.getIndex().getStatus().getReason().get());
    Assert.assertFalse(
        "Staged index with MONGO_CLIENT_NOT_AVAILABLE should NOT be recoverable",
        stagedIndex.getIndex().getStatus().canBeRecovered());
    verify(this.mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void addStagedIndex_withUnknownReason_addsUnresolvedStagedIndexWithResolutionRetryStatus()
      throws Exception {
    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    this.actions.addNewIndexes(List.of(definitionGeneration));
    this.mocks.clearInvocations();

    // Default constructor uses UNKNOWN reason
    doThrow(new MaterializedViewTransientException("Transient MongoDB error"))
        .when(this.mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var stagedDefinitionGeneration =
        definitionGeneration.incrementUser(definitionGeneration.definition());
    this.actions.addStagedIndex(stagedDefinitionGeneration);

    var stagedIndex = this.mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    Assert.assertTrue(stagedIndex instanceof UnresolvedAutoEmbeddingIndexGeneration);
    Assert.assertEquals(
        IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY,
        stagedIndex.getIndex().getStatus().getReason().get());
    Assert.assertTrue(
        "Staged index with UNKNOWN reason should be recoverable immediately",
        stagedIndex.getIndex().getStatus().canBeRecovered());
    verify(this.mocks.lifecycleManager, never()).add(any());
  }
}
