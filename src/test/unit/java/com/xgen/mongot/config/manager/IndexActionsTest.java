package com.xgen.mongot.config.manager;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndex;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSearchEngine;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingCommitMetadata;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.StaleStateInfo;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.IndexGeneration;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
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
  public void testCustomVectorEngineIndexLeftUninitialized() throws Exception {
    var definitionGeneration = mockCustomVectorEngineDefinitionGeneration();

    this.actions.addNewIndexes(List.of(definitionGeneration));

    // The index stays in the catalog so the SLS conf call can still see it and request a log...
    Assert.assertTrue(
        this.mocks
            .indexCatalog
            .getIndexById(definitionGeneration.getIndexDefinition().getIndexId())
            .isPresent());
    // ...but it is never enqueued for initialization until the SLS-driven init path exists.
    verify(this.mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void testCustomVectorEngineStagedIndexLeftUninitialized() throws Exception {
    var definitionGeneration = mockCustomVectorEngineDefinitionGeneration();
    this.actions.addNewIndexes(List.of(definitionGeneration));
    this.mocks.clearInvocations();

    var stagedDefinitionGeneration =
        definitionGeneration.incrementUser(definitionGeneration.getIndexDefinition());
    this.actions.addStagedIndex(stagedDefinitionGeneration);

    Assert.assertTrue(
        this.mocks
            .staged
            .getIndex(definitionGeneration.getIndexDefinition().getIndexId())
            .isPresent());
    verify(this.mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void testNonCustomVectorEngineIndexStillInitialized() throws Exception {
    // Control: a regular (non custom vector engine) vector index is still enqueued as usual.
    this.actions.addNewIndexes(List.of(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION));

    verify(this.mocks.lifecycleManager).add(any());
  }

  private static VectorIndexDefinitionGeneration mockCustomVectorEngineDefinitionGeneration() {
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

  @Test
  public void addNewIndexes_offlineServingUnresolvedIndex_registersReconstructedIndex()
      throws Exception {
    // The materialized view is unresolvable, but the last on-disk commit carries everything needed
    // to rebuild and timestamp the derived Lucene, so the generation serves offline.
    when(this.mocks.indexFactory.readPersistedCommitUserData(any()))
        .thenReturn(Optional.of(offlineServableCommit()));
    doThrow(new MaterializedViewTransientException("Transient MongoDB error"))
        .when(this.mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    this.actions.addNewIndexes(List.of(definitionGeneration));

    var catalogIndex =
        this.mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    Assert.assertEquals(
        IndexStatus.StatusCode.STALE, catalogIndex.getIndex().getStatus().getStatusCode());
    var reconstructed =
        ((UnresolvedAutoEmbeddingIndex) catalogIndex.getIndex()).getReconstructedDerivedIndex();
    Assert.assertTrue(
        "Offline-serving generation must carry a reconstructed derived index",
        reconstructed.isPresent());
    // The reconstructed index is registered under the raw generation id, so queries and cursors
    // resolve it from the initialized-index catalog like any other index...
    Assert.assertEquals(
        reconstructed,
        this.mocks.initializedIndexCatalog.getIndex(definitionGeneration.getGenerationId()));
    // ...while the generation itself stays out of replication.
    verify(this.mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void addNewIndexes_offlineServingUnresolvedIndex_duplicateRegistrationSkipped()
      throws Exception {
    when(this.mocks.indexFactory.readPersistedCommitUserData(any()))
        .thenReturn(Optional.of(offlineServableCommit()));
    doThrow(new MaterializedViewTransientException("Transient MongoDB error"))
        .when(this.mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    // Occupy the generation id the reconstructed index registers under. The add paths make this
    // unreachable, but registering over it would throw and fail the whole update cycle, so the
    // registration skips instead -- and leaves the entry queries may be serving from in place.
    InitializedVectorIndex alreadyRegistered = mock(InitializedVectorIndex.class);
    when(alreadyRegistered.getGenerationId()).thenReturn(definitionGeneration.getGenerationId());
    this.mocks.initializedIndexCatalog.addIndex(alreadyRegistered);

    this.actions.addNewIndexes(List.of(definitionGeneration));

    Assert.assertEquals(
        Optional.of(alreadyRegistered),
        this.mocks.initializedIndexCatalog.getIndex(definitionGeneration.getGenerationId()));
    // The generation is still catalogued and still out of replication, exactly as it would be had
    // the registration gone through.
    Assert.assertTrue(this.mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).isPresent());
    verify(this.mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void dropFromCatalog_offlineServingUnresolvedIndex_tearsDownWithoutLifecycleDrop()
      throws Exception {
    // Add an offline-serving generation so its reconstructed derived index is registered in the
    // initialized-index catalog, mirroring the add-path coverage above.
    when(this.mocks.indexFactory.readPersistedCommitUserData(any()))
        .thenReturn(Optional.of(offlineServableCommit()));
    doThrow(new MaterializedViewTransientException("Transient MongoDB error"))
        .when(this.mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    this.actions.addNewIndexes(List.of(definitionGeneration));

    var catalogIndex =
        this.mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    InitializedIndex reconstructed =
        ((UnresolvedAutoEmbeddingIndex) catalogIndex.getIndex())
            .getReconstructedDerivedIndex()
            .orElseThrow();
    var generationId = definitionGeneration.getGenerationId();
    this.mocks.clearInvocations();

    this.actions.dropFromCatalog(List.of(catalogIndex));

    // The reconstructed index is removed from the initialized-index catalog and its reader closed.
    Assert.assertTrue(
        "Reconstructed index must be removed from the initialized-index catalog after drop",
        this.mocks.initializedIndexCatalog.getIndex(generationId).isEmpty());
    verify(reconstructed, atLeastOnce()).close();
    // Its cursors are killed directly, since the generation never entered the lifecycle...
    verify(this.mocks.cursorManager).killIndexCursors(generationId);
    // ...and the lifecycle drop is skipped entirely (it would only log a spurious "not added").
    verify(this.mocks.lifecycleManager, never()).dropIndex(any());
  }

  @Test
  public void dropFromPhasingOut_offlineServingUnresolvedIndex_tearsDownWithoutLifecycleDrop()
      throws Exception {
    // The recovery-swap route: once full resolution succeeds, the offline-serving generation is
    // moved to phasing out and dropped by PhasingOutIndexesDropper. That drop must tear down the
    // reconstructed index the same way the catalog drop does, since the generation never entered
    // the lifecycle.
    when(this.mocks.indexFactory.readPersistedCommitUserData(any()))
        .thenReturn(Optional.of(offlineServableCommit()));
    doThrow(new MaterializedViewTransientException("Transient MongoDB error"))
        .when(this.mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    this.actions.addNewIndexes(List.of(definitionGeneration));

    var catalogIndex =
        this.mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    InitializedIndex reconstructed =
        ((UnresolvedAutoEmbeddingIndex) catalogIndex.getIndex())
            .getReconstructedDerivedIndex()
            .orElseThrow();
    var generationId = definitionGeneration.getGenerationId();
    // Move the generation from the live catalog to phasing out, mirroring the swap.
    this.mocks.indexCatalog.removeIndex(VectorIndex.MOCK_INDEX_ID);
    this.mocks.phaseOutIndex(catalogIndex);
    this.mocks.clearInvocations();

    this.actions.dropFromPhasingOut(List.of(catalogIndex));

    Assert.assertTrue(
        "Reconstructed index must be removed from the initialized-index catalog after drop",
        this.mocks.initializedIndexCatalog.getIndex(generationId).isEmpty());
    verify(reconstructed, atLeastOnce()).close();
    verify(this.mocks.cursorManager).killIndexCursors(generationId);
    verify(this.mocks.lifecycleManager, never()).dropIndex(any());
  }

  @Test
  public void addStagedIndex_offlineServingUnresolvedIndex_registersReconstructedIndex()
      throws Exception {
    // Covers that the staged path registers what it reconstructs, the same as the live path. The
    // commit is stubbed for every generation, whereas a real staged attempt reads its own directory
    // and only finds a commit when replaying a generation that previously replicated.
    //
    // The materialized view cannot resolve, but the last on-disk commit lets the generation
    // reconstruct and serve offline. Staging requires a corresponding live index in the catalog, so
    // add one first (also offline-serving) and stage a new attempt on top of it.
    when(this.mocks.indexFactory.readPersistedCommitUserData(any()))
        .thenReturn(Optional.of(offlineServableCommit()));
    doThrow(new MaterializedViewTransientException("Transient MongoDB error"))
        .when(this.mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    this.actions.addNewIndexes(List.of(definitionGeneration));
    this.mocks.clearInvocations();

    var stagedDefinitionGeneration =
        definitionGeneration.incrementUser(definitionGeneration.definition());
    this.actions.addStagedIndex(stagedDefinitionGeneration);

    var stagedIndex = this.mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    Assert.assertEquals(
        IndexStatus.StatusCode.STALE, stagedIndex.getIndex().getStatus().getStatusCode());
    var reconstructed =
        ((UnresolvedAutoEmbeddingIndex) stagedIndex.getIndex()).getReconstructedDerivedIndex();
    Assert.assertTrue(
        "Offline-serving staged generation must carry a reconstructed derived index",
        reconstructed.isPresent());
    // The staged reconstructed index is registered under its own generation id, so it resolves from
    // the initialized-index catalog like any other index...
    Assert.assertEquals(
        reconstructed,
        this.mocks.initializedIndexCatalog.getIndex(stagedDefinitionGeneration.getGenerationId()));
    // ...while the staged generation itself stays out of replication.
    verify(this.mocks.lifecycleManager, never()).add(any());
  }

  @Test
  public void addNewIndexes_failedUnresolvedIndex_registersNothing() throws Exception {
    // No persisted commit (the index factory's default), so there is nothing to reconstruct and the
    // generation is FAILED rather than offline-serving.
    doThrow(new MaterializedViewTransientException("Transient MongoDB error"))
        .when(this.mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    this.actions.addNewIndexes(List.of(definitionGeneration));

    var catalogIndex =
        this.mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    Assert.assertEquals(
        IndexStatus.StatusCode.FAILED, catalogIndex.getIndex().getStatus().getStatusCode());
    Assert.assertTrue(
        ((UnresolvedAutoEmbeddingIndex) catalogIndex.getIndex())
            .getReconstructedDerivedIndex()
            .isEmpty());
    Assert.assertTrue(
        this.mocks
            .initializedIndexCatalog
            .getIndex(definitionGeneration.getGenerationId())
            .isEmpty());
    verify(this.mocks.lifecycleManager, never()).add(any());
  }

  /**
   * A commit whose auto-embedding metadata is complete enough to rebuild the derived Lucene offline
   * (ready view, resolved database name and collection metadata) and whose stale-state info
   * supplies the optime the reconstructed index is served STALE as of.
   */
  private static EncodedUserData offlineServableCommit() {
    MaterializedViewSchemaMetadata schema =
        new MaterializedViewSchemaMetadata(
            1L, Map.of(FieldPath.parse("field"), FieldPath.parse("_autoEmbed.field")));
    var collectionMetadata =
        new MaterializedViewCollectionMetadata(
            schema, UUID.randomUUID(), VectorIndex.MOCK_INDEX_ID.toHexString());
    return IndexCommitUserData.createStale(
            StaleStateInfo.create(new BsonTimestamp(42, 1), StaleStatusReason.DOCS_EXCEEDED))
        .withAutoEmbeddingMetadata(
            new AutoEmbeddingCommitMetadata(
                true, Optional.of("acme___mdb_internal_search"), Optional.of(collectionMetadata)))
        .toEncodedData();
  }
}
