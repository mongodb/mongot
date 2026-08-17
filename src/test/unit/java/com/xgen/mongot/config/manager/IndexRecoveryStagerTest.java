package com.xgen.mongot.config.manager;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.status.IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_FAILED;
import static com.xgen.mongot.index.status.IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY;
import static com.xgen.mongot.index.status.IndexStatus.Reason.COLLECTION_NOT_FOUND;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Var;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class IndexRecoveryStagerTest {

  /** All IndexStatuses except RECOVERING. */
  @DataPoints("nonRecoveringIndexStatuses")
  public static Set<IndexStatus> nonRecoveringIndex() {
    return Stream.of(IndexStatus.StatusCode.values())
        .filter(statusCode -> statusCode != IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT)
        .map(IndexStatus::new)
        .collect(Collectors.toSet());
  }

  /** Test that all index statuses besides recovering lead to no changes. */
  @Theory
  public void testNoRecoveringIndexesDoesNothing(
      @FromDataPoints("nonRecoveringIndexStatuses") IndexStatus status) throws Exception {
    var mocks = getEmptyMocks();
    var index = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    index.getIndex().setStatus(status);
    mocks.clearInvocations();

    stageIndexes(mocks, Set.of());

    mocks.assertLiveIndexesAre(index);
    mocks.assertNoIndexActivity();
    verifyNoMoreInteractions(mocks.journalWriter);
  }

  @Test
  public void testDoesNotExistIndexWithCollectionNotFoundStagesCorrectly() throws Exception {
    var mocks = getEmptyMocks();
    var index = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    index.getIndex().setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    mocks.clearInvocations();

    UUID mockCollectionUuid = index.getDefinition().getCollectionUuid();
    String mockDatabase = index.getDefinition().getDatabase();
    String mockCollectionName = index.getDefinition().getLastObservedCollectionName();

    // Scenario 1: Collection doesn't exist on mongod
    stageIndexes(mocks, Set.of());

    // Verify that no index is staged
    @Var var stagedIndex = mocks.staged.getIndex(index.getDefinition().getIndexId());
    assertTrue(stagedIndex.isEmpty());
    mocks.assertNoIndexActivity();

    // Scenario 2: Collection exists on mongod
    var correctCollectionInfos =
        new MongoDbCollectionInfos(
            ImmutableMap.of(
                new MongoNamespace(mockDatabase, mockCollectionName),
                new MongoDbCollectionInfo.Collection(
                    mockCollectionName,
                    new MongoDbCollectionInfo.Collection.Info(mockCollectionUuid))));

    stageIndexes(mocks, correctCollectionInfos.getAllCollectionUuids());

    // Verify the index is staged
    stagedIndex = mocks.staged.getIndex(index.getDefinition().getIndexId());
    assertTrue(stagedIndex.isPresent());
    assertEquals(
        index.getDefinitionGeneration().generation().nextAttempt(),
        stagedIndex.get().getGenerationId().generation);
    mocks.clearInvocations();
  }

  @Test
  public void regularIndexRecoveryDoesNotDependOnMongodReachability() throws Exception {
    var mocks = getEmptyMocks();
    var index = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    assertFalse(index instanceof AutoEmbeddingIndexGeneration);
    index.getIndex().setStatus(IndexStatus.recoveringNonTransient(new BsonTimestamp()));
    var materializedViewIndexFactory = mocks.configState.materializedViewIndexFactory.get();
    when(materializedViewIndexFactory.isMongodReachableFromCachedTopology()).thenReturn(false);
    mocks.clearInvocations();
    clearInvocations(materializedViewIndexFactory);

    stageIndexes(mocks, Set.of());

    mocks.assertOneIndexCreated();
    mocks.assertAtLeastOneIndexStagedAndReplicated();
    mocks.assertJournalPersistedAtLeastOnce();

    var stagedIndex = mocks.staged.getIndex(index.getDefinition().getIndexId());
    assertTrue(stagedIndex.isPresent());
    assertEquals(
        index.getDefinitionGeneration().generation().nextAttempt(),
        stagedIndex.get().getGenerationId().generation);
    verify(materializedViewIndexFactory, never()).isMongodReachableFromCachedTopology();
  }

  @Test
  public void regularFailedStagedIndexRetriesWhenMongodIsUnreachable() throws Exception {
    var mocks = getEmptyMocks();
    var definitionGeneration = mockDefinitionGeneration(new ObjectId());
    mocks.addIndex(definitionGeneration, ConfigStateMocks.State.LIVE);
    IndexGeneration failedAttempt =
        mocks.addIndex(definitionGeneration.incrementAttempt(), ConfigStateMocks.State.STAGED);
    assertFalse(failedAttempt instanceof AutoEmbeddingIndexGeneration);
    failedAttempt
        .getIndex()
        .setStatus(IndexStatus.failed("init failed", IndexStatus.Reason.INITIALIZATION_FAILED));
    var materializedViewIndexFactory = mocks.configState.materializedViewIndexFactory.get();
    when(materializedViewIndexFactory.isMongodReachableFromCachedTopology()).thenReturn(false);
    mocks.clearInvocations();
    clearInvocations(materializedViewIndexFactory);

    stageIndexes(mocks, Set.of());

    assertEquals(
        failedAttempt.getDefinitionGeneration().incrementAttempt().getGenerationId(),
        mocks
            .staged
            .getIndex(failedAttempt.getDefinition().getIndexId())
            .orElseThrow()
            .getGenerationId());
    assertThat(mocks.phasingOut.getIndexes()).contains(failedAttempt);
    verify(materializedViewIndexFactory, never()).isMongodReachableFromCachedTopology();
  }

  @Test
  public void resolvedAutoEmbeddingIndexRecoveryDoesNotDependOnMongodReachability()
      throws Exception {
    var mocks = getEmptyMocks();
    var definitionGeneration =
        mockDefinitionGeneration(
            MaterializedViewIndex.mockAutoEmbeddingVectorDefinition(new ObjectId(), 0));
    IndexGeneration index = mocks.addIndex(definitionGeneration, ConfigStateMocks.State.LIVE);
    assertTrue(index instanceof AutoEmbeddingIndexGeneration);
    AutoEmbeddingIndexGeneration autoEmbeddingIndex = (AutoEmbeddingIndexGeneration) index;
    autoEmbeddingIndex
        .getMaterializedViewIndexGeneration()
        .getIndex()
        .setStatus(IndexStatus.recoveringNonTransient(new BsonTimestamp()));

    var materializedViewIndexFactory = mocks.configState.materializedViewIndexFactory.get();
    when(materializedViewIndexFactory.isMongodReachableFromCachedTopology()).thenReturn(false);
    mocks.clearInvocations();
    clearInvocations(materializedViewIndexFactory);

    stageIndexes(mocks, Set.of());

    assertEquals(
        definitionGeneration.incrementAttempt().getGenerationId(),
        mocks
            .staged
            .getIndex(definitionGeneration.getIndexDefinition().getIndexId())
            .orElseThrow()
            .getGenerationId());
    verify(materializedViewIndexFactory, never()).isMongodReachableFromCachedTopology();
  }

  @Test
  public void testIndexNotStagedIfAlreadyStagedUser() throws Exception {
    var mocks = getEmptyMocks();
    var index = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    index.getIndex().setStatus(IndexStatus.steady());

    var stagedIndex = mocks.stageIndex(index.getDefinition().getIndexId());
    stagedIndex.getIndex().setStatus(IndexStatus.initialSync());
    mocks.clearInvocations();

    stageIndexes(mocks, Set.of());

    mocks.assertNoIndexActivity();
  }

  @Test
  public void testIndexNotStagedIfAlreadyStagedAttempt() throws Exception {
    var mocks = getEmptyMocks();
    var index = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    index.getIndex().setStatus(IndexStatus.steady());

    var stagedIndex = mocks.stageIndexAttempt(index.getDefinition().getIndexId());
    stagedIndex.getIndex().setStatus(IndexStatus.initialSync());
    mocks.clearInvocations();

    stageIndexes(mocks, Set.of());

    mocks.assertNoIndexActivity();
  }

  @Test
  public void unresolvedAutoEmbeddingIndex_nosyncsource_afterUpdateSyncSource_isRecoveredByStager()
      throws Exception {
    var mocks = getEmptyMocks();

    // Simulate missing sync source with MONGO_CLIENT_NOT_AVAILABLE reason
    doThrow(
            new MaterializedViewTransientException(
                "No sync source available",
                MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE))
        .when(mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);

    IndexActions.withReplication(mocks.configState).addNewIndexes(List.of(definitionGeneration));

    var unresolvedIndex = mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    assertFalse(
        "Index with MONGO_CLIENT_NOT_AVAILABLE should NOT be recoverable before updateSyncSource",
        unresolvedIndex.getIndex().getStatus().canBeRecovered());
    assertThat(unresolvedIndex.getIndex().getStatus().getReason())
        .isEqualTo(Optional.of(AUTO_EMBEDDING_RESOLUTION_FAILED));
    assertTrue(unresolvedIndex instanceof UnresolvedAutoEmbeddingIndexGeneration);

    mocks.clearInvocations();

    // Restore MaterializedViewIndexFactory to succeed on subsequent calls
    reset(mocks.configState.materializedViewIndexFactory.get());
    when(mocks
            .configState
            .materializedViewIndexFactory
            .get()
            .getIndex(any(IndexDefinitionGeneration.class)))
        .thenAnswer(
            invocation ->
                MaterializedViewIndex.mockIndex(
                    (MaterializedViewIndexDefinitionGeneration) invocation.getArgument(0)));
    when(mocks.configState.materializedViewIndexFactory.get().isMongodReachableFromCachedTopology())
        .thenReturn(false);

    // updateSyncSource resets unresolved indexes to AUTO_EMBEDDING_RESOLUTION_RETRY (recoverable)
    mocks.configState.updateSyncSource(ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);

    assertEquals(
        IndexStatus.StatusCode.FAILED, unresolvedIndex.getIndex().getStatus().getStatusCode());
    assertThat(unresolvedIndex.getIndex().getStatus().getReason())
        .isEqualTo(Optional.of(AUTO_EMBEDDING_RESOLUTION_RETRY));

    assertTrue(
        "Index should be recoverable after updateSyncSource",
        unresolvedIndex.getIndex().getStatus().canBeRecovered());

    mocks.clearInvocations();

    stageIndexes(mocks, Set.of());
    assertTrue(mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).isEmpty());
    mocks.assertNoIndexActivity();

    when(mocks.configState.materializedViewIndexFactory.get().isMongodReachableFromCachedTopology())
        .thenReturn(true);

    // IndexRecoveryStager should stage a recovery attempt
    stageIndexes(mocks, Set.of());

    var stagedIndex = mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID);
    assertTrue(
        "Unresolved auto-embedding index should be staged for recovery after updateSyncSource",
        stagedIndex.isPresent());
    assertEquals(
        definitionGeneration.generation().nextAttempt(),
        stagedIndex.get().getGenerationId().generation);
  }

  @Test
  public void unresolvedAutoEmbeddingIndex_withUnknownReason_stagesOnlyWhenMongodIsReachable()
      throws Exception {
    var mocks = getEmptyMocks();

    // Simulate transient error with UNKNOWN reason (default) - should be recoverable immediately
    doThrow(new MaterializedViewTransientException("Transient MongoDB error"))
        .when(mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);

    IndexActions.withReplication(mocks.configState).addNewIndexes(List.of(definitionGeneration));

    var unresolvedIndex = mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    assertTrue(unresolvedIndex instanceof UnresolvedAutoEmbeddingIndexGeneration);

    // With UNKNOWN reason, index should have AUTO_EMBEDDING_RESOLUTION_RETRY and be recoverable
    assertThat(unresolvedIndex.getIndex().getStatus().getReason())
        .isEqualTo(Optional.of(AUTO_EMBEDDING_RESOLUTION_RETRY));
    assertTrue(
        "Index with UNKNOWN reason should be recoverable immediately",
        unresolvedIndex.getIndex().getStatus().canBeRecovered());

    mocks.clearInvocations();

    // The status is recoverable, but the unresolved index must remain untouched until mongod is
    // reachable.
    stageIndexes(mocks, Set.of());
    assertTrue(mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).isEmpty());
    mocks.assertNoIndexActivity();

    // Restore MaterializedViewIndexFactory to succeed on subsequent calls
    reset(mocks.configState.materializedViewIndexFactory.get());
    when(mocks
            .configState
            .materializedViewIndexFactory
            .get()
            .getIndex(any(IndexDefinitionGeneration.class)))
        .thenAnswer(
            invocation ->
                MaterializedViewIndex.mockIndex(
                    (MaterializedViewIndexDefinitionGeneration) invocation.getArgument(0)));
    when(mocks.configState.materializedViewIndexFactory.get().isMongodReachableFromCachedTopology())
        .thenReturn(true);

    // IndexRecoveryStager should stage a recovery attempt immediately (no updateSyncSource needed)
    stageIndexes(mocks, Set.of());

    var stagedIndex = mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID);
    assertTrue(
        "Unresolved auto-embedding index with UNKNOWN reason should be staged for recovery",
        stagedIndex.isPresent());
    assertEquals(
        definitionGeneration.generation().nextAttempt(),
        stagedIndex.get().getGenerationId().generation);
  }

  @Test
  public void unresolvedAutoEmbeddingFailedStage_isClosedAndDroppedAfterRestaging()
      throws Exception {
    var mocks = getEmptyMocks();
    var liveDefinitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    mocks.addIndex(liveDefinitionGeneration, ConfigStateMocks.State.LIVE);
    IndexDefinitionGeneration failedDefinitionGeneration =
        liveDefinitionGeneration.incrementAttempt();
    Index cleanupIndex = mock(Index.class);
    var failedIndex =
        UnresolvedAutoEmbeddingIndexGeneration.createFailed(
            failedDefinitionGeneration,
            MaterializedViewTransientException.Reason.UNKNOWN,
            Optional.of(cleanupIndex));
    mocks.staged.addIndex(failedIndex);
    when(mocks.configState.materializedViewIndexFactory.get().isMongodReachableFromCachedTopology())
        .thenReturn(true);

    stageIndexes(mocks, Set.of());

    assertThat(mocks.phasingOut.getIndexes()).containsExactly(failedIndex);
    var replacement = mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    assertEquals(
        failedDefinitionGeneration.generation().nextAttempt(),
        replacement.getGenerationId().generation);

    PhasingOutIndexesDropper.dropUnused(
        mocks.configState, IndexActions.withReplication(mocks.configState));

    var inOrder = inOrder(cleanupIndex);
    inOrder.verify(cleanupIndex).close();
    inOrder.verify(cleanupIndex).drop();
    assertThat(mocks.phasingOut.getIndexes()).isEmpty();
    // An unresolved auto-embedding generation never entered the lifecycle, so its teardown skips
    // the lifecycle drop rather than invoking it.
    verify(mocks.lifecycleManager, never()).dropIndex(failedIndex.getGenerationId());
  }

  @Test
  public void unresolvedAutoEmbeddingIndex_staleOfflineServing_isStagedForRecoveryFromLiveCatalog()
      throws Exception {
    var mocks = getEmptyMocks();

    // A live, unresolved auto-embedding index whose on-disk derived Lucene was queryable at commit
    // time: it serves offline from the reconstructed derived index and reports a real STALE status,
    // so it parks in the live indexCatalog and recovery re-stages it by type (not by a recoverable
    // FAILED reason).
    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    InitializedVectorIndex reconstructed = mock(InitializedVectorIndex.class);
    when(reconstructed.getStatus())
        .thenReturn(IndexStatus.stale("serving offline", new BsonTimestamp(1, 1)));
    var unresolvedIndex =
        UnresolvedAutoEmbeddingIndexGeneration.createStale(
            definitionGeneration, reconstructed, Map.of());
    mocks.indexCatalog.addIndex(unresolvedIndex);

    assertEquals(
        IndexStatus.StatusCode.STALE, unresolvedIndex.getIndex().getStatus().getStatusCode());
    // Recovery is driven by type for a STALE offline-serving index, not by canBeRecovered().
    assertFalse(unresolvedIndex.getIndex().getStatus().canBeRecovered());

    mocks.clearInvocations();

    // Mongod is still unreachable, so the live offline-serving generation remains untouched.
    stageIndexes(mocks, Set.of());
    assertTrue(mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).isEmpty());
    mocks.assertNoIndexActivity();

    // Materialized view is resolvable again (mongod reachable).
    reset(mocks.configState.materializedViewIndexFactory.get());
    when(mocks
            .configState
            .materializedViewIndexFactory
            .get()
            .getIndex(any(IndexDefinitionGeneration.class)))
        .thenAnswer(
            invocation ->
                MaterializedViewIndex.mockIndex(
                    (MaterializedViewIndexDefinitionGeneration) invocation.getArgument(0)));
    when(mocks.configState.materializedViewIndexFactory.get().isMongodReachableFromCachedTopology())
        .thenReturn(true);

    // Recovery must stage a new attempt for the live queryable index (driven off the live
    // indexCatalog via IndexRecoveryStager::getRecoveringIndexesToStage).
    stageIndexes(mocks, Set.of());

    var stagedIndex = mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID);
    assertTrue(
        "Queryable unresolved auto-embedding index in the live catalog should be staged for "
            + "recovery",
        stagedIndex.isPresent());
    assertEquals(
        definitionGeneration.generation().nextAttempt(),
        stagedIndex.get().getGenerationId().generation);
  }

  @Test
  public void unresolvedAutoEmbeddingIndex_failedStageRetriesOnlyWhenMongodIsReachable()
      throws Exception {
    var mocks = getEmptyMocks();
    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    IndexActions.withReplication(mocks.configState).addNewIndexes(List.of(definitionGeneration));

    doThrow(new MaterializedViewTransientException("Mongod unavailable"))
        .when(mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));
    IndexDefinitionGeneration failedAttempt = definitionGeneration.incrementAttempt();
    IndexActions.withReplication(mocks.configState).addStagedIndex(failedAttempt);

    var unresolvedStagedIndex = mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    assertThat(unresolvedStagedIndex.getIndex().getStatus().getReason())
        .isEqualTo(Optional.of(AUTO_EMBEDDING_RESOLUTION_RETRY));
    mocks.clearInvocations();

    // Mongod is still unreachable: keep the failed attempt staged instead of dropping and
    // immediately recreating another failed attempt.
    stageIndexes(mocks, Set.of());
    assertEquals(
        failedAttempt.getGenerationId(),
        mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow().getGenerationId());
    assertTrue(mocks.phasingOut.getIndexes().isEmpty());

    reset(mocks.configState.materializedViewIndexFactory.get());
    when(mocks
            .configState
            .materializedViewIndexFactory
            .get()
            .getIndex(any(IndexDefinitionGeneration.class)))
        .thenAnswer(
            invocation ->
                MaterializedViewIndex.mockIndex(
                    (MaterializedViewIndexDefinitionGeneration) invocation.getArgument(0)));
    when(mocks.configState.materializedViewIndexFactory.get().isMongodReachableFromCachedTopology())
        .thenReturn(true);

    stageIndexes(mocks, Set.of());

    assertEquals(
        failedAttempt.incrementAttempt().getGenerationId(),
        mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow().getGenerationId());
    assertThat(mocks.phasingOut.getIndexes()).contains(unresolvedStagedIndex);
  }

  @Test
  public void unresolvedAutoEmbeddingIndex_beforeUpdateSyncSource_isNotRecovered()
      throws Exception {
    var mocks = getEmptyMocks();

    doThrow(
            new MaterializedViewTransientException(
                "No sync source available",
                MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE))
        .when(mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any(IndexDefinitionGeneration.class));

    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);

    IndexActions.withReplication(mocks.configState).addNewIndexes(List.of(definitionGeneration));

    var unresolvedIndex = mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    assertThat(unresolvedIndex.getIndex().getStatus().getReason())
        .isEqualTo(Optional.of(AUTO_EMBEDDING_RESOLUTION_FAILED));
    // Status is FAILED but without INITIALIZATION_FAILED reason, so not recoverable yet
    assertFalse(unresolvedIndex.getIndex().getStatus().canBeRecovered());
    mocks.clearInvocations();

    // Without updateSyncSource, the stager should not pick it up
    stageIndexes(mocks, Set.of());

    var stagedIndex = mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID);
    assertTrue(
        "Unresolved index should NOT be staged before updateSyncSource is called",
        stagedIndex.isEmpty());
  }

  @Test
  public void unresolvedAutoEmbeddingIndex_collectionNotFound_isStagedWhenCollectionReappears()
      throws Exception {
    var mocks = getEmptyMocks();

    // Gate: source collection absent on this shard, so getAutoEmbeddingIndexGeneration short-
    // circuits to UnresolvedAutoEmbeddingIndexGeneration.createCollectionNotFound(...): no
    // materialized view collection, lease, or leaderStatus gauge is created (verified by
    // AutoEmbeddingIndexGenerationFactoryTest). This is the DOES_NOT_EXIST/COLLECTION_NOT_FOUND
    // generation whose recovery the ConfigState reset-skip relies on.
    when(mocks
            .configState
            .materializedViewIndexFactory
            .get()
            .isSourceCollectionAbsentOnMongod(any()))
        .thenReturn(true);

    var definitionGeneration =
        mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    IndexActions.withReplication(mocks.configState).addNewIndexes(List.of(definitionGeneration));

    var unresolvedIndex = mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    assertTrue(
        "Absent source collection must yield an unresolved auto-embedding generation",
        unresolvedIndex instanceof UnresolvedAutoEmbeddingIndexGeneration);
    assertEquals(
        IndexStatus.StatusCode.DOES_NOT_EXIST,
        unresolvedIndex.getIndex().getStatus().getStatusCode());
    assertThat(unresolvedIndex.getIndex().getStatus().getReason())
        .isEqualTo(Optional.of(COLLECTION_NOT_FOUND));
    mocks.clearInvocations();

    // The COLLECTION_NOT_FOUND generation must carry the *source* collection UUID so the recovery
    // stager can match it against directMongodCollectionSet when the collection reappears.
    UUID sourceCollectionUuid = unresolvedIndex.getDefinition().getCollectionUuid();

    // Scenario 1: source collection still absent on mongod → not staged for recovery.
    stageIndexes(mocks, Set.of());
    assertTrue(mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).isEmpty());
    mocks.assertNoIndexActivity();

    // Scenario 2: source collection reappears on this shard (its UUID is now in
    // directMongodCollectionSet). Restore the factory so the re-staged attempt resolves to a real
    // composite (isSourceCollectionAbsentOnMongod defaults to false after reset).
    reset(mocks.configState.materializedViewIndexFactory.get());
    when(mocks
            .configState
            .materializedViewIndexFactory
            .get()
            .getIndex(any(IndexDefinitionGeneration.class)))
        .thenAnswer(
            invocation ->
                MaterializedViewIndex.mockIndex(
                    (MaterializedViewIndexDefinitionGeneration) invocation.getArgument(0)));

    stageIndexes(mocks, Set.of(sourceCollectionUuid));

    var stagedIndex = mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID);
    assertTrue(
        "Auto-embedding COLLECTION_NOT_FOUND index should be staged for recovery when its source "
            + "collection reappears (same reappearance path regular search indexes use)",
        stagedIndex.isPresent());
    assertEquals(
        definitionGeneration.generation().nextAttempt(),
        stagedIndex.get().getGenerationId().generation);
  }

  private void stageIndexes(ConfigStateMocks mocks, Set<UUID> directMongodCollectionSet)
      throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults()
            .enable(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)
            .build();
    IndexRecoveryStager.stageRecoveryAttempts(
        mocks.configState, directMongodCollectionSet, featureFlags);
  }

  private ConfigStateMocks getEmptyMocks() throws Exception {
    return ConfigStateMocks.create();
  }
}
