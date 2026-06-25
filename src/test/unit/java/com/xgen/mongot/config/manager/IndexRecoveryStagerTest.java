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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Var;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
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
  public void testRecoveringIndexStagedWhenNoOtherStaged() throws Exception {
    var mocks = getEmptyMocks();
    var index = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    index.getIndex().setStatus(IndexStatus.recoveringNonTransient(new BsonTimestamp()));
    mocks.clearInvocations();

    stageIndexes(mocks, Set.of());

    mocks.assertOneIndexCreated();
    mocks.assertAtLeastOneIndexStagedAndReplicated();
    mocks.assertJournalPersistedAtLeastOnce();

    var stagedIndex = mocks.staged.getIndex(index.getDefinition().getIndexId());
    assertTrue(stagedIndex.isPresent());
    assertEquals(
        index.getDefinitionGeneration().generation().nextAttempt(),
        stagedIndex.get().getGenerationId().generation);
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

    // updateSyncSource resets unresolved indexes to AUTO_EMBEDDING_RESOLUTION_RETRY (recoverable)
    mocks.configState.updateSyncSource(ConfigStateMocks.MOCK_SYNC_SOURCE_CONFIG);

    assertEquals(
        IndexStatus.StatusCode.FAILED, unresolvedIndex.getIndex().getStatus().getStatusCode());
    assertThat(unresolvedIndex.getIndex().getStatus().getReason())
        .isEqualTo(Optional.of(AUTO_EMBEDDING_RESOLUTION_RETRY));

    assertTrue(
        "Index should be recoverable after updateSyncSource",
        unresolvedIndex.getIndex().getStatus().canBeRecovered());

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
  public void unresolvedAutoEmbeddingIndex_withUnknownReason_isRecoverableImmediately()
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
