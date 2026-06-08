package com.xgen.mongot.config.manager;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexGenerationFactory;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class IndexInitialSyncRecoveryTest {
  /** Counter to ensure each index has a unique name. */
  private final AtomicInteger indexCounter = new AtomicInteger();
  /** Mocks for the config state and its dependencies. */
  private ConfigStateMocks mocks;

  @Before
  public void setUp() throws Exception {
    this.mocks = ConfigStateMocks.create();
  }

  private IndexActions indexActions() {
    return IndexActions.withReplication(this.mocks.configState);
  }

  @Test
  public void getRetryableInitialSyncIndexes_returnsOnlyFailedRetryIndexes() throws Exception {
    // Only FAILED with INITIAL_SYNC_REPLICATION_FAILED_RETRY is retryable.
    var retryableIndex = failedIndex("initial sync failed retryable",
        IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY);

    // FAILED with other reasons should NOT be retryable.
    failedIndex("initial sync failed",
        IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED);
    failedIndex("init failed", IndexStatus.Reason.INITIALIZATION_FAILED);
    var failedNoReason = addUniqueNamedIndex(ConfigStateMocks.State.LIVE);
    failedNoReason.getIndex().setStatus(IndexStatus.failed("generic failure"));

    // Non-FAILED indexes should NOT be retryable.
    var steadyIndex = addUniqueNamedIndex(ConfigStateMocks.State.LIVE);
    steadyIndex.getIndex().setStatus(IndexStatus.steady());
    var initialSyncIndex = addUniqueNamedIndex(ConfigStateMocks.State.LIVE);
    initialSyncIndex.getIndex().setStatus(IndexStatus.initialSync());

    // Phased out indexes are not in the catalog, so not retryable.
    var phasedOutIndex = addUniqueNamedIndex(ConfigStateMocks.State.PHASE_OUT);
    phasedOutIndex.getIndex().setStatus(IndexStatus.failed("phased out failed",
        IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY));

    List<IndexGeneration> result =
        IndexInitialSyncRecovery.getRetryableInitialSyncIndexes(this.mocks.configState);
    assertThat(result).containsExactly(retryableIndex);
  }

  @Test
  public void getRetryableInitialSyncIndexes_multipleRetryableIndexes_returnsAll()
      throws Exception {
    var retryable1 = failedIndex("fail 1",
        IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY);
    var retryable2 = failedIndex("fail 2",
        IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY);

    // Non-retryable failed index.
    failedIndex("init failed", IndexStatus.Reason.INITIALIZATION_FAILED);

    List<IndexGeneration> result =
        IndexInitialSyncRecovery.getRetryableInitialSyncIndexes(this.mocks.configState);
    assertThat(result).containsExactly(retryable1, retryable2);
  }

  @Test
  public void getRetryableInitialSyncIndexes_noRetryableIndexes_returnsEmpty() throws Exception {
    var healthyIndex = addUniqueNamedIndex(ConfigStateMocks.State.LIVE);
    healthyIndex.getIndex().setStatus(IndexStatus.initialSync());

    failedIndex("user error", IndexStatus.Reason.USER_ERROR);

    List<IndexGeneration> result =
        IndexInitialSyncRecovery.getRetryableInitialSyncIndexes(this.mocks.configState);
    assertThat(result).isEmpty();
  }

  @Test
  public void getRetryableInitialSyncIndexes_emptyIndexCatalog_returnsEmpty() throws Exception {
    List<IndexGeneration> result =
        IndexInitialSyncRecovery.getRetryableInitialSyncIndexes(this.mocks.configState);
    assertThat(result).isEmpty();
  }

  @Test
  public void getRetryableInitialSyncIndexes_excludesNonFailedStagedReplacement()
      throws Exception {
    var failedIndex = failedInitialSyncRetryIndex();
    var indexId = failedIndex.getDefinition().getIndexId();

    // The live index has a retryable status, but a non-failed staged replacement already
    // exists (active sync in progress), so the live index should NOT be retryable.
    this.mocks.stageIndex(indexId);

    List<IndexGeneration> result =
        IndexInitialSyncRecovery.getRetryableInitialSyncIndexes(this.mocks.configState);
    assertThat(result).isEmpty();
  }

  @Test
  public void getRetryableInitialSyncIndexes_includesFailedStagedReplacement() throws Exception {
    var failedIndex = failedInitialSyncRetryIndex();
    var indexId = failedIndex.getDefinition().getIndexId();

    // Stage a replacement and mark it as a retryable failure too.
    var staged = this.mocks.stageIndex(indexId);
    staged.getIndex().setStatus(IndexStatus.failed("staged also failed",
        IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY));

    List<IndexGeneration> result =
        IndexInitialSyncRecovery.getRetryableInitialSyncIndexes(this.mocks.configState);
    assertThat(result).containsExactly(failedIndex);
  }

  @Test
  public void retryFailedIndexes_singleRetryableIndex_addedAsStaged() throws Exception {
    var failedIndex = failedInitialSyncRetryIndex();
    var indexId = failedIndex.getDefinition().getIndexId();
    this.mocks.clearInvocations();

    IndexInitialSyncRecovery.retryFailedInitialSyncIndexes(this.mocks.configState, indexActions());

    // The live FAILED index stays in the catalog (not phased out).
    assertThat(this.mocks.indexCatalog.getIndexById(indexId)).isPresent();
    assertThat(this.mocks.indexCatalog.getIndexById(indexId).get()).isEqualTo(failedIndex);
    this.mocks.assertPhasingOutIndexesAre();

    // A new attempt should be staged.
    var stagedIndex = this.mocks.configState.staged.getIndex(indexId).orElseThrow();
    assertThat(stagedIndex.getDefinitionGeneration())
        .isEqualTo(failedIndex.getDefinitionGeneration().incrementAttempt());
    this.mocks.assertJournalPersistedAtLeastOnce();
  }

  @Test
  public void retryFailedIndexes_withFailedStaged_incrementsFromStagedGeneration()
      throws Exception {
    var failedLive = failedInitialSyncRetryIndex();
    var indexId = failedLive.getDefinition().getIndexId();

    // Stage a replacement (with a newer user version), also a retryable failure.
    var staged = this.mocks.stageIndex(indexId);
    staged.getIndex().setStatus(IndexStatus.failed("staged failed",
        IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY));
    this.mocks.clearInvocations();

    IndexInitialSyncRecovery.retryFailedInitialSyncIndexes(this.mocks.configState, indexActions());

    // The live FAILED index stays in the catalog.
    assertThat(this.mocks.indexCatalog.getIndexById(indexId).get()).isEqualTo(failedLive);

    // The new staged attempt should increment from the staged generation, not the live generation.
    var newStaged = this.mocks.configState.staged.getIndex(indexId).orElseThrow();
    assertThat(newStaged.getDefinitionGeneration())
        .isEqualTo(staged.getDefinitionGeneration().incrementAttempt());
    assertThat(newStaged.getDefinitionGeneration())
        .isNotEqualTo(failedLive.getDefinitionGeneration().incrementAttempt());

    // The old failed staged index should have been moved to dropped in the journal.
    ArgumentCaptor<ConfigJournalV1> captor = ArgumentCaptor.forClass(ConfigJournalV1.class);
    verify(this.mocks.configState, times(1)).persist(captor.capture());
    assertThat(captor.getValue().getDeletedIndexes()).contains(staged.getDefinitionGeneration());
  }

  @Test
  public void retryFailedIndexes_multipleRetryableIndexes_allStagedWithIncrementedAttempts()
      throws Exception {
    var failed1 = failedInitialSyncRetryIndex();
    var failed2 = failedInitialSyncRetryIndex();
    var indexId1 = failed1.getDefinition().getIndexId();
    var indexId2 = failed2.getDefinition().getIndexId();
    this.mocks.clearInvocations();

    IndexInitialSyncRecovery.retryFailedInitialSyncIndexes(this.mocks.configState, indexActions());

    // Live indexes stay, nothing phased out.
    this.mocks.assertPhasingOutIndexesAre();

    // Both new attempts should be staged.
    var staged1 = this.mocks.configState.staged.getIndex(indexId1).orElseThrow();
    var staged2 = this.mocks.configState.staged.getIndex(indexId2).orElseThrow();
    assertThat(staged1.getDefinitionGeneration())
        .isEqualTo(failed1.getDefinitionGeneration().incrementAttempt());
    assertThat(staged2.getDefinitionGeneration())
        .isEqualTo(failed2.getDefinitionGeneration().incrementAttempt());
  }

  @Test
  public void retryFailedIndexes_multipleIndexes_journaledAtomically() throws Exception {
    var failed1 = failedInitialSyncRetryIndex();
    var failed2 = failedInitialSyncRetryIndex();
    this.mocks.clearInvocations();

    IndexInitialSyncRecovery.retryFailedInitialSyncIndexes(this.mocks.configState, indexActions());

    // All staged additions must be journaled in a single persist.
    ArgumentCaptor<ConfigJournalV1> captor = ArgumentCaptor.forClass(ConfigJournalV1.class);
    verify(this.mocks.configState, times(1)).persist(captor.capture());

    ConfigJournalV1 journal = captor.getValue();
    List<IndexDefinitionGeneration> expectedAttempts = List.of(
        failed1.getDefinitionGeneration().incrementAttempt(),
        failed2.getDefinitionGeneration().incrementAttempt());
    assertThat(journal.getStagedIndexes()).containsAtLeastElementsIn(expectedAttempts);
    // Live indexes should still be in the journal as live.
    assertThat(journal.getLiveIndexes()).containsAtLeast(
        failed1.getDefinitionGeneration(), failed2.getDefinitionGeneration());
  }

  @Test
  public void retryFailedInitialSyncIndexes_mixOfHealthyAndFailed_onlyFailedRetried()
      throws Exception {
    var failedIndex = failedInitialSyncRetryIndex();
    var healthyIndex = addUniqueNamedIndex(ConfigStateMocks.State.LIVE);
    healthyIndex.getIndex().setStatus(IndexStatus.initialSync());
    var indexId = failedIndex.getDefinition().getIndexId();
    this.mocks.clearInvocations();

    IndexInitialSyncRecovery.retryFailedInitialSyncIndexes(this.mocks.configState, indexActions());

    // Nothing is phased out; the failed live index stays.
    this.mocks.assertPhasingOutIndexesAre();

    // The retry is staged.
    var stagedIndex = this.mocks.configState.staged.getIndex(indexId).orElseThrow();
    assertThat(stagedIndex.getDefinitionGeneration())
        .isEqualTo(failedIndex.getDefinitionGeneration().incrementAttempt());

    // The healthy index is untouched.
    assertThat(this.mocks.indexCatalog.getIndexById(healthyIndex.getDefinition().getIndexId()))
        .isPresent();
  }

  @Test
  public void getRetryableInitialSyncIndexes_excludesMaterializedViewIndexes() throws Exception {
    // A materialized view index with retryable status should NOT be considered retryable,
    // because MaterializedViewGeneration.nextAttempt() is a no-op and would produce a
    // duplicate GenerationId during retryFailedInitialSyncIndexes.
    var matViewDefGen = MaterializedViewIndex.mockMatViewDefinitionGeneration(new ObjectId());
    var matViewIndexGen =
        IndexGenerationFactory.getIndexGeneration(this.mocks.indexFactory, matViewDefGen);
    this.mocks.indexCatalog.addIndex(matViewIndexGen);
    matViewIndexGen.getIndex().setStatus(IndexStatus.failed("initial sync failed",
        IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY));

    // A regular search index with retryable status SHOULD be returned.
    var retryableSearchIndex = failedIndex("initial sync failed retryable",
        IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY);

    List<IndexGeneration> result =
        IndexInitialSyncRecovery.getRetryableInitialSyncIndexes(this.mocks.configState);
    // Only the search index should be retryable, not the materialized view.
    assertThat(result).containsExactly(retryableSearchIndex);
  }

  @Test
  public void retryFailedIndexes_failedStagedWithHealthyLive_dropsWithoutRetry() throws Exception {
    // Set up a healthy (STEADY) live index.
    var liveIndex = addUniqueNamedIndex(ConfigStateMocks.State.LIVE);
    liveIndex.getIndex().setStatus(IndexStatus.steady());
    var indexId = liveIndex.getDefinition().getIndexId();

    // Stage a replacement that failed with a retryable reason.
    var staged = this.mocks.stageIndex(indexId);
    staged.getIndex().setStatus(IndexStatus.failed("staged initial sync failed",
        IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY));
    this.mocks.clearInvocations();

    IndexInitialSyncRecovery.retryFailedInitialSyncIndexes(this.mocks.configState, indexActions());

    // The live index should remain healthy and untouched.
    assertThat(this.mocks.indexCatalog.getIndexById(indexId)).isPresent();
    assertThat(this.mocks.indexCatalog.getIndexById(indexId).get()
        .getIndex().getStatus().getStatusCode()).isEqualTo(IndexStatus.StatusCode.STEADY);

    // The failed staged index should have been dropped with no retry.
    assertThat(this.mocks.configState.staged.getIndex(indexId)).isEmpty();

    // Nothing should be phased out.
    this.mocks.assertPhasingOutIndexesAre();

    // The drop should have been journaled with no new staged entries.
    ArgumentCaptor<ConfigJournalV1> captor = ArgumentCaptor.forClass(ConfigJournalV1.class);
    verify(this.mocks.configState, times(1)).persist(captor.capture());
    assertThat(captor.getValue().getDeletedIndexes()).contains(staged.getDefinitionGeneration());
    assertThat(captor.getValue().getStagedIndexes())
        .doesNotContain(staged.getDefinitionGeneration().incrementAttempt());
  }

  @Test
  public void retryFailedIndexes_failedStagedWithNonQueryableLive_retriesNormally()
      throws Exception {
    // Set up a live index that is FAILED with retryable reason (not queryable).
    var liveIndex = failedInitialSyncRetryIndex();
    var indexId = liveIndex.getDefinition().getIndexId();

    // Stage a replacement that also failed with a retryable reason.
    var staged = this.mocks.stageIndex(indexId);
    staged.getIndex().setStatus(IndexStatus.failed("staged also failed",
        IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY));
    this.mocks.clearInvocations();

    IndexInitialSyncRecovery.retryFailedInitialSyncIndexes(this.mocks.configState, indexActions());

    // The live index stays in the catalog as FAILED.
    assertThat(this.mocks.indexCatalog.getIndexById(indexId)).isPresent();

    // A new staged retry should exist (incremented from the old staged).
    var newStaged = this.mocks.configState.staged.getIndex(indexId).orElseThrow();
    assertThat(newStaged.getDefinitionGeneration())
        .isEqualTo(staged.getDefinitionGeneration().incrementAttempt());
  }

  @Test
  public void retryFailedIndexes_nonRetryableStagedWithHealthyLive_notDropped() throws Exception {
    // Set up a healthy (STEADY) live index.
    var liveIndex = addUniqueNamedIndex(ConfigStateMocks.State.LIVE);
    liveIndex.getIndex().setStatus(IndexStatus.steady());
    var indexId = liveIndex.getDefinition().getIndexId();

    // Stage a replacement that failed with a NON-retryable reason.
    var staged = this.mocks.stageIndex(indexId);
    staged.getIndex().setStatus(IndexStatus.failed("init failed",
        IndexStatus.Reason.INITIALIZATION_FAILED));
    this.mocks.clearInvocations();

    IndexInitialSyncRecovery.retryFailedInitialSyncIndexes(this.mocks.configState, indexActions());

    // The staged index should NOT have been dropped because it is not an
    // INITIAL_SYNC_REPLICATION_FAILED_RETRY failure.
    assertThat(this.mocks.configState.staged.getIndex(indexId)).isPresent();
    assertThat(this.mocks.configState.staged.getIndex(indexId).get()).isEqualTo(staged);
  }

  /** Created an index with retryable initial sync failure reason. */
  private IndexGeneration failedInitialSyncRetryIndex() throws Exception {
    return failedIndex("initial sync failed",
        IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY);
  }

  /** Adds a live index with a unique name to avoid duplicate-name invariant violations. */
  private IndexGeneration failedIndex(String message, IndexStatus.Reason reason)
      throws Exception {
    var index = addUniqueNamedIndex(ConfigStateMocks.State.LIVE);
    index.getIndex().setStatus(IndexStatus.failed(message, reason));
    return index;
  }

  /** Adds a live index with a unique name to avoid duplicate-name invariant violations. */
  private IndexGeneration addUniqueNamedIndex(ConfigStateMocks.State state) throws Exception {
    var indexId = new ObjectId();
    SearchIndexDefinition definition = SearchIndexDefinitionBuilder.builder()
        .indexId(indexId)
        .name("index_" + this.indexCounter.getAndIncrement())
        .database(SearchIndex.MOCK_INDEX_DATABASE_NAME)
        .lastObservedCollectionName(SearchIndex.MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
        .collectionUuid(SearchIndex.MOCK_INDEX_COLLECTION_UUID)
        .mappings(SearchIndex.MOCK_INDEX_MAPPINGS)
        .synonyms(SearchIndex.MOCK_SYNONYM_MAPPING_DEFINITIONS)
        .build();
    var defGen =
        com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration(definition);
    return this.mocks.addIndex(defGen, state);
  }
}
