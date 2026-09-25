package com.xgen.mongot.config.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.index.version.Generation;
import com.xgen.testing.mongot.config.backup.ConfigJournalV1Builder;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.mock.index.IndexGeneration;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.Tags;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.InOrder;

@RunWith(Theories.class)
public class StagedIndexesSwapperTest {

  /** All index statuses. */
  @DataPoints("allStatuses")
  public static Set<IndexStatus> allStatuses() {
    return Stream.of(StatusCode.values()).map(IndexStatus::new).collect(Collectors.toSet());
  }

  /** All statuses that can service queries. */
  public static Set<StatusCode> serviceableStatusCodes() {
    return Stream.of(StatusCode.values())
        .map(IndexStatus::new)
        .filter(IndexStatus::canServiceQueries)
        .map(IndexStatus::getStatusCode)
        .collect(Collectors.toSet());
  }

  /** All statuses except STEADY and RECOVERING. */
  public static Set<StatusCode> nonServiceableStatusCodes() {
    return Stream.of(StatusCode.values())
        .map(IndexStatus::new)
        .filter(status -> !status.canServiceQueries())
        .map(IndexStatus::getStatusCode)
        .collect(Collectors.toSet());
  }

  /** All statuses except STEADY. */
  public static Set<StatusCode> nonSteadyStatusCodes() {
    return Stream.of(StatusCode.values())
        .filter(statusCode -> statusCode != StatusCode.STEADY)
        .collect(Collectors.toSet());
  }

  private static final Map<Pair<StatusCode, StatusCode>, Boolean> liveStagedStatusToShouldSwapMap =
      new HashMap<>();

  @BeforeClass
  public static void populateLiveStagedStatusResultMap() {
    Function<List<StatusCode>, Pair<StatusCode, StatusCode>> statusListToPair =
        l -> Pair.of(l.get(0), l.get(1));

    // If the live index is serviceable, we should only swap in a serviceable staged index.
    Sets.cartesianProduct(serviceableStatusCodes(), serviceableStatusCodes()).stream()
        .map(statusListToPair)
        .forEach(pair -> liveStagedStatusToShouldSwapMap.put(pair, true));
    Sets.cartesianProduct(serviceableStatusCodes(), nonServiceableStatusCodes()).stream()
        .map(statusListToPair)
        .forEach(pair -> liveStagedStatusToShouldSwapMap.put(pair, false));

    // If the live index is not serviceable, we should always swap.
    Sets.cartesianProduct(nonServiceableStatusCodes(), Set.of(StatusCode.values())).stream()
        .map(statusListToPair)
        .forEach(pair -> liveStagedStatusToShouldSwapMap.put(pair, true));

    // If the live index unknown, we should only swap in a serviceable staged index.
    Sets.cartesianProduct(Set.of(StatusCode.UNKNOWN), serviceableStatusCodes()).stream()
        .map(statusListToPair)
        .forEach(pair -> liveStagedStatusToShouldSwapMap.put(pair, true));
    Sets.cartesianProduct(Set.of(StatusCode.UNKNOWN), nonServiceableStatusCodes()).stream()
        .map(statusListToPair)
        .forEach(pair -> liveStagedStatusToShouldSwapMap.put(pair, false));
  }

  @Test
  public void testNoStagedDoesNothing() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    runSwaps(mocks);

    mocks.assertNoIndexActivity();
    verifyNoMoreInteractions(mocks.journalWriter);
  }

  @Test
  public void testLiveNotStartedStagedReadyPerformsSwapAndJournals() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    ObjectId id = new ObjectId();
    var current = mocks.addIndex(id, ConfigStateMocks.State.LIVE);
    var staged = mocks.stageIndex(id);
    current.getIndex().setStatus(IndexStatus.notStarted());
    staged.getIndex().setStatus(IndexStatus.steady());
    mocks.clearInvocations();

    runSwaps(mocks);
    mocks.assertLiveIndexesAre(staged);
    mocks.assertPhasingOutIndexesAre(current);
    verifyNoMoreInteractions(mocks.lifecycleManager);
    ConfigJournalV1 expectedJournal =
        ConfigJournalV1Builder.builder()
            .deletedIndex(current.getDefinitionGeneration())
            .liveIndex(staged.getDefinitionGeneration())
            .build();
    mocks.assertPersistedJournalEquals(expectedJournal);

    // make sure we wrote our journal before moving the indexes.
    InOrder inOrder = inOrder(mocks.journalWriter, mocks.phasingOut, mocks.indexCatalog);
    inOrder.verify(mocks.journalWriter).persist(argThat(expectedJournal::equals));
    inOrder.verify(mocks.phasingOut).addIndex(any());
    inOrder.verify(mocks.indexCatalog).addIndex(any());
  }

  @Test
  public void testStagedReadyPerformsSwapAndJournals() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    ObjectId id = new ObjectId();
    var current = mocks.addIndex(id, ConfigStateMocks.State.LIVE);
    var staged = mocks.stageIndex(id);
    staged.getIndex().setStatus(IndexStatus.steady());
    mocks.clearInvocations();

    runSwaps(mocks);
    mocks.assertLiveIndexesAre(staged);
    mocks.assertPhasingOutIndexesAre(current);
    verifyNoMoreInteractions(mocks.lifecycleManager);
    ConfigJournalV1 expectedJournal =
        ConfigJournalV1Builder.builder()
            .deletedIndex(current.getDefinitionGeneration())
            .liveIndex(staged.getDefinitionGeneration())
            .build();
    mocks.assertPersistedJournalEquals(expectedJournal);

    // make sure we wrote our journal before moving the indexes.
    InOrder inOrder = inOrder(mocks.journalWriter, mocks.phasingOut, mocks.indexCatalog);
    inOrder.verify(mocks.journalWriter).persist(argThat(expectedJournal::equals));
    inOrder.verify(mocks.phasingOut).addIndex(any());
    inOrder.verify(mocks.indexCatalog).addIndex(any());
  }

  @Test
  public void testLiveUnknownDoesNothing() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    ObjectId id = new ObjectId();
    var current = mocks.addIndex(id, ConfigStateMocks.State.LIVE);
    var staged = mocks.stageIndex(id);
    current.getIndex().setStatus(IndexStatus.unknown());
    staged.getIndex().setStatus(IndexStatus.notStarted());
    mocks.clearInvocations();

    runSwaps(mocks);
    mocks.assertLiveIndexesAre(current);
    mocks.assertStagedIndexesAre(staged);
    mocks.assertPhasingOutIndexesAre();
    verifyNoMoreInteractions(mocks.lifecycleManager);
    verifyNoMoreInteractions(mocks.journalWriter);
  }

  @Test
  public void testStagedReadyAttemptPerformsSwapAndJournals() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    ObjectId id = new ObjectId();
    var current = mocks.addIndex(id, ConfigStateMocks.State.LIVE);
    var staged = mocks.stageIndex(id);
    staged.getIndex().setStatus(IndexStatus.steady());
    mocks.clearInvocations();

    runSwaps(mocks);
    mocks.assertLiveIndexesAre(staged);
    mocks.assertPhasingOutIndexesAre(current);
    verifyNoMoreInteractions(mocks.lifecycleManager);
    ConfigJournalV1 expectedJournal =
        ConfigJournalV1Builder.builder()
            .deletedIndex(current.getDefinitionGeneration())
            .liveIndex(staged.getDefinitionGeneration())
            .build();
    mocks.assertPersistedJournalEquals(expectedJournal);

    // make sure we wrote our journal before moving the indexes.
    InOrder inOrder = inOrder(mocks.journalWriter, mocks.phasingOut, mocks.indexCatalog);
    inOrder.verify(mocks.journalWriter).persist(argThat(expectedJournal::equals));
    inOrder.verify(mocks.phasingOut).addIndex(any());
    inOrder.verify(mocks.indexCatalog).addIndex(any());
  }

  /**
   * Tests that all combinations of live and staged index statuses swap according to the behavior
   * specified in {@link #liveStagedStatusToShouldSwapMap}.
   */
  @Theory
  public void testAllLiveStagedCombinations(
      @FromDataPoints("allStatuses") IndexStatus liveStatus,
      @FromDataPoints("allStatuses") IndexStatus stagedStatus)
      throws Exception {
    var mocks = getEmptyMocks();
    var live = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    live.getIndex().setStatus(liveStatus);
    var staged = mocks.stageIndex(live.getDefinition().getIndexId());
    staged.getIndex().setStatus(stagedStatus);

    mocks.clearInvocations();
    runSwaps(mocks);

    Optional<Boolean> shouldSwap =
        Optional.ofNullable(
            liveStagedStatusToShouldSwapMap.get(
                Pair.of(liveStatus.getStatusCode(), stagedStatus.getStatusCode())));
    if (shouldSwap.isEmpty()) {
      fail("Unmapped (live, staged) index status pair");
    }

    if (shouldSwap.get()) {
      mocks.assertStagedIndexesAre();
      mocks.assertLiveIndexesAre(staged);
      mocks.assertPhasingOutIndexesAre(live);
    } else {
      Assert.assertEquals(0, mocks.phasingOut.getIndexes().size());
      mocks.assertStagedIndexesAre(staged);
      mocks.assertLiveIndexesAre(live);
      mocks.assertNoIndexActivity();
      verifyNoMoreInteractions(mocks.lifecycleManager);
    }
  }

  /**
   * Besides a swap between `stagedReady` and `liveGoingOut` we will have 3 other indexes, one in
   * each state, these should be un-changed and journaled correctly.
   */
  @Test
  public void testSwapDoesNotInterfereWithOtherStates() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();

    var phaseOut = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.PHASE_OUT);
    var liveGoingOut = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    // this index will replace liveGoingOut
    var stagedReady = mocks.stageIndex(liveGoingOut.getDefinition().getIndexId());
    stagedReady.getIndex().setStatus(IndexStatus.steady());

    // can't use addIndex because it breaks unique namespace invariant in the catalog
    var live =
        mocks.addIndex(
            SearchIndexDefinitionGenerationBuilder.create(
                SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build(),
                Generation.CURRENT,
                Collections.emptyList()),
            ConfigStateMocks.State.LIVE);
    var stagedNotReady = mocks.stageIndex(live.getDefinition().getIndexId());
    stagedNotReady.getIndex().setStatus(IndexStatus.initialSync());

    mocks.clearInvocations();

    runSwaps(mocks);
    // should perform one swap, but leave the rest in place
    verifyNoMoreInteractions(mocks.lifecycleManager);

    mocks.assertStagedIndexesAre(stagedNotReady);
    mocks.assertLiveIndexesAre(live, stagedReady);
    mocks.assertPhasingOutIndexesAre(phaseOut, liveGoingOut);

    mocks.assertPersistedJournalEquals(
        ConfigJournalV1Builder.builder()
            .stagedIndex(stagedNotReady.getDefinitionGeneration())
            .liveIndex(live.getDefinitionGeneration())
            .liveIndex(stagedReady.getDefinitionGeneration())
            .deletedIndex(phaseOut.getDefinitionGeneration())
            .deletedIndex(liveGoingOut.getDefinitionGeneration())
            .build());
  }

  @Test
  public void testSwapWhereNoLiveIndexIsPresentWorks() throws Exception {
    // ConfigState can't be in this state to begin with, however, the swapper shouldn't care about
    // this invariant.
    ConfigStateMocks mocks = getEmptyMocks();

    var stagedReady = IndexGeneration.mockIndexGeneration();
    stagedReady.getIndex().setStatus(IndexStatus.steady());
    mocks.staged.addIndex(stagedReady);
    mocks.clearInvocations();

    runSwaps(mocks);
    // should perform one swap, but leave the rest in place
    verifyNoMoreInteractions(mocks.lifecycleManager);
    mocks.assertStagedIndexesAre();
    mocks.assertLiveIndexesAre(stagedReady);
    mocks.assertPhasingOutIndexesAre();
    mocks.assertPersistedJournalEquals(
        ConfigJournalV1Builder.builder().liveIndex(stagedReady.getDefinitionGeneration()).build());
  }

  @Test
  public void testCollectionNotFoundStatusSwap() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    ObjectId id = new ObjectId();
    var current = mocks.addIndex(id, ConfigStateMocks.State.LIVE);
    var staged = mocks.stageIndex(id);

    // Set both live and staged indexes to DOES_NOT_EXIST with reason COLLECTION_NOT_FOUND
    current.getIndex().setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    staged.getIndex().setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    mocks.clearInvocations();

    runSwaps(mocks);

    // Verify that the swap occurred
    mocks.assertLiveIndexesAre(staged);
    mocks.assertPhasingOutIndexesAre(current);

    ConfigJournalV1 expectedJournal =
        ConfigJournalV1Builder.builder()
            .deletedIndex(current.getDefinitionGeneration())
            .liveIndex(staged.getDefinitionGeneration())
            .build();
    mocks.assertPersistedJournalEquals(expectedJournal);

    InOrder inOrder = inOrder(mocks.journalWriter, mocks.phasingOut, mocks.indexCatalog);
    inOrder.verify(mocks.journalWriter).persist(argThat(expectedJournal::equals));
    inOrder.verify(mocks.phasingOut).addIndex(any());
    inOrder.verify(mocks.indexCatalog).addIndex(any());

    assertEquals(
        1,
        mocks
            .metricsFactory
            .counter("swappedInIndexes", Tags.of("indexStatus", "DOES_NOT_EXIST"))
            .count(),
        0.0);

    assertEquals(
        1,
        mocks
            .metricsFactory
            .counter("swappedOutIndexes", Tags.of("indexStatus", "DOES_NOT_EXIST"))
            .count(),
        0.0);
  }

  @Test
  public void swapReady_autoEmbeddingCollectionNotFoundReplacesFailedLiveGeneration()
      throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    var definitionGeneration =
        IndexGeneration.mockDefinitionGeneration(
            VectorIndex.MOCK_MATERIALIZED_VIEW_AUTO_EMBEDDING_INDEX_DEFINITION);
    var indexActions = IndexActions.withReplication(mocks.configState);

    doThrow(new MaterializedViewTransientException("mongod unavailable"))
        .when(mocks.configState.materializedViewIndexFactory.get())
        .getIndex(any());
    indexActions.addNewIndexes(List.of(definitionGeneration));
    var failedLiveIndex = mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).orElseThrow();

    reset(mocks.configState.materializedViewIndexFactory.get());
    when(mocks
            .configState
            .materializedViewIndexFactory
            .get()
            .isSourceCollectionAbsentOnMongod(any()))
        .thenReturn(true);
    indexActions.addStagedIndex(definitionGeneration.incrementAttempt());
    var collectionNotFoundStagedIndex =
        mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).orElseThrow();
    mocks.clearInvocations();

    runSwaps(mocks);

    mocks.assertLiveIndexesAre(collectionNotFoundStagedIndex);
    mocks.assertPhasingOutIndexesAre(failedLiveIndex);
  }

  private void runSwaps(ConfigStateMocks mocks) throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults()
            .enable(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)
            .enable(Feature.INDEX_FEATURE_VERSION_FOUR)
            .build();
    StagedIndexesSwapper.swapReady(mocks.configState, featureFlags, mocks.metricsFactory);
  }

  private ConfigStateMocks getEmptyMocks() throws Exception {
    return ConfigStateMocks.create();
  }
}
