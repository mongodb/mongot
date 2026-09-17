package com.xgen.mongot.config.manager;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.index.version.VectorliteFormatVersion;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Triple;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class IndexFormatVersionUpgraderTest {
  /** All IndexStatuses. */
  @DataPoints("allIndexStatuses")
  public static Set<IndexStatus> allIndexStatuses() {
    return Stream.of(IndexStatus.StatusCode.values())
        .map(IndexStatus::new)
        .collect(Collectors.toSet());
  }

  /** Active index states (staged or live). */
  @DataPoints("indexStates")
  public static Set<ConfigStateMocks.State> indexStates() {
    return Set.of(ConfigStateMocks.State.LIVE, ConfigStateMocks.State.STAGED);
  }

  /** All non-current index format versions (old to new). */
  @DataPoints("nonCurrentIndexFormatVersions")
  public static Set<IndexFormatVersion> nonCurrentIndexFormatVersions() {
    return Sets.difference(allIndexFormatVersions(), Set.of(IndexFormatVersion.CURRENT));
  }

  /** All index format versions, old to new. */
  @DataPoints("allIndexFormatVersions")
  public static Set<IndexFormatVersion> allIndexFormatVersions() {
    return IntStream.rangeClosed(
            IndexFormatVersion.MIN_SUPPORTED_VERSION.versionNumber - 1,
            IndexFormatVersion.CURRENT.versionNumber + 1)
        .boxed()
        .map(IndexFormatVersion::create)
        .collect(Collectors.toSet());
  }

  @DataPoints("allOutdatedIndexFormatVersions")
  public static Set<IndexFormatVersion> allOutdatedIndexFormatVersions() {
    return Set.of(IndexFormatVersion.MIN_SUPPORTED_VERSION);
  }

  /** Test that any current indexes in any state, are untouched. */
  @Theory
  public void testCurrentFormatVersionIndexesAreUntouched(
      @FromDataPoints("allIndexStatuses") IndexStatus status,
      @FromDataPoints("indexStates") ConfigStateMocks.State indexState)
      throws Exception {
    var mocks = getEmptyMocks();
    var indexId = new ObjectId();

    // If we're adding a STAGED index we have to add a corresponding LIVE one, with different
    // generation id.
    if (ConfigStateMocks.State.STAGED.equals(indexState)) {
      mocks.addIndex(
          mockIndexGeneration(indexId, 1).getDefinitionGeneration(), ConfigStateMocks.State.LIVE);
    }
    var index = mocks.addIndex(indexId, indexState);
    index.getIndex().setStatus(status);

    mocks.clearInvocations();
    IndexFormatVersionUpgrader.upgradeAndDrop(mocks.configState);

    mocks.assertNoIndexActivity();
  }

  /** Test that non-current indexes in any state result in a new generation being staged. */
  @Theory
  public void testOutdatedFormatVersionIndexesAreUpgraded(
      @FromDataPoints("allIndexStatuses") IndexStatus status,
      @FromDataPoints("indexStates") ConfigStateMocks.State indexState,
      @FromDataPoints("nonCurrentIndexFormatVersions") IndexFormatVersion indexFormatVersion)
      throws Exception {
    var mocks = getEmptyMocks();
    var indexId = new ObjectId();
    var generationId =
        new GenerationId(indexId, new Generation(UserIndexVersion.FIRST, indexFormatVersion));
    var definitionGeneration = mockDefinitionGeneration(generationId);

    // If we're adding a STAGED index we always have to add a corresponding LIVE one.
    @Var var index = mocks.addIndex(definitionGeneration, ConfigStateMocks.State.LIVE);
    if (ConfigStateMocks.State.STAGED.equals(indexState)) {
      // Our staged index here will be of the same index format version, with incremented attempt.
      index = mocks.addIndex(definitionGeneration.incrementAttempt(), indexState);
    }
    index.getIndex().setStatus(status);

    mocks.clearInvocations();
    IndexFormatVersionUpgrader.upgradeAndDrop(mocks.configState);

    mocks.assertNoIndexPhasedOut();
    mocks.assertJournalPersistedAtLeastOnce();

    if (indexFormatVersion.isSupported()) {
      // If the index format version we're upgrading from is supported, we should have a live index
      // and a staged index.
      var liveIndex = mocks.indexCatalog.getIndexById(indexId);
      var stagedIndex = mocks.staged.getIndex(indexId);

      Assert.assertTrue("live index should be present", liveIndex.isPresent());
      Assert.assertTrue("staged index should be present", stagedIndex.isPresent());

      mocks.assertLiveIndexesAre(liveIndex.get());
      mocks.assertStagedIndexesAre(stagedIndex.get());

      // This staged index should be the first generation of the current format version.
      assertEquals(
          Generation.CURRENT,
          mocks.staged.getIndex(indexId).orElseThrow().getDefinitionGeneration().generation());
    } else {
      // Otherwise, we should have one live index of a current format version and no staged index.
      var liveIndex = mocks.indexCatalog.getIndexById(indexId);
      var stagedIndex = mocks.staged.getIndex(indexId);

      Assert.assertTrue("live index should be present", liveIndex.isPresent());
      Assert.assertTrue("staged index should not be present", stagedIndex.isEmpty());

      mocks.assertLiveIndexesAre(liveIndex.get());
      mocks.assertStagedIndexesAre();
    }
  }

  /*
   * All "new" indexes are based off the index definition of the *staged* index.
   *
   *                                        Staged Format Version
   *                          |  Unsupported  |    Outdated   |     Current    |
   *             -------------+---------------+---------------+----------------|
   *             Unsupported  | L: New        | L: New        | L: Old Staged  |
   *                          | S: None       | S: None       | S: None        |
   *     Live    -------------+---------------+---------------+----------------|
   *   Format       Outdated  | L: No Change  | L: No Change  | L: No Change   |
   *  Version                 | S: New        | S: New        | S: No Change   |
   *             -------------+---------------+---------------+----------------|
   *                 Current  | L: No Change  | L: No Change  | L: No Change   |
   *                          | S: None       | S: None       | S: No Change   |
   *             --------------------------------------------------------------|
   */

  /** Tests that unsupported live indexes with an existing staged index are upgraded as expected. */
  @Theory
  public void testUnsupportedLiveIndexWithStagedIndex(
      @FromDataPoints("allIndexFormatVersions") IndexFormatVersion stagedFormatVersion)
      throws Exception {
    IndexFormatVersion liveFormatVersion =
        IndexFormatVersion.create(IndexFormatVersion.MIN_SUPPORTED_VERSION.versionNumber - 1);
    var setupTriple = setupUpgradeTest(liveFormatVersion, stagedFormatVersion);
    var mocks = setupTriple.getLeft();
    var liveIndex = setupTriple.getMiddle();
    var stagedIndex = setupTriple.getRight();
    var indexId = liveIndex.getDefinition().getIndexId();
    InitializedIndex initializedIndex =
        mocks.initializedIndexCatalog.getIndex(liveIndex.getGenerationId()).orElseThrow();
    InitializedIndex initializedStagedIndex =
        mocks.initializedIndexCatalog.getIndex(stagedIndex.getGenerationId()).orElseThrow();
    IndexFormatVersionUpgrader.upgradeAndDrop(mocks.configState);

    // All paths result in index modifications, and a dropped live index.
    mocks.assertJournalPersistedAtLeastOnce();
    verify(liveIndex.getIndex(), atLeastOnce()).drop();
    verify(initializedIndex).close();

    if (stagedFormatVersion.isCurrent()) {
      // If the staged index was current, we should have promoted it to live, and nothing else.
      mocks.assertLiveIndexesAre(stagedIndex);
      mocks.assertNoIndexCreated();
      mocks.assertStagedIndexesAre();
    } else {
      // Otherwise, we should have only a new, current-format-version live index with the same
      // index definition of the old live index.
      mocks.assertStagedIndexesAre();
      verify(stagedIndex.getIndex(), atLeastOnce()).drop();
      verify(initializedStagedIndex).close();

      assertCurrentLiveIndexCreatedFrom(mocks, indexId, stagedIndex.getDefinitionGeneration());
    }
  }

  /**
   * Tests that outdated but supported live indexes with an existing staged index are upgraded as
   * expected.
   */
  @Theory
  public void testOutdatedSupportedLiveIndexWithStagedIndex(
      @FromDataPoints("allOutdatedIndexFormatVersions")
          IndexFormatVersion outdatedIndexFormatVersion,
      @FromDataPoints("allIndexFormatVersions") IndexFormatVersion stagedFormatVersion)
      throws Exception {
    // We need to support past versions for this test to be useful.
    // We can't use assumptions here, because the Theories runner will complain if it can't find
    // any DataPoints that satisfy this method's assumptions. So, just return.
    if (IndexFormatVersion.MIN_SUPPORTED_VERSION.equals(IndexFormatVersion.CURRENT)) {
      return;
    }

    var setupTriple = setupUpgradeTest(outdatedIndexFormatVersion, stagedFormatVersion);
    var mocks = setupTriple.getLeft();
    var liveIndex = setupTriple.getMiddle();
    var stagedIndex = setupTriple.getRight();
    var indexId = liveIndex.getDefinition().getIndexId();
    InitializedIndex initializedStagedIndex =
        mocks.initializedIndexCatalog.getIndex(stagedIndex.getGenerationId()).orElseThrow();
    IndexFormatVersionUpgrader.upgradeAndDrop(mocks.configState);

    // If the staged index is current, then we shouldn't have made any modifications.
    // Otherwise, we should have dropped the existing staged index and staged an upgraded one based
    // off the current staged index.
    if (stagedFormatVersion.isCurrent()) {
      mocks.assertNoIndexActivity();
    } else {
      mocks.assertLiveIndexesAre(liveIndex);
      mocks.assertJournalPersistedAtLeastOnce();

      verify(stagedIndex.getIndex()).drop();
      verify(initializedStagedIndex).close();

      assertCurrentStagedIndexCreatedFrom(mocks, indexId, stagedIndex.getDefinitionGeneration());
    }
  }

  /**
   * Tests that current live indexes are untouched, while the staged index is dropped unless it's
   * also current.
   */
  @Theory
  public void testCurrentLiveIndexWithStagedIndex(
      @FromDataPoints("allIndexFormatVersions") IndexFormatVersion stagedFormatVersion)
      throws Exception {
    var setupTriple = setupUpgradeTest(IndexFormatVersion.CURRENT, stagedFormatVersion);
    var mocks = setupTriple.getLeft();
    var liveIndex = setupTriple.getMiddle();
    var stagedIndex = setupTriple.getRight();
    InitializedIndex initializedStagedIndex =
        mocks.initializedIndexCatalog.getIndex(stagedIndex.getGenerationId()).orElseThrow();
    IndexFormatVersionUpgrader.upgradeAndDrop(mocks.configState);

    // If the staged index is current, then we shouldn't have made any modifications.
    // Otherwise, we should have dropped the existing staged index.
    if (stagedFormatVersion.isCurrent()) {
      mocks.assertNoIndexActivity();
    } else {
      mocks.assertLiveIndexesAre(liveIndex);
      mocks.assertStagedIndexesAre();
      mocks.assertJournalPersistedAtLeastOnce();

      verify(stagedIndex.getIndex(), atLeastOnce()).drop();

      verify(initializedStagedIndex).close();
    }
  }

  /** Tests that all varieties of live indexes are correctly handled if no staged index exists. */
  @Theory
  public void testLiveIndexesHandledWhenStagedIndexAbsent(
      @FromDataPoints("allIndexFormatVersions") IndexFormatVersion liveFormatVersion)
      throws Exception {
    var mocks = getEmptyMocks();
    var indexId = new ObjectId();

    var liveGenerationId =
        new GenerationId(indexId, new Generation(UserIndexVersion.FIRST, liveFormatVersion));
    var liveDefinitionGeneration = mockDefinitionGeneration(liveGenerationId);
    var liveIndex = mocks.addIndex(liveDefinitionGeneration, ConfigStateMocks.State.LIVE);
    InitializedIndex initializedIndex =
        mocks.initializedIndexCatalog.getIndex(liveGenerationId).orElseThrow();
    mocks.clearInvocations();

    IndexFormatVersionUpgrader.upgradeAndDrop(mocks.configState);

    if (!liveFormatVersion.isSupported()) {
      // If the live index was unsupported, it should have been dropped, and a new, current version
      // added in its place, with the current live index definition.
      verify(liveIndex.getIndex()).drop();
      verify(initializedIndex).close();
      mocks.assertJournalPersistedAtLeastOnce();

      mocks.assertStagedIndexesAre();
      assertCurrentLiveIndexCreatedFrom(mocks, indexId, liveIndex.getDefinitionGeneration());
    } else {
      // If the live index was supported, it shouldn't have been touched.
      mocks.assertNoIndexPhasedOut();
      mocks.assertLiveIndexesAre(liveIndex);

      if (!liveFormatVersion.isCurrent()) {
        // If the live index wasn't current, we expect to have staged a new index.
        mocks.assertAtLeastOneIndexStaged();
        mocks.assertJournalPersistedAtLeastOnce();

        assertCurrentStagedIndexCreatedFrom(mocks, indexId, liveIndex.getDefinitionGeneration());
      } else {
        // Otherwise, we have a current live index, and shouldn't have made any changes.
        mocks.assertNoIndexActivity();
      }
    }
  }

  /** A vectorlite index whose generation predates the engine tag is rebuilt. */
  @Test
  public void testVectorliteLiveIndexOnLuceneGenerationIsReplaced() throws Exception {
    assertVectorliteLiveIndexReplaced(
        new Generation(UserIndexVersion.FIRST, IndexFormatVersion.CURRENT));
  }

  /** Rollback direction: a vectorlite version above current is rebuilt too. */
  @Test
  public void testVectorliteLiveIndexFromNewerReleaseIsReplaced() throws Exception {
    assertVectorliteLiveIndexReplaced(
        new Generation(
            UserIndexVersion.FIRST,
            new VectorliteFormatVersion(VectorliteFormatVersion.CURRENT.versionNumber() + 1)));
  }

  /** A non-current staged vectorlite generation is dropped; a current live one needs no staging. */
  @Test
  public void testNonCurrentStagedVectorliteIndexIsDropped() throws Exception {
    var mocks = vectorliteReadyMocks();
    var indexId = new ObjectId();
    var liveIndex =
        mocks.addIndex(
            vectorliteDefinitionGeneration(
                indexId, new Generation(UserIndexVersion.FIRST, VectorliteFormatVersion.CURRENT)),
            ConfigStateMocks.State.LIVE);
    var stagedIndex =
        mocks.addIndex(
            vectorliteDefinitionGeneration(
                indexId, new Generation(UserIndexVersion.FIRST, IndexFormatVersion.CURRENT)),
            ConfigStateMocks.State.STAGED);
    mocks.clearInvocations();

    IndexFormatVersionUpgrader.upgradeAndDrop(mocks.configState);

    verify(stagedIndex.getIndex()).drop();
    mocks.assertJournalPersistedAtLeastOnce();
    mocks.assertStagedIndexesAre();
    mocks.assertLiveIndexesAre(liveIndex);
    mocks.assertNoIndexCreated();
  }

  private void assertVectorliteLiveIndexReplaced(Generation liveGeneration) throws Exception {
    var mocks = vectorliteReadyMocks();
    var indexId = new ObjectId();
    var liveDefinitionGeneration = vectorliteDefinitionGeneration(indexId, liveGeneration);
    var liveIndex = mocks.addIndex(liveDefinitionGeneration, ConfigStateMocks.State.LIVE);
    InitializedIndex initializedIndex =
        mocks
            .initializedIndexCatalog
            .getIndex(liveDefinitionGeneration.getGenerationId())
            .orElseThrow();
    mocks.clearInvocations();

    IndexFormatVersionUpgrader.upgradeAndDrop(mocks.configState);

    verify(liveIndex.getIndex()).drop();
    verify(initializedIndex).close();
    mocks.assertJournalPersistedAtLeastOnce();
    mocks.assertStagedIndexesAre();
    verify(mocks.indexCatalog, atLeastOnce()).addIndex(any());
    mocks.assertIndexCreated(liveDefinitionGeneration.getIndexDefinition());
    assertEquals(
        new Generation(UserIndexVersion.FIRST, VectorliteFormatVersion.CURRENT),
        mocks
            .indexCatalog
            .getIndexById(indexId)
            .orElseThrow()
            .getDefinitionGeneration()
            .generation());
  }

  /** A vectorlite index at the vectorlite current version is left alone. */
  @Test
  public void testVectorliteLiveIndexOnVectorliteCurrentIsUntouched() throws Exception {
    var mocks = vectorliteReadyMocks();
    mocks.addIndex(
        vectorliteDefinitionGeneration(
            new ObjectId(),
            new Generation(UserIndexVersion.FIRST, VectorliteFormatVersion.CURRENT)),
        ConfigStateMocks.State.LIVE);
    mocks.clearInvocations();

    IndexFormatVersionUpgrader.upgradeAndDrop(mocks.configState);

    mocks.assertNoIndexActivity();
  }

  /**
   * A Lucene vector index stays on the Lucene line; the vectorlite version is not current for it.
   */
  @Test
  public void testLuceneVectorIndexIsNotMovedToVectorliteLine() throws Exception {
    var mocks = getEmptyMocks();
    mocks.addVectorIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    mocks.clearInvocations();

    IndexFormatVersionUpgrader.upgradeAndDrop(mocks.configState);

    mocks.assertNoIndexActivity();
  }

  /** The mock factory defers custom engine indexes until ready; report them ready. */
  private ConfigStateMocks vectorliteReadyMocks() throws Exception {
    var mocks = getEmptyMocks();
    when(mocks.indexFactory.isReadyToInitialize(any())).thenReturn(true);
    return mocks;
  }

  private static VectorIndexDefinitionGeneration vectorliteDefinitionGeneration(
      ObjectId indexId, Generation generation) {
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCustomEngineVectorField("vector", 3)
            .build();
    return new VectorIndexDefinitionGeneration(definition, generation);
  }

  private void assertCurrentStagedIndexCreatedFrom(
      ConfigStateMocks mocks, ObjectId indexId, IndexDefinitionGeneration baseDefinitionGeneration)
      throws Exception {
    mocks.assertAtLeastOneIndexStaged();
    mocks.assertIndexCreated(baseDefinitionGeneration.getIndexDefinition());
    assertEquals(
        IndexFormatVersion.CURRENT,
        mocks
            .staged
            .getIndex(indexId)
            .orElseThrow()
            .getDefinitionGeneration()
            .generation()
            .formatVersion
            .asLucene());
  }

  private void assertCurrentLiveIndexCreatedFrom(
      ConfigStateMocks mocks, ObjectId indexId, IndexDefinitionGeneration baseDefinitionGeneration)
      throws Exception {
    verify(mocks.indexCatalog, atLeastOnce()).addIndex(any());
    mocks.assertIndexCreated(baseDefinitionGeneration.getIndexDefinition());
    assertEquals(
        IndexFormatVersion.CURRENT,
        mocks
            .indexCatalog
            .getIndexById(indexId)
            .orElseThrow()
            .getDefinitionGeneration()
            .generation()
            .formatVersion
            .asLucene());
  }

  private Triple<ConfigStateMocks, IndexGeneration, IndexGeneration> setupUpgradeTest(
      IndexFormatVersion liveFormatVersion, IndexFormatVersion stagedFormatVersion)
      throws Exception {
    var mocks = getEmptyMocks();
    var indexId = new ObjectId();

    var liveGenerationId =
        new GenerationId(indexId, new Generation(UserIndexVersion.FIRST, liveFormatVersion));
    var liveDefinitionGeneration = mockDefinitionGeneration(liveGenerationId);

    var stagedGenerationId =
        new GenerationId(indexId, new Generation(UserIndexVersion.FIRST, stagedFormatVersion));
    var stagedDefinitionGeneration =
        liveGenerationId.equals(stagedGenerationId)
            ? liveDefinitionGeneration.incrementAttempt()
            : mockDefinitionGeneration(stagedGenerationId);

    var liveIndex = mocks.addIndex(liveDefinitionGeneration, ConfigStateMocks.State.LIVE);
    var stagedIndex = mocks.addIndex(stagedDefinitionGeneration, ConfigStateMocks.State.STAGED);

    mocks.clearInvocations();

    return Triple.of(mocks, liveIndex, stagedIndex);
  }

  private ConfigStateMocks getEmptyMocks() throws Exception {
    return ConfigStateMocks.create();
  }
}
