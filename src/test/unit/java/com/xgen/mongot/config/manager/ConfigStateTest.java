package com.xgen.mongot.config.manager;

import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.metrics.ServerStatusDataExtractor;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import com.xgen.mongot.util.mongodb.ConnectionStringUtil;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.testing.mongot.config.backup.ConfigJournalV1Builder;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import com.xgen.testing.mongot.mock.index.IndexGeneration;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class ConfigStateTest {

  @DataPoints("indexDefinitionGenerations")
  public static List<IndexDefinitionGeneration> indexDefinitionGenerations() {
    return List.of(
        SearchIndex.MOCK_INDEX_DEFINITION_GENERATION, VectorIndex.MOCK_INDEX_DEFINITION_GENERATION);
  }

  @DataPoints("indexGenerations")
  public static List<com.xgen.mongot.index.IndexGeneration> indexGenerations() {
    return List.of(
        IndexGeneration.mockIndexGeneration(), IndexGeneration.mockVectorIndexGeneration());
  }

  @Test
  public void testUpdateSyncSource() throws Exception {
    var mocks = ConfigStateMocks.create();
    SyncSourceConfig newSyncSourceConfig =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfo("mongodb://newString"))
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfo("mongodb://newString"))
            .mongodClusterReadWriteUri(ConnectionStringUtil.toConnectionInfo("mongodb://newString"))
            .build();
    mocks.configState.updateSyncSource(newSyncSourceConfig);
    Assert.assertTrue(
        mocks
            .configState
            .getLifecycleManager()
            .getSyncSourceConfig()
            .get()
            .equals(newSyncSourceConfig));
    Assert.assertTrue(mocks.configState.getLifecycleManager().isReplicationSupported());

    mocks.replicationGate.close();
    mocks.configState.updateSyncSource(newSyncSourceConfig);
    Assert.assertTrue(
        mocks
            .configState
            .getLifecycleManager()
            .getReplicationManager()
            .getSyncSourceConfig()
            .get()
            .equals(newSyncSourceConfig));
    Assert.assertTrue(
        mocks
            .configState
            .getLifecycleManager()
            .getSyncSourceConfig()
            .get()
            .equals(newSyncSourceConfig));
    Assert.assertFalse(mocks.configState.getLifecycleManager().isReplicationSupported());
  }

  @Test
  public void testUpdateSyncSourceResetsTransientUnresolvedButKeepsCollectionNotFound()
      throws Exception {
    var mocks = ConfigStateMocks.create();

    // Transient resolution failure: should be reset to AUTO_EMBEDDING_RESOLUTION_RETRY so
    // IndexRecoveryStager retries it.
    UnresolvedAutoEmbeddingIndexGeneration transientGen =
        UnresolvedAutoEmbeddingIndexGeneration.createFailed(
            autoEmbedVectorGen(),
            MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE);
    // Source collection absent on this shard: must stay a quiet COLLECTION_NOT_FOUND.
    UnresolvedAutoEmbeddingIndexGeneration collectionNotFoundGen =
        UnresolvedAutoEmbeddingIndexGeneration.createCollectionNotFound(autoEmbedVectorGen());
    mocks.indexCatalog.addIndex(transientGen);
    mocks.staged.addIndex(collectionNotFoundGen);

    SyncSourceConfig newSyncSourceConfig =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfo("mongodb://newString"))
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfo("mongodb://newString"))
            .mongodClusterReadWriteUri(ConnectionStringUtil.toConnectionInfo("mongodb://newString"))
            .build();
    mocks.configState.updateSyncSource(newSyncSourceConfig);

    // The transient failure is reset so the recovery stager retries it.
    Assert.assertEquals(
        IndexStatus.StatusCode.FAILED, transientGen.getIndex().getStatus().getStatusCode());
    Assert.assertEquals(
        Optional.of(IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY),
        transientGen.getIndex().getStatus().getReason());
    // The collection-not-found generation is left untouched (no spurious failure overwrite).
    Assert.assertEquals(
        IndexStatus.StatusCode.DOES_NOT_EXIST,
        collectionNotFoundGen.getIndex().getStatus().getStatusCode());
    Assert.assertEquals(
        Optional.of(IndexStatus.Reason.COLLECTION_NOT_FOUND),
        collectionNotFoundGen.getIndex().getStatus().getReason());
  }

  private static VectorIndexDefinitionGeneration autoEmbedVectorGen() {
    VectorIndexDefinition def =
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("mock")
            .database("mock")
            .lastObservedCollectionName("mock")
            .collectionUuid(UUID.randomUUID())
            .withAutoEmbedField("title")
            .build();
    return new VectorIndexDefinitionGeneration(def, Generation.CURRENT);
  }

  @Test
  public void testUpdateMongosUri() throws Exception {
    var mocks = ConfigStateMocks.create();

    // only mongosUri changes
    SyncSourceConfig syncSourceConfigWithMongos =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfo(ConfigStateMocks.DEFAULT_MDB_URI))
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfo(ConfigStateMocks.DEFAULT_MDB_URI))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfo(ConfigStateMocks.DEFAULT_MDB_URI))
            .mongosSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfo("mongodb://mongos"))
            .mongosClusterReadWriteUri(ConnectionStringUtil.toConnectionInfo("mongodb://mongos"))
            .isSharded(true)
            .build();
    mocks.configState.updateSyncSource(syncSourceConfigWithMongos);
    Assert.assertTrue(
        mocks
            .configState
            .getLifecycleManager()
            .getSyncSourceConfig()
            .get()
            .equals(syncSourceConfigWithMongos));
    Assert.assertTrue(
        mocks
            .configState
            .getLifecycleManager()
            .getSyncSourceConfig()
            .get()
            .equals(syncSourceConfigWithMongos));
  }

  @Test
  public void testUpdateMongoDbClusterUri() throws Exception {
    var mocks = ConfigStateMocks.create();

    // only mongoDbClusterUri changes
    SyncSourceConfig syncSourceConfigWithCulsterSeed =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfo(ConfigStateMocks.DEFAULT_MDB_URI))
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfo("mongodb://cluster-seed"))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfo("mongodb://cluster-seed"))
            .build();
    mocks.configState.updateSyncSource(syncSourceConfigWithCulsterSeed);
    Assert.assertTrue(
        mocks
            .configState
            .getLifecycleManager()
            .getSyncSourceConfig()
            .get()
            .equals(syncSourceConfigWithCulsterSeed));
    Assert.assertTrue(
        mocks
            .configState
            .getLifecycleManager()
            .getSyncSourceConfig()
            .get()
            .equals(syncSourceConfigWithCulsterSeed));
  }

  @Test
  public void testInitRegistersMetricGauges() throws Exception {
    ConfigStateMocks mocks = ConfigStateMocks.create();

    // there will be a gauge per IndexFormatVersion
    var nameToGauge =
        mocks.meterRegistry.getMeters().stream()
            .filter(meter -> meter.getId().getType() == Meter.Type.GAUGE)
            .map(meter -> (Gauge) meter)
            .collect(
                Collectors.toMap(
                    gauge -> gauge.getId().getName(),
                    Set::of,
                    (prevSet, newSet) ->
                        Stream.concat(prevSet.stream(), newSet.stream())
                            .collect(Collectors.toSet())));

    Assert.assertTrue(nameToGauge.containsKey(getFullMetricName("stagedIndexes")));
    Assert.assertTrue(nameToGauge.containsKey(getFullMetricName("indexesInCatalog")));
    Assert.assertTrue(nameToGauge.containsKey(getFullMetricName("indexesPhasingOut")));

    nameToGauge
        .get(getFullMetricName("stagedIndexes"))
        .forEach(gauge -> Assert.assertEquals(0, gauge.value(), 0.0));
    nameToGauge
        .get(getFullMetricName("indexesInCatalog"))
        .forEach(gauge -> Assert.assertEquals(0, gauge.value(), 0.0));
    nameToGauge
        .get(getFullMetricName("indexesPhasingOut"))
        .forEach(gauge -> Assert.assertEquals(0, gauge.value(), 0.0));

    Assert.assertTrue(
        nameToGauge.containsKey(getFullMetricName("stagedIndexesFeatureVersionFour")));
    Assert.assertTrue(
        nameToGauge.containsKey(getFullMetricName("indexesInCatalogFeatureVersionFour")));
    Assert.assertTrue(
        nameToGauge.containsKey(getFullMetricName("indexesPhasingOutFeatureVersionFour")));
    nameToGauge
        .get(getFullMetricName("stagedIndexesFeatureVersionFour"))
        .forEach(gauge -> Assert.assertEquals(0, gauge.value(), 0.0));
    nameToGauge
        .get(getFullMetricName("indexesInCatalogFeatureVersionFour"))
        .forEach(gauge -> Assert.assertEquals(0, gauge.value(), 0.0));
    nameToGauge
        .get(getFullMetricName("indexesPhasingOutFeatureVersionFour"))
        .forEach(gauge -> Assert.assertEquals(0, gauge.value(), 0.0));
  }

  @Test
  public void testUpdatesCorrectGauge() throws Exception {
    ConfigStateMocks mocks = ConfigStateMocks.create();

    // there will be a gauge per IndexFormatVersion
    var nameToGauge =
        mocks.meterRegistry.getMeters().stream()
            .filter(meter -> meter.getId().getType() == Meter.Type.GAUGE)
            .map(meter -> (Gauge) meter)
            .collect(
                Collectors.toMap(
                    gauge -> gauge.getId().getName(),
                    Set::of,
                    (prevSet, newSet) ->
                        Stream.concat(prevSet.stream(), newSet.stream())
                            .collect(Collectors.toSet())));

    Assert.assertTrue(nameToGauge.containsKey(getFullMetricName("stagedIndexes")));
    Assert.assertTrue(nameToGauge.containsKey(getFullMetricName("indexesInCatalog")));
    Assert.assertTrue(nameToGauge.containsKey(getFullMetricName("indexesPhasingOut")));

    mocks.staged.addIndex(IndexGeneration.mockIndexGeneration());

    // check that IndexFormatVersion.CURRENT specific gauge is updated
    Optional<Gauge> currentIndexFormatVersionGauge =
        nameToGauge.get(getFullMetricName("stagedIndexes")).stream()
            .filter(
                gauge ->
                    Objects.equals(
                        gauge.getId().getTag("indexFormatVersion"),
                        String.valueOf(IndexFormatVersion.CURRENT.versionNumber)))
            .findFirst();
    Check.isPresent(currentIndexFormatVersionGauge, "gauge");

    Assert.assertEquals(1, currentIndexFormatVersionGauge.get().value(), 0.0);

    // ensure that other IndexFormatVersion specific gauges are not updated
    nameToGauge.get(getFullMetricName("stagedIndexes")).stream()
        .filter(gauge -> !gauge.equals(currentIndexFormatVersionGauge.get()))
        .forEach(gauge -> Assert.assertEquals(0, gauge.value(), 0.0));
  }

  private String getFullMetricName(String metric) {
    return String.format("%s.%s", "configState", metric);
  }

  @Test
  public void testPersist() throws Exception {
    var mocks = ConfigStateMocks.create();
    mocks.configState.persist(ConfigJournalV1Builder.builder().build());
    mocks.assertPersistedJournalEmpty();
  }

  @Theory
  public void testPersistValidatesInvariants(
      @FromDataPoints("indexDefinitionGenerations") IndexDefinitionGeneration definitionGeneration)
      throws Exception {
    var mocks = ConfigStateMocks.create();
    ConfigJournalV1 invalidJournal =
        ConfigJournalV1Builder.builder()
            .liveIndex(definitionGeneration)
            .stagedIndex(definitionGeneration)
            .build();
    Assert.assertThrows(
        Invariants.InvariantException.class, () -> mocks.configState.persist(invalidJournal));
  }

  @Test
  public void testNoIndexesJournalEmpty() throws Exception {
    var mocks = ConfigStateMocks.create();
    var expected = ConfigJournalV1Builder.builder().build();
    assertJournalForCurrentState(mocks, expected);
  }

  @Test
  public void testConfigStateTransitionCounter_noExistingJorunal() throws Exception {
    var mocks = ConfigStateMocks.create();
    ObjectId indexId = new ObjectId();
    IndexDefinitionGeneration oldIndex =
        IndexGeneration.mockDefinitionGeneration(
            new GenerationId(
                indexId, new Generation(new UserIndexVersion(0), IndexFormatVersion.CURRENT)));
    IndexDefinitionGeneration newIndex =
        IndexGeneration.mockDefinitionGeneration(
            new GenerationId(
                indexId, new Generation(new UserIndexVersion(1), IndexFormatVersion.CURRENT)));
    mocks.configState.postInitializationFromConfigJournal();

    mocks.configState.persist(ConfigJournalV1Builder.builder().liveIndex(oldIndex).build());
    assertStateTransitions(mocks, ConfigState.State.NOT_EXIST, ConfigState.State.LIVE, 1);
    mocks.configState.persist(
        ConfigJournalV1Builder.builder().liveIndex(oldIndex).stagedIndex(newIndex).build());
    assertStateTransitions(mocks, ConfigState.State.NOT_EXIST, ConfigState.State.STAGED, 1);
    mocks.configState.persist(
        ConfigJournalV1Builder.builder().liveIndex(newIndex).deletedIndex(oldIndex).build());
    assertStateTransitions(mocks, ConfigState.State.STAGED, ConfigState.State.LIVE, 1);
    assertStateTransitions(mocks, ConfigState.State.LIVE, ConfigState.State.DELETED, 1);
    mocks.configState.persist(ConfigJournalV1Builder.builder().liveIndex(newIndex).build());
    assertStateTransitions(mocks, ConfigState.State.DELETED, ConfigState.State.NOT_EXIST, 1);
  }

  @Test
  public void testConfigStateTransitionCounter_withExistingJorunal() throws Exception {
    var mocks = ConfigStateMocks.create();
    ObjectId indexId = new ObjectId();
    IndexDefinitionGeneration oldIndex =
        IndexGeneration.mockDefinitionGeneration(
            new GenerationId(
                indexId, new Generation(new UserIndexVersion(0), IndexFormatVersion.CURRENT)));
    IndexDefinitionGeneration newIndex =
        IndexGeneration.mockDefinitionGeneration(
            new GenerationId(
                indexId, new Generation(new UserIndexVersion(1), IndexFormatVersion.CURRENT)));
    mocks.configState.persist(ConfigJournalV1Builder.builder().liveIndex(oldIndex).build());
    mocks.configState.postInitializationFromConfigJournal();

    assertStateTransitions(mocks, ConfigState.State.NOT_EXIST, ConfigState.State.LIVE, 0);
    mocks.configState.persist(
        ConfigJournalV1Builder.builder().liveIndex(oldIndex).stagedIndex(newIndex).build());
    assertStateTransitions(mocks, ConfigState.State.NOT_EXIST, ConfigState.State.STAGED, 1);
    mocks.configState.persist(
        ConfigJournalV1Builder.builder().liveIndex(newIndex).deletedIndex(oldIndex).build());
    assertStateTransitions(mocks, ConfigState.State.STAGED, ConfigState.State.LIVE, 1);
    assertStateTransitions(mocks, ConfigState.State.LIVE, ConfigState.State.DELETED, 1);
    mocks.configState.persist(ConfigJournalV1Builder.builder().liveIndex(newIndex).build());
    assertStateTransitions(mocks, ConfigState.State.DELETED, ConfigState.State.NOT_EXIST, 1);
  }

  @Theory
  public void testJournalsIndexInCatalogAsLiveIndex(
      @FromDataPoints("indexGenerations") com.xgen.mongot.index.IndexGeneration index)
      throws Exception {
    var mocks = ConfigStateMocks.create();
    mocks.indexCatalog.addIndex(index);
    var expected =
        ConfigJournalV1Builder.builder().liveIndex(index.getDefinitionGeneration()).build();
    assertJournalForCurrentState(mocks, expected);
  }

  @Theory
  public void testJournalsStagedAsStaged(
      @FromDataPoints("indexGenerations") com.xgen.mongot.index.IndexGeneration index)
      throws Exception {
    var mocks = ConfigStateMocks.create();
    mocks.staged.addIndex(index);
    var expected =
        ConfigJournalV1Builder.builder().stagedIndex(index.getDefinitionGeneration()).build();
    assertJournalForCurrentState(mocks, expected);
  }

  @Theory
  public void testJournalsPhaseOutIndexAsDeletedIndex(
      @FromDataPoints("indexGenerations") com.xgen.mongot.index.IndexGeneration index)
      throws Exception {
    // The only reason we keep phasing out indexes is if they have open cursors. If we restart, we
    // could simply drop them as our cursors are not persisted
    var mocks = ConfigStateMocks.create();
    mocks.phasingOut.addIndex(index);
    var expected =
        ConfigJournalV1Builder.builder().deletedIndex(index.getDefinitionGeneration()).build();
    assertJournalForCurrentState(mocks, expected);
  }

  @Theory
  public void testJournalsMultipleIndexesInVariousStates(
      @FromDataPoints("indexDefinitionGenerations") IndexDefinitionGeneration definitionGeneration)
      throws Exception {
    var mocks = ConfigStateMocks.create();
    CheckedBiFunction<
            ObjectId, ConfigStateMocks.State, com.xgen.mongot.index.IndexGeneration, Exception>
        addIndexFunc =
            definitionGeneration.getType() == IndexDefinitionGeneration.Type.SEARCH
                ? mocks::addIndex
                : mocks::addVectorIndex;

    var live1 = addIndexFunc.apply(new ObjectId(), ConfigStateMocks.State.LIVE);
    var live2 = addIndexFunc.apply(new ObjectId(), ConfigStateMocks.State.LIVE);

    var phaseOut1 = addIndexFunc.apply(new ObjectId(), ConfigStateMocks.State.PHASE_OUT);
    var phaseOut2 = addIndexFunc.apply(new ObjectId(), ConfigStateMocks.State.PHASE_OUT);

    // two staged indexes with the common index-ids with other indexes
    var staged1 = mocks.stageIndex(live1.getDefinition().getIndexId());
    var staged2 = mocks.stageIndex(live2.getDefinition().getIndexId());

    var expectedJournal =
        ConfigJournalV1Builder.builder()
            .stagedIndex(staged1.getDefinitionGeneration())
            .stagedIndex(staged2.getDefinitionGeneration())
            .liveIndex(live1.getDefinitionGeneration())
            .liveIndex(live2.getDefinitionGeneration())
            .deletedIndex(phaseOut1.getDefinitionGeneration())
            .deletedIndex(phaseOut2.getDefinitionGeneration())
            .build();
    assertJournalForCurrentState(mocks, expectedJournal);
  }

  @Theory
  public void testStagedIndexesMustHaveUniqueIndexId(
      @FromDataPoints("indexGenerations") com.xgen.mongot.index.IndexGeneration index)
      throws Exception {
    var sameGenerationId =
        index.getDefinitionGeneration().getType() == IndexDefinitionGeneration.Type.SEARCH
            ? IndexGeneration.mockIndexGeneration(index.getDefinitionGeneration().asSearch())
            : IndexGeneration.mockIndexGeneration(index.getDefinitionGeneration().asVector());
    var sameIndexId =
        IndexGeneration.mockIndexGeneration(
            GenerationIdBuilder.incrementUser(index.getGenerationId()));

    var mocks = ConfigStateMocks.create();
    mocks.staged.addIndex(index);
    Assert.assertThrows(RuntimeException.class, () -> mocks.staged.addIndex(sameGenerationId));
    Assert.assertThrows(RuntimeException.class, () -> mocks.staged.addIndex(sameIndexId));
  }

  @Theory
  public void testPhasingOutIndexesMustHaveUniqueGenerationIds(
      @FromDataPoints("indexGenerations") com.xgen.mongot.index.IndexGeneration index)
      throws Exception {
    var sameGenerationId =
        index.getDefinitionGeneration().getType() == IndexDefinitionGeneration.Type.SEARCH
            ? IndexGeneration.mockIndexGeneration(index.getGenerationId())
            : IndexGeneration.mockVectorIndexGeneration(index.getGenerationId());
    var sameIndexId =
        IndexGeneration.mockIndexGeneration(
            GenerationIdBuilder.incrementUser(index.getGenerationId()));

    var mocks = ConfigStateMocks.create();
    mocks.phasingOut.addIndex(index);
    Assert.assertThrows(RuntimeException.class, () -> mocks.phasingOut.addIndex(sameGenerationId));
    // allowed, we don't care about the identity of these indexes, as they are scheduled to drop.
    mocks.phasingOut.addIndex(sameIndexId);
  }

  private void assertJournalForCurrentState(ConfigStateMocks mocks, ConfigJournalV1 expected) {
    ConfigJournalV1 journal = mocks.configState.currentJournal();
    Assert.assertEquals(expected, journal);
  }

  @Test
  public void testGetNewUserIndexVersion() throws Exception {
    var mocks = ConfigStateMocks.create();
    ObjectId indexId1 = new ObjectId();
    ObjectId indexId2 = new ObjectId();
    ObjectId indexId3 = new ObjectId();
    mocks.indexCatalog.addIndex(IndexGeneration.mockIndexGeneration(indexId1, 0));
    mocks.indexCatalog.addIndex(IndexGeneration.mockIndexGeneration(indexId2, 1));
    mocks.staged.addIndex(IndexGeneration.mockIndexGeneration(indexId2, 3));
    mocks.configState.postInitializationFromConfigJournal();
    mocks.indexCatalog.removeIndex(indexId1);
    mocks.indexCatalog.removeIndex(indexId2);
    mocks.staged.removeIndex(IndexGeneration.mockIndexGeneration(indexId2, 3));

    Assert.assertEquals(
        new UserIndexVersion(1), mocks.configState.getNewUserIndexVersion(indexId1));
    Assert.assertEquals(
        new UserIndexVersion(2), mocks.configState.getNewUserIndexVersion(indexId1));
    Assert.assertEquals(
        new UserIndexVersion(4), mocks.configState.getNewUserIndexVersion(indexId2));
    Assert.assertEquals(
        new UserIndexVersion(5), mocks.configState.getNewUserIndexVersion(indexId2));
    Assert.assertEquals(
        new UserIndexVersion(0), mocks.configState.getNewUserIndexVersion(indexId3));
    Assert.assertEquals(
        new UserIndexVersion(1), mocks.configState.getNewUserIndexVersion(indexId3));
  }

  @Test
  public void testGetNewUserIndexVersion_conservative() throws Exception {
    var mocks = ConfigStateMocks.create();
    mocks.configState.postInitializationFromConfigJournal();

    ObjectId indexId1 = new ObjectId();
    ObjectId indexId2 = new ObjectId();
    ObjectId indexId3 = new ObjectId();
    mocks.indexCatalog.addIndex(IndexGeneration.mockIndexGeneration(indexId1, 0));
    mocks.indexCatalog.addIndex(IndexGeneration.mockIndexGeneration(indexId2, 1));
    mocks.staged.addIndex(IndexGeneration.mockIndexGeneration(indexId2, 3));

    Assert.assertEquals(
        new UserIndexVersion(1), mocks.configState.getNewUserIndexVersion(indexId1));
    Assert.assertEquals(
        new UserIndexVersion(2), mocks.configState.getNewUserIndexVersion(indexId1));
    Assert.assertEquals(
        new UserIndexVersion(4), mocks.configState.getNewUserIndexVersion(indexId2));
    Assert.assertEquals(
        new UserIndexVersion(5), mocks.configState.getNewUserIndexVersion(indexId2));
    Assert.assertEquals(
        new UserIndexVersion(0), mocks.configState.getNewUserIndexVersion(indexId3));
    Assert.assertEquals(
        new UserIndexVersion(1), mocks.configState.getNewUserIndexVersion(indexId3));
  }

  private void assertStateTransitions(
      ConfigStateMocks mocks, ConfigState.State fromState, ConfigState.State toState, int count) {
    Assert.assertEquals(
        count,
        mocks
            .meterRegistry
            .counter(
                "configState.stateTransition",
                Tags.of(ServerStatusDataExtractor.Scope.REPLICATION.getTag())
                    .and("fromState", fromState.name(), "toState", toState.name()))
            .count(),
        0.01);
  }
}
