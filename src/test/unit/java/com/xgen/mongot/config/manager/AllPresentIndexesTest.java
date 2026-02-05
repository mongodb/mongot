package com.xgen.mongot.config.manager;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.config.manager.metrics.IndexConfigState;
import com.xgen.mongot.index.AggregatedIndexMetrics;
import com.xgen.mongot.index.DocCounts;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import java.util.Collections;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AllPresentIndexesTest {
  private ConfigStateMocks mocks;

  @Before
  public void setUp() throws Exception {
    this.mocks = ConfigStateMocks.create();
  }

  @Test
  public void testNoIndexesReturnsEmpty() {
    Assert.assertTrue(
        AllPresentIndexes.allIndexInfos(this.mocks.configState).indexInformations().isEmpty());
  }

  @Test
  public void testAllIndexGenerationsGrouped() throws Exception {
    ObjectId firstIndexId = new ObjectId();
    IndexGeneration generation = this.mocks.addIndex(firstIndexId, ConfigStateMocks.State.LIVE);
    this.mocks.stageIndex(firstIndexId);
    this.mocks.phaseOutIndex(generation);

    ObjectId secondIndexId = new ObjectId();
    this.mocks.addIndex(secondIndexId, ConfigStateMocks.State.LIVE);

    var indexGenerationsGrouped =
        AllPresentIndexes.allIndexInfos(this.mocks.configState).allIndexGenerationsGrouped();

    Assert.assertEquals(2, indexGenerationsGrouped.size());
    Assert.assertEquals(3, indexGenerationsGrouped.get(firstIndexId).size());
    Assert.assertEquals(1, indexGenerationsGrouped.get(secondIndexId).size());
  }

  @Test
  public void testAllIndexGenerationsGroupedState() throws Exception {
    ObjectId firstIndexId = new ObjectId();
    IndexGeneration generation = this.mocks.addIndex(firstIndexId, ConfigStateMocks.State.LIVE);
    this.mocks.stageIndex(firstIndexId);
    this.mocks.phaseOutIndex(generation);

    ObjectId secondIndexId = new ObjectId();
    this.mocks.addIndex(secondIndexId, ConfigStateMocks.State.LIVE);

    var indexGenerationsGrouped =
        AllPresentIndexes.allIndexInfos(this.mocks.configState).allIndexGenerationsGrouped();

    Assert.assertEquals(2, indexGenerationsGrouped.size());
    Assert.assertEquals(3, indexGenerationsGrouped.get(firstIndexId).size());
    Assert.assertEquals(1, indexGenerationsGrouped.get(secondIndexId).size());

    Assert.assertEquals(
        IndexConfigState.STAGED, indexGenerationsGrouped.get(firstIndexId).get(0).state());
    Assert.assertEquals(
        IndexConfigState.LIVE, indexGenerationsGrouped.get(firstIndexId).get(1).state());
    Assert.assertEquals(
        IndexConfigState.PHASING_OUT, indexGenerationsGrouped.get(firstIndexId).get(2).state());

    Assert.assertEquals(
        IndexConfigState.LIVE, indexGenerationsGrouped.get(secondIndexId).get(0).state());
  }

  @Test
  public void testAllIndexGenerationsGroupedReadsStatusOnce() throws Exception {
    ObjectId firstIndexId = new ObjectId();
    IndexGeneration indexGeneration =
        this.mocks.addIndex(firstIndexId, ConfigStateMocks.State.LIVE);
    IndexGeneration stagedIndexGeneration = this.mocks.stageIndex(firstIndexId);
    IndexGeneration phasedOutIndexGeneration =
        this.mocks.addIndex(
            indexGeneration.getDefinitionGeneration().incrementAttempt(),
            ConfigStateMocks.State.PHASE_OUT);

    ObjectId secondIndexId = new ObjectId();
    IndexGeneration secondIndexGeneration =
        this.mocks.addIndex(secondIndexId, ConfigStateMocks.State.LIVE);

    this.mocks.waitAndGetInitializedIndex(phasedOutIndexGeneration.getGenerationId());

    var indexGenerationsGrouped =
        AllPresentIndexes.allIndexInfos(this.mocks.configState).allIndexGenerationsGrouped();

    // getStatus() is called:
    // 1. Once when building the indexStatuses map
    // 2. Once when creating IndexGenerationStateMetrics (via InitializedIndex.getMetrics())
    // 3. Once when creating IndexDetailedStatus for mainIndex/stagedIndex (via
    //    InitializedIndex.getMetrics())
    // For indexes that are mainIndex or stagedIndex: 3 calls
    // For indexes that are only in IndexGenerationStateMetrics (like phased out): 2 calls
    verify(indexGeneration.getIndex(), times(3)).getStatus();
    verify(stagedIndexGeneration.getIndex(), times(3)).getStatus();
    verify(phasedOutIndexGeneration.getIndex(), times(2)).getStatus();
    verify(secondIndexGeneration.getIndex(), times(3)).getStatus();

    Assert.assertEquals(2, indexGenerationsGrouped.size());
    Assert.assertEquals(3, indexGenerationsGrouped.get(firstIndexId).size());
    Assert.assertEquals(1, indexGenerationsGrouped.get(secondIndexId).size());
  }

  @Test
  public void testLookupAllIdsHaveEntry() throws Exception {
    ObjectId id = new ObjectId();
    IndexGeneration generation = this.mocks.addIndex(id, ConfigStateMocks.State.LIVE);
    this.mocks.stageIndex(id);
    this.mocks.phaseOutIndex(generation);

    this.mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);

    var lookupAll = AllPresentIndexes.allIndexInfos(this.mocks.configState).indexInformations();
    Assert.assertEquals(2, lookupAll.size());
  }

  /** We take the status from staged / live / phase out in this order of precedence. */
  @Test
  public void testIndexInformationTakesCorrectStatus() throws Exception {
    ObjectId id = new ObjectId();

    IndexGeneration generation = this.mocks.addIndex(id, ConfigStateMocks.State.PHASE_OUT);
    Index phaseOut = generation.getIndex();
    phaseOut.setStatus(IndexStatus.initialSync());
    Assert.assertSame(getIndexInfo(id).getStatus(), phaseOut.getStatus());

    var index =
        this.mocks
            .addIndex(
                generation.getDefinitionGeneration().incrementAttempt(),
                ConfigStateMocks.State.LIVE)
            .getIndex();
    index.setStatus(IndexStatus.failed("live fail"));
    Assert.assertSame(getIndexInfo(id).getStatus(), index.getStatus());
    Assert.assertSame(getIndexInfo(id).getMainIndex().get().indexStatus(), index.getStatus());
    Assert.assertFalse(getIndexInfo(id).getStagedIndex().isPresent());

    var stagedIndex = this.mocks.stageIndex(id).getIndex();

    stagedIndex.setStatus(IndexStatus.initialSync());
    Assert.assertSame(
        IndexStatus.StatusCode.INITIAL_SYNC, getIndexInfo(id).getStatus().getStatusCode());
    Assert.assertSame(
        IndexStatus.StatusCode.INITIAL_SYNC,
        getIndexInfo(id).getStagedIndex().get().indexStatus().getStatusCode());

    stagedIndex.setStatus(IndexStatus.failed("staged fail"));
    Assert.assertSame(IndexStatus.StatusCode.FAILED, getIndexInfo(id).getStatus().getStatusCode());
    Assert.assertSame(
        IndexStatus.StatusCode.FAILED,
        getIndexInfo(id).getStagedIndex().get().indexStatus().getStatusCode());
  }

  /** From a user's standpoint, the staged index is still building. */
  @Test
  public void testStagedIndexInSteadyStateIsReportedAsInitialSync() throws Exception {
    ObjectId id = new ObjectId();

    var index = this.mocks.addIndex(id, ConfigStateMocks.State.LIVE).getIndex();
    Assert.assertSame(getIndexInfo(id).getStatus(), index.getStatus());

    var stagedIndex = this.mocks.stageIndex(id).getIndex();

    // even when a staged index is steady, we still want to report it as building:
    stagedIndex.setStatus(IndexStatus.steady());

    Assert.assertSame(
        IndexStatus.StatusCode.INITIAL_SYNC, getIndexInfo(id).getStatus().getStatusCode());
  }

  /** Same order of precedence as the above. */
  @Test
  public void testIndexInformationTakesCorrectIndexDefinition() throws Exception {
    // we use "same" instead of equality to assert where the definitions were taken.
    ObjectId id = new ObjectId();
    SearchIndexDefinitionGeneration definitionGeneration =
        com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration(id);
    var phaseOut = this.mocks.addIndex(definitionGeneration, ConfigStateMocks.State.PHASE_OUT);

    // only phase out present, take one of those definitions
    Assert.assertSame(phaseOut.getDefinition(), getIndexInfo(id).getDefinition());
    Assert.assertFalse(getIndexInfo(id).getMainIndex().isPresent());
    Assert.assertFalse(getIndexInfo(id).getStagedIndex().isPresent());

    // in presence of a live one prefer that over phase out
    var live =
        this.mocks.addIndex(
            new SearchIndexDefinitionGeneration(
                definitionGeneration.definition(),
                definitionGeneration.generation().incrementUser()),
            ConfigStateMocks.State.LIVE);
    Assert.assertNotEquals(live.getDefinitionGeneration(), phaseOut.getDefinitionGeneration());
    Assert.assertSame(live.getDefinition(), getIndexInfo(id).getDefinition());
    Assert.assertSame(live.getDefinition(), getIndexInfo(id).getMainIndex().get().definition());
    Assert.assertFalse(getIndexInfo(id).getStagedIndex().isPresent());

    IndexActions.withReplication(this.mocks.configState)
        .addStagedIndex(
            SearchIndexDefinitionGenerationBuilder.create(
                SearchIndexDefinitionBuilder.from(live.getDefinition().asSearchDefinition())
                    .analyzerName("lucene.keyword")
                    .build(),
                live.getDefinitionGeneration().generation().incrementUser(),
                Collections.emptyList()));

    // in presence of a staged one prefer that over all
    var staged = this.mocks.staged.getIndex(id).orElseThrow();
    Assert.assertNotSame(staged.getDefinition(), live.getDefinition());
    Assert.assertSame(staged.getDefinition(), getIndexInfo(id).getDefinition());
    var stagedIndex = getIndexInfo(id).getStagedIndex().get();
    Assert.assertEquals(staged.getDefinition(), stagedIndex.definition());
  }

  /**
   * Essentially we want to see the #docs from the regular order of precedence. the disk size we
   * want to sum across all indexes.
   */
  @Test
  public void testIndexInformationAggregatedIndexMetrics() throws Exception {
    ObjectId id = new ObjectId();
    SearchIndexDefinitionGeneration definitionGeneration =
        com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration(id);

    this.mocks.addIndex(definitionGeneration, ConfigStateMocks.State.PHASE_OUT);
    var phaseOut = this.mocks.waitAndGetInitializedIndex(definitionGeneration.getGenerationId());

    when(phaseOut.getMetricsUpdater().getIndexMetricValuesSupplier().computeIndexSize())
        .thenAnswer(ignored -> 3L);
    // only phase out present, take its stats
    @Var AggregatedIndexMetrics stats = getIndexInfo(id).getAggregatedMetrics();
    Assert.assertEquals(
        phaseOut.getMetricsUpdater().getMetrics().indexingMetrics().indexSize().toBytes(),
        stats.dataSize());
    Assert.assertEquals(
        phaseOut.getMetricsUpdater().getMetrics().indexingMetrics().numMongoDbDocs(),
        stats.numDocs());

    var live =
        getInitializedIndex(
            this.mocks.addIndex(
                new SearchIndexDefinitionGeneration(
                    definitionGeneration.definition(),
                    definitionGeneration.generation().incrementUser()),
                ConfigStateMocks.State.LIVE));
    when(live.getMetricsUpdater().getIndexMetricValuesSupplier().computeIndexSize())
        .thenAnswer(ignored -> 2L);

    stats = getIndexInfo(id).getAggregatedMetrics();
    Assert.assertEquals(5, stats.dataSize());
    // sum the data size across all
    Assert.assertEquals(
        live.getMetricsUpdater().getMetrics().indexingMetrics().indexSize().toBytes()
            + phaseOut.getMetricsUpdater().getMetrics().indexingMetrics().indexSize().toBytes(),
        stats.dataSize());
    // in presence of a live index take #doc from it
    Assert.assertEquals(
        live.getMetricsUpdater().getMetrics().indexingMetrics().numMongoDbDocs(), stats.numDocs());

    IndexActions.withReplication(this.mocks.configState)
        .addStagedIndex(
            SearchIndexDefinitionGenerationBuilder.create(
                SearchIndexDefinitionBuilder.from(live.getDefinition().asSearchDefinition())
                    .analyzerName("lucene.keyword")
                    .build(),
                // Increment generation again.
                definitionGeneration.generation().incrementUser().incrementUser(),
                Collections.emptyList()));

    var staged =
        this.mocks.waitAndGetInitializedIndex(
            this.mocks.staged.getIndex(id).orElseThrow().getGenerationId());
    when(staged.getMetricsUpdater().getIndexMetricValuesSupplier().computeIndexSize())
        .thenAnswer(ignored -> 3L);
    when(staged.getMetricsUpdater().getIndexMetricValuesSupplier().getDocCounts())
        .thenAnswer(ignored -> new DocCounts(6, 5, 3, 6));
    when(staged.getMetricsUpdater().getIndexingMetricsUpdater().getReplicationOpTimeInfo())
        .thenAnswer(ignored -> new ReplicationOpTimeInfo(5L, 10L, 5L));

    stats = getIndexInfo(id).getAggregatedMetrics();
    // sum the data size across all
    Assert.assertEquals(
        3L
            + live.getMetricsUpdater().getMetrics().indexingMetrics().indexSize().toBytes()
            + phaseOut.getMetricsUpdater().getMetrics().indexingMetrics().indexSize().toBytes(),
        stats.dataSize());
    // in presence of a staged index take #doc from it
    Assert.assertEquals(6, stats.numDocs());
    Assert.assertEquals(new BsonTimestamp(5L), stats.opTime());
  }

  @Test
  public void testEmbeddedIndexAggregatedIndexMetrics() throws Exception {
    // Create an index that has an embeddedDocuments field
    ObjectId id = new ObjectId();
    IndexDefinitionGeneration indexDefinitionGeneration =
        com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration(
            SearchIndexDefinitionBuilder.builder()
                .defaultMetadata()
                .indexId(id)
                .mappings(
                    DocumentFieldDefinitionBuilder.builder()
                        .field(
                            "a",
                            FieldDefinitionBuilder.builder()
                                .embeddedDocuments(
                                    EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                        .dynamic(true)
                                        .build())
                                .build())
                        .build())
                .build());

    var index =
        getInitializedIndex(
            this.mocks.addIndex(indexDefinitionGeneration, ConfigStateMocks.State.LIVE));

    // Expect the "numDocs" value reported in AggregatedIndexMetrics to be 11, because it should
    // originate from getNumEmbeddedRootDocs (and not getNumDocs).
    when(index.getMetricsUpdater().getIndexMetricValuesSupplier().computeIndexSize())
        .thenAnswer(ignored -> 3L);
    when(index.getMetricsUpdater().getIndexMetricValuesSupplier().getDocCounts())
        .thenAnswer(ignored -> new DocCounts(12345, 12345, 5555, 11L));
    when(index.getMetricsUpdater().getIndexingMetricsUpdater().getReplicationOpTimeInfo())
        .thenAnswer(ignored -> new ReplicationOpTimeInfo(5L, 10L, 5L));

    AggregatedIndexMetrics stats = getIndexInfo(id).getAggregatedMetrics();

    Assert.assertEquals(11, stats.numDocs());
    Assert.assertEquals(new BsonTimestamp(5L), stats.opTime());
  }

  private IndexInformation getIndexInfo(ObjectId indexId) {
    return AllPresentIndexes.allIndexInfos(this.mocks.configState).indexInformations().stream()
        .filter(info -> info.getDefinition().getIndexId().equals(indexId))
        .findFirst()
        .orElseThrow(() -> new AssertionError(String.format("can not find index id %s", indexId)));
  }

  private InitializedIndex getInitializedIndex(IndexGeneration generation) {
    return this.mocks.initializedIndexCatalog.getIndex(generation.getGenerationId()).orElseThrow();
  }
}
