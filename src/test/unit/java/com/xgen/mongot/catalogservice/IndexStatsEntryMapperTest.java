package com.xgen.mongot.catalogservice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.catalogservice.schema.IndexStatsEntry;
import com.xgen.mongot.catalogservice.schema.IndexStatusP;
import com.xgen.mongot.index.AggregatedIndexMetrics;
import com.xgen.mongot.index.IndexDetailedStatus;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.SynonymStatus;
import com.xgen.mongot.index.synonym.SynonymDetailedStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Test;

public class IndexStatsEntryMapperTest {

  private static final ObjectId SERVER_ID = new ObjectId("695301d3bb11192ef11c42f6");
  private static final ObjectId INDEX_ID = new ObjectId("695301d3bb11192ef11c42f7");
  private static final UUID COLLECTION_UUID =
      UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa");

  @Test
  public void testFromIndexStatsEntry_searchIndexWithoutStaged() {
    var searchDef =
        SearchIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .name("searchIndex")
            .database("testDb")
            .lastObservedCollectionName("testColl")
            .collectionUuid(COLLECTION_UUID)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();

    var synonymStatusMap =
        Map.of(
            "synonym1",
            new SynonymDetailedStatus(SynonymStatus.READY, Optional.of("Ready")),
            "synonym2",
            new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.empty()));

    var mainIndexStatus =
        new IndexDetailedStatus.Search(
            synonymStatusMap,
            searchDef,
            List.of(),
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            Generation.CURRENT.generationId(INDEX_ID),
            Optional.of(new AggregatedIndexMetrics(100L, 200L, new BsonTimestamp(1L), 50L, 0L)));

    var indexInfo =
        new IndexInformation.Search(
            searchDef,
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            Collections.emptyList(),
            new AggregatedIndexMetrics(100L, 200L, new BsonTimestamp(1L), 50L, 0L),
            Optional.of(mainIndexStatus),
            Optional.empty(),
            Map.of("synonym1", SynonymStatus.READY, "synonym2", SynonymStatus.INITIAL_SYNC));

    IndexStatsEntry result = IndexStatsEntryMapper.fromIndexInformation(indexInfo, SERVER_ID);

    assertEquals(SERVER_ID, result.key().serverId());
    assertEquals(INDEX_ID, result.key().indexId());
    assertEquals(IndexDefinition.Type.SEARCH, result.type());
    assertTrue(result.mainIndex().isPresent());
    assertEquals(IndexStatusP.StatusCode.STEADY, result.mainIndex().get().status().statusCode());
    assertEquals(searchDef, result.mainIndex().get().definition());
    assertTrue(result.mainIndex().get().synonymDetailedStatusMap().isPresent());
    assertEquals(synonymStatusMap, result.mainIndex().get().synonymDetailedStatusMap().get());
    assertFalse(result.stagedIndex().isPresent());
  }

  @Test
  public void testFromIndexStatsEntry_searchIndexWithStaged() {
    var searchDef =
        SearchIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .name("searchIndex")
            .database("testDb")
            .lastObservedCollectionName("testColl")
            .collectionUuid(COLLECTION_UUID)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();

    var mainSynonymStatusMap =
        Map.of("synonym1", new SynonymDetailedStatus(SynonymStatus.READY, Optional.of("Ready")));

    var stagedSynonymStatusMap =
        Map.of(
            "synonym1",
            new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.of("Syncing")));

    var mainIndexStatus =
        new IndexDetailedStatus.Search(
            mainSynonymStatusMap,
            searchDef,
            List.of(),
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            Generation.CURRENT.generationId(INDEX_ID),
            Optional.empty());

    var stagedIndexStatus =
        new IndexDetailedStatus.Search(
            stagedSynonymStatusMap,
            searchDef,
            List.of(),
            new IndexStatus(IndexStatus.StatusCode.INITIAL_SYNC),
            Generation.CURRENT.generationId(INDEX_ID),
            Optional.empty());

    var indexInfo =
        new IndexInformation.Search(
            searchDef,
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            Collections.emptyList(),
            new AggregatedIndexMetrics(100L, 200L, new BsonTimestamp(1L), 50L, 0L),
            Optional.of(mainIndexStatus),
            Optional.of(stagedIndexStatus),
            Map.of("synonym1", SynonymStatus.READY));

    IndexStatsEntry result = IndexStatsEntryMapper.fromIndexInformation(indexInfo, SERVER_ID);

    assertEquals(SERVER_ID, result.key().serverId());
    assertEquals(INDEX_ID, result.key().indexId());
    assertEquals(IndexDefinition.Type.SEARCH, result.type());
    assertTrue(result.mainIndex().isPresent());
    assertEquals(IndexStatusP.StatusCode.STEADY, result.mainIndex().get().status().statusCode());
    assertTrue(result.mainIndex().get().synonymDetailedStatusMap().isPresent());
    assertEquals(mainSynonymStatusMap, result.mainIndex().get().synonymDetailedStatusMap().get());
    assertTrue(result.stagedIndex().isPresent());
    assertEquals(
        IndexStatusP.StatusCode.INITIAL_SYNC, result.stagedIndex().get().status().statusCode());
    assertTrue(result.stagedIndex().get().synonymDetailedStatusMap().isPresent());
    assertEquals(
        stagedSynonymStatusMap, result.stagedIndex().get().synonymDetailedStatusMap().get());
  }

  @Test
  public void testFromIndexStatsEntry_searchIndexWithEmptySynonymMap() {
    var searchDef =
        SearchIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .name("searchIndex")
            .database("testDb")
            .lastObservedCollectionName("testColl")
            .collectionUuid(COLLECTION_UUID)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();

    var mainIndexStatus =
        new IndexDetailedStatus.Search(
            Collections.emptyMap(),
            searchDef,
            List.of(),
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            Generation.CURRENT.generationId(INDEX_ID),
            Optional.empty());

    var indexInfo =
        new IndexInformation.Search(
            searchDef,
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            Collections.emptyList(),
            new AggregatedIndexMetrics(100L, 200L, new BsonTimestamp(1L), 50L, 0L),
            Optional.of(mainIndexStatus),
            Optional.empty(),
            Collections.emptyMap());

    IndexStatsEntry result = IndexStatsEntryMapper.fromIndexInformation(indexInfo, SERVER_ID);

    assertEquals(SERVER_ID, result.key().serverId());
    assertEquals(INDEX_ID, result.key().indexId());
    assertEquals(IndexDefinition.Type.SEARCH, result.type());
    assertTrue(result.mainIndex().isPresent());
    assertTrue(result.mainIndex().get().synonymDetailedStatusMap().isPresent());
    assertTrue(result.mainIndex().get().synonymDetailedStatusMap().get().isEmpty());
  }

  @Test
  public void testFromIndexStatsEntry_vectorIndexWithoutStaged() {
    var vectorDef =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .name("vectorIndex")
            .database("testDb")
            .lastObservedCollectionName("testColl")
            .collectionUuid(COLLECTION_UUID)
            .withCosineVectorField("embeddings", 1536)
            .withFilterPath("metadata.category")
            .build();

    var mainIndexStatus =
        new IndexDetailedStatus.Vector(
            vectorDef,
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            Generation.CURRENT.generationId(INDEX_ID),
            Optional.of(new AggregatedIndexMetrics(500L, 1000L, new BsonTimestamp(2L), 100L, 0L)));

    var indexInfo =
        new IndexInformation.Vector(
            vectorDef,
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            Collections.emptyList(),
            new AggregatedIndexMetrics(500L, 1000L, new BsonTimestamp(2L), 100L, 0L),
            Optional.of(mainIndexStatus),
            Optional.empty());

    IndexStatsEntry result = IndexStatsEntryMapper.fromIndexInformation(indexInfo, SERVER_ID);

    assertEquals(SERVER_ID, result.key().serverId());
    assertEquals(INDEX_ID, result.key().indexId());
    assertEquals(IndexDefinition.Type.VECTOR_SEARCH, result.type());
    assertTrue(result.mainIndex().isPresent());
    assertEquals(IndexStatusP.StatusCode.STEADY, result.mainIndex().get().status().statusCode());
    assertEquals(vectorDef, result.mainIndex().get().definition());
    assertFalse(result.mainIndex().get().synonymDetailedStatusMap().isPresent());
    assertFalse(result.stagedIndex().isPresent());
  }

  @Test
  public void testFromIndexStatsEntry_vectorIndexWithStaged() {
    var vectorDef =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .name("vectorIndex")
            .database("testDb")
            .lastObservedCollectionName("testColl")
            .collectionUuid(COLLECTION_UUID)
            .withCosineVectorField("embeddings", 1536)
            .build();

    var mainIndexStatus =
        new IndexDetailedStatus.Vector(
            vectorDef,
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            Generation.CURRENT.generationId(INDEX_ID),
            Optional.empty());

    var stagedIndexStatus =
        new IndexDetailedStatus.Vector(
            vectorDef,
            new IndexStatus(IndexStatus.StatusCode.INITIAL_SYNC),
            Generation.CURRENT.generationId(INDEX_ID),
            Optional.empty());

    var indexInfo =
        new IndexInformation.Vector(
            vectorDef,
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            Collections.emptyList(),
            new AggregatedIndexMetrics(500L, 1000L, new BsonTimestamp(2L), 100L, 0L),
            Optional.of(mainIndexStatus),
            Optional.of(stagedIndexStatus));

    IndexStatsEntry result = IndexStatsEntryMapper.fromIndexInformation(indexInfo, SERVER_ID);

    assertEquals(SERVER_ID, result.key().serverId());
    assertEquals(INDEX_ID, result.key().indexId());
    assertEquals(IndexDefinition.Type.VECTOR_SEARCH, result.type());
    assertTrue(result.mainIndex().isPresent());
    assertEquals(IndexStatusP.StatusCode.STEADY, result.mainIndex().get().status().statusCode());
    assertFalse(result.mainIndex().get().synonymDetailedStatusMap().isPresent());
    assertTrue(result.stagedIndex().isPresent());
    assertEquals(
        IndexStatusP.StatusCode.INITIAL_SYNC, result.stagedIndex().get().status().statusCode());
    assertFalse(result.stagedIndex().get().synonymDetailedStatusMap().isPresent());
  }

  @Test
  public void testFromIndexStatsEntry_withEmptyMainIndex() {
    var searchDef =
        SearchIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .name("searchIndex")
            .database("testDb")
            .lastObservedCollectionName("testColl")
            .collectionUuid(COLLECTION_UUID)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();

    var indexInfo =
        new IndexInformation.Search(
            searchDef,
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            Collections.emptyList(),
            new AggregatedIndexMetrics(100L, 200L, new BsonTimestamp(1L), 50L, 0L),
            Optional.empty(), // Empty main index
            Optional.empty(),
            Collections.emptyMap());

    IndexStatsEntry result = IndexStatsEntryMapper.fromIndexInformation(indexInfo, SERVER_ID);

    assertEquals(SERVER_ID, result.key().serverId());
    assertEquals(INDEX_ID, result.key().indexId());
    assertEquals(IndexDefinition.Type.SEARCH, result.type());
    assertFalse(result.mainIndex().isPresent());
    assertFalse(result.stagedIndex().isPresent());
  }
}
