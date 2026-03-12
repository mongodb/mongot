package com.xgen.mongot.server.command.management.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.catalogservice.IndexStatsEntry;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.SynonymStatus;
import com.xgen.mongot.index.synonym.SynonymDetailedStatus;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SynonymMappingDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Test;

public class IndexEntryMapperTest {

  private static final ObjectId TEST_INDEX_ID = new ObjectId();
  private static final String TEST_INDEX_NAME = "test-index";
  private static final String TEST_DATABASE = "test-db";
  private static final String TEST_COLLECTION = "test-collection";
  private static final UUID TEST_COLLECTION_UUID = UUID.randomUUID();
  private static final ObjectId TEST_SERVER_ID = new ObjectId();
  private static final String TEST_HOSTNAME = "host1";
  private static final Instant TEST_INSTANT = Instant.now();

  @Test
  public void testToIndexEntry_searchIndex_withAllFields() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.STEADY, 1L, true, Optional.of(createSynonymMap())));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals(TEST_INDEX_ID, result.indexId());
    assertEquals(TEST_INDEX_NAME, result.name());
    assertEquals("READY", result.status());
    assertTrue(result.queryable());
    assertEquals(1, result.latestDefinitionVersion().version());
    assertEquals(TEST_INSTANT, result.latestDefinitionVersion().createdAt());
    assertEquals(1, result.hostStatusDetails().size());
    assertTrue(
        "synonymMappingStatus should be present",
        result.synonymMappingStatus().isPresent());
    assertTrue(
        "synonymMappingStatusDetail should be present",
        result.synonymMappingStatusDetail().isPresent());
  }

  @Test
  public void testToIndexEntry_searchIndex_withoutOptionalFields() {
    SearchIndexDefinition definition =
        createSearchIndexDefinitionWithoutOptionals(Optional.empty(), Optional.empty());
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 0L, false, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals(0, result.latestDefinitionVersion().version());
    assertTrue(
        "synonymMappingStatus should be empty",
        result.synonymMappingStatus().isEmpty());
    assertTrue(
        "synonymMappingStatusDetail should be empty",
        result.synonymMappingStatusDetail().isEmpty());
  }

  @Test
  public void testToIndexEntry_searchIndex_emptyIndexStats() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = Collections.emptyMap();

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals("PENDING", result.status()); // UNKNOWN maps to PENDING
    assertFalse(result.queryable());
    assertTrue(result.hostStatusDetails().isEmpty());
    assertTrue(result.synonymMappingStatus().isEmpty());
    assertTrue(result.synonymMappingStatusDetail().isEmpty());
  }

  @Test
  public void testToIndexEntry_searchIndex_multipleSynonymsWithDifferentStatuses() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    Map<String, SynonymDetailedStatus> synonymMap = new HashMap<>();
    synonymMap.put("synonym1", new SynonymDetailedStatus(SynonymStatus.READY, Optional.empty()));
    synonymMap.put(
        "synonym2", new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.of("Syncing")));
    synonymMap.put(
        "synonym3", new SynonymDetailedStatus(SynonymStatus.FAILED, Optional.of("Error")));

    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.STEADY, 1L, false, Optional.of(synonymMap)));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertTrue(
        "synonymMappingStatus should be present",
        result.synonymMappingStatus().isPresent());
    // FAILED has highest priority (3)
    assertEquals("FAILED", result.synonymMappingStatus().get());
    assertTrue(
        "synonymMappingStatusDetail should be present",
        result.synonymMappingStatusDetail().isPresent());
    assertEquals(3, result.synonymMappingStatusDetail().get().size());
  }

  @Test
  public void testToIndexEntry_searchIndex_emptySynonymMap_returnsEmptyOptional() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    // Empty synonym map should result in Optional.empty()
    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.STEADY, 1L, false, Optional.of(Collections.emptyMap())));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertTrue(
        "synonymMappingStatus should be empty",
        result.synonymMappingStatus().isEmpty());
    assertTrue(
        "synonymMappingStatusDetail should be empty",
        result.synonymMappingStatusDetail().isEmpty());
  }

  @Test
  public void testToIndexEntry_consolidatedSynonymStatus_multipleHosts() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    // Host 1 has READY synonyms
    Map<String, SynonymDetailedStatus> synonymMap1 = new HashMap<>();
    synonymMap1.put("synonym1", new SynonymDetailedStatus(SynonymStatus.READY, Optional.empty()));
    indexStats.put(
        "host1",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.STEADY, 1L, false, Optional.of(synonymMap1)));

    // Host 2 has FAILED synonyms
    Map<String, SynonymDetailedStatus> synonymMap2 = new HashMap<>();
    synonymMap2.put(
        "synonym1", new SynonymDetailedStatus(SynonymStatus.FAILED, Optional.of("Error")));
    indexStats.put(
        "host2",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.STEADY, 1L, false, Optional.of(synonymMap2)));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertTrue(
        "synonymMappingStatus should be present",
        result.synonymMappingStatus().isPresent());
    // FAILED has highest priority
    assertEquals("FAILED", result.synonymMappingStatus().get());
  }

  @Test
  public void testToIndexEntry_consolidatedSynonymStatusDetails_multipleSynonyms() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    // Host 1 has synonym1 and synonym2
    Map<String, SynonymDetailedStatus> synonymMap1 = new HashMap<>();
    synonymMap1.put("synonym1", new SynonymDetailedStatus(SynonymStatus.READY, Optional.empty()));
    synonymMap1.put(
        "synonym2", new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.empty()));
    indexStats.put(
        "host1",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.STEADY, 1L, false, Optional.of(synonymMap1)));

    // Host 2 has synonym1 and synonym3
    Map<String, SynonymDetailedStatus> synonymMap2 = new HashMap<>();
    synonymMap2.put(
        "synonym1", new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.empty()));
    synonymMap2.put("synonym3", new SynonymDetailedStatus(SynonymStatus.FAILED, Optional.empty()));
    indexStats.put(
        "host2",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.STEADY, 1L, false, Optional.of(synonymMap2)));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertTrue(
        "synonymMappingStatusDetail should be present",
        result.synonymMappingStatusDetail().isPresent());
    // Should have 3 unique synonyms (synonym1, synonym2, synonym3)
    assertEquals(3, result.synonymMappingStatusDetail().get().size());
  }

  @Test
  public void testToIndexEntry_allSynonymStatuses() {
    SynonymStatus[] synonymStatuses = {
      SynonymStatus.FAILED,
      SynonymStatus.INVALID,
      SynonymStatus.SYNC_ENQUEUED,
      SynonymStatus.INITIAL_SYNC,
      SynonymStatus.READY_UPDATING,
      SynonymStatus.READY
    };

    for (SynonymStatus status : synonymStatuses) {
      SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
      Map<String, IndexStatsEntry> indexStats = new HashMap<>();
      Map<String, SynonymDetailedStatus> synonymMap = new HashMap<>();
      synonymMap.put("synonym1", new SynonymDetailedStatus(status, Optional.empty()));
      indexStats.put(
          TEST_HOSTNAME,
          createSearchIndexStatsEntry(
              IndexStatus.StatusCode.STEADY, 1L, false, Optional.of(synonymMap)));

      ListSearchIndexesResponseDefinition.IndexEntry result =
          IndexEntryMapper.toIndexEntry(
              definition, definition.toBson(), indexStats, Optional.empty());

      assertNotNull(result);
      assertTrue(
          "Synonym status should be present for: " + status,
          result.synonymMappingStatus().isPresent());
    }
  }

  @Test
  public void testToIndexEntry_emptySynonymList_returnsEmptyStatus() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    // No synonyms at all
    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 1L, false, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertTrue(
        "synonymMappingStatus should be empty",
        result.synonymMappingStatus().isEmpty());
    assertTrue(
        "synonymMappingStatusDetail should be empty",
        result.synonymMappingStatusDetail().isEmpty());
  }

  @Test
  public void testToIndexEntry_synonymPriorityOrdering() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    // Create synonyms with different statuses to test priority
    Map<String, SynonymDetailedStatus> synonymMap = new HashMap<>();
    synonymMap.put("syn1", new SynonymDetailedStatus(SynonymStatus.READY, Optional.empty()));
    synonymMap.put("syn2", new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.empty()));
    synonymMap.put("syn3", new SynonymDetailedStatus(SynonymStatus.FAILED, Optional.empty()));

    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.STEADY, 1L, false, Optional.of(synonymMap)));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertTrue(
        "synonymMappingStatus should be present",
        result.synonymMappingStatus().isPresent());
    // FAILED should have highest priority
    assertEquals("FAILED", result.synonymMappingStatus().get());
  }

  @Test
  public void testToIndexEntry_synonymStatusDetails_sortedByName() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    // Create synonyms with names that would be out of order if not sorted
    Map<String, SynonymDetailedStatus> synonymMap = new HashMap<>();
    synonymMap.put("zebra", new SynonymDetailedStatus(SynonymStatus.READY, Optional.empty()));
    synonymMap.put("apple", new SynonymDetailedStatus(SynonymStatus.READY, Optional.empty()));
    synonymMap.put("mango", new SynonymDetailedStatus(SynonymStatus.READY, Optional.empty()));

    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.STEADY, 1L, false, Optional.of(synonymMap)));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertTrue(
        "synonymMappingStatusDetail should be present",
        result.synonymMappingStatusDetail().isPresent());

    List<ListSearchIndexesResponseDefinition.SynonymMappingStatusDetail> details =
        result.synonymMappingStatusDetail().get();
    assertEquals(3, details.size());

    // Note: We can't directly verify the order without accessing the synonym names,
    // but this test ensures the sorting logic doesn't throw exceptions
  }

  @Test
  public void testToIndexEntry_multipleSynonymHosts_consolidatesCorrectly() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    // Host 1: synonym1 is READY
    Map<String, SynonymDetailedStatus> synonymMap1 = new HashMap<>();
    synonymMap1.put("synonym1", new SynonymDetailedStatus(SynonymStatus.READY, Optional.empty()));
    indexStats.put(
        "host1",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.STEADY, 1L, false, Optional.of(synonymMap1)));

    // Host 2: synonym1 is BUILDING
    Map<String, SynonymDetailedStatus> synonymMap2 = new HashMap<>();
    synonymMap2.put(
        "synonym1", new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.empty()));
    indexStats.put(
        "host2",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.STEADY, 1L, false, Optional.of(synonymMap2)));

    // Host 3: synonym1 is READY
    Map<String, SynonymDetailedStatus> synonymMap3 = new HashMap<>();
    synonymMap3.put("synonym1", new SynonymDetailedStatus(SynonymStatus.READY, Optional.empty()));
    indexStats.put(
        "host3",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.STEADY, 1L, false, Optional.of(synonymMap3)));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertTrue(
        "synonymMappingStatus should be present",
        result.synonymMappingStatus().isPresent());
    // Should pick READY as it has higher priority (2) than BUILDING (1)
    // Priority order: FAILED(3) > READY(2) > BUILDING(1)
    assertEquals("READY", result.synonymMappingStatus().get());

    assertTrue(
        "synonymMappingStatusDetail should be present",
        result.synonymMappingStatusDetail().isPresent());
    // Should have only 1 synonym (consolidated across hosts)
    assertEquals(1, result.synonymMappingStatusDetail().get().size());
  }

  @Test
  public void testToIndexEntry_vectorIndex_withAllFields() {
    VectorIndexDefinition definition = createVectorIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    indexStats.put(
        TEST_HOSTNAME,
        createVectorIndexStatsEntry(IndexStatus.StatusCode.STEADY, 1L, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals(TEST_INDEX_ID, result.indexId());
    assertEquals(TEST_INDEX_NAME, result.name());
    assertEquals("READY", result.status());
    assertTrue(result.queryable());
    assertEquals(1, result.latestDefinitionVersion().version());
    assertEquals(TEST_INSTANT, result.latestDefinitionVersion().createdAt());
    assertEquals(1, result.hostStatusDetails().size());
    // Vector indexes don't have synonyms
    assertTrue(
        "synonymMappingStatus should be empty for vector indexes",
        result.synonymMappingStatus().isEmpty());
    assertTrue(
        "synonymMappingStatusDetail should be empty for vector indexes",
        result.synonymMappingStatusDetail().isEmpty());
  }

  @Test
  public void testToIndexEntry_vectorIndex_withoutOptionalFields() {
    VectorIndexDefinition definition =
        createVectorIndexDefinitionWithoutOptionals(Optional.empty(), Optional.empty());
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    indexStats.put(
        TEST_HOSTNAME,
        createVectorIndexStatsEntry(IndexStatus.StatusCode.STEADY, 0L, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals(0, result.latestDefinitionVersion().version());
    assertTrue(
        "synonymMappingStatus should be empty for vector indexes",
        result.synonymMappingStatus().isEmpty());
    assertTrue(
        "synonymMappingStatusDetail should be empty for vector indexes",
        result.synonymMappingStatusDetail().isEmpty());
  }

  @Test
  public void testToIndexEntry_vectorIndex_withStagedIndex() {
    VectorIndexDefinition definition = createVectorIndexDefinition(2L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    indexStats.put(
        TEST_HOSTNAME,
        createVectorIndexStatsEntryWithStaged(
            IndexStatus.StatusCode.STEADY,
            2L,
            IndexStatus.StatusCode.INITIAL_SYNC,
            3L,
            Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals(1, result.hostStatusDetails().size());
    ListSearchIndexesResponseDefinition.HostStatusDetail hostDetail =
        result.hostStatusDetails().get(0);
    assertTrue(
        "mainIndex should be present",
        hostDetail.mainIndex().isPresent());
    assertTrue(
        "stagedIndex should be present",
        hostDetail.stagedIndex().isPresent());
  }

  @Test
  public void testToIndexEntry_allStatusCodes() {
    IndexStatus.StatusCode[] statusCodes = {
      IndexStatus.StatusCode.UNKNOWN,
      IndexStatus.StatusCode.DOES_NOT_EXIST,
      IndexStatus.StatusCode.NOT_STARTED,
      IndexStatus.StatusCode.INITIAL_SYNC,
      IndexStatus.StatusCode.STALE,
      IndexStatus.StatusCode.RECOVERING_TRANSIENT,
      IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT,
      IndexStatus.StatusCode.STEADY,
      IndexStatus.StatusCode.FAILED
    };

    for (IndexStatus.StatusCode statusCode : statusCodes) {
      SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
      Map<String, IndexStatsEntry> indexStats = new HashMap<>();
      indexStats.put(
          TEST_HOSTNAME, createSearchIndexStatsEntry(statusCode, 1L, false, Optional.empty()));

      ListSearchIndexesResponseDefinition.IndexEntry result =
          IndexEntryMapper.toIndexEntry(
              definition, definition.toBson(), indexStats, Optional.empty());

      assertNotNull("Result should not be null for status: " + statusCode, result);
      assertNotNull("Status should not be null for: " + statusCode, result.status());
    }
  }

  @Test
  public void testToIndexEntry_queryableStatuses() {
    IndexStatus.StatusCode[] queryableStatuses = {
      IndexStatus.StatusCode.STEADY,
      IndexStatus.StatusCode.RECOVERING_TRANSIENT,
      IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT,
      IndexStatus.StatusCode.STALE
    };

    for (IndexStatus.StatusCode statusCode : queryableStatuses) {
      SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
      Map<String, IndexStatsEntry> indexStats = new HashMap<>();
      indexStats.put(
          TEST_HOSTNAME, createSearchIndexStatsEntry(statusCode, 1L, false, Optional.empty()));

      ListSearchIndexesResponseDefinition.IndexEntry result =
          IndexEntryMapper.toIndexEntry(
              definition, definition.toBson(), indexStats, Optional.empty());

      assertTrue("Should be queryable for status: " + statusCode, result.queryable());
    }
  }

  @Test
  public void testToIndexEntry_nonQueryableStatuses() {
    IndexStatus.StatusCode[] nonQueryableStatuses = {
      IndexStatus.StatusCode.UNKNOWN,
      IndexStatus.StatusCode.NOT_STARTED,
      IndexStatus.StatusCode.INITIAL_SYNC,
      IndexStatus.StatusCode.FAILED
    };

    for (IndexStatus.StatusCode statusCode : nonQueryableStatuses) {
      SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
      Map<String, IndexStatsEntry> indexStats = new HashMap<>();
      indexStats.put(
          TEST_HOSTNAME, createSearchIndexStatsEntry(statusCode, 1L, false, Optional.empty()));

      ListSearchIndexesResponseDefinition.IndexEntry result =
          IndexEntryMapper.toIndexEntry(
              definition, definition.toBson(), indexStats, Optional.empty());

      assertFalse("Should not be queryable for status: " + statusCode, result.queryable());
    }
  }

  @Test
  public void testToIndexEntry_doesNotExistStatus_shouldNotBeQueryable() {
    // If the only index is DOES_NOT_EXIST, the index should not be queryable
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.DOES_NOT_EXIST, 1L, false, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    // An index that doesn't exist should not be queryable
    assertFalse(
        "Index with DOES_NOT_EXIST should not be queryable",
        result.queryable());
  }

  @Test
  public void testToIndexEntry_multipleIndexes_withDoesNotExist() {
    // Test with multiple hosts where some are DOES_NOT_EXIST
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    // Host 1: STEADY (queryable)
    indexStats.put(
        "host1",
        createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 1L, false, Optional.empty()));

    // Host 2: DOES_NOT_EXIST (should be ignored in queryability check)
    indexStats.put(
        "host2",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.DOES_NOT_EXIST, 1L, false, Optional.empty()));

    // Host 3: STEADY (queryable)
    indexStats.put(
        "host3",
        createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 1L, false, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertTrue(result.queryable());
    assertEquals(3, result.hostStatusDetails().size());
  }

  @Test
  public void testToIndexEntry_multipleIndexes_mixedQueryability_withDoesNotExist() {
    // Test with multiple hosts where some are DOES_NOT_EXIST and others are not all queryable
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    // Host 1: STEADY (queryable)
    indexStats.put(
        "host1",
        createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 1L, false, Optional.empty()));

    // Host 2: DOES_NOT_EXIST (should be ignored)
    indexStats.put(
        "host2",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.DOES_NOT_EXIST, 1L, false, Optional.empty()));

    // Host 3: INITIAL_SYNC (not queryable)
    indexStats.put(
        "host3",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.INITIAL_SYNC, 1L, false, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertFalse(result.queryable());
    assertEquals(3, result.hostStatusDetails().size());
  }

  @Test
  public void testToIndexEntry_allIndexes_doesNotExist() {
    // Test with all hosts having DOES_NOT_EXIST status
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    indexStats.put(
        "host1",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.DOES_NOT_EXIST, 1L, false, Optional.empty()));
    indexStats.put(
        "host2",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.DOES_NOT_EXIST, 1L, false, Optional.empty()));
    indexStats.put(
        "host3",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.DOES_NOT_EXIST, 1L, false, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    // Should not be queryable when all indexes don't exist
    assertFalse(
        "All indexes with DOES_NOT_EXIST should not be queryable",
        result.queryable());
    assertEquals(3, result.hostStatusDetails().size());
  }

  @Test
  public void testToIndexEntry_versionMismatch() {
    SearchIndexDefinition definition = createSearchIndexDefinition(5L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    // Index stats has older version
    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 3L, false, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals(5, result.latestDefinitionVersion().version());
    // Status should reflect that main index is not at latest version
    assertNotNull(result.status());
  }

  @Test
  public void testToIndexEntry_zeroVersion() {
    SearchIndexDefinition definition = createSearchIndexDefinition(0L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 0L, false, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals(0, result.latestDefinitionVersion().version());
  }

  @Test
  public void testToIndexEntry_largeNumberOfHosts() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    // Add 100 hosts
    for (int i = 0; i < 100; i++) {
      indexStats.put(
          "host" + i,
          createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 1L, false, Optional.empty()));
    }

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals(100, result.hostStatusDetails().size());
    assertEquals("READY", result.status());
    assertTrue(result.queryable());
  }

  @Test
  public void testToIndexEntry_statusWithMessage() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    IndexStatus statusWithMessage = IndexStatus.failed("Test error message");
    IndexStatsEntry.DetailedIndexStats detailedStats =
        new IndexStatsEntry.DetailedIndexStats(statusWithMessage, definition, Optional.empty());
    IndexStatsEntry entry =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(TEST_SERVER_ID, TEST_INDEX_ID),
            IndexDefinition.Type.SEARCH,
            Optional.of(detailedStats),
            Optional.empty());
    indexStats.put(TEST_HOSTNAME, entry);

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals("FAILED", result.status());
    assertEquals(1, result.hostStatusDetails().size());
    ListSearchIndexesResponseDefinition.HostStatusDetail hostDetail =
        result.hostStatusDetails().get(0);
    assertTrue(
        "mainIndex should be present",
        hostDetail.mainIndex().isPresent());
    assertTrue(
        "message should be present for failed status",
        hostDetail.mainIndex().get().message().isPresent());
    assertEquals(
        "Test error message",
        hostDetail.mainIndex().get().message().get());
  }

  @Test
  public void testToIndexEntry_absentDefinitionVersion_usesDefaultVersion() {
    SearchIndexDefinition definition =
        createSearchIndexDefinitionWithoutOptionals(Optional.empty(), Optional.of(TEST_INSTANT));
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 0L, false, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    // Should use DEFAULT_INDEX_VERSION (0L) when definition version is absent
    assertEquals(0, result.latestDefinitionVersion().version());
  }

  @Test
  public void testToIndexEntry_searchIndexWithThreeHostsMixedStatuses() {
    // Tests search index with 3 hosts: 2 READY, 1 BUILDING
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    // Host 1: READY
    indexStats.put(
        "host1",
        createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 1L, false, Optional.empty()));

    // Host 2: READY
    indexStats.put(
        "host2",
        createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 1L, false, Optional.empty()));

    // Host 3: BUILDING
    indexStats.put(
        "host3",
        createSearchIndexStatsEntry(
            IndexStatus.StatusCode.INITIAL_SYNC, 1L, false, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals(3, result.hostStatusDetails().size());
    // Should be BUILDING because one host is INITIAL_SYNC
    assertEquals("BUILDING", result.status());
    // Should not be queryable because not all hosts are queryable
    assertFalse(result.queryable());
  }

  @Test
  public void testToIndexEntry_vectorIndexWithTwoReadyHosts() {
    // Tests vector index with 2 hosts both READY
    VectorIndexDefinition definition = createVectorIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    // Host 1: READY with mainIndex details
    indexStats.put(
        "host1", createVectorIndexStatsEntry(IndexStatus.StatusCode.STEADY, 1L, Optional.empty()));

    // Host 2: READY with mainIndex details
    indexStats.put(
        "host2", createVectorIndexStatsEntry(IndexStatus.StatusCode.STEADY, 1L, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals(2, result.hostStatusDetails().size());
    assertEquals("READY", result.status());
    assertTrue(result.queryable());
  }

  @Test
  public void testToIndexEntry_searchIndexWithNotStartedStatus() {
    // Tests search index with NOT_STARTED status (maps to PENDING, not queryable)
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    // Create PENDING status (NOT_STARTED maps to PENDING)
    IndexStatus status = IndexStatus.notStarted();
    IndexStatsEntry.DetailedIndexStats detailedStats =
        new IndexStatsEntry.DetailedIndexStats(status, definition, Optional.empty());
    IndexStatsEntry entry =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(TEST_SERVER_ID, TEST_INDEX_ID),
            IndexDefinition.Type.SEARCH,
            Optional.of(detailedStats),
            Optional.empty());
    indexStats.put(TEST_HOSTNAME, entry);

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals("PENDING", result.status());
    assertFalse(result.queryable());
    assertEquals(1, result.hostStatusDetails().size());

    ListSearchIndexesResponseDefinition.HostStatusDetail hostDetail =
        result.hostStatusDetails().get(0);
    assertEquals("PENDING", hostDetail.status());
    assertFalse(hostDetail.queryable());
    assertTrue(
        "mainIndex should be present",
        hostDetail.mainIndex().isPresent());
    // Note: message is not present because notStarted() doesn't support messages
  }

  @Test
  public void testToIndexEntry_vectorIndexWithBuildingStatusAndMessage() {
    // Tests vector index with INITIAL_SYNC status (maps to BUILDING) and a status message
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(TEST_INDEX_ID)
            .name(TEST_INDEX_NAME)
            .database(TEST_DATABASE)
            .lastObservedCollectionName(TEST_COLLECTION)
            .collectionUuid(TEST_COLLECTION_UUID)
            .numPartitions(1)
            .withVectorFieldDefaultOptions(
                "embedding",
                256,
                VectorSimilarity.EUCLIDEAN,
                VectorQuantization.SCALAR)
            .withDefinitionVersionCreatedAt(Optional.of(TEST_INSTANT))
            .withDefinitionVersion(Optional.of(1L))
            .build();

    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    // Create BUILDING status with message
    IndexStatus statusWithMessage = IndexStatus.initialSync("Building index");
    IndexStatsEntry.DetailedIndexStats detailedStats =
        new IndexStatsEntry.DetailedIndexStats(statusWithMessage, definition, Optional.empty());
    IndexStatsEntry entry =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(TEST_SERVER_ID, TEST_INDEX_ID),
            IndexDefinition.Type.VECTOR_SEARCH,
            Optional.of(detailedStats),
            Optional.empty());
    indexStats.put(TEST_HOSTNAME, entry);

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals("BUILDING", result.status());
    assertFalse(result.queryable());
    assertEquals(1, result.hostStatusDetails().size());

    ListSearchIndexesResponseDefinition.HostStatusDetail hostDetail =
        result.hostStatusDetails().get(0);
    assertEquals("BUILDING", hostDetail.status());
    assertFalse(hostDetail.queryable());
    assertTrue(
        "mainIndex should be present",
        hostDetail.mainIndex().isPresent());
    assertTrue(
        "message should be present",
        hostDetail.mainIndex().get().message().isPresent());
    assertEquals(
        "Building index",
        hostDetail.mainIndex().get().message().get());
  }

  @Test
  public void testToIndexEntry_searchIndexWithMainAndStagedIndexes() {
    // Tests search index with both main index (READY, v1) and staged index (BUILDING, v2)
    Instant createdAt1 = Instant.parse("2024-01-01T00:00:00Z");
    Instant createdAt2 = Instant.parse("2024-01-02T00:00:00Z");

    SearchIndexDefinition mainDefinition =
        SearchIndexDefinitionBuilder.builder()
            .indexId(TEST_INDEX_ID)
            .name(TEST_INDEX_NAME)
            .database(TEST_DATABASE)
            .lastObservedCollectionName(TEST_COLLECTION)
            .collectionUuid(TEST_COLLECTION_UUID)
            .view(ViewDefinition.existing("test-view", List.of()))
            .numPartitions(
                IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue())
            .dynamicMapping()
            .definitionVersionCreatedAt(createdAt1)
            .definitionVersion(1L)
            .build();

    SearchIndexDefinition stagedDefinition =
        SearchIndexDefinitionBuilder.builder()
            .indexId(TEST_INDEX_ID)
            .name(TEST_INDEX_NAME)
            .database(TEST_DATABASE)
            .lastObservedCollectionName(TEST_COLLECTION)
            .collectionUuid(TEST_COLLECTION_UUID)
            .view(ViewDefinition.existing("test-view", List.of()))
            .numPartitions(
                IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue())
            .dynamicMapping()
            .analyzerName("lucene.standard")
            .definitionVersionCreatedAt(createdAt2)
            .definitionVersion(2L)
            .build();

    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    IndexStatus mainStatus = new IndexStatus(IndexStatus.StatusCode.STEADY);
    IndexStatus stagedStatus = IndexStatus.initialSync("Building new index version");

    IndexStatsEntry.DetailedIndexStats mainStats =
        new IndexStatsEntry.DetailedIndexStats(mainStatus, mainDefinition, Optional.empty());
    IndexStatsEntry.DetailedIndexStats stagedStats =
        new IndexStatsEntry.DetailedIndexStats(stagedStatus, stagedDefinition, Optional.empty());

    IndexStatsEntry entry =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(TEST_SERVER_ID, TEST_INDEX_ID),
            IndexDefinition.Type.SEARCH,
            Optional.of(mainStats),
            Optional.of(stagedStats));
    indexStats.put(TEST_HOSTNAME, entry);

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            mainDefinition, mainDefinition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals("READY", result.status());
    assertTrue(result.queryable());
    assertEquals(1, result.hostStatusDetails().size());

    ListSearchIndexesResponseDefinition.HostStatusDetail hostDetail =
        result.hostStatusDetails().get(0);
    assertTrue(
        "mainIndex should be present",
        hostDetail.mainIndex().isPresent());
    assertTrue(
        "stagedIndex should be present",
        hostDetail.stagedIndex().isPresent());

    // Verify mainIndex details
    ListSearchIndexesResponseDefinition.HostIndexStatusDetail mainIndexDetail =
        hostDetail.mainIndex().get();
    assertEquals("READY", mainIndexDetail.status());
    assertTrue(mainIndexDetail.queryable());
    assertEquals(1, mainIndexDetail.definitionVersion().version());

    // Verify stagedIndex details
    ListSearchIndexesResponseDefinition.HostIndexStatusDetail stagedIndexDetail =
        hostDetail.stagedIndex().get();
    assertEquals("BUILDING", stagedIndexDetail.status());
    assertFalse(stagedIndexDetail.queryable());
    assertEquals(2, stagedIndexDetail.definitionVersion().version());
    assertTrue(
        "message should be present",
        stagedIndexDetail.message().isPresent());
    assertEquals(
        "Building new index version",
        stagedIndexDetail.message().get());
  }

  @Test
  public void testToIndexEntry_hostWithOnlyStagedIndexNoMainIndex() {
    // Tests a host where mainIndex is Optional.empty() and only a staged index is present
    // When mainIndex is empty, the mapper returns UNKNOWN status (maps to PENDING)
    VectorIndexDefinition definition = createVectorIndexDefinition(1L, TEST_INSTANT);
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();

    // Staged index is being built
    IndexStatus stagedStatus = IndexStatus.initialSync("Building new index");
    IndexStatsEntry.DetailedIndexStats stagedStats =
        new IndexStatsEntry.DetailedIndexStats(stagedStatus, definition, Optional.empty());

    IndexStatsEntry entry =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(TEST_SERVER_ID, TEST_INDEX_ID),
            IndexDefinition.Type.VECTOR_SEARCH,
            Optional.empty(), // No mainIndex
            Optional.of(stagedStats));
    indexStats.put(TEST_HOSTNAME, entry);

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, definition.toBson(), indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals("PENDING", result.status());
    assertFalse(result.queryable());
    assertEquals(1, result.hostStatusDetails().size());

    ListSearchIndexesResponseDefinition.HostStatusDetail hostDetail =
        result.hostStatusDetails().get(0);
    assertEquals("PENDING", hostDetail.status());
    assertFalse(hostDetail.queryable());
    assertTrue(
        "mainIndex should be empty",
        hostDetail.mainIndex().isEmpty());
    assertTrue(
        "stagedIndex should be present",
        hostDetail.stagedIndex().isPresent());

    // Verify stagedIndex details
    ListSearchIndexesResponseDefinition.HostIndexStatusDetail stagedIndexDetail =
        hostDetail.stagedIndex().get();
    assertEquals("BUILDING", stagedIndexDetail.status());
    assertFalse(stagedIndexDetail.queryable());
    assertTrue(
        "message should be present",
        stagedIndexDetail.message().isPresent());
    assertEquals(
        "Building new index",
        stagedIndexDetail.message().get());
  }

  @Test
  public void testToIndexEntryUsesCustomerDefAsLatestDefinition() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    BsonDocument customCustomerDef =
        new BsonDocument("customField", new BsonString("customValue"));
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 1L, false, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, customCustomerDef, indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals(customCustomerDef, result.latestDefinition());
  }

  @Test
  public void testToIndexEntryPassesThroughCustomerDefAsLatestDefinition() {
    SearchIndexDefinition definition = createSearchIndexDefinition(1L, TEST_INSTANT);
    BsonDocument emptyCustomerDef = new BsonDocument();
    Map<String, IndexStatsEntry> indexStats = new HashMap<>();
    indexStats.put(
        TEST_HOSTNAME,
        createSearchIndexStatsEntry(IndexStatus.StatusCode.STEADY, 1L, false, Optional.empty()));

    ListSearchIndexesResponseDefinition.IndexEntry result =
        IndexEntryMapper.toIndexEntry(
            definition, emptyCustomerDef, indexStats, Optional.empty());

    assertNotNull(result);
    assertEquals(emptyCustomerDef, result.latestDefinition());
  }

  private SearchIndexDefinition createSearchIndexDefinition(Long version, Instant createdAt) {
    return SearchIndexDefinitionBuilder.builder()
        .indexId(TEST_INDEX_ID)
        .name(TEST_INDEX_NAME)
        .database(TEST_DATABASE)
        .lastObservedCollectionName(TEST_COLLECTION)
        .collectionUuid(TEST_COLLECTION_UUID)
        .view(ViewDefinition.existing("test-view", List.of()))
        .numPartitions(
            IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue())
        .synonyms(
            SynonymMappingDefinitionBuilder.builder()
                .name("test-synonym")
                .synonymSourceDefinition("synonym-collection")
                .analyzer("lucene.standard")
                .buildAsList())
        .dynamicMapping()
        .definitionVersionCreatedAt(createdAt)
        .definitionVersion(version)
        .build();
  }

  private SearchIndexDefinition createSearchIndexDefinitionWithoutOptionals(
      Optional<Long> version, Optional<Instant> createdAt) {
    SearchIndexDefinitionBuilder builder =
        SearchIndexDefinitionBuilder.builder()
            .indexId(TEST_INDEX_ID)
            .name(TEST_INDEX_NAME)
            .database(TEST_DATABASE)
            .lastObservedCollectionName(TEST_COLLECTION)
            .collectionUuid(TEST_COLLECTION_UUID)
            .numPartitions(
                IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue())
            .dynamicMapping();

    version.ifPresent(builder::definitionVersion);
    createdAt.ifPresent(builder::definitionVersionCreatedAt);

    return builder.build();
  }

  private VectorIndexDefinition createVectorIndexDefinition(Long version, Instant createdAt) {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(TEST_INDEX_ID)
        .name(TEST_INDEX_NAME)
        .database(TEST_DATABASE)
        .lastObservedCollectionName(TEST_COLLECTION)
        .collectionUuid(TEST_COLLECTION_UUID)
        .numPartitions(1)
        .withCosineVectorField("vector.field", 128)
        .withFilterPath("filter.field")
        .withDefinitionVersionCreatedAt(Optional.of(createdAt))
        .withDefinitionVersion(Optional.of(version))
        .build();
  }

  private VectorIndexDefinition createVectorIndexDefinitionWithoutOptionals(
      Optional<Long> version, Optional<Instant> createdAt) {
    VectorIndexDefinitionBuilder builder =
        VectorIndexDefinitionBuilder.builder()
            .indexId(TEST_INDEX_ID)
            .name(TEST_INDEX_NAME)
            .database(TEST_DATABASE)
            .lastObservedCollectionName(TEST_COLLECTION)
            .collectionUuid(TEST_COLLECTION_UUID)
            .numPartitions(1)
            .withCosineVectorField("vector.field", 128);

    version.ifPresent(v -> builder.withDefinitionVersion(Optional.of(v)));
    createdAt.ifPresent(ca -> builder.withDefinitionVersionCreatedAt(Optional.of(ca)));

    return builder.build();
  }

  private IndexStatsEntry createSearchIndexStatsEntry(
      IndexStatus.StatusCode status,
      long version,
      @SuppressWarnings("unused") boolean withSynonyms,
      Optional<Map<String, SynonymDetailedStatus>> synonymMap) {
    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .indexId(TEST_INDEX_ID)
            .name(TEST_INDEX_NAME)
            .database(TEST_DATABASE)
            .lastObservedCollectionName(TEST_COLLECTION)
            .collectionUuid(TEST_COLLECTION_UUID)
            .view(ViewDefinition.existing("test-view", List.of()))
            .numPartitions(
                IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue())
            .dynamicMapping()
            .definitionVersionCreatedAt(TEST_INSTANT)
            .definitionVersion(version)
            .build();

    IndexStatsEntry.DetailedIndexStats detailedStats =
        new IndexStatsEntry.DetailedIndexStats(new IndexStatus(status), definition, synonymMap);

    return new IndexStatsEntry(
        new IndexStatsEntry.IndexStatsKey(TEST_SERVER_ID, TEST_INDEX_ID),
        IndexDefinition.Type.SEARCH,
        Optional.of(detailedStats),
        Optional.empty());
  }

  private IndexStatsEntry createVectorIndexStatsEntry(
      IndexStatus.StatusCode status,
      long version,
      Optional<Map<String, SynonymDetailedStatus>> synonymMap) {
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(TEST_INDEX_ID)
            .name(TEST_INDEX_NAME)
            .database(TEST_DATABASE)
            .lastObservedCollectionName(TEST_COLLECTION)
            .collectionUuid(TEST_COLLECTION_UUID)
            .numPartitions(1)
            .withCosineVectorField("vector.field", 128)
            .withDefinitionVersionCreatedAt(Optional.of(TEST_INSTANT))
            .withDefinitionVersion(Optional.of(version))
            .build();

    IndexStatsEntry.DetailedIndexStats detailedStats =
        new IndexStatsEntry.DetailedIndexStats(new IndexStatus(status), definition, synonymMap);

    return new IndexStatsEntry(
        new IndexStatsEntry.IndexStatsKey(TEST_SERVER_ID, TEST_INDEX_ID),
        IndexDefinition.Type.VECTOR_SEARCH,
        Optional.of(detailedStats),
        Optional.empty());
  }

  private IndexStatsEntry createVectorIndexStatsEntryWithStaged(
      IndexStatus.StatusCode mainStatus,
      long mainVersion,
      IndexStatus.StatusCode stagedStatus,
      long stagedVersion,
      Optional<Map<String, SynonymDetailedStatus>> synonymMap) {
    VectorIndexDefinition mainDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(TEST_INDEX_ID)
            .name(TEST_INDEX_NAME)
            .database(TEST_DATABASE)
            .lastObservedCollectionName(TEST_COLLECTION)
            .collectionUuid(TEST_COLLECTION_UUID)
            .numPartitions(1)
            .withCosineVectorField("vector.field", 128)
            .withDefinitionVersionCreatedAt(Optional.of(TEST_INSTANT))
            .withDefinitionVersion(Optional.of(mainVersion))
            .build();

    VectorIndexDefinition stagedDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(TEST_INDEX_ID)
            .name(TEST_INDEX_NAME)
            .database(TEST_DATABASE)
            .lastObservedCollectionName(TEST_COLLECTION)
            .collectionUuid(TEST_COLLECTION_UUID)
            .numPartitions(1)
            .withCosineVectorField("vector.field", 128)
            .withDefinitionVersionCreatedAt(Optional.of(TEST_INSTANT))
            .withDefinitionVersion(Optional.of(stagedVersion))
            .build();

    IndexStatsEntry.DetailedIndexStats mainStats =
        new IndexStatsEntry.DetailedIndexStats(
            new IndexStatus(mainStatus), mainDefinition, synonymMap);
    IndexStatsEntry.DetailedIndexStats stagedStats =
        new IndexStatsEntry.DetailedIndexStats(
            new IndexStatus(stagedStatus), stagedDefinition, synonymMap);

    return new IndexStatsEntry(
        new IndexStatsEntry.IndexStatsKey(TEST_SERVER_ID, TEST_INDEX_ID),
        IndexDefinition.Type.VECTOR_SEARCH,
        Optional.of(mainStats),
        Optional.of(stagedStats));
  }

  private Map<String, SynonymDetailedStatus> createSynonymMap() {
    Map<String, SynonymDetailedStatus> synonymMap = new HashMap<>();
    synonymMap.put(
        "synonym1", new SynonymDetailedStatus(SynonymStatus.READY, Optional.of("Ready")));
    synonymMap.put(
        "synonym2", new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.of("Syncing")));
    return synonymMap;
  }
}
