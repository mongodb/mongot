package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoException;
import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.CatalogAccessGuard;
import com.xgen.mongot.catalogservice.IndexStats;
import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.catalogservice.ServerState;
import com.xgen.mongot.catalogservice.TopologyMismatchException;
import com.xgen.mongot.catalogservice.schema.AuthoritativeIndexKey;
import com.xgen.mongot.catalogservice.schema.IndexStatsEntry;
import com.xgen.mongot.catalogservice.schema.ServerStateEntry;
import com.xgen.mongot.index.AggregatedIndexMetrics;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.MongoDbMetadataClient;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SynonymMappingDefinitionBuilder;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

public class DroppedCollectionIndexCleanerTest {

  private MetadataService metadataService;
  private ServerState serverState;
  private IndexStats indexStats;
  private AuthoritativeIndexCatalog authoritativeIndexCatalog;
  private MongoDbMetadataClient mongoDbMetadataClient;
  private CatalogAccessGuard catalogAccessGuard;
  private DroppedCollectionIndexCleaner cleaner;

  @Before
  public void setUp() throws Exception {
    this.metadataService = mock(MetadataService.class);
    this.serverState = mock(ServerState.class);
    this.indexStats = mock(IndexStats.class);
    this.authoritativeIndexCatalog = mock(AuthoritativeIndexCatalog.class);
    this.mongoDbMetadataClient = mock(MongoDbMetadataClient.class);
    this.catalogAccessGuard = mock(CatalogAccessGuard.class);
    when(this.metadataService.getServerState()).thenReturn(this.serverState);
    when(this.metadataService.getIndexStats()).thenReturn(this.indexStats);
    when(this.metadataService.getAuthoritativeIndexCatalog())
        .thenReturn(this.authoritativeIndexCatalog);
    this.cleaner =
        new DroppedCollectionIndexCleaner(
            this.metadataService, this.mongoDbMetadataClient, this.catalogAccessGuard);
  }

  @Test
  public void indexDeletedByAllServers_AllServersHaveEmptyIndexes_ReturnsTrue() throws Exception {
    ObjectId serverId = new ObjectId();
    ObjectId indexId = new ObjectId();
    IndexStatsEntry entry =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(serverId, indexId),
            IndexDefinition.Type.SEARCH,
            Optional.empty(),
            Optional.empty());
    when(this.indexStats.list(IndexStatsEntry.indexIdFilter(indexId))).thenReturn(List.of(entry));

    assertTrue(this.cleaner.indexDeletedByAllServers(indexId, Set.of(serverId)));
  }

  @Test
  public void indexDeletedByAllServers_ServerHasNoStatsEntry_ReturnsFalse() throws Exception {
    ObjectId serverId = new ObjectId();
    ObjectId indexId = new ObjectId();
    // No stats entry for serverId
    when(this.indexStats.list(IndexStatsEntry.indexIdFilter(indexId)))
        .thenReturn(Collections.emptyList());

    assertFalse(this.cleaner.indexDeletedByAllServers(indexId, Set.of(serverId)));
  }

  @Test
  public void indexDeletedByAllServers_OneServerOfTwoLacksEntry_ReturnsFalse() throws Exception {
    ObjectId serverId1 = new ObjectId();
    ObjectId serverId2 = new ObjectId();
    ObjectId indexId = new ObjectId();
    // Only server1 has an entry
    IndexStatsEntry entry =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(serverId1, indexId),
            IndexDefinition.Type.SEARCH,
            Optional.empty(),
            Optional.empty());
    when(this.indexStats.list(IndexStatsEntry.indexIdFilter(indexId))).thenReturn(List.of(entry));

    assertFalse(this.cleaner.indexDeletedByAllServers(indexId, Set.of(serverId1, serverId2)));
  }

  @Test
  public void indexDeletedByAllServers_ServerHasActiveMainIndex_ReturnsFalse() throws Exception {
    ObjectId serverId = new ObjectId();
    ObjectId indexId = new ObjectId();
    IndexStatsEntry entry =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(serverId, indexId),
            IndexDefinition.Type.SEARCH,
            Optional.of(
                new IndexStatsEntry.DetailedIndexStats(
                    new IndexStatus(IndexStatus.StatusCode.STEADY),
                    createSearchIndexDefinition(indexId, "test-db", UUID.randomUUID()),
                    Optional.empty())),
            Optional.empty());
    when(this.indexStats.list(IndexStatsEntry.indexIdFilter(indexId))).thenReturn(List.of(entry));

    assertFalse(this.cleaner.indexDeletedByAllServers(indexId, Set.of(serverId)));
  }

  /**
   * A server whose mainIndex is present but carries DOES_NOT_EXIST status counts as agreed — it has
   * acknowledged the index is gone but hasn't yet dropped it from its local catalog (which itself
   * only happens after the AIC entry is deleted).
   */
  @Test
  public void indexDeletedByAllServers_MainIndexCarriesDoesNotExistStatus_ReturnsTrue()
      throws Exception {
    ObjectId serverId = new ObjectId();
    ObjectId indexId = new ObjectId();
    IndexStatsEntry entry =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(serverId, indexId),
            IndexDefinition.Type.SEARCH,
            Optional.of(
                new IndexStatsEntry.DetailedIndexStats(
                    IndexStatus.doesNotExist(IndexStatus.Reason.INDEX_DROPPED),
                    createSearchIndexDefinition(indexId, "test-db", UUID.randomUUID()),
                    Optional.empty())),
            Optional.empty());
    when(this.indexStats.list(IndexStatsEntry.indexIdFilter(indexId))).thenReturn(List.of(entry));

    assertTrue(this.cleaner.indexDeletedByAllServers(indexId, Set.of(serverId)));
  }

  @Test
  public void indexDeletedByAllServers_EmptyActiveServers_ReturnsTrue() throws Exception {
    ObjectId indexId = new ObjectId();
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());

    // allMatch over an empty set is vacuously true. The caller guards against
    // empty activeServerIds before reaching this method.
    assertTrue(this.cleaner.indexDeletedByAllServers(indexId, Set.of()));
  }

  @Test
  public void indexDeletedByAllServers_MetadataException_ReturnsFalse() throws Exception {
    ObjectId indexId = new ObjectId();
    doThrow(MetadataServiceException.createTransient(new RuntimeException("DB error")))
        .when(this.indexStats)
        .list(any());

    assertFalse(this.cleaner.indexDeletedByAllServers(indexId, Set.of(new ObjectId())));
  }

  @Test
  public void getIndexesDroppedByAllServersGroupedByDb_NoIndexesPassConsensus_ReturnsEmpty()
      throws Exception {
    ObjectId serverId = new ObjectId();
    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createSearchIndexDefinition(indexId, "db1", UUID.randomUUID());
    IndexInformation droppedInfo = droppedIndex(def);

    // No stats entry for serverId means consensus fails
    when(this.indexStats.list(IndexStatsEntry.indexIdFilter(indexId)))
        .thenReturn(Collections.emptyList());

    Map<String, List<IndexInformation>> result =
        this.cleaner.getIndexesDroppedByAllServersGroupedByDb(
            List.of(droppedInfo), Set.of(serverId));

    assertTrue(result.isEmpty());
  }

  @Test
  public void getIndexesDroppedByAllServersGroupedByDb_ConsensusReached_GroupsIndexesByDatabase()
      throws Exception {
    ObjectId serverId = new ObjectId();
    ObjectId indexId1 = new ObjectId();
    ObjectId indexId2 = new ObjectId();
    ObjectId indexId3 = new ObjectId();

    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();
    UUID uuid3 = UUID.randomUUID();

    SearchIndexDefinition def1 = createSearchIndexDefinition(indexId1, "db-a", uuid1);
    SearchIndexDefinition def2 = createSearchIndexDefinition(indexId2, "db-a", uuid2);
    SearchIndexDefinition def3 = createSearchIndexDefinition(indexId3, "db-b", uuid3);

    IndexInformation dropped1 = droppedIndex(def1);
    IndexInformation dropped2 = droppedIndex(def2);
    IndexInformation dropped3 = droppedIndex(def3);

    stubServerWithNoActiveIndexData(serverId, indexId1);
    stubServerWithNoActiveIndexData(serverId, indexId2);
    stubServerWithNoActiveIndexData(serverId, indexId3);

    Map<String, List<IndexInformation>> result =
        this.cleaner.getIndexesDroppedByAllServersGroupedByDb(
            List.of(dropped1, dropped2, dropped3), Set.of(serverId));

    assertEquals(2, result.size());
    assertEquals(2, result.get("db-a").size());
    assertEquals(1, result.get("db-b").size());
  }

  @Test
  public void getIndexesDroppedByAllServersGroupedByDb_SomeIndexesFailConsensus_ExcludesThose()
      throws Exception {
    ObjectId serverId = new ObjectId();
    ObjectId indexId1 = new ObjectId();
    ObjectId indexId2 = new ObjectId();

    SearchIndexDefinition def1 = createSearchIndexDefinition(indexId1, "db-a", UUID.randomUUID());
    SearchIndexDefinition def2 = createSearchIndexDefinition(indexId2, "db-a", UUID.randomUUID());

    IndexInformation dropped1 = droppedIndex(def1);
    IndexInformation dropped2 = droppedIndex(def2);

    stubServerWithNoActiveIndexData(serverId, indexId1);
    // indexId2 has no entry, fails consensus
    when(this.indexStats.list(IndexStatsEntry.indexIdFilter(indexId2)))
        .thenReturn(Collections.emptyList());

    Map<String, List<IndexInformation>> result =
        this.cleaner.getIndexesDroppedByAllServersGroupedByDb(
            List.of(dropped1, dropped2), Set.of(serverId));

    assertEquals(1, result.get("db-a").size());
    assertEquals(indexId1, result.get("db-a").get(0).getDefinition().getIndexId());
  }

  @Test
  public void deleteDroppedIndexesForDatabase_CollectionUuidAbsent_DeletesAicEntry()
      throws Exception {
    UUID absentUuid = UUID.randomUUID();
    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createSearchIndexDefinition(indexId, "test-db", absentUuid);
    IndexInformation info = droppedIndex(def);

    MongoDbCollectionInfos collectionInfos = mock(MongoDbCollectionInfos.class);
    when(collectionInfos.getAllCollectionUuids()).thenReturn(Set.of());
    when(this.mongoDbMetadataClient.resolveCollectionInfos(Set.of("test-db")))
        .thenReturn(collectionInfos);

    this.cleaner.deleteDroppedIndexesForDatabase("test-db", List.of(info));

    AuthoritativeIndexKey expectedKey = new AuthoritativeIndexKey(absentUuid, "test-index");
    verify(this.authoritativeIndexCatalog, times(1)).deleteIndex(expectedKey);
  }

  @Test
  public void deleteDroppedIndexesForDatabase_CollectionUuidStillExists_SkipsIndex()
      throws Exception {
    UUID existingUuid = UUID.randomUUID();
    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createSearchIndexDefinition(indexId, "test-db", existingUuid);
    IndexInformation info = droppedIndex(def);

    MongoDbCollectionInfos collectionInfos = mock(MongoDbCollectionInfos.class);
    when(collectionInfos.getAllCollectionUuids()).thenReturn(Set.of(existingUuid));
    when(this.mongoDbMetadataClient.resolveCollectionInfos(any())).thenReturn(collectionInfos);

    this.cleaner.deleteDroppedIndexesForDatabase("test-db", List.of(info));

    verify(this.authoritativeIndexCatalog, never()).deleteIndex(any());
  }

  @Test
  public void deleteDroppedIndexesForDatabase_DeleteFailsForOne_ContinuesToNext() throws Exception {
    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();
    ObjectId indexId1 = new ObjectId();
    ObjectId indexId2 = new ObjectId();
    SearchIndexDefinition def1 = createSearchIndexDefinition(indexId1, "test-db", uuid1, "idx-one");
    SearchIndexDefinition def2 = createSearchIndexDefinition(indexId2, "test-db", uuid2, "idx-two");

    MongoDbCollectionInfos collectionInfos = mock(MongoDbCollectionInfos.class);
    when(collectionInfos.getAllCollectionUuids()).thenReturn(Set.of());
    when(this.mongoDbMetadataClient.resolveCollectionInfos(any())).thenReturn(collectionInfos);

    AuthoritativeIndexKey key1 = new AuthoritativeIndexKey(uuid1, "idx-one");
    doThrow(MetadataServiceException.createTransient(new RuntimeException("delete failed")))
        .when(this.authoritativeIndexCatalog)
        .deleteIndex(key1);

    this.cleaner.deleteDroppedIndexesForDatabase(
        "test-db", List.of(droppedIndex(def1), droppedIndex(def2)));

    verify(this.authoritativeIndexCatalog, times(1)).deleteIndex(key1);
    verify(this.authoritativeIndexCatalog, times(1))
        .deleteIndex(new AuthoritativeIndexKey(uuid2, "idx-two"));
  }

  @Test
  public void deleteDroppedIndexesForDatabase_ResolveCollectionInfosFails_SkipsDatabase()
      throws Exception {
    doThrow(new CheckedMongoException(new MongoException("mongod unavailable")))
        .when(this.mongoDbMetadataClient)
        .resolveCollectionInfos(any());

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createSearchIndexDefinition(indexId, "test-db", UUID.randomUUID());

    this.cleaner.deleteDroppedIndexesForDatabase("test-db", List.of(droppedIndex(def)));

    verify(this.authoritativeIndexCatalog, never()).deleteIndex(any());
  }

  @Test
  public void deleteIndexesForDroppedCollections_NoDroppedIndexes_NoOp() throws Exception {
    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createSearchIndexDefinition(indexId, "test-db", UUID.randomUUID());
    IndexInformation steadyInfo =
        new IndexInformation.Search(
            def,
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            Collections.emptyList(),
            new AggregatedIndexMetrics(0L, 0L, new org.bson.BsonTimestamp(0L), 0L),
            Optional.empty(),
            Optional.empty(),
            Collections.emptyMap());

    this.cleaner.deleteIndexesForDroppedCollections(List.of(steadyInfo));

    verify(this.serverState, never()).list(any());
    verify(this.authoritativeIndexCatalog, never()).deleteIndex(any());
  }

  @Test
  public void deleteIndexesForDroppedCollections_TopologyMismatch_NoOp() throws Exception {
    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createSearchIndexDefinition(indexId, "test-db", UUID.randomUUID());

    doThrow(new TopologyMismatchException("mismatch"))
        .when(this.catalogAccessGuard)
        .requireTopologyMatch();

    this.cleaner.deleteIndexesForDroppedCollections(List.of(droppedIndex(def)));

    verify(this.serverState, never()).list(any());
    verify(this.authoritativeIndexCatalog, never()).deleteIndex(any());
  }

  @Test
  public void deleteIndexesForDroppedCollections_TopologyCheckFails_NoOp() throws Exception {
    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createSearchIndexDefinition(indexId, "test-db", UUID.randomUUID());

    doThrow(new CheckedMongoException(new MongoException("unavailable")))
        .when(this.catalogAccessGuard)
        .requireTopologyMatch();

    this.cleaner.deleteIndexesForDroppedCollections(List.of(droppedIndex(def)));

    verify(this.serverState, never()).list(any());
    verify(this.authoritativeIndexCatalog, never()).deleteIndex(any());
  }

  @Test
  public void deleteIndexesForDroppedCollections_ActiveServersFetchFails_NoOp() throws Exception {
    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createSearchIndexDefinition(indexId, "test-db", UUID.randomUUID());

    doThrow(MetadataServiceException.createTransient(new RuntimeException("list failed")))
        .when(this.serverState)
        .list(any());

    this.cleaner.deleteIndexesForDroppedCollections(List.of(droppedIndex(def)));

    verify(this.mongoDbMetadataClient, never()).resolveCollectionInfos(any());
    verify(this.authoritativeIndexCatalog, never()).deleteIndex(any());
  }

  @Test
  public void deleteIndexesForDroppedCollections_NoActiveServers_NoOp() throws Exception {
    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createSearchIndexDefinition(indexId, "test-db", UUID.randomUUID());

    when(this.serverState.list(any())).thenReturn(Collections.emptyList());

    this.cleaner.deleteIndexesForDroppedCollections(List.of(droppedIndex(def)));

    verify(this.mongoDbMetadataClient, never()).resolveCollectionInfos(any());
    verify(this.authoritativeIndexCatalog, never()).deleteIndex(any());
  }

  @Test
  public void deleteIndexesForDroppedCollections_AllConditionsMet_DeletesAicEntry()
      throws Exception {
    ObjectId serverId = new ObjectId();
    ObjectId indexId = new ObjectId();
    UUID collectionUuid = UUID.randomUUID();
    SearchIndexDefinition def = createSearchIndexDefinition(indexId, "test-db", collectionUuid);

    when(this.serverState.list(any()))
        .thenReturn(List.of(new ServerStateEntry(serverId, "srv", Instant.now())));
    stubServerWithNoActiveIndexData(serverId, indexId);

    MongoDbCollectionInfos collectionInfos = mock(MongoDbCollectionInfos.class);
    when(collectionInfos.getAllCollectionUuids()).thenReturn(Set.of());
    when(this.mongoDbMetadataClient.resolveCollectionInfos(any())).thenReturn(collectionInfos);

    this.cleaner.deleteIndexesForDroppedCollections(List.of(droppedIndex(def)));

    verify(this.authoritativeIndexCatalog, times(1))
        .deleteIndex(new AuthoritativeIndexKey(collectionUuid, "test-index"));
  }

  @Test
  public void
      deleteIndexesForDroppedCollections_MultipleIndexesSameDatabase_BatchesResolveCollectionInfos()
          throws Exception {
    ObjectId serverId = new ObjectId();
    ObjectId indexId1 = new ObjectId();
    ObjectId indexId2 = new ObjectId();
    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();
    SearchIndexDefinition def1 = createSearchIndexDefinition(indexId1, "same-db", uuid1, "idx-one");
    SearchIndexDefinition def2 = createSearchIndexDefinition(indexId2, "same-db", uuid2, "idx-two");

    when(this.serverState.list(any()))
        .thenReturn(List.of(new ServerStateEntry(serverId, "srv", Instant.now())));
    stubServerWithNoActiveIndexData(serverId, indexId1);
    stubServerWithNoActiveIndexData(serverId, indexId2);

    MongoDbCollectionInfos collectionInfos = mock(MongoDbCollectionInfos.class);
    when(collectionInfos.getAllCollectionUuids()).thenReturn(Set.of());
    when(this.mongoDbMetadataClient.resolveCollectionInfos(any())).thenReturn(collectionInfos);

    this.cleaner.deleteIndexesForDroppedCollections(
        List.of(droppedIndex(def1), droppedIndex(def2)));

    // Both indexes are in the same database; resolveCollectionInfos is called exactly once.
    verify(this.mongoDbMetadataClient, times(1)).resolveCollectionInfos(Set.of("same-db"));
    verify(this.authoritativeIndexCatalog, times(1))
        .deleteIndex(new AuthoritativeIndexKey(uuid1, "idx-one"));
    verify(this.authoritativeIndexCatalog, times(1))
        .deleteIndex(new AuthoritativeIndexKey(uuid2, "idx-two"));
  }

  private SearchIndexDefinition createSearchIndexDefinition(
      ObjectId indexId, String database, UUID collectionUuid) {
    return createSearchIndexDefinition(indexId, database, collectionUuid, "test-index");
  }

  private SearchIndexDefinition createSearchIndexDefinition(
      ObjectId indexId, String database, UUID collectionUuid, String indexName) {
    return SearchIndexDefinitionBuilder.builder()
        .indexId(indexId)
        .name(indexName)
        .database(database)
        .lastObservedCollectionName("test-collection")
        .collectionUuid(collectionUuid)
        .view(ViewDefinition.existing("test-view", List.of()))
        .numPartitions(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue())
        .synonyms(
            SynonymMappingDefinitionBuilder.builder()
                .name("test-synonym")
                .synonymSourceDefinition("test-coll")
                .analyzer("lucene.english")
                .buildAsList())
        .dynamicMapping()
        .definitionVersionCreatedAt(Instant.now())
        .definitionVersion(0L)
        .build();
  }

  private IndexInformation.Search droppedIndex(SearchIndexDefinition def) {
    return new IndexInformation.Search(
        def,
        IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND),
        Collections.emptyList(),
        new AggregatedIndexMetrics(0L, 0L, new org.bson.BsonTimestamp(0L), 0L),
        Optional.empty(),
        Optional.empty(),
        Collections.emptyMap());
  }

  private void stubServerWithNoActiveIndexData(ObjectId serverId, ObjectId indexId)
      throws MetadataServiceException {
    IndexStatsEntry entry =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(serverId, indexId),
            IndexDefinition.Type.SEARCH,
            Optional.empty(),
            Optional.empty());
    when(this.indexStats.list(IndexStatsEntry.indexIdFilter(indexId))).thenReturn(List.of(entry));
  }
}
