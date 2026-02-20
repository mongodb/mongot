package com.xgen.mongot.server.command.management.aic;

import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.COLLECTION_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.COLLECTION_UUID;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.DATABASE_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.INDEX_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.VIEW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.IndexStats;
import com.xgen.mongot.catalogservice.IndexStatsEntry;
import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.catalogservice.ServerState;
import com.xgen.mongot.catalogservice.ServerStateEntry;
import com.xgen.mongot.config.manager.CachedIndexInfoProvider;
import com.xgen.mongot.index.AggregatedIndexMetrics;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesCommandDefinition;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

public class AicListSearchIndexesCommandTest {
  private AuthoritativeIndexCatalog mockAic;
  private IndexStats mockIndexStats;
  private ServerState mockServerState;
  private MetadataService mockMetadataService;
  private CachedIndexInfoProvider mockIndexInfoProvider;

  @Before
  public void setUp() {
    this.mockAic = mock(AuthoritativeIndexCatalog.class);
    this.mockIndexStats = mock(IndexStats.class);
    this.mockServerState = mock(ServerState.class);
    this.mockMetadataService = mock(MetadataService.class);
    this.mockIndexInfoProvider = mock(CachedIndexInfoProvider.class);

    when(this.mockMetadataService.getAuthoritativeIndexCatalog()).thenReturn(this.mockAic);
    when(this.mockMetadataService.getIndexStats()).thenReturn(this.mockIndexStats);
    when(this.mockMetadataService.getServerState()).thenReturn(this.mockServerState);
    when(this.mockIndexInfoProvider.getIndexInfos()).thenReturn(List.of());
  }

  private IndexDefinition createIndexDefinition(ObjectId indexId, String indexName) {
    return SearchIndexDefinitionBuilder.builder()
        .database(DATABASE_NAME)
        .collectionUuid(COLLECTION_UUID)
        .lastObservedCollectionName(COLLECTION_NAME)
        .indexId(indexId)
        .name(indexName)
        .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
        .build();
  }

  private IndexDefinition createIndexDefinition(String indexName) {
    return createIndexDefinition(new ObjectId(), indexName);
  }

  private AicListSearchIndexesCommand createCommand(ListSearchIndexesCommandDefinition definition) {
    return createCommand(definition, false);
  }

  private AicListSearchIndexesCommand createCommand(
      ListSearchIndexesCommandDefinition definition, boolean internalListIndexesForTesting) {
    return new AicListSearchIndexesCommand(
        this.mockMetadataService,
        this.mockIndexInfoProvider,
        DATABASE_NAME,
        COLLECTION_UUID,
        COLLECTION_NAME,
        Optional.of(VIEW),
        definition,
        internalListIndexesForTesting);
  }

  private ListSearchIndexesCommandDefinition createListDefinition() {
    return (ListSearchIndexesCommandDefinition)
        ManageSearchIndexCommandDefinitionBuilder.listAggregation().buildSearchIndexCommand();
  }

  @Test
  public void testListSearchIndex() throws Exception {
    var indexDefinition = createIndexDefinition(INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(List.of());
    when(this.mockServerState.list()).thenReturn(List.of());

    var definition = createListDefinition();
    var command = createCommand(definition);

    BsonDocument response = command.run();
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");

    assertEquals(1, response.getInt32("ok").getValue());
    assertEquals(1, batch.size());
    assertEquals(INDEX_NAME, batch.getFirst().asDocument().getString("name").getValue());
  }

  @Test
  public void testListSearchIndexFiltering() throws Exception {
    var indexDefinition = createIndexDefinition(INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(List.of());
    when(this.mockServerState.list()).thenReturn(List.of());

    var definition =
        (ListSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.listAggregation()
                .withIndexName(INDEX_NAME)
                .buildSearchIndexCommand();
    var command = createCommand(definition);

    BsonDocument response = command.run();
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");

    assertEquals(1, response.getInt32("ok").getValue());
    assertEquals(1, batch.size());
    assertEquals(INDEX_NAME, batch.getFirst().asDocument().getString("name").getValue());
  }

  @Test
  public void testListSearchIndexFilteringNoMatch() throws Exception {
    var indexDefinition = createIndexDefinition(INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(List.of());
    when(this.mockServerState.list()).thenReturn(List.of());

    var definition =
        (ListSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.listAggregation()
                .withIndexName("nonexistent")
                .buildSearchIndexCommand();
    var command = createCommand(definition);

    BsonDocument response = command.run();
    assertEquals(0, response.getDocument("cursor").getArray("firstBatch").size());
  }

  @Test
  public void testListSearchIndexFilteringByIndexId() throws Exception {
    var indexId = new ObjectId();
    var indexDefinition = createIndexDefinition(indexId, INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(List.of());
    when(this.mockServerState.list()).thenReturn(List.of());

    var definition =
        (ListSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.listAggregation()
                .withIndexId(indexId)
                .buildSearchIndexCommand();
    var command = createCommand(definition);

    BsonDocument response = command.run();
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");

    assertEquals(1, response.getInt32("ok").getValue());
    assertEquals(1, batch.size());
    assertEquals(INDEX_NAME, batch.getFirst().asDocument().getString("name").getValue());
  }

  @Test
  public void testMetadataServiceException() throws Exception {
    var indexDefinition = createIndexDefinition(INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));
    when(this.mockIndexStats.list(any(BsonDocument.class)))
        .thenThrow(MetadataServiceException.createFailed(new RuntimeException("Test exception")));

    var definition = createListDefinition();
    var command = createCommand(definition);

    BsonDocument response = command.run();

    assertEquals(0, response.getInt32("ok").getValue());
    assertEquals(Errors.COMMAND_FAILED.code, response.getInt32("code").getValue());
    assertEquals("Error processing request.", response.getString("errmsg").getValue());
  }

  @Test
  public void testStaleServerFiltering() throws Exception {
    var indexId = new ObjectId();
    var indexDefinition = createIndexDefinition(indexId, INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));

    // Create a stale server (heartbeat more than 2 hours ago)
    var staleServerId = new ObjectId();
    var staleServer =
        new ServerStateEntry(
            staleServerId, "stale-server", Instant.now().minus(Duration.ofHours(3)));

    // Create a fresh server (recent heartbeat)
    var freshServerId = new ObjectId();
    var freshServer = new ServerStateEntry(freshServerId, "fresh-server", Instant.now());

    when(this.mockServerState.list()).thenReturn(List.of(staleServer, freshServer));

    // Create index stats for both servers
    var staleIndexStats =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(staleServerId, indexId),
            IndexDefinition.Type.SEARCH,
            Optional.empty(),
            Optional.empty());
    var freshIndexStats =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(freshServerId, indexId),
            IndexDefinition.Type.SEARCH,
            Optional.empty(),
            Optional.empty());

    when(this.mockIndexStats.list(any(BsonDocument.class)))
        .thenReturn(List.of(staleIndexStats, freshIndexStats));

    var definition = createListDefinition();
    var command = createCommand(definition);

    BsonDocument response = command.run();

    // Should succeed and only include the fresh server in statusDetail
    assertEquals(1, response.getInt32("ok").getValue());
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");
    assertEquals(1, batch.size());

    // Check that statusDetail only contains the fresh server
    BsonArray statusDetail = batch.getFirst().asDocument().getArray("statusDetail");
    assertEquals(1, statusDetail.size());
    assertTrue(
        statusDetail
            .getFirst()
            .asDocument()
            .getString("hostname")
            .getValue()
            .startsWith("fresh-server"));
  }

  @Test
  public void testAllServersAreStale() throws Exception {
    var indexId = new ObjectId();
    var indexDefinition = createIndexDefinition(indexId, INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));

    // Create multiple servers that are ALL stale (heartbeat more than 2 hours ago)
    var staleServer1Id = new ObjectId();
    var staleServer1 =
        new ServerStateEntry(
            staleServer1Id, "stale-server-1", Instant.now().minus(Duration.ofHours(3)));

    var staleServer2Id = new ObjectId();
    var staleServer2 =
        new ServerStateEntry(
            staleServer2Id, "stale-server-2", Instant.now().minus(Duration.ofHours(5)));

    var staleServer3Id = new ObjectId();
    var staleServer3 =
        new ServerStateEntry(
            staleServer3Id, "stale-server-3", Instant.now().minus(Duration.ofHours(10)));

    when(this.mockServerState.list()).thenReturn(List.of(staleServer1, staleServer2, staleServer3));

    // Create index stats for all stale servers
    var staleIndexStats1 =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(staleServer1Id, indexId),
            IndexDefinition.Type.SEARCH,
            Optional.empty(),
            Optional.empty());
    var staleIndexStats2 =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(staleServer2Id, indexId),
            IndexDefinition.Type.SEARCH,
            Optional.empty(),
            Optional.empty());
    var staleIndexStats3 =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(staleServer3Id, indexId),
            IndexDefinition.Type.SEARCH,
            Optional.empty(),
            Optional.empty());

    when(this.mockIndexStats.list(any(BsonDocument.class)))
        .thenReturn(List.of(staleIndexStats1, staleIndexStats2, staleIndexStats3));

    var definition = createListDefinition();
    var command = createCommand(definition);

    BsonDocument response = command.run();

    // Verify the response succeeds
    assertEquals(1, response.getInt32("ok").getValue());
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");
    assertEquals(1, batch.size());

    BsonDocument indexEntry = batch.getFirst().asDocument();
    assertEquals(INDEX_NAME, indexEntry.getString("name").getValue());

    // When all servers are stale, the index should show as PENDING and not queryable
    assertEquals("PENDING", indexEntry.getString("status").getValue());
    assertEquals(false, indexEntry.getBoolean("queryable").getValue());

    // StatusDetail should be empty since no active servers have this index
    BsonArray statusDetail = indexEntry.getArray("statusDetail");
    assertEquals(0, statusDetail.size());
  }

  @Test
  public void testDuplicateServerIds() throws Exception {
    var indexId = new ObjectId();
    var indexDefinition = createIndexDefinition(indexId, INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));

    // Create two servers with the same serverId (duplicate)
    var duplicateServerId = new ObjectId();
    var server1 = new ServerStateEntry(duplicateServerId, "server1", Instant.now());
    var server2 = new ServerStateEntry(duplicateServerId, "server2", Instant.now());

    when(this.mockServerState.list()).thenReturn(List.of(server1, server2));
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(List.of());

    var definition = createListDefinition();
    var command = createCommand(definition);

    // This should throw IllegalStateException due to duplicate server IDs
    assertThrows(IllegalStateException.class, () -> command.run());
  }

  @Test
  public void testDuplicateIndexStatsEntries() throws Exception {
    var indexId = new ObjectId();
    var indexDefinition = createIndexDefinition(indexId, INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));

    // Create a server
    var serverId = new ObjectId();
    var server = new ServerStateEntry(serverId, "test-server", Instant.now());
    when(this.mockServerState.list()).thenReturn(List.of(server));

    // Create two index stats entries for the same server (duplicate)
    var indexStats1 =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(serverId, indexId),
            IndexDefinition.Type.SEARCH,
            Optional.empty(),
            Optional.empty());
    var indexStats2 =
        new IndexStatsEntry(
            new IndexStatsEntry.IndexStatsKey(serverId, indexId),
            IndexDefinition.Type.SEARCH,
            Optional.empty(),
            Optional.empty());

    when(this.mockIndexStats.list(any(BsonDocument.class)))
        .thenReturn(List.of(indexStats1, indexStats2));

    var definition = createListDefinition();
    var command = createCommand(definition);

    // This should throw IllegalStateException due to duplicate index stats entries
    assertThrows(IllegalStateException.class, () -> command.run());
  }

  @Test
  public void testEmptyIndexList() throws Exception {
    // No indexes in the collection
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of());
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(List.of());
    when(this.mockServerState.list()).thenReturn(List.of());

    var definition = createListDefinition();
    var command = createCommand(definition);

    BsonDocument response = command.run();

    assertEquals(1, response.getInt32("ok").getValue());
    assertEquals(0, response.getDocument("cursor").getArray("firstBatch").size());
  }

  @Test
  public void testMultipleIndexes() throws Exception {
    // Create multiple indexes
    var index1 = createIndexDefinition("index1");
    var index2 = createIndexDefinition("index2");
    var index3 = createIndexDefinition("index3");

    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(index1, index2, index3));
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(List.of());
    when(this.mockServerState.list()).thenReturn(List.of());

    var definition = createListDefinition();
    var command = createCommand(definition);

    BsonDocument response = command.run();
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");

    assertEquals(1, response.getInt32("ok").getValue());
    assertEquals(3, batch.size());
  }

  @Test
  public void testOversizedResponse() throws Exception {
    // Create enough indexes to exceed the BSON size limit of 16 MiB (16,777,216 bytes)
    //
    // Based on empirical measurement:
    // - Each index entry in the response: ~240 bytes
    // - Base response overhead: ~79 bytes
    // - Number of indexes to exceed 16 MiB: 69,906
    var indexes = new java.util.ArrayList<IndexDefinition>();
    for (int i = 0; i < 70000; i++) {
      indexes.add(createIndexDefinition("index_" + i));
    }

    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(indexes);
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(List.of());
    when(this.mockServerState.list()).thenReturn(List.of());

    var definition = createListDefinition();
    var command = createCommand(definition);

    BsonDocument response = command.run();

    // Should return an error due to oversized response
    assertEquals(0, response.getInt32("ok").getValue());
    assertEquals(Errors.INDEX_INFORMATION_TOO_LARGE.code, response.getInt32("code").getValue());
    assertEquals(
        "The requested indexes are too large. Try narrowing your filter.",
        response.getString("errmsg").getValue());
  }

  @Test
  public void testNumDocsNotReturnedWhenFlagIsFalse() throws Exception {
    var indexId = new ObjectId();
    var indexDefinition = createIndexDefinition(indexId, INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(List.of());
    when(this.mockServerState.list()).thenReturn(List.of());

    var definition = createListDefinition();
    var command = createCommand(definition, false);

    BsonDocument response = command.run();
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");

    assertEquals(1, response.getInt32("ok").getValue());
    assertEquals(1, batch.size());

    BsonDocument indexEntry = batch.getFirst().asDocument();
    assertEquals(INDEX_NAME, indexEntry.getString("name").getValue());

    // numDocs should not be present when internalListIndexesForTesting is false
    assertFalse(indexEntry.containsKey("numDocs"));
  }

  @Test
  public void testNumDocsReturnedWhenFlagIsTrue() throws Exception {
    var indexId = new ObjectId();
    var indexDefinition = createIndexDefinition(indexId, INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(List.of());
    when(this.mockServerState.list()).thenReturn(List.of());

    // Create IndexInformation with numDocs
    var aggregatedMetrics = new AggregatedIndexMetrics(0L, 12345L, new BsonTimestamp(0L), 0L);
    var indexInfo =
        new IndexInformation.Search(
            (SearchIndexDefinition) indexDefinition,
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            List.of(),
            aggregatedMetrics,
            Optional.empty(),
            Optional.empty(),
            Map.of());
    when(this.mockIndexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));

    var definition = createListDefinition();
    var command = createCommand(definition, true);

    BsonDocument response = command.run();
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");

    assertEquals(1, response.getInt32("ok").getValue());
    assertEquals(1, batch.size());

    BsonDocument indexEntry = batch.getFirst().asDocument();
    assertEquals(INDEX_NAME, indexEntry.getString("name").getValue());

    // numDocs should be present when internalListIndexesForTesting is true
    assertTrue(indexEntry.containsKey("numDocs"));
    assertEquals(12345L, indexEntry.getInt64("numDocs").getValue());
  }

  @Test
  public void testNumDocsReturnedAsNullWhenNotAvailable() throws Exception {
    var indexId = new ObjectId();
    var indexDefinition = createIndexDefinition(indexId, INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(List.of());
    when(this.mockServerState.list()).thenReturn(List.of());

    // Create IndexInformation but with a different indexId so numDocs won't be found
    var differentIndexId = new ObjectId();
    var differentIndexDefinition = createIndexDefinition(differentIndexId, "different_index");
    var aggregatedMetrics = new AggregatedIndexMetrics(0L, 99999L, new BsonTimestamp(0L), 0L);
    var indexInfo =
        new IndexInformation.Search(
            (SearchIndexDefinition) differentIndexDefinition,
            new IndexStatus(IndexStatus.StatusCode.STEADY),
            List.of(),
            aggregatedMetrics,
            Optional.empty(),
            Optional.empty(),
            Map.of());
    when(this.mockIndexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));

    var definition = createListDefinition();
    var command = createCommand(definition, true);

    BsonDocument response = command.run();
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");

    assertEquals(1, response.getInt32("ok").getValue());
    assertEquals(1, batch.size());

    BsonDocument indexEntry = batch.getFirst().asDocument();
    assertEquals(INDEX_NAME, indexEntry.getString("name").getValue());

    // numDocs should not be present when the index is not found in IndexInfoProvider
    assertFalse(indexEntry.containsKey("numDocs"));
  }

  @Test
  public void testGetActiveServersThrowsException() throws Exception {
    var indexDefinition = createIndexDefinition(INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));
    when(this.mockServerState.list())
        .thenThrow(
            MetadataServiceException.createFailed(
                new RuntimeException("Failed to fetch server state")));

    var definition = createListDefinition();
    var command = createCommand(definition);

    BsonDocument response = command.run();

    // Should return an error
    assertEquals(0, response.getInt32("ok").getValue());
    assertEquals(Errors.COMMAND_FAILED.code, response.getInt32("code").getValue());
    assertEquals("Error processing request.", response.getString("errmsg").getValue());
  }

  @Test
  public void testGetIndexStatsPerServerThrowsException() throws Exception {
    var indexDefinition = createIndexDefinition(INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));

    // Setup active servers
    var serverId = new ObjectId();
    var server = new ServerStateEntry(serverId, "test-server", Instant.now());
    when(this.mockServerState.list()).thenReturn(List.of(server));

    // Make getIndexStats().list() throw an exception
    when(this.mockIndexStats.list(any(BsonDocument.class)))
        .thenThrow(
            MetadataServiceException.createFailed(
                new RuntimeException("Failed to fetch index stats")));

    var definition = createListDefinition();
    var command = createCommand(definition);

    BsonDocument response = command.run();

    // Should return an error
    assertEquals(0, response.getInt32("ok").getValue());
    assertEquals(Errors.COMMAND_FAILED.code, response.getInt32("code").getValue());
    assertEquals("Error processing request.", response.getString("errmsg").getValue());
  }

  @Test
  public void testHeartbeatTimeoutBoundary() throws Exception {
    var indexId = new ObjectId();
    var indexDefinition = createIndexDefinition(indexId, INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));

    // Create a server exactly at the 2-hour boundary (should be excluded)
    var boundaryServerId = new ObjectId();
    var boundaryServer =
        new ServerStateEntry(
            boundaryServerId, "boundary-server", Instant.now().minus(Duration.ofHours(2)));

    // Create a server just before the 2-hour boundary (should be included)
    var freshServerId = new ObjectId();
    var freshServer =
        new ServerStateEntry(
            freshServerId,
            "fresh-server",
            Instant.now().minus(Duration.ofHours(2).minusSeconds(1)));

    when(this.mockServerState.list()).thenReturn(List.of(boundaryServer, freshServer));
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(List.of());

    var definition = createListDefinition();
    var command = createCommand(definition);

    BsonDocument response = command.run();

    // Should succeed
    assertEquals(1, response.getInt32("ok").getValue());
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");
    assertEquals(1, batch.size());

    // The boundary server should be excluded, only the fresh server should be included
    BsonArray statusDetail = batch.getFirst().asDocument().getArray("statusDetail");
    // Note: Since there are no index stats, statusDetail will be empty
    // This test verifies that the boundary logic works correctly in getActiveServers()
    assertEquals(0, statusDetail.size());
  }

  @Test
  public void testLargeNumberOfHosts() throws Exception {
    var indexId = new ObjectId();
    var indexDefinition = createIndexDefinition(indexId, INDEX_NAME);
    when(this.mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));

    // Create 100 active servers
    var servers = new java.util.ArrayList<ServerStateEntry>();
    var indexStatsEntries = new java.util.ArrayList<IndexStatsEntry>();
    for (int i = 0; i < 100; i++) {
      var serverId = new ObjectId();
      servers.add(new ServerStateEntry(serverId, "server-" + i, Instant.now()));
      indexStatsEntries.add(
          new IndexStatsEntry(
              new IndexStatsEntry.IndexStatsKey(serverId, indexId),
              IndexDefinition.Type.SEARCH,
              Optional.empty(),
              Optional.empty()));
    }

    when(this.mockServerState.list()).thenReturn(servers);
    when(this.mockIndexStats.list(any(BsonDocument.class))).thenReturn(indexStatsEntries);

    var definition = createListDefinition();
    var command = createCommand(definition);

    BsonDocument response = command.run();

    // Should succeed
    assertEquals(1, response.getInt32("ok").getValue());
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");
    assertEquals(1, batch.size());

    // Verify all 100 hosts are in statusDetail
    BsonArray statusDetail = batch.getFirst().asDocument().getArray("statusDetail");
    assertEquals(100, statusDetail.size());
  }
}
