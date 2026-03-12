package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.IndexStats;
import com.xgen.mongot.catalogservice.IndexStatsEntry;
import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.catalogservice.ServerState;
import com.xgen.mongot.catalogservice.ServerStateEntry;
import com.xgen.mongot.config.manager.CachedIndexInfoProvider;
import com.xgen.mongot.config.manager.ConfigManager;
import com.xgen.mongot.index.AggregatedIndexMetrics;
import com.xgen.mongot.index.IndexDetailedStatus;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.server.CommandServer;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

public class CommunityReadinessCheckerTest {

  private CommunityServerInfo serverInfo;
  private ConfigManager configManager;
  private CachedIndexInfoProvider indexInfoProvider;
  private MetadataService metadataService;
  private ServerState serverState;
  private AuthoritativeIndexCatalog authoritativeIndexCatalog;
  private IndexStats indexStats;
  private CommandServer commandServer;
  private CommunityReadinessChecker checker;

  @Before
  public void setUp() throws MetadataServiceException {
    this.serverInfo = new CommunityServerInfo(new ObjectId(), Optional.of("server-name"));
    this.configManager = mock(ConfigManager.class);
    this.indexInfoProvider = mock(CachedIndexInfoProvider.class);
    this.metadataService = mock(MetadataService.class);
    this.serverState = mock(ServerState.class);
    this.authoritativeIndexCatalog = mock(AuthoritativeIndexCatalog.class);
    this.indexStats = mock(IndexStats.class);
    this.commandServer = mock(CommandServer.class);

    // Default: server state entry exists, not shutdown, not yet ready.
    ServerStateEntry defaultEntry =
        new ServerStateEntry(
            this.serverInfo.id(),
            "server-name",
            Instant.now(),
            false /* isReady */,
            false /* shutdown */);
    when(this.serverState.get(this.serverInfo.id())).thenReturn(Optional.of(defaultEntry));
    when(this.metadataService.getServerState()).thenReturn(this.serverState);
    when(this.metadataService.getAuthoritativeIndexCatalog())
        .thenReturn(this.authoritativeIndexCatalog);
    when(this.metadataService.getIndexStats()).thenReturn(this.indexStats);

    this.checker =
        new CommunityReadinessChecker(
            this.serverInfo,
            this.configManager,
            this.indexInfoProvider,
            this.metadataService,
            List.of(this.commandServer));
  }

  @Test
  public void testIsReady_serverStateEntryAbsent_returnsNotReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);
    when(this.serverState.get(this.serverInfo.id())).thenReturn(Optional.empty());

    assertFalse(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_serverStateEntryShutdown_returnsNotReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);
    ServerStateEntry shutdownEntry =
        new ServerStateEntry(
            this.serverInfo.id(),
            "server-name",
            Instant.now(),
            false /* isReady */,
            true /* shutdown */);
    when(this.serverState.get(this.serverInfo.id())).thenReturn(Optional.of(shutdownEntry));

    assertFalse(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_serverStateEntryReadinessExpired_returnsNotReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);
    ServerStateEntry expiredReadinessEntry =
        new ServerStateEntry(
            this.serverInfo.id(),
            "server-name",
            Instant.now().minus(Duration.ofMinutes(20)),
            true,
            false);
    when(this.serverState.get(this.serverInfo.id())).thenReturn(Optional.of(expiredReadinessEntry));

    assertFalse(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_serverStateEntryAlreadyReady_returnsReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);
    ServerStateEntry alreadyReadyEntry =
        new ServerStateEntry(
            this.serverInfo.id(),
            "server-name",
            Instant.now(),
            true /* isReady */,
            false /* shutdown */);
    when(this.serverState.get(this.serverInfo.id())).thenReturn(Optional.of(alreadyReadyEntry));

    assertTrue(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_serverNotStarted_returnsNotReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.NOT_STARTED);

    assertFalse(this.checker.isReady(false));
    verify(this.configManager, never()).isReplicationInitialized();
  }

  @Test
  public void testIsReady_replicationNotInitialized_returnsNotReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(false);

    assertFalse(this.checker.isReady(false));
    verify(this.indexInfoProvider, never()).refreshIndexInfos();
  }

  @Test
  public void testIsReady_missingIndexesFromCatalog_returnsNotReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId1 = new ObjectId();
    ObjectId indexId2 = new ObjectId();

    SearchIndexDefinition def1 = createIndexDefinition(indexId1, "index1");
    SearchIndexDefinition def2 = createIndexDefinition(indexId2, "index2");

    IndexInformation indexInfo1 = createIndexInformation(def1, IndexStatus.steady());

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo1));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def1, def2));

    assertFalse(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_emptyIndexes_returnsReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());
    when(this.authoritativeIndexCatalog.listIndexes()).thenReturn(Collections.emptyList());

    assertTrue(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_indexInSteadyState_returnsReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo = createIndexInformation(def, IndexStatus.steady());

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));

    assertTrue(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_indexInRecoveringTransientState_returnsReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo =
        createIndexInformation(def, IndexStatus.recoveringTransient("recovering"));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));

    assertTrue(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_indexInRecoveringNonTransientState_returnsReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo =
        createIndexInformation(def, IndexStatus.recoveringNonTransient(new BsonTimestamp(1L)));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));

    assertTrue(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_indexInStaleState_returnsReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo =
        createIndexInformation(def, IndexStatus.stale("stale reason", new BsonTimestamp(1L)));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));

    assertTrue(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_indexDoesNotExist_returnsReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo =
        createIndexInformation(def, new IndexStatus(IndexStatus.StatusCode.DOES_NOT_EXIST));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));

    assertTrue(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_indexInFailedState_returnsNotReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo = createIndexInformation(def, IndexStatus.failed("error message"));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));

    assertFalse(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_indexInFailedStateWithRegularFailure_returnsNotReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    // Regular failure message (not "Invalid definition: ...")
    IndexInformation indexInfo =
        createIndexInformation(def, IndexStatus.failed("Replication error occurred"));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));
    when(this.indexStats.list()).thenReturn(Collections.emptyList());

    // Regular failures should block readiness regardless of IndexStatsEntry data
    assertFalse(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_indexInFailedStateWithBypass_returnsReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo = createIndexInformation(def, IndexStatus.failed("error message"));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));

    assertTrue(this.checker.isReady(true));
  }

  @Test
  public void testIsReady_indexInFailedStateWithInvalidDefinition_returnsReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo =
        createIndexInformation(def, IndexStatus.failed("Invalid definition: bad config"));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));
    when(this.indexStats.list()).thenReturn(Collections.emptyList());

    assertTrue(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_indexInNotStartedState_returnsNotReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo = createIndexInformation(def, IndexStatus.notStarted());

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));

    assertFalse(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_indexWithNoMainIndex_returnsNotReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo =
        new IndexInformation.Search(
            def,
            IndexStatus.steady(),
            Collections.emptyList(),
            new AggregatedIndexMetrics(0L, 0L, new BsonTimestamp(0L), 0L),
            Optional.empty(), // No main index
            Optional.empty(),
            Collections.emptyMap());

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));

    assertFalse(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_multipleServersOneNotStarted_returnsNotReady() throws Exception {
    CommandServer server1 = mock(CommandServer.class);
    CommandServer server2 = mock(CommandServer.class);

    when(server1.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(server2.getServerStatus()).thenReturn(CommandServer.ServerStatus.NOT_STARTED);

    CommunityReadinessChecker multiServerChecker =
        new CommunityReadinessChecker(
            this.serverInfo,
            this.configManager,
            this.indexInfoProvider,
            this.metadataService,
            List.of(server1, server2));

    assertFalse(multiServerChecker.isReady(false));
  }

  @Test
  public void testIsReady_multipleServersAllStarted_returnsReady() throws Exception {
    CommandServer server1 = mock(CommandServer.class);
    CommandServer server2 = mock(CommandServer.class);

    when(server1.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(server2.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());
    when(this.authoritativeIndexCatalog.listIndexes()).thenReturn(Collections.emptyList());

    CommunityReadinessChecker multiServerChecker =
        new CommunityReadinessChecker(
            this.serverInfo,
            this.configManager,
            this.indexInfoProvider,
            this.metadataService,
            List.of(server1, server2));

    assertTrue(multiServerChecker.isReady(false));
  }

  @Test
  public void testIsReady_onceReadyStaysReady_returnsReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());
    when(this.authoritativeIndexCatalog.listIndexes()).thenReturn(Collections.emptyList());

    // First call should return true
    assertTrue(this.checker.isReady(false));

    // Change conditions to make it not ready
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.NOT_STARTED);

    // Should still return true because it was already ready
    assertTrue(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_multipleIndexesMixedStates_returnsNotReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId1 = new ObjectId();
    ObjectId indexId2 = new ObjectId();
    SearchIndexDefinition def1 = createIndexDefinition(indexId1, "index1");
    SearchIndexDefinition def2 = createIndexDefinition(indexId2, "index2");

    IndexInformation indexInfo1 = createIndexInformation(def1, IndexStatus.steady());
    IndexInformation indexInfo2 = createIndexInformation(def2, IndexStatus.notStarted());

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo1, indexInfo2));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def1, def2));

    assertFalse(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_multipleIndexesAllReady_returnsReady() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId1 = new ObjectId();
    ObjectId indexId2 = new ObjectId();
    SearchIndexDefinition def1 = createIndexDefinition(indexId1, "index1");
    SearchIndexDefinition def2 = createIndexDefinition(indexId2, "index2");

    IndexInformation indexInfo1 = createIndexInformation(def1, IndexStatus.steady());
    IndexInformation indexInfo2 =
        createIndexInformation(def2, IndexStatus.recoveringTransient("recovering"));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo1, indexInfo2));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def1, def2));

    assertTrue(this.checker.isReady(false));
  }

  @Test
  public void testIsReady_invalidDefinitionFailure_withMixedStatsEntries_returnsNotReady()
      throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    ObjectId serverId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo =
        createIndexInformation(def, IndexStatus.failed("Invalid definition: bad config"));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));

    IndexStatsEntry entry1 =
        createIndexStatsEntry(
            serverId, indexId, def, IndexStatus.failed("Invalid definition: schema error"));
    IndexStatsEntry entry2 =
        createIndexStatsEntry(serverId, indexId, def, IndexStatus.failed("Replication error"));

    when(this.indexStats.list()).thenReturn(List.of(entry1, entry2));

    // Should return false because not all stats entries are invalid definition failures
    assertFalse(
        "When IndexStatsEntry contains mixed failure types, should not be ready",
        this.checker.isReady(false));
  }

  @Test
  public void testIsReady_invalidDefinitionFailure_withAllInvalidStatsEntries_shouldBeReady()
      throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    ObjectId serverId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo =
        createIndexInformation(def, IndexStatus.failed("Invalid definition: bad config"));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));

    // Mock getIndexStatsPerIndex() to return IndexStatsEntry where ALL entries have invalid
    // definition failures
    IndexStatsEntry entry1 =
        createIndexStatsEntry(
            serverId, indexId, def, IndexStatus.failed("Invalid definition: schema error"));
    IndexStatsEntry entry2 =
        createIndexStatsEntry(
            serverId, indexId, def, IndexStatus.failed("Invalid definition: mapping issue"));

    when(this.indexStats.list()).thenReturn(List.of(entry1, entry2));

    // Should return true because all stats entries show invalid definition
    assertTrue(
        "When all IndexStatsEntry have invalid definition failures, should be ready",
        this.checker.isReady(false));
  }

  @Test(expected = MetadataServiceException.class)
  public void testIsReady_getIndexStatsPerIndexThrowsException_ExceptionThrown() throws Exception {
    when(this.commandServer.getServerStatus()).thenReturn(CommandServer.ServerStatus.STARTED);
    when(this.configManager.isReplicationInitialized()).thenReturn(true);

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition def = createIndexDefinition(indexId, "index1");
    IndexInformation indexInfo = createIndexInformation(def, IndexStatus.steady());

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));
    when(this.authoritativeIndexCatalog.listIndexDefinitions()).thenReturn(List.of(def));

    RuntimeException cause = new RuntimeException("Failed to fetch index stats");
    when(this.indexStats.list()).thenThrow(MetadataServiceException.createFailed(cause));

    // Should throw MetadataServiceException
    this.checker.isReady(false);
  }

  private SearchIndexDefinition createIndexDefinition(ObjectId indexId, String name) {
    return SearchIndexDefinitionBuilder.builder()
        .indexId(indexId)
        .name(name)
        .database("testDb")
        .lastObservedCollectionName("testColl")
        .collectionUuid(java.util.UUID.randomUUID())
        .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
        .build();
  }

  private IndexInformation createIndexInformation(
      SearchIndexDefinition definition, IndexStatus status) {
    IndexDetailedStatus.Search mainIndex =
        new IndexDetailedStatus.Search(
            Collections.emptyMap(), definition, status, Optional.empty());

    return new IndexInformation.Search(
        definition,
        status,
        Collections.emptyList(),
        new AggregatedIndexMetrics(0L, 0L, new BsonTimestamp(0L), 0L),
        Optional.of(mainIndex),
        Optional.empty(),
        Collections.emptyMap());
  }

  private IndexStatsEntry createIndexStatsEntry(
      ObjectId serverId, ObjectId indexId, SearchIndexDefinition definition, IndexStatus status) {
    IndexStatsEntry.DetailedIndexStats detailedStats =
        new IndexStatsEntry.DetailedIndexStats(status, definition, Optional.empty());

    return new IndexStatsEntry(
        new IndexStatsEntry.IndexStatsKey(serverId, indexId),
        IndexDefinition.Type.SEARCH,
        Optional.of(detailedStats),
        Optional.empty());
  }
}
