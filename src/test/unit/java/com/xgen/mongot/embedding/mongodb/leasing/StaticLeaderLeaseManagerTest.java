package com.xgen.mongot.embedding.mongodb.leasing;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.mongodb.common.DefaultInternalDatabaseResolver;
import com.xgen.mongot.embedding.mongodb.common.InternalDatabaseResolver;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.testing.mongot.metrics.SimpleMetricsFactory;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link StaticLeaderLeaseManager}.
 *
 * <p>These tests verify the static leader behavior, focusing on the initializeLease method which
 * handles lease initialization for both leaders and followers in Community edition.
 */
@SuppressWarnings("deprecation")
public class StaticLeaderLeaseManagerTest {

  private static final String HOSTNAME = "test-host";
  private static final String DATABASE_NAME = "test-db";
  private static final InternalDatabaseResolver DB_RESOLVER =
      new DefaultInternalDatabaseResolver(DATABASE_NAME);

  private MongoClient mockMongoClient;
  private MongoDatabase mockDatabase;
  private MongoCollection<BsonDocument> mockCollection;
  private FindIterable<BsonDocument> mockFindIterable;
  private MaterializedViewCollectionMetadataCatalog mvMetadataCatalog;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    this.mockMongoClient = mock(MongoClient.class);
    this.mockDatabase = mock(MongoDatabase.class);
    this.mockCollection = mock(MongoCollection.class);
    this.mockFindIterable = mock(FindIterable.class);
    this.mvMetadataCatalog = new MaterializedViewCollectionMetadataCatalog();

    when(this.mockMongoClient.getDatabase(DATABASE_NAME)).thenReturn(this.mockDatabase);
    when(this.mockDatabase.getCollection(
            StaticLeaderLeaseManager.LEASE_COLLECTION_NAME, BsonDocument.class))
        .thenReturn(this.mockCollection);
    when(this.mockCollection.withReadConcern(any())).thenReturn(this.mockCollection);
    when(this.mockCollection.withReadPreference(any())).thenReturn(this.mockCollection);
    when(this.mockCollection.find()).thenReturn(this.mockFindIterable);
    when(this.mockCollection.find(any(Bson.class))).thenReturn(this.mockFindIterable);
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>());
  }

  // ==================== initializeLease Tests ====================

  @Test
  public void initializeLease_noExistingLease_returnsProposedMetadata() throws Exception {
    // Arrange
    StaticLeaderLeaseManager leaseManager = createLeaseManager(true);

    ObjectId indexId = new ObjectId();
    MaterializedViewIndexDefinitionGeneration indexDefGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(indexId);

    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(
            VERSION_ZERO, UUID.randomUUID(), "mv-collection-name");

    // Act
    MaterializedViewCollectionMetadata result =
        leaseManager.initializeLease(indexDefGen, proposedMetadata);

    // Assert - should return proposed metadata (no existing lease)
    assertThat(result).isEqualTo(proposedMetadata);
    assertThat(leaseManager.getLeaseKeyToDatabase()).containsKey("mv-collection-name");
    assertThat(leaseManager.getLeaseKeyToDatabase().get("mv-collection-name"))
        .isEqualTo(DATABASE_NAME);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void initializeLease_existingLeaseInMemory_returnsExistingMetadata() throws Exception {
    // Arrange - setup a lease in the database that will be loaded on construction
    String collectionName = "existing-mv-collection";
    UUID existingUuid = UUID.randomUUID();
    MaterializedViewCollectionMetadata existingMetadata =
        new MaterializedViewCollectionMetadata(VERSION_ZERO, existingUuid, collectionName);

    Lease existingLease = createLeaseWithMetadata(collectionName, existingMetadata);
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(existingLease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    // Mock find(Document).first() to return the existing lease when queried by collection name
    FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
    when(this.mockCollection.find(any(Document.class))).thenReturn(findIterable);
    when(findIterable.first()).thenReturn(existingLease.toBson());

    StaticLeaderLeaseManager leaseManager = createLeaseManager(true);

    ObjectId indexId = new ObjectId();
    MaterializedViewIndexDefinitionGeneration indexDefGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(indexId);

    // Different proposed metadata but same collection name
    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(1, Map.of()),
            UUID.randomUUID(),
            collectionName);

    // Act
    leaseManager.syncLeasesFromMongod();
    MaterializedViewCollectionMetadata result =
        leaseManager.initializeLease(indexDefGen, proposedMetadata);

    // Assert - should return the existing lease's metadata, not the proposed one
    assertThat(result.collectionName()).isEqualTo(collectionName);
    assertThat(result.collectionUuid()).isEqualTo(existingUuid);
    assertThat(result.schemaMetadata().materializedViewSchemaVersion()).isEqualTo(0L);
    assertThat(leaseManager.getLeases()).containsKey(collectionName);
    assertThat(leaseManager.getLeaseKeyToDatabase()).containsKey(collectionName);
    assertThat(leaseManager.getLeaseKeyToDatabase().get(collectionName)).isEqualTo(DATABASE_NAME);
  }

  @Test
  public void initializeLease_followerWithNoExistingLease_returnsProposedMetadata()
      throws Exception {
    // Arrange - follower (isLeader = false)
    StaticLeaderLeaseManager followerLeaseManager = createLeaseManager(false);

    ObjectId indexId = new ObjectId();
    MaterializedViewIndexDefinitionGeneration indexDefGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(indexId);

    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(
            VERSION_ZERO, UUID.randomUUID(), "follower-mv-collection");

    // Act
    MaterializedViewCollectionMetadata result =
        followerLeaseManager.initializeLease(indexDefGen, proposedMetadata);

    // Assert - follower should return proposed metadata when no existing lease
    assertThat(result).isEqualTo(proposedMetadata);
    assertThat(followerLeaseManager.getLeaseKeyToDatabase()).containsKey("follower-mv-collection");
    assertThat(followerLeaseManager.getLeaseKeyToDatabase().get("follower-mv-collection"))
        .isEqualTo(DATABASE_NAME);
  }

  // ==================== drop Tests ====================

  @Test
  public void drop_asLeader_onlyUntracksGenerationId() {
    // Arrange
    StaticLeaderLeaseManager leaseManager = createLeaseManager(true);
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    leaseManager.add(indexGeneration, false);

    // Verify generationId is tracked
    assertThat(leaseManager.getLeaderGenerationIds()).contains(generationId);

    // Act
    leaseManager.drop(generationId);

    // Assert - generationId is untracked from managedGenerationIds
    assertThat(leaseManager.getLeaderGenerationIds()).doesNotContain(generationId);
    // Assert - lease is still in local cache (drop() does not remove from leases map)
    assertThat(leaseManager.getLeases().get(getLeaseKeyFromCatalog(generationId))).isNotNull();
    // Assert - deleteOne was NOT called (drop does not touch DB)
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  @Test
  public void drop_asFollower_onlyUntracksGenerationId() {
    // Arrange
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    leaseManager.add(indexGeneration, false);

    // Act
    leaseManager.drop(generationId);

    // Assert - generationId is untracked
    assertThat(leaseManager.getLeaderGenerationIds()).doesNotContain(generationId);
    // Assert - deleteOne was NOT called
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  // ==================== dropLease Tests ====================

  @Test
  public void dropLease_asLeader_deletesFromDatabase() {
    // Arrange
    StaticLeaderLeaseManager leaseManager = createLeaseManager(true);
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    leaseManager.add(indexGeneration, false);

    // Setup delete mock
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(this.mockCollection.deleteOne(any(Document.class))).thenReturn(deleteResult);

    // Act - dropLease deletes from memory and database
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    leaseManager.dropLease(leaseKey).join();

    // Assert - verify deleteOne was called
    verify(this.mockCollection).deleteOne(any(Document.class));
    // Assert - lease is removed from local cache
    assertThat(leaseManager.getLeases()).doesNotContainKey(leaseKey);
  }

  @Test
  public void dropLease_asLeader_metersLeaseDrop() {
    // Hold the metrics factory so we can read the lease-drop counter (createLeaseManager hides it).
    SimpleMetricsFactory metricsFactory = new SimpleMetricsFactory();
    StaticLeaderLeaseManager leaseManager =
        new StaticLeaderLeaseManager(
            this.mockMongoClient,
            metricsFactory,
            HOSTNAME,
            DB_RESOLVER,
            true,
            this.mvMetadataCatalog);
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    leaseManager.add(indexGeneration, false);
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(this.mockCollection.deleteOne(any(Document.class))).thenReturn(deleteResult);

    leaseManager.dropLease(getLeaseKeyFromCatalog(generationId)).join();

    // Same series as the GC delete, distinguished by operation="dropLease" (vs "deleteStaleLease").
    assertThat(
            metricsFactory
                .meterRegistry
                .find(metricsFactory.namespace + ".leaseTableCollection.successfulRequests")
                .tag("operation", "dropLease")
                .counter()
                .count())
        .isEqualTo(1.0);
  }

  @Test
  public void dropLease_asFollower_removesFromMemoryOnly() {
    // Arrange
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    leaseManager.add(indexGeneration, false);

    // Act - dropLease removes from memory but does not delete from DB
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    leaseManager.dropLease(leaseKey).join();

    // Assert - verify deleteOne was NOT called (follower doesn't own the lease in DB)
    verify(this.mockCollection, never()).deleteOne(any(Document.class));
    // Assert - lease is removed from local cache
    assertThat(leaseManager.getLeases()).doesNotContainKey(leaseKey);
  }

  @Test
  public void evictCachedLease_removesCachedStateOnly() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(true);
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    leaseManager.add(indexGeneration, false);
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    assertThat(leaseManager.getLeases()).containsKey(leaseKey);

    leaseManager.evictCachedLease(leaseKey);

    // In-memory lease cache and database mapping are gone; the lease document is untouched.
    assertThat(leaseManager.getLeases()).doesNotContainKey(leaseKey);
    assertThat(leaseManager.getLeaseKeyToDatabase()).doesNotContainKey(leaseKey);
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  // ==================== updateCommitInfo Tests ====================

  @Test
  public void updateCommitInfo_asFollower_isNoOp() throws Exception {
    // Arrange - follower with a lease tracked in memory (followers track leases too)
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    leaseManager.add(indexGeneration, false);

    // Act - MaterializedViewWriter.commit calls updateCommitInfo unconditionally, so a
    // follower can reach this path (e.g. from a PeriodicIndexCommitter tick).
    // DynamicLeaderLeaseManager no-ops here; the static manager must behave the same.
    leaseManager.updateCommitInfo(indexGeneration.getGenerationId(), EncodedUserData.EMPTY);

    // Assert - follower must not write to the lease collection
    verify(this.mockCollection, never()).replaceOne(any(Bson.class), any(), any());
  }

  @Test
  public void pollFollowerStatuses_returnsReplicationOpTimeFromLeaseCheckpoint() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    leaseManager.add(indexGeneration, false);
    BsonTimestamp replicationOpTime = new BsonTimestamp(123, 1);
    EncodedUserData commitInfo =
        IndexCommitUserData.createChangeStreamResume(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace("test", "collection"),
                    ChangeStreamUtils.resumeToken(replicationOpTime)),
                IndexFormatVersion.CURRENT)
            .toEncodedData();
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    Lease lease =
        new Lease(
            leaseKey,
            Lease.SCHEMA_VERSION,
            UUID.randomUUID().toString(),
            "source-collection",
            HOSTNAME,
            Instant.now().plusSeconds(60),
            1L,
            commitInfo.asString(),
            "0",
            Map.of(
                "0", new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY)),
            this.mvMetadataCatalog.getMetadata(generationId),
            null,
            Lease.CleanupState.NOT_ELIGIBLE);
    when(this.mockFindIterable.first()).thenReturn(lease.toBson());

    LeaseManager.FollowerPollResult result = leaseManager.pollFollowerStatuses();

    assertThat(result.replicationOpTimes()).containsEntry(generationId, replicationOpTime);
  }

  @Test
  public void pollFollowerStatuses_versionKeyNotPublishedInLease_reportsUnknown() {
    // Static-leader counterpart of the same race covered by DynamicLeaderLeaseManagerTest
    // (HELP-97792): the lease does not carry this generation's version key yet, so the previous
    // version's STEADY status and checkpoint must not be inherited.
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration(1);
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    leaseManager.add(indexGeneration, false);
    EncodedUserData commitInfo =
        IndexCommitUserData.createChangeStreamResume(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace("test", "collection"),
                    ChangeStreamUtils.resumeToken(new BsonTimestamp(123, 1))),
                IndexFormatVersion.CURRENT)
            .toEncodedData();
    Lease leaseWithoutVersionOne =
        new Lease(
            getLeaseKeyFromCatalog(generationId),
            Lease.SCHEMA_VERSION,
            UUID.randomUUID().toString(),
            "source-collection",
            HOSTNAME,
            Instant.now().plusSeconds(60),
            1L,
            commitInfo.asString(),
            "0",
            Map.of(
                "0", new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY)),
            this.mvMetadataCatalog.getMetadata(generationId),
            null,
            Lease.CleanupState.NOT_ELIGIBLE);
    when(this.mockFindIterable.first()).thenReturn(leaseWithoutVersionOne.toBson());

    LeaseManager.FollowerPollResult result = leaseManager.pollFollowerStatuses();

    assertThat(result.statuses().get(generationId).getStatusCode())
        .isEqualTo(IndexStatus.StatusCode.UNKNOWN);
    assertThat(result.replicationOpTimes()).doesNotContainKey(generationId);
  }

  // ==================== findGcCandidates / markEligibleForCleanup ====================
  // The GC APIs are leaderless — they must work on followers too, so tests use isLeader=false.

  @Test
  public void findGcCandidates_returnsLeases_andRegistersDatabaseMapping() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "gc-orphan-collection";
    Lease lease =
        createLeaseWithMetadata(
            collectionName,
            new MaterializedViewCollectionMetadata(
                VERSION_ZERO, UUID.randomUUID(), collectionName));
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    List<Lease> leases = leaseManager.findGcCandidates().leases();

    assertThat(leases).hasSize(1);
    assertThat(leases.get(0).id()).isEqualTo(collectionName);
    // The lease→database mapping must be registered even though no configured generation ever
    // added this lease, so a follow-up markEligibleForCleanup can resolve the database.
    assertThat(leaseManager.getLeaseKeyToDatabase()).containsKey(collectionName);
    assertThat(leaseManager.getLeaseKeyToDatabase().get(collectionName)).isEqualTo(DATABASE_NAME);
  }

  @Test
  public void findGcCandidates_skipsCorruptedLeases() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "good-collection";
    Lease good =
        createLeaseWithMetadata(
            collectionName,
            new MaterializedViewCollectionMetadata(
                VERSION_ZERO, UUID.randomUUID(), collectionName));
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(new BsonDocument("_id", new BsonString("corrupted")));
    leaseList.add(good.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    List<Lease> leases = leaseManager.findGcCandidates().leases();

    assertThat(leases).hasSize(1);
    assertThat(leases.get(0).id()).isEqualTo(collectionName);
  }

  @Test
  public void findGcCandidates_corruptedDocumentsOnly_notReportedEmpty() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(new BsonDocument("_id", new BsonString("corrupted")));
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    LeaseManager.GcScanResult result = leaseManager.findGcCandidates();

    // No parseable lease, but the table is not empty: the sweep must keep scanning so the
    // corrupted document keeps being counted.
    assertThat(result.leases()).isEmpty();
    assertThat(result.complete()).isTrue();
    assertThat(result.sawDocuments()).isTrue();
    assertThat(result.isCompleteAndEmpty()).isFalse();
  }

  @Test
  public void findGcCandidates_queryFailure_returnsEmpty() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    when(this.mockCollection.find()).thenThrow(new RuntimeException("find failed"));

    assertThat(leaseManager.findGcCandidates().leases()).isEmpty();
  }

  @Test
  public void findGcCandidates_queryFailure_reportsIncompleteScan() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    when(this.mockCollection.find()).thenThrow(new RuntimeException("find failed"));

    // Empty because the read failed, not because there are no leases — callers must be able to
    // tell the two apart.
    LeaseManager.GcScanResult result = leaseManager.findGcCandidates();
    assertThat(result.complete()).isFalse();
    assertThat(result.isCompleteAndEmpty()).isFalse();
  }

  @Test
  public void findGcCandidates_noLeases_reportsCompleteAndEmpty() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);

    LeaseManager.GcScanResult result = leaseManager.findGcCandidates();

    assertThat(result.leases()).isEmpty();
    assertThat(result.complete()).isTrue();
    assertThat(result.isCompleteAndEmpty()).isTrue();
  }

  @Test
  public void markEligibleForCleanup_modified_returnsTrueAndUpdatesInMemoryLease() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "stale-collection";
    Lease lease =
        createLeaseWithMetadata(
            collectionName,
            new MaterializedViewCollectionMetadata(
                VERSION_ZERO, UUID.randomUUID(), collectionName));
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);
    // Registers the database mapping (findGcCandidates) and the in-memory copy (sync).
    leaseManager.findGcCandidates();
    leaseManager.syncLeasesFromMongod();

    UpdateResult updateResult = mock(UpdateResult.class);
    when(updateResult.getModifiedCount()).thenReturn(1L);
    when(this.mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

    boolean marked = leaseManager.markEligibleForCleanup(collectionName, Instant.now());

    assertThat(marked).isTrue();
    assertThat(leaseManager.getLeases().get(collectionName).cleanupState())
        .isEqualTo(Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
  }

  @Test
  public void markEligibleForCleanup_occFilterMiss_returnsFalseAndLeavesInMemoryLease() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "fresh-collection";
    Lease lease =
        createLeaseWithMetadata(
            collectionName,
            new MaterializedViewCollectionMetadata(
                VERSION_ZERO, UUID.randomUUID(), collectionName));
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);
    leaseManager.findGcCandidates();
    leaseManager.syncLeasesFromMongod();

    UpdateResult updateResult = mock(UpdateResult.class);
    when(updateResult.getModifiedCount()).thenReturn(0L);
    when(this.mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

    boolean marked = leaseManager.markEligibleForCleanup(collectionName, Instant.now());

    assertThat(marked).isFalse();
    assertThat(leaseManager.getLeases().get(collectionName).cleanupState())
        .isEqualTo(Lease.CleanupState.NOT_ELIGIBLE);
  }

  @Test
  public void markEligibleForCleanup_unknownLease_returnsFalseWithoutDbWrite() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);

    boolean marked = leaseManager.markEligibleForCleanup("never-seen-lease", Instant.now());

    assertThat(marked).isFalse();
    verify(this.mockCollection, never()).updateOne(any(Bson.class), any(Bson.class));
  }

  @Test
  public void markEligibleForCleanup_updateFailure_returnsFalse() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "error-collection";
    Lease lease =
        createLeaseWithMetadata(
            collectionName,
            new MaterializedViewCollectionMetadata(
                VERSION_ZERO, UUID.randomUUID(), collectionName));
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);
    leaseManager.findGcCandidates();
    when(this.mockCollection.updateOne(any(Bson.class), any(Bson.class)))
        .thenThrow(new RuntimeException("update failed"));

    assertThat(leaseManager.markEligibleForCleanup(collectionName, Instant.now())).isFalse();
  }

  // ==================== Helper Methods ====================

  private StaticLeaderLeaseManager createLeaseManager(boolean isLeader) {
    return new StaticLeaderLeaseManager(
        this.mockMongoClient,
        new SimpleMetricsFactory(),
        HOSTNAME,
        DB_RESOLVER,
        isLeader,
        this.mvMetadataCatalog);
  }

  /**
   * Creates a MaterializedViewIndexGeneration and registers its generationId in the catalog so that
   * StaticLeaderLeaseManager.getLeaseKey (which uses catalog) works.
   */
  private MaterializedViewIndexGeneration createTestIndexGeneration() {
    return createTestIndexGeneration(0);
  }

  /** As above, for a generation at a specific index definition version. */
  private MaterializedViewIndexGeneration createTestIndexGeneration(long indexDefinitionVersion) {
    MaterializedViewIndexDefinitionGeneration defGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(
            new ObjectId(), indexDefinitionVersion);
    MaterializedViewIndexGeneration gen = mockMatViewIndexGeneration(defGen);
    addCatalogMetadataForGeneration(gen.getGenerationId());
    return gen;
  }

  private void addCatalogMetadataForGeneration(MaterializedViewGenerationId generationId) {
    String collectionName = generationId.indexId.toHexString();
    MaterializedViewCollectionMetadata metadata =
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
            UUID.randomUUID(),
            collectionName);
    this.mvMetadataCatalog.addMetadata(generationId, metadata);
  }

  /** Returns the lease key (collection name) for the given generation from the catalog. */
  private String getLeaseKeyFromCatalog(MaterializedViewGenerationId generationId) {
    return this.mvMetadataCatalog.getMetadata(generationId).collectionName();
  }

  // ==================== resolveStaleMatViewNamespace / deleteStaleLease ====================

  /** A lease already marked {@code ELIGIBLE_FOR_CLEANUP} whose heartbeat lapsed long ago. */
  private Lease markedStaleLease(String collectionName) {
    return markedLease(collectionName, Instant.now().minusSeconds(7200));
  }

  private Lease markedLease(String collectionName, Instant leaseExpiration) {
    return new Lease(
        collectionName,
        Lease.SCHEMA_VERSION,
        UUID.randomUUID().toString(),
        "source-collection",
        HOSTNAME,
        leaseExpiration,
        1L,
        "",
        "0",
        Map.of("0", new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN)),
        new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), collectionName),
        null,
        Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
  }

  /** Seeds the lease→database mapping (as the GC scan would) for the given lease. */
  private void registerViaGcScan(StaticLeaderLeaseManager leaseManager, Lease lease) {
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);
    leaseManager.findGcCandidates();
  }

  @Test
  public void resolveStaleMatViewNamespace_stillEligible_returnsNamespace() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "cleanup-collection";
    Lease lease = markedStaleLease(collectionName);
    registerViaGcScan(leaseManager, lease);
    // The re-read returns the still-eligible lease.
    when(this.mockFindIterable.first()).thenReturn(lease.toBson());

    Optional<MongoNamespace> namespace =
        leaseManager.resolveStaleMatViewNamespace(collectionName, 1L, Instant.now());

    assertThat(namespace).isPresent();
    assertThat(namespace.get().getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(namespace.get().getCollectionName()).isEqualTo(collectionName);
  }

  @Test
  public void resolveStaleMatViewNamespace_noLongerEligible_returnsEmpty() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "fresh-collection";
    registerViaGcScan(leaseManager, markedStaleLease(collectionName));
    // The re-read sees an unmarked lease: it must be vetoed.
    Lease unmarked =
        createLeaseWithMetadata(
            collectionName,
            new MaterializedViewCollectionMetadata(
                VERSION_ZERO, UUID.randomUUID(), collectionName));
    when(this.mockFindIterable.first()).thenReturn(unmarked.toBson());

    assertThat(leaseManager.resolveStaleMatViewNamespace(collectionName, 1L, Instant.now()))
        .isEmpty();
  }

  @Test
  public void resolveStaleMatViewNamespace_renewedPastCutoff_returnsEmpty() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    Instant now = Instant.now();
    String collectionName = "renewed-collection";
    registerViaGcScan(leaseManager, markedStaleLease(collectionName));
    // By resolve time the lease was renewed past the cutoff: still marked, but no longer stale.
    Lease renewed = markedLease(collectionName, now.plusSeconds(60));
    when(this.mockFindIterable.first()).thenReturn(renewed.toBson());

    assertThat(leaseManager.resolveStaleMatViewNamespace(collectionName, 1L, now)).isEmpty();
  }

  @Test
  public void deleteStaleLease_deleted_returnsTrueAndForgetsLease() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "deleted-collection";
    Lease lease = markedStaleLease(collectionName);
    registerViaGcScan(leaseManager, lease);
    leaseManager.getLeases().put(collectionName, lease);
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(deleteResult.getDeletedCount()).thenReturn(1L);
    when(this.mockCollection.deleteOne(any(Bson.class))).thenReturn(deleteResult);

    assertThat(leaseManager.deleteStaleLease(collectionName, 1L, Instant.now())).isTrue();
    assertThat(leaseManager.getLeases()).doesNotContainKey(collectionName);
    assertThat(leaseManager.getLeaseKeyToDatabase()).doesNotContainKey(collectionName);
  }

  @Test
  public void deleteStaleLease_occFilterMiss_returnsFalseAndKeepsMapping() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "kept-collection";
    registerViaGcScan(leaseManager, markedStaleLease(collectionName));
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(deleteResult.getDeletedCount()).thenReturn(0L);
    when(this.mockCollection.deleteOne(any(Bson.class))).thenReturn(deleteResult);

    assertThat(leaseManager.deleteStaleLease(collectionName, 1L, Instant.now())).isFalse();
    assertThat(leaseManager.getLeaseKeyToDatabase()).containsKey(collectionName);
  }

  @Test
  public void resolveStaleMatViewNamespace_leaseAlreadyDeleted_returnsEmpty() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "already-gone-collection";
    registerViaGcScan(leaseManager, markedStaleLease(collectionName));
    when(this.mockFindIterable.first()).thenReturn(null);

    assertThat(leaseManager.resolveStaleMatViewNamespace(collectionName, 1L, Instant.now()))
        .isEmpty();
  }

  @Test
  public void resolveStaleMatViewNamespace_unknownLease_returnsEmpty() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    assertThat(leaseManager.resolveStaleMatViewNamespace("never-seen", 1L, Instant.now()))
        .isEmpty();
  }

  @Test
  public void resolveStaleMatViewNamespace_reReadThrows_returnsEmpty() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "reread-error-collection";
    registerViaGcScan(leaseManager, markedStaleLease(collectionName));
    when(this.mockFindIterable.first()).thenThrow(new RuntimeException("read failed"));

    assertThat(leaseManager.resolveStaleMatViewNamespace(collectionName, 1L, Instant.now()))
        .isEmpty();
  }

  @Test
  public void deleteStaleLease_unknownLease_returnsFalseWithoutDbWrite() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    assertThat(leaseManager.deleteStaleLease("never-seen-lease", 1L, Instant.now())).isFalse();
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  @Test
  public void deleteStaleLease_deleteFailure_returnsFalse() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "error-delete-collection";
    registerViaGcScan(leaseManager, markedStaleLease(collectionName));
    when(this.mockCollection.deleteOne(any(Bson.class)))
        .thenThrow(new RuntimeException("delete failed"));

    assertThat(leaseManager.deleteStaleLease(collectionName, 1L, Instant.now())).isFalse();
  }

  @Test
  public void deleteStaleLease_filtersOnMarkedStateVersionAndExpiration() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "filtered-collection";
    registerViaGcScan(leaseManager, markedStaleLease(collectionName));
    Instant cutoff = Instant.parse("2026-01-01T00:00:00Z");
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(deleteResult.getDeletedCount()).thenReturn(0L);
    when(this.mockCollection.deleteOne(any(Bson.class))).thenReturn(deleteResult);

    leaseManager.deleteStaleLease(collectionName, 7L, cutoff);

    ArgumentCaptor<Bson> filterCaptor = ArgumentCaptor.forClass(Bson.class);
    verify(this.mockCollection).deleteOne(filterCaptor.capture());
    BsonDocument filter =
        filterCaptor
            .getValue()
            .toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
    BsonArray clauses = filter.getArray("$and");
    assertThat(clauses.contains(new BsonDocument("_id", new BsonString(collectionName)))).isTrue();
    assertThat(
            clauses.contains(
                new BsonDocument(
                    "cleanupState",
                    new BsonString(Lease.CleanupState.ELIGIBLE_FOR_CLEANUP.name()))))
        .isTrue();
    // Fencing token: the OCC delete commits only against the exact leaseVersion the sweep saw.
    assertThat(clauses.stream().anyMatch(c -> c.asDocument().containsKey("leaseVersion"))).isTrue();
    assertThat(
            clauses.contains(
                new BsonDocument(
                    "leaseExpiration",
                    new BsonDocument("$lt", new BsonDateTime(cutoff.toEpochMilli())))))
        .isTrue();
  }

  // ==================== deleted-index deliberate cleanup ====================

  @Test
  public void cleanupLeaseForDeletedIndex_deleteFailure_evictsCacheAndReturnsFalse() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "deleted-index-error-lease";
    registerViaGcScan(
        leaseManager,
        createLeaseWithMetadata(
            collectionName,
            new MaterializedViewCollectionMetadata(
                VERSION_ZERO, UUID.randomUUID(), collectionName)));
    when(this.mockCollection.deleteOne(any(Bson.class)))
        .thenThrow(new RuntimeException("delete failed"));

    assertThat(leaseManager.cleanupLeaseForDeletedIndex(collectionName)).isFalse();
    // The cached copy is evicted even when the delete fails; the document is left for the GC.
    assertThat(leaseManager.getLeaseKeyToDatabase()).doesNotContainKey(collectionName);
  }

  @Test
  public void cleanupLeaseForDeletedIndex_deletesByIdRegardlessOfOwnerAndState() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    String collectionName = "deleted-index-lease";
    Lease fresh =
        createLeaseWithMetadata(
            collectionName,
            new MaterializedViewCollectionMetadata(
                VERSION_ZERO, UUID.randomUUID(), collectionName));
    registerViaGcScan(leaseManager, fresh);
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(this.mockCollection.deleteOne(any(Bson.class))).thenReturn(deleteResult);

    assertThat(leaseManager.cleanupLeaseForDeletedIndex(collectionName)).isTrue();

    // By id only: no owner, staleness, or cleanup-state predicate. Local cache evicted.
    ArgumentCaptor<Bson> filterCaptor = ArgumentCaptor.forClass(Bson.class);
    verify(this.mockCollection).deleteOne(filterCaptor.capture());
    String filterJson =
        filterCaptor
            .getValue()
            .toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
            .toJson();
    assertThat(filterJson).doesNotContain(Lease.Fields.LEASE_OWNER.getName());
    assertThat(filterJson).doesNotContain(Lease.Fields.CLEANUP_STATE.getName());
    assertThat(filterJson).doesNotContain(Lease.Fields.LEASE_EXPIRATION.getName());
    assertThat(leaseManager.getLeases()).doesNotContainKey(collectionName);
    assertThat(leaseManager.getLeaseKeyToDatabase()).doesNotContainKey(collectionName);
  }

  @Test
  public void findLeasesForDeletedIndex_unparseableDocument_skippedNotFatal() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    ObjectId indexId = new ObjectId();
    String knownKey = indexId.toHexString() + "-hash1-1-0";
    String leftoverKey = indexId.toHexString() + "-hash2-1-0";
    registerViaGcScan(
        leaseManager,
        createLeaseWithMetadata(
            knownKey,
            new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), knownKey)));
    ArrayList<BsonDocument> found = new ArrayList<>();
    found.add(
        createLeaseWithMetadata(
                knownKey,
                new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), knownKey))
            .toBson());
    // Malformed lease document (missing required fields): must be skipped per document, not
    // abort the scan and hide the remaining leases.
    found.add(new BsonDocument("_id", new BsonString(indexId.toHexString() + "-bad-1-0")));
    found.add(
        createLeaseWithMetadata(
                leftoverKey,
                new MaterializedViewCollectionMetadata(
                    VERSION_ZERO, UUID.randomUUID(), leftoverKey))
            .toBson());
    when(this.mockFindIterable.into(any())).thenReturn(found);

    List<LeaseManager.DeletedIndexLease> leases =
        leaseManager.findLeasesForDeletedIndex(indexId, knownKey);

    assertThat(leases.stream().map(entry -> entry.lease().id()))
        .containsExactly(knownKey, leftoverKey);
  }

  @Test
  public void findLeasesForDeletedIndex_returnsLeasesWithNamespacesAndRegistersMappings() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    ObjectId indexId = new ObjectId();
    String knownKey = indexId.toHexString() + "-hash1-1-0";
    String leftoverKey = indexId.toHexString() + "-hash2-1-0";
    registerViaGcScan(
        leaseManager,
        createLeaseWithMetadata(
            knownKey,
            new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), knownKey)));
    ArrayList<BsonDocument> found = new ArrayList<>();
    found.add(
        createLeaseWithMetadata(
                knownKey,
                new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), knownKey))
            .toBson());
    found.add(
        createLeaseWithMetadata(
                leftoverKey,
                new MaterializedViewCollectionMetadata(
                    VERSION_ZERO, UUID.randomUUID(), leftoverKey))
            .toBson());
    when(this.mockFindIterable.into(any())).thenReturn(found);

    List<LeaseManager.DeletedIndexLease> leases =
        leaseManager.findLeasesForDeletedIndex(indexId, knownKey);

    assertThat(leases.stream().map(entry -> entry.lease().id()))
        .containsExactly(knownKey, leftoverKey);
    // The scan pattern covers both id layouts: bare v0 ids and dash-suffixed v1+ ids.
    ArgumentCaptor<Bson> filterCaptor = ArgumentCaptor.forClass(Bson.class);
    verify(this.mockCollection).find(filterCaptor.capture());
    assertThat(
            filterCaptor
                .getValue()
                .toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
                .toJson())
        .contains("^" + indexId.toHexString());
    // Each lease arrives paired with the MV namespace to drop — no per-lease re-read needed.
    assertThat(
            leases.stream()
                .map(entry -> entry.matViewNamespace().orElseThrow().getCollectionName()))
        .containsExactly(knownKey, leftoverKey);
    assertThat(leaseManager.getLeaseKeyToDatabase()).containsKey(leftoverKey);
  }

  private Lease createLeaseWithMetadata(
      String collectionName, MaterializedViewCollectionMetadata metadata) {
    return new Lease(
        collectionName,
        Lease.SCHEMA_VERSION,
        UUID.randomUUID().toString(),
        "source-collection",
        HOSTNAME,
        Instant.now().plusSeconds(60),
        1L,
        "",
        "0",
        Map.of("0", new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN)),
        metadata,
        null,
        Lease.CleanupState.NOT_ELIGIBLE);
  }
}
