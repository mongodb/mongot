package com.xgen.mongot.embedding.mongodb.leasing;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.testing.mongot.metrics.SimpleMetricsFactory;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

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
    assertThat(leaseManager.getLeaseKeyToDatabase())
        .containsKey("mv-collection-name");
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
    assertThat(leaseManager.getLeaseKeyToDatabase().get(collectionName))
        .isEqualTo(DATABASE_NAME);
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
    assertThat(followerLeaseManager.getLeaseKeyToDatabase())
        .containsKey("follower-mv-collection");
    assertThat(
            followerLeaseManager.getLeaseKeyToDatabase().get("follower-mv-collection"))
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

    List<Lease> candidates = leaseManager.findGcCandidates(Instant.now());

    assertThat(candidates).hasSize(1);
    assertThat(candidates.get(0).id()).isEqualTo(collectionName);
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

    List<Lease> candidates = leaseManager.findGcCandidates(Instant.now());

    assertThat(candidates).hasSize(1);
    assertThat(candidates.get(0).id()).isEqualTo(collectionName);
  }

  @Test
  public void findGcCandidates_queryFailure_returnsEmpty() {
    StaticLeaderLeaseManager leaseManager = createLeaseManager(false);
    when(this.mockCollection.find(any(Bson.class))).thenThrow(new RuntimeException("find failed"));

    assertThat(leaseManager.findGcCandidates(Instant.now())).isEmpty();
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
    leaseManager.findGcCandidates(Instant.now());
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
    leaseManager.findGcCandidates(Instant.now());
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
    leaseManager.findGcCandidates(Instant.now());
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
    MaterializedViewIndexDefinitionGeneration defGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(new ObjectId());
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
