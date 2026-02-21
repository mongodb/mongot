package com.xgen.mongot.embedding.mongodb.leasing;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
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
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.testing.mongot.metrics.SimpleMetricsFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DynamicLeaderLeaseManager}.
 *
 * <p>These tests verify the dynamic leader election behavior at the index level, including
 * leadership acquisition, renewal, and state transitions.
 */
public class DynamicLeaderLeaseManagerTest {

  private static final String HOSTNAME = "test-host";
  private static final String DATABASE_NAME = "test-db";
  private static final String OTHER_HOSTNAME = "other-host";

  private MongoClient mockMongoClient;
  private MongoDatabase mockDatabase;
  private MongoCollection<BsonDocument> mockCollection;
  private FindIterable<BsonDocument> mockFindIterable;
  private DynamicLeaderLeaseManager leaseManager;
  private MaterializedViewCollectionMetadataCatalog mvMetadataCatalog;

  @Before
  public void setUp() {
    this.mockMongoClient = mock(MongoClient.class);
    this.mockDatabase = mock(MongoDatabase.class);
    this.mockCollection = mock(MongoCollection.class);
    this.mockFindIterable = mock(FindIterable.class);
    this.mvMetadataCatalog = new MaterializedViewCollectionMetadataCatalog();

    when(this.mockMongoClient.getDatabase(DATABASE_NAME)).thenReturn(this.mockDatabase);
    when(this.mockDatabase.getCollection(
            DynamicLeaderLeaseManager.LEASE_COLLECTION_NAME, BsonDocument.class))
        .thenReturn(this.mockCollection);
    when(this.mockCollection.withReadConcern(any())).thenReturn(this.mockCollection);
    when(this.mockCollection.withReadPreference(any())).thenReturn(this.mockCollection);
    when(this.mockCollection.find()).thenReturn(this.mockFindIterable);
    when(this.mockCollection.find(any(Bson.class))).thenReturn(this.mockFindIterable);
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>());

    this.leaseManager =
        new DynamicLeaderLeaseManager(
            this.mockMongoClient,
            new SimpleMetricsFactory(),
            HOSTNAME,
            DATABASE_NAME,
            this.mvMetadataCatalog);
  }

  // ==================== Basic State Management ====================

  @Test
  public void add_newGeneration_startsAsFollowerAndIsLeaderReturnsFalse() {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();

    // Act
    this.leaseManager.add(indexGeneration);

    // Assert
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);
    assertThat(this.leaseManager.getLeaderGenerationIds()).doesNotContain(generationId);
  }

  @Test
  public void getLeaderAndFollowerGenerationIds_afterAddAndDrop_returnsCorrectSets() {
    // Arrange
    IndexGeneration indexGeneration1 = createTestIndexGeneration();
    IndexGeneration indexGeneration2 = createTestIndexGeneration();
    GenerationId generationId1 = indexGeneration1.getGenerationId();
    GenerationId generationId2 = indexGeneration2.getGenerationId();

    // Act
    this.leaseManager.add(indexGeneration1);
    this.leaseManager.add(indexGeneration2);

    // Assert - both should be followers initially
    assertThat(this.leaseManager.getFollowerGenerationIds())
        .containsExactly(generationId1, generationId2);
    assertThat(this.leaseManager.getLeaderGenerationIds()).isEmpty();

    // Act - drop one
    this.leaseManager.drop(generationId1);

    // Assert - only one should remain
    assertThat(this.leaseManager.getFollowerGenerationIds()).containsExactly(generationId2);
    assertThat(this.leaseManager.getLeaderGenerationIds()).isEmpty();
  }

  @Test
  public void drop_existingGeneration_removesFromBothSets() {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Act
    this.leaseManager.drop(generationId);

    // Assert
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(generationId);
    assertThat(this.leaseManager.getLeaderGenerationIds()).doesNotContain(generationId);
  }

  // ==================== Leadership Acquisition ====================

  @Test
  public void tryAcquireLeadership_expiredLease_acquiresLeadership() {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Create an expired lease owned by another host
    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();

    // Poll first to update in-memory lease (matches production flow)
    this.leaseManager.pollFollowerStatuses();

    // Act
    boolean acquired = this.leaseManager.tryAcquireLeadership(generationId);

    // Assert
    assertThat(acquired).isTrue();
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    assertThat(this.leaseManager.getLeaderGenerationIds()).contains(generationId);
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(generationId);
  }

  @Test
  public void tryAcquireLeadership_weOwnLease_reacquiresLeadership() {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Create a lease owned by us (simulating restart scenario)
    Lease ourLease = createLease(generationId, HOSTNAME, Instant.now().plusSeconds(30));
    setupFindLeaseFromDatabase(ourLease);
    setupSuccessfulLeaseUpdate();

    // Poll first to update in-memory lease (matches production flow)
    this.leaseManager.pollFollowerStatuses();

    // Act
    boolean acquired = this.leaseManager.tryAcquireLeadership(generationId);

    // Assert
    assertThat(acquired).isTrue();
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    assertThat(this.leaseManager.getLeaderGenerationIds()).contains(generationId);
  }

  @Test
  public void tryAcquireLeadership_activeLeaseBelongsToOther_returnsFalse() {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Create an active lease owned by another host
    Lease activeLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().plusSeconds(60));
    setupFindLeaseFromDatabase(activeLease);

    // Poll first to update in-memory lease (matches production flow)
    this.leaseManager.pollFollowerStatuses();

    // Act
    boolean acquired = this.leaseManager.tryAcquireLeadership(generationId);

    // Assert
    assertThat(acquired).isFalse();
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);
    assertThat(this.leaseManager.getLeaderGenerationIds()).doesNotContain(generationId);
  }

  @Test
  public void heartbeat_asFollower_doesNothing() {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);
    // Note: not acquiring leadership, so we're still a follower

    // Act
    this.leaseManager.heartbeat();

    // Assert - still a follower, no database writes
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);
    verify(this.mockCollection, never()).replaceOne(any(), any(BsonDocument.class));
  }

  @Test
  public void tryAcquireLeadership_noLeaseExists_createsLeaseAndBecomesLeader() {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Setup: no lease exists in database, upsert succeeds, and we verify ownership
    setupNoLeaseInDatabase();
    setupSuccessfulLeaseUpdate();
    // After upsert, we query DB to verify ownership - return a lease owned by us
    Lease ourLease = createLease(generationId, HOSTNAME, Instant.now().plusSeconds(60));
    setupFindLeaseFromDatabase(ourLease);

    // Poll first to confirm no lease in DB (matches production flow)
    this.leaseManager.pollFollowerStatuses();

    // Act
    boolean acquired = this.leaseManager.tryAcquireLeadership(generationId);

    // Assert - should create lease and become leader
    assertThat(acquired).isTrue();
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    assertThat(this.leaseManager.getLeaderGenerationIds()).contains(generationId);
  }

  @Test
  public void tryAcquireLeadership_raceCondition_returnsFalse() {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Setup: expired lease exists, but update fails due to race condition
    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupFailedLeaseUpdate();

    // Poll first to update in-memory lease (matches production flow)
    this.leaseManager.pollFollowerStatuses();

    // Act
    boolean acquired = this.leaseManager.tryAcquireLeadership(generationId);

    // Assert - should fail and remain follower
    assertThat(acquired).isFalse();
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);
  }

  // ==================== Leadership Renewal ====================

  @Test
  public void heartbeat_asLeader_renewsLease() {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // First, acquire leadership via tryAcquireLeadership
    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // Act - heartbeat to renew
    setupSuccessfulLeaseUpdate();
    this.leaseManager.heartbeat();

    // Assert - still leader
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
  }

  @Test
  public void heartbeat_renewalFails_transitionsToFollower() {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // First, acquire leadership via tryAcquireLeadership
    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // Setup renewal to fail (version mismatch - another instance took over)
    setupFailedLeaseUpdate();
    Lease otherLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().plusSeconds(60));
    setupFindLeaseFromDatabase(otherLease);

    // Act
    this.leaseManager.heartbeat();

    // Assert - should transition to follower
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);
  }

  // ==================== Leader-Only Operations ====================

  @Test
  public void updateCommitInfo_asLeader_succeeds() throws Exception {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Acquire leadership via tryAcquireLeadership
    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // Act - should succeed without exception
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY);

    // Assert - verify replaceOne was called at least once (for the update)
    // Note: replaceOne is called multiple times: once for acquiring leadership,
    // and once for updating commit info
    verify(this.mockCollection, org.mockito.Mockito.atLeast(1))
        .replaceOne(any(), any(BsonDocument.class));
  }

  @Test
  public void updateReplicationStatus_notLeader_doesNotUpdateDatabase() throws Exception {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);
    // Note: not acquiring leadership, so we're still a follower

    // Act - should not throw, but should not update database either
    this.leaseManager.updateReplicationStatus(
        generationId, 0L, new IndexStatus(IndexStatus.StatusCode.UNKNOWN));

    // Assert - verify replaceOne was NOT called (since we're not leader)
    verify(this.mockCollection, never())
        .replaceOne(any(Bson.class), any(BsonDocument.class), any());
    verify(this.mockCollection, never()).replaceOne(any(Bson.class), any());
  }

  // ==================== Follower Operations ====================

  @Test
  public void pollFollowerStatuses_andGetCommitInfo_readsFromDatabase() throws Exception {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Setup lease in database with commit info (active lease owned by another host)
    Lease leaseWithCommitInfo =
        createLeaseWithCommitInfo(generationId, OTHER_HOSTNAME, "test-commit-info");
    setupFindLeaseFromDatabase(leaseWithCommitInfo);

    // Act - poll follower statuses
    LeaseManager.FollowerPollResult result = this.leaseManager.pollFollowerStatuses();

    // Assert - statuses should contain the generation
    assertThat(result.statuses()).containsKey(generationId);
    // Active lease owned by another host should NOT be in acquirableLeases
    assertThat(result.acquirableLeases()).doesNotContain(generationId);

    // Act - get commit info as follower
    EncodedUserData commitInfo = this.leaseManager.getCommitInfo(generationId);

    // Assert
    assertThat(commitInfo.asString()).isEqualTo("test-commit-info");
  }

  @Test
  public void pollFollowerStatuses_expiredLease_includesInAcquirableLeases() throws Exception {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Setup expired lease in database owned by another host
    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);

    // Act - poll follower statuses
    LeaseManager.FollowerPollResult result = this.leaseManager.pollFollowerStatuses();

    // Assert - expired lease should be in acquirableLeases
    assertThat(result.statuses()).containsKey(generationId);
    assertThat(result.acquirableLeases()).contains(generationId);
  }

  @Test
  public void pollFollowerStatuses_weOwnLease_includesInAcquirableLeases() throws Exception {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Setup active lease owned by us (restart scenario)
    Lease ourLease = createLease(generationId, HOSTNAME, Instant.now().plusSeconds(60));
    setupFindLeaseFromDatabase(ourLease);

    // Act - poll follower statuses
    LeaseManager.FollowerPollResult result = this.leaseManager.pollFollowerStatuses();

    // Assert - lease owned by us should be in acquirableLeases (eligible for re-acquisition)
    assertThat(result.statuses()).containsKey(generationId);
    assertThat(result.acquirableLeases()).contains(generationId);
  }

  // ==================== Edge Cases ====================

  @Test
  public void drop_asLeader_deletesFromDatabase() throws Exception {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Acquire leadership via tryAcquireLeadership
    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // Setup delete mock
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(this.mockCollection.deleteOne(any(Bson.class))).thenReturn(deleteResult);

    // Act
    this.leaseManager.drop(generationId).join();

    // Assert - verify deleteOne was called
    verify(this.mockCollection).deleteOne(any(Bson.class));
  }

  @Test
  public void drop_asFollower_doesNotDeleteFromDatabase() {
    // Arrange
    IndexGeneration indexGeneration = createTestIndexGeneration();
    GenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);
    // Note: not acquiring leadership, so we're still a follower

    // Act
    this.leaseManager.drop(generationId);

    // Assert - verify deleteOne was NOT called
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  // ==================== Helper Methods ====================

  /**
   * Creates an index generation and registers its generationId in the catalog so that
   * DynamicLeaderLeaseManager.getLeaseKey (which uses catalog) works.
   */
  private IndexGeneration createTestIndexGeneration() {
    IndexGeneration gen = mockIndexGeneration(new ObjectId());
    addCatalogMetadataForGeneration(gen.getGenerationId());
    return gen;
  }

  private void addCatalogMetadataForGeneration(GenerationId generationId) {
    String collectionName = generationId.indexId.toHexString();
    MaterializedViewCollectionMetadata metadata =
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
            UUID.randomUUID(),
            collectionName);
    this.mvMetadataCatalog.addMetadata(generationId, metadata);
  }

  /** Returns the lease key (collection name) for the given generation from the catalog. */
  private String getLeaseKeyFromCatalog(GenerationId generationId) {
    return this.mvMetadataCatalog.getMetadata(generationId).collectionName();
  }

  private Lease createLease(GenerationId generationId, String owner, Instant expiration) {
    return new Lease(
        getLeaseKeyFromCatalog(generationId),
        1,
        "fa41efe9-dd13-4976-a6ce-009682ec4257",
        "collection-name",
        owner,
        expiration,
        1L,
        "",
        "0",
        Map.of(
            "0",
            new Lease.IndexDefinitionVersionStatus(
                false, IndexStatus.StatusCode.UNKNOWN)), // IndexStatus.StatusCode.UNKNOWN
        new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), "collection-name"));
  }

  private Lease createLeaseWithCommitInfo(
      GenerationId generationId, String owner, String commitInfo) {
    return new Lease(
        getLeaseKeyFromCatalog(generationId),
        1,
        "fa41efe9-dd13-4976-a6ce-009682ec4257",
        "collection-name",
        owner,
        Instant.now().plusSeconds(60),
        1L,
        commitInfo,
        "0",
        Map.of("0", new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN)),
        new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), "collection-name"));
  }

  @SuppressWarnings("unchecked")
  private void setupFindLeaseFromDatabase(Lease lease) {
    // Create a single FindIterable mock that supports both single and batch reads
    FindIterable<BsonDocument> findIterable = mock(FindIterable.class);

    // Setup for single lease lookup (getLeaseFromDatabase uses find(Document).first())
    when(findIterable.first()).thenReturn(lease.toBson());

    // Setup for batch read (pollFollowerStatuses uses find(Bson).into())
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(findIterable.into(any())).thenReturn(leaseList);

    // Both find(Document) and find(Bson) should return the same iterable
    when(this.mockCollection.find(any(Document.class))).thenReturn(findIterable);
    when(this.mockCollection.find(any(Bson.class))).thenReturn(findIterable);
  }

  private void setupSuccessfulLeaseUpdate() {
    UpdateResult updateResult = mock(UpdateResult.class);
    when(updateResult.getMatchedCount()).thenReturn(1L);
    when(this.mockCollection.replaceOne(any(), any(BsonDocument.class))).thenReturn(updateResult);
  }

  private void setupFailedLeaseUpdate() {
    UpdateResult updateResult = mock(UpdateResult.class);
    when(updateResult.getMatchedCount()).thenReturn(0L);
    when(this.mockCollection.replaceOne(any(), any(BsonDocument.class))).thenReturn(updateResult);
  }

  @SuppressWarnings("unchecked")
  private void setupNoLeaseInDatabase() {
    FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
    when(this.mockCollection.find(any(Document.class))).thenReturn(findIterable);
    when(findIterable.first()).thenReturn(null);
  }
}
