package com.xgen.mongot.embedding.mongodb.leasing;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.testing.mongot.metrics.SimpleMetricsFactory;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
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

  private AutoEmbeddingMongoClient mockAutoEmbeddingMongoClient;
  private MongoDatabase mockDatabase;
  private MongoCollection<BsonDocument> mockCollection;
  private FindIterable<BsonDocument> mockFindIterable;
  private DynamicLeaderLeaseManager leaseManager;
  private MaterializedViewCollectionMetadataCatalog mvMetadataCatalog;
  private SimpleMetricsFactory metricsFactory;

  @Before
  public void setUp() {
    this.mockAutoEmbeddingMongoClient = mock(AutoEmbeddingMongoClient.class);
    this.mockDatabase = mock(MongoDatabase.class);
    this.mockCollection = mock(MongoCollection.class);
    this.mockFindIterable = mock(FindIterable.class);
    this.mvMetadataCatalog = new MaterializedViewCollectionMetadataCatalog();
    var mockMongoClient = mock(MongoClient.class);
    when(this.mockAutoEmbeddingMongoClient.getLeaseManagerMongoClient())
        .thenReturn(Optional.of(mockMongoClient));
    when(mockMongoClient.getDatabase(DATABASE_NAME)).thenReturn(this.mockDatabase);
    when(this.mockDatabase.getCollection(
            DynamicLeaderLeaseManager.LEASE_COLLECTION_NAME, BsonDocument.class))
        .thenReturn(this.mockCollection);
    when(this.mockCollection.withReadConcern(any())).thenReturn(this.mockCollection);
    when(this.mockCollection.withReadPreference(any())).thenReturn(this.mockCollection);
    when(this.mockCollection.withWriteConcern(any())).thenReturn(this.mockCollection);
    when(this.mockCollection.find()).thenReturn(this.mockFindIterable);
    when(this.mockCollection.find(any(Bson.class))).thenReturn(this.mockFindIterable);
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>());

    this.metricsFactory = new SimpleMetricsFactory();
    this.leaseManager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            this.metricsFactory,
            HOSTNAME,
            DATABASE_NAME,
            this.mvMetadataCatalog);
  }

  // ==================== Basic State Management ====================

  @Test
  public void isInLeaseAcquisitionBlackout_whenNoGiveUpApplied_returnsFalse() {
    assertThat(this.leaseManager.isInLeaseAcquisitionBlackout()).isFalse();
  }

  @Test
  public void isInLeaseAcquisitionBlackout_whenGiveUpExpired_returnsFalse() {
    // Create a lease manager with an ops command that has already expired
    LeaseManagerOpsCommands expiredOpsCommand =
        new LeaseManagerOpsCommands(
            java.util.Optional.of(
                new LeaseManagerOpsCommands.OpsGiveUpLeaseCommand(
                    HOSTNAME,
                    java.util.List.of(
                        "69a7ab02ac4c64cd5800caaf-66392faf9727adb4c26e76dc37b98b9f-1"),
                    Instant.now().minusSeconds(60))));

    DynamicLeaderLeaseManager managerWithExpiredOps =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            new SimpleMetricsFactory(),
            HOSTNAME,
            DATABASE_NAME,
            this.mvMetadataCatalog);
    // Call opsGiveUpLease via reflection since it is private
    try {
      java.lang.reflect.Method method =
          DynamicLeaderLeaseManager.class.getDeclaredMethod(
              "opsGiveUpLease", java.util.Optional.class);
      method.setAccessible(true);
      method.invoke(managerWithExpiredOps, expiredOpsCommand.opsGiveUpLease());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Blackout should NOT be set because the command is expired
    assertThat(managerWithExpiredOps.isInLeaseAcquisitionBlackout()).isFalse();
  }

  @Test
  public void isInLeaseAcquisitionBlackout_whenGiveUpEmptyLeaseNames_stillSetsBlackout() {
    LeaseManagerOpsCommands emptyLeaseNamesCommand =
        new LeaseManagerOpsCommands(
            java.util.Optional.of(
                new LeaseManagerOpsCommands.OpsGiveUpLeaseCommand(
                    HOSTNAME, java.util.List.of(), Instant.now().plusSeconds(60))));

    DynamicLeaderLeaseManager manager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            new SimpleMetricsFactory(),
            HOSTNAME,
            DATABASE_NAME,
            this.mvMetadataCatalog);
    try {
      java.lang.reflect.Method method =
          DynamicLeaderLeaseManager.class.getDeclaredMethod(
              "opsGiveUpLease", java.util.Optional.class);
      method.setAccessible(true);
      method.invoke(manager, emptyLeaseNamesCommand.opsGiveUpLease());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertThat(manager.isInLeaseAcquisitionBlackout()).isTrue();
  }

  @Test
  public void isInLeaseAcquisitionBlackout_whenGiveUpNotExpired_returnsTrue() {
    // Create a lease manager with an ops command that has not expired
    LeaseManagerOpsCommands notExpiredOpsCommand =
        new LeaseManagerOpsCommands(
            java.util.Optional.of(
                new LeaseManagerOpsCommands.OpsGiveUpLeaseCommand(
                    HOSTNAME,
                    java.util.List.of(
                        "69a7ab02ac4c64cd5800caaf-66392faf9727adb4c26e76dc37b98b9f-1"),
                    Instant.now().plusSeconds(60))));

    DynamicLeaderLeaseManager managerWithNotExpiredOps =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            new SimpleMetricsFactory(),
            HOSTNAME,
            DATABASE_NAME,
            this.mvMetadataCatalog);
    try {
      java.lang.reflect.Method method =
          DynamicLeaderLeaseManager.class.getDeclaredMethod(
              "opsGiveUpLease", java.util.Optional.class);
      method.setAccessible(true);
      method.invoke(managerWithNotExpiredOps, notExpiredOpsCommand.opsGiveUpLease());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertThat(managerWithNotExpiredOps.isInLeaseAcquisitionBlackout()).isTrue();
  }

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
    // Normal drop should not increment the dangling lease counter.
    assertThat(getDanglingLeaseOnDropCount()).isEqualTo(0.0);
  }

  @Test
  public void drop_metadataAlreadyRemoved_incrementsDanglingLeaseCounter() {
    // Arrange
    IndexGeneration gen = createTestIndexGeneration();
    GenerationId generationId = gen.getGenerationId();
    this.leaseManager.add(gen);
    this.mvMetadataCatalog.removeMetadata(generationId);

    // Act
    this.leaseManager.drop(generationId);

    // Assert - dangling lease counter should be incremented
    assertThat(getDanglingLeaseOnDropCount()).isEqualTo(1.0);
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
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

  @Test
  public void heartbeat_metadataRemovedDuringRenewal_removesFromLeadersAndContinues() {
    // Arrange - set up two leader generations
    IndexGeneration gen1 = createTestIndexGeneration();
    GenerationId genId1 = gen1.getGenerationId();
    this.leaseManager.add(gen1);

    IndexGeneration gen2 = createTestIndexGeneration();
    GenerationId genId2 = gen2.getGenerationId();
    this.leaseManager.add(gen2);

    // Acquire leadership for both
    Lease expiredLease1 = createLease(genId1, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease1);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(genId1);
    assertThat(this.leaseManager.isLeader(genId1)).isTrue();

    Lease expiredLease2 = createLease(genId2, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease2);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(genId2);
    assertThat(this.leaseManager.isLeader(genId2)).isTrue();

    // Simulate concurrent index deletion: remove gen1's metadata from catalog
    this.mvMetadataCatalog.removeMetadata(genId1);

    // Setup renewal to succeed for gen2
    setupSuccessfulLeaseUpdate();

    // Act - heartbeat should handle gen1's IllegalStateException and continue to gen2
    this.leaseManager.heartbeat();

    // Assert
    // gen1: removed from leaders, NOT added to followers (index is gone)
    assertThat(this.leaseManager.getLeaderGenerationIds()).doesNotContain(genId1);
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(genId1);

    // gen2: still a leader (heartbeat continued past gen1's error)
    assertThat(this.leaseManager.isLeader(genId2)).isTrue();
  }

  // ==================== Leader-Only Operations ====================

  @Test
  public void updateCommitInfo_metadataRemovedDuringUpdate_doesNotCrash() throws Exception {
    // Arrange - become leader for a generation
    IndexGeneration gen = createTestIndexGeneration();
    GenerationId generationId = gen.getGenerationId();
    this.leaseManager.add(gen);

    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // Simulate concurrent index deletion: remove metadata
    this.mvMetadataCatalog.removeMetadata(generationId);

    // Act - updateCommitInfo should handle ISE gracefully, not crash
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY);

    // Assert - generation should no longer be leader
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
  }

  @Test
  public void updateReplicationStatus_metadataRemovedDuringUpdate_doesNotCrash() throws Exception {
    // Arrange - become leader for a generation
    IndexGeneration gen = createTestIndexGeneration();
    GenerationId generationId = gen.getGenerationId();
    this.leaseManager.add(gen);

    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // Simulate concurrent index deletion: remove metadata
    this.mvMetadataCatalog.removeMetadata(generationId);

    // Act - updateReplicationStatus should handle ISE gracefully, not crash
    this.leaseManager.updateReplicationStatus(
        generationId, 0L, new IndexStatus(IndexStatus.StatusCode.UNKNOWN));

    // Assert - generation should no longer be leader
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
  }

  @Test
  public void getCommitInfo_metadataRemovedAsLeader_doesNotCrash() throws Exception {
    // Arrange - become leader for a generation
    IndexGeneration gen = createTestIndexGeneration();
    GenerationId generationId = gen.getGenerationId();
    this.leaseManager.add(gen);

    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // Simulate concurrent index deletion: remove metadata
    this.mvMetadataCatalog.removeMetadata(generationId);

    // Act - getCommitInfo should handle ISE gracefully, not crash
    EncodedUserData result = this.leaseManager.getCommitInfo(generationId);

    // Assert - should return empty rather than crash
    assertThat(result).isEqualTo(EncodedUserData.EMPTY);
  }

  @Test
  public void getSteadyAsOfOplogPosition_metadataRemoved_returnsEmpty() {
    // Arrange - become leader for a generation
    IndexGeneration gen = createTestIndexGeneration();
    GenerationId generationId = gen.getGenerationId();
    this.leaseManager.add(gen);

    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // Simulate concurrent index deletion: remove metadata
    this.mvMetadataCatalog.removeMetadata(generationId);

    // Act - should handle ISE gracefully, not crash
    Optional<BsonTimestamp> result = this.leaseManager.getSteadyAsOfOplogPosition(generationId);

    // Assert - should return empty rather than crash
    assertThat(result).isEmpty();
  }

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
  public void pollFollowerStatuses_metadataRemovedDuringPoll_skipsRemovedAndContinues()
      throws Exception {
    // Arrange - set up two follower generations
    IndexGeneration gen1 = createTestIndexGeneration();
    GenerationId genId1 = gen1.getGenerationId();
    this.leaseManager.add(gen1);

    IndexGeneration gen2 = createTestIndexGeneration();
    GenerationId genId2 = gen2.getGenerationId();
    this.leaseManager.add(gen2);

    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(genId1);
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(genId2);

    // Simulate concurrent index deletion: remove gen1's metadata from catalog
    this.mvMetadataCatalog.removeMetadata(genId1);

    // Act - pollFollowerStatuses should handle gen1's IllegalStateException and continue to gen2
    LeaseManager.FollowerPollResult result = this.leaseManager.pollFollowerStatuses();

    // Assert
    // gen1: removed from followers (index is gone)
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(genId1);

    // gen2: still a follower with a status result
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(genId2);
    assertThat(result.statuses()).containsKey(genId2);
  }

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

  // ==================== initializeLease ====================

  @Test
  public void initializeLease_noExistingLease_insertsAndReturnsProposedMetadata() throws Exception {
    // Arrange
    ObjectId indexId = new ObjectId();
    MaterializedViewIndexDefinitionGeneration indexDefGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(indexId);

    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(
            MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO,
            UUID.randomUUID(),
            "mv-collection-name");

    InsertOneResult insertResult = mock(InsertOneResult.class);
    when(this.mockCollection.insertOne(any(BsonDocument.class))).thenReturn(insertResult);

    // Act
    MaterializedViewCollectionMetadata result =
        this.leaseManager.initializeLease(indexDefGen, proposedMetadata);

    // Assert - should return proposed metadata and insert into DB
    assertThat(result).isEqualTo(proposedMetadata);
    verify(this.mockCollection).insertOne(any(BsonDocument.class));

    // Verify lease was stored in memory
    assertThat(this.leaseManager.getLeases()).containsKey("mv-collection-name");
  }

  @Test
  public void initializeLease_leaseAlreadyInMemory_returnsExistingMetadata() throws Exception {
    // Arrange - first initialize a lease successfully
    ObjectId indexId = new ObjectId();
    MaterializedViewIndexDefinitionGeneration indexDefGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(indexId);

    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(
            MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO,
            UUID.randomUUID(),
            "mv-collection-name");

    // Track inserted lease to return on subsequent find calls
    AtomicReference<BsonDocument> insertedLease = new AtomicReference<>();

    InsertOneResult insertResult = mock(InsertOneResult.class);
    when(this.mockCollection.insertOne(any(BsonDocument.class)))
        .thenAnswer(
            invocation -> {
              insertedLease.set(invocation.getArgument(0));
              return insertResult;
            });

    // First find returns null, subsequent finds return the inserted lease
    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
    when(this.mockCollection.find(any(Document.class))).thenReturn(findIterable);
    when(findIterable.first()).thenAnswer(invocation -> insertedLease.get());

    this.leaseManager.initializeLease(indexDefGen, proposedMetadata);

    // Act - call initializeLease again with different proposed metadata but same collection name
    MaterializedViewCollectionMetadata differentProposed =
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(1, Map.of()),
            UUID.randomUUID(),
            "mv-collection-name");

    MaterializedViewCollectionMetadata result =
        this.leaseManager.initializeLease(indexDefGen, differentProposed);

    // Assert - should return the existing lease's metadata (from first call), not the new proposed
    assertThat(result.collectionName()).isEqualTo("mv-collection-name");
    assertThat(result.schemaMetadata().materializedViewSchemaVersion()).isEqualTo(0L);
    // insertOne should only have been called once (from the first initializeLease)
    verify(this.mockCollection, org.mockito.Mockito.times(1)).insertOne(any(BsonDocument.class));
  }

  @Test
  public void initializeLease_duplicateKeyError_returnsExistingMetadataFromDb() throws Exception {
    // Arrange
    ObjectId indexId = new ObjectId();
    MaterializedViewIndexDefinitionGeneration indexDefGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(indexId);

    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(
            MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO,
            UUID.randomUUID(),
            "mv-collection-name");

    // Simulate duplicate key error on insertOne
    WriteError writeError = new WriteError(11000, "duplicate key error", new BsonDocument());
    MongoWriteException duplicateKeyException =
        new MongoWriteException(writeError, new ServerAddress());
    when(this.mockCollection.insertOne(any(BsonDocument.class))).thenThrow(duplicateKeyException);

    // Setup: existing lease in DB with different metadata (winner's metadata)
    UUID winnerUuid = UUID.randomUUID();
    MaterializedViewCollectionMetadata winnerMetadata =
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(1, Map.of()),
            winnerUuid,
            "mv-collection-name");
    Lease existingLease =
        Lease.initialLease(
            "mv-collection-name",
            indexDefGen.getIndexDefinition().getCollectionUuid(),
            indexDefGen.getIndexDefinition().getLastObservedCollectionName(),
            "0",
            IndexStatus.unknown(),
            winnerMetadata);

    // Setup find to return the existing lease from DB
    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
    when(this.mockCollection.find(any(Document.class))).thenReturn(findIterable);
    when(findIterable.first()).thenReturn(existingLease.toBson());

    // Act
    MaterializedViewCollectionMetadata result =
        this.leaseManager.initializeLease(indexDefGen, proposedMetadata);

    // Assert - should return the winner's metadata, not our proposed one
    assertThat(result.collectionName()).isEqualTo("mv-collection-name");
    assertThat(result.schemaMetadata().materializedViewSchemaVersion()).isEqualTo(1L);
    assertThat(result.collectionUuid()).isEqualTo(winnerUuid);
  }

  // ==================== normalizeLeaseIfNeeded ====================

  @Test
  public void normalizeLeaseIfNeeded_notNilUuid_returnsLeaseUnchanged() {
    // Arrange - create a lease with no MaterializedViewCollectionMetadata (needs to be normalized)
    String collectionName = "test-mv-collection";
    UUID originalUuid = UUID.randomUUID();
    Lease lease = createLeaseWithMatViewUuid(collectionName, originalUuid);

    // Put the lease in the mock collection for syncLeasesFromMongod
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    // Act - create a new manager (syncLeasesFromMongod will be called in constructor)
    DynamicLeaderLeaseManager manager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            new SimpleMetricsFactory(),
            HOSTNAME,
            DATABASE_NAME,
            this.mvMetadataCatalog);
    manager.syncLeasesFromMongod();

    Map<String, Lease> storedLeases = manager.getLeases();
    assertThat(storedLeases).containsKey(collectionName);
    assertThat(
            storedLeases.get(collectionName).materializedViewCollectionMetadata().collectionUuid())
        .isEqualTo(originalUuid);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void normalizeLeaseIfNeeded_resolvesWithCollectionUuid() {
    // Arrange - create a lease with no MaterializedViewCollectionMetadata (needs to be normalized)
    String collectionName = "test-mv-collection";
    UUID expectedUuid = UUID.randomUUID();
    var rawLease =
        BsonDocument.parse(
            "{\"_id\": \"test-mv-collection\",\n"
                + "        \"schemaVersion\": 1,\n"
                + "        \"collectionUuid\": \"550e8400-e29b-41d4-a716-446655440000\",\n"
                + "        \"collectionName\": \"source-collection\",\n"
                + "        \"leaseOwner\": \"localhost\",\n"
                + "        \"leaseExpiration\": {\n"
                + "          \"$date\": \"2024-12-06T00:57:15.661Z\"\n"
                + "        },\n"
                + "        \"leaseVersion\": 1,\n"
                + "        \"commitInfo\": \"{}\",\n"
                + "        \"latestIndexDefinitionVersion\": \"1\",\n"
                + "        \"indexDefinitionVersionStatusMap\": {\n"
                + "          \"1\": {\n"
                + "            \"isQueryable\": false,\n"
                + "            \"indexStatusCode\": \"INITIAL_SYNC\"\n"
                + "          }\n"
                + "        }}");

    // Put the lease in the mock collection for syncLeasesFromMongod
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(rawLease);
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    // Mock the internal database for getCollectionInfo
    setupInternalDatabaseCollectionInfo(collectionName, expectedUuid);

    // Act - create a new manager (syncLeasesFromMongod will call normalizeLeaseIfNeeded)
    DynamicLeaderLeaseManager manager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            new SimpleMetricsFactory(),
            HOSTNAME,
            DATABASE_NAME,
            this.mvMetadataCatalog);
    manager.syncLeasesFromMongod();

    // Assert - lease should be normalized with the new UUID
    Map<String, Lease> storedLeases = manager.getLeases();
    assertThat(storedLeases).containsKey(collectionName);
    assertThat(
            storedLeases.get(collectionName).materializedViewCollectionMetadata().collectionUuid())
        .isEqualTo(expectedUuid);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void normalizeLeaseIfNeeded_getCollectionInfoFails_leaseNotStored() {
    // Arrange - create a lease with no MaterializedViewCollectionMetadata (needs to be normalized)
    String collectionName = "test-mv-collection";
    var rawLease =
        BsonDocument.parse(
            "{\"_id\": \"test-mv-collection\",\n"
                + "        \"schemaVersion\": 1,\n"
                + "        \"collectionUuid\": \"550e8400-e29b-41d4-a716-446655440000\",\n"
                + "        \"collectionName\": \"source-collection\",\n"
                + "        \"leaseOwner\": \"localhost\",\n"
                + "        \"leaseExpiration\": {\n"
                + "          \"$date\": \"2024-12-06T00:57:15.661Z\"\n"
                + "        },\n"
                + "        \"leaseVersion\": 1,\n"
                + "        \"commitInfo\": \"{}\",\n"
                + "        \"latestIndexDefinitionVersion\": \"1\",\n"
                + "        \"indexDefinitionVersionStatusMap\": {\n"
                + "          \"1\": {\n"
                + "            \"isQueryable\": false,\n"
                + "            \"indexStatusCode\": \"INITIAL_SYNC\"\n"
                + "          }\n"
                + "        }}");

    // Put the lease in the mock collection for syncLeasesFromMongod
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(rawLease);
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    // Mock the internal database to fail (no collections returned). Reuse this.mockDatabase to
    // avoid overwriting the getCollection stub needed by the constructor.
    ListCollectionsIterable<BsonDocument> emptyIterable = mock(ListCollectionsIterable.class);
    when(this.mockDatabase.listCollections(BsonDocument.class)).thenReturn(emptyIterable);
    MongoCursor<BsonDocument> emptyCursor = mock(MongoCursor.class);
    when(emptyCursor.hasNext()).thenReturn(false);
    when(emptyIterable.iterator()).thenReturn(emptyCursor);

    // Act - create a new manager (normalizeLeaseIfNeeded will fail and return null)
    DynamicLeaderLeaseManager manager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            new SimpleMetricsFactory(),
            HOSTNAME,
            DATABASE_NAME,
            this.mvMetadataCatalog);

    // Assert - lease should NOT be stored (normalizeLeaseIfNeeded returned null)
    Map<String, Lease> storedLeases = manager.getLeases();
    assertThat(storedLeases).doesNotContainKey(collectionName);
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
        Lease.SCHEMA_VERSION,
        "fa41efe9-dd13-4976-a6ce-009682ec4257",
        "collection-name",
        owner,
        expiration,
        1L,
        "",
        "0",
        Map.of("0", new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN)),
        new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), "collection-name"),
        null);
  }

  private Lease createLeaseWithCommitInfo(
      GenerationId generationId, String owner, String commitInfo) {
    return new Lease(
        getLeaseKeyFromCatalog(generationId),
        Lease.SCHEMA_VERSION,
        "fa41efe9-dd13-4976-a6ce-009682ec4257",
        "collection-name",
        owner,
        Instant.now().plusSeconds(60),
        1L,
        commitInfo,
        "0",
        Map.of("0", new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN)),
        new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), "collection-name"),
        null);
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

  private Lease createLeaseWithMatViewUuid(String collectionName, UUID uuid) {
    return new Lease(
        collectionName,
        Lease.SCHEMA_VERSION,
        uuid.toString(),
        collectionName,
        HOSTNAME,
        Instant.now().plusSeconds(60),
        1L,
        "",
        "0",
        Map.of("0", new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN)),
        new MaterializedViewCollectionMetadata(
            MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO,
            uuid,
            collectionName),
        null);
  }

  @SuppressWarnings("unchecked")
  private void setupInternalDatabaseCollectionInfo(String collectionName, UUID uuid) {
    // Reuse this.mockDatabase to avoid overwriting the getCollection stub needed by the
    // constructor.
    ListCollectionsIterable<BsonDocument> listCollIterable = mock(ListCollectionsIterable.class);
    when(this.mockDatabase.listCollections(BsonDocument.class)).thenReturn(listCollIterable);

    BsonDocument collectionInfoDoc =
        new BsonDocument()
            .append("type", new BsonString("collection"))
            .append("name", new BsonString(collectionName))
            .append("info", new MongoDbCollectionInfo.Collection.Info(uuid).toBson());

    MongoCursor<BsonDocument> cursor = mock(MongoCursor.class);
    when(cursor.hasNext()).thenReturn(true).thenReturn(false);
    when(cursor.next()).thenReturn(collectionInfoDoc);
    when(listCollIterable.iterator()).thenReturn(cursor);
  }

  private double getDanglingLeaseOnDropCount() {
    return this.metricsFactory.get("danglingLeaseOnDrop").counter().count();
  }
}
