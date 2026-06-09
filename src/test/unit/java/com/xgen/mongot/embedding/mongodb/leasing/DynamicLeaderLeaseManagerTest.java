package com.xgen.mongot.embedding.mongodb.leasing;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.common.DefaultInternalDatabaseResolver;
import com.xgen.mongot.embedding.mongodb.common.InternalDatabaseResolver;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.testing.mongot.metrics.SimpleMetricsFactory;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
  private static final InternalDatabaseResolver DB_RESOLVER =
      new DefaultInternalDatabaseResolver(DATABASE_NAME);
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
            DB_RESOLVER,
            this.mvMetadataCatalog);
  }

  // ==================== Basic State Management ====================

  @Test
  public void isInLeaseAcquisitionBlackout_whenNoGiveUpApplied_returnsFalse() {
    assertThat(this.leaseManager.isInLeaseAcquisitionBlackout()).isFalse();
  }

  @Test
  public void isInLeaseAcquisitionBlackout_whenGiveUpExpired_returnsFalse() {
    LeaseManagerOpsCommands expiredOpsCommand =
        new LeaseManagerOpsCommands(
            Optional.of(
                new LeaseManagerOpsCommands.OpsGiveUpLeaseCommand(
                    HOSTNAME,
                    List.of("69a7ab02ac4c64cd5800caaf-66392faf9727adb4c26e76dc37b98b9f-1"),
                    Instant.now().minusSeconds(60))));

    DynamicLeaderLeaseManager managerWithExpiredOps =
        DynamicLeaderLeaseManager.create(
            this.mockAutoEmbeddingMongoClient,
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            HOSTNAME,
            DB_RESOLVER,
            this.mvMetadataCatalog,
            expiredOpsCommand);
    managerWithExpiredOps.executeOpsCommandsAfterInitializeLease("any-lease-key");

    assertThat(managerWithExpiredOps.isInLeaseAcquisitionBlackout()).isFalse();
  }

  @Test
  public void isInLeaseAcquisitionBlackout_whenGiveUpEmptyLeaseNames_returnsFalse() {
    LeaseManagerOpsCommands emptyLeaseNamesCommand =
        new LeaseManagerOpsCommands(
            Optional.of(
                new LeaseManagerOpsCommands.OpsGiveUpLeaseCommand(
                    HOSTNAME, List.of(), Instant.now().plusSeconds(60))));

    DynamicLeaderLeaseManager manager =
        DynamicLeaderLeaseManager.create(
            this.mockAutoEmbeddingMongoClient,
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            HOSTNAME,
            DB_RESOLVER,
            this.mvMetadataCatalog,
            emptyLeaseNamesCommand);
    manager.executeOpsCommandsAfterInitializeLease("any-lease-key");
    assertThat(manager.isInLeaseAcquisitionBlackout()).isFalse();
  }

  @Test
  public void isInLeaseAcquisitionBlackout_whenGiveUpMismatchingLeaseNames_returnsFalse() {
    LeaseManagerOpsCommands emptyLeaseNamesCommand =
        new LeaseManagerOpsCommands(
            Optional.of(
                new LeaseManagerOpsCommands.OpsGiveUpLeaseCommand(
                    HOSTNAME,
                    List.of("69a7ab02ac4c64cd5800caaf-66392faf9727adb4c26e76dc37b98b9f-1"),
                    Instant.now().plusSeconds(60))));

    DynamicLeaderLeaseManager manager =
        DynamicLeaderLeaseManager.create(
            this.mockAutoEmbeddingMongoClient,
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            HOSTNAME,
            DB_RESOLVER,
            this.mvMetadataCatalog,
            emptyLeaseNamesCommand);
    manager.executeOpsCommandsAfterInitializeLease("any-lease-key");
    assertThat(manager.isInLeaseAcquisitionBlackout()).isFalse();
  }

  @Test
  public void isInLeaseAcquisitionBlackout_whenGiveUpNotExpired_returnsTrue() {
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    LeaseManagerOpsCommands notExpiredOpsCommand =
        new LeaseManagerOpsCommands(
            Optional.of(
                new LeaseManagerOpsCommands.OpsGiveUpLeaseCommand(
                    HOSTNAME, List.of(leaseKey), Instant.now().plusSeconds(60))));

    DynamicLeaderLeaseManager managerWithNotExpiredOps =
        DynamicLeaderLeaseManager.create(
            this.mockAutoEmbeddingMongoClient,
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            HOSTNAME,
            DB_RESOLVER,
            this.mvMetadataCatalog,
            notExpiredOpsCommand);
    managerWithNotExpiredOps.add(indexGeneration);
    managerWithNotExpiredOps.putLease(
        leaseKey, createLease(generationId, HOSTNAME, Instant.now().plusSeconds(3600)));

    UpdateResult updateResult = mock(UpdateResult.class);
    when(updateResult.getModifiedCount()).thenReturn(1L);
    when(this.mockCollection.replaceOne(any(), any(BsonDocument.class))).thenReturn(updateResult);

    managerWithNotExpiredOps.executeOpsCommandsAfterInitializeLease(leaseKey);
    assertThat(managerWithNotExpiredOps.isInLeaseAcquisitionBlackout()).isTrue();
  }

  @Test
  public void executeOpsCommandsAfterInitializeLease_successIncrementsCounter() {
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    LeaseManagerOpsCommands opsCommand =
        new LeaseManagerOpsCommands(
            Optional.of(
                new LeaseManagerOpsCommands.OpsGiveUpLeaseCommand(
                    HOSTNAME, List.of(leaseKey), Instant.now().plusSeconds(60))));

    MeterAndFtdcRegistry meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
    String successMeterName =
        "embedding.leasing.stats."
            + DynamicLeaderLeaseManager.OPS_GIVE_UP_LEASE_SUCCESS_COUNTER_NAME;
    assertThat(meterAndFtdcRegistry.meterRegistry().find(successMeterName).counter()).isNull();

    DynamicLeaderLeaseManager manager =
        DynamicLeaderLeaseManager.create(
            this.mockAutoEmbeddingMongoClient,
            meterAndFtdcRegistry,
            HOSTNAME,
            DB_RESOLVER,
            this.mvMetadataCatalog,
            opsCommand);
    manager.add(indexGeneration);
    manager.putLease(
        leaseKey, createLease(generationId, HOSTNAME, Instant.now().plusSeconds(3600)));

    UpdateResult updateResult = mock(UpdateResult.class);
    when(updateResult.getModifiedCount()).thenReturn(1L);
    when(this.mockCollection.replaceOne(any(), any(BsonDocument.class))).thenReturn(updateResult);

    manager.executeOpsCommandsAfterInitializeLease(leaseKey);

    assertThat(meterAndFtdcRegistry.meterRegistry().find(successMeterName).counter()).isNotNull();
    assertThat(meterAndFtdcRegistry.meterRegistry().find(successMeterName).counter().count())
        .isEqualTo(1.0);
  }

  @Test
  public void executeOpsCommandsAfterInitializeLease_replaceNoopDoesNotRegisterSuccessCounter() {
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    LeaseManagerOpsCommands opsCommand =
        new LeaseManagerOpsCommands(
            Optional.of(
                new LeaseManagerOpsCommands.OpsGiveUpLeaseCommand(
                    HOSTNAME, List.of(leaseKey), Instant.now().plusSeconds(60))));

    MeterAndFtdcRegistry meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
    String successMeterName =
        "embedding.leasing.stats."
            + DynamicLeaderLeaseManager.OPS_GIVE_UP_LEASE_SUCCESS_COUNTER_NAME;

    DynamicLeaderLeaseManager manager =
        DynamicLeaderLeaseManager.create(
            this.mockAutoEmbeddingMongoClient,
            meterAndFtdcRegistry,
            HOSTNAME,
            DB_RESOLVER,
            this.mvMetadataCatalog,
            opsCommand);
    manager.add(indexGeneration);
    manager.putLease(
        leaseKey, createLease(generationId, HOSTNAME, Instant.now().plusSeconds(3600)));

    UpdateResult updateResult = mock(UpdateResult.class);
    when(updateResult.getModifiedCount()).thenReturn(0L);
    when(this.mockCollection.replaceOne(any(), any(BsonDocument.class))).thenReturn(updateResult);

    manager.executeOpsCommandsAfterInitializeLease(leaseKey);

    assertThat(meterAndFtdcRegistry.meterRegistry().find(successMeterName).counter()).isNull();
  }

  @Test
  public void add_newGeneration_startsAsFollowerAndIsLeaderReturnsFalse() {
    // Arrange
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();

    // Act
    this.leaseManager.add(indexGeneration);

    // Assert
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);
    assertThat(this.leaseManager.getLeaderGenerationIds()).doesNotContain(generationId);
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).containsKey(leaseKey);
    assertThat(this.leaseManager.getLeaseKeyToDatabase().get(leaseKey)).isEqualTo(DATABASE_NAME);
  }

  @Test
  public void getLeaderAndFollowerGenerationIds_afterAddAndDrop_returnsCorrectSets() {
    // Arrange
    MaterializedViewIndexGeneration indexGeneration1 = createTestIndexGeneration();
    MaterializedViewIndexGeneration indexGeneration2 = createTestIndexGeneration();
    MaterializedViewGenerationId generationId1 = indexGeneration1.getGenerationId();
    MaterializedViewGenerationId generationId2 = indexGeneration2.getGenerationId();

    // Act
    this.leaseManager.add(indexGeneration1);
    this.leaseManager.add(indexGeneration2);

    // Assert - both should be followers initially
    assertThat(this.leaseManager.getFollowerGenerationIds())
        .containsExactly(generationId1, generationId2);
    assertThat(this.leaseManager.getLeaderGenerationIds()).isEmpty();
    String leaseKey1 = getLeaseKeyFromCatalog(generationId1);
    String leaseKey2 = getLeaseKeyFromCatalog(generationId2);
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).containsKey(leaseKey1);
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).containsKey(leaseKey2);

    // Act - drop one
    this.leaseManager.drop(generationId1);

    // Assert - only one should remain in the tracking sets
    assertThat(this.leaseManager.getFollowerGenerationIds()).containsExactly(generationId2);
    assertThat(this.leaseManager.getLeaderGenerationIds()).isEmpty();
    // leaseKeyToDatabase is associated with the lease, not the generation tracking sets,
    // and is only cleaned up by dropLease().
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).containsKey(leaseKey1);
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).containsKey(leaseKey2);
  }

  @Test
  public void drop_existingGeneration_removesFromBothSets() {
    // Arrange
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);
    String leaseKey = getLeaseKeyFromCatalog(generationId);

    // Act
    this.leaseManager.drop(generationId);

    // Assert - generationId is untracked from both sets
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(generationId);
    assertThat(this.leaseManager.getLeaderGenerationIds()).doesNotContain(generationId);
    // Assert - lease is still in local cache (drop() does not remove from leases map)
    assertThat(this.leaseManager.getLeases()).containsKey(leaseKey);
  }

  // ==================== Leadership Acquisition ====================

  @Test
  public void tryAcquireLeadership_expiredLease_acquiresLeadership() {
    // Arrange
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
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
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
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
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
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
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
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
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
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
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
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
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // First, acquire leadership via tryAcquireLeadership
    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // Put the lease into the renewal window (< 50% TTL remaining) so heartbeat
    // actually attempts renewal instead of skipping it.
    putLeaseInRenewalWindow(generationId);

    // Act - heartbeat to renew
    setupSuccessfulLeaseUpdate();
    this.leaseManager.heartbeat();

    // Assert - still leader
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
  }

  @Test
  public void heartbeat_renewalFails_transitionsToFollower() {
    // Arrange
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // First, acquire leadership via tryAcquireLeadership
    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // Put the lease into the renewal window (< 50% TTL remaining) so heartbeat
    // actually attempts renewal instead of skipping it.
    putLeaseInRenewalWindow(generationId);

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
    MaterializedViewIndexGeneration gen1 = createTestIndexGeneration();
    MaterializedViewGenerationId genId1 = gen1.getGenerationId();
    this.leaseManager.add(gen1);

    MaterializedViewIndexGeneration gen2 = createTestIndexGeneration();
    MaterializedViewGenerationId genId2 = gen2.getGenerationId();
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

  @Test
  public void heartbeat_asLeader_skipsRenewalWhenLeaseRecentlyExtended() {
    // Arrange - acquire leadership
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // Act - update commit info, which extends the lease to now+5min
    setupSuccessfulLeaseUpdate();
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY);

    // Record how many replaceOne calls happened so far (acquisition + commitInfo update)
    int replaceOneCallsBeforeHeartbeat = 2;

    // Act - heartbeat should skip renewal because lease was recently extended
    this.leaseManager.heartbeat();

    // Assert - still leader
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    // replaceOne should NOT have been called again by heartbeat because the lease still
    // has more than half its TTL remaining after the commit info update
    verify(this.mockCollection, times(replaceOneCallsBeforeHeartbeat))
        .replaceOne(any(), any(BsonDocument.class));
  }

  // ==================== Leader-Only Operations ====================

  @Test
  public void updateCommitInfo_metadataRemovedDuringUpdate_doesNotCrash() throws Exception {
    // Arrange - become leader for a generation
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = gen.getGenerationId();
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
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = gen.getGenerationId();
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
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = gen.getGenerationId();
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
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = gen.getGenerationId();
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
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
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
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
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
    MaterializedViewIndexGeneration gen1 = createTestIndexGeneration();
    MaterializedViewGenerationId genId1 = gen1.getGenerationId();
    this.leaseManager.add(gen1);

    MaterializedViewIndexGeneration gen2 = createTestIndexGeneration();
    MaterializedViewGenerationId genId2 = gen2.getGenerationId();
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
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
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
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
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
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
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
  public void drop_asLeader_onlyUntracksGenerationId() throws Exception {
    // Arrange
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Acquire leadership via tryAcquireLeadership
    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // Act - drop only untracks generationId, does NOT delete from database
    this.leaseManager.drop(generationId);

    // Assert - generationId is untracked
    assertThat(this.leaseManager.getLeaderGenerationIds()).doesNotContain(generationId);
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(generationId);
    // Assert - lease is still in local cache (drop() does not remove from leases map)
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    assertThat(this.leaseManager.getLeases()).containsKey(leaseKey);
    // Assert - deleteOne was NOT called (drop no longer touches DB)
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  @Test
  public void drop_asFollower_onlyUntracksGenerationId() {
    // Arrange
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);
    // Note: not acquiring leadership, so we're still a follower

    // Act
    this.leaseManager.drop(generationId);

    // Assert - generationId is untracked
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(generationId);
    // Assert - lease is still in local cache (drop() does not remove from leases map)
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    assertThat(this.leaseManager.getLeases()).containsKey(leaseKey);
    // Assert - verify deleteOne was NOT called
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  @Test
  public void dropLease_asLeader_deletesFromDatabase() throws Exception {
    // Arrange
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);
    String leaseKey = getLeaseKeyFromCatalog(generationId);

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

    // Act - dropLease deletes from memory and database
    this.leaseManager.dropLease(leaseKey).join();

    // Assert - verify deleteOne was called and maps are cleaned
    verify(this.mockCollection).deleteOne(any(Bson.class));
    // Assert - lease is removed from local cache
    assertThat(this.leaseManager.getLeases()).doesNotContainKey(leaseKey);
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).doesNotContainKey(leaseKey);
  }

  @Test
  public void dropLease_asFollower_removesFromMemoryOnly() {
    // Arrange
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    // Note: not acquiring leadership, so we're still a follower

    // Act - dropLease removes from memory but does not delete from DB since we don't own it
    this.leaseManager.dropLease(leaseKey).join();

    // Assert - verify deleteOne was NOT called (we don't own the lease)
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
    // Assert - lease is removed from local cache
    assertThat(this.leaseManager.getLeases()).doesNotContainKey(leaseKey);
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).doesNotContainKey(leaseKey);
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
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).containsKey("mv-collection-name");
    assertThat(this.leaseManager.getLeaseKeyToDatabase().get("mv-collection-name"))
        .isEqualTo(DATABASE_NAME);
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

  @Test
  @SuppressWarnings("unchecked")
  public void executeOpsCommandsAfterInitializeLease_whenOwnedTarget_appliesGiveUpReplace()
      throws Exception {
    MeterAndFtdcRegistry meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
    MaterializedViewCollectionMetadataCatalog catalog =
        new MaterializedViewCollectionMetadataCatalog();
    String collectionName = "mv-collection-name";
    LeaseManagerOpsCommands ops =
        LeaseManagerOpsCommands.create(HOSTNAME, List.of(collectionName), "2099-01-01T00:00:00Z");
    DynamicLeaderLeaseManager manager =
        DynamicLeaderLeaseManager.create(
            this.mockAutoEmbeddingMongoClient,
            meterAndFtdcRegistry,
            HOSTNAME,
            DB_RESOLVER,
            catalog,
            ops);

    ObjectId indexId = new ObjectId();
    MaterializedViewIndexDefinitionGeneration indexDefGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(indexId);

    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), collectionName);

    Lease ownedLeaseOnDb =
        Lease.initialLease(
                collectionName,
                indexDefGen.getIndexDefinition().getCollectionUuid(),
                indexDefGen.getIndexDefinition().getLastObservedCollectionName(),
                "0",
                IndexStatus.unknown(),
                proposedMetadata)
            .withRenewedOwnership(HOSTNAME);

    FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
    when(this.mockCollection.find(any(Document.class))).thenReturn(findIterable);
    when(findIterable.first()).thenReturn(ownedLeaseOnDb.toBson());

    manager.initializeLease(indexDefGen, proposedMetadata);

    UpdateResult updateResult = mock(UpdateResult.class);
    when(updateResult.getModifiedCount()).thenReturn(1L);
    when(this.mockCollection.replaceOne(any(), any(BsonDocument.class))).thenReturn(updateResult);
    clearInvocations(this.mockCollection);

    manager.executeOpsCommandsAfterInitializeLease(collectionName);

    verify(this.mockCollection).replaceOne(any(), any(BsonDocument.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void executeOpsCommandsAfterInitializeLease_whenLeaseNotListed_doesNotApplyGiveUp()
      throws Exception {
    MeterAndFtdcRegistry meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
    MaterializedViewCollectionMetadataCatalog catalog =
        new MaterializedViewCollectionMetadataCatalog();
    String collectionName = "mv-collection-name";
    LeaseManagerOpsCommands ops =
        LeaseManagerOpsCommands.create(HOSTNAME, List.of(collectionName), "2099-01-01T00:00:00Z");
    DynamicLeaderLeaseManager manager =
        DynamicLeaderLeaseManager.create(
            this.mockAutoEmbeddingMongoClient,
            meterAndFtdcRegistry,
            HOSTNAME,
            DB_RESOLVER,
            catalog,
            ops);

    ObjectId indexId = new ObjectId();
    MaterializedViewIndexDefinitionGeneration indexDefGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(indexId);

    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), collectionName);

    Lease ownedLeaseOnDb =
        Lease.initialLease(
                collectionName,
                indexDefGen.getIndexDefinition().getCollectionUuid(),
                indexDefGen.getIndexDefinition().getLastObservedCollectionName(),
                "0",
                IndexStatus.unknown(),
                proposedMetadata)
            .withRenewedOwnership(HOSTNAME);

    FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
    when(this.mockCollection.find(any(Document.class))).thenReturn(findIterable);
    when(findIterable.first()).thenReturn(ownedLeaseOnDb.toBson());

    manager.initializeLease(indexDefGen, proposedMetadata);
    clearInvocations(this.mockCollection);

    manager.executeOpsCommandsAfterInitializeLease("unrelated-lease-key");

    verify(this.mockCollection, never()).replaceOne(any(), any(BsonDocument.class));
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
            DB_RESOLVER,
            this.mvMetadataCatalog);
    manager.syncLeasesFromMongod();

    Map<String, Lease> storedLeases = manager.getLeases();
    assertThat(storedLeases).containsKey(collectionName);
    assertThat(
            storedLeases.get(collectionName).materializedViewCollectionMetadata().collectionUuid())
        .isEqualTo(originalUuid);
    assertThat(manager.getLeaseKeyToDatabase()).containsKey(collectionName);
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
            DB_RESOLVER,
            this.mvMetadataCatalog);
    manager.syncLeasesFromMongod();

    // Assert - lease should be normalized with the new UUID
    Map<String, Lease> storedLeases = manager.getLeases();
    assertThat(storedLeases).containsKey(collectionName);
    assertThat(
            storedLeases.get(collectionName).materializedViewCollectionMetadata().collectionUuid())
        .isEqualTo(expectedUuid);
    assertThat(manager.getLeaseKeyToDatabase()).containsKey(collectionName);
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
            DB_RESOLVER,
            this.mvMetadataCatalog);

    // Assert - lease should NOT be stored (normalizeLeaseIfNeeded returned null)
    Map<String, Lease> storedLeases = manager.getLeases();
    assertThat(storedLeases).doesNotContainKey(collectionName);
    assertThat(manager.getLeaseKeyToDatabase()).doesNotContainKey(collectionName);
  }

  // ==================== Corruption Counter ====================

  @Test
  public void normalizeLeaseIfNeeded_corruptedLease_incrementsCounter() {
    // Arrange - create a lease with NIL UUID that will fail normalization
    var rawLease =
        BsonDocument.parse(
            "{\"_id\": \"corrupt-collection\",\n"
                + "        \"schemaVersion\": 1,\n"
                + "        \"collectionUuid\":"
                + " \"550e8400-e29b-41d4-a716-446655440000\",\n"
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

    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(rawLease);
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    // Mock getCollectionInfo to fail — triggers normalizeLeaseIfNeeded returning null
    ListCollectionsIterable<BsonDocument> emptyIterable = mock(ListCollectionsIterable.class);
    when(this.mockDatabase.listCollections(BsonDocument.class)).thenReturn(emptyIterable);
    MongoCursor<BsonDocument> emptyCursor = mock(MongoCursor.class);
    when(emptyCursor.hasNext()).thenReturn(false);
    when(emptyIterable.iterator()).thenReturn(emptyCursor);

    // Act
    SimpleMetricsFactory metricsFactory = new SimpleMetricsFactory();
    DynamicLeaderLeaseManager manager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            metricsFactory,
            HOSTNAME,
            DB_RESOLVER,
            this.mvMetadataCatalog);
    manager.syncLeasesFromMongod();

    // Assert
    assertThat(
            metricsFactory
                .meterRegistry
                .counter(metricsFactory.namespace + ".corruptedLeases")
                .count())
        .isEqualTo(1.0);
  }

  @Test
  public void parseLeaseOrThrow_malformedBson_incrementsCounterAndSkips() {
    // Arrange - a BSON document missing required fields so Lease.fromBson throws
    var malformedLease = BsonDocument.parse("{\"_id\": \"bad-lease\"}");

    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(malformedLease);
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    // Act
    SimpleMetricsFactory metricsFactory = new SimpleMetricsFactory();
    DynamicLeaderLeaseManager manager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            metricsFactory,
            HOSTNAME,
            DB_RESOLVER,
            this.mvMetadataCatalog);
    manager.syncLeasesFromMongod();

    // Assert - counter incremented by parseLeaseOrThrow, lease not stored
    assertThat(
            metricsFactory
                .meterRegistry
                .counter(metricsFactory.namespace + ".corruptedLeases")
                .count())
        .isEqualTo(1.0);
    assertThat(manager.getLeases()).isEmpty();
  }

  @Test
  public void parseLeaseOrThrow_nonStringId_extractsUnparseable() {
    // Arrange - a BSON document with non-string _id (e.g. ObjectId)
    var badIdLease = new BsonDocument();
    badIdLease.put("_id", new org.bson.BsonInt32(12345));

    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(badIdLease);
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    // Act
    SimpleMetricsFactory metricsFactory = new SimpleMetricsFactory();
    DynamicLeaderLeaseManager manager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            metricsFactory,
            HOSTNAME,
            DB_RESOLVER,
            this.mvMetadataCatalog);
    manager.syncLeasesFromMongod();

    // Assert - counter incremented, lease not stored, no crash
    assertThat(
            metricsFactory
                .meterRegistry
                .counter(metricsFactory.namespace + ".corruptedLeases")
                .count())
        .isEqualTo(1.0);
    assertThat(manager.getLeases()).isEmpty();
  }

  // ==================== Helper Methods ====================

  /**
   * Creates a MaterializedViewIndexGeneration and registers its generationId in the catalog so that
   * DynamicLeaderLeaseManager.getLeaseKey (which uses catalog) works.
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

  /**
   * Replaces the in-memory lease with one that has less than 50% TTL remaining, so that heartbeat's
   * skip-renewal optimization does not short-circuit the renewal attempt.
   */
  private void putLeaseInRenewalWindow(MaterializedViewGenerationId generationId) {
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    Lease current = this.leaseManager.getLeases().get(leaseKey);
    Lease nearExpiry =
        new Lease(
            current.id(),
            Lease.SCHEMA_VERSION,
            current.collectionUuid(),
            current.collectionName(),
            current.leaseOwner(),
            Instant.now().plusMillis(Lease.LEASE_EXPIRATION_MS / 4),
            current.leaseVersion(),
            current.commitInfo(),
            current.latestIndexDefinitionVersion(),
            current.indexDefinitionVersionStatusMap(),
            current.materializedViewCollectionMetadata(),
            current.steadyAsOfOplogPosition(),
            current.cleanupState());
    this.leaseManager.putLease(leaseKey, nearExpiry);
  }

  private Lease createLease(
      MaterializedViewGenerationId generationId, String owner, Instant expiration) {
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
        null,
        Lease.CleanupState.NOT_ELIGIBLE);
  }

  private Lease createLeaseWithCommitInfo(
      MaterializedViewGenerationId generationId, String owner, String commitInfo) {
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
        null,
        Lease.CleanupState.NOT_ELIGIBLE);
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
        null,
        Lease.CleanupState.NOT_ELIGIBLE);
  }

  @Test
  public void materializedViewTransientException_reasonConstructors_preserveReason() {
    var ex1 =
        new MaterializedViewTransientException(
            "test", MaterializedViewTransientException.Reason.LEASE_OPERATION_FAILED);
    assertThat(ex1.getReason())
        .isEqualTo(MaterializedViewTransientException.Reason.LEASE_OPERATION_FAILED);

    var ex2 =
        new MaterializedViewTransientException(
            new RuntimeException("cause"),
            MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE);
    assertThat(ex2.getReason())
        .isEqualTo(MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE);

    var ex3 =
        new MaterializedViewTransientException(
            "msg",
            new RuntimeException("cause"),
            MaterializedViewTransientException.Reason.BULK_WRITE_ERROR);
    assertThat(ex3.getReason())
        .isEqualTo(MaterializedViewTransientException.Reason.BULK_WRITE_ERROR);
  }

  @Test
  public void materializedViewTransientException_legacyConstructors_defaultToUnknown() {
    var ex1 = new MaterializedViewTransientException("test");
    assertThat(ex1.getReason()).isEqualTo(MaterializedViewTransientException.Reason.UNKNOWN);

    var ex2 = new MaterializedViewTransientException(new RuntimeException("cause"));
    assertThat(ex2.getReason()).isEqualTo(MaterializedViewTransientException.Reason.UNKNOWN);

    var ex3 = new MaterializedViewTransientException("msg", new RuntimeException("cause"));
    assertThat(ex3.getReason()).isEqualTo(MaterializedViewTransientException.Reason.UNKNOWN);
  }

  @Test
  public void materializedViewTransientException_nullReason_defaultsToUnknown() {
    MaterializedViewTransientException.Reason nullReason = null;
    var ex = new MaterializedViewTransientException("test", nullReason);
    assertThat(ex.getReason()).isEqualTo(MaterializedViewTransientException.Reason.UNKNOWN);
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
}
