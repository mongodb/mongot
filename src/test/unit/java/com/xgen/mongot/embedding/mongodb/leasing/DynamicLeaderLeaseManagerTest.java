package com.xgen.mongot.embedding.mongodb.leasing;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
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
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.testing.mongot.metrics.SimpleMetricsFactory;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

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
  private MongoClient mockMongoClient;
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
    this.mockMongoClient = mock(MongoClient.class);
    when(this.mockAutoEmbeddingMongoClient.getLeaseManagerMongoClient())
        .thenReturn(Optional.of(this.mockMongoClient));
    when(this.mockMongoClient.getDatabase(DATABASE_NAME)).thenReturn(this.mockDatabase);
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
  public void postRestoreSanitize_atLeaseLoad_repairsOrFallsBack() {
    // 24-hex tenant id; if seen as a db-name prefix on a dedicated cluster, it signals a stale
    // post-flex→dedicated lease that the load-time sanitize must clean before any consumer reads.
    String tenantId = "647343c92726405e854130e4";
    setupSuccessfulLeaseUpdate();
    ArgumentCaptor<BsonDocument> writtenLease = ArgumentCaptor.forClass(BsonDocument.class);

    // CASE A: stale lease (tenant-prefixed namespace). listCollections mocks are not set up, so
    // the load-time sanitize fails inside pollFollowerStatuses → normalizeLeaseIfNeeded returns
    // null (bubble-up). The stale lease is DROPPED before anything downstream can see it, so
    // nothing with stale tenant-prefixed commitInfo ever reaches replaceOne.
    MaterializedViewIndexGeneration staleGen = createTestIndexGeneration();
    String prefixedNamespaceCommitInfo =
        IndexCommitUserData.createChangeStreamResume(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace(tenantId + "_sample_mflix", "embedded_movies"),
                    ChangeStreamUtils.resumeToken(new BsonTimestamp(1L))),
                IndexFormatVersion.CURRENT)
            .toEncodedData()
            .asString();
    Lease staleLease =
        createLeaseWithCommitInfo(
            staleGen.getGenerationId(), HOSTNAME, prefixedNamespaceCommitInfo);
    setupFindLeaseFromDatabase(staleLease);
    this.leaseManager.add(staleGen);
    this.leaseManager.pollFollowerStatuses();

    this.leaseManager.tryAcquireLeadership(staleGen.getGenerationId());
    verify(this.mockCollection, never())
        .replaceOne(
            any(),
            argThat(
                (BsonDocument doc) ->
                    doc != null
                        && doc.containsKey("commitInfo")
                        && doc.getString("commitInfo").getValue().contains(tenantId)));
    assertThat(
            repairCounter(DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME))
        .isEqualTo(1.0);
    assertThat(
            repairCounter(DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME))
        .isEqualTo(0.0);

    // CASE B: lease with no tenant prefix. requiresPostRestoreRepair returns false at load time;
    // no sanitize attempted, no counter touched, and the written lease's commitInfo is unchanged.
    clearInvocations(this.mockCollection);
    UpdateResult successUpdate = mockSuccessfulUpdate();
    when(this.mockCollection.replaceOne(any(), any(BsonDocument.class))).thenReturn(successUpdate);
    MaterializedViewIndexGeneration normalGen = createTestIndexGeneration();
    String unprefixedCommitInfo =
        IndexCommitUserData.createChangeStreamResume(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace("sample_mflix", "embedded_movies"),
                    ChangeStreamUtils.resumeToken(new BsonTimestamp(1L))),
                IndexFormatVersion.CURRENT)
            .toEncodedData()
            .asString();
    Lease normalLease =
        createLeaseWithCommitInfo(normalGen.getGenerationId(), HOSTNAME, unprefixedCommitInfo);
    setupFindLeaseFromDatabase(normalLease);
    this.leaseManager.add(normalGen);
    this.leaseManager.pollFollowerStatuses();

    // Snapshot counters before CASE B so the assertion is a delta, not an absolute value
    // tied to CASE A.
    double failureBefore =
        repairCounter(DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME);
    double successBefore =
        repairCounter(DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME);

    assertThat(this.leaseManager.tryAcquireLeadership(normalGen.getGenerationId())).isTrue();
    verify(this.mockCollection).replaceOne(any(), writtenLease.capture());
    assertThat(writtenLease.getValue().getString("commitInfo").getValue())
        .isEqualTo(unprefixedCommitInfo);
    // No repair attempted on the non-stale lease — neither counter moves.
    assertThat(
            repairCounter(DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME))
        .isEqualTo(failureBefore);
    assertThat(
            repairCounter(DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME))
        .isEqualTo(successBefore);
  }

  @Test
  public void postRestoreSanitize_atLeaseLoad_writesRepairedLeaseOnSuccess() {
    String tenantId = "647343c92726405e854130e4";
    UUID newSourceUuid = UUID.randomUUID();
    UUID newMvUuid = UUID.randomUUID();

    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease staleLease =
        createLeaseWithCommitInfo(
            gen.getGenerationId(),
            HOSTNAME,
            staleCommitInfo(tenantId, "sample_mflix", "embedded_movies"));
    String leaseCollectionName = staleLease.materializedViewCollectionMetadata().collectionName();
    setupFindLeaseFromDatabase(staleLease);
    // listCollections returns Collection variants with our fresh UUIDs on both source and MV dbs.
    setupListCollections("sample_mflix", leaseCollectionName, newSourceUuid);
    // Caller passes dbResolver.resolveDefault() — DATABASE_NAME ("test-db") here, not the
    // hardcoded __mdb_internal_search constant.
    setupListCollections(DATABASE_NAME, leaseCollectionName, newMvUuid);
    setupSuccessfulLeaseUpdate();

    this.leaseManager.add(gen);
    this.leaseManager.pollFollowerStatuses();

    Lease inMem = this.leaseManager.getLeases().get(staleLease.id());
    assertThat(inMem.commitInfo()).isEqualTo(EncodedUserData.EMPTY.asString());
    assertThat(inMem.collectionUuid()).isEqualTo(newSourceUuid.toString());
    assertThat(inMem.materializedViewCollectionMetadata().collectionUuid()).isEqualTo(newMvUuid);
    assertThat(inMem.steadyAsOfOplogPosition()).isNull();
    assertThat(
            repairCounter(DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME))
        .isAtLeast(1.0);
    assertThat(
            repairCounter(DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME))
        .isEqualTo(0.0);
  }

  @Test
  public void syncLeasesFromMongod_staleLease_dropsFromInMemoryOnRepairFailure() {
    String tenantId = "647343c92726405e854130e4";
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease staleLease =
        createLeaseWithCommitInfo(
            gen.getGenerationId(),
            HOSTNAME,
            staleCommitInfo(tenantId, "sample_mflix", "embedded_movies"));
    // Pre-register the leaseKey→db mapping. add() also puts a fresh placeholder into this.leases,
    // which syncLeasesFromMongod would normally overwrite with the fetched-and-normalized lease.
    this.leaseManager.add(gen);
    Lease placeholderBeforeSync = this.leaseManager.getLeases().get(staleLease.id());
    // Bulk find() returns the stale lease.
    when(this.mockFindIterable.into(any()))
        .thenReturn(new ArrayList<>(List.of(staleLease.toBson())));
    // No listCollections mocks — sanitize fails, so normalizeLeaseIfNeeded returns null and the
    // stale lease is dropped instead of being handed back with stale namespace/UUIDs.

    this.leaseManager.syncLeasesFromMongod();

    // The stale in-DB lease was not adopted; whatever's in memory is NOT the stale one (it's the
    // placeholder from add(), or absent). Either way, the tenant-prefixed commitInfo did NOT
    // survive the load.
    Lease inMem = this.leaseManager.getLeases().get(staleLease.id());
    if (inMem != null) {
      assertThat(inMem.commitInfo()).doesNotContain(tenantId);
      assertThat(inMem).isEqualTo(placeholderBeforeSync);
    }
    assertThat(
            repairCounter(DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME))
        .isAtLeast(1.0);
    assertThat(
            repairCounter(DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME))
        .isEqualTo(0.0);
  }

  private static String staleCommitInfo(String tenantId, String db, String coll) {
    return IndexCommitUserData.createChangeStreamResume(
            ChangeStreamResumeInfo.create(
                new MongoNamespace(tenantId + "_" + db, coll),
                ChangeStreamUtils.resumeToken(new BsonTimestamp(1L))),
            IndexFormatVersion.CURRENT)
        .toEncodedData()
        .asString();
  }

  /**
   * Stubs {@code getDatabase(dbName).listCollections(BsonDocument.class)} to yield a single
   * Collection-shaped document with the given UUID, so {@code buildUuidResolver} resolves to it.
   * Reuses the already-mocked DB for {@code DATABASE_NAME} so the existing lease-collection stub is
   * preserved.
   */
  @SuppressWarnings("unchecked")
  private void setupListCollections(String dbName, String collName, UUID uuid) {
    MongoDatabase db;
    if (dbName.equals(DATABASE_NAME)) {
      db = this.mockDatabase;
    } else {
      db = mock(MongoDatabase.class);
      when(this.mockMongoClient.getDatabase(dbName)).thenReturn(db);
    }
    ListCollectionsIterable<BsonDocument> listIter = mock(ListCollectionsIterable.class);
    BsonDocument collectionInfoDoc =
        new BsonDocument()
            .append("type", new BsonString("collection"))
            .append("name", new BsonString(collName))
            .append(
                "info",
                new BsonDocument("uuid", new BsonBinary(uuid, UuidRepresentation.STANDARD)));
    List<BsonDocument> docs = List.of(collectionInfoDoc);
    when(listIter.iterator()).thenAnswer(inv -> wrapAsCursor(docs.iterator()));
    when(db.listCollections(BsonDocument.class)).thenReturn(listIter);
  }

  @SuppressWarnings("unchecked")
  private static <T> MongoCursor<T> wrapAsCursor(java.util.Iterator<T> iter) {
    MongoCursor<T> cursor = mock(MongoCursor.class);
    when(cursor.hasNext()).thenAnswer(inv -> iter.hasNext());
    when(cursor.next()).thenAnswer(inv -> iter.next());
    return cursor;
  }

  private static UpdateResult mockSuccessfulUpdate() {
    UpdateResult r = mock(UpdateResult.class);
    when(r.getMatchedCount()).thenReturn(1L);
    return r;
  }

  private double repairCounter(String counterName) {
    return this.metricsFactory
        .meterRegistry
        .counter(this.metricsFactory.namespace + "." + counterName)
        .count();
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

  @Test
  @SuppressWarnings("unchecked")
  public void pollFollowerStatuses_batchFetchThrows_returnsEmptyStatusesNotUnknown() {
    // Verify that a DB failure (e.g. mongod unreachable during cluster pause) does NOT force
    // followers to UNKNOWN. Returning empty statuses lets callers preserve whatever status was
    // last known (e.g. STEADY), which allows processReplicationStatus() in MMS to correctly
    // transition the auto-embedding index to STALE rather than filtering it as transient.
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Make the batch find throw to simulate mongod being unreachable.
    FindIterable<BsonDocument> failingIterable = mock(FindIterable.class);
    when(this.mockCollection.find(any(Bson.class))).thenReturn(failingIterable);
    when(failingIterable.into(any())).thenThrow(new RuntimeException("mongod unreachable"));

    LeaseManager.FollowerPollResult result = this.leaseManager.pollFollowerStatuses();

    // Statuses must be empty (not UNKNOWN) so the caller's existing status is preserved.
    assertThat(result.statuses()).isEmpty();
    // No acquirable leases on failure — avoid spurious leadership acquisition attempts.
    assertThat(result.acquirableLeases()).isEmpty();
    // The generation should still be tracked as a follower (not silently dropped).
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void pollFollowerStatuses_batchFetchThrows_preservesPreviousStatus() {
    // 3-step test verifying the end-to-end preservation contract:
    // a successful STEADY status must survive a subsequent failed poll.
    // If the failed poll returned UNKNOWN instead of EMPTY, a caller that applied that result
    // would silently regress the index — this test guards against that regression.
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    // Step 1: successful poll — DB returns a STEADY lease.
    Lease steadyLease =
        createLeaseWithStatus(
            generationId,
            OTHER_HOSTNAME,
            Instant.now().plusSeconds(60),
            IndexStatus.StatusCode.STEADY);
    setupFindLeaseFromDatabase(steadyLease);
    LeaseManager.FollowerPollResult successResult = this.leaseManager.pollFollowerStatuses();
    assertThat(successResult.statuses()).containsKey(generationId);
    assertThat(successResult.statuses().get(generationId).getStatusCode())
        .isEqualTo(IndexStatus.StatusCode.STEADY);

    // Step 2: next poll throws (e.g. mongod temporarily unreachable).
    FindIterable<BsonDocument> failingIterable = mock(FindIterable.class);
    when(this.mockCollection.find(any(Bson.class))).thenReturn(failingIterable);
    when(failingIterable.into(any())).thenThrow(new RuntimeException("mongod unreachable"));
    LeaseManager.FollowerPollResult failResult = this.leaseManager.pollFollowerStatuses();

    // Step 3: EMPTY is returned — not {generationId: UNKNOWN}. A caller that treats EMPTY as
    // "no new information" will retain its existing STEADY status rather than resetting to UNKNOWN.
    assertThat(failResult.statuses()).isEmpty();
    assertThat(failResult.acquirableLeases()).isEmpty();
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);
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
    // The index-deletion lease delete goes through the op-executor, which emits the
    // leaseTableCollection success counter tagged operation="dropLease" (vs the GC delete's
    // operation="deleteStaleLease" on the same series).
    assertThat(
            this.metricsFactory
                .meterRegistry
                .find(this.metricsFactory.namespace + ".leaseTableCollection.successfulRequests")
                .tag("operation", "dropLease")
                .counter()
                .count())
        .isEqualTo(1.0);
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

    // In production the winner and loser both resolve to the same live MV collection via
    // MaterializedViewCollectionResolver.getCollectionInfo, so proposedMetadata.uuid always
    // equals existingLease.mvUuid on the race-loser path. Use the same UUID here to reflect that
    // — the loser semantic (adopt winner's schema) still comes through via schemaMetadata.
    UUID liveMvUuid = UUID.randomUUID();
    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(
            MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO,
            liveMvUuid,
            "mv-collection-name");

    // Simulate duplicate key error on insertOne
    WriteError writeError = new WriteError(11000, "duplicate key error", new BsonDocument());
    MongoWriteException duplicateKeyException =
        new MongoWriteException(writeError, new ServerAddress());
    when(this.mockCollection.insertOne(any(BsonDocument.class))).thenThrow(duplicateKeyException);

    // Winner's persisted lease carries the same live MV UUID but a different schema version.
    MaterializedViewCollectionMetadata winnerMetadata =
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(1, Map.of()),
            liveMvUuid,
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

    // Assert - loser adopts the winner's schema (proves the DB lease was reused, not overwritten
    // by a fresh insertOne of proposedMetadata).
    assertThat(result.collectionName()).isEqualTo("mv-collection-name");
    assertThat(result.schemaMetadata().materializedViewSchemaVersion()).isEqualTo(1L);
    assertThat(result.collectionUuid()).isEqualTo(liveMvUuid);
    // Since UUIDs match, no post-restore repair should have fired.
    assertThat(
            repairCounter(DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME))
        .isEqualTo(0.0);
  }

  @Test
  public void initializeLease_existingLeaseWithStaleMvUuid_repairsInMemoryFromProposedMetadata()
      throws Exception {
    // Empty-commitInfo flex→dedicated migration: the DB lease was in INITIAL_SYNC at migration
    // time so it has no commitInfo namespace for the tenant-prefix check to fire, but its MV UUID
    // is stale relative to the live restored MV. initializeLease detects the mismatch via
    // proposedMetadata (which carries the live UUID resolved from the current MV collection).
    ObjectId indexId = new ObjectId();
    MaterializedViewIndexDefinitionGeneration indexDefGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(indexId);
    String collectionName = "mv-collection-name";
    UUID staleMvUuid = UUID.randomUUID();
    UUID freshMvUuid = UUID.randomUUID();

    MaterializedViewCollectionMetadata staleMetadata =
        new MaterializedViewCollectionMetadata(VERSION_ZERO, staleMvUuid, collectionName);
    Lease staleLease =
        Lease.initialLease(
            collectionName,
            indexDefGen.getIndexDefinition().getCollectionUuid(),
            indexDefGen.getIndexDefinition().getLastObservedCollectionName(),
            "0",
            IndexStatus.unknown(),
            staleMetadata);

    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
    when(this.mockCollection.find(any(Document.class))).thenReturn(findIterable);
    when(findIterable.first()).thenReturn(staleLease.toBson());

    MaterializedViewCollectionMetadata proposedMetadata =
        new MaterializedViewCollectionMetadata(VERSION_ZERO, freshMvUuid, collectionName);

    MaterializedViewCollectionMetadata result =
        this.leaseManager.initializeLease(indexDefGen, proposedMetadata);

    assertThat(result.collectionUuid()).isEqualTo(freshMvUuid);
    assertThat(
            this.leaseManager
                .getLeases()
                .get(staleLease.id())
                .materializedViewCollectionMetadata()
                .collectionUuid())
        .isEqualTo(freshMvUuid);
    assertThat(
            repairCounter(DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME))
        .isAtLeast(1.0);
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

  // ==================== findGcCandidates ====================

  @Test
  public void findGcCandidates_returnsLeases_andRegistersDatabaseMapping() {
    String collectionName = "gc-orphan-collection";
    Lease lease = createLeaseWithMatViewUuid(collectionName, UUID.randomUUID());
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    List<Lease> candidates = this.leaseManager.findGcCandidates(Instant.now());

    assertThat(candidates).hasSize(1);
    assertThat(candidates.get(0).id()).isEqualTo(collectionName);
    // The lease→database mapping must be registered even though no configured generation ever
    // added this lease, so a follow-up markEligibleForCleanup can resolve the database.
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).containsKey(collectionName);
    assertThat(this.leaseManager.getLeaseKeyToDatabase().get(collectionName))
        .isEqualTo(DATABASE_NAME);
  }

  @Test
  public void findGcCandidates_queriesByExpirationBeforeCutoff() {
    Instant cutoff = Instant.parse("2026-01-01T00:00:00Z");

    this.leaseManager.findGcCandidates(cutoff);

    ArgumentCaptor<Bson> filterCaptor = ArgumentCaptor.forClass(Bson.class);
    verify(this.mockCollection).find(filterCaptor.capture());
    BsonDocument filter =
        filterCaptor
            .getValue()
            .toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
    assertThat(filter.getDocument("leaseExpiration").getDateTime("$lt").getValue())
        .isEqualTo(cutoff.toEpochMilli());
  }

  @Test
  public void findGcCandidates_skipsCorruptedLeases() {
    Lease good = createLeaseWithMatViewUuid("good-collection", UUID.randomUUID());
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(new BsonDocument("_id", new BsonString("corrupted")));
    leaseList.add(good.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    List<Lease> candidates = this.leaseManager.findGcCandidates(Instant.now());

    assertThat(candidates).hasSize(1);
    assertThat(candidates.get(0).id()).isEqualTo("good-collection");
  }

  @Test
  public void findGcCandidates_queryFailure_returnsEmpty() {
    when(this.mockCollection.find(any(Bson.class))).thenThrow(new RuntimeException("find failed"));

    assertThat(this.leaseManager.findGcCandidates(Instant.now())).isEmpty();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void findGcCandidates_scansTenantDatabasesKnownToThisManager() {
    // On multi-tenant clusters each source database resolves to its own internal database; the
    // scan must cover every internal database the manager has learned, not just the default.
    String tenantDb = "tenantA-" + DATABASE_NAME;
    InternalDatabaseResolver tenantResolver =
        new InternalDatabaseResolver() {
          @Override
          public String resolve(String sourceDatabaseName) {
            return tenantDb;
          }

          @Override
          public String resolveDefault() {
            return DATABASE_NAME;
          }
        };

    // Tenant database mocks: one orphaned lease lives there; the default database has none.
    MongoClient mongoClient =
        this.mockAutoEmbeddingMongoClient.getLeaseManagerMongoClient().orElseThrow();
    MongoDatabase tenantDatabase = mock(MongoDatabase.class);
    MongoCollection<BsonDocument> tenantCollection = mock(MongoCollection.class);
    FindIterable<BsonDocument> tenantFindIterable = mock(FindIterable.class);
    when(mongoClient.getDatabase(tenantDb)).thenReturn(tenantDatabase);
    when(tenantDatabase.getCollection(
            DynamicLeaderLeaseManager.LEASE_COLLECTION_NAME, BsonDocument.class))
        .thenReturn(tenantCollection);
    when(tenantCollection.withReadConcern(any())).thenReturn(tenantCollection);
    when(tenantCollection.withReadPreference(any())).thenReturn(tenantCollection);
    when(tenantCollection.withWriteConcern(any())).thenReturn(tenantCollection);
    when(tenantCollection.find(any(Bson.class))).thenReturn(tenantFindIterable);
    Lease tenantLease = createLeaseWithMatViewUuid("tenant-orphan-lease", UUID.randomUUID());
    ArrayList<BsonDocument> tenantLeases = new ArrayList<>();
    tenantLeases.add(tenantLease.toBson());
    when(tenantFindIterable.into(any())).thenReturn(tenantLeases);

    DynamicLeaderLeaseManager manager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            new SimpleMetricsFactory(),
            HOSTNAME,
            tenantResolver,
            this.mvMetadataCatalog);
    // A configured generation teaches the manager the tenant's internal database.
    manager.add(createTestIndexGeneration());

    List<Lease> candidates = manager.findGcCandidates(Instant.now());

    assertThat(candidates.stream().anyMatch(c -> c.id().equals("tenant-orphan-lease"))).isTrue();
    // The orphan's mapping points at the tenant database it was found in.
    assertThat(manager.getLeaseKeyToDatabase().get("tenant-orphan-lease")).isEqualTo(tenantDb);
    // Completeness of the restriction: across the whole test only the default database and the
    // single learned tenant database were ever touched — no other tenant is queried.
    ArgumentCaptor<String> dbCaptor = ArgumentCaptor.forClass(String.class);
    verify(mongoClient, atLeastOnce()).getDatabase(dbCaptor.capture());
    assertThat(new HashSet<>(dbCaptor.getAllValues())).containsExactly(DATABASE_NAME, tenantDb);
  }

  @Test
  public void findGcCandidates_scansOnlyDatabasesHostingAutoEmbeddingGenerations() {
    // The scan set is learned from configured generations, never enumerated from the cluster: a
    // multi-tenant deployment with no auto-embedding generation registered on this mongot sweeps
    // the default internal database only, regardless of how many tenants exist.
    MongoClient mongoClient =
        this.mockAutoEmbeddingMongoClient.getLeaseManagerMongoClient().orElseThrow();
    clearInvocations(mongoClient);

    this.leaseManager.findGcCandidates(Instant.now());

    ArgumentCaptor<String> dbCaptor = ArgumentCaptor.forClass(String.class);
    verify(mongoClient, times(1)).getDatabase(dbCaptor.capture());
    assertThat(dbCaptor.getAllValues()).containsExactly(DATABASE_NAME);
  }

  // ==================== markEligibleForCleanup ====================

  @Test
  public void markEligibleForCleanup_modified_returnsTrueAndUpdatesInMemoryLease() {
    String collectionName = "stale-collection";
    Lease lease = createLeaseWithMatViewUuid(collectionName, UUID.randomUUID());
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);
    // Registers the database mapping and seeds the in-memory copy.
    this.leaseManager.findGcCandidates(Instant.now());
    this.leaseManager.putLease(collectionName, lease);

    UpdateResult updateResult = mock(UpdateResult.class);
    when(updateResult.getModifiedCount()).thenReturn(1L);
    when(this.mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

    boolean marked = this.leaseManager.markEligibleForCleanup(collectionName, Instant.now());

    assertThat(marked).isTrue();
    assertThat(this.leaseManager.getLeases().get(collectionName).cleanupState())
        .isEqualTo(Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
  }

  @Test
  public void markEligibleForCleanup_occFilterMiss_returnsFalseAndLeavesInMemoryLease() {
    String collectionName = "fresh-collection";
    Lease lease = createLeaseWithMatViewUuid(collectionName, UUID.randomUUID());
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);
    this.leaseManager.findGcCandidates(Instant.now());
    this.leaseManager.putLease(collectionName, lease);

    UpdateResult updateResult = mock(UpdateResult.class);
    when(updateResult.getModifiedCount()).thenReturn(0L);
    when(this.mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

    boolean marked = this.leaseManager.markEligibleForCleanup(collectionName, Instant.now());

    assertThat(marked).isFalse();
    assertThat(this.leaseManager.getLeases().get(collectionName).cleanupState())
        .isEqualTo(Lease.CleanupState.NOT_ELIGIBLE);
  }

  @Test
  public void markEligibleForCleanup_unknownLease_returnsFalseWithoutDbWrite() {
    boolean marked = this.leaseManager.markEligibleForCleanup("never-seen-lease", Instant.now());

    assertThat(marked).isFalse();
    verify(this.mockCollection, never()).updateOne(any(Bson.class), any(Bson.class));
  }

  @Test
  public void markEligibleForCleanup_updateFailure_returnsFalse() {
    String collectionName = "error-collection";
    Lease lease = createLeaseWithMatViewUuid(collectionName, UUID.randomUUID());
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);
    this.leaseManager.findGcCandidates(Instant.now());
    when(this.mockCollection.updateOne(any(Bson.class), any(Bson.class)))
        .thenThrow(new RuntimeException("update failed"));

    assertThat(this.leaseManager.markEligibleForCleanup(collectionName, Instant.now())).isFalse();
  }

  // ==================== resolveStaleMatViewNamespace / deleteStaleLease ====================

  /** A lease already marked {@code ELIGIBLE_FOR_CLEANUP} with the given heartbeat expiration. */
  private static Lease markedLease(String collectionName, Instant leaseExpiration) {
    return new Lease(
        collectionName,
        Lease.SCHEMA_VERSION,
        UUID.randomUUID().toString(),
        collectionName,
        "some-other-mongot",
        leaseExpiration,
        1L,
        "",
        "0",
        Map.of(),
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
            UUID.randomUUID(),
            collectionName),
        null,
        Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
  }

  private static Lease unmarkedLease(String collectionName, Instant leaseExpiration) {
    return new Lease(
        collectionName,
        Lease.SCHEMA_VERSION,
        UUID.randomUUID().toString(),
        collectionName,
        "some-other-mongot",
        leaseExpiration,
        1L,
        "",
        "0",
        Map.of(),
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
            UUID.randomUUID(),
            collectionName),
        null,
        Lease.CleanupState.NOT_ELIGIBLE);
  }

  /** Seeds the lease→database mapping (as the GC scan would) for the given lease. */
  private void registerViaGcScan(Lease lease) {
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);
    this.leaseManager.findGcCandidates(Instant.now());
  }

  @Test
  public void resolveStaleMatViewNamespace_stillEligible_returnsNamespace() {
    String collectionName = "cleanup-collection";
    Lease lease = markedLease(collectionName, Instant.now().minusSeconds(7200));
    registerViaGcScan(lease);
    // The re-read returns the still-eligible lease.
    when(this.mockFindIterable.first()).thenReturn(lease.toBson());

    Optional<MongoNamespace> namespace =
        this.leaseManager.resolveStaleMatViewNamespace(collectionName, Instant.now());

    assertThat(namespace).isPresent();
    assertThat(namespace.get().getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(namespace.get().getCollectionName()).isEqualTo(collectionName);
  }

  @Test
  public void resolveStaleMatViewNamespace_invalidMvCollectionName_returnsEmpty() {
    String leaseKey = "bad-ns-lease";
    // Eligible and stale, but its MV metadata carries an empty collection name, so the
    // MongoNamespace construction throws. The method must swallow it and return empty rather than
    // propagate — the best-effort / never-throw contract.
    Lease lease =
        new Lease(
            leaseKey,
            Lease.SCHEMA_VERSION,
            UUID.randomUUID().toString(),
            leaseKey,
            "some-other-mongot",
            Instant.now().minusSeconds(7200),
            1L,
            "",
            "0",
            Map.of(),
            new MaterializedViewCollectionMetadata(
                new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
                UUID.randomUUID(),
                ""),
            null,
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    registerViaGcScan(lease);
    when(this.mockFindIterable.first()).thenReturn(lease.toBson());

    assertThat(this.leaseManager.resolveStaleMatViewNamespace(leaseKey, Instant.now())).isEmpty();
  }

  @Test
  public void resolveStaleMatViewNamespace_resurrectedLease_returnsEmpty() {
    Instant now = Instant.now();
    String collectionName = "resurrected-collection";
    registerViaGcScan(markedLease(collectionName, now.minusSeconds(7200)));
    // By resolve time the lease was renewed past the cutoff: the re-read must veto it.
    Lease renewed = markedLease(collectionName, now.plusSeconds(60));
    when(this.mockFindIterable.first()).thenReturn(renewed.toBson());

    assertThat(this.leaseManager.resolveStaleMatViewNamespace(collectionName, now)).isEmpty();
  }

  @Test
  public void resolveStaleMatViewNamespace_unmarkedLease_returnsEmpty() {
    String collectionName = "unmarked-collection";
    registerViaGcScan(markedLease(collectionName, Instant.now().minusSeconds(7200)));
    // By resolve time the cleanup mark was reverted (e.g. the lease was reused, CLOUDP-412595):
    // the re-read must veto it even though it is still expired.
    Lease unmarked = unmarkedLease(collectionName, Instant.now().minusSeconds(7200));
    when(this.mockFindIterable.first()).thenReturn(unmarked.toBson());

    assertThat(this.leaseManager.resolveStaleMatViewNamespace(collectionName, Instant.now()))
        .isEmpty();
  }

  @Test
  public void resolveStaleMatViewNamespace_leaseAlreadyDeleted_returnsEmpty() {
    String collectionName = "already-gone-collection";
    registerViaGcScan(markedLease(collectionName, Instant.now().minusSeconds(7200)));
    when(this.mockFindIterable.first()).thenReturn(null);

    assertThat(this.leaseManager.resolveStaleMatViewNamespace(collectionName, Instant.now()))
        .isEmpty();
  }

  @Test
  public void resolveStaleMatViewNamespace_unknownLease_returnsEmpty() {
    assertThat(this.leaseManager.resolveStaleMatViewNamespace("never-seen", Instant.now()))
        .isEmpty();
  }

  @Test
  public void deleteStaleLease_deleted_returnsTrueAndForgetsLease() {
    String collectionName = "deleted-collection";
    Lease lease = markedLease(collectionName, Instant.now().minusSeconds(7200));
    registerViaGcScan(lease);
    this.leaseManager.putLease(collectionName, lease);
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(deleteResult.getDeletedCount()).thenReturn(1L);
    when(this.mockCollection.deleteOne(any(Bson.class))).thenReturn(deleteResult);

    assertThat(this.leaseManager.deleteStaleLease(collectionName, Instant.now())).isTrue();
    assertThat(this.leaseManager.getLeases()).doesNotContainKey(collectionName);
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).doesNotContainKey(collectionName);
  }

  @Test
  public void deleteStaleLease_occFilterMiss_returnsFalseAndKeepsMapping() {
    String collectionName = "kept-collection";
    registerViaGcScan(markedLease(collectionName, Instant.now().minusSeconds(7200)));
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(deleteResult.getDeletedCount()).thenReturn(0L);
    when(this.mockCollection.deleteOne(any(Bson.class))).thenReturn(deleteResult);

    assertThat(this.leaseManager.deleteStaleLease(collectionName, Instant.now())).isFalse();
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).containsKey(collectionName);
  }

  @Test
  public void deleteStaleLease_filtersOnMarkedStateAndExpiration() {
    String collectionName = "filtered-collection";
    registerViaGcScan(markedLease(collectionName, Instant.now().minusSeconds(7200)));
    Instant cutoff = Instant.parse("2026-01-01T00:00:00Z");
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(deleteResult.getDeletedCount()).thenReturn(0L);
    when(this.mockCollection.deleteOne(any(Bson.class))).thenReturn(deleteResult);

    this.leaseManager.deleteStaleLease(collectionName, cutoff);

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
    assertThat(
            clauses.contains(
                new BsonDocument(
                    "leaseExpiration",
                    new BsonDocument("$lt", new BsonDateTime(cutoff.toEpochMilli())))))
        .isTrue();
  }

  @Test
  public void deleteStaleLease_unknownLease_returnsFalseWithoutDbWrite() {
    assertThat(this.leaseManager.deleteStaleLease("never-seen-lease", Instant.now())).isFalse();
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  @Test
  public void deleteStaleLease_deleteFailure_returnsFalse() {
    String collectionName = "error-delete-collection";
    registerViaGcScan(markedLease(collectionName, Instant.now().minusSeconds(7200)));
    when(this.mockCollection.deleteOne(any(Bson.class)))
        .thenThrow(new RuntimeException("delete failed"));

    assertThat(this.leaseManager.deleteStaleLease(collectionName, Instant.now())).isFalse();
  }

  @Test
  public void resolveStaleMatViewNamespace_reReadThrows_returnsEmpty() {
    String collectionName = "reread-error-collection";
    registerViaGcScan(markedLease(collectionName, Instant.now().minusSeconds(7200)));
    when(this.mockFindIterable.first()).thenThrow(new RuntimeException("read failed"));

    assertThat(this.leaseManager.resolveStaleMatViewNamespace(collectionName, Instant.now()))
        .isEmpty();
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

  private Lease createLeaseWithStatus(
      MaterializedViewGenerationId generationId,
      String owner,
      Instant expiration,
      IndexStatus.StatusCode statusCode) {
    boolean isQueryable = statusCode == IndexStatus.StatusCode.STEADY;
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
        Map.of("0", new Lease.IndexDefinitionVersionStatus(isQueryable, statusCode)),
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
