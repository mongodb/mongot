package com.xgen.mongot.embedding.mongodb.leasing;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static com.xgen.mongot.embedding.mongodb.leasing.DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME;
import static com.xgen.mongot.embedding.mongodb.leasing.DynamicLeaderLeaseManager.POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME;
import static com.xgen.mongot.embedding.mongodb.leasing.DynamicLeaderLeaseManager.POST_RESTORE_REPAIR_SUPPRESSED_COUNTER_NAME;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static org.mockito.ArgumentMatchers.any;
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
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.testing.mongot.metrics.SimpleMetricsFactory;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.search.Search;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
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
  private static final String TENANT_ID = "647343c92726405e854130e4"; // 24 hex chars
  private static final String SOURCE_COLLECTION = "embedded_movies";

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
                    Instant.now().minusSeconds(60))),
            Optional.empty());

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
                    HOSTNAME, List.of(), Instant.now().plusSeconds(60))),
            Optional.empty());

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
                    Instant.now().plusSeconds(60))),
            Optional.empty());

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
                    HOSTNAME, List.of(leaseKey), Instant.now().plusSeconds(60))),
            Optional.empty());

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
                    HOSTNAME, List.of(leaseKey), Instant.now().plusSeconds(60))),
            Optional.empty());

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
                    HOSTNAME, List.of(leaseKey), Instant.now().plusSeconds(60))),
            Optional.empty());

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
  public void add_missingCachedLease_clearsStaleLeaderState() {
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    this.leaseManager.putLease(
        leaseKey, createLease(generationId, HOSTNAME, Instant.now().plusSeconds(3600)));
    this.leaseManager.add(indexGeneration);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    this.leaseManager.evictCachedLease(leaseKey);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    this.leaseManager.add(indexGeneration);

    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);
  }

  @Test
  public void add_expiredOwnedLeaseWithNewRemoteOwner_demotesExistingLeader() {
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    String leaseKey = getLeaseKeyFromCatalog(generationId);

    Lease ownedLease = createLease(generationId, HOSTNAME, Instant.now().plusSeconds(60));
    this.leaseManager.putLease(leaseKey, ownedLease);
    this.leaseManager.add(indexGeneration);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    Lease expiredCachedLease = createLease(generationId, HOSTNAME, Instant.now().minusSeconds(60));
    this.leaseManager.putLease(leaseKey, expiredCachedLease);
    Lease newRemoteLease = expiredCachedLease.withNewTenure(OTHER_HOSTNAME);
    setupFindLeaseFromDatabase(newRemoteLease);

    this.leaseManager.add(indexGeneration);

    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);

    LeaseManager.FollowerPollResult result = this.leaseManager.pollFollowerStatuses();
    assertThat(result.acquirableLeases()).doesNotContain(generationId);
    assertThat(this.leaseManager.getLeases().get(leaseKey).leaseOwner()).isEqualTo(OTHER_HOSTNAME);
    assertThat(this.leaseManager.getLeases().get(leaseKey).leaseVersion())
        .isEqualTo(newRemoteLease.leaseVersion());
  }

  @Test
  public void add_reownedLeaseAfterLocalDemotion_removesFromFollowerGenerationIds() {
    // Regression test for CLOUDP-437181: retryFailedGeneratorCreations() calls add() again for a
    // generationId that becomeFollower() (via releaseLeadership()) demoted moments earlier in the
    // same refreshStatus() tick, e.g. after a terminal-state generator (collection drop) is
    // recreated while this host still owns an unexpired lease. add()'s "we own the lease" branch
    // must clear any lingering followerGenerationIds membership left by that becomeFollower() call,
    // or the next pollFollowerStatuses() reports the already-active lease as acquirable and
    // refreshStatus() redundantly reacquires leadership, tripping the unsafe-reacquisition alert.
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    this.leaseManager.putLease(
        leaseKey, createLease(generationId, HOSTNAME, Instant.now().plusSeconds(3600)));

    this.leaseManager.add(indexGeneration);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    this.leaseManager.releaseLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);

    this.leaseManager.add(indexGeneration);

    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(generationId);
    LeaseManager.FollowerPollResult result = this.leaseManager.pollFollowerStatuses();
    assertThat(result.acquirableLeases()).doesNotContain(generationId);
  }

  @Test
  public void add_missingEpochUnexpiredOwnedLease_resumesLeadershipWithHealedEpoch()
      throws Exception {
    // A rolled-back (pre-epoch) binary can drop the epoch field from a lease this host still owns
    // while continuing to renew it under old code, advancing leaseVersion. On roll-forward, add()'s
    // "we own an unexpired lease" fast path resumes leadership straight from whatever is already in
    // memory -- it never calls withNewTenure(). The epoch healed by Lease#fromBson (defaulted to
    // leaseVersion when absent) must already be safe by the time that fast path runs, and must
    // survive the next ordinary write (heartbeat) unchanged.
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    String leaseKey = getLeaseKeyFromCatalog(generationId);

    Lease ownedLease = createLease(generationId, HOSTNAME, Instant.now().plusSeconds(3600));
    BsonDocument writtenByOlderBinary = ownedLease.toBson();
    writtenByOlderBinary.remove("epoch");
    writtenByOlderBinary.put("leaseVersion", new BsonInt64(45L));
    Lease healedLease = Lease.fromBson(writtenByOlderBinary);
    assertThat(healedLease.epoch()).isEqualTo(45L);
    this.leaseManager.putLease(leaseKey, healedLease);

    // Act: add() takes the fast path (weOwnLease && !leaseExpired) -- no DB write, no acquisition.
    this.leaseManager.add(indexGeneration);

    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    verify(this.mockCollection, never()).replaceOne(any(), any(BsonDocument.class));
    assertThat(this.leaseManager.getLeases().get(leaseKey).epoch()).isEqualTo(45L);

    // The next ordinary write (a heartbeat renewal) must preserve the healed epoch exactly --
    // withRenewedOwnership() (the flag-off renewal path), not withNewTenure(), is on this path.
    putLeaseInRenewalWindow(generationId);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.heartbeat();

    assertThat(this.leaseManager.getLeases().get(leaseKey).epoch()).isEqualTo(45L);
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
    // Acquiring leadership from another host's expired lease is a new tenure: epoch bumps (rebased
    // above leaseVersion, per Lease#withNewTenure).
    assertThat(this.leaseManager.getLeases().get(getLeaseKeyFromCatalog(generationId)).epoch())
        .isEqualTo(Math.max(expiredLease.epoch() + 1, expiredLease.leaseVersion() + 1));
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
    // Same-host reacquisition is still a new tenure: epoch bumps (rebased above leaseVersion, per
    // Lease#withNewTenure) even though the owner is unchanged.
    assertThat(this.leaseManager.getLeases().get(getLeaseKeyFromCatalog(generationId)).epoch())
        .isEqualTo(Math.max(ourLease.epoch() + 1, ourLease.leaseVersion() + 1));
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
  public void tryAcquireLeadership_takeoverFilter_requiresStillExpired() {
    // Arrange - take over another host's expired lease.
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();

    // Act
    assertThat(this.leaseManager.tryAcquireLeadership(generationId)).isTrue();

    // Assert - a takeover must only win if the lease is still expired at write time (CLOUDP-449935:
    // the heartbeat no longer bumps leaseVersion, so version alone is no longer a fence — an
    // acquirer reading V could otherwise steal a lease the owner just renewed).
    ArgumentCaptor<Bson> filterCaptor = ArgumentCaptor.forClass(Bson.class);
    verify(this.mockCollection).replaceOne(filterCaptor.capture(), any(BsonDocument.class));
    BsonDocument filter = filterCaptor.getValue().toBsonDocument();
    assertThat(filter.toJson()).contains("leaseExpiration");
    assertThat(filter.toJson()).contains("$lt");
  }

  @Test
  public void tryAcquireLeadership_reclaimOwnLeaseFilter_doesNotRequireExpired() {
    // Arrange - reclaim our own still-valid lease (restart-like).
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    Lease ourLease = createLease(generationId, HOSTNAME, Instant.now().plusSeconds(30));
    setupFindLeaseFromDatabase(ourLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();

    // Act
    assertThat(this.leaseManager.tryAcquireLeadership(generationId)).isTrue();

    // Assert - reclaiming an own, still-valid lease is not gated on expiry. Only takeovers of
    // another host's lease require the lease to be expired.
    ArgumentCaptor<Bson> filterCaptor = ArgumentCaptor.forClass(Bson.class);
    verify(this.mockCollection).replaceOne(filterCaptor.capture(), any(BsonDocument.class));
    BsonDocument filter = filterCaptor.getValue().toBsonDocument();
    assertThat(filter.toJson()).doesNotContain("leaseExpiration");
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
  public void postRestoreSanitize_atLeaseLoad_repairsMigrationArtifact() {
    UUID newSourceUuid = UUID.randomUUID();
    UUID newMvUuid = UUID.randomUUID();
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    String liveDb = gen.getDefinition().getDatabase();
    Lease staleLease =
        createLeaseWithCommitInfo(
            gen.getGenerationId(), HOSTNAME, staleCommitInfo(TENANT_ID, liveDb, SOURCE_COLLECTION));
    setupFindLeaseFromDatabase(staleLease);
    // Source UUID resolves against the base collection in the commitInfo namespace, not the lease's
    // collectionName; the MV UUID resolves against the MV name under dbResolver.resolveDefault().
    setupListCollections(liveDb, SOURCE_COLLECTION, newSourceUuid);
    setupListCollections(
        DATABASE_NAME, staleLease.materializedViewCollectionMetadata().collectionName(), newMvUuid);
    setupSuccessfulLeaseUpdate();

    this.leaseManager.add(gen);
    this.leaseManager.pollFollowerStatuses();

    Lease inMem = this.leaseManager.getLeases().get(staleLease.id());
    assertThat(inMem.commitInfo()).isEqualTo(EncodedUserData.EMPTY.asString());
    assertThat(inMem.collectionUuid()).isEqualTo(newSourceUuid.toString());
    assertThat(inMem.collectionName()).isEqualTo(SOURCE_COLLECTION);
    assertThat(inMem.materializedViewCollectionMetadata().collectionUuid()).isEqualTo(newMvUuid);
    assertThat(inMem.steadyAsOfOplogPosition()).isNull();
    assertThat(repairCounter(POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME)).isAtLeast(1.0);
    assertThat(repairCounter(POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME)).isEqualTo(0.0);
    assertThat(repairCounter(POST_RESTORE_REPAIR_SUPPRESSED_COUNTER_NAME)).isEqualTo(0.0);
  }

  @Test
  public void postRestoreSanitize_atLeaseLoad_customerNamedDatabase_suppressesRepair() {
    // "sample_mflix" is not the live definition database, so the tenant prefix is part of a
    // customer-chosen name. No listCollections mocks: any repair attempt would fail.
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    String commitInfo = staleCommitInfo(TENANT_ID, "sample_mflix", SOURCE_COLLECTION);

    Lease inMem = loadLeaseThroughPoll(gen, commitInfo);

    assertThat(inMem.commitInfo()).isEqualTo(commitInfo);
    assertThat(repairCounter(POST_RESTORE_REPAIR_SUPPRESSED_COUNTER_NAME)).isAtLeast(1.0);
    assertThat(repairCounter(POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME)).isEqualTo(0.0);
    assertThat(repairCounter(POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME)).isEqualTo(0.0);
  }

  @Test
  public void postRestoreSanitize_atLeaseLoad_repairIndeterminate_dropsLease() {
    // Real migration shape, but no listCollections mocks so the lookup fails. A repaired lease is
    // indistinguishable from a stale one here, so it must not be adopted.
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease staleLease =
        createLeaseWithCommitInfo(
            gen.getGenerationId(),
            HOSTNAME,
            staleCommitInfo(TENANT_ID, gen.getDefinition().getDatabase(), SOURCE_COLLECTION));
    setupFindLeaseFromDatabase(staleLease);

    this.leaseManager.add(gen);
    this.leaseManager.pollFollowerStatuses();

    assertThat(this.leaseManager.getLeases()).doesNotContainEntry(staleLease.id(), staleLease);
    assertThat(repairCounter(POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME)).isAtLeast(1.0);
    assertThat(repairCounter(POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME)).isEqualTo(0.0);
  }

  @Test
  public void postRestoreSanitize_atLeaseLoad_unprefixedNamespace_leavesEverythingAlone() {
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    String commitInfo =
        IndexCommitUserData.createChangeStreamResume(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace("sample_mflix", SOURCE_COLLECTION),
                    ChangeStreamUtils.resumeToken(new BsonTimestamp(1L))),
                IndexFormatVersion.CURRENT.versionNumber)
            .toEncodedData()
            .asString();
    setupSuccessfulLeaseUpdate();
    ArgumentCaptor<BsonDocument> writtenLease = ArgumentCaptor.forClass(BsonDocument.class);

    assertThat(loadLeaseThroughPoll(gen, commitInfo).commitInfo()).isEqualTo(commitInfo);

    assertThat(this.leaseManager.tryAcquireLeadership(gen.getGenerationId())).isTrue();
    verify(this.mockCollection).replaceOne(any(), writtenLease.capture());
    assertThat(writtenLease.getValue().getString("commitInfo").getValue()).isEqualTo(commitInfo);
    assertThat(repairCounter(POST_RESTORE_REPAIR_SUPPRESSED_COUNTER_NAME)).isEqualTo(0.0);
    assertThat(repairCounter(POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME)).isEqualTo(0.0);
    assertThat(repairCounter(POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME)).isEqualTo(0.0);
  }

  @Test
  public void syncLeasesFromMongod_liveDatabaseUnknown_skipsRepair() {
    // No add() first, so the live source database is unknown. Without it the tenant-prefix shape
    // proves nothing, so the lease is adopted untouched rather than repaired on shape alone.
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    String commitInfo = staleCommitInfo(TENANT_ID, "sample_mflix", SOURCE_COLLECTION);
    Lease lease = createLeaseWithCommitInfo(gen.getGenerationId(), HOSTNAME, commitInfo);
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>(List.of(lease.toBson())));

    this.leaseManager.syncLeasesFromMongod();

    assertThat(this.leaseManager.getLeases().get(lease.id()).commitInfo()).isEqualTo(commitInfo);
    assertThat(repairCounter(POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME)).isEqualTo(0.0);
    assertThat(repairCounter(POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME)).isEqualTo(0.0);
    assertThat(repairCounter(POST_RESTORE_REPAIR_SUPPRESSED_COUNTER_NAME)).isEqualTo(0.0);
  }

  /** Registers {@code gen}, loads its lease via the follower poll, returns the cached copy. */
  private Lease loadLeaseThroughPoll(MaterializedViewIndexGeneration gen, String commitInfo) {
    Lease lease = createLeaseWithCommitInfo(gen.getGenerationId(), HOSTNAME, commitInfo);
    setupFindLeaseFromDatabase(lease);
    this.leaseManager.add(gen);
    this.leaseManager.pollFollowerStatuses();
    return this.leaseManager.getLeases().get(lease.id());
  }

  // ==================== Corrupt Lease Repair ====================

  @Test
  public void syncLeasesFromMongod_unparseableLease_recordsUnparseableReason() {
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease lease = createLease(gen.getGenerationId(), HOSTNAME, Instant.now().plusSeconds(60));
    when(this.mockFindIterable.into(any()))
        .thenReturn(new ArrayList<>(List.of(unparseable(lease))));

    this.leaseManager.syncLeasesFromMongod();

    assertThat(this.leaseManager.getCorruptLeases())
        .containsEntry(
            lease.id(),
            new DynamicLeaderLeaseManager.CorruptLease(
                DATABASE_NAME, LeaseRepairUtil.CorruptionReason.UNPARSEABLE));
    assertThat(this.leaseManager.getLeases()).doesNotContainKey(lease.id());
  }

  @Test
  public void syncLeasesFromMongod_unresolvableMvUuid_recordsUnresolvableReason() {
    when(this.mockFindIterable.into(any()))
        .thenReturn(new ArrayList<>(List.of(nilMvUuidLease(NIL_MV_LEASE_ID))));
    setupEmptyListCollections();

    this.leaseManager.syncLeasesFromMongod();

    // The document parses cleanly. Only the collection lookup fails, so the reason must say so.
    assertThat(this.leaseManager.getCorruptLeases())
        .containsEntry(
            NIL_MV_LEASE_ID,
            new DynamicLeaderLeaseManager.CorruptLease(
                DATABASE_NAME, LeaseRepairUtil.CorruptionReason.UNRESOLVABLE_MV_UUID));
    assertThat(this.leaseManager.getLeases()).doesNotContainKey(NIL_MV_LEASE_ID);
  }

  @Test
  public void findGcCandidates_leaseVanishes_prunesRecord() {
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease lease = createLease(gen.getGenerationId(), HOSTNAME, Instant.now().plusSeconds(60));
    when(this.mockFindIterable.into(any()))
        .thenReturn(new ArrayList<>(List.of(unparseable(lease))));
    this.leaseManager.findGcCandidates();
    assertThat(this.leaseManager.getCorruptLeases()).containsKey(lease.id());

    // The document was removed out of band, so the sweep no longer returns it. The GC sweep is the
    // only path with a complete view of every database, so pruning belongs there.
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>());
    this.leaseManager.findGcCandidates();

    assertThat(this.leaseManager.getCorruptLeases()).isEmpty();
  }

  @Test
  public void findGcCandidates_unparseableLease_recordsItFromALivePath() {
    // parseLeaseOrThrow is the funnel every live read passes through, so recording there is what
    // makes rejections visible in production. syncLeasesFromMongod has no production caller.
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease lease = createLease(gen.getGenerationId(), HOSTNAME, Instant.now().plusSeconds(60));
    when(this.mockFindIterable.into(any()))
        .thenReturn(new ArrayList<>(List.of(unparseable(lease))));

    this.leaseManager.findGcCandidates();

    assertThat(this.leaseManager.getCorruptLeases())
        .containsEntry(
            lease.id(),
            new DynamicLeaderLeaseManager.CorruptLease(
                DATABASE_NAME, LeaseRepairUtil.CorruptionReason.UNPARSEABLE));
  }

  @Test
  public void findGcCandidates_leaseParsesButStillRejected_keepsRecordAndStaysACandidate() {
    // A lease with a nil MV UUID whose collection is absent parses fine but the load path still
    // rejects it. The GC sweep used to clear the rejection on parse success alone, which silently
    // un-recorded leases an operator still needs to repair.
    BsonDocument nilMvLease = nilMvUuidLease(NIL_MV_LEASE_ID);
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>(List.of(nilMvLease)));
    setupEmptyListCollections();
    this.leaseManager.syncLeasesFromMongod();
    assertThat(this.leaseManager.getCorruptLeases()).containsKey(NIL_MV_LEASE_ID);

    var result = this.leaseManager.findGcCandidates();

    assertThat(this.leaseManager.getCorruptLeases()).containsKey(NIL_MV_LEASE_ID);
    // Still a GC candidate, so gating the record retirement does not change the candidate list.
    assertThat(result.leases().stream().map(Lease::id)).contains(NIL_MV_LEASE_ID);
  }

  @Test
  public void findGcCandidates_leaseWithoutRecord_skipsLoadPathCheck() {
    // The load-path re-check exists only to retire an existing rejection record. Running it for
    // every lease on every sweep would issue listCollections per nil-MV lease per sweep and inflate
    // the post-restore counters, so an unrecorded lease must be parsed and nothing more.
    BsonDocument nilMvLease = nilMvUuidLease(NIL_MV_LEASE_ID);
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>(List.of(nilMvLease)));

    var result = this.leaseManager.findGcCandidates();

    verify(this.mockDatabase, never()).listCollections(BsonDocument.class);
    assertThat(result.leases().stream().map(Lease::id)).contains(NIL_MV_LEASE_ID);
  }

  @Test
  public void findGcCandidates_leaseLoadsCleanly_retiresRecord() {
    // The counterpart: a lease the load path accepts must have its stale rejection retired, which
    // is what stops the repair command deleting a lease that has recovered.
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease lease = createLease(gen.getGenerationId(), HOSTNAME, Instant.now().plusSeconds(60));
    when(this.mockFindIterable.into(any()))
        .thenReturn(new ArrayList<>(List.of(unparseable(lease))));
    this.leaseManager.findGcCandidates();
    assertThat(this.leaseManager.getCorruptLeases()).containsKey(lease.id());
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>(List.of(lease.toBson())));

    this.leaseManager.findGcCandidates();

    assertThat(this.leaseManager.getCorruptLeases()).doesNotContainKey(lease.id());
  }

  @Test
  public void syncLeasesFromMongod_mixedBatch_recordsOnlyTheCorruptLease() {
    MaterializedViewIndexGeneration corruptGen = createTestIndexGeneration();
    Lease corruptLease =
        createLease(corruptGen.getGenerationId(), HOSTNAME, Instant.now().plusSeconds(60));
    MaterializedViewIndexGeneration healthyGen = createTestIndexGeneration();
    Lease healthyLease =
        createLease(healthyGen.getGenerationId(), HOSTNAME, Instant.now().plusSeconds(60));
    when(this.mockFindIterable.into(any()))
        .thenReturn(new ArrayList<>(List.of(unparseable(corruptLease), healthyLease.toBson())));

    this.leaseManager.syncLeasesFromMongod();

    assertThat(this.leaseManager.getCorruptLeases().keySet()).containsExactly(corruptLease.id());
    assertThat(this.leaseManager.getLeases()).containsKey(healthyLease.id());
  }

  @Test
  public void syncLeasesFromMongod_leaseRecovers_clearsRecord() {
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease lease = createLease(gen.getGenerationId(), HOSTNAME, Instant.now().plusSeconds(60));
    when(this.mockFindIterable.into(any()))
        .thenReturn(new ArrayList<>(List.of(unparseable(lease))));
    this.leaseManager.syncLeasesFromMongod();
    assertThat(this.leaseManager.getCorruptLeases()).containsKey(lease.id());

    // The document is rewritten cleanly, so the next sweep must retire the record.
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>(List.of(lease.toBson())));
    this.leaseManager.syncLeasesFromMongod();

    assertThat(this.leaseManager.getCorruptLeases()).isEmpty();
    assertThat(this.leaseManager.getLeases()).containsKey(lease.id());
  }

  @Test
  public void deleteCorruptLease_unparseable_deletesAndClearsRecord() {
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    // Expired: the runbook has the operator wait for expiry, and deleteCorruptLease enforces it.
    Lease lease = createLease(gen.getGenerationId(), HOSTNAME, Instant.now().minusSeconds(60));
    BsonDocument corrupt = unparseable(lease);
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>(List.of(corrupt)));
    this.leaseManager.syncLeasesFromMongod();
    when(this.mockFindIterable.first()).thenReturn(corrupt);
    setupSuccessfulDelete();

    boolean deleted = this.leaseManager.deleteCorruptLease(lease.id());

    assertThat(deleted).isTrue();
    verify(this.mockCollection).deleteOne(any(Bson.class));
    assertThat(this.leaseManager.getCorruptLeases()).isEmpty();
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_DELETED))
        .isEqualTo(1.0);
  }

  @Test
  public void deleteCorruptLease_unresolvableMvUuid_deletes() {
    // The whole point of the tool: this document parses cleanly and is still unusable, so a
    // parse-only guard would refuse to delete it and leave the materialized view blocked forever.
    BsonDocument nilMvLease = nilMvUuidLease(NIL_MV_LEASE_ID);
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>(List.of(nilMvLease)));
    setupEmptyListCollections();
    this.leaseManager.syncLeasesFromMongod();
    when(this.mockFindIterable.first()).thenReturn(nilMvLease);
    setupSuccessfulDelete();

    boolean deleted = this.leaseManager.deleteCorruptLease(NIL_MV_LEASE_ID);

    assertThat(deleted).isTrue();
    verify(this.mockCollection).deleteOne(any(Bson.class));
    assertThat(this.leaseManager.getCorruptLeases()).isEmpty();
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_DELETED))
        .isEqualTo(1.0);
  }

  @Test
  public void deleteCorruptLease_leaseLoadsCleanly_doesNotDelete() {
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease lease = createLease(gen.getGenerationId(), HOSTNAME, Instant.now().plusSeconds(60));
    when(this.mockFindIterable.into(any()))
        .thenReturn(new ArrayList<>(List.of(unparseable(lease))));
    this.leaseManager.syncLeasesFromMongod();
    // Another mongot rewrote the document cleanly between the sweep and the operator command.
    // Deleting now would drop a live fencing token.
    when(this.mockFindIterable.first()).thenReturn(lease.toBson());

    boolean deleted = this.leaseManager.deleteCorruptLease(lease.id());

    assertThat(deleted).isFalse();
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
    assertThat(this.leaseManager.getCorruptLeases()).isEmpty();
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_REFUSED_STILL_USABLE))
        .isEqualTo(1.0);
    // A refusal is the guard working. It must not land on the counter an operator would alert on.
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_ERROR)).isEqualTo(0.0);
  }

  @Test
  public void deleteCorruptLease_documentAlreadyGone_doesNotDelete() {
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease lease = createLease(gen.getGenerationId(), HOSTNAME, Instant.now().plusSeconds(60));
    when(this.mockFindIterable.into(any()))
        .thenReturn(new ArrayList<>(List.of(unparseable(lease))));
    this.leaseManager.syncLeasesFromMongod();
    when(this.mockFindIterable.first()).thenReturn(null);

    boolean deleted = this.leaseManager.deleteCorruptLease(lease.id());

    assertThat(deleted).isFalse();
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  @Test
  public void deleteCorruptLease_noDocumentAnywhere_doesNotDelete() {
    // No prior record and no document in any known database. The delete must not depend on a
    // background sweep having seen the lease, so this resolves to "already gone" rather than a
    // refusal.
    when(this.mockFindIterable.first()).thenReturn(null);

    boolean deleted = this.leaseManager.deleteCorruptLease("never-seen-lease");

    assertThat(deleted).isFalse();
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_ALREADY_GONE))
        .isEqualTo(1.0);
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_ERROR)).isEqualTo(0.0);
  }

  @Test
  public void deleteCorruptLease_multiTenantWithNoMappings_doesNotReportAlreadyGone() {
    // The one-shot command fires on the first poll after a restart, which can precede conf-call
    // index registration. On a multi-tenant cluster that leaves leaseKeyToDatabase empty, so only
    // the default internal database gets searched and the tenant database holding the corrupt
    // lease is unreachable. Reporting alreadyGone would burn the one-shot on a false verdict.
    InternalDatabaseResolver multiTenantResolver =
        new InternalDatabaseResolver() {
          @Override
          public String resolve(String sourceDatabaseName) {
            return DATABASE_NAME;
          }

          @Override
          public String resolveDefault() {
            return DATABASE_NAME;
          }
        };
    DynamicLeaderLeaseManager manager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            this.metricsFactory,
            HOSTNAME,
            multiTenantResolver,
            this.mvMetadataCatalog);
    when(this.mockFindIterable.first()).thenReturn(null);

    boolean deleted = manager.deleteCorruptLease("lease-in-unmapped-tenant-db");

    assertThat(deleted).isFalse();
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_INDETERMINATE))
        .isEqualTo(1.0);
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_ALREADY_GONE))
        .isEqualTo(0.0);
  }

  @Test
  public void deleteCorruptLease_everyDatabaseLookupFails_doesNotReportAlreadyGone() {
    // No prior record, so the search actually runs, and every candidate lookup throws. "Not found"
    // then proves nothing: reporting alreadyGone here would tell an operator the lease does not
    // exist on the strength of an outage, and would clear any recorded rejection with it.
    when(this.mockCollection.find(any(Bson.class)))
        .thenThrow(new RuntimeException("mongo unreachable"));

    boolean deleted = this.leaseManager.deleteCorruptLease("never-seen-lease");

    assertThat(deleted).isFalse();
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_INDETERMINATE))
        .isEqualTo(1.0);
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_ALREADY_GONE))
        .isEqualTo(0.0);
  }

  @Test
  public void deleteCorruptLease_onSuccess_dropsCachedLeaseButKeepsDatabaseMapping() {
    // The generation stays registered after a repair, so the mappings must survive. Without them
    // getDatabaseForLease throws and the follower poll skips the key on every pass, leaving the
    // materialized view with no leader instead of unblocking it.
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease lease = createLease(gen.getGenerationId(), HOSTNAME, Instant.now().minusSeconds(60));
    this.leaseManager.add(gen);
    when(this.mockFindIterable.first()).thenReturn(unparseable(lease));
    setupSuccessfulDelete();

    boolean deleted = this.leaseManager.deleteCorruptLease(lease.id());

    assertThat(deleted).isTrue();
    // The cached lease must not outlive the document.
    assertThat(this.leaseManager.getLeases()).doesNotContainKey(lease.id());
    // The mapping must, so the next poll can still resolve the database for this key.
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).containsKey(lease.id());
  }

  @Test
  public void deleteCorruptLease_locateThrowsAssertionError_doesNotEscape() {
    // deleteCorruptLease runs at the top of pollFollowerStatuses, which is scheduled with
    // scheduleWithFixedDelay: a throw there cancels every future run, so the host stops refreshing
    // statuses for all generations until restart. locateLease used to run outside the try, and
    // Check.instanceOf on this path throws AssertionError, which a plain catch(Exception) misses.
    when(this.mockCollection.find(any(Bson.class))).thenThrow(new AssertionError("check failed"));

    boolean deleted = this.leaseManager.deleteCorruptLease("never-seen-lease");

    assertThat(deleted).isFalse();
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_ERROR)).isEqualTo(1.0);
  }

  @Test
  public void pollFollowerStatuses_repairThrows_pollStillCompletes() {
    // The barrier that matters: whatever the repair does, the poll itself must return normally.
    DynamicLeaderLeaseManager manager =
        managerWithRepairCommand(HOSTNAME, List.of("lease-a", "lease-b"));
    when(this.mockCollection.find(any(Bson.class))).thenThrow(new AssertionError("check failed"));

    manager.pollFollowerStatuses();

    assertThat(manager.pollFollowerStatuses()).isNotNull();
  }

  @Test
  public void deleteCorruptLease_neverSweptButDocumentPresent_stillDeletes() {
    // The conf-call directive names an explicit lease id, so a delete must work even when no sweep
    // has recorded the lease. This is the regression guard for that independence.
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease lease = createLease(gen.getGenerationId(), HOSTNAME, Instant.now().minusSeconds(60));
    this.leaseManager.add(gen);
    when(this.mockFindIterable.first()).thenReturn(unparseable(lease));
    setupSuccessfulDelete();

    boolean deleted = this.leaseManager.deleteCorruptLease(lease.id());

    assertThat(deleted).isTrue();
    verify(this.mockCollection).deleteOne(any(Bson.class));
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_DELETED))
        .isEqualTo(1.0);
  }

  @Test
  public void deleteCorruptLease_versionBumpedBeforeDelete_doesNotDelete() {
    // A clean rewrite slips into the window between the re-check and the delete, bumping the
    // leaseVersion the delete is pinned to. The OCC filter matches nothing, so nothing is dropped.
    BsonDocument nilMvLease = nilMvUuidLease(NIL_MV_LEASE_ID);
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>(List.of(nilMvLease)));
    setupEmptyListCollections();
    this.leaseManager.syncLeasesFromMongod();
    when(this.mockFindIterable.first()).thenReturn(nilMvLease);
    DeleteResult noMatch = mock(DeleteResult.class);
    when(noMatch.getDeletedCount()).thenReturn(0L);
    when(this.mockCollection.deleteOne(any(Bson.class))).thenReturn(noMatch);

    boolean deleted = this.leaseManager.deleteCorruptLease(NIL_MV_LEASE_ID);

    assertThat(deleted).isFalse();
    verify(this.mockCollection).deleteOne(any(Bson.class));
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_LOST_RACE))
        .isEqualTo(1.0);
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_ERROR)).isEqualTo(0.0);
  }

  @Test
  public void deleteCorruptLease_lookupFailsAtRecheck_doesNotDelete() {
    // A transient Mongo failure during the re-check must not be read as corruption. The tool
    // declines rather than delete a possibly-valid lease.
    BsonDocument nilMvLease = nilMvUuidLease(NIL_MV_LEASE_ID);
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>(List.of(nilMvLease)));
    setupEmptyListCollections();
    this.leaseManager.syncLeasesFromMongod();
    when(this.mockFindIterable.first()).thenReturn(nilMvLease);
    // The re-check's collection lookup fails outright rather than returning "collection absent".
    when(this.mockDatabase.listCollections(BsonDocument.class))
        .thenThrow(new RuntimeException("transient mongo failure"));

    boolean deleted = this.leaseManager.deleteCorruptLease(NIL_MV_LEASE_ID);

    assertThat(deleted).isFalse();
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_INDETERMINATE))
        .isEqualTo(1.0);
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_ERROR)).isEqualTo(0.0);
  }

  @Test
  public void deleteCorruptLease_postRestoreRepairDefinitivelyFails_deletes() {
    // A migration-shaped lease whose materialized-view collection the resolver positively
    // establishes is absent. That is the load path re-rejecting the document, not a lookup that
    // went unanswered, so the tool may clear it under the usual expiry and OCC guards. Without the
    // DEFINITIVE/LOOKUP_FAILED split this reported indeterminate and the lease stayed unrepairable
    // while still blocking initialization.
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    String liveDb = gen.getDefinition().getDatabase();
    Lease staleLease =
        createLeaseWithCommitInfo(
            gen.getGenerationId(), HOSTNAME, staleCommitInfo(TENANT_ID, liveDb, SOURCE_COLLECTION));
    BsonDocument expiredRaw = staleLease.toBson();
    expiredRaw.put(
        "leaseExpiration", new BsonDateTime(Instant.now().minusSeconds(60).toEpochMilli()));
    this.leaseManager.add(gen);
    // The source collection resolves. The internal database lists an unrelated collection, so the
    // MV lookup answers "definitely not there" rather than failing.
    setupListCollections(liveDb, SOURCE_COLLECTION, UUID.randomUUID());
    setupListCollections(DATABASE_NAME, "some-other-collection", UUID.randomUUID());
    when(this.mockFindIterable.first()).thenReturn(expiredRaw);
    setupSuccessfulDelete();

    boolean deleted = this.leaseManager.deleteCorruptLease(staleLease.id());

    assertThat(deleted).isTrue();
    verify(this.mockCollection).deleteOne(any(Bson.class));
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_DELETED))
        .isEqualTo(1.0);
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_INDETERMINATE))
        .isEqualTo(0.0);
  }

  @Test
  public void deleteCorruptLease_postRestoreLookupFails_doesNotDelete() {
    // Same migration shape, but the MV lookup throws instead of answering. Nothing was learned
    // about the lease, so the tool must fail closed. This is the pair to the test above: the two
    // differ only in whether the resolver answered, and they must not reach the same verdict.
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    String liveDb = gen.getDefinition().getDatabase();
    Lease staleLease =
        createLeaseWithCommitInfo(
            gen.getGenerationId(), HOSTNAME, staleCommitInfo(TENANT_ID, liveDb, SOURCE_COLLECTION));
    BsonDocument expiredRaw = staleLease.toBson();
    expiredRaw.put(
        "leaseExpiration", new BsonDateTime(Instant.now().minusSeconds(60).toEpochMilli()));
    this.leaseManager.add(gen);
    setupListCollections(liveDb, SOURCE_COLLECTION, UUID.randomUUID());
    when(this.mockDatabase.listCollections(BsonDocument.class))
        .thenThrow(new RuntimeException("transient mongo failure"));
    when(this.mockFindIterable.first()).thenReturn(expiredRaw);

    boolean deleted = this.leaseManager.deleteCorruptLease(staleLease.id());

    assertThat(deleted).isFalse();
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_INDETERMINATE))
        .isEqualTo(1.0);
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_DELETED))
        .isEqualTo(0.0);
  }

  @Test
  public void deleteCorruptLease_neverRegisteredLease_recordsRejectionAgainstReadDatabase() {
    // No sweep and no add() has seen this lease, so leaseKeyToDatabase is empty and the
    // mapping-based recordCorruptLease overload would silently skip the record. The re-check knows
    // exactly which database it read, and the record is what lets the operator's next attempt
    // short-circuit to it, so it must be written with that database.
    BsonDocument nilMvLease = nilMvUuidLease(NIL_MV_LEASE_ID);
    nilMvLease.put(
        "leaseExpiration", new BsonDateTime(Instant.now().plusSeconds(600).toEpochMilli()));
    setupEmptyListCollections();
    when(this.mockFindIterable.first()).thenReturn(nilMvLease);

    boolean deleted = this.leaseManager.deleteCorruptLease(NIL_MV_LEASE_ID);

    // Unexpired, so the delete declines after classification, leaving the record observable.
    assertThat(deleted).isFalse();
    assertThat(this.leaseManager.getCorruptLeases())
        .containsEntry(
            NIL_MV_LEASE_ID,
            new DynamicLeaderLeaseManager.CorruptLease(
                DATABASE_NAME, LeaseRepairUtil.CorruptionReason.UNRESOLVABLE_MV_UUID));
  }

  @Test
  public void deleteCorruptLease_leaseNotExpired_doesNotDelete() {
    // The document is genuinely corrupt but has not expired, so another host may still hold it.
    // Deleting now would let the recreated lease restart at version 1 and defeat fencing.
    BsonDocument nilMvLease = nilMvUuidLease(NIL_MV_LEASE_ID);
    nilMvLease.put(
        "leaseExpiration", new BsonDateTime(Instant.now().plusSeconds(600).toEpochMilli()));
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>(List.of(nilMvLease)));
    setupEmptyListCollections();
    this.leaseManager.syncLeasesFromMongod();
    when(this.mockFindIterable.first()).thenReturn(nilMvLease);

    boolean deleted = this.leaseManager.deleteCorruptLease(NIL_MV_LEASE_ID);

    assertThat(deleted).isFalse();
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_REFUSED_NOT_EXPIRED))
        .isEqualTo(1.0);
    assertThat(repairOutcomeCounter(DynamicLeaderLeaseManager.REPAIR_OUTCOME_ERROR)).isEqualTo(0.0);
  }

  @Test
  public void pollFollowerStatuses_leaseVanishes_prunesRecordFromAnyDatabase() {
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease lease = createLease(gen.getGenerationId(), OTHER_HOSTNAME, Instant.now().plusSeconds(60));
    when(this.mockFindIterable.into(any()))
        .thenReturn(new ArrayList<>(List.of(unparseable(lease))));
    this.leaseManager.syncLeasesFromMongod();
    assertThat(this.leaseManager.getCorruptLeases()).containsKey(lease.id());
    this.leaseManager.add(gen);

    // The follower poll asks for this lease by id and gets nothing back, so it no longer exists.
    when(this.mockFindIterable.into(any())).thenReturn(new ArrayList<>());
    this.leaseManager.pollFollowerStatuses();

    assertThat(this.leaseManager.getCorruptLeases()).isEmpty();
  }

  @Test
  public void pollFollowerStatuses_repairCommandTargetsRejectedLease_deletesIt() {
    // The wiring guard. The first attempt at this feature hung execution off a method with no
    // production caller, so every test passed while the command could never fire. This drives the
    // real periodic entry point, pollFollowerStatuses, rather than calling the applier directly.
    Lease lease = corruptLeaseInDb();
    setupSuccessfulDelete();
    DynamicLeaderLeaseManager manager = managerWithRepairCommand(HOSTNAME, List.of(lease.id()));

    manager.pollFollowerStatuses();

    verify(this.mockCollection).deleteOne(any(Bson.class));
  }

  @Test
  public void pollFollowerStatuses_noRegisteredGenerations_stillAppliesRepairCommand() {
    // A lease that blocks lease creation can leave the host with no registered generation, which
    // is the early-return path in pollFollowerStatuses. The command must still be applied.
    Lease lease = corruptLeaseInDb();
    setupSuccessfulDelete();
    DynamicLeaderLeaseManager manager = managerWithRepairCommand(HOSTNAME, List.of(lease.id()));

    assertThat(manager.getFollowerGenerationIds()).isEmpty();
    manager.pollFollowerStatuses();

    verify(this.mockCollection).deleteOne(any(Bson.class));
  }

  @Test
  public void pollFollowerStatuses_repairCommandAppliedOnce_doesNotReapply() {
    // Bootstrap config cannot be withdrawn while the host runs, so re-attempting every poll would
    // keep incrementing the repair counter long after the work was done.
    Lease lease = corruptLeaseInDb();
    setupSuccessfulDelete();
    DynamicLeaderLeaseManager manager = managerWithRepairCommand(HOSTNAME, List.of(lease.id()));

    manager.pollFollowerStatuses();
    manager.pollFollowerStatuses();
    manager.pollFollowerStatuses();

    verify(this.mockCollection, times(1)).deleteOne(any(Bson.class));
  }

  @Test
  public void pollFollowerStatuses_repairCommandForAnotherInstance_doesNotDelete() {
    Lease lease = corruptLeaseInDb();
    DynamicLeaderLeaseManager manager =
        managerWithRepairCommand(OTHER_HOSTNAME, List.of(lease.id()));

    manager.pollFollowerStatuses();

    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  @Test
  public void pollFollowerStatuses_repairCommandExpired_doesNotDelete() {
    Lease lease = corruptLeaseInDb();
    DynamicLeaderLeaseManager manager =
        managerWithRepairCommand(
            HOSTNAME, List.of(lease.id()), Instant.now().minusSeconds(60).toString());

    manager.pollFollowerStatuses();

    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  @Test
  public void pollFollowerStatuses_repairCommandTargetsHealthyLease_doesNotDelete() {
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    Lease healthy = createLease(gen.getGenerationId(), HOSTNAME, Instant.now().plusSeconds(60));
    when(this.mockFindIterable.first()).thenReturn(healthy.toBson());
    DynamicLeaderLeaseManager manager = managerWithRepairCommand(HOSTNAME, List.of(healthy.id()));

    manager.pollFollowerStatuses();

    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  /**
   * Puts a single unparseable lease in the mocked collection and returns the lease it came from.
   */
  private Lease corruptLeaseInDb() {
    MaterializedViewIndexGeneration gen = createTestIndexGeneration();
    // Expired: deleteCorruptLease only deletes a lease past its expiry, matching the runbook.
    Lease lease = createLease(gen.getGenerationId(), HOSTNAME, Instant.now().minusSeconds(60));
    when(this.mockFindIterable.first()).thenReturn(unparseable(lease));
    return lease;
  }

  private DynamicLeaderLeaseManager managerWithRepairCommand(
      String instance, List<String> leaseIds) {
    return managerWithRepairCommand(instance, leaseIds, Instant.now().plusSeconds(300).toString());
  }

  private DynamicLeaderLeaseManager managerWithRepairCommand(
      String instance, List<String> leaseIds, String expiresAt) {
    LeaseManagerOpsCommands opsCommands =
        new LeaseManagerOpsCommands(
            Optional.empty(),
            LeaseManagerOpsCommands.getOpsRepairLeaseCommand(instance, leaseIds, expiresAt));
    return DynamicLeaderLeaseManager.create(
        this.mockAutoEmbeddingMongoClient,
        MeterAndFtdcRegistry.createWithSimpleRegistries(),
        HOSTNAME,
        DB_RESOLVER,
        this.mvMetadataCatalog,
        opsCommands);
  }

  private static final String NIL_MV_LEASE_ID = "nil-mv-uuid-lease";

  /** The lease's BSON with a required field dropped, so {@code Lease.fromBson} throws. */
  private static BsonDocument unparseable(Lease lease) {
    BsonDocument raw = lease.toBson();
    raw.remove("leaseVersion");
    return raw;
  }

  /**
   * A lease document that parses cleanly but carries no materialized-view metadata, so its MV UUID
   * falls back to nil and has to be resolved against a live collection.
   */
  private static BsonDocument nilMvUuidLease(String leaseId) {
    return BsonDocument.parse(
        "{\"_id\": \""
            + leaseId
            + "\","
            + "\"schemaVersion\": 1,"
            + "\"collectionUuid\": \"550e8400-e29b-41d4-a716-446655440000\","
            + "\"collectionName\": \"source-collection\","
            + "\"leaseOwner\": \"localhost\","
            + "\"leaseExpiration\": {\"$date\": \"2024-12-06T00:57:15.661Z\"},"
            + "\"leaseVersion\": 1,"
            + "\"commitInfo\": \"\","
            + "\"latestIndexDefinitionVersion\": \"1\","
            + "\"indexDefinitionVersionStatusMap\": {"
            + "  \"1\": {\"isQueryable\": false, \"indexStatusCode\": \"INITIAL_SYNC\"}}}");
  }

  /** Makes the MV collection lookup find nothing, so normalization cannot resolve the nil UUID. */
  @SuppressWarnings("unchecked")
  private void setupEmptyListCollections() {
    ListCollectionsIterable<BsonDocument> emptyIterable = mock(ListCollectionsIterable.class);
    MongoCursor<BsonDocument> emptyCursor = mock(MongoCursor.class);
    when(emptyCursor.hasNext()).thenReturn(false);
    when(emptyIterable.iterator()).thenReturn(emptyCursor);
    when(this.mockDatabase.listCollections(BsonDocument.class)).thenReturn(emptyIterable);
  }

  private double repairOutcomeCounter(String outcome) {
    return this.metricsFactory
        .meterRegistry
        .counter(
            this.metricsFactory.namespace
                + "."
                + DynamicLeaderLeaseManager.OPS_REPAIR_LEASE_COUNTER_NAME,
            DynamicLeaderLeaseManager.OPS_REPAIR_OUTCOME_TAG,
            outcome)
        .count();
  }

  private void setupSuccessfulDelete() {
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(deleteResult.getDeletedCount()).thenReturn(1L);
    when(this.mockCollection.deleteOne(any(Bson.class))).thenReturn(deleteResult);
  }

  private static String staleCommitInfo(String tenantId, String db, String coll) {
    return IndexCommitUserData.createChangeStreamResume(
            ChangeStreamResumeInfo.create(
                new MongoNamespace(tenantId + "_" + db, coll),
                ChangeStreamUtils.resumeToken(new BsonTimestamp(1L))),
            IndexFormatVersion.CURRENT.versionNumber)
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
    long epochAfterAcquisition =
        this.leaseManager.getLeases().get(getLeaseKeyFromCatalog(generationId)).epoch();

    // Act - heartbeat to renew
    setupSuccessfulLeaseUpdate();
    this.leaseManager.heartbeat();

    // Assert - still leader
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    // Heartbeat renewal is not a new tenure: epoch must not bump.
    assertThat(this.leaseManager.getLeases().get(getLeaseKeyFromCatalog(generationId)).epoch())
        .isEqualTo(epochAfterAcquisition);
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

  // ==================== Checkpoint Failure Counters ====================

  @Test
  public void heartbeat_renewalRejected_isNotCountedAsCheckpointFailure() {
    // Arrange
    MaterializedViewGenerationId generationId = acquireLeadership();
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    putLeaseInRenewalWindow(generationId);

    // Another host owns the lease in the store: this instance is a zombie leader.
    Lease stolenLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().plusSeconds(60));
    setupFailedLeaseUpdate();
    setupFindLeaseFromDatabase(stolenLease);

    // Act
    this.leaseManager.heartbeat();

    // Assert
    // A lost renewal implies no MV write, so it is deliberately uncounted.
    assertThat(checkpointRejectedCount()).isEqualTo(0.0);
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    // refreshLeaseFromDatabase still installs the foreign lease — the cache entry is
    // becomeFollower's follower-transition signal — but the accessor no longer hands its version
    // out as a fencing token (CLOUDP-429754).
    assertThat(this.leaseManager.getLeases().get(leaseKey).leaseOwner()).isEqualTo(OTHER_HOSTNAME);
    assertThat(this.leaseManager.getOwnedLeaseVersion(generationId))
        .isEqualTo(LeaseManager.INVALID_LEASE_VERSION);
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);
  }

  @Test
  public void getOwnedLeaseVersion_foreignLeaseInCache_returnsInvalid() {
    // Arrange - the Issue 2 regression: seed the cache with a lease owned by another host at a
    // higher version, exactly as refreshLeaseFromDatabase does on the leadership-loss paths.
    MaterializedViewGenerationId generationId = acquireLeadership();
    String leaseKey = getLeaseKeyFromCatalog(generationId);

    Lease foreignLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().plusSeconds(60));
    this.leaseManager.putLease(leaseKey, foreignLease);

    // Act + Assert - no token is handed out, so no MV write can be stamped with the new leader's
    // version.
    assertThat(this.leaseManager.getOwnedLeaseVersion(generationId))
        .isEqualTo(LeaseManager.INVALID_LEASE_VERSION);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(generationId);
  }

  @Test
  public void getOwnedLeaseVersion_expiredOwnedLeaseBeforeHeartbeat_returnsInvalid() {
    MaterializedViewGenerationId generationId = acquireLeadership();
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    Lease expiredOwnedLease = createLease(generationId, HOSTNAME, Instant.now().minusSeconds(60));
    this.leaseManager.putLease(leaseKey, expiredOwnedLease);

    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    assertThat(this.leaseManager.getOwnedLeaseVersion(generationId))
        .isEqualTo(LeaseManager.INVALID_LEASE_VERSION);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(generationId);
  }

  @Test
  public void getOwnedLeaseVersion_ownedLease_returnsVersion() {
    MaterializedViewGenerationId generationId = acquireLeadership();
    String leaseKey = getLeaseKeyFromCatalog(generationId);

    assertThat(this.leaseManager.getOwnedLeaseVersion(generationId))
        .isEqualTo(this.leaseManager.getLeases().get(leaseKey).leaseVersion());
  }

  @Test
  public void isUsableLeaseVersion_onlyAcceptsPositiveVersions() {
    assertThat(LeaseManager.isUsableLeaseVersion(LeaseManager.INVALID_LEASE_VERSION)).isFalse();
    assertThat(LeaseManager.isUsableLeaseVersion(0)).isFalse();
    assertThat(LeaseManager.isUsableLeaseVersion(1)).isTrue();
    assertThat(LeaseManager.isUsableLeaseVersion(Long.MAX_VALUE)).isTrue();
  }

  @Test
  public void getOwnedLeaseVersion_metadataRemoved_returnsInvalidWithoutDemotion() {
    MaterializedViewGenerationId generationId = acquireLeadership();
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    this.mvMetadataCatalog.removeMetadata(generationId);

    assertThat(this.leaseManager.getOwnedLeaseVersion(generationId))
        .isEqualTo(LeaseManager.INVALID_LEASE_VERSION);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(generationId);
    assertThat(this.leaseManager.getLeases()).containsKey(leaseKey);
  }

  @Test
  public void getOwnedLeaseVersion_noLease_returnsInvalid() {
    MaterializedViewGenerationId generationId = acquireLeadership();
    this.leaseManager.evictCachedLease(getLeaseKeyFromCatalog(generationId));

    assertThat(this.leaseManager.getOwnedLeaseVersion(generationId))
        .isEqualTo(LeaseManager.INVALID_LEASE_VERSION);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(generationId);
  }

  @Test
  public void heartbeat_renewalRejected_leaseDeleted_transitionsToFollower() {
    // Arrange
    MaterializedViewGenerationId generationId = acquireLeadership();
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    putLeaseInRenewalWindow(generationId);

    // No lease document in the store, as after a concurrent index drop.
    setupFailedLeaseUpdate();
    setupNoLeaseInDatabase();

    // Act
    this.leaseManager.heartbeat();

    // Assert
    assertThat(checkpointRejectedCount()).isEqualTo(0.0);
    // Pins pre-existing behaviour: an empty read caches nothing, so the stale entry survives —
    // which is what lets becomeFollower demote and the next poll re-detect the lease as acquirable.
    assertThat(this.leaseManager.getLeases()).containsKey(leaseKey);
    assertThat(this.leaseManager.getFollowerGenerationIds()).contains(generationId);
  }

  @Test
  public void heartbeat_asLeader_renewsLease_writesMaxExpirationAndCleanupState() {
    // Arrange - flag on, so the heartbeat runs the field-scoped updateOne path (CLOUDP-449935).
    this.leaseManager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            this.metricsFactory,
            HOSTNAME,
            DB_RESOLVER,
            this.mvMetadataCatalog,
            () -> true);
    MaterializedViewGenerationId generationId = acquireLeadership();
    putLeaseInRenewalWindow(generationId);
    long versionBefore = this.leaseManager.getOwnedLeaseVersion(generationId);
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    Instant expirationBefore = this.leaseManager.getLeases().get(leaseKey).leaseExpiration();

    // Act - heartbeat to renew
    this.leaseManager.heartbeat();

    // Assert - the renewal is an updateOne: filter is _id plus owner with no version predicate, and
    // update is exactly $max on leaseExpiration and $set on cleanupState. It must not carry this
    // host's cached commitInfo or bump leaseVersion: either would race a concurrent checkpoint on
    // the same cached version and cause the CLOUDP-397647 demotion / commitInfo rewind. $max, not
    // $set, so the heartbeat never moves expiration backwards.
    ArgumentCaptor<Bson> filterCaptor = ArgumentCaptor.forClass(Bson.class);
    ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);
    verify(this.mockCollection).updateOne(filterCaptor.capture(), updateCaptor.capture());
    BsonDocument filter = filterCaptor.getValue().toBsonDocument();
    assertThat(filter.toJson()).contains("_id");
    assertThat(filter.toJson()).contains(Lease.Fields.LEASE_OWNER.getName());
    assertThat(filter.toJson()).doesNotContain(Lease.Fields.LEASE_VERSION.getName());

    BsonDocument update =
        updateCaptor
            .getValue()
            .toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
    BsonDocument max = update.getDocument("$max");
    assertThat(max.keySet()).containsExactly(Lease.Fields.LEASE_EXPIRATION.getName());
    BsonDocument set = update.getDocument("$set");
    assertThat(set.keySet()).containsExactly(Lease.Fields.CLEANUP_STATE.getName());
    assertThat(update).doesNotContainKey(Lease.Fields.LEASE_VERSION.getName());
    assertThat(update).doesNotContainKey(Lease.Fields.COMMIT_INFO.getName());
    assertThat(update).doesNotContainKey(Lease.Fields.EPOCH.getName());

    // Renewal is not a new tenure: the in-memory version is unchanged, and with no concurrent
    // writer the merge does extend the cached expiration.
    assertThat(this.leaseManager.getOwnedLeaseVersion(generationId)).isEqualTo(versionBefore);
    assertThat(this.leaseManager.getLeases().get(leaseKey).leaseExpiration())
        .isGreaterThan(expirationBefore);
  }

  @Test
  public void heartbeat_renewal_afterConcurrentCheckpoint_preservesNewerCachedVersion() {
    // Arrange - flag on, so the renewal cache path is the field-scoped merge.
    this.leaseManager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            this.metricsFactory,
            HOSTNAME,
            DB_RESOLVER,
            this.mvMetadataCatalog,
            () -> true);
    MaterializedViewGenerationId generationId = acquireLeadership();
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    putLeaseInRenewalWindow(generationId);

    // A concurrent checkpoint advances the cached lease to a newer version/commitInfo between the
    // heartbeat's read and its success path, and its whole-document replace also lands on the
    // server after the heartbeat's $max -- so the database holds the checkpoint's earlier
    // build-time expiration. The merge must therefore keep the cache at the checkpoint's lease
    // rather than extend it: extending would leave the cache holding a later expiration than the
    // database, and the renewal-window skip reads this cache, so the store could go unrenewed
    // while the lease is takeover-able.
    Lease current = this.leaseManager.getLeases().get(leaseKey);
    Lease checkpointed =
        new Lease(
            current.id(),
            Lease.SCHEMA_VERSION,
            current.collectionUuid(),
            current.collectionName(),
            current.leaseOwner(),
            current.leaseExpiration(),
            current.leaseVersion() + 1,
            "checkpoint_v3",
            current.latestIndexDefinitionVersion(),
            current.indexDefinitionVersionStatusMap(),
            current.materializedViewCollectionMetadata(),
            current.steadyAsOfOplogPosition(),
            current.cleanupState(),
            current.epoch());
    when(this.mockCollection.updateOne(any(Bson.class), any(Bson.class)))
        .thenAnswer(
            invocation -> {
              this.leaseManager.putLease(leaseKey, checkpointed);
              UpdateResult ok = mock(UpdateResult.class);
              when(ok.getMatchedCount()).thenReturn(1L);
              return ok;
            });

    // Act
    this.leaseManager.heartbeat();

    // Assert - the cache retains the checkpoint's version/commitInfo/epoch AND its expiration; the
    // heartbeat's extension is not applied once the version advanced underneath the tick.
    Lease cached = this.leaseManager.getLeases().get(leaseKey);
    assertThat(cached.leaseVersion()).isEqualTo(checkpointed.leaseVersion());
    assertThat(cached.commitInfo()).isEqualTo("checkpoint_v3");
    assertThat(cached.epoch()).isEqualTo(checkpointed.epoch());
    assertThat(cached.leaseExpiration()).isEqualTo(checkpointed.leaseExpiration());
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
  }

  @Test
  public void heartbeat_asLeader_renewalZeroMatch_demotes() {
    // Arrange - flag on. Another host acquired the lease in the store, so the renewal's owner
    // filter matches zero rows and the leader must demote.
    this.leaseManager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            this.metricsFactory,
            HOSTNAME,
            DB_RESOLVER,
            this.mvMetadataCatalog,
            () -> true);
    MaterializedViewGenerationId generationId = acquireLeadership();
    putLeaseInRenewalWindow(generationId);

    Lease stolenLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().plusSeconds(60));
    setupFailedLeaseUpdate();
    setupFindLeaseFromDatabase(stolenLease);

    // Act
    this.leaseManager.heartbeat();

    // Assert - a renewal that matches zero rows (lease taken by another host) demotes the leader.
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
  }

  @Test
  public void updateCommitInfo_rejected_recordsCheckpointRejected() throws Exception {
    // Arrange
    MaterializedViewGenerationId generationId = acquireLeadership();
    long localVersionBefore = this.leaseManager.getOwnedLeaseVersion(generationId);

    // The commit path is the only place a zombie is caught when its MV writes were silently
    // dropped rather than rejected.
    Lease stolenLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().plusSeconds(60));
    setupFailedLeaseUpdate();
    setupFindLeaseFromDatabase(stolenLease);

    // Act
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY);

    // The legacy counter still records all rejections, while the risk gauge requires positive
    // confirmation that a materialized-view write completed.
    assertThat(checkpointRejectedCount()).isEqualTo(1.0);
    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(0.0);
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    // Nothing on this path refreshes the cache, so the local token is untouched.
    assertThat(this.leaseManager.getOwnedLeaseVersion(generationId)).isEqualTo(localVersionBefore);
    assertThat(this.leaseManager.getOwnedLeaseVersion(generationId))
        .isNotEqualTo(stolenLease.leaseVersion());
  }

  @Test
  public void updateCommitInfo_rejectedAfterMaterializedViewWrite_recordsPendingRisk()
      throws Exception {
    MaterializedViewGenerationId generationId = acquireLeadership();
    setupFailedLeaseUpdate();

    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);

    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);
  }

  @Test
  public void updateCommitInfo_sameGenerationReacquired_keepsPendingRisk() throws Exception {
    MaterializedViewGenerationId generationId = acquireLeadership();
    setupFailedLeaseUpdate();
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);
    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);

    setupSuccessfulLeaseUpdate();
    assertThat(this.leaseManager.tryAcquireLeadership(generationId)).isTrue();

    // Reacquisition carries commitInfo forward unchanged, so it does not re-anchor the write.
    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);
  }

  @Test
  public void updateCommitInfo_emptyCommitAfterReacquisition_resolvesPendingRisk()
      throws Exception {
    MaterializedViewGenerationId generationId = acquireLeadership();
    setupFailedLeaseUpdate();
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);
    setupSuccessfulLeaseUpdate();
    assertThat(this.leaseManager.tryAcquireLeadership(generationId)).isTrue();
    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);

    // The next commit tick advances commitInfo and clears, even with nothing to write.
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, false);

    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(0.0);
    assertThat(checkpointCommitRiskResolvedCount(generationId)).isEqualTo(1.0);
  }

  @Test
  public void updateCommitInfo_reacquiredAfterDifferentOwner_keepsPendingRisk() throws Exception {
    MaterializedViewGenerationId generationId = acquireLeadership();
    setupFailedLeaseUpdate();
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);
    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);

    Lease expiredOtherOwnerLease =
        createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredOtherOwnerLease);
    this.leaseManager.pollFollowerStatuses();
    setupSuccessfulLeaseUpdate();
    assertThat(this.leaseManager.tryAcquireLeadership(generationId)).isTrue();

    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);
  }

  @Test
  public void updateCommitInfo_successAfterDifferentOwnerReacquisition_resolvesPendingRisk()
      throws Exception {
    MaterializedViewGenerationId generationId = acquireLeadership();
    setupFailedLeaseUpdate();
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);

    Lease expiredOtherOwnerLease =
        createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredOtherOwnerLease);
    this.leaseManager.pollFollowerStatuses();
    setupSuccessfulLeaseUpdate();
    assertThat(this.leaseManager.tryAcquireLeadership(generationId)).isTrue();
    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);

    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);

    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(0.0);
    assertThat(checkpointCommitRiskResolvedCount(generationId)).isEqualTo(1.0);
  }

  @Test
  public void clearCheckpointCommitRisk_repeatedClears_countOneResolution() throws Exception {
    MaterializedViewGenerationId generationId = acquireLeadership();
    setupFailedLeaseUpdate();
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);

    this.leaseManager.clearCheckpointCommitRisk(generationId);
    this.leaseManager.clearCheckpointCommitRisk(generationId);

    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(0.0);
    assertThat(checkpointCommitRiskResolvedCount(generationId)).isEqualTo(1.0);
  }

  @Test
  public void add_staleCachedOwnership_keepsPendingRisk() throws Exception {
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    assertThat(this.leaseManager.tryAcquireLeadership(generationId)).isTrue();

    setupFailedLeaseUpdate();
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);
    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();

    // updateCommitInfo() deliberately does not refresh after an OCC miss, so the cached lease
    // still names this host even though the database may have moved to another owner.
    this.leaseManager.add(indexGeneration);

    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);
  }

  @Test
  public void clearCheckpointCommitRiskForIndex_retiredPredecessor_clearsBothGenerations()
      throws Exception {
    MaterializedViewGenerationId retiredGeneration = acquireLeadership();
    setupFailedLeaseUpdate();
    this.leaseManager.updateCommitInfo(retiredGeneration, EncodedUserData.EMPTY, true);

    // A later user version of the same index, as a definition update would produce.
    MaterializedViewGenerationId currentGeneration =
        new MaterializedViewGenerationId(
            retiredGeneration.indexId, retiredGeneration.generation.incrementUser());
    addCatalogMetadataForGeneration(currentGeneration);
    this.leaseManager.add(
        mockMatViewIndexGeneration(
            MaterializedViewIndex.mockMatViewDefinitionGeneration(currentGeneration)));
    this.leaseManager.updateCommitInfo(currentGeneration, EncodedUserData.EMPTY, true);

    MaterializedViewGenerationId otherIndexGeneration = acquireLeadership();
    setupFailedLeaseUpdate();
    this.leaseManager.updateCommitInfo(otherIndexGeneration, EncodedUserData.EMPTY, true);

    assertThat(checkpointCommitRiskPending(retiredGeneration)).isEqualTo(1.0);
    assertThat(checkpointCommitRiskPending(currentGeneration)).isEqualTo(1.0);
    assertThat(checkpointCommitRiskPending(otherIndexGeneration)).isEqualTo(1.0);

    this.leaseManager.clearCheckpointCommitRiskForIndex(retiredGeneration.indexId);

    assertThat(checkpointCommitRiskPending(retiredGeneration)).isEqualTo(0.0);
    assertThat(checkpointCommitRiskPending(currentGeneration)).isEqualTo(0.0);
    assertThat(checkpointCommitRiskPending(otherIndexGeneration)).isEqualTo(1.0);
  }

  @Test
  public void clearCheckpointCommitRiskForIndex_terminal_retiresStateAndMeters() throws Exception {
    MaterializedViewGenerationId generationId = acquireLeadership();
    setupFailedLeaseUpdate();
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);
    assertThat(riskMeterCount(generationId)).isEqualTo(3);

    this.leaseManager.clearCheckpointCommitRiskForIndex(generationId.indexId);

    // Terminal for the index, so the timeseries retire rather than lingering at zero forever.
    assertThat(riskMeterCount(generationId)).isEqualTo(0);
  }

  @Test
  public void expireStaleCheckpointCommitRisk_pastTtl_clearsAndCountsExpiry() throws Exception {
    MaterializedViewGenerationId generationId = acquireLeadership();
    setupFailedLeaseUpdate();
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);
    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);

    this.leaseManager.expireStaleCheckpointCommitRisk(
        Instant.now().plus(CheckpointCommitRiskTracker.TTL).plusSeconds(1));

    // Expiry is the absence of observed recovery, so it must not feed the alert's resolved guard.
    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(0.0);
    assertThat(checkpointCommitRiskExpiredCount(generationId)).isEqualTo(1.0);
    assertThat(checkpointCommitRiskResolvedCount(generationId)).isEqualTo(0.0);
  }

  @Test
  public void expireStaleCheckpointCommitRisk_withinTtl_keepsPendingRisk() throws Exception {
    MaterializedViewGenerationId generationId = acquireLeadership();
    setupFailedLeaseUpdate();
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);

    this.leaseManager.expireStaleCheckpointCommitRisk(
        Instant.now().plus(CheckpointCommitRiskTracker.TTL).minusSeconds(60));

    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);
    assertThat(checkpointCommitRiskExpiredCount(generationId)).isEqualTo(0.0);
  }

  @Test
  public void drop_retainedMaterializedView_keepsPendingRisk() throws Exception {
    MaterializedViewGenerationId generationId = acquireLeadership();
    setupFailedLeaseUpdate();
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);

    this.leaseManager.drop(generationId);

    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);
  }

  @Test
  public void updateCommitInfo_afterDemotion_recordsSkippedCheckpoint() throws Exception {
    // Arrange - a demoted generator still draining. commit() calls updateCommitInfo even for an
    // empty batch, so this catches drains where no document was even written.
    MaterializedViewGenerationId generationId = acquireLeadership();
    this.leaseManager.releaseLeadership(generationId);
    clearInvocations(this.mockCollection);

    // Act
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY);

    // Assert - dropped before reaching the store, so no OCC rejection either
    assertThat(skippedCheckpointCount()).isEqualTo(1.0);
    assertThat(checkpointRejectedCount()).isEqualTo(0.0);
    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(0.0);
    verify(this.mockCollection, never()).replaceOne(any(), any(BsonDocument.class));
  }

  @Test
  public void updateCommitInfo_afterDemotionAndMaterializedViewWrite_recordsPendingRisk()
      throws Exception {
    MaterializedViewGenerationId generationId = acquireLeadership();
    this.leaseManager.releaseLeadership(generationId);

    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY, true);

    assertThat(checkpointCommitRiskPending(generationId)).isEqualTo(1.0);
  }

  @Test
  public void updateCommitInfo_asLeader_recordsNoSkippedCheckpoint() throws Exception {
    // Arrange
    MaterializedViewGenerationId generationId = acquireLeadership();
    setupSuccessfulLeaseUpdate();

    // Act
    this.leaseManager.updateCommitInfo(generationId, EncodedUserData.EMPTY);

    // Assert
    assertThat(skippedCheckpointCount()).isEqualTo(0.0);
  }

  private double skippedCheckpointCount() {
    Counter counter =
        this.metricsFactory
            .meterRegistry
            .find(
                this.metricsFactory.namespace
                    + "."
                    + DynamicLeaderLeaseManager.SKIPPED_CHECKPOINT_COUNTER_NAME)
            .counter();
    return counter == null ? 0.0 : counter.count();
  }

  /** Looks up a per-generation risk meter by the same tags the alert joins on. */
  private Search findRiskMeter(String name, MaterializedViewGenerationId generationId) {
    return this.metricsFactory
        .meterRegistry
        .find(this.metricsFactory.namespace + "." + name)
        .tags(PerIndexMetricsFactory.tagsFor(generationId));
  }

  private double checkpointCommitRiskPending(MaterializedViewGenerationId generationId) {
    var gauge = findRiskMeter(CheckpointCommitRiskTracker.PENDING_GAUGE_NAME, generationId).gauge();
    return gauge == null ? 0.0 : gauge.value();
  }

  /** How many of the three per-generation risk meters are currently registered. */
  private long riskMeterCount(MaterializedViewGenerationId generationId) {
    return Stream.of(
            CheckpointCommitRiskTracker.PENDING_GAUGE_NAME,
            CheckpointCommitRiskTracker.RESOLVED_COUNTER_NAME,
            CheckpointCommitRiskTracker.EXPIRED_COUNTER_NAME)
        .filter(name -> findRiskMeter(name, generationId).meter() != null)
        .count();
  }

  private double checkpointCommitRiskExpiredCount(MaterializedViewGenerationId generationId) {
    Counter counter =
        findRiskMeter(CheckpointCommitRiskTracker.EXPIRED_COUNTER_NAME, generationId).counter();
    return counter == null ? 0.0 : counter.count();
  }

  private double checkpointCommitRiskResolvedCount(MaterializedViewGenerationId generationId) {
    Counter counter =
        findRiskMeter(CheckpointCommitRiskTracker.RESOLVED_COUNTER_NAME, generationId).counter();
    return counter == null ? 0.0 : counter.count();
  }

  /** Adds a generation and takes leadership of its expired lease. */
  private MaterializedViewGenerationId acquireLeadership() {
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();
    return generationId;
  }

  private double checkpointRejectedCount() {
    Counter counter =
        this.metricsFactory
            .meterRegistry
            .find(
                this.metricsFactory.namespace
                    + "."
                    + DynamicLeaderLeaseManager.CHECKPOINT_REJECTED_COUNTER_NAME)
            .counter();
    return counter == null ? 0.0 : counter.count();
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
    // The renewal window is skipped, so the heartbeat issues no updateOne (or any other write).
    verify(this.mockCollection, never()).updateOne(any(Bson.class), any(Bson.class));
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
  public void pollFollowerStatuses_returnsReplicationOpTimeFromLeaseCheckpoint() {
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);
    BsonTimestamp replicationOpTime = new BsonTimestamp(123, 1);
    EncodedUserData commitInfo =
        IndexCommitUserData.createChangeStreamResume(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace("test", "collection"),
                    ChangeStreamUtils.resumeToken(replicationOpTime)),
                IndexFormatVersion.CURRENT.versionNumber)
            .toEncodedData();
    Lease lease =
        createLeaseWithStatus(
                generationId,
                OTHER_HOSTNAME,
                Instant.now().plusSeconds(60),
                IndexStatus.StatusCode.STEADY)
            .withUpdatedCheckpoint(commitInfo);
    setupFindLeaseFromDatabase(lease);

    LeaseManager.FollowerPollResult result = this.leaseManager.pollFollowerStatuses();

    assertThat(result.replicationOpTimes()).containsEntry(generationId, replicationOpTime);
  }

  @Test
  public void pollFollowerStatuses_malformedCheckpoint_skipsOptimeAndContinues() {
    assertThat(
            this.metricsFactory
                .meterRegistry
                .find(this.metricsFactory.namespace + ".corruptedLeases")
                .counter())
        .isNull();

    MaterializedViewIndexGeneration malformedGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId malformedGenerationId = malformedGeneration.getGenerationId();
    this.leaseManager.add(malformedGeneration);
    EncodedUserData malformedCommitInfo =
        IndexCommitUserData.createChangeStreamResume(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace("test", "collection"), new BsonDocument()),
                IndexFormatVersion.CURRENT.versionNumber)
            .toEncodedData();
    Lease malformedLease =
        createLeaseWithStatus(
                malformedGenerationId,
                OTHER_HOSTNAME,
                Instant.now().minusSeconds(60),
                IndexStatus.StatusCode.STEADY)
            .withUpdatedCheckpoint(malformedCommitInfo)
            .withReleasedOwnership();

    MaterializedViewIndexGeneration validGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId validGenerationId = validGeneration.getGenerationId();
    this.leaseManager.add(validGeneration);
    BsonTimestamp validReplicationOpTime = new BsonTimestamp(123, 1);
    EncodedUserData validCommitInfo =
        IndexCommitUserData.createChangeStreamResume(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace("test", "collection"),
                    ChangeStreamUtils.resumeToken(validReplicationOpTime)),
                IndexFormatVersion.CURRENT.versionNumber)
            .toEncodedData();
    Lease validLease =
        createLeaseWithStatus(
                validGenerationId,
                OTHER_HOSTNAME,
                Instant.now().plusSeconds(60),
                IndexStatus.StatusCode.STEADY)
            .withUpdatedCheckpoint(validCommitInfo);
    when(this.mockFindIterable.into(any()))
        .thenReturn(new ArrayList<>(List.of(malformedLease.toBson(), validLease.toBson())));

    LeaseManager.FollowerPollResult result = this.leaseManager.pollFollowerStatuses();

    assertThat(result.statuses()).containsKey(malformedGenerationId);
    assertThat(result.statuses()).containsKey(validGenerationId);
    assertThat(result.replicationOpTimes()).doesNotContainKey(malformedGenerationId);
    assertThat(result.replicationOpTimes())
        .containsEntry(validGenerationId, validReplicationOpTime);
    assertThat(result.acquirableLeases()).contains(malformedGenerationId);
    assertThat(
            this.metricsFactory
                .meterRegistry
                .counter(this.metricsFactory.namespace + ".corruptedLeases")
                .count())
        .isEqualTo(1.0);
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
  public void pollFollowerStatuses_versionKeyNotPublishedInLease_reportsUnknown() throws Exception {
    // A follower that creates its generation inside the leader's publication window polls a lease
    // that does not carry its version key yet (HELP-97792). The lease's latest version is the
    // previous definition, STEADY and checkpointed; inheriting it would report a queryable
    // materialized view for one that is still empty, and that spurious STEADY latches the
    // "was queryable" ratchet that then authorises a premature staged-index swap.
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration(1);
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    Lease leaseWithoutVersionOne =
        createLeaseWithVersionStatuses(
            generationId,
            OTHER_HOSTNAME,
            "0",
            Map.of(
                "0", new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY)),
            checkpointCommitInfo(new BsonTimestamp(123, 1)));
    setupFindLeaseFromDatabase(leaseWithoutVersionOne);

    LeaseManager.FollowerPollResult result = this.leaseManager.pollFollowerStatuses();

    assertThat(result.statuses().get(generationId).getStatusCode())
        .isEqualTo(IndexStatus.StatusCode.UNKNOWN);
    // The previous version's checkpoint must not be adopted as this generation's replication
    // position either — an unpublished version has no progress to report.
    assertThat(result.replicationOpTimes()).doesNotContainKey(generationId);
  }

  @Test
  public void pollFollowerStatuses_versionKeyPublishedAsInitialSync_reportsInitialSync()
      throws Exception {
    // Same shape as the test above, one lease write later: once the leader publishes the version
    // key, the follower reports the truthful status rather than UNKNOWN.
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration(1);
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);

    Lease leaseWithVersionOne =
        createLeaseWithVersionStatuses(
            generationId,
            OTHER_HOSTNAME,
            "1",
            Map.of(
                "0",
                new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY),
                "1",
                new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.INITIAL_SYNC)),
            "");
    setupFindLeaseFromDatabase(leaseWithVersionOne);

    LeaseManager.FollowerPollResult result = this.leaseManager.pollFollowerStatuses();

    assertThat(result.statuses().get(generationId).getStatusCode())
        .isEqualTo(IndexStatus.StatusCode.INITIAL_SYNC);
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
  public void releaseLeadership_afterDrop_doesNotResurrectUntrackedGeneration() throws Exception {
    // Arrange - acquire leadership so the generation starts out LEADER with a cached lease.
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);
    Lease expiredLease = createLease(generationId, OTHER_HOSTNAME, Instant.now().minusSeconds(60));
    setupFindLeaseFromDatabase(expiredLease);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();
    this.leaseManager.tryAcquireLeadership(generationId);
    assertThat(this.leaseManager.isLeader(generationId)).isTrue();

    // drop() untracks the generation but deliberately leaves its lease cached (see
    // drop_asLeader_onlyUntracksGenerationId above).
    this.leaseManager.drop(generationId);
    assertThat(this.leaseManager.getLeaderGenerationIds()).doesNotContain(generationId);
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(generationId);

    // Act - releaseLeadership() (-> becomeFollower()) firing on an already-untracked generation
    // (e.g. a stale demotion racing with drop()) must not resurrect it as a follower just because
    // its lease is still cached.
    this.leaseManager.releaseLeadership(generationId);

    // Assert - still untracked, not silently re-registered as a follower.
    assertThat(this.leaseManager.isLeader(generationId)).isFalse();
    assertThat(this.leaseManager.getLeaderGenerationIds()).doesNotContain(generationId);
    assertThat(this.leaseManager.getFollowerGenerationIds()).doesNotContain(generationId);
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

  @Test
  public void evictCachedLease_removesCachedStateOnly() {
    // Arrange - a superseded generation left its lease behind for the stale-lease GC
    MaterializedViewIndexGeneration indexGeneration = createTestIndexGeneration();
    MaterializedViewGenerationId generationId = indexGeneration.getGenerationId();
    this.leaseManager.add(indexGeneration);
    String leaseKey = getLeaseKeyFromCatalog(generationId);
    assertThat(this.leaseManager.getLeases()).containsKey(leaseKey);

    this.leaseManager.evictCachedLease(leaseKey);

    // In-memory lease cache and database mapping are gone; the lease document is untouched, so a
    // later initializeLease for the same definition re-reads the database instead of stale
    // metadata.
    assertThat(this.leaseManager.getLeases()).doesNotContainKey(leaseKey);
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).doesNotContainKey(leaseKey);
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
    assertThat(repairCounter(POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME)).isEqualTo(0.0);
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
    assertThat(repairCounter(POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME)).isAtLeast(1.0);
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
            .withNewTenure(HOSTNAME);

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
            .withNewTenure(HOSTNAME);

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
    // No readable string id, so there is no key to record the rejection under. The counter moves
    // but the map stays empty, which is why parseLeaseOrThrow logs the BSON type of the _id: the
    // runbook tells operators to read the lease id out of the logs, and this document has none to
    // give. Documented here rather than fixed, because the map is only a lookup hint.
    assertThat(manager.getCorruptLeases()).isEmpty();
  }

  // ==================== findGcCandidates ====================

  @Test
  public void findGcCandidates_returnsLeases_andRegistersDatabaseMapping() {
    String collectionName = "gc-orphan-collection";
    Lease lease = createLeaseWithMatViewUuid(collectionName, UUID.randomUUID());
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    List<Lease> leases = this.leaseManager.findGcCandidates().leases();

    assertThat(leases).hasSize(1);
    assertThat(leases.get(0).id()).isEqualTo(collectionName);
    // The lease→database mapping must be registered even though no configured generation ever
    // added this lease, so a follow-up markEligibleForCleanup can resolve the database.
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).containsKey(collectionName);
    assertThat(this.leaseManager.getLeaseKeyToDatabase().get(collectionName))
        .isEqualTo(DATABASE_NAME);
  }

  @Test
  public void findGcCandidates_scansUnfiltered() {
    // The scan must see every lease, fresh ones included — the never-mark-current safeguard is
    // derived from it, and the current version is normally renewed. Staleness filtering is the
    // sweep's job, client-side.
    this.leaseManager.findGcCandidates();

    verify(this.mockCollection).find();
    verify(this.mockCollection, never()).find(any(Bson.class));
  }

  @Test
  public void findGcCandidates_skipsCorruptedLeases() {
    Lease good = createLeaseWithMatViewUuid("good-collection", UUID.randomUUID());
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(new BsonDocument("_id", new BsonString("corrupted")));
    leaseList.add(good.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    List<Lease> leases = this.leaseManager.findGcCandidates().leases();

    assertThat(leases).hasSize(1);
    assertThat(leases.get(0).id()).isEqualTo("good-collection");
  }

  @Test
  public void findGcCandidates_corruptedDocumentsOnly_notReportedEmpty() {
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(new BsonDocument("_id", new BsonString("corrupted")));
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);

    LeaseManager.GcScanResult result = this.leaseManager.findGcCandidates();

    // No parseable lease, but the table is not empty: the sweep must keep scanning so the
    // corrupted document keeps being counted.
    assertThat(result.leases()).isEmpty();
    assertThat(result.complete()).isTrue();
    assertThat(result.sawDocuments()).isTrue();
    assertThat(result.isCompleteAndEmpty()).isFalse();
  }

  @Test
  public void findGcCandidates_queryFailure_returnsEmpty() {
    when(this.mockCollection.find()).thenThrow(new RuntimeException("find failed"));

    assertThat(this.leaseManager.findGcCandidates().leases()).isEmpty();
  }

  @Test
  public void findGcCandidates_queryFailure_reportsIncompleteScan() {
    when(this.mockCollection.find()).thenThrow(new RuntimeException("find failed"));

    // Empty because the read failed, not because there are no leases — callers must be able to
    // tell the two apart.
    LeaseManager.GcScanResult result = this.leaseManager.findGcCandidates();
    assertThat(result.complete()).isFalse();
    assertThat(result.isCompleteAndEmpty()).isFalse();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void findGcCandidates_oneDatabaseFails_returnsOthersLeasesAndReportsIncomplete() {
    // Two databases in the scan set: the default read fails, the tenant read succeeds. The tenant's
    // lease must still come back (partial results are safe for marking) but the scan must not be
    // reported complete, or an empty default database would look like an idle cluster.
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
    when(tenantCollection.find()).thenReturn(tenantFindIterable);
    Lease tenantLease = createLeaseWithMatViewUuid("tenant-lease", UUID.randomUUID());
    ArrayList<BsonDocument> tenantLeases = new ArrayList<>();
    tenantLeases.add(tenantLease.toBson());
    when(tenantFindIterable.into(any())).thenReturn(tenantLeases);
    // The default database is the one that fails.
    when(this.mockCollection.find()).thenThrow(new RuntimeException("find failed"));

    DynamicLeaderLeaseManager manager =
        new DynamicLeaderLeaseManager(
            this.mockAutoEmbeddingMongoClient,
            new SimpleMetricsFactory(),
            HOSTNAME,
            tenantResolver,
            this.mvMetadataCatalog);
    manager.add(createTestIndexGeneration());

    LeaseManager.GcScanResult result = manager.findGcCandidates();

    assertThat(result.leases().stream().map(Lease::id)).containsExactly("tenant-lease");
    assertThat(result.complete()).isFalse();
    assertThat(result.isCompleteAndEmpty()).isFalse();
  }

  @Test
  public void findGcCandidates_noLeases_reportsCompleteAndEmpty() {
    LeaseManager.GcScanResult result = this.leaseManager.findGcCandidates();

    assertThat(result.leases()).isEmpty();
    assertThat(result.complete()).isTrue();
    assertThat(result.isCompleteAndEmpty()).isTrue();
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
    when(tenantCollection.find()).thenReturn(tenantFindIterable);
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

    List<Lease> leases = manager.findGcCandidates().leases();

    assertThat(leases.stream().anyMatch(c -> c.id().equals("tenant-orphan-lease"))).isTrue();
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

    this.leaseManager.findGcCandidates();

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
    this.leaseManager.findGcCandidates();
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
    this.leaseManager.findGcCandidates();
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
    this.leaseManager.findGcCandidates();
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
        Lease.CleanupState.ELIGIBLE_FOR_CLEANUP,
        Lease.FIRST_EPOCH);
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
        Lease.CleanupState.NOT_ELIGIBLE,
        Lease.FIRST_EPOCH);
  }

  /** Seeds the lease→database mapping (as the GC scan would) for the given lease. */
  private void registerViaGcScan(Lease lease) {
    ArrayList<BsonDocument> leaseList = new ArrayList<>();
    leaseList.add(lease.toBson());
    when(this.mockFindIterable.into(any())).thenReturn(leaseList);
    this.leaseManager.findGcCandidates();
  }

  @Test
  public void resolveStaleMatViewNamespace_stillEligible_returnsNamespace() {
    String collectionName = "cleanup-collection";
    Lease lease = markedLease(collectionName, Instant.now().minusSeconds(7200));
    registerViaGcScan(lease);
    // The re-read returns the still-eligible lease.
    when(this.mockFindIterable.first()).thenReturn(lease.toBson());

    Optional<MongoNamespace> namespace =
        this.leaseManager.resolveStaleMatViewNamespace(collectionName, 1L, Instant.now());

    assertThat(namespace).isPresent();
    assertThat(namespace.get().getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(namespace.get().getCollectionName()).isEqualTo(collectionName);
  }

  @Test
  public void resolveStaleMatViewNamespace_versionMismatch_returnsEmpty() {
    String collectionName = "reused-collection";
    Lease lease = markedLease(collectionName, Instant.now().minusSeconds(7200));
    registerViaGcScan(lease);
    // The re-read lease is still marked and stale, but at leaseVersion 1; the sweep expected a
    // different version, meaning the lease was reused since the snapshot. The fencing-token check
    // must veto the drop.
    when(this.mockFindIterable.first()).thenReturn(lease.toBson());

    assertThat(this.leaseManager.resolveStaleMatViewNamespace(collectionName, 2L, Instant.now()))
        .isEmpty();
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
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP,
            Lease.FIRST_EPOCH);
    registerViaGcScan(lease);
    when(this.mockFindIterable.first()).thenReturn(lease.toBson());

    assertThat(this.leaseManager.resolveStaleMatViewNamespace(leaseKey, 1L, Instant.now()))
        .isEmpty();
  }

  @Test
  public void resolveStaleMatViewNamespace_resurrectedLease_returnsEmpty() {
    Instant now = Instant.now();
    String collectionName = "resurrected-collection";
    registerViaGcScan(markedLease(collectionName, now.minusSeconds(7200)));
    // By resolve time the lease was renewed past the cutoff: the re-read must veto it.
    Lease renewed = markedLease(collectionName, now.plusSeconds(60));
    when(this.mockFindIterable.first()).thenReturn(renewed.toBson());

    assertThat(this.leaseManager.resolveStaleMatViewNamespace(collectionName, 1L, now)).isEmpty();
  }

  @Test
  public void resolveStaleMatViewNamespace_unmarkedLease_returnsEmpty() {
    String collectionName = "unmarked-collection";
    registerViaGcScan(markedLease(collectionName, Instant.now().minusSeconds(7200)));
    // By resolve time the cleanup mark was reverted (e.g. the lease was reused, CLOUDP-412595):
    // the re-read must veto it even though it is still expired.
    Lease unmarked = unmarkedLease(collectionName, Instant.now().minusSeconds(7200));
    when(this.mockFindIterable.first()).thenReturn(unmarked.toBson());

    assertThat(this.leaseManager.resolveStaleMatViewNamespace(collectionName, 1L, Instant.now()))
        .isEmpty();
  }

  @Test
  public void resolveStaleMatViewNamespace_leaseAlreadyDeleted_returnsEmpty() {
    String collectionName = "already-gone-collection";
    registerViaGcScan(markedLease(collectionName, Instant.now().minusSeconds(7200)));
    when(this.mockFindIterable.first()).thenReturn(null);

    assertThat(this.leaseManager.resolveStaleMatViewNamespace(collectionName, 1L, Instant.now()))
        .isEmpty();
  }

  @Test
  public void resolveStaleMatViewNamespace_unknownLease_returnsEmpty() {
    assertThat(this.leaseManager.resolveStaleMatViewNamespace("never-seen", 1L, Instant.now()))
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

    assertThat(this.leaseManager.deleteStaleLease(collectionName, 1L, Instant.now())).isTrue();
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

    assertThat(this.leaseManager.deleteStaleLease(collectionName, 1L, Instant.now())).isFalse();
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).containsKey(collectionName);
  }

  @Test
  public void deleteStaleLease_filtersOnMarkedStateVersionAndExpiration() {
    String collectionName = "filtered-collection";
    registerViaGcScan(markedLease(collectionName, Instant.now().minusSeconds(7200)));
    Instant cutoff = Instant.parse("2026-01-01T00:00:00Z");
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(deleteResult.getDeletedCount()).thenReturn(0L);
    when(this.mockCollection.deleteOne(any(Bson.class))).thenReturn(deleteResult);

    this.leaseManager.deleteStaleLease(collectionName, 7L, cutoff);

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

  @Test
  public void deleteStaleLease_unknownLease_returnsFalseWithoutDbWrite() {
    assertThat(this.leaseManager.deleteStaleLease("never-seen-lease", 1L, Instant.now())).isFalse();
    verify(this.mockCollection, never()).deleteOne(any(Bson.class));
  }

  @Test
  public void deleteStaleLease_deleteFailure_returnsFalse() {
    String collectionName = "error-delete-collection";
    registerViaGcScan(markedLease(collectionName, Instant.now().minusSeconds(7200)));
    when(this.mockCollection.deleteOne(any(Bson.class)))
        .thenThrow(new RuntimeException("delete failed"));

    assertThat(this.leaseManager.deleteStaleLease(collectionName, 1L, Instant.now())).isFalse();
  }

  // ==================== deleted-index deliberate cleanup ====================

  @Test
  public void cleanupLeaseForDeletedIndex_deletesByIdRegardlessOfOwnerAndState() {
    String collectionName = "deleted-index-lease";
    Lease lease = createLeaseWithMatViewUuid(collectionName, UUID.randomUUID());
    registerViaGcScan(lease);
    this.leaseManager.putLease(collectionName, lease);
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(this.mockCollection.deleteOne(any(Bson.class))).thenReturn(deleteResult);

    assertThat(this.leaseManager.cleanupLeaseForDeletedIndex(collectionName)).isTrue();

    // By id only: no owner, staleness, or cleanup-state predicate — the index is gone, so even a
    // fresh NOT_ELIGIBLE lease is deleted. The local cache is evicted with it.
    ArgumentCaptor<Bson> filterCaptor = ArgumentCaptor.forClass(Bson.class);
    verify(this.mockCollection).deleteOne(filterCaptor.capture());
    String filterJson = filterCaptor.getValue().toBsonDocument().toJson();
    assertThat(filterJson).doesNotContain(Lease.Fields.LEASE_OWNER.getName());
    assertThat(filterJson).doesNotContain(Lease.Fields.CLEANUP_STATE.getName());
    assertThat(filterJson).doesNotContain(Lease.Fields.LEASE_EXPIRATION.getName());
    assertThat(this.leaseManager.getLeases()).doesNotContainKey(collectionName);
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).doesNotContainKey(collectionName);
  }

  @Test
  public void cleanupLeaseForDeletedIndex_deleteFailure_returnsFalse() {
    String collectionName = "deleted-index-error-lease";
    registerViaGcScan(createLeaseWithMatViewUuid(collectionName, UUID.randomUUID()));
    when(this.mockCollection.deleteOne(any(Bson.class)))
        .thenThrow(new RuntimeException("delete failed"));

    assertThat(this.leaseManager.cleanupLeaseForDeletedIndex(collectionName)).isFalse();
    // The cached copy is evicted even when the delete fails; the document is left for the GC.
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).doesNotContainKey(collectionName);
  }

  @Test
  public void findLeasesForDeletedIndex_returnsLeasesWithNamespacesAndRegistersMappings() {
    ObjectId indexId = new ObjectId();
    String knownKey = indexId.toHexString() + "-hash1-1-0";
    String leftoverKey = indexId.toHexString() + "-hash2-1-0";
    // v0 lease ids are the bare indexId with no dash (see Lease#indexId).
    String v0Key = indexId.toHexString();
    // Register the known key's database mapping first.
    registerViaGcScan(createLeaseWithMatViewUuid(knownKey, UUID.randomUUID()));
    ArrayList<BsonDocument> found = new ArrayList<>();
    found.add(createLeaseWithMatViewUuid(knownKey, UUID.randomUUID()).toBson());
    found.add(createLeaseWithMatViewUuid(leftoverKey, UUID.randomUUID()).toBson());
    found.add(createLeaseWithMatViewUuid(v0Key, UUID.randomUUID()).toBson());
    when(this.mockFindIterable.into(any())).thenReturn(found);

    List<LeaseManager.DeletedIndexLease> leases =
        this.leaseManager.findLeasesForDeletedIndex(indexId, knownKey);

    assertThat(leases.stream().map(entry -> entry.lease().id()))
        .containsExactly(knownKey, leftoverKey, v0Key);
    // Each lease arrives paired with the MV namespace to drop — no per-lease re-read needed.
    assertThat(
            leases.stream()
                .map(entry -> entry.matViewNamespace().orElseThrow().getCollectionName()))
        .containsExactly(knownKey, leftoverKey, v0Key);
    // The leftover's mapping is registered so the deliberate cleanup calls can resolve it.
    assertThat(this.leaseManager.getLeaseKeyToDatabase()).containsKey(leftoverKey);
    // The scan pattern covers both id layouts: bare v0 ids and dash-suffixed v1+ ids.
    ArgumentCaptor<Bson> filterCaptor = ArgumentCaptor.forClass(Bson.class);
    verify(this.mockCollection).find(filterCaptor.capture());
    assertThat(filterCaptor.getValue().toBsonDocument().toJson())
        .contains("^" + indexId.toHexString());
  }

  @Test
  public void findLeasesForDeletedIndex_unparseableDocument_skippedNotFatal() {
    ObjectId indexId = new ObjectId();
    String knownKey = indexId.toHexString() + "-hash1-1-0";
    String leftoverKey = indexId.toHexString() + "-hash2-1-0";
    registerViaGcScan(createLeaseWithMatViewUuid(knownKey, UUID.randomUUID()));
    ArrayList<BsonDocument> found = new ArrayList<>();
    found.add(createLeaseWithMatViewUuid(knownKey, UUID.randomUUID()).toBson());
    // Malformed lease document (missing required fields): must be skipped per document, not
    // abort the scan and hide the remaining leases.
    found.add(new BsonDocument("_id", new BsonString(indexId.toHexString() + "-bad-1-0")));
    found.add(createLeaseWithMatViewUuid(leftoverKey, UUID.randomUUID()).toBson());
    when(this.mockFindIterable.into(any())).thenReturn(found);

    List<LeaseManager.DeletedIndexLease> leases =
        this.leaseManager.findLeasesForDeletedIndex(indexId, knownKey);

    assertThat(leases.stream().map(entry -> entry.lease().id()))
        .containsExactly(knownKey, leftoverKey);
  }

  @Test
  public void findLeasesForDeletedIndex_unknownDatabase_returnsEmpty() {
    assertThat(this.leaseManager.findLeasesForDeletedIndex(new ObjectId(), "never-seen-lease"))
        .isEmpty();
    verify(this.mockCollection, never()).find(any(Bson.class));
  }

  @Test
  public void resolveStaleMatViewNamespace_reReadThrows_returnsEmpty() {
    String collectionName = "reread-error-collection";
    registerViaGcScan(markedLease(collectionName, Instant.now().minusSeconds(7200)));
    when(this.mockFindIterable.first()).thenThrow(new RuntimeException("read failed"));

    assertThat(this.leaseManager.resolveStaleMatViewNamespace(collectionName, 1L, Instant.now()))
        .isEmpty();
  }

  // ==================== Helper Methods ====================

  /**
   * Creates a MaterializedViewIndexGeneration and registers its generationId in the catalog so that
   * DynamicLeaderLeaseManager.getLeaseKey (which uses catalog) works.
   */
  // ====== recording the definition version on lease reuse (revert to a previous definition) ====

  @Test
  public void tryAcquireLeadership_reusedLeaseMissingDefinitionVersion_recordsIt() {
    MaterializedViewIndexDefinitionGeneration defGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(new ObjectId(), 4);
    MaterializedViewIndexGeneration gen = mockMatViewIndexGeneration(defGen);
    addCatalogMetadataForGeneration(gen.getGenerationId());
    this.leaseManager.add(gen);
    String leaseKey = getLeaseKeyFromCatalog(gen.getGenerationId());
    // Reused lease in the DB predates the revert: expired, still on version 2. The poll
    // overwrites the in-memory record from add() with this copy.
    Lease dbCopy =
        leaseWithVersions(
            leaseKey,
            "2",
            Map.of(
                "2",
                new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN)));
    setupFindLeaseFromDatabase(dbCopy);
    setupSuccessfulLeaseUpdate();
    this.leaseManager.pollFollowerStatuses();

    boolean acquired = this.leaseManager.tryAcquireLeadership(gen.getGenerationId());

    assertThat(acquired).isTrue();
    Lease cached = this.leaseManager.getLeases().get(leaseKey);
    assertThat(cached.latestIndexDefinitionVersion()).isEqualTo("4");
    assertThat(cached.indexDefinitionVersionStatusMap()).containsKey("4");
  }

  @Test
  public void tryAcquireLeadership_generationDroppedConcurrently_doesNotAcquire() {
    MaterializedViewIndexDefinitionGeneration defGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(new ObjectId(), 4);
    MaterializedViewIndexGeneration gen = mockMatViewIndexGeneration(defGen);
    addCatalogMetadataForGeneration(gen.getGenerationId());
    this.leaseManager.add(gen);
    String leaseKey = getLeaseKeyFromCatalog(gen.getGenerationId());
    this.leaseManager.putLease(
        leaseKey,
        leaseWithVersions(
            leaseKey,
            "2",
            Map.of(
                "2",
                new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN))));
    this.leaseManager.drop(gen.getGenerationId());

    boolean acquired = this.leaseManager.tryAcquireLeadership(gen.getGenerationId());

    assertThat(acquired).isFalse();
    verify(this.mockCollection, never()).replaceOne(any(), any(BsonDocument.class));
  }

  private Lease leaseWithVersions(
      String leaseKey, String latestVersion, Map<String, Lease.IndexDefinitionVersionStatus> map) {
    return new Lease(
        leaseKey,
        Lease.SCHEMA_VERSION,
        "fa41efe9-dd13-4976-a6ce-009682ec4257",
        "collection-name",
        OTHER_HOSTNAME,
        Instant.now().minusSeconds(60),
        5L,
        "",
        latestVersion,
        map,
        new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), leaseKey),
        null,
        Lease.CleanupState.NOT_ELIGIBLE,
        Lease.FIRST_EPOCH);
  }

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
            current.cleanupState(),
            current.epoch());
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
        Lease.CleanupState.NOT_ELIGIBLE,
        Lease.FIRST_EPOCH);
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
        Lease.CleanupState.NOT_ELIGIBLE,
        Lease.FIRST_EPOCH);
  }

  /** A lease with an explicit latest version and version-status map. */
  private Lease createLeaseWithVersionStatuses(
      MaterializedViewGenerationId generationId,
      String owner,
      String latestIndexDefinitionVersion,
      Map<String, Lease.IndexDefinitionVersionStatus> versionStatusMap,
      String commitInfo) {
    return new Lease(
        getLeaseKeyFromCatalog(generationId),
        Lease.SCHEMA_VERSION,
        "fa41efe9-dd13-4976-a6ce-009682ec4257",
        "collection-name",
        owner,
        Instant.now().plusSeconds(60),
        1L,
        commitInfo,
        latestIndexDefinitionVersion,
        versionStatusMap,
        new MaterializedViewCollectionMetadata(VERSION_ZERO, UUID.randomUUID(), "collection-name"),
        null,
        Lease.CleanupState.NOT_ELIGIBLE,
        Lease.FIRST_EPOCH);
  }

  /** Commit info carrying a change-stream checkpoint at {@code opTime}. */
  private static String checkpointCommitInfo(BsonTimestamp opTime) {
    return IndexCommitUserData.createChangeStreamResume(
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "collection"), ChangeStreamUtils.resumeToken(opTime)),
            IndexFormatVersion.CURRENT.versionNumber)
        .toEncodedData()
        .asString();
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
        Lease.CleanupState.NOT_ELIGIBLE,
        Lease.FIRST_EPOCH);
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
    when(updateResult.getModifiedCount()).thenReturn(1L);
    when(this.mockCollection.replaceOne(any(), any(BsonDocument.class))).thenReturn(updateResult);
    // Lease renewal is a field-scoped updateOne; acquisition/checkpoint/status are replaceOne.
    when(this.mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);
  }

  private void setupFailedLeaseUpdate() {
    UpdateResult updateResult = mock(UpdateResult.class);
    when(updateResult.getMatchedCount()).thenReturn(0L);
    when(updateResult.getModifiedCount()).thenReturn(0L);
    when(this.mockCollection.replaceOne(any(), any(BsonDocument.class))).thenReturn(updateResult);
    when(this.mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);
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
        Lease.CleanupState.NOT_ELIGIBLE,
        Lease.FIRST_EPOCH);
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
