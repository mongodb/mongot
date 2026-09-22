package com.xgen.mongot.embedding.mongodb.leasing;

import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

/** Unit tests for {@link Lease}. */
@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      LeaseTest.TestDeserialization.class,
      LeaseTest.TestSerialization.class,
      LeaseTest.TestLease.class,
    })
public class LeaseTest {

  private static final String LEASE_ID = "test-lease-id";
  private static final String COLLECTION_UUID = "550e8400-e29b-41d4-a716-446655440000";
  private static final String COLLECTION_NAME = "test-collection";
  private static final String LEASE_OWNER = "localhost";
  private static final String RESOURCES_PATH = "src/test/unit/resources/embedding/mongodb/leasing";

  @RunWith(Parameterized.class)
  public static class TestDeserialization {

    private static final String SUITE_NAME = "lease-deserialization";
    private static final BsonDeserializationTestSuite<Lease> TEST_SUITE =
        fromDocument(RESOURCES_PATH, SUITE_NAME, Lease::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<Lease> testSpec;

    public TestDeserialization(BsonDeserializationTestSuite.TestSpecWrapper<Lease> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<Lease>> data() {
      return TEST_SUITE.withExamples(
          leaseWithMaterializedViewMetadata(), leaseWithEmptyMaterializedViewMetadata());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Lease>
        leaseWithMaterializedViewMetadata() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "leaseWithMaterializedViewMetadata",
          new Lease(
              LEASE_ID,
              1L,
              COLLECTION_UUID,
              COLLECTION_NAME,
              LEASE_OWNER,
              Instant.ofEpochMilli(1733446635661L),
              Lease.FIRST_LEASE_VERSION,
              EncodedUserData.EMPTY.asString(),
              "1",
              Map.of(
                  "1",
                  new Lease.IndexDefinitionVersionStatus(
                      false, IndexStatus.StatusCode.INITIAL_SYNC)),
              new MaterializedViewCollectionMetadata(
                  new MaterializedViewSchemaMetadata(
                      1L, Map.of(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title"))),
                  UUID.fromString(COLLECTION_UUID),
                  COLLECTION_NAME),
              null,
              Lease.CleanupState.NOT_ELIGIBLE,
              // No epoch in the fixture (a legacy-shaped document): defaults to leaseVersion.
              Lease.FIRST_LEASE_VERSION));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Lease>
        leaseWithEmptyMaterializedViewMetadata() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "leaseWithEmptyMaterializedViewMetadata",
          new Lease(
              LEASE_ID,
              1L,
              COLLECTION_UUID,
              COLLECTION_NAME,
              LEASE_OWNER,
              Instant.ofEpochMilli(1733446635661L),
              Lease.FIRST_LEASE_VERSION,
              EncodedUserData.EMPTY.asString(),
              "1",
              Map.of(
                  "1",
                  new Lease.IndexDefinitionVersionStatus(
                      false, IndexStatus.StatusCode.INITIAL_SYNC)),
              new MaterializedViewCollectionMetadata(VERSION_ZERO, new UUID(0, 0), LEASE_ID),
              null,
              Lease.CleanupState.NOT_ELIGIBLE,
              // No epoch in the fixture (a legacy-shaped document): defaults to leaseVersion.
              Lease.FIRST_LEASE_VERSION));
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {

    private static final String SUITE_NAME = "lease-serialization";
    private static final BsonSerializationTestSuite<Lease> TEST_SUITE =
        fromEncodable(RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<Lease> testSpec;

    public TestSerialization(BsonSerializationTestSuite.TestSpec<Lease> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<Lease>> data() {
      return Arrays.asList(
          leaseWithMaterializedViewMetadata(), leaseWithEmptyMaterializedViewMetadata());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<Lease> leaseWithMaterializedViewMetadata() {
      var customMetadata =
          new MaterializedViewCollectionMetadata(
              new MaterializedViewSchemaMetadata(
                  1L, Map.of(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title"))),
              UUID.fromString(COLLECTION_UUID),
              COLLECTION_NAME);
      var lease =
          new Lease(
              LEASE_ID,
              1L,
              COLLECTION_UUID,
              COLLECTION_NAME,
              LEASE_OWNER,
              Instant.parse("2024-12-06T00:57:15.661Z"), // Fixed timestamp for reproducibility
              Lease.FIRST_LEASE_VERSION,
              EncodedUserData.EMPTY.asString(),
              "1",
              Map.of(
                  "1",
                  new Lease.IndexDefinitionVersionStatus(
                      false, IndexStatus.StatusCode.INITIAL_SYNC)),
              customMetadata,
              null,
              Lease.CleanupState.NOT_ELIGIBLE,
              Lease.FIRST_EPOCH);
      return BsonSerializationTestSuite.TestSpec.create("leaseWithMaterializedViewMetadata", lease);
    }

    private static BsonSerializationTestSuite.TestSpec<Lease>
        leaseWithEmptyMaterializedViewMetadata() {
      var lease =
          new Lease(
              LEASE_ID,
              1L,
              COLLECTION_UUID,
              COLLECTION_NAME,
              LEASE_OWNER,
              Instant.parse("2024-12-06T00:57:15.661Z"), // Fixed timestamp for reproducibility
              Lease.FIRST_LEASE_VERSION,
              EncodedUserData.EMPTY.asString(),
              "1",
              Map.of(
                  "1",
                  new Lease.IndexDefinitionVersionStatus(
                      false, IndexStatus.StatusCode.INITIAL_SYNC)),
              new MaterializedViewCollectionMetadata(
                  VERSION_ZERO, UUID.fromString(COLLECTION_UUID), COLLECTION_NAME),
              null,
              Lease.CleanupState.NOT_ELIGIBLE,
              Lease.FIRST_EPOCH);
      return BsonSerializationTestSuite.TestSpec.create(
          "leaseWithEmptyMaterializedViewMetadata", lease);
    }
  }

  public static class TestLease {
    @Test
    public void testExtractHighWaterMarkFromSteadyState() {
      // Set up a lease with steady state commitInfo containing a resume token
      var opTime = new BsonTimestamp(9876543210L);
      var resumeToken = ChangeStreamUtils.resumeToken(opTime);
      var indexCommitUserData =
          IndexCommitUserData.createChangeStreamResume(
              ChangeStreamResumeInfo.create(new MongoNamespace("test", "collection"), resumeToken),
              IndexFormatVersion.CURRENT.versionNumber);

      var lease = createLeaseWithCommitInfo(indexCommitUserData.toEncodedData().asString());

      var extractedHighWaterMark = lease.extractHighWaterMark();

      assertTrue(extractedHighWaterMark.isPresent());
      assertEquals(opTime, extractedHighWaterMark.get());
    }

    @Test
    public void testExtractHighWaterMarkFromEmptyCommitInfo() {
      // Lease with empty commitInfo (V1 hasn't started yet)
      var lease = createLeaseWithCommitInfo(EncodedUserData.EMPTY.asString());

      var extractedHighWaterMark = lease.extractHighWaterMark();

      assertFalse(extractedHighWaterMark.isPresent());
    }

    @Test
    public void testIndexIdMatchesNameFormatLayout() {
      // v1+ MV collection name: "<indexId>-<hash>-<nameFormatVersion>-<autoEmbeddingVersion>".
      assertEquals(
          "507f1f77bcf86cd799439011",
          createLeaseWithId("507f1f77bcf86cd799439011-abc123-1-0").indexId());
      // A future format version keeping the layout parses without a code change.
      assertEquals(
          "507f1f77bcf86cd799439011",
          createLeaseWithId("507f1f77bcf86cd799439011-abc123-2-0").indexId());
      // v0 MV collection name is just the indexId, with no delimiter.
      assertEquals(
          "507f1f77bcf86cd799439011", createLeaseWithId("507f1f77bcf86cd799439011").indexId());
    }

    @Test
    public void testIndexIdUnknownNameLayoutKeptWhole() {
      // An unrecognized layout returns the full id: the lease groups only with itself, so the GC
      // current-version safeguard protects it rather than comparing it across indexes.
      assertEquals(
          "507f1f77bcf86cd799439011-abc123",
          createLeaseWithId("507f1f77bcf86cd799439011-abc123").indexId());
    }

    @Test
    public void testWithNewIndexDefinitionVersionPreservesCommitInfoWhenReuseTrue() {
      // Set up a lease with steady state commitInfo (change stream resume)
      var opTime = new BsonTimestamp(9876543210L);
      var resumeToken = ChangeStreamUtils.resumeToken(opTime);
      var indexCommitUserData =
          IndexCommitUserData.createChangeStreamResume(
              ChangeStreamResumeInfo.create(new MongoNamespace("test", "collection"), resumeToken),
              IndexFormatVersion.CURRENT.versionNumber);

      var lease = createLeaseWithCommitInfo(indexCommitUserData.toEncodedData().asString());

      // Create V2 lease with skipInitialSync=true - should preserve original commitInfo
      var v2Lease = lease.withNewIndexDefinitionVersion("2", IndexStatus.initialSync(), true);

      // Verify commitInfo is preserved unchanged
      assertEquals(lease.commitInfo(), v2Lease.commitInfo());
    }

    @Test
    public void testWithNewIndexDefinitionVersionResetsToInitialSyncWhenReuseFalse() {
      // Set up a lease with steady state commitInfo (change stream resume)
      var opTime = new BsonTimestamp(9876543210L);
      var resumeToken = ChangeStreamUtils.resumeToken(opTime);
      var indexCommitUserData =
          IndexCommitUserData.createChangeStreamResume(
              ChangeStreamResumeInfo.create(new MongoNamespace("test", "collection"), resumeToken),
              IndexFormatVersion.CURRENT.versionNumber);

      var lease = createLeaseWithCommitInfo(indexCommitUserData.toEncodedData().asString());

      // Create V2 lease with skipInitialSync=false - should reset to initial-sync from high water
      // mark
      var v2Lease = lease.withNewIndexDefinitionVersion("2", IndexStatus.initialSync(), false);

      // Parse V2's commitInfo and verify it contains initial-sync resume from high water mark
      var encodedUserData = EncodedUserData.fromString(v2Lease.commitInfo());
      var userData = IndexCommitUserData.fromEncodedData(encodedUserData, Optional.empty());

      assertTrue(userData.getInitialSyncResumeInfo().isPresent());
      assertEquals(opTime, userData.getInitialSyncResumeInfo().get().getResumeOperationTime());
      // Verify scan position is reset to MIN_KEY
      assertEquals(BsonUtils.MIN_KEY, userData.getInitialSyncResumeInfo().get().getResumeToken());
    }

    @Test
    public void testWithNewIndexDefinitionVersionWithEmptyCommitInfo() {
      // Lease with empty commitInfo (V1 hasn't started yet)
      var lease = createLeaseWithCommitInfo(EncodedUserData.EMPTY.asString());

      // Create V2 lease
      var v2Lease = lease.withNewIndexDefinitionVersion("2", IndexStatus.initialSync(), true);

      // V2's commitInfo should also be empty since there's no highWaterMark to preserve
      assertEquals(EncodedUserData.EMPTY.asString(), v2Lease.commitInfo());
    }

    @Test
    public void testWithNewIndexDefinitionVersion_skipInitialSync_preservesIsQueryable() {
      // Lease where V1 is queryable (index was in steady state).
      var lease =
          new Lease(
              LEASE_ID,
              Lease.FIRST_LEASE_VERSION,
              COLLECTION_UUID,
              COLLECTION_NAME,
              LEASE_OWNER,
              Instant.now(),
              Lease.FIRST_LEASE_VERSION,
              EncodedUserData.EMPTY.asString(),
              "1",
              Map.of(
                  "1", new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY)),
              new MaterializedViewCollectionMetadata(
                  VERSION_ZERO, UUID.fromString(COLLECTION_UUID), COLLECTION_NAME),
              null,
              Lease.CleanupState.NOT_ELIGIBLE,
              Lease.FIRST_EPOCH);

      // skipInitialSync=true (Lucene-only change): isQueryable should be preserved from V1.
      var v2SkipSync = lease.withNewIndexDefinitionVersion("2", IndexStatus.initialSync(), true);
      assertTrue(
          "isQueryable should be preserved from latest version when skipInitialSync=true",
          v2SkipSync.indexDefinitionVersionStatusMap().get("2").isQueryable());

      // skipInitialSync=false (schema change): isQueryable should reset to false.
      var v2NoSkip = lease.withNewIndexDefinitionVersion("2", IndexStatus.initialSync(), false);
      assertFalse(
          "isQueryable should be false when skipInitialSync=false",
          v2NoSkip.indexDefinitionVersionStatusMap().get("2").isQueryable());
    }

    /**
     * Rollback safety (CLOUDP-401373): a newer binary may add fields to {@code
     * IndexDefinitionVersionStatus} entries. An older binary loading those documents must skip the
     * unknown fields instead of throwing, otherwise all lease loading for the affected index
     * breaks.
     */
    @Test
    public void testFromBson_tolerantOfUnknownFieldsInIndexDefinitionVersionStatusMap()
        throws Exception {
      var statusWithUnknownField =
          new BsonDocument()
              .append("isQueryable", new BsonBoolean(true))
              .append("indexStatusCode", new BsonString("STEADY"))
              .append("futureFieldFromNewerBinary", new BsonString("ignored"));

      var leaseDoc =
          new BsonDocument()
              .append("_id", new BsonString(LEASE_ID))
              .append("schemaVersion", new BsonInt64(1L))
              .append("collectionUuid", new BsonString(COLLECTION_UUID))
              .append("collectionName", new BsonString(COLLECTION_NAME))
              .append("leaseOwner", new BsonString(LEASE_OWNER))
              .append("leaseExpiration", new BsonDateTime(1733446635661L))
              .append("leaseVersion", new BsonInt64(Lease.FIRST_LEASE_VERSION))
              .append("commitInfo", new BsonString(EncodedUserData.EMPTY.asString()))
              .append("latestIndexDefinitionVersion", new BsonString("1"))
              .append(
                  "indexDefinitionVersionStatusMap", new BsonDocument("1", statusWithUnknownField));

      Lease lease = Lease.fromBson(leaseDoc);

      var status = lease.indexDefinitionVersionStatusMap().get("1");
      assertTrue(status.isQueryable());
      assertEquals(IndexStatus.StatusCode.STEADY, status.indexStatusCode());
    }

    /**
     * Rollback safety (CLOUDP-401373): a newer binary may introduce a new {@code
     * IndexStatus.StatusCode} value. An older binary loading those documents must fall back to
     * {@code UNKNOWN} instead of throwing, otherwise all lease loading for the affected index
     * breaks.
     */
    @Test
    public void testFromBson_tolerantOfUnknownStatusCodeEnumValue() throws Exception {
      var statusWithUnknownEnum =
          new BsonDocument()
              .append("isQueryable", new BsonBoolean(true))
              .append("indexStatusCode", new BsonString("FUTURE_STATUS_FROM_NEWER_BINARY"));

      var leaseDoc =
          new BsonDocument()
              .append("_id", new BsonString(LEASE_ID))
              .append("schemaVersion", new BsonInt64(1L))
              .append("collectionUuid", new BsonString(COLLECTION_UUID))
              .append("collectionName", new BsonString(COLLECTION_NAME))
              .append("leaseOwner", new BsonString(LEASE_OWNER))
              .append("leaseExpiration", new BsonDateTime(1733446635661L))
              .append("leaseVersion", new BsonInt64(Lease.FIRST_LEASE_VERSION))
              .append("commitInfo", new BsonString(EncodedUserData.EMPTY.asString()))
              .append("latestIndexDefinitionVersion", new BsonString("1"))
              .append(
                  "indexDefinitionVersionStatusMap", new BsonDocument("1", statusWithUnknownEnum));

      Lease lease = Lease.fromBson(leaseDoc);

      var status = lease.indexDefinitionVersionStatusMap().get("1");
      assertTrue(status.isQueryable());
      assertEquals(IndexStatus.StatusCode.UNKNOWN, status.indexStatusCode());
    }

    @Test
    public void testSteadyAsOfOplogPosition_serialization() {
      // Field present - serialized to BSON correctly
      BsonTimestamp position = new BsonTimestamp(1000, 1);
      Lease leaseWithPosition = createLeaseWithSteadyPosition(position);

      var bson = leaseWithPosition.toBson();
      assertTrue(bson.containsKey("steadyAsOfOplogPosition"));
      assertEquals(position, bson.getTimestamp("steadyAsOfOplogPosition"));

      // Field absent - omitted from BSON
      Lease leaseWithoutPosition = createLeaseWithSteadyPosition(null);

      var bsonEmpty = leaseWithoutPosition.toBson();
      assertFalse(bsonEmpty.containsKey("steadyAsOfOplogPosition"));
    }

    @Test
    public void testWithUpdatedStatus_steadyAsOfOplogPositionLifecycle() {
      BsonTimestamp firstSteadyPosition = new BsonTimestamp(1000, 1);
      BsonTimestamp laterPosition = new BsonTimestamp(2000, 1);

      // Start with no position
      Lease initialLease = createLeaseWithSteadyPosition(null);
      assertFalse(initialLease.getSteadyAsOfOplogPosition().isPresent());

      // Non-STEADY transition doesn't set position
      Lease afterInitialSync =
          initialLease.withUpdatedStatus(IndexStatus.initialSync(), 1L, firstSteadyPosition);
      assertFalse(afterInitialSync.getSteadyAsOfOplogPosition().isPresent());

      // First STEADY transition sets position
      Lease afterFirstSteady =
          afterInitialSync.withUpdatedStatus(IndexStatus.steady(), 1L, firstSteadyPosition);
      assertTrue(afterFirstSteady.getSteadyAsOfOplogPosition().isPresent());
      assertEquals(firstSteadyPosition, afterFirstSteady.getSteadyAsOfOplogPosition().get());

      // Subsequent STEADY transition preserves original position
      Lease afterSecondSteady =
          afterFirstSteady.withUpdatedStatus(IndexStatus.steady(), 1L, laterPosition);
      assertTrue(afterSecondSteady.getSteadyAsOfOplogPosition().isPresent());
      assertEquals(firstSteadyPosition, afterSecondSteady.getSteadyAsOfOplogPosition().get());
    }

    @Test
    public void testSteadyAsOfOplogPositionLifecycle() {
      // newLease starts with null steadyAsOfOplogPosition
      var newLease =
          Lease.newLease(
              LEASE_ID,
              UUID.fromString(COLLECTION_UUID),
              COLLECTION_NAME,
              LEASE_OWNER,
              "1",
              IndexStatus.initialSync(),
              new MaterializedViewCollectionMetadata(
                  VERSION_ZERO, UUID.fromString(COLLECTION_UUID), COLLECTION_NAME));
      assertFalse(
          "New lease should have empty steadyAsOfOplogPosition",
          newLease.getSteadyAsOfOplogPosition().isPresent());

      // withUpdatedCheckpoint preserves steadyAsOfOplogPosition
      var steadyPosition = new BsonTimestamp(1234567890L);
      var leaseWithSteadyPosition = createLeaseWithSteadyPosition(steadyPosition);
      var afterCheckpoint = leaseWithSteadyPosition.withUpdatedCheckpoint(EncodedUserData.EMPTY);
      assertTrue(
          "withUpdatedCheckpoint should preserve steadyAsOfOplogPosition",
          afterCheckpoint.getSteadyAsOfOplogPosition().isPresent());
      assertEquals(steadyPosition, afterCheckpoint.getSteadyAsOfOplogPosition().get());

      // withNewIndexDefinitionVersion(skipInitialSync=true) preserves steadyAsOfOplogPosition
      var afterNewVersionReuse =
          leaseWithSteadyPosition.withNewIndexDefinitionVersion(
              "2", IndexStatus.initialSync(), true);
      assertTrue(
          "withNewIndexDefinitionVersion(skipInitialSync=true) should preserve "
              + "steadyAsOfOplogPosition",
          afterNewVersionReuse.getSteadyAsOfOplogPosition().isPresent());
      assertEquals(steadyPosition, afterNewVersionReuse.getSteadyAsOfOplogPosition().get());

      // withNewIndexDefinitionVersion(skipInitialSync=false) resets steadyAsOfOplogPosition to null
      var afterNewVersionResync =
          leaseWithSteadyPosition.withNewIndexDefinitionVersion(
              "3", IndexStatus.initialSync(), false);
      assertFalse(
          "withNewIndexDefinitionVersion(skipInitialSync=false) should reset "
              + "steadyAsOfOplogPosition",
          afterNewVersionResync.getSteadyAsOfOplogPosition().isPresent());
    }

    @Test
    public void testInitialLease_createsLeaseWithEmptyOwnerAndCorrectFields() {
      UUID collectionUuid = UUID.fromString(COLLECTION_UUID);
      MaterializedViewCollectionMetadata mvMetadata =
          new MaterializedViewCollectionMetadata(
              new MaterializedViewSchemaMetadata(
                  1L, Map.of(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title"))),
              collectionUuid,
              COLLECTION_NAME);

      Lease lease =
          Lease.initialLease(
              LEASE_ID, collectionUuid, COLLECTION_NAME, "0", IndexStatus.unknown(), mvMetadata);

      assertEquals(LEASE_ID, lease.id());
      // initialLease sets owner to empty string so any instance can acquire it
      assertEquals("", lease.leaseOwner());
      assertEquals(Lease.FIRST_LEASE_VERSION, lease.leaseVersion());
      assertEquals(EncodedUserData.EMPTY.asString(), lease.commitInfo());
      assertEquals("0", lease.latestIndexDefinitionVersion());
      assertEquals(collectionUuid.toString(), lease.collectionUuid());
      assertEquals(COLLECTION_NAME, lease.collectionName());
      // Verify materialized view metadata is preserved
      assertEquals(mvMetadata, lease.materializedViewCollectionMetadata());
      assertEquals(
          1L,
          lease
              .materializedViewCollectionMetadata()
              .schemaMetadata()
              .materializedViewSchemaVersion());
      // steadyAsOfOplogPosition should be null
      assertFalse(lease.getSteadyAsOfOplogPosition().isPresent());
      // Index definition version status map should contain the version with UNKNOWN status
      assertTrue(lease.indexDefinitionVersionStatusMap().containsKey("0"));
      assertEquals(
          IndexStatus.StatusCode.UNKNOWN,
          lease.indexDefinitionVersionStatusMap().get("0").indexStatusCode());
    }

    @Test
    public void testWithResolvedMatViewUuidForNewSchemaVersion_updatesOnlyMatViewUuid() {
      UUID originalMatViewUuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
      UUID newMatViewUuid = UUID.fromString("660e8400-e29b-41d4-a716-446655440001");

      MaterializedViewCollectionMetadata originalMvMetadata =
          new MaterializedViewCollectionMetadata(
              new MaterializedViewSchemaMetadata(
                  1L, Map.of(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title"))),
              originalMatViewUuid,
              "mv-collection");

      Lease originalLease =
          new Lease(
              LEASE_ID,
              Lease.FIRST_LEASE_VERSION,
              COLLECTION_UUID,
              COLLECTION_NAME,
              LEASE_OWNER,
              Instant.now(),
              5L,
              "some-commit-info",
              "2",
              Map.of(
                  "2", new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY)),
              originalMvMetadata,
              new BsonTimestamp(1234567890L),
              Lease.CleanupState.NOT_ELIGIBLE,
              3L);

      Lease updatedLease = originalLease.withResolvedMatViewUuid(newMatViewUuid);

      // Mat view UUID should be updated
      assertEquals(
          newMatViewUuid, updatedLease.materializedViewCollectionMetadata().collectionUuid());
      // Schema metadata and collection name should be preserved
      assertEquals(
          1L,
          updatedLease
              .materializedViewCollectionMetadata()
              .schemaMetadata()
              .materializedViewSchemaVersion());
      assertEquals(
          "mv-collection", updatedLease.materializedViewCollectionMetadata().collectionName());
      assertEquals(
          Map.of(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title")),
          updatedLease
              .materializedViewCollectionMetadata()
              .schemaMetadata()
              .autoEmbeddingFieldsMapping());
      // All other lease fields should remain unchanged
      assertEquals(LEASE_ID, updatedLease.id());
      assertEquals(LEASE_OWNER, updatedLease.leaseOwner());
      assertEquals(5L, updatedLease.leaseVersion());
      assertEquals("some-commit-info", updatedLease.commitInfo());
      assertEquals("2", updatedLease.latestIndexDefinitionVersion());
      assertEquals(COLLECTION_UUID, updatedLease.collectionUuid());
      assertEquals(COLLECTION_NAME, updatedLease.collectionName());
      assertTrue(updatedLease.getSteadyAsOfOplogPosition().isPresent());
      assertEquals(new BsonTimestamp(1234567890L), updatedLease.getSteadyAsOfOplogPosition().get());
      // withResolvedMatViewUuid is a metadata-only update; it must not touch epoch.
      assertEquals(3L, updatedLease.epoch());
    }

    @Test
    public void testWithUpdatedStatus_isQueryableIsOneWayRatchet() {
      // Start with a lease at INITIAL_SYNC (not queryable)
      Lease lease = createLeaseWithSteadyPosition(null);
      assertFalse(lease.indexDefinitionVersionStatusMap().get("1").isQueryable());

      // Transition to STEADY — isQueryable becomes true
      Lease afterSteady = lease.withUpdatedStatus(IndexStatus.steady(), 1L, null);
      assertTrue(
          "isQueryable should be true after reaching STEADY",
          afterSteady.indexDefinitionVersionStatusMap().get("1").isQueryable());

      // Fall off the oplog → INITIAL_SYNC — isQueryable must remain true
      Lease afterResync = afterSteady.withUpdatedStatus(IndexStatus.initialSync(), 1L, null);
      assertTrue(
          "isQueryable should remain true after falling off the oplog (STEADY → INITIAL_SYNC)",
          afterResync.indexDefinitionVersionStatusMap().get("1").isQueryable());
    }

    @Test
    public void testWithUpdatedStatus_isQueryableStartsFalseForNewVersion() {
      // A version that has never been STEADY starts with isQueryable=false
      Lease lease = createLeaseWithSteadyPosition(null);
      Lease withNewVersion = lease.withUpdatedStatus(IndexStatus.initialSync(), 2L, null);
      assertFalse(
          "New version should start with isQueryable=false",
          withNewVersion.indexDefinitionVersionStatusMap().get("2").isQueryable());
    }

    /**
     * Builds a BSON document that mimics a lease persisted before the {@code cleanupState} field
     * existed in the schema. Used by backwards-compatibility tests.
     */
    private static org.bson.BsonDocument legacyDocWithoutCleanupState() {
      return org.bson.BsonDocument.parse(
          "{\"_id\": \""
              + LEASE_ID
              + "\",\n"
              + "\"schemaVersion\": 1,\n"
              + "\"collectionUuid\": \""
              + COLLECTION_UUID
              + "\",\n"
              + "\"collectionName\": \""
              + COLLECTION_NAME
              + "\",\n"
              + "\"leaseOwner\": \""
              + LEASE_OWNER
              + "\",\n"
              + "\"leaseExpiration\": {\"$date\": \"2024-12-06T00:57:15.661Z\"},\n"
              + "\"leaseVersion\": 1,\n"
              + "\"commitInfo\": \"{}\",\n"
              + "\"latestIndexDefinitionVersion\": \"1\",\n"
              + "\"indexDefinitionVersionStatusMap\": {\n"
              + "  \"1\": {\"isQueryable\": false, \"indexStatusCode\": \"INITIAL_SYNC\"}\n"
              + "}}");
    }

    /**
     * Legacy documents written before the {@code cleanupState} field existed must parse without
     * error and default to {@link Lease.CleanupState#NOT_ELIGIBLE}. This is the load-bearing
     * backwards-compat guarantee for the online migration: existing production leases stay readable
     * after the new code rolls out.
     */
    @Test
    public void testFromBson_missingCleanupState_defaultsToNotEligible() throws Exception {
      Lease parsed = Lease.fromBson(legacyDocWithoutCleanupState());
      assertEquals(Lease.CleanupState.NOT_ELIGIBLE, parsed.cleanupState());
    }

    /**
     * Rollback / rolling-upgrade safety: a newer binary may write a {@link Lease.CleanupState}
     * value this binary doesn't recognize. The field's {@code withFallback(NOT_ELIGIBLE)} must keep
     * parsing from throwing — otherwise the {@code BsonParseException} would break {@code
     * syncLeasesFromMongod} for the index (the failure mode tracked in CLOUDP-401373).
     */
    @Test
    public void testFromBson_unrecognizedCleanupState_fallsBackToNotEligible() throws Exception {
      org.bson.BsonDocument doc = legacyDocWithoutCleanupState();
      doc.put("cleanupState", new org.bson.BsonString("FUTURE_UNKNOWN_STATE"));

      Lease parsed = Lease.fromBson(doc);

      assertEquals(Lease.CleanupState.NOT_ELIGIBLE, parsed.cleanupState());
    }

    /**
     * After a legacy lease is parsed and re-serialized through {@code toBson} (which is what every
     * write path — heartbeat renewal, status update, ownership change — already does), the
     * persisted document carries the new {@code cleanupState} field. This is the "free backfill"
     * mechanism: no separate migration code path is needed because any write naturally upgrades the
     * document.
     */
    @Test
    public void testFromBsonToBson_legacyDocIsUpgradedOnWrite() throws Exception {
      // Parse legacy doc (no cleanupState field).
      Lease parsed = Lease.fromBson(legacyDocWithoutCleanupState());

      // Round-trip through toBson — simulates the next write that this lease will see.
      var upgraded = parsed.toBson();
      assertTrue(
          "Re-serialized legacy lease must include the cleanupState field",
          upgraded.containsKey("cleanupState"));
      assertEquals("NOT_ELIGIBLE", upgraded.getString("cleanupState").getValue());

      // Re-parsing the upgraded BSON yields the same in-memory representation.
      Lease reparsed = Lease.fromBson(upgraded);
      assertEquals(Lease.CleanupState.NOT_ELIGIBLE, reparsed.cleanupState());
    }

    /**
     * A legacy lease (no cleanupState field in the persisted doc) can be marked {@link
     * Lease.CleanupState#ELIGIBLE_FOR_CLEANUP} by {@link Lease#withCleanupState} and the mark
     * survives the round-trip to BSON and back. This is the case the stale-lease scanner relies on:
     * legacy/orphan leases — which never had a {@code cleanupState} field on disk — must still be
     * mark-and-cleanable, because they will never get upgraded by a heartbeat (they have no
     * heartbeater).
     */
    @Test
    public void testFromBsonWithCleanupStateToBson_legacyDocCanBeMarkedEligible() throws Exception {
      Lease parsed = Lease.fromBson(legacyDocWithoutCleanupState());

      // Simulate scanner phase-1: mark eligible for cleanup.
      Lease marked = parsed.withCleanupState(Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);

      // Persist + reload.
      Lease reloaded = Lease.fromBson(marked.toBson());

      assertEquals(Lease.CleanupState.ELIGIBLE_FOR_CLEANUP, reloaded.cleanupState());
      // The rest of the lease must be unchanged — withCleanupState is a cleanupState-only update.
      assertEquals(parsed.id(), reloaded.id());
      assertEquals(parsed.leaseVersion(), reloaded.leaseVersion());
      assertEquals(parsed.leaseOwner(), reloaded.leaseOwner());
      assertEquals(parsed.collectionUuid(), reloaded.collectionUuid());
      assertEquals(parsed.collectionName(), reloaded.collectionName());
    }

    /**
     * Round-tripping a v1 document through fromBson → toBson upgrades it to include the status
     * field. This is the durable-upgrade-by-write path: any write that goes through toBson()
     * (heartbeat renewal, status update, etc.) carries the new field, so older documents naturally
     * converge to the new shape without any explicit backfill code.
     */
    @Test
    public void testToBson_writesCleanupState() {
      Lease lease = createLeaseWithSteadyPosition(null);
      var bson = lease.toBson();
      assertTrue("toBson must include the cleanupState field", bson.containsKey("cleanupState"));
      assertEquals("NOT_ELIGIBLE", bson.getString("cleanupState").getValue());
    }

    /**
     * Active-use builders — any write that extends the lease's expiration (renewal/acquisition,
     * checkpoint, status update, new index-definition version) — clear an {@code
     * ELIGIBLE_FOR_CLEANUP} mark back to {@code NOT_ELIGIBLE} (CLOUDP-411009): a lease being
     * renewed or written to is in active replication use, so it must never remain a stale-cleanup
     * target. A genuinely abandoned lease receives none of these writes, so the scanner's mark
     * survives to the delete pass.
     */
    @Test
    public void testActiveUseBuilders_clearCleanupState() {
      Lease eligibleLease =
          createLeaseWithSteadyPosition(null)
              .withCleanupState(Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);

      assertEquals(
          Lease.CleanupState.NOT_ELIGIBLE,
          eligibleLease.withUpdatedCheckpoint(EncodedUserData.EMPTY).cleanupState());
      assertEquals(
          Lease.CleanupState.NOT_ELIGIBLE, eligibleLease.withRenewedOwnership().cleanupState());
      assertEquals(
          Lease.CleanupState.NOT_ELIGIBLE,
          eligibleLease.withUpdatedStatus(IndexStatus.steady(), 1L, null).cleanupState());
      assertEquals(
          Lease.CleanupState.NOT_ELIGIBLE,
          eligibleLease
              .withNewIndexDefinitionVersion("2", IndexStatus.initialSync(), false)
              .cleanupState());
    }

    /**
     * Builders that do not extend the lease's expiration leave the cleanup mark untouched: {@code
     * withReleasedOwnership} (giving up the lease — it should stay a cleanup target if marked) and
     * {@code withResolvedMatViewUuid} (a metadata-only update that is not a liveness signal).
     */
    @Test
    public void testNonLivenessBuilders_preserveCleanupState() {
      Lease eligibleLease =
          createLeaseWithSteadyPosition(null)
              .withCleanupState(Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);

      assertEquals(
          Lease.CleanupState.ELIGIBLE_FOR_CLEANUP,
          eligibleLease.withReleasedOwnership().cleanupState());
      assertEquals(
          Lease.CleanupState.ELIGIBLE_FOR_CLEANUP,
          eligibleLease.withResolvedMatViewUuid(UUID.randomUUID()).cleanupState());
    }

    /** {@code withCleanupState} changes only the cleanupState field. */
    @Test
    public void testWithCleanupState_changesOnlyCleanupState() {
      Lease lease = createLeaseWithSteadyPosition(null);
      Lease updated = lease.withCleanupState(Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);

      assertEquals(Lease.CleanupState.ELIGIBLE_FOR_CLEANUP, updated.cleanupState());
      // Every other field unchanged — leaseVersion in particular must not bump, because
      // withCleanupState is a cleanupState-only change and bumping would invalidate concurrent OCC
      // updates on other fields.
      assertEquals(lease.id(), updated.id());
      assertEquals(lease.leaseOwner(), updated.leaseOwner());
      assertEquals(lease.leaseExpiration(), updated.leaseExpiration());
      assertEquals(lease.leaseVersion(), updated.leaseVersion());
      assertEquals(lease.commitInfo(), updated.commitInfo());
      assertEquals(lease.latestIndexDefinitionVersion(), updated.latestIndexDefinitionVersion());
      assertEquals(
          lease.indexDefinitionVersionStatusMap(), updated.indexDefinitionVersionStatusMap());
      assertEquals(
          lease.materializedViewCollectionMetadata(), updated.materializedViewCollectionMetadata());
    }

    /**
     * Constructing a Lease with a null status must fail at the call site rather than surface as an
     * NPE deep inside the BSON writer (where toBson() does {@code Optional.of(this.status)}).
     */
    @Test(expected = NullPointerException.class)
    public void testConstructor_nullCleanupState_throws() {
      new Lease(
          LEASE_ID,
          Lease.FIRST_LEASE_VERSION,
          COLLECTION_UUID,
          COLLECTION_NAME,
          LEASE_OWNER,
          Instant.now(),
          Lease.FIRST_LEASE_VERSION,
          "",
          "1",
          Map.of(
              "1",
              new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.INITIAL_SYNC)),
          new MaterializedViewCollectionMetadata(
              VERSION_ZERO, UUID.fromString(COLLECTION_UUID), COLLECTION_NAME),
          null,
          /* status= */ null,
          Lease.FIRST_EPOCH);
    }

    /**
     * A newly created lease (via {@link Lease#initialLease} or {@link Lease#newLease}) starts at
     * {@link Lease#FIRST_EPOCH}.
     */
    @Test
    public void testInitialLeaseAndNewLease_startAtFirstEpoch() {
      var initial =
          Lease.initialLease(
              LEASE_ID,
              UUID.fromString(COLLECTION_UUID),
              COLLECTION_NAME,
              "0",
              IndexStatus.unknown(),
              new MaterializedViewCollectionMetadata(
                  VERSION_ZERO, UUID.fromString(COLLECTION_UUID), COLLECTION_NAME));
      var fresh =
          Lease.newLease(
              LEASE_ID,
              UUID.fromString(COLLECTION_UUID),
              COLLECTION_NAME,
              LEASE_OWNER,
              "0",
              IndexStatus.unknown(),
              new MaterializedViewCollectionMetadata(
                  VERSION_ZERO, UUID.fromString(COLLECTION_UUID), COLLECTION_NAME));

      assertEquals(Lease.FIRST_EPOCH, initial.epoch());
      assertEquals(Lease.FIRST_EPOCH, fresh.epoch());
    }

    /**
     * {@link Lease#withNewTenure} is the only builder that bumps epoch, since it is the only one
     * that represents an ownership transfer to a new tenure. Starting from a "synced" lease (epoch
     * equal to leaseVersion, as every lease is immediately after an acquisition), a tenure change
     * increments both together; renewals in between advance leaseVersion only, but the following
     * tenure change still moves epoch strictly forward — never backward, never stuck.
     */
    @Test
    public void testWithNewTenure_normalAcquisition_epochAndLeaseVersionIncreaseMonotonically() {
      Lease synced = leaseWithVersionAndEpoch(5L, 5L);

      Lease firstAcquisition = synced.withNewTenure("new-owner");
      assertEquals(6L, firstAcquisition.leaseVersion());
      assertEquals(6L, firstAcquisition.epoch());

      // Renewals advance leaseVersion only, pulling it ahead of epoch.
      Lease afterRenewals =
          firstAcquisition.withRenewedOwnership().withRenewedOwnership().withRenewedOwnership();
      assertEquals(9L, afterRenewals.leaseVersion());
      assertEquals(6L, afterRenewals.epoch());

      Lease secondAcquisition = afterRenewals.withNewTenure("another-owner");

      assertEquals(10L, secondAcquisition.leaseVersion());
      assertEquals(10L, secondAcquisition.epoch());
      assertTrue(secondAcquisition.epoch() > firstAcquisition.epoch());
      assertTrue(secondAcquisition.leaseVersion() > afterRenewals.leaseVersion());
    }

    /**
     * A lease whose {@code epoch} has fallen behind {@code leaseVersion} — the ordinary
     * in-between-tenures state, e.g. after renewals with no intervening tenure change — must have
     * its next acquisition rebase {@code epoch} to {@code leaseVersion + 1} rather than just
     * incrementing the stale value, so it never falls behind the {@code leaseVersion}-derived
     * tokens already embedded in MV documents. (A document with {@code epoch} entirely absent is a
     * separate case, healed at read time by {@link Lease#fromBson}; see {@link
     * #testFromBson_missingEpoch_defaultsToLeaseVersion}.)
     */
    @Test
    public void testWithNewTenure_epochBehindLeaseVersion_rebasesAboveLeaseVersion() {
      Lease staleLease = leaseWithVersionAndEpoch(50L, Lease.FIRST_EPOCH);

      Lease acquired = staleLease.withNewTenure("new-owner");

      assertEquals(51L, acquired.leaseVersion());
      assertEquals(51L, acquired.epoch());
    }

    /**
     * Same-host reacquisition of an expired lease is still a new tenure, so {@link
     * Lease#withNewTenure} bumps epoch even when the owner string does not change — and, when
     * leaseVersion has advanced past epoch (e.g. from renewals since the last acquisition), the
     * bump uses the rebased value rather than a plain increment of the stale epoch.
     */
    @Test
    public void testWithNewTenure_sameHostReacquisition_usesRebasedEpoch() {
      Lease lease = leaseWithVersionAndEpoch(20L, 5L);

      Lease reacquired = lease.withNewTenure(lease.leaseOwner());

      assertEquals(lease.leaseOwner(), reacquired.leaseOwner());
      assertEquals(21L, reacquired.leaseVersion());
      assertEquals(21L, reacquired.epoch());
    }

    /**
     * An older writer that predates the epoch field drops it on write — its {@code toBson()} never
     * emits the key — while leaseVersion keeps advancing normally through that writer's own
     * renewals. Unlike a plain re-acquisition, this is healed at read time ({@link
     * Lease#fromBson(BsonDocument)} defaults the missing field to leaseVersion), not by the next
     * acquisition: the healed value is exactly what {@code add()}'s same-host/unexpired-lease fast
     * path resumes leadership with, without ever calling {@link Lease#withNewTenure}. The next
     * ordinary write (e.g. a renewal) durably persists that healed value, and the next real
     * acquisition after that advances both leaseVersion and epoch together.
     */
    @Test
    public void testFromBsonToBson_epochDroppedByOlderWriter_healedAtReadTime() throws Exception {
      Lease beforeDowngrade = leaseWithVersionAndEpoch(30L, 30L);

      // Simulate an older binary taking over: it round-trips every field it knows about but drops
      // epoch entirely, and its own renewals advance leaseVersion further before roll-forward.
      BsonDocument writtenByOlderBinary = beforeDowngrade.toBson();
      writtenByOlderBinary.remove("epoch");
      writtenByOlderBinary.put("leaseVersion", new BsonInt64(45L));

      Lease readByNewerBinary = Lease.fromBson(writtenByOlderBinary);
      assertEquals(45L, readByNewerBinary.leaseVersion());
      // Healed immediately on read — no acquisition needed — so add()'s same-host/unexpired-lease
      // fast path, which never calls withNewTenure, resumes with this same safe value.
      assertEquals(45L, readByNewerBinary.epoch());

      // The next ordinary write (e.g. a heartbeat renewal) persists the healed value durably.
      Lease afterRenewal = readByNewerBinary.withRenewedOwnership();
      assertEquals(46L, afterRenewal.leaseVersion());
      assertEquals(45L, afterRenewal.epoch());
      BsonDocument persisted = afterRenewal.toBson();
      assertEquals(45L, persisted.getInt64("epoch").getValue());

      // The next real acquisition advances both leaseVersion and epoch together.
      Lease nextAcquisition = afterRenewal.withNewTenure("new-owner");
      assertEquals(47L, nextAcquisition.leaseVersion());
      assertEquals(47L, nextAcquisition.epoch());
    }

    /**
     * {@link Lease#withRenewedOwnership} is used for heartbeat renewal, which must not bump epoch —
     * only a new tenure does.
     */
    @Test
    public void testWithRenewedOwnership_preservesEpoch() {
      Lease lease = createLeaseWithSteadyPosition(null).withNewTenure(LEASE_OWNER);

      Lease renewed = lease.withRenewedOwnership();

      assertEquals(lease.epoch(), renewed.epoch());
      assertEquals(lease.leaseOwner(), renewed.leaseOwner());
    }

    /**
     * Builders unrelated to ownership transfer — checkpoint, status, and index-definition-version
     * updates — must preserve epoch.
     */
    @Test
    public void testNonOwnershipBuilders_preserveEpoch() {
      Lease lease = createLeaseWithSteadyPosition(null).withNewTenure(LEASE_OWNER);

      assertEquals(lease.epoch(), lease.withUpdatedCheckpoint(EncodedUserData.EMPTY).epoch());
      assertEquals(lease.epoch(), lease.withUpdatedStatus(IndexStatus.steady(), 1L, null).epoch());
      assertEquals(
          lease.epoch(),
          lease.withNewIndexDefinitionVersion("2", IndexStatus.initialSync(), false).epoch());
      assertEquals(lease.epoch(), lease.withRecordedIndexDefinitionVersion("2").epoch());
      assertEquals(lease.epoch(), lease.withReleasedOwnership().epoch());
      assertEquals(
          lease.epoch(), lease.withCleanupState(Lease.CleanupState.ELIGIBLE_FOR_CLEANUP).epoch());
    }

    /**
     * Legacy documents written before the {@code epoch} field existed must parse without error and
     * default {@code epoch} to that document's own {@code leaseVersion} — a safe fencing floor —
     * not to a fixed constant that could be lower than tenure changes the lease already went
     * through before this field was introduced.
     */
    @Test
    public void testFromBson_missingEpoch_defaultsToLeaseVersion() throws Exception {
      Lease parsed = Lease.fromBson(legacyDocWithoutCleanupState());
      assertEquals(parsed.leaseVersion(), parsed.epoch());
    }

    /**
     * An explicitly present {@code epoch} lower than {@code leaseVersion} is legitimate (renewals
     * advance {@code leaseVersion} between tenure changes) and must be read back exactly as stored
     * — the {@code leaseVersion} fallback applies only when the field is absent.
     */
    @Test
    public void testFromBsonToBson_presentEpochBelowLeaseVersion_isPreservedExactly()
        throws Exception {
      Lease lease = leaseWithVersionAndEpoch(20L, 5L);

      Lease roundTripped = Lease.fromBson(lease.toBson());

      assertEquals(5L, roundTripped.epoch());
      assertEquals(20L, roundTripped.leaseVersion());
    }

    @Test
    public void withRecordedIndexDefinitionVersion_missingVersion_addsEntryAndBumpsLatest() {
      var lease =
          createLeaseWithVersions(
              "2",
              Map.of(
                  "2",
                  new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY)));

      var recorded = lease.withRecordedIndexDefinitionVersion("4");

      assertEquals("4", recorded.latestIndexDefinitionVersion());
      // Not queryable until the leader's status writes report the truth: the change behind the
      // new version may require a resync.
      assertEquals(
          new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN),
          recorded.indexDefinitionVersionStatusMap().get("4"));
      // Everything else is untouched.
      assertEquals(lease.leaseVersion(), recorded.leaseVersion());
      assertEquals(lease.commitInfo(), recorded.commitInfo());
      assertEquals(lease.leaseExpiration(), recorded.leaseExpiration());
      assertEquals(lease.leaseOwner(), recorded.leaseOwner());
      assertEquals(lease.cleanupState(), recorded.cleanupState());
    }

    @Test
    public void withRecordedIndexDefinitionVersion_existingVersionStaleLatest_bumpsLatestOnly() {
      // Production shape of the stuck lease: the status write landed but latest never bumped.
      var lease =
          createLeaseWithVersions(
              "2",
              Map.of(
                  "2",
                  new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN),
                  "4",
                  new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY)));

      var recorded = lease.withRecordedIndexDefinitionVersion("4");

      assertEquals("4", recorded.latestIndexDefinitionVersion());
      assertEquals(
          lease.indexDefinitionVersionStatusMap(), recorded.indexDefinitionVersionStatusMap());
    }

    @Test
    public void withRecordedIndexDefinitionVersion_alreadyConsistent_returnsSameInstance() {
      var lease =
          createLeaseWithVersions(
              "4",
              Map.of(
                  "4",
                  new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY)));

      assertEquals(lease, lease.withRecordedIndexDefinitionVersion("4"));
      assertTrue(lease == lease.withRecordedIndexDefinitionVersion("4"));
    }

    @Test(expected = NullPointerException.class)
    public void withRecordedIndexDefinitionVersion_nullVersion_rejected() {
      createLeaseWithVersions(
              "4",
              Map.of(
                  "4", new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY)))
          .withRecordedIndexDefinitionVersion(null);
    }

    private static Lease createLeaseWithVersions(
        String latestVersion, Map<String, Lease.IndexDefinitionVersionStatus> versionStatusMap) {
      return new Lease(
          LEASE_ID,
          Lease.SCHEMA_VERSION,
          COLLECTION_UUID,
          COLLECTION_NAME,
          LEASE_OWNER,
          Instant.now(),
          7L,
          "some-commit-info",
          latestVersion,
          versionStatusMap,
          new MaterializedViewCollectionMetadata(
              VERSION_ZERO, UUID.fromString(COLLECTION_UUID), COLLECTION_NAME),
          null,
          Lease.CleanupState.NOT_ELIGIBLE,
          Lease.FIRST_EPOCH);
    }

    private static Lease createLeaseWithCommitInfo(String commitInfo) {
      return new Lease(
          LEASE_ID,
          Lease.FIRST_LEASE_VERSION,
          COLLECTION_UUID,
          COLLECTION_NAME,
          LEASE_OWNER,
          Instant.now(),
          Lease.FIRST_LEASE_VERSION,
          commitInfo,
          "1",
          Map.of(
              "1",
              new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.INITIAL_SYNC)),
          new MaterializedViewCollectionMetadata(
              VERSION_ZERO, UUID.fromString(COLLECTION_UUID), COLLECTION_NAME),
          null,
          Lease.CleanupState.NOT_ELIGIBLE,
          Lease.FIRST_EPOCH);
    }

    private static Lease createLeaseWithId(String id) {
      return new Lease(
          id,
          Lease.SCHEMA_VERSION,
          COLLECTION_UUID,
          COLLECTION_NAME,
          LEASE_OWNER,
          Instant.now(),
          Lease.FIRST_LEASE_VERSION,
          "",
          "1",
          Map.of(
              "1",
              new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.INITIAL_SYNC)),
          new MaterializedViewCollectionMetadata(
              VERSION_ZERO, UUID.fromString(COLLECTION_UUID), COLLECTION_NAME),
          null,
          Lease.CleanupState.NOT_ELIGIBLE,
          Lease.FIRST_EPOCH);
    }

    private static Lease createLeaseWithSteadyPosition(@Nullable BsonTimestamp steadyPosition) {
      return new Lease(
          LEASE_ID,
          Lease.FIRST_LEASE_VERSION,
          COLLECTION_UUID,
          COLLECTION_NAME,
          LEASE_OWNER,
          Instant.now(),
          Lease.FIRST_LEASE_VERSION,
          "",
          "1",
          Map.of(
              "1",
              new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.INITIAL_SYNC)),
          new MaterializedViewCollectionMetadata(
              VERSION_ZERO, UUID.fromString(COLLECTION_UUID), COLLECTION_NAME),
          steadyPosition,
          Lease.CleanupState.NOT_ELIGIBLE,
          Lease.FIRST_EPOCH);
    }

    /** A lease with the given leaseVersion and epoch set independently, everything else default. */
    private static Lease leaseWithVersionAndEpoch(long leaseVersion, long epoch) {
      return new Lease(
          LEASE_ID,
          Lease.SCHEMA_VERSION,
          COLLECTION_UUID,
          COLLECTION_NAME,
          LEASE_OWNER,
          Instant.now(),
          leaseVersion,
          "",
          "1",
          Map.of(
              "1",
              new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.INITIAL_SYNC)),
          new MaterializedViewCollectionMetadata(
              VERSION_ZERO, UUID.fromString(COLLECTION_UUID), COLLECTION_NAME),
          null,
          Lease.CleanupState.NOT_ELIGIBLE,
          epoch);
    }
  }
}
