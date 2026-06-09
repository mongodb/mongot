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
              null));
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
              null));
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
              null);
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
              null);
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
              IndexFormatVersion.CURRENT);

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
    public void testWithNewIndexDefinitionVersionPreservesCommitInfoWhenReuseTrue() {
      // Set up a lease with steady state commitInfo (change stream resume)
      var opTime = new BsonTimestamp(9876543210L);
      var resumeToken = ChangeStreamUtils.resumeToken(opTime);
      var indexCommitUserData =
          IndexCommitUserData.createChangeStreamResume(
              ChangeStreamResumeInfo.create(new MongoNamespace("test", "collection"), resumeToken),
              IndexFormatVersion.CURRENT);

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
              IndexFormatVersion.CURRENT);

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
                  "1",
                  new Lease.IndexDefinitionVersionStatus(
                      true, IndexStatus.StatusCode.STEADY)),
              new MaterializedViewCollectionMetadata(
                  VERSION_ZERO, UUID.fromString(COLLECTION_UUID), COLLECTION_NAME),
              null);

      // skipInitialSync=true (Lucene-only change): isQueryable should be preserved from V1.
      var v2SkipSync =
          lease.withNewIndexDefinitionVersion("2", IndexStatus.initialSync(), true);
      assertTrue(
          "isQueryable should be preserved from latest version when skipInitialSync=true",
          v2SkipSync.indexDefinitionVersionStatusMap().get("2").isQueryable());

      // skipInitialSync=false (schema change): isQueryable should reset to false.
      var v2NoSkip =
          lease.withNewIndexDefinitionVersion("2", IndexStatus.initialSync(), false);
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
                  "indexDefinitionVersionStatusMap",
                  new BsonDocument("1", statusWithUnknownField));

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
                  "indexDefinitionVersionStatusMap",
                  new BsonDocument("1", statusWithUnknownEnum));

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
              new BsonTimestamp(1234567890L));

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
    }

    @Test
    public void testWithUpdatedStatus_isQueryableIsOneWayRatchet() {
      // Start with a lease at INITIAL_SYNC (not queryable)
      Lease lease = createLeaseWithSteadyPosition(null);
      assertFalse(
          lease.indexDefinitionVersionStatusMap().get("1").isQueryable());

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
          null);
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
          steadyPosition);
    }
  }
}
