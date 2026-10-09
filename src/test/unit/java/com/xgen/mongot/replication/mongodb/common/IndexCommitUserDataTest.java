package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.status.IndexStatus.Reason;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.UuidRepresentation;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      IndexCommitUserDataTest.DeserializationTest.class,
      IndexCommitUserDataTest.SerializationTest.class,
      IndexCommitUserDataTest.EncodedDataTest.class
    })
public class IndexCommitUserDataTest {

  // No factory builds a record carrying the format version alone, so this one is assembled
  // directly.
  @IndexCommitUserData.TestOnlyShape
  private static final IndexCommitUserData USER_DATA_WITH_INDEX_FORMAT_VERSION_ONLY =
      new IndexCommitUserData(
          Optional.empty(),
          Optional.of(IndexFormatVersion.CURRENT.versionNumber),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());

  private static final IndexCommitUserData USER_DATA_WITH_CHANGE_STREAM_RESUME_INFO =
      IndexCommitUserData.createChangeStreamResume(
          ChangeStreamResumeInfo.create(
              new MongoNamespace("database", "collection"),
              new BsonDocument("_data", new BsonString("resume token"))),
          IndexFormatVersion.CURRENT.versionNumber);

  private static final IndexCommitUserData USER_DATA_WITH_INITIAL_SYNC_RESUME =
      IndexCommitUserData.createInitialSyncResume(
          IndexFormatVersion.CURRENT.versionNumber,
          new BufferlessIdOrderInitialSyncResumeInfo(
              new BsonTimestamp(1234567890L), new BsonString("a")));
  private static final IndexCommitUserData USER_DATA_WITH_NATURAL_ORDER_INITIAL_SYNC_RESUME =
      IndexCommitUserData.createInitialSyncResume(
          IndexFormatVersion.CURRENT.versionNumber,
          new BufferlessNaturalOrderInitialSyncResumeInfo(
              new BsonTimestamp(1234567890L),
              new BsonDocument()
                  .append("$recordId", new BsonInt64(1))
                  .append(
                      "$initialSyncId",
                      new BsonBinary(
                          UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee"),
                          UuidRepresentation.STANDARD)),
              Optional.of("testHost")));
  private static final IndexCommitUserData USER_DATA_WITH_STALE_INFO_NO_RESUME_TOKEN =
      IndexCommitUserData.createStale(
          StaleStateInfo.create(new BsonTimestamp(1234567890L), StaleStatusReason.DOCS_EXCEEDED));
  private static final IndexCommitUserData USER_DATA_WITH_STALE_INFO_WITH_RESUME_TOKEN =
      IndexCommitUserData.createStale(
          StaleStateInfo.create(
              StaleStatusReason.DOCS_EXCEEDED,
              StaleStatusReason.DOCS_EXCEEDED.formatMessage(),
              IndexCommitUserData.createChangeStreamResume(
                  ChangeStreamResumeInfo.create(
                      new MongoNamespace("database", "collection"),
                      ChangeStreamUtils.resumeToken(new BsonTimestamp(1234567890L))),
                  IndexFormatVersion.SIX.versionNumber)));

  private static final IndexCommitUserData USER_DATA_WITH_INDEX_STATE_INFO =
      IndexCommitUserData.createFromIndexStateInfo(
          IndexStateInfo.create(StatusCode.DOES_NOT_EXIST, Reason.COLLECTION_NOT_FOUND));

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "index-commit-user-data-deserialization";
    private static final String SUITE_PATH = "src/test/unit/resources/replication/mongodb/common";

    private static final BsonDeserializationTestSuite<IndexCommitUserData> TEST_SUITE =
        BsonDeserializationTestSuite.fromDocument(
            SUITE_PATH, SUITE_NAME, IndexCommitUserData::fromBson);
    private final BsonDeserializationTestSuite.TestSpecWrapper<IndexCommitUserData> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<IndexCommitUserData> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<IndexCommitUserData>>
        data() {
      return TEST_SUITE.withExamples(
          empty(),
          indexFormatVersionOnly(),
          exceeded(),
          withResumeInfo(),
          withInitialSyncResume(),
          withNaturalOrderInitialSyncResume(),
          withStaleInfoNoResumeToken(),
          withStaleInfoWithResumeToken(),
          withIndexStateInfo());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexCommitUserData> empty() {
      return BsonDeserializationTestSuite.TestSpec.valid("empty", IndexCommitUserData.EMPTY);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexCommitUserData>
        indexFormatVersionOnly() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "index format version only", USER_DATA_WITH_INDEX_FORMAT_VERSION_ONLY);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexCommitUserData> exceeded() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "exceeded", IndexCommitUserData.createExceeded("reason"));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexCommitUserData> withResumeInfo() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with resume info", USER_DATA_WITH_CHANGE_STREAM_RESUME_INFO);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexCommitUserData>
        withInitialSyncResume() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with initial sync resume", USER_DATA_WITH_INITIAL_SYNC_RESUME);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexCommitUserData>
        withNaturalOrderInitialSyncResume() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with natural order initial sync resume",
          USER_DATA_WITH_NATURAL_ORDER_INITIAL_SYNC_RESUME);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexCommitUserData>
        withStaleInfoNoResumeToken() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with stale info without resume token", USER_DATA_WITH_STALE_INFO_NO_RESUME_TOKEN);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexCommitUserData>
        withStaleInfoWithResumeToken() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with stale info with resume token", USER_DATA_WITH_STALE_INFO_WITH_RESUME_TOKEN);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexCommitUserData>
        withIndexStateInfo() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with index state info", USER_DATA_WITH_INDEX_STATE_INFO);
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "index-commit-user-data-serialization";
    private static final String SUITE_PATH = "src/test/unit/resources/replication/mongodb/common";
    private static final BsonSerializationTestSuite<IndexCommitUserData> TEST_SUITE =
        fromEncodable(SUITE_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<IndexCommitUserData> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<IndexCommitUserData> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<IndexCommitUserData>> data() {
      return List.of(
          empty(),
          indexFormatVersionOnly(),
          exceeded(),
          withResumeInfo(),
          withInitialSyncResume(),
          withNaturalOrderInitialSyncResume(),
          withStaleInfo(),
          withIndexStateInfo());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexCommitUserData> empty() {
      return BsonSerializationTestSuite.TestSpec.create("empty", IndexCommitUserData.EMPTY);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexCommitUserData>
        indexFormatVersionOnly() {
      return BsonSerializationTestSuite.TestSpec.create(
          "index format version only", USER_DATA_WITH_INDEX_FORMAT_VERSION_ONLY);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexCommitUserData> exceeded() {
      return BsonSerializationTestSuite.TestSpec.create(
          "exceeded", IndexCommitUserData.createExceeded("reason"));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexCommitUserData> withResumeInfo() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with resume info", USER_DATA_WITH_CHANGE_STREAM_RESUME_INFO);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexCommitUserData>
        withInitialSyncResume() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with initial sync resume", USER_DATA_WITH_INITIAL_SYNC_RESUME);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexCommitUserData>
        withNaturalOrderInitialSyncResume() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with natural order initial sync resume",
          USER_DATA_WITH_NATURAL_ORDER_INITIAL_SYNC_RESUME);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexCommitUserData> withStaleInfo() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with stale info", USER_DATA_WITH_STALE_INFO_NO_RESUME_TOKEN);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexCommitUserData> withIndexStateInfo() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with index state info", USER_DATA_WITH_INDEX_STATE_INFO);
    }
  }

  public static class EncodedDataTest {
    @Test
    public void testEncodeDecodeAsString() {
      IndexCommitUserData data =
          IndexCommitUserData.fromEncodedData(
              EncodedUserData.fromString(
                  USER_DATA_WITH_CHANGE_STREAM_RESUME_INFO.toEncodedString()),
              Optional.empty());
      Assert.assertEquals(USER_DATA_WITH_CHANGE_STREAM_RESUME_INFO, data);
    }

    @Test
    public void testDecodeInvalidData() {
      EncodedUserData invalidStringData = EncodedUserData.fromString("invalid");
      Assert.assertEquals(
          IndexCommitUserData.EMPTY,
          IndexCommitUserData.fromEncodedData(invalidStringData, Optional.empty()));
    }

    @Test
    public void testToEncodedData() {
      EncodedUserData encodedUserData = USER_DATA_WITH_CHANGE_STREAM_RESUME_INFO.toEncodedData();
      IndexCommitUserData data =
          IndexCommitUserData.fromEncodedData(encodedUserData, Optional.empty());

      Assert.assertEquals(USER_DATA_WITH_CHANGE_STREAM_RESUME_INFO, data);
    }

    /**
     * Tests forwards compatibility: StaleStateInfo with unknown fields should still be parseable.
     * This simulates the scenario where new mongot writes a StaleStateInfo with a new field (e.g.,
     * changeStreamResumeInfo), and old mongot (after rollback) tries to read it.
     */
    @Test
    public void testStaleStateInfoForwardsCompatibility_unknownFieldsIgnored() {
      // Simulate a StaleStateInfo serialized by a newer mongot version with an extra field
      BsonDocument staleStateWithUnknownField =
          new BsonDocument()
              .append("lastOptime", new BsonTimestamp(1234567890L))
              .append("reason", new BsonString("DOCS_EXCEEDED"))
              .append("message", new BsonString("Document limit exceeded"))
              .append(
                  "futureUnknownField",
                  new BsonDocument("nested", new BsonString("value"))); // Unknown to old mongot

      BsonDocument userData =
          new BsonDocument().append("staleStateInfo", staleStateWithUnknownField);

      // This should NOT throw an exception - unknown fields should be ignored
      IndexCommitUserData parsed =
          IndexCommitUserData.fromEncodedData(
              EncodedUserData.fromString(userData.toJson()), Optional.empty());

      // Verify that the known fields were correctly parsed
      Assert.assertTrue(parsed.staleStateInfo().isPresent());
      Assert.assertEquals(
          new BsonTimestamp(1234567890L), parsed.staleStateInfo().get().getLastOptime());
      Assert.assertEquals(
          StaleStatusReason.DOCS_EXCEEDED, parsed.staleStateInfo().get().getReason());
    }

    @Test
    public void testAutoEmbeddingMetadataRoundTrips() {
      IndexCommitUserData data =
          USER_DATA_WITH_CHANGE_STREAM_RESUME_INFO.withAutoEmbeddingMetadata(
              new AutoEmbeddingCommitMetadata(true));
      Assert.assertEquals(
          Optional.of(new AutoEmbeddingCommitMetadata(true)), data.autoEmbeddingMetadata());

      IndexCommitUserData decoded =
          IndexCommitUserData.fromEncodedData(data.toEncodedData(), Optional.empty());
      Assert.assertEquals(data, decoded);
      Assert.assertEquals(
          Optional.of(new AutoEmbeddingCommitMetadata(true)), decoded.autoEmbeddingMetadata());
    }

    @Test
    public void testAutoEmbeddingMetadataAbsentByDefault() {
      Assert.assertEquals(Optional.empty(), IndexCommitUserData.EMPTY.autoEmbeddingMetadata());
      Assert.assertEquals(
          Optional.empty(), USER_DATA_WITH_CHANGE_STREAM_RESUME_INFO.autoEmbeddingMetadata());
    }

    /**
     * Rollback compatibility: a binary that predates a top-level field sees that field as unknown.
     * Current code simulates that by reading an arbitrary future top-level field; an older binary
     * would treat autoEmbeddingMetadata the same way.
     */
    @Test
    public void testUnknownTopLevelFieldReturnsEmptyForRollbackCompatibility() {
      BsonDocument userData = USER_DATA_WITH_CHANGE_STREAM_RESUME_INFO.toBson();
      userData.append("futureTopLevelField", new BsonString("value"));

      Assert.assertEquals(
          IndexCommitUserData.EMPTY,
          IndexCommitUserData.fromEncodedData(
              EncodedUserData.fromString(userData.toJson()), Optional.empty()));
    }

    /**
     * Forwards compatibility: an autoEmbeddingMetadata sub-document written by a newer mongot with
     * an extra inner field must still be parseable by a binary that predates that field. The
     * top-level key is known, and the sub-document is parsed with allowUnknownFields().
     */
    @Test
    public void testAutoEmbeddingMetadataForwardsCompatibility_unknownFieldsIgnored() {
      BsonDocument metadataWithUnknownField =
          new BsonDocument()
              .append("wasMatViewReady", BsonBoolean.TRUE)
              .append("futureUnknownField", new BsonString("value"));
      BsonDocument userData =
          new BsonDocument().append("autoEmbeddingMetadata", metadataWithUnknownField);

      IndexCommitUserData parsed =
          IndexCommitUserData.fromEncodedData(
              EncodedUserData.fromString(userData.toJson()), Optional.empty());

      Assert.assertEquals(
          Optional.of(new AutoEmbeddingCommitMetadata(true)), parsed.autoEmbeddingMetadata());
    }

    @Test
    public void testAutoEmbeddingMetadataWithCollectionMetadataRoundTrips() {
      AutoEmbeddingCommitMetadata metadata =
          new AutoEmbeddingCommitMetadata(
              true,
              Optional.of("acme___mdb_internal_search"),
              Optional.of(
                  new MaterializedViewCollectionMetadata(
                      new MaterializedViewSchemaMetadata(
                          1L,
                          Map.of(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title"))),
                      UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee"),
                      "123")));
      IndexCommitUserData data =
          USER_DATA_WITH_CHANGE_STREAM_RESUME_INFO.withAutoEmbeddingMetadata(metadata);

      IndexCommitUserData decoded =
          IndexCommitUserData.fromEncodedData(data.toEncodedData(), Optional.empty());
      Assert.assertEquals(data, decoded);
      Assert.assertEquals(Optional.of(metadata), decoded.autoEmbeddingMetadata());
    }

    @Test
    public void testAutoEmbeddingMetadataWithoutCollectionMetadataIsAbsent() {
      // A derived Lucene last committed by a binary that wrote wasMatViewReady alone (before the
      // reconstruction metadata existed) must still parse, with the metadata reported absent.
      BsonDocument metadataWithoutBundle =
          new BsonDocument().append("wasMatViewReady", BsonBoolean.TRUE);
      BsonDocument userData =
          new BsonDocument().append("autoEmbeddingMetadata", metadataWithoutBundle);

      IndexCommitUserData parsed =
          IndexCommitUserData.fromEncodedData(
              EncodedUserData.fromString(userData.toJson()), Optional.empty());

      Assert.assertEquals(
          Optional.of(new AutoEmbeddingCommitMetadata(true)), parsed.autoEmbeddingMetadata());
      Assert.assertTrue(
          parsed
              .autoEmbeddingMetadata()
              .orElseThrow()
              .materializedViewCollectionMetadata()
              .isEmpty());
    }
  }
}
