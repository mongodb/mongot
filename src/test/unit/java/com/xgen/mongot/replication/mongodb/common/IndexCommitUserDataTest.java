package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.status.IndexStatus.Reason;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonBinary;
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
      IndexCommitUserDataTest.TestDeserialization.class,
      IndexCommitUserDataTest.TestSerialization.class,
      IndexCommitUserDataTest.TestEncodedData.class
    })
public class IndexCommitUserDataTest {

  private static final IndexCommitUserData USER_DATA_WITH_INDEX_FORMAT_VERSION_ONLY =
      new IndexCommitUserData(
          Optional.empty(),
          Optional.of(IndexFormatVersion.CURRENT),
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
          IndexFormatVersion.CURRENT);

  private static final IndexCommitUserData USER_DATA_WITH_INITIAL_SYNC_RESUME =
      IndexCommitUserData.createInitialSyncResume(
          IndexFormatVersion.CURRENT,
          new BufferlessIdOrderInitialSyncResumeInfo(
              new BsonTimestamp(1234567890L), new BsonString("a")));
  private static final IndexCommitUserData USER_DATA_WITH_NATURAL_ORDER_INITIAL_SYNC_RESUME =
      IndexCommitUserData.createInitialSyncResume(
          IndexFormatVersion.CURRENT,
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
  private static final IndexCommitUserData USER_DATA_WITH_STALE_INFO =
      IndexCommitUserData.createStale(
          StaleStateInfo.create(new BsonTimestamp(1234567890L), StaleStatusReason.DOCS_EXCEEDED));

  private static final IndexCommitUserData USER_DATA_WITH_INDEX_STATE_INFO =
      IndexCommitUserData.createFromIndexStateInfo(
          IndexStateInfo.create(StatusCode.DOES_NOT_EXIST, Reason.COLLECTION_NOT_FOUND));

  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "index-commit-user-data-deserialization";
    private static final String SUITE_PATH = "src/test/unit/resources/replication/mongodb/common";

    private static final BsonDeserializationTestSuite<IndexCommitUserData> TEST_SUITE =
        BsonDeserializationTestSuite.fromDocument(
            SUITE_PATH, SUITE_NAME, IndexCommitUserData::fromBson);
    private final BsonDeserializationTestSuite.TestSpecWrapper<IndexCommitUserData> testSpec;

    public TestDeserialization(
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
          withStaleInfo(),
          withStaleInfoUnknownFields(),
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

    private static BsonDeserializationTestSuite.ValidSpec<IndexCommitUserData> withStaleInfo() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with stale info", USER_DATA_WITH_STALE_INFO);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexCommitUserData>
        withStaleInfoUnknownFields() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with stale info with unknown fields", USER_DATA_WITH_STALE_INFO);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexCommitUserData>
        withIndexStateInfo() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with index state info", USER_DATA_WITH_INDEX_STATE_INFO);
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "index-commit-user-data-serialization";
    private static final String SUITE_PATH = "src/test/unit/resources/replication/mongodb/common";
    private static final BsonSerializationTestSuite<IndexCommitUserData> TEST_SUITE =
        fromEncodable(SUITE_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<IndexCommitUserData> testSpec;

    public TestSerialization(BsonSerializationTestSuite.TestSpec<IndexCommitUserData> testSpec) {
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
          "with stale info", USER_DATA_WITH_STALE_INFO);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexCommitUserData> withIndexStateInfo() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with index state info", USER_DATA_WITH_INDEX_STATE_INFO);
    }
  }

  public static class TestEncodedData {
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
  }
}
