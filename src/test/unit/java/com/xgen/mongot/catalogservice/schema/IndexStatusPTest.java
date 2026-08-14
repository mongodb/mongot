package com.xgen.mongot.catalogservice.schema;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import java.util.Arrays;
import java.util.Optional;
import org.bson.BsonTimestamp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {IndexStatusPTest.DeserializationTest.class, IndexStatusPTest.SerializationTest.class})
public class IndexStatusPTest {
  private static final String RESOURCE_PATH = "src/test/unit/resources/catalogservice/schema";

  private static IndexStatusP status(
      IndexStatusP.StatusCode statusCode,
      Optional<String> message,
      Optional<BsonTimestamp> optime,
      Optional<IndexStatusP.Reason> reason) {
    return new IndexStatusP(statusCode, message, optime, reason);
  }

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "index-status-p-deserialization";
    private static final BsonDeserializationTestSuite<IndexStatusP> TEST_SUITE =
        fromDocument(RESOURCE_PATH, SUITE_NAME, IndexStatusP::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<IndexStatusP> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<IndexStatusP> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<IndexStatusP>> data() {
      return TEST_SUITE.withExamples(
          simple(),
          initialSyncWithMessage(),
          stale(),
          recoveringNonTransient(),
          recoveringTransient(),
          failed());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatusP> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          status(
              IndexStatusP.StatusCode.INITIAL_SYNC,
              Optional.empty(),
              Optional.empty(),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatusP> initialSyncWithMessage() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "initial sync with message",
          status(
              IndexStatusP.StatusCode.INITIAL_SYNC,
              Optional.of("foo-bar"),
              Optional.empty(),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatusP> stale() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "stale index",
          status(
              IndexStatusP.StatusCode.STALE,
              Optional.of("foo-bar"),
              Optional.of(new BsonTimestamp(1000)),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatusP> recoveringNonTransient() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "recovering non transient",
          status(
              IndexStatusP.StatusCode.RECOVERING_NON_TRANSIENT,
              Optional.of(
                  "Recovering: The index has encountered an issue and is attempting automatic"
                      + " recovery."),
              Optional.of(new BsonTimestamp(1000)),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatusP> recoveringTransient() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "recovering transient",
          status(
              IndexStatusP.StatusCode.RECOVERING_TRANSIENT,
              Optional.of("recovering"),
              Optional.empty(),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatusP> failed() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "failed",
          status(
              IndexStatusP.StatusCode.FAILED,
              Optional.of("recovering"),
              Optional.empty(),
              Optional.of(IndexStatusP.Reason.INITIAL_SYNC_REPLICATION_FAILED)));
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "index-status-p-serialization";
    private static final BsonSerializationTestSuite<IndexStatusP> TEST_SUITE =
        fromEncodable(RESOURCE_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<IndexStatusP> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<IndexStatusP> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<IndexStatusP>> data() {
      return Arrays.asList(
          simple(),
          initialSyncWithMessage(),
          stale(),
          recoveringNonTransient(),
          recoveringTransient(),
          failed());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatusP> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          status(
              IndexStatusP.StatusCode.INITIAL_SYNC,
              Optional.empty(),
              Optional.empty(),
              Optional.empty()));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatusP> initialSyncWithMessage() {
      return BsonSerializationTestSuite.TestSpec.create(
          "initial sync with message",
          status(
              IndexStatusP.StatusCode.INITIAL_SYNC,
              Optional.of("foo-bar"),
              Optional.empty(),
              Optional.empty()));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatusP> stale() {
      return BsonSerializationTestSuite.TestSpec.create(
          "stale index",
          status(
              IndexStatusP.StatusCode.STALE,
              Optional.of("foo-bar"),
              Optional.of(new BsonTimestamp(1000)),
              Optional.empty()));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatusP> recoveringNonTransient() {
      return BsonSerializationTestSuite.TestSpec.create(
          "recovering non transient",
          status(
              IndexStatusP.StatusCode.RECOVERING_NON_TRANSIENT,
              Optional.of(
                  "Recovering: The index has encountered an issue and is attempting automatic"
                      + " recovery."),
              Optional.of(new BsonTimestamp(1000)),
              Optional.empty()));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatusP> recoveringTransient() {
      return BsonSerializationTestSuite.TestSpec.create(
          "recovering transient",
          status(
              IndexStatusP.StatusCode.RECOVERING_TRANSIENT,
              Optional.of("recovering"),
              Optional.empty(),
              Optional.empty()));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatusP> failed() {
      return BsonSerializationTestSuite.TestSpec.create(
          "failed",
          status(
              IndexStatusP.StatusCode.FAILED,
              Optional.of("recovering"),
              Optional.empty(),
              Optional.of(IndexStatusP.Reason.INITIAL_SYNC_REPLICATION_FAILED)));
    }
  }
}
