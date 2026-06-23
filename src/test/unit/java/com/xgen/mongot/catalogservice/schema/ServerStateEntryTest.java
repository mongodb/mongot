package com.xgen.mongot.catalogservice.schema;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      ServerStateEntryTest.DeserializationTest.class,
      ServerStateEntryTest.SerializationTest.class,
      ServerStateEntryTest.ReadinessStateTest.class
    })
public class ServerStateEntryTest {
  private static final String RESOURCE_PATH = "src/test/unit/resources/catalogservice/schema";

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "server-state-deserialization";
    private static final BsonDeserializationTestSuite<ServerStateEntry> TEST_SUITE =
        fromDocument(RESOURCE_PATH, SUITE_NAME, ServerStateEntry::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<ServerStateEntry> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<ServerStateEntry> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<ServerStateEntry>> data() {
      return TEST_SUITE.withExamples(simple(), shutdownFieldUnset(), readyFieldUnset());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<ServerStateEntry> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          new ServerStateEntry(
              new ObjectId("000003e8464f5e2393000000"),
              "server",
              Instant.parse("2025-12-29T10:00:00Z"),
              false,
              false));
    }

    private static BsonDeserializationTestSuite.ValidSpec<ServerStateEntry> shutdownFieldUnset() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "shutdown field unset",
          new ServerStateEntry(
              new ObjectId("000003e8464f5e2393000001"),
              "server-old",
              Instant.parse("2025-12-29T11:00:00Z"),
              false,
              false));
    }

    private static BsonDeserializationTestSuite.ValidSpec<ServerStateEntry> readyFieldUnset() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "ready field unset",
          new ServerStateEntry(
              new ObjectId("000003e8464f5e2393000002"),
              "server-old",
              Instant.parse("2025-12-29T12:00:00Z"),
              false,
              false));
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "server-state-serialization";
    private static final BsonSerializationTestSuite<ServerStateEntry> TEST_SUITE =
        fromEncodable(RESOURCE_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<ServerStateEntry> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<ServerStateEntry> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<ServerStateEntry>> data() {
      return Arrays.asList(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<ServerStateEntry> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          new ServerStateEntry(
              new ObjectId("000003e8464f5e2393000000"),
              "server",
              Instant.parse("2025-12-29T10:00:00Z"),
              false,
              false));
    }
  }

  public static class ReadinessStateTest {

    private static final ObjectId SERVER_ID = new ObjectId();
    private static final String SERVER_NAME = "test-server";
    private static final Duration RECENT = Duration.ofMinutes(5);
    private static final Duration EXPIRED = Duration.ofMinutes(20);

    @Test
    public void isReadinessStateExpired_heartbeat20MinutesAgo_returnsTrue() {
      ServerStateEntry entry =
          new ServerStateEntry(SERVER_ID, SERVER_NAME, Instant.now().minus(EXPIRED), false, false);
      assertTrue(entry.isReadinessStateExpired());
    }

    @Test
    public void isReadinessStateExpired_heartbeat5MinutesAgo_returnsFalse() {
      ServerStateEntry entry =
          new ServerStateEntry(SERVER_ID, SERVER_NAME, Instant.now().minus(RECENT), false, false);
      assertFalse(entry.isReadinessStateExpired());
    }

    @Test
    public void shouldMaintainReadinessState_readyTrueRecentHeartbeat_returnsTrue() {
      ServerStateEntry entry =
          new ServerStateEntry(SERVER_ID, SERVER_NAME, Instant.now().minus(RECENT), true, false);
      assertTrue(entry.shouldMaintainReadinessState());
    }

    @Test
    public void shouldMaintainReadinessState_readyTrueExpiredHeartbeat_returnsFalse() {
      ServerStateEntry entry =
          new ServerStateEntry(SERVER_ID, SERVER_NAME, Instant.now().minus(EXPIRED), true, false);
      assertFalse(entry.shouldMaintainReadinessState());
    }

    @Test
    public void shouldMaintainReadinessState_readyFalseRecentHeartbeat_returnsFalse() {
      ServerStateEntry entry =
          new ServerStateEntry(SERVER_ID, SERVER_NAME, Instant.now().minus(RECENT), false, false);
      assertFalse(entry.shouldMaintainReadinessState());
    }

    @Test
    public void shouldMaintainReadinessState_readyFalseExpiredHeartbeat_returnsFalse() {
      ServerStateEntry entry =
          new ServerStateEntry(SERVER_ID, SERVER_NAME, Instant.now().minus(EXPIRED), false, false);
      assertFalse(entry.shouldMaintainReadinessState());
    }
  }
}
