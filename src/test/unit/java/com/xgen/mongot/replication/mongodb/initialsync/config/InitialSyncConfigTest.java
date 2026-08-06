package com.xgen.mongot.replication.mongodb.initialsync.config;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonSerializationTestSuite;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      InitialSyncConfigTest.DeserializationTest.class,
      InitialSyncConfigTest.SerializationTest.class
    })
public class InitialSyncConfigTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "initial-sync-config-deserialization";
    private static final BsonDeserializationTestSuite<InitialSyncConfig> TEST_SUITE =
        BsonDeserializationTestSuite.fromDocument(
            "src/test/unit/resources/replication/mongodb/initialsync/config",
            SUITE_NAME,
            InitialSyncConfig::fromOuterBson);

    private final TestSpecWrapper<InitialSyncConfig> testSpec;

    public DeserializationTest(TestSpecWrapper<InitialSyncConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestSpecWrapper<InitialSyncConfig>> data() {
      return TEST_SUITE.withExamples(
          bufferless(),
          bufferlessWithCollectionScanTime(),
          bufferlessWithChangeStreamCatchupTimeout(),
          bufferlessWithChangeStreamLagTime(),
          bufferlessWithEnableNaturalOrderScan(),
          bufferlessWithParallelNaturalOrderScan());
    }

    private static BsonDeserializationTestSuite.ValidSpec<InitialSyncConfig> bufferless() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "bufferless with default", new InitialSyncConfig());
    }

    private static BsonDeserializationTestSuite.ValidSpec<InitialSyncConfig>
        bufferlessWithCollectionScanTime() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "bufferless with collection scan time",
          InitialSyncConfigTest.createWithCollectionScanTime(Duration.ofMillis(600)));
    }

    private static BsonDeserializationTestSuite.ValidSpec<InitialSyncConfig>
        bufferlessWithChangeStreamCatchupTimeout() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "bufferless with change stream catchup time",
          InitialSyncConfigTest.createWithChangeStreamCatchupTimeout(Duration.ofMillis(600)));
    }

    private static BsonDeserializationTestSuite.ValidSpec<InitialSyncConfig>
        bufferlessWithChangeStreamLagTime() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "bufferless with change stream lag time",
          InitialSyncConfigTest.createWithChangeStreamLagTime(Duration.ofMillis(600)));
    }

    private static BsonDeserializationTestSuite.ValidSpec<InitialSyncConfig>
        bufferlessWithEnableNaturalOrderScan() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "bufferless with enable natural order scan",
          InitialSyncConfigTest.createWithEnableNaturalOrderScan(true));
    }

    private static BsonDeserializationTestSuite.ValidSpec<InitialSyncConfig>
        bufferlessWithParallelNaturalOrderScan() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "bufferless with parallel natural order scan",
          InitialSyncConfigTest.createWithParallelNaturalOrderScan(true, 8));
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "initial-sync-config-serialization";
    private static final BsonSerializationTestSuite<InitialSyncConfig> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(
            "src/test/unit/resources/replication/mongodb/initialsync/config", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<InitialSyncConfig> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<InitialSyncConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<InitialSyncConfig>> data() {
      return List.of(
          bufferless(),
          bufferlessWithCollectionScanTimeMs(),
          bufferlessWithChangeStreamCatchupTimeoutMs(),
          bufferlessWithChangeStreamLagTimeMs(),
          bufferlessWithEnableNaturalOrderScan(),
          bufferlessWithParallelNaturalOrderScan());
    }

    private static BsonSerializationTestSuite.TestSpec<InitialSyncConfig> bufferless() {
      return BsonSerializationTestSuite.TestSpec.create(
          "bufferless with default", new InitialSyncConfig());
    }

    private static BsonSerializationTestSuite.TestSpec<InitialSyncConfig>
        bufferlessWithCollectionScanTimeMs() {
      return BsonSerializationTestSuite.TestSpec.create(
          "bufferless with collection scan time",
          InitialSyncConfigTest.createWithCollectionScanTime(Duration.ofMillis(600)));
    }

    private static BsonSerializationTestSuite.TestSpec<InitialSyncConfig>
        bufferlessWithChangeStreamCatchupTimeoutMs() {
      return BsonSerializationTestSuite.TestSpec.create(
          "bufferless with change stream catchup time",
          InitialSyncConfigTest.createWithChangeStreamCatchupTimeout(Duration.ofMillis(600)));
    }

    private static BsonSerializationTestSuite.TestSpec<InitialSyncConfig>
        bufferlessWithChangeStreamLagTimeMs() {
      return BsonSerializationTestSuite.TestSpec.create(
          "bufferless with change stream lag time",
          InitialSyncConfigTest.createWithChangeStreamLagTime(Duration.ofMillis(600)));
    }

    private static BsonSerializationTestSuite.TestSpec<InitialSyncConfig>
        bufferlessWithEnableNaturalOrderScan() {
      return BsonSerializationTestSuite.TestSpec.create(
          "bufferless with enable natural order scan",
          InitialSyncConfigTest.createWithEnableNaturalOrderScan(true));
    }

    private static BsonSerializationTestSuite.TestSpec<InitialSyncConfig>
        bufferlessWithParallelNaturalOrderScan() {
      return BsonSerializationTestSuite.TestSpec.create(
          "bufferless with parallel natural order scan",
          InitialSyncConfigTest.createWithParallelNaturalOrderScan(true, 8));
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }

  public static InitialSyncConfig createWithCollectionScanTime(Duration collectionScanTime) {
    return new InitialSyncConfig(
        Optional.of(collectionScanTime),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(false),
        Optional.of(false),
        Optional.empty(),
        Optional.empty());
  }

  public static InitialSyncConfig createWithChangeStreamCatchupTimeout(
      Duration changeStreamCatchupTimeout) {
    return new InitialSyncConfig(
        Optional.empty(),
        Optional.of(changeStreamCatchupTimeout),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  public static InitialSyncConfig createWithChangeStreamLagTime(Duration changeStreamLagTime) {
    return new InitialSyncConfig(
        Optional.empty(),
        Optional.empty(),
        Optional.of(changeStreamLagTime),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  public static InitialSyncConfig createWithParallelNaturalOrderScan(
      boolean enableParallelNaturalOrderScan, int parallelPartitionCount) {
    return new InitialSyncConfig(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(true),
        Optional.of(false),
        Optional.of(enableParallelNaturalOrderScan),
        Optional.of(parallelPartitionCount));
  }

  public static InitialSyncConfig createWithEnableNaturalOrderScan(boolean enableNaturalOrderScan) {
    return new InitialSyncConfig(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(enableNaturalOrderScan),
        Optional.of(true),
        Optional.empty(),
        Optional.empty());
  }
}
