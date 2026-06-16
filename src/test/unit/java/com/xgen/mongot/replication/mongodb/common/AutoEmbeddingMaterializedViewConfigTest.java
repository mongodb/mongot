package com.xgen.mongot.replication.mongodb.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.xgen.mongot.util.Runtime;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.util.MockRuntimeBuilder;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      AutoEmbeddingMaterializedViewConfigTest.SerializationTest.class,
      AutoEmbeddingMaterializedViewConfigTest.ConfigTest.class
    })
public class AutoEmbeddingMaterializedViewConfigTest {

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME =
        "auto-embedding-materialized-view-config-serialization";
    private static final BsonSerializationTestSuite<AutoEmbeddingMaterializedViewConfig>
        TEST_SUITE =
            BsonSerializationTestSuite.fromEncodable(
                "src/test/unit/resources/replication/mongodb/common", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<AutoEmbeddingMaterializedViewConfig> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<AutoEmbeddingMaterializedViewConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<AutoEmbeddingMaterializedViewConfig>>
        data() {
      return Arrays.asList(fullConfig());
    }

    private static BsonSerializationTestSuite.TestSpec<AutoEmbeddingMaterializedViewConfig>
        fullConfig() {
      return BsonSerializationTestSuite.TestSpec.create(
          "full config",
          AutoEmbeddingMaterializedViewConfig.create(
              new CommonReplicationConfig.GlobalReplicationConfig(
                  false,
                  List.of(
                      new ObjectId("68784215b86a4a2d55787ae6"),
                      new ObjectId("687d201de90e474dfbc7c1d4")),
                  false,
                  false,
                  List.of("updateDescription.disambiguatedPaths"),
                  true),
              Optional.of(1),
              Optional.of(2),
              Optional.of(3),
              Optional.of(4),
              Optional.of(5),
              Optional.of(6),
              Optional.of(7),
              Optional.of(8),
              Optional.of(10),
              Optional.of(8),
              Optional.of(9),
              Optional.of(2),
              Optional.of(42),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(30_000L),
              Optional.of(45_000L),
              Optional.of(1L),
              Optional.of(75),
              Optional.of(60),
              Optional.of(1000L),
              Optional.of(2000L),
              Optional.of(3000L),
              Optional.empty()));
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }

  public static class ConfigTest {
    // Makes sure returns expected default config that won't break community version
    @Test
    public void getDefault_returnsExpectedConfig() {
      Runtime runtime = MockRuntimeBuilder.buildDefault();
      int numCpus = 8;
      when(runtime.getNumCpus()).thenReturn(numCpus);
      AutoEmbeddingMaterializedViewConfig config =
          AutoEmbeddingMaterializedViewConfig.getDefaultWithRuntime(runtime);

      // Test GlobalReplicationConfig fields (inherited from CommonReplicationConfig)
      assertFalse(config.pauseAllInitialSyncs);
      assertEquals(List.of(), config.pauseInitialSyncOnIndexIds);
      assertFalse(config.enableSplitLargeChangeStreamEvents);
      assertEquals(List.of(), config.excludedChangestreamFields);
      assertFalse(config.matchCollectionUuidForUpdateLookup);

      // maxConcurrentEmbeddingInitialSyncs = Math.min(numCpus, 1) = 1
      assertEquals(1, config.maxConcurrentEmbeddingInitialSyncs);

      // numConcurrentChangeStreams = numCpus * 2 = 16
      assertEquals(16, config.numConcurrentChangeStreams);

      // numIndexingThreads = Math.max(1, numCpus) = Math.max(1, 8) = 8
      assertEquals(8, config.numIndexingThreads);

      // numChangeStreamDecodingThreads = Math.max(1, Math.floorDiv(numCpus, 2)) = Math.max(1, 4) =
      // 4
      assertEquals(4, config.numChangeStreamDecodingThreads);

      // maxInFlightEmbeddingGetMores = Math.max(1, numConcurrentChangeStreams / 4) = Math.max(1, 4)
      // = 4
      assertEquals(4, config.maxInFlightEmbeddingGetMores);

      // Test constant defaults
      assertEquals(1000, config.changeStreamMaxTimeMs);
      assertEquals(1800, config.changeStreamCursorMaxTimeSec);
      assertEquals(100, config.requestRateLimitBackoffMs);

      // Test matViewWriterMaxConnections default
      assertEquals(4, config.matViewWriterMaxConnections);

      // Test Optional fields that should be empty by default
      assertEquals(Optional.empty(), config.embeddingGetMoreBatchSize);
      assertEquals(Optional.empty(), config.materializedViewSchemaVersion);
      assertEquals(Optional.empty(), config.mvWriteRateLimitRps);
      assertEquals(Optional.empty(), config.congestionControl);
      assertEquals(Optional.empty(), config.flexTierWorkloads);
      assertEquals(Optional.empty(), config.resyncBackoff);
      assertEquals(Optional.empty(), config.transientBackoff);

      assertEquals(numCpus, config.numEmbeddingThreads);

      // Memory budget defaults
      assertEquals(
          AutoEmbeddingMaterializedViewConfig.DEFAULT_GLOBAL_MEMORY_BUDGET_HEAP_PERCENT,
          config.globalMemoryBudgetHeapPercent);
      assertEquals(
          AutoEmbeddingMaterializedViewConfig.DEFAULT_PER_BATCH_MEMORY_BUDGET_HEAP_PERCENT,
          config.perBatchMemoryBudgetHeapPercent);

      // Interval defaults (milliseconds)
      assertEquals(30_000L, config.leaseManagerHeartbeatIntervalMs);
      assertEquals(30_000L, config.materializedViewStatusRefreshIntervalMs);
      assertEquals(10_000L, config.materializedViewOptimeUpdateIntervalMs);

      assertEquals(
          Duration.ofSeconds(60).toMillis(), config.materializedViewWriterSocketTimeoutMs);

      assertEquals(1L, config.defaultMaterializedViewNameFormatVersion);
    }

    @Test
    public void testMvWriteRateLimitRps_defaultCustomAndValidation() {
      AutoEmbeddingMaterializedViewConfig defaultConfig =
          AutoEmbeddingMaterializedViewConfig.getDefault();
      assertEquals(Optional.empty(), defaultConfig.getMvWriteRateLimitRps());

      AutoEmbeddingMaterializedViewConfig customConfig =
          AutoEmbeddingMaterializedViewConfig.create(
              CommonReplicationConfig.defaultGlobalReplicationConfig(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(50),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      assertEquals(Optional.of(50), customConfig.getMvWriteRateLimitRps());

      assertThrows(
          IllegalArgumentException.class,
          () ->
              AutoEmbeddingMaterializedViewConfig.create(
                  CommonReplicationConfig.defaultGlobalReplicationConfig(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.of(-1),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()));
    }

    @Test
    public void testToBson_withAndWithoutMvWriteRateLimitRps() {
      AutoEmbeddingMaterializedViewConfig configWith =
          AutoEmbeddingMaterializedViewConfig.create(
              CommonReplicationConfig.defaultGlobalReplicationConfig(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(75),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      BsonDocument bsonWith = configWith.toBson();
      assertTrue(
          "BSON should contain mvWriteRateLimitRps",
          bsonWith.containsKey("mvWriteRateLimitRps"));
      assertEquals(75, bsonWith.getInt32("mvWriteRateLimitRps").getValue());

      AutoEmbeddingMaterializedViewConfig configWithout =
          AutoEmbeddingMaterializedViewConfig.getDefault();
      BsonDocument bsonWithout = configWithout.toBson();
      assertFalse(
          "BSON should not contain mvWriteRateLimitRps when empty",
          bsonWithout.containsKey("mvWriteRateLimitRps"));
    }

    @Test
    public void testMatViewWriterMaxConnections_explicitValue() {
      Runtime runtime = MockRuntimeBuilder.buildDefault();
      when(runtime.getNumCpus()).thenReturn(8);
      AutoEmbeddingMaterializedViewConfig config =
          AutoEmbeddingMaterializedViewConfig.create(
              runtime,
              new CommonReplicationConfig.GlobalReplicationConfig(
                  false, List.of(), false, false, List.of(), false),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(8),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      assertEquals(8, config.matViewWriterMaxConnections);
    }

    @Test
    public void testMatViewWriterMaxConnections_defaultsTo4WhenAbsent() {
      Runtime runtime = MockRuntimeBuilder.buildDefault();
      when(runtime.getNumCpus()).thenReturn(8);
      AutoEmbeddingMaterializedViewConfig config =
          AutoEmbeddingMaterializedViewConfig.create(
              runtime,
              new CommonReplicationConfig.GlobalReplicationConfig(
                  false, List.of(), false, false, List.of(), false),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      assertEquals(4, config.matViewWriterMaxConnections);
    }

    @Test
    public void testIntervalMs_customValues() {
      AutoEmbeddingMaterializedViewConfig config =
          AutoEmbeddingMaterializedViewConfig.create(
              CommonReplicationConfig.defaultGlobalReplicationConfig(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(5000L),
              Optional.of(15000L),
              Optional.of(3000L),
              Optional.empty());
      assertEquals(5000L, config.leaseManagerHeartbeatIntervalMs);
      assertEquals(15000L, config.materializedViewStatusRefreshIntervalMs);
      assertEquals(3000L, config.materializedViewOptimeUpdateIntervalMs);
    }

    @Test
    public void testIntervalMs_zeroFallsBackToDefault() {
      AutoEmbeddingMaterializedViewConfig config =
          AutoEmbeddingMaterializedViewConfig.create(
              CommonReplicationConfig.defaultGlobalReplicationConfig(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(0L),
              Optional.of(0L),
              Optional.of(0L),
              Optional.empty());
      assertEquals(30_000L, config.leaseManagerHeartbeatIntervalMs);
      assertEquals(30_000L, config.materializedViewStatusRefreshIntervalMs);
      assertEquals(10_000L, config.materializedViewOptimeUpdateIntervalMs);
    }

    @Test
    public void testIntervalMs_negativeFallsBackToDefault() {
      AutoEmbeddingMaterializedViewConfig config =
          AutoEmbeddingMaterializedViewConfig.create(
              CommonReplicationConfig.defaultGlobalReplicationConfig(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(-1L),
              Optional.of(-100L),
              Optional.of(-50L),
              Optional.empty());
      assertEquals(30_000L, config.leaseManagerHeartbeatIntervalMs);
      assertEquals(30_000L, config.materializedViewStatusRefreshIntervalMs);
      assertEquals(10_000L, config.materializedViewOptimeUpdateIntervalMs);
    }

    @Test
    public void materializedViewWriterSocketTimeout_mustBePositive() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              AutoEmbeddingMaterializedViewConfig.create(
                  CommonReplicationConfig.defaultGlobalReplicationConfig(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.of(0L)));
    }

    @Test
    public void materializedViewWriterSocketTimeout_mustNotExceedIntegerMaxValue() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              AutoEmbeddingMaterializedViewConfig.create(
                  CommonReplicationConfig.defaultGlobalReplicationConfig(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.of((long) Integer.MAX_VALUE + 1)));
    }
  }
}
