package com.xgen.mongot.replication.mongodb.common;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Runtime;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.replication.mongodb.common.MongoDbReplicationConfigBuilder;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      MongoDbReplicationConfigTest.TestSerialization.class,
      MongoDbReplicationConfigTest.TestConfig.class
    })
public class MongoDbReplicationConfigTest {

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "replication-config-serialization";
    private static final BsonSerializationTestSuite<MongoDbReplicationConfig> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(
            "src/test/unit/resources/replication/mongodb/common", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<MongoDbReplicationConfig> testSpec;

    public TestSerialization(
        BsonSerializationTestSuite.TestSpec<MongoDbReplicationConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<MongoDbReplicationConfig>> data() {
      return Arrays.asList(fullConfig());
    }

    private static BsonSerializationTestSuite.TestSpec<MongoDbReplicationConfig> fullConfig() {
      return BsonSerializationTestSuite.TestSpec.create(
          "full config",
          MongoDbReplicationConfig.create(
              new CommonReplicationConfig.GlobalReplicationConfig(
                  false,
                  List.of(
                      new ObjectId("68784215b86a4a2d55787ae6"),
                      new ObjectId("687d201de90e474dfbc7c1d4")),
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
              Optional.of(true),
              Optional.of(8),
              Optional.of(9),
              Optional.of(10),
              Optional.of(11)));
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }

  public static class TestConfig {
    private static <T> void assertExpectedDefault(
        Function<MongoDbReplicationConfig, T> resultSupplier, Runtime runtime, T expected) {

      var config = MongoDbReplicationConfigBuilder.builder().runtime(runtime).build();
      T result = resultSupplier.apply(config);
      Assert.assertEquals(expected, result);
    }

    @Test
    public void testNumConcurrentInitialSyncsDefault() {
      // Should be numCpus.
      assertExpectedNumConcurrentInitialSyncsDefault(1, 1);
      assertExpectedNumConcurrentInitialSyncsDefault(2, 2);
      assertExpectedNumConcurrentInitialSyncsDefault(50, 2);
    }

    @Test
    public void testNumConcurrentInitialSyncsExplicit() {
      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder()
          .optionalNumConcurrentInitialSyncs(Optional.of(1))
          .build();

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalNumConcurrentInitialSyncs(Optional.of(0))
                  .build());

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalNumConcurrentInitialSyncs(Optional.of(-1))
                  .build());
    }

    @Test
    public void testNumConcurrentChangeStreamsDefault() {
      // Should be numCpus * 2.
      assertExpectedNumConcurrentChangeStreamsDefault(1, 2);
      assertExpectedNumConcurrentChangeStreamsDefault(2, 4);
      assertExpectedNumConcurrentChangeStreamsDefault(50, 100);
    }

    @Test
    public void testNumConcurrentChangeStreamsExplicit() {
      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder()
          .optionalNumConcurrentChangeStreams(Optional.of(1))
          .build();

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalNumConcurrentChangeStreams(Optional.of(0))
                  .build());

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalNumConcurrentChangeStreams(Optional.of(-1))
                  .build());
    }

    @Test
    public void testChangeStreamMaxTimeMsExplicit() {
      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder()
          .optionalChangeStreamMaxTimeMs(Optional.of(1))
          .build();

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalChangeStreamMaxTimeMs(Optional.of(0))
                  .build());

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalChangeStreamMaxTimeMs(Optional.of(-1))
                  .build());
    }

    @Test
    public void testChangeStreamMaxTimeMsDefault() {
      MongoDbReplicationConfig config = MongoDbReplicationConfigBuilder.builder().build();
      Assert.assertEquals(1000, config.changeStreamMaxTimeMs);
    }

    @Test
    public void testNumIndexingThreadsExplicit() {
      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder().optionalNumIndexingThreads(Optional.of(1)).build();

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalNumIndexingThreads(Optional.of(0))
                  .build());

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalNumIndexingThreads(Optional.of(-1))
                  .build());
    }

    @Test
    public void testNumIndexingThreadsDefault() {
      MongoDbReplicationConfig config = MongoDbReplicationConfigBuilder.builder().build();

      Assert.assertEquals(
          Math.max(1, Math.floorDiv(Runtime.INSTANCE.getNumCpus(), 2)), config.numIndexingThreads);
    }

    @Test
    public void testNumConcurrentSynonymSyncsExplicit() {
      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder()
          .optionalNumConcurrentSynonymSyncs(Optional.of(1))
          .build();

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalNumConcurrentSynonymSyncs(Optional.of(0))
                  .build());

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalNumConcurrentSynonymSyncs(Optional.of(-1))
                  .build());
    }

    @Test
    public void testChangeStreamCursorMaxTimeSecExplicit() {
      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder()
          .optionalChangeStreamCursorMaxTimeSec(Optional.of(10))
          .build();

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalChangeStreamCursorMaxTimeSec(Optional.of(0))
                  .build());

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalChangeStreamCursorMaxTimeSec(Optional.of(-1))
                  .build());
    }

    @Test
    public void testChangeStreamCursorMaxTimeSecDefault() {
      MongoDbReplicationConfig config = MongoDbReplicationConfigBuilder.builder().build();

      Assert.assertEquals(Duration.ofMinutes(30).toSeconds(), config.changeStreamCursorMaxTimeSec);
    }

    @Test
    public void testNumChangeStreamDecodingThreadsExplicit() {
      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder()
          .optionalNumChangeStreamDecodingThreads(Optional.of(10))
          .build();

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalNumChangeStreamDecodingThreads(Optional.of(0))
                  .build());

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalNumChangeStreamDecodingThreads(Optional.of(-1))
                  .build());
    }

    @Test
    public void testNumChangeStreamDecodingThreadsDefault() {
      MongoDbReplicationConfig config = MongoDbReplicationConfigBuilder.builder().build();

      Assert.assertEquals(
          Math.max(1, Math.floorDiv(Runtime.INSTANCE.getNumCpus(), 2)),
          config.numChangeStreamDecodingThreads);
    }

    @Test
    public void testEnableSteadyStateChangeStreamProjectionDefault() {
      MongoDbReplicationConfig config = MongoDbReplicationConfigBuilder.builder().build();

      Assert.assertEquals(Optional.empty(), config.enableSteadyStateChangeStreamProjection);
    }

    @Test
    public void testMaxInFlightEmbeddingGetMoresExplicit() {
      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder().build();

      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder()
          .optionalMaxInFlightEmbeddingGetMores(Optional.of(1))
          .build();

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalMaxInFlightEmbeddingGetMores(Optional.of(0))
                  .build());

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalMaxInFlightEmbeddingGetMores(Optional.of(-1))
                  .build());
    }

    @Test
    public void testMaxInFlightEmbeddingGetMoresDefault() {
      assertExpectedMaxInFlightEmbeddingGetMoresDefault(1, 1);
      assertExpectedMaxInFlightEmbeddingGetMoresDefault(5, 2);
      assertExpectedMaxInFlightEmbeddingGetMoresDefault(100, 50);
    }

    @Test
    public void testMaxConcurrentEmbeddingInitialSyncsExplicit() {
      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder().build();

      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder()
          .optionalMaxConcurrentEmbeddingInitialSyncs(Optional.of(1))
          .build();

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalMaxConcurrentEmbeddingInitialSyncs(Optional.of(0))
                  .build());

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalMaxConcurrentEmbeddingInitialSyncs(Optional.of(-1))
                  .build());
    }

    @Test
    public void testMaxConcurrentEmbeddingInitialSyncsDefault() {
      assertExpectedMaxConcurrentEmbeddingInitialSyncsDefault(1, 1);
      assertExpectedMaxConcurrentEmbeddingInitialSyncsDefault(2, 1);
      assertExpectedMaxConcurrentEmbeddingInitialSyncsDefault(50, 1);
    }

    @Test
    public void testEmbeddingGetMoreBatchSizeExplicit() {
      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder().embeddingGetMoreBatchSize(Optional.empty()).build();

      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder().embeddingGetMoreBatchSize(Optional.of(1)).build();

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .embeddingGetMoreBatchSize(Optional.of(0))
                  .build());

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .embeddingGetMoreBatchSize(Optional.of(-1))
                  .build());
    }

    @Test
    public void testMatchCollectionUuidForUpdateLookupDefault() {
      MongoDbReplicationConfig config = MongoDbReplicationConfigBuilder.builder().build();

      Assert.assertEquals(false, config.matchCollectionUuidForUpdateLookup);
    }

    @Test
    public void testEmbeddingGetMoreBatchSizeDefault() {
      MongoDbReplicationConfig config = MongoDbReplicationConfigBuilder.builder().build();

      Assert.assertEquals(Optional.empty(), config.embeddingGetMoreBatchSize);
    }

    @Test
    public void testRequestRateLimitBackoffMs() {
      // Shouldn't throw
      MongoDbReplicationConfigBuilder.builder()
          .optionalRequestRateLimitBackoffMs(Optional.of(1))
          .build();

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalRequestRateLimitBackoffMs(Optional.of(0))
                  .build());

      assertThrowsIllegalArgumentException(
          () ->
              MongoDbReplicationConfigBuilder.builder()
                  .optionalRequestRateLimitBackoffMs(Optional.of(-1))
                  .build());
    }

    private static void assertExpectedNumConcurrentInitialSyncsDefault(int numCpus, int expected) {
      var runtime = mockRuntime();
      when(runtime.getNumCpus()).thenReturn(numCpus);
      assertExpectedDefault(c -> c.numConcurrentInitialSyncs, runtime, expected);
    }

    private static void assertExpectedNumConcurrentChangeStreamsDefault(int numCpus, int expected) {
      var runtime = mockRuntime();
      when(runtime.getNumCpus()).thenReturn(numCpus);
      assertExpectedDefault(c -> c.numConcurrentChangeStreams, runtime, expected);
    }

    private static void assertExpectedMaxInFlightEmbeddingGetMoresDefault(
        int numCpus, int expected) {
      var runtime = mockRuntime();
      when(runtime.getNumCpus()).thenReturn(numCpus);
      assertExpectedDefault(c -> c.maxInFlightEmbeddingGetMores, runtime, expected);
    }

    private static void assertExpectedMaxConcurrentEmbeddingInitialSyncsDefault(
        int numCpus, int expected) {
      var runtime = mockRuntime();
      when(runtime.getNumCpus()).thenReturn(numCpus);
      assertExpectedDefault(c -> c.maxConcurrentEmbeddingInitialSyncs, runtime, expected);
    }

    private static void assertThrowsIllegalArgumentException(ThrowingRunnable supplier) {
      Assert.assertThrows(IllegalArgumentException.class, supplier);
    }

    private static Runtime mockRuntime() {
      var mock = mock(Runtime.class);
      when(mock.getNumCpus()).thenReturn(1);
      when(mock.getMaxHeapSize()).thenReturn(Bytes.ofMebi(512));

      return mock;
    }
  }
}
