package com.xgen.mongot.index.lucene.config;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.when;

import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Runtime;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.util.MockRuntimeBuilder;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.lucene.index.IndexWriter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {LuceneConfigTest.SerializationTest.class, LuceneConfigTest.ConfigTest.class})
public class LuceneConfigTest {

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "lucene-config-serialization";
    private static final BsonSerializationTestSuite<LuceneConfig> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(
            "src/test/unit/resources/index/lucene/config", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<LuceneConfig> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<LuceneConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<LuceneConfig>> data() {
      return Collections.singletonList(simple());
    }

    private static BsonSerializationTestSuite.TestSpec<LuceneConfig> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "full config",
          LuceneConfig.create(
              Path.of("some-path"),
              Optional.of(Duration.ofSeconds(10)),
              Optional.of(2),
              Optional.of(3),
              Optional.of(4),
              Optional.of(5.0),
              Optional.of(true),
              Optional.of(50.0),
              Optional.of(10.0),
              Optional.of(Bytes.ofBytes(2)),
              Optional.of(42),
              Optional.of(100),
              Optional.empty(),
              Optional.empty(),
              Optional.of(false),
              Optional.of(100000),
              Optional.of(false),
              Optional.of(4),
              Optional.of(8),
              Optional.of(16),
              Optional.of(32),
              Optional.of(new LuceneConfig.VectorMergePolicyConfig(100, 1024, 2048, 1024, 2048)),
              Optional.of(false),
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
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }

  public static class ConfigTest {

    @Test
    public void create_aggregationMaxBucketsZero_throws() {
      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  Path.of("temp"),
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
                  Optional.of(0),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()));
    }

    @Test
    public void create_aggregationMaxBucketsNegative_throws() {
      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  Path.of("temp"),
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
                  Optional.of(-1),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()));
    }

    @Test
    public void create_aggregationMaxBucketsPositive_accepted() {
      LuceneConfig config =
          LuceneConfig.create(
              Path.of("temp"),
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
              Optional.of(1000000),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      assertThat(config.aggregationMaxBuckets()).hasValue(1000000);
    }

    @Test
    public void testRefreshIntervalExplicit() {
      // Shouldn't throw
      LuceneConfig.create(
          Path.of("temp"),
          Optional.of(Duration.ofSeconds(1)),
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
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());

      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  Path.of("temp"),
                  Optional.of(Duration.ofSeconds(0)),
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
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()));

      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  Path.of("temp"),
                  Optional.of(Duration.ofSeconds(-1)),
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
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()));
    }

    @Test
    public void testRefreshIntervalDefault() {
      LuceneConfig config =
          LuceneConfig.create(
              MockRuntimeBuilder.buildDefault(),
              Path.of("temp"),
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
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      Assert.assertEquals(Duration.ofSeconds(1), config.refreshInterval());
    }

    @Test
    public void testRefreshExecutorThreadsExplicit() {
      // Shouldn't throw
      LuceneConfig.create(
          Path.of("temp"),
          Optional.empty(),
          Optional.of(1),
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
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());

      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  Path.of("temp"),
                  Optional.empty(),
                  Optional.of(0),
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
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()));

      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  Path.of("temp"),
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
                  Optional.empty()));
    }

    @Test
    public void testRefreshExecutorThreadsDefault() {
      // Half of the num CPUs capped at 10.
      assertExpectedRefreshExecutorThreadsDefault(1, 1);
      assertExpectedRefreshExecutorThreadsDefault(4, 2);
      assertExpectedRefreshExecutorThreadsDefault(15, 7);
      assertExpectedRefreshExecutorThreadsDefault(20, 10);
      assertExpectedRefreshExecutorThreadsDefault(22, 10);
      assertExpectedRefreshExecutorThreadsDefault(64, 10);
    }

    @Test
    public void testNumMaxMergeThreadsExplicit() {
      // Shouldn't throw
      LuceneConfig.create(
          Path.of("temp"),
          Optional.empty(),
          Optional.empty(),
          Optional.of(1),
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
          Optional.empty(),
          Optional.empty(),
          Optional.empty());

      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  Path.of("temp"),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.of(0),
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
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()));

      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  Path.of("temp"),
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
                  Optional.empty()));
    }

    @Test
    public void testNumMaxMergeThreadsDefault() {
      // If numMaxMerges is specified, default to the smaller of it or (numCpus / 2).
      assertExpectedNumMaxMergeThreadsExplicitNumMaxMerges(1, 1, 1);
      assertExpectedNumMaxMergeThreadsExplicitNumMaxMerges(10, 10, 5);
      assertExpectedNumMaxMergeThreadsExplicitNumMaxMerges(10, 30, 10);

      // max(1, floor(numCpus / 2))
      assertExpectedNumMaxMergeThreads(1, 1);
      assertExpectedNumMaxMergeThreads(4, 2);
      assertExpectedNumMaxMergeThreads(15, 7);
      assertExpectedNumMaxMergeThreads(20, 10);
      assertExpectedNumMaxMergeThreads(64, 32);
    }

    @Test
    public void testNumMaxMergesExplicit() {
      Runtime runtime = MockRuntimeBuilder.buildDefault();
      when(runtime.getNumCpus()).thenReturn(8);

      // Shouldn't throw
      LuceneConfig.create(
          runtime,
          Path.of("temp"),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.of(1),
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
          Optional.empty(),
          Optional.empty());

      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  runtime,
                  Path.of("temp"),
                  Optional.empty(),
                  Optional.empty(),
                  // Explicitly set numMaxMergeThreads so we're not testing the positivity of that
                  // default value.
                  Optional.of(5),
                  Optional.of(0),
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
                  Optional.empty(),
                  Optional.empty()));

      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  runtime,
                  Path.of("temp"),
                  Optional.empty(),
                  Optional.empty(),
                  // Explicitly set numMaxMergeThreads so we're not testing the positivity of that
                  // default value.
                  Optional.of(5),
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
                  Optional.empty()));

      // numMaxMerge must be >= numMaxMergeThreads
      LuceneConfig.create(
          Path.of("temp"),
          Optional.empty(),
          Optional.empty(),
          Optional.of(5),
          Optional.of(5),
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
          Optional.empty(),
          Optional.empty());

      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  Path.of("temp"),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.of(5),
                  Optional.of(4),
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
                  Optional.empty(),
                  Optional.empty()));
    }

    @Test
    public void testNumMaxMergesDefault() {
      // Low memory. Should match the numMaxMergeThreads.
      assertExpectedNumMaxMerges(Bytes.ofMebi(256), 1, 1);
      assertExpectedNumMaxMerges(Bytes.ofMebi(256), 2, 2);
      assertExpectedNumMaxMerges(Bytes.ofMebi(256), 6, 6);

      // Medium memory, low threads. Should match heapSize() / 512 MiB, up until
      // numMaxMergeThreads + 5.
      assertExpectedNumMaxMerges(Bytes.ofMebi(512), 1, 1);
      assertExpectedNumMaxMerges(Bytes.ofMebi(512 * 2), 1, 2);
      assertExpectedNumMaxMerges(Bytes.ofMebi(512 * 5), 1, 5);
      assertExpectedNumMaxMerges(Bytes.ofMebi((512 * 5) + 100), 1, 5);
      assertExpectedNumMaxMerges(Bytes.ofMebi(512 * 6), 1, 6);
      assertExpectedNumMaxMerges(Bytes.ofMebi(512 * 7), 1, 6);

      // High memory. Should match numMaxMergeThreads + 5.
      assertExpectedNumMaxMerges(Bytes.ofGibi(100), 1, 6);
      assertExpectedNumMaxMerges(Bytes.ofGibi(100), 10, 15);
      assertExpectedNumMaxMerges(Bytes.ofGibi(100), 20, 25);
    }

    @Test
    public void testMaxMergedSegmentSizeDefault() {
      // Match heap size up to 5 GiB.
      assertExpectedMaxMergedSegmentSize(Bytes.ofMebi(512), Bytes.ofMebi(512));
      assertExpectedMaxMergedSegmentSize(Bytes.ofGibi(2), Bytes.ofGibi(2));
      assertExpectedMaxMergedSegmentSize(Bytes.ofGibi(3), Bytes.ofGibi(3));
      assertExpectedMaxMergedSegmentSize(Bytes.ofGibi(5), Bytes.ofGibi(5));
      assertExpectedMaxMergedSegmentSize(Bytes.ofGibi(6), Bytes.ofGibi(5));
    }

    @Test
    public void testRamBufferSizeExplicit() {
      // Shouldn't throw
      LuceneConfig.create(
          Path.of("temp"),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.of(16D),
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

      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  Path.of("temp"),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.of(0D),
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
                  Optional.empty()));

      assertThrowsIllegalArgumentException(
          () ->
              LuceneConfig.create(
                  Path.of("temp"),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.of(-2D),
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
                  Optional.empty()));
    }

    @Test
    public void testRamBufferSizeDefault() {
      Runtime runtime = MockRuntimeBuilder.buildDefault();
      LuceneConfig config =
          LuceneConfig.create(
              runtime,
              Path.of("temp"),
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
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      Assert.assertEquals(
          Math.floor(Math.pow(runtime.getMaxHeapSize().toMebi(), 0.49)),
          config.ramBufferSizeMb(),
          0);
    }

    @Test
    public void testNrtCacheEnabledDefaultUnderThreshold() {
      Runtime runtime =
          new MockRuntimeBuilder().withNumCpus(1).withMaxHeapSize(Bytes.ofGibi(2)).build();
      LuceneConfig config =
          LuceneConfig.create(
              runtime,
              Path.of("temp"),
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
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      Assert.assertFalse(config.nrtCacheEnabled());
    }

    @Test
    public void testNrtCacheEnabledDefaultAboveThreshold() {
      Runtime runtime =
          new MockRuntimeBuilder().withNumCpus(1).withMaxHeapSize(Bytes.ofGibi(4)).build();
      LuceneConfig config =
          LuceneConfig.create(
              runtime,
              Path.of("temp"),
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
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      Assert.assertTrue(config.nrtCacheEnabled());
    }

    @Test
    public void testNrtCacheSizeDefault() {
      Runtime runtime = MockRuntimeBuilder.buildDefault();
      LuceneConfig config =
          LuceneConfig.create(
              runtime,
              Path.of("temp"),
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
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      Assert.assertEquals(
          Math.floor(Math.pow(runtime.getMaxHeapSize().toMebi(), 0.45)),
          config.nrtTotalCacheSizeMb(),
          0);
    }

    @Test
    public void testNrtMergeSizeDefault() {
      Runtime runtime = MockRuntimeBuilder.buildDefault();
      LuceneConfig config =
          LuceneConfig.create(
              runtime,
              Path.of("temp"),
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
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      Assert.assertEquals(
          Math.min(16, Math.floor(Math.pow(runtime.getMaxHeapSize().toMebi(), 0.45)) / 10),
          config.nrtMaxMergeSizeMb(),
          0);
    }

    private static void assertExpectedRefreshExecutorThreadsDefault(int numCpus, int expected) {
      Runtime runtime = MockRuntimeBuilder.buildDefault();
      when(runtime.getNumCpus()).thenReturn(numCpus);
      assertExpectedDefault(c -> c.refreshExecutorThreads(), runtime, expected);
    }

    private static void assertExpectedNumMaxMergeThreadsExplicitNumMaxMerges(
        int numMaxMerges, int numCpus, int expected) {
      Runtime runtime = MockRuntimeBuilder.buildDefault();
      when(runtime.getNumCpus()).thenReturn(numCpus);

      LuceneConfig config =
          LuceneConfig.create(
              runtime,
              Path.of("temp"),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(numMaxMerges),
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
              Optional.empty(),
              Optional.empty());

      assertExpectedDefault(c -> c.numMaxMergeThreads(), config, expected);
    }

    private static void assertExpectedNumMaxMergeThreads(int numCpus, int expected) {
      Runtime runtime = MockRuntimeBuilder.buildDefault();
      when(runtime.getNumCpus()).thenReturn(numCpus);
      assertExpectedDefault(c -> c.numMaxMergeThreads(), runtime, expected);
    }

    private static void assertExpectedNumMaxMerges(
        Bytes heapSize, int numMaxMergeThreads, int expected) {
      Runtime runtime = MockRuntimeBuilder.buildDefault();
      when(runtime.getMaxHeapSize()).thenReturn(heapSize);

      LuceneConfig config =
          LuceneConfig.create(
              runtime,
              Path.of("temp"),
              Optional.empty(),
              Optional.empty(),
              Optional.of(numMaxMergeThreads),
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
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      assertExpectedDefault(c -> c.numMaxMerges(), config, expected);
    }

    @Test
    public void testFieldLimitMustBeLargerThanEqualToThree() {
      Runtime runtime = MockRuntimeBuilder.buildDefault();

      // should not throw
      LuceneConfig.create(
          runtime,
          Path.of("temp"),
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
          Optional.of(3),
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

      for (int limit : List.of(-1, 0, 1, 2)) {
        assertThrowsIllegalArgumentException(
            () ->
                LuceneConfig.create(
                    runtime,
                    Path.of("temp"),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(limit),
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
                    Optional.empty()));
      }
    }

    @Test
    public void testDocsLimitMustBeLargerThanEqualToThree() {
      Runtime runtime = MockRuntimeBuilder.buildDefault();

      // should not throw
      LuceneConfig.create(
          runtime,
          Path.of("temp"),
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
          Optional.of(100),
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

      for (int limit : List.of(-1, 0, IndexWriter.MAX_DOCS, Integer.MAX_VALUE)) {
        assertThrowsIllegalArgumentException(
            "Must throw an exception for: " + limit,
            () ->
                LuceneConfig.create(
                    runtime,
                    Path.of("temp"),
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
                    Optional.of(limit),
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
                    Optional.empty()));
      }
    }

    private static void assertExpectedMaxMergedSegmentSize(Bytes maxHeapSize, Bytes expected) {
      Runtime runtime = MockRuntimeBuilder.buildDefault();
      when(runtime.getMaxHeapSize()).thenReturn(maxHeapSize);
      assertExpectedDefault(c -> c.maxMergedSegmentSize(), runtime, expected);
    }

    private static <T> void assertExpectedDefault(
        Function<LuceneConfig, T> resultSupplier, Runtime runtime, T expected) {
      LuceneConfig config =
          LuceneConfig.create(
              runtime,
              Path.of("temp"),
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
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      assertExpectedDefault(resultSupplier, config, expected);
    }

    private static <T> void assertExpectedDefault(
        Function<LuceneConfig, T> resultSupplier, LuceneConfig config, T expected) {
      T result = resultSupplier.apply(config);
      Assert.assertEquals(expected, result);
    }

    private static void assertThrowsIllegalArgumentException(ThrowingRunnable supplier) {
      Assert.assertThrows(IllegalArgumentException.class, supplier);
    }

    private static void assertThrowsIllegalArgumentException(
        String message, ThrowingRunnable supplier) {
      Assert.assertThrows(message, IllegalArgumentException.class, supplier);
    }
  }
}
