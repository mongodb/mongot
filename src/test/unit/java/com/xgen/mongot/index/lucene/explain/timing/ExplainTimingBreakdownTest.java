package com.xgen.mongot.index.lucene.explain.timing;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.lucene.explain.timing.ExplainTimingBreakdownBuilder;
import com.xgen.testing.mongot.index.lucene.explain.timing.QueryExecutionAreaBuilder;
import com.xgen.testing.mongot.index.lucene.explain.timing.TimingTestUtil;
import java.util.Arrays;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      ExplainTimingBreakdownTest.DeserializationTest.class,
      ExplainTimingBreakdownTest.SerializationTest.class,
    })
public class ExplainTimingBreakdownTest {
  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "explain-timing-breakdown-deserialization";
    private static final BsonDeserializationTestSuite<ExplainTimingBreakdown> TEST_SUITE =
        fromDocument(TimingTestUtil.RESOURCES_PATH, SUITE_NAME, ExplainTimingBreakdown::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<ExplainTimingBreakdown> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<ExplainTimingBreakdown> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<ExplainTimingBreakdown>>
        data() {
      return TEST_SUITE.withExamples(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<ExplainTimingBreakdown> simple() {
      QueryExecutionArea queryExecutionArea =
          QueryExecutionAreaBuilder.builder()
              .nanosElapsed(10)
              .invocationCounts(Map.of("first", 1L, "second", 2L))
              .build();

      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          ExplainTimingBreakdownBuilder.builder()
              .context(queryExecutionArea)
              .match(queryExecutionArea)
              .score(queryExecutionArea)
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "explain-timing-breakdown-serialization";
    private static final BsonSerializationTestSuite<ExplainTimingBreakdown> TEST_SUITE =
        fromEncodable(TimingTestUtil.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<ExplainTimingBreakdown> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<ExplainTimingBreakdown> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<ExplainTimingBreakdown>> data() {
      return Arrays.asList(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<ExplainTimingBreakdown> simple() {
      QueryExecutionArea queryExecutionArea =
          QueryExecutionAreaBuilder.builder()
              .nanosElapsed(10)
              .invocationCounts(Map.of("first", 1L, "second", 2L))
              .build();

      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          ExplainTimingBreakdownBuilder.builder()
              .context(queryExecutionArea)
              .match(queryExecutionArea)
              .score(queryExecutionArea)
              .build());
    }
  }
}
