package com.xgen.mongot.index.lucene.explain.timing;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
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
      QueryExecutionAreaTest.DeserializationTest.class,
      QueryExecutionAreaTest.SerializationTest.class,
    })
public class QueryExecutionAreaTest {
  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "query-exec-area-deserialization";
    private static final BsonDeserializationTestSuite<QueryExecutionArea> TEST_SUITE =
        fromDocument(TimingTestUtil.RESOURCES_PATH, SUITE_NAME, QueryExecutionArea::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<QueryExecutionArea> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<QueryExecutionArea> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<QueryExecutionArea>>
        data() {
      return TEST_SUITE.withExamples(simple(), missingInvocationCounts());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<QueryExecutionArea> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          QueryExecutionAreaBuilder.builder()
              .nanosElapsed(10)
              .invocationCounts(Map.of("first", 1L, "second", 2L))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<QueryExecutionArea>
        missingInvocationCounts() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "missing-invocationCounts", QueryExecutionAreaBuilder.builder().nanosElapsed(10).build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "query-exec-area-serialization";
    private static final BsonSerializationTestSuite<QueryExecutionArea> TEST_SUITE =
        fromEncodable(TimingTestUtil.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<QueryExecutionArea> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<QueryExecutionArea> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<QueryExecutionArea>> data() {
      return Arrays.asList(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<QueryExecutionArea> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          QueryExecutionAreaBuilder.builder()
              .nanosElapsed(10)
              .invocationCounts(Map.of("first", 1L, "second", 2L))
              .build());
    }
  }
}
