package com.xgen.mongot.index.lucene.explain.information;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.PointRangeQueryBuilder;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      PointRangeQuerySpecTest.DeserializationTest.class,
      PointRangeQuerySpecTest.SerializationTest.class,
    })
public class PointRangeQuerySpecTest {
  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "point-range-query-deserialization";
    private static final BsonDeserializationTestSuite<PointRangeQuerySpec> TEST_SUITE =
        fromDocument(
            ExplainInformationTestUtil.RESOURCES_PATH, SUITE_NAME, PointRangeQuerySpec::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<PointRangeQuerySpec> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<PointRangeQuerySpec> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<PointRangeQuerySpec>>
        data() {
      return TEST_SUITE.withExamples(simple(), missingRepresentation());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<PointRangeQuerySpec> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          PointRangeQueryBuilder.builder()
              .path("a")
              .representation(Representation.INT_64)
              .greaterThan(new LongPoint(1L))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<PointRangeQuerySpec>
        missingRepresentation() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "missing-representation",
          PointRangeQueryBuilder.builder().path("a").greaterThan(new LongPoint(1L)).build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "point-range-query-serialization";
    private static final BsonSerializationTestSuite<PointRangeQuerySpec> TEST_SUITE =
        fromEncodable(ExplainInformationTestUtil.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<PointRangeQuerySpec> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<PointRangeQuerySpec> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<PointRangeQuerySpec>> data() {
      return Arrays.asList(simple(), missingRepresentation());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<PointRangeQuerySpec> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          PointRangeQueryBuilder.builder()
              .path("a")
              .representation(Representation.DOUBLE)
              .greaterThan(new DoublePoint(1.0))
              .lessThan(new DoublePoint(2.0))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<PointRangeQuerySpec>
        missingRepresentation() {
      return BsonSerializationTestSuite.TestSpec.create(
          "missing-representation",
          PointRangeQueryBuilder.builder()
              .path("a")
              .greaterThan(new DoublePoint(1.0))
              .lessThan(new DoublePoint(2.0))
              .build());
    }
  }
}
