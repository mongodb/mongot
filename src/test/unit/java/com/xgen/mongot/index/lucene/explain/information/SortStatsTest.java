package com.xgen.mongot.index.lucene.explain.information;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.google.common.collect.ImmutableSetMultimap;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.SortStatsBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      SortStatsTest.ClassTest.class,
      SortStatsTest.DeserializationTest.class,
      SortStatsTest.SerializationTest.class,
    })
public class SortStatsTest {
  public static class ClassTest {
    @Test
    public void testCreate() {
      SortStats expected =
          SortStatsBuilder.builder()
              .profilingIteratorExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .sortExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .fieldInfos(
                  Map.of("foo", List.of(FieldName.TypeField.DATE, FieldName.TypeField.TOKEN)))
              .usesIndexSort(true)
              .build();

      SortStats result =
          SortStats.create(
              Optional.of(ExplainInformationTestUtil.QUERY_EXECUTION_AREA),
              Optional.of(ExplainInformationTestUtil.QUERY_EXECUTION_AREA),
              ImmutableSetMultimap.of(
                  FieldPath.parse("foo"),
                  FieldName.TypeField.DATE,
                  FieldPath.parse("foo"),
                  FieldName.TypeField.TOKEN),
              Optional.of(true));

      Assert.assertEquals(expected, result);
    }
  }

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "sort-stats-deserialization";

    private static final BsonDeserializationTestSuite<SortStats> TEST_SUITE =
        fromDocument(ExplainInformationTestUtil.RESOURCES_PATH, SUITE_NAME, SortStats::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<SortStats> testSpec;

    public DeserializationTest(BsonDeserializationTestSuite.TestSpecWrapper<SortStats> testSpec) {
      this.testSpec = testSpec;
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<SortStats>> data() {
      return TEST_SUITE.withExamples(
          full(),
          fullWithUsesIndexSort());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortStats> full() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "full",
          SortStatsBuilder.builder()
              .profilingIteratorExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .sortExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .fieldInfos(
                  Map.of("foo", List.of(FieldName.TypeField.DATE, FieldName.TypeField.TOKEN)))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortStats>
        fullWithUsesIndexSort() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "full-with-uses-index-sort",
          SortStatsBuilder.builder()
              .profilingIteratorExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .sortExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .fieldInfos(
                  Map.of("foo", List.of(FieldName.TypeField.DATE, FieldName.TypeField.TOKEN)))
              .usesIndexSort(true)
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "sort-stats-serialization";

    private static final BsonSerializationTestSuite<SortStats> TEST_SUITE =
        fromEncodable(ExplainInformationTestUtil.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<SortStats> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<SortStats> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<SortStats>> data() {
      return List.of(
          full(),
          fullWithUsesIndexSort());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<SortStats> full() {
      return BsonSerializationTestSuite.TestSpec.create(
          "full",
          SortStatsBuilder.builder()
              .profilingIteratorExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .sortExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .fieldInfos(
                  Map.of("foo", List.of(FieldName.TypeField.DATE, FieldName.TypeField.TOKEN)))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SortStats>
        fullWithUsesIndexSort() {
      return BsonSerializationTestSuite.TestSpec.create(
          "full-with-uses-index-sort",
          SortStatsBuilder.builder()
              .profilingIteratorExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .sortExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .fieldInfos(
                  Map.of("foo", List.of(FieldName.TypeField.DATE, FieldName.TypeField.TOKEN)))
              .usesIndexSort(true)
              .build());
    }
  }
}
