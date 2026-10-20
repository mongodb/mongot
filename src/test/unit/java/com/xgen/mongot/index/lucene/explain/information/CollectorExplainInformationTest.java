package com.xgen.mongot.index.lucene.explain.information;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.lucene.explain.information.CollectStatsBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.FacetStatsBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.SortStatsBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      CollectorExplainInformationTest.ComparisonTest.class,
      CollectorExplainInformationTest.DeserializationTest.class,
      CollectorExplainInformationTest.SerializationTest.class,
    })
public class CollectorExplainInformationTest {
  public static class ComparisonTest {
    @Test
    public void equate_scopedBooleanClausesReordered_equal() {
      BsonDocument expected = childPlans();
      BsonDocument actual = childPlans();
      BsonArray filters = filters(actual);
      var first = filters.get(0);
      filters.set(0, filters.get(1));
      filters.set(1, first);

      boolean equal = equate(collector(expected), collector(actual));

      assertThat(equal).isTrue();
      assertThat(expected).isEqualTo(childPlans());
    }

    @Test
    public void equate_scopedPredicateChanged_notEqual() {
      BsonDocument actual = childPlans();
      filters(actual).get(1).asDocument().getDocument("args").put("value", new BsonString("other"));

      boolean equal = equate(collector(childPlans()), collector(actual));

      assertThat(equal).isFalse();
    }

    @Test
    public void equate_scopedClauseMissing_notEqual() {
      BsonDocument actual = childPlans();
      filters(actual).remove(0);

      boolean equal = equate(collector(childPlans()), collector(actual));

      assertThat(equal).isFalse();
    }

    @Test
    public void equate_scopePathChanged_notEqual() {
      BsonDocument actual = childPlans();
      actual.getDocument("variants.price").put("path", new BsonString("other"));

      boolean equal = equate(collector(childPlans()), collector(actual));

      assertThat(equal).isFalse();
    }

    @Test
    public void equate_scopedKeyMissing_notEqual() {
      boolean equal = equate(collector(childPlans()), collector(new BsonDocument()));

      assertThat(equal).isFalse();
    }

    @Test
    public void equate_unexpectedScopedPlans_notEqual() {
      var unscoped =
          new CollectorExplainInformation(
              Optional.empty(),
              Optional.empty(),
              Optional.of(
                  new SortStats(Optional.empty(), Map.of(), Optional.empty(), Optional.empty())));

      boolean equal = equate(unscoped, collector(childPlans()));

      assertThat(equal).isFalse();
    }

    @Test
    public void equate_otherSortMetadataChanged_notEqual() {
      var actual =
          new CollectorExplainInformation(
              Optional.empty(),
              Optional.empty(),
              Optional.of(
                  new SortStats(
                      Optional.empty(),
                      Map.of("other", List.of(FieldName.TypeField.TOKEN)),
                      Optional.empty(),
                      Optional.of(childPlans()))));

      boolean equal = equate(collector(childPlans()), actual);

      assertThat(equal).isFalse();
    }

    private static boolean equate(
        CollectorExplainInformation expected, CollectorExplainInformation actual) {
      return new ExplainInformationTestUtil.IgnoreTimingExplainInformationEquator<
              CollectorExplainInformation>()
          .equate(expected, actual);
    }

    private static CollectorExplainInformation collector(BsonDocument plans) {
      return new CollectorExplainInformation(
          Optional.empty(),
          Optional.empty(),
          Optional.of(
              new SortStats(Optional.empty(), Map.of(), Optional.empty(), Optional.of(plans))));
    }

    private static BsonArray filters(BsonDocument plans) {
      return plans
          .getDocument("variants.price")
          .getDocument("query")
          .getDocument("args")
          .getDocument("query")
          .getDocument("args")
          .getArray("filter");
    }

    private static BsonDocument childPlans() {
      return BsonDocument.parse(
          """
          {"variants.price":{"path":"variants","query":{
            "type":"ConstantScoreQuery","args":{"query":{
              "type":"BooleanQuery","args":{
                "must":[],"mustNot":[],"should":[],"minimumShouldMatch":0,
                "filter":[
                  {"type":"TermQuery","args":{"path":"embeddedPath","value":"variants"}},
                  {"type":"TermQuery","args":{"path":"fieldNames","value":"variants.price"}}
                ]
              }
            }}
          }}}
          """);
    }
  }

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "collect-stats-deserialization";

    private static final BsonDeserializationTestSuite<CollectorExplainInformation> TEST_SUITE =
        fromDocument(
            ExplainInformationTestUtil.RESOURCES_PATH,
            SUITE_NAME,
            CollectorExplainInformation::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<CollectorExplainInformation>
        testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<CollectorExplainInformation> testSpec) {
      this.testSpec = testSpec;
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<
            BsonDeserializationTestSuite.TestSpecWrapper<CollectorExplainInformation>>
        data() {
      return TEST_SUITE.withExamples(full());
    }

    private static BsonDeserializationTestSuite.ValidSpec<CollectorExplainInformation> full() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "full",
          CollectStatsBuilder.builder()
              .allCollectorStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .sortStats(
                  SortStatsBuilder.builder()
                      .profilingIteratorExecutionArea(
                          ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .sortExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .fieldInfos(
                          Map.of(
                              "foo", List.of(FieldName.TypeField.DATE, FieldName.TypeField.TOKEN)))
                      .build())
              .facetStats(
                  FacetStatsBuilder.builder()
                      .collectorStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .createFacetCountStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .stringFacetCardinalityInfo(
                          Map.of(
                              "foo",
                              new FacetStats.CardinalityInfo(5, 10),
                              "bar",
                              new FacetStats.CardinalityInfo(3, 6)))
                      .build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "collect-stats-serialization";

    private static final BsonSerializationTestSuite<CollectorExplainInformation> TEST_SUITE =
        fromEncodable(ExplainInformationTestUtil.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<CollectorExplainInformation> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<CollectorExplainInformation> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<CollectorExplainInformation>>
        data() {
      return Arrays.asList(full());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<CollectorExplainInformation> full() {
      return BsonSerializationTestSuite.TestSpec.create(
          "full",
          CollectStatsBuilder.builder()
              .allCollectorStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .sortStats(
                  SortStatsBuilder.builder()
                      .profilingIteratorExecutionArea(
                          ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .sortExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .fieldInfos(
                          Map.of(
                              "foo", List.of(FieldName.TypeField.DATE, FieldName.TypeField.TOKEN)))
                      .build())
              .facetStats(
                  FacetStatsBuilder.builder()
                      .collectorStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .createFacetCountStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .stringFacetCardinalityInfo(
                          Map.of(
                              "foo",
                              new FacetStats.CardinalityInfo(5, 10),
                              "bar",
                              new FacetStats.CardinalityInfo(3, 6)))
                      .build())
              .build());
    }
  }
}
