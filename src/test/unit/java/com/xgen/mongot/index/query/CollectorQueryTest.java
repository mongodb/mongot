package com.xgen.mongot.index.query;

import static com.xgen.testing.BsonDeserializationTestSuite.fromRootDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.AggregationFacetDefinition;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.query.collectors.MetricSpec;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.query.CollectorQueryBuilder;
import com.xgen.testing.mongot.index.query.collectors.CollectorBuilder;
import com.xgen.testing.mongot.index.query.collectors.FacetDefinitionBuilder;
import com.xgen.testing.mongot.index.query.counts.CountBuilder;
import com.xgen.testing.mongot.index.query.highlights.UnresolvedHighlightBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      CollectorQueryTest.DeserializationTest.class,
      CollectorQueryTest.SerializationTest.class
    })
public class CollectorQueryTest {
  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "collector-query-deserialization";
    private static final BsonDeserializationTestSuite<Query> TEST_SUITE =
        fromRootDocument("src/test/unit/resources/index/query/", SUITE_NAME, SearchQuery::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<Query> testSpec;

    public DeserializationTest(BsonDeserializationTestSuite.TestSpecWrapper<Query> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<Query>> data() {
      return TEST_SUITE.withExamples(
          simple(),
          explicitIndex(),
          withHighlights(),
          withCount(),
          withReturnStoredSource(),
          withReturnStoredSourceAndReturnScope(),
          withScoreDetails(),
          withTracking(),
          withSearchNodePreference());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Query> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          CollectorQueryBuilder.builder()
              .collector(
                  CollectorBuilder.facet()
                      .operator(OperatorBuilder.text().path("review").query("good").build())
                      .facetDefinitions(
                          Map.of(
                              "directorFacet",
                              FacetDefinitionBuilder.string()
                                  .numBuckets(5)
                                  .path("director")
                                  .build(),
                              "ratingFacet",
                              FacetDefinitionBuilder.numeric()
                                  .boundaries(List.of(new BsonInt32(1), new BsonInt32(2)))
                                  .path("rating")
                                  .build()))
                      .build())
              .returnStoredSource(false)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Query> explicitIndex() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit index",
          CollectorQueryBuilder.builder()
              .index("my-index")
              .collector(
                  CollectorBuilder.facet()
                      .operator(OperatorBuilder.text().path("review").query("good").build())
                      .facetDefinitions(
                          Map.of(
                              "directorFacet",
                              FacetDefinitionBuilder.string()
                                  .numBuckets(5)
                                  .path("director")
                                  .build()))
                      .build())
              .returnStoredSource(false)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Query> withHighlights() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with highlights",
          CollectorQueryBuilder.builder()
              .collector(
                  CollectorBuilder.facet()
                      .operator(OperatorBuilder.text().path("review").query("good").build())
                      .facetDefinitions(
                          Map.of(
                              "directorFacet",
                              FacetDefinitionBuilder.string().path("director").build()))
                      .build())
              .highlight(UnresolvedHighlightBuilder.builder().path("review").build())
              .returnStoredSource(false)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Query> withCount() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with count",
          CollectorQueryBuilder.builder()
              .collector(
                  CollectorBuilder.facet()
                      .operator(OperatorBuilder.text().path("review").query("good").build())
                      .facetDefinitions(
                          Map.of(
                              "directorFacet",
                              FacetDefinitionBuilder.string().path("director").build()))
                      .build())
              .count(CountBuilder.builder().type(Count.Type.TOTAL).threshold(5000).build())
              .returnStoredSource(false)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Query> withReturnStoredSource() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with explicit returnStoredSource",
          CollectorQueryBuilder.builder()
              .collector(
                  CollectorBuilder.facet()
                      .operator(OperatorBuilder.text().path("review").query("good").build())
                      .facetDefinitions(
                          Map.of(
                              "directorFacet",
                              FacetDefinitionBuilder.string().path("director").build()))
                      .build())
              .returnStoredSource(true)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Query>
        withReturnStoredSourceAndReturnScope() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with explicit returnStoredSource and returnScope",
          CollectorQueryBuilder.builder()
              .collector(
                  CollectorBuilder.facet()
                      .operator(
                          OperatorBuilder.text().path("my-scope.review").query("good").build())
                      .facetDefinitions(
                          Map.of(
                              "directorFacet",
                              FacetDefinitionBuilder.string().path("my-scope.director").build()))
                      .build())
              .returnStoredSource(true)
              .returnScope(new ReturnScope(FieldPath.parse("my-scope")))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Query> withScoreDetails() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with explicit scoreDetails",
          CollectorQueryBuilder.builder()
              .collector(
                  CollectorBuilder.facet()
                      .operator(OperatorBuilder.text().path("review").query("good").build())
                      .facetDefinitions(
                          Map.of(
                              "directorFacet",
                              FacetDefinitionBuilder.string().path("director").build()))
                      .build())
              .returnStoredSource(false)
              .scoreDetails(true)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Query> withTracking() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with tracking",
          CollectorQueryBuilder.builder()
              .collector(
                  CollectorBuilder.facet()
                      .operator(OperatorBuilder.text().path("review").query("good").build())
                      .facetDefinitions(
                          Map.of(
                              "directorFacet",
                              FacetDefinitionBuilder.string()
                                  .numBuckets(5)
                                  .path("director")
                                  .build(),
                              "ratingFacet",
                              FacetDefinitionBuilder.numeric()
                                  .boundaries(List.of(new BsonInt32(1), new BsonInt32(2)))
                                  .path("rating")
                                  .build()))
                      .build())
              .returnStoredSource(false)
              .tracking(new Tracking("baz"))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Query> withSearchNodePreference() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with searchNodePreference",
          CollectorQueryBuilder.builder()
              .collector(
                  CollectorBuilder.facet()
                      .operator(OperatorBuilder.text().path("review").query("good").build())
                      .facetDefinitions(
                          Map.of(
                              "directorFacet",
                              FacetDefinitionBuilder.string()
                                  .numBuckets(5)
                                  .path("director")
                                  .build(),
                              "ratingFacet",
                              FacetDefinitionBuilder.numeric()
                                  .boundaries(List.of(new BsonInt32(1), new BsonInt32(2)))
                                  .path("rating")
                                  .build()))
                      .build())
              .returnStoredSource(false)
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "collector-query-serialization";
    private static final BsonSerializationTestSuite<Query> TEST_SUITE =
        fromEncodable("src/test/unit/resources/index/query/", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<Query> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<Query> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<Query>> data() {
      return List.of(collectorQuery(), aggregationCollectorWithSort());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<Query> collectorQuery() {
      return BsonSerializationTestSuite.TestSpec.create(
          "collector-query",
          CollectorQueryBuilder.builder()
              .collector(
                  CollectorBuilder.facet()
                      .operator(OperatorBuilder.text().path("review").query("good").build())
                      .facetDefinitions(
                          Map.of(
                              "directorFacet",
                              FacetDefinitionBuilder.string()
                                  .numBuckets(50)
                                  .path("director")
                                  .build(),
                              "ratingFacet",
                              FacetDefinitionBuilder.numeric()
                                  .boundaries(List.of(new BsonDouble(1.12), new BsonDouble(2.64)))
                                  .path("rating")
                                  .build(),
                              "eventFacet",
                              FacetDefinitionBuilder.date()
                                  .boundaries(
                                      List.of(
                                          new BsonDateTime(
                                              new Calendar.Builder()
                                                  .setDate(2020, Calendar.MAY, 8)
                                                  .setTimeOfDay(12, 11, 0, 0)
                                                  .setTimeZone(TimeZone.getTimeZone("UTC"))
                                                  .build()
                                                  .getTimeInMillis()),
                                          new BsonDateTime(
                                              new Calendar.Builder()
                                                  .setDate(2021, Calendar.MAY, 8)
                                                  .setTimeOfDay(12, 11, 0, 0)
                                                  .setTimeZone(TimeZone.getTimeZone("UTC"))
                                                  .build()
                                                  .getTimeInMillis())))
                                  .path("start")
                                  .build()))
                      .build())
              .index("my-index")
              .count(CountBuilder.builder().type(Count.Type.TOTAL).build())
              .highlight(UnresolvedHighlightBuilder.builder().path("my-path").build())
              .returnStoredSource(true)
              .concurrent(true)
              .scoreDetails(true)
              .tracking(new Tracking("par"))
              .returnScope(new ReturnScope(FieldPath.parse("my-scope")))
              .build());
    }

    /**
     * The global {@code sort} on an aggregation collector is serialized as a top-level sibling of
     * the {@code facet} document (mirroring how {@code $search}/{@code $searchMeta} emit sort for
     * an operator query), never inside {@code facet}. This is the query-level analog of {@code
     * AggregationCollectorParseTest#collectorToBson_omitsBorrowedSortSpec}.
     */
    private static BsonSerializationTestSuite.TestSpec<Query> aggregationCollectorWithSort() {
      AggregationCollector collector =
          AggregationCollector.create(
                  AllDocumentsOperator.INSTANCE,
                  Map.of(
                      "byCategory",
                      AggregationFacetDefinition.create(
                          new FacetDefinition.StringFacetDefinition("category", 10),
                          Map.of(
                              "total",
                              new MetricSpec(
                                  com.xgen.mongot.index.query.aggregation.Operator.SUM,
                                  java.util.Optional.of(FieldPath.parse("price")))),
                          Map.of())))
              .withSortSpec(
                  java.util.Optional.of(
                      new Sort(
                          ImmutableList.of(
                              new MongotSortField(
                                  FieldPath.newRoot("price"), UserFieldSortOptions.DEFAULT_ASC)))));
      return BsonSerializationTestSuite.TestSpec.create(
          "aggregation-collector-with-sort",
          CollectorQueryBuilder.builder()
              .collector(collector)
              .index("my-index")
              .sort(
                  new Sort(
                      ImmutableList.of(
                          new MongotSortField(
                              FieldPath.newRoot("price"), UserFieldSortOptions.DEFAULT_ASC))))
              .returnStoredSource(false)
              .build());
    }
  }
}
