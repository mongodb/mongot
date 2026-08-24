package com.xgen.mongot.server.command.search;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.aggregation.Operator;
import com.xgen.mongot.index.query.aggregation.SearchAggregationBootstrap;
import com.xgen.mongot.index.query.aggregation.TestOperatorSpecs;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.AggregationFacetDefinition;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.query.collectors.MetricSpec;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.server.command.search.definition.PlanShardedSearchCommandResponseDefinition;
import com.xgen.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinition;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.query.OperatorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.sort.SortFieldBuilder;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import com.xgen.testing.mongot.server.command.search.definition.ShardedSearchPlanBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ShardedSearchPlannerTest {

  private static final FeatureFlags DEFAULT_FEATURE_FLAGS = FeatureFlags.getDefault();
  private static final FeatureFlags AGGREGATION_ENABLED_FEATURE_FLAGS =
      FeatureFlags.withDefaults().enable(Feature.SEARCH_AGGREGATION).build();

  @BeforeClass
  public static void setUpAggregationRegistry() {
    TestOperatorSpecs.installRegistry();
  }

  /**
   * Restores the fake-spec registry after any test that swapped in the production one. Idempotent,
   * so it is safe for every test in the class.
   */
  @After
  public void restoreTestOperatorRegistry() {
    TestOperatorSpecs.installRegistry();
  }

  private static BsonDocument getOperatorQuery() {
    return new BsonDocument()
        .append(
            "text",
            new BsonDocument()
                .append("path", new BsonString("title"))
                .append("query", new BsonString("godfather")));
  }

  private static BsonDocument getOperatorQueryWithCount() {
    return new BsonDocument()
        .append(
            "text",
            new BsonDocument()
                .append("path", new BsonString("title"))
                .append("query", new BsonString("godfather")))
        .append(
            "count",
            new BsonDocument()
                .append("type", new BsonString("total"))
                .append("threshold", new BsonInt32(5000)));
  }

  private static BsonDocument getSimpleStringFacet() {
    return new BsonDocument(
        "directorFacet",
        new BsonDocument()
            .append("type", new BsonString("string"))
            .append("path", new BsonString("director")));
  }

  private static BsonDocument getStringFacetWithNumBuckets() {
    return new BsonDocument(
        "directorFacet",
        new BsonDocument()
            .append("type", new BsonString("string"))
            .append("path", new BsonString("director"))
            .append("numBuckets", new BsonInt32(1000)));
  }

  private static BsonDocument getSimpleNumericFacet() {
    return new BsonDocument(
        "ratingFacet",
        new BsonDocument()
            .append("type", new BsonString("number"))
            .append("path", new BsonString("rating"))
            .append("boundaries", new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2)))));
  }

  private static BsonDocument getSimpleDateFacet() {
    return new BsonDocument(
        "eventFacet",
        new BsonDocument()
            .append("type", new BsonString("date"))
            .append("path", new BsonString("start"))
            .append(
                "boundaries",
                new BsonArray(List.of(new BsonDateTime(2021), new BsonDateTime(2022)))));
  }

  private static BsonDocument getMultipleFacets() {
    return new BsonDocument()
        .append("directorFacet", getSimpleStringFacet().getDocument("directorFacet"))
        .append("ratingFacet", getSimpleNumericFacet().getDocument("ratingFacet"))
        .append("eventFacet", getSimpleDateFacet().getDocument("eventFacet"));
  }

  private static BsonDocument getMultipleStringFacets() {
    return new BsonDocument()
        .append("directorFacet", getSimpleStringFacet().getDocument("directorFacet"))
        .append(
            "genreFacet",
            new BsonDocument()
                .append("type", new BsonString("string"))
                .append("path", new BsonString("genre"))
                .append("numBuckets", new BsonInt32(50)))
        .append(
            "languageFacet",
            new BsonDocument()
                .append("type", new BsonString("string"))
                .append("path", new BsonString("language")));
  }

  private static BsonDocument createFacetQuery(BsonDocument facets) {
    return new BsonDocument()
        .append(
            "facet",
            new BsonDocument().append("operator", getOperatorQuery()).append("facets", facets));
  }

  private static BsonDocument createStringFacetDoc(String facetName, int numBuckets) {
    return new BsonDocument(
        facetName,
        new BsonArray(
            List.of(
                new BsonDocument(
                    "$match",
                    new BsonDocument()
                        .append("_id.type", new BsonDocument("$eq", new BsonString("facet")))
                        .append("_id.tag", new BsonDocument("$eq", new BsonString(facetName)))),
                new BsonDocument(
                    "$sort",
                    new BsonDocument("value", new BsonInt32(-1)).append("_id", new BsonInt32(1))),
                new BsonDocument("$limit", new BsonInt32(numBuckets)))));
  }

  private static BsonDocument createBucketDoc(String facetName) {
    return new BsonDocument(
        "buckets",
        new BsonDocument(
            "$map",
            new BsonDocument()
                .append("input", new BsonString("$" + facetName))
                .append("as", new BsonString("bucket"))
                .append(
                    "in",
                    new BsonDocument()
                        .append("_id", new BsonString("$$bucket._id.bucket"))
                        .append("count", new BsonString("$$bucket.value")))));
  }

  private static BsonDocument createNumericFacetDoc(String facetName) {
    return new BsonDocument(
        facetName,
        new BsonArray(
            List.of(
                new BsonDocument(
                    "$match",
                    new BsonDocument()
                        .append("_id.type", new BsonDocument("$eq", new BsonString("facet")))
                        .append("_id.tag", new BsonDocument("$eq", new BsonString(facetName)))),
                new BsonDocument("$sort", new BsonDocument("_id.bucket", new BsonInt32(1))))));
  }

  private static BsonDocument createDateFacetDoc(String facetName) {
    return new BsonDocument(
        facetName,
        new BsonArray(
            List.of(
                new BsonDocument(
                    "$match",
                    new BsonDocument()
                        .append("_id.type", new BsonDocument("$eq", new BsonString("facet")))
                        .append("_id.tag", new BsonDocument("$eq", new BsonString(facetName)))),
                new BsonDocument(
                    "$addFields",
                    new BsonDocument(
                        "sortField",
                        new BsonDocument(
                            "$convert",
                            new BsonDocument()
                                .append("input", new BsonString("$_id.bucket"))
                                .append("to", new BsonString("long"))
                                .append("onError", new BsonString("$_id.bucket"))))),
                new BsonDocument("$sort", new BsonDocument("sortField", new BsonInt32(1))))));
  }

  @Test
  public void testSortSpec() throws Exception {
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            SearchQuery.fromBson(getOperatorQuery()),
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            DEFAULT_FEATURE_FLAGS);
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan expected =
        ShardedSearchPlanBuilder.operator()
            .countType("lowerBound")
            .sortSpec(
                PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan.DEFAULT_SORT_SPEC)
            .build();
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testSortInQueryWithSearchFeature() throws ShardedSearchPlannerException {
    OperatorQuery query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.text().path("title").query("godfather").build())
            .returnStoredSource(false)
            .sort(
                SortSpecBuilder.builder()
                    .sortField(
                        SortFieldBuilder.builder()
                            .path("a")
                            .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                            .build())
                    .sortField(
                        SortFieldBuilder.builder()
                            .path("b")
                            .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                            .build())
                    .buildSort())
            .build();

    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            query, new PlanShardedSearchCommandDefinition.SearchFeatures(1), DEFAULT_FEATURE_FLAGS);
    BsonDocument expected =
        new BsonDocument()
            .append("$searchSortValues._0", new BsonInt32(1))
            .append("$searchSortValues._1", new BsonInt32(-1));
    Assert.assertEquals(expected, result.sortSpec);
  }

  @Test
  public void testSortInQueryWithoutSearchFeature() {
    OperatorQuery query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.text().path("title").query("godfather").build())
            .returnStoredSource(false)
            .sort(
                SortSpecBuilder.builder()
                    .sortField(
                        SortFieldBuilder.builder()
                            .path("a")
                            .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                            .build())
                    .sortField(
                        SortFieldBuilder.builder()
                            .path("b")
                            .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                            .build())
                    .buildSort())
            .build();

    TestUtils.assertThrows(
        "Sharded sort is not supported for this MongoDB deployment",
        ShardedSearchPlannerException.class,
        () ->
            ShardedSearchPlanner.planSearch(
                query,
                new PlanShardedSearchCommandDefinition.SearchFeatures(0),
                DEFAULT_FEATURE_FLAGS));
  }

  @Test
  public void testSortBetaInQuery() throws ShardedSearchPlannerException {
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            OperatorQueryBuilder.builder()
                .operator(OperatorBuilder.text().path("title").query("godfather").build())
                .returnStoredSource(false)
                .sortBetaV1(
                    SortSpecBuilder.builder()
                        .sortField(
                            SortFieldBuilder.builder()
                                .path("title")
                                .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                                .build())
                        .buildSortBetaV1())
                .build(),
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            DEFAULT_FEATURE_FLAGS);

    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan expected =
        ShardedSearchPlanBuilder.operator()
            .countType("lowerBound")
            .sortSpec(
                PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan.DEFAULT_SORT_SPEC)
            .build();
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testOperator() throws Exception {
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            SearchQuery.fromBson(getOperatorQuery()),
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            DEFAULT_FEATURE_FLAGS);
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan expected =
        ShardedSearchPlanBuilder.operator().countType("lowerBound").build();
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testOperatorWithCount() throws Exception {
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            SearchQuery.fromBson(getOperatorQueryWithCount()),
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            DEFAULT_FEATURE_FLAGS);
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan expected =
        ShardedSearchPlanBuilder.operator().countType("total").build();
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testStringFacet() throws Exception {
    var query = createFacetQuery(getSimpleStringFacet());
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            SearchQuery.fromBson(query),
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            DEFAULT_FEATURE_FLAGS);

    var facetDocs = List.of(createStringFacetDoc("directorFacet", 10));
    var bucketDoc = new BsonDocument("directorFacet", createBucketDoc("directorFacet"));
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan expected =
        ShardedSearchPlanBuilder.collector()
            .facetDocs(facetDocs)
            .bucketDoc(bucketDoc)
            .countType("lowerBound")
            .build();

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testStringFacetWithNumBuckets() throws Exception {
    var query = createFacetQuery(getStringFacetWithNumBuckets());
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            SearchQuery.fromBson(query),
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            DEFAULT_FEATURE_FLAGS);

    var facetDocs = List.of(createStringFacetDoc("directorFacet", 1000));
    var bucketDoc = new BsonDocument("directorFacet", createBucketDoc("directorFacet"));
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan expected =
        ShardedSearchPlanBuilder.collector()
            .facetDocs(facetDocs)
            .bucketDoc(bucketDoc)
            .countType("lowerBound")
            .build();

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testNumericFacet() throws Exception {
    var query = createFacetQuery(getSimpleNumericFacet());
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            SearchQuery.fromBson(query),
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            DEFAULT_FEATURE_FLAGS);

    var facetDocs = List.of(createNumericFacetDoc("ratingFacet"));
    var bucketDoc = new BsonDocument("ratingFacet", createBucketDoc("ratingFacet"));
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan expected =
        ShardedSearchPlanBuilder.collector()
            .facetDocs(facetDocs)
            .bucketDoc(bucketDoc)
            .countType("lowerBound")
            .build();

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testDateFacet() throws Exception {
    var query = createFacetQuery(getSimpleDateFacet());
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            SearchQuery.fromBson(query),
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            DEFAULT_FEATURE_FLAGS);

    var facetDocs = List.of(createDateFacetDoc("eventFacet"));
    var bucketDoc = new BsonDocument("eventFacet", createBucketDoc("eventFacet"));
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan expected =
        ShardedSearchPlanBuilder.collector()
            .facetDocs(facetDocs)
            .bucketDoc(bucketDoc)
            .countType("lowerBound")
            .build();

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testMultipleFacets() throws Exception {
    var query = createFacetQuery(getMultipleFacets());
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            SearchQuery.fromBson(query),
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            DEFAULT_FEATURE_FLAGS);

    var facetDocs =
        List.of(
            createStringFacetDoc("directorFacet", 10),
            createNumericFacetDoc("ratingFacet"),
            createDateFacetDoc("eventFacet"));
    var bucketDoc =
        new BsonDocument()
            .append("directorFacet", createBucketDoc("directorFacet"))
            .append("ratingFacet", createBucketDoc("ratingFacet"))
            .append("eventFacet", createBucketDoc("eventFacet"));
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan expected =
        ShardedSearchPlanBuilder.collector()
            .facetDocs(facetDocs)
            .bucketDoc(bucketDoc)
            .countType("lowerBound")
            .build();

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testMultipleStringFacets() throws Exception {
    var query = createFacetQuery(getMultipleStringFacets());
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            SearchQuery.fromBson(query),
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            DEFAULT_FEATURE_FLAGS);

    var facetDocs =
        List.of(
            createStringFacetDoc("directorFacet", 10),
            createStringFacetDoc("genreFacet", 50),
            createStringFacetDoc("languageFacet", 10));
    var bucketDoc =
        new BsonDocument()
            .append("directorFacet", createBucketDoc("directorFacet"))
            .append("genreFacet", createBucketDoc("genreFacet"))
            .append("languageFacet", createBucketDoc("languageFacet"));
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan expected =
        ShardedSearchPlanBuilder.collector()
            .facetDocs(facetDocs)
            .bucketDoc(bucketDoc)
            .countType("lowerBound")
            .build();

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testFlatSumAggregationMetaPipeline() throws Exception {
    CollectorQuery query = createFlatSumAggregationQuery();
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            query,
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            AGGREGATION_ENABLED_FEATURE_FLAGS);

    List<BsonDocument> pipeline = result.metaPipeline;
    Assert.assertEquals(3, pipeline.size());

    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "group_name": {
                  "$getField": {
                    "field": {"$literal": "$meta/group_name"},
                    "input": "$$ROOT"
                  }
                },
                "keys": {"$getField": {"field": {"$literal": "$meta/keys"}, "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "m0": {"$sum": "$m0"}
            }}
            """);
    Assert.assertEquals(expectedGroup, pipeline.get(0));

    BsonDocument expectedFacet =
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$sort": {"count_value": -1, "_id.keys.k0": 1}},
                {"$limit": 10},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "sum_price": "$m0"
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """);
    Assert.assertEquals(expectedFacet, pipeline.get(1));
  }

  private static CollectorQuery createFlatSumAggregationQuery() {
    AggregationFacetDefinition agg =
        AggregationFacetDefinition.create(
            new FacetDefinition.StringFacetDefinition("city", 10),
            Map.of(
                "sum_price", new MetricSpec(Operator.SUM, Optional.of(FieldPath.parse("price")))),
            Map.of());
    AggregationCollector collector =
        AggregationCollector.create(
            OperatorBuilder.text().path("title").query("godfather").build(),
            Map.of("by_city", agg));
    return new CollectorQuery(
        collector,
        "testIndex",
        Count.DEFAULT,
        Optional.empty(),
        Optional.empty(),
        false,
        false,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  @Test
  public void testFlatAvgAggregationMetaPipeline() throws Exception {
    CollectorQuery query = createFlatAvgAggregationQuery();
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            query,
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            AGGREGATION_ENABLED_FEATURE_FLAGS);

    List<BsonDocument> pipeline = result.metaPipeline;
    Assert.assertEquals(3, pipeline.size());

    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "group_name": {
                  "$getField": {
                    "field": {"$literal": "$meta/group_name"},
                    "input": "$$ROOT"
                  }
                },
                "keys": {"$getField": {"field": {"$literal": "$meta/keys"}, "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "m0": {"$sum": "$m0"},
              "m1": {"$sum": "$m1"}
            }}
            """);
    Assert.assertEquals(expectedGroup, pipeline.get(0));

    BsonDocument expectedFacet =
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$sort": {"count_value": -1, "_id.keys.k0": 1}},
                {"$limit": 10},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "avg_price": {
                    "$cond": {
                    "if": {"$eq": ["$m1", 0]},
                    "then": null,
                    "else": {"$divide": ["$m0", "$m1"]}
                    }
                  }
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """);
    Assert.assertEquals(expectedFacet, pipeline.get(1));
  }

  @Test
  public void testFlatBoundarySumAggregationMetaPipeline() throws Exception {
    CollectorQuery query = createFlatBoundarySumAggregationQuery();
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            query,
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            AGGREGATION_ENABLED_FEATURE_FLAGS);

    List<BsonDocument> pipeline = result.metaPipeline;
    Assert.assertEquals(3, pipeline.size());

    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "group_name": {
                  "$getField": {
                    "field": {"$literal": "$meta/group_name"},
                    "input": "$$ROOT"
                  }
                },
                "keys": {"$getField": {"field": {"$literal": "$meta/keys"}, "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "m0": {"$sum": "$m0"}
            }}
            """);
    Assert.assertEquals(expectedGroup, pipeline.get(0));

    BsonDocument expectedFacet =
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$sort": {"_id.keys.k0": 1}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "sum_price": "$m0"
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """);
    Assert.assertEquals(expectedFacet, pipeline.get(1));
  }

  @Test
  public void testFlatBoundaryAvgAggregationMetaPipeline() throws Exception {
    CollectorQuery query = createFlatBoundaryAvgAggregationQuery();
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            query,
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            AGGREGATION_ENABLED_FEATURE_FLAGS);

    List<BsonDocument> pipeline = result.metaPipeline;
    Assert.assertEquals(3, pipeline.size());

    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "group_name": {
                  "$getField": {
                    "field": {"$literal": "$meta/group_name"},
                    "input": "$$ROOT"
                  }
                },
                "keys": {"$getField": {"field": {"$literal": "$meta/keys"}, "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "m0": {"$sum": "$m0"},
              "m1": {"$sum": "$m1"}
            }}
            """);
    Assert.assertEquals(expectedGroup, pipeline.get(0));

    BsonDocument expectedFacet =
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$sort": {"_id.keys.k0": 1}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "avg_price": {
                    "$cond": {
                    "if": {"$eq": ["$m1", 0]},
                    "then": null,
                    "else": {"$divide": ["$m0", "$m1"]}
                    }
                  }
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """);
    Assert.assertEquals(expectedFacet, pipeline.get(1));
  }

  @Test
  public void testFlatDateBoundarySumAggregationMetaPipeline() throws Exception {
    CollectorQuery query = createFlatDateBoundarySumAggregationQuery();
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            query,
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            AGGREGATION_ENABLED_FEATURE_FLAGS);

    List<BsonDocument> pipeline = result.metaPipeline;
    Assert.assertEquals(3, pipeline.size());

    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "group_name": {
                  "$getField": {
                    "field": {"$literal": "$meta/group_name"},
                    "input": "$$ROOT"
                  }
                },
                "keys": {"$getField": {"field": {"$literal": "$meta/keys"}, "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "m0": {"$sum": "$m0"}
            }}
            """);
    Assert.assertEquals(expectedGroup, pipeline.get(0));

    BsonDocument expectedFacet =
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$sort": {"_id.keys.k0": 1}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "sum_price": "$m0"
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """);
    Assert.assertEquals(expectedFacet, pipeline.get(1));
  }

  @Test
  public void testFlatDateBoundarySumWithDefaultAggregationMetaPipeline() throws Exception {
    CollectorQuery query = createFlatDateBoundarySumWithDefaultAggregationQuery();
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            query,
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            AGGREGATION_ENABLED_FEATURE_FLAGS);

    List<BsonDocument> pipeline = result.metaPipeline;
    Assert.assertEquals(3, pipeline.size());

    BsonDocument expectedFacet =
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$addFields": {
                  "__boundarySortRank": {
                    "$cond": [
                      {"$eq": ["$_id.keys.k0", "other"]},
                      1,
                      0
                    ]
                  }
                }},
                {"$sort": {"__boundarySortRank": 1, "_id.keys.k0": 1}},
                {"$unset": "__boundarySortRank"},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "sum_price": "$m0"
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """);
    Assert.assertEquals(expectedFacet, pipeline.get(1));
  }

  private static CollectorQuery createFlatBoundarySumAggregationQuery() {
    AggregationFacetDefinition agg =
        AggregationFacetDefinition.create(
            new FacetDefinition.NumericFacetDefinition(
                "price",
                Optional.empty(),
                List.of(
                    new BsonInt32(0), new BsonInt32(100), new BsonInt32(200), new BsonInt32(300))),
            Map.of(
                "sum_price", new MetricSpec(Operator.SUM, Optional.of(FieldPath.parse("price")))),
            Map.of());
    AggregationCollector collector =
        AggregationCollector.create(
            OperatorBuilder.text().path("title").query("godfather").build(), Map.of("agg", agg));
    return new CollectorQuery(
        collector,
        "default",
        Count.DEFAULT,
        Optional.empty(),
        Optional.empty(),
        false,
        false,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  /**
   * Boundary grouping with {@code min}. Query-only merge planning is keyed on the grouping rather
   * than the metric, so min/max inherit the same boundary key sort as {@code sum}/{@code avg}.
   */
  @Test
  public void testFlatBoundaryMinAggregationMetaPipeline() throws Exception {
    assertBoundaryMinMaxMetaPipeline(Operator.MIN, "min_price", "$min");
  }

  /**
   * Boundary grouping with {@code max}; see {@link #testFlatBoundaryMinAggregationMetaPipeline}.
   */
  @Test
  public void testFlatBoundaryMaxAggregationMetaPipeline() throws Exception {
    assertBoundaryMinMaxMetaPipeline(Operator.MAX, "max_price", "$max");
  }

  private static void assertBoundaryMinMaxMetaPipeline(
      Operator operator, String metricName, String mergeOperator) throws Exception {
    // The production specs, not TestOperatorSpecs' fakes: the point is the real merge shape.
    SearchAggregationBootstrap.installRegistry();
    CollectorQuery query = createFlatBoundaryMinMaxAggregationQuery(operator, metricName);
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            query,
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            AGGREGATION_ENABLED_FEATURE_FLAGS);

    List<BsonDocument> pipeline = result.metaPipeline;
    Assert.assertEquals(3, pipeline.size());

    // Metrics are aliased positionally in the merge pipeline; the request name only reappears in
    // the final $project.
    String partial = "m0";
    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "group_name": {
                  "$getField": {
                    "field": {"$literal": "$meta/group_name"},
                    "input": "$$ROOT"
                  }
                },
                "keys": {"$getField": {"field": {"$literal": "$meta/keys"}, "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "%s": {"%s": "$%s"}
            }}
            """
                .formatted(partial, mergeOperator, partial));
    Assert.assertEquals(expectedGroup, pipeline.get(0));

    // Boundary buckets order by the request's boundary list (key ascending), not by doc count.
    BsonDocument expectedFacet =
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$sort": {"_id.keys.k0": 1}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "%s": {"$cond": {
                    "if": {"$eq": [{"$type": "$%s"}, "minKey"]},
                    "then": null,
                    "else": "$%s"
                  }}
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """
                .formatted(metricName, partial, partial));
    Assert.assertEquals(expectedFacet, pipeline.get(1));
  }

  private static CollectorQuery createFlatBoundaryMinMaxAggregationQuery(
      Operator operator, String metricName) {
    AggregationFacetDefinition agg =
        AggregationFacetDefinition.create(
            new FacetDefinition.NumericFacetDefinition(
                "price",
                Optional.empty(),
                List.of(
                    new BsonInt32(0), new BsonInt32(100), new BsonInt32(200), new BsonInt32(300))),
            Map.of(metricName, new MetricSpec(operator, Optional.of(FieldPath.parse("price")))),
            Map.of());
    AggregationCollector collector =
        AggregationCollector.create(
            OperatorBuilder.text().path("title").query("godfather").build(), Map.of("agg", agg));
    return new CollectorQuery(
        collector,
        "default",
        Count.DEFAULT,
        Optional.empty(),
        Optional.empty(),
        false,
        false,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  private static CollectorQuery createFlatBoundaryAvgAggregationQuery() {
    AggregationFacetDefinition agg =
        AggregationFacetDefinition.create(
            new FacetDefinition.NumericFacetDefinition(
                "price",
                Optional.empty(),
                List.of(
                    new BsonInt32(0), new BsonInt32(100), new BsonInt32(200), new BsonInt32(300))),
            Map.of(
                "avg_price", new MetricSpec(Operator.AVG, Optional.of(FieldPath.parse("price")))),
            Map.of());
    AggregationCollector collector =
        AggregationCollector.create(
            OperatorBuilder.text().path("title").query("godfather").build(), Map.of("agg", agg));
    return new CollectorQuery(
        collector,
        "default",
        Count.DEFAULT,
        Optional.empty(),
        Optional.empty(),
        false,
        false,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  private static CollectorQuery createFlatDateBoundarySumAggregationQuery() {
    AggregationFacetDefinition agg =
        AggregationFacetDefinition.create(
            new FacetDefinition.DateFacetDefinition(
                "release",
                Optional.empty(),
                List.of(
                    new BsonDateTime(0L),
                    new BsonDateTime(1_000L),
                    new BsonDateTime(2_000L),
                    new BsonDateTime(3_000L))),
            Map.of(
                "sum_price", new MetricSpec(Operator.SUM, Optional.of(FieldPath.parse("price")))),
            Map.of());
    AggregationCollector collector =
        AggregationCollector.create(
            OperatorBuilder.text().path("title").query("godfather").build(), Map.of("agg", agg));
    return new CollectorQuery(
        collector,
        "default",
        Count.DEFAULT,
        Optional.empty(),
        Optional.empty(),
        false,
        false,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  private static CollectorQuery createFlatDateBoundarySumWithDefaultAggregationQuery() {
    AggregationFacetDefinition agg =
        AggregationFacetDefinition.create(
            new FacetDefinition.DateFacetDefinition(
                "release",
                Optional.of("other"),
                List.of(
                    new BsonDateTime(0L),
                    new BsonDateTime(1_000L),
                    new BsonDateTime(2_000L),
                    new BsonDateTime(3_000L))),
            Map.of(
                "sum_price", new MetricSpec(Operator.SUM, Optional.of(FieldPath.parse("price")))),
            Map.of());
    AggregationCollector collector =
        AggregationCollector.create(
            OperatorBuilder.text().path("title").query("godfather").build(), Map.of("agg", agg));
    return new CollectorQuery(
        collector,
        "default",
        Count.DEFAULT,
        Optional.empty(),
        Optional.empty(),
        false,
        false,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  @Test
  public void planSearch_aggregationFeatureFlagDisabled_throwsShardedSearchPlannerException() {
    CollectorQuery query = createFlatAvgAggregationQuery();

    TestUtils.assertThrows(
        "Aggregation is not enabled. Please contact support.",
        ShardedSearchPlannerException.class,
        () ->
            ShardedSearchPlanner.planSearch(
                query,
                new PlanShardedSearchCommandDefinition.SearchFeatures(0),
                DEFAULT_FEATURE_FLAGS));
  }

  @Test
  public void testFlatMinAggregationMetaPipeline() throws Exception {
    // The production specs, not TestOperatorSpecs' fakes: the point is the real $min merge shape.
    SearchAggregationBootstrap.installRegistry();
    CollectorQuery query = createFlatMinMaxAggregationQuery(Operator.MIN, "min_price");
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            query,
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            AGGREGATION_ENABLED_FEATURE_FLAGS);

    List<BsonDocument> pipeline = result.metaPipeline;
    Assert.assertEquals(3, pipeline.size());

    // mongod $min ignores BSON null, so shards encode a real null as MinKey and the $project
    // below turns that stand-in back into null. Missing extrema are omitted, not encoded.

    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "group_name": {
                  "$getField": {
                    "field": {"$literal": "$meta/group_name"},
                    "input": "$$ROOT"
                  }
                },
                "keys": {"$getField": {"field": {"$literal": "$meta/keys"}, "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "m0": {"$min": "$m0"}
            }}
            """);
    Assert.assertEquals(expectedGroup, pipeline.get(0));

    BsonDocument expectedFacet =
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$sort": {"count_value": -1, "_id.keys.k0": 1}},
                {"$limit": 10},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "min_price": {"$cond": {
                    "if": {"$eq": [{"$type": "$m0"}, "minKey"]},
                    "then": null,
                    "else": "$m0"
                  }}
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """);
    Assert.assertEquals(expectedFacet, pipeline.get(1));
  }

  @Test
  public void testFlatMaxAggregationMetaPipeline() throws Exception {
    // The production specs, not TestOperatorSpecs' fakes: the point is the real $max merge shape.
    SearchAggregationBootstrap.installRegistry();
    CollectorQuery query = createFlatMinMaxAggregationQuery(Operator.MAX, "max_price");
    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
        ShardedSearchPlanner.planSearch(
            query,
            new PlanShardedSearchCommandDefinition.SearchFeatures(0),
            AGGREGATION_ENABLED_FEATURE_FLAGS);

    List<BsonDocument> pipeline = result.metaPipeline;
    Assert.assertEquals(3, pipeline.size());

    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "group_name": {
                  "$getField": {
                    "field": {"$literal": "$meta/group_name"},
                    "input": "$$ROOT"
                  }
                },
                "keys": {"$getField": {"field": {"$literal": "$meta/keys"}, "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "m0": {"$max": "$m0"}
            }}
            """);
    Assert.assertEquals(expectedGroup, pipeline.get(0));

    BsonDocument expectedFacet =
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$sort": {"count_value": -1, "_id.keys.k0": 1}},
                {"$limit": 10},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "max_price": {"$cond": {
                    "if": {"$eq": [{"$type": "$m0"}, "minKey"]},
                    "then": null,
                    "else": "$m0"
                  }}
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """);
    Assert.assertEquals(expectedFacet, pipeline.get(1));
  }

  private static CollectorQuery createFlatMinMaxAggregationQuery(
      Operator operator, String metricName) {
    AggregationFacetDefinition agg =
        AggregationFacetDefinition.create(
            new FacetDefinition.StringFacetDefinition("city", 10),
            Map.of(metricName, new MetricSpec(operator, Optional.of(FieldPath.parse("price")))),
            Map.of());
    AggregationCollector collector =
        AggregationCollector.create(
            OperatorBuilder.text().path("title").query("godfather").build(),
            Map.of("by_city", agg));
    return new CollectorQuery(
        collector,
        "testIndex",
        Count.DEFAULT,
        Optional.empty(),
        Optional.empty(),
        false,
        false,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  @Test
  public void planSearch_aggregationFeatureFlagDisabled_producesCommandErrorResponse() {
    CollectorQuery query = createFlatAvgAggregationQuery();

    ShardedSearchPlannerException exception =
        Assert.assertThrows(
            ShardedSearchPlannerException.class,
            () ->
                ShardedSearchPlanner.planSearch(
                    query,
                    new PlanShardedSearchCommandDefinition.SearchFeatures(0),
                    DEFAULT_FEATURE_FLAGS));

    BsonDocument errorBody = MessageUtils.createErrorBody(exception);
    Assert.assertEquals(0, errorBody.getInt32("ok").getValue());
    Assert.assertEquals(
        "Aggregation is not enabled. Please contact support.",
        errorBody.getString("errmsg").getValue());
  }

  /**
   * A sorted {@code first} merges across shards with {@code $top} over the sort key each shard put
   * on its bucket rows -- the query's own sort directions, since the key is emitted the query's way
   * round for both operators.
   */
  @Test
  public void planSearch_sortedFirstAggregation_mergesWithTopOverTheSortKey() throws Exception {
    SearchAggregationBootstrap.installRegistry();
    try {
      PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
          ShardedSearchPlanner.planSearch(
              createFlatFirstLastAggregationQuery(Operator.FIRST, Optional.of(priceThenRating())),
              new PlanShardedSearchCommandDefinition.SearchFeatures(1),
              AGGREGATION_ENABLED_FEATURE_FLAGS);

      Assert.assertEquals(
          BsonDocument.parse(
              """
              {"$top": {
                "output": {"$ifNull": ["$m0", null]},
                "sortBy": {
                  "m1": -1,
                  "m2._0": 1,
                  "m2._1": -1
                }
              }}
              """),
          result.metaPipeline.get(0).getDocument("$group").getDocument("m3"));
    } finally {
      TestOperatorSpecs.installRegistry();
    }
  }

  /** {@code last} takes the other end of the same key. */
  @Test
  public void planSearch_sortedLastAggregation_mergesWithBottomOverTheSortKey() throws Exception {
    SearchAggregationBootstrap.installRegistry();
    try {
      PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
          ShardedSearchPlanner.planSearch(
              createFlatFirstLastAggregationQuery(Operator.LAST, Optional.of(priceThenRating())),
              new PlanShardedSearchCommandDefinition.SearchFeatures(1),
              AGGREGATION_ENABLED_FEATURE_FLAGS);

      Assert.assertEquals(
          BsonDocument.parse(
              """
              {"$bottom": {
                "output": {"$ifNull": ["$m0", null]},
                "sortBy": {
                  "m1": 1,
                  "m2._0": 1,
                  "m2._1": -1
                }
              }}
              """),
          result.metaPipeline.get(0).getDocument("$group").getDocument("m3"));
    } finally {
      TestOperatorSpecs.installRegistry();
    }
  }

  /**
   * With no sort the shards send no key, so the merge takes one shard's winner rather than ordering
   * by a field that is not there.
   */
  @Test
  public void planSearch_unsortedFirstAggregation_mergesWithFirst() throws Exception {
    SearchAggregationBootstrap.installRegistry();
    try {
      PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan result =
          ShardedSearchPlanner.planSearch(
              createFlatFirstLastAggregationQuery(Operator.FIRST, Optional.empty()),
              new PlanShardedSearchCommandDefinition.SearchFeatures(1),
              AGGREGATION_ENABLED_FEATURE_FLAGS);

      Assert.assertEquals(
          BsonDocument.parse(
              """
              {"$top": {
                "output": {"$ifNull": ["$m0", null]},
                "sortBy": {"m1": -1}
              }}
              """),
          result.metaPipeline.get(0).getDocument("$group").getDocument("m2"));
    } finally {
      TestOperatorSpecs.installRegistry();
    }
  }

  /** Price ascending, then rating descending. */
  private static Sort priceThenRating() {
    return SortSpecBuilder.builder()
        .sortField(
            SortFieldBuilder.builder()
                .path("price")
                .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                .build())
        .sortField(
            SortFieldBuilder.builder()
                .path("rating")
                .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                .build())
        .buildSort();
  }

  private static CollectorQuery createFlatFirstLastAggregationQuery(
      Operator operator, Optional<SortSpec> sortSpec) {
    AggregationFacetDefinition agg =
        AggregationFacetDefinition.create(
            new FacetDefinition.StringFacetDefinition("city", 10),
            Map.of("metric", new MetricSpec(operator, Optional.of(FieldPath.parse("color")))),
            Map.of());
    AggregationCollector collector =
        AggregationCollector.create(
            OperatorBuilder.text().path("title").query("godfather").build(),
            Map.of("by_city", agg));
    return new CollectorQuery(
        collector,
        "testIndex",
        Count.DEFAULT,
        Optional.empty(),
        Optional.empty(),
        false,
        false,
        false,
        sortSpec,
        Optional.empty(),
        Optional.empty());
  }

  private static CollectorQuery createFlatAvgAggregationQuery() {
    AggregationFacetDefinition agg =
        AggregationFacetDefinition.create(
            new FacetDefinition.StringFacetDefinition("city", 10),
            Map.of(
                "avg_price", new MetricSpec(Operator.AVG, Optional.of(FieldPath.parse("price")))),
            Map.of());
    AggregationCollector collector =
        AggregationCollector.create(
            OperatorBuilder.text().path("title").query("godfather").build(),
            Map.of("by_city", agg));
    return new CollectorQuery(
        collector,
        "testIndex",
        Count.DEFAULT,
        Optional.empty(),
        Optional.empty(),
        false,
        false,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }
}
