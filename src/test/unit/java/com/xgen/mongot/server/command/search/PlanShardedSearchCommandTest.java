package com.xgen.mongot.server.command.search;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.query.aggregation.TestOperatorSpecs;
import com.xgen.mongot.index.query.aggregation.TopAggregationSpec;
import com.xgen.mongot.server.command.search.definition.PlanShardedSearchCommandResponseDefinition;
import com.xgen.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinition;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.testing.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinitionBuilder;
import java.util.List;
import java.util.UUID;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PlanShardedSearchCommandTest {

  @BeforeClass
  public static void installAggregationRegistry() {
    TestOperatorSpecs.installRegistry();
  }

  @AfterClass
  public static void restoreAggregationRegistry() {
    TestOperatorSpecs.installRegistry();
  }

  private static final String DATABASE_NAME = "testDb";
  private static final String COLLECTION_NAME = "testCollection";
  private static final UUID COLLECTION_UUID =
      UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7");
  private static final String INDEX_NAME = "default";

  private static final SearchCommandsRegister.BootstrapperMetadata BOOTSTRAPPER_METADATA =
      new SearchCommandsRegister.BootstrapperMetadata(
          "testVersion",
          "localhost",
          () -> MongoDbServerInfo.EMPTY,
          FeatureFlags.getDefault(),
          DynamicFeatureFlagRegistry.empty());

  private static final SearchCommandsRegister.BootstrapperMetadata
      AGGREGATION_BOOTSTRAPPER_METADATA =
          new SearchCommandsRegister.BootstrapperMetadata(
              "testVersion",
              "localhost",
              () -> MongoDbServerInfo.EMPTY,
              FeatureFlags.withDefaults().enable(Feature.SEARCH_AGGREGATION).build(),
              DynamicFeatureFlagRegistry.empty());

  private static final BsonDocument VALID_OPERATOR_QUERY =
      new BsonDocument()
          .append(
              "text",
              new BsonDocument()
                  .append("path", new BsonString("a"))
                  .append("query", new BsonString("b")));

  private static final int STRING_FACET_NUM_BUCKETS_2000 = 2000;

  /** String facet with numBuckets above the 1k cap unless ENABLE_10K_BUCKET_LIMIT DFF is on. */
  private static final BsonDocument FACET_QUERY_NUM_BUCKETS_2000 =
      new BsonDocument()
          .append(
              "facet",
              new BsonDocument()
                  .append("operator", VALID_OPERATOR_QUERY)
                  .append(
                      "facets",
                      new BsonDocument()
                          .append(
                              "directorFacet",
                              new BsonDocument()
                                  .append("type", new BsonString("string"))
                                  .append("path", new BsonString("director"))
                                  .append(
                                      "numBuckets",
                                      new BsonInt32(STRING_FACET_NUM_BUCKETS_2000)))));

  private static final BsonDocument BOUNDARY_SUM_AGGREGATION_QUERY =
      new BsonDocument()
          .append("index", new BsonString(INDEX_NAME))
          .append(
              "facet",
              new BsonDocument()
                  .append("operator", VALID_OPERATOR_QUERY)
                  .append(
                      "facets",
                      new BsonDocument()
                          .append(
                              "agg",
                              new BsonDocument()
                                  .append("type", new BsonString("number"))
                                  .append("path", new BsonString("price"))
                                  .append(
                                      "boundaries",
                                      new BsonArray(
                                          List.of(
                                              new BsonInt32(0),
                                              new BsonInt32(100),
                                              new BsonInt32(200),
                                              new BsonInt32(300))))
                                  .append(
                                      "aggregations",
                                      new BsonDocument(
                                          "sum_price",
                                          new BsonDocument()
                                              .append("type", new BsonString("sum"))
                                              .append("path", new BsonString("price")))))));

  private static PlanShardedSearchCommandDefinition getPlanShardedSearchCommandDefinition(
      BsonDocument query) {
    return PlanShardedSearchCommandDefinitionBuilder.builder()
        .db(DATABASE_NAME)
        .collectionName(COLLECTION_NAME)
        .collectionUuid(COLLECTION_UUID)
        .query(query)
        .build();
  }

  private static PlanShardedSearchCommand createCommand(
      PlanShardedSearchCommandDefinition definition,
      SearchCommandsRegister.BootstrapperMetadata metadata) {
    return new PlanShardedSearchCommand(definition, metadata);
  }

  @Test
  public void testValidQuery() {
    PlanShardedSearchCommand command =
        createCommand(
            getPlanShardedSearchCommandDefinition(VALID_OPERATOR_QUERY), BOOTSTRAPPER_METADATA);

    List<BsonDocument> metaPipeline =
        List.of(
            new BsonDocument(
                "$group",
                new BsonDocument()
                    .append(
                        "_id",
                        new BsonDocument()
                            .append("type", new BsonString("$type"))
                            .append("tag", new BsonString("$tag"))
                            .append("bucket", new BsonString("$bucket")))
                    .append("value", new BsonDocument("$sum", new BsonString("$count")))),
            new BsonDocument(
                "$facet",
                new BsonDocument()
                    .append(
                        "count",
                        new BsonArray(
                            List.of(
                                new BsonDocument(
                                    "$match",
                                    new BsonDocument(
                                        "_id.type",
                                        new BsonDocument("$eq", new BsonString("count")))))))),
            new BsonDocument(
                "$replaceWith",
                new BsonDocument()
                    .append(
                        "count",
                        new BsonDocument(
                            "lowerBound",
                            new BsonDocument("$first", new BsonString("$count.value"))))));

    BsonDocument expected =
        PlanShardedSearchCommandResponseDefinition.create(
                new PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan(
                    metaPipeline,
                    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan.DEFAULT_SORT_SPEC))
            .toBson();
    BsonDocument actual = command.run();
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testStringFacetNumBucketsAbove1kFailsWhen10kDffOff() {
    PlanShardedSearchCommand command =
        createCommand(
            getPlanShardedSearchCommandDefinition(FACET_QUERY_NUM_BUCKETS_2000),
            BOOTSTRAPPER_METADATA);

    BsonDocument result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.containsKey("errmsg"));
  }

  @Test
  public void testStringFacetNumBucketsAbove1kPlansWhen10kDffOn() {
    DynamicFeatureFlagRegistry registry = mock(DynamicFeatureFlagRegistry.class);
    when(registry.evaluateClusterInvariant(DynamicFeatureFlags.ENABLE_10K_BUCKET_LIMIT))
        .thenReturn(true);
    SearchCommandsRegister.BootstrapperMetadata metadata =
        new SearchCommandsRegister.BootstrapperMetadata(
            "testVersion",
            "localhost",
            () -> MongoDbServerInfo.EMPTY,
            FeatureFlags.getDefault(),
            registry);

    PlanShardedSearchCommand command =
        createCommand(
            getPlanShardedSearchCommandDefinition(FACET_QUERY_NUM_BUCKETS_2000), metadata);

    BsonDocument result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
    BsonArray metaPipeline = result.getArray("metaPipeline");
    BsonDocument facetStage = metaPipeline.get(1).asDocument();
    BsonArray directorPipeline = facetStage.getDocument("$facet").getArray("directorFacet");
    BsonDocument limitStage = directorPipeline.get(2).asDocument();
    Assert.assertEquals(STRING_FACET_NUM_BUCKETS_2000, limitStage.getInt32("$limit").getValue());
  }

  @Test
  public void testFlatBoundarySumAggregationPlansWithoutIndexCatalog() {
    PlanShardedSearchCommand command =
        createCommand(
            getPlanShardedSearchCommandDefinition(BOUNDARY_SUM_AGGREGATION_QUERY),
            AGGREGATION_BOOTSTRAPPER_METADATA);

    BsonDocument result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
    Assert.assertEquals(3, result.getArray("metaPipeline").size());
  }

  @Test
  public void testFlatBoundarySumAggregationPlansOnExtensionPathWithEmptyDbAndCollection() {
    PlanShardedSearchCommandDefinition definition =
        PlanShardedSearchCommandDefinitionBuilder.builder()
            .db("")
            .collectionName("")
            .query(BOUNDARY_SUM_AGGREGATION_QUERY)
            .build();

    PlanShardedSearchCommand command = createCommand(definition, AGGREGATION_BOOTSTRAPPER_METADATA);

    BsonDocument result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
    Assert.assertEquals(3, result.getArray("metaPipeline").size());
  }

  @Test
  public void testNestedAggregationExactly100kPlans() {
    PlanShardedSearchCommand command =
        createCommand(
            getPlanShardedSearchCommandDefinition(nestedTermsQuery(1000, 100)),
            AGGREGATION_BOOTSTRAPPER_METADATA);

    BsonDocument result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
  }

  @Test
  public void testNestedAggregationOver100kRejectedWhenDffOn() {
    PlanShardedSearchCommand command =
        createCommand(
            getPlanShardedSearchCommandDefinition(nestedTermsQuery(1000, 101)),
            AGGREGATION_BOOTSTRAPPER_METADATA);

    BsonDocument result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    String errmsg = result.getString("errmsg").getValue();
    Assert.assertTrue(
        errmsg.contains(TopAggregationSpec.requestedBucketsLimitExceededMessage(101_000)));
  }

  @Test
  public void testNestedAggregationOver100kPlansWhenDffOff() {
    DynamicFeatureFlagRegistry registry = mock(DynamicFeatureFlagRegistry.class);
    when(registry.evaluateClusterInvariant(
            DynamicFeatureFlags.ENABLE_SEARCH_AGGREGATION_REQUESTED_BUCKET_LIMIT))
        .thenReturn(false);
    SearchCommandsRegister.BootstrapperMetadata metadata =
        new SearchCommandsRegister.BootstrapperMetadata(
            "testVersion",
            "localhost",
            () -> MongoDbServerInfo.EMPTY,
            FeatureFlags.withDefaults().enable(Feature.SEARCH_AGGREGATION).build(),
            registry);

    PlanShardedSearchCommand command =
        createCommand(getPlanShardedSearchCommandDefinition(nestedTermsQuery(1000, 101)), metadata);

    BsonDocument result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
  }

  @Test
  public void run_scopedSortFlagOff_rejected() {
    var command =
        createCommand(
            getPlanShardedSearchCommandDefinition(scopedSortQuery()), BOOTSTRAPPER_METADATA);

    BsonDocument result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.getString("errmsg").getValue().contains("not currently supported"));
  }

  @Test
  public void run_scopedSortFlagOn_plans() {
    DynamicFeatureFlagRegistry registry = mock(DynamicFeatureFlagRegistry.class);
    when(registry.evaluateClusterInvariant(DynamicFeatureFlags.ENABLE_EMBEDDED_SCOPED_SORT))
        .thenReturn(true);
    var metadata =
        new SearchCommandsRegister.BootstrapperMetadata(
            "testVersion",
            "localhost",
            () -> MongoDbServerInfo.EMPTY,
            FeatureFlags.getDefault(),
            registry);
    var command =
        createCommand(
            PlanShardedSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(scopedSortQuery())
                .searchFeatures(new PlanShardedSearchCommandDefinition.SearchFeatures(1))
                .build(),
            metadata);

    BsonDocument result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
  }

  private static BsonDocument scopedSortQuery() {
    return BsonDocument.parse(
        """
        {"exists": {"path": "variants"}, "sort": {"variants.price": {
          "order": 1, "embeddedDocument": {"path": "variants", "operator": {
            "exists": {"path": "variants.price"}
          }}
        }}}
        """);
  }

  private static BsonDocument nestedTermsQuery(int outerBuckets, int innerBuckets) {
    BsonDocument inner =
        new BsonDocument()
            .append("type", new BsonString("string"))
            .append("path", new BsonString("color"))
            .append("numBuckets", new BsonInt32(innerBuckets))
            .append(
                "aggregations",
                new BsonDocument("n", new BsonDocument("type", new BsonString("count"))));
    return new BsonDocument()
        .append(
            "facet",
            new BsonDocument()
                .append(
                    "facets",
                    new BsonDocument(
                        "byCity",
                        new BsonDocument()
                            .append("type", new BsonString("string"))
                            .append("path", new BsonString("city"))
                            .append("numBuckets", new BsonInt32(outerBuckets))
                            .append("facets", new BsonDocument("byColor", inner)))));
  }
}
