package com.xgen.mongot.server.command.search;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.query.aggregation.TestOperatorSpecs;
import com.xgen.mongot.server.command.search.definition.PlanShardedSearchCommandResponseDefinition;
import com.xgen.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinition;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinitionBuilder;
import java.util.List;
import java.util.Optional;
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
      SearchCommandsRegister.BootstrapperMetadata metadata,
      IndexCatalog indexCatalog) {
    return new PlanShardedSearchCommand(definition, metadata, indexCatalog);
  }

  private static SearchIndexDefinition priceIndexDefinition() {
    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .database(DATABASE_NAME)
        .lastObservedCollectionName(COLLECTION_NAME)
        .collectionUuid(COLLECTION_UUID)
        .name(INDEX_NAME)
        .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
        .mappings(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "price",
                    FieldDefinitionBuilder.builder()
                        .number(
                            NumericFieldDefinitionBuilder.builder()
                                .representation(NumericFieldOptions.Representation.INT64)
                                .buildNumberField())
                        .build())
                .build())
        .build();
  }

  @Test
  public void testValidQuery() {
    PlanShardedSearchCommand command =
        createCommand(
            getPlanShardedSearchCommandDefinition(VALID_OPERATOR_QUERY),
            BOOTSTRAPPER_METADATA,
            mock(IndexCatalog.class));

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
            BOOTSTRAPPER_METADATA,
            mock(IndexCatalog.class));

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
            getPlanShardedSearchCommandDefinition(FACET_QUERY_NUM_BUCKETS_2000),
            metadata,
            mock(IndexCatalog.class));

    BsonDocument result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
    BsonArray metaPipeline = result.getArray("metaPipeline");
    BsonDocument facetStage = metaPipeline.get(1).asDocument();
    BsonArray directorPipeline = facetStage.getDocument("$facet").getArray("directorFacet");
    BsonDocument limitStage = directorPipeline.get(2).asDocument();
    Assert.assertEquals(STRING_FACET_NUM_BUCKETS_2000, limitStage.getInt32("$limit").getValue());
  }

  @Test
  public void testFlatBoundarySumAggregationPlansWithoutCollectionUuid() {
    IndexCatalog indexCatalog = mock(IndexCatalog.class);
    SearchIndexDefinition indexDefinition = priceIndexDefinition();
    IndexGeneration indexGeneration = mockIndexGeneration(indexDefinition);
    when(indexCatalog.getIndexes()).thenReturn(List.of(indexGeneration));

    PlanShardedSearchCommandDefinition definition =
        PlanShardedSearchCommandDefinitionBuilder.builder()
            .db(DATABASE_NAME)
            .collectionName(COLLECTION_NAME)
            .query(BOUNDARY_SUM_AGGREGATION_QUERY)
            .build();

    PlanShardedSearchCommand command =
        createCommand(definition, AGGREGATION_BOOTSTRAPPER_METADATA, indexCatalog);

    BsonDocument result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
    Assert.assertEquals(3, result.getArray("metaPipeline").size());
  }

  @Test
  public void testFlatBoundarySumAggregationPlansWithIndexCatalog() {
    IndexCatalog indexCatalog = mock(IndexCatalog.class);
    SearchIndexDefinition indexDefinition = priceIndexDefinition();
    IndexGeneration indexGeneration = mockIndexGeneration(indexDefinition);
    when(indexCatalog.getIndex(eq(DATABASE_NAME), eq(COLLECTION_UUID), any(), eq(INDEX_NAME)))
        .thenReturn(Optional.of(indexGeneration));

    PlanShardedSearchCommand command =
        createCommand(
            getPlanShardedSearchCommandDefinition(BOUNDARY_SUM_AGGREGATION_QUERY),
            AGGREGATION_BOOTSTRAPPER_METADATA,
            indexCatalog);

    BsonDocument result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
    BsonArray metaPipeline = result.getArray("metaPipeline");
    Assert.assertEquals(3, metaPipeline.size());
  }

  @Test
  public void testFlatBoundarySumAggregationIndexNotFoundReturnsError() {
    IndexCatalog indexCatalog = mock(IndexCatalog.class);
    when(indexCatalog.getIndex(eq(DATABASE_NAME), eq(COLLECTION_UUID), any(), eq(INDEX_NAME)))
        .thenReturn(Optional.empty());

    PlanShardedSearchCommand command =
        createCommand(
            getPlanShardedSearchCommandDefinition(BOUNDARY_SUM_AGGREGATION_QUERY),
            AGGREGATION_BOOTSTRAPPER_METADATA,
            indexCatalog);

    BsonDocument result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.getString("errmsg").getValue().contains("not found"));
  }

  /**
   * A legacy numberFacet grouping field carries no v2 doc values, so boundary spec building would
   * raise a bare NoSuchElementException. Plan time must report the same query error the query path
   * reports instead.
   */
  @Test
  public void testBoundaryGroupingOnLegacyNumberFacetFieldReturnsError() {
    IndexCatalog indexCatalog = mock(IndexCatalog.class);
    IndexGeneration indexGeneration = mockIndexGeneration(legacyNumberFacetPriceIndexDefinition());
    when(indexCatalog.getIndex(eq(DATABASE_NAME), eq(COLLECTION_UUID), any(), eq(INDEX_NAME)))
        .thenReturn(Optional.of(indexGeneration));

    PlanShardedSearchCommand command =
        createCommand(
            getPlanShardedSearchCommandDefinition(BOUNDARY_SUM_AGGREGATION_QUERY),
            AGGREGATION_BOOTSTRAPPER_METADATA,
            indexCatalog);

    BsonDocument result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(
        result
            .getString("errmsg")
            .getValue()
            .contains("because it is indexed as a legacy \"numberFacet\""));
  }

  private static SearchIndexDefinition legacyNumberFacetPriceIndexDefinition() {
    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .database(DATABASE_NAME)
        .lastObservedCollectionName(COLLECTION_NAME)
        .collectionUuid(COLLECTION_UUID)
        .name(INDEX_NAME)
        .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
        .mappings(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "price",
                    FieldDefinitionBuilder.builder()
                        .numberFacet(
                            NumericFieldDefinitionBuilder.builder()
                                .representation(NumericFieldOptions.Representation.INT64)
                                .buildNumberFacetField())
                        .build())
                .build())
        .build();
  }
}
