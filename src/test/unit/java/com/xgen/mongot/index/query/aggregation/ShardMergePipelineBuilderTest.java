package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.util.FieldPath;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.Test;

/**
 * Validates the merge pipeline produced by ShardMergePipelineBuilder for representative spec trees.
 * Pipeline BSON is asserted verbatim so accidental changes are caught.
 */
public class ShardMergePipelineBuilderTest {

  private static final FieldPath CITY = FieldPath.parse("city");
  private static final FieldPath COLOR = FieldPath.parse("color");
  private static final FieldPath REGION = FieldPath.parse("region");
  private static final FieldPath COUNTRY = FieldPath.parse("country");
  private static final FieldPath PRICE = FieldPath.parse("price");

  @Test
  public void singleGroupWithMetricLowerBound() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec group = new TermsSpec("by_city", CITY, List.of(leaf));
    TopAggregationSpec top = new TopAggregationSpec(List.of(group));

    ShardMergePipelineBuilder builder = new ShardMergePipelineBuilder();
    top.buildShardMergePipeline(builder);
    List<BsonDocument> pipeline = builder.build("lowerBound");

    assertEquals(3, pipeline.size());

    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "$meta/group_name": {"$getField": {"field": "$meta/group_name", "input": "$$ROOT"}},
                "$meta/keys": {"$getField": {"field": "$meta/keys", "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "sum_sum_price": {"$sum": "$sum_sum_price"}
            }}
            """);
    assertEquals(expectedGroup, pipeline.get(0));

    BsonDocument expectedFacet =
        BsonDocument.parse(
            """
            {"$facet": {
              "by_city": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "$meta/group_name", "input": "$_id"}},
                  "by_city"
                ]}}},
                {"$project": {
                  "_id": "$_id['$meta/keys'].city",
                  "sum_price": "$sum_sum_price"
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$getField": {"field": "$meta/group_name", "input": "$_id"}},
                null
              ]}}}]
            }}
            """);
    assertEquals(expectedFacet, pipeline.get(1));

    BsonDocument expectedReplaceWith =
        BsonDocument.parse(
            """
            {"$replaceWith": {
              "count": {"lowerBound": {"$first": "$count.count_value"}},
              "facet": {
                "by_city": {"buckets": "$by_city"}
              }
            }}
            """);
    assertEquals(expectedReplaceWith, pipeline.get(2));
  }

  @Test
  public void singleGroupWithAvgMetricFinalizesInFacetProject() {
    AggregationSpec leaf = new AvgAggregationSpec("avg_price", PRICE);
    AggregationSpec group = new TermsSpec("by_city", CITY, List.of(leaf));
    TopAggregationSpec top = new TopAggregationSpec(List.of(group));

    ShardMergePipelineBuilder builder = new ShardMergePipelineBuilder();
    top.buildShardMergePipeline(builder);
    List<BsonDocument> pipeline = builder.build("lowerBound");

    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "$meta/group_name": {"$getField": {"field": "$meta/group_name", "input": "$$ROOT"}},
                "$meta/keys": {"$getField": {"field": "$meta/keys", "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "avg_sum_avg_price": {"$sum": "$avg_sum_avg_price"},
              "avg_count_avg_price": {"$sum": "$avg_count_avg_price"}
            }}
            """);
    assertEquals(expectedGroup, pipeline.get(0));

    BsonDocument projectStage =
        pipeline
            .get(1)
            .getDocument("$facet")
            .getArray("by_city")
            .get(1)
            .asDocument()
            .getDocument("$project");
    BsonDocument expectedAvgPrice =
        BsonDocument.parse(
            """
            {"$cond": {
              "if": {"$eq": ["$avg_count_avg_price", 0]},
              "then": null,
              "else": {"$divide": ["$avg_sum_avg_price", "$avg_count_avg_price"]}
            }}
            """);
    assertEquals(expectedAvgPrice, projectStage.get("avg_price"));
  }

  @Test
  public void countTypeTotalChangesCountField() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec group = new TermsSpec("by_city", CITY, List.of(leaf));
    TopAggregationSpec top = new TopAggregationSpec(List.of(group));

    ShardMergePipelineBuilder builder = new ShardMergePipelineBuilder();
    top.buildShardMergePipeline(builder);
    List<BsonDocument> pipeline = builder.build("total");

    BsonDocument replaceWith = pipeline.get(2);
    BsonDocument count = replaceWith.getDocument("$replaceWith").getDocument("count");
    assertEquals(BsonDocument.parse("{\"total\": {\"$first\": \"$count.count_value\"}}"), count);
  }

  @Test
  public void nestedGroupsReconstructChildBranchInFacetArm() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec innerGroup = new TermsSpec("by_color", COLOR, List.of(leaf));
    AggregationSpec outerGroup = new TermsSpec("by_city", CITY, List.of(innerGroup));
    TopAggregationSpec top = new TopAggregationSpec(List.of(outerGroup));

    ShardMergePipelineBuilder builder = new ShardMergePipelineBuilder();
    top.buildShardMergePipeline(builder);
    List<BsonDocument> pipeline = builder.build("lowerBound");

    BsonArray expectedByCityArm =
        BsonArray.parse(
            """
            [
              {"$match": {"$expr": {"$eq": [
                {"$getField": {"field": "$meta/group_name", "input": "$_id"}},
                "by_city"
              ]}}},
              {"$group": {
                "_id": "$_id['$meta/keys'].city",
                "_rows": {"$push": {
                  "color": "$_id['$meta/keys'].color",
                  "sum_sum_price": "$sum_sum_price"
                }}
              }},
              {"$project": {
                "_id": "$_id",
                "by_color": {"buckets": {"$map": {
                  "input": "$_rows",
                  "as": "row",
                  "in": {
                    "_id": "$$row.color",
                    "sum_price": "$$row.sum_sum_price"
                  }
                }}}
              }}
            ]
            """);
    assertEquals(expectedByCityArm, pipeline.get(1).getDocument("$facet").getArray("by_city"));

    BsonDocument expectedReplaceWith =
        BsonDocument.parse(
            """
            {"$replaceWith": {
              "count": {"lowerBound": {"$first": "$count.count_value"}},
              "facet": {
                "by_city": {"buckets": "$by_city"}
              }
            }}
            """);
    assertEquals(expectedReplaceWith, pipeline.get(2));
  }

  @Test
  public void nestedDepth4_emitsThreeGroupStagesAndTripleNestedProject() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec byColor = new TermsSpec("by_color", COLOR, List.of(leaf));
    AggregationSpec byCity = new TermsSpec("by_city", CITY, List.of(byColor));
    AggregationSpec byRegion = new TermsSpec("by_region", REGION, List.of(byCity));
    AggregationSpec byCountry = new TermsSpec("by_country", COUNTRY, List.of(byRegion));
    TopAggregationSpec top = new TopAggregationSpec(List.of(byCountry));

    ShardMergePipelineBuilder builder = new ShardMergePipelineBuilder();
    top.buildShardMergePipeline(builder);
    List<BsonDocument> pipeline = builder.build("lowerBound");

    BsonArray expectedByCountryArm =
        BsonArray.parse(
            """
            [
              {"$match": {"$expr": {"$eq": [
                {"$getField": {"field": "$meta/group_name", "input": "$_id"}},
                "by_country"
              ]}}},
              {"$group": {
                "_id": {
                  "country": "$_id['$meta/keys'].country",
                  "region": "$_id['$meta/keys'].region",
                  "city": "$_id['$meta/keys'].city"
                },
                "_rows": {"$push": {
                  "color": "$_id['$meta/keys'].color",
                  "sum_sum_price": "$sum_sum_price"
                }}
              }},
              {"$group": {
                "_id": {
                  "country": "$_id.country",
                  "region": "$_id.region"
                },
                "_rows": {"$push": {
                  "city": "$_id.city",
                  "_leaf_rows": "$_rows"
                }}
              }},
              {"$group": {
                "_id": "$_id.country",
                "_rows": {"$push": {
                  "region": "$_id.region",
                  "_leaf_rows": "$_rows"
                }}
              }},
              {"$project": {
                "_id": "$_id",
                "by_region": {"buckets": {"$map": {
                  "input": "$_rows",
                  "as": "row",
                  "in": {
                    "_id": "$$row.region",
                    "by_city": {"buckets": {"$map": {
                      "input": "$$row._leaf_rows",
                      "as": "leaf",
                      "in": {
                        "_id": "$$leaf.city",
                        "by_color": {"buckets": {"$map": {
                          "input": "$$leaf._leaf_rows",
                          "as": "leaf",
                          "in": {
                            "_id": "$$leaf.color",
                            "sum_price": "$$leaf.sum_sum_price"
                          }
                        }}}
                      }
                    }}}
                  }
                }}}
              }}
            ]
            """);
    assertEquals(
        expectedByCountryArm, pipeline.get(1).getDocument("$facet").getArray("by_country"));

    BsonDocument expectedReplaceWith =
        BsonDocument.parse(
            """
            {"$replaceWith": {
              "count": {"lowerBound": {"$first": "$count.count_value"}},
              "facet": {
                "by_country": {"buckets": "$by_country"}
              }
            }}
            """);
    assertEquals(expectedReplaceWith, pipeline.get(2));
  }

  @Test
  public void multipleTopLevelGroupsProduceSeparateFacets() {
    AggregationSpec leaf1 = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec leaf2 = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec group1 = new TermsSpec("by_city", CITY, List.of(leaf1));
    AggregationSpec group2 = new TermsSpec("by_color", COLOR, List.of(leaf2));
    TopAggregationSpec top = new TopAggregationSpec(List.of(group1, group2));

    ShardMergePipelineBuilder builder = new ShardMergePipelineBuilder();
    top.buildShardMergePipeline(builder);
    List<BsonDocument> pipeline = builder.build("lowerBound");

    BsonDocument expectedFacet =
        BsonDocument.parse(
            """
            {"$facet": {
              "by_city": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "$meta/group_name", "input": "$_id"}},
                  "by_city"
                ]}}},
                {"$project": {
                  "_id": "$_id['$meta/keys'].city",
                  "sum_price": "$sum_sum_price"
                }}
              ],
              "by_color": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "$meta/group_name", "input": "$_id"}},
                  "by_color"
                ]}}},
                {"$project": {
                  "_id": "$_id['$meta/keys'].color",
                  "sum_price": "$sum_sum_price"
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$getField": {"field": "$meta/group_name", "input": "$_id"}},
                null
              ]}}}]
            }}
            """);
    assertEquals(expectedFacet, pipeline.get(1));
  }

  private static final class TermsSpec extends GroupAggregationSpec {
    TermsSpec(String name, FieldPath path, List<AggregationSpec> children) {
      super(path, name, children);
    }

    @Override
    protected Accumulator createAccumulator(
        AggregationContext context, List<Accumulator> childAccumulators) {
      throw new UnsupportedOperationException("not exercised in pipeline test");
    }
  }
}
