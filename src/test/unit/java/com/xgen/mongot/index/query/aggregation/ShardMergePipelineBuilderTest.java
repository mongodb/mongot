package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.util.FieldPath;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

/**
 * Validates the merge pipeline produced by ShardMergePipelineBuilder for representative spec trees.
 * Pipeline BSON is asserted verbatim so accidental changes are caught.
 */
public class ShardMergePipelineBuilderTest {

  private static final FieldPath CITY = FieldPath.parse("city");
  private static final FieldPath COLOR = FieldPath.parse("color");
  private static final FieldPath PRICE = FieldPath.parse("price");

  @Test
  public void singleGroupWithMetricLowerBound() {
    AggregationSpec leaf = new SumSpec("sum_price", PRICE);
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
              "by_city": [{"$match": {"_id": {"$meta/group_name": "by_city"}}}],
              "count": [{"$match": {"_id": {"$meta/group_name": null}}}]
            }}
            """);
    assertEquals(expectedFacet, pipeline.get(1));

    BsonDocument expectedReplaceWith =
        BsonDocument.parse(
            """
            {"$replaceWith": {
              "count": {"lowerBound": {"$first": "$count.count_value"}},
              "facet": {
                "by_city": {"buckets": {"$map": {
                  "input": "$by_city", "as": "b",
                  "in": {
                    "city": "$$b._id['$meta/keys'].city",
                    "sum_price": "$$b.sum_sum_price"
                  }
                }}}
              }
            }}
            """);
    assertEquals(expectedReplaceWith, pipeline.get(2));
  }

  @Test
  public void singleGroupWithAvgMetricFinalizesInReplaceWith() {
    AggregationSpec leaf = new AvgSpec("avg_price", PRICE);
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

    BsonDocument inFields =
        pipeline
            .get(2)
            .getDocument("$replaceWith")
            .getDocument("facet")
            .getDocument("by_city")
            .getDocument("buckets")
            .getDocument("$map")
            .getDocument("in");
    BsonDocument expectedAvgPrice =
        BsonDocument.parse(
            """
            {"$cond": {
              "if": {"$eq": ["$$b.avg_count_avg_price", 0]},
              "then": null,
              "else": {"$divide": ["$$b.avg_sum_avg_price", "$$b.avg_count_avg_price"]}
            }}
            """);
    assertEquals(expectedAvgPrice, inFields.get("avg_price"));
  }

  @Test
  public void countTypeTotalChangesCountField() {
    AggregationSpec leaf = new SumSpec("sum_price", PRICE);
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
  public void nestedGroupsCarryNestedKeysThroughIdKeys() {
    AggregationSpec leaf = new SumSpec("sum_price", PRICE);
    AggregationSpec innerGroup = new TermsSpec("by_color", COLOR, List.of(leaf));
    AggregationSpec outerGroup = new TermsSpec("by_city", CITY, List.of(innerGroup));
    TopAggregationSpec top = new TopAggregationSpec(List.of(outerGroup));

    ShardMergePipelineBuilder builder = new ShardMergePipelineBuilder();
    top.buildShardMergePipeline(builder);
    List<BsonDocument> pipeline = builder.build("lowerBound");

    BsonDocument expectedReplaceWith =
        BsonDocument.parse(
            """
            {"$replaceWith": {
              "count": {"lowerBound": {"$first": "$count.count_value"}},
              "facet": {
                "by_city": {"buckets": {"$map": {
                  "input": "$by_city", "as": "b",
                  "in": {
                    "city": "$$b._id['$meta/keys'].city",
                    "color": "$$b._id['$meta/keys'].color",
                    "sum_price": "$$b.sum_sum_price"
                  }
                }}}
              }
            }}
            """);
    assertEquals(expectedReplaceWith, pipeline.get(2));
  }

  @Test
  public void multipleTopLevelGroupsProduceSeparateFacets() {
    AggregationSpec leaf1 = new SumSpec("sum_price", PRICE);
    AggregationSpec leaf2 = new SumSpec("sum_price", PRICE);
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
              "by_city": [{"$match": {"_id": {"$meta/group_name": "by_city"}}}],
              "by_color": [{"$match": {"_id": {"$meta/group_name": "by_color"}}}],
              "count": [{"$match": {"_id": {"$meta/group_name": null}}}]
            }}
            """);
    assertEquals(expectedFacet, pipeline.get(1));
  }

  private static final class SumSpec extends OperatorAggregationSpec {
    @SuppressWarnings("unused")
    private final FieldPath path;

    SumSpec(String name, FieldPath path) {
      super(Operator.SUM, name);
      this.path = path;
    }

    @Override
    public Accumulator buildAccumulator(AggregationContext context) {
      throw new UnsupportedOperationException("not exercised in pipeline test");
    }

    @Override
    public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
      builder.addMetricAccumulator(
          "sum_" + this.name, new BsonDocument("$sum", new BsonString("$sum_" + this.name)));
      builder.addMetricProjection(this.name, new BsonString("$$b.sum_" + this.name));
    }
  }

  private static final class AvgSpec extends OperatorAggregationSpec {
    @SuppressWarnings("unused")
    private final FieldPath path;

    AvgSpec(String name, FieldPath path) {
      super(Operator.AVG, name);
      this.path = path;
    }

    @Override
    public Accumulator buildAccumulator(AggregationContext context) {
      throw new UnsupportedOperationException("not exercised in pipeline test");
    }

    @Override
    public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
      builder.addMetricAccumulator(
          "avg_sum_" + this.name,
          new BsonDocument("$sum", new BsonString("$avg_sum_" + this.name)));
      builder.addMetricAccumulator(
          "avg_count_" + this.name,
          new BsonDocument("$sum", new BsonString("$avg_count_" + this.name)));
      builder.addMetricProjection(
          this.name,
          BsonDocument.parse(
              """
              {"$cond": {
                "if": {"$eq": ["$$b.avg_count_%s", 0]},
                "then": null,
                "else": {"$divide": ["$$b.avg_sum_%s", "$$b.avg_count_%s"]}
              }}
              """
                  .formatted(this.name, this.name, this.name)));
    }
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
