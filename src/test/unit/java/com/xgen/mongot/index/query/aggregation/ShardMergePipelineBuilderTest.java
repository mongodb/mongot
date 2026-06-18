package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

/**
 * Validates the merge pipeline produced by ShardMergePipelineBuilder for representative spec
 * trees. Pipeline BSON is asserted verbatim so accidental changes are caught.
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
              "_id": {"tag": "$tag", "keys": "$keys"},
              "count_value": {"$sum": "$count"},
              "sum_sum_price": {"$sum": "$sum_sum_price"}
            }}
            """);
    assertEquals(expectedGroup, pipeline.get(0));

    BsonDocument expectedFacet =
        BsonDocument.parse(
            """
            {"$facet": {
              "by_city": [{"$match": {"_id.tag": "by_city"}}],
              "count": [{"$match": {"_id.tag": null}}]
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
                    "city": "$$b._id.keys.city",
                    "sum_price": "$$b.sum_sum_price"
                  }
                }}}
              }
            }}
            """);
    assertEquals(expectedReplaceWith, pipeline.get(2));
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
                    "city": "$$b._id.keys.city",
                    "color": "$$b._id.keys.color",
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
              "by_city": [{"$match": {"_id.tag": "by_city"}}],
              "by_color": [{"$match": {"_id.tag": "by_color"}}],
              "count": [{"$match": {"_id.tag": null}}]
            }}
            """);
    assertEquals(expectedFacet, pipeline.get(1));
  }

  // ===== inline spec fixtures (no accumulator/result needed for pipeline tests) =====

  private static final class SumSpec extends OperatorAggregationSpec {
    @SuppressWarnings("unused")
    private final FieldPath path;

    SumSpec(String name, FieldPath path) {
      super(Operator.SUM, name);
      this.path = path;
    }

    @Override
    public Accumulator buildAccumulator(AggregationContext context, Optional<Accumulator> parent) {
      throw new UnsupportedOperationException("not exercised in pipeline test");
    }

    @Override
    public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
      builder.addMetricAccumulator(
          "sum_" + this.name, new BsonDocument("$sum", new BsonString("$sum_" + this.name)));
      builder.addMetricProjection(this.name, new BsonString("$$b.sum_" + this.name));
    }
  }

  private static final class TermsSpec extends GroupAggregationSpec {
    TermsSpec(String name, FieldPath path, List<AggregationSpec> children) {
      super(path, name, children);
    }

    @Override
    protected Accumulator createAccumulator(
        AggregationContext context, Optional<Accumulator> parent) {
      throw new UnsupportedOperationException("not exercised in pipeline test");
    }
  }
}
