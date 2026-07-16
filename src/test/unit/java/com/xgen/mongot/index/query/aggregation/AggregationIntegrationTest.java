package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.util.FieldPath;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

/**
 * Verifies {@link AggregationResult#toIntermediateBson()} field names align with the merge pipeline
 * produced by {@link AggregationSpec#buildShardMergePipeline(ShardMergePipelineBuilder)}.
 */
public class AggregationIntegrationTest extends AggregationTestSupport {

  private static final FieldPath CITY = FieldPath.parse("city");
  private static final FieldPath COLOR = FieldPath.parse("color");
  private static final FieldPath PRICE = FieldPath.parse("price");

  @Test
  public void flatSum_intermediateBsonMatchesGeneratedPipeline() {
    AggregationSpec specTree =
        new TopAggregationSpec(
            List.of(
                new TermsSpec(
                    "by_city", CITY, List.of(new TestOperatorSpecs.SumSpec("sum_price", PRICE)))));

    AggregationResult resultTree =
        new TopLevelAggregationResult(
            List.of(
                new TermsResult(
                    "by_city",
                    CITY,
                    List.of(
                        new TermsResult.TermsBucket(
                            "NYC",
                            AggregationResults.from(
                                List.of(new TestOperatorSpecs.SumResult("sum_price", 30.0)))),
                        new TermsResult.TermsBucket(
                            "SF",
                            AggregationResults.from(
                                List.of(new TestOperatorSpecs.SumResult("sum_price", 50.0))))))));

    BsonArray intermediate = resultTree.toIntermediateBson();
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertIntermediateDocHasFields(
        intermediate.get(0).asDocument(),
        IntermediateAggregationMetaField.GROUP_NAME,
        IntermediateAggregationMetaField.KEYS,
        "sum_sum_price");
    assertGroupStageReferencesFields(pipeline.get(0), "sum_sum_price", "count_value");
    assertFacetMatchesGroupName(pipeline.get(1), "by_city");
  }

  @Test
  public void flatAvg_intermediateBsonMatchesGeneratedPipeline() {
    AggregationSpec specTree =
        new TopAggregationSpec(
            List.of(
                new TermsSpec(
                    "by_city", CITY, List.of(new TestOperatorSpecs.AvgSpec("avg_price", PRICE)))));

    AggregationResult resultTree =
        new TopLevelAggregationResult(
            List.of(
                new TermsResult(
                    "by_city",
                    CITY,
                    List.of(
                        new TermsResult.TermsBucket(
                            "NYC",
                            AggregationResults.from(
                                List.of(new AvgResult("avg_price", 30.0, 2))))))));

    BsonArray intermediate = resultTree.toIntermediateBson();
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertIntermediateDocHasFields(
        intermediate.get(0).asDocument(),
        IntermediateAggregationMetaField.GROUP_NAME,
        IntermediateAggregationMetaField.KEYS,
        "avg_sum_avg_price",
        "avg_count_avg_price");
    assertGroupStageReferencesFields(
        pipeline.get(0), "avg_sum_avg_price", "avg_count_avg_price", "count_value");
    assertFacetMatchesGroupName(pipeline.get(1), "by_city");
  }

  @Test
  public void multipleTopLevelFacets_intermediateBsonAndPipelineUseDistinctGroupNames() {
    AggregationSpec specTree =
        new TopAggregationSpec(
            List.of(
                new TermsSpec(
                    "by_city", CITY, List.of(new TestOperatorSpecs.SumSpec("sum_price", PRICE))),
                new TermsSpec(
                    "by_color",
                    COLOR,
                    List.of(new TestOperatorSpecs.SumSpec("sum_price", PRICE)))));

    AggregationResult resultTree =
        new TopLevelAggregationResult(
            List.of(
                new TermsResult(
                    "by_city",
                    CITY,
                    List.of(
                        new TermsResult.TermsBucket(
                            "NYC",
                            AggregationResults.from(
                                List.of(new TestOperatorSpecs.SumResult("sum_price", 10.0)))))),
                new TermsResult(
                    "by_color",
                    COLOR,
                    List.of(
                        new TermsResult.TermsBucket(
                            "red",
                            AggregationResults.from(
                                List.of(new TestOperatorSpecs.SumResult("sum_price", 35.0))))))));

    BsonArray intermediate = resultTree.toIntermediateBson();
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertEquals(
        "by_city",
        intermediate
            .get(0)
            .asDocument()
            .getString(IntermediateAggregationMetaField.GROUP_NAME)
            .getValue());
    assertEquals(
        "by_color",
        intermediate
            .get(1)
            .asDocument()
            .getString(IntermediateAggregationMetaField.GROUP_NAME)
            .getValue());
    assertFacetMatchesGroupName(pipeline.get(1), "by_city");
    assertFacetMatchesGroupName(pipeline.get(1), "by_color");
  }

  private static List<BsonDocument> buildPipeline(AggregationSpec specTree) {
    ShardMergePipelineBuilder builder = new ShardMergePipelineBuilder();
    specTree.buildShardMergePipeline(builder);
    return builder.build("lowerBound");
  }

  private static void assertIntermediateDocHasFields(BsonDocument doc, String... fieldNames) {
    for (String fieldName : fieldNames) {
      assertTrue("missing field: " + fieldName, doc.containsKey(fieldName));
    }
  }

  private static void assertGroupStageReferencesFields(
      BsonDocument groupStage, String... accumulatorFieldNames) {
    BsonDocument groupBody = groupStage.getDocument("$group");
    for (String fieldName : accumulatorFieldNames) {
      assertTrue("group stage missing field: " + fieldName, groupBody.containsKey(fieldName));
    }
    BsonDocument id = groupBody.getDocument("_id");
    assertTrue(id.containsKey(IntermediateAggregationMetaField.GROUP_NAME));
    assertTrue(id.containsKey(IntermediateAggregationMetaField.KEYS));
  }

  private static void assertFacetMatchesGroupName(BsonDocument facetStage, String groupName) {
    BsonArray facetArm = facetStage.getDocument("$facet").getArray(groupName);
    BsonDocument matchStage = facetArm.get(0).asDocument().getDocument("$match");
    assertEquals(
        new BsonString(groupName),
        matchStage.getDocument("_id").getString(IntermediateAggregationMetaField.GROUP_NAME));
  }

  // ===== inline spec fixtures =====

  private static final class TermsSpec extends GroupAggregationSpec {
    TermsSpec(String name, FieldPath path, List<AggregationSpec> children) {
      super(path, name, children);
    }

    @Override
    protected Accumulator createAccumulator(
        AggregationContext context, List<Accumulator> childAccumulators) {
      throw new UnsupportedOperationException("not exercised");
    }
  }
}
