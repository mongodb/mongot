package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import org.bson.BsonArray;
import org.junit.Test;

public class ShardMergeAliasPlanTest {

  private static final FieldPath PRICE = FieldPath.parse("price");
  private static final FieldPath CITY = FieldPath.parse("city");

  @Test
  public void rewriteIntermediateBson_aliasesFacetAndMetricWireFields() {
    TopAggregationSpec spec =
        new TopAggregationSpec(
            List.of(
                new TermsGroupAggregationSpec(
                    "by.city",
                    CITY,
                    List.of(new SumAggregationSpec("sum.Amt", PRICE)),
                    new TermsArgs(true, TypeField.TOKEN, 10))));

    ShardMergeAliasPlan plan = ShardMergeAliasPlan.from(spec, List.of());

    BsonArray raw =
        BsonArray.parse(
            """
            [
              {"$meta/group_name": "by.city", "$meta/keys": {"k0": "NYC"}, "sum_sum.Amt": 10}
            ]
            """);

    BsonArray aliased = plan.rewriteIntermediateBson(raw);

    assertEquals(
        BsonArray.parse(
            """
            [
              {"$meta/group_name": "f0", "$meta/keys": {"k0": "NYC"}, "m0": 10}
            ]
            """),
        aliased);
  }
}
