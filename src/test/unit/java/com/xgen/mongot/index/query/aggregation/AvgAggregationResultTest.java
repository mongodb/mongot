package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.junit.Test;

public class AvgAggregationResultTest {

  @Test
  public void toStandaloneBson_zeroCount_returnsNull() {
    AvgAggregationResult result = new AvgAggregationResult("avg_price", 0.0, 0);
    assertSame(BsonNull.VALUE, result.toStandaloneBson());
  }

  @Test
  public void toStandaloneBson_nonZeroCount_returnsAverage() {
    AvgAggregationResult result = new AvgAggregationResult("avg_price", 30.0, 2);
    assertEquals(new BsonDouble(15.0), result.toStandaloneBson());
  }

  @Test
  public void mergePartitionResult_sumsPartials() {
    AvgAggregationResult left = new AvgAggregationResult("avg_price", 30.0, 2);
    AvgAggregationResult right = new AvgAggregationResult("avg_price", 10.0, 1);
    AvgAggregationResult merged =
        (AvgAggregationResult) AggregationTestSupport.mergePartitionResults(left, right);
    assertEquals(40.0, merged.getSum(), 0.0001);
    assertEquals(3, merged.getCount());
  }

  @Test
  public void toIntermediateMetricPartials_emitsSumAndCountFields() {
    AvgAggregationResult result = new AvgAggregationResult("avg_price", 30.0, 2);
    assertEquals(
        new BsonDouble(30.0), result.toIntermediateMetricPartials().get("avg_sum_avg_price"));
    assertEquals(
        new BsonInt64(2), result.toIntermediateMetricPartials().get("avg_count_avg_price"));
  }
}
