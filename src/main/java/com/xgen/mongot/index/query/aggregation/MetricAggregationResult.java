package com.xgen.mongot.index.query.aggregation;

import org.bson.BsonArray;

/** Base class for leaf metric aggregation results (e.g. {@code sum}, {@code avg}). */
abstract sealed class MetricAggregationResult implements AggregationResult
    permits CountAggregationResult,
        SumAggregationResult,
        AvgAggregationResult,
        MinAggregationResult,
        MaxAggregationResult,
        FirstLastAggregationResultBase {
  private final String name;

  protected MetricAggregationResult(String name) {
    this.name = name;
  }

  @Override
  public final String getName() {
    return this.name;
  }

  /**
   * Leaf metrics do not emit intermediate rows; grouping parents flatten bucket paths and attach
   * {@link #toIntermediateMetricPartials()} per row.
   */
  @Override
  public final BsonArray toIntermediateBson() {
    return new BsonArray();
  }
}
