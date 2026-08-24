package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;

/**
 * Production leaf spec for the {@code max} aggregation operator.
 *
 * <p>Max reads every doc-values column its path is indexed under, so it is supported on any
 * sortable field type and on paths carrying several types at once. Paths indexed under none of
 * those types are rejected at field-resolution time.
 */
public final class MaxAggregationSpec extends OperatorAggregationSpec {
  private final FieldPath path;

  public MaxAggregationSpec(String name, FieldPath path) {
    super(Operator.MAX, name);
    this.path = path;
  }

  public FieldPath getPath() {
    return this.path;
  }

  @Override
  public Accumulator buildAccumulator(AggregationContext context) {
    return new MinMaxAccumulator(this.name, this.path, MinMaxAccumulator.Extremum.MAX, context);
  }

  @Override
  public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    MinMaxShardMerge.buildShardMergePipeline(builder, this.operator, this.name);
  }
}
