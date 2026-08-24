package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;

/**
 * Production leaf spec for the {@code min} aggregation operator.
 *
 * <p>Min reads every doc-values column its path is indexed under, so it is supported on any
 * sortable field type and on paths carrying several types at once. Paths indexed under none of
 * those types are rejected at field-resolution time.
 */
public final class MinAggregationSpec extends OperatorAggregationSpec {
  private final FieldPath path;

  public MinAggregationSpec(String name, FieldPath path) {
    super(Operator.MIN, name);
    this.path = path;
  }

  public FieldPath getPath() {
    return this.path;
  }

  @Override
  public Accumulator buildAccumulator(AggregationContext context) {
    return new MinMaxAccumulator(this.name, this.path, MinMaxAccumulator.Extremum.MIN, context);
  }

  @Override
  public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    MinMaxShardMerge.buildShardMergePipeline(builder, this.operator, this.name);
  }
}
