package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.List;

/**
 * Leaf spec node.
 *
 * <pre>
 *   {
 *      groupName: { $sum: path-to-field }
 *   }
 * </pre>
 */
public abstract non-sealed class OperatorAggregationSpec implements AggregationSpec {
  protected final Operator operator;
  protected final String name;

  public OperatorAggregationSpec(Operator operator, String name) {
    this.operator = operator;
    this.name = name;
  }

  @Override
  public final String name() {
    return this.name;
  }

  @Override
  public final List<AggregationSpec> children() {
    return List.of();
  }

  @Override
  public final long requestedBucketCount() {
    return 0L;
  }

  @Override
  public abstract Accumulator buildAccumulator(AggregationContext context) throws IOException;

  @Override
  public abstract void buildShardMergePipeline(ShardMergePipelineBuilder builder);

  public final Operator getOperator() {
    return this.operator;
  }
}
