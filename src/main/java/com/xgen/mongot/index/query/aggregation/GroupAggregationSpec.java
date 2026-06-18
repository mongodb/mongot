package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;

/**
 * A bucket/ intermediate node in the aggregation spec tree.
 *
 * <pre>
 *   {
 *      bySkuColor: { _id: { sku: "$sku" , color: "$color" } ... }
 *   }
 *   This builds a single list spec tree: Group("sku") -> Group("color") -> Operator("count").
 * </pre>
 */
public abstract non-sealed class GroupAggregationSpec implements AggregationSpec {
  protected final FieldPath path;
  protected final String name;
  protected final List<AggregationSpec> children;

  protected GroupAggregationSpec(FieldPath path, String name, List<AggregationSpec> children) {
    this.path = path;
    this.name = name;
    this.children = children;
  }

  @Override
  public final String name() {
    return this.name;
  }

  @Override
  public final List<AggregationSpec> children() {
    return this.children;
  }

  @Override
  public final Accumulator buildAccumulator(
      AggregationContext context, Optional<Accumulator> parent) {
    return createAccumulator(context, parent);
  }

  // Derived class should implement to provide the accumulator.
  protected abstract Accumulator createAccumulator(
      AggregationContext context, Optional<Accumulator> parent);

  @Override
  public final void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    // Track the key projection so the $group has a chance to group by on current path.
    builder.addKeyProjection(this.path.toString());
    for (AggregationSpec spec : this.children) {
      spec.buildShardMergePipeline(builder);
    }
  }

  public FieldPath getPath() {
    return this.path;
  }
}
