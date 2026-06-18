package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
  public final Accumulator buildAccumulator(AggregationContext context) throws IOException {
    // Bottom-up: build the children's accumulators first; the derived accumulator's constructor
    // wires itself as their parent.
    List<Accumulator> childAccumulators = new ArrayList<>(this.children.size());
    for (AggregationSpec childSpec : this.children) {
      childAccumulators.add(childSpec.buildAccumulator(context));
    }
    return createAccumulator(context, childAccumulators);
  }

  // Derived class should implement to provide the accumulator.
  protected abstract Accumulator createAccumulator(
      AggregationContext context, List<Accumulator> childAccumulators) throws IOException;

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
