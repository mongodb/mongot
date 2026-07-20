package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
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
    List<GroupAggregationSpec> groupChildren =
        this.children.stream()
            .filter(GroupAggregationSpec.class::isInstance)
            .map(GroupAggregationSpec.class::cast)
            .toList();

    Optional<String> nestedBranchName;
    if (groupChildren.size() == 1) {
      nestedBranchName = Optional.of(groupChildren.getFirst().name());
    } else {
      // Private preview supports a single nested facet group per intermediate level (leaf levels
      // have zero group children). Parsing rejects multiple group children at one level.
      Check.checkState(
          groupChildren.size() == 0,
          "grouping level %s must have exactly one nested facet group child, found %s",
          this.name,
          groupChildren.size());
      nestedBranchName = Optional.empty();
    }
    builder.addGroupLevel(this.path.toString(), nestedBranchName);

    for (AggregationSpec spec : this.children) {
      spec.buildShardMergePipeline(builder);
    }
  }

  public FieldPath getPath() {
    return this.path;
  }
}
