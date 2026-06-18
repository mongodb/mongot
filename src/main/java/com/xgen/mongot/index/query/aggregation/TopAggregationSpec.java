package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Root-level aggregation spec.
 */
public record TopAggregationSpec(List<AggregationSpec> children) implements AggregationSpec {

  @Override
  public String name() {
    throw new UnsupportedOperationException("TopAggregationSpec has no name");
  }

  @Override
  public Accumulator buildAccumulator(AggregationContext context) throws IOException {
    // Bottom-up: build the children's accumulators first.
    List<Accumulator> childAccumulators = new ArrayList<>(this.children.size());
    for (AggregationSpec childSpec : this.children) {
      childAccumulators.add(childSpec.buildAccumulator(context));
    }
    return new TopLevelAccumulator(context, childAccumulators);
  }

  @Override
  public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    // The final aggregation result top level name is tracked through begin/end path.
    for (AggregationSpec child : this.children) {
      builder.beginTag(child.name());
      child.buildShardMergePipeline(builder);
      builder.endTag();
    }
  }

  private static final class TopLevelAccumulator extends Accumulator {
    private final BucketCollector multiBucketCollector;

    TopLevelAccumulator(AggregationContext context, List<Accumulator> childAccumulators) {
      super("_top", childAccumulators, context);
      this.multiBucketCollector = MultiBucketCollector.of(this.children);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationContext context) throws IOException {
      // Dispatch to children collectors.
      return this.multiBucketCollector.getLeafCollector(context);
    }

    @Override
    protected List<AggregationResult> buildResults(LongArray ordsToCollect) throws IOException {
      // Since only bucket ordinal 0 is used to collect from root node, we only collect that.
      if (ordsToCollect.size() != 1 || ordsToCollect.get(0) != 0) {
        throw new IllegalStateException(
            "Top-level accumulator expects exactly ordinal 0, got: " + ordsToCollect.size()
                + " ords");
      }
      // Plain loop instead of stream: buildTopLevelResult throws a checked IOException.
      List<AggregationResult> childResults = new ArrayList<>(this.children.size());
      for (Accumulator child : this.children) {
        childResults.add(child.buildTopLevelResult());
      }
      return List.of(new TopLevelAggregationResult(childResults));
    }
  }
}
