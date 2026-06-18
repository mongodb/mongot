package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Root-level aggregation spec.
 */
public record TopAggregationSpec(List<AggregationSpec> children) implements AggregationSpec {

  @Override
  public String name() {
    throw new UnsupportedOperationException("TopAggregationSpec has no name");
  }

  @Override
  public Accumulator buildAccumulator(AggregationContext context, Optional<Accumulator> parent) {
    return new TopLevelAccumulator(context, this.children);
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
    private final MultiBucketCollector multiBucketCollector;

    TopLevelAccumulator(AggregationContext context, List<AggregationSpec> childrenSpecs) {
      super("_top", Optional.empty(), childrenSpecs, context);
      this.multiBucketCollector = MultiBucketCollector.of(this.children);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationContext context) throws IOException {
      // Dispatch to children collectors.
      return this.multiBucketCollector.getLeafCollector(context);
    }

    @Override
    protected List<AggregationResult> buildResults(LongArray ordsToCollect) {
      // Since only bucket ordinal 0 is used to collect from root node, we only collect that.
      if (ordsToCollect.size() != 1 || ordsToCollect.get(0) != 0) {
        throw new IllegalStateException(
            "Top-level accumulator expects exactly ordinal 0, got: " + ordsToCollect.size()
                + " ords");
      }
      List<AggregationResult> childResults =
          this.children.stream().map(Accumulator::buildTopLevelResult).toList();
      return List.of(new TopLevelAggregationResult(childResults));
    }
  }
}
