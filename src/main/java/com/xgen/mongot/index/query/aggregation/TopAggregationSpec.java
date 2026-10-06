package com.xgen.mongot.index.query.aggregation;

import com.google.common.math.LongMath;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Root-level aggregation spec. */
public record TopAggregationSpec(List<AggregationSpec> children) implements AggregationSpec {

  /**
   * Maximum requested bucket space for a search-aggregation query. Nested grouping is a cartesian
   * product: the product of per-node bucket counts down each path, summed across independent
   * top-level trees, must not exceed this cap.
   */
  public static final long MAX_REQUESTED_BUCKETS = 100_000L;

  @Override
  public String name() {
    throw new UnsupportedOperationException("TopAggregationSpec has no name");
  }

  /**
   * Requested bucket space of this tree: the sum of each top-level child's {@link
   * AggregationSpec#requestedBucketCount()}. Nested grouping is a cartesian product computed on
   * each {@link GroupAggregationSpec}; metric leaves contribute 0.
   */
  @Override
  public long requestedBucketCount() {
    return this.children.stream()
        .mapToLong(AggregationSpec::requestedBucketCount)
        .reduce(0L, LongMath::saturatedAdd);
  }

  /**
   * User-facing error when {@link #requestedBucketCount()} exceeds {@link #MAX_REQUESTED_BUCKETS}.
   * Wording follows search-facet parse errors ({@code must be within bounds [1..N]}).
   */
  public static String requestedBucketsLimitExceededMessage(long requested) {
    return String.format(
        "The requested number of aggregation buckets must be within bounds [1..%d] (is %d)",
        MAX_REQUESTED_BUCKETS, requested);
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
      builder.beginPath(child.name());
      child.buildShardMergePipeline(builder);
      builder.endPath();
    }
  }

  /** Builds a merge-pipeline builder with facet/metric aliases for this spec tree. */
  public ShardMergePipelineBuilder newShardMergePipelineBuilder(List<Integer> sortDirections) {
    ShardMergeAliasPlan plan = ShardMergeAliasPlan.from(this, sortDirections);
    ShardMergePipelineBuilder builder =
        sortDirections.isEmpty()
            ? ShardMergePipelineBuilder.create(plan)
            : ShardMergePipelineBuilder.forSort(plan, sortDirections);
    buildShardMergePipeline(builder);
    return builder;
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
    public boolean supportsStateMerge() {
      // One bucket, no state of its own. Mergeability is the children's.
      return childrenSupportStateMerge();
    }

    @Override
    public void mergeStateFrom(Accumulator other) {
      mergeChildStatesFrom(Check.instanceOf(other, TopLevelAccumulator.class));
    }

    @Override
    protected List<AggregationResult> buildResults(LongArray ordsToCollect) throws IOException {
      // Since only bucket ordinal 0 is used to collect from root node, we only collect that.
      if (ordsToCollect.size() != 1 || ordsToCollect.get(0) != 0) {
        throw new IllegalStateException(
            "Top-level accumulator expects exactly ordinal 0, got: "
                + ordsToCollect.size()
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
