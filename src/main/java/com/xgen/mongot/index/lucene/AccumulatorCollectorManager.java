package com.xgen.mongot.index.lucene;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.query.aggregation.Accumulator;
import com.xgen.mongot.index.query.aggregation.AggregationContext;
import com.xgen.mongot.index.query.aggregation.AggregationNodeStats;
import com.xgen.mongot.index.query.aggregation.AggregationResult;
import com.xgen.mongot.index.query.aggregation.AggregationResultBuilder;
import com.xgen.mongot.index.query.aggregation.AggregationSpec;
import com.xgen.mongot.index.query.aggregation.BucketCollector.BucketCollectorWrapper;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CheckedStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.lucene.search.CollectorManager;

/**
 * Bridges the Accumulator tree to Lucene. {@code newCollector()} builds a new accumulator tree per
 * search slice. {@code reduce()} merges primitive per-bucket state when the tree shares an ordinal
 * space, and otherwise folds per-slice results with an {@link AggregationResultBuilder}.
 */
public class AccumulatorCollectorManager
    implements CollectorManager<BucketCollectorWrapper, AggregationResult> {

  private final AggregationSpec rootSpec;
  private final AggregationContext context;

  /** Receives the merged per-node stats once every slice has been reduced. */
  private final Consumer<AggregationNodeStats> nodeStatsSink;

  // Only legal on the outermost count merge (single partition, non-sharded). Top-N by count is
  // not associative, and it only runs after slice state has been merged.
  private final boolean applyLimitOnReduce;

  public AccumulatorCollectorManager(
      AggregationSpec rootSpec,
      AggregationContext context,
      Consumer<AggregationNodeStats> nodeStatsSink,
      boolean applyLimitOnReduce) {
    this.rootSpec = rootSpec;
    this.context = context;
    this.nodeStatsSink = nodeStatsSink;
    this.applyLimitOnReduce = applyLimitOnReduce;
  }

  @Override
  public BucketCollectorWrapper newCollector() throws IOException {
    Accumulator accumulator = this.rootSpec.buildAccumulator(this.context);
    return accumulator.asCollector(this.context);
  }

  @Override
  public AggregationResult reduce(Collection<BucketCollectorWrapper> collectors)
      throws IOException {
    Check.checkState(!collectors.isEmpty(), "No collectors to reduce");
    List<Accumulator> accumulators = new ArrayList<>(collectors.size());
    for (BucketCollectorWrapper collector : collectors) {
      accumulators.add(Check.instanceOf(collector.bucketCollector(), Accumulator.class));
    }

    Accumulator first = accumulators.get(0);
    AggregationResult result;
    if (first.supportsStateMerge()) {
      for (int i = 1; i < accumulators.size(); i++) {
        first.mergeStateFrom(accumulators.get(i));
      }
      if (this.applyLimitOnReduce) {
        // Counts are fully merged, so this selects the same buckets a later applyLimit would,
        // without resolving terms for the ones that would be dropped.
        first.enableTerminalTopK();
      }
      result = first.buildTopLevelResult();
    } else {
      // Term fold. Partitions, nested groups, and first/last land here. buildTopLevelResult
      // throws IOException, so CheckedStream stands in for Stream.map. Each slice still resolves
      // every ordinal, because top-N on a partial slice would drop buckets another slice needs.
      List<AggregationResult> results =
          CheckedStream.from(accumulators).mapAndCollectChecked(Accumulator::buildTopLevelResult);
      AggregationResultBuilder builder = results.get(0).toBuilder();
      results.forEach(builder::add);
      result = builder.build();
    }

    // After build, so observed-bucket counters recorded while materializing are included.
    // Ordinary searches already recorded the process histogram on the context.
    if (this.context.recordExplainNodeStats()) {
      recordNodeStats(collectors);
    }
    return result;
  }

  /**
   * Folds every slice's node-stats tree into one and hands it to the sink. The process observed-
   * bucket histogram is recorded separately from {@link AggregationContext#observedBuckets()}; this
   * sink is the explain tree.
   */
  private void recordNodeStats(Collection<BucketCollectorWrapper> collectors) {
    @Var Optional<AggregationNodeStats> merged = Optional.empty();
    for (BucketCollectorWrapper collector : collectors) {
      AggregationNodeStats sliceStats =
          Check.instanceOf(collector.bucketCollector(), Accumulator.class)
              .nodeStats()
              .orElseThrow();
      if (merged.isEmpty()) {
        // Clone rather than merging into a slice's own tree, which would double-count that slice.
        merged = Optional.of(sliceStats.clone());
      } else {
        merged.get().merge(sliceStats);
      }
    }
    merged.ifPresent(this.nodeStatsSink);
  }
}
