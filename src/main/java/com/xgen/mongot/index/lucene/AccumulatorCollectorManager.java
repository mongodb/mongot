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
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.lucene.search.CollectorManager;

/**
 * Bridges the Accumulator tree to Lucene. {@code newCollector()} builds a new accumulator tree per
 * search slice; {@code reduce()} folds slice results with an {@link AggregationResultBuilder}.
 */
public class AccumulatorCollectorManager
    implements CollectorManager<BucketCollectorWrapper, AggregationResult> {

  private final AggregationSpec rootSpec;
  private final AggregationContext context;

  /** Receives the merged per-node stats once every slice has been reduced. */
  private final Consumer<AggregationNodeStats> nodeStatsSink;

  public AccumulatorCollectorManager(
      AggregationSpec rootSpec,
      AggregationContext context,
      Consumer<AggregationNodeStats> nodeStatsSink) {
    this.rootSpec = rootSpec;
    this.context = context;
    this.nodeStatsSink = nodeStatsSink;
  }

  @Override
  public BucketCollectorWrapper newCollector() throws IOException {
    Accumulator accumulator = this.rootSpec.buildAccumulator(this.context);
    return accumulator.asCollector(this.context);
  }

  @Override
  public AggregationResult reduce(Collection<BucketCollectorWrapper> collectors)
      throws IOException {
    // buildTopLevelResult() throws IOException, so CheckedStream stands in for Stream.map.
    List<AggregationResult> results =
        CheckedStream.from(collectors)
            .mapAndCollectChecked(
                collector ->
                    Check.instanceOf(collector.bucketCollector(), Accumulator.class)
                        .buildTopLevelResult());
    Check.checkState(!results.isEmpty(), "No collectors to reduce");

    // Slice-tree merge is explain-only: ordinary searches already recorded the process
    // observed-bucket histogram on the context and do not need a merged stats tree.
    if (this.context.recordExplainNodeStats()) {
      recordNodeStats(collectors);
    }
    // get(0) only supplies the shape: the builder starts empty and every result folds in
    // uniformly. A future spec.newResultBuilder() would remove the need for a representative.
    AggregationResultBuilder builder = results.get(0).toBuilder();
    results.forEach(builder::add);
    return builder.build();
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
