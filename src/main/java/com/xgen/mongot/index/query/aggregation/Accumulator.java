package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Runtime aggregation state that collects data during Lucene search.
 *
 * <p>Accumulator forms a tree through parent-child pointers. A child Accumulator corresponds to a
 * sub-aggregation. e.g. { bySku: {_id: { $sku: "$sku" }, total: { $count: {}}}} it forms a
 * Accumulator tree as Accumulator("a") -> Accumulator("b") -> CountAccumulator
 *
 * <p>The tree is constructed bottom-up: children are built first (see {@link
 * AggregationSpec#buildAccumulator}), then passed into the parent's constructor, which wires itself
 * as their parent backpointer. Consequently {@link #parent()} is empty during a node's own
 * construction and must not be read there.
 */
public abstract class Accumulator extends BucketCollector {
  protected final String name;
  protected final List<Accumulator> children;
  protected final AggregationContext context;

  // Wired by the parent's constructor after this node is fully built; empty for the root.
  private Optional<Accumulator> parent = Optional.empty();

  /**
   * Per-node explain counters. Present only when the query is explained, so ordinary searches never
   * allocate the stats tree. Presence is also the collect-path gate: {@link
   * #maybeRecordCollectInvocation} and {@link #maybeRecordObservedBuckets} no-op when empty.
   */
  private final Optional<AggregationNodeStats> nodeStats;

  // Note: it is safe to pass this pointer to child accumulator since children Accumulators
  // won't access the parent pointer until after the constructor returns.
  @SuppressWarnings("this-escape")
  public Accumulator(String name, List<Accumulator> children, AggregationContext context) {
    this.name = name;
    this.children = List.copyOf(children);
    this.context = context;
    this.nodeStats =
        context.recordExplainNodeStats()
            ? Optional.of(
                new AggregationNodeStats(
                    name,
                    this.children.stream().map(child -> child.nodeStats().orElseThrow()).toList()))
            : Optional.empty();
    for (Accumulator child : this.children) {
      child.parent = Optional.of(this);
    }
  }

  /**
   * Per-node runtime counters for this accumulator; empty when the query is not being explained.
   */
  public final Optional<AggregationNodeStats> nodeStats() {
    return this.nodeStats;
  }

  /**
   * Declares this node as a bucket-producing grouping. No-op when explain stats were not allocated.
   */
  protected final void declareGrouping(
      AggregationNodeStats.NodeType type, String path, int requestedBuckets) {
    this.nodeStats.ifPresent(stats -> stats.declareGrouping(type, path, requestedBuckets));
  }

  /** Declares this node as a metric leaf. No-op when explain stats were not allocated. */
  protected final void declareMetric(Optional<String> path) {
    this.nodeStats.ifPresent(stats -> stats.declareMetric(path));
  }

  /** One leaf-collector invocation, recorded only when the query is explained. */
  protected final void maybeRecordCollectInvocation() {
    this.nodeStats.ifPresent(AggregationNodeStats::recordCollectInvocation);
  }

  /** Per-node observed-bucket count, recorded only when the query is explained. */
  protected final void maybeRecordObservedBuckets(long count) {
    this.nodeStats.ifPresent(stats -> stats.recordObservedBuckets(count));
  }

  /** User set name of the aggregation. */
  public final String getName() {
    return this.name;
  }

  /** Walk up the tree and return None if this is the root. */
  public final Optional<Accumulator> parent() {
    return this.parent;
  }

  /** Get the accumulators for immediate sub-aggregations. */
  public final List<Accumulator> children() {
    return this.children;
  }

  /**
   * Build results for the given bucket ordinals.
   *
   * @param ordsToCollect the ordinals of the buckets that we want to collect from this aggregation.
   * @return the results for each ordinal, in the same order as the array of ordinals.
   */
  protected abstract List<AggregationResult> buildResults(LongArray ordsToCollect)
      throws IOException;

  /**
   * Build the result of this aggregation if it is at the root of the accumulator tree. If, instead,
   * it is a sub aggregation of another accumulator then the parent accumulator will call
   * buildResults(Collection&lt;Long&gt;).
   */
  public final AggregationResult buildTopLevelResult() throws IOException {
    return buildResults(LongArray.from(0L)).getFirst();
  }
}
