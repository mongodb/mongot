package com.xgen.mongot.index.query.aggregation;

import java.util.List;
import java.util.Optional;

/**
 * Runtime aggregation state that collects data during Lucene search.
 *
 * <p>Accumulator forms a tree through parent-child pointers. A child Accumulator corresponds to a
 * sub-aggregation. e.g. { bySku:  {_id: { $sku: "$sku" }, total: { $count: {}}}} it
 * forms a Accumulator tree as Accumulator("a") -> Accumulator("b") -> CountAccumulator
 */
public abstract class Accumulator extends BucketCollector {
  protected final String name;
  protected final Optional<Accumulator> parent;
  protected final List<Accumulator> children;
  protected final AggregationContext context;

  public Accumulator(
      String name,
      Optional<Accumulator> parent,
      List<AggregationSpec> childrenSpecs,
      AggregationContext context) {
    this.name = name;
    this.parent = parent;
    this.children =
        childrenSpecs.stream()
            .map(spec -> spec.buildAccumulator(context, Optional.of(this)))
            .toList();
    this.context = context;
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
  protected abstract List<AggregationResult> buildResults(LongArray ordsToCollect);

  /**
   * Build the result of this aggregation if it is at the root of the accumulator tree. If, instead,
   * it is a sub aggregation of another accumulator then the parent accumulator will call
   * buildResults(Collection&lt;Long&gt;).
   */
  public final AggregationResult buildTopLevelResult() {
    return buildResults(LongArray.from(0L)).getFirst();
  }
}
