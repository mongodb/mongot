package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

/** Represent an intermediate node in the accumulator tree. */
public abstract class BucketsAccumulator extends Accumulator {

  protected final BucketCollector childrenCollector;

  // Per-bucket document counts, indexed by this accumulator's bucket ordinal. Shared by all
  // bucket-producing accumulators (terms now, boundary later) for top-N ordering and for
  // reporting bucket counts in results.
  private final LongArray docCounts = new LongArray(64);

  protected BucketsAccumulator(
      String name, List<Accumulator> childAccumulators, AggregationContext context) {
    super(name, childAccumulators, context);
    this.childrenCollector = MultiBucketCollector.of(this.children);
  }

  /**
   * Count one document into {@code bucketOrd} and forward the collection to children. This is the
   * single entry point bucket-producing accumulators should use from their leaf collectors.
   */
  protected final void collectBucket(LeafBucketCollector children, int doc, long bucketOrd)
      throws IOException {
    maybeRecordCollectInvocation();
    this.docCounts.increment(Math.toIntExact(bucketOrd), 1);
    children.collect(doc, bucketOrd);
  }

  /** Number of documents collected into {@code bucketOrd}; 0 if the bucket never received docs. */
  protected final long bucketDocCount(long bucketOrd) {
    int index = Math.toIntExact(bucketOrd);
    return index >= this.docCounts.size() ? 0 : this.docCounts.get(index);
  }

  @Override
  public final LeafBucketCollector getLeafCollector(AggregationContext context) throws IOException {
    // Use this accumulator's build-time context (e.g. branch-scoped MultiValueDocMarker) with the
    // runtime leaf binding, not the root context passed from asCollector().
    AggregationContext bound = this.context.withLeafReaderContext(context.getLeafReaderContext());
    bound.getMultiValueDocMarker().ifPresent(MultiValueDocMarker::reset);
    LeafBucketCollector children = this.childrenCollector.getLeafCollector(bound);
    return this.getLeafCollector(bound, children);
  }

  /**
   * Collect results for this leaf.
   *
   * <p>Most implementations will return a custom LeafBucketCollector that collects additional
   * document information for every hit. Intermediate tree node will have to call collection on
   * children leaf collector, while the leaf tree node will expect empty children collector.
   */
  protected abstract LeafBucketCollector getLeafCollector(
      AggregationContext context, LeafBucketCollector children) throws IOException;

  /**
   * Utility to build results of the children accumulators of the buckets at each of the provided
   * ordinals.
   *
   * @param bucketOrdsToCollect the ordinals of the buckets that we want to collect from this
   *     aggregation. They have to be the owning bucket ordinals used when collecting children.
   * @return a function that, given a bucket ordinal, returns the results for the children
   *     accumulators of that bucket. Each AggregationResult within the AggregationResults
   *     represents one sub accumulator.
   */
  protected final IntFunction<AggregationResults> buildSubAggsForBuckets(
      LongArray bucketOrdsToCollect) throws IOException {
    List<List<AggregationResult>> results = new ArrayList<>();

    for (int i = 0; i < this.children.size(); i++) {
      List<AggregationResult> childResults = this.children.get(i).buildResults(bucketOrdsToCollect);
      results.add(i, childResults);
    }

    // Transpose the results matrix to have ordinal as the first index.
    return ord ->
        AggregationResults.from(
            new AbstractList<>() {
              @Override
              public AggregationResult get(int index) {
                return results.get(index).get(ord);
              }

              @Override
              public int size() {
                return results.size();
              }
            });
  }
}
