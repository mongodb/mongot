package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;

/**
 * Represent an intermediate node in the accumulator tree.
 * */
public abstract class BucketsAccumulator extends Accumulator {

  protected final BucketCollector childrenCollector;

  protected BucketsAccumulator(
      String name,
      Optional<Accumulator> parent,
      List<AggregationSpec> childrenSpecs,
      AggregationContext context) {
    super(name, parent, childrenSpecs, context);
    this.childrenCollector = MultiBucketCollector.of(this.children);
  }

  @Override
  public final LeafBucketCollector getLeafCollector(AggregationContext context) throws IOException {
    LeafBucketCollector children = this.childrenCollector.getLeafCollector(context);
    return this.getLeafCollector(context, children);
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
      LongArray bucketOrdsToCollect) {
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
