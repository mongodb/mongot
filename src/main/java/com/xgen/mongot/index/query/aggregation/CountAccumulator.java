package com.xgen.mongot.index.query.aggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Lucene leaf accumulator for the {@code count} aggregation operator. */
final class CountAccumulator extends Accumulator {
  private final LongArray counts;

  CountAccumulator(String name, AggregationContext context) {
    super(name, List.of(), context);
    this.counts = new LongArray(8);
    declareMetric(Optional.empty());
  }

  @Override
  public LeafBucketCollector getLeafCollector(AggregationContext context) {
    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) {
        maybeRecordCollectInvocation();
        // Deliberately no context.isMultiValued(doc) guard, and no metric doc-values lookup.
        //
        // sum/avg skip a document that an ancestor group field fanned into several buckets, because
        // adding its value to each of them double-counts a single measurement. A count is not a
        // measurement of the document, it is a measurement of the bucket: a document that genuinely
        // belongs to three buckets contributes one document to each. Skipping it here would make
        // count disagree with the bucket doc count that the grouping level already reports for the
        // same bucket, and with the legacy string facet's count for the same query.
        CountAccumulator.this.counts.increment(Math.toIntExact(owningBucketOrd), 1L);
      }
    };
  }

  @Override
  protected List<AggregationResult> buildResults(LongArray ordsToCollect) {
    List<AggregationResult> results = new ArrayList<>();
    for (int i = 0; i < ordsToCollect.size(); i++) {
      int bucketOrd = (int) ordsToCollect.get(i);
      // A bucket that exists but collected nothing counts zero rather than being absent, matching
      // sum's 0.0 for an empty bucket.
      long count = bucketOrd < this.counts.size() ? this.counts.get(bucketOrd) : 0L;
      results.add(new CountAggregationResult(this.name, count));
    }
    return results;
  }
}
