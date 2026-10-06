package com.xgen.mongot.index.query.aggregation;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;

/**
 * TermsAccumulator when it comes to the root level.
 *
 * <p>This is an optimization by saving the cost remapping parent ordinals to children ordinals.
 */
final class DenseTermsAccumulator extends TermsAccumulator {

  // Set after slice state is merged, and only on the terminal reduce. childOwningOrds then
  // returns the top-limit buckets so term lookup and result objects run for survivors only.
  private boolean terminalTopK = false;

  public DenseTermsAccumulator(
      TermsGroupAggregationSpec spec,
      AggregationContext context,
      List<Accumulator> childAccumulators,
      SortedSetDocValues topLevelValues,
      Optional<OrdinalMap> ordinalMap,
      String luceneField) {
    super(spec, context, childAccumulators, topLevelValues, ordinalMap, luceneField);
  }

  @Override
  protected void collectGlobalOrd(
      long owningBucketOrd, int doc, long globalOrd, LeafBucketCollector children)
      throws IOException {
    // Top level only so only expect single parent bucket.
    assert owningBucketOrd == 0;
    // use globalOrd as it is for child owning bucket; doc count tracked per global ord.
    collectBucket(children, doc, globalOrd);
  }

  @Override
  public boolean supportsStateMerge() {
    // Bucket ordinal is the reader-wide global ordinal, so slices of one reader share keys.
    return childrenSupportStateMerge();
  }

  @Override
  public void mergeStateFrom(Accumulator other) {
    DenseTermsAccumulator that = Check.instanceOf(other, DenseTermsAccumulator.class);
    mergeDocCountsFrom(that);
    mergeChildStatesFrom(that);
  }

  @Override
  public void enableTerminalTopK() {
    this.terminalTopK = true;
  }

  @Override
  protected List<List<OrdPair>> childOwningOrds(LongArray parentOwningOrds) {
    assert parentOwningOrds.size() == 1 && parentOwningOrds.get(0) == 0;
    if (this.terminalTopK) {
      SelectedBuckets selected = topBucketsByCount();
      // buildResults records the survivors. Record the rest so observed-bucket metrics stay the
      // full non-empty cardinality.
      long omitted = selected.nonEmpty() - selected.pairs().size();
      this.context.addObservedBuckets(omitted);
      maybeRecordObservedBuckets(omitted);
      return List.of(selected.pairs());
    }
    List<OrdPair> childPairs = new ArrayList<>();
    // Dense uses the identity mapping: the child owning ord IS the global ord. A bucket
    // exists iff it received at least one document.
    for (long ord = 0; ord < this.valueCount; ord++) {
      if (bucketDocCount(ord) > 0) {
        childPairs.add(new OrdPair(ord, ord));
      }
    }
    return List.of(childPairs);
  }

  private record OrdCount(long ord, long count) {}

  private record SelectedBuckets(List<OrdPair> pairs, long nonEmpty) {}

  /**
   * The {@code limit} most frequent non-empty buckets. Order matches {@link
   * TermsAggregationResult#applyLimit()}: count descending, ties broken by ascending term. Global
   * ordinal order is term order, so selection does not resolve keys. Survivors come back in
   * ascending ordinal order so the later term-dictionary walk is forward.
   */
  private SelectedBuckets topBucketsByCount() {
    Comparator<OrdCount> topKOrder =
        Comparator.comparingLong(OrdCount::count).reversed().thenComparingLong(OrdCount::ord);
    PriorityQueue<OrdCount> heap = new PriorityQueue<>(topKOrder.reversed());
    @Var long nonEmpty = 0;
    for (long ord = 0; ord < this.valueCount; ord++) {
      long count = bucketDocCount(ord);
      if (count == 0) {
        continue;
      }
      nonEmpty++;
      OrdCount candidate = new OrdCount(ord, count);
      if (heap.size() < this.limit) {
        heap.add(candidate);
      } else if (this.limit > 0 && topKOrder.compare(candidate, heap.peek()) < 0) {
        heap.poll();
        heap.add(candidate);
      }
    }
    List<OrdPair> pairs =
        heap.stream()
            .sorted(Comparator.comparingLong(OrdCount::ord))
            .map(oc -> new OrdPair(oc.ord(), oc.ord()))
            .toList();
    return new SelectedBuckets(pairs, nonEmpty);
  }
}
