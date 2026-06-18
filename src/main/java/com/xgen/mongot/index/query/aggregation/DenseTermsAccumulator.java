package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;

/**
 * TermsAccumulator when it comes to the root level.
 *
 * <p>
 *   This is an optimization by saving the cost remapping parent ordinals to children ordinals.
 * </p>
 */
final class DenseTermsAccumulator extends TermsAccumulator {

  public DenseTermsAccumulator(
      TermsGroupAggregationSpec spec,
      AggregationContext context,
      List<Accumulator> childAccumulators,
      SortedSetDocValues topLevelValues,
      Optional<OrdinalMap> ordinalMap) {
    super(spec, context, childAccumulators, topLevelValues, ordinalMap);
  }

  @Override
  protected void collectGlobalOrd(long owningBucketOrd, int doc, long globalOrd,
      LeafBucketCollector children) throws IOException {
    // Top level only so only expect single parent bucket.
    assert owningBucketOrd == 0;
    // use globalOrd as it is for child owning bucket; doc count tracked per global ord.
    collectBucket(children, doc, globalOrd);
  }

  @Override
  protected List<List<OrdPair>> childOwningOrds(LongArray parentOwningOrds) {
    assert parentOwningOrds.size() == 1 && parentOwningOrds.get(0) == 0;
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
}
