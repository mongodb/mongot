package com.xgen.mongot.index.query.aggregation;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;

/**
 * TermsAccumulator when it comes to the intermediate level.
 *
 * <p>It maps a pair of parent owning bucket ordinal and global ordinal to the child owning bucket
 * ordinal.
 */
public final class RemapTermsAccumulator extends TermsAccumulator {
  // Maps (parentOwningBucketOrd << 32 | globalOrd) to the child owning bucket ordinal.
  // Note that bucket ord is bounded by Integer.MAX_VALUE.
  private final LongLongHashMap bucketOrds = new LongLongHashMap();
  @Var private long nextBucketOrd = 0;

  public RemapTermsAccumulator(
      TermsGroupAggregationSpec spec,
      AggregationContext context,
      List<Accumulator> childAccumulators,
      SortedSetDocValues topLevelValues,
      Optional<OrdinalMap> ordinalMap) {
    super(spec, context, childAccumulators, topLevelValues, ordinalMap);
  }

  @Override
  protected void collectGlobalOrd(
      long owningBucketOrd, int doc, long globalOrd, LeafBucketCollector children)
      throws IOException {
    long key = encode(owningBucketOrd, globalOrd);
    int slot = this.bucketOrds.indexOf(key);
    long bucketOrd;
    if (this.bucketOrds.indexExists(slot)) {
      bucketOrd = this.bucketOrds.indexGet(slot);
    } else {
      // allocate a new child bucket ord
      bucketOrd = this.nextBucketOrd++;
      this.bucketOrds.indexInsert(slot, key, bucketOrd);
    }
    collectBucket(children, doc, bucketOrd);
  }

  @Override
  protected List<List<OrdPair>> childOwningOrds(LongArray parentOwningOrd) {
    // build a index from parent ord -> index in the output list
    LongIntHashMap parentToIndex = new LongIntHashMap(Math.toIntExact(parentOwningOrd.size()));
    List<List<OrdPair>> result = new ArrayList<>(parentOwningOrd.size());
    for (int i = 0; i < parentOwningOrd.size(); i++) {
      parentToIndex.put(parentOwningOrd.get(i), i);
      result.add(new ArrayList<>());
    }

    // now iterate all buckets and extract parent bucket -> [child bucket, global ord]
    for (LongLongCursor cursor : this.bucketOrds) {
      long parentOrd = decodeOwningBucketOrd(cursor.key);
      // -1 indicates the parent bucket is not in the parentOwningOrd array.
      int index = parentToIndex.getOrDefault(parentOrd, -1);
      if (index >= 0) {
        result.get(index).add(new OrdPair(cursor.value, decodeGlobalOrd(cursor.key)));
      }
    }
    return result;
  }

  private static long encode(long owningBucketOrd, long globalOrd) {
    return owningBucketOrd << 32 | globalOrd;
  }

  private static long decodeOwningBucketOrd(long k) {
    return k >>> 32;
  }

  private static long decodeGlobalOrd(long k) {
    return k & 0xFFFFFFFFL;
  }
}
