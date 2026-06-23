package com.xgen.mongot.index.query.aggregation;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.bson.BsonValue;

/**
 * Buckets numeric / date doc values into a fixed set of half-open ranges.
 *
 * <p>Bucket ordinals are a fixed dense grid -- no hash map and no per-strategy class -- because the
 * number of buckets per parent ({@code numRanges (+1 default)}) is known at construction.
 */
final class BoundaryAccumulator extends BucketsAccumulator {
  private final FieldPath path;
  private final String luceneField;
  private final long[] edges; // encoded boundary edges, length numRanges + 1
  private final int numRanges;
  private final boolean lastEdgeInclusive;
  private final boolean hasDefault;
  private final int defaultSlot;
  private final int bucketsPerParent;
  private final List<BsonValue> rangeKeys; // lower-bound key per range (size numRanges)
  private final Optional<BsonValue> defaultKey;

  BoundaryAccumulator(
      BoundaryGroupAggregationSpec spec,
      AggregationContext context,
      List<Accumulator> childAccumulators) {
    super(spec.name(), childAccumulators, context);
    this.path = spec.getPath();
    this.luceneField = context.resolveLuceneFieldName(this.path);
    this.edges = spec.boundaryEdges();
    this.numRanges = this.edges.length - 1;
    this.lastEdgeInclusive = spec.lastEdgeInclusive();
    this.rangeKeys = spec.rangeKeys();
    this.defaultKey = spec.defaultBucketKey();
    this.hasDefault = this.defaultKey.isPresent();
    this.defaultSlot = this.numRanges;
    this.bucketsPerParent = this.numRanges + (this.hasDefault ? 1 : 0);
  }

  private long childBucketOrd(long owningBucketOrd, int slot) {
    return owningBucketOrd * this.bucketsPerParent + slot;
  }

  /**
   * Index of the half-open range [edges[i], edges[i+1]) containing {@code v}, or -1 (NO_MATCH) when
   * {@code v} is below the first edge or at/above the last edge (unless the last edge is
   * inclusive). Comparison happens in encoded long space; every encoder is order-preserving and the
   * doc value uses the same encoding as the edges, so this matches value-space comparison exactly.
   */
  private int rangeIndexOf(long v) {
    if (v < this.edges[0]) {
      return -1;
    }
    if (v >= this.edges[this.numRanges]) {
      return (this.lastEdgeInclusive && v == this.edges[this.numRanges]) ? this.numRanges - 1 : -1;
    }
    // Largest i in [0, numRanges-1] with edges[i] <= v. edges[i+1] > v holds by the bound above.
    @Var int lo = 0;
    @Var int hi = this.numRanges - 1;
    while (lo < hi) {
      // Ensure we terminate with lo+1 == hi
      int mid = (lo + hi + 1) >>> 1;
      if (this.edges[mid] <= v) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }

  @Override
  protected LeafBucketCollector getLeafCollector(
      AggregationContext context, LeafBucketCollector children) throws IOException {
    LeafReaderContext leaf = context.getLeafReaderContext();
    SortedNumericDocValues values = DocValues.getSortedNumeric(leaf.reader(), this.luceneField);

    // Single-valued fast path (V1 facet fields, or any single-valued V2 field).
    Optional<NumericDocValues> single = Optional.ofNullable(DocValues.unwrapSingleton(values));
    if (single.isPresent()) {
      NumericDocValues docValues = single.get();
      return new LeafBucketCollector() {
        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
          if (!docValues.advanceExact(doc)) {
            collectDefault(children, doc, owningBucketOrd);
            return;
          }
          int r = rangeIndexOf(docValues.longValue());
          if (r >= 0) {
            collectBucket(children, doc, childBucketOrd(owningBucketOrd, r));
          } else {
            collectDefault(children, doc, owningBucketOrd);
          }
        }
      };
    }

    // Multi-valued path (V2 SortedNumericDocValues). Values arrive sorted ascending, so the matched
    // range index is monotonic non-decreasing; the lastRange guard de-dupes a doc that has several
    // values in the same range.
    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) throws IOException {
        if (!values.advanceExact(doc)) {
          collectDefault(children, doc, owningBucketOrd);
          return;
        }
        int count = values.docValueCount();
        @Var int lastRange = -1;
        @Var boolean matchedAny = false;
        for (int i = 0; i < count; i++) {
          int r = rangeIndexOf(values.nextValue());
          if (r >= 0 && r != lastRange) {
            collectBucket(children, doc, childBucketOrd(owningBucketOrd, r));
            lastRange = r;
            matchedAny = true;
          }
        }
        if (!matchedAny) {
          collectDefault(children, doc, owningBucketOrd);
        }
      }
    };
  }

  private void collectDefault(LeafBucketCollector children, int doc, long owningBucketOrd)
      throws IOException {
    if (this.hasDefault) {
      collectBucket(children, doc, childBucketOrd(owningBucketOrd, this.defaultSlot));
    }
    // No default configured => out-of-range docs are simply not counted, matching legacy facets.
  }

  @Override
  protected List<AggregationResult> buildResults(LongArray parentOrdsToCollect) throws IOException {
    int numParents = parentOrdsToCollect.size();

    // Flatten every (parent, slot) child owning ord in parent-major / slot-minor order so the
    // childResults index advances in lockstep with the build loop below.
    long[] allChildOrds = new long[numParents * this.bucketsPerParent];
    @Var int k = 0;
    for (int p = 0; p < numParents; p++) {
      long parentOrd = parentOrdsToCollect.get(p);
      for (int slot = 0; slot < this.bucketsPerParent; slot++) {
        allChildOrds[k++] = childBucketOrd(parentOrd, slot);
      }
    }
    IntFunction<AggregationResults> childResultsFor =
        buildSubAggsForBuckets(LongArray.from(allChildOrds));

    List<AggregationResult> results = new ArrayList<>(numParents);
    @Var int childIndex = 0;
    for (int p = 0; p < numParents; p++) {
      long parentOrd = parentOrdsToCollect.get(p);
      List<BoundaryAggregationResult.BoundaryBucket> buckets =
          new ArrayList<>(this.bucketsPerParent);
      // Every range bucket is emitted, even when count == 0 (range facets report all buckets).
      for (int r = 0; r < this.numRanges; r++) {
        long childOrd = childBucketOrd(parentOrd, r);
        buckets.add(
            new BoundaryAggregationResult.BoundaryBucket(
                this.rangeKeys.get(r),
                bucketDocCount(childOrd),
                childResultsFor.apply(childIndex++)));
      }
      if (this.hasDefault) {
        long childOrd = childBucketOrd(parentOrd, this.defaultSlot);
        buckets.add(
            new BoundaryAggregationResult.BoundaryBucket(
                this.defaultKey.get(),
                bucketDocCount(childOrd),
                childResultsFor.apply(childIndex++)));
      }
      results.add(new BoundaryAggregationResult(this.name, this.path, buckets));
    }
    return results;
  }
}
