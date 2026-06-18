package com.xgen.mongot.index.query.aggregation;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;

/**
 * The base class to aggregate on term based doc-value fields (TOKEN, UUID, ObjectID, etc).
 */
public abstract sealed class TermsAccumulator extends BucketsAccumulator
    permits DenseTermsAccumulator, RemapTermsAccumulator {
  protected final FieldPath path;
  // Max number of buckets to return per parent bucket.
  protected final int limit;
  // For buildResults() -> ord->BytesRef
  protected final SortedSetDocValues topLevelValues;
  // None if single segment
  protected final Optional<OrdinalMap> ordinalMap;
  // global ordinal count.
  protected final long valueCount;

  protected TermsAccumulator(
      TermsGroupAggregationSpec spec,
      AggregationContext context,
      List<Accumulator> childAccumulators,
      SortedSetDocValues topLevelValues,
      Optional<OrdinalMap> ordinalMap) {
    super(spec.name(), childAccumulators, context);
    this.path = spec.getPath();
    this.limit = spec.getLimit();
    this.topLevelValues = topLevelValues;
    this.ordinalMap = ordinalMap;
    this.valueCount = topLevelValues.getValueCount();
  }

  /**
   * Map a segment-local ordinal to a global ordinal.
   *
   * <p>segmentOrd is returned when there is only single segment, i.e. no ordinalMap.
   */
  protected final long segmentToGlobal(int segmentIndex, long segmentOrd) {
    return this.ordinalMap
        .map(map -> map.getGlobalOrds(segmentIndex).get(segmentOrd)).orElse(segmentOrd);
  }

  /**
   * Strategy-specific hook called from getLeafCollector for each (doc, globalOrd) pair.
   *
   * @param owningBucketOrd The ordinal from the parent collector's bucket that owns this document.
   * @param doc The document id.
   * @param globalOrd The global ordinal of the doc's term.
   * @param children The child collector to collect to.
   */
  protected abstract void collectGlobalOrd(long owningBucketOrd, int doc, long globalOrd,
      LeafBucketCollector children) throws IOException;

  // A pair of (the child bucket ordinal, the global ordinal of the term)
  protected record OrdPair(long childOwningBucketOrd, long globalOrd) {}

  /**
   * Strategy-specific iteration over children owning ordinals given parent ordinals.
   *
   * <p>
   *   In the collectGlobalOrd it maps from pair (the parent bucket ordinal that owns this doc, the
   *   global ordinal of the term) to the next-level owning bucket ordinal. This function aggregates
   *   all child-level owning buckets by parent-level owning bucket.
   * </p>
   */
  protected abstract List<List<OrdPair>> childOwningOrds(LongArray parentOrds);

  @Override
  protected final LeafBucketCollector getLeafCollector(
      AggregationContext context, LeafBucketCollector children) throws IOException {
    LeafReaderContext leaf = context.getLeafReaderContext();
    SortedSetDocValues segmentValues =
        DocValues.getSortedSet(leaf.reader(), context.resolveLuceneFieldName(this.path));
    int segmentIndex = leaf.ord;

    // Single-value unwrap to avoid wrapping class overhead.
    Optional<SortedDocValues> singleValues =
        Optional.ofNullable(DocValues.unwrapSingleton(segmentValues));
    if (singleValues.isPresent()) {
      return new LeafBucketCollector() {
        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
          if (!singleValues.get().advanceExact(doc)) {
            return;
          }
          long globalOrd = segmentToGlobal(segmentIndex, singleValues.get().ordValue());
          collectGlobalOrd(owningBucketOrd, doc, globalOrd, children);
        }
      };
    }
    // Multi-value path
    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) throws IOException {
        if (!segmentValues.advanceExact(doc)) {
          return;
        }
        for (int i = 0; i < segmentValues.docValueCount(); i++) {
          long globalOrd = segmentToGlobal(segmentIndex, segmentValues.nextOrd());
          collectGlobalOrd(owningBucketOrd, doc, globalOrd, children);
        }
      }
    };
  }

  /**
   * Given all the parent bucket ordinals to collect, return the results for each ordinal.
   *
   * @return the results for each ordinal, in the same order as the array of ordinals.
   */
  @Override
  protected final List<AggregationResult> buildResults(
      LongArray parentOrdsToCollect) throws IOException {
    // Collect all children owning buckets. Full cardinality is returned -- no top-N pruning here.
    // The `limit` is carried into the result and applied later during mongot integration (so that
    // segment / partition / shard merges all operate on the complete bucket set).
    List<List<OrdPair>> childPairsPerParent = childOwningOrds(parentOrdsToCollect);

    // Flatten all child-level owning bucket ordinals to build children results.
    // Note: no distinct() is needed here as each child buckets should be uniq per parent
    // bucket.
    long[] allChildOwningOrds =
        childPairsPerParent.stream()
            .flatMap(List::stream)
            .mapToLong(OrdPair::childOwningBucketOrd)
            .toArray();
    IntFunction<AggregationResults> childResultsFor =
        this.buildSubAggsForBuckets(LongArray.from(allChildOwningOrds));

    List<AggregationResult> results = new ArrayList<>(childPairsPerParent.size());
    @Var int childOrdIndex = 0;
    for (List<OrdPair> childPairs : childPairsPerParent) {
      List<TermsAggregationResult.TermsBucket> buckets = new ArrayList<>(childPairs.size());
      for (OrdPair pair : childPairs) {
        BytesRef key = BytesRef.deepCopyOf(this.topLevelValues.lookupOrd(pair.globalOrd()));
        buckets.add(new TermsAggregationResult.TermsBucket(
            key,
            bucketDocCount(pair.childOwningBucketOrd()),
            childResultsFor.apply(childOrdIndex++)));
      }
      results.add(new TermsAggregationResult(this.name, this.path, this.limit, buckets));
    }
    return results;
  }

  /**
   * Logic forked from {@Link org.apache.lucene.facet.StringDocValuesReaderState} to build a
   * global SSDV across all the segments. The global OrdinalMap is included as well.
   */
  static SortedSetDocValues buildTopLevelValues(
      IndexReader reader, String luceneField) throws IOException {
    List<LeafReaderContext> leaves = reader.leaves();
    int leafCount = leaves.size();
    if (leafCount == 0) {
      return DocValues.emptySortedSet();
    }

    if (leafCount == 1) {
      return DocValues.getSortedSet(leaves.getFirst().reader(), luceneField);
    }

    SortedSetDocValues[] docValues = new SortedSetDocValues[leafCount];
    int[] starts = new int[leafCount + 1];
    @Var long cost = 0;
    for (int i = 0; i < leafCount; i++) {
      LeafReaderContext context = leaves.get(i);
      docValues[i] = DocValues.getSortedSet(context.reader(), luceneField);
      starts[i] = context.docBase;
      cost += docValues[i].cost();
    }
    starts[leafCount] = reader.maxDoc();
    Optional<IndexReader.CacheHelper> cacheHelper =
        Optional.ofNullable(reader.getReaderCacheHelper());
    Optional<IndexReader.CacheKey> owner = cacheHelper.map(IndexReader.CacheHelper::getKey);
    // Lucene OrdinalMap expect null CacheKey if it is not cacheable.
    // DEFAULT packed strategy is picked here to be aligned with lucene but can revisit if
    // memory cost is high.
    OrdinalMap map = OrdinalMap.build(owner.orElse(null), docValues, PackedInts.DEFAULT);
    SortedSetDocValues multiValues = new MultiSortedSetDocValues(docValues, starts, map, cost);
    if (multiValues.getValueCount() > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Aggregation on string field is not supported when the number of unique terms exceeds "
              + "Integer.MAX_VALUE");
    }
    return multiValues;
  }

  static TermsAccumulator create(TermsGroupAggregationSpec spec,
      AggregationContext context,
      List<Accumulator> childAccumulators,
      boolean isRoot) throws IOException {
    String luceneField = context.resolveLuceneFieldName(spec.getPath());
    SortedSetDocValues values = buildTopLevelValues(context.getIndexReader(), luceneField);
    // None if single segment
    @Var Optional<OrdinalMap> ordinalMap = Optional.empty();
    if (values instanceof MultiSortedSetDocValues multiValues) {
      ordinalMap = Optional.of(multiValues.mapping);
    }
    if (isRoot) {
      return new DenseTermsAccumulator(spec, context, childAccumulators, values, ordinalMap);
    } else {
      return new RemapTermsAccumulator(spec, context, childAccumulators, values, ordinalMap);
    }
  }
}
