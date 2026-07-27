package com.xgen.mongot.index.query.aggregation;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.util.TopLevelDocValues;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

/** The base class to aggregate on term based doc-value fields (TOKEN, UUID, ObjectID, etc). */
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
  protected final String luceneField;

  protected TermsAccumulator(
      TermsGroupAggregationSpec spec,
      AggregationContext context,
      List<Accumulator> childAccumulators,
      SortedSetDocValues topLevelValues,
      Optional<OrdinalMap> ordinalMap,
      String luceneField) {
    super(spec.name(), childAccumulators, context);
    this.path = spec.getPath();
    this.limit = spec.getLimit();
    this.topLevelValues = topLevelValues;
    this.ordinalMap = ordinalMap;
    this.valueCount = topLevelValues.getValueCount();
    this.luceneField = luceneField;
  }

  /**
   * Map a segment-local ordinal to a global ordinal.
   *
   * <p>segmentOrd is returned when there is only single segment, i.e. no ordinalMap.
   */
  protected final long segmentToGlobal(int segmentIndex, long segmentOrd) {
    return this.ordinalMap
        .map(map -> map.getGlobalOrds(segmentIndex).get(segmentOrd))
        .orElse(segmentOrd);
  }

  /**
   * Strategy-specific hook called from getLeafCollector for each (doc, globalOrd) pair.
   *
   * @param owningBucketOrd The ordinal from the parent collector's bucket that owns this document.
   * @param doc The document id.
   * @param globalOrd The global ordinal of the doc's term.
   * @param children The child collector to collect to.
   */
  protected abstract void collectGlobalOrd(
      long owningBucketOrd, int doc, long globalOrd, LeafBucketCollector children)
      throws IOException;

  // A pair of (the child bucket ordinal, the global ordinal of the term)
  protected record OrdPair(long childOwningBucketOrd, long globalOrd) {}

  /**
   * Strategy-specific iteration over children owning ordinals given parent ordinals.
   *
   * <p>In the collectGlobalOrd it maps from pair (the parent bucket ordinal that owns this doc, the
   * global ordinal of the term) to the next-level owning bucket ordinal. This function aggregates
   * all child-level owning buckets by parent-level owning bucket.
   */
  protected abstract List<List<OrdPair>> childOwningOrds(LongArray parentOrds);

  @Override
  protected final LeafBucketCollector getLeafCollector(
      AggregationContext context, LeafBucketCollector children) throws IOException {
    LeafReaderContext leaf = context.getLeafReaderContext();
    SortedSetDocValues segmentValues = DocValues.getSortedSet(leaf.reader(), this.luceneField);
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
        if (segmentValues.docValueCount() > 1) {
          context.markMultiValued(doc);
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
  protected final List<AggregationResult> buildResults(LongArray parentOrdsToCollect)
      throws IOException {
    // Collect all children owning buckets. Full cardinality is returned -- no top-N pruning here.
    // The `limit` is carried into the result and applied later (see AggregationResult#applyLimit)
    // so that segment / partition / shard merges all operate on the complete bucket set.
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
        buckets.add(
            new TermsAggregationResult.TermsBucket(
                key,
                bucketDocCount(pair.childOwningBucketOrd()),
                childResultsFor.apply(childOrdIndex++)));
      }
      results.add(new TermsAggregationResult(this.name, this.path, this.limit, buckets));
    }
    return results;
  }

  static TermsAccumulator create(
      TermsGroupAggregationSpec spec,
      AggregationContext context,
      List<Accumulator> childAccumulators,
      boolean isRoot,
      TypeField typeField)
      throws IOException {
    String luceneField = typeField.getLuceneFieldName(spec.getPath(), Optional.empty());
    // Prefer a cached top-level view (reuses a previously built global OrdinalMap); building the
    // OrdinalMap from scratch is expensive on large indexes.
    Optional<SortedSetDocValues> cachedValues =
        context.getCachedGlobalValueLookup().lookup(luceneField);
    SortedSetDocValues values =
        cachedValues.isPresent()
            ? cachedValues.get()
            : TopLevelDocValues.getDocValues(
                context.getIndexReader(), luceneField, Optional.empty());
    // None if single segment
    @Var Optional<OrdinalMap> ordinalMap = Optional.empty();
    if (values instanceof MultiSortedSetDocValues multiValues) {
      if (values.getValueCount() > Integer.MAX_VALUE) {
        throw new IllegalStateException(
            "Aggregation on string field is not supported when the number of unique terms exceeds "
                + "Integer.MAX_VALUE");
      }
      ordinalMap = Optional.of(multiValues.mapping);
    }
    if (isRoot) {
      return new DenseTermsAccumulator(
          spec, context, childAccumulators, values, ordinalMap, luceneField);
    } else {
      return new RemapTermsAccumulator(
          spec, context, childAccumulators, values, ordinalMap, luceneField);
    }
  }
}
