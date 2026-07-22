package com.xgen.mongot.index.lucene.facet;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.jetbrains.annotations.Nullable;

/**
 * Facet counts over the <b>matching children</b> of an {@code embeddedDocument}-scoped facet,
 * deduplicated to the root document: a root contributes at most once to each bucket, no matter how
 * many of its matching children fall in that bucket, but it contributes to <em>every</em> bucket it
 * has a matching child in.
 *
 * <p>Unlike {@link SortedSetDocValuesFacetCounts}, which increments a bucket once per matching
 * Lucene doc, this counter is fed the <em>child</em> docs that match the facet's child operator and
 * groups them by their block-join parent (identified by {@code parentFilter}). Within each parent
 * block it gathers the distinct facet ordinals across all matching children and increments each by
 * one. This yields the distinct-parent-per-bucket semantic required by embedded-scoped faceting.
 *
 * <p>Children of a parent form a contiguous block that precedes the parent's root doc, and the
 * matching-child iterator visits docs in ascending order, so grouping by {@code
 * parents.nextSetBit(childDoc)} correctly partitions children by parent without buffering the whole
 * segment.
 *
 * <p>Counting is gated to <b>result parents</b> — the root documents that satisfy the whole query.
 * A matching child whose parent did not match the main query is skipped, so a facet reports counts
 * only over returned documents, not over every root in the index that happens to have a matching
 * child.
 */
public class ParentDedupedSortedSetDocValuesFacetCounts
    extends AbstractSortedSetDocValuesFacetCounts {

  private final int[] counts;
  private final BitSetProducer parentFilter;

  /** Per-leaf (keyed by {@link LeafReaderContext#ord}) bitset of result-parent root doc ids. */
  private final Map<Integer, FixedBitSet> resultParentsByLeaf;

  /**
   * Builds per-root-deduplicated facet counts over the matching children, gated to result parents.
   *
   * @param state facet state for the child-scoped facet field (e.g. the {@code token} field indexed
   *     under the embedded path).
   * @param childHits the matching <em>child</em> docs, collected by running the facet's {@code
   *     embeddedDocument.operator} as a Lucene query.
   * @param parentFilter identifies the block-join parent (root) doc of each child; typically the
   *     root parent filter from {@code EmbeddedDocumentQueryFactory}.
   * @param resultParentHits the main query's hits — the result-parent root docs. Counting is
   *     restricted to matching children of these roots, so facets reflect only returned documents.
   */
  public ParentDedupedSortedSetDocValuesFacetCounts(
      SortedSetDocValuesReaderState state,
      FacetsCollector childHits,
      BitSetProducer parentFilter,
      FacetsCollector resultParentHits)
      throws IOException {
    super(state);
    this.counts = new int[state.getSize()];
    this.parentFilter = parentFilter;
    this.resultParentsByLeaf = buildResultParentsByLeaf(resultParentHits, state.getReader());
    count(childHits.getMatchingDocs());
  }

  /**
   * Guards against a caller collecting {@code childHits}/{@code resultParentHits} against a
   * different top-level reader than the facet {@code state} was built for. Without this, {@link
   * LeafReaderContext#ord} would not align across inputs and counts could be silently dropped or
   * throw an out-of-bounds error. Mirrors the check in {@link SortedSetDocValuesFacetCounts}.
   */
  private static void checkSameReader(MatchingDocs hits, IndexReader stateReader) {
    if (ReaderUtil.getTopLevelContext(hits.context()).reader() != stateReader) {
      throw new IllegalStateException(
          "the SortedSetDocValuesReaderState provided to this class does not match the reader being"
              + " searched; you must create a new SortedSetDocValuesReaderState every time you open"
              + " a new IndexReader");
    }
  }

  /** Materializes the main query's per-leaf matching root docs into random-access bitsets. */
  private static Map<Integer, FixedBitSet> buildResultParentsByLeaf(
      FacetsCollector resultParentHits, IndexReader stateReader) throws IOException {
    Map<Integer, FixedBitSet> byLeaf = new HashMap<>();
    for (MatchingDocs hits : resultParentHits.getMatchingDocs()) {
      if (hits.totalHits() == 0) {
        continue;
      }
      checkSameReader(hits, stateReader);
      FixedBitSet bits = new FixedBitSet(hits.context().reader().maxDoc());
      DocIdSetIterator it = hits.bits().iterator();
      if (it == null) {
        continue;
      }
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        bits.set(doc);
      }
      byLeaf.put(hits.context().ord, bits);
    }
    return byLeaf;
  }

  @Override
  boolean hasCounts() {
    return true;
  }

  @Override
  int getCount(int ord) {
    return this.counts[ord];
  }

  @Override
  PrimitiveIterator.OfInt getOrdIterator() {
    return IntStream.range(0, this.counts.length).iterator();
  }

  private void count(List<MatchingDocs> matchingDocs) throws IOException {
    @Nullable
    OrdinalMap ordinalMap =
        this.dv instanceof org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues multi
                && matchingDocs.size() > 1
            ? multi.mapping
            : null;

    for (MatchingDocs hits : matchingDocs) {
      if (hits.totalHits() == 0) {
        continue;
      }
      checkSameReader(hits, this.state.getReader());
      countOneSegment(ordinalMap, hits);
    }
  }

  private void countOneSegment(@Nullable OrdinalMap ordinalMap, MatchingDocs hits)
      throws IOException {
    LeafReaderContext context = hits.context();
    SortedSetDocValues values = DocValues.getSortedSet(context.reader(), this.field);
    if (values == null) {
      // No values for this field in this segment: nothing to count.
      return;
    }
    BitSet parents = this.parentFilter.getBitSet(context);
    if (parents == null) {
      return;
    }
    // Result parents in this leaf; if the main query matched no roots here, nothing to count.
    FixedBitSet resultParents = this.resultParentsByLeaf.get(context.ord);
    if (resultParents == null) {
      return;
    }

    // Maps this segment's local ordinals to global ordinals; null when a single segment is present
    // (local ordinals are already global).
    LongValues ordMap = ordinalMap == null ? null : ordinalMap.getGlobalOrds(context.ord);

    // Advance the matching-child iterator and the doc-values iterator together, so `values` is
    // positioned on each matching child that has a value.
    DocIdSetIterator childIt =
        ConjunctionUtils.intersectIterators(Arrays.asList(hits.bits().iterator(), values));

    // Distinct global ordinals seen among the current parent's matching children.
    IntHashSet parentOrds = new IntHashSet();
    @Var int currentParent = -1;
    @Var boolean currentParentIsResult = false;
    for (int doc = childIt.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = childIt.nextDoc()) {
      int parent = parents.nextSetBit(doc);
      if (parent != currentParent) {
        flush(parentOrds);
        parentOrds.clear();
        currentParent = parent;
        // Gate to result parents: skip a whole block whose root did not satisfy the main query.
        currentParentIsResult =
            parent != DocIdSetIterator.NO_MORE_DOCS && resultParents.get(parent);
      }
      if (!currentParentIsResult) {
        continue;
      }
      for (int i = 0; i < values.docValueCount(); i++) {
        int ord = (int) values.nextOrd();
        parentOrds.add(ordMap == null ? ord : (int) ordMap.get(ord));
      }
    }
    flush(parentOrds);
  }

  /** Increments each distinct ordinal in the current parent block exactly once. */
  private void flush(IntHashSet parentOrds) {
    for (var cursor : parentOrds) {
      this.counts[cursor.value]++;
    }
  }
}
