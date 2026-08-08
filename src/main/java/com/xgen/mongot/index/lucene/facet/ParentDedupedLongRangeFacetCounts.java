package com.xgen.mongot.index.lucene.facet;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;

/**
 * Boundary (number/date) facet counts over the <b>matching children</b> of an {@code
 * embeddedDocument}-scoped facet, deduplicated to the root document: a root contributes at most
 * once to each range bucket (and at most once to the default bucket), no matter how many of its
 * matching children fall in that bucket, but it contributes to <em>every</em> bucket it has a
 * matching child in.
 *
 * <p>This is the number/date analog of {@link ParentDedupedSortedSetDocValuesFacetCounts}. It reads
 * the child-indexed numeric doc-values (numbers as sortable longs, dates as epoch-millis) of the
 * matching children, buckets each value against the facet's {@link LongRange}s, groups by the
 * block-join parent, and increments each distinct (parent, bucket) pair once.
 *
 * <p>The default bucket cannot be derived by subtracting range counts from the result-parent total
 * (as the document-indexed path does), because per-root dedup lets one root land in several range
 * buckets — the sum can exceed the parent count. Instead this counter tracks, per root, whether any
 * matching child fell outside every range, and reports that distinct-parent count as {@link
 * #defaultCount()}.
 *
 * <p>Counting is gated to <b>result parents</b> — the root documents that satisfy the whole query —
 * exactly as in {@link ParentDedupedSortedSetDocValuesFacetCounts}.
 */
public class ParentDedupedLongRangeFacetCounts {

  private final LongRange[] ranges;
  private final String field;
  private final BitSetProducer parentFilter;
  private final IndexReader reader;

  /** Per-leaf (keyed by {@link LeafReaderContext#ord}) bitset of result-parent root doc ids. */
  private final Map<Integer, FixedBitSet> resultParentsByLeaf;

  private final int[] counts;
  private int defaultCount;

  /**
   * Builds per-root-deduplicated boundary facet counts over the matching children, gated to result
   * parents.
   *
   * @param field the child-scoped numeric Lucene field (e.g. the number/date field indexed under
   *     the embedded path).
   * @param ranges the facet's boundary ranges, in the same encoded space as the doc-values
   *     (sortable long for doubles, raw long for int64/date) — as produced by {@code
   *     LuceneFacetContext#getRanges}.
   * @param childHits the matching <em>child</em> docs, collected by running the facet's {@code
   *     embeddedDocument.operator} as a Lucene query.
   * @param parentFilter identifies the block-join parent (root) doc of each child.
   * @param resultParentHits the main query's hits — the result-parent root docs.
   */
  public ParentDedupedLongRangeFacetCounts(
      String field,
      LongRange[] ranges,
      FacetsCollector childHits,
      BitSetProducer parentFilter,
      FacetsCollector resultParentHits,
      IndexReader reader)
      throws IOException {
    this.ranges = ranges;
    this.field = field;
    this.parentFilter = parentFilter;
    this.reader = reader;
    this.counts = new int[ranges.length];
    this.resultParentsByLeaf = buildResultParentsByLeaf(resultParentHits, reader);
    count(childHits.getMatchingDocs());
  }

  /**
   * Per-range distinct-result-parent counts, aligned with the {@code ranges} passed at
   * construction.
   */
  public int[] counts() {
    return this.counts;
  }

  /**
   * Distinct result parents that have at least one matching child whose value falls outside every
   * range (the default bucket).
   */
  public int defaultCount() {
    return this.defaultCount;
  }

  /**
   * Guards against a caller collecting hits against a different top-level reader than this counter
   * was built for; without it {@link LeafReaderContext#ord} would not align across inputs. Mirrors
   * {@link ParentDedupedSortedSetDocValuesFacetCounts}.
   */
  private static void checkSameReader(MatchingDocs hits, IndexReader reader) {
    if (ReaderUtil.getTopLevelContext(hits.context()).reader() != reader) {
      throw new IllegalStateException(
          "the reader the hits were collected against does not match the reader provided to this"
              + " counter");
    }
  }

  private static Map<Integer, FixedBitSet> buildResultParentsByLeaf(
      FacetsCollector resultParentHits, IndexReader reader) throws IOException {
    Map<Integer, FixedBitSet> byLeaf = new HashMap<>();
    for (MatchingDocs hits : resultParentHits.getMatchingDocs()) {
      if (hits.totalHits() == 0) {
        continue;
      }
      checkSameReader(hits, reader);
      FixedBitSet bits = new FixedBitSet(hits.context().reader().maxDoc());
      DocIdSetIterator it = hits.bits().iterator();
      if (it == null) {
        continue;
      }
      bits.or(it);
      byLeaf.put(hits.context().ord, bits);
    }
    return byLeaf;
  }

  private void count(java.util.List<MatchingDocs> matchingDocs) throws IOException {
    for (MatchingDocs hits : matchingDocs) {
      if (hits.totalHits() == 0) {
        continue;
      }
      checkSameReader(hits, this.reader);
      countOneSegment(hits);
    }
  }

  private void countOneSegment(MatchingDocs hits) throws IOException {
    LeafReaderContext context = hits.context();
    SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), this.field);
    if (values == null) {
      return;
    }
    BitSet parents = this.parentFilter.getBitSet(context);
    if (parents == null) {
      return;
    }
    FixedBitSet resultParents = this.resultParentsByLeaf.get(context.ord);
    if (resultParents == null) {
      return;
    }

    // Iterate the matching children directly (NOT intersected with the doc-values iterator): a
    // matching child that lacks a value for the facet field must still contribute to the default
    // bucket, matching the document-indexed boundary-facet semantics (default counts anything not
    // in a range, including missing values). `values.advanceExact(doc)` positions the doc-values on
    // each matching child that has a value; children in ascending doc order keep it monotonic.
    DocIdSetIterator childIt = hits.bits().iterator();

    // Distinct range buckets (and the default bucket) seen among the current parent's matching
    // children, reset per parent block. A boolean-per-range set avoids per-parent allocation.
    boolean[] parentBuckets = new boolean[this.ranges.length];
    @Var boolean parentHasDefault = false;
    @Var int currentParent = -1;
    @Var boolean currentParentIsResult = false;
    for (int doc = childIt.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = childIt.nextDoc()) {
      int parent = parents.nextSetBit(doc);
      if (parent != currentParent) {
        flush(parentBuckets, parentHasDefault);
        Arrays.fill(parentBuckets, false);
        parentHasDefault = false;
        currentParent = parent;
        currentParentIsResult =
            parent != DocIdSetIterator.NO_MORE_DOCS && resultParents.get(parent);
      }
      if (!currentParentIsResult) {
        continue;
      }
      if (!values.advanceExact(doc)) {
        // Matching child with no value for the facet field: contributes to the default bucket.
        parentHasDefault = true;
        continue;
      }
      for (int i = 0; i < values.docValueCount(); i++) {
        int bucket = rangeIndexOf(values.nextValue());
        if (bucket >= 0) {
          parentBuckets[bucket] = true;
        } else {
          parentHasDefault = true;
        }
      }
    }
    flush(parentBuckets, parentHasDefault);
  }

  /**
   * Index of the range that accepts {@code value}, or {@code -1} for the default bucket. The ranges
   * are sorted ascending by lower bound and are non-overlapping, so a binary search for the last
   * range whose lower bound is {@code <= value} finds the only candidate; {@link
   * org.apache.lucene.facet.range.LongRange#accept} then confirms it, handling the exclusive upper
   * bound and values that fall past the last range.
   */
  private int rangeIndexOf(long value) {
    @Var int lo = 0;
    @Var int hi = this.ranges.length - 1;
    @Var int candidate = -1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      if (this.ranges[mid].min <= value) {
        candidate = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return candidate >= 0 && this.ranges[candidate].accept(value) ? candidate : -1;
  }

  /** Increments each distinct bucket the current parent block landed in exactly once. */
  private void flush(boolean[] parentBuckets, boolean parentHasDefault) {
    for (int r = 0; r < parentBuckets.length; r++) {
      if (parentBuckets[r]) {
        this.counts[r]++;
      }
    }
    if (parentHasDefault) {
      this.defaultCount++;
    }
  }
}
