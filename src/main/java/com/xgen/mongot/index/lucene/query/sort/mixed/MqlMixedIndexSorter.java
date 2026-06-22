package com.xgen.mongot.index.lucene.query.sort.mixed;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortSelector;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.bson.BsonValue;

/**
 * {@link IndexSorter} for multi-type fields. Both code paths delegate to {@link
 * MixedLeafFieldComparator} via {@link #createSortedLeaves}:
 *
 * <ul>
 *   <li>{@link #getDocComparator} — called during segment flush. Iterates each leaf via {@link
 *       MixedLeafFieldComparator#nextDoc()} to materialize a per-doc {@code BsonValue[]}. Leaves
 *       are processed in reverse bracket order so the most competitive type overwrites last.
 *   <li>{@link #getComparableProviders} — called during segment merge. Uses lazy per-doc reading
 *       via {@link MixedLeafFieldComparator#hasValue}/{@link
 *       MixedLeafFieldComparator#getCurrentValue()} and K-way merge with a {@link PriorityQueue}.
 * </ul>
 */
class MqlMixedIndexSorter implements IndexSorter {

  private final MqlMixedSort parent;
  private final UserFieldSortOptions userOpts;
  private final NullEmptySortPosition nullPos;
  private final BsonValue missingValue;
  private final boolean reverse;
  private final SortSelector selector;

  MqlMixedIndexSorter(MqlMixedSort parent, UserFieldSortOptions userOpts) {
    this.parent = parent;
    this.userOpts = userOpts;
    this.nullPos = userOpts.nullEmptySortPosition();
    this.missingValue = this.nullPos.getNullMissingSortValue();
    this.reverse = parent.getReverse();
    this.selector = userOpts.selector();
  }

  /**
   * Materializes per-doc values from a segment by iterating each type column via {@link
   * MixedLeafFieldComparator#nextDoc()}, which works for both buffered (flush-time) and committed
   * readers. Leaves from {@link #createSortedLeaves} are iterated in reverse so that the most
   * competitive bracket overwrites last (MIN → lowest bracket last; MAX → highest bracket last).
   */
  @Override
  public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
    BsonValue[] values = new BsonValue[maxDoc];
    Arrays.fill(values, this.missingValue);

    MixedLeafFieldComparator[] leaves = createSortedLeaves(reader);
    for (int i = leaves.length - 1; i >= 0; i--) {
      MixedLeafFieldComparator leaf = leaves[i];
      for (int doc = leaf.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = leaf.nextDoc()) {
        values[doc] = leaf.getCurrentValue();
      }
    }

    return (docID1, docID2) -> {
      int cmp = SortUtil.mqlMixedCompare(values[docID1], values[docID2], this.nullPos);
      return this.reverse ? -cmp : cmp;
    };
  }

  /**
   * Cursor over a single segment's documents in docID order. Wraps sorted leaf comparators and
   * exposes a single {@link #currentValue} per document, resolved by bracket priority (first leaf
   * with a value wins).
   */
  private static class SegmentCursor {
    final int segmentIndex;
    private final MixedLeafFieldComparator[] leaves;
    private final BsonValue missingValue;
    private final int maxDoc;
    int currentDoc;
    BsonValue currentValue;

    SegmentCursor(
        int segmentIndex, MixedLeafFieldComparator[] leaves, int maxDoc, BsonValue missingValue)
        throws IOException {
      this.segmentIndex = segmentIndex;
      this.leaves = leaves;
      this.missingValue = missingValue;
      this.maxDoc = maxDoc;
      this.currentDoc = 0;
      this.currentValue = readValue(0);
    }

    boolean advance() throws IOException {
      if (++this.currentDoc >= this.maxDoc) {
        return false;
      }
      this.currentValue = readValue(this.currentDoc);
      return true;
    }

    private BsonValue readValue(int doc) throws IOException {
      for (MixedLeafFieldComparator leaf : this.leaves) {
        if (leaf.hasValue(doc)) {
          return leaf.getCurrentValue();
        }
      }
      return this.missingValue;
    }
  }

  /**
   * Creates leaf comparators for a segment, sorted by bracket priority so the first matching leaf
   * yields the most competitive value (MIN → ascending brackets first; MAX → descending brackets
   * first). Reuses the bracket-ordering logic from {@link CompositeComparator#create}.
   */
  private MixedLeafFieldComparator[] createSortedLeaves(LeafReader reader) throws IOException {
    MixedFieldComparator[] comparators = this.parent.createMixedFieldComparators();

    int selectMultiplier = this.selector == SortSelector.MIN ? 1 : -1;
    Arrays.sort(
        comparators,
        Comparator.comparingInt(
            c -> selectMultiplier * SortUtil.getBracketPriority(c.getBsonType(), this.nullPos)));

    CompositeComparator composite = CompositeComparator.create(comparators, this.userOpts, 1);

    List<MixedLeafFieldComparator> leaves = new ArrayList<>(comparators.length);
    for (MixedFieldComparator c : comparators) {
      c.getLeaf(composite, reader, SortUtil.getBracketPriority(c.getBsonType(), this.nullPos))
          .ifPresent(leaves::add);
    }
    return leaves.toArray(MixedLeafFieldComparator[]::new);
  }

  /**
   * Builds per-segment rank arrays via K-way merge over already-sorted segments. Each segment's
   * documents are in sort order by docID, so iterating docIDs 0..maxDoc-1 yields values in sort
   * order. A {@link PriorityQueue} of size K merges these K sorted streams in O(N log K) time. Uses
   * {@link PriorityQueue#updateTop()} (one sift-down per doc) instead of poll+add. Ranks use {@link
   * PackedLongValues} for memory efficiency.
   */
  @Override
  public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
      throws IOException {
    PackedLongValues.Builder[] builders = new PackedLongValues.Builder[readers.size()];
    for (int i = 0; i < readers.size(); i++) {
      // MONOTONIC encoding exploits the fact that ranks are assigned in non-decreasing order
      // within each segment, so deltas between consecutive values are small and compress well.
      // FAST mode trades slightly more memory for faster random access during the merge.
      builders[i] = PackedLongValues.monotonicBuilder(PackedInts.FAST);
    }

    PriorityQueue<SegmentCursor> pq =
        new PriorityQueue<>(Math.max(1, readers.size())) {
          @Override
          protected boolean lessThan(SegmentCursor a, SegmentCursor b) {
            int cmp =
                SortUtil.mqlMixedCompare(
                    a.currentValue, b.currentValue, MqlMixedIndexSorter.this.nullPos);
            return MqlMixedIndexSorter.this.reverse ? cmp > 0 : cmp < 0;
          }
        };

    for (int i = 0; i < readers.size(); i++) {
      LeafReader reader = readers.get(i);
      if (reader.maxDoc() > 0) {
        pq.add(
            new SegmentCursor(i, createSortedLeaves(reader), reader.maxDoc(), this.missingValue));
      }
    }

    @Var long currentRank = 0;
    while (pq.size() > 0) {
      SegmentCursor cursor = pq.top();
      builders[cursor.segmentIndex].add(currentRank++);
      if (cursor.advance()) {
        pq.updateTop();
      } else {
        pq.pop();
      }
    }

    ComparableProvider[] providers = new ComparableProvider[readers.size()];
    for (int i = 0; i < readers.size(); i++) {
      PackedLongValues segRanks = builders[i].build();
      providers[i] = docID -> segRanks.get(docID);
    }
    return providers;
  }

  /**
   * Required by SPI but unused — MqlMixedSort is never persisted via Lucene's SortFieldProvider
   * mechanism. Returns "SortField" so Lucene's built-in provider can handle serialization across
   * segment flushes.
   */
  @Override
  public String getProviderName() {
    return "SortField";
  }
}
