package com.xgen.mongot.index.lucene.query.sort.comparator;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.jetbrains.annotations.Nullable;

/**
 * A per-parent {@link NumericDocValues} view that reduces a scoped sort key over only the matching
 * children of an {@code embeddedDocuments} block. On {@link #advanceExact(int)} for a parent doc it
 * walks that parent's matching children (those in {@code children}, between the previous parent and
 * this one) and reduces their per-child values -- MIN for an ascending sort, MAX for a descending
 * one -- to a single value for the parent.
 *
 * <p>This is the injection point for embeddedDoc-scoped sort: mongot's numeric comparators read
 * their doc-values through the overridable {@code getNumericDocValues} seam, so wrapping the
 * selector-reduced per-child values in this reduction gives the comparator a per-parent key while
 * every other aspect of the sort (null placement via {@code NullEmptySortPosition}, BSON {@code
 * $searchSortValues}) is reused unchanged.
 *
 * <p>Only {@link #advanceExact(int)} and {@link #longValue()} are supported. Scoped sort forces
 * pruning off (see {@code MongotNumericComparator.NumericLeafComparator#getNumericDocValues}), so
 * the comparator never drives this as a competitive iterator via {@link #nextDoc()}/{@link
 * #advance(int)}. Parents are visited in non-decreasing doc order during collection. Repeated reads
 * of the same parent reuse its cached reduction, keeping the forward-only {@code childValues}
 * iterator monotonic when Lucene compares and then copies a hit.
 */
public final class BlockJoinReducedNumericDocValues extends NumericDocValues {

  private final @Nullable NumericDocValues childValues;
  private final @Nullable BitSet parents;
  private final @Nullable BitSet children;
  private final boolean max;

  private int docID = -1;
  private boolean hasValue;
  private long value;

  /**
   * Creates a per-parent reduction over the given segment's children.
   *
   * @param childValues the selector-reduced (one value per child doc) values of the sort field, or
   *     {@code null} if the field has no values in this segment
   * @param parents per-segment bitset marking root (parent) docs; a block is {@code [child..,
   *     root]}
   * @param children per-segment bitset marking the matching children (the scope operator's hits)
   * @param max {@code true} to reduce with MAX (descending sort), {@code false} for MIN (ascending)
   */
  public BlockJoinReducedNumericDocValues(
      @Nullable NumericDocValues childValues,
      @Nullable BitSet parents,
      @Nullable BitSet children,
      boolean max) {
    this.childValues = childValues;
    this.parents = parents;
    this.children = children;
    this.max = max;
  }

  @Override
  public boolean advanceExact(int parentDoc) throws IOException {
    if (parentDoc == this.docID) {
      return this.hasValue;
    }
    this.docID = parentDoc;
    this.hasValue = false;
    if (this.childValues == null || this.parents == null || this.children == null) {
      return false;
    }

    // Children of parentDoc are the docs after the previous parent and before this one.
    int firstChild = parentDoc == 0 ? 0 : this.parents.prevSetBit(parentDoc - 1) + 1;
    if (firstChild >= parentDoc) {
      return false;
    }

    @Var boolean found = false;
    @Var long reduced = 0L;
    for (int child = this.children.nextSetBit(firstChild);
        child != DocIdSetIterator.NO_MORE_DOCS && child < parentDoc;
        child = this.children.nextSetBit(child + 1)) {
      if (!this.childValues.advanceExact(child)) {
        continue;
      }
      long v = this.childValues.longValue();
      reduced = !found ? v : this.max ? Math.max(reduced, v) : Math.min(reduced, v);
      found = true;
    }

    if (!found) {
      return false;
    }
    this.value = reduced;
    this.hasValue = true;
    return true;
  }

  @Override
  public long longValue() {
    return this.value;
  }

  @Override
  public int docID() {
    return this.docID;
  }

  @Override
  public int nextDoc() {
    throw new UnsupportedOperationException("advanceExact only");
  }

  @Override
  public int advance(int target) {
    throw new UnsupportedOperationException("advanceExact only");
  }

  @Override
  public long cost() {
    return 0L;
  }
}
