package com.xgen.mongot.index.lucene.query.sort.comparator;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;

/**
 * A per-parent {@link SortedDocValues} view that reduces a scoped token/string sort key over only
 * the matching children of an {@code embeddedDocuments} block. On {@link #advanceExact(int)} for a
 * parent doc it walks that parent's matching children and reduces their (selector-reduced) ordinals
 * -- MIN for an ascending sort, MAX for a descending one. Because ordinals are assigned in term
 * order, the MIN ordinal is the lexicographically smallest term (symmetric for MAX), so reducing on
 * ordinals is equivalent to reducing on terms. Ordinal resolution ({@link #lookupOrd}) and the
 * value count delegate to the underlying per-child values, whose ordinal space is shared.
 *
 * <p>Only {@link #advanceExact(int)} + the ordinal accessors are supported; scoped sort forces
 * pruning off so the comparator never drives this as a competitive iterator. Parents are visited in
 * non-decreasing doc order. Repeated reads reuse the cached ordinal or missing result so comparison
 * followed by copying a hit never rewinds the forward-only {@code childValues} iterator.
 */
public final class BlockJoinReducedSortedDocValues extends SortedDocValues {

  private final @Nullable SortedDocValues childValues;
  private final @Nullable BitSet parents;
  private final @Nullable BitSet children;
  private final boolean max;

  private int docID = -1;
  private int ord = -1;

  public BlockJoinReducedSortedDocValues(
      @Nullable SortedDocValues childValues,
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
      return this.ord >= 0;
    }
    this.docID = parentDoc;
    this.ord = -1;
    if (this.childValues == null || this.parents == null || this.children == null) {
      return false;
    }

    int firstChild = parentDoc == 0 ? 0 : this.parents.prevSetBit(parentDoc - 1) + 1;
    if (firstChild >= parentDoc) {
      return false;
    }

    @Var boolean found = false;
    @Var int reduced = -1;
    for (int child = this.children.nextSetBit(firstChild);
        child != DocIdSetIterator.NO_MORE_DOCS && child < parentDoc;
        child = this.children.nextSetBit(child + 1)) {
      if (!this.childValues.advanceExact(child)) {
        continue;
      }
      int o = this.childValues.ordValue();
      reduced = !found ? o : this.max ? Math.max(reduced, o) : Math.min(reduced, o);
      found = true;
    }

    if (!found) {
      return false;
    }
    this.ord = reduced;
    return true;
  }

  @Override
  public int ordValue() {
    return this.ord;
  }

  @Override
  public BytesRef lookupOrd(int ord) throws IOException {
    // Only reachable after a successful advanceExact, which requires non-null child values.
    return Objects.requireNonNull(this.childValues).lookupOrd(ord);
  }

  @Override
  public int getValueCount() {
    return this.childValues == null ? 0 : this.childValues.getValueCount();
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
