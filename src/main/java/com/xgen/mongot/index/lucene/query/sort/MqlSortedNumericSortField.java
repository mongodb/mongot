package com.xgen.mongot.index.lucene.query.sort;

import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;

/**
 * Base class for MQL sort fields backed by {@link SortedNumericSortField}. Provides a relaxed
 * {@link #equals(Object)} override that uses {@code instanceof} instead of {@code getClass()} when
 * the field was created in a sorted-index context ({@code indexSorted = true}).
 *
 * <p>Lucene deserializes custom {@code SortedNumericSortField} subclasses as the base class after a
 * segment round-trip. The default {@code SortedNumericSortField.equals()} uses a strict {@code
 * getClass()} check, which breaks {@code TopFieldCollector.canEarlyTerminateOnPrefix()}, {@code
 * IndexWriter.validateIndexSort()}, and our own {@code IndexSortUtils.usesIndexSort()}.
 */
abstract class MqlSortedNumericSortField extends SortedNumericSortField {

  /** Set when index sort is defined in the index definition. */
  private final boolean indexSorted;

  MqlSortedNumericSortField(
      String field,
      Type type,
      boolean reverse,
      SortedNumericSelector.Type selector,
      boolean indexSorted) {
    super(field, type, reverse, selector);
    this.indexSorted = indexSorted;
  }

  /**
   * Reproduces the full {@code SortField.equals()} and {@code SortedNumericSortField.equals()}
   * logic but replaces the strict {@code getClass()} check with {@code instanceof}. Java does not
   * allow {@code super.super.equals()}, so both levels are inlined here.
   */
  @Override
  public boolean equals(Object obj) {
    if (!this.indexSorted) {
      return super.equals(obj);
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SortedNumericSortField other)) {
      return false;
    }

    // SortField-level checks (mirrors SortField.equals)
    if (!IndexSortUtils.sortFieldBaseEquals(this, other)) {
      return false;
    }

    // SortedNumericSortField-level checks (original minus getClass())
    if (getSelector() != other.getSelector()) {
      return false;
    }
    if (getNumericType() != other.getNumericType()) {
      return false;
    }
    return true;
  }
}
