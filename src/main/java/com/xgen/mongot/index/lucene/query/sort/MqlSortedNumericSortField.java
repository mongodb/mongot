package com.xgen.mongot.index.lucene.query.sort;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
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

  /**
   * Set when index sort is defined in the index definition.
   *
   * <p>Query sort uses index sort when query sort soec prefix-equals index sort spec through java
   * equals function. We relax the base SortedNumericSortField equality check for index sort fields
   * to survive the lucene serialization boundary.
   */
  private final boolean indexSorted;

  /**
   * True if this SortField is being configured for IndexWriterConfig's index sort. And False for
   * query sort path.
   *
   * <p>Distinct from {@code indexSorted}: that flag means the index has some index sort defined and
   * is set on both index-sort and query-time sort objects to relax equals check. This flag instead
   * is used by IndexSortSortedNumericDocValuesRangeQuery, which calls {@code getComparator()} and
   * expects a plain FieldComparator<Number> instead of the Bson-aware version.
   */
  protected final boolean isIndexWriterConfigSortField;

  MqlSortedNumericSortField(
      String field,
      Type type,
      boolean reverse,
      SortedNumericSelector.Type selector,
      boolean indexSorted,
      boolean isIndexWriterConfigSortField) {
    super(field, type, reverse, selector);
    this.indexSorted = indexSorted;
    this.isIndexWriterConfigSortField = isIndexWriterConfigSortField;
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

  /**
   * If this SortField lives in IndexWriterConfig's index sort, return the Lucene's FieldComparator.
   * Numeric / date range query with sorted index requires a comparator that works with long[]
   * values.
   */
  public final FieldComparator<?> getComparator(int numHits, Pruning pruning) {
    if (this.isIndexWriterConfigSortField) {
      return super.getComparator(numHits, pruning);
    }
    return createQueryTimeFieldComparator(numHits, pruning);
  }

  /**
   * Builds the FieldComparator used when this SortField is not the one attached to
   * IndexWriterConfig's index sort - i.e. for real $sort query execution
   * isIndexWriterConfigSortField is false. Subclasses implement this to return their BSON-aware
   * comparator (e.g. wrapping the raw numeric comparator returned by super.getComparator() in
   * FieldComparatorBsonWrapper), matching what $sort needs to produce BsonValue results.
   *
   * <p>Never called when isIndexWriterConfigSortField is true.
   */
  protected abstract FieldComparator<?> createQueryTimeFieldComparator(
      int numHits, Pruning pruning);

  /**
   * Returns the plain, non-BSON-aware comparator SortedNumericSortField would have returned.
   * Subclasses must call this - not super.getComparator() - when they need the raw Lucene
   * comparator.
   */
  protected final FieldComparator<?> createPlainComparator(int numHits, Pruning pruning) {
    return super.getComparator(numHits, pruning);
  }
}
