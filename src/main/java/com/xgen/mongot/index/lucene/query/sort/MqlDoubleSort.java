package com.xgen.mongot.index.lucene.query.sort;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.sort.comparator.DoubleFieldConverter;
import com.xgen.mongot.index.lucene.query.sort.comparator.FieldComparatorBsonWrapper;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.comparators.LongComparator;

/**
 * This class sorts numberV2 fields indexed with `Representation: DOUBLE` and sortableNumberBetaV1
 *
 * <p>This is a thin wrapper around Lucene's long sort because numberV2 encodes doubles as
 * LongPoints that preserve the MQL sort order. The encoding reserves Long.MIN_VALUE and
 * Long.MAX_VALUE, which we use here to encode nulls.
 */
class MqlDoubleSort extends MqlSortedNumericSortField {

  /**
   * Map nulls to either MIN_VALUE or MAX_VALUE depending on `position`.
   *
   * <p>Unlike Longs and Dates, missing doubles can be represented in the primitive long space
   * because neither `Long.MIN_VALUE` and `Long.MAX_VALUE` are valid `toMqlSortableLong` encoding as
   * double: Double.NaN is encoded as {@code MIN_VALUE + 1} and {@code Double.POSITIVE_INFINITY <
   * MAX_VALUE}
   */
  private static long getLuceneMissingValue(NullEmptySortPosition position) {
    return switch (position) {
      case LOWEST -> Long.MIN_VALUE;
      case HIGHEST -> Long.MAX_VALUE;
    };
  }

  private final boolean enablePruning;
  protected final NullEmptySortPosition nullEmptySortPosition;

  MqlDoubleSort(
      FieldName.TypeField fieldType,
      MongotSortField sortField,
      boolean enablePruning,
      Optional<FieldPath> embeddedRoot,
      boolean indexSorted,
      boolean isSortableNumberBetaV1) {
    super(
        fieldType.getLuceneFieldName(sortField.field(), embeddedRoot),
        Type.LONG, // Use LONG because we index doubles with custom encoding + LongPoint
        sortField.options().isReverse(),
        sortField.options().selector().numericSelector,
        indexSorted,
        isSortableNumberBetaV1);
    this.enablePruning = enablePruning;
    this.nullEmptySortPosition =
        ((UserFieldSortOptions) sortField.options()).nullEmptySortPosition();
    super.setMissingValue(getLuceneMissingValue(this.nullEmptySortPosition));
  }

  @Override
  protected final FieldComparator<?> createQueryTimeFieldComparator(
      int numHits, Pruning lucenePruning) {
    // Consider both mongot & lucene pruning configuration
    Pruning configurePruning = this.enablePruning ? lucenePruning : Pruning.NONE;

    // Wrap the inner (encoded-long) comparator with an encoded Long -> BsonDouble converter
    return new FieldComparatorBsonWrapper<>(
        createInnerComparator(numHits, configurePruning),
        createConverter(),
        this.nullEmptySortPosition.getNullMissingSortValue());
  }

  /** Matches the inner comparator's representation of missing values. */
  protected DoubleFieldConverter createConverter() {
    return new DoubleFieldConverter((Long) super.missingValue);
  }

  /**
   * Builds the inner comparator over the encoded-long doubles. Overridden by the embeddedDoc-scoped
   * variant to reduce over a parent's matching children instead of reading the parent's own value.
   * Uses {@code createPlainComparator} (not {@code super.getComparator}, which would recurse back
   * through {@code MqlSortedNumericSortField.getComparator}) for the raw Lucene comparator.
   */
  protected FieldComparator<Long> createInnerComparator(int numHits, Pruning pruning) {
    return (LongComparator) createPlainComparator(numHits, pruning);
  }
}
