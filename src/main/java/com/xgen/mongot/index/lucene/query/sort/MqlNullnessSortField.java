package com.xgen.mongot.index.lucene.query.sort;

import com.xgen.mongot.index.lucene.query.sort.comparator.BsonConverter;
import com.xgen.mongot.index.lucene.query.sort.comparator.FieldComparatorBsonWrapper;
import com.xgen.mongot.index.lucene.query.sort.comparator.MqlLongComparator;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.bson.BsonInt64;
import org.bson.BsonValue;

/**
 * A {@link MqlSortedNumericSortField} for {@code $meta/nullness} fields that wraps its comparator
 * in {@link FieldComparatorBsonWrapper}, so {@code FieldDoc.fields[]} produces {@link BsonInt64}
 * (or {@code BsonMinKey}/{@code BsonMaxKey} for missing values) instead of raw {@code Long}.
 *
 * <p>This keeps nullness sort fields consistent with other MQL sort types (e.g. {@link
 * MqlLongSort}, {@link MqlDateSort}), eliminating the need for post-hoc Long-to-BsonValue coercion
 * in {@code SequenceToken} and {@code AbstractLuceneSearchManager}.
 *
 * <p>Nullness fields are only created in a sorted-index context, so the relaxed {@code equals()}
 * from {@link MqlSortedNumericSortField} is always active ({@code indexSorted = true}).
 */
public class MqlNullnessSortField extends MqlSortedNumericSortField {

  private static final BsonConverter<Long> LONG_CONVERTER =
      new BsonConverter<>(null) {
        @Override
        public Long decodeNonMissingValue(BsonValue value) {
          return value.asInt64().longValue();
        }

        @Override
        public BsonValue encodeNonMissingValue(Long value) {
          return new BsonInt64(value);
        }
      };

  private final NullEmptySortPosition nullEmptySortPosition;

  MqlNullnessSortField(MongotSortField mongotSortField) {
    this(mongotSortField, checkedOptions(mongotSortField));
  }

  private MqlNullnessSortField(MongotSortField mongotSortField, UserFieldSortOptions options) {
    super(
        // Callers pre-wrap the field path with FieldName.getNullnessFieldName(), so
        // field().toString() is already the full "$meta/nullness/<path>" Lucene field name.
        // This is unique within the index because $meta/ is a reserved namespace.
        mongotSortField.field().toString(),
        Type.LONG,
        mongotSortField.options().isReverse(),
        options.selector().numericSelector,
        true,
        // isIndexWriterConfigSortField is always false: nothing ever calls getComparator() on the
        // index sort's own nullness field. IndexSortSortedNumericDocValuesRangeQuery only calls
        // getComparator() on the primary sort field after its field name matches the query field,
        // and a $meta/nullness/ field name can never match a user query field.
        false);
    this.nullEmptySortPosition = options.nullEmptySortPosition();
    // The $nullness field has value 0 for documents that have the sort field value. Documents
    // missing the sort field also miss the $nullness field, so Lucene uses this configured
    // missing value to place them at the correct end of the sort order.
    setMissingValue(this.nullEmptySortPosition.getNullnessMissingValue());
  }

  private static UserFieldSortOptions checkedOptions(MongotSortField mongotSortField) {
    if (!(mongotSortField.options() instanceof UserFieldSortOptions userOpts)) {
      throw new IllegalArgumentException(
          "MqlNullnessSortField requires UserFieldSortOptions, got: "
              + mongotSortField.options().getClass().getSimpleName());
    }
    return userOpts;
  }

  @Override
  protected final FieldComparator<?> createQueryTimeFieldComparator(
      int numHits, Pruning lucenePruning) {
    return new FieldComparatorBsonWrapper<>(
        new MqlLongComparator(
            numHits,
            getField(),
            getReverse(),
            Pruning.NONE,
            getNumericType(),
            getSelector(),
            this.nullEmptySortPosition),
        LONG_CONVERTER,
        this.nullEmptySortPosition.getNullMissingSortValue());
  }
}
