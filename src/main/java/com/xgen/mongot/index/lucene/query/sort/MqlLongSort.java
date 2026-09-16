package com.xgen.mongot.index.lucene.query.sort;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.sort.comparator.BsonConverter;
import com.xgen.mongot.index.lucene.query.sort.comparator.FieldComparatorBsonWrapper;
import com.xgen.mongot.index.lucene.query.sort.comparator.MqlLongComparator;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.bson.BsonInt64;
import org.bson.BsonValue;

public class MqlLongSort extends MqlSortedNumericSortField {
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

  private final boolean enablePruning;
  private final NullEmptySortPosition nullEmptySortPosition;

  MqlLongSort(
      FieldName.TypeField fieldType,
      MongotSortField sortField,
      boolean enablePruning,
      Optional<FieldPath> embeddedRoot,
      boolean indexSorted,
      boolean isSortableNumberBetaV1) {
    super(
        fieldType.getLuceneFieldName(sortField.field(), embeddedRoot),
        Type.LONG,
        sortField.options().isReverse(),
        sortField.options().selector().numericSelector,
        indexSorted,
        isSortableNumberBetaV1);
    this.enablePruning = enablePruning;
    this.nullEmptySortPosition =
        ((UserFieldSortOptions) sortField.options()).nullEmptySortPosition();
  }

  @Override
  protected final FieldComparator<?> createQueryTimeFieldComparator(
      int numHits, Pruning lucenePruning) {
    // Consider both mongot & lucene pruning configuration
    Pruning configurePruning = this.enablePruning ? lucenePruning : Pruning.NONE;

    return new FieldComparatorBsonWrapper<>(
        new MqlLongComparator(
            numHits,
            getField(),
            getReverse(),
            configurePruning,
            getNumericType(),
            getSelector(),
            this.nullEmptySortPosition),
        LONG_CONVERTER,
        this.nullEmptySortPosition.getNullMissingSortValue());
  }
}
