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
import org.bson.BsonDateTime;
import org.bson.BsonValue;

public class MqlDateSort extends MqlSortedNumericSortField {
  private static final BsonConverter<Long> DATE_CONVERTER =
      new BsonConverter<>(null) {
        @Override
        public Long decodeNonMissingValue(BsonValue value) {
          return value.asDateTime().getValue();
        }

        @Override
        public BsonValue encodeNonMissingValue(Long value) {
          return new BsonDateTime(value);
        }
      };

  private final boolean enablePruning;
  protected final NullEmptySortPosition nullEmptySortPosition;

  MqlDateSort(
      FieldName.TypeField fieldType,
      MongotSortField sortField,
      boolean enablePruning,
      Optional<FieldPath> embeddedRoot,
      boolean indexSorted,
      boolean isIndexWriterConfigSortField) {
    super(
        fieldType.getLuceneFieldName(sortField.field(), embeddedRoot),
        Type.LONG,
        sortField.options().isReverse(),
        sortField.options().selector().numericSelector,
        indexSorted,
        isIndexWriterConfigSortField);
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
        createComparator(numHits, configurePruning),
        DATE_CONVERTER,
        this.nullEmptySortPosition.getNullMissingSortValue());
  }

  /**
   * Builds the underlying long comparator over the encoded dates. Overridden by the
   * embeddedDoc-scoped variant to reduce over a parent's matching children.
   */
  protected MqlLongComparator createComparator(int numHits, Pruning pruning) {
    return new MqlLongComparator(
        numHits,
        getField(),
        getReverse(),
        pruning,
        getNumericType(),
        getSelector(),
        this.nullEmptySortPosition);
  }
}
