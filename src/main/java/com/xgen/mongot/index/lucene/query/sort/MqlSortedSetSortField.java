package com.xgen.mongot.index.lucene.query.sort;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.query.sort.comparator.BsonConverter;
import com.xgen.mongot.index.lucene.query.sort.comparator.FieldComparatorBsonWrapper;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOptions;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.comparators.TermOrdValComparator;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBoolean;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jetbrains.annotations.NotNull;

/**
 * MQL sort field backed by {@link SortedSetSortField} for string, UUID, objectId, and boolean
 * sorts. Like {@link MqlSortedNumericSortField}, provides a relaxed {@code equals()} override when
 * created in a sorted-index context to survive Lucene's serialization boundary.
 */
public class MqlSortedSetSortField extends SortedSetSortField {
  private static final BytesRef TRUE_BYTES = new BytesRef(FieldValue.BOOLEAN_TRUE_FIELD_VALUE);

  private final BsonConverter<BytesRef> converter;
  private final boolean enablePruning;
  private final BsonValue missingValueSortValue;

  /** Set when index sort is defined in the index definition. */
  private final boolean indexSorted;

  private MqlSortedSetSortField(
      String fieldName,
      SortOptions options,
      BsonConverter<BytesRef> converter,
      boolean enablePruning,
      boolean indexSorted) {
    super(fieldName, options.isReverse(), options.selector().sortedSetSelector);

    setMissingValue(
        ((UserFieldSortOptions) options).nullEmptySortPosition() == NullEmptySortPosition.HIGHEST
            ? STRING_LAST
            : STRING_FIRST);

    this.missingValueSortValue =
        ((UserFieldSortOptions) options).nullEmptySortPosition().getNullMissingSortValue();

    this.converter = converter;
    this.enablePruning = enablePruning;
    this.indexSorted = indexSorted;
  }

  /**
   * Reproduces the full {@code SortField.equals()} and {@code SortedSetSortField.equals()} logic
   * but replaces the strict {@code getClass()} check with {@code instanceof}. Java does not allow
   * {@code super.super.equals()}, so both levels are inlined here.
   */
  @Override
  public boolean equals(Object obj) {
    if (!this.indexSorted) {
      return super.equals(obj);
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SortedSetSortField other)) {
      return false;
    }

    // SortField-level checks (mirrors SortField.equals)
    if (!IndexSortUtils.sortFieldBaseEquals(this, other)) {
      return false;
    }

    // SortedSetSortField-level checks (original minus getClass())
    if (getSelector() != other.getSelector()) {
      return false;
    }
    return true;
  }

  @Override
  public FieldComparator<?> getComparator(int numHits, Pruning lucenePruning) {
    // Consider both mongot & lucene pruning configuration
    Pruning configurePruning = this.enablePruning ? lucenePruning : Pruning.NONE;

    TermOrdValComparator termOrdValComparator =
        new TermOrdValComparator(
            numHits,
            getField(),
            this.missingValue == STRING_LAST,
            super.getReverse(),
            configurePruning) {

          @Override
          protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field)
              throws IOException {
            return SortedSetSelector.wrap(
                DocValues.getSortedSet(context.reader(), field),
                MqlSortedSetSortField.super.getSelector());
          }
        };

    return new FieldComparatorBsonWrapper<>(
        termOrdValComparator, this.converter, MqlSortedSetSortField.this.missingValueSortValue);
  }

  public static MqlSortedSetSortField booleanSort(
      MongotSortField sortField, Optional<FieldPath> embeddedRoot, boolean indexSorted) {
    var converter =
        new BsonConverter<BytesRef>(null) {
          @Override
          public BytesRef decodeNonMissingValue(BsonValue value) {
            return new BytesRef(FieldValue.fromBoolean(value.asBoolean().getValue()));
          }

          @Override
          public BsonValue encodeNonMissingValue(BytesRef value) {
            return BsonBoolean.valueOf(value.bytesEquals(TRUE_BYTES));
          }
        };

    var fieldName = resolveFieldName(FieldName.TypeField.BOOLEAN, sortField.field(), embeddedRoot);
    return new MqlSortedSetSortField(fieldName, sortField.options(), converter, true, indexSorted);
  }

  public static MqlSortedSetSortField uuidSort(
      MongotSortField sortField, Optional<FieldPath> embeddedRoot, boolean indexSorted) {
    var converter =
        new BsonConverter<BytesRef>(null) {
          @Override
          public BytesRef decodeNonMissingValue(BsonValue value) {
            return new BytesRef(value.asBinary().asUuid().toString());
          }

          @Override
          public BsonValue encodeNonMissingValue(BytesRef value) {
            return BsonUtils.UUID_CONVERTER.apply(value);
          }
        };

    var fieldName = resolveFieldName(FieldName.TypeField.UUID, sortField.field(), embeddedRoot);
    return new MqlSortedSetSortField(fieldName, sortField.options(), converter, true, indexSorted);
  }

  public static MqlSortedSetSortField objectIdSort(
      MongotSortField sortField, Optional<FieldPath> embeddedRoot, boolean indexSorted) {
    var converter =
        new BsonConverter<BytesRef>(null) {
          @Override
          public BytesRef decodeNonMissingValue(BsonValue value) {
            return new BytesRef(value.asObjectId().getValue().toByteArray());
          }

          @Override
          public BsonValue encodeNonMissingValue(BytesRef value) {
            return BsonUtils.OBJECT_ID_CONVERTER.apply(value);
          }
        };

    var fieldName =
        resolveFieldName(FieldName.TypeField.OBJECT_ID, sortField.field(), embeddedRoot);
    return new MqlSortedSetSortField(fieldName, sortField.options(), converter, true, indexSorted);
  }

  public static MqlSortedSetSortField stringSort(
      FieldName.TypeField fieldType,
      MongotSortField sortField,
      boolean enablePruning,
      Optional<FieldPath> embeddedRoot,
      boolean indexSorted) {
    var converter =
        new BsonConverter<BytesRef>(null) {
          @Override
          public BytesRef decodeNonMissingValue(BsonValue value) {
            return new BytesRef(value.asString().getValue());
          }

          @Override
          public BsonValue encodeNonMissingValue(BytesRef value) {
            return new BsonString(value.utf8ToString());
          }
        };

    var fieldName = resolveFieldName(fieldType, sortField.field(), embeddedRoot);
    return new MqlSortedSetSortField(
        fieldName, sortField.options(), converter, enablePruning, indexSorted);
  }

  private static @NotNull String resolveFieldName(
      FieldName.TypeField fieldType, FieldPath fieldPath, Optional<FieldPath> embeddedRoot) {
    return fieldType.getLuceneFieldName(fieldPath, embeddedRoot);
  }
}
