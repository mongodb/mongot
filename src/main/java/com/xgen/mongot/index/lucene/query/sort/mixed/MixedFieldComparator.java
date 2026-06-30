package com.xgen.mongot.index.lucene.query.sort.mixed;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.sort.SortSelector;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;

public class MixedFieldComparator {

  private final TypeField typeField;
  private final BsonType bsonType;
  private final String path;

  public MixedFieldComparator(
      TypeField typeField, BsonType bsonType, FieldPath path, Optional<FieldPath> embeddedRoot) {
    this.typeField = typeField;
    this.bsonType = bsonType;
    this.path = typeField.getLuceneFieldName(path, embeddedRoot);
  }

  public BsonType getBsonType() {
    return this.bsonType;
  }

  Optional<MixedLeafFieldComparator> getLeaf(
      CompositeComparator parent, LeafReader reader, int bracketPriority) throws IOException {
    SortSelector selector = parent.options.selector();
    return switch (this.typeField) {
      case TOKEN -> createOrdComparatorForTokenFieldType(parent, reader, selector, bracketPriority);
      case NUMBER_DOUBLE_V2 ->
          Optional.ofNullable(reader.getSortedNumericDocValues(this.path))
              .map(
                  setDv -> {
                    NumericDocValues dv =
                        SortedNumericSelector.wrap(setDv, selector.numericSelector, Type.LONG);
                    return new MixedDoubleLeafComparator(parent, dv);
                  });
      case NUMBER_INT64_V2 ->
          Optional.ofNullable(reader.getSortedNumericDocValues(this.path))
              .map(
                  setDv -> {
                    NumericDocValues dv =
                        SortedNumericSelector.wrap(setDv, selector.numericSelector, Type.LONG);
                    return new MixedLongLeafComparator(parent, dv);
                  });
      case DATE_V2 ->
          Optional.ofNullable(reader.getSortedNumericDocValues(this.path))
              .map(
                  setDv -> {
                    NumericDocValues dv =
                        SortedNumericSelector.wrap(setDv, selector.numericSelector, Type.LONG);
                    return new MixedDateLeafComparator(parent, dv);
                  });
      case UUID -> createOrdComparatorForUuidFieldType(parent, reader, selector, bracketPriority);
      case NULL ->
          createOrdComparatorForNullFieldType(
              reader,
              bracketPriority,
              parent.options.nullEmptySortPosition().getNullMissingSortValue());
      case OBJECT_ID ->
          createOrdComparatorForObjectIdFieldType(parent, reader, selector, bracketPriority);
      case BOOLEAN -> createOrdComparatorForBooleanFieldType(parent, reader, selector);
      case SORTABLE_DATE_BETA_V1,
          SORTABLE_NUMBER_BETA_V1,
          SORTABLE_STRING_BETA_V1,
          NUMBER_DOUBLE,
          NUMBER_INT64,
          NUMBER_INT64_MULTIPLE,
          NUMBER_DOUBLE_MULTIPLE,
          KNN_VECTOR,
          KNN_BYTE,
          KNN_BIT,
          KNN_F32_Q7,
          KNN_F32_Q1,
          KNN_BYTE_Q1,
          GEO_SHAPE,
          GEO_POINT,
          DATE_FACET,
          AUTOCOMPLETE,
          DATE,
          DATE_MULTIPLE,
          NUMBER_DOUBLE_FACET,
          NUMBER_INT64_FACET,
          STRING ->
          throw new IllegalArgumentException("Sort Unsupported for TypeField: " + this.typeField);
    };
  }

  private Optional<MixedLeafFieldComparator> createOrdComparatorForTokenFieldType(
      CompositeComparator parent, LeafReader reader, SortSelector selector, int bracketPriority)
      throws IOException {
    return createOrdComparator(
        parent, reader, selector, BsonUtils.STRING_CONVERTER, bracketPriority);
  }

  private Optional<MixedLeafFieldComparator> createOrdComparatorForUuidFieldType(
      CompositeComparator parent, LeafReader reader, SortSelector selector, int bracketPriority)
      throws IOException {
    return createOrdComparator(parent, reader, selector, BsonUtils.UUID_CONVERTER, bracketPriority);
  }

  private Optional<MixedLeafFieldComparator> createOrdComparatorForBooleanFieldType(
      CompositeComparator parent, LeafReader reader, SortSelector selector) throws IOException {
    @Nullable SortedSetDocValues setDv = reader.getSortedSetDocValues(this.path);
    if (setDv == null) {
      return Optional.empty();
    }
    return Optional.of(
        new MixedBooleanLeafComparator(
            parent, SortedSetSelector.wrap(setDv, selector.sortedSetSelector)));
  }

  private Optional<MixedLeafFieldComparator> createOrdComparatorForObjectIdFieldType(
      CompositeComparator parent, LeafReader reader, SortSelector selector, int bracketPriority)
      throws IOException {
    return createOrdComparator(
        parent, reader, selector, BsonUtils.OBJECT_ID_CONVERTER, bracketPriority);
  }

  private Optional<MixedLeafFieldComparator> createOrdComparator(
      CompositeComparator parent,
      LeafReader reader,
      SortSelector selector,
      Function<BytesRef, BsonValue> converter,
      int bracketPriority)
      throws IOException {
    @Nullable SortedSetDocValues setDv = reader.getSortedSetDocValues(this.path);
    if (setDv == null) {
      return Optional.empty();
    }
    SortedDocValues dv = SortedSetSelector.wrap(setDv, selector.sortedSetSelector);
    return Optional.of(new MixedOrdinalLeafComparator(parent, dv, bracketPriority, converter));
  }

  private Optional<MixedLeafFieldComparator> createOrdComparatorForNullFieldType(
      LeafReader reader, int bracketPriority, BsonValue nullMissingSortValue) throws IOException {
    @Nullable SortedDocValues dv = reader.getSortedDocValues(this.path);
    if (dv == null) {
      return Optional.empty();
    }
    return Optional.of(new MixedNullLeafComparator(dv, bracketPriority, nullMissingSortValue));
  }
}
