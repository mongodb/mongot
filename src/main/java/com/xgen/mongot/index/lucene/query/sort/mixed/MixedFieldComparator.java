package com.xgen.mongot.index.lucene.query.sort.mixed;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.query.sort.comparator.BlockJoinReducedNumericDocValues;
import com.xgen.mongot.index.lucene.query.sort.comparator.BlockJoinReducedSortedDocValues;
import com.xgen.mongot.index.lucene.query.sort.comparator.ScopeFilters;
import com.xgen.mongot.index.query.sort.SortSelector;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
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

  /**
   * When present (embeddedDoc-scoped multi-type sort), each type reads a block-join-reduced view of
   * its doc-values (MIN/MAX over the parent's matching children). The composite comparator then
   * resolves cross-type ordering by bracket over those per-type reduced values.
   */
  private final Optional<ScopeFilters> scope;

  public MixedFieldComparator(
      TypeField typeField, BsonType bsonType, FieldPath path, Optional<FieldPath> embeddedRoot) {
    this(typeField, bsonType, path, embeddedRoot, Optional.empty());
  }

  public MixedFieldComparator(
      TypeField typeField,
      BsonType bsonType,
      FieldPath path,
      Optional<FieldPath> embeddedRoot,
      Optional<ScopeFilters> scope) {
    this.typeField = typeField;
    this.bsonType = bsonType;
    this.path = typeField.getLuceneFieldName(path, embeddedRoot);
    this.scope = scope;
  }

  /** Reduces numeric doc-values over a parent's matching children when scoped; else identity. */
  private NumericDocValues maybeReduce(
      NumericDocValues dv, LeafReader reader, SortSelector selector) throws IOException {
    if (this.scope.isEmpty()) {
      return dv;
    }
    ScopeFilters scopeFilters = this.scope.get();
    LeafReaderContext ctx = reader.getContext();
    return new BlockJoinReducedNumericDocValues(
        dv,
        scopeFilters.parents().getBitSet(ctx),
        scopeFilters.children().getBitSet(ctx),
        selector == SortSelector.MAX);
  }

  /** Reduces sorted (ordinal) doc-values over a parent's matching children when scoped. */
  private SortedDocValues maybeReduce(SortedDocValues dv, LeafReader reader, SortSelector selector)
      throws IOException {
    if (this.scope.isEmpty()) {
      return dv;
    }
    ScopeFilters scopeFilters = this.scope.get();
    LeafReaderContext ctx = reader.getContext();
    return new BlockJoinReducedSortedDocValues(
        dv,
        scopeFilters.parents().getBitSet(ctx),
        scopeFilters.children().getBitSet(ctx),
        selector == SortSelector.MAX);
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
          createNumericLeaf(parent, reader, selector, MixedDoubleLeafComparator::new);
      case NUMBER_INT64_V2 ->
          createNumericLeaf(parent, reader, selector, MixedLongLeafComparator::new);
      case DATE_V2 -> createNumericLeaf(parent, reader, selector, MixedDateLeafComparator::new);
      case UUID -> createOrdComparatorForUuidFieldType(parent, reader, selector, bracketPriority);
      case NULL ->
          createOrdComparatorForNullFieldType(
              reader,
              selector,
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
    SortedDocValues dv =
        maybeReduce(SortedSetSelector.wrap(setDv, selector.sortedSetSelector), reader, selector);
    return Optional.of(new MixedBooleanLeafComparator(parent, dv));
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
    SortedDocValues dv =
        maybeReduce(SortedSetSelector.wrap(setDv, selector.sortedSetSelector), reader, selector);
    return Optional.of(new MixedOrdinalLeafComparator(parent, dv, bracketPriority, converter));
  }

  private Optional<MixedLeafFieldComparator> createOrdComparatorForNullFieldType(
      LeafReader reader, SortSelector selector, int bracketPriority, BsonValue nullMissingSortValue)
      throws IOException {
    @Nullable SortedDocValues dv = reader.getSortedDocValues(this.path);
    if (dv == null) {
      return Optional.empty();
    }
    return Optional.of(
        new MixedNullLeafComparator(
            maybeReduce(dv, reader, selector), bracketPriority, nullMissingSortValue));
  }

  private Optional<MixedLeafFieldComparator> createNumericLeaf(
      CompositeComparator parent,
      LeafReader reader,
      SortSelector selector,
      BiFunction<CompositeComparator, NumericDocValues, MixedLeafFieldComparator> factory)
      throws IOException {
    @Nullable SortedNumericDocValues setDv = reader.getSortedNumericDocValues(this.path);
    if (setDv == null) {
      return Optional.empty();
    }
    NumericDocValues dv = SortedNumericSelector.wrap(setDv, selector.numericSelector, Type.LONG);
    return Optional.of(factory.apply(parent, maybeReduce(dv, reader, selector)));
  }
}
