package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongFunction;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonValue;

/**
 * Reads a token-valued metric's value for one segment from the per-type doc-values columns backing
 * a path, as an alternative to a stored-source fetch.
 *
 * <p>A path indexed as several BSON types has one column per type, so a value is assembled by
 * reading every column present in the segment. One value comes back as a scalar, several as a
 * {@link BsonArray}. Columns are opened in BSON bracket order and each is internally ascending, so
 * that array is in BSON order without being sorted.
 *
 * <p>Built once per segment, and doc values are forward-only: callers must request documents in
 * increasing segment-local doc-id order.
 *
 * <p>{@code min}/{@code max} open the same columns under {@link Selection#MIN} / {@link
 * Selection#MAX} and pick the BSON-order extremum across {@link Column#values}. {@link #read} is
 * not used there: several columns would come back as a {@link BsonArray} rather than as that pick.
 *
 * <p>A column holds the indexed form, not the source form, and the differences reach users of
 * {@code first}/{@code last}:
 *
 * <ul>
 *   <li>Numeric subtype is lost: {@code int32} reads back as {@code BsonInt64}, and an integer
 *       under a {@code double} representation as {@code BsonDouble}. Under an {@code int64}
 *       representation {@code NaN} is indexed as {@code Long.MIN_VALUE} (see {@code
 *       sortableNumericIndexedValue}) and so reads back as that number, not as {@code NaN}.
 * </ul>
 *
 * <p>Callers needing the source form verbatim have to read stored source instead.
 */
final class DocValuesFieldReader implements FieldValueFetcher.ValueSource {

  /**
   * Every column this reader can decode, in BSON bracket order -- opening them in this order is
   * what emits a document's values low to high. {@code DocValuesFieldReaderTest} checks the order
   * against {@code SortUtil}'s bracket priorities.
   */
  private static final List<TypeField> TYPE_ORDER =
      List.of(
          TypeField.NULL,
          TypeField.NUMBER_DOUBLE_V2,
          TypeField.NUMBER_INT64_V2,
          TypeField.TOKEN,
          TypeField.UUID,
          TypeField.OBJECT_ID,
          TypeField.BOOLEAN,
          TypeField.DATE_V2);

  /** Every type this reader can decode. */
  static final Set<TypeField> SUPPORTED_TYPES = Set.copyOf(TYPE_ORDER);

  /** Which of a document's values in a column are decoded. */
  enum Selection {
    /** All of them, ascending. */
    ALL,
    /** Only the lowest. */
    MIN,
    /** Only the highest. */
    MAX
  }

  private final List<Column> columns;

  private DocValuesFieldReader(List<Column> columns) {
    this.columns = columns;
  }

  /** Opens the columns for {@code types} that exist in {@code leaf}, decoding every value. */
  static DocValuesFieldReader create(FieldPath path, Set<TypeField> types, LeafReaderContext leaf)
      throws IOException {
    return create(path, types, leaf, Selection.ALL);
  }

  /**
   * Opens the columns for {@code types} that exist in {@code leaf}.
   *
   * <p>{@code types} is what the index definition allows the path to hold; a segment whose
   * documents never carried a given type has no column for it, so absent columns are skipped rather
   * than treated as an error -- mirroring {@code CompositeComparator#getLeafComparator}.
   *
   * <p>Under {@link Selection#MIN} / {@link Selection#MAX} each column is wrapped in Lucene's
   * {@code SortedNumericSelector} / {@code SortedSetSelector}, so a document's values are collapsed
   * to the requested end without the rest being decoded, and any future codec-level shortcut behind
   * those selectors lands here for free.
   */
  static DocValuesFieldReader create(
      FieldPath path, Set<TypeField> types, LeafReaderContext leaf, Selection selection)
      throws IOException {
    if (!TYPE_ORDER.containsAll(types)) {
      throw new IllegalArgumentException(
          "Token metric values cannot be read from doc values for TypeFields: "
              + types.stream().filter(type -> !TYPE_ORDER.contains(type)).toList());
    }
    return new DocValuesFieldReader(
        CheckedStream.from(TYPE_ORDER.stream().filter(types::contains))
            .flatMapAndCollectChecked(
                type ->
                    CheckedStream.from(openColumn(path, type, leaf.reader(), selection).stream())));
  }

  /**
   * The value for {@code leafDoc}: a scalar when the document held one value at the path, a {@link
   * BsonArray} in mixed-type order when it held several, or empty when it held none.
   */
  @Override
  public Optional<BsonValue> read(int leafDoc) throws IOException {
    List<BsonValue> values =
        CheckedStream.from(this.columns)
            .flatMapAndCollectChecked(column -> CheckedStream.from(column.values(leafDoc)));
    if (values.isEmpty()) {
      return Optional.empty();
    }
    if (values.size() == 1) {
      return Optional.of(values.get(0));
    }
    return Optional.of(new BsonArray(values));
  }

  /** The path's columns, in BSON bracket order. */
  List<Column> columns() {
    return this.columns;
  }

  private static Optional<Column> openColumn(
      FieldPath path, TypeField type, LeafReader reader, Selection selection) throws IOException {
    String field = type.getLuceneFieldName(path, Optional.empty());
    return switch (type) {
      // A numeric field's representation picks the column it is written to, so each column has one
      // encoding to undo: sortableNumberFieldName sends an int64 representation to
      // NUMBER_INT64_V2, holding the value as-is, and a double representation to NUMBER_DOUBLE_V2,
      // holding toMqlSortableLong of it. Decoding is the inverse of whichever the column applied.
      case NUMBER_INT64_V2 -> numericColumn(reader, field, selection, BsonInt64::new);
      case NUMBER_DOUBLE_V2 ->
          numericColumn(
              reader,
              field,
              selection,
              encoded -> new BsonDouble(LuceneDoubleConversionUtils.fromMqlSortableLong(encoded)));
      case DATE_V2 -> numericColumn(reader, field, selection, BsonDateTime::new);
      case TOKEN -> ordinalColumn(reader, field, selection, BsonUtils.STRING_CONVERTER);
      case UUID -> ordinalColumn(reader, field, selection, BsonUtils.UUID_CONVERTER);
      case OBJECT_ID -> ordinalColumn(reader, field, selection, BsonUtils.OBJECT_ID_CONVERTER);
      case BOOLEAN -> ordinalColumn(reader, field, selection, DocValuesFieldReader::toBoolean);
      case NULL -> nullColumn(reader, field);
      default ->
          throw new IllegalArgumentException(
              "Token metric values cannot be read from doc values for TypeField: " + type);
    };
  }

  private static Optional<Column> numericColumn(
      LeafReader reader, String field, Selection selection, LongFunction<BsonValue> converter)
      throws IOException {
    return Optional.ofNullable(reader.getSortedNumericDocValues(field))
        .map(
            values -> {
              if (selection == Selection.ALL) {
                return leafDoc -> {
                  if (!values.advanceExact(leafDoc)) {
                    return List.of();
                  }
                  int count = values.docValueCount();
                  List<BsonValue> decoded = new ArrayList<>(count);
                  for (int i = 0; i < count; i++) {
                    decoded.add(converter.apply(values.nextValue()));
                  }
                  return decoded;
                };
              }
              NumericDocValues selected =
                  SortedNumericSelector.wrap(
                      values,
                      selection == Selection.MIN
                          ? SortedNumericSelector.Type.MIN
                          : SortedNumericSelector.Type.MAX,
                      SortField.Type.LONG);
              return leafDoc ->
                  selected.advanceExact(leafDoc)
                      ? List.of(converter.apply(selected.longValue()))
                      : List.of();
            });
  }

  private static Optional<Column> ordinalColumn(
      LeafReader reader, String field, Selection selection, Function<BytesRef, BsonValue> converter)
      throws IOException {
    return Optional.ofNullable(reader.getSortedSetDocValues(field))
        .map(
            values -> {
              if (selection == Selection.ALL) {
                return leafDoc -> {
                  if (!values.advanceExact(leafDoc)) {
                    return List.of();
                  }
                  int count = values.docValueCount();
                  List<BsonValue> decoded = new ArrayList<>(count);
                  for (int i = 0; i < count; i++) {
                    decoded.add(converter.apply(values.lookupOrd(values.nextOrd())));
                  }
                  return decoded;
                };
              }
              SortedDocValues selected =
                  SortedSetSelector.wrap(
                      values,
                      selection == Selection.MIN
                          ? SortedSetSelector.Type.MIN
                          : SortedSetSelector.Type.MAX);
              return singleOrdinalColumn(selected, converter);
            });
  }

  /** The null column is single-valued, so there is nothing to select. */
  private static Optional<Column> nullColumn(LeafReader reader, String field) throws IOException {
    return Optional.ofNullable(reader.getSortedDocValues(field))
        .map(values -> singleOrdinalColumn(values, term -> BsonNull.VALUE));
  }

  private static Column singleOrdinalColumn(
      SortedDocValues values, Function<BytesRef, BsonValue> converter) {
    return leafDoc ->
        values.advanceExact(leafDoc)
            ? List.of(converter.apply(values.lookupOrd(values.ordValue())))
            : List.of();
  }

  private static BsonValue toBoolean(BytesRef term) {
    return BsonBoolean.valueOf(FieldValue.BOOLEAN_TRUE_FIELD_VALUE.equals(term.utf8ToString()));
  }

  /** One BSON type's doc-values column within a segment. */
  @FunctionalInterface
  interface Column {
    /**
     * The document's values in this column, ascending; empty when it has none here. At most one
     * value under {@link Selection#MIN} / {@link Selection#MAX}.
     */
    List<BsonValue> values(int leafDoc) throws IOException;
  }
}
