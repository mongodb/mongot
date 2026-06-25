package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.definition.NumericFieldDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions.Representation;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import org.bson.BsonNumber;
import org.bson.BsonValue;

/**
 * Encodes boundary BSON values into the stored doc-values long space so that boundary edges and
 * stored doc values are directly comparable.
 *
 * <p>Encoding depends on two field attributes, supplied by the caller: the numeric {@link
 * Representation} comes from the resolved {@link FieldTypeDefinition} (a {@link
 * NumericFieldDefinition}'s options), and the V1/V2 encoding comes from the {@link TypeField} (e.g.
 * resolved via {@code LuceneFacetContext.getFacetFieldType}).
 *
 * <p>This mirrors the indexer's own conversion ({@code IndexableFieldFactory.numericIndexedValue} /
 * {@code sortableNumericIndexedValue}) by delegating to the same {@link
 * LuceneDoubleConversionUtils} primitives. The indexer helpers are package-private in another
 * package; consolidating all three call sites onto one shared util is a follow-up so they can never
 * drift.
 */
final class BoundaryEncoder {

  private BoundaryEncoder() {}

  /**
   * Encode one boundary into the stored doc-values long space. The {@link Representation} comes
   * from {@code fieldDefinition}; the V1/V2 encoding comes from {@code typeField}.
   * Order-preserving.
   */
  static long encode(BsonValue boundary, FieldTypeDefinition fieldDefinition, TypeField typeField) {
    if (boundary.isDateTime()) {
      return boundary.asDateTime().getValue(); // raw epoch millis; date has no representation
    }
    BsonNumber n = boundary.asNumber();
    Representation rep = representationOf(fieldDefinition);
    boolean isV2 = isV2(typeField);
    return switch (n.getBsonType()) {
      // Integer-typed boundary -> long overload (DOUBLE rep routes through toIndexedLong).
      case INT32, INT64 -> encodeLong(n.longValue(), rep, isV2);
      // Double-typed boundary -> double overload (INT64 rep applies the NaN / Inf / cap rules).
      case DOUBLE -> encodeDouble(n.doubleValue(), rep, isV2);
      default -> throw new IllegalArgumentException("non-numeric boundary: " + n.getBsonType());
    };
  }

  private static long encodeLong(long value, Representation rep, boolean isV2) {
    return switch (rep) {
      case INT64 -> value;
      case DOUBLE ->
          isV2
              ? LuceneDoubleConversionUtils.toMqlIndexedLong(value)
              : LuceneDoubleConversionUtils.toIndexedLong(value);
    };
  }

  private static long encodeDouble(double value, Representation rep, boolean isV2) {
    return switch (rep) {
      // Matches IndexableFieldFactory: V2 NaN -> Long.MIN_VALUE; the cast saturates Inf / overflow.
      case INT64 -> isV2 ? (Double.isNaN(value) ? Long.MIN_VALUE : (long) value) : (long) value;
      case DOUBLE ->
          isV2
              ? LuceneDoubleConversionUtils.toMqlSortableLong(value)
              : LuceneDoubleConversionUtils.toLong(value);
    };
  }

  /**
   * Whether the top range's upper edge is inclusive -- true only in the INT64-representation
   * overflow case (a double boundary &gt; Long.MAX_VALUE), so values that cap to Long.MAX_VALUE
   * (huge / +Inf) land in the top bucket. Same predicate as {@code
   * LuceneFacetContext.isOverflowingDouble}.
   */
  static boolean lastEdgeInclusive(BsonValue lastBoundary, FieldTypeDefinition fieldDefinition) {
    // isDouble() is checked first so date fields (non-numeric definitions) short-circuit to false
    // without consulting representationOf.
    return lastBoundary.isDouble()
        && representationOf(fieldDefinition) == Representation.INT64
        && lastBoundary.asDouble().getValue() > Long.MAX_VALUE;
  }

  /** Numeric representation read from the resolved field definition's options. */
  private static Representation representationOf(FieldTypeDefinition fieldDefinition) {
    if (fieldDefinition instanceof NumericFieldDefinition numeric) {
      return numeric.options().representation();
    }
    throw new IllegalArgumentException(
        "not a numeric boundary field: " + fieldDefinition.getType());
  }

  /** Whether the field uses the V2 (non-facet) encoding, from its {@link TypeField}. */
  private static boolean isV2(TypeField typeField) {
    return switch (typeField) {
      case NUMBER_INT64_V2, NUMBER_DOUBLE_V2, DATE_V2 -> true;
      default ->
          throw new IllegalArgumentException(
              "not a numeric/date boundary field type: " + typeField);
    };
  }
}
