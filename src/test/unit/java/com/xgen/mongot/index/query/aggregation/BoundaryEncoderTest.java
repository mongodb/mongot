package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.definition.DateFieldDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.definition.NumberFieldDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.NumericFieldOptions.Representation;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.junit.Test;

/**
 * Search aggregation supports only V2 numeric/date field types (V1 {@code *_FACET} is rejected).
 */
public class BoundaryEncoderTest {

  @Test
  public void encoderInt64Representation() {
    assertEquals(
        100L,
        BoundaryEncoder.encode(
            new BsonInt64(100), number(Representation.INT64), TypeField.NUMBER_INT64_V2));
    assertEquals(
        5L,
        BoundaryEncoder.encode(
            new BsonInt32(5), number(Representation.INT64), TypeField.NUMBER_INT64_V2));
    // Double-typed boundary on an INT64 field is cast.
    assertEquals(
        3L,
        BoundaryEncoder.encode(
            new BsonDouble(3.7), number(Representation.INT64), TypeField.NUMBER_INT64_V2));
    // V2 maps NaN -> Long.MIN_VALUE.
    assertEquals(
        Long.MIN_VALUE,
        BoundaryEncoder.encode(
            new BsonDouble(Double.NaN), number(Representation.INT64), TypeField.NUMBER_INT64_V2));
  }

  @Test
  public void encoderDoubleRepresentationMatchesConversionUtils() {
    assertEquals(
        LuceneDoubleConversionUtils.toMqlSortableLong(1.5),
        BoundaryEncoder.encode(
            new BsonDouble(1.5), number(Representation.DOUBLE), TypeField.NUMBER_DOUBLE_V2));
    // Integer-typed boundary on a DOUBLE field routes through the indexed-long conversion.
    assertEquals(
        LuceneDoubleConversionUtils.toMqlIndexedLong(7),
        BoundaryEncoder.encode(
            new BsonInt64(7), number(Representation.DOUBLE), TypeField.NUMBER_DOUBLE_V2));

    // Order preservation across negative/zero/positive (the property rangeIndexOf relies on).
    FieldTypeDefinition doubleField = number(Representation.DOUBLE);
    long a = BoundaryEncoder.encode(new BsonDouble(-1.5), doubleField, TypeField.NUMBER_DOUBLE_V2);
    long b = BoundaryEncoder.encode(new BsonDouble(0.0), doubleField, TypeField.NUMBER_DOUBLE_V2);
    long c = BoundaryEncoder.encode(new BsonDouble(2.5), doubleField, TypeField.NUMBER_DOUBLE_V2);
    assertTrue(a < b);
    assertTrue(b < c);
  }

  @Test
  public void encoderDateIsRawEpochMillis() {
    assertEquals(
        1_700_000_000_000L,
        BoundaryEncoder.encode(
            new BsonDateTime(1_700_000_000_000L), new DateFieldDefinition(), TypeField.DATE_V2));
  }

  @Test
  public void encoderLastEdgeInclusiveMatrix() {
    double overflow = (double) Long.MAX_VALUE * 2; // > Long.MAX_VALUE
    assertTrue(
        BoundaryEncoder.lastEdgeInclusive(new BsonDouble(overflow), number(Representation.INT64)));
    assertTrue(
        !BoundaryEncoder.lastEdgeInclusive(
            new BsonDouble(overflow), number(Representation.DOUBLE)));
    assertTrue(
        !BoundaryEncoder.lastEdgeInclusive(new BsonInt64(100), number(Representation.INT64)));
    assertTrue(
        !BoundaryEncoder.lastEdgeInclusive(new BsonDouble(50.0), number(Representation.INT64)));
  }

  private static FieldTypeDefinition number(Representation representation) {
    return new NumberFieldDefinition(new NumericFieldOptions(representation, true, true));
  }
}
