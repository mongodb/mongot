package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.definition.NumericFieldOptions.Representation;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.junit.Test;

public class NumericMetricValuesTest {

  @Test
  public void decode_doubleRepresentation_roundTripsMqlSortableEncoding() {
    for (double value : new double[] {0.0, -1.5, 1.5, Double.MAX_VALUE, Double.NEGATIVE_INFINITY}) {
      long encoded = LuceneDoubleConversionUtils.toMqlSortableLong(value);
      assertEquals(value, NumericMetricValues.decode(encoded, Representation.DOUBLE), 0.0);
    }
  }

  @Test
  public void decode_doubleRepresentation_roundTripsNaN() {
    long encoded = LuceneDoubleConversionUtils.toMqlSortableLong(Double.NaN);
    assertEquals(Double.NaN, NumericMetricValues.decode(encoded, Representation.DOUBLE), 0.0);
  }

  @Test
  public void decode_int64Representation_widensRawLong() {
    assertEquals(42.0, NumericMetricValues.decode(42L, Representation.INT64), 0.0);
    assertEquals(
        (double) Long.MAX_VALUE,
        NumericMetricValues.decode(Long.MAX_VALUE, Representation.INT64),
        0.0);
    assertEquals(
        (double) Long.MIN_VALUE,
        NumericMetricValues.decode(Long.MIN_VALUE, Representation.INT64),
        0.0);
  }

  @Test
  public void decode_int64Representation_doesNotRecoverIndexedNaN() {
    // sortableNumericIndexedValue(INT64, NaN) stores Long.MIN_VALUE; decode cannot distinguish
    // that sentinel from a true Long.MIN_VALUE and does not restore NaN.
    assertFalse(Double.isNaN(NumericMetricValues.decode(Long.MIN_VALUE, Representation.INT64)));
  }

  @Test
  public void decodeToBson_int64Representation_returnsInt64WithoutWidening() {
    // min/max report a value drawn from the data, so the int64 field's type survives. sum/avg,
    // which compute a value, go through decode(...) and widen to double instead.
    assertEquals(new BsonInt64(42), NumericMetricValues.decodeToBson(42L, Representation.INT64));
  }

  @Test
  public void decodeToBson_int64Representation_isExactBeyond2To53() {
    // The whole point of not routing min/max through decode(...): these magnitudes are not
    // representable in a double, so widening would report a neighbouring integer as the min/max.
    long justOver2To53 = (1L << 53) + 1;
    assertEquals(
        new BsonInt64(justOver2To53),
        NumericMetricValues.decodeToBson(justOver2To53, Representation.INT64));
    assertEquals(
        new BsonInt64(Long.MAX_VALUE),
        NumericMetricValues.decodeToBson(Long.MAX_VALUE, Representation.INT64));
    assertEquals(
        new BsonInt64(Long.MIN_VALUE),
        NumericMetricValues.decodeToBson(Long.MIN_VALUE, Representation.INT64));
  }

  @Test
  public void decodeToBson_int64Representation_indexedNaNSurfacesAsLongMinValue() {
    // Same sentinel collision decode(...) has: an indexed NaN is stored as Long.MIN_VALUE, so a
    // min over a column containing one reports NumberLong(-9223372036854775808) rather than NaN.
    // Documented here so the behaviour is a known quantity rather than a surprise in the field.
    assertEquals(
        new BsonInt64(Long.MIN_VALUE),
        NumericMetricValues.decodeToBson(Long.MIN_VALUE, Representation.INT64));
  }

  @Test
  public void decodeToBson_doubleRepresentation_roundTripsMqlSortableEncoding() {
    for (double value :
        new double[] {
          0.0,
          -1.5,
          1.5,
          Double.MAX_VALUE,
          -Double.MAX_VALUE,
          Double.MIN_VALUE,
          Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY
        }) {
      long encoded = LuceneDoubleConversionUtils.toMqlSortableLong(value);
      assertEquals(
          new BsonDouble(value), NumericMetricValues.decodeToBson(encoded, Representation.DOUBLE));
    }
  }

  @Test
  public void decodeToBson_doubleRepresentation_roundTripsNaN() {
    long encoded = LuceneDoubleConversionUtils.toMqlSortableLong(Double.NaN);
    BsonValue decoded = NumericMetricValues.decodeToBson(encoded, Representation.DOUBLE);
    assertTrue(Double.isNaN(decoded.asDouble().getValue()));
  }

  @Test
  public void decodeToBson_doubleRepresentation_negativeZeroCoalescesToPositiveZero() {
    // toMqlSortableLong folds -0.0 into +0.0 so both sort identically; decoding cannot and should
    // not resurrect the sign.
    long encoded = LuceneDoubleConversionUtils.toMqlSortableLong(-0.0);
    assertEquals(
        new BsonDouble(0.0), NumericMetricValues.decodeToBson(encoded, Representation.DOUBLE));
  }
}
