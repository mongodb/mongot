package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.xgen.mongot.index.definition.NumericFieldOptions.Representation;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
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
}
