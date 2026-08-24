package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.Test;

/**
 * Ordering rules used when min/max partials are folded across partitions and shards.
 *
 * <p>These are only reached from the {@link MinAggregationResult} / {@link MaxAggregationResult}
 * builders -- collection compares decoded BSON values inside {@link MinMaxAccumulator} -- so cases
 * here cover both cross-numeric-type ordering and cross-bracket BSON order.
 */
public class MinMaxSelectionTest {

  @Test
  public void isSmaller_ordersNumbersAscending() {
    assertTrue(MinMaxSelection.isSmaller(new BsonInt64(3), new BsonInt64(10)));
    assertFalse(MinMaxSelection.isSmaller(new BsonInt64(10), new BsonInt64(3)));
  }

  @Test
  public void isLarger_ordersNumbersDescending() {
    assertTrue(MinMaxSelection.isLarger(new BsonInt64(10), new BsonInt64(3)));
    assertFalse(MinMaxSelection.isLarger(new BsonInt64(3), new BsonInt64(10)));
  }

  @Test
  public void equalValues_neitherSmallerNorLarger() {
    // Both comparisons are strict, so an equal candidate leaves the incumbent in place. If either
    // were non-strict the merge would keep churning its result for no reason.
    assertFalse(MinMaxSelection.isSmaller(new BsonInt64(7), new BsonInt64(7)));
    assertFalse(MinMaxSelection.isLarger(new BsonInt64(7), new BsonInt64(7)));
  }

  @Test
  public void comparesAcrossNumericTypes() {
    // A partition of an int64 field and a shard partial that came back as a double must still be
    // ordered by numeric value, not by BSON type. Comparing by type first would make every int64
    // smaller than every double regardless of magnitude.
    assertTrue(MinMaxSelection.isSmaller(new BsonDouble(2.5), new BsonInt64(5)));
    assertFalse(MinMaxSelection.isSmaller(new BsonInt64(5), new BsonDouble(2.5)));
    assertTrue(MinMaxSelection.isLarger(new BsonDouble(9.5), new BsonInt64(5)));
    assertTrue(MinMaxSelection.isSmaller(new BsonInt32(1), new BsonInt64(2)));
  }

  @Test
  public void numericallyEqualAcrossTypes_neitherSmallerNorLarger() {
    // 5 and 5.0 are the same number, so neither displaces the other no matter which arrives first.
    assertFalse(MinMaxSelection.isSmaller(new BsonDouble(5.0), new BsonInt64(5)));
    assertFalse(MinMaxSelection.isLarger(new BsonDouble(5.0), new BsonInt64(5)));
  }

  @Test
  public void nanSortsBelowEveryNumber() {
    // Matches the encoded-space ordering the accumulator gets for free from NAN_SENTINEL, so a
    // partition whose min is NaN still wins the cross-partition fold.
    assertTrue(
        MinMaxSelection.isSmaller(new BsonDouble(Double.NaN), new BsonInt64(Long.MIN_VALUE)));
    assertTrue(
        MinMaxSelection.isSmaller(
            new BsonDouble(Double.NaN), new BsonDouble(Double.NEGATIVE_INFINITY)));
    assertFalse(MinMaxSelection.isLarger(new BsonDouble(Double.NaN), new BsonDouble(0.0)));
  }

  @Test
  public void infinitiesBoundTheOrdering() {
    assertTrue(
        MinMaxSelection.isLarger(
            new BsonDouble(Double.POSITIVE_INFINITY), new BsonInt64(Long.MAX_VALUE)));
    assertTrue(
        MinMaxSelection.isSmaller(
            new BsonDouble(Double.NEGATIVE_INFINITY), new BsonInt64(Long.MIN_VALUE)));
  }

  @Test
  public void comparesAcrossBsonBrackets() {
    // Merge now sees non-numeric partials, so bracket order must match BSON total ordering:
    // null < number < string < boolean < date.
    assertTrue(MinMaxSelection.isSmaller(BsonNull.VALUE, new BsonInt64(0)));
    assertTrue(MinMaxSelection.isSmaller(new BsonInt64(42), new BsonString("apple")));
    assertTrue(MinMaxSelection.isSmaller(new BsonString("z"), BsonBoolean.TRUE));
    assertTrue(MinMaxSelection.isSmaller(BsonBoolean.TRUE, new BsonDateTime(1)));
    assertTrue(MinMaxSelection.isLarger(new BsonDateTime(1), new BsonInt64(Long.MAX_VALUE)));
  }
}
