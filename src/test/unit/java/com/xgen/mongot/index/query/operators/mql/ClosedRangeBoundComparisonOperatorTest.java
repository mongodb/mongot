package com.xgen.mongot.index.query.operators.mql;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.StringRangeBound;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.points.StringPoint;
import java.util.Date;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.junit.Test;

public class ClosedRangeBoundComparisonOperatorTest {

  @Test
  public void constructor_missingLowerBound_throws() {
    NumericRangeBound halfOpen =
        new NumericRangeBound(Optional.empty(), Optional.of(new DoublePoint(4.0)), false, true);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> new ClosedRangeBoundComparisonOperator(halfOpen));

    assertThat(e).hasMessageThat().contains("both lower and upper bounds");
  }

  @Test
  public void constructor_missingUpperBound_throws() {
    NumericRangeBound halfOpen =
        new NumericRangeBound(Optional.of(new DoublePoint(2.0)), Optional.empty(), true, false);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> new ClosedRangeBoundComparisonOperator(halfOpen));

    assertThat(e).hasMessageThat().contains("both lower and upper bounds");
  }

  @Test
  public void constructor_unsupportedBoundType_throws() {
    StringRangeBound stringBounds =
        new StringRangeBound(
            Optional.of(new StringPoint("a")), Optional.of(new StringPoint("b")), true, true);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new ClosedRangeBoundComparisonOperator(stringBounds));

    assertThat(e).hasMessageThat().contains("StringRangeBound");
  }

  @Test
  public void constructor_invertedNumericBounds_throws() {
    NumericRangeBound inverted =
        new NumericRangeBound(
            Optional.of(new DoublePoint(4.0)), Optional.of(new DoublePoint(2.0)), true, true);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> new ClosedRangeBoundComparisonOperator(inverted));

    assertThat(e).hasMessageThat().contains("lower <= upper");
  }

  @Test
  public void constructor_invertedDateBounds_throws() {
    DateRangeBound inverted =
        new DateRangeBound(
            Optional.of(new DatePoint(new Date(2000L))),
            Optional.of(new DatePoint(new Date(1000L))),
            true,
            true);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> new ClosedRangeBoundComparisonOperator(inverted));

    assertThat(e).hasMessageThat().contains("lower <= upper");
  }

  @Test
  public void constructor_equalBoundsWithExclusiveEnd_throws() {
    NumericRangeBound emptyRange =
        new NumericRangeBound(
            Optional.of(new DoublePoint(5.0)), Optional.of(new DoublePoint(5.0)), true, false);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new ClosedRangeBoundComparisonOperator(emptyRange));

    assertThat(e).hasMessageThat().contains("both bounds inclusive");
  }

  @Test
  public void constructor_equalInclusiveBounds_succeeds() {
    NumericRangeBound pointRange =
        new NumericRangeBound(
            Optional.of(new DoublePoint(5.0)), Optional.of(new DoublePoint(5.0)), true, true);

    var operator = new ClosedRangeBoundComparisonOperator(pointRange);

    assertEquals(pointRange, operator.bounds());
  }

  /**
   * {@code NaN} compares equal to any long under the numeric-point total order, so a NaN/non-NaN
   * pair reaches the equal-bounds branch without being an empty range. Such a false equality must
   * be admitted even with exclusive ends, matching the parse-path validation in {@code RangeBound}.
   */
  @Test
  public void constructor_nanFalseEqualityWithExclusiveEnds_succeeds() {
    NumericRangeBound falseEquality =
        new NumericRangeBound(
            Optional.of(new DoublePoint(Double.NaN)), Optional.of(new LongPoint(5L)), false, false);

    var operator = new ClosedRangeBoundComparisonOperator(falseEquality);

    assertEquals(falseEquality, operator.bounds());
  }

  @Test
  public void getCategory_returnsClosedRange() {
    var operator =
        new ClosedRangeBoundComparisonOperator(
            new NumericRangeBound(
                Optional.of(new DoublePoint(2.0)), Optional.of(new DoublePoint(4.0)), true, true));

    // A closed range is both bounds at once; counting it as LOWER_BOUND (or UPPER_BOUND) would
    // misreport metrics the first time post-optimization trees are recorded.
    assertEquals(MqlFilterOperator.Category.CLOSED_RANGE, operator.getCategory());
  }

  @Test
  public void toBson_numericBounds_emitsClosedRangeSigilShape() {
    var operator =
        new ClosedRangeBoundComparisonOperator(
            new NumericRangeBound(
                Optional.of(new DoublePoint(2.0)), Optional.of(new DoublePoint(4.0)), true, false));

    BsonDocument expected =
        new BsonDocument(
            ClosedRangeBoundComparisonOperator.SIGIL,
            new BsonDocument()
                .append("lower", new BsonDouble(2.0))
                .append("lowerInclusive", BsonBoolean.TRUE)
                .append("upper", new BsonDouble(4.0))
                .append("upperInclusive", BsonBoolean.FALSE));
    assertEquals(expected, operator.toBson());
  }

  @Test
  public void toBson_dateBounds_emitsClosedRangeSigilShape() {
    var operator =
        new ClosedRangeBoundComparisonOperator(
            new DateRangeBound(
                Optional.of(new DatePoint(new Date(1000L))),
                Optional.of(new DatePoint(new Date(2000L))),
                false,
                true));

    BsonDocument expected =
        new BsonDocument(
            ClosedRangeBoundComparisonOperator.SIGIL,
            new BsonDocument()
                .append("lower", new BsonDateTime(1000L))
                .append("lowerInclusive", BsonBoolean.FALSE)
                .append("upper", new BsonDateTime(2000L))
                .append("upperInclusive", BsonBoolean.TRUE));
    assertEquals(expected, operator.toBson());
  }
}
