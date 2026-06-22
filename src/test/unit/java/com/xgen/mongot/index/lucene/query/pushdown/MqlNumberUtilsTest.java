package com.xgen.mongot.index.lucene.query.pushdown;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import org.bson.types.Decimal128;
import org.junit.Test;

public class MqlNumberUtilsTest {

  private static void compare(int expected, Decimal128 left, Decimal128 right) {
    assertEquals(expected, MqlNumberUtils.compare(left, right));
    assertEquals(-expected, MqlNumberUtils.compare(right, left));

    if (!left.isNaN() && left.isFinite() && !right.isNaN() && right.isFinite()) {
      var bigDecimalLeft = new BigDecimal(left.toString());
      var bigDecimalRight = new BigDecimal(right.toString());
      assertEquals(expected, bigDecimalLeft.compareTo(bigDecimalRight));
    }
  }

  @Test
  public void decimal128() {
    compare(0, Decimal128.NaN, Decimal128.NaN);
    compare(0, Decimal128.NaN, Decimal128.NEGATIVE_NaN);
    compare(-1, Decimal128.NaN, Decimal128.NEGATIVE_INFINITY);
    compare(-1, Decimal128.NaN, Decimal128.POSITIVE_ZERO);
    compare(-1, Decimal128.NaN, Decimal128.NEGATIVE_ZERO);

    compare(0, Decimal128.NEGATIVE_ZERO, Decimal128.NEGATIVE_ZERO);
    compare(0, Decimal128.POSITIVE_ZERO, Decimal128.POSITIVE_ZERO);
    compare(0, Decimal128.NEGATIVE_ZERO, Decimal128.POSITIVE_ZERO);

    compare(-1, Decimal128.NEGATIVE_ZERO, new Decimal128(1));
    compare(-1, Decimal128.POSITIVE_ZERO, new Decimal128(1));
    compare(1, Decimal128.NEGATIVE_ZERO, new Decimal128(-1));
    compare(1, Decimal128.POSITIVE_ZERO, new Decimal128(-1));

    compare(0, Decimal128.NEGATIVE_INFINITY, Decimal128.NEGATIVE_INFINITY);
    compare(-1, Decimal128.NEGATIVE_INFINITY, Decimal128.POSITIVE_INFINITY);

    compare(1, Decimal128.parse("5"), Decimal128.parse("-5"));
    compare(0, Decimal128.parse("5"), Decimal128.parse("5"));

    compare(
        1,
        Decimal128.fromIEEE754BIDEncoding(Decimal128.POSITIVE_ZERO.getHigh() | 1, 0),
        Decimal128.fromIEEE754BIDEncoding(Decimal128.NEGATIVE_ZERO.getHigh() | 1, 0));
  }
}
