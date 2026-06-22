package com.xgen.mongot.index.lucene.query.pushdown;

class MqlDouble {

  // Ints with magnitude <= 2**53 can be precisely represented as doubles.
  public static final long END_OF_PRECISE_DOUBLES = 1L << 53;

  // Large magnitude doubles (including +/- Inf) are strictly > or < all Longs.
  public static final double BOUND_OF_LONG_RANGE = -(double) Long.MIN_VALUE; // positive 2**63

  public static int compare(double lhs, long rhs) {
    // We need this overload to prevent implicit loss of precision if developer swaps arguments.
    return -compare(rhs, lhs);
  }

  public static int compare(long lhs, double rhs) {
    // Copied from github.com/mongodb/mongo/blob/master/src/mongo/base/compare_numbers.h#L77-L100
    // All Longs are > NaN
    if (Double.isNaN(rhs)) {
      return 1;
    }

    // Additionally, doubles outside of this range can't have a fractional component.
    if (lhs <= END_OF_PRECISE_DOUBLES && lhs >= -END_OF_PRECISE_DOUBLES) {
      return compare((double) lhs, rhs);
    }

    if (rhs >= BOUND_OF_LONG_RANGE) {
      return -1; // Can't be represented in a Long.
    }
    if (rhs < -BOUND_OF_LONG_RANGE) {
      return 1; // Can be represented in a Long.
    }

    // Remaining Doubles can have their integer component precisely represented as longs.
    // If they have a fractional component, they must be strictly > or < lhs even after
    // truncation of the fractional component since low-magnitude lhs were handled above.
    // long > 2^53, double < 2^64
    return Long.compare(lhs, (long) rhs);
  }

  /**
   * Compares two doubles using the same semantics of as the MQL aggregation pipeline. This differs
   * from {@link Double#compare(double, double)} in two ways:
   *
   * <ol>
   *   <li>This method considers NaN as less than all other numbers
   *   <li>This method considers -0.0 and +0.0 equal
   * </ol>
   */
  public static int compare(double lhs, double rhs) {
    if (lhs < rhs) {
      return -1;
    }
    if (lhs > rhs) {
      return 1;
    }
    if (lhs == rhs) {
      return 0;
    }

    // If none of the above cases returned, lhs or rhs must be NaN.
    if (Double.isNaN(lhs)) {
      return Double.isNaN(rhs) ? 0 : -1;
    }
    return 1;
  }
}
