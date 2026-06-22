package com.xgen.mongot.index.lucene.query.sort.mixed;

import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import java.util.Arrays;
import java.util.Comparator;
import org.bson.BsonBinary;
import org.bson.BsonType;
import org.bson.BsonValue;

public final class SortUtil {

  public static final Comparator<BsonValue> MQL_COMPARATOR_NULLS_LOWEST =
      (first, second) -> mqlMixedCompare(first, second, NullEmptySortPosition.LOWEST);

  // TODO(CLOUDP-280897): For the purpose of sorting, is it necessary to use utf8 conversion? I
  // think we should be able to decode as Latin-1 for optimize string comparison + memory and still
  // preserve sort order
  /**
   * Compares two BsonValues of potentially different types. Comparison leverages the sort priority
   * of nulls/missing/[] values.
   *
   * <p>Only a subset of types is valid: BsonInt64, BsonDouble, BsonDate, BsonNull, BsonString. See
   * <a href=https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order>Bson Sort
   * Order</a> for definition of mixed-type sort.
   *
   * <p>Note: this implementation does not handle some edge cases in the same way as the aggregation
   * pipeline. Notably, NaN and -0.0 have incorrect sort order, and BsonDouble may be sorted
   * incorrectly against large-magnitude BsonInt64 values.
   *
   * @return A value < 1 if `left` is less than right, 0 if they are equal, >1 otherwise.
   * @throws UnsupportedOperationException if either value is of an unsupported type.
   */
  public static int mqlMixedCompare(
      BsonValue first, BsonValue second, NullEmptySortPosition nullEmptySortPosition) {
    // Check if values fall into different type brackets
    BsonType firstType = first.getBsonType();
    BsonType secondType = second.getBsonType();
    int typeCompare =
        Integer.compare(
            getBracketPriority(firstType, nullEmptySortPosition),
            getBracketPriority(secondType, nullEmptySortPosition));
    if (typeCompare != 0) {
      return typeCompare;
    }

    // From here on, everything has same bracket but possibly different types
    return compareWithinBracketUnsafe(first, second);
  }

  /**
   * Maps BsonType to sorting priority based on the specified null and empty values sort position.
   *
   * <p>Sort priority values are taken from the mongo repo's key_string.cpp file: <a
   * href="https://tinyurl.com/mwfam9xc"></a>
   *
   * @param nullEmptySortPosition Determines how nulls and empty array values should be prioritized
   *     in sorting. Only affects null-like types (NULL, UNDEFINED, END_OF_DOCUMENT) and
   *     MIN_KEY/MAX_KEY since null values are temporarily mapped to BsonMinKey/BsonMaxKey until
   *     SERVER-93244 is completed. For all other types, this parameter has no effect.
   */
  public static int getBracketPriority(BsonType type, NullEmptySortPosition nullEmptySortPosition) {
    return switch (type) {
      // Mostly copied from key_string.cpp
      case MIN_KEY -> NullEmptySortPosition.LOWEST.nullMissingPriority;
      case UNDEFINED -> nullEmptySortPosition.emptyArrayPriority;
      case END_OF_DOCUMENT, NULL -> nullEmptySortPosition.nullMissingPriority;
      case DOUBLE, INT32, INT64, DECIMAL128 -> 30;
      case SYMBOL, STRING -> 60;
      case DOCUMENT -> 70;
      case ARRAY -> 80;
      case BINARY -> 90;
      case OBJECT_ID -> 100;
      case BOOLEAN -> 110;
      case DATE_TIME -> 120;
      case TIMESTAMP -> 130;
      case REGULAR_EXPRESSION -> 140;
      case MAX_KEY -> NullEmptySortPosition.HIGHEST.nullMissingPriority;
      case JAVASCRIPT -> 160;
      case DB_POINTER, JAVASCRIPT_WITH_SCOPE ->
          throw new UnsupportedOperationException(
              String.format("The following BsonType cannot be sorted: %s", type));
    };
  }

  /**
   * Compare two BsonValues which may be different types but are known to be within the same bracket
   *
   * <p>For example, this method can compare BsonInt64 to BsonDouble but not to BsonString.
   */
  public static int compareWithinBracketUnsafe(BsonValue left, BsonValue right) {
    return switch (left.getBsonType()) {
      case NULL, UNDEFINED, MIN_KEY, MAX_KEY, END_OF_DOCUMENT ->
          // If values have same bracket and one is a singleton type, they must be equal
          0;
      case INT64 ->
          // secondType must be INT64 or DOUBLE (or unsupported INT32, DECIMAL128)
          (right.getBsonType() == BsonType.INT64)
              ? Long.compare(left.asInt64().getValue(), right.asInt64().getValue())
              // If right is not INT64, promote first to double and compare. This comparison should
              // be kept identical to the case below.
              : mqlDoubleCompare(left.asNumber().doubleValue(), right.asNumber().doubleValue());
      case DOUBLE ->
          /*
           Compare first and second as doubles regardless of whether they are both doubles or a
           combination of long and double since long/double comparison will require converting longs
           to doubles before comparison.
          */
          mqlDoubleCompare(left.asNumber().doubleValue(), right.asNumber().doubleValue());
      case STRING -> left.asString().compareTo(right.asString());
      case DATE_TIME -> Long.compare(left.asDateTime().getValue(), right.asDateTime().getValue());
      case BINARY -> compare(left.asBinary(), right.asBinary());
      case OBJECT_ID -> left.asObjectId().compareTo(right.asObjectId());
      case BOOLEAN -> left.asBoolean().compareTo(right.asBoolean());
      case TIMESTAMP,
          INT32,
          DECIMAL128,
          JAVASCRIPT_WITH_SCOPE,
          SYMBOL,
          JAVASCRIPT,
          DB_POINTER,
          REGULAR_EXPRESSION,
          DOCUMENT,
          ARRAY ->
          throw new UnsupportedOperationException(
              String.format("BsonType %s is not yet support in sort", left.getBsonType()));
    };
  }

  /**
   * Sorts binary typed data in accordance with MQL sort order, and the assumptions made in the
   * implementation have been validated by the server query team. For more information, see
   * https://tinyurl.com/mr2zmtkv.
   */
  public static int compare(BsonBinary left, BsonBinary right) {
    byte[] leftData = left.getData();
    byte[] rightData = right.getData();

    // Compare by length of data
    int lengthCompare = Integer.compare(leftData.length, rightData.length);
    if (lengthCompare != 0) {
      return lengthCompare;
    }

    // Compare by binary subtype
    int subTypeCompare = Byte.compareUnsigned(left.getType(), right.getType());
    if (subTypeCompare != 0) {
      return subTypeCompare;
    }

    // Compare the data byte by byte
    return Arrays.compareUnsigned(leftData, rightData);
  }

  /** Similar to {@link Double#compare(double, double)} but orders NaNs first rather than last. */
  public static int mqlDoubleCompare(double first, double second) {
    // NaN should sort lower than all other values
    if (Double.isNaN(first) && Double.isNaN(second)) {
      return 0;
    }

    if (Double.isNaN(first)) {
      return -1;
    }

    if (Double.isNaN(second)) {
      return 1;
    }

    return Double.compare(first, second);
  }
}
