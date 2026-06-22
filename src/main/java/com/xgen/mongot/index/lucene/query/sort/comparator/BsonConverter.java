package com.xgen.mongot.index.lucene.query.sort.comparator;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;

/**
 * This class represents an order-preserving Bijection from a java type T to a BsonType.
 *
 * <p>This class wraps a Comparator that may be efficiently able to compare primitive values or
 * Strings, but needs to box them into BsonValues to populate {@link FieldDoc#fields}. It's
 * necessary for this conversion to be lossless and order-preserving so that GetMore's can pick up
 * where they left off and MongoS can correctly merge sorted results.
 */
public abstract class BsonConverter<T> {

  /**
   * The value used by the internal {@link FieldComparator} to represent a document with no value
   * for the sorted field.
   */
  private final @Nullable T luceneMissingValue;

  protected BsonConverter(@Nullable T luceneMissingValue) {
    this.luceneMissingValue = luceneMissingValue;
  }

  /**
   * Decodes a BsonValue into a comparable type T
   *
   * @param value - This value is guaranteed to have been produced by an earlier call to {@link
   *     #encodeToBson(T, BsonValue)}. It is either the current `bottom` value from a previous
   *     segment or a `top` value supplied by a GetMore or paginated query.
   * @param missingValueSortKey - The value that {@link #encodeToBson(T, BsonValue)} uses to
   *     represent a missing value. One of {MinKey, BsonNull, MaxKey}
   * @return A representation of `value` than can be understood by a {@link FieldComparator}
   */
  @Nullable
  final T decodeFromBson(BsonValue value, BsonValue missingValueSortKey) {
    // TODO(CLOUDP-266197): change this to only value.isNull() once mongos has completed
    // its work to support custom reordering of BsonNulls in SERVER-93244.
    if (missingValueSortKey.equals(value)) {
      return this.luceneMissingValue;
    }
    return decodeNonMissingValue(value);
  }

  /**
   * Encodes a comparable type T to its equivalent BsonValue
   *
   * @param value - This is a value produced by a {@link FieldComparator}. It must be converted into
   *     a BsonValue so that it can be merge-sorted by MongoS. We also need to ensure we can recover
   *     the original value for GetMores.
   * @param missingValueSortKey - The value that should be used to represent a missing or null
   *     value. One of {MinKey, BsonNull, MaxKey}
   * @return A representation of `value` that preserves its order in the space of BsonValues
   */
  final BsonValue encodeToBson(@Nullable T value, BsonValue missingValueSortKey) {
    if (value == null || value.equals(this.luceneMissingValue)) {
      return missingValueSortKey;
    }
    return encodeNonMissingValue(value);
  }

  /**
   * This method is only to be called internally by BsonConverter. It allows subclasses to define a
   * mapping without worrying about the representation of missing values.
   *
   * @param value - This value is guaranteed to have been produced by an earlier call to {@link
   *     #encodeToBson(T, BsonValue)}. It is either the current `bottom` value from a previous
   *     segment or a `top` value supplied by a GetMore or paginated query.
   * @return A representation of `value` than can be understood by a {@link FieldComparator}
   */
  protected abstract @Nullable T decodeNonMissingValue(BsonValue value);

  /**
   * This method is only to be called internally by BsonConverter. It allows subclasses define a
   * mapping without worrying about the representation of missing values.
   *
   * @param value - This is a value produced by a {@link FieldComparator}. It must be converted into
   *     a BsonValue so that it can be merge-sorted by MongoS. We also need to ensure we can recover
   *     the original value for GetMores.
   * @return A representation of `value` that preserves its order in the space of BsonValues
   */
  protected abstract BsonValue encodeNonMissingValue(T value);
}
