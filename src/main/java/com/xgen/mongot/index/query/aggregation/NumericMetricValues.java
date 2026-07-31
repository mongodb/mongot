package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonValue;

/**
 * Decodes encoded {@link org.apache.lucene.index.SortedNumericDocValues} for aggregation metrics.
 *
 * <p>Search aggregation reads {@code NUMBER_*_V2} fields, which are indexed with {@code
 * IndexableFieldFactory.sortableNumericIndexedValue}. This class is the inverse of that encoding:
 * stored long doc-values are mapped back to the {@code double} values used in {@code sum}/{@code
 * avg} math.
 *
 * <p>{@link NumericFieldOptions.Representation#INT64 INT64}: doc values store the raw {@code long}.
 * Double {@code NaN} inputs are collapsed to {@code Long.MIN_VALUE} at index time (see {@code
 * IndexableFieldFactory.sortableNumericIndexedValue}) and cannot be distinguished from a true
 * {@code Long.MIN_VALUE} on decode. Decoding widens to {@code double}; magnitudes greater than 2^53
 * may lose integer precision in sum/avg, matching MQL {@code $avg} which aggregates in IEEE-754
 * double.
 *
 * <p>Follow-up: distinguish INT64 {@code NaN} from {@code Long.MIN_VALUE} at decode time without
 * re-encoding indexed values (CLOUDP ticket pending).
 *
 * <p>{@link NumericFieldOptions.Representation#DOUBLE DOUBLE}: doc values use the MQL sortable
 * double encoding ({@link LuceneDoubleConversionUtils#toMqlSortableLong} / {@link
 * LuceneDoubleConversionUtils#fromMqlSortableLong}); {@code NaN} round-trips through {@link
 * LuceneDoubleConversionUtils#NAN_SENTINEL}.
 */
final class NumericMetricValues {
  private NumericMetricValues() {}

  /**
   * Decode one stored doc-value long into the {@code double} used for metric aggregation.
   *
   * <p>Inverse of {@code IndexableFieldFactory.sortableNumericIndexedValue(...)} for {@code
   * NUMBER_*_V2} fields.
   */
  static double decode(long encoded, NumericFieldOptions.Representation representation) {
    return switch (representation) {
      case INT64 -> (double) encoded;
      case DOUBLE -> LuceneDoubleConversionUtils.fromMqlSortableLong(encoded);
    };
  }

  /**
   * Decode one stored doc-value long into BSON, preserving the field's representation.
   *
   * <p>Used by {@code min}/{@code max}, which return a value drawn from the data rather than a
   * computed one, so an {@code INT64} field yields a {@link BsonInt64} instead of widening to
   * {@code double} the way {@link #decode} does for sum/avg.
   */
  static BsonValue decodeToBson(long encoded, NumericFieldOptions.Representation representation) {
    return switch (representation) {
      case INT64 -> new BsonInt64(encoded);
      case DOUBLE -> new BsonDouble(LuceneDoubleConversionUtils.fromMqlSortableLong(encoded));
    };
  }
}
