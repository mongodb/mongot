package com.xgen.mongot.index.query.aggregation;

import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * The cross-shard contract for {@code min} and {@code max}: how a shard writes a BSON-null extremum
 * onto the wire, and how the merge pipeline turns that encoding back into null.
 *
 * <p>MongoDB {@code $min}/{@code $max} ignore BSON null (and missing). That is correct for a
 * missing field -- those partials are omitted -- but a real null is a BSON type below every number,
 * string, and so on. Sending it as null would let a shard whose min is 5 beat a shard whose min is
 * null. {@link BsonMinKey} is below every value those accumulators compare, and is not a type
 * min/max can collect, so it is a safe stand-in. The projection undoes that substitution after the
 * merge.
 */
final class MinMaxShardMerge {

  private MinMaxShardMerge() {}

  /**
   * Wire form of one shard's extremum. Null becomes MinKey; every other value is sent as-is.
   * Missing extrema never reach here -- the result omits the partial entirely.
   */
  static BsonValue encode(BsonValue value) {
    return value.isNull() ? new BsonMinKey() : value;
  }

  /**
   * User-facing metric value after {@code $min}/{@code $max} over the encoded partials. MinKey is
   * the null stand-in; anything else -- including a true merged null, which is how {@code $min}
   * over zero inputs reports "no value" -- is passed through.
   */
  static BsonDocument decodeProjection(String fieldRef) {
    return new BsonDocument(
        "$cond",
        new BsonDocument()
            .append(
                "if",
                new BsonDocument(
                    "$eq",
                    new BsonArray(
                        List.of(
                            new BsonDocument("$type", new BsonString(fieldRef)),
                            new BsonString("minKey")))))
            .append("then", BsonNull.VALUE)
            .append("else", new BsonString(fieldRef)));
  }

  static void buildShardMergePipeline(
      ShardMergePipelineBuilder builder, Operator operator, String name) {
    String partial =
        switch (operator) {
          case MIN -> MinAggregationResult.partialFieldName(name);
          case MAX -> MaxAggregationResult.partialFieldName(name);
          default ->
              throw new IllegalArgumentException(
                  "min/max shard merge does not support " + operator);
        };
    String mergeOp = operator == Operator.MIN ? "$min" : "$max";
    builder.addMetricAccumulator(partial, new BsonDocument(mergeOp, new BsonString("$" + partial)));
    builder.addMetricProjection(name, decodeProjection("$$b." + partial));
  }
}
