package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.junit.Test;

/**
 * Both halves of the min/max cross-shard null encoding: the wire substitution, and the projection
 * that undoes it after mongod {@code $min}/{@code $max}.
 */
public class MinMaxShardMergeTest {

  @Test
  public void encode_null_becomesMinKey() {
    assertEquals(new BsonMinKey(), MinMaxShardMerge.encode(BsonNull.VALUE));
  }

  @Test
  public void encode_nonNull_isUnchanged() {
    BsonInt64 value = new BsonInt64(5);
    assertSame(value, MinMaxShardMerge.encode(value));
  }

  @Test
  public void decodeProjection_replacesMinKeyWithNull() {
    assertEquals(
        BsonDocument.parse(
            """
            {"$cond": {
              "if": {"$eq": [{"$type": "$$b.min_m"}, "minKey"]},
              "then": null,
              "else": "$$b.min_m"
            }}
            """),
        MinMaxShardMerge.decodeProjection("$$b.min_m"));
  }
}
