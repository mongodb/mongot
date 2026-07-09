package com.xgen.mongot.index.query;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;

/** Covers the per-query {@code concurrent} knob on {@code $vectorSearch}. */
public class VectorSearchConcurrentKnobTest {

  private static BsonDocument baseQuery() {
    BsonArray queryVector = new BsonArray();
    queryVector.add(new BsonDouble(1.0));
    queryVector.add(new BsonDouble(2.0));
    queryVector.add(new BsonDouble(3.0));
    return new BsonDocument()
        .append("index", new BsonString("myVectorIndex"))
        .append("path", new BsonString("description"))
        .append("queryVector", queryVector)
        .append("numCandidates", new BsonInt32(10))
        .append("limit", new BsonInt32(5));
  }

  @Test
  public void absentConcurrent_defaultsToTrue_andIsNotSerialized() throws BsonParseException {
    VectorSearchQuery query = VectorSearchQuery.fromBson(baseQuery());

    assertThat(query.concurrentOverride()).isEmpty();
    assertThat(query.concurrent()).isTrue();
    assertThat(query.toBson().containsKey("concurrent")).isFalse();
  }

  @Test
  public void concurrentFalse_isRespected_andRoundTrips() throws BsonParseException {
    VectorSearchQuery query =
        VectorSearchQuery.fromBson(baseQuery().append("concurrent", BsonBoolean.FALSE));

    assertThat(query.concurrentOverride()).hasValue(false);
    assertThat(query.concurrent()).isFalse();

    BsonDocument serialized = query.toBson();
    assertThat(serialized.getBoolean("concurrent").getValue()).isFalse();
    assertThat(VectorSearchQuery.fromBson(serialized).concurrent()).isFalse();
  }

  @Test
  public void concurrentTrue_isRespected_andRoundTrips() throws BsonParseException {
    VectorSearchQuery query =
        VectorSearchQuery.fromBson(baseQuery().append("concurrent", BsonBoolean.TRUE));

    assertThat(query.concurrentOverride()).hasValue(true);
    assertThat(query.concurrent()).isTrue();

    BsonDocument serialized = query.toBson();
    assertThat(serialized.getBoolean("concurrent").getValue()).isTrue();
    assertThat(VectorSearchQuery.fromBson(serialized).concurrent()).isTrue();
  }
}
