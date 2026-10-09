package com.xgen.mongot.index;

import static org.junit.Assert.assertEquals;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.junit.Test;

public class TieredStorageMetricsTest {

  @Test
  public void fromBson_readsAllFourKeys() {
    BsonDocument doc =
        new BsonDocument()
            .append("coldStorageSizeBytes", new BsonInt64(1))
            .append("warmCacheSizeBytes", new BsonInt64(2))
            .append("warmQueriesCount", new BsonInt64(3))
            .append("coldQueriesCount", new BsonInt64(4));
    assertEquals(new TieredStorageMetrics(1, 2, 3, 4), TieredStorageMetrics.fromBson(doc));
  }

  @Test
  public void fromBson_absentKeysDefaultToZero() {
    assertEquals(TieredStorageMetrics.ZERO, TieredStorageMetrics.fromBson(new BsonDocument()));
  }
}
