package com.xgen.mongot.index;

import static org.junit.Assert.assertEquals;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.junit.Test;

public class VectorliteIndexMetricsTest {

  @Test
  public void fromBson_readsTieredMetricsAndItemCount() {
    BsonDocument doc =
        new BsonDocument()
            .append("coldStorageSizeBytes", new BsonInt64(1))
            .append("warmCacheSizeBytes", new BsonInt64(2))
            .append("warmQueriesCount", new BsonInt64(3))
            .append("coldQueriesCount", new BsonInt64(4))
            .append("totalItemCount", new BsonInt64(5));
    assertEquals(
        new VectorliteIndexMetrics(new VectorliteIndexMetrics.TieredStorageMetrics(1, 2, 3, 4), 5),
        VectorliteIndexMetrics.fromBson(doc));
  }

  @Test
  public void fromBson_absentKeysDefaultToZero() {
    assertEquals(VectorliteIndexMetrics.ZERO, VectorliteIndexMetrics.fromBson(new BsonDocument()));
  }

  @Test
  public void tieredStorageMetricsFromBson_readsAllFourKeys() {
    BsonDocument doc =
        new BsonDocument()
            .append("coldStorageSizeBytes", new BsonInt64(1))
            .append("warmCacheSizeBytes", new BsonInt64(2))
            .append("warmQueriesCount", new BsonInt64(3))
            .append("coldQueriesCount", new BsonInt64(4));
    assertEquals(
        new VectorliteIndexMetrics.TieredStorageMetrics(1, 2, 3, 4),
        VectorliteIndexMetrics.TieredStorageMetrics.fromBson(doc));
  }

  @Test
  public void tieredStorageMetricsFromBson_absentKeysDefaultToZero() {
    assertEquals(
        VectorliteIndexMetrics.TieredStorageMetrics.ZERO,
        VectorliteIndexMetrics.TieredStorageMetrics.fromBson(new BsonDocument()));
  }
}
