package com.xgen.mongot.index;

import static org.junit.Assert.assertEquals;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.junit.Test;

public class VectorliteIndexMetricsTest {

  @Test
  public void fromBson_readsTieredMetricsAndItemCount() {
    // The engine no longer reports warmCacheSizeBytes; it is derived as the sum
    // of the pinned and dynamic partitions (6 + 7).
    BsonDocument doc =
        new BsonDocument()
            .append("coldStorageSizeBytes", new BsonInt64(1))
            .append("pinnedWarmCacheSizeBytes", new BsonInt64(6))
            .append("dynamicWarmCacheSizeBytes", new BsonInt64(7))
            .append("warmQueriesCount", new BsonInt64(3))
            .append("coldQueriesCount", new BsonInt64(4))
            .append("totalItemCount", new BsonInt64(5));
    assertEquals(
        new VectorliteIndexMetrics(
            new VectorliteIndexMetrics.TieredStorageMetrics(1, 13, 6, 7, 3, 4), 5),
        VectorliteIndexMetrics.fromBson(doc));
  }

  @Test
  public void fromBson_absentKeysDefaultToZero() {
    assertEquals(VectorliteIndexMetrics.ZERO, VectorliteIndexMetrics.fromBson(new BsonDocument()));
  }

  @Test
  public void tieredStorageMetricsFromBson_readsAllKeys() {
    BsonDocument doc =
        new BsonDocument()
            .append("coldStorageSizeBytes", new BsonInt64(1))
            .append("pinnedWarmCacheSizeBytes", new BsonInt64(6))
            .append("dynamicWarmCacheSizeBytes", new BsonInt64(7))
            .append("warmQueriesCount", new BsonInt64(3))
            .append("coldQueriesCount", new BsonInt64(4));
    assertEquals(
        new VectorliteIndexMetrics.TieredStorageMetrics(1, 13, 6, 7, 3, 4),
        VectorliteIndexMetrics.TieredStorageMetrics.fromBson(doc));
  }

  @Test
  public void tieredStorageMetricsFromBson_absentKeysDefaultToZero() {
    assertEquals(
        VectorliteIndexMetrics.TieredStorageMetrics.ZERO,
        VectorliteIndexMetrics.TieredStorageMetrics.fromBson(new BsonDocument()));
  }

  @Test
  public void tieredStorageMetricsFromBson_ignoresTheDeprecatedWarmTotal() {
    // The deprecated warmCacheSizeBytes is ignored and the split is summed; with
    // no partitions present the derived warm total is zero.
    BsonDocument doc =
        new BsonDocument()
            .append("coldStorageSizeBytes", new BsonInt64(1))
            .append("warmCacheSizeBytes", new BsonInt64(2))
            .append("warmQueriesCount", new BsonInt64(3))
            .append("coldQueriesCount", new BsonInt64(4));
    assertEquals(
        new VectorliteIndexMetrics.TieredStorageMetrics(1, 0, 0, 0, 3, 4),
        VectorliteIndexMetrics.TieredStorageMetrics.fromBson(doc));
  }
}
