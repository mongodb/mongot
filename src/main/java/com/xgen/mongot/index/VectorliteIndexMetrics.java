package com.xgen.mongot.index;

import org.bson.BsonDocument;
import org.bson.BsonInt64;

/**
 * The metrics the Vectorlite engine reports for one index: its {@link TieredStorageMetrics} plus
 * the index-level live item count.
 *
 * @param tieredStorage the index's tiered storage metrics (cold/warm bytes and query counts)
 * @param totalItemCount live indexed items (vectors/documents) in the index; 0 until the engine
 *     reports it
 */
public record VectorliteIndexMetrics(TieredStorageMetrics tieredStorage, long totalItemCount) {

  public static final VectorliteIndexMetrics ZERO =
      new VectorliteIndexMetrics(TieredStorageMetrics.ZERO, 0);

  public static final String TOTAL_ITEM_COUNT = "totalItemCount";

  /** Reads the tiered storage metrics and the item count from the engine's metrics document. */
  public static VectorliteIndexMetrics fromBson(BsonDocument doc) {
    return new VectorliteIndexMetrics(
        TieredStorageMetrics.fromBson(doc),
        doc.getInt64(TOTAL_ITEM_COUNT, new BsonInt64(0)).getValue());
  }

  /**
   * Tiered storage metrics for an index.
   *
   * <p>{@code warmQueriesCount + coldQueriesCount} is not a partition of {@code totalQueriesCount}:
   * the tiered counters count searches that reached the vector engine, including ones that then
   * failed, while the total counts attempts.
   *
   * @param coldStorageSizeBytes bytes of index data held in cold storage; 0 until the engine
   *     reports it
   * @param warmCacheSizeBytes bytes of index data held in the local warm cache; 0 until the engine
   *     reports it
   * @param warmQueriesCount searches admitted against a warm tenant cache
   * @param coldQueriesCount searches not served warm: a warming, untracked or tenant-less index, or
   *     a failure before any read
   */
  public record TieredStorageMetrics(
      long coldStorageSizeBytes,
      long warmCacheSizeBytes,
      long warmQueriesCount,
      long coldQueriesCount) {

    public static final TieredStorageMetrics ZERO = new TieredStorageMetrics(0, 0, 0, 0);

    public static final String COLD_STORAGE_SIZE_BYTES = "coldStorageSizeBytes";
    public static final String WARM_CACHE_SIZE_BYTES = "warmCacheSizeBytes";
    public static final String WARM_QUERIES_COUNT = "warmQueriesCount";
    public static final String COLD_QUERIES_COUNT = "coldQueriesCount";

    /** Reads each metric as an int64, defaulting to 0 when absent. */
    public static TieredStorageMetrics fromBson(BsonDocument doc) {
      return new TieredStorageMetrics(
          readLong(doc, COLD_STORAGE_SIZE_BYTES),
          readLong(doc, WARM_CACHE_SIZE_BYTES),
          readLong(doc, WARM_QUERIES_COUNT),
          readLong(doc, COLD_QUERIES_COUNT));
    }

    private static long readLong(BsonDocument doc, String key) {
      return doc.getInt64(key, new BsonInt64(0)).getValue();
    }
  }
}
