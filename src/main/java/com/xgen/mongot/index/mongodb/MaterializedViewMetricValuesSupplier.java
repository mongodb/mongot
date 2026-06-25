package com.xgen.mongot.index.mongodb;

import com.google.common.base.Suppliers;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.index.DocCounts;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.Check;
import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Supplies metric values for materialized view indexes. Unlike Lucene indexes, materialized views
 * store data in MongoDB collections, so metrics are sourced from MongoDB collection stats.
 */
public class MaterializedViewMetricValuesSupplier implements IndexMetricValuesSupplier {

  private static final Logger LOG =
      LoggerFactory.getLogger(MaterializedViewMetricValuesSupplier.class);

  // Cache duration for mongodb queries
  private static final Duration CACHE_DURATION = Duration.ofMinutes(1);

  private final Supplier<IndexStatus> indexStatusSupplier;
  private final AutoEmbeddingMongoClient autoEmbeddingMongoClient;
  private final MongoNamespace namespace;
  private final Supplier<CollectionStats> cachedStatsSupplier;
  private volatile CollectionStats lastKnownGoodStats = new CollectionStats(0, 0);

  /**
   * Creates a MaterializedViewMetricValuesSupplier.
   *
   * @param indexStatusSupplier supplier for the current index status
   * @param autoEmbeddingMongoClient MongoDB client for querying collection stats
   * @param namespace the namespace (database.collection) of the materialized view
   */
  public MaterializedViewMetricValuesSupplier(
      Supplier<IndexStatus> indexStatusSupplier,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MongoNamespace namespace) {
    this.indexStatusSupplier = indexStatusSupplier;
    this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
    this.namespace = namespace;
    this.cachedStatsSupplier =
        Suppliers.memoizeWithExpiration(this::fetchCollectionStats, CACHE_DURATION);
  }

  @Override
  public long getCachedIndexSize() {
    return this.lastKnownGoodStats.storageSize();
  }

  @Override
  public int getNumFields() {
    // Not applicable for MongoDB collections - this is a Lucene-specific metric
    return 0;
  }

  @Override
  public Map<FieldName.TypeField, Double> getNumFieldsPerDatatype() {
    // Not applicable for MongoDB collections - this is a Lucene-specific metric
    return Map.of();
  }

  @Override
  public DocCounts getDocCounts() {
    long docCount = this.cachedStatsSupplier.get().docCount;
    // For materialized views, all doc count metrics should be the same
    // numDocs = numLuceneMaxDocs = maxLuceneMaxDocs = numMongoDbDocs
    return new DocCounts(docCount, docCount, (int) Math.min(docCount, Integer.MAX_VALUE), docCount);
  }

  @Override
  public IndexStatus getIndexStatus() {
    return this.indexStatusSupplier.get();
  }

  @Override
  public void close() {}

  /** Fetches collection stats from MongoDB using the collStats command */
  private CollectionStats fetchCollectionStats() {
    try {
      // It's Ok to have some transient error when syncsource is missing or being updated, which may
      // cause some client be closed earlier.
      var mongoClient =
          Check.isPresent(
              this.autoEmbeddingMongoClient.getMaterializedViewResolverMongoClient(),
              "materializedViewCollectionMongoClient");
      Document stats =
          mongoClient
              .getDatabase(this.namespace.getDatabaseName())
              .runCommand(new Document("collStats", this.namespace.getCollectionName()));
      long storageSize =
          stats.containsKey("storageSize") ? stats.get("storageSize", Number.class).longValue() : 0;
      long docCount = stats.containsKey("count") ? stats.get("count", Number.class).longValue() : 0;
      this.lastKnownGoodStats = new CollectionStats(storageSize, docCount);
      return this.lastKnownGoodStats;
    } catch (Exception | AssertionError e) {
      LOG.warn(
          "Failed to get collection stats for {}, returning last known value", this.namespace, e);
      return this.lastKnownGoodStats;
    }
  }

  // storage and count related stats for the collection
  private record CollectionStats(long storageSize, long docCount) {}
}
