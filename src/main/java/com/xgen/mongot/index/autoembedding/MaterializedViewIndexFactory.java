package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.util.mongodb.MongoDbDatabase.getCollectionInfos;

import com.mongodb.client.MongoClient;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.MaterializedViewCollectionResolver;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.common.InternalDatabaseResolver;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.mongodb.MaterializedViewMetricValuesSupplier;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Materialized View Index factory generates Mat View index type that manages states and checkpoints
 * for Materialized View collections.
 */
public class MaterializedViewIndexFactory implements IndexFactory {
  public static final String NAMESPACE = "embedding.materializedView.stats";
  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewIndexFactory.class);
  private final MeterAndFtdcRegistry meterAndFtdcRegistry;
  private final MetricsFactory metricsFactory;
  private final FeatureFlags featureFlags;
  private final MaterializedViewWriter.Factory materializedViewWriterFactory;
  private final LeaseManager leaseManager;
  private final MaterializedViewCollectionResolver collectionResolver;
  private final AutoEmbeddingMongoClient autoEmbeddingMongoClient;
  private final InternalDatabaseResolver dbResolver;

  public MaterializedViewIndexFactory(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      FeatureFlags featureFlags,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      LeaseManager leaseManager,
      MaterializedViewCollectionResolver collectionResolver,
      InternalDatabaseResolver dbResolver,
      Optional<Integer> mvWriteRateLimitRps) {
    this.meterAndFtdcRegistry = meterAndFtdcRegistry;
    this.metricsFactory = new MetricsFactory(NAMESPACE, meterAndFtdcRegistry.meterRegistry());
    this.featureFlags = featureFlags;
    this.materializedViewWriterFactory =
        new MaterializedViewWriter.Factory(
            autoEmbeddingMongoClient, meterAndFtdcRegistry.meterRegistry(), mvWriteRateLimitRps);
    this.leaseManager = leaseManager;
    this.collectionResolver = collectionResolver;
    this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
    this.dbResolver = dbResolver;
  }

  /**
   * Returns {@code true} only when the auto-embedding source collection is confirmed ABSENT on this
   * node's local mongod -- e.g. an unsharded collection on a non-owner shard. Returns {@code false}
   * when the collection is present, or when presence cannot be determined right now (resolver
   * client not ready, or a transient query failure) -- those fall through to the normal resolution
   * / transient-retry path. A missing or unreachable client makes getIndex itself fail to a
   * recoverable FAILED/RETRY, so there is no MV/lease to leak in that case.
   *
   * <p>Used to gate materialized-view setup: when the source collection is confirmed not on this
   * shard, the caller must not create the MV collection, lease, or {@code leaderStatus} gauge, and
   * reports a quiet DOES_NOT_EXIST / COLLECTION_NOT_FOUND. The config manager's recovery loop
   * re-stages the index once the collection appears on this shard.
   */
  public boolean isSourceCollectionAbsentOnMongod(IndexDefinition indexDefinition) {
    return isSourceCollectionAbsentOnMongod(
        this.autoEmbeddingMongoClient.getMaterializedViewResolverMongoClient(), indexDefinition);
  }

  // Package-private for unit testing: absence decision given an already-resolved client.
  boolean isSourceCollectionAbsentOnMongod(
      Optional<MongoClient> resolverClient, IndexDefinition indexDefinition) {
    // Only declare the source collection absent when we can positively confirm it: the resolver
    // client is available AND a successful listing shows the UUID is not present. On uncertainty --
    // client not ready, or a transient query failure -- return false and let the normal resolution
    // path handle it. A missing/unreachable client makes getIndex itself fail to a recoverable
    // FAILED/RETRY (no MV/lease created), which the config manager already recovers from on the
    // next update cycle -- so there is nothing to leak and nothing to fail safe against here.
    if (resolverClient.isEmpty()) {
      return false;
    }
    try {
      Set<UUID> collectionUuidsOnShard =
          collectionUuidsOnShard(resolverClient.get(), indexDefinition.getDatabase());
      return !collectionUuidsOnShard.contains(indexDefinition.getCollectionUuid());
    } catch (CheckedMongoException e) {
      LOG.atWarn()
          .setCause(e)
          .addKeyValue("database", indexDefinition.getDatabase())
          .addKeyValue("collectionUuid", indexDefinition.getCollectionUuid())
          .log("Could not verify source collection presence on direct mongod; proceeding");
      return false;
    }
  }

  // Package-private for unit testing: source collection UUIDs present on this node's local mongod.
  Set<UUID> collectionUuidsOnShard(MongoClient resolverClient, String database)
      throws CheckedMongoException {
    return getCollectionInfos(resolverClient, database).getAllCollectionUuids();
  }

  /** Must be called after all associated indexes are closed. */
  @Override
  public void close() {
    LOG.info("Shutting down.");
    this.metricsFactory.close();
    this.materializedViewWriterFactory.close();
  }

  @Override
  public InitializedMaterializedViewIndex getIndex(
      IndexDefinitionGeneration indexDefinitionGeneration)
      throws InvalidAnalyzerDefinitionException, IOException, MaterializedViewTransientException {
    Check.expectedType(
        IndexDefinitionGeneration.Type.AUTO_EMBEDDING, indexDefinitionGeneration.getType());
    MaterializedViewIndexDefinitionGeneration matViewIndexDefinitionGeneration =
        indexDefinitionGeneration.asMaterializedView();
    // May throw MaterializedViewTransientException which will be retried by the caller.
    MaterializedViewCollectionMetadata collectionMetadata =
        this.collectionResolver.getOrCreateMaterializedViewForIndex(
            matViewIndexDefinitionGeneration);

    String matViewDatabaseName =
        this.dbResolver.resolve(
            matViewIndexDefinitionGeneration.getIndexDefinition().getDatabase());
    MaterializedViewWriter writer =
        this.materializedViewWriterFactory.create(
            matViewDatabaseName,
            collectionMetadata.collectionName(),
            matViewIndexDefinitionGeneration.getGenerationId(),
            this.leaseManager,
            collectionMetadata.collectionUuid());

    // Shared status reference for supplier and index
    AtomicReference<IndexStatus> statusRef = new AtomicReference<>(IndexStatus.unknown());

    // Metric values supplier with writer mongo client and namespace
    MaterializedViewMetricValuesSupplier metricValuesSupplier =
        new MaterializedViewMetricValuesSupplier(
            statusRef::get, writer.getMongoClient(), writer.getNamespace());

    // Avoid metrics collision with Lucene index by setting different NAMESPACE.
    // Use the collection name (lease key, indexIdHex-hash-version) as indexId_logString so that
    // metrics align with lease id and the tag is easier to use when debugging.
    var indexMetricsUpdaterBuilder =
        new IndexMetricsUpdater.Builder(
            matViewIndexDefinitionGeneration.getIndexDefinition(),
            new PerIndexMetricsFactory(
                NAMESPACE,
                this.meterAndFtdcRegistry,
                matViewIndexDefinitionGeneration.getGenerationId().uniqueString(),
                collectionMetadata.collectionName()),
            this.featureFlags.isEnabled(Feature.INDEX_FEATURE_VERSION_FOUR));

    return new InitializedMaterializedViewIndex(
        matViewIndexDefinitionGeneration,
        writer,
        indexMetricsUpdaterBuilder.build(metricValuesSupplier),
        statusRef,
        this.leaseManager,
        collectionMetadata.schemaMetadata(),
        matViewDatabaseName);
  }

  @Override
  public InitializedMaterializedViewIndex getInitializedIndex(
      Index index, IndexDefinitionGeneration definitionGeneration) throws IOException {
    return Check.instanceOf(index, InitializedMaterializedViewIndex.class);
  }

  public void updateSyncSource(SyncSourceConfig syncSourceConfig) {
    this.autoEmbeddingMongoClient.updateSyncSource(syncSourceConfig);
  }
}
