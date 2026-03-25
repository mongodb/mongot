package com.xgen.mongot.index.autoembedding;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.MaterializedViewCollectionResolver;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.mongodb.MaterializedViewMetricValuesSupplier;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.io.IOException;
import java.util.Optional;
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

  public MaterializedViewIndexFactory(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      FeatureFlags featureFlags,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      LeaseManager leaseManager,
      MaterializedViewCollectionResolver collectionResolver,
      Optional<Integer> mvWriteRateLimitRps,
      int matViewWriterMaxConnections) {
    this.meterAndFtdcRegistry = meterAndFtdcRegistry;
    this.metricsFactory = new MetricsFactory(NAMESPACE, meterAndFtdcRegistry.meterRegistry());
    this.featureFlags = featureFlags;
    this.materializedViewWriterFactory =
        new MaterializedViewWriter.Factory(
            autoEmbeddingMongoClient, meterAndFtdcRegistry.meterRegistry(), mvWriteRateLimitRps);
    this.leaseManager = leaseManager;
    this.collectionResolver = collectionResolver;
    this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
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

    MaterializedViewWriter writer =
        this.materializedViewWriterFactory.create(
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
    // TODO(CLOUDP-353553): Handle search index version - getIndexDefinition() now returns
    //  IndexDefinition which may be a SearchIndexDefinition.
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
        collectionMetadata.schemaMetadata());
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
