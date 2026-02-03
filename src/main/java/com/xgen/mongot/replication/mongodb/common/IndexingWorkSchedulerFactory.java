package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;

import com.google.common.base.Supplier;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.util.concurrent.Executors;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The IndexingWorkSchedulerFactory is used to retrieve the appropriate {@link
 * IndexingWorkScheduler} to be used for each {@link IndexingStrategy}.
 */
public class IndexingWorkSchedulerFactory {

  private static final Logger log = LoggerFactory.getLogger(IndexingWorkSchedulerFactory.class);
  private final Map<IndexingStrategy, IndexingWorkScheduler> indexingWorkSchedulers;

  private IndexingWorkSchedulerFactory(
      Map<IndexingStrategy, IndexingWorkScheduler> indexingWorkSchedulers) {
    if (!indexingWorkSchedulers.containsKey(IndexingStrategy.DEFAULT)) {
      throw new IllegalArgumentException("DEFAULT indexing strategy is required.");
    }
    this.indexingWorkSchedulers = indexingWorkSchedulers;
  }

  public Map<IndexingStrategy, IndexingWorkScheduler> getIndexingWorkSchedulers() {
    return this.indexingWorkSchedulers;
  }

  /**
   * Creates a new IndexingWorkSchedulerFactory with a work scheduler for each supported strategy,
   * including the EMBEDDING strategy.
   *
   * @return an IndexingWorkSchedulerFactory.
   */
  public static IndexingWorkSchedulerFactory create(
      int numIndexingThreads,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      MeterRegistry registry) {
    log.info("Creating IndexingWorkSchedulerFactory with DEFAULT, EMBEDDING and "
        + "EMBEDDING_MATERIALIZED_VIEW strategies");
    var executor = Executors.fixedSizeThreadPool("indexing-work", numIndexingThreads, registry);
    DefaultIndexingWorkScheduler defaultIndexingWorkScheduler =
        DefaultIndexingWorkScheduler.create(executor);
    EmbeddingIndexingWorkScheduler embeddingIndexingWorkScheduler =
        EmbeddingIndexingWorkScheduler.create(executor, embeddingServiceManagerSupplier);
    EmbeddingIndexingWorkScheduler embeddingMaterializedViewWorkScheduler =
        EmbeddingIndexingWorkScheduler.createForMaterializedViewIndex(
            executor, embeddingServiceManagerSupplier);
    return new IndexingWorkSchedulerFactory(
        Map.of(
            IndexingStrategy.DEFAULT, defaultIndexingWorkScheduler,
            IndexingStrategy.EMBEDDING, embeddingIndexingWorkScheduler,
            IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW, embeddingMaterializedViewWorkScheduler));
  }

  /**
   * Creates a new IndexingWorkSchedulerFactory without a work scheduler for the EMBEDDING strategy.
   * This is used for Community.
   */
  public static IndexingWorkSchedulerFactory createWithoutEmbeddingStrategy(
      int numIndexingThreads, MeterRegistry registry) {
    log.info("Creating IndexingWorkSchedulerFactory without EMBEDDING strategy");
    var executor = Executors.fixedSizeThreadPool("indexing", numIndexingThreads, registry);
    DefaultIndexingWorkScheduler defaultIndexingWorkScheduler =
        DefaultIndexingWorkScheduler.create(executor);
    return new IndexingWorkSchedulerFactory(
        Map.of(IndexingStrategy.DEFAULT, defaultIndexingWorkScheduler));
  }

  /**
   * Returns the {@link IndexingWorkScheduler} for the given {@link IndexDefinition}.
   *
   * @param indexDefinition the index definition that determines which {@link IndexingWorkScheduler}
   *     to use. Auto-embedding indexes will use the EMBEDDING strategy if available.
   * @return the {@link IndexingWorkScheduler} to use for the given index definition
   */
  public IndexingWorkScheduler getIndexingWorkScheduler(IndexDefinition indexDefinition) {
    if (indexDefinition.isAutoEmbeddingIndex()) {
      if (!this.getIndexingWorkSchedulers().containsKey(IndexingStrategy.EMBEDDING)) {
        throw new IllegalStateException("Auto-embedding vector search indexes are not supported.");
      }
      if (indexDefinition.asVectorDefinition().getParsedAutoEmbeddingFeatureVersion()
          >= MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING) {
        return this.getIndexingWorkSchedulers().get(IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW);
      }
      return this.getIndexingWorkSchedulers().get(IndexingStrategy.EMBEDDING);
    }
    return this.getIndexingWorkSchedulers().get(IndexingStrategy.DEFAULT);
  }

  /**
   * Each indexing strategy must correspond to a {@link IndexingWorkScheduler} in the
   * indexingWorkSchedulers map. When adding a new indexing strategy, ensure that the corresponding
   * {@link IndexingWorkScheduler} is added in {@link #create(int, Supplier, MeterRegistry)}.
   *
   * <ul>
   *   <li>DEFAULT: Uses the {@link DefaultIndexingWorkScheduler} to exclusively index a batch.
   *   <li>EMBEDDING, EMBEDDING_MATERIALIZED_VIEW: Uses the {@link EmbeddingIndexingWorkScheduler}
   *       to generate embeddings for vector text fields before indexing a batch.
   * </ul>
   */
  public enum IndexingStrategy {
    DEFAULT("DefaultIndexingWorkSchedulerThread"),
    EMBEDDING("EmbeddingIndexingWorkSchedulerThread"),
    EMBEDDING_MATERIALIZED_VIEW("EmbeddingMaterializedViewIndexingWorkSchedulerThread");

    private final String threadName;

    IndexingStrategy(String threadName) {
      this.threadName = threadName;
    }

    public String getThreadName() {
      return this.threadName;
    }
  }
}
