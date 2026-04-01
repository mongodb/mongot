package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;

import com.google.common.base.Supplier;
import com.xgen.mongot.embedding.AutoEmbeddingMemoryBudget;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.util.Runtime;
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
    log.info(
        "Creating IndexingWorkSchedulerFactory with DEFAULT, CUSTOM_VECTOR_ENGINE and "
            + "EMBEDDING strategies");
    var executor = Executors.fixedSizeThreadPool("indexing-work", numIndexingThreads, registry);
    DefaultIndexingWorkScheduler defaultIndexingWorkScheduler =
        DefaultIndexingWorkScheduler.create(executor);
    CustomVectorEngineIndexingWorkScheduler customVectorEngineIndexingWorkScheduler =
        CustomVectorEngineIndexingWorkScheduler.create(executor);
    EmbeddingIndexingWorkScheduler embeddingIndexingWorkScheduler =
        EmbeddingIndexingWorkScheduler.create(executor, embeddingServiceManagerSupplier);
    return new IndexingWorkSchedulerFactory(
        Map.of(
            IndexingStrategy.DEFAULT, defaultIndexingWorkScheduler,
            IndexingStrategy.CUSTOM_VECTOR_ENGINE, customVectorEngineIndexingWorkScheduler,
            IndexingStrategy.EMBEDDING, embeddingIndexingWorkScheduler));
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
    CustomVectorEngineIndexingWorkScheduler customVectorEngineIndexingWorkScheduler =
        CustomVectorEngineIndexingWorkScheduler.create(executor);
    return new IndexingWorkSchedulerFactory(
        Map.of(
            IndexingStrategy.DEFAULT, defaultIndexingWorkScheduler,
            IndexingStrategy.CUSTOM_VECTOR_ENGINE, customVectorEngineIndexingWorkScheduler));
  }

  /**
   * Creates a new IndexingWorkSchedulerFactory with EmbeddingIndexingScheduler for the
   * MaterializedView index only. Used by MaterializedViewManager.
   *
   * @param globalMemoryBudgetHeapPercent percentage of JVM max heap for the global embedding memory
   *     budget (1–100; 100 disables the limit)
   * @param perBatchMemoryBudgetHeapPercent percentage of JVM max heap for the per-batch embedding
   *     memory budget (1–100; 100 disables the limit)
   */
  public static IndexingWorkSchedulerFactory createEmbeddingIndexingSchedulerOnly(
      int numIndexingThreads,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      MaterializedViewCollectionMetadataCatalog matViewCollectionMetadataCatalog,
      MeterRegistry registry,
      int globalMemoryBudgetHeapPercent,
      int perBatchMemoryBudgetHeapPercent) {
    log.info("Creating IndexingWorkSchedulerFactory with EmbeddingIndexingWorkScheduler only");
    var executor =
        Executors.fixedSizeThreadPool("indexing-auto-embedding", numIndexingThreads, registry);
    // A single global budget is shared across all embedding schedulers so that the limit is
    // enforced at the mongot level across all indexes.
    var globalBudget =
        AutoEmbeddingMemoryBudget.fromHeapPercent(globalMemoryBudgetHeapPercent, Runtime.INSTANCE);
    long perBatchBudgetBytes =
        perBatchMemoryBudgetHeapPercent >= 100
            ? Long.MAX_VALUE
            : Runtime.INSTANCE.getMaxHeapSize().toBytes() * perBatchMemoryBudgetHeapPercent / 100;
    EmbeddingIndexingWorkScheduler embeddingIndexingWorkScheduler =
        EmbeddingIndexingWorkScheduler.createForMaterializedViewIndex(
            executor,
            embeddingServiceManagerSupplier,
            matViewCollectionMetadataCatalog,
            globalBudget,
            perBatchBudgetBytes);
    return new IndexingWorkSchedulerFactory(
        Map.of(
            IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW,
            embeddingIndexingWorkScheduler,
            IndexingStrategy.EMBEDDING,
            embeddingIndexingWorkScheduler,
            IndexingStrategy.CUSTOM_VECTOR_ENGINE,
            embeddingIndexingWorkScheduler,
            IndexingStrategy.DEFAULT,
            embeddingIndexingWorkScheduler));
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
      if (indexDefinition.getParsedAutoEmbeddingFeatureVersion()
          >= MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING) {
        return this.getIndexingWorkSchedulers().get(IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW);
      }
      return this.getIndexingWorkSchedulers().get(IndexingStrategy.EMBEDDING);
    }
    if (indexDefinition instanceof VectorIndexDefinition vectorDef
        && vectorDef.isCustomVectorEngineIndex()) {
      return this.getIndexingWorkSchedulers().get(IndexingStrategy.CUSTOM_VECTOR_ENGINE);
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
   *   <li>CUSTOM_VECTOR_ENGINE: Uses the {@link CustomVectorEngineIndexingWorkScheduler} to perform
   *       batch preparation (e.g. vectorlite ID resolution) before indexing a batch. Used for
   *       vector indexes with a custom vector engine.
   *   <li>EMBEDDING, EMBEDDING_MATERIALIZED_VIEW: Uses the {@link EmbeddingIndexingWorkScheduler}
   *       to generate embeddings for vector text fields before indexing a batch.
   * </ul>
   */
  public enum IndexingStrategy {
    DEFAULT("DefaultIndexingWorkSchedulerThread"),
    CUSTOM_VECTOR_ENGINE("CustomVectorEngineIndexingWorkSchedulerThread"),
    // TODO(CLOUDP-344117): Remove IndexingStrategy.EMBEDDING once we deprecate type:text index.
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
