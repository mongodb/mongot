package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ServiceTier;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildAutoEmbeddingDocumentEvent;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildMaterializedViewDocumentEvent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping.AutoEmbedField;
import com.xgen.mongot.embedding.AutoEmbeddingMemoryBudget;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.cache.EmbeddingCacheKey;
import com.xgen.mongot.embedding.cache.EmbeddingRetryCache;
import com.xgen.mongot.embedding.cache.EmbeddingRetryCache.ReleaseTrigger;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator;
import com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils;
import com.xgen.mongot.embedding.utils.MaterializedViewArrayElementType;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.IndexTypeData.IndexTypeTag;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorFieldAutoEmbeddingSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory.IndexingStrategy;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.Priority;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.LoggableIdUtils;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import net.jodah.failsafe.FailsafeException;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.TestOnly;

/**
 * The embedding indexing work scheduler accepts embedding and indexing work and schedules it to be
 * completed on an executor. Calls the embedding provider to generate embeddings for each batch and
 * indexes the document with its fields embedded.
 *
 * <p>This scheduler should only be used for indexes that use auto-embedding (have vector text
 * embedding fields).
 *
 * <p>Two memory budgets are enforced:
 *
 * <ul>
 *   <li><b>Global budget</b> ({@link AutoEmbeddingMemoryBudget}): a mongot-level limit shared
 *       across all indexes. If the budget is exceeded when a batch starts, the batch is fast-failed
 *       with a transient exception. Controlled by {@code
 *       AutoEmbeddingMaterializedViewConfig#globalMemoryBudgetHeapPercent}.
 *   <li><b>Per-batch budget</b> (field {@code perBatchBudgetBytes}): limits how much embedding
 *       memory a single materialized-view batch holds at once. When active, the batch is divided
 *       into sub-batches that are embedded and flushed sequentially, so the next sub-batch's
 *       embedding call is only issued after the previous sub-batch is committed to MongoDB.
 *       Controlled by {@code AutoEmbeddingMaterializedViewConfig#perBatchMemoryBudgetHeapPercent}.
 *       The constant {@link #PER_BATCH_AUTO_EMBEDDING_MEMORY_BUDGET_BYTES} is only used by the
 *       legacy {@code EMBEDDING} strategy (type:text indexes), not by this configurable path.
 * </ul>
 *
 * <p>The global budget defaults to 100% (unbounded). The per-batch budget defaults to 50% of JVM
 * heap. Setting either to 100% disables it.
 */
final class EmbeddingIndexingWorkScheduler extends IndexingWorkScheduler {

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  // Max number of auto embed documents for an indexing bundle within the same
  // IndexingSchedulerBatch, greater number means more vectors to hold in memory before indexing
  private static final int MAX_AUTO_EMBED_DOCUMENT_BUNDLE_SIZE = 1000;

  /**
   * The full-build duplicated-array-element counters sample one document in this many and scale
   * each hit by the same factor. This is exposure telemetry for the pre-CLOUDP-459586 writer bug,
   * not a correctness signal, so the estimate is sufficient.
   */
  @VisibleForTesting static final int DUPLICATED_ARRAY_ELEMENT_SAMPLE_MODULUS = 100;

  /**
   * Default per-batch memory budget for the legacy {@code EMBEDDING} strategy (type:text indexes).
   * Unbounded because this strategy does not go through the configurable budget path.
   */
  static final long PER_BATCH_AUTO_EMBEDDING_MEMORY_BUDGET_BYTES = Long.MAX_VALUE;

  IndexingStrategy indexingStrategy;

  private final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;

  private final MaterializedViewCollectionMetadataCatalog materializedViewCollectionMetadataCatalog;
  private final MetricsFactory metricsFactory;
  private final ImmutableMap<IndexTypeTag, MaterializedViewDuplicatedArrayElementMetricCounters>
      duplicatedArrayElementCounters;

  /**
   * The registry this scheduler's metrics are exported on. Both MongoDbReplicationManager and
   * MaterializedViewManager construct an EmbeddingIndexingWorkScheduler over the same process-wide
   * registry and the same "embeddingIndexingWorkScheduler" namespace, so meters here are shared
   * across those scheduler instances.
   */
  private final MeterRegistry meterRegistry;

  /**
   * Process-wide state for the {@code embeddingBatchErrorsLastEventEpochSeconds} gauges, keyed by
   * registry and tag set. Gauges with identical names and tags must be registered once per registry
   * — their state object belongs to the registry, not the caller, and a second registration (from
   * the other scheduler instance sharing this registry) would replace the first's gauge and strand
   * its AtomicLong, silently dropping that scheduler's events from the exported timestamp. Keyed
   * per tag set because the set of (exceptionType, reason) pairs is bounded by the exception reason
   * enums.
   */
  private static final ConcurrentHashMap<MeterRegistry, ConcurrentHashMap<Tags, AtomicLong>>
      BATCH_ERROR_LAST_EVENT_EPOCH_SECONDS_GAUGES = new ConcurrentHashMap<>();

  private final AutoEmbeddingMemoryBudget globalBudget;

  private final long perBatchBudgetBytes;

  /**
   * Reuses embeddings across materialized-view write retries. The real cache is wired for the
   * materialized-view strategy but consulted only when {@link #cacheEnabled} is true; the legacy
   * {@code EMBEDDING} strategy gets a no-op instance.
   */
  private final EmbeddingRetryCache embeddingRetryCache;

  /**
   * Evaluated per batch to decide whether to route embeddings through {@link #embeddingRetryCache}.
   * Backed by the {@code EMBEDDING_RETRY_CACHE} dynamic feature flag so the cache can be toggled at
   * runtime without restarting the manager. When false, embeddings are fetched directly and nothing
   * is cached or claimed — identical to the pre-cache behavior.
   */
  private final BooleanSupplier cacheEnabled;

  /**
   * Evaluated per batch (keyed by {@link IndexDefinition#getIndexId()}) to decide whether an
   * otherwise-unhandled Voyage HTTP 400 should be skipped rather than left to block the index.
   * Backed by the {@code AUTO_EMBEDDING_400_SKIP} dynamic feature flag. False for the legacy {@code
   * EMBEDDING} strategy, which this flag does not cover.
   */
  private final Predicate<ObjectId> skipInvalidRequestEnabledForIndex;

  private final Predicate<MaterializedViewGenerationId> ownsUsableLease;

  EmbeddingIndexingWorkScheduler(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      MaterializedViewCollectionMetadataCatalog materializedViewCollectionMetadataCatalog,
      IndexingStrategy indexingStrategy,
      AutoEmbeddingMemoryBudget globalBudget,
      long perBatchBudgetBytes,
      EmbeddingRetryCache embeddingRetryCache,
      BooleanSupplier cacheEnabled,
      Predicate<ObjectId> skipInvalidRequestEnabledForIndex,
      Predicate<MaterializedViewGenerationId> ownsUsableLease) {
    super(indexingExecutor, indexingStrategy);
    this.embeddingServiceManagerSupplier = embeddingServiceManagerSupplier;
    this.materializedViewCollectionMetadataCatalog = materializedViewCollectionMetadataCatalog;
    this.indexingStrategy = indexingStrategy;
    this.globalBudget = globalBudget;
    this.perBatchBudgetBytes = perBatchBudgetBytes;
    this.embeddingRetryCache = embeddingRetryCache;
    this.cacheEnabled = cacheEnabled;
    this.skipInvalidRequestEnabledForIndex = skipInvalidRequestEnabledForIndex;
    this.ownsUsableLease = ownsUsableLease;
    this.meterRegistry = indexingExecutor.getMeterRegistry();
    this.metricsFactory =
        new MetricsFactory("embeddingIndexingWorkScheduler", indexingExecutor.getMeterRegistry());
    this.duplicatedArrayElementCounters =
        ImmutableMap.of(
            IndexTypeTag.TAG_SEARCH_AUTO_EMBEDDING,
            new MaterializedViewDuplicatedArrayElementMetricCounters(
                this.metricsFactory,
                Tags.of(
                    IndexTypeData.INDEX_TYPE_TAG_NAME,
                    IndexTypeTag.TAG_SEARCH_AUTO_EMBEDDING.tagValue,
                    MaterializedViewDuplicatedArrayElementMetricCounters.SAMPLE_MODULUS_TAG,
                    Integer.toString(DUPLICATED_ARRAY_ELEMENT_SAMPLE_MODULUS))),
            IndexTypeTag.TAG_VECTOR_SEARCH_AUTO_EMBEDDING,
            new MaterializedViewDuplicatedArrayElementMetricCounters(
                this.metricsFactory,
                Tags.of(
                    IndexTypeData.INDEX_TYPE_TAG_NAME,
                    IndexTypeTag.TAG_VECTOR_SEARCH_AUTO_EMBEDDING.tagValue,
                    MaterializedViewDuplicatedArrayElementMetricCounters.SAMPLE_MODULUS_TAG,
                    Integer.toString(DUPLICATED_ARRAY_ELEMENT_SAMPLE_MODULUS))));
  }

  /**
   * Creates and starts a new EmbeddingIndexingWorkScheduler.
   *
   * @return an EmbeddingIndexingWorkScheduler.
   * @deprecated use createForMaterializedViewIndex instead. This will be removed after type:text
   *     index is deprecated.
   */
  public static EmbeddingIndexingWorkScheduler create(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier) {
    return create(
        indexingExecutor,
        embeddingServiceManagerSupplier,
        AutoEmbeddingMemoryBudget.createDefault());
  }

  static EmbeddingIndexingWorkScheduler create(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      AutoEmbeddingMemoryBudget globalBudget) {
    EmbeddingIndexingWorkScheduler scheduler =
        new EmbeddingIndexingWorkScheduler(
            indexingExecutor,
            embeddingServiceManagerSupplier,
            // Creates empty catalog for now.
            new MaterializedViewCollectionMetadataCatalog(),
            IndexingStrategy.EMBEDDING,
            globalBudget,
            PER_BATCH_AUTO_EMBEDDING_MEMORY_BUDGET_BYTES,
            // Legacy type:text indexing has no per-batch MV commit to drive eviction.
            EmbeddingRetryCache.noOp(),
            () -> false,
            // AUTO_EMBEDDING_400_SKIP does not cover the legacy EMBEDDING strategy.
            id -> false,
            id -> true);
    scheduler.start();
    return scheduler;
  }

  /**
   * Creates and starts a new EmbeddingIndexingWorkScheduler for an auto-embedding materialized view
   * index.
   *
   * @return an EmbeddingIndexingWorkScheduler.
   */
  @TestOnly
  public static EmbeddingIndexingWorkScheduler createForMaterializedViewIndex(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      MaterializedViewCollectionMetadataCatalog matViewCollectionMetadataCatalog) {
    return createForMaterializedViewIndex(
        indexingExecutor,
        embeddingServiceManagerSupplier,
        matViewCollectionMetadataCatalog,
        AutoEmbeddingMemoryBudget.createDefault());
  }

  @TestOnly
  static EmbeddingIndexingWorkScheduler createForMaterializedViewIndex(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      MaterializedViewCollectionMetadataCatalog matViewCollectionMetadataCatalog,
      AutoEmbeddingMemoryBudget globalBudget) {
    return createForMaterializedViewIndex(
        indexingExecutor,
        embeddingServiceManagerSupplier,
        matViewCollectionMetadataCatalog,
        globalBudget,
        PER_BATCH_AUTO_EMBEDDING_MEMORY_BUDGET_BYTES);
  }

  @TestOnly
  static EmbeddingIndexingWorkScheduler createForMaterializedViewIndex(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      MaterializedViewCollectionMetadataCatalog matViewCollectionMetadataCatalog,
      AutoEmbeddingMemoryBudget globalBudget,
      long perBatchBudgetBytes) {
    return createForMaterializedViewIndex(
        indexingExecutor,
        embeddingServiceManagerSupplier,
        matViewCollectionMetadataCatalog,
        globalBudget,
        perBatchBudgetBytes,
        EmbeddingRetryCache.noOp(),
        () -> false,
        id -> false);
  }

  @TestOnly
  static EmbeddingIndexingWorkScheduler createForMaterializedViewIndex(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      MaterializedViewCollectionMetadataCatalog matViewCollectionMetadataCatalog,
      AutoEmbeddingMemoryBudget globalBudget,
      long perBatchBudgetBytes,
      EmbeddingRetryCache embeddingRetryCache,
      BooleanSupplier cacheEnabled) {
    return createForMaterializedViewIndex(
        indexingExecutor,
        embeddingServiceManagerSupplier,
        matViewCollectionMetadataCatalog,
        globalBudget,
        perBatchBudgetBytes,
        embeddingRetryCache,
        cacheEnabled,
        id -> false);
  }

  @TestOnly
  static EmbeddingIndexingWorkScheduler createForMaterializedViewIndex(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      MaterializedViewCollectionMetadataCatalog matViewCollectionMetadataCatalog,
      AutoEmbeddingMemoryBudget globalBudget,
      long perBatchBudgetBytes,
      EmbeddingRetryCache embeddingRetryCache,
      BooleanSupplier cacheEnabled,
      Predicate<ObjectId> skipInvalidRequestEnabledForIndex) {
    return createForMaterializedViewIndex(
        indexingExecutor,
        embeddingServiceManagerSupplier,
        matViewCollectionMetadataCatalog,
        globalBudget,
        perBatchBudgetBytes,
        embeddingRetryCache,
        cacheEnabled,
        skipInvalidRequestEnabledForIndex,
        id -> true);
  }

  static EmbeddingIndexingWorkScheduler createForMaterializedViewIndex(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      MaterializedViewCollectionMetadataCatalog matViewCollectionMetadataCatalog,
      AutoEmbeddingMemoryBudget globalBudget,
      long perBatchBudgetBytes,
      EmbeddingRetryCache embeddingRetryCache,
      BooleanSupplier cacheEnabled,
      Predicate<ObjectId> skipInvalidRequestEnabledForIndex,
      Predicate<MaterializedViewGenerationId> ownsUsableLease) {
    EmbeddingIndexingWorkScheduler scheduler =
        new EmbeddingIndexingWorkScheduler(
            indexingExecutor,
            embeddingServiceManagerSupplier,
            matViewCollectionMetadataCatalog,
            IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW,
            globalBudget,
            perBatchBudgetBytes,
            embeddingRetryCache,
            cacheEnabled,
            skipInvalidRequestEnabledForIndex,
            ownsUsableLease);
    scheduler.start();
    return scheduler;
  }

  @Override
  CompletableFuture<Void> getBatchTasksFuture(IndexingSchedulerBatch batch) {
    // Replace text fields in event documents with embedded vectors for auto-embedding indexes.
    IndexDefinition indexDefinition = batch.indexer.getIndexDefinition();

    ImmutableMap<FieldPath, String> modelNamePerPath = indexDefinition.getModelNamePerPath();

    // Look up all configs and verify models are registered
    ImmutableMap.Builder<FieldPath, EmbeddingModelConfig> modelConfigPerPathBuilder =
        ImmutableMap.builder();
    for (var entry : modelNamePerPath.entrySet()) {
      String modelName = entry.getValue();
      if (!EmbeddingModelCatalog.isModelRegistered(modelName.toLowerCase())) {
        return CompletableFuture.failedFuture(
            new EmbeddingProviderNonTransientException(
                String.format(
                    "CanonicalModel: %s not registered yet, supported models are: [%s]",
                    modelName, String.join(", ", EmbeddingModelCatalog.getAllSupportedModels())),
                EmbeddingProviderNonTransientException.Reason.MODEL_NOT_REGISTERED));
      }
      modelConfigPerPathBuilder.put(
          entry.getKey(), EmbeddingModelCatalog.getModelConfig(modelName));
    }
    Optional<MaterializedViewSchemaMetadata> matViewCollectionMetadataOpt =
        this.materializedViewCollectionMetadataCatalog
            .getMetadataIfPresent(batch.generationId)
            .map(MaterializedViewCollectionMetadata::schemaMetadata);
    if (matViewCollectionMetadataOpt.isEmpty()
        && this.indexingStrategy == IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW) {
      return CompletableFuture.failedFuture(
          new EmbeddingProviderNonTransientException(
              String.format(
                  "Unable to process materialized view index batch because mat view metadata is"
                      + " not present for generationId: %s",
                  batch.generationId),
              EmbeddingProviderNonTransientException.Reason.MV_METADATA_NOT_PRESENT));
    }

    try {
      ensureValidLease(batch.generationId);
    } catch (RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }

    ImmutableMap<FieldPath, EmbeddingModelConfig> modelConfigPerPath =
        modelConfigPerPathBuilder.build();
    AutoEmbedFieldMapping autoEmbedMapping =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition);
    ImmutableMap<FieldPath, EmbedConfigurationForBatch> embedBatchKeyPerPath =
        computeEmbedBatchKeyPerPath(autoEmbedMapping, modelConfigPerPath);

    // Cache keys this batch claimed, released on its successful commit. Releasing only this batch's
    // keys (not the whole generation) is essential: a generation lives across many batches, and the
    // steady stream of empty change-stream batches also reach finalize() — releasing the whole
    // generation on every such commit would evict a different, still-in-flight batch's claims.
    String generationKey =
        MaterializedViewCollectionMetadataCatalog.canonicalKey(batch.generationId);
    Set<EmbeddingCacheKey> batchClaimedKeys = ConcurrentHashMap.newKeySet();
    // batch.future completes normally only after a successful commit in the base class finalize(),
    // and exceptionally on any failure, so a failed batch keeps its claims for the retry. The
    // sub-batch path releases per sub-batch instead and leaves this set empty (a harmless no-op).
    batch.future.thenRun(
        () ->
            this.embeddingRetryCache.releaseClaims(
                generationKey, batchClaimedKeys, ReleaseTrigger.COMMIT));

    // Check global memory budget before proceeding. If exceeded, fast-fail with a transient
    // exception so the batch can be retried when memory becomes available.
    long estimatedBatchBytes = estimateBatchMemoryBytes(batch.events, autoEmbedMapping);
    if (!this.globalBudget.tryAcquire(estimatedBatchBytes)) {
      return CompletableFuture.failedFuture(
          new MaterializedViewTransientException(
              "Global auto-embedding memory budget exceeded; batch will be retried",
              MaterializedViewTransientException.Reason.MEMORY_BUDGET_EXCEEDED));
    }

    CompletableFuture<Void> batchFuture;
    try {
      if (this.indexingStrategy == IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW
          && this.perBatchBudgetBytes != Long.MAX_VALUE
          && estimatedBatchBytes > this.perBatchBudgetBytes) {
        // Per-batch budget is configured and the batch exceeds it: divide the batch into
        // sub-batches and pipeline embedding with flushing. The next sub-batch's embedding call is
        // only issued after the previous sub-batch has been committed to MongoDB. If the batch fits
        // within the budget, fall through to the parallel path to preserve the original behavior.
        long embeddableDocCount =
            batch.events.stream()
                .filter(e -> containsValidDocument(e) && !e.isFilterOnlyUpdate())
                .count();
        long bytesPerDoc = embeddableDocCount > 0 ? estimatedBatchBytes / embeddableDocCount : 0;
        int subBatchSize = computeSubBatchSize(bytesPerDoc, this.perBatchBudgetBytes);
        batchFuture =
            processSubBatchesSequentially(
                batch,
                indexDefinition,
                autoEmbedMapping,
                embedBatchKeyPerPath,
                matViewCollectionMetadataOpt,
                subBatchSize,
                generationKey,
                batch.generationId);
      } else {
        // No per-batch budget: process all bundles in parallel.
        List<CompletableFuture<List<DocumentEvent>>> indexingBundles =
            embed(
                batch.events,
                indexDefinition,
                autoEmbedMapping,
                batch.priority,
                embedBatchKeyPerPath,
                matViewCollectionMetadataOpt,
                generationKey,
                batchClaimedKeys,
                batch.generationId);

        batchFuture =
            FutureUtils.allOf(
                indexingBundles.stream()
                    // Convert each indexing bundle to multiple indexing tasks.
                    .map(
                        bundleFuture ->
                            bundleFuture.thenComposeAsync(
                                eventBundleList ->
                                    FutureUtils.allOf(
                                        eventBundleList.stream()
                                            .map((doc) -> new IndexingTask(batch.indexer, doc))
                                            .map(
                                                (task) ->
                                                    FutureUtils.checkedRunAsync(
                                                        task,
                                                        this.executor,
                                                        FieldExceededLimitsException.class))
                                            .collect(Collectors.toList())),
                                this.executor))
                    .collect(Collectors.toList()));
      }
    } catch (Exception e) {
      this.globalBudget.release(estimatedBatchBytes);
      return CompletableFuture.failedFuture(e);
    }

    // Release the global budget when all indexing tasks complete (success or failure).
    // Use whenComplete (no executor) so the release is not subject to executor rejection or
    // shutdown, which would leak budget and block future batches.
    return batchFuture.whenComplete(
        (result, throwable) -> this.globalBudget.release(estimatedBatchBytes));
  }

  /**
   * Processes sub-batches sequentially for the per-batch memory budget case. For each sub-batch: 1)
   * issues the embedding call, 2) applies embeddings and indexes all events, 3) commits (flushes to
   * MongoDB) before starting the next sub-batch.
   */
  private CompletableFuture<Void> processSubBatchesSequentially(
      IndexingSchedulerBatch batch,
      IndexDefinition indexDefinition,
      AutoEmbedFieldMapping autoEmbedMapping,
      ImmutableMap<FieldPath, EmbedConfigurationForBatch> embedBatchKeyPerPath,
      Optional<MaterializedViewSchemaMetadata> matViewSchemaMetadata,
      int subBatchSize,
      String generationKey,
      GenerationId generationId) {
    List<EmbedBundle> bundles;
    try {
      bundles =
          getTextValueBundles(batch.events, autoEmbedMapping, embedBatchKeyPerPath, subBatchSize);
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }

    FLOGGER.atFine().log(
        "Processing embedding sub-batches: index=%s, generationId=%s, subBatchSize=%d,"
            + " totalSubBatches=%d, totalEvents=%d",
        batch.indexer.getIndexDefinition().getName(),
        batch.generationId,
        subBatchSize,
        bundles.size(),
        batch.events.size());

    @Var CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
    for (int bundleIndex = 0; bundleIndex < bundles.size(); bundleIndex++) {
      EmbedBundle bundle = bundles.get(bundleIndex);
      // Capture loop variables for use in lambdas.
      List<DocumentEvent> events = new ArrayList<>(bundle.events());
      SetMultimap<EmbedConfigurationForBatch, String> textsPerModel = bundle.textsPerBatchKey();
      int subBatchNumber = bundleIndex + 1;
      // Keys this sub-batch claims, released on its own commit below.
      Set<EmbeddingCacheKey> subBatchClaimedKeys = ConcurrentHashMap.newKeySet();

      chain =
          chain
              // Wait for the previous sub-batch flush before issuing the next embedding call.
              .thenComposeAsync(
                  ignored -> {
                    FLOGGER.atFine().log(
                        "Embedding sub-batch: index=%s, subBatch=%d/%d, eventCount=%d,"
                            + " embedBatchKeys=%d",
                        batch.indexer.getIndexDefinition().getName(),
                        subBatchNumber,
                        bundles.size(),
                        events.size(),
                        textsPerModel.size());
                    return getEmbeddings(
                            textsPerModel,
                            batch.priority,
                            indexDefinition,
                            generationKey,
                            subBatchClaimedKeys,
                            generationId)
                        .handleAsync(
                            (embeddingsPerModel, throwable) -> {
                              if (throwable != null) {
                                return skipBundleOrRethrow(throwable, indexDefinition, events);
                              }
                              return applyEmbeddingsToEvents(
                                  events,
                                  embeddingsPerModel,
                                  embedBatchKeyPerPath,
                                  indexDefinition,
                                  autoEmbedMapping,
                                  matViewSchemaMetadata);
                            },
                            this.executor);
                  },
                  this.executor)
              // Index all events in this sub-batch.
              .thenComposeAsync(
                  embeddedEvents ->
                      FutureUtils.allOf(
                          embeddedEvents.stream()
                              .map(doc -> new IndexingTask(batch.indexer, doc))
                              .map(
                                  task ->
                                      FutureUtils.checkedRunAsync(
                                          task, this.executor, FieldExceededLimitsException.class))
                              .collect(Collectors.toList())),
                  this.executor)
              // Flush this sub-batch to MongoDB before processing the next one.
              .thenRunAsync(
                  () -> {
                    try {
                      batch.indexer.commit();
                    } catch (MaterializedViewTransientException
                        | MaterializedViewNonTransientException e) {
                      throw e;
                    } catch (Exception e) {
                      throw new MaterializedViewTransientException(
                          "Failed intermediate sub-batch commit", e);
                    }
                    // This sub-batch is durable: release exactly its claimed keys so its committed
                    // entries are evicted without waiting for the whole batch to finish, and
                    // without disturbing other sub-batches' (or a failed round's) claims.
                    this.embeddingRetryCache.releaseClaims(
                        generationKey, subBatchClaimedKeys, ReleaseTrigger.COMMIT);
                  },
                  this.executor);
    }
    return chain;
  }

  /**
   * Unwraps wrapper exceptions (CompletionException, ExecutionException, FailsafeException) to find
   * the root cause. The embedding pipeline can wrap typed exceptions in multiple layers:
   * CompletionException → FailsafeException → EmbeddingProviderTransientException.
   */
  private static Throwable unwrapCause(Throwable throwable) {
    @Var Throwable current = throwable;
    while (current.getCause() != null
        && current.getCause() != current
        && (current instanceof CompletionException
            || current instanceof ExecutionException
            || current instanceof FailsafeException)) {
      current = current.getCause();
    }
    return current;
  }

  /**
   * Handles a failed embedding call for a bundle. When the failure is an otherwise-unhandled Voyage
   * 400 ({@link EmbeddingProviderTransientException} with reason {@code HTTP_INVALID_REQUEST}) and
   * {@code AUTO_EMBEDDING_400_SKIP} is enabled for this index, logs the skipped documents and
   * returns an empty event list so the whole bundle is dropped — the batch still commits and
   * advances the checkpoint. Dropping the bundle (rather than building its documents without
   * embeddings) is what makes SKIP uniform across event types: a change-stream partial embed update
   * cannot be written without its new vector, so the only coherent skip is to not apply it at all.
   * Any other failure, or the flag being off, is rethrown unchanged so the batch fails and the
   * index blocks at its checkpoint.
   */
  private List<DocumentEvent> skipBundleOrRethrow(
      Throwable throwable, IndexDefinition indexDefinition, List<DocumentEvent> events) {
    Throwable cause = unwrapCause(throwable);
    if (cause instanceof EmbeddingProviderTransientException ex
        && ex.getReason() == EmbeddingProviderTransientException.Reason.HTTP_INVALID_REQUEST
        && this.skipInvalidRequestEnabledForIndex.test(indexDefinition.getIndexId())) {
      logSkippedInvalidRequestBatch(indexDefinition, events, ex);
      return List.of();
    }
    if (throwable instanceof RuntimeException runtimeException) {
      throw runtimeException;
    }
    if (throwable instanceof Error error) {
      throw error;
    }
    throw new AssertionError("threw unexpected checked exception: " + throwable, throwable);
  }

  /**
   * Logs the skipped batch's {@code _id} range (first/last document, in scan order) for support to
   * pull, labeled with the index name and database. Rate-limited per log call site (not deduped per
   * index or checkpoint position — a stuck index can suppress another index's log line) so a
   * persistently failing index does not spam the log on every subsequent update to the same range.
   */
  private void logSkippedInvalidRequestBatch(
      IndexDefinition indexDefinition,
      List<DocumentEvent> events,
      EmbeddingProviderTransientException cause) {
    logDefensively(
        () -> {
          LoggableIdRange idRange = loggableIdRange(events);
          FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
              "AUTO_EMBEDDING_400_SKIP enabled: skipping %d document(s) after an invalid-request"
                  + " error instead of blocking. index=%s, database=%s, idRange=[%s, %s],"
                  + " cause=%s",
              events.size(),
              indexDefinition.getName(),
              indexDefinition.getDatabase(),
              idRange.firstId(),
              idRange.lastId(),
              cause.getMessage());
        });
  }

  private record LoggableIdRange(String firstId, String lastId) {}

  /**
   * Computes the batch's {@code _id} range (first/last event, in scan order) as loggable strings
   * via {@link LoggableIdUtils#getLoggableId}, falling back to {@link
   * LoggableIdUtils#UNKNOWN_LOGGABLE_ID} for both ends if extraction or formatting throws.
   */
  private static LoggableIdRange loggableIdRange(List<DocumentEvent> events) {
    try {
      return new LoggableIdRange(
          LoggableIdUtils.getLoggableId(events.get(0).getDocumentId()),
          LoggableIdUtils.getLoggableId(events.get(events.size() - 1).getDocumentId()));
    } catch (Exception e) {
      return new LoggableIdRange(
          LoggableIdUtils.UNKNOWN_LOGGABLE_ID, LoggableIdUtils.UNKNOWN_LOGGABLE_ID);
    }
  }

  /**
   * Runs a diagnostic logging statement, swallowing any exception so a broken log call can never
   * affect the caller — the SKIP recovery path and {@link #handleBatchException} (which must
   * resolve {@code batch.future} regardless) both log through this. Per {@code
   * docs/internal/development/patterns/defensive-indexing-path-logging.md}: logging in the indexing
   * path must never throw uncaught, and callers should guard the entire diagnostic call, not just
   * the id-formatting step, even though {@link #loggableIdRange} already guards itself.
   */
  private static void logDefensively(Runnable logStatement) {
    try {
      logStatement.run();
    } catch (Exception e) {
      FLOGGER.atSevere().atMostEvery(1, TimeUnit.HOURS).log(
          "Unexpected error emitting a diagnostic log in the auto-embedding indexing path");
    }
  }

  @Override
  void handleBatchException(IndexingSchedulerBatch batch, Throwable throwable) {
    Throwable cause = unwrapCause(throwable);
    if (cause instanceof EmbeddingProviderNonTransientException ex) {
      recordBatchErrorMetric(
          EmbeddingProviderNonTransientException.class.getSimpleName(), ex.getReason().name());
      if (batch.priority == Priority.INITIAL_SYNC_COLLECTION_SCAN
          || batch.priority == Priority.INITIAL_SYNC_CHANGE_STREAM) {
        batch.future.completeExceptionally(InitialSyncException.createFailed(ex));
      } else {
        batch.future.completeExceptionally(SteadyStateException.createNonInvalidatingResync(ex));
      }
    } else if (cause instanceof EmbeddingProviderTransientException ex) {
      recordBatchErrorMetric(
          EmbeddingProviderTransientException.class.getSimpleName(), ex.getReason().name());
      if (ex.getReason() == EmbeddingProviderTransientException.Reason.HTTP_INVALID_REQUEST) {
        logBlockedInvalidRequestBatch(batch, ex);
      }
      // TODO(CLOUDP-305372): Find a way to skip already indexed document event in retries.
      if (batch.priority == Priority.INITIAL_SYNC_COLLECTION_SCAN
          || batch.priority == Priority.INITIAL_SYNC_CHANGE_STREAM) {
        batch.future.completeExceptionally(InitialSyncException.createResumableTransient(ex));
      } else {
        batch.future.completeExceptionally(SteadyStateException.createTransient(ex));
      }
    } else if (cause instanceof MaterializedViewTransientException ex) {
      recordBatchErrorMetric(
          MaterializedViewTransientException.class.getSimpleName(), ex.getReason().name());
      if (batch.priority == Priority.INITIAL_SYNC_COLLECTION_SCAN
          || batch.priority == Priority.INITIAL_SYNC_CHANGE_STREAM) {
        batch.future.completeExceptionally(InitialSyncException.createResumableTransient(ex));
      } else {
        batch.future.completeExceptionally(SteadyStateException.createTransient(ex));
      }
    } else if (cause instanceof MaterializedViewNonTransientException ex) {
      recordBatchErrorMetric(
          MaterializedViewNonTransientException.class.getSimpleName(), ex.getReason().name());
      if (ex.getReason() == MaterializedViewNonTransientException.Reason.INVALID_LEASE) {
        if (batch.priority == Priority.INITIAL_SYNC_COLLECTION_SCAN
            || batch.priority == Priority.INITIAL_SYNC_CHANGE_STREAM) {
          batch.future.completeExceptionally(InitialSyncException.createShutDown());
        } else {
          batch.future.completeExceptionally(SteadyStateException.createShutDown());
        }
      } else if (batch.priority == Priority.INITIAL_SYNC_COLLECTION_SCAN
          || batch.priority == Priority.INITIAL_SYNC_CHANGE_STREAM) {
        batch.future.completeExceptionally(InitialSyncException.createFailed(ex));
      } else {
        batch.future.completeExceptionally(SteadyStateException.createNonInvalidatingResync(ex));
      }
    } else {
      batch.future.completeExceptionally(throwable);
    }
  }

  /**
   * Logs the blocked batch's {@code _id} range (first/last event in the batch, since bisecting down
   * to the exact failing document is post-GA) for support to pull, labeled with the index name and
   * database. Rate-limited per log call site (not deduped per index or checkpoint position — a
   * stuck index can suppress another index's log line): the index retries this same batch
   * indefinitely while blocked, so without rate-limiting this would log on every retry.
   */
  private void logBlockedInvalidRequestBatch(
      IndexingSchedulerBatch batch, EmbeddingProviderTransientException cause) {
    if (batch.events.isEmpty()) {
      return;
    }
    logDefensively(
        () -> {
          IndexDefinition indexDefinition = batch.indexer.getIndexDefinition();
          LoggableIdRange idRange = loggableIdRange(batch.events);
          FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
              "Index blocked on an invalid-request error and will retry indefinitely at its"
                  + " checkpoint. index=%s, database=%s, idRange=[%s, %s], cause=%s",
              indexDefinition.getName(),
              indexDefinition.getDatabase(),
              idRange.firstId(),
              idRange.lastId(),
              cause.getMessage());
        });
  }

  private void recordBatchErrorMetric(String exceptionType, String reason) {
    var tags = Tags.of("exceptionType", exceptionType, "reason", reason.toLowerCase(Locale.ROOT));
    this.metricsFactory.counter("embeddingBatchErrors", tags).increment();
    // CLOUDP-451834: alongside the counter, record when the error happened as an absolute
    // epoch-seconds gauge. Alerting on the counter's increase over a window cannot distinguish
    // a new event from an old value replayed after a scrape gap, which false-paged these
    // alerts; an absolute timestamp is unaffected by gaps and process restarts, so consumers
    // can detect a new event with a single time() comparison and no gap reasoning.
    BATCH_ERROR_LAST_EVENT_EPOCH_SECONDS_GAUGES
        .computeIfAbsent(this.meterRegistry, r -> new ConcurrentHashMap<>())
        .computeIfAbsent(
            tags, t -> this.metricsFactory.numGauge("embeddingBatchErrorsLastEventEpochSeconds", t))
        .set(System.currentTimeMillis() / 1000);
  }

  @Override
  protected boolean shouldCommitOnFinalize() {
    return this.indexingStrategy == IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW;
  }

  /**
   * Splits events in this batch into multiple indexing bundles and replaces the given events with
   * events that contain the embedded vectors by calling external embedding service.
   *
   * <p>All bundles are embedded concurrently. For sequential per-batch processing see {@link
   * #processSubBatchesSequentially}.
   *
   * <p>This modifies the List!
   */
  private List<CompletableFuture<List<DocumentEvent>>> embed(
      List<DocumentEvent> allEventsInBatch,
      IndexDefinition indexDefinition,
      AutoEmbedFieldMapping autoEmbedMapping,
      SchedulerQueue.Priority priority,
      ImmutableMap<FieldPath, EmbedConfigurationForBatch> embedBatchKeyPerPath,
      Optional<MaterializedViewSchemaMetadata> matViewSchemaMetadata,
      String generationKey,
      Set<EmbeddingCacheKey> claimedKeys,
      GenerationId generationId) {
    List<EmbedBundle> embedBundles =
        getTextValueBundles(
            allEventsInBatch,
            autoEmbedMapping,
            embedBatchKeyPerPath,
            MAX_AUTO_EMBED_DOCUMENT_BUNDLE_SIZE);
    List<CompletableFuture<List<DocumentEvent>>> resultFutures = new ArrayList<>();
    for (EmbedBundle bundle : embedBundles) {
      // Needs to create a shallow copy for List<DocumentEvent> to avoid original list to be
      // referenced by CompletableFuture::UniApply even after completing futures, which may cause
      // memory leak.
      var events = new ArrayList<>(bundle.events());
      resultFutures.add(
          // Change executor to use indexing executor here for better chaining with indexer.
          getEmbeddings(
                  bundle.textsPerBatchKey(),
                  priority,
                  indexDefinition,
                  generationKey,
                  claimedKeys,
                  generationId)
              .handleAsync(
                  (embeddingsPerModel, throwable) -> {
                    if (throwable != null) {
                      return skipBundleOrRethrow(throwable, indexDefinition, events);
                    }
                    return applyEmbeddingsToEvents(
                        events,
                        embeddingsPerModel,
                        embedBatchKeyPerPath,
                        indexDefinition,
                        autoEmbedMapping,
                        matViewSchemaMetadata);
                  },
                  this.executor));
    }
    return resultFutures;
  }

  /**
   * Applies the generated embeddings to the given events and returns the updated list. Events that
   * are deletes, filter-only updates, or have no valid document are passed through unchanged.
   */
  private List<DocumentEvent> applyEmbeddingsToEvents(
      List<DocumentEvent> events,
      Map<EmbedConfigurationForBatch, Map<String, Vector>> embeddingsPerBatchKey,
      ImmutableMap<FieldPath, EmbedConfigurationForBatch> embedBatchKeyPerPath,
      IndexDefinition indexDefinition,
      AutoEmbedFieldMapping autoEmbedMapping,
      Optional<MaterializedViewSchemaMetadata> matViewSchemaMetadata) {
    FLOGGER.atFine().log(
        "Applying embeddings to events: eventCount=%d, embeddingBatchKeys=%d,"
            + " autoEmbedFieldPaths=%d",
        events.size(), embeddingsPerBatchKey.size(), embedBatchKeyPerPath.size());

    // One map per auto-embed path from embedBatchKeyPerPath; empty if no embeddings.
    var embeddingMapPerField =
        embedBatchKeyPerPath.entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Map.Entry::getKey,
                    e ->
                        ImmutableMap.copyOf(
                            embeddingsPerBatchKey.getOrDefault(e.getValue(), ImmutableMap.of()))));
    Optional<FieldPath> sourceNestedRoot =
        indexDefinition instanceof VectorIndexDefinition vectorIndexDefinition
            ? vectorIndexDefinition.getNestedRoot()
            : Optional.empty();

    for (int i = 0; i < events.size(); i++) {
      DocumentEvent event = events.get(i);
      if (!containsValidDocument(event)) {
        continue;
      }

      // Filter-only updates have no embedding work; the $set body is used as-is — except for nested
      // indexes. CLOUDP-402522: nested indexes co-locate filters with the vector under the MV
      // nestedRoot (e.g. _autoEmbed.sections.<i>.section_name), but the filter-only path only wrote
      // the source-path copies, so augment the $set with those co-located _autoEmbed.* values here;
      // otherwise a nested $vectorSearch combining `filter` with a nestedRoot path reads stale.
      if (event.isFilterOnlyUpdate()) {
        if (event.getDocument().isPresent()
            && AutoEmbeddingDocumentUtils.hasFilterUnderNestedRoot(
                autoEmbedMapping, sourceNestedRoot)) {
          BsonDocument augmentedSetBody =
              AutoEmbeddingDocumentUtils.buildNestedFilterPartialSetBody(
                  event.getFilterFieldUpdates().get(),
                  event.getDocument().get(),
                  autoEmbedMapping,
                  sourceNestedRoot,
                  Check.isPresent(matViewSchemaMetadata, "matViewSchemaMetadata"));
          events.set(i, event.withFilterFieldUpdates(augmentedSetBody));
        }
        continue;
      }

      // For partial-embed updates, augment the existing filter-field $set body with vector + hash
      // entries for the embed paths that actually changed, then emit the augmented event. Other
      // embed fields remain untouched on the MV document because the $set only references the
      // changed paths.
      if (!event.getChangedEmbedTexts().isEmpty()) {
        BsonDocument augmentedSetBody =
            AutoEmbeddingDocumentUtils.buildPartialEmbedUpdateSetBody(
                event.getFilterFieldUpdates().get(),
                event.getChangedEmbedTexts(),
                embeddingMapPerField,
                Check.isPresent(matViewSchemaMetadata, "matViewSchemaMetadata"));
        events.set(i, event.withFilterFieldUpdates(augmentedSetBody));
        continue;
      }

      try {
        DocumentEvent autoEmbeddingDocumentEvent;
        if (this.indexingStrategy == IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW) {
          // TODO(CLOUDP-363914): Pass mv schema metadata from catalog.
          autoEmbeddingDocumentEvent =
              buildMaterializedViewDocumentEvent(
                  event,
                  autoEmbedMapping,
                  embeddingMapPerField,
                  Check.isPresent(matViewSchemaMetadata, "matViewSchemaMetadata"),
                  sourceNestedRoot);
          if (shouldSampleDuplicatedArrayElements(event.getDocumentId())) {
            event
                .getDocument()
                .ifPresent(
                    document ->
                        recordDuplicatedArrayElements(
                            indexDefinition,
                            AutoEmbeddingDocumentUtils.detectDuplicatedArrayElementTypes(
                                document, autoEmbedMapping)));
          }
        } else {
          autoEmbeddingDocumentEvent =
              buildAutoEmbeddingDocumentEvent(event, autoEmbedMapping, embeddingMapPerField);
        }
        events.set(i, autoEmbeddingDocumentEvent);
      } catch (IOException e) {
        FLOGGER.atSevere().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
            "Failed to replace string field values");
      }
    }
    return events;
  }

  private void recordDuplicatedArrayElements(
      IndexDefinition indexDefinition,
      ImmutableSet<MaterializedViewArrayElementType> passthroughArrayElements) {
    if (passthroughArrayElements.isEmpty()) {
      return;
    }
    MaterializedViewDuplicatedArrayElementMetricCounters counters =
        this.duplicatedArrayElementCounters.get(IndexTypeData.getIndexTypeTag(indexDefinition));
    Check.stateNotNull(counters, "duplicatedArrayElementCounters");
    passthroughArrayElements.forEach(
        element -> counters.record(element, DUPLICATED_ARRAY_ELEMENT_SAMPLE_MODULUS));
  }

  /** Deterministically samples documents by id; one in every sample modulus is inspected. */
  @VisibleForTesting
  static boolean shouldSampleDuplicatedArrayElements(BsonValue documentId) {
    @Var int hash = documentId.hashCode();
    // Finalize the hash so sequential or patterned ids do not cluster on the modulus.
    hash ^= hash >>> 16;
    hash *= 0x85EBCA6B;
    hash ^= hash >>> 13;
    return Math.floorMod(hash, DUPLICATED_ARRAY_ELEMENT_SAMPLE_MODULUS) == 0;
  }

  /**
   * Splits Documents events by bundle size, deletion and non-autoembedding(no matching text) events
   * are not limited by bundle size.
   */
  static List<EmbedBundle> getTextValueBundles(
      List<DocumentEvent> events,
      AutoEmbedFieldMapping fieldMapping,
      ImmutableMap<FieldPath, EmbedConfigurationForBatch> embedBatchKeyPerPath,
      int maxDocumentBundleSize) {
    // Step 1: Gets all (document, Map<EmbedBatchKey, Set<String>>) pairs
    List<DocumentTexts> documentModelTextMapPairs =
        events.stream()
            .map(
                event -> {
                  // Skip text extraction for filter-only updates - they don't need embeddings.
                  if (!containsValidDocument(event) || event.isFilterOnlyUpdate()) {
                    return new DocumentTexts(event, HashMultimap.create());
                  }
                  SetMultimap<EmbedConfigurationForBatch, String> textsPerBatchKey =
                      HashMultimap.create();
                  if (!event.getChangedEmbedTexts().isEmpty()) {
                    // Partial-embed updates already carry the chosen text per changed path from
                    // classify-time; bundle those directly without re-walking the source document.
                    // Full-replace events take the walk path below because they don't have a
                    // precomputed map.
                    for (Map.Entry<FieldPath, String> e : event.getChangedEmbedTexts().entrySet()) {
                      EmbedConfigurationForBatch batchKey = embedBatchKeyPerPath.get(e.getKey());
                      Check.checkState(
                          batchKey != null,
                          "missing embed batch key for auto-embed path: %s",
                          e.getKey());
                      textsPerBatchKey.put(batchKey, e.getValue());
                    }
                    return new DocumentTexts(event, textsPerBatchKey);
                  }
                  try {
                    var autoEmbeddingTextPathMap =
                        AutoEmbeddingDocumentUtils.getVectorTextPathMap(
                            event.getDocument().get(), fieldMapping);
                    for (var entry : autoEmbeddingTextPathMap.entrySet()) {
                      FieldPath fieldPath = entry.getKey();
                      Set<String> textsInField = entry.getValue();
                      EmbedConfigurationForBatch batchKey = embedBatchKeyPerPath.get(fieldPath);
                      Check.checkState(
                          batchKey != null,
                          "missing embed batch key for auto-embed path: %s",
                          fieldPath);

                      // Get reusable embeddings for this field
                      var reusableForField =
                          event.getAutoEmbeddings().getOrDefault(fieldPath, ImmutableMap.of());

                      // Only add texts that don't have reusable embeddings
                      for (String text : textsInField) {
                        if (!reusableForField.containsKey(text)) {
                          textsPerBatchKey.put(batchKey, text);
                        }
                      }
                    }
                  } catch (IOException e) {
                    FLOGGER.atSevere().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
                        "Failed to get string values");
                  }
                  return new DocumentTexts(event, textsPerBatchKey);
                })
            .toList();

    // TODO(CLOUDP-331321): Move batching logic into embedding service manager
    // Step 2: For all pair with non empty autoEmbedding text set, partition them by max document
    // bundle size, and aggregate their autoEmbedding text set into one set per partition
    List<EmbedBundle> documentEventBundles =
        Lists.partition(
                documentModelTextMapPairs.stream()
                    .filter(pair -> !pair.textsPerBatchKey().isEmpty())
                    .toList(),
                maxDocumentBundleSize)
            .stream()
            .map(
                eventBundle ->
                    new EmbedBundle(
                        eventBundle.stream().map(DocumentTexts::event).toList(),
                        mergeTextsByBatchKey(eventBundle)))
            .collect(Collectors.toList());

    // Step 3: Append all no-op document events without any auto embedding text.
    List<DocumentEvent> noAutoEmbeddingEvents =
        documentModelTextMapPairs.stream()
            .filter(eventPair -> eventPair.textsPerBatchKey().isEmpty())
            .map(DocumentTexts::event)
            .toList();
    if (!noAutoEmbeddingEvents.isEmpty()) {
      documentEventBundles.add(new EmbedBundle(noAutoEmbeddingEvents, HashMultimap.create()));
    }
    return documentEventBundles;
  }

  private CompletableFuture<Map<EmbedConfigurationForBatch, Map<String, Vector>>> getEmbeddings(
      SetMultimap<EmbedConfigurationForBatch, String> stringsToEmbedPerBatchKey,
      Priority priority,
      IndexDefinition indexDefinition,
      String generationKey,
      Set<EmbeddingCacheKey> claimedKeys,
      GenerationId generationId) {
    if (stringsToEmbedPerBatchKey.isEmpty()) {
      FLOGGER.atFine().log(
          "No strings to embed for index %s, skipping embedding call", indexDefinition.getName());
      return CompletableFuture.completedFuture(Map.of());
    }

    EmbeddingServiceManager serviceManager = this.embeddingServiceManagerSupplier.get();
    ServiceTier tier =
        switch (priority) {
          case INITIAL_SYNC_COLLECTION_SCAN, INITIAL_SYNC_CHANGE_STREAM ->
              ServiceTier.COLLECTION_SCAN;
          case STEADY_STATE_CHANGE_STREAM -> ServiceTier.CHANGE_STREAM;
        };

    for (var entry : stringsToEmbedPerBatchKey.asMap().entrySet()) {
      FLOGGER.atFine().log(
          "Requesting embeddings: index=%s, model=%s, tier=%s, numDimensions=%d,"
              + " quantization=%s, stringCount=%d, database=%s, collection=%s",
          indexDefinition.getName(),
          entry.getKey().modelConfig().name(),
          tier,
          entry.getKey().numDimensions(),
          entry.getKey().quantization(),
          entry.getValue().size(),
          indexDefinition.getDatabase(),
          indexDefinition.getLastObservedCollectionName());
    }
    Map<EmbedConfigurationForBatch, CompletableFuture<Map<String, Vector>>> futuresPerBatchKey =
        new HashMap<>();

    for (var entry : stringsToEmbedPerBatchKey.asMap().entrySet()) {
      EmbedConfigurationForBatch batchKey = entry.getKey();
      Set<String> stringsToEmbed = Set.copyOf(entry.getValue());

      if (stringsToEmbed.isEmpty()) {
        continue;
      }

      EmbeddingRequestContext context =
          new EmbeddingRequestContext(
              indexDefinition.getDatabase(),
              indexDefinition.getName(),
              indexDefinition.getLastObservedCollectionName(),
              batchKey.numDimensions(),
              batchKey.quantization());
      // Evaluated per batch so the EMBEDDING_RETRY_CACHE flag toggles at runtime. When off, embed
      // directly without caching or claiming — identical to the pre-cache behavior.
      futuresPerBatchKey.put(
          batchKey,
          this.cacheEnabled.getAsBoolean()
              ? getEmbeddingsThroughCache(
                  batchKey,
                  stringsToEmbed,
                  generationKey,
                  claimedKeys,
                  serviceManager,
                  tier,
                  context,
                  generationId)
              : embedWithoutCache(
                  batchKey, stringsToEmbed, serviceManager, tier, context, generationId));
    }
    return FutureUtils.transposeMap(futuresPerBatchKey);
  }

  /**
   * Embeds the batch key's strings directly, bypassing the retry cache (the {@code
   * EMBEDDING_RETRY_CACHE} flag is off). Mirrors the pre-cache behavior: no key is cached and no
   * claim is taken, so a failed batch's retry re-embeds.
   */
  private CompletableFuture<Map<String, Vector>> embedWithoutCache(
      EmbedConfigurationForBatch batchKey,
      Set<String> stringsToEmbed,
      EmbeddingServiceManager serviceManager,
      ServiceTier tier,
      EmbeddingRequestContext context,
      GenerationId generationId) {
    List<String> orderedStrings = new ArrayList<>(stringsToEmbed);
    ensureValidLease(generationId);
    return serviceManager
        .embedAsync(orderedStrings, batchKey.modelConfig(), tier, context)
        .thenApply(
            embeddingList -> {
              Check.checkState(
                  embeddingList.size() == orderedStrings.size(),
                  "Result vectors size doesn't match input text size");
              Map<String, Vector> embeddings = new HashMap<>();
              for (int i = 0; i < embeddingList.size(); i++) {
                VectorOrError result = embeddingList.get(i);
                if (result.vector.isPresent()) {
                  embeddings.put(orderedStrings.get(i), result.vector.get());
                } else if (result != VectorOrError.EMPTY_INPUT_ERROR
                    && result.errorMessage.isPresent()) {
                  FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
                      "No embedding for %s due to error: %s",
                      orderedStrings.get(i), result.errorMessage.get());
                }
              }
              return embeddings;
            });
  }

  /**
   * Serves the batch key's embeddings from the retry cache, calling the embedding service once for
   * the keys it does not already hold, then claims every key for {@code generationKey} so the
   * vectors survive until the generation commits or its index is dropped. The claimed keys are also
   * added to {@code claimedKeys} so the caller can release exactly this batch's keys on commit.
   */
  private CompletableFuture<Map<String, Vector>> getEmbeddingsThroughCache(
      EmbedConfigurationForBatch batchKey,
      Set<String> stringsToEmbed,
      String generationKey,
      Set<EmbeddingCacheKey> claimedKeys,
      EmbeddingServiceManager serviceManager,
      ServiceTier tier,
      EmbeddingRequestContext context,
      GenerationId generationId) {
    Map<EmbeddingCacheKey, String> textByCacheKey = new HashMap<>();
    for (String text : stringsToEmbed) {
      textByCacheKey.put(
          EmbeddingCacheKey.forDocument(
              batchKey.modelConfig().name(),
              batchKey.numDimensions(),
              batchKey.quantization(),
              text),
          text);
    }
    Set<EmbeddingCacheKey> cacheKeys = textByCacheKey.keySet();

    CompletableFuture<Map<EmbeddingCacheKey, Vector>> vectorsByKey =
        this.embeddingRetryCache.getAll(
            cacheKeys,
            missingKeys ->
                embedMissingKeys(
                    missingKeys,
                    textByCacheKey,
                    batchKey,
                    serviceManager,
                    tier,
                    context,
                    generationId));
    // Claim every key — hits, freshly-loaded keys, and failed embeds alike. A hit loaded by another
    // batch still needs to be held under this generation so it is not evicted before this batch
    // commits; a failed embed simply has no cached entry, so its claim is released as a no-op.
    this.embeddingRetryCache.claim(generationKey, cacheKeys);
    claimedKeys.addAll(cacheKeys);
    return vectorsByKey.thenApply(
        keyedVectors -> {
          Map<String, Vector> embeddings = new HashMap<>();
          keyedVectors.forEach((key, vector) -> embeddings.put(textByCacheKey.get(key), vector));
          return embeddings;
        });
  }

  /** Embeds exactly the cache-missing keys and maps the results back by cache key. */
  private CompletableFuture<Map<EmbeddingCacheKey, Vector>> embedMissingKeys(
      Set<EmbeddingCacheKey> missingKeys,
      Map<EmbeddingCacheKey, String> textByCacheKey,
      EmbedConfigurationForBatch batchKey,
      EmbeddingServiceManager serviceManager,
      ServiceTier tier,
      EmbeddingRequestContext context,
      GenerationId generationId) {
    List<EmbeddingCacheKey> orderedKeys = new ArrayList<>(missingKeys);
    List<String> orderedStrings =
        orderedKeys.stream().map(textByCacheKey::get).collect(Collectors.toList());
    ensureValidLease(generationId);
    return serviceManager
        .embedAsync(orderedStrings, batchKey.modelConfig(), tier, context)
        .thenApply(
            embeddingList -> {
              Check.checkState(
                  embeddingList.size() == orderedStrings.size(),
                  "Result vectors size doesn't match input text size");
              Map<EmbeddingCacheKey, Vector> vectors = new HashMap<>();
              for (int i = 0; i < embeddingList.size(); i++) {
                VectorOrError result = embeddingList.get(i);
                if (result.vector.isPresent()) {
                  // Omitting failed keys from the result leaves them uncached (no negative cache).
                  vectors.put(orderedKeys.get(i), result.vector.get());
                } else if (result != VectorOrError.EMPTY_INPUT_ERROR
                    && result.errorMessage.isPresent()) {
                  FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
                      "No embedding for %s due to error: %s",
                      orderedStrings.get(i), result.errorMessage.get());
                }
              }
              return vectors;
            });
  }

  private void ensureValidLease(GenerationId generationId) {
    if (this.indexingStrategy != IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW
        || !(generationId instanceof MaterializedViewGenerationId matViewGenerationId)) {
      return;
    }
    if (!this.ownsUsableLease.test(matViewGenerationId)) {
      throw new MaterializedViewNonTransientException(
          "Cannot process materialized view embeddings without a valid lease for " + generationId,
          MaterializedViewNonTransientException.Reason.INVALID_LEASE);
    }
  }

  /**
   * Estimates payload bytes for all auto-embed vectors on one document (dominant term for batch
   * memory). {@link VectorIndexFieldDefinition.Type#AUTO_EMBED} uses {@link
   * VectorAutoEmbedQuantization#estimatedEmbeddingPayloadBytes(int)}; legacy {@link
   * VectorIndexFieldDefinition.Type#TEXT} uses float32 per dimension.
   */
  private static long estimateBytesPerDoc(AutoEmbedFieldMapping autoEmbedMapping) {
    return autoEmbedMapping.embedFields().values().stream()
        .map(AutoEmbedFieldMapping.AutoEmbedField.EmbedField::specification)
        .mapToLong(
            spec ->
                spec.autoEmbedQuantization().estimatedEmbeddingPayloadBytes(spec.numDimensions()))
        .sum();
  }

  /**
   * Estimates the total memory for a batch of events in bytes, accounting for both the input
   * documents (held in memory while embeddings are computed) and the embedding output vectors. Only
   * events that will actually produce embeddings are counted (non-deletes, non-filter-only updates
   * with a valid document).
   */
  private static long estimateBatchMemoryBytes(
      List<DocumentEvent> events, AutoEmbedFieldMapping autoEmbedMapping) {
    long embeddingBytesPerDoc = estimateBytesPerDoc(autoEmbedMapping);
    @Var long totalBytes = 0;
    for (DocumentEvent event : events) {
      if (!containsValidDocument(event) || event.isFilterOnlyUpdate()) {
        continue;
      }
      // Input document bytes (held in memory throughout the embedding call).
      totalBytes += event.getDocument().get().getByteBuffer().remaining();
      // Embedding output bytes (one float[] per auto-embed field; over-estimates for partial-embed
      // updates which only re-embed a subset, but over-estimation is safe: we'd rather split
      // batches conservatively than risk OOM).
      totalBytes += embeddingBytesPerDoc;
    }
    return totalBytes;
  }

  /**
   * Computes the sub-batch size (in number of documents) from the per-batch memory budget. Returns
   * {@link Integer#MAX_VALUE} when the budget is unbounded or {@code bytesPerDoc} is zero, which
   * causes all events to be treated as a single sub-batch.
   */
  private static int computeSubBatchSize(long bytesPerDoc, long perBatchBudgetBytes) {
    if (perBatchBudgetBytes == Long.MAX_VALUE || bytesPerDoc == 0) {
      return Integer.MAX_VALUE;
    }
    return (int) Math.max(1, Math.min((long) Integer.MAX_VALUE, perBatchBudgetBytes / bytesPerDoc));
  }

  private static boolean containsValidDocument(DocumentEvent event) {
    return event.getEventType() != DocumentEvent.EventType.DELETE
        && event.getDocument().isPresent();
  }

  private static SetMultimap<EmbedConfigurationForBatch, String> mergeTextsByBatchKey(
      List<DocumentTexts> eventBundle) {
    SetMultimap<EmbedConfigurationForBatch, String> merged = HashMultimap.create();
    for (var docTexts : eventBundle) {
      merged.putAll(docTexts.textsPerBatchKey());
    }
    return merged;
  }

  /**
   * Precomputes {@link EmbedConfigurationForBatch} for each auto-embed path so callers (e.g. {@link
   * #getTextValueBundles}, {@link #applyEmbeddingsToEvents}) need not re-derive keys from {@link
   * EmbeddingModelConfig} and field mapping on every use. Dims and quantization come from the
   * per-field {@link VectorFieldAutoEmbeddingSpecification} stored in the index-neutral {@link
   * AutoEmbedFieldMapping}.
   */
  static ImmutableMap<FieldPath, EmbedConfigurationForBatch> computeEmbedBatchKeyPerPath(
      AutoEmbedFieldMapping autoEmbedMapping,
      ImmutableMap<FieldPath, EmbeddingModelConfig> modelConfigPerPath) {
    ImmutableMap.Builder<FieldPath, EmbedConfigurationForBatch> builder = ImmutableMap.builder();
    for (var entry : modelConfigPerPath.entrySet()) {
      FieldPath fieldPath = entry.getKey();
      EmbeddingModelConfig config = entry.getValue();
      VectorFieldAutoEmbeddingSpecification spec =
          autoEmbeddingFieldSpecForPath(autoEmbedMapping, fieldPath);
      builder.put(
          fieldPath,
          new EmbedConfigurationForBatch(
              config, spec.numDimensions(), spec.autoEmbedQuantization()));
    }
    return builder.build();
  }

  private static VectorFieldAutoEmbeddingSpecification autoEmbeddingFieldSpecForPath(
      AutoEmbedFieldMapping autoEmbedMapping, FieldPath path) {
    AutoEmbedField.EmbedField embedField =
        Optional.ofNullable(autoEmbedMapping.embedFields().get(path))
            .orElseThrow(
                () -> new IllegalStateException("missing auto-embed field for path: " + path));

    return embedField.specification();
  }

  record EmbedConfigurationForBatch(
      EmbeddingModelConfig modelConfig,
      int numDimensions,
      VectorAutoEmbedQuantization quantization) {}

  record EmbedBundle(
      List<DocumentEvent> events,
      SetMultimap<EmbedConfigurationForBatch, String> textsPerBatchKey) {}

  record DocumentTexts(
      DocumentEvent event, SetMultimap<EmbedConfigurationForBatch, String> textsPerBatchKey) {}
}
