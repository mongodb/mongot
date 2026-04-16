package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ServiceTier;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildAutoEmbeddingDocumentEvent;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildMaterializedViewDocumentEvent;

import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.AutoEmbeddingMemoryBudget;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
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
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorFieldAutoEmbeddingSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory.IndexingStrategy;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.Priority;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import net.jodah.failsafe.FailsafeException;

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
   * Default per-batch memory budget for the legacy {@code EMBEDDING} strategy (type:text indexes).
   * Unbounded because this strategy does not go through the configurable budget path.
   */
  static final long PER_BATCH_AUTO_EMBEDDING_MEMORY_BUDGET_BYTES = Long.MAX_VALUE;

  IndexingStrategy indexingStrategy;

  private final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;

  private final MaterializedViewCollectionMetadataCatalog materializedViewCollectionMetadataCatalog;
  private final MetricsFactory metricsFactory;

  private final AutoEmbeddingMemoryBudget globalBudget;

  private final long perBatchBudgetBytes;

  EmbeddingIndexingWorkScheduler(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      MaterializedViewCollectionMetadataCatalog materializedViewCollectionMetadataCatalog,
      IndexingStrategy indexingStrategy,
      AutoEmbeddingMemoryBudget globalBudget,
      long perBatchBudgetBytes) {
    super(indexingExecutor, indexingStrategy);
    this.embeddingServiceManagerSupplier = embeddingServiceManagerSupplier;
    this.materializedViewCollectionMetadataCatalog = materializedViewCollectionMetadataCatalog;
    this.indexingStrategy = indexingStrategy;
    this.globalBudget = globalBudget;
    this.perBatchBudgetBytes = perBatchBudgetBytes;
    this.metricsFactory =
        new MetricsFactory("embeddingIndexingWorkScheduler", indexingExecutor.getMeterRegistry());
  }

  /**
   * Creates and starts a new EmbeddingIndexingWorkScheduler.
   *
   * @return an EmbeddingIndexingWorkScheduler.
   * @deprecated Please use createForMaterializedViewIndex instead. This will be removed after
   *     type:text index is deprecated.
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
            PER_BATCH_AUTO_EMBEDDING_MEMORY_BUDGET_BYTES);
    scheduler.start();
    return scheduler;
  }

  /**
   * Creates and starts a new EmbeddingIndexingWorkScheduler for an auto-embedding materialized view
   * index.
   *
   * @return an EmbeddingIndexingWorkScheduler.
   */
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

  static EmbeddingIndexingWorkScheduler createForMaterializedViewIndex(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      MaterializedViewCollectionMetadataCatalog matViewCollectionMetadataCatalog,
      AutoEmbeddingMemoryBudget globalBudget,
      long perBatchBudgetBytes) {
    EmbeddingIndexingWorkScheduler scheduler =
        new EmbeddingIndexingWorkScheduler(
            indexingExecutor,
            embeddingServiceManagerSupplier,
            matViewCollectionMetadataCatalog,
            IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW,
            globalBudget,
            perBatchBudgetBytes);
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

    ImmutableMap<FieldPath, EmbeddingModelConfig> modelConfigPerPath =
        modelConfigPerPathBuilder.build();
    VectorIndexDefinition vectorIndexDefinition = indexDefinition.asVectorDefinition();
    AutoEmbedFieldMapping autoEmbedMapping =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorIndexDefinition);
    ImmutableMap<FieldPath, EmbedConfigurationForBatch> embedBatchKeyPerPath =
        computeEmbedBatchKeyPerPath(vectorIndexDefinition, modelConfigPerPath);

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
                .filter(e -> containsValidDocument(e) && e.getFilterFieldUpdates().isEmpty())
                .count();
        long bytesPerDoc = embeddableDocCount > 0 ? estimatedBatchBytes / embeddableDocCount : 0;
        int subBatchSize = computeSubBatchSize(bytesPerDoc, this.perBatchBudgetBytes);
        batchFuture =
            processSubBatchesSequentially(
                batch,
                vectorIndexDefinition,
                autoEmbedMapping,
                embedBatchKeyPerPath,
                matViewCollectionMetadataOpt,
                subBatchSize);
      } else {
        // No per-batch budget: process all bundles in parallel.
        List<CompletableFuture<List<DocumentEvent>>> indexingBundles =
            embed(
                batch.events,
                vectorIndexDefinition,
                autoEmbedMapping,
                batch.priority,
                embedBatchKeyPerPath,
                matViewCollectionMetadataOpt);

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
      VectorIndexDefinition vectorIndexDefinition,
      AutoEmbedFieldMapping autoEmbedMapping,
      ImmutableMap<FieldPath, EmbedConfigurationForBatch> embedBatchKeyPerPath,
      Optional<MaterializedViewSchemaMetadata> matViewSchemaMetadata,
      int subBatchSize) {
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
                    return getEmbeddings(textsPerModel, batch.priority, vectorIndexDefinition)
                        .thenApplyAsync(
                            embeddingsPerModel ->
                                applyEmbeddingsToEvents(
                                    events,
                                    embeddingsPerModel,
                                    embedBatchKeyPerPath,
                                    autoEmbedMapping,
                                    matViewSchemaMetadata),
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
      if (batch.priority == Priority.INITIAL_SYNC_COLLECTION_SCAN
          || batch.priority == Priority.INITIAL_SYNC_CHANGE_STREAM) {
        batch.future.completeExceptionally(InitialSyncException.createFailed(ex));
      } else {
        batch.future.completeExceptionally(SteadyStateException.createNonInvalidatingResync(ex));
      }
    } else {
      batch.future.completeExceptionally(throwable);
    }
  }

  private void recordBatchErrorMetric(String exceptionType, String reason) {
    this.metricsFactory
        .counter(
            "embeddingBatchErrors",
            Tags.of("exceptionType", exceptionType, "reason", reason.toLowerCase(Locale.ROOT)))
        .increment();
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
      VectorIndexDefinition vectorIndexDefinition,
      AutoEmbedFieldMapping autoEmbedMapping,
      SchedulerQueue.Priority priority,
      ImmutableMap<FieldPath, EmbedConfigurationForBatch> embedBatchKeyPerPath,
      Optional<MaterializedViewSchemaMetadata> matViewSchemaMetadata) {
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
      CompletableFuture<Map<EmbedConfigurationForBatch, Map<String, Vector>>> embeddingsFuture =
          getEmbeddings(bundle.textsPerBatchKey(), priority, vectorIndexDefinition);
      resultFutures.add(
          // Change executor to use indexing executor here for better chaining with indexer.
          embeddingsFuture.thenApplyAsync(
              embeddingsPerModel ->
                  applyEmbeddingsToEvents(
                      events,
                      embeddingsPerModel,
                      embedBatchKeyPerPath,
                      autoEmbedMapping,
                      matViewSchemaMetadata),
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
    for (int i = 0; i < events.size(); i++) {
      DocumentEvent event = events.get(i);
      if (!containsValidDocument(event)) {
        continue;
      }

      // For filter-only updates, skip embedding transformation and pass through
      // unchanged. The event already has filterFieldUpdates set, which
      // MaterializedViewWriter will use for partial update.
      if (event.getFilterFieldUpdates().isPresent()) {
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
                  Check.isPresent(matViewSchemaMetadata, "matViewSchemaMetadata"));
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
                  // Skip text extraction for filter-only updates - they don't need embeddings
                  if (!containsValidDocument(event) || event.getFilterFieldUpdates().isPresent()) {
                    return new DocumentTexts(event, HashMultimap.create());
                  }
                  SetMultimap<EmbedConfigurationForBatch, String> textsPerBatchKey =
                      HashMultimap.create();
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
      IndexDefinition indexDefinition) {
    if (stringsToEmbedPerBatchKey.isEmpty()) {
      FLOGGER.atFine().log(
          "No strings to embed for index %s, skipping embedding call", indexDefinition.getName());
      return CompletableFuture.completedFuture(Map.of());
    }

    EmbeddingServiceManager serviceManager = this.embeddingServiceManagerSupplier.get();
    ServiceTier tier =
        priority == Priority.INITIAL_SYNC_COLLECTION_SCAN
            ? ServiceTier.COLLECTION_SCAN
            : ServiceTier.CHANGE_STREAM;

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

      List<String> orderedStrings = new ArrayList<>(stringsToEmbed);
      EmbeddingRequestContext context =
          new EmbeddingRequestContext(
              indexDefinition.getDatabase(),
              indexDefinition.getName(),
              indexDefinition.getLastObservedCollectionName(),
              batchKey.numDimensions(),
              batchKey.quantization());
      CompletableFuture<Map<String, Vector>> embeddingsFuture =
          serviceManager
              .embedAsync(orderedStrings, batchKey.modelConfig(), tier, context)
              .thenApply(
                  embeddingList -> {
                    Map<String, Vector> embeddings = new HashMap<>();
                    Check.checkState(
                        embeddingList.size() == orderedStrings.size(),
                        "Result vectors size doesn't match input text size");
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
      futuresPerBatchKey.put(batchKey, embeddingsFuture);
    }
    return FutureUtils.transposeMap(futuresPerBatchKey);
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
      if (!containsValidDocument(event) || event.getFilterFieldUpdates().isPresent()) {
        continue;
      }
      // Input document bytes (held in memory throughout the embedding call).
      totalBytes += event.getDocument().get().getByteBuffer().remaining();
      // Embedding output bytes (one float[] per auto-embed field).
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
   * EmbeddingModelConfig} and field mapping on every use.
   */
  static ImmutableMap<FieldPath, EmbedConfigurationForBatch> computeEmbedBatchKeyPerPath(
      VectorIndexDefinition vectorIndexDefinition,
      ImmutableMap<FieldPath, EmbeddingModelConfig> modelConfigPerPath) {
    VectorIndexFieldMapping mappings = vectorIndexDefinition.getMappings();
    ImmutableMap.Builder<FieldPath, EmbedConfigurationForBatch> builder = ImmutableMap.builder();
    for (var entry : modelConfigPerPath.entrySet()) {
      builder.put(
          entry.getKey(), batchConfigurationForPath(mappings, entry.getKey(), entry.getValue()));
    }
    return builder.build();
  }

  private static EmbedConfigurationForBatch batchConfigurationForPath(
      VectorIndexFieldMapping mappings, FieldPath path, EmbeddingModelConfig modelConfig) {
    VectorFieldAutoEmbeddingSpecification spec = autoEmbeddingFieldSpecForPath(mappings, path);
    return new EmbedConfigurationForBatch(
        modelConfig, spec.numDimensions(), spec.autoEmbedQuantization());
  }

  private static VectorFieldAutoEmbeddingSpecification autoEmbeddingFieldSpecForPath(
      VectorIndexFieldMapping fieldMapping, FieldPath path) {
    return fieldMapping
        .getFieldDefinition(path)
        .filter(VectorIndexFieldDefinition::isAutoEmbedField)
        .map(def -> (VectorFieldAutoEmbeddingSpecification) def.asVectorField().specification())
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "missing vector field definition for auto-embed path: " + path));
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
