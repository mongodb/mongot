package com.xgen.mongot.embedding.providers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderBatchingException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.providers.clients.EmbeddingClientFactory;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ServiceTier;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * EmbeddingServiceManager holds all EmbeddingProviderManagers for different models and maintains a
 * thread pool to execute embed calls.
 *
 * <p>Note: We assume exactly one EmbeddingProvider(e.g., VOYAGE, AWS_BEDROCK) can serve the given
 * model in the Mongot node.
 */
public class EmbeddingServiceManager {
  // TODO(CLOUDP-318302): Add AVG_CHAR_SIZE_PER_TOKEN into MongotConfigs.
  private static final double DEFAULT_AVG_CHAR_SIZE_PER_TOKEN = 3.0;
  // In most extreme cases, avg char size per token can be 0.33, like "\uffff"
  private static final double FALLBACK_AVG_CHAR_SIZE_PER_TOKEN = 0.33;

  private final NamedScheduledExecutorService namedScheduledExecutorService;
  private final EmbeddingClientFactory embeddingClientFactory;
  private final MetricsFactory metricsFactory;
  private final Counter tokenEstimationFailsCounter;

  /** Creates an EmbeddingServiceManager from a list of EmbeddingServiceConfig. */
  public EmbeddingServiceManager(
      List<EmbeddingServiceConfig> embeddingServiceConfigs,
      EmbeddingClientFactory embeddingClientFactory,
      NamedScheduledExecutorService namedScheduledExecutorService,
      MeterRegistry meterRegistry) {
    this.embeddingClientFactory = embeddingClientFactory;
    this.namedScheduledExecutorService = namedScheduledExecutorService;
    this.metricsFactory = new MetricsFactory("embeddingServiceManager", meterRegistry);
    this.tokenEstimationFailsCounter = this.metricsFactory.counter("tokenEstimationFailsCounter");

    updateEmbeddingProviderManagers(embeddingServiceConfigs);
  }

  private EmbeddingModelConfig.ConsolidatedWorkloadParams getWorkloadParamsByTier(
      EmbeddingModelConfig embeddingModelConfig, ServiceTier serviceTier) {
    return switch (serviceTier) {
      case QUERY -> embeddingModelConfig.query();
      case CHANGE_STREAM -> embeddingModelConfig.changeStream();
      case COLLECTION_SCAN -> embeddingModelConfig.collectionScan();
    };
  }

  /**
   * Populates or updates embedding provider managers by latest config from Control Plane or
   * Bootstrapper config.
   */
  public void updateEmbeddingProviderManagers(
      List<EmbeddingServiceConfig> embeddingServiceConfigs) {
    // TODO(CLOUDP-312089): Assumes that there is no provider change on the fly for the given model.
    // There is always just one provider per canonical model in the given Mongot node, but this may
    // subject to future changes
    EmbeddingServiceRegistry.updateRegisteredProviders(
        embeddingServiceConfigs,
        this.namedScheduledExecutorService,
        this.embeddingClientFactory,
        this.metricsFactory);
  }

  /**
   * Embeds input texts into a list of vector for vector search index queries without retries. This
   * should only be used in query since it doesn't split input texts into batches.
   */
  public List<VectorOrError> embed(
      List<String> inputs,
      EmbeddingModelConfig embeddingModelConfig,
      ServiceTier serviceTier,
      EmbeddingRequestContext context)
      throws EmbeddingProviderNonTransientException, EmbeddingProviderTransientException {
    EmbeddingProviderManager clientManager =
        EmbeddingServiceRegistry.getProviderManager(embeddingModelConfig.name());
    if (serviceTier != ServiceTier.QUERY) {
      throw new IllegalStateException("Sync embed call should be for Query workload only.");
    }
    return clientManager.embed(inputs, serviceTier, context);
  }

  /** Async version of embed method for indexing purposes, caller needs to handle exceptions */
  public CompletableFuture<List<VectorOrError>> embedAsync(
      List<String> allInputs,
      EmbeddingModelConfig embeddingModelConfig,
      ServiceTier serviceTier,
      EmbeddingRequestContext context)
      throws EmbeddingProviderNonTransientException, EmbeddingProviderTransientException {
    EmbeddingProviderManager clientManager =
        EmbeddingServiceRegistry.getProviderManager(embeddingModelConfig.name());
    if (clientManager == null) {
      return CompletableFuture.failedFuture(
          new EmbeddingProviderNonTransientException(
              String.format(
                  "CanonicalModel: %s not registered yet, supported models are: %s",
                  embeddingModelConfig.name(), EmbeddingModelCatalog.getAllSupportedModels())));
    }
    EmbeddingModelConfig.ConsolidatedWorkloadParams workloadSpecificParams =
        getWorkloadParamsByTier(embeddingModelConfig, serviceTier);
    EmbeddingServiceConfig.ModelConfig workloadModelConfig = workloadSpecificParams.modelConfig();

    List<CompletableFuture<List<VectorOrError>>> futures =
        // 1. batching based on options and client api
        // Set avg char size per token to be DEFAULT_AVG_CHAR_SIZE_PER_TOKEN then we can use a
        // heuristic token estimation (tokenLen = charLen/DEFAULT_AVG_CHAR_SIZE_PER_TOKEN) here
        generateBatches(
                allInputs,
                workloadModelConfig.getBatchSize(),
                workloadModelConfig.getBatchTokenLimit(),
                DEFAULT_AVG_CHAR_SIZE_PER_TOKEN)
            // 2. run api async with retries
            .stream()
            .map(
                inputList ->
                    clientManager
                        .embedAsync(inputList, serviceTier, context)
                        .exceptionallyCompose(
                            throwable ->
                                handleBatchingException(
                                    throwable,
                                    clientManager,
                                    inputList,
                                    serviceTier,
                                    context,
                                    workloadModelConfig)))
            .toList();
    // 3. collect and join results
    return FutureUtils.transposeList(futures)
        .thenApply(
            listOfList -> listOfList.stream().flatMap(List::stream).collect(Collectors.toList()));
  }

  /**
   * Handles EmbeddingProviderBatchingException by rebatching with more conservative token
   * estimation.
   */
  private CompletableFuture<List<VectorOrError>> handleBatchingException(
      Throwable throwable,
      EmbeddingProviderManager clientManager,
      List<String> inputList,
      ServiceTier serviceTier,
      EmbeddingRequestContext context,
      EmbeddingServiceConfig.ModelConfig workloadModelConfig) {

    Throwable unwrapped = unwrapThrowable(throwable);
    if (unwrapped instanceof EmbeddingProviderBatchingException) {
      // TODO(CLOUDP-297078): We may use tokenizer as long term fix.
      // Rebatches it by using FALLBACK_AVG_CHAR_SIZE_PER_TOKEN, which we
      // use to divide total char length and we just rebatch it once since
      // this token estimation should be conservative enough.
      return FutureUtils.transposeList(
              generateBatches(
                      inputList,
                      workloadModelConfig.getBatchSize(),
                      workloadModelConfig.getBatchTokenLimit(),
                      FALLBACK_AVG_CHAR_SIZE_PER_TOKEN)
                  .stream()
                  .map(subInputList -> clientManager.embedAsync(subInputList, serviceTier, context))
                  .toList())
          // Merge responses from rebatched requests into one list.
          .thenApply(
              listOfOutputLists -> listOfOutputLists.stream().flatMap(Collection::stream).toList())
          // Convert EmbeddingProviderBatchingException to
          // EmbeddingProviderNonTransientException to fail it loudly if
          // rebatch also fails
          .exceptionally(
              unrecoverableThrowable -> {
                Throwable unrecoverableEx = unwrapThrowable(unrecoverableThrowable);
                if (unrecoverableEx
                    instanceof EmbeddingProviderBatchingException batchingException) {
                  this.tokenEstimationFailsCounter.increment();
                  throw new EmbeddingProviderNonTransientException(
                      Strings.nullToEmpty(batchingException.getMessage()));
                }
                throw new CompletionException(unrecoverableEx);
              });
    }
    return CompletableFuture.failedFuture(unwrapped);
  }

  /**
   * Generates a list of subLists which has size <= batchSize and has total token length <
   * batchTokenLimit. *
   *
   * <p>Note: Using charLength/avgCharSizePerToken to estimate token size for each text.
   */
  @VisibleForTesting
  static List<List<String>> generateBatches(
      List<String> inputs, int batchSize, double batchTokenLimit, double avgCharSizePerToken) {
    List<List<String>> batches = new ArrayList<>();
    @Var List<String> currentBatch = new ArrayList<>();
    @Var double currentBatchTokenCount = 0;
    for (String input : inputs) {
      if (currentBatch.size() == batchSize) {
        batches.add(currentBatch);
        currentBatch = new ArrayList<>();
        currentBatchTokenCount = 0;
      }
      // TODO(CLOUDP-318302): Find a better way to check if batch is oversized
      if (currentBatchTokenCount + input.length() / avgCharSizePerToken >= batchTokenLimit
          && !currentBatch.isEmpty()) {
        batches.add(currentBatch);
        currentBatch = new ArrayList<>();
        currentBatchTokenCount = 0;
      }
      currentBatch.add(input);
      currentBatchTokenCount += input.length() / avgCharSizePerToken;
    }
    if (!currentBatch.isEmpty()) {
      batches.add(currentBatch);
    }
    return batches;
  }

  @Nullable
  private static Throwable unwrapThrowable(Throwable throwable) {
    return throwable instanceof CompletionException ? throwable.getCause() : throwable;
  }
}
