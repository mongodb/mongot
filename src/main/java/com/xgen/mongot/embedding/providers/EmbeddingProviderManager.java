package com.xgen.mongot.embedding.providers;

import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ErrorHandlingConfig;
import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ServiceTier;
import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ServiceTier.CHANGE_STREAM;
import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN;
import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ServiceTier.QUERY;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.providers.clients.ClientInterface;
import com.xgen.mongot.embedding.providers.clients.EmbeddingClientFactory;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingCredentials;
import com.xgen.mongot.embedding.providers.congestion.AimdCongestionControl;
import com.xgen.mongot.embedding.providers.congestion.AimdCongestionControl.CongestionControlParams;
import com.xgen.mongot.embedding.providers.congestion.DynamicSemaphore;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.Timed;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.retry.ExponentialBackoffPolicy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeExecutor;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EmbeddingProviderManager manages all API ClientInterfaces for different ServiceTiers that belong
 * to the same EmbeddingProvider type and the same CanonicalModel (e.g., AWS_BEDROCK and
 * COHERE_EMBED_MULTILINGUAL_V3)
 */
public class EmbeddingProviderManager {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddingProviderManager.class);
  private static final int DEFAULT_BACKOFF_FACTOR = 2;
  private static final Tag QUERY_TAG = Tag.of("workload", QUERY.name());
  private static final Tag COLLECTION_SCAN_TAG = Tag.of("workload", COLLECTION_SCAN.name());
  private static final Tag CHANGE_STREAM_TAG = Tag.of("workload", CHANGE_STREAM.name());
  private final Map<ServiceTier, ClientInterface> clients;
  private final EmbeddingClientFactory embeddingClientFactory;
  private final Map<ServiceTier, FailsafeExecutor<List<VectorOrError>>> failsafeExecutors;
  private final Map<String, RateLimiter> rateLimiters;
  private final NamedScheduledExecutorService namedScheduledExecutorService;
  private final Map<ServiceTier, Counter> successfulRequestCounters = new HashMap<>();
  private final Map<ServiceTier, Counter> failedRequestCounters = new HashMap<>();
  private final Map<ServiceTier, DistributionSummary> batchSizeDistributions = new HashMap<>();
  private final Map<ServiceTier, Timer> requestTimers = new HashMap<>();
  private final Map<ServiceTier, Counter> retriedAttemptsCounters = new HashMap<>();
  private final Map<ServiceTier, DistributionSummary> attemptsPerRequestDistributions =
      new HashMap<>();

  private EmbeddingModelConfig embeddingModelConfig;
  private final AimdCongestionControl aimdCongestionControl;
  private final DynamicSemaphore congestionControlSemaphore;

  private EmbeddingModelConfig.ConsolidatedWorkloadParams getWorkloadParamsByTier(
      ServiceTier serviceTier) {
    return switch (serviceTier) {
      case QUERY -> this.embeddingModelConfig.query();
      case CHANGE_STREAM -> this.embeddingModelConfig.changeStream();
      case COLLECTION_SCAN -> this.embeddingModelConfig.collectionScan();
    };
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public EmbeddingProviderManager(
      EmbeddingModelConfig embeddingModelConfig,
      EmbeddingClientFactory embeddingClientFactory,
      NamedScheduledExecutorService namedScheduledExecutorService,
      MetricsFactory metricsFactory) {
    this(
        embeddingModelConfig,
        embeddingClientFactory,
        namedScheduledExecutorService,
        metricsFactory,
        Optional.empty());
  }

  /**
   * Creates a manager with optional AIMD congestion control parameters.
   *
   * @param congestionControl optional AIMD parameters from MMS auto-embedding config; when empty,
   *     {@link AimdCongestionControl} defaults are used.
   */
  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public EmbeddingProviderManager(
      EmbeddingModelConfig embeddingModelConfig,
      EmbeddingClientFactory embeddingClientFactory,
      NamedScheduledExecutorService namedScheduledExecutorService,
      MetricsFactory metricsFactory,
      Optional<CongestionControlParams> congestionControl) {
    initializeMetrics(embeddingModelConfig, metricsFactory);
    this.embeddingClientFactory = embeddingClientFactory;
    this.embeddingModelConfig = embeddingModelConfig;
    this.namedScheduledExecutorService = namedScheduledExecutorService;
    // AIMD congestion control for Voyage flex-tier workloads (tiers in flexTierWorkloads on Atlas)
    this.aimdCongestionControl = CongestionControlParams.toAimdCongestionControl(congestionControl);
    this.congestionControlSemaphore = new DynamicSemaphore(this.aimdCongestionControl);
    // Register gauges for monitoring congestion window
    Tag modelTag = Tag.of("canonicalModel", embeddingModelConfig.name());
    metricsFactory.objectValueGauge(
        "aimdCongestionWindow",
        this.aimdCongestionControl,
        AimdCongestionControl::getCwnd,
        Tags.of(modelTag));
    metricsFactory.objectValueGauge(
        "aimdUsedPermits",
        this.congestionControlSemaphore,
        DynamicSemaphore::getUsedPermits,
        Tags.of(modelTag));

    this.rateLimiters = new HashMap<>();
    for (var workloadParams :
        List.of(
            embeddingModelConfig.query(),
            embeddingModelConfig.collectionScan(),
            embeddingModelConfig.changeStream())) {
      String credUuid = workloadParams.credentials().getCredentialsUuID();
      int rps =
          workloadParams
              .rpsPerProvider()
              .orElse(EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER);
      this.rateLimiters.putIfAbsent(credUuid, RateLimiter.create(rps));
    }
    this.failsafeExecutors = initializeExecutors();
    this.clients =
        Arrays.stream(ServiceTier.values())
            .collect(
                CollectionUtils.toMapUnsafe(
                    Function.identity(),
                    tier ->
                        this.embeddingClientFactory.createEmbeddingClient(
                            this.embeddingModelConfig,
                            tier,
                            getWorkloadParamsByTier(tier),
                            Optional.of(this.congestionControlSemaphore))));
  }

  /**
   * Updates embedding provider manager by new EmbeddingModelConfig from Control Plane or
   * Bootstrapper.
   */
  public void updateEmbeddingProviderManager(EmbeddingModelConfig newModelConfig) {
    // TODO(CLOUDP-335133): We need to only check provider level configs in the future
    if (this.embeddingModelConfig.equals(newModelConfig)) {
      LOG.debug("Skips refreshing embedding provider manager for unchanged embeddingModelConfig");
      return;
    }
    LOG.info(
        "Updating embedding provider manager for model {} with provider {} with new config.",
        newModelConfig.name(),
        newModelConfig.provider().name());
    this.embeddingModelConfig = newModelConfig;
    // TODO(CLOUDP-312089): Update retry executor if needed from embeddingModelConfig
    Arrays.stream(ServiceTier.values())
        .forEach(
            serviceTier ->
                this.clients
                    .computeIfAbsent(
                        serviceTier,
                        tier ->
                            this.embeddingClientFactory.createEmbeddingClient(
                                this.embeddingModelConfig,
                                tier,
                                getWorkloadParamsByTier(tier),
                                Optional.of(this.congestionControlSemaphore)))
                    .updateConfig(getWorkloadParamsByTier(serviceTier)));
  }

  private Map<ServiceTier, FailsafeExecutor<List<VectorOrError>>> initializeExecutors() {
    return Arrays.stream(ServiceTier.values())
        .collect(
            Collectors.toMap(
                Function.identity(),
                serviceTier -> {
                  ErrorHandlingConfig errorHandlingConfig =
                      getWorkloadParamsByTier(serviceTier).errorHandlingConfig();
                  return Failsafe.with(
                          ExponentialBackoffPolicy.builder()
                              .initialDelay(
                                  Duration.ofMillis(errorHandlingConfig.initialRetryWaitMs))
                              .backoffFactor(DEFAULT_BACKOFF_FACTOR)
                              .maxDelay(Duration.ofMillis(errorHandlingConfig.maxRetryWaitMs))
                              .maxRetries(errorHandlingConfig.maxRetries)
                              .jitter(errorHandlingConfig.jitter)
                              .build()
                              .applyParameters(
                                  new RetryPolicy<List<VectorOrError>>()
                                      // Only retries for
                                      // EmbeddingProviderTransientException
                                      .handle(EmbeddingProviderTransientException.class)
                                      .onRetry(
                                          e -> {
                                            this.retriedAttemptsCounters
                                                .get(serviceTier)
                                                .increment();
                                            LOG.warn(
                                                "Failed embedding call in retry time: {}, retrying",
                                                e.getAttemptCount(),
                                                e.getLastFailure());
                                          })
                                      .onSuccess(
                                          e ->
                                              this.attemptsPerRequestDistributions
                                                  .get(serviceTier)
                                                  .record(e.getAttemptCount()))
                                      .onFailure(
                                          e ->
                                              this.attemptsPerRequestDistributions
                                                  .get(serviceTier)
                                                  .record(e.getAttemptCount()))))
                      .with(this.namedScheduledExecutorService);
                },
                (prev, curr) -> curr));
  }

  private void initializeMetrics(
      EmbeddingModelConfig embeddingModelConfig, MetricsFactory metricsFactory) {
    Tag providerTag = Tag.of("provider", embeddingModelConfig.provider().name());
    Tag modelTag = Tag.of("canonicalModel", embeddingModelConfig.name());
    for (var entry :
        ImmutableMap.of(
                QUERY,
                QUERY_TAG,
                COLLECTION_SCAN,
                COLLECTION_SCAN_TAG,
                CHANGE_STREAM,
                CHANGE_STREAM_TAG)
            .entrySet()) {
      // Tracks duration of each request (including all attempts)
      this.requestTimers.put(
          entry.getKey(),
          metricsFactory.timer(
              "requestLatency",
              Tags.of(providerTag, modelTag, entry.getValue()),
              0.5,
              0.75,
              0.9,
              0.99));
      this.successfulRequestCounters.put(
          entry.getKey(),
          metricsFactory.counter(
              "successfulRequests", Tags.of(providerTag, modelTag, entry.getValue())));
      this.failedRequestCounters.put(
          entry.getKey(),
          metricsFactory.counter(
              "failedRequests", Tags.of(providerTag, modelTag, entry.getValue())));
      /* Tracks the number of texts in each request */
      this.batchSizeDistributions.put(
          entry.getKey(),
          metricsFactory.summary(
              "batchSizePerRequestDistribution",
              Tags.of(providerTag, modelTag, entry.getValue()),
              0.5,
              0.75,
              0.9,
              0.99));
      // Number of retried request attempts before final success or fail.
      this.retriedAttemptsCounters.put(
          entry.getKey(),
          metricsFactory.counter(
              "retriedAttempts", Tags.of(providerTag, modelTag, entry.getValue())));
      // Number distribution of attempts per request including final success and fail.
      this.attemptsPerRequestDistributions.put(
          entry.getKey(),
          metricsFactory.summary(
              "attemptsPerRequestDistribution",
              Tags.of(providerTag, modelTag, entry.getValue()),
              0.5,
              0.75,
              0.9,
              0.99));
    }
  }

  /** Synchronous embed call without retries, used for query */
  public List<VectorOrError> embed(
      List<String> texts, ServiceTier serviceTier, EmbeddingRequestContext context)
      throws EmbeddingProviderTransientException, EmbeddingProviderNonTransientException {
    this.batchSizeDistributions.get(serviceTier).record(texts.size());
    try {
      return Timed.supplier(
          this.requestTimers.get(serviceTier),
          () -> {
            var result = this.clients.get(serviceTier).embed(texts, context);
            this.successfulRequestCounters.get(serviceTier).increment();
            return result;
          });
    } catch (Exception e) {
      this.failedRequestCounters.get(serviceTier).increment();
      throw e;
    }
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public CompletableFuture<List<VectorOrError>> embedAsync(
      List<String> texts, ServiceTier serviceTier, EmbeddingRequestContext context) {
    LOG.debug(
        "embedAsync called: model={}, provider={}, tier={}, batchSize={},"
            + " database={}, collection={}",
        this.embeddingModelConfig.name(),
        this.embeddingModelConfig.provider().name(),
        serviceTier,
        texts.size(),
        context.database(),
        context.collectionName());
    return this.failsafeExecutors
        .get(serviceTier)
        .getAsync(
            () -> {
              EmbeddingModelConfig.ConsolidatedWorkloadParams params =
                  getWorkloadParamsByTier(serviceTier);
              EmbeddingCredentials creds = params.credentials();
              if (this.rateLimiters.containsKey(creds.getCredentialsUuID())) {
                if (!this.rateLimiters.get(creds.getCredentialsUuID()).tryAcquire()) {
                  LOG.warn(
                      "Rate limit exceeded for model={}, tier={}, database={}",
                      this.embeddingModelConfig.name(),
                      serviceTier,
                      context.database());
                  throw new EmbeddingProviderTransientException(
                      "Client side rate limit exceeded, retry it later");
                }
              }
              this.batchSizeDistributions.get(serviceTier).record(texts.size());
              return Timed.supplier(
                  this.requestTimers.get(serviceTier),
                  () -> this.clients.get(serviceTier).embed(texts, context));
            })
        .whenComplete(
            (result, exception) -> {
              if (result != null) {
                this.successfulRequestCounters.get(serviceTier).increment();
                LOG.debug(
                    "embedAsync succeeded: model={}, tier={}, resultSize={}",
                    this.embeddingModelConfig.name(),
                    serviceTier,
                    result.size());
              } else if (exception != null) {
                this.failedRequestCounters.get(serviceTier).increment();
                LOG.error(
                    "embedAsync failed after all retries: model={}, tier={}, database={}",
                    this.embeddingModelConfig.name(),
                    serviceTier,
                    context.database(),
                    exception);
              }
            });
  }
}
