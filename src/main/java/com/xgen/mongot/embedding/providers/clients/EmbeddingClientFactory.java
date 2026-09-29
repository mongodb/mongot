package com.xgen.mongot.embedding.providers.clients;

import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingProvider;
import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ServiceTier;

import com.xgen.mongot.embedding.MongotMetadata;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.congestion.DynamicSemaphore;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.DeploymentEnvironment;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.Optional;
import java.util.Set;

/**
 * EmbeddingClientFactory will be used to create a variety of clients to be managed by
 * EmbeddingServiceManager
 */
public class EmbeddingClientFactory {

  /** Default service tiers for which Voyage flex tier is used when deployment is Atlas. */
  private static final Set<ServiceTier> DEFAULT_FLEX_TIERS = Set.of(ServiceTier.COLLECTION_SCAN);

  private final MeterRegistry meterRegistry;
  private final Optional<MongotMetadata> mongotMetadata;
  private final DeploymentEnvironment deploymentEnvironment;
  private final Set<ServiceTier> flexTiers;

  public EmbeddingClientFactory(
      MeterRegistry meterRegistry, DeploymentEnvironment deploymentEnvironment) {
    this(meterRegistry, Optional.empty(), deploymentEnvironment, Optional.empty());
  }

  /**
   * Constructor with optional flex tiers override (no metadata). When {@code flexTiersOverride} is
   * empty, {@link #DEFAULT_FLEX_TIERS} is used.
   */
  public EmbeddingClientFactory(
      MeterRegistry meterRegistry,
      DeploymentEnvironment deploymentEnvironment,
      Optional<Set<ServiceTier>> flexTiersOverride) {
    this(meterRegistry, Optional.empty(), deploymentEnvironment, flexTiersOverride);
  }

  /**
   * Constructor for bootstrappers with metadata.
   *
   * @param meterRegistry the meter registry for metrics
   * @param metadata mongot metadata
   */
  public EmbeddingClientFactory(
      MeterRegistry meterRegistry,
      Optional<MongotMetadata> metadata,
      DeploymentEnvironment deploymentEnvironment) {
    this(meterRegistry, metadata, deploymentEnvironment, Optional.empty());
  }

  /**
   * Constructor with optional flex tiers override. When {@code flexTiersOverride} is empty, {@link
   * #DEFAULT_FLEX_TIERS} is used (COLLECTION_SCAN only).
   *
   * @param meterRegistry the meter registry for metrics
   * @param metadata mongot metadata
   * @param deploymentEnvironment deployment environment
   * @param flexTiersOverride optional set of tiers that use Voyage flex tier when deployment is
   *     Atlas; if empty, defaults to COLLECTION_SCAN only
   */
  public EmbeddingClientFactory(
      MeterRegistry meterRegistry,
      Optional<MongotMetadata> metadata,
      DeploymentEnvironment deploymentEnvironment,
      Optional<Set<ServiceTier>> flexTiersOverride) {
    this.meterRegistry = meterRegistry;
    this.mongotMetadata = metadata;
    this.deploymentEnvironment = deploymentEnvironment;
    this.flexTiers = flexTiersOverride.map(Set::copyOf).orElse(DEFAULT_FLEX_TIERS);
  }

  /**
   * Create an embedding client for the given embedding model config, service tier and workload
   * params.
   *
   * @param embeddingModelConfig the embedding model config
   * @param serviceTier the service tier
   * @param workloadParams the workload params
   * @return the embedding client
   */
  public ClientInterface createEmbeddingClient(
      EmbeddingModelConfig embeddingModelConfig,
      ServiceTier serviceTier,
      EmbeddingModelConfig.ConsolidatedWorkloadParams workloadParams) {
    return createEmbeddingClient(
        embeddingModelConfig, serviceTier, workloadParams, Optional.empty());
  }

  /**
   * Create an embedding client, optionally wiring AIMD congestion control for Voyage.
   *
   * <p>Voyage flex tier ({@code service_tier}) is enabled only when {@code
   * congestionControlSemaphore} is present and the tier is in {@link #flexTiers} on Atlas, the
   * model allows flex ({@link EmbeddingModelConfig#useFlexTier()}), and the model supports flex
   * ({@link VoyageClient#supportsFlexTierForModel(String)}).
   *
   * @param congestionControlSemaphore when present, may be attached to Voyage clients that use flex
   *     tier; when empty, flex tier is not used regardless of deployment flex-tier configuration
   */
  public ClientInterface createEmbeddingClient(
      EmbeddingModelConfig embeddingModelConfig,
      ServiceTier serviceTier,
      EmbeddingModelConfig.ConsolidatedWorkloadParams workloadParams,
      Optional<DynamicSemaphore> congestionControlSemaphore) {
    EmbeddingProvider provider = embeddingModelConfig.provider();
    MetricsFactory metricsFactory =
        new MetricsFactory(
            "embeddingClient",
            this.meterRegistry,
            Tags.of(
                Tag.of("provider", provider.name()),
                Tag.of("canonicalModel", embeddingModelConfig.name()),
                Tag.of("workload", serviceTier.name())));
    return switch (provider) {
      case EmbeddingProvider.AWS_BEDROCK, EmbeddingProvider.COHERE ->
          throw new UnsupportedOperationException("Unsupported cloud provider: " + provider);
      case EmbeddingProvider.VOYAGE -> {
        boolean useFlexTier =
            embeddingModelConfig.useFlexTier()
                && VoyageClient.supportsFlexTierForModel(embeddingModelConfig.name())
                && this.flexTiers.contains(serviceTier)
                && this.deploymentEnvironment == DeploymentEnvironment.ATLAS
                && congestionControlSemaphore.isPresent();
        VoyageClient voyageClient =
            new VoyageClient(
                embeddingModelConfig,
                serviceTier,
                workloadParams,
                metricsFactory,
                this.mongotMetadata,
                this.deploymentEnvironment == DeploymentEnvironment.ATLAS,
                useFlexTier);
        // Set congestion semaphore only when flex tier is enabled
        // and congestion control semaphore is present
        if (useFlexTier) {
          voyageClient.setCongestionSemaphore(congestionControlSemaphore.get());
        }
        yield voyageClient;
      }
    };
  }
}
