package com.xgen.mongot.embedding.providers.configs;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingCredentials;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingProvider;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ErrorHandlingConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.TenantWorkloadCredentials;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageEmbeddingCredentials;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.WorkloadParams;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public record EmbeddingModelConfig(
    String name,
    EmbeddingProvider provider,
    boolean useFlexTier,
    ConsolidatedWorkloadParams query,
    ConsolidatedWorkloadParams changeStream,
    ConsolidatedWorkloadParams collectionScan) {
  public static EmbeddingModelConfig create(
      String model, EmbeddingProvider provider, EmbeddingConfig config) {
    return new EmbeddingModelConfig(
        model,
        provider,
        config.useFlexTier,
        consolidateWorkloadParams(
            config.getQueryParams(),
            config.getModelConfigBase(),
            config.getErrorHandlingConfigBase(),
            config.getCredentialsBase(),
            config.getProviderEndpoint(),
            config.rpsPerProvider,
            config.getQueryParams().flatMap(params -> params.tenantCredentials),
            config.tenantCredentials,
            config.isDedicatedCluster),
        consolidateWorkloadParams(
            config.getChangeStreamParams(),
            config.getModelConfigBase(),
            config.getErrorHandlingConfigBase(),
            config.getCredentialsBase(),
            config.getProviderEndpoint(),
            config.rpsPerProvider,
            config.getChangeStreamParams().flatMap(params -> params.tenantCredentials),
            config.tenantCredentials,
            config.isDedicatedCluster),
        consolidateWorkloadParams(
            config.getCollectionScanParams(),
            config.getModelConfigBase(),
            config.getErrorHandlingConfigBase(),
            config.getCredentialsBase(),
            config.getProviderEndpoint(),
            config.rpsPerProvider,
            config.getCollectionScanParams().flatMap(params -> params.tenantCredentials),
            config.tenantCredentials,
            config.isDedicatedCluster));
  }

  private static final ModelConfig DEFAULT_CONFIG =
      new VoyageModelConfig(
          // TODO(CLOUDP-335133): Model dimensions can be updated on the fly in the future
          Optional.of(1024),
          Optional.of(EmbeddingServiceConfig.TruncationOption.END),
          Optional.of(1000),
          Optional.of(120_000));
  private static final ErrorHandlingConfig DEFAULT_ERROR_CONFIG =
      new ErrorHandlingConfig(10, 200L, 50L, 0.1);
  private static final EmbeddingCredentials VOYAGE_CREDENTIALS =
      new VoyageEmbeddingCredentials("no-op-token", "2024-10-15T22:32:20.925Z");

  public static final EmbeddingModelConfig DEFAULT_EMBEDDING_MODEL_CONFIG =
      new EmbeddingModelConfig(
          "voyage-3-large",
          EmbeddingProvider.VOYAGE,
          EmbeddingConfig.DEFAULT_USE_FLEX_TIER,
          new ConsolidatedWorkloadParams(
              DEFAULT_CONFIG,
              DEFAULT_ERROR_CONFIG,
              VOYAGE_CREDENTIALS,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              true),
          new ConsolidatedWorkloadParams(
              DEFAULT_CONFIG,
              DEFAULT_ERROR_CONFIG,
              VOYAGE_CREDENTIALS,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              true),
          new ConsolidatedWorkloadParams(
              DEFAULT_CONFIG,
              DEFAULT_ERROR_CONFIG,
              VOYAGE_CREDENTIALS,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              true));

  /**
   * This method validates and consolidates all base provider configs with model level overrides and
   * returns a single ConsolidatedWorkloadParams object that has all the information our manager
   * needs.
   *
   * @param overrideWorkloadParams Workload specific parameters to used to override base values
   * @param baseModelConfig Base provider level ModelConfig
   * @param baseErrorHandlingConfig Base provider level ErrorHandlingConfig
   * @param baseCredentials Base provider level credential information
   * @param baseProviderEndpoint Base provider endpoint URL
   * @param tenantCredentials Default/dedicated cluster credentials
   * @param perTenantCredentials Per-tenant credentials map
   * @param isDedicatedCluster Whether this is a dedicated cluster
   */
  private static ConsolidatedWorkloadParams consolidateWorkloadParams(
      Optional<WorkloadParams> overrideWorkloadParams,
      ModelConfig baseModelConfig,
      ErrorHandlingConfig baseErrorHandlingConfig,
      EmbeddingCredentials baseCredentials,
      Optional<String> baseProviderEndpoint,
      Optional<Integer> rpsPerProvider,
      Optional<EmbeddingCredentials> tenantCredentials,
      Optional<Map<String, TenantWorkloadCredentials>> perTenantCredentials,
      boolean isDedicatedCluster) {
    @Var ModelConfig consolidatedModelConfig = baseModelConfig;
    @Var ErrorHandlingConfig consolidatedErrorHandlingConfig = baseErrorHandlingConfig;
    @Var EmbeddingCredentials consolidatedCredentials = baseCredentials;
    // Go over individual workload level overrides and apply them to the base params
    if (overrideWorkloadParams.isPresent()) {
      WorkloadParams currentWorkloadOverrides = overrideWorkloadParams.get();

      // Override model config if provided
      if (currentWorkloadOverrides.modelConfigOverride.isPresent()) {
        ModelConfig overrideModelConfig = currentWorkloadOverrides.modelConfigOverride.get();
        if (overrideModelConfig.getModelProvider() != baseModelConfig.getModelProvider()) {
          throw new IllegalArgumentException(
              "Model provider mismatch: "
                  + overrideModelConfig.getModelProvider()
                  + " vs "
                  + baseModelConfig.getModelProvider());
        }
        if (baseModelConfig.getModelProvider() == EmbeddingProvider.VOYAGE) {
          consolidatedModelConfig =
              consolidateVoyageModelConfig(
                  (VoyageModelConfig) baseModelConfig, (VoyageModelConfig) overrideModelConfig);
        } else {
          throw new IllegalArgumentException(
              "Unsupported model provider: " + baseModelConfig.getModelProvider());
        }
      }

      // Override error handling config if provided
      if (currentWorkloadOverrides.errorHandlingConfigOverride.isPresent()) {
        consolidatedErrorHandlingConfig =
            currentWorkloadOverrides.errorHandlingConfigOverride.get();
      }

      // Override credentials if provided
      if (currentWorkloadOverrides.credentialsOverride.isPresent()) {
        EmbeddingCredentials overrideCredentials =
            currentWorkloadOverrides.credentialsOverride.get();
        if (overrideCredentials.getCredentialProvider()
            != baseCredentials.getCredentialProvider()) {
          throw new IllegalArgumentException(
              "Credential provider mismatch: "
                  + overrideCredentials.getCredentialProvider()
                  + " vs "
                  + baseCredentials.getCredentialProvider());
        }
        if (baseCredentials.getCredentialProvider() == EmbeddingProvider.VOYAGE) {
          consolidatedCredentials = overrideCredentials;
        } else {
          throw new IllegalArgumentException(
              "Unsupported credential provider: " + baseCredentials.getCredentialProvider());
        }
      }
    }
    return new ConsolidatedWorkloadParams(
        consolidatedModelConfig,
        consolidatedErrorHandlingConfig,
        consolidatedCredentials,
        baseProviderEndpoint,
        rpsPerProvider,
        tenantCredentials,
        perTenantCredentials,
        isDedicatedCluster);
  }

  private static ModelConfig consolidateVoyageModelConfig(
      VoyageModelConfig baseModelConfig, VoyageModelConfig overrideModelConfig) {
    return new VoyageModelConfig(
        overrideModelConfig.outputDimensions.isPresent()
            ? overrideModelConfig.outputDimensions
            : baseModelConfig.outputDimensions,
        overrideModelConfig.truncation.isPresent()
            ? overrideModelConfig.truncation
            : baseModelConfig.truncation,
        overrideModelConfig.batchSize.isPresent()
            ? overrideModelConfig.batchSize
            : baseModelConfig.batchSize,
        overrideModelConfig.batchTokenLimit.isPresent()
            ? overrideModelConfig.batchTokenLimit
            : baseModelConfig.batchTokenLimit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EmbeddingModelConfig other)) {
      return false;
    }
    return Objects.equals(this.name, other.name)
        && this.provider == other.provider
        && this.useFlexTier == other.useFlexTier
        && Objects.equals(this.query, other.query)
        && Objects.equals(this.changeStream, other.changeStream)
        && Objects.equals(this.collectionScan, other.collectionScan);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.name,
        this.provider,
        this.useFlexTier,
        this.query,
        this.changeStream,
        this.collectionScan);
  }

  public record ConsolidatedWorkloadParams(
      EmbeddingServiceConfig.ModelConfig modelConfig,
      EmbeddingServiceConfig.ErrorHandlingConfig errorHandlingConfig,
      EmbeddingCredentials credentials,
      Optional<String> providerEndpoint,
      Optional<Integer> rpsPerProvider,
      // default/dedicated clustercredentials
      Optional<EmbeddingCredentials> tenantCredentials,
      Optional<Map<String, TenantWorkloadCredentials>> perTenantCredentials,
      boolean isDedicatedCluster) {

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ConsolidatedWorkloadParams other)) {
        return false;
      }
      return Objects.equals(this.modelConfig, other.modelConfig)
          && Objects.equals(this.errorHandlingConfig, other.errorHandlingConfig)
          && Objects.equals(this.credentials, other.credentials)
          && Objects.equals(this.providerEndpoint.orElse(null), other.providerEndpoint.orElse(null))
          && Objects.equals(this.rpsPerProvider, other.rpsPerProvider)
          && Objects.equals(
              this.tenantCredentials.orElse(null), other.tenantCredentials.orElse(null))
          && Objects.equals(
              this.perTenantCredentials.orElse(null), other.perTenantCredentials.orElse(null))
          && this.isDedicatedCluster == other.isDedicatedCluster;
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          this.modelConfig,
          this.errorHandlingConfig,
          this.credentials,
          this.providerEndpoint.orElse(null),
          this.tenantCredentials.orElse(null),
          this.perTenantCredentials.orElse(null),
          this.isDedicatedCluster);
    }
  }
}
