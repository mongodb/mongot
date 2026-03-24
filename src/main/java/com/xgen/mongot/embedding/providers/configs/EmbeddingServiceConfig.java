package com.xgen.mongot.embedding.providers.configs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.bson.BsonDocument;

public class EmbeddingServiceConfig implements DocumentEncodable {
  public static final int DEFAULT_RPS_PER_PROVIDER = 30;
  public static final int MAX_RPS_PER_PROVIDER = 100;
  public static final ErrorHandlingConfig DEFAULT_ERROR_HANDLING_CONFIG =
      new ErrorHandlingConfig(50, 200L, 10000L, 0.1 /* 10% jitter */);

  public static class Fields {
    static final Field.Required<EmbeddingProvider> EMBEDDING_PROVIDER =
        Field.builder("embeddingProvider")
            .enumField(EmbeddingProvider.class)
            .asUpperUnderscore()
            .required();
    static final Field.Required<String> MODEL_NAME =
        Field.builder("modelName").stringField().required();

    static final Field.Required<EmbeddingConfig> EMBEDDING_CONFIG =
        Field.builder("config")
            .classField(EmbeddingConfig::fromBson)
            .allowUnknownFields()
            .required();

    static final Field.WithDefault<List<String>> COMPATIBLE_MODELS =
        Field.builder("compatibleModels").stringField().asList().optional().withDefault(List.of());

    static final Field.WithDefault<Integer> RPS_PER_PROVIDER =
        Field.builder("rpsPerProvider")
            .intField()
            .mustBePositive()
            .optional()
            .withDefault(DEFAULT_RPS_PER_PROVIDER);
  }

  public static EmbeddingServiceConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new EmbeddingServiceConfig(
        parser.getField(Fields.EMBEDDING_PROVIDER).unwrap(),
        parser.getField(Fields.MODEL_NAME).unwrap(),
        parser.getField(Fields.RPS_PER_PROVIDER).unwrap(),
        parser.getField(Fields.EMBEDDING_CONFIG).unwrap(),
        Set.copyOf(parser.getField(Fields.COMPATIBLE_MODELS).unwrap()));
  }

  public final EmbeddingProvider embeddingProvider;
  public final String modelName;
  public final EmbeddingConfig embeddingConfig;

  /**
   * Set of model names that are compatible with this model for query-time model override. When a
   * user specifies a model in the query, it must be in this set if the index was created with this
   * model.
   */
  public final Set<String> compatibleModels;
  public final Integer rpsPerProvider;

  @Override
  public BsonDocument toBson() {
    // Only include compatibleModels if non-empty
    BsonDocumentBuilder builder =
        BsonDocumentBuilder.builder()
            .field(Fields.EMBEDDING_PROVIDER, this.embeddingProvider)
            .field(Fields.MODEL_NAME, this.modelName)
            .field(Fields.RPS_PER_PROVIDER, this.rpsPerProvider)
            .field(Fields.EMBEDDING_CONFIG, this.embeddingConfig);
    return this.compatibleModels.isEmpty()
        ? builder.build()
        : builder.field(Fields.COMPATIBLE_MODELS, List.copyOf(this.compatibleModels)).build();
  }

  public EmbeddingServiceConfig(
      EmbeddingProvider embeddingProvider,
      String modelName,
      Integer rpsPerProvider,
      EmbeddingConfig embeddingConfig) {
    this(embeddingProvider, modelName, rpsPerProvider, embeddingConfig, Set.of());
  }

  public EmbeddingServiceConfig(
      EmbeddingProvider embeddingProvider,
      String modelName,
      Integer rpsPerProvider,
      EmbeddingConfig embeddingConfig,
      Set<String> compatibleModels) {
    Check.checkArg(
        rpsPerProvider <= MAX_RPS_PER_PROVIDER,
        "rpsPerProvider must be at most %s, got %s",
        MAX_RPS_PER_PROVIDER,
        rpsPerProvider);
    this.embeddingProvider = embeddingProvider;
    this.modelName = modelName;
    this.embeddingConfig = embeddingConfig;
    this.rpsPerProvider = rpsPerProvider;
    this.compatibleModels = compatibleModels;
  }

  public enum EmbeddingProvider {
    AWS_BEDROCK,
    COHERE,
    VOYAGE
  }

  public enum ServiceTier {
    COLLECTION_SCAN,
    CHANGE_STREAM,
    QUERY
  }

  public static class TenantWorkloadCredentials implements DocumentEncodable {
    public final Optional<EmbeddingCredentials> queryCredentials;
    public final Optional<EmbeddingCredentials> changeStreamCredentials;
    public final Optional<EmbeddingCredentials> collectionScanCredentials;

    public TenantWorkloadCredentials(
        Optional<EmbeddingCredentials> queryCredentials,
        Optional<EmbeddingCredentials> changeStreamCredentials,
        Optional<EmbeddingCredentials> collectionScanCredentials) {
      this.queryCredentials = queryCredentials;
      this.changeStreamCredentials = changeStreamCredentials;
      this.collectionScanCredentials = collectionScanCredentials;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.QUERY_CREDENTIALS, this.queryCredentials)
          .field(Fields.CHANGE_STREAM_CREDENTIALS, this.changeStreamCredentials)
          .field(Fields.COLLECTION_SCAN_CREDENTIALS, this.collectionScanCredentials)
          .build();
    }

    public static TenantWorkloadCredentials fromBson(DocumentParser parser)
        throws BsonParseException {
      return new TenantWorkloadCredentials(
          parser.getField(Fields.QUERY_CREDENTIALS).unwrap(),
          parser.getField(Fields.CHANGE_STREAM_CREDENTIALS).unwrap(),
          parser.getField(Fields.COLLECTION_SCAN_CREDENTIALS).unwrap());
    }

    public TenantWorkloadCredentials copySanitized(String placeholder) {
      return new TenantWorkloadCredentials(
          this.queryCredentials.map(creds -> creds.copySanitized(placeholder)),
          this.changeStreamCredentials.map(creds -> creds.copySanitized(placeholder)),
          this.collectionScanCredentials.map(creds -> creds.copySanitized(placeholder)));
    }

    static class Fields {
      static final Field.Optional<EmbeddingCredentials> QUERY_CREDENTIALS =
          Field.builder("query")
              .classField(EmbeddingConfigFactory::getCredentials)
              .allowUnknownFields()
              .optional()
              .noDefault();
      static final Field.Optional<EmbeddingCredentials> CHANGE_STREAM_CREDENTIALS =
          Field.builder("changeStream")
              .classField(EmbeddingConfigFactory::getCredentials)
              .allowUnknownFields()
              .optional()
              .noDefault();
      static final Field.Optional<EmbeddingCredentials> COLLECTION_SCAN_CREDENTIALS =
          Field.builder("collectionScan")
              .classField(EmbeddingConfigFactory::getCredentials)
              .allowUnknownFields()
              .optional()
              .noDefault();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TenantWorkloadCredentials that = (TenantWorkloadCredentials) o;
      return Objects.equals(this.queryCredentials.orElse(null), that.queryCredentials.orElse(null))
          && Objects.equals(
              this.changeStreamCredentials.orElse(null), that.changeStreamCredentials.orElse(null))
          && Objects.equals(
              this.collectionScanCredentials.orElse(null),
              that.collectionScanCredentials.orElse(null));
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          this.queryCredentials.orElse(null),
          this.changeStreamCredentials.orElse(null),
          this.collectionScanCredentials.orElse(null));
    }
  }

  // TODO(CLOUDP-370950): Update TruncationOption to match Voyage API
  public enum TruncationOption {
    NONE,
    START,
    END
  }

  public EmbeddingServiceConfig copySanitized(String sanitizedPlaceholder) {
    return new EmbeddingServiceConfig(
        this.embeddingProvider,
        this.modelName,
        this.rpsPerProvider,
        this.embeddingConfig.copySanitized(sanitizedPlaceholder),
        this.compatibleModels);
  }

  public static class EmbeddingConfig implements DocumentEncodable {
    /**
     * Default for {@link #useFlexTier} when absent from BSON or when using the constructor without
     * an explicit {@code useFlexTier} argument.
     */
    public static final boolean DEFAULT_USE_FLEX_TIER = true;

    public static class Fields {
      static final Field.Optional<String> REGION =
          Field.builder("region").stringField().optional().noDefault();
      static final Field.Required<ModelConfig> MODEL_CONFIG =
          Field.builder("modelConfig")
              .classField(EmbeddingConfigFactory::getModelConfig)
              .allowUnknownFields()
              .required();
      static final Field.Required<ErrorHandlingConfig> ERROR_HANDLING_CONFIG =
          Field.builder("errorHandlingConfig")
              .classField(ErrorHandlingConfig::fromBson)
              .allowUnknownFields()
              .required();
      static final Field.Required<EmbeddingCredentials> CREDENTIALS =
          Field.builder("credentials")
              .classField(EmbeddingConfigFactory::getCredentials)
              .allowUnknownFields()
              .required();
      static final Field.Optional<WorkloadParams> QUERY_PARAMS =
          Field.builder("query")
              .classField(WorkloadParams::fromBson)
              .allowUnknownFields()
              .optional()
              .noDefault();
      static final Field.Optional<WorkloadParams> CHANGE_STREAM_PARAMS =
          Field.builder("changeStream")
              .classField(WorkloadParams::fromBson)
              .allowUnknownFields()
              .optional()
              .noDefault();
      static final Field.Optional<WorkloadParams> COLLECTION_SCAN_PARAMS =
          Field.builder("collectionScan")
              .classField(WorkloadParams::fromBson)
              .allowUnknownFields()
              .optional()
              .noDefault();
      static final Field.Optional<Map<String, TenantWorkloadCredentials>> TENANT_CREDENTIALS =
          Field.builder("tenantCredentials")
              .mapOf(
                  Value.builder()
                      .classValue(TenantWorkloadCredentials::fromBson)
                      .allowUnknownFields()
                      .required())
              .optional()
              .noDefault();
      static final Field.WithDefault<Boolean> IS_DEDICATED_CLUSTER =
          Field.builder("isDedicatedCluster").booleanField().optional().withDefault(true);
      static final Field.Optional<String> PROVIDER_ENDPOINT =
          Field.builder("providerEndpoint").stringField().optional().noDefault();
      static final Field.WithDefault<Boolean> USE_FLEX_TIER =
          Field.builder("useFlexTier")
              .booleanField()
              .optional()
              .withDefault(DEFAULT_USE_FLEX_TIER);
      static final Field.Optional<Integer> RPS_PER_PROVIDER =
          Field.builder("rpsPerProvider").intField().mustBePositive().optional().noDefault();
    }

    public static EmbeddingConfig fromBson(DocumentParser parser) throws BsonParseException {
      return new EmbeddingConfig(
          parser.getField(Fields.REGION).unwrap(),
          parser.getField(Fields.MODEL_CONFIG).unwrap(),
          parser.getField(Fields.ERROR_HANDLING_CONFIG).unwrap(),
          parser.getField(Fields.CREDENTIALS).unwrap(),
          parser.getField(Fields.QUERY_PARAMS).unwrap(),
          parser.getField(Fields.COLLECTION_SCAN_PARAMS).unwrap(),
          parser.getField(Fields.CHANGE_STREAM_PARAMS).unwrap(),
          parser.getField(Fields.TENANT_CREDENTIALS).unwrap(),
          parser.getField(Fields.IS_DEDICATED_CLUSTER).unwrap(),
          parser.getField(Fields.PROVIDER_ENDPOINT).unwrap(),
          parser.getField(Fields.USE_FLEX_TIER).unwrap(),
          parser.getField(Fields.RPS_PER_PROVIDER).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.REGION, this.region)
          .field(Fields.MODEL_CONFIG, this.modelConfigBase)
          .field(Fields.ERROR_HANDLING_CONFIG, this.errorHandlingConfigBase)
          .field(Fields.CREDENTIALS, this.credentialsBase)
          .field(Fields.QUERY_PARAMS, this.queryParams)
          .field(Fields.COLLECTION_SCAN_PARAMS, this.collectionScanParams)
          .field(Fields.CHANGE_STREAM_PARAMS, this.changeStreamParams)
          .field(Fields.TENANT_CREDENTIALS, this.tenantCredentials)
          .field(Fields.IS_DEDICATED_CLUSTER, this.isDedicatedCluster)
          .field(Fields.PROVIDER_ENDPOINT, this.providerEndpoint)
          .fieldOmitDefaultValue(Fields.USE_FLEX_TIER, this.useFlexTier)
          .field(Fields.RPS_PER_PROVIDER, this.rpsPerProvider)
          .build();
    }

    public final Optional<String> region;
    public final ModelConfig modelConfigBase;
    public final ErrorHandlingConfig errorHandlingConfigBase;
    public final EmbeddingCredentials credentialsBase;
    public final Optional<WorkloadParams> queryParams;
    public final Optional<WorkloadParams> collectionScanParams;
    public final Optional<WorkloadParams> changeStreamParams;
    public final Optional<Map<String, TenantWorkloadCredentials>> tenantCredentials;
    public final Boolean isDedicatedCluster;
    public final Optional<String> providerEndpoint;
    /**
     * When true ({@link #DEFAULT_USE_FLEX_TIER}), Voyage flex tier may be used on Atlas for
     * workloads in the deployment flex-tier set. When false, flex tier is never used for this
     * model.
     */
    public final boolean useFlexTier;
    public final Optional<Integer> rpsPerProvider;

    public EmbeddingConfig(
        Optional<String> region,
        ModelConfig modelConfig,
        ErrorHandlingConfig errorHandlingConfig,
        EmbeddingCredentials credentials,
        Optional<WorkloadParams> queryParams,
        Optional<WorkloadParams> collectionScanParams,
        Optional<WorkloadParams> changeStreamParams,
        Optional<Map<String, TenantWorkloadCredentials>> tenantCredentials,
        Boolean isDedicatedCluster,
        Optional<String> providerEndpoint) {
      this(
          region,
          modelConfig,
          errorHandlingConfig,
          credentials,
          queryParams,
          collectionScanParams,
          changeStreamParams,
          tenantCredentials,
          isDedicatedCluster,
          providerEndpoint,
          DEFAULT_USE_FLEX_TIER,
          Optional.empty());
    }

    public EmbeddingConfig(
        Optional<String> region,
        ModelConfig modelConfig,
        ErrorHandlingConfig errorHandlingConfig,
        EmbeddingCredentials credentials,
        Optional<WorkloadParams> queryParams,
        Optional<WorkloadParams> collectionScanParams,
        Optional<WorkloadParams> changeStreamParams,
        Optional<Map<String, TenantWorkloadCredentials>> tenantCredentials,
        Boolean isDedicatedCluster,
        Optional<String> providerEndpoint,
        boolean useFlexTier,
        Optional<Integer> rpsPerProvider) {
      this.region = region;
      this.modelConfigBase = modelConfig;
      this.errorHandlingConfigBase = errorHandlingConfig;
      this.credentialsBase = credentials;
      this.queryParams = queryParams;
      this.collectionScanParams = collectionScanParams;
      this.changeStreamParams = changeStreamParams;
      this.tenantCredentials = tenantCredentials;
      this.isDedicatedCluster = isDedicatedCluster;
      this.providerEndpoint = providerEndpoint;
      this.useFlexTier = useFlexTier;
      this.rpsPerProvider = rpsPerProvider;
    }

    public ModelConfig getModelConfigBase() {
      return this.modelConfigBase;
    }

    public ErrorHandlingConfig getErrorHandlingConfigBase() {
      return this.errorHandlingConfigBase;
    }

    public EmbeddingCredentials getCredentialsBase() {
      return this.credentialsBase;
    }

    public Optional<WorkloadParams> getQueryParams() {
      return this.queryParams;
    }

    public Optional<WorkloadParams> getCollectionScanParams() {
      return this.collectionScanParams;
    }

    public Optional<WorkloadParams> getChangeStreamParams() {
      return this.changeStreamParams;
    }

    public Optional<Map<String, TenantWorkloadCredentials>> getTenantCredentials() {
      return this.tenantCredentials;
    }

    public Optional<String> getProviderEndpoint() {
      return this.providerEndpoint;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EmbeddingConfig that = (EmbeddingConfig) o;
      return Objects.equals(this.region.orElse(null), that.region.orElse(null))
          && Objects.equals(this.modelConfigBase, that.modelConfigBase)
          && Objects.equals(this.errorHandlingConfigBase, that.errorHandlingConfigBase)
          && Objects.equals(this.credentialsBase, that.credentialsBase)
          && Objects.equals(this.queryParams.orElse(null), that.queryParams.orElse(null))
          && Objects.equals(
              this.collectionScanParams.orElse(null), that.collectionScanParams.orElse(null))
          && Objects.equals(
              this.changeStreamParams.orElse(null), that.changeStreamParams.orElse(null))
          && Objects.equals(
              this.tenantCredentials.orElse(null), that.tenantCredentials.orElse(null))
          && Objects.equals(this.isDedicatedCluster, that.isDedicatedCluster)
          && Objects.equals(this.providerEndpoint.orElse(null), that.providerEndpoint.orElse(null))
          && this.useFlexTier == that.useFlexTier;
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          this.region.orElse(null),
          this.modelConfigBase,
          this.errorHandlingConfigBase,
          this.credentialsBase,
          this.queryParams.orElse(null),
          this.collectionScanParams.orElse(null),
          this.changeStreamParams.orElse(null),
          this.tenantCredentials.orElse(null),
          this.isDedicatedCluster,
          this.providerEndpoint.orElse(null),
          this.useFlexTier);
    }

    /**
     * Get the sanitized version of the embedding config with scrubbed credentials
     *
     * @param placeholder placeholder string for the credentials
     */
    public EmbeddingConfig copySanitized(String placeholder) {
      return new EmbeddingConfig(
          this.region,
          this.modelConfigBase,
          this.errorHandlingConfigBase,
          this.credentialsBase.copySanitized(placeholder),
          this.queryParams.isPresent()
              ? this.queryParams.get().copySanitized(placeholder)
              : Optional.empty(),
          this.collectionScanParams.isPresent()
              ? this.collectionScanParams.get().copySanitized(placeholder)
              : Optional.empty(),
          this.changeStreamParams.isPresent()
              ? this.changeStreamParams.get().copySanitized(placeholder)
              : Optional.empty(),
          this.tenantCredentials.map(
              map -> {
                Map<String, TenantWorkloadCredentials> sanitizedMap = new HashMap<>();
                map.forEach(
                    (key, value) -> sanitizedMap.put(key, value.copySanitized(placeholder)));
                return sanitizedMap;
              }),
          this.isDedicatedCluster,
          this.providerEndpoint,
          this.useFlexTier,
          this.rpsPerProvider);
    }
  }

  public interface ModelConfig extends DocumentEncodable {
    EmbeddingProvider getModelProvider();

    int getBatchSize();

    int getBatchTokenLimit();

    int getOutputDimensions();
  }

  public static class VoyageModelConfig implements ModelConfig {
    public final Optional<Integer> batchSize;
    public final Optional<Integer> batchTokenLimit;
    public final Optional<Integer> outputDimensions;
    public final Optional<TruncationOption> truncation;

    public VoyageModelConfig(
        Optional<Integer> outputDimensions,
        Optional<TruncationOption> truncation,
        Optional<Integer> batchSize,
        Optional<Integer> batchTokenLimit) {
      this.outputDimensions = outputDimensions;
      this.truncation = truncation;
      this.batchSize = batchSize;
      this.batchTokenLimit = batchTokenLimit;
    }

    @Override
    public EmbeddingProvider getModelProvider() {
      return EmbeddingProvider.VOYAGE;
    }

    @Override
    public int getBatchSize() {
      return this.batchSize.orElse(1000); // Default batch size
    }

    @Override
    public int getBatchTokenLimit() {
      return this.batchTokenLimit.orElse(120_000);
    }

    @Override
    public int getOutputDimensions() {
      return this.outputDimensions.orElse(1024);
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.BATCH_SIZE, this.batchSize)
          .field(Fields.BATCH_TOKEN_LIMIT, this.batchTokenLimit)
          .field(Fields.OUTPUT_DIMENSIONS, this.outputDimensions)
          .field(Fields.TRUNCATION, this.truncation)
          .build();
    }

    public static VoyageModelConfig fromBson(DocumentParser parser) throws BsonParseException {
      return new VoyageModelConfig(
          parser.getField(Fields.OUTPUT_DIMENSIONS).unwrap(),
          parser.getField(Fields.TRUNCATION).unwrap(),
          parser.getField(Fields.BATCH_SIZE).unwrap(),
          parser.getField(Fields.BATCH_TOKEN_LIMIT).unwrap());
    }

    public static class Fields {
      static final Field.Optional<Integer> BATCH_SIZE =
          Field.builder("batchSize").intField().optional().noDefault();
      static final Field.Optional<Integer> BATCH_TOKEN_LIMIT =
          Field.builder("batchTokenLimit").intField().optional().noDefault();
      static final Field.Optional<Integer> OUTPUT_DIMENSIONS =
          Field.builder("outputDimensions").intField().optional().noDefault();
      static final Field.Optional<TruncationOption> TRUNCATION =
          Field.builder("truncation")
              .enumField(TruncationOption.class)
              .asUpperUnderscore()
              .optional()
              .noDefault();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      VoyageModelConfig that = (VoyageModelConfig) o;
      return Objects.equals(this.batchSize.orElse(null), that.batchSize.orElse(null))
          && Objects.equals(this.batchTokenLimit.orElse(null), that.batchTokenLimit.orElse(null))
          && Objects.equals(this.outputDimensions.orElse(null), that.outputDimensions.orElse(null))
          && Objects.equals(this.truncation.orElse(null), that.truncation.orElse(null));
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          this.batchSize.orElse(null),
          this.batchTokenLimit.orElse(null),
          this.outputDimensions.orElse(null),
          this.truncation.orElse(null));
    }
  }

  public interface EmbeddingCredentials extends DocumentEncodable {
    EmbeddingProvider getCredentialProvider();

    EmbeddingCredentials copySanitized(String placeholder);

    // Used for client side rate limiting
    String getCredentialsUuID();
  }

  public static class VoyageEmbeddingCredentials implements EmbeddingCredentials {
    public final String apiToken;
    public final String expirationDate;
    public final EmbeddingProvider provider;

    public VoyageEmbeddingCredentials(String apiToken, String expirationDate) {
      this.apiToken = apiToken;
      this.expirationDate = expirationDate;
      this.provider = EmbeddingProvider.VOYAGE;
    }

    @VisibleForTesting
    public VoyageEmbeddingCredentials(String apiToken) {
      this(apiToken, "");
    }

    // TODO(CLOUDP-310761): Skips deserializing expiration date for now to avoid logging too
    // frequently.
    public static VoyageEmbeddingCredentials fromBson(DocumentParser parser)
        throws BsonParseException {
      return new VoyageEmbeddingCredentials(parser.getField(Fields.API_TOKEN).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.API_TOKEN, this.apiToken)
          .field(Fields.EXPIRATION, this.expirationDate)
          .build();
    }

    @Override
    public EmbeddingProvider getCredentialProvider() {
      return this.provider;
    }

    @Override
    public EmbeddingCredentials copySanitized(String placeholder) {
      return new VoyageEmbeddingCredentials(placeholder, this.expirationDate);
    }

    @Override
    public String getCredentialsUuID() {
      return UUID.nameUUIDFromBytes(
              Hashing.sha256().hashString(this.apiToken, StandardCharsets.UTF_8).asBytes())
          .toString();
    }

    public static class Fields {
      static final Field.Required<String> API_TOKEN =
          Field.builder("apiToken").stringField().required();
      static final Field.Required<String> EXPIRATION =
          Field.builder("expirationDate").stringField().required();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      VoyageEmbeddingCredentials that = (VoyageEmbeddingCredentials) o;
      // For VoyageEmbeddingCredentials, expiration date isn't an actionable field so we don't need
      // to consider it for comparing two of these objects
      return Objects.equals(this.apiToken, that.apiToken) && this.provider == that.provider;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.apiToken, this.provider);
    }
  }

  public static class WorkloadParams implements DocumentEncodable {
    public final Optional<ModelConfig> modelConfigOverride;
    public final Optional<ErrorHandlingConfig> errorHandlingConfigOverride;
    public final Optional<EmbeddingCredentials> credentialsOverride;
    public final Optional<EmbeddingCredentials> tenantCredentials;

    public WorkloadParams(
        Optional<ModelConfig> workloadModelConfig,
        Optional<ErrorHandlingConfig> workloadErrorHandlingConfig,
        Optional<EmbeddingCredentials> workloadCredentials,
        Optional<EmbeddingCredentials> tenantCredentials) {
      this.modelConfigOverride = workloadModelConfig;
      this.errorHandlingConfigOverride = workloadErrorHandlingConfig;
      this.credentialsOverride = workloadCredentials;
      this.tenantCredentials = tenantCredentials;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.MODEL_CONFIG_OVERRIDE, this.modelConfigOverride)
          .field(Fields.ERROR_HANDLING_CONFIG_OVERRIDE, this.errorHandlingConfigOverride)
          .field(Fields.CREDENTIALS_OVERRIDE, this.credentialsOverride)
          .field(Fields.TENANT_CREDENTIALS, this.tenantCredentials)
          .build();
    }

    public static WorkloadParams fromBson(DocumentParser parser) throws BsonParseException {
      return new WorkloadParams(
          parser.getField(Fields.MODEL_CONFIG_OVERRIDE).unwrap(),
          parser.getField(Fields.ERROR_HANDLING_CONFIG_OVERRIDE).unwrap(),
          parser.getField(Fields.CREDENTIALS_OVERRIDE).unwrap(),
          parser.getField(Fields.TENANT_CREDENTIALS).unwrap());
    }

    public Optional<WorkloadParams> copySanitized(String placeholder) {
      return Optional.of(
          new WorkloadParams(
              this.modelConfigOverride,
              this.errorHandlingConfigOverride,
              this.credentialsOverride.map(
                  embeddingCredentials -> embeddingCredentials.copySanitized(placeholder)),
              this.tenantCredentials.map(
                  embeddingCredentials -> embeddingCredentials.copySanitized(placeholder))));
    }

    static class Fields {
      static final Field.Optional<ModelConfig> MODEL_CONFIG_OVERRIDE =
          Field.builder("modelConfig")
              .classField(EmbeddingConfigFactory::getModelConfig)
              .allowUnknownFields()
              .optional()
              .noDefault();
      static final Field.Optional<ErrorHandlingConfig> ERROR_HANDLING_CONFIG_OVERRIDE =
          Field.builder("errorHandlingConfig")
              .classField(ErrorHandlingConfig::fromBson)
              .allowUnknownFields()
              .optional()
              .noDefault();
      static final Field.Optional<EmbeddingCredentials> CREDENTIALS_OVERRIDE =
          Field.builder("credentials")
              .classField(EmbeddingConfigFactory::getCredentials)
              .allowUnknownFields()
              .optional()
              .noDefault();
      static final Field.Optional<EmbeddingCredentials> TENANT_CREDENTIALS =
          Field.builder("tenantCredentials")
              .classField(EmbeddingConfigFactory::getCredentials)
              .allowUnknownFields()
              .optional()
              .noDefault();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WorkloadParams that = (WorkloadParams) o;
      return Objects.equals(
              this.modelConfigOverride.orElse(null), that.modelConfigOverride.orElse(null))
          && Objects.equals(
              this.errorHandlingConfigOverride.orElse(null),
              that.errorHandlingConfigOverride.orElse(null))
          && Objects.equals(
              this.credentialsOverride.orElse(null), that.credentialsOverride.orElse(null))
          && Objects.equals(
              this.tenantCredentials.orElse(null), that.tenantCredentials.orElse(null));
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          this.modelConfigOverride.orElse(null),
          this.errorHandlingConfigOverride.orElse(null),
          this.credentialsOverride.orElse(null),
          this.tenantCredentials.orElse(null));
    }
  }

  public static class ErrorHandlingConfig implements DocumentEncodable {
    public final Long initialRetryWaitMs;
    public final Integer maxRetries;
    public final Long maxRetryWaitMs;
    public final Double jitter;

    public ErrorHandlingConfig(
        Integer maxRetries, Long initialRetrayWaitMs, Long maxRetryWaitMs, Double jitter) {
      this.maxRetries = maxRetries;
      this.initialRetryWaitMs = initialRetrayWaitMs;
      this.maxRetryWaitMs = maxRetryWaitMs;
      this.jitter = jitter;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.MAX_RETRIES, this.maxRetries)
          .field(Fields.INITIAL_RETRY_WAIT_MS, this.initialRetryWaitMs)
          .field(Fields.MAX_RETRY_WAIT_MS, this.maxRetryWaitMs)
          .field(Fields.JITTER, this.jitter)
          .build();
    }

    public static ErrorHandlingConfig fromBson(DocumentParser parser) throws BsonParseException {
      return new ErrorHandlingConfig(
          parser.getField(Fields.MAX_RETRIES).unwrap(),
          parser.getField(Fields.INITIAL_RETRY_WAIT_MS).unwrap(),
          parser.getField(Fields.MAX_RETRY_WAIT_MS).unwrap(),
          parser.getField(Fields.JITTER).unwrap());
    }

    public static class Fields {
      static final Field.Required<Integer> MAX_RETRIES =
          Field.builder("maxRetries").intField().required();
      static final Field.Required<Long> INITIAL_RETRY_WAIT_MS =
          Field.builder("initialRetryWaitMs").longField().required();
      static final Field.Required<Long> MAX_RETRY_WAIT_MS =
          Field.builder("maxRetryWaitMs").longField().required();
      static final Field.Required<Double> JITTER = Field.builder("jitter").doubleField().required();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ErrorHandlingConfig that = (ErrorHandlingConfig) o;
      return Objects.equals(this.initialRetryWaitMs, that.initialRetryWaitMs)
          && Objects.equals(this.maxRetries, that.maxRetries)
          && Objects.equals(this.maxRetryWaitMs, that.maxRetryWaitMs)
          && Objects.equals(this.jitter, that.jitter);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          this.initialRetryWaitMs, this.maxRetries, this.maxRetryWaitMs, this.jitter);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EmbeddingServiceConfig that = (EmbeddingServiceConfig) o;
    return this.embeddingProvider == that.embeddingProvider
        && Objects.equals(this.modelName, that.modelName)
        && Objects.equals(this.embeddingConfig, that.embeddingConfig)
        && Objects.equals(this.compatibleModels, that.compatibleModels);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.embeddingProvider, this.modelName, this.embeddingConfig, this.compatibleModels);
  }
}
