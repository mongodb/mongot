package com.xgen.mongot.embedding.providers.clients;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.MongotMetadata;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderBatchingException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderRateLimitException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageEmbeddingCredentials;
import com.xgen.mongot.embedding.providers.configs.VoyageApiSchema;
import com.xgen.mongot.embedding.providers.congestion.DynamicSemaphore;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per model and service tier VoyageClient created by using service params from Control Plane or
 * Bootstrapper config.
 */
public class VoyageClient implements ClientInterface {
  private static final Logger LOG = LoggerFactory.getLogger(VoyageClient.class);
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);
  private static final String BATCH_SIZE_TOO_LARGE_ERROR_MESSAGE =
      "Please lower the number of tokens in the batch";
  private final String inputType;
  private final String modelId;
  private URI endpoint;
  private final DistributionSummary inputTokenDistribution;
  private final Counter invalidRequestCounter;
  private final Counter congestionEventCounter;
  private final Counter aimdSuccessCounter;
  private @Nullable DynamicSemaphore congestionSemaphore;

  private boolean isDedicatedCluster;
  private final boolean attachBillingMetadata;
  private final EmbeddingServiceConfig.ServiceTier serviceTier;
  private final Optional<String> serviceTierApiValue;
  private @Nullable String credentialToken; // Dedicated cluster credentials, can be null for MTM
  private final Map<String, String> tenantCredentials = new HashMap<>(); // MTM Cluster credentials
  private final boolean truncation;

  private HttpClient voyageHttpClient;
  private final Optional<MongotMetadata> mongotMetadata;

  @VisibleForTesting
  public static final String DEFAULT_ENDPOINT = "https://api.voyageai.com/v1/embeddings";

  private static final int MAX_INDEX_NAME_LENGTH = 256;

  @VisibleForTesting public static final String VOYAGE_API_FLEX_TIER = "flex";

  VoyageClient(
      EmbeddingModelConfig embeddingModelConfig,
      EmbeddingServiceConfig.ServiceTier tier,
      EmbeddingModelConfig.ConsolidatedWorkloadParams workloadParams,
      MetricsFactory metricsFactory,
      Optional<MongotMetadata> metadata,
      boolean attachBillingMetadata) {
    this.inputType = tier == EmbeddingServiceConfig.ServiceTier.QUERY ? "query" : "document";
    // TODO(CLOUDP-370950): Support truncation parameter from configs or query time.
    // Enable truncation in indexing time only.
    this.truncation = tier != EmbeddingServiceConfig.ServiceTier.QUERY;
    this.serviceTierApiValue =
        tier == EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN
            ? Optional.of(VOYAGE_API_FLEX_TIER)
            : Optional.empty();
    this.modelId = embeddingModelConfig.name();
    this.serviceTier = tier;
    this.endpoint = URI.create(workloadParams.providerEndpoint().orElse(DEFAULT_ENDPOINT));
    this.voyageHttpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();
    this.mongotMetadata = metadata;
    this.attachBillingMetadata = attachBillingMetadata;
    this.inputTokenDistribution = metricsFactory.summary("inputTokenDistribution");
    this.invalidRequestCounter = metricsFactory.counter("invalidRequestCounter");
    this.congestionEventCounter = metricsFactory.counter("aimdCongestionEvents");
    this.aimdSuccessCounter = metricsFactory.counter("aimdSuccessfulRequests");
    updateConfig(workloadParams);

    // Dedicated clusters must have credentialToken set
    if (workloadParams.isDedicatedCluster()) {
      Check.checkState(
          this.credentialToken != null, "Dedicated cluster initialization failed: no credentials");
    }
  }

  @Override
  public List<VectorOrError> embed(List<String> inputs, EmbeddingRequestContext context)
      throws EmbeddingProviderTransientException, EmbeddingProviderNonTransientException {
    @Var Boolean isAck = null;
    try {
      // Acquire permit if congestion control is enabled
      if (this.congestionSemaphore != null) {
        try {
          this.congestionSemaphore.acquire();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new EmbeddingProviderTransientException("Interrupted while acquiring semaphore");
        }
      }

      // Voyage Service can't handle empty list or empty string for embedding, needs to filter them
      // here.
      List<String> filteredInput = inputs.stream().filter(text -> !text.isEmpty()).toList();
      if (filteredInput.isEmpty()) {
        return inputs.stream().map(ignored -> VectorOrError.EMPTY_INPUT_ERROR).toList();
      }

      // Extract tenant ID if needed and select appropriate credentials
      Optional<String> tenantId = extractTenantIdIfNeeded(context);
      String apiToken = selectApiToken(tenantId);

      HttpRequest request;
      try {
        request = buildRequest(filteredInput, apiToken, context);
      } catch (IllegalArgumentException e) {
        String message = e.getMessage();
        String cleanedMessage = message != null ? removeApiKeyFromHttpHeader(message) : null;
        IllegalArgumentException cleanedException =
            new IllegalArgumentException(cleanedMessage, e.getCause());
        LOG.error("HTTP Request Error", cleanedException);
        throw new EmbeddingProviderTransientException(cleanedException);
      }
      try {
        HttpResponse<String> response =
            this.voyageHttpClient.send(request, HttpResponse.BodyHandlers.ofString());
        var vectorResponse = extractVectorsFromResponse(response, inputs);
        isAck = true;
        return vectorResponse;
      } catch (HttpTimeoutException e) {
        LOG.error("Got timeout error when sending voyage API request", e);
        isAck = false;
        throw new EmbeddingProviderTransientException(e);
      } catch (EmbeddingProviderRateLimitException e) {
        LOG.error("Got rate-limit error when sending voyage API request", e);
        isAck = false;
        throw new EmbeddingProviderTransientException(e);
      } catch (InterruptedException | IOException e) {
        LOG.error("Got an error when sending voyage API request", e);
        throw new EmbeddingProviderTransientException(e);
      } catch (EmbeddingProviderTransientException e) {
        LOG.error("Got an error when processing voyage API response", e);
        throw e;
      }
    } finally {
      if (this.congestionSemaphore != null) {
        if (isAck != null) {
          if (isAck) {
            this.aimdSuccessCounter.increment();
          } else {
            this.congestionEventCounter.increment();
            LOG.debug("AIMD congestion signal received, reducing window");
          }
          this.congestionSemaphore.release(isAck);
        } else {
          this.congestionSemaphore.release();
        }
      }
    }
  }

  /**
   * Extract tenant ID from the database name if this is an MTM cluster. For dedicated clusters,
   * returns empty. For MTM clusters, extracts tenant ID from database string.
   */
  private Optional<String> extractTenantIdIfNeeded(EmbeddingRequestContext context) {
    // For dedicated clusters, no tenant ID needed
    if (this.isDedicatedCluster) {
      return Optional.empty();
    }

    // MTM cluster: extract tenant ID from database string
    // Database format: "tenant123_mydb" -> extract "tenant123"
    String database = context.database();
    if (database.contains("_")) {
      String tenantId = database.split("_", 2)[0];
      return Optional.of(tenantId);
    }
    return Optional.empty();
  }

  /**
   * Select the appropriate API token based on cluster type and tenant ID. For dedicated clusters,
   * use the default credentialToken. For MTM clusters, look up tenant-specific credentials.
   */
  private String selectApiToken(Optional<String> tenantId)
      throws EmbeddingProviderTransientException, IllegalStateException {
    if (this.isDedicatedCluster) {
      LOG.debug("Using dedicated cluster credentials");
      if (this.credentialToken == null) {
        throw new IllegalStateException("Dedicated cluster credentials not configured. ");
      }
      return this.credentialToken;
    } else {
      // MTM cluster: tenant ID is required
      if (tenantId.isEmpty()) {
        throw new EmbeddingProviderTransientException(
            "Unable to extract tenant ID from database name for MTM cluster. "
                + "Database name must be in format 'tenantId_dbName'.");
      }
      String tenant = tenantId.get();
      String apiToken = this.tenantCredentials.get(tenant);
      if (apiToken == null) {
        throw new EmbeddingProviderTransientException(
            String.format("Unable to find credentials for tenant: %s", tenant));
      }
      LOG.debug("Using tenant-specific credentials for tenant: {}", tenant);
      return apiToken;
    }
  }

  @Override
  public void updateConfig(EmbeddingModelConfig.ConsolidatedWorkloadParams workloadParams) {
    this.isDedicatedCluster = workloadParams.isDedicatedCluster();
    this.endpoint = URI.create(workloadParams.providerEndpoint().orElse(DEFAULT_ENDPOINT));

    if (this.isDedicatedCluster) {
      configureDedicatedClusterCredentials(workloadParams);
    } else {
      configureMultiTenantCredentials(workloadParams);
    }
  }

  @Override
  public void setCongestionSemaphore(DynamicSemaphore semaphore) {
    this.congestionSemaphore = semaphore;
  }

  /**
   * Configure credentials for a dedicated cluster. Uses the tenantCredentials field from
   * workloadParams as the default, falling back to base credentials if not present.
   */
  private void configureDedicatedClusterCredentials(
      EmbeddingModelConfig.ConsolidatedWorkloadParams workloadParams) {
    // Try tenantCredentials first, then fall back to credentials
    Optional<EmbeddingServiceConfig.EmbeddingCredentials> creds =
        workloadParams.tenantCredentials().or(() -> Optional.of(workloadParams.credentials()));

    if (creds.isEmpty()) {
      throw new IllegalStateException("Dedicated cluster configuration must have credentials");
    }

    VoyageEmbeddingCredentials credentials = (VoyageEmbeddingCredentials) creds.get();
    this.credentialToken = credentials.apiToken;
    LOG.debug("Configured dedicated cluster credentials");
  }

  /**
   * Configure credentials for a multi-tenant cluster. Populates the tenantCredentials map from
   * perTenantCredentials.
   */
  private void configureMultiTenantCredentials(
      EmbeddingModelConfig.ConsolidatedWorkloadParams workloadParams) {
    this.tenantCredentials.clear();

    if (workloadParams.perTenantCredentials().isEmpty()) {
      LOG.warn("MTM cluster configuration has no per-tenant credentials");
      return;
    }

    Map<String, EmbeddingServiceConfig.TenantWorkloadCredentials> perTenantCreds =
        workloadParams.perTenantCredentials().get();

    for (Map.Entry<String, EmbeddingServiceConfig.TenantWorkloadCredentials> entry :
        perTenantCreds.entrySet()) {
      String tenantId = entry.getKey();
      EmbeddingServiceConfig.TenantWorkloadCredentials tenantWorkloadCreds = entry.getValue();

      // Select the appropriate credentials for this service tier
      Optional<EmbeddingServiceConfig.EmbeddingCredentials> tierCredentials =
          selectCredentialsForServiceTier(tenantWorkloadCreds);

      if (tierCredentials.isPresent()) {
        VoyageEmbeddingCredentials voyageCreds = (VoyageEmbeddingCredentials) tierCredentials.get();
        this.tenantCredentials.put(tenantId, voyageCreds.apiToken);
        LOG.debug("Configured credentials for tenant: {} (tier: {})", tenantId, this.serviceTier);
      } else {
        LOG.warn(
            "No credentials found for tenant: {} and service tier: {}", tenantId, this.serviceTier);
      }
    }

    LOG.debug("Configured {} tenant credential(s) for MTM cluster", this.tenantCredentials.size());
  }

  /** Redact the API key from error messages */
  private static String removeApiKeyFromHttpHeader(String message) {
    return message.replaceAll("Bearer [^\"\\s]+", "Bearer <REDACTED-API-KEY>");
  }

  /** Select the appropriate credentials for the current service tier. */
  private Optional<EmbeddingServiceConfig.EmbeddingCredentials> selectCredentialsForServiceTier(
      EmbeddingServiceConfig.TenantWorkloadCredentials tenantWorkloadCreds) {
    return switch (this.serviceTier) {
      case QUERY -> tenantWorkloadCreds.queryCredentials;
      case CHANGE_STREAM -> tenantWorkloadCreds.changeStreamCredentials;
      case COLLECTION_SCAN -> tenantWorkloadCreds.collectionScanCredentials;
    };
  }

  private HttpRequest buildRequest(
      List<String> inputs, String apiToken, EmbeddingRequestContext context) {
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder()
            .uri(this.endpoint)
            .timeout(DEFAULT_TIMEOUT)
            .header("Authorization", "Bearer " + apiToken);

    String userAgent =
        this.mongotMetadata
            .map(
                metadata ->
                    String.format(
                        "mongot/%s (%s)", metadata.mongotVersion(), metadata.mongotHostName()))
            .orElse("mongot/UNKNOWN (UNKNOWN)");

    requestBuilder.header("User-Agent", userAgent);
    BsonDocument body =
        new VoyageApiSchema.EmbedRequest(
                this.modelId,
                this.inputType,
                inputs,
                this.truncation,
                this.attachBillingMetadata
                    ? Optional.of(buildBillingMetadata(context))
                    : Optional.empty(),
                this.serviceTierApiValue)
            .toBson();

    return requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body.toJson())).build();
  }

  /**
   * Builds billing metadata for downstream cost attribution. The key ordering must remain stable
   * since downstream pipelines hash on the serialized JSON.
   */
  private static BsonDocument buildBillingMetadata(EmbeddingRequestContext context) {
    String indexName =
        context.indexName().length() > MAX_INDEX_NAME_LENGTH
            ? context.indexName().substring(0, MAX_INDEX_NAME_LENGTH)
            : context.indexName();

    // BsonDocument preserves insertion order. Do not reorder these keys.
    BsonDocument metadata = new BsonDocument();
    metadata.put("database", new BsonString(context.database()));
    metadata.put("collectionName", new BsonString(context.collectionName()));
    metadata.put("indexName", new BsonString(indexName));
    return metadata;
  }

  private List<VectorOrError> extractVectorsFromResponse(
      HttpResponse<String> response, List<String> inputs)
      throws EmbeddingProviderTransientException,
          EmbeddingProviderBatchingException,
          HttpTimeoutException {
    int statusCode = response.statusCode();
    if (statusCode == 400) {
      String errorMessage =
          String.format(
              "Got invalid request, fail fast and give up retries." + " Response body: %s.",
              response.body());
      // TODO(CLOUDP-344098): Formalize the voyage response format or error code to avoid
      // miscategorizing the oversized batch request (VOYAGE-471)
      if (response.body().contains(BATCH_SIZE_TOO_LARGE_ERROR_MESSAGE)) {
        throw new EmbeddingProviderBatchingException(errorMessage);
      } else {
        LOG.warn(errorMessage);
        // TODO(CLOUDP-344098): Add alerts in Control Plane to notify clients directly.
        this.invalidRequestCounter.increment();
        return inputs.stream().map(ignored -> new VectorOrError(errorMessage)).toList();
      }
    }
    if (statusCode == 429) {
      throw new EmbeddingProviderRateLimitException(
          String.format("Rate limit exceeded (HTTP 429). Response body: %s", response.body()));
    }
    if (statusCode == 408) {
      throw new HttpTimeoutException(
          String.format("Timeout exception (HTTP 408). Response body: %s", response.body()));
    }
    if (statusCode > 400) {
      throw new EmbeddingProviderTransientException(
          String.format("Got non OK status from response, status code: %s", statusCode));
    }
    try {
      var embedResponse =
          VoyageApiSchema.EmbedResponse.fromBson(
              BsonDocumentParser.fromRoot(JsonCodec.fromJson(response.body()))
                  .allowUnknownFields(true)
                  .build());
      this.inputTokenDistribution.record(embedResponse.embedUsage.totalTokens);
      List<VectorOrError> results = new ArrayList<>();
      var iterator = embedResponse.data.iterator();
      for (String input : inputs) {
        if (input.isEmpty()) {
          results.add(VectorOrError.EMPTY_INPUT_ERROR);
        } else {
          results.add(new VectorOrError(iterator.next().embedding));
        }
      }
      return results;
    } catch (BsonParseException e) {
      throw new EmbeddingProviderTransientException(e);
    }
  }

  // For test only.
  @VisibleForTesting
  static void injectVoyageClient(VoyageClient target, HttpClient mockHttpClient) {
    target.voyageHttpClient = mockHttpClient;
  }
}
