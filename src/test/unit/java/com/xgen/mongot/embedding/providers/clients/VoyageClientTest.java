package com.xgen.mongot.embedding.providers.clients;

import static com.xgen.mongot.embedding.providers.clients.VoyageClient.VOYAGE_API_FLEX_TIER;
import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ErrorHandlingConfig;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.congestion.AimdCongestionControl;
import com.xgen.mongot.embedding.providers.congestion.DynamicSemaphore;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.bson.Vector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class VoyageClientTest {
  private static EmbeddingRequestContext dummyContext() {
    return new EmbeddingRequestContext("testdb", "testIndex", "testCollection");
  }

  private static HttpClient createMockHttpClient() throws Exception {
    HttpClient mockClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    doReturn(200).when(mockResponse).statusCode();
    doReturn(
            "{\"object\":\"list\",\"data\":[{\"object\":\"embedding\","
                + "\"embedding\":\"AKBEPACgSbw=\",\"index\":0}],\"model\":\"voyage-3-large\","
                + "\"usage\":{\"total_tokens\":1}}")
        .when(mockResponse)
        .body();
    doReturn(mockResponse)
        .when(mockClient)
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    return mockClient;
  }

  private static EmbeddingServiceConfig.EmbeddingConfig createDedicatedClusterConfig(
      String apiToken) {
    EmbeddingServiceConfig.VoyageEmbeddingCredentials creds =
        new EmbeddingServiceConfig.VoyageEmbeddingCredentials(apiToken, "2024-10-15T22:32:20.925Z");

    EmbeddingServiceConfig.WorkloadParams queryParams =
        new EmbeddingServiceConfig.WorkloadParams(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(creds));

    return new EmbeddingServiceConfig.EmbeddingConfig(
        Optional.empty(),
        new EmbeddingServiceConfig.VoyageModelConfig(
            Optional.of(1024),
            Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
            Optional.of(100),
            Optional.of(120_000)),
        RETRY_CONFIG,
        creds,
        Optional.of(queryParams),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        true,
        Optional.empty(),
        false,
        Optional.empty());
  }

  private static EmbeddingServiceConfig.EmbeddingConfig createMtmClusterConfig(
      Map<String, EmbeddingServiceConfig.TenantWorkloadCredentials> perTenantCredentials) {
    return new EmbeddingServiceConfig.EmbeddingConfig(
        Optional.empty(), // region
        new EmbeddingServiceConfig.VoyageModelConfig(
            Optional.of(1024),
            Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
            Optional.of(100),
            Optional.of(120_000)),
        RETRY_CONFIG,
        new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
            "default-token", "2024-10-15T22:32:20.925Z"),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(perTenantCredentials),
        false,
        Optional.empty(),
        false,
        Optional.empty());
  }

  /** Helper to create tenant credentials for a specific tenant. */
  private static EmbeddingServiceConfig.TenantWorkloadCredentials createTenantCredentials(
      String apiToken) {
    EmbeddingServiceConfig.VoyageEmbeddingCredentials creds =
        new EmbeddingServiceConfig.VoyageEmbeddingCredentials(apiToken, "2024-10-15T22:32:20.925Z");
    return new EmbeddingServiceConfig.TenantWorkloadCredentials(
        Optional.of(creds), Optional.empty(), Optional.empty());
  }

  /** Helper to create and inject a VoyageClient with mocked HTTP client. */
  private static VoyageClient createMockedVoyageClient(
      EmbeddingServiceConfig.EmbeddingConfig config, HttpClient mockClient) {
    return createMockedVoyageClient(config, mockClient, false);
  }

  private static VoyageClient createMockedVoyageClient(
      EmbeddingServiceConfig.EmbeddingConfig config,
      HttpClient mockClient,
      boolean attachBillingMetadata) {
    EmbeddingModelConfig modelConfig =
        EmbeddingModelConfig.create(
            "voyage-3-large", EmbeddingServiceConfig.EmbeddingProvider.VOYAGE, config);

    VoyageClient voyageClient =
        new VoyageClient(
            modelConfig,
            EmbeddingServiceConfig.ServiceTier.QUERY,
            modelConfig.query(),
            METRICS_FACTORY,
            Optional.empty(),
            attachBillingMetadata,
            false);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);
    return voyageClient;
  }

  private static final ErrorHandlingConfig RETRY_CONFIG = new ErrorHandlingConfig(3, 10L, 10L, 0.1);
  private static final MetricsFactory METRICS_FACTORY =
      new MetricsFactory("test", new SimpleMeterRegistry());
  private static final EmbeddingModelConfig VOYAGE_3_LARGE =
      EmbeddingModelConfig.create(
          "voyage-3-large",
          EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
          new EmbeddingServiceConfig.EmbeddingConfig(
              Optional.empty(),
              new EmbeddingServiceConfig.VoyageModelConfig(
                  Optional.of(1024),
                  Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
                  Optional.of(100),
                  Optional.of(120_000)),
              RETRY_CONFIG,
              new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                  "token123", "2024-10-15T22:32:20.925Z"),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              true,
              Optional.empty(),
              false,
              Optional.empty()));

  @Test
  public void testEmbed_okStatus() throws Exception {
    HttpClient mockClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    doReturn(
            "{\"object\":\"list\",\"data\":"
                + "[{\"object\": \"embedding\", \"embedding\":"
                + "\"ACBNvQDAAj0AIOQ8AGA1vA==\", "
                + "\"index\":0},"
                + "{\"object\": \"embedding\", \"embedding\":"
                + "\"AKBEPACgSbwAwKi8AMBdvA==\", "
                + "\"index\":1},"
                + "{\"object\": \"embedding\", \"embedding\":"
                + "\"AGAFPQBA0Tsi+I081GAavQ==\", "
                + "\"index\":2}],"
                + "\"model\": \"voyage-large-3\","
                + "\"usage\": {\"total_tokens\": 10}"
                + "}")
        .when(mockResponse)
        .body();
    doReturn(mockResponse)
        .when(mockClient)
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.QUERY,
            VOYAGE_3_LARGE.query(),
            METRICS_FACTORY,
            Optional.empty(),
            false,
            false);

    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    assertEquals(
        voyageClient.embed(List.of("one", "two", "three"), dummyContext()),
        List.of(
            new VectorOrError(
                Vector.fromFloats(
                    new float[] {-0.050079346f, 0.031921387f, 0.02784729f, -0.011070251f}, NATIVE)),
            new VectorOrError(
                Vector.fromFloats(
                    new float[] {0.012001038f, -0.012306213f, -0.020599365f, -0.013534546f},
                    NATIVE)),
            new VectorOrError(
                Vector.fromFloats(
                    new float[] {0.032562256f, 0.006385803f, 0.0173302338f, -0.03769f}, NATIVE))));
  }

  @Test
  public void testEmbed_error() throws Exception {
    HttpClient mockClient = mock(HttpClient.class);
    doThrow(new IOException("failed test intentionally"))
        .when(mockClient)
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.QUERY,
            VOYAGE_3_LARGE.query(),
            METRICS_FACTORY,
            Optional.empty(),
            false,
            false);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);
    EmbeddingProviderTransientException ex =
        assertThrows(
            EmbeddingProviderTransientException.class,
            () -> voyageClient.embed(List.of("one", "two", "three"), dummyContext()));
    assertEquals("failed test intentionally", ex.getCause().getMessage());
  }

  @Test
  public void testEmbed_skipEmptyStrings() throws Exception {
    HttpClient mockClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    doReturn(
            "{\"object\":\"list\",\"data\":"
                + "[{\"object\": \"embedding\", \"embedding\":"
                + "\"ACBNvQDAAj0AIOQ8AGA1vA==\", "
                + "\"index\":0},"
                + "{\"object\": \"embedding\", \"embedding\":"
                + "\"AGAFPQBA0Tsi+I081GAavQ==\", "
                + "\"index\":1}],"
                + "\"model\": \"voyage-large-3\","
                + "\"usage\": {\"total_tokens\": 6}"
                + "}")
        .when(mockResponse)
        .body();
    doReturn(mockResponse)
        .when(mockClient)
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.QUERY,
            VOYAGE_3_LARGE.query(),
            METRICS_FACTORY,
            Optional.empty(),
            false,
            false);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    assertEquals(
        voyageClient.embed(List.of("one", "", "three"), dummyContext()),
        List.of(
            new VectorOrError(
                Vector.fromFloats(
                    new float[] {-0.050079346f, 0.031921387f, 0.02784729f, -0.011070251f}, NATIVE)),
            VectorOrError.EMPTY_INPUT_ERROR,
            new VectorOrError(
                Vector.fromFloats(
                    new float[] {0.032562256f, 0.006385803f, 0.0173302338f, -0.03769f}, NATIVE))));

    assertEquals(
        voyageClient.embed(List.of("", "", ""), dummyContext()),
        List.of(
            VectorOrError.EMPTY_INPUT_ERROR,
            VectorOrError.EMPTY_INPUT_ERROR,
            VectorOrError.EMPTY_INPUT_ERROR));
  }

  @Test
  public void testEmbed_returnsErrorOnEmptyList() throws Exception {
    HttpClient mockClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    doReturn("some error occurred").when(mockResponse).body();
    doReturn(400).when(mockResponse).statusCode();
    doReturn(mockResponse)
        .when(mockClient)
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.QUERY,
            VOYAGE_3_LARGE.query(),
            METRICS_FACTORY,
            Optional.empty(),
            false,
            false);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    assertEquals(
        voyageClient.embed(List.of("invalid input"), dummyContext()),
        List.of(
            new VectorOrError(
                "Got invalid request, fail fast and give up retries. "
                    + "Response body: some error occurred.")));
  }

  @Test
  public void customEndpoint_usesProvidedEndpoint() throws Exception {
    // Create config with custom endpoint
    String customEndpoint = "https://custom.voyageai.com/v1/embeddings";
    EmbeddingModelConfig customConfig =
        EmbeddingModelConfig.create(
            "voyage-3-large",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(1024),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
                    Optional.of(100),
                    Optional.of(120_000)),
                RETRY_CONFIG,
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "token123", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.of(customEndpoint),
                false,
                Optional.empty()));

    HttpClient mockClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    doReturn(
            "{\"object\":\"list\",\"data\":"
                + "[{\"object\": \"embedding\", \"embedding\":"
                + "\"ACBNvQDAAj0AIOQ8AGA1vA==\", "
                + "\"index\":0}],"
                + "\"model\": \"voyage-large-3\","
                + "\"usage\": {\"total_tokens\": 5}"
                + "}")
        .when(mockResponse)
        .body();
    doReturn(mockResponse)
        .when(mockClient)
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    VoyageClient voyageClient =
        new VoyageClient(
            customConfig,
            EmbeddingServiceConfig.ServiceTier.QUERY,
            customConfig.query(),
            METRICS_FACTORY,
            Optional.empty(),
            false,
            false);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    voyageClient.embed(List.of("test"), dummyContext());

    // Verify the request was sent to the custom endpoint
    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));
    assertEquals(customEndpoint, requestCaptor.getValue().uri().toString());
  }

  @Test
  public void defaultEndpoint_usedWhenNotProvided() throws Exception {
    HttpClient mockClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    doReturn(
            "{\"object\":\"list\",\"data\":"
                + "[{\"object\": \"embedding\", \"embedding\":"
                + "\"ACBNvQDAAj0AIOQ8AGA1vA==\", "
                + "\"index\":0}],"
                + "\"model\": \"voyage-large-3\","
                + "\"usage\": {\"total_tokens\": 5}"
                + "}")
        .when(mockResponse)
        .body();
    doReturn(mockResponse)
        .when(mockClient)
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.QUERY,
            VOYAGE_3_LARGE.query(),
            METRICS_FACTORY,
            Optional.empty(),
            false,
            false);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    voyageClient.embed(List.of("test"), dummyContext());

    // Verify the request was sent to the default endpoint
    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));
    assertEquals(VoyageClient.DEFAULT_ENDPOINT, requestCaptor.getValue().uri().toString());
  }

  @Test
  public void updateConfig_updatesEndpoint() throws Exception {
    // Start with default endpoint
    HttpClient mockClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    doReturn(
            "{\"object\":\"list\",\"data\":"
                + "[{\"object\": \"embedding\", \"embedding\":"
                + "\"ACBNvQDAAj0AIOQ8AGA1vA==\", "
                + "\"index\":0}],"
                + "\"model\": \"voyage-large-3\","
                + "\"usage\": {\"total_tokens\": 5}"
                + "}")
        .when(mockResponse)
        .body();
    doReturn(mockResponse)
        .when(mockClient)
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.QUERY,
            VOYAGE_3_LARGE.query(),
            METRICS_FACTORY,
            Optional.empty(),
            false,
            false);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    // Update config with new endpoint
    String newEndpoint = "https://updated.voyageai.com/v1/embeddings";
    EmbeddingModelConfig updatedConfig =
        EmbeddingModelConfig.create(
            "voyage-3-large",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(1024),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
                    Optional.of(100),
                    Optional.of(120_000)),
                RETRY_CONFIG,
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "newtoken", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.of(newEndpoint),
                false,
                Optional.empty()));

    voyageClient.updateConfig(updatedConfig.query());
    voyageClient.embed(List.of("test"), dummyContext());

    // Verify the request was sent to the updated endpoint
    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));
    assertEquals(newEndpoint, requestCaptor.getValue().uri().toString());
  }

  @Test
  public void testDedicatedCluster_usesDefaultCredentials() throws Exception {
    EmbeddingServiceConfig.EmbeddingConfig config =
        createDedicatedClusterConfig("dedicated-token-123");
    HttpClient mockClient = createMockHttpClient();
    VoyageClient voyageClient = createMockedVoyageClient(config, mockClient);

    EmbeddingRequestContext dedicatedContext =
        new EmbeddingRequestContext("tenant123_mydb", "testIndex", "testCollection");

    voyageClient.embed(List.of("test"), dedicatedContext);

    // Verify the correct credentials were used
    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));

    HttpRequest capturedRequest = requestCaptor.getValue();
    List<String> authHeaders = capturedRequest.headers().allValues("Authorization");
    assertEquals(1, authHeaders.size());
    assertEquals("Bearer dedicated-token-123", authHeaders.getFirst());
  }

  @Test
  public void testMtmCluster_usesPerTenantCredentials() throws Exception {
    Map<String, EmbeddingServiceConfig.TenantWorkloadCredentials> perTenantCreds =
        Map.of("tenant1", createTenantCredentials("tenant1-query-token"));
    EmbeddingServiceConfig.EmbeddingConfig config = createMtmClusterConfig(perTenantCreds);
    HttpClient mockClient = createMockHttpClient();
    VoyageClient voyageClient = createMockedVoyageClient(config, mockClient);

    EmbeddingRequestContext mtmContext =
        new EmbeddingRequestContext("tenant1_mydb", "testIndex", "testCollection");

    voyageClient.embed(List.of("test"), mtmContext);

    // Verify the correct tenant-specific credentials were used
    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));

    HttpRequest capturedRequest = requestCaptor.getValue();
    List<String> authHeaders = capturedRequest.headers().allValues("Authorization");
    assertEquals(1, authHeaders.size());
    assertEquals("Bearer tenant1-query-token", authHeaders.get(0));
  }

  @Test
  public void testMtmCluster_throwsWhenTenantCredentialsNotFound() {
    // MTM cluster with credentials for tenant1 only
    Map<String, EmbeddingServiceConfig.TenantWorkloadCredentials> perTenantCreds =
        Map.of("tenant1", createTenantCredentials("tenant1-query-token"));
    EmbeddingServiceConfig.EmbeddingConfig config = createMtmClusterConfig(perTenantCreds);
    VoyageClient voyageClient =
        createMockedVoyageClient(config, mock(HttpClient.class)); // No need for response

    // Request with tenant2 (which has no credentials)
    EmbeddingRequestContext unknownTenantContext =
        new EmbeddingRequestContext("tenant2_mydb", "testIndex", "testCollection");

    EmbeddingProviderTransientException ex =
        assertThrows(
            EmbeddingProviderTransientException.class,
            () -> voyageClient.embed(List.of("test"), unknownTenantContext));

    assertEquals("Unable to find credentials for tenant: tenant2", ex.getMessage());
  }

  @Test
  public void testExtractTenantId_fromDatabaseString() throws Exception {
    Map<String, EmbeddingServiceConfig.TenantWorkloadCredentials> perTenantCreds =
        Map.of("tenant123", createTenantCredentials("tenant123-query-token"));
    EmbeddingServiceConfig.EmbeddingConfig config = createMtmClusterConfig(perTenantCreds);
    HttpClient mockClient = createMockHttpClient();
    VoyageClient voyageClient = createMockedVoyageClient(config, mockClient);

    EmbeddingRequestContext context =
        new EmbeddingRequestContext("tenant123_mydb", "testIndex", "testCollection");

    // Should successfully extract tenant ID and embed
    voyageClient.embed(List.of("test"), context);
  }

  @Test
  public void testNoTenantIdExtracted_whenDedicatedCluster() throws Exception {
    EmbeddingServiceConfig.EmbeddingConfig config = createDedicatedClusterConfig("dedicated-token");
    HttpClient mockClient = createMockHttpClient();
    VoyageClient voyageClient = createMockedVoyageClient(config, mockClient);

    // Database looks like MTM format, but dedicated cluster ignores it
    EmbeddingRequestContext context =
        new EmbeddingRequestContext("tenant123_mydb", "testIndex", "testCollection");

    voyageClient.embed(List.of("test"), context);

    // Verify dedicated credentials were used
    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));
    HttpRequest capturedRequest = requestCaptor.getValue();
    List<String> authHeaders = capturedRequest.headers().allValues("Authorization");
    assertEquals("Bearer dedicated-token", authHeaders.get(0));
  }

  @Test
  public void testNoTenantId_whenNoPrefixInDatabase() {
    Map<String, EmbeddingServiceConfig.TenantWorkloadCredentials> perTenantCreds =
        Map.of("tenant1", createTenantCredentials("tenant1-query-token"));
    EmbeddingServiceConfig.EmbeddingConfig config = createMtmClusterConfig(perTenantCreds);
    VoyageClient voyageClient =
        createMockedVoyageClient(config, mock(HttpClient.class)); // No need for response

    EmbeddingRequestContext context =
        new EmbeddingRequestContext("mydb", "testIndex", "testCollection");

    EmbeddingProviderTransientException ex =
        assertThrows(
            EmbeddingProviderTransientException.class,
            () -> voyageClient.embed(List.of("test"), context));

    assertEquals(
        "Unable to extract tenant ID from database name for MTM cluster. "
            + "Database name must be in format 'tenantId_dbName'.",
        ex.getMessage());
  }

  @Test
  public void collectionScanTier_includesServiceTierFlex() throws Exception {
    HttpClient mockClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    doReturn(200).when(mockResponse).statusCode();
    doReturn(
            "{\"object\":\"list\",\"data\":[{\"object\":\"embedding\","
                + "\"embedding\":\"AKBEPACgSbw=\",\"index\":0}],\"model\":\"voyage-3-large\","
                + "\"usage\":{\"total_tokens\":1}}")
        .when(mockResponse)
        .body();
    doReturn(mockResponse)
        .when(mockClient)
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN,
            VOYAGE_3_LARGE.collectionScan(),
            METRICS_FACTORY,
            Optional.empty(),
            false,
            true);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    voyageClient.embed(List.of("test"), dummyContext());

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));

    String requestBody = extractRequestBody(requestCaptor.getValue());
    assertTrue(
        "Request body should contain service_tier for COLLECTION_SCAN tier",
        requestBody.contains("\"service_tier\""));
    assertTrue(
        "service_tier should be 'flex' for COLLECTION_SCAN tier",
        requestBody.contains("\"" + VOYAGE_API_FLEX_TIER + "\""));
  }

  @Test
  public void queryAndChangeStreamTiers_doNotIncludeServiceTier() throws Exception {
    HttpClient mockClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    doReturn(200).when(mockResponse).statusCode();
    doReturn(
            "{\"object\":\"list\",\"data\":[{\"object\":\"embedding\","
                + "\"embedding\":\"AKBEPACgSbw=\",\"index\":0}],\"model\":\"voyage-3-large\","
                + "\"usage\":{\"total_tokens\":1}}")
        .when(mockResponse)
        .body();
    doReturn(mockResponse)
        .when(mockClient)
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    // Test QUERY tier
    VoyageClient queryClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.QUERY,
            VOYAGE_3_LARGE.query(),
            METRICS_FACTORY,
            Optional.empty(),
            false,
            false);
    VoyageClient.injectVoyageClient(queryClient, mockClient);

    queryClient.embed(List.of("test"), dummyContext());

    @Var ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));

    @Var String requestBody = extractRequestBody(requestCaptor.getValue());
    assertFalse(
        "Request body should NOT contain service_tier for QUERY tier",
        requestBody.contains("service_tier"));

    // Test CHANGE_STREAM tier
    VoyageClient changeStreamClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.CHANGE_STREAM,
            VOYAGE_3_LARGE.changeStream(),
            METRICS_FACTORY,
            Optional.empty(),
            false,
            false);
    VoyageClient.injectVoyageClient(changeStreamClient, mockClient);

    changeStreamClient.embed(List.of("test"), dummyContext());

    requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient, org.mockito.Mockito.times(2)).send(requestCaptor.capture(), any());

    requestBody = extractRequestBody(requestCaptor.getValue());
    assertFalse(
        "Request body should NOT contain service_tier for CHANGE_STREAM tier",
        requestBody.contains("service_tier"));
  }

  private static String extractRequestBody(HttpRequest request) {
    return request
        .bodyPublisher()
        .map(
            publisher -> {
              java.util.concurrent.CompletableFuture<String> future =
                  new java.util.concurrent.CompletableFuture<>();
              List<java.nio.ByteBuffer> buffers =
                  java.util.Collections.synchronizedList(new ArrayList<>());
              publisher.subscribe(
                  new java.util.concurrent.Flow.Subscriber<java.nio.ByteBuffer>() {
                    @Override
                    public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
                      subscription.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(java.nio.ByteBuffer item) {
                      buffers.add(item);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                      future.completeExceptionally(throwable);
                    }

                    @Override
                    public void onComplete() {
                      java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                      for (java.nio.ByteBuffer buf : buffers) {
                        byte[] bytes = new byte[buf.remaining()];
                        buf.get(bytes);
                        out.writeBytes(bytes);
                      }
                      future.complete(out.toString(java.nio.charset.StandardCharsets.UTF_8));
                    }
                  });
              return future.join();
            })
        .orElse("");
  }

  @Test
  public void testEmbed_includesBillingMetadata() throws Exception {
    EmbeddingServiceConfig.EmbeddingConfig config = createDedicatedClusterConfig("test-token");
    HttpClient mockClient = createMockHttpClient();
    VoyageClient voyageClient = createMockedVoyageClient(config, mockClient, true);

    EmbeddingRequestContext context =
        new EmbeddingRequestContext("myDb", "myIndex", "myCollection");
    voyageClient.embed(List.of("test"), context);

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));

    String body = extractRequestBody(requestCaptor.getValue());
    BsonDocument doc = BsonDocument.parse(body);
    assertTrue(doc.containsKey("metadata"));
    BsonDocument metadata = doc.getDocument("metadata");
    assertEquals("myCollection", metadata.getString("collectionName").getValue());
    assertEquals("myDb", metadata.getString("database").getValue());
    assertEquals("myIndex", metadata.getString("indexName").getValue());
  }

  @Test
  public void testEmbed_noBillingMetadataWhenDisabled() throws Exception {
    EmbeddingServiceConfig.EmbeddingConfig config = createDedicatedClusterConfig("test-token");
    HttpClient mockClient = createMockHttpClient();
    VoyageClient voyageClient = createMockedVoyageClient(config, mockClient, false);

    EmbeddingRequestContext context =
        new EmbeddingRequestContext("myDb", "myIndex", "myCollection");
    voyageClient.embed(List.of("test"), context);

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));

    String body = extractRequestBody(requestCaptor.getValue());
    BsonDocument doc = BsonDocument.parse(body);
    assertFalse(doc.containsKey("metadata"));
  }

  @Test
  public void testEmbed_billingMetadataHasDeterministicKeyOrdering() throws Exception {
    EmbeddingServiceConfig.EmbeddingConfig config = createDedicatedClusterConfig("test-token");
    HttpClient mockClient = createMockHttpClient();
    VoyageClient voyageClient = createMockedVoyageClient(config, mockClient, true);

    EmbeddingRequestContext context =
        new EmbeddingRequestContext("myDb", "myIndex", "myCollection");
    voyageClient.embed(List.of("test"), context);

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));

    String body = extractRequestBody(requestCaptor.getValue());
    BsonDocument doc = BsonDocument.parse(body);
    BsonDocument metadata = doc.getDocument("metadata");
    List<String> keys = new ArrayList<>(metadata.keySet());
    assertEquals(List.of("database", "collectionName", "indexName"), keys);
  }

  @Test
  public void testEmbed_billingMetadataTruncatesLongIndexName() throws Exception {
    EmbeddingServiceConfig.EmbeddingConfig config = createDedicatedClusterConfig("test-token");
    HttpClient mockClient = createMockHttpClient();
    VoyageClient voyageClient = createMockedVoyageClient(config, mockClient, true);

    String longName = "a".repeat(257);
    EmbeddingRequestContext context = new EmbeddingRequestContext("db", longName, "coll");
    voyageClient.embed(List.of("test"), context);

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));

    String body = extractRequestBody(requestCaptor.getValue());
    BsonDocument doc = BsonDocument.parse(body);
    BsonDocument metadata = doc.getDocument("metadata");
    assertEquals(256, metadata.getString("indexName").getValue().length());
  }

  @Test
  public void embed_withCongestionSemaphore_incrementsSuccessCounter() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("test", registry);

    HttpClient mockClient = createMockHttpClient();
    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN,
            VOYAGE_3_LARGE.collectionScan(),
            metricsFactory,
            Optional.empty(),
            false,
            true);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    AimdCongestionControl aimd = new AimdCongestionControl();
    DynamicSemaphore semaphore = new DynamicSemaphore(aimd);
    voyageClient.setCongestionSemaphore(semaphore);

    voyageClient.embed(List.of("test"), dummyContext());

    assertEquals(1.0, registry.find("test.aimdSuccessfulRequests").counter().count(), 1E-7);
    assertEquals(0.0, registry.find("test.aimdCongestionEvents").counter().count(), 1E-7);
  }

  @Test
  public void embed_withCongestionSemaphore_incrementsCongestionCounterOn429() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("test", registry);

    HttpClient mockClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    doReturn(429).when(mockResponse).statusCode();
    doReturn("Rate limit exceeded").when(mockResponse).body();
    doReturn(mockResponse)
        .when(mockClient)
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN,
            VOYAGE_3_LARGE.collectionScan(),
            metricsFactory,
            Optional.empty(),
            false,
            true);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    AimdCongestionControl aimd = new AimdCongestionControl();
    DynamicSemaphore semaphore = new DynamicSemaphore(aimd);
    voyageClient.setCongestionSemaphore(semaphore);

    assertThrows(
        EmbeddingProviderTransientException.class,
        () -> voyageClient.embed(List.of("test"), dummyContext()));

    assertEquals(0.0, registry.find("test.aimdSuccessfulRequests").counter().count(), 1E-7);
    assertEquals(1.0, registry.find("test.aimdCongestionEvents").counter().count(), 1E-7);
  }

  @Test
  public void embed_withoutCongestionSemaphore_worksNormally() throws Exception {
    HttpClient mockClient = createMockHttpClient();
    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN,
            VOYAGE_3_LARGE.collectionScan(),
            METRICS_FACTORY,
            Optional.empty(),
            false,
            true);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    List<VectorOrError> result = voyageClient.embed(List.of("test"), dummyContext());
    assertEquals(1, result.size());
    assertTrue(result.getFirst().vector.isPresent());
  }

  @Test
  public void embed_withCongestionSemaphore_handlesIoErrorAsNonAck() throws Exception {
    HttpClient mockClient = mock(HttpClient.class);
    doThrow(new IOException("network error"))
        .when(mockClient)
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("test", registry);

    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN,
            VOYAGE_3_LARGE.collectionScan(),
            metricsFactory,
            Optional.empty(),
            false,
            true);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    AimdCongestionControl aimd = new AimdCongestionControl();
    DynamicSemaphore semaphore = new DynamicSemaphore(aimd);
    voyageClient.setCongestionSemaphore(semaphore);

    assertThrows(
        EmbeddingProviderTransientException.class,
        () -> voyageClient.embed(List.of("test"), dummyContext()));

    // IO errors should not increment either counter (isAck = null)
    assertEquals(0.0, registry.find("test.aimdSuccessfulRequests").counter().count(), 1E-7);
    assertEquals(0.0, registry.find("test.aimdCongestionEvents").counter().count(), 1E-7);
    assertEquals(0, semaphore.getUsedPermits());
  }

  @Test
  public void embed_withCongestionSemaphore_handlesEmptyInputWithoutSignaling() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("test", registry);

    // No need to mock HTTP client since we won't make any HTTP calls
    HttpClient mockClient = mock(HttpClient.class);
    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN,
            VOYAGE_3_LARGE.collectionScan(),
            metricsFactory,
            Optional.empty(),
            false,
            true);
    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    AimdCongestionControl aimd = new AimdCongestionControl();
    DynamicSemaphore semaphore = new DynamicSemaphore(aimd);
    voyageClient.setCongestionSemaphore(semaphore);

    double initialCwnd = aimd.getCwnd();

    // Call with all empty strings
    List<VectorOrError> result = voyageClient.embed(List.of("", "", ""), dummyContext());

    // Should return empty input errors
    assertEquals(3, result.size());
    assertEquals(VectorOrError.EMPTY_INPUT_ERROR, result.get(0));
    assertEquals(VectorOrError.EMPTY_INPUT_ERROR, result.get(1));
    assertEquals(VectorOrError.EMPTY_INPUT_ERROR, result.get(2));

    // Semaphore should be properly released
    assertEquals(0, semaphore.getUsedPermits());

    // No counters should be incremented (isAck = null for early return)
    assertEquals(0.0, registry.find("test.aimdSuccessfulRequests").counter().count(), 1E-7);
    assertEquals(0.0, registry.find("test.aimdCongestionEvents").counter().count(), 1E-7);

    // Window should not change
    assertEquals(initialCwnd, aimd.getCwnd(), 1e-7);
  }

  @Test
  public void embed_withUseFlexTierFalse_ignoresCongestionSemaphore() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("test", registry);

    HttpClient mockClient = createMockHttpClient();
    VoyageClient voyageClient =
        new VoyageClient(
            VOYAGE_3_LARGE,
            EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN,
            VOYAGE_3_LARGE.collectionScan(),
            metricsFactory,
            Optional.empty(),
            false,
            false); // useFlexTier = false: congestion control should not run
    VoyageClient.injectVoyageClient(voyageClient, mockClient);

    AimdCongestionControl aimd = new AimdCongestionControl();
    DynamicSemaphore semaphore = new DynamicSemaphore(aimd);
    voyageClient.setCongestionSemaphore(semaphore);

    List<VectorOrError> result = voyageClient.embed(List.of("test"), dummyContext());

    assertEquals(1, result.size());
    assertTrue(result.getFirst().vector.isPresent());
    // Congestion control is disabled when useFlexTier is false: no acquire, no counters
    assertEquals(0, semaphore.getUsedPermits());
    assertEquals(0.0, registry.find("test.aimdSuccessfulRequests").counter().count(), 1E-7);
    assertEquals(0.0, registry.find("test.aimdCongestionEvents").counter().count(), 1E-7);
  }
}
