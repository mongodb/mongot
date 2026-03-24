package com.xgen.mongot.embedding.providers;

import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN;
import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ServiceTier.QUERY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.providers.clients.ClientInterface;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.testing.mongot.embedding.providers.FakeEmbeddingClientFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

public class EmbeddingServiceManagerTest {
  private static EmbeddingRequestContext dummyContext() {
    return new EmbeddingRequestContext("testdb", "testIndex", "testCollection");
  }

  private static final EmbeddingServiceConfig.EmbeddingConfig VOYAGE_3_CONFIG =
      new EmbeddingServiceConfig.EmbeddingConfig(
          Optional.empty(),
          new EmbeddingServiceConfig.VoyageModelConfig(
              Optional.of(1024),
              Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
              Optional.of(100),
              Optional.of(120_000)),
          new EmbeddingServiceConfig.ErrorHandlingConfig(3, 100L, 200L, 0.1),
          new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
              "token123", "2024-10-15T22:32:20.925Z"),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          true,
          Optional.empty(),
          false,
          Optional.empty());
  private static final EmbeddingServiceConfig.EmbeddingConfig VOYAGE_CONFIG_FOR_REBATCHING =
      new EmbeddingServiceConfig.EmbeddingConfig(
          Optional.empty(),
          new EmbeddingServiceConfig.VoyageModelConfig(
              Optional.of(1024),
              Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
              Optional.of(100),
              Optional.of(48)),
          VOYAGE_3_CONFIG.errorHandlingConfigBase,
          VOYAGE_3_CONFIG.credentialsBase,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          true,
          Optional.empty(),
          false,
          Optional.empty());
  private static final NamedScheduledExecutorService EXECUTOR =
      Executors.singleThreadScheduledExecutor("test", new SimpleMeterRegistry());

  @After
  public void clearRegistry() {
    EmbeddingServiceRegistry.clearRegistry();
  }

  @Test
  public void testOkStatus() {
    EmbeddingServiceConfig embeddingServiceConfig =
        new EmbeddingServiceConfig(
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            "voyage-3-large",
            EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
            VOYAGE_3_CONFIG);

    EmbeddingServiceManager embeddingServiceManager =
        new EmbeddingServiceManager(
            List.of(embeddingServiceConfig),
            new FakeEmbeddingClientFactory(),
            EXECUTOR,
            new SimpleMeterRegistry(), Optional.empty());

    List<VectorOrError> result =
        embeddingServiceManager.embed(
            Arrays.asList("one", "two", "three"),
            EmbeddingModelConfig.create(
                "voyage-3-large", EmbeddingServiceConfig.EmbeddingProvider.VOYAGE, VOYAGE_3_CONFIG),
            QUERY,
            dummyContext());

    // Verify the result
    assertEquals(
        Arrays.asList(
            new VectorOrError(
                Vector.fromBytes(
                    new byte[] {
                      118, -110, -61, -83, 53, 64, -69, -128, 60, 2, 11, 58, -18, 102, -51, -120,
                      -121, 18, 50, 52, -22, 12, 110, 113, 67, -64, -83, -41, 63, -12, 49, -19
                    })),
            new VectorOrError(
                Vector.fromBytes(
                    new byte[] {
                      63, -60, -52, -2, 116, 88, 112, -30, -64, -39, -97, 113, -13, 15, -16, 101,
                      108, -115, -19, -44, 28, -63, -41, -45, -45, 118, -80, -37, -26, -123, -30,
                      -13
                    })),
            new VectorOrError(
                Vector.fromBytes(
                    new byte[] {
                      -117, 91, -99, -80, -63, 61, -78, 66, 86, -56, 41, -86, 54, 74, -87, 12,
                      109, 46, -70, 49, -117, -110, 50, -92, -85, -109, 19, -71, 84, -45, 85, 95
                    }))),
        result);
  }

  @Test
  public void testNonTransientErrorThrow() {

    EmbeddingServiceConfig embeddingServiceConfig =
        new EmbeddingServiceConfig(
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            "voyage-3-large",
            EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
            VOYAGE_3_CONFIG);

    EmbeddingServiceManager embeddingServiceManager =
        new EmbeddingServiceManager(
            List.of(embeddingServiceConfig),
            new FakeEmbeddingClientFactory(
                new SimpleMeterRegistry(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of("one")),
            EXECUTOR,
            new SimpleMeterRegistry(), Optional.empty());

    // Verify the result
    Throwable ex =
        assertThrows(
            EmbeddingProviderNonTransientException.class,
            () ->
                embeddingServiceManager.embed(
                    Arrays.asList("one", "two", "three"),
                    EmbeddingModelConfig.create(
                        "voyage-3-large",
                        EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
                        VOYAGE_3_CONFIG),
                    QUERY,
                    dummyContext()));
    assertEquals("Non transient error", ex.getMessage());
  }

  @Test
  public void testLocalErrorInVectorOrError() {
    EmbeddingServiceConfig embeddingServiceConfig =
        new EmbeddingServiceConfig(
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            "voyage-3-large",
            EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
            VOYAGE_3_CONFIG);

    EmbeddingServiceManager embeddingServiceManager =
        new EmbeddingServiceManager(
            List.of(embeddingServiceConfig),
            new FakeEmbeddingClientFactory(
                new SimpleMeterRegistry(),
                ImmutableSet.of("invalid token"),
                ImmutableSet.of(),
                ImmutableSet.of()),
            EXECUTOR,
            new SimpleMeterRegistry(), Optional.empty());

    List<VectorOrError> result =
        embeddingServiceManager.embed(
            Arrays.asList("invalid token", "invalid token", "three"),
            EmbeddingModelConfig.create(
                "voyage-3-large", EmbeddingServiceConfig.EmbeddingProvider.VOYAGE, VOYAGE_3_CONFIG),
            QUERY, dummyContext());
    // Verify the result
    assertEquals(
        Arrays.asList(
            new VectorOrError("invalid token"),
            new VectorOrError("invalid token"),
            new VectorOrError(
                Vector.fromBytes(
                    new byte[] {
                      -117, 91, -99, -80, -63, 61, -78, 66, 86, -56, 41, -86, 54, 74, -87, 12, 109,
                      46, -70, 49, -117, -110, 50, -92, -85, -109, 19, -71, 84, -45, 85, 95
                    }))),
        result);
  }

  @Test
  public void testTransientErrorThrowWithRetries() {
    EmbeddingServiceConfig embeddingServiceConfig =
        new EmbeddingServiceConfig(
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            "voyage-3-large",
            EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
            VOYAGE_3_CONFIG);

    MeterRegistry registry = new SimpleMeterRegistry();
    EmbeddingServiceManager embeddingServiceManager =
        new EmbeddingServiceManager(
            List.of(embeddingServiceConfig),
            new FakeEmbeddingClientFactory(
                new SimpleMeterRegistry(),
                ImmutableSet.of(),
                ImmutableSet.of("retryable token"),
                ImmutableSet.of()),
            EXECUTOR,
            registry,
            Optional.empty());

    Throwable ex =
        assertThrows(
                CompletionException.class,
                () ->
                    embeddingServiceManager
                        .embedAsync(
                            Arrays.asList("retryable token", "retryable token", "three"),
                            EmbeddingModelConfig.create(
                                "voyage-3-large",
                                EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
                                VOYAGE_3_CONFIG),
                            QUERY, dummyContext())
                        .join())
            .getCause();

    assertTrue(ex instanceof EmbeddingProviderTransientException);

    assertEquals("Transient error", ex.getMessage());

    Map<String, Double> expectedMetrics =
        Map.of(
            "embeddingServiceManager.failedRequests",
            1.0,
            "embeddingServiceManager.successfulRequests",
            0.0);
    List<Counter> counters =
        registry.getMeters().stream()
            .filter(meter -> expectedMetrics.containsKey(meter.getId().getName()))
            .map(meter -> (Counter) meter)
            .toList();
    assertEquals(6, counters.size());
    for (Counter counter : counters) {
      assertEquals("voyage-3-large", counter.getId().getTag("canonicalModel"));
      assertEquals(
          EmbeddingServiceConfig.EmbeddingProvider.VOYAGE.name(),
          counter.getId().getTag("provider"));
      String workload = counter.getId().getTag("workload");
      if (workload.equals(QUERY.name())) {
        assertEquals(
            counter.getId().getName(),
            expectedMetrics.get(counter.getId().getName()),
            counter.count(),
            1E-7);
      }
    }
  }

  @Test
  public void testNonTransientErrorThrowWithRebatches() {
    EmbeddingServiceConfig embeddingServiceConfig =
        new EmbeddingServiceConfig(
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            "voyage-3-large",
            EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
            VOYAGE_3_CONFIG);

    MeterRegistry registry = new SimpleMeterRegistry();
    EmbeddingServiceManager embeddingServiceManager =
        new EmbeddingServiceManager(
            List.of(embeddingServiceConfig),
            new FakeEmbeddingClientFactory(
                new SimpleMeterRegistry(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                47), // only allow single text to pass.
            EXECUTOR,
            registry,
            Optional.empty());

    List<VectorOrError> result =
        embeddingServiceManager
            .embedAsync(
                Arrays.asList(
                    // 48 chars text will be estimated as 12 tokens initially
                    "long text with estimated token length as twelve",
                    "LONG text with estimated token length as twelve",
                    "LONG TEXT with estimated token length as twelve"),
                EmbeddingModelConfig.create(
                    "voyage-3-large",
                    EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
                    VOYAGE_CONFIG_FOR_REBATCHING),
                COLLECTION_SCAN, dummyContext())
            .join();

    // Verify the result
    assertEquals(
        Arrays.asList(
            new VectorOrError(
                Vector.fromBytes(
                    new byte[] {
                      -16, 96, 95, -25, -15, -96, -124, -22, -84, -51, -63, -116, -32, -21, -83,
                      111, -9, -47, 17, 113, -23, -105, 89, 43, 12, 73, 26, 90, -111, 54, -108, 114
                    })),
            new VectorOrError(
                Vector.fromBytes(
                    new byte[] {
                      -98, 48, 65, 113, 40, -121, 127, -3, 57, -40, -112, 21, -63, -68, -95, -10,
                      -122, -125, 106, -54, 68, 5, 110, -98, -39, 47, -86, -47, -30, -2, 77, -20
                    })),
            new VectorOrError(
                Vector.fromBytes(
                    new byte[] {
                      109, 5, 48, -6, 127, 8, 73, -73, -59, 90, 102, -82, -16, -22, -74, -65, 5,
                      -18, -78, -1, -97, -49, -76, -86, -63, 8, 79, 18, 107, -96, -17, -68
                    }))),
        result);

    Map<String, Double> expectedMetrics =
        Map.of(
            "embeddingServiceManager.failedRequests",
            1.0, // First request fails fast and will be rebatched.
            "embeddingServiceManager.successfulRequests",
            3.0); // After being rebacthed, there will be 3 requests.
    List<Counter> counters =
        registry.getMeters().stream()
            .filter(meter -> expectedMetrics.containsKey(meter.getId().getName()))
            .map(meter -> (Counter) meter)
            .toList();
    assertEquals(6, counters.size());
    for (Counter counter : counters) {
      assertEquals("voyage-3-large", counter.getId().getTag("canonicalModel"));
      assertEquals(
          EmbeddingServiceConfig.EmbeddingProvider.VOYAGE.name(),
          counter.getId().getTag("provider"));
      String workload = counter.getId().getTag("workload");
      if (workload.equals(COLLECTION_SCAN.name())) {
        assertEquals(
            counter.getId().getName(),
            expectedMetrics.get(counter.getId().getName()),
            counter.count(),
            1E-7);
      }
    }
  }

  @Test
  public void testNonTransientErrorThrowWithRebatches_fails() {
    EmbeddingServiceConfig embeddingServiceConfig =
        new EmbeddingServiceConfig(
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            "voyage-3-large",
            EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
            VOYAGE_3_CONFIG);

    MeterRegistry registry = new SimpleMeterRegistry();
    EmbeddingServiceManager embeddingServiceManager =
        new EmbeddingServiceManager(
            List.of(embeddingServiceConfig),
            new FakeEmbeddingClientFactory(
                new SimpleMeterRegistry(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                46), // only allow single text to pass.
            EXECUTOR,
            registry,
            Optional.empty());

    Throwable ex =
        assertThrows(
                CompletionException.class,
                () ->
                    embeddingServiceManager
                        .embedAsync(
                            Arrays.asList(
                                // 48 chars text will be estimated as 12 tokens initially
                                "long text with estimated token length as twelve",
                                "LONG text with estimated token length as twelve",
                                "LONG TEXT with estimated token length as twelve"),
                            EmbeddingModelConfig.create(
                                "voyage-3-large",
                                EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
                                VOYAGE_CONFIG_FOR_REBATCHING),
                            COLLECTION_SCAN, dummyContext())
                        .join())
            .getCause();
    assertTrue(ex instanceof EmbeddingProviderNonTransientException);
    assertEquals("Please lower the number of tokens in the batch.", ex.getMessage());
  }

  @Test
  public void testTransientErrorThrowByRateLimiter() {
    EmbeddingServiceConfig.EmbeddingConfig disabledRetriesConfig =
        new EmbeddingServiceConfig.EmbeddingConfig(
            Optional.empty(),
            new EmbeddingServiceConfig.VoyageModelConfig(
                Optional.of(1024),
                Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
                Optional.of(100),
                Optional.of(120_000)),
            new EmbeddingServiceConfig.ErrorHandlingConfig(0, 50L, 100L, 0.1),
            new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                "token123", "2024-10-15T22:32:20.925Z"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            true,
            Optional.empty(),
            false,
            Optional.empty());
    EmbeddingServiceConfig embeddingServiceConfig =
        new EmbeddingServiceConfig(
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            "voyage-3-large",
            EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
            disabledRetriesConfig);

    EmbeddingServiceManager embeddingServiceManager =
        new EmbeddingServiceManager(
            List.of(embeddingServiceConfig),
            new FakeEmbeddingClientFactory(),
            EXECUTOR,
            new SimpleMeterRegistry(), Optional.empty());

    Throwable ex =
        assertThrows(
                CompletionException.class,
                () -> {
                  // Sends 2x QPS limit to get error.
                  for (int i = 0; i <= EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER * 2; i++) {
                    embeddingServiceManager
                        .embedAsync(
                            List.of("test"),
                            EmbeddingModelConfig.create(
                                "voyage-3-large",
                                EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
                                disabledRetriesConfig),
                            QUERY, dummyContext())
                        .join();
                  }
                })
            .getCause();
    assertTrue(ex instanceof EmbeddingProviderTransientException);
    assertEquals("Client side rate limit exceeded, retry it later", ex.getMessage());
  }

  @Test
  public void testUpdatingEmbeddingProviderManagersWithNewProvider() {
    EmbeddingServiceConfig.EmbeddingConfig disabledRetriesConfig =
        new EmbeddingServiceConfig.EmbeddingConfig(
            Optional.empty(),
            new EmbeddingServiceConfig.VoyageModelConfig(
                Optional.of(1024),
                Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
                Optional.of(100),
                Optional.of(120_000)),
            new EmbeddingServiceConfig.ErrorHandlingConfig(0, 50L, 100L, 0.1),
            new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                "token123", "2024-10-15T22:32:20.925Z"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            true,
            Optional.empty(),
            false,
            Optional.empty());
    EmbeddingServiceConfig embeddingServiceConfig =
        new EmbeddingServiceConfig(
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            "voyage-3-large",
            EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
            disabledRetriesConfig);

    EmbeddingServiceManager embeddingServiceManager =
        new EmbeddingServiceManager(
            List.of(),
            new FakeEmbeddingClientFactory(),
            EXECUTOR,
            new SimpleMeterRegistry(),
            Optional.empty());

    // Verify the result
    Throwable ex =
        assertThrows(
            EmbeddingProviderNonTransientException.class,
            () ->
                embeddingServiceManager.embed(
                    Arrays.asList("one", "two", "three"),
                    EmbeddingModelConfig.create(
                        "voyage-3-large",
                        EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
                        disabledRetriesConfig),
                    QUERY,
                    dummyContext()));
    assertEquals(
        "CanonicalModel: voyage-3-large not registered yet, supported models are: []",
        ex.getMessage());
    // Test config updates.
    embeddingServiceManager.updateEmbeddingProviderManagers(List.of(embeddingServiceConfig));
    List<VectorOrError> result =
        embeddingServiceManager.embed(
            List.of("one"),
            EmbeddingModelConfig.create(
                "voyage-3-large",
                EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
                disabledRetriesConfig),
            QUERY, dummyContext());
    // Verify the result
    assertEquals(
        List.of(
            new VectorOrError(
                Vector.fromBytes(
                    new byte[] {
                      118, -110, -61, -83, 53, 64, -69, -128, 60, 2, 11, 58, -18, 102, -51, -120,
                      -121, 18, 50, 52, -22, 12, 110, 113, 67, -64, -83, -41, 63, -12, 49, -19
                    }))),
        result);
  }

  @Test
  public void testUpdatingEmbeddingProviderManagersWithExistingProvider() {
    EmbeddingServiceConfig embeddingServiceConfig1 =
        new EmbeddingServiceConfig(
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            "voyage-3-large",
            EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
            VOYAGE_3_CONFIG);

    FakeEmbeddingClientFactory embeddingClientFactory = mock(FakeEmbeddingClientFactory.class);
    ClientInterface mockClient = mock(ClientInterface.class);
    doNothing()
        .when(mockClient)
        .updateConfig(any(EmbeddingModelConfig.ConsolidatedWorkloadParams.class));
    doReturn(mockClient)
        .when(embeddingClientFactory)
        .createEmbeddingClient(
            any(EmbeddingModelConfig.class),
            any(EmbeddingServiceConfig.ServiceTier.class),
            any(EmbeddingModelConfig.ConsolidatedWorkloadParams.class),
            any(Optional.class));

    EmbeddingServiceManager embeddingServiceManager =
        new EmbeddingServiceManager(
            List.of(embeddingServiceConfig1),
            embeddingClientFactory,
            EXECUTOR,
            new SimpleMeterRegistry(), Optional.empty());

    // Create one client per service tier
    verify(embeddingClientFactory, times(3))
        .createEmbeddingClient(
            any(EmbeddingModelConfig.class),
            any(EmbeddingServiceConfig.ServiceTier.class),
            any(EmbeddingModelConfig.ConsolidatedWorkloadParams.class),
            any(Optional.class));
    // Reset invocation count before next set of assertions
    Mockito.reset(embeddingClientFactory, mockClient);

    embeddingServiceManager.updateEmbeddingProviderManagers(List.of(embeddingServiceConfig1));

    // Expects embeddingClientFactory not creating embedding client with identical config
    verify(embeddingClientFactory, times(0))
        .createEmbeddingClient(
            any(EmbeddingModelConfig.class),
            eq(COLLECTION_SCAN),
            any(EmbeddingModelConfig.ConsolidatedWorkloadParams.class),
            any(Optional.class));
    verify(mockClient, times(0))
        .updateConfig(any(EmbeddingModelConfig.ConsolidatedWorkloadParams.class));

    EmbeddingServiceConfig.EmbeddingCredentials sameApiTokenDifferentTimestampCredentials =
        new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
            "token123", "2025-11-01T22:32:20.925Z");
    EmbeddingServiceConfig embeddingServiceConfig2 =
        new EmbeddingServiceConfig(
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            "voyage-3-large",
            EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(1024),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
                    Optional.of(100),
                    Optional.of(120_000)),
                new EmbeddingServiceConfig.ErrorHandlingConfig(3, 100L, 200L, 0.1),
                sameApiTokenDifferentTimestampCredentials,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty()));
    embeddingServiceManager.updateEmbeddingProviderManagers(List.of(embeddingServiceConfig2));
    verify(embeddingClientFactory, times(0))
        .createEmbeddingClient(
            any(EmbeddingModelConfig.class),
            eq(COLLECTION_SCAN),
            any(EmbeddingModelConfig.ConsolidatedWorkloadParams.class),
            any(Optional.class));
    // We don't expect a call to updateConfig() for our ClientInterface since we effectively have
    // the same credentials (only varying by different expiration time which doesn't matter for
    // VoyageEmbeddingCredentials)
    verify(mockClient, times(0))
        .updateConfig(any(EmbeddingModelConfig.ConsolidatedWorkloadParams.class));

    EmbeddingServiceConfig.EmbeddingCredentials updatedCredential =
        VOYAGE_3_CONFIG.getCredentialsBase().copySanitized("apiToken");
    EmbeddingServiceConfig embeddingServiceConfig3 =
        new EmbeddingServiceConfig(
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            "voyage-3-large",
            EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(1024),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
                    Optional.of(100),
                    Optional.of(120_000)),
                new EmbeddingServiceConfig.ErrorHandlingConfig(50, 200L, 10000L, 0.1),
                updatedCredential,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty()));
    embeddingServiceManager.updateEmbeddingProviderManagers(List.of(embeddingServiceConfig3));

    // We don't create a new client since this is an in-place update of a part of the config
    verify(embeddingClientFactory, times(0))
        .createEmbeddingClient(
            any(EmbeddingModelConfig.class),
            eq(COLLECTION_SCAN),
            any(EmbeddingModelConfig.ConsolidatedWorkloadParams.class),
            any(Optional.class));
    // We only expect the underlying config to be updated for all service tiers
    verify(mockClient, times(3))
        .updateConfig(any(EmbeddingModelConfig.ConsolidatedWorkloadParams.class));
  }

  @Test
  public void generateBatchesTest() {
    List<String> inputs = IntStream.range(0, 100).mapToObj(ignored -> "dummy").toList();
    // each batch has 5.0 tokens, not hit batchTokenLimit, batch controlled by batch size.
    @Var List<List<String>> batches = EmbeddingServiceManager.generateBatches(inputs, 3, 10.0, 3.0);
    assertEquals(34, batches.size());
    batches.subList(0, 32).forEach(batch -> assertEquals(3, batch.size()));
    assertEquals(1, batches.getLast().size());

    // batch size 8 can have 10 total tokens (5 chars/4.0*8), greater than tokens limit (9.0
    // tokens), so each batch can only have max 7 texts.
    batches = EmbeddingServiceManager.generateBatches(inputs, 8, 9.0, 4.0);
    assertEquals(15, batches.size());
    batches.subList(0, 13).forEach(batch -> assertEquals(7, batch.size()));
    assertEquals(2, batches.getLast().size());

    // Even 1 text is beyond batch token limit, so each batch can only has 1 text
    batches = EmbeddingServiceManager.generateBatches(inputs, 8, .99, 3.0);
    assertEquals(100, batches.size());
    batches.forEach(batch -> assertEquals(1, batch.size()));

    // Mix the small text and large text, some large text is beyond batch token limit
    batches =
        EmbeddingServiceManager.generateBatches(
            List.of("2 tokens", "1tok", "2 tokens", "2 tokens", "1tok", "1tok", ".5", ".5", ".5"),
            3,
            3.01,
            4.0);
    assertEquals(
        List.of(
            List.of("2 tokens", "1tok"), // 3 tokens, can't take one more
            List.of("2 tokens"), // 2 tokens, can't take one more
            List.of("2 tokens", "1tok"), // 3 tokens, can't take one more
            List.of("1tok", ".5", ".5"), // 2.5 tokens, can take one more but already has 3 texts
            List.of(".5")), // remaining text in a single batch
        batches);
  }
}
