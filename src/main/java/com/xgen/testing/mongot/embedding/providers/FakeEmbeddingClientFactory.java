package com.xgen.testing.mongot.embedding.providers;

import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.embedding.providers.clients.ClientInterface;
import com.xgen.mongot.embedding.providers.clients.EmbeddingClientFactory;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.congestion.DynamicSemaphore;
import com.xgen.mongot.util.DeploymentEnvironment;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FakeEmbeddingClientFactory extends EmbeddingClientFactory {
  private final Set<String> localErrorInputSet;
  private final Set<String> transientErrorInputSet;
  private final Set<String> nonTransientErrorInputSet;
  private final int maxBatchSizeCharLimit;
  private final Map<EmbeddingServiceConfig.ServiceTier, FakeEmbeddingProviderClient>
      createdClients = new ConcurrentHashMap<>();

  public FakeEmbeddingClientFactory(
      MeterRegistry meterRegistry,
      Set<String> localErrorInputSet,
      Set<String> transientErrorInputSet,
      Set<String> nonTransientErrorInputSet,
      int maxBatchSizeCharLimit) {
    super(meterRegistry, DeploymentEnvironment.TEST);
    this.localErrorInputSet = localErrorInputSet;
    this.transientErrorInputSet = transientErrorInputSet;
    this.nonTransientErrorInputSet = nonTransientErrorInputSet;
    this.maxBatchSizeCharLimit = maxBatchSizeCharLimit;
  }

  public FakeEmbeddingClientFactory(
      MeterRegistry meterRegistry,
      Set<String> localErrorInputSet,
      Set<String> transientErrorInputSet,
      Set<String> nonTransientErrorInputSet) {
    this(
        meterRegistry,
        localErrorInputSet,
        transientErrorInputSet,
        nonTransientErrorInputSet,
        Integer.MAX_VALUE);
  }

  public FakeEmbeddingClientFactory() {
    this(new SimpleMeterRegistry(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of());
  }

  @Override
  public ClientInterface createEmbeddingClient(
      EmbeddingModelConfig embeddingModelConfig,
      EmbeddingServiceConfig.ServiceTier serviceTier,
      EmbeddingModelConfig.ConsolidatedWorkloadParams workloadParams) {
    return createEmbeddingClient(
        embeddingModelConfig, serviceTier, workloadParams, Optional.empty());
  }

  @Override
  public ClientInterface createEmbeddingClient(
      EmbeddingModelConfig embeddingModelConfig,
      EmbeddingServiceConfig.ServiceTier serviceTier,
      EmbeddingModelConfig.ConsolidatedWorkloadParams workloadParams,
      Optional<DynamicSemaphore> congestionControlSemaphore) {
    FakeEmbeddingProviderClient client =
        new FakeEmbeddingProviderClient(
            this.localErrorInputSet,
            this.transientErrorInputSet,
            this.nonTransientErrorInputSet,
            this.maxBatchSizeCharLimit);
    this.createdClients.put(serviceTier, client);
    return client;
  }

  /**
   * Returns the fake client created for {@code serviceTier}, once {@code createEmbeddingClient} has
   * been called for it (e.g. via constructing an {@code EmbeddingServiceManager}).
   */
  public FakeEmbeddingProviderClient getCreatedClient(
      EmbeddingServiceConfig.ServiceTier serviceTier) {
    return this.createdClients.get(serviceTier);
  }
}
