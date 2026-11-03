package com.xgen.mongot.config.provider.community.embedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageEmbeddingCredentials;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

public class EmbeddingServiceManagerConfigTest {

  @Test
  public void loadEmbeddingServiceConfig_withValidCredentials_returnsConfig() {
    EmbeddingServiceManagerConfig.VoyageCredentials credentials =
        new EmbeddingServiceManagerConfig.VoyageCredentials(
            new VoyageEmbeddingCredentials("test-query-key"),
            new VoyageEmbeddingCredentials("test-indexing-key"));

    Optional<EmbeddingServiceManagerConfig> result =
        EmbeddingServiceManagerConfig.loadEmbeddingServiceConfig(Optional.of(credentials));

    assertTrue("Expected non-empty Optional", result.isPresent());
    EmbeddingServiceManagerConfig config = result.get();

    // Verify that configs were loaded from the YAML file
    assertFalse("Expected at least one config", config.configs().isEmpty());

    // Verify expected models are present (from embedding-service-configs.yml)
    Set<String> expectedModels =
        Set.of("voyage-4-large", "voyage-4", "voyage-4-lite", "voyage-code-3");
    Set<String> actualModels =
        config.configs().stream()
            .map(serviceConfig -> serviceConfig.modelName)
            .collect(java.util.stream.Collectors.toSet());

    assertEquals("Expected all models from YAML to be loaded", expectedModels, actualModels);

    // Verify each config has the correct provider
    for (EmbeddingServiceConfig serviceConfig : config.configs()) {
      assertEquals(
          "Expected VOYAGE provider for model: " + serviceConfig.modelName,
          EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
          serviceConfig.embeddingProvider);

      // Verify credentials were injected
      assertNotNull(
          "Expected credentials to be present for model: " + serviceConfig.modelName,
          serviceConfig.embeddingConfig.credentialsBase);
    }
  }

  @Test
  public void loadEmbeddingServiceConfig_voyage4_loadsSupportedDimensions() {
    EmbeddingServiceManagerConfig.VoyageCredentials credentials =
        new EmbeddingServiceManagerConfig.VoyageCredentials(
            new VoyageEmbeddingCredentials("test-query-key"),
            new VoyageEmbeddingCredentials("test-indexing-key"));

    EmbeddingServiceManagerConfig config =
        EmbeddingServiceManagerConfig.loadEmbeddingServiceConfig(Optional.of(credentials))
            .orElseThrow();

    EmbeddingServiceConfig voyage4 =
        config.configs().stream()
            .filter(serviceConfig -> serviceConfig.modelName.equals("voyage-4"))
            .findFirst()
            .orElseThrow();
    assertEquals(
        Optional.of(List.of(256, 512, 1024, 2048)),
        ((EmbeddingServiceConfig.VoyageModelConfig) voyage4.embeddingConfig.modelConfigBase)
            .supportedDimensions);
  }

  @Test
  public void loadEmbeddingServiceConfig_noCredentials_returnsEmpty() {
    Optional<EmbeddingServiceManagerConfig> result =
        EmbeddingServiceManagerConfig.loadEmbeddingServiceConfig(Optional.empty());

    assertTrue(
        "Expected empty Optional when credentials not configured (graceful degradation)",
        result.isEmpty());
  }
}
