package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import java.util.Optional;

/** Registers deterministic embedding models for tests that parse or construct autoEmbed fields. */
public final class EmbeddingModelCatalogTestUtils {

  private EmbeddingModelCatalogTestUtils() {}

  /** Clears the catalog and registers one deterministic model. */
  public static void resetCatalogAndRegisterAutoEmbedModel(String modelName) {
    EmbeddingModelCatalog.clear();
    registerAutoEmbedModel(modelName);
  }

  /** Registers a deterministic model config with dimensions and float quantization. */
  public static void registerAutoEmbedModel(String modelName) {
    EmbeddingModelCatalog.registerModelConfig(
        modelName,
        EmbeddingModelConfig.create(
            modelName,
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(1024),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
                    Optional.of(100),
                    Optional.of(120_000),
                    Optional.of("text"),
                    Optional.of(VectorAutoEmbedQuantization.FLOAT),
                    Optional.empty()),
                EmbeddingServiceConfig.DEFAULT_ERROR_HANDLING_CONFIG,
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "token-for-test", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty())));
  }
}
