package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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

  /**
   * Captures the state of the process-wide catalog so a test class can put it back the way it found
   * it: the registered model configs plus the compatible query models of each index model. Clearing
   * the catalog outright is not safe when another class in the same JVM fork still relies on models
   * it registered.
   */
  public static CatalogSnapshot snapshotCatalog() {
    Map<String, EmbeddingModelConfig> modelConfigs =
        EmbeddingModelCatalog.getAllSupportedModels().stream()
            .collect(Collectors.toMap(Function.identity(), EmbeddingModelCatalog::getModelConfig));
    return new CatalogSnapshot(modelConfigs, EmbeddingModelCatalog.snapshotCompatibleQueryModels());
  }

  /** Restores the catalog to the state captured by {@link #snapshotCatalog()}. */
  public static void restoreCatalog(CatalogSnapshot snapshot) {
    EmbeddingModelCatalog.clear();
    snapshot.modelConfigs().forEach(EmbeddingModelCatalog::registerModelConfig);
    EmbeddingModelCatalog.restoreCompatibleQueryModels(snapshot.compatibleQueryModels());
  }

  /**
   * A snapshot of the catalog state that {@link #restoreCatalog} puts back. The compatible query
   * models are captured separately because {@link EmbeddingModelCatalog#clear()} drops them along
   * with the model configs.
   */
  public record CatalogSnapshot(
      Map<String, EmbeddingModelConfig> modelConfigs,
      Map<String, Set<String>> compatibleQueryModels) {}
}
