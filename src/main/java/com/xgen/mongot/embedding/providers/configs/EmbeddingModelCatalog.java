package com.xgen.mongot.embedding.providers.configs;

import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class EmbeddingModelCatalog {
  private static final Map<String, EmbeddingModelConfig> REGISTERED_MODEL_CONFIGS =
      new ConcurrentHashMap<>();
  private static final AtomicBoolean MAT_VIEW_ENABLED = new AtomicBoolean(false);

  /**
   * A map of each index model to the set of query models that are compatible with it. When a user
   * specifies a model in the query, it must be in this set for the index model used to create the
   * index. This is populated from the compatibleModels field in the embedding service config.
   */
  private static final Map<String, Set<String>> COMPATIBLE_QUERY_MODEL_MAP =
      new ConcurrentHashMap<>();

  /**
   * Hardcoded default compatible models for known model families. This is used when the embedding
   * service config doesn't specify compatibleModels (e.g., MMS bootstrapper). The voyage-4 family
   * models are all interchangeable for query-time override.
   */
  private static final Map<String, Set<String>> DEFAULT_COMPATIBLE_MODELS =
      Map.of(
          "voyage-4-large", Set.of("voyage-4", "voyage-4-lite"),
          "voyage-4", Set.of("voyage-4-large", "voyage-4-lite"),
          "voyage-4-lite", Set.of("voyage-4", "voyage-4-large"));

  public static Set<String> getAllSupportedModels() {
    return REGISTERED_MODEL_CONFIGS.keySet();
  }

  public static EmbeddingModelConfig getModelConfig(String modelName) {
    if (!isModelRegistered(modelName)) {
      throw new EmbeddingProviderNonTransientException(
          String.format(
              "CanonicalModel: %s not registered yet, supported models are: [%s]",
              modelName, String.join(", ", REGISTERED_MODEL_CONFIGS.keySet())));
    }
    return REGISTERED_MODEL_CONFIGS.get(modelName);
  }

  public static boolean isModelRegistered(String modelName) {
    return REGISTERED_MODEL_CONFIGS.containsKey(modelName);
  }

  public static void registerModelConfig(String modelName, EmbeddingModelConfig config) {
    REGISTERED_MODEL_CONFIGS.put(modelName, config);
  }

  /**
   * Registers the compatible query models for an index model. This is called when loading the
   * embedding service config. The model itself is automatically included as compatible.
   *
   * <p>If no compatible models are provided, falls back to hardcoded defaults for known model
   * families (e.g., voyage-4 family). This ensures MMS bootstrapper has the same compatibility
   * rules as Community Edition without requiring the conf call to include compatibleModels.
   */
  public static void registerCompatibleModels(
      String indexModel, Set<String> additionalCompatibleModels) {
    String indexModelLower = indexModel.toLowerCase();
    // Always include the model itself as compatible, and ensure all models are lowercase
    Set<String> allCompatibleModels = new HashSet<>();

    // If no compatible models provided, use hardcoded defaults for known model families
    Set<String> modelsToAdd =
        additionalCompatibleModels.isEmpty()
            ? DEFAULT_COMPATIBLE_MODELS.getOrDefault(indexModelLower, Set.of())
            : additionalCompatibleModels;

    for (String model : modelsToAdd) {
      allCompatibleModels.add(model.toLowerCase());
    }
    allCompatibleModels.add(indexModelLower);
    COMPATIBLE_QUERY_MODEL_MAP.put(indexModelLower, allCompatibleModels);
  }

  public static void updateModelConfigs(List<EmbeddingServiceConfig> embeddingServiceConfigs) {
    if (embeddingServiceConfigs.isEmpty()) {
      clear();
      return;
    }
    embeddingServiceConfigs.stream()
        .distinct()
        .forEach(
            serviceConfig -> {
              String modelKey = serviceConfig.modelName.toLowerCase();
              EmbeddingModelConfig newConfig =
                  EmbeddingModelConfig.create(
                      modelKey, serviceConfig.embeddingProvider, serviceConfig.embeddingConfig);
              EmbeddingModelCatalog.registerModelConfig(modelKey, newConfig);
              // Register compatible models from the config (model itself is auto-included)
              EmbeddingModelCatalog.registerCompatibleModels(
                  modelKey, serviceConfig.compatibleModels);
            });
  }

  public static void enableMatView(boolean enabled) {
    MAT_VIEW_ENABLED.set(enabled);
  }

  public static void clear() {
    REGISTERED_MODEL_CONFIGS.clear();
    COMPATIBLE_QUERY_MODEL_MAP.clear();
  }

  public static boolean isMatViewEnabled() {
    return MAT_VIEW_ENABLED.get();
  }

  /**
   * Returns true if the given query model is allowed as a query model. A model is allowed if it
   * appears in any index model's compatible models set. Model names are case-insensitive.
   */
  public static boolean isQueryModelAllowed(String queryModel) {
    String queryModelLower = queryModel.toLowerCase();
    return COMPATIBLE_QUERY_MODEL_MAP.values().stream()
        .anyMatch(compatibleModels -> compatibleModels.contains(queryModelLower));
  }

  /** Returns the set of all allowed query models, sorted alphabetically. */
  public static Set<String> getAllowedQueryModels() {
    return COMPATIBLE_QUERY_MODEL_MAP.values().stream()
        .flatMap(Set::stream)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  /**
   * Returns the set of index models that are compatible with the given query model, or empty if the
   * query model is not allowed. Model names are case-insensitive.
   */
  public static Set<String> getCompatibleIndexModels(String queryModel) {
    // Find all index models that have this query model in their compatible set
    String queryModelLower = queryModel.toLowerCase();
    return COMPATIBLE_QUERY_MODEL_MAP.entrySet().stream()
        .filter(entry -> entry.getValue().contains(queryModelLower))
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }
}
