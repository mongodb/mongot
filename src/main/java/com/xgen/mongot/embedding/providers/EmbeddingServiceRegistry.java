package com.xgen.mongot.embedding.providers;

import static com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog.getAllSupportedModels;

import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.providers.clients.EmbeddingClientFactory;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EmbeddingServiceRegistry {
  // We will only have a single model associated with a single provider manager
  private static final Map<String, EmbeddingProviderManager> REGISTERED_PROVIDER_MANAGERS =
      new ConcurrentHashMap<>();

  private EmbeddingServiceRegistry() {}

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public static void updateRegisteredProviders(
      List<EmbeddingServiceConfig> embeddingServiceConfigs,
      NamedScheduledExecutorService namedScheduledExecutorService,
      EmbeddingClientFactory embeddingClientFactory,
      MetricsFactory metricsFactory) {
    // TODO(CLOUDP-310761): Implement deregister individual model/provider config later.
    if (embeddingServiceConfigs.isEmpty()) {
      clearRegistry();
      return;
    }

    embeddingServiceConfigs.stream()
        .distinct()
        .forEach(
            serviceConfig -> {
              String modelKey = serviceConfig.modelName.toLowerCase();
              // TODO(CLOUDP-335133): Decouple model and provider specific configs
              EmbeddingModelConfig newConfig =
                  EmbeddingModelConfig.create(
                      modelKey, serviceConfig.embeddingProvider, serviceConfig.embeddingConfig);
              EmbeddingModelCatalog.registerModelConfig(modelKey, newConfig);
              // Register compatible models from the config (model itself is auto-included)
              EmbeddingModelCatalog.registerCompatibleModels(
                  modelKey, serviceConfig.compatibleModels);
              REGISTERED_PROVIDER_MANAGERS
                  .computeIfAbsent(
                      modelKey,
                      key ->
                          new EmbeddingProviderManager(
                              newConfig,
                              embeddingClientFactory,
                              namedScheduledExecutorService,
                              metricsFactory))
                  .updateEmbeddingProviderManager(newConfig);
            });
  }

  public static EmbeddingProviderManager getProviderManager(String modelName) {
    if (!REGISTERED_PROVIDER_MANAGERS.containsKey(modelName)) {
      throw new EmbeddingProviderNonTransientException(
          String.format(
              "CanonicalModel: %s not registered yet, supported models are: %s",
              modelName, getAllSupportedModels()));
    }
    return REGISTERED_PROVIDER_MANAGERS.get(modelName);
  }

  public static void clearRegistry() {
    REGISTERED_PROVIDER_MANAGERS.clear();
    EmbeddingModelCatalog.clear();
  }
}
