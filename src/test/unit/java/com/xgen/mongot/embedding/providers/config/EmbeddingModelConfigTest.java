package com.xgen.mongot.embedding.providers.config;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingCredentials;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageEmbeddingCredentials;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.WorkloadParams;
import java.util.Optional;
import org.junit.Test;

public class EmbeddingModelConfigTest {

  private static final VoyageModelConfig BASE_MODEL_CONFIG =
      new VoyageModelConfig(
          Optional.of(512),
          Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
          Optional.of(1000),
          Optional.of(100_000));

  private static final EmbeddingServiceConfig.ErrorHandlingConfig BASE_ERROR_CONFIG =
      new EmbeddingServiceConfig.ErrorHandlingConfig(10, 200L, 50L, 0.1);

  private static final EmbeddingCredentials BASE_CREDENTIALS =
      new VoyageEmbeddingCredentials("token123", "");

  @Test
  public void testConsolidateWorkloadParams_noOverrides_returnsBaseParams() {
    EmbeddingModelConfig result =
        EmbeddingModelConfig.create(
            "voyage-3.5-lite",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                BASE_MODEL_CONFIG,
                BASE_ERROR_CONFIG,
                BASE_CREDENTIALS,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty()));

    assertEquals(BASE_MODEL_CONFIG, result.query().modelConfig());
    assertEquals(BASE_MODEL_CONFIG, result.changeStream().modelConfig());
    assertEquals(BASE_MODEL_CONFIG, result.collectionScan().modelConfig());

    assertEquals(BASE_ERROR_CONFIG, result.query().errorHandlingConfig());
    assertEquals(BASE_ERROR_CONFIG, result.changeStream().errorHandlingConfig());
    assertEquals(BASE_ERROR_CONFIG, result.collectionScan().errorHandlingConfig());

    assertEquals(BASE_CREDENTIALS, result.query().credentials());
    assertEquals(BASE_CREDENTIALS, result.changeStream().credentials());
    assertEquals(BASE_CREDENTIALS, result.collectionScan().credentials());
  }

  @Test
  public void testConsolidateWorkloadParams_modelOverride_appliesOverrideCorrectly() {
    VoyageModelConfig override =
        new VoyageModelConfig(
            Optional.of(1024), // override dimension
            Optional.empty(),
            Optional.of(2000),
            Optional.empty());

    WorkloadParams overrideParams =
        new WorkloadParams(
            Optional.of(override), Optional.empty(), Optional.empty(), Optional.empty());

    EmbeddingModelConfig result =
        EmbeddingModelConfig.create(
            "voyage-3.5-lite",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                BASE_MODEL_CONFIG,
                BASE_ERROR_CONFIG,
                BASE_CREDENTIALS,
                Optional.empty(),
                Optional.of(overrideParams),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty()));

    VoyageModelConfig actual = (VoyageModelConfig) result.collectionScan().modelConfig();
    assertEquals(Optional.of(1024), actual.outputDimensions);
    assertEquals(BASE_MODEL_CONFIG.truncation, actual.truncation);
    assertEquals(Optional.of(2000), actual.batchSize);
    assertEquals(BASE_MODEL_CONFIG.batchTokenLimit, actual.batchTokenLimit);
  }

  @Test
  public void testConsolidateWorkloadParams_credentialOverride() {
    EmbeddingCredentials overrideCreds = new VoyageEmbeddingCredentials("override-api-key", "");

    WorkloadParams overrideParams =
        new WorkloadParams(
            Optional.empty(), Optional.empty(), Optional.of(overrideCreds), Optional.empty());

    EmbeddingModelConfig result =
        EmbeddingModelConfig.create(
            "voyage-3.5-lite",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                BASE_MODEL_CONFIG,
                BASE_ERROR_CONFIG,
                BASE_CREDENTIALS,
                Optional.of(overrideParams),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty()));

    VoyageEmbeddingCredentials actual = (VoyageEmbeddingCredentials) result.query().credentials();
    assertEquals("override-api-key", actual.apiToken);
  }

  @Test
  public void testConsolidateVoyageModelConfig_onlyOverrideFieldsChange() {
    VoyageModelConfig modelConfigOverride =
        new VoyageModelConfig(
            Optional.empty(),
            Optional.of(EmbeddingServiceConfig.TruncationOption.END),
            Optional.empty(),
            Optional.of(200_000));

    WorkloadParams overrideParams =
        new WorkloadParams(
            Optional.of(modelConfigOverride), Optional.empty(), Optional.empty(), Optional.empty());

    EmbeddingModelConfig consolidatedConfig =
        EmbeddingModelConfig.create(
            "voyage-3.5-lite",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                BASE_MODEL_CONFIG,
                BASE_ERROR_CONFIG,
                BASE_CREDENTIALS,
                Optional.empty(),
                Optional.of(overrideParams),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty()));

    VoyageModelConfig result =
        (VoyageModelConfig) consolidatedConfig.collectionScan().modelConfig();

    assertEquals(BASE_MODEL_CONFIG.outputDimensions, result.outputDimensions);
    assertEquals(Optional.of(EmbeddingServiceConfig.TruncationOption.END), result.truncation);
    assertEquals(BASE_MODEL_CONFIG.batchSize, result.batchSize);
    assertEquals(Optional.of(200_000), result.batchTokenLimit);
  }

  @Test
  public void testRpsPerProvider_threadsToAllConsolidatedWorkloadParams() {
    EmbeddingModelConfig result =
        EmbeddingModelConfig.create(
            "voyage-3.5-lite",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                BASE_MODEL_CONFIG,
                BASE_ERROR_CONFIG,
                BASE_CREDENTIALS,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.of(50)));

    assertEquals(Optional.of(50), result.query().rpsPerProvider());
    assertEquals(Optional.of(50), result.changeStream().rpsPerProvider());
    assertEquals(Optional.of(50), result.collectionScan().rpsPerProvider());
  }

  @Test
  public void testRpsPerProvider_emptyWhenAbsent() {
    EmbeddingModelConfig result =
        EmbeddingModelConfig.create(
            "voyage-3.5-lite",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                BASE_MODEL_CONFIG,
                BASE_ERROR_CONFIG,
                BASE_CREDENTIALS,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty()));

    assertEquals(Optional.empty(), result.query().rpsPerProvider());
    assertEquals(Optional.empty(), result.changeStream().rpsPerProvider());
    assertEquals(Optional.empty(), result.collectionScan().rpsPerProvider());
  }
}
