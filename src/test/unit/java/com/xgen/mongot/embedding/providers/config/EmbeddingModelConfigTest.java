package com.xgen.mongot.embedding.providers.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingCredentials;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageEmbeddingCredentials;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.WorkloadParams;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;

public class EmbeddingModelConfigTest {

  private static final VoyageModelConfig BASE_MODEL_CONFIG =
      new VoyageModelConfig(
          Optional.of(512),
          Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
          Optional.of(1000),
          Optional.of(100_000));

  /**
   * Similarity map keys are MMS per-quantization buckets ({@code scalar}, {@code float}, …), not
   * modality.
   */
  private static final VoyageModelConfig BASE_ADVANCED_MODEL_CONFIG =
      new VoyageModelConfig(
          Optional.of(512),
          Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
          Optional.of(1000),
          Optional.of(100_000),
          Optional.of("text"),
          Optional.of(VectorAutoEmbedQuantization.SCALAR),
          Optional.empty());

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
  public void testConsolidateWorkloadParams_modelOverride_preservesSimilarityByQuantization() {
    VoyageModelConfig baseWithSimilarity =
        new VoyageModelConfig(
            Optional.of(512),
            Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
            Optional.of(1000),
            Optional.of(100_000),
            Optional.of("text"),
            Optional.of(VectorAutoEmbedQuantization.SCALAR),
            Optional.of(Map.of("scalar", "cosine")));

    // Override carries only a batchSize; it has no similarity map of its own.
    VoyageModelConfig override =
        new VoyageModelConfig(
            Optional.empty(), Optional.empty(), Optional.of(2000), Optional.empty());

    WorkloadParams overrideParams =
        new WorkloadParams(
            Optional.of(override), Optional.empty(), Optional.empty(), Optional.empty());

    EmbeddingModelConfig result =
        EmbeddingModelConfig.create(
            "voyage-3.5-lite",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                baseWithSimilarity,
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
    assertEquals(Optional.of(Map.of("scalar", "cosine")), actual.similarityByQuantization);
  }

  @Test
  public void testConsolidateWorkloadParams_modelOverride_preservesSupportedDimensions() {
    VoyageModelConfig baseWithSupportedDimensions =
        new VoyageModelConfig(
            Optional.of(1024),
            Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
            Optional.of(1000),
            Optional.of(100_000),
            Optional.of("text"),
            Optional.of(VectorAutoEmbedQuantization.FLOAT),
            Optional.empty(),
            Optional.of(List.of(256, 1024)));
    VoyageModelConfig override =
        new VoyageModelConfig(
            Optional.empty(), Optional.empty(), Optional.of(2000), Optional.empty());
    WorkloadParams overrideParams =
        new WorkloadParams(
            Optional.of(override), Optional.empty(), Optional.empty(), Optional.empty());

    EmbeddingModelConfig result =
        EmbeddingModelConfig.create(
            "voyage-4",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                baseWithSupportedDimensions,
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
    assertEquals(Optional.of(List.of(256, 1024)), actual.supportedDimensions);
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
  public void testConsolidateVoyageModelConfig_advancedParams_mergeFieldByField() {
    VoyageModelConfig override =
        new VoyageModelConfig(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    WorkloadParams overrideParams =
        new WorkloadParams(
            Optional.of(override), Optional.empty(), Optional.empty(), Optional.empty());

    EmbeddingModelConfig consolidatedConfig =
        EmbeddingModelConfig.create(
            "voyage-3.5-lite",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                BASE_ADVANCED_MODEL_CONFIG,
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

    assertEquals(BASE_ADVANCED_MODEL_CONFIG.modality, result.modality);
    assertEquals(BASE_ADVANCED_MODEL_CONFIG.quantization, result.quantization);
  }

  @Test
  public void testVoyageModelConfig_quantization_roundTrip_allValues() throws BsonParseException {

    for (VectorAutoEmbedQuantization quantization : VectorAutoEmbedQuantization.values()) {
      VoyageModelConfig original =
          new VoyageModelConfig(
              Optional.of(512),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(quantization),
              Optional.empty());
      BsonDocument encoded = original.toBson();
      BsonDocument root = new BsonDocument();
      encoded.keySet().forEach(k -> root.put(k, encoded.get(k)));
      root.put("_provider", new BsonString("VOYAGE"));
      VoyageModelConfig parsed =
          VoyageModelConfig.fromBson(
              BsonDocumentParser.fromRoot(root).allowUnknownFields(true).build());
      assertEquals(Optional.of(quantization), parsed.quantization);
    }
  }

  @Test
  public void testVoyageModelConfig_quantization_rejectsIndexDefinitionNoneAndInvalidStrings() {
    assertQuantizationParseFails("none");
    assertQuantizationParseFails("invalid");
  }

  @Test
  public void testVoyageModelConfig_quantization_rejectsNonString() {
    BsonDocument root = new BsonDocument();
    root.put("_provider", new BsonString("VOYAGE"));
    root.put("quantization", new BsonInt32(1));
    assertThrows(
        BsonParseException.class,
        () ->
            VoyageModelConfig.fromBson(
                BsonDocumentParser.fromRoot(root).allowUnknownFields(true).build()));
  }

  @Test
  public void testVoyageModelConfig_bsonRoundTrip_preservesAdvancedParams()
      throws BsonParseException {
    VoyageModelConfig original =
        new VoyageModelConfig(
            Optional.of(512),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of("text"),
            Optional.of(VectorAutoEmbedQuantization.SCALAR),
            Optional.of(Map.of("scalar", "cosine")));
    BsonDocument root = new BsonDocument();
    BsonDocument encoded = original.toBson();
    encoded.keySet().forEach(k -> root.put(k, encoded.get(k)));
    root.put("_provider", new BsonString("VOYAGE"));
    VoyageModelConfig parsed =
        VoyageModelConfig.fromBson(
            BsonDocumentParser.fromRoot(root).allowUnknownFields(true).build());
    assertEquals(original, parsed);
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
  public void testRpsPerProvider_usesMinOfOuterAndInner() {
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
                Optional.of(50)),
            Optional.of(75));

    assertEquals(Optional.of(50), result.query().rpsPerProvider());
    assertEquals(Optional.of(50), result.changeStream().rpsPerProvider());
    assertEquals(Optional.of(50), result.collectionScan().rpsPerProvider());
  }

  @Test
  public void testRpsPerProvider_outerLowerThanInner() {
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
                Optional.of(50)),
            Optional.of(30));

    assertEquals(Optional.of(30), result.query().rpsPerProvider());
    assertEquals(Optional.of(30), result.changeStream().rpsPerProvider());
    assertEquals(Optional.of(30), result.collectionScan().rpsPerProvider());
  }

  @Test
  public void testRpsPerProvider_outerAbsentFallsToInner() {
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
                Optional.of(50)),
            Optional.empty());

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

  private static void assertQuantizationParseFails(String wire) {
    BsonDocument root = new BsonDocument();
    root.put("_provider", new BsonString("VOYAGE"));
    root.put("quantization", new BsonString(wire));
    assertThrows(
        BsonParseException.class,
        () ->
            VoyageModelConfig.fromBson(
                BsonDocumentParser.fromRoot(root).allowUnknownFields(true).build()));
  }
}
