package com.xgen.mongot.index.definition;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification.ResolvedAutoEmbedVectorParams;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class VectorAutoEmbedFieldSpecificationTest {

  private static final String MODEL = "voyage-3-large";

  private static final ch.qos.logback.classic.Logger specificationLogger =
      (ch.qos.logback.classic.Logger)
          LoggerFactory.getLogger(VectorAutoEmbedFieldSpecification.class);

  @Before
  public void clearCatalog() {
    EmbeddingModelCatalog.clear();
  }

  @After
  public void tearDown() {
    EmbeddingModelCatalog.clear();
  }

  @Test
  public void resolveSimilarity_userSetValue_takesPrecedence() {
    VectorSimilarity result =
        VectorAutoEmbedFieldSpecification.resolveSimilarity(
            MODEL,
            Optional.of(VectorSimilarity.COSINE),
            VectorAutoEmbedQuantization.SCALAR,
            Optional.of(Map.of("scalar", "euclidean")));

    assertThat(result).isEqualTo(VectorSimilarity.COSINE);
  }

  @Test
  public void resolveSimilarity_userUnset_usesModelConfigDefault() {
    VectorSimilarity result =
        VectorAutoEmbedFieldSpecification.resolveSimilarity(
            MODEL,
            Optional.empty(),
            VectorAutoEmbedQuantization.SCALAR,
            Optional.of(Map.of("scalar", "cosine")));

    assertThat(result).isEqualTo(VectorSimilarity.COSINE);
  }

  @Test
  public void resolveSimilarity_noModelConfig_fallsBackToHardcodedDefault() {
    VectorSimilarity result =
        VectorAutoEmbedFieldSpecification.resolveSimilarity(
            MODEL, Optional.empty(), VectorAutoEmbedQuantization.SCALAR, Optional.empty());

    assertThat(result).isEqualTo(VectorSimilarity.DOT_PRODUCT);
  }

  @Test
  public void resolveSimilarity_modelConfigMissingQuantization_fallsBackToHardcodedDefault() {
    VectorSimilarity result =
        VectorAutoEmbedFieldSpecification.resolveSimilarity(
            MODEL,
            Optional.empty(),
            VectorAutoEmbedQuantization.BINARY,
            Optional.of(Map.of("scalar", "cosine")));

    assertThat(result).isEqualTo(VectorSimilarity.EUCLIDEAN);
  }

  /**
   * A model-config similarity value mongot doesn't recognize must not fail the index: the field
   * falls back to the hardcoded quantization default, and the skipped config value is surfaced as a
   * WARN carrying the model, quantization, and offending value an operator needs to find the bad
   * conf-call config.
   */
  @Test
  public void resolveSimilarity_unrecognizedModelConfigValue_fallsBackAndWarns() {
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.start();
    specificationLogger.addAppender(listAppender);
    try {
      VectorSimilarity result =
          VectorAutoEmbedFieldSpecification.resolveSimilarity(
              MODEL,
              Optional.empty(),
              VectorAutoEmbedQuantization.SCALAR,
              Optional.of(Map.of("scalar", "bogusSimilarity")));

      assertThat(result).isEqualTo(VectorSimilarity.DOT_PRODUCT);

      ILoggingEvent warning =
          listAppender.list.stream()
              .filter(event -> event.getLevel() == Level.WARN)
              .filter(event -> event.getFormattedMessage().contains("Unrecognized model-config"))
              .findFirst()
              .orElseThrow(
                  () -> new AssertionError("expected a WARN for the unrecognized similarity"));
      assertThat(warning.getKeyValuePairs().toString()).contains(MODEL);
      assertThat(warning.getKeyValuePairs().toString()).contains("bogusSimilarity");
    } finally {
      specificationLogger.detachAppender(listAppender);
    }
  }

  /**
   * Fast-path: all three optional values provided. The model is deliberately NOT registered, so a
   * catalog lookup would throw — success proves the catalog is never consulted.
   */
  @Test
  public void resolveAutoEmbedVectorParams_allValuesProvided_doesNotConsultCatalog()
      throws BsonParseException {
    ResolvedAutoEmbedVectorParams result =
        VectorAutoEmbedFieldSpecification.resolveAutoEmbedVectorParams(
            MODEL,
            Optional.of(256),
            Optional.of(VectorSimilarity.COSINE),
            Optional.of(VectorAutoEmbedQuantization.SCALAR));

    assertThat(result.numDimensions()).isEqualTo(256);
    assertThat(result.similarity()).isEqualTo(VectorSimilarity.COSINE);
    assertThat(result.providerQuantization()).isEqualTo(VectorAutoEmbedQuantization.SCALAR);
  }

  /**
   * When only {@code similarity} is omitted, resolution falls through the fast-path and picks up
   * the per-quantization default from the model config.
   */
  @Test
  public void resolveAutoEmbedVectorParams_similarityOmitted_usesModelConfigDefault()
      throws BsonParseException {
    registerModel(MODEL, Optional.of(Map.of("scalar", "cosine")));

    ResolvedAutoEmbedVectorParams result =
        VectorAutoEmbedFieldSpecification.resolveAutoEmbedVectorParams(
            MODEL,
            Optional.of(256),
            Optional.empty(),
            Optional.of(VectorAutoEmbedQuantization.SCALAR));

    assertThat(result.similarity()).isEqualTo(VectorSimilarity.COSINE);
    assertThat(result.numDimensions()).isEqualTo(256);
    assertThat(result.providerQuantization()).isEqualTo(VectorAutoEmbedQuantization.SCALAR);
  }

  /**
   * When {@code quantization} and {@code similarity} are both omitted, the quantization resolved
   * from the model config is the key used to look up the similarity default.
   */
  @Test
  public void resolveAutoEmbedVectorParams_quantizationAndSimilarityOmitted_resolvesBothFromConfig()
      throws BsonParseException {
    registerModel(MODEL, Optional.of(Map.of("float", "cosine")));

    ResolvedAutoEmbedVectorParams result =
        VectorAutoEmbedFieldSpecification.resolveAutoEmbedVectorParams(
            MODEL, Optional.of(256), Optional.empty(), Optional.empty());

    assertThat(result.providerQuantization()).isEqualTo(VectorAutoEmbedQuantization.FLOAT);
    assertThat(result.similarity()).isEqualTo(VectorSimilarity.COSINE);
  }

  /**
   * With {@code numDimensions} and {@code quantization} provided but {@code similarity} omitted,
   * the fall-through consults the catalog; an unregistered model surfaces as {@link
   * BsonParseException} (routed to the invalid-index map by the conf-call handler) rather than the
   * raw embedding-provider exception.
   */
  @Test
  public void resolveAutoEmbedVectorParams_similarityOmittedModelUnregistered_throwsBsonParse() {
    BsonParseException e =
        assertThrows(
            BsonParseException.class,
            () ->
                VectorAutoEmbedFieldSpecification.resolveAutoEmbedVectorParams(
                    "unregistered-model",
                    Optional.of(256),
                    Optional.empty(),
                    Optional.of(VectorAutoEmbedQuantization.SCALAR)));

    assertThat(e.getMessage()).contains("unregistered-model");
  }

  private static void registerModel(
      String modelName, Optional<Map<String, String>> similarityByQuantization) {
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
                    similarityByQuantization),
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
