package com.xgen.mongot.index.definition;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SearchAutoEmbedFieldDefinitionTest {

  private static final String MODEL = "voyage-3-large";

  @Before
  public void setUp() {
    EmbeddingModelCatalog.clear();
    registerModel(MODEL, 1024, VectorAutoEmbedQuantization.FLOAT, Optional.empty());
  }

  @After
  public void tearDown() {
    EmbeddingModelCatalog.clear();
  }

  @Test
  public void fromBson_advancedVectorConfigs_parsesAll() throws Exception {
    BsonDocument bson =
        BsonDocument.parse(
            """
            {
              "type": "autoEmbed",
              "model": "voyage-3-large",
              "modality": "text",
              "path": "content",
              "numDimensions": 256,
              "similarity": "cosine",
              "quantization": "scalar",
              "indexingMethod": "hnsw",
              "hnswOptions": {"maxEdges": 32, "numEdgeCandidates": 200}
            }
            """);

    SearchAutoEmbedFieldDefinition definition = parse(bson);

    assertThat(definition.path()).isEqualTo(FieldPath.parse("content"));
    assertThat(definition.specification().numDimensions()).isEqualTo(256);
    assertThat(definition.specification().similarity()).isEqualTo(VectorSimilarity.COSINE);
    assertThat(definition.specification().autoEmbedQuantization())
        .isEqualTo(VectorAutoEmbedQuantization.SCALAR);
    assertThat(definition.specification().indexingAlgorithm())
        .isEqualTo(
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                new VectorFieldSpecification.HnswOptions(32, 200)));
  }

  @Test
  public void fromBson_normalizesModelAndModalityToLowerCase() throws Exception {
    BsonDocument bson =
        BsonDocument.parse(
            """
            {
              "type": "autoEmbed",
              "model": "Voyage-3-Large",
              "modality": "TEXT",
              "path": "content"
            }
            """);

    SearchAutoEmbedFieldDefinition definition = parse(bson);
    BsonDocument serialized = definition.fieldTypeToBson();

    assertThat(definition.modelName()).isEqualTo("voyage-3-large");
    assertThat(definition.modality()).isEqualTo("text");
    assertThat(serialized.getString("model").getValue()).isEqualTo("voyage-3-large");
    assertThat(serialized.getString("modality").getValue()).isEqualTo("text");
  }

  @Test
  public void resolvedSpecConstructor_binaryNoRescoreWithCosine_throws() {
    VectorAutoEmbedFieldSpecification specification =
        new VectorAutoEmbedFieldSpecification(
            256,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.BINARY_NO_RESCORE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm(),
            MODEL,
            "text");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchAutoEmbedFieldDefinition(FieldPath.parse("content"), specification));

    assertThat(exception.getMessage()).contains("binaryNoRescore");
    assertThat(exception.getMessage()).contains("euclidean");
  }

  @Test
  public void resolvedSpecConstructor_binaryDimensionsNotMultipleOfEight_throws() {
    VectorAutoEmbedFieldSpecification specification =
        new VectorAutoEmbedFieldSpecification(
            255,
            VectorSimilarity.EUCLIDEAN,
            VectorAutoEmbedQuantization.BINARY,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm(),
            MODEL,
            "text");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchAutoEmbedFieldDefinition(FieldPath.parse("content"), specification));

    assertThat(exception.getMessage()).contains("multiple of 8");
  }

  @Test
  public void toBson_roundTrip_preservesAdvancedVectorConfigs() throws Exception {
    SearchAutoEmbedFieldDefinition original =
        new SearchAutoEmbedFieldDefinition(
            FieldPath.parse("content"),
            new VectorAutoEmbedFieldSpecification(
                256,
                VectorSimilarity.COSINE,
                VectorAutoEmbedQuantization.SCALAR,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                    new VectorFieldSpecification.HnswOptions(32, 200)),
                "voyage-3-large",
                "text"));

    SearchAutoEmbedFieldDefinition roundTripped = parse(original.toBson());
    BsonDocument roundTrippedBson = roundTripped.fieldTypeToBson();

    assertThat(roundTripped).isEqualTo(original);
    assertThat(roundTrippedBson.getInt32("numDimensions").getValue()).isEqualTo(256);
    assertThat(roundTrippedBson.getString("similarity").getValue()).isEqualTo("cosine");
    assertThat(roundTrippedBson.getString("quantization").getValue()).isEqualTo("scalar");
    assertThat(roundTrippedBson.containsKey("indexingMethod")).isFalse();
    assertThat(roundTrippedBson.getDocument("hnswOptions").getInt32("maxEdges").getValue())
        .isEqualTo(32);
    assertThat(roundTrippedBson.getDocument("hnswOptions").getInt32("numEdgeCandidates").getValue())
        .isEqualTo(200);
  }

  @Test
  public void fromBson_omittedFields_serializesResolvedValues() throws Exception {
    BsonDocument bson =
        BsonDocument.parse(
            """
            {
              "type": "autoEmbed",
              "model": "voyage-3-large",
              "modality": "text",
              "path": "content"
            }
            """);

    SearchAutoEmbedFieldDefinition definition = parse(bson);
    BsonDocument serialized = definition.fieldTypeToBson();

    assertThat(serialized.getInt32("numDimensions").getValue()).isEqualTo(1024);
    assertThat(serialized.getString("similarity").getValue()).isEqualTo("dotProduct");
    assertThat(serialized.getString("quantization").getValue()).isEqualTo("float");
    assertThat(serialized.containsKey("indexingMethod")).isFalse();
    assertThat(serialized.containsKey("hnswOptions")).isFalse();
  }

  @Test
  public void fromBson_serializedDefinition_reparsesWithEmptyCatalog() throws Exception {
    BsonDocument bson =
        BsonDocument.parse(
            """
            {
              "type": "autoEmbed",
              "model": "voyage-3-large",
              "modality": "text",
              "path": "content"
            }
            """);
    SearchAutoEmbedFieldDefinition definition = parse(bson);
    BsonDocument persisted = definition.toBson();

    // A restart can load the config journal before any embedding model is registered.
    EmbeddingModelCatalog.clear();

    assertThat(parse(persisted)).isEqualTo(definition);
  }

  @Test
  public void fromBson_omittedFields_usesModelConfigDefaults() throws Exception {
    EmbeddingModelCatalog.clear();
    registerModel(
        "voyage-4-lite",
        512,
        VectorAutoEmbedQuantization.SCALAR,
        Optional.of(Map.of("scalar", "cosine")));
    BsonDocument bson =
        BsonDocument.parse(
            """
            {
              "type": "autoEmbed",
              "model": "voyage-4-lite",
              "modality": "text",
              "path": "content"
            }
            """);

    SearchAutoEmbedFieldDefinition definition = parse(bson);
    BsonDocument serialized = definition.fieldTypeToBson();

    assertThat(definition.specification().numDimensions()).isEqualTo(512);
    assertThat(definition.specification().similarity()).isEqualTo(VectorSimilarity.COSINE);
    assertThat(definition.specification().autoEmbedQuantization())
        .isEqualTo(VectorAutoEmbedQuantization.SCALAR);
    assertThat(serialized.getInt32("numDimensions").getValue()).isEqualTo(512);
    assertThat(serialized.getString("similarity").getValue()).isEqualTo("cosine");
    assertThat(serialized.getString("quantization").getValue()).isEqualTo("scalar");
  }

  @Test
  public void fromBson_explicitQuantization_omittedSimilarity_usesModelConfigSimilarity()
      throws Exception {
    EmbeddingModelCatalog.clear();
    registerModel(
        MODEL, 1024, VectorAutoEmbedQuantization.FLOAT, Optional.of(Map.of("binary", "cosine")));
    BsonDocument bson =
        BsonDocument.parse(
            """
            {
              "type": "autoEmbed",
              "model": "voyage-3-large",
              "modality": "text",
              "path": "content",
              "numDimensions": 256,
              "quantization": "binary"
            }
            """);

    SearchAutoEmbedFieldDefinition definition = parse(bson);

    assertThat(definition.specification().similarity()).isEqualTo(VectorSimilarity.COSINE);
    assertThat(definition.specification().autoEmbedQuantization())
        .isEqualTo(VectorAutoEmbedQuantization.BINARY);
  }

  @Test
  public void fromBson_unregisteredModel_withOmittedFields_throws() {
    EmbeddingModelCatalog.clear();
    BsonDocument bson =
        BsonDocument.parse(
            """
            {
              "type": "autoEmbed",
              "model": "unregistered-model",
              "modality": "text",
              "path": "content"
            }
            """);

    BsonParseException exception = assertThrows(BsonParseException.class, () -> parse(bson));

    assertThat(exception.getMessage()).contains("unregistered-model");
  }

  @Test
  public void fromBson_hnswOptionsWithFlatIndexingMethod_throws() {
    BsonDocument bson =
        BsonDocument.parse(
            """
            {
              "type": "autoEmbed",
              "model": "voyage-3-large",
              "modality": "text",
              "path": "content",
              "indexingMethod": "flat",
              "hnswOptions": {"maxEdges": 32}
            }
            """);

    BsonParseException exception = assertThrows(BsonParseException.class, () -> parse(bson));

    assertThat(exception.getMessage()).contains("hnswOptions is only supported");
  }

  @Test
  public void fromBson_binaryNoRescoreWithoutSimilarity_defaultsToEuclidean() throws Exception {
    BsonDocument bson =
        BsonDocument.parse(
            """
            {
              "type": "autoEmbed",
              "model": "voyage-3-large",
              "modality": "text",
              "path": "content",
              "numDimensions": 256,
              "quantization": "binaryNoRescore"
            }
            """);

    SearchAutoEmbedFieldDefinition definition = parse(bson);

    assertThat(definition.specification().similarity()).isEqualTo(VectorSimilarity.EUCLIDEAN);
    assertThat(definition.specification().autoEmbedQuantization())
        .isEqualTo(VectorAutoEmbedQuantization.BINARY_NO_RESCORE);
  }

  @Test
  public void fromBson_binaryNoRescoreWithCosineSimilarity_throws() {
    BsonDocument bson =
        BsonDocument.parse(
            """
            {
              "type": "autoEmbed",
              "model": "voyage-3-large",
              "modality": "text",
              "path": "content",
              "numDimensions": 256,
              "similarity": "cosine",
              "quantization": "binaryNoRescore"
            }
            """);

    BsonParseException exception = assertThrows(BsonParseException.class, () -> parse(bson));

    assertThat(exception.getMessage()).contains("binaryNoRescore");
    assertThat(exception.getMessage()).contains("euclidean");
  }

  @Test
  public void fromBson_binaryQuantizationWithNonMultipleOfEightDimensions_throws() {
    BsonDocument bson =
        BsonDocument.parse(
            """
            {
              "type": "autoEmbed",
              "model": "voyage-3-large",
              "modality": "text",
              "path": "content",
              "numDimensions": 255,
              "quantization": "binary"
            }
            """);

    BsonParseException exception = assertThrows(BsonParseException.class, () -> parse(bson));

    assertThat(exception.getMessage()).contains("multiple of 8");
  }

  private static void registerModel(
      String modelName,
      int outputDimensions,
      VectorAutoEmbedQuantization quantization,
      Optional<Map<String, String>> similarityByQuantization) {
    EmbeddingModelCatalog.registerModelConfig(
        modelName,
        EmbeddingModelConfig.create(
            modelName,
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.empty(),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(outputDimensions),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
                    Optional.of(100),
                    Optional.of(120_000),
                    Optional.of("text"),
                    Optional.of(quantization),
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

  private static SearchAutoEmbedFieldDefinition parse(BsonDocument bson) throws BsonParseException {
    try (BsonDocumentParser parser = BsonDocumentParser.fromRoot(bson).build()) {
      return (SearchAutoEmbedFieldDefinition) FieldTypeDefinition.fromBson(parser);
    }
  }
}
