package com.xgen.mongot.embedding.providers.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageModelConfig;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;

public class VoyageModelConfigTest {

  @Test
  public void fromBson_withSimilarityMap_parsesPerQuantizationDefaults() throws Exception {
    BsonDocument doc =
        new BsonDocument()
            .append("outputDimensions", new BsonInt32(1024))
            .append("quantization", new BsonString("scalar"))
            .append(
                "similarity",
                new BsonDocument()
                    .append("scalar", new BsonString("cosine"))
                    .append("float", new BsonString("dotProduct")));

    VoyageModelConfig config;
    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      config = VoyageModelConfig.fromBson(parser);
    }

    assertTrue(config.similarityByQuantization.isPresent());
    Map<String, String> byQuantization = config.similarityByQuantization.get();
    assertEquals("cosine", byQuantization.get("scalar"));
    assertEquals("dotProduct", byQuantization.get("float"));
  }

  @Test
  public void fromBson_withoutSimilarityMap_isEmpty() throws Exception {
    BsonDocument doc =
        new BsonDocument()
            .append("outputDimensions", new BsonInt32(1024))
            .append("quantization", new BsonString("scalar"));

    VoyageModelConfig config;
    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      config = VoyageModelConfig.fromBson(parser);
    }

    assertFalse(config.similarityByQuantization.isPresent());
  }

  @Test
  public void toBson_fromBson_roundTripsSupportedDimensions() throws Exception {
    VoyageModelConfig config =
        new VoyageModelConfig(
            Optional.of(1024),
            Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
            Optional.of(1000),
            Optional.of(120_000),
            Optional.of("text"),
            Optional.of(VectorAutoEmbedQuantization.FLOAT),
            Optional.empty(),
            Optional.of(List.of(256, 512, 1024, 2048)));

    BsonDocument document = config.toBson();

    VoyageModelConfig parsed;
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      parsed = VoyageModelConfig.fromBson(parser);
    }

    assertEquals(config, parsed);
  }

  @Test
  public void toBson_withoutSupportedDimensions_omitsField() {
    VoyageModelConfig config =
        new VoyageModelConfig(
            Optional.of(1024), Optional.empty(), Optional.empty(), Optional.empty());

    assertFalse(config.toBson().containsKey("supportedDimensions"));
  }
}
