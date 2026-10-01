package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.junit.Test;

public class CommunityAutoEmbeddingConfigTest {

  private static final Field.Optional<String> ADVANCED_CONFIGS_FIELD =
      Field.builder("advancedConfigs").stringField().optional().noDefault();
  private static final Field.Optional<String> AUTO_EMBEDDING_FIELD =
      Field.builder("autoEmbedding").stringField().optional().noDefault();

  /**
   * Registries that mount the autoEmbedding block under {@code advancedConfigs.autoEmbedding}, as
   * the Community config root does, so traversal emits the absolute YAML paths.
   */
  private static final class AdvancedConfigsTestRegistry extends CliOptionRegistry {
    final CommunityAutoEmbeddingConfig.CommunityAutoEmbeddingConfigCliOptionRegistry autoEmbedding =
        new CommunityAutoEmbeddingConfig.CommunityAutoEmbeddingConfigCliOptionRegistry();

    AdvancedConfigsTestRegistry() {
      addChildRegistry(AUTO_EMBEDDING_FIELD, this.autoEmbedding);
    }
  }

  private static final class RootConfigTestRegistry extends CliOptionRegistry {
    final AdvancedConfigsTestRegistry advancedConfigs = new AdvancedConfigsTestRegistry();

    RootConfigTestRegistry() {
      addChildRegistry(ADVANCED_CONFIGS_FIELD, this.advancedConfigs);
    }
  }

  private static CommunityAutoEmbeddingConfig parse(BsonDocument doc) throws BsonParseException {
    PermissiveBsonParseContext context = PermissiveBsonParseContext.root();
    try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
      return CommunityAutoEmbeddingConfig.fromBson(parser);
    }
  }

  @Test
  public void roundTrip_full_preservesAllFields() throws BsonParseException {
    CommunityAutoEmbeddingConfig original =
        new CommunityAutoEmbeddingConfig(
            Optional.of(
                new CommunityAutoEmbeddingConfig.MaterializedViewConfig(
                    Optional.of(10),
                    Optional.of(5),
                    Optional.of(6),
                    Optional.of(2),
                    Optional.of(8),
                    Optional.of(3),
                    Optional.of(2000),
                    Optional.of(700),
                    Optional.of(1200),
                    Optional.of(3),
                    Optional.of(250),
                    Optional.of(50),
                    Optional.of(25),
                    Optional.of(80),
                    Optional.of(40))));
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void roundTrip_empty_preservesEmptyBlock() throws BsonParseException {
    CommunityAutoEmbeddingConfig original = new CommunityAutoEmbeddingConfig(Optional.empty());
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void parse_exposesAllFields() throws BsonParseException {
    BsonDocument materializedView =
        new BsonDocument()
            .append("numConcurrentChangeStreams", new BsonInt32(10))
            .append("numIndexingThreads", new BsonInt32(5))
            .append("numEmbeddingThreads", new BsonInt32(6))
            .append("numConcurrentInitialSyncs", new BsonInt32(2))
            .append("matViewWriterMaxConnections", new BsonInt32(8))
            .append("maxInFlightEmbeddingGetMores", new BsonInt32(3))
            .append("embeddingGetMoreBatchSize", new BsonInt32(2000))
            .append("changeStreamMaxTimeMs", new BsonInt32(700))
            .append("changeStreamCursorMaxTimeSec", new BsonInt32(1200))
            .append("numChangeStreamDecodingThreads", new BsonInt32(3))
            .append("requestRateLimitBackoffMs", new BsonInt32(250))
            .append("mvWriteRateLimitRps", new BsonInt32(50))
            .append("embeddingProviderRpsLimit", new BsonInt32(25))
            .append("globalMemoryBudgetHeapPercent", new BsonInt32(80))
            .append("perBatchMemoryBudgetHeapPercent", new BsonInt32(40));
    var matView =
        parse(new BsonDocument("materializedView", materializedView))
            .materializedViewConfig()
            .get();
    assertEquals(Optional.of(10), matView.numConcurrentChangeStreams());
    assertEquals(Optional.of(5), matView.numIndexingThreads());
    assertEquals(Optional.of(6), matView.numEmbeddingThreads());
    assertEquals(Optional.of(2), matView.numConcurrentInitialSyncs());
    assertEquals(Optional.of(8), matView.matViewWriterMaxConnections());
    assertEquals(Optional.of(3), matView.maxInFlightEmbeddingGetMores());
    assertEquals(Optional.of(2000), matView.embeddingGetMoreBatchSize());
    assertEquals(Optional.of(700), matView.changeStreamMaxTimeMs());
    assertEquals(Optional.of(1200), matView.changeStreamCursorMaxTimeSec());
    assertEquals(Optional.of(3), matView.numChangeStreamDecodingThreads());
    assertEquals(Optional.of(250), matView.requestRateLimitBackoffMs());
    assertEquals(Optional.of(50), matView.mvWriteRateLimitRps());
    assertEquals(Optional.of(25), matView.embeddingProviderRpsLimit());
    assertEquals(Optional.of(80), matView.globalMemoryBudgetHeapPercent());
    assertEquals(Optional.of(40), matView.perBatchMemoryBudgetHeapPercent());
  }

  @Test
  public void parse_memoryBudgetHeapPercentAbove100_throws() {
    BsonDocument doc =
        new BsonDocument(
            "materializedView",
            new BsonDocument("globalMemoryBudgetHeapPercent", new BsonInt32(101)));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_negativeRequestRateLimitBackoffMs_throws() {
    BsonDocument doc =
        new BsonDocument(
            "materializedView", new BsonDocument("requestRateLimitBackoffMs", new BsonInt32(-1)));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_negativeNumEmbeddingThreads_throws() {
    BsonDocument doc =
        new BsonDocument(
            "materializedView", new BsonDocument("numEmbeddingThreads", new BsonInt32(-1)));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_unknownFieldInMaterializedViewBlock_isIgnored() throws BsonParseException {
    BsonDocument materializedView =
        new BsonDocument("numIndexingThreads", new BsonInt32(5))
            .append("notARealField", new BsonInt32(1));
    CommunityAutoEmbeddingConfig parsed =
        parse(new BsonDocument("materializedView", materializedView));
    assertEquals(Optional.of(5), parsed.materializedViewConfig().get().numIndexingThreads());
  }

  @Test
  public void cliOptionRegistry_traverse_withAllOptions_returnsYamlPaths() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    var materializedView =
        rootRegistry.advancedConfigs.autoEmbedding.materializedViewConfigCliOptionRegistry;
    materializedView.numConcurrentChangeStreams = Optional.of(10);
    materializedView.numIndexingThreads = Optional.of(5);
    materializedView.numEmbeddingThreads = Optional.of(6);
    materializedView.numConcurrentInitialSyncs = Optional.of(2);
    materializedView.matViewWriterMaxConnections = Optional.of(8);
    materializedView.mvWriteRateLimitRps = Optional.of(50);
    materializedView.embeddingProviderRpsLimit = Optional.of(25);

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(7, cliValues.size());
    assertEquals(
        new BsonInt32(10),
        cliValues.get("advancedConfigs.autoEmbedding.materializedView.numConcurrentChangeStreams"));
    assertEquals(
        new BsonInt32(5),
        cliValues.get("advancedConfigs.autoEmbedding.materializedView.numIndexingThreads"));
    assertEquals(
        new BsonInt32(6),
        cliValues.get("advancedConfigs.autoEmbedding.materializedView.numEmbeddingThreads"));
    assertEquals(
        new BsonInt32(2),
        cliValues.get("advancedConfigs.autoEmbedding.materializedView.numConcurrentInitialSyncs"));
    assertEquals(
        new BsonInt32(8),
        cliValues.get(
            "advancedConfigs.autoEmbedding.materializedView.matViewWriterMaxConnections"));
    assertEquals(
        new BsonInt32(50),
        cliValues.get("advancedConfigs.autoEmbedding.materializedView.mvWriteRateLimitRps"));
    assertEquals(
        new BsonInt32(25),
        cliValues.get("advancedConfigs.autoEmbedding.materializedView.embeddingProviderRpsLimit"));
  }

  @Test
  public void cliOptionRegistry_traverse_unsetOptionsAreOmitted() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    rootRegistry
            .advancedConfigs
            .autoEmbedding
            .materializedViewConfigCliOptionRegistry
            .numEmbeddingThreads =
        Optional.of(6);

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(1, cliValues.size());
    assertEquals(
        new BsonInt32(6),
        cliValues.get("advancedConfigs.autoEmbedding.materializedView.numEmbeddingThreads"));
  }
}
