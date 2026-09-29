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

public class CommunityIndexingConfigTest {

  private static final Field.Optional<String> ADVANCED_CONFIGS_FIELD =
      Field.builder("advancedConfigs").stringField().optional().noDefault();
  private static final Field.Optional<String> INDEXING_FIELD =
      Field.builder("indexing").stringField().optional().noDefault();

  /**
   * Registries that mount the indexing block under {@code advancedConfigs.indexing}, as the
   * Community config root does, so traversal emits the absolute YAML paths.
   */
  private static final class AdvancedConfigsTestRegistry extends CliOptionRegistry {
    final CommunityIndexingConfig.CommunityIndexingConfigCliOptionRegistry indexing =
        new CommunityIndexingConfig.CommunityIndexingConfigCliOptionRegistry();

    AdvancedConfigsTestRegistry() {
      addChildRegistry(INDEXING_FIELD, this.indexing);
    }
  }

  private static final class RootConfigTestRegistry extends CliOptionRegistry {
    final AdvancedConfigsTestRegistry advancedConfigs = new AdvancedConfigsTestRegistry();

    RootConfigTestRegistry() {
      addChildRegistry(ADVANCED_CONFIGS_FIELD, this.advancedConfigs);
    }
  }

  private static CommunityIndexingConfig fullConfig() {
    var vector =
        new CommunityIndexingConfig.LuceneConfig.MergePolicyConfig.TieredMergePolicyConfig
            .VectorMergePolicyConfig(Optional.of(512));
    var tiered =
        new CommunityIndexingConfig.LuceneConfig.MergePolicyConfig.TieredMergePolicyConfig(
            Optional.of(vector));
    var mergePolicy =
        new CommunityIndexingConfig.LuceneConfig.MergePolicyConfig(Optional.of(tiered));
    var concurrentScheduler =
        new CommunityIndexingConfig.LuceneConfig.MergeSchedulerConfig
            .ConcurrentMergeSchedulerConfig(Optional.of(6));
    var mergeScheduler =
        new CommunityIndexingConfig.LuceneConfig.MergeSchedulerConfig(
            Optional.of(concurrentScheduler));
    var refresh = new CommunityIndexingConfig.LuceneConfig.RefreshConfig(Optional.of(2000));
    var lucene =
        new CommunityIndexingConfig.LuceneConfig(
            Optional.of(refresh),
            Optional.of(mergePolicy),
            Optional.of(mergeScheduler),
            Optional.of(500));
    var definition = new CommunityIndexingConfig.IndexDefinitionConfig(Optional.of(7));
    return new CommunityIndexingConfig(Optional.of(lucene), Optional.of(definition));
  }

  private static CommunityIndexingConfig parse(BsonDocument doc) throws BsonParseException {
    PermissiveBsonParseContext context = PermissiveBsonParseContext.root();
    try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
      return CommunityIndexingConfig.fromBson(parser);
    }
  }

  @Test
  public void roundTrip_full_preservesAllFields() throws BsonParseException {
    CommunityIndexingConfig original = fullConfig();
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void roundTrip_empty_preservesEmptyBlocks() throws BsonParseException {
    CommunityIndexingConfig original =
        new CommunityIndexingConfig(Optional.empty(), Optional.empty());
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void parse_full_exposesExpectedValues() throws BsonParseException {
    CommunityIndexingConfig parsed = parse(fullConfig().toBson());

    var lucene = parsed.luceneConfig().get();
    assertEquals(Optional.of(2000), lucene.refreshConfig().get().intervalMs());
    assertEquals(Optional.of(500), lucene.fieldLimit());
    var tiered = lucene.mergePolicyConfig().get().tieredMergePolicyConfig().get();
    assertEquals(Optional.of(512), tiered.vectorMergePolicyConfig().get().mergeBudgetMb());
    assertEquals(
        Optional.of(6),
        lucene.mergeSchedulerConfig().get().concurrentSchedulerConfig().get().maxThreadCount());
    assertEquals(
        Optional.of(7), parsed.definitionConfig().get().maxEmbeddedDocumentsNestingLevel());
  }

  @Test
  public void parse_negativeFieldLimit_throws() {
    BsonDocument doc =
        new BsonDocument("lucene", new BsonDocument("fieldLimit", new BsonInt32(-1)));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_unknownFieldInLuceneBlock_isIgnored() throws BsonParseException {
    BsonDocument lucene =
        new BsonDocument("fieldLimit", new BsonInt32(500))
            .append("notARealField", new BsonInt32(1));
    CommunityIndexingConfig parsed = parse(new BsonDocument("lucene", lucene));
    assertEquals(Optional.of(500), parsed.luceneConfig().get().fieldLimit());
  }

  @Test
  public void cliOptionRegistry_traverse_withAllOptions_returnsYamlPaths() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    var lucene = rootRegistry.advancedConfigs.indexing.luceneConfigCliOptionRegistry;
    lucene.fieldLimit = Optional.of(500);
    lucene.refreshConfigCliOptionRegistry.intervalMs = Optional.of(2000);
    lucene
            .mergePolicyConfigCliOptionRegistry
            .tieredMergePolicyConfigCliOptionRegistry
            .vectorMergePolicyConfigCliOptionRegistry
            .mergeBudgetMb =
        Optional.of(512);
    lucene
            .mergeSchedulerConfigCliOptionRegistry
            .concurrentMergeSchedulerConfigCliOptionRegistry
            .maxThreadCount =
        Optional.of(6);

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(4, cliValues.size());
    assertEquals(new BsonInt32(500), cliValues.get("advancedConfigs.indexing.lucene.fieldLimit"));
    assertEquals(
        new BsonInt32(2000), cliValues.get("advancedConfigs.indexing.lucene.refresh.intervalMs"));
    assertEquals(
        new BsonInt32(512),
        cliValues.get(
            "advancedConfigs.indexing.lucene.mergePolicy.tiered.vectorMergePolicy.mergeBudgetMb"));
    assertEquals(
        new BsonInt32(6),
        cliValues.get("advancedConfigs.indexing.lucene.mergeScheduler.concurrent.maxThreadCount"));
  }

  @Test
  public void cliOptionRegistry_traverse_unsetOptionsAreOmitted() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    rootRegistry.advancedConfigs.indexing.luceneConfigCliOptionRegistry.fieldLimit =
        Optional.of(500);

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(1, cliValues.size());
    assertEquals(new BsonInt32(500), cliValues.get("advancedConfigs.indexing.lucene.fieldLimit"));
  }
}
