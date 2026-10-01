package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.junit.Test;

public class AdvancedConfigsTest {

  private static final Field.Optional<String> ADVANCED_CONFIGS_FIELD =
      Field.builder("advancedConfigs").stringField().optional().noDefault();

  /**
   * Root registry that mounts the {@link AdvancedConfigs} registry under the {@code
   * advancedConfigs} block, as the Community config root does, so traversal emits the absolute YAML
   * paths.
   */
  private static final class RootConfigTestRegistry extends CliOptionRegistry {
    final AdvancedConfigs.AdvancedConfigsCliOptionRegistry advancedConfigs =
        new AdvancedConfigs.AdvancedConfigsCliOptionRegistry();

    RootConfigTestRegistry() {
      addChildRegistry(ADVANCED_CONFIGS_FIELD, this.advancedConfigs);
    }
  }

  @Test
  public void cliOptionRegistry_traverse_withAllOptions_returnsYamlPaths() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    rootRegistry
            .advancedConfigs
            .indexingConfigCliOptionRegistry
            .luceneConfigCliOptionRegistry
            .fieldLimit =
        Optional.of(500);
    rootRegistry
            .advancedConfigs
            .queryingConfigCliOptionRegistry
            .luceneConfigCliOptionRegistry
            .maxClauseLimit =
        Optional.of(1024);
    rootRegistry
            .advancedConfigs
            .replicationConfigCliOptionRegistry
            .mongoDbConfigCliOptionRegistry
            .numIndexingThreads =
        Optional.of(8);
    rootRegistry
            .advancedConfigs
            .autoEmbeddingConfigCliOptionRegistry
            .materializedViewConfigCliOptionRegistry
            .numEmbeddingThreads =
        Optional.of(4);
    rootRegistry.advancedConfigs.ftdcConfigCliOptionRegistry.directorySizeMb = Optional.of(100);

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(5, cliValues.size());
    assertEquals(new BsonInt32(500), cliValues.get("advancedConfigs.indexing.lucene.fieldLimit"));
    assertEquals(
        new BsonInt32(1024), cliValues.get("advancedConfigs.querying.lucene.maxClauseLimit"));
    assertEquals(
        new BsonInt32(8), cliValues.get("advancedConfigs.replication.mongodb.numIndexingThreads"));
    assertEquals(
        new BsonInt32(4),
        cliValues.get("advancedConfigs.autoEmbedding.materializedView.numEmbeddingThreads"));
    assertEquals(new BsonInt32(100), cliValues.get("advancedConfigs.ftdc.directorySizeMb"));
  }

  @Test
  public void cliOptionRegistry_traverse_unsetOptionsAreOmitted() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    rootRegistry
            .advancedConfigs
            .queryingConfigCliOptionRegistry
            .luceneConfigCliOptionRegistry
            .maxClauseLimit =
        Optional.of(1024);

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(1, cliValues.size());
    assertEquals(
        new BsonInt32(1024), cliValues.get("advancedConfigs.querying.lucene.maxClauseLimit"));
  }
}
