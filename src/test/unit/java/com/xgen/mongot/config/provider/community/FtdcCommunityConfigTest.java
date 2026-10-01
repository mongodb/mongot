package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.junit.Test;

public class FtdcCommunityConfigTest {

  private static final Field.Optional<String> ADVANCED_CONFIGS_FIELD =
      Field.builder("advancedConfigs").stringField().optional().noDefault();
  private static final Field.Optional<String> FTDC_FIELD =
      Field.builder("ftdc").stringField().optional().noDefault();

  /**
   * Registries that mount the ftdc block under {@code advancedConfigs.ftdc}, as the Community
   * config root does, so traversal emits the absolute YAML paths.
   */
  private static final class AdvancedConfigsTestRegistry extends CliOptionRegistry {
    final FtdcCommunityConfig.FtdcCommunityConfigCliOptionRegistry ftdc =
        new FtdcCommunityConfig.FtdcCommunityConfigCliOptionRegistry();

    AdvancedConfigsTestRegistry() {
      addChildRegistry(FTDC_FIELD, this.ftdc);
    }
  }

  private static final class RootConfigTestRegistry extends CliOptionRegistry {
    final AdvancedConfigsTestRegistry advancedConfigs = new AdvancedConfigsTestRegistry();

    RootConfigTestRegistry() {
      addChildRegistry(ADVANCED_CONFIGS_FIELD, this.advancedConfigs);
    }
  }

  @Test
  public void cliOptionRegistry_traverse_withAllOptions_returnsYamlPaths() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    FtdcCommunityConfig.FtdcCommunityConfigCliOptionRegistry registry =
        rootRegistry.advancedConfigs.ftdc;
    registry.noFtdc = Optional.of(false);
    registry.directorySizeMb = Optional.of(100);
    registry.fileSizeMb = Optional.of(10);
    registry.collectionPeriodMillis = Optional.of(1000);

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(4, cliValues.size());
    assertEquals(BsonBoolean.TRUE, cliValues.get("advancedConfigs.ftdc.enabled"));
    assertEquals(new BsonInt32(100), cliValues.get("advancedConfigs.ftdc.directorySizeMb"));
    assertEquals(new BsonInt32(10), cliValues.get("advancedConfigs.ftdc.fileSizeMb"));
    assertEquals(new BsonInt32(1000), cliValues.get("advancedConfigs.ftdc.collectionPeriodMillis"));
  }

  @Test
  public void cliOptionRegistry_traverse_unsetOptionsAreOmitted() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    rootRegistry.advancedConfigs.ftdc.directorySizeMb = Optional.of(100);

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(1, cliValues.size());
    assertEquals(new BsonInt32(100), cliValues.get("advancedConfigs.ftdc.directorySizeMb"));
  }
}
