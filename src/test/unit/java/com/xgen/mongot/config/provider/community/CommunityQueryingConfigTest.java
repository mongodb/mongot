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
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.junit.Test;

public class CommunityQueryingConfigTest {

  private static final Field.Optional<String> ADVANCED_CONFIGS_FIELD =
      Field.builder("advancedConfigs").stringField().optional().noDefault();
  private static final Field.Optional<String> QUERYING_FIELD =
      Field.builder("querying").stringField().optional().noDefault();

  /**
   * Registries that mount the querying block under {@code advancedConfigs.querying}, as the
   * Community config root does, so traversal emits the absolute YAML paths.
   */
  private static final class AdvancedConfigsTestRegistry extends CliOptionRegistry {
    final CommunityQueryingConfig.CommunityQueryingConfigCliOptionRegistry querying =
        new CommunityQueryingConfig.CommunityQueryingConfigCliOptionRegistry();

    AdvancedConfigsTestRegistry() {
      addChildRegistry(QUERYING_FIELD, this.querying);
    }
  }

  private static final class RootConfigTestRegistry extends CliOptionRegistry {
    final AdvancedConfigsTestRegistry advancedConfigs = new AdvancedConfigsTestRegistry();

    RootConfigTestRegistry() {
      addChildRegistry(ADVANCED_CONFIGS_FIELD, this.advancedConfigs);
    }
  }

  private static CommunityQueryingConfig parse(BsonDocument doc) throws BsonParseException {
    PermissiveBsonParseContext context = PermissiveBsonParseContext.root();
    try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
      return CommunityQueryingConfig.fromBson(parser);
    }
  }

  @Test
  public void roundTrip_full_preservesValue() throws BsonParseException {
    CommunityQueryingConfig original =
        new CommunityQueryingConfig(
            Optional.of(
                new CommunityQueryingConfig.LuceneConfig(Optional.of(2048), Optional.of(128.0))));
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void roundTrip_empty_preservesEmptyBlock() throws BsonParseException {
    CommunityQueryingConfig original = new CommunityQueryingConfig(Optional.empty());
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void parse_exposesMaxClauseLimit() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument("lucene", new BsonDocument("maxClauseLimit", new BsonInt32(2048)));
    assertEquals(Optional.of(2048), parse(doc).luceneConfig().get().maxClauseLimit());
  }

  @Test
  public void parse_negativeMaxClauseLimit_throws() {
    BsonDocument doc =
        new BsonDocument("lucene", new BsonDocument("maxClauseLimit", new BsonInt32(-1)));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_exposesFloorSegmentMB() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument("lucene", new BsonDocument("vectorFloorSegmentMB", new BsonDouble(128.0)));
    assertEquals(Optional.of(128.0), parse(doc).luceneConfig().get().floorSegmentMB());
  }

  @Test
  public void parse_unsetFloorSegmentMB_isEmpty() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument("lucene", new BsonDocument("maxClauseLimit", new BsonInt32(2048)));
    assertEquals(Optional.empty(), parse(doc).luceneConfig().get().floorSegmentMB());
  }

  @Test
  public void parse_negativeFloorSegmentMB_throws() {
    BsonDocument doc =
        new BsonDocument("lucene", new BsonDocument("vectorFloorSegmentMB", new BsonDouble(-1.0)));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_zeroFloorSegmentMB_throws() {
    BsonDocument doc =
        new BsonDocument("lucene", new BsonDocument("vectorFloorSegmentMB", new BsonDouble(0.0)));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_unknownFieldInLuceneBlock_isIgnored() throws BsonParseException {
    BsonDocument lucene =
        new BsonDocument("maxClauseLimit", new BsonInt32(1024))
            .append("notARealField", new BsonInt32(1));
    CommunityQueryingConfig parsed = parse(new BsonDocument("lucene", lucene));
    assertEquals(Optional.of(1024), parsed.luceneConfig().get().maxClauseLimit());
  }

  @Test
  public void cliOptionRegistry_traverse_withAllOptions_returnsYamlPaths() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    var lucene = rootRegistry.advancedConfigs.querying.luceneConfigCliOptionRegistry;
    lucene.maxClauseLimit = Optional.of(1024);
    lucene.vectorFloorSegmentMB = Optional.of(128.0);

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(2, cliValues.size());
    assertEquals(
        new BsonInt32(1024), cliValues.get("advancedConfigs.querying.lucene.maxClauseLimit"));
    assertEquals(
        new BsonDouble(128.0),
        cliValues.get("advancedConfigs.querying.lucene.vectorFloorSegmentMB"));
  }

  @Test
  public void cliOptionRegistry_traverse_unsetOptionsAreOmitted() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    rootRegistry.advancedConfigs.querying.luceneConfigCliOptionRegistry.maxClauseLimit =
        Optional.of(1024);

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(1, cliValues.size());
    assertEquals(
        new BsonInt32(1024), cliValues.get("advancedConfigs.querying.lucene.maxClauseLimit"));
  }
}
