package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.Test;

public class CommunityReplicationConfigTest {

  private static final Field.Optional<String> ADVANCED_CONFIGS_FIELD =
      Field.builder("advancedConfigs").stringField().optional().noDefault();
  private static final Field.Optional<String> REPLICATION_FIELD =
      Field.builder("replication").stringField().optional().noDefault();

  /**
   * Registries that mount the replication block under {@code advancedConfigs.replication}, as the
   * Community config root does, so traversal emits the absolute YAML paths.
   */
  private static final class AdvancedConfigsTestRegistry extends CliOptionRegistry {
    final CommunityReplicationConfig.CommunityReplicationConfigCliOptionRegistry replication =
        new CommunityReplicationConfig.CommunityReplicationConfigCliOptionRegistry();

    AdvancedConfigsTestRegistry() {
      addChildRegistry(REPLICATION_FIELD, this.replication);
    }
  }

  private static final class RootConfigTestRegistry extends CliOptionRegistry {
    final AdvancedConfigsTestRegistry advancedConfigs = new AdvancedConfigsTestRegistry();

    RootConfigTestRegistry() {
      addChildRegistry(ADVANCED_CONFIGS_FIELD, this.advancedConfigs);
    }
  }

  private static CommunityReplicationConfig parse(BsonDocument doc) throws BsonParseException {
    PermissiveBsonParseContext context = PermissiveBsonParseContext.root();
    try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
      return CommunityReplicationConfig.fromBson(parser);
    }
  }

  @Test
  public void roundTrip_full_preservesAllFields() throws BsonParseException {
    var bufferless =
        new CommunityReplicationConfig.InitialSyncConfig.BufferlessInitialSyncConfig(
            Optional.of(true));
    CommunityReplicationConfig original =
        new CommunityReplicationConfig(
            Optional.of(
                new CommunityReplicationConfig.MongoDbConfig(
                    Optional.of(3),
                    Optional.of(12),
                    Optional.of(8),
                    Optional.of(2),
                    Optional.of(500),
                    Optional.of(900),
                    Optional.of(4),
                    Optional.of(true),
                    Optional.of(List.of(new ObjectId("507f1f77bcf86cd799439011"))),
                    Optional.of(List.of("lsid", "txnNumber")))),
            Optional.of(new CommunityReplicationConfig.InitialSyncConfig(Optional.of(bufferless))));
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void roundTrip_empty_preservesEmptyBlock() throws BsonParseException {
    CommunityReplicationConfig original =
        new CommunityReplicationConfig(Optional.empty(), Optional.empty());
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void parse_exposesEnableNaturalOrderScan() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument(
            "initialSync",
            new BsonDocument(
                "bufferless", new BsonDocument("enableNaturalOrderScan", new BsonBoolean(true))));
    var bufferless = parse(doc).initialSyncConfig().get().bufferlessConfig().get();
    assertEquals(Optional.of(true), bufferless.enableNaturalOrderScan());
  }

  @Test
  public void parse_enableNaturalOrderScanAbsent_isEmpty() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument("initialSync", new BsonDocument("bufferless", new BsonDocument()));
    var bufferless = parse(doc).initialSyncConfig().get().bufferlessConfig().get();
    assertEquals(Optional.empty(), bufferless.enableNaturalOrderScan());
  }

  @Test
  public void parse_unknownFieldInBufferlessBlock_isIgnored() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument(
            "initialSync",
            new BsonDocument(
                "bufferless",
                new BsonDocument("enableNaturalOrderScan", new BsonBoolean(true))
                    .append("notARealField", new BsonInt32(1))));
    var bufferless = parse(doc).initialSyncConfig().get().bufferlessConfig().get();
    assertEquals(Optional.of(true), bufferless.enableNaturalOrderScan());
  }

  @Test
  public void parse_exposesAllFields() throws BsonParseException {
    BsonDocument mongodb =
        new BsonDocument()
            .append("numConcurrentInitialSyncs", new BsonInt32(3))
            .append("numConcurrentChangeStreams", new BsonInt32(12))
            .append("numIndexingThreads", new BsonInt32(8))
            .append("numConcurrentSynonymSyncs", new BsonInt32(2))
            .append("changeStreamMaxTimeMs", new BsonInt32(500))
            .append("changeStreamCursorMaxTimeSec", new BsonInt32(900))
            .append("numChangeStreamDecodingThreads", new BsonInt32(4))
            .append("pauseAllInitialSyncs", new BsonBoolean(true))
            .append(
                "pauseInitialSyncOnIndexIds",
                new BsonArray(List.of(new BsonString("507f1f77bcf86cd799439011"))))
            .append(
                "excludedChangestreamFields",
                new BsonArray(List.of(new BsonString("lsid"), new BsonString("txnNumber"))));
    var mongo = parse(new BsonDocument("mongodb", mongodb)).mongoDbConfig().get();
    assertEquals(Optional.of(3), mongo.numConcurrentInitialSyncs());
    assertEquals(Optional.of(12), mongo.numConcurrentChangeStreams());
    assertEquals(Optional.of(8), mongo.numIndexingThreads());
    assertEquals(Optional.of(2), mongo.numConcurrentSynonymSyncs());
    assertEquals(Optional.of(500), mongo.changeStreamMaxTimeMs());
    assertEquals(Optional.of(900), mongo.changeStreamCursorMaxTimeSec());
    assertEquals(Optional.of(4), mongo.numChangeStreamDecodingThreads());
    assertEquals(Optional.of(true), mongo.pauseAllInitialSyncs());
    assertEquals(
        Optional.of(List.of(new ObjectId("507f1f77bcf86cd799439011"))),
        mongo.pauseInitialSyncOnIndexIds());
    assertEquals(Optional.of(List.of("lsid", "txnNumber")), mongo.excludedChangestreamFields());
  }

  @Test
  public void parse_invalidPauseInitialSyncOnIndexId_throws() {
    BsonDocument doc =
        new BsonDocument(
            "mongodb",
            new BsonDocument(
                "pauseInitialSyncOnIndexIds",
                new BsonArray(List.of(new BsonString("not-an-object-id")))));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_negativeChangeStreamMaxTimeMs_throws() {
    BsonDocument doc =
        new BsonDocument("mongodb", new BsonDocument("changeStreamMaxTimeMs", new BsonInt32(-1)));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_negativeNumIndexingThreads_throws() {
    BsonDocument doc =
        new BsonDocument("mongodb", new BsonDocument("numIndexingThreads", new BsonInt32(-1)));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_unknownFieldInMongodbBlock_isIgnored() throws BsonParseException {
    BsonDocument mongodb =
        new BsonDocument("numIndexingThreads", new BsonInt32(8))
            .append("notARealField", new BsonInt32(1));
    CommunityReplicationConfig parsed = parse(new BsonDocument("mongodb", mongodb));
    assertEquals(Optional.of(8), parsed.mongoDbConfig().get().numIndexingThreads());
  }

  @Test
  public void cliOptionRegistry_traverse_withAllOptions_returnsYamlPaths() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    var mongodb = rootRegistry.advancedConfigs.replication.mongoDbConfigCliOptionRegistry;
    mongodb.numConcurrentInitialSyncs = Optional.of(3);
    mongodb.numConcurrentChangeStreams = Optional.of(12);
    mongodb.numIndexingThreads = Optional.of(8);
    mongodb.numConcurrentSynonymSyncs = Optional.of(2);

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(4, cliValues.size());
    assertEquals(
        new BsonInt32(3),
        cliValues.get("advancedConfigs.replication.mongodb.numConcurrentInitialSyncs"));
    assertEquals(
        new BsonInt32(12),
        cliValues.get("advancedConfigs.replication.mongodb.numConcurrentChangeStreams"));
    assertEquals(
        new BsonInt32(8), cliValues.get("advancedConfigs.replication.mongodb.numIndexingThreads"));
    assertEquals(
        new BsonInt32(2),
        cliValues.get("advancedConfigs.replication.mongodb.numConcurrentSynonymSyncs"));
  }

  @Test
  public void cliOptionRegistry_traverse_unsetOptionsAreOmitted() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    rootRegistry.advancedConfigs.replication.mongoDbConfigCliOptionRegistry.numIndexingThreads =
        Optional.of(8);

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(1, cliValues.size());
    assertEquals(
        new BsonInt32(8), cliValues.get("advancedConfigs.replication.mongodb.numIndexingThreads"));
  }
}
