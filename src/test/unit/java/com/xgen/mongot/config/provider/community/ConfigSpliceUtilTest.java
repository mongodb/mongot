package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

/** Tests for {@link ConfigSpliceUtil} and the CLI-merge path of {@link CommunityConfig}. */
public class ConfigSpliceUtilTest {

  @Test
  public void splice_cliValueAbsentFromYaml_mergesValueAtPath() throws BsonParseException {
    BsonDocument config = BsonDocument.parse("{\"server\":{\"grpc\":{}}}");
    Map<String, BsonValue> cliValues =
        Map.of("server.grpc.address", new BsonString("localhost:27027"));

    ConfigSpliceUtil.splice(config, cliValues);

    assertEquals(
        new BsonString("localhost:27027"),
        config.get("server").asDocument().get("grpc").asDocument().get("address"));
  }

  @Test
  public void splice_missingIntermediateDocuments_createsThem() throws BsonParseException {
    BsonDocument config = BsonDocument.parse("{}");
    Map<String, BsonValue> cliValues =
        Map.of("server.grpc.address", new BsonString("localhost:27027"));

    ConfigSpliceUtil.splice(config, cliValues);

    assertEquals(
        new BsonString("localhost:27027"),
        config.get("server").asDocument().get("grpc").asDocument().get("address"));
  }

  @Test
  public void splice_twoFieldsUnderSamePath_createAncestorOnce() throws BsonParseException {
    BsonDocument config = BsonDocument.parse("{}");
    Map<String, BsonValue> cliValues =
        Map.of(
            "metrics.enabled",
            BsonBoolean.TRUE,
            "metrics.address",
            new BsonString("localhost:9946"));

    ConfigSpliceUtil.splice(config, cliValues);

    BsonDocument metrics = config.get("metrics").asDocument();
    assertEquals(2, metrics.size());
    assertEquals(BsonBoolean.TRUE, metrics.get("enabled"));
    assertEquals(new BsonString("localhost:9946"), metrics.get("address"));
  }

  @Test
  public void splice_cliValueAlsoInYaml_throws() {
    BsonDocument config =
        BsonDocument.parse("{\"server\":{\"grpc\":{\"address\":\"myhost:27027\"}}}");
    Map<String, BsonValue> cliValues =
        Map.of("server.grpc.address", new BsonString("localhost:27027"));

    BsonParseException exception =
        assertThrows(BsonParseException.class, () -> ConfigSpliceUtil.splice(config, cliValues));

    assertTrue(exception.getMessage().contains("server.grpc.address"));
    assertEquals(
        "the YAML value must not be overwritten",
        new BsonString("myhost:27027"),
        config.get("server").asDocument().get("grpc").asDocument().get("address"));
  }

  @Test
  public void splice_multipleCliValues_mergesAll() throws BsonParseException {
    BsonDocument config = BsonDocument.parse("{}");
    Map<String, BsonValue> cliValues =
        Map.of(
            "server.grpc.address",
            new BsonString("localhost:27027"),
            "metrics.enabled",
            BsonBoolean.TRUE);

    ConfigSpliceUtil.splice(config, cliValues);

    assertEquals(
        new BsonString("localhost:27027"),
        config.get("server").asDocument().get("grpc").asDocument().get("address"));
    assertEquals(BsonBoolean.TRUE, config.get("metrics").asDocument().get("enabled"));
  }

  @Test
  public void splice_cliValueAtRoot_mergesValue() throws BsonParseException {
    BsonDocument config = BsonDocument.parse("{}");
    Map<String, BsonValue> cliValues = Map.of("storage", new BsonString("data/mongot"));

    ConfigSpliceUtil.splice(config, cliValues);

    assertEquals(new BsonString("data/mongot"), config.get("storage"));
  }

  @Test
  public void splice_ancestorIsNotAnObject_throws() {
    BsonDocument config = BsonDocument.parse("{\"server\":\"scalar\"}");
    Map<String, BsonValue> cliValues =
        Map.of("server.grpc.address", new BsonString("localhost:27027"));

    BsonParseException exception =
        assertThrows(BsonParseException.class, () -> ConfigSpliceUtil.splice(config, cliValues));

    assertTrue(exception.getMessage().contains("server"));
  }

  @Test
  public void readFromFile_withCliValues_mergesThemIntoConfig()
      throws IOException, BsonParseException {
    Path configPath =
        Path.of("src/test/unit/resources/config/provider/community/communityConfig.yaml");
    Map<String, BsonValue> cliValues = Map.of("server.name", new BsonString("cli-named"));

    CommunityConfig result =
        CommunityConfig.readFromFile(
                configPath, cliValues, Optional.of(DeploymentEnvironment.COMMUNITY))
            .config();

    assertEquals(Optional.of("cli-named"), result.serverConfig().name());
  }

  @Test
  public void readFromFile_withEmptyYamlAndCliValues_deserializes()
      throws IOException, BsonParseException {
    Path configPath =
        Path.of("src/test/unit/resources/config/provider/community/communityConfigEmpty.yaml");
    Map<String, BsonValue> cliValues =
        Map.of(
            "syncSource.replicaSet.hostAndPort",
            new BsonArray(List.of(new BsonString("mongod:27017"))),
            "syncSource.replicaSet.scramAuth.username",
            new BsonString("user"),
            "syncSource.replicaSet.scramAuth.passwordFile",
            new BsonString("replicaSet.passwd"),
            "storage.dataPath",
            new BsonString("data/mongot"),
            "server.grpc.address",
            new BsonString("localhost:27027"));

    CommunityConfig result =
        CommunityConfig.readFromFile(
                configPath, cliValues, Optional.of(DeploymentEnvironment.COMMUNITY))
            .config();

    assertEquals(
        List.of(HostAndPort.fromParts("mongod", 27017)),
        result.syncSourceConfig().replicaSet().hostandPorts());
    assertEquals("user", result.syncSourceConfig().replicaSet().scram().get().username());
    assertEquals(Path.of("data/mongot"), result.storageConfig().dataPath());
    assertEquals("localhost:27027", result.serverConfig().grpc().address());
  }

  @Test
  public void readFromFile_withCliValueAlsoInYaml_throws() {
    Path configPath =
        Path.of("src/test/unit/resources/config/provider/community/communityConfig.yaml");
    Map<String, BsonValue> cliValues =
        Map.of("server.grpc.address", new BsonString("localhost:27027"));

    assertThrows(
        BsonParseException.class,
        () ->
            CommunityConfig.readFromFile(
                configPath, cliValues, Optional.of(DeploymentEnvironment.COMMUNITY)));
  }
}
