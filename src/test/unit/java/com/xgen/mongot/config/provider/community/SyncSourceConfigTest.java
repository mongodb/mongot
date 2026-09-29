package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;

import com.google.common.net.HostAndPort;
import com.mongodb.ReadPreference;
import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonParseException;
import org.junit.Test;

public class SyncSourceConfigTest {

  private static final List<HostAndPort> HOSTS = List.of(HostAndPort.fromParts("localhost", 27017));

  private static final Field.Optional<String> SYNC_SOURCE_FIELD =
      Field.builder("syncSource").stringField().optional().noDefault();

  /**
   * Root registry that mounts the {@link SyncSourceConfig} registry under the {@code syncSource}
   * block, as the Community config root does, so traversal emits the absolute YAML paths.
   */
  private static final class RootConfigTestRegistry extends CliOptionRegistry {
    final SyncSourceConfig.SyncSourceConfigCliOptionRegistry syncSourceConfigCliOptionRegistry =
        new SyncSourceConfig.SyncSourceConfigCliOptionRegistry();

    RootConfigTestRegistry() {
      addChildRegistry(SYNC_SOURCE_FIELD, this.syncSourceConfigCliOptionRegistry);
    }
  }

  private static ReplicaSetConfig replicaSet() {
    return new ReplicaSetConfig(HOSTS, Optional.empty(), Optional.empty(), Optional.empty());
  }

  private static SyncSourceConfig syncSource(Optional<ReadPreferenceConfig> replicationReader) {
    return new SyncSourceConfig(replicaSet(), Optional.empty(), replicationReader);
  }

  @Test
  public void getReplicationReaderReadPreference_replicationReaderPresent_usesIt() {
    var config =
        syncSource(
            Optional.of(
                new ReadPreferenceConfig(MongoReadPreferenceName.NEAREST, Optional.empty())));
    assertEquals(ReadPreference.nearest(), config.getReplicationReaderReadPreference());
  }

  @Test
  public void
      getReplicationReaderReadPreference_replicationReaderAbsent_defaultsToSecondaryPreferred() {
    var config = syncSource(Optional.empty());
    assertEquals(ReadPreference.secondaryPreferred(), config.getReplicationReaderReadPreference());
  }

  @Test
  public void cliOptionRegistry_traverse_withAllReplicaSetOptions_returnsYamlPaths() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    SyncSourceConfig.SyncSourceConfigCliOptionRegistry registry =
        rootRegistry.syncSourceConfigCliOptionRegistry;
    ReplicaSetConfig.ReplicaSetConfigCliOptionRegistry replicaSet =
        registry.replicaSetConfigCliOptionRegistry;
    replicaSet.hostPorts = List.of("mongod1:27017", "mongod2:27017");
    replicaSet.srvHost = Optional.of("replicaSet.mongodb-dev.net");
    replicaSet.scramConfigCliOptionRegistry.username = Optional.of("user");
    replicaSet.scramConfigCliOptionRegistry.passwordFile =
        Optional.of(Path.of("replicaSet.passwd"));
    replicaSet.scramConfigCliOptionRegistry.authSource = Optional.of("admin");
    replicaSet.scramConfigCliOptionRegistry.tlsCliOptionRegistry.enabled =
        Optional.of(Boolean.TRUE);
    replicaSet.scramConfigCliOptionRegistry.tlsCliOptionRegistry.tlsCertificateKeyFile =
        Optional.of(Path.of("/etc/mongot/client-combined.pem"));
    replicaSet.scramConfigCliOptionRegistry.tlsCliOptionRegistry.caFile =
        Optional.of(Path.of("/etc/mongot/ca.pem"));
    replicaSet.x509ConfigCliOptionRegistry.tlsCertificateKeyFile =
        Optional.of(Path.of("/etc/mongot/x509.pem"));
    replicaSet.x509ConfigCliOptionRegistry.caFile = Optional.of(Path.of("/etc/mongot/x509-ca.pem"));

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(10, cliValues.size());
    assertEquals(
        new BsonArray(List.of(new BsonString("mongod1:27017"), new BsonString("mongod2:27017"))),
        cliValues.get("syncSource.replicaSet.hostAndPort"));
    assertEquals(
        new BsonString("replicaSet.mongodb-dev.net"),
        cliValues.get("syncSource.replicaSet.srvHost"));
    assertEquals(new BsonString("user"), cliValues.get("syncSource.replicaSet.scramAuth.username"));
    assertEquals(
        new BsonString("replicaSet.passwd"),
        cliValues.get("syncSource.replicaSet.scramAuth.passwordFile"));
    assertEquals(
        new BsonString("admin"), cliValues.get("syncSource.replicaSet.scramAuth.authSource"));
    assertEquals(
        new BsonString("/etc/mongot/client-combined.pem"),
        cliValues.get("syncSource.replicaSet.scramAuth.tls.tlsCertificateKeyFile"));
    assertEquals(BsonBoolean.TRUE, cliValues.get("syncSource.replicaSet.scramAuth.tls.enabled"));
    assertEquals(
        new BsonString("/etc/mongot/ca.pem"),
        cliValues.get("syncSource.replicaSet.scramAuth.tls.caFile"));
    assertEquals(
        new BsonString("/etc/mongot/x509.pem"),
        cliValues.get("syncSource.replicaSet.x509.tlsCertificateKeyFile"));
    assertEquals(
        new BsonString("/etc/mongot/x509-ca.pem"),
        cliValues.get("syncSource.replicaSet.x509.caFile"));
  }

  @Test
  public void cliOptionRegistry_traverse_unsetOptionsAreOmitted() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    rootRegistry.syncSourceConfigCliOptionRegistry.replicaSetConfigCliOptionRegistry.hostPorts =
        List.of("mongod1:27017");

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(1, cliValues.size());
    assertEquals(
        new BsonArray(List.of(new BsonString("mongod1:27017"))),
        cliValues.get("syncSource.replicaSet.hostAndPort"));
  }

  @Test
  public void cliOptionRegistry_traverse_withAllRouterOptions_returnsYamlPaths() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    SyncSourceConfig.SyncSourceConfigCliOptionRegistry registry =
        rootRegistry.syncSourceConfigCliOptionRegistry;
    RouterConfig.RouterConfigCliOptionRegistry router = registry.routerConfigCliOptionRegistry;
    router.hostPorts = List.of("mongos:27017");
    router.srvHost = Optional.of("router.mongodb-dev.net");
    router.scramConfigCliOptionRegistry.username = Optional.of("router-user");
    router.scramConfigCliOptionRegistry.passwordFile = Optional.of(Path.of("router.passwd"));
    router.scramConfigCliOptionRegistry.authSource = Optional.of("admin");
    router.scramConfigCliOptionRegistry.tlsCliOptionRegistry.enabled = Optional.of(Boolean.TRUE);
    router.x509ConfigCliOptionRegistry.tlsCertificateKeyFile =
        Optional.of(Path.of("/etc/mongot/router-x509.pem"));

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(7, cliValues.size());
    assertEquals(
        new BsonArray(List.of(new BsonString("mongos:27017"))),
        cliValues.get("syncSource.router.hostAndPort"));
    assertEquals(
        new BsonString("router.mongodb-dev.net"), cliValues.get("syncSource.router.srvHost"));
    assertEquals(
        new BsonString("router-user"), cliValues.get("syncSource.router.scramAuth.username"));
    assertEquals(
        new BsonString("router.passwd"), cliValues.get("syncSource.router.scramAuth.passwordFile"));
    assertEquals(new BsonString("admin"), cliValues.get("syncSource.router.scramAuth.authSource"));
    assertEquals(BsonBoolean.TRUE, cliValues.get("syncSource.router.scramAuth.tls.enabled"));
    assertEquals(
        new BsonString("/etc/mongot/router-x509.pem"),
        cliValues.get("syncSource.router.x509.tlsCertificateKeyFile"));
  }

  @Test
  public void replicationReaderCliOptionRegistry_traverse_withReadPreference_returnsYamlPath() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    rootRegistry
            .syncSourceConfigCliOptionRegistry
            .replicationReaderCliOptionRegistry
            .readPreference =
        Optional.of("nearest");

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(1, cliValues.size());
    assertEquals(
        new BsonString("nearest"), cliValues.get("syncSource.replicationReader.readPreference"));
  }

  @Test
  public void
      replicationReaderCliOptionRegistry_traverse_withTagSets_parsesJsonArrayIntoBsonArray() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    rootRegistry.syncSourceConfigCliOptionRegistry.replicationReaderCliOptionRegistry.tagSets =
        Optional.of(
            "[[{\"name\":\"dc\",\"value\":\"ny\"}],[{\"name\":\"env\",\"value\":\"prod\"}]]");

    Map<String, BsonValue> cliValues = rootRegistry.traverse();

    assertEquals(
        BsonArray.parse(
            "[[{\"name\":\"dc\",\"value\":\"ny\"}],[{\"name\":\"env\",\"value\":\"prod\"}]]"),
        cliValues.get("syncSource.replicationReader.tagSets"));
  }

  @Test(expected = JsonParseException.class)
  public void replicationReaderCliOptionRegistry_traverse_withMalformedTagSets_throws() {
    RootConfigTestRegistry rootRegistry = new RootConfigTestRegistry();
    rootRegistry.syncSourceConfigCliOptionRegistry.replicationReaderCliOptionRegistry.tagSets =
        Optional.of("not a valid JSON array: [");

    rootRegistry.traverse();
  }
}
