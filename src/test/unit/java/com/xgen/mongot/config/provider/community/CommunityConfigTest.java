package com.xgen.mongot.config.provider.community;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.net.HostAndPort;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.xgen.mongot.config.provider.community.ServerConfig.GrpcServerConfig;
import com.xgen.mongot.config.provider.community.ServerConfig.GrpcServerConfig.GrpcTls;
import com.xgen.mongot.config.provider.community.embedding.EmbeddingConfig;
import com.xgen.mongot.config.util.TlsMode;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.Databases;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      CommunityConfigTest.DeserializationTest.class,
      CommunityConfigTest.SerializationTest.class,
      CommunityConfigTest.CommunityConfigTestClass.class,
    })
public class CommunityConfigTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "communityConfigDeserialization";
    private static final BsonDeserializationTestSuite<CommunityConfig> TEST_SUITE =
        fromDocument(
            "src/test/unit/resources/config/provider/community",
            SUITE_NAME,
            CommunityConfig::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<CommunityConfig> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<CommunityConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<CommunityConfig>> data() {
      return TEST_SUITE.withExamples(
          simple(),
          full(),
          replicaSetX509(),
          withDefaultLogVerbosity(),
          grpcDisabledTls(),
          grpcTls(),
          grpcTlsWithPassword(),
          grpcMtls(),
          scramTlsWithCertKeyPasswordAndCa(),
          withEmbeddingEndpointOverride(),
          withMaterializedViewWriteRateLimitRps(),
          ftdcOverrides(),
          withReplicationReader(),
          withReplicationReaderTagSets(),
          withDiskMonitor());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  false, Optional.empty(), Optional.empty(), Optional.empty())))),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig("localhost:27028", Optional.empty()), Optional.empty()),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty(),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(FtdcCommunityConfig.getDefault())))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> replicaSetX509() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "replicaSet x509",
          (CommunityConfig config) -> {
            var replicaSet = config.syncSourceConfig().replicaSet();
            assertNotNull("replicaSet x509 should be present", replicaSet.x509());
            assertTrue("replicaSet should use x509 auth", replicaSet.x509().isPresent());
            assertEquals(
                Path.of("/etc/mongot/tls/client-combined.pem"),
                replicaSet.x509().get().tlsConfig().tlsCertificateKeyFile().get());
            assertEquals(
                Optional.empty(),
                replicaSet.x509().get().tlsConfig().tlsCertificateKeyFilePasswordFile());
            assertTrue(
                "caFile should be present within x509",
                replicaSet.x509().get().tlsConfig().caFile().isPresent());
            assertEquals(
                Path.of("/etc/mongot/ca.pem"), replicaSet.x509().get().tlsConfig().caFile().get());
          });
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> full() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "full",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(
                          HostAndPort.fromParts("mongod1", 27017),
                          HostAndPort.fromParts("mongod2", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.LOCAL,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  true,
                                  Optional.of(Path.of("/etc/mongot/tls/client-combined.pem")),
                                  Optional.empty(),
                                  Optional.of(Path.of("/etc/mongot/ca.pem")))))),
                  Optional.of(
                      new RouterConfig(
                          List.of(
                              HostAndPort.fromParts("mongos1", 27017),
                              HostAndPort.fromParts("mongos2", 27017)),
                          Optional.empty(),
                          Optional.of(
                              new ScramConfig(
                                  Databases.LOCAL,
                                  "user",
                                  Path.of("/etc/mongot/router.passwd"),
                                  new TlsConfig(
                                      false,
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.empty()))))),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig(
                      "localhost:27028",
                      Optional.of(
                          new GrpcTls(
                              TlsMode.MTLS,
                              Optional.of(Path.of("/etc/ssl/common-cert.pem")),
                              Optional.empty(),
                              Optional.of(Path.of("/etc/mongot-tls/ca.pem"))))),
                  Optional.of("server-name")),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty(),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(new FtdcCommunityConfig(false, 200, 20, 3000))))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig>
        withDefaultLogVerbosity() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with default log verbosity",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  false, Optional.empty(), Optional.empty(), Optional.empty())))),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig("localhost:27028", Optional.empty()), Optional.empty()),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("INFO", Optional.of("/var/log/mongot"))),
              Optional.empty(),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(FtdcCommunityConfig.getDefault())))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> grpcDisabledTls() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "grpcDisabledTls",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  false, Optional.empty(), Optional.empty(), Optional.empty())))),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig(
                      "localhost:27028",
                      Optional.of(
                          new GrpcTls(
                              TlsMode.DISABLED,
                              Optional.empty(),
                              Optional.empty(),
                              Optional.empty()))),
                  Optional.empty()),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty(),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(FtdcCommunityConfig.getDefault())))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> grpcTls() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "grpcTls",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  false, Optional.empty(), Optional.empty(), Optional.empty())))),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig(
                      "localhost:27028",
                      Optional.of(
                          new GrpcTls(
                              TlsMode.TLS,
                              Optional.of(Path.of("/etc/ssl/common-cert.pem")),
                              Optional.empty(),
                              Optional.empty()))),
                  Optional.empty()),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty(),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(FtdcCommunityConfig.getDefault())))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> grpcTlsWithPassword() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "grpcTlsWithPassword",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  false, Optional.empty(), Optional.empty(), Optional.empty())))),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig(
                      "localhost:27028",
                      Optional.of(
                          new GrpcTls(
                              TlsMode.TLS,
                              Optional.of(Path.of("/etc/ssl/common-cert.pem")),
                              Optional.of(Path.of("/etc/ssl/common-cert.pass")),
                              Optional.empty()))),
                  Optional.empty()),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty(),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(FtdcCommunityConfig.getDefault())))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> grpcMtls() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "grpcMtls",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  false, Optional.empty(), Optional.empty(), Optional.empty())))),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig(
                      "localhost:27028",
                      Optional.of(
                          new GrpcTls(
                              TlsMode.MTLS,
                              Optional.of(Path.of("/etc/ssl/common-cert.pem")),
                              Optional.empty(),
                              Optional.of(Path.of("/etc/mongot-tls/ca.pem"))))),
                  Optional.empty()),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty(),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(FtdcCommunityConfig.getDefault())))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig>
        scramTlsWithCertKeyPasswordAndCa() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "scramTlsWithCertKeyPasswordAndCa",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  true,
                                  Optional.of(Path.of("/etc/mongot/tls/client-combined.pem")),
                                  Optional.of(Path.of("/etc/mongot/secrets/cert-key-pass")),
                                  Optional.of(Path.of("/etc/mongot/ca.pem")))))),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig("localhost:27028", Optional.empty()), Optional.empty()),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty(),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(FtdcCommunityConfig.getDefault())))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig>
        withEmbeddingEndpointOverride() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with embedding endpoint override",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  false, Optional.empty(), Optional.empty(), Optional.empty())))),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig("localhost:27028", Optional.empty()), Optional.empty()),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.of(
                  new EmbeddingConfig(
                      Optional.of("https://custom-api.example.com/v1/embeddings"),
                      Optional.empty(),
                      Optional.empty(),
                      false)),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(FtdcCommunityConfig.getDefault())))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig>
        withMaterializedViewWriteRateLimitRps() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with materialized view write rate limit rps",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  false, Optional.empty(), Optional.empty(), Optional.empty())))),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig("localhost:27028", Optional.empty()), Optional.empty()),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.of(
                  new EmbeddingConfig(Optional.empty(), Optional.empty(), Optional.empty(), true)),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(
                          new CommunityAutoEmbeddingConfig(
                              Optional.of(
                                  new CommunityAutoEmbeddingConfig.MaterializedViewConfig(
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.of(50),
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.empty())))),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(FtdcCommunityConfig.getDefault())))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> ftdcOverrides() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "override ftdc default configs",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  false, Optional.empty(), Optional.empty(), Optional.empty())))),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig("localhost:27028", Optional.empty()), Optional.empty()),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty(),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(new FtdcCommunityConfig(false, 200, 20, 3000))))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> withReplicationReader() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with replicationReader",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  false, Optional.empty(), Optional.empty(), Optional.empty())))),
                  Optional.empty(),
                  Optional.of(
                      new ReadPreferenceConfig(MongoReadPreferenceName.NEAREST, Optional.empty()))),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig("localhost:27028", Optional.empty()), Optional.empty()),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty(),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(FtdcCommunityConfig.getDefault())))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> withDiskMonitor() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with diskMonitor",
          (CommunityConfig config) -> {
            var diskMonitor = config.diskMonitorConfig();
            assertEquals(0.92, diskMonitor.pauseReplicationThreshold(), 0.0);
            assertEquals(0.87, diskMonitor.resumeReplicationThreshold(), 0.0);
            assertEquals(0.97, diskMonitor.crashThreshold(), 0.0);
            assertEquals(0.82, diskMonitor.pauseInitialSyncThreshold(), 0.0);
            assertEquals(0.77, diskMonitor.resumeInitialSyncThreshold(), 0.0);
          });
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig>
        withReplicationReaderTagSets() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with replicationReader tagSets",
          (CommunityConfig config) -> {
            var replicationReader = config.syncSourceConfig().replicationReader();
            assertTrue("replicationReader should be present", replicationReader.isPresent());
            assertEquals(MongoReadPreferenceName.NEAREST, replicationReader.get().readPreference());
            assertTrue("tagSets should be present", replicationReader.get().tagSets().isPresent());
            List<TagSet> tagSets = replicationReader.get().tagSets().get();
            assertEquals(2, tagSets.size());
            assertTrue(
                "first tagSet should contain dc=east",
                tagSets.get(0).containsAll(new TagSet(new Tag("dc", "east"))));
            assertTrue(
                "second tagSet should contain dc=west",
                tagSets.get(1).containsAll(new TagSet(new Tag("dc", "west"))));
          });
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "communityConfigSerialization";
    private static final BsonSerializationTestSuite<CommunityConfig> TEST_SUITE =
        fromEncodable("src/test/unit/resources/config/provider/community", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<CommunityConfig> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<CommunityConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<CommunityConfig>> data() {
      return Collections.singletonList(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<CommunityConfig> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("/etc/mongot/replicaSet.passwd"),
                              new TlsConfig(
                                  false, Optional.empty(), Optional.empty(), Optional.empty())))),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig(
                      "localhost:27028",
                      Optional.of(
                          new GrpcTls(
                              TlsMode.MTLS,
                              Optional.of(Path.of("/etc/ssl/common-cert.pem")),
                              Optional.empty(),
                              Optional.of(Path.of("/etc/mongot-tls/ca.pem"))))),
                  Optional.of("server-name")),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty(),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(DiskMonitorConfig.getDefault()),
                      Optional.of(FtdcCommunityConfig.getDefault())))));
    }
  }

  @SuppressWarnings("NewClassNamingConvention")
  public static class CommunityConfigTestClass {

    @Test
    public void deserializeFromYaml() throws IOException, BsonParseException {
      Path configPath =
          Path.of("src/test/unit/resources/config/provider/community/communityConfig.yaml");
      CommunityConfig result = CommunityConfig.readFromFile(configPath).config();
      CommunityConfig expected =
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(
                          HostAndPort.fromParts("mongod1", 27017),
                          HostAndPort.fromParts("mongod2", 27017)),
                      Optional.empty(),
                      Optional.of(
                          new ScramConfig(
                              Databases.ADMIN,
                              "user",
                              Path.of("replicaSet.passwd"),
                              new TlsConfig(
                                  true, Optional.empty(), Optional.empty(), Optional.empty())))),
                  Optional.of(
                      new RouterConfig(
                          List.of(HostAndPort.fromParts("mongos", 27017)),
                          Optional.empty(),
                          Optional.of(
                              new ScramConfig(
                                  Databases.ADMIN,
                                  "user",
                                  Path.of("router.passwd"),
                                  new TlsConfig(
                                      false,
                                      Optional.empty(),
                                      Optional.empty(),
                                      Optional.empty()))))),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig(
                      "localhost:27028",
                      Optional.of(
                          new GrpcTls(
                              TlsMode.MTLS,
                              Optional.of(Path.of("/etc/ssl/common-cert.pem")),
                              Optional.empty(),
                              Optional.of(Path.of("/etc/mongot-tls/ca.pem"))))),
                  Optional.empty()),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("WARNING", Optional.of("/var/log/mongot"))),
              Optional.empty(),
              Optional.of(
                  new AdvancedConfigs(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(new FtdcCommunityConfig(false, 200, 20, 3000)))));

      assertEquals(expected, result);
    }

    @Test
    public void readFromFile_withX509_replicaSetHasX509Config()
        throws IOException, BsonParseException {
      Path configPath =
          Path.of("src/test/unit/resources/config/provider/community/communityConfigX509.yaml");
      CommunityConfig result = CommunityConfig.readFromFile(configPath).config();

      var replicaSet = result.syncSourceConfig().replicaSet();
      assertTrue("replicaSet should use x509", replicaSet.x509().isPresent());
      assertEquals(
          Path.of("/etc/mongot/tls/client-combined.pem"),
          replicaSet.x509().get().tlsConfig().tlsCertificateKeyFile().get());
      assertTrue(
          "tlsCertificateKeyFilePasswordFile should be present",
          replicaSet.x509().get().tlsConfig().tlsCertificateKeyFilePasswordFile().isPresent());
      assertEquals(
          Path.of("/etc/mongot/secrets/cert-key-pass"),
          replicaSet.x509().get().tlsConfig().tlsCertificateKeyFilePasswordFile().get());
      assertTrue(
          "caFile should be present within x509",
          replicaSet.x509().get().tlsConfig().caFile().isPresent());
      assertEquals(
          Path.of("/etc/mongot/tls/ca.pem"), replicaSet.x509().get().tlsConfig().caFile().get());
    }

    @Test
    public void readFromFile_withScram_tlsCertKeyFileNoCaFile()
        throws IOException, BsonParseException {
      Path configPath =
          Path.of(
              "src/test/unit/resources/config/provider/community/communityConfigScramNoCa.yaml");
      CommunityConfig result = CommunityConfig.readFromFile(configPath).config();

      var replicaSet = result.syncSourceConfig().replicaSet();
      assertTrue("replicaSet should use scram auth", replicaSet.scram().isPresent());
      ScramConfig scram = replicaSet.scram().get();
      assertTrue("tls should be enabled", scram.tls().enabled());
      assertTrue(
          "tlsCertificateKeyFile should be present",
          scram.tls().tlsCertificateKeyFile().isPresent());
      assertEquals(
          Path.of("/etc/mongot/tls/client-combined.pem"),
          scram.tls().tlsCertificateKeyFile().get());
      assertTrue(
          "caFile should be absent — JVM default trust store is used for server verification",
          scram.tls().caFile().isEmpty());
    }

    @Test
    public void readFromFile_withScram_replicaSetHasScramConfig()
        throws IOException, BsonParseException {
      Path configPath =
          Path.of("src/test/unit/resources/config/provider/community/communityConfigScram.yaml");
      CommunityConfig result = CommunityConfig.readFromFile(configPath).config();

      var replicaSet = result.syncSourceConfig().replicaSet();
      assertTrue("replicaSet should use scram auth", replicaSet.scram().isPresent());
      ScramConfig scram = replicaSet.scram().get();
      assertEquals("user", scram.username());
      assertEquals(Path.of("/etc/mongot/router.passwd"), scram.passwordFile());
      assertEquals("admin", scram.authSource());
      assertTrue("tls should be enabled", scram.tls().enabled());
      assertTrue(
          "tlsCertificateKeyFile should be present",
          scram.tls().tlsCertificateKeyFile().isPresent());
      assertEquals(
          Path.of("/etc/mongot/tls/client-combined.pem"),
          scram.tls().tlsCertificateKeyFile().get());
      assertTrue("tls.caFile should be present", scram.tls().caFile().isPresent());
      assertEquals(Path.of("/etc/mongot/ca.pem"), scram.tls().caFile().get());
    }

    @Test
    public void readFromFile_withTuningConfigs() throws IOException, BsonParseException {
      Path configPath =
          Path.of("src/test/unit/resources/config/provider/community/communityConfigTuning.yaml");
      CommunityConfig result = CommunityConfig.readFromFile(configPath).config();
      var advanced = result.advancedConfigs().get();

      var lucene = advanced.indexingConfig().get().luceneConfig().get();
      assertEquals(Optional.of(2000), lucene.refreshConfig().get().intervalMs());
      var tiered = lucene.mergePolicyConfig().get().tieredMergePolicyConfig().get();
      assertEquals(Optional.of(512), tiered.vectorMergePolicyConfig().get().mergeBudgetMb());
      assertEquals(
          Optional.of(6),
          lucene.mergeSchedulerConfig().get().concurrentSchedulerConfig().get().maxThreadCount());
      assertEquals(Optional.of(500), lucene.fieldLimit());
      var definition = advanced.indexingConfig().get().definitionConfig().get();
      assertEquals(Optional.of(7), definition.maxEmbeddedDocumentsNestingLevel());

      var queryingLucene = advanced.queryingConfig().get().luceneConfig().get();
      assertEquals(Optional.of(2048), queryingLucene.maxClauseLimit());
      assertEquals(Optional.of(128.0), queryingLucene.floorSegmentMB());

      var mongodb = advanced.replicationConfig().get().mongoDbConfig().get();
      assertEquals(Optional.of(3), mongodb.numConcurrentInitialSyncs());
      assertEquals(Optional.of(12), mongodb.numConcurrentChangeStreams());
      assertEquals(Optional.of(8), mongodb.numIndexingThreads());
      assertEquals(Optional.of(2), mongodb.numConcurrentSynonymSyncs());
      assertEquals(Optional.of(500), mongodb.changeStreamMaxTimeMs());
      assertEquals(Optional.of(900), mongodb.changeStreamCursorMaxTimeSec());
      assertEquals(Optional.of(4), mongodb.numChangeStreamDecodingThreads());
      assertEquals(Optional.of(true), mongodb.pauseAllInitialSyncs());
      assertEquals(
          Optional.of(
              List.of(
                  new ObjectId("507f1f77bcf86cd799439011"),
                  new ObjectId("507f1f77bcf86cd799439012"))),
          mongodb.pauseInitialSyncOnIndexIds());
      assertEquals(Optional.of(List.of("lsid", "txnNumber")), mongodb.excludedChangestreamFields());

      var initialSync = advanced.replicationConfig().get().initialSyncConfig().get();
      assertTrue(initialSync.bufferlessConfig().get().enableNaturalOrderScan().get());

      var matView = advanced.autoEmbeddingConfig().get().materializedViewConfig().get();
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
      assertEquals(Optional.of(60), matView.mvWriteRateLimitRps());
      assertEquals(Optional.of(30), matView.embeddingProviderRpsLimit());
      assertEquals(Optional.of(80), matView.globalMemoryBudgetHeapPercent());
      assertEquals(Optional.of(40), matView.perBatchMemoryBudgetHeapPercent());

      var cursor = advanced.cursorConfig().get();
      assertEquals(Optional.of(1800000), cursor.idleCursorHandlingRateMs());
      assertEquals(Optional.of(3600000), cursor.cursorIdleTimeMs());

      var regularBlockingRequest = advanced.regularBlockingRequestConfig().get();
      assertEquals(Optional.of(3.0), regularBlockingRequest.threadPoolSizeMultiplier());
      assertEquals(Optional.of(30.0), regularBlockingRequest.queueCapacityMultiplier());
      assertEquals(Optional.of(true), regularBlockingRequest.virtualQueueCapacity());

      var diskMonitor = advanced.diskMonitorConfig().get();
      assertEquals(0.93, diskMonitor.pauseReplicationThreshold(), 0.0);
      assertEquals(0.88, diskMonitor.resumeReplicationThreshold(), 0.0);
      assertEquals(0.98, diskMonitor.crashThreshold(), 0.0);
      assertEquals(0.86, diskMonitor.pauseInitialSyncThreshold(), 0.0);
      assertEquals(0.83, diskMonitor.resumeInitialSyncThreshold(), 0.0);

      var ftdc = advanced.ftdcConfig().get();
      assertFalse(ftdc.enabled());
      assertEquals(Integer.valueOf(250), ftdc.directorySizeMB());
      assertEquals(Integer.valueOf(25), ftdc.fileSizeMB());
      assertEquals(Integer.valueOf(2500), ftdc.collectionPeriodMillis());
    }

    @Test
    public void readFromFile_withUnsupportedTuningKeys_warnsAndIgnoresButKeepsKnownFields()
        throws IOException, BsonParseException {
      Path configPath =
          Path.of(
              "src/test/unit/resources/config/provider/community/"
                  + "communityConfigTuningUnknownKey.yaml");
      CommunityConfig.ParsedCommunityConfig parsed = CommunityConfig.readFromFile(configPath);
      CommunityConfig result = parsed.config();
      var advanced = result.advancedConfigs().get();

      assertFalse(parsed.unknownFieldExceptions().isEmpty());
      assertEquals(
          Optional.of(500), advanced.indexingConfig().get().luceneConfig().get().fieldLimit());
      assertEquals(
          Optional.of(1024), advanced.queryingConfig().get().luceneConfig().get().maxClauseLimit());
    }

    @Test
    public void readFromFile_withMalformedValue_failsFast() {
      Path configPath =
          Path.of(
              "src/test/unit/resources/config/provider/community/"
                  + "communityConfigInvalidTuning.yaml");
      assertThrows(BsonParseException.class, () -> CommunityConfig.readFromFile(configPath));
    }

    @Test
    public void readFromFile_ignoreDuplicateFieldInAdvancedConfigs()
        throws BsonParseException, IOException {
      Path configPath =
          Path.of(
              "src/test/unit/resources/config/provider/community/"
                  + "communityConfigInvalidTuningDuplicateField.yaml");
      CommunityConfig.ParsedCommunityConfig parsedConfig = CommunityConfig.readFromFile(configPath);
      assertEquals("data/mongot", parsedConfig.config().storageConfig().dataPath().toString());
      assertFalse(parsedConfig.unknownFieldExceptions().isEmpty());
    }
  }
}
