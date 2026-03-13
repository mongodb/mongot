package com.xgen.mongot.config.provider.community;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;
import static org.junit.Assert.assertEquals;

import com.google.common.net.HostAndPort;
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
          withDefaultLogVerbosity(),
          grpcDisabledTls(),
          grpcTls(),
          grpcMtls(),
          withEmbeddingEndpointOverride(),
          withEmbeddingMvWriteRateLimitRps(),
          ftdcOverrides());
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
                      "user",
                      Path.of("/etc/mongot/replicaSet.passwd"),
                      Databases.ADMIN,
                      false,
                      MongoReadPreferenceName.SECONDARY_PREFERRED),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig("localhost:27028", Optional.empty()), Optional.empty()),
              FtdcCommunityConfig.getDefault(),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty()));
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
                      "user",
                      Path.of("/etc/mongot/replicaSet.passwd"),
                      Databases.LOCAL,
                      true,
                      MongoReadPreferenceName.PRIMARY_PREFERRED),
                  Optional.of(
                      new RouterConfig(
                          List.of(
                              HostAndPort.fromParts("mongos1", 27017),
                              HostAndPort.fromParts("mongos2", 27017)),
                          "user",
                          Path.of("/etc/mongot/router.passwd"),
                          Databases.LOCAL,
                          false,
                          MongoReadPreferenceName.PRIMARY_PREFERRED)),
                  Optional.of(Path.of("/etc/mongot/ca.pem"))),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig(
                      "localhost:27028",
                      Optional.of(
                          new GrpcTls(
                              TlsMode.MTLS,
                              Optional.of(Path.of("/etc/ssl/common-cert.pem")),
                              Optional.of(Path.of("/etc/mongot-tls/ca.pem"))))),
                  Optional.of("server-name")),
              new FtdcCommunityConfig(false, 200, 20, 3000),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig>
        withDefaultLogVerbosity() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with default log verbosity",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      "user",
                      Path.of("/etc/mongot/replicaSet.passwd"),
                      Databases.ADMIN,
                      false,
                      MongoReadPreferenceName.SECONDARY_PREFERRED),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig("localhost:27028", Optional.empty()), Optional.empty()),
              FtdcCommunityConfig.getDefault(),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("INFO", Optional.of("/var/log/mongot"))),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> grpcDisabledTls() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "grpcDisabledTls",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      "user",
                      Path.of("/etc/mongot/replicaSet.passwd"),
                      Databases.ADMIN,
                      false,
                      MongoReadPreferenceName.SECONDARY_PREFERRED),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig(
                      "localhost:27028",
                      Optional.of(
                          new GrpcTls(TlsMode.DISABLED, Optional.empty(), Optional.empty()))),
                  Optional.empty()),
              FtdcCommunityConfig.getDefault(),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> grpcTls() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "grpcTls",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      "user",
                      Path.of("/etc/mongot/replicaSet.passwd"),
                      Databases.ADMIN,
                      false,
                      MongoReadPreferenceName.SECONDARY_PREFERRED),
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
                              Optional.empty()))),
                  Optional.empty()),
              FtdcCommunityConfig.getDefault(),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> grpcMtls() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "grpcMtls",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      "user",
                      Path.of("/etc/mongot/replicaSet.passwd"),
                      Databases.ADMIN,
                      false,
                      MongoReadPreferenceName.SECONDARY_PREFERRED),
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
                              Optional.of(Path.of("/etc/mongot-tls/ca.pem"))))),
                  Optional.empty()),
              FtdcCommunityConfig.getDefault(),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig>
        withEmbeddingEndpointOverride() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with embedding endpoint override",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      "user",
                      Path.of("/etc/mongot/replicaSet.passwd"),
                      Databases.ADMIN,
                      false,
                      MongoReadPreferenceName.SECONDARY_PREFERRED),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig("localhost:27028", Optional.empty()), Optional.empty()),
              FtdcCommunityConfig.getDefault(),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.of(
                  new EmbeddingConfig(
                      Optional.of("https://custom-api.example.com/v1/embeddings"),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      false))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig>
        withEmbeddingMvWriteRateLimitRps() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with embedding mv write rate limit rps",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      "user",
                      Path.of("/etc/mongot/replicaSet.passwd"),
                      Databases.ADMIN,
                      false,
                      MongoReadPreferenceName.SECONDARY_PREFERRED),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig("localhost:27028", Optional.empty()), Optional.empty()),
              FtdcCommunityConfig.getDefault(),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.of(
                  new EmbeddingConfig(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.of(50),
                      true))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<CommunityConfig> ftdcOverrides() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "override ftdc default configs",
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(HostAndPort.fromParts("mongod", 27017)),
                      "user",
                      Path.of("/etc/mongot/replicaSet.passwd"),
                      Databases.ADMIN,
                      false,
                      MongoReadPreferenceName.SECONDARY_PREFERRED),
                  Optional.empty(),
                  Optional.empty()),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig("localhost:27028", Optional.empty()), Optional.empty()),
              new FtdcCommunityConfig(false, 200, 20, 3000),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty()));
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
                      "user",
                      Path.of("/etc/mongot/replicaSet.passwd"),
                      Databases.ADMIN,
                      false,
                      MongoReadPreferenceName.SECONDARY_PREFERRED),
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
                              Optional.of(Path.of("/etc/mongot-tls/ca.pem"))))),
                  Optional.of("server-name")),
              FtdcCommunityConfig.getDefault(),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("DEBUG", Optional.of("/var/log/mongot"))),
              Optional.empty()));
    }
  }

  @SuppressWarnings("NewClassNamingConvention")
  public static class CommunityConfigTestClass {

    @Test
    public void deserializeFromYaml() throws IOException, BsonParseException {
      Path configPath =
          Path.of("src/test/unit/resources/config/provider/community/communityConfig.yaml");
      CommunityConfig result = CommunityConfig.readFromFile(configPath);
      CommunityConfig expected =
          new CommunityConfig(
              new SyncSourceConfig(
                  new ReplicaSetConfig(
                      List.of(
                          HostAndPort.fromParts("mongod1", 27017),
                          HostAndPort.fromParts("mongod2", 27017)),
                      "user",
                      Path.of("replicaSet.passwd"),
                      Databases.ADMIN,
                      true,
                      MongoReadPreferenceName.PRIMARY_PREFERRED),
                  Optional.of(
                      new RouterConfig(
                          List.of(HostAndPort.fromParts("mongos", 27017)),
                          "user",
                          Path.of("router.passwd"),
                          Databases.ADMIN,
                          false,
                          MongoReadPreferenceName.PRIMARY)),
                  Optional.of(Path.of("/etc/mongot/ca.pem"))),
              new StorageConfig(Path.of("data/mongot")),
              new ServerConfig(
                  new GrpcServerConfig(
                      "localhost:27028",
                      Optional.of(
                          new GrpcTls(
                              TlsMode.MTLS,
                              Optional.of(Path.of("/etc/ssl/common-cert.pem")),
                              Optional.of(Path.of("/etc/mongot-tls/ca.pem"))))),
                  Optional.empty()),
              new FtdcCommunityConfig(false, 200, 20, 3000),
              Optional.of(new MetricsConfig(true, "localhost:9946")),
              Optional.of(new HealthCheckConfig("localhost:8080")),
              Optional.of(new LoggingConfig("WARNING", Optional.of("/var/log/mongot"))),
              Optional.empty());

      assertEquals(expected, result);
    }
  }
}
