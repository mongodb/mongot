package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.net.HostAndPort;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Test;
import picocli.CommandLine;

/**
 * Integrated tests for the wired CLI plumbing: {@code picocli} parses command-line options into the
 * {@link CommunityConfig.CommunityConfigCliOptionRegistry}, {@link CommunityConfig#readFromFile}
 * merges the resulting values into a YAML config, and the built {@link CommunityConfig} runs
 * existing validations on each overridden field.
 */
public class CommunityConfigPicoCliInjectionTest {

  private static final Path EMPTY_CONFIG =
      Path.of("src/test/unit/resources/config/provider/community/communityConfigEmpty.yaml");
  private static final Path COMPLETE_CONFIG =
      Path.of("src/test/unit/resources/config/provider/community/communityConfig.yaml");

  /**
   * One CLI option for every field that can be set on the command line. The auth blocks use SCRAM
   * on the replicaSet and x509 on the router so that both authentication mechanisms are exercised
   * without configuring two auth strategies on the same connection (which is rejected by the
   * config).
   */
  private static final String[] EVERY_OPTION = {
    "--storageDataPath",
    "data/mongot",
    "--serverName",
    "cli-server",
    "--serverGrpcAddress",
    "localhost:27027",
    "--serverGrpcTlsMode",
    "mtls",
    "--serverGrpcTlsCertificateKeyFile",
    "/etc/mongot/server.pem",
    "--serverGrpcTlsCertificateKeyFilePasswordFile",
    "/etc/mongot/server-pass.pem",
    "--serverGrpcTlsCaFile",
    "/etc/mongot/ca.pem",
    "--replicaSetHostPorts",
    "mongod1:27017",
    "--replicaSetHostPorts",
    "mongod2:27017",
    "--replicaSetScramAuthUsername",
    "user",
    "--replicaSetScramAuthPasswordFile",
    "replicaSet.passwd",
    "--replicaSetScramAuthSource",
    "admin",
    "--replicaSetScramAuthTlsEnabled",
    "--replicaSetScramAuthTlsCertificateKeyFile",
    "/etc/mongot/scram-client.pem",
    "--replicaSetScramAuthTlsCertificateKeyFilePasswordFile",
    "/etc/mongot/scram-client-pass.pem",
    "--replicaSetScramAuthTlsCaFile",
    "/etc/mongot/ca.pem",
    "--routerHostPorts",
    "mongos:27017",
    "--routerX509TlsCertificateKeyFile",
    "/etc/mongot/router-x509.pem",
    "--routerX509TlsCertificateKeyFilePasswordFile",
    "/etc/mongot/router-x509-pass.pem",
    "--routerX509CaFile",
    "/etc/mongot/ca.pem",
    "--replicationReaderReadPreference",
    "nearest",
    "--replicationReaderTagSets",
    "[[{\"name\":\"dc\",\"value\":\"ny\"}],[{\"name\":\"env\",\"value\":\"prod\"}]]",
    "--enableMetrics",
    "--metricsAddress",
    "localhost:9946",
    "--healthCheckAddress",
    "localhost:8080",
    "--loggingVerbosity",
    "WARNING",
    "--logPath",
    "/var/log/mongot",
    "--embeddingProviderEndpoint",
    "https://embedding.example.com",
    "--embeddingQueryKeyFile",
    "/etc/mongot/query-key.pem",
    "--embeddingIndexingKeyFile",
    "/etc/mongot/indexing-key.pem",
    "--isAutoEmbeddingViewWriter",
    "--indexingLuceneRefreshIntervalMs",
    "1000",
    "--indexingLuceneFieldLimit",
    "1024",
    "--indexingLuceneMergeBudgetMb",
    "128",
    "--indexingLuceneMergeMaxThreadCount",
    "4",
    "--queryingLuceneMaxClauseLimit",
    "2048",
    "--queryingLuceneVectorFloorSegmentMB",
    "128.0",
    "--replicationMongodbNumConcurrentInitialSyncs",
    "2",
    "--replicationMongodbNumConcurrentChangeStreams",
    "4",
    "--replicationMongodbNumIndexingThreads",
    "8",
    "--replicationMongodbNumConcurrentSynonymSyncs",
    "1",
    "--autoEmbeddingMatViewNumConcurrentChangeStreams",
    "4",
    "--autoEmbeddingMatViewNumIndexingThreads",
    "8",
    "--autoEmbeddingMatViewNumEmbeddingThreads",
    "8",
    "--autoEmbeddingMatViewNumConcurrentInitialSyncs",
    "2",
    "--autoEmbeddingMatViewWriterMaxConnections",
    "10",
    "--autoEmbeddingMatViewWriteRateLimitRps",
    "100",
    "--autoEmbeddingMatViewProviderRpsLimit",
    "50",
    "--noFtdc",
    "--ftdcDirectorySizeMb",
    "200",
    "--ftdcFileSizeMb",
    "20",
    "--ftdcCollectionPeriodMs",
    "3000",
  };

  /**
   * The smallest CLI option set that builds a valid {@link CommunityConfig} from the empty YAML.
   */
  private static final String[] MINIMUM_VALID_CLI_OPTIONS = {
    "--storageDataPath",
    "data/mongot",
    "--serverGrpcAddress",
    "localhost:27027",
    "--replicaSetHostPorts",
    "mongod1:27017",
    "--replicaSetScramAuthUsername",
    "user",
    "--replicaSetScramAuthPasswordFile",
    "replicaSet.passwd",
  };

  private static Map<String, BsonValue> cliValues(String... args) {
    CommunityConfig.CommunityConfigCliOptionRegistry registry =
        new CommunityConfig.CommunityConfigCliOptionRegistry();
    new CommandLine(registry).parseArgs(args);
    return registry.traverse();
  }

  /** Parses {@code baseOptions} plus any {@code extraOptions} into CLI values. */
  private static Map<String, BsonValue> cliValues(String[] baseOptions, String... extraOptions) {
    String[] options = new String[baseOptions.length + extraOptions.length];
    System.arraycopy(baseOptions, 0, options, 0, baseOptions.length);
    System.arraycopy(extraOptions, 0, options, baseOptions.length, extraOptions.length);
    return cliValues(options);
  }

  private static CommunityConfig parse(BsonDocument doc) throws BsonParseException {
    PermissiveBsonParseContext context = PermissiveBsonParseContext.root();
    try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
      return CommunityConfig.fromBson(parser);
    }
  }

  @Test
  public void readFromFile_emptyYamlAndEveryCliOption_buildsCompleteConfig() throws Exception {
    CommunityConfig config =
        CommunityConfig.readFromFile(
                EMPTY_CONFIG, cliValues(EVERY_OPTION), Optional.of(DeploymentEnvironment.COMMUNITY))
            .config();

    assertEquals(Path.of("data/mongot"), config.storageConfig().dataPath());
    assertEquals("cli-server", config.serverConfig().name().get());
    assertEquals("localhost:27027", config.serverConfig().grpc().address());

    var replicaSet = config.syncSourceConfig().replicaSet();
    assertEquals(
        List.of(HostAndPort.fromString("mongod1:27017"), HostAndPort.fromString("mongod2:27017")),
        replicaSet.hostandPorts());
    assertEquals("user", replicaSet.scram().get().username());
    assertEquals(Path.of("replicaSet.passwd"), replicaSet.scram().get().passwordFile());
    assertEquals("admin", replicaSet.scram().get().authSource());

    var router = config.syncSourceConfig().router().get();
    assertEquals(List.of(HostAndPort.fromString("mongos:27017")), router.hostandPorts());
    assertEquals(
        Path.of("/etc/mongot/router-x509.pem"),
        router.x509().get().tlsConfig().tlsCertificateKeyFile().get());

    var replicationReader = config.syncSourceConfig().replicationReader().get();
    assertEquals(MongoReadPreferenceName.NEAREST, replicationReader.readPreference());
    assertEquals(
        List.of(new TagSet(new Tag("dc", "ny")), new TagSet(new Tag("env", "prod"))),
        replicationReader.tagSets().get());

    assertEquals(true, config.metricsConfig().get().enabled());
    assertEquals("localhost:9946", config.metricsConfig().get().address());
    assertEquals("localhost:8080", config.healthCheckConfig().get().address());
    assertEquals("WARNING", config.loggingConfig().get().verbosity());
    assertEquals("/var/log/mongot", config.loggingConfig().get().logPath().get());

    assertEquals(new FtdcCommunityConfig(false, 200, 20, 3000), config.ftdcConfig());
  }

  @Test
  public void readFromFile_noFtdcFlag_setsFtdcEnabledToFalse() throws Exception {
    CommunityConfig config =
        CommunityConfig.readFromFile(
                EMPTY_CONFIG,
                cliValues(MINIMUM_VALID_CLI_OPTIONS, "--noFtdc"),
                Optional.of(DeploymentEnvironment.COMMUNITY))
            .config();

    assertEquals(false, config.ftdcConfig().enabled());
  }

  @Test
  public void readFromFile_emptyYamlAndCliOptionsMissingRequiredServerGrpcAddress_throws() {
    Map<String, BsonValue> cliValues =
        cliValues(
            "--storageDataPath", "data/mongot",
            "--replicaSetHostPorts", "mongod1:27017",
            "--replicaSetScramAuthUsername", "user",
            "--replicaSetScramAuthPasswordFile", "replicaSet.passwd");

    assertThrows(
        BsonParseException.class,
        () ->
            CommunityConfig.readFromFile(
                EMPTY_CONFIG, cliValues, Optional.of(DeploymentEnvironment.COMMUNITY)));
  }

  @Test
  public void readFromFile_completeYamlAndCliOverrideOfSpecifiedField_throws() {
    Map<String, BsonValue> cliValues = cliValues("--serverGrpcAddress", "localhost:9999");

    assertThrows(
        BsonParseException.class,
        () ->
            CommunityConfig.readFromFile(
                COMPLETE_CONFIG, cliValues, Optional.of(DeploymentEnvironment.COMMUNITY)));
  }

  @Test
  public void readFromFile_cliOptionsSpecifyBothX509AndScramAuth_throws() {
    Map<String, BsonValue> cliValues =
        cliValues(
            MINIMUM_VALID_CLI_OPTIONS,
            "--replicaSetX509TlsCertificateKeyFile",
            "/etc/mongot/x509.pem",
            "--replicaSetX509CaFile",
            "/etc/mongot/ca.pem");

    assertThrows(
        BsonParseException.class,
        () ->
            CommunityConfig.readFromFile(
                EMPTY_CONFIG, cliValues, Optional.of(DeploymentEnvironment.COMMUNITY)));
  }

  @Test
  public void readFromFile_emptyYamlAndReplicaSetHostPortWithNonNumericPort_throws() {
    Map<String, BsonValue> cliValues =
        cliValues(
            "--storageDataPath", "data/mongot",
            "--serverGrpcAddress", "localhost:27027",
            "--replicaSetHostPorts", "localhost:notaport");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            CommunityConfig.readFromFile(
                EMPTY_CONFIG, cliValues, Optional.of(DeploymentEnvironment.COMMUNITY)));
  }

  @Test
  public void readFromFile_emptyYamlAndRouterHostPortWithNonNumericPort_throws() {
    Map<String, BsonValue> cliValues =
        cliValues(
            "--storageDataPath", "data/mongot",
            "--serverGrpcAddress", "localhost:27027",
            "--replicaSetHostPorts", "mongod1:27017",
            "--replicaSetScramAuthUsername", "user",
            "--replicaSetScramAuthPasswordFile", "replicaSet.passwd",
            "--routerHostPorts", "mongos:notaport");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            CommunityConfig.readFromFile(
                EMPTY_CONFIG, cliValues, Optional.of(DeploymentEnvironment.COMMUNITY)));
  }

  @Test
  public void cliOptionsApplied_thenSerializeDeserialize_roundTripsToExpectedConfig()
      throws Exception {
    CommunityConfig config =
        CommunityConfig.readFromFile(
                EMPTY_CONFIG,
                cliValues(MINIMUM_VALID_CLI_OPTIONS, "--serverName", "roundtrip-server"),
                Optional.of(DeploymentEnvironment.COMMUNITY))
            .config();

    CommunityConfig roundTripped = parse(config.toBson());

    assertEquals(config, roundTripped);
  }

  @Test
  public void readFromFile_cliOptionsOmittingDefaults_useDefaultValues() throws Exception {
    CommunityConfig config =
        CommunityConfig.readFromFile(
                EMPTY_CONFIG,
                cliValues(MINIMUM_VALID_CLI_OPTIONS),
                Optional.of(DeploymentEnvironment.COMMUNITY))
            .config();

    // replicaSet.scramAuth.authSource was not supplied, so it falls back to its default.
    assertEquals("admin", config.syncSourceConfig().replicaSet().scram().get().authSource());
    // No FTDC options were supplied, so the FTDC block falls back to its defaults.
    assertEquals(new FtdcCommunityConfig(true, 100, 10, 1000), config.ftdcConfig());
  }
}
