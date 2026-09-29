package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.community.embedding.EmbeddingConfig;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.YamlCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record CommunityConfig(
    SyncSourceConfig syncSourceConfig,
    StorageConfig storageConfig,
    ServerConfig serverConfig,
    Optional<MetricsConfig> metricsConfig,
    Optional<HealthCheckConfig> healthCheckConfig,
    Optional<LoggingConfig> loggingConfig,
    Optional<EmbeddingConfig> embeddingConfig,
    Optional<AdvancedConfigs> advancedConfigs,
    Optional<TracingConfig> tracingConfig,
    Optional<CommunityDurabilityConfig> durabilityConfig)
    implements DocumentEncodable {

  private static final Logger LOG = LoggerFactory.getLogger(CommunityConfig.class);

  private static class Fields {
    public static final Field.Required<SyncSourceConfig> SYNC_SOURCE =
        Field.builder("syncSource")
            .classField(SyncSourceConfig::fromBson)
            .disallowUnknownFields()
            .required();

    public static final Field.Required<StorageConfig> STORAGE =
        Field.builder("storage")
            .classField(StorageConfig::fromBson)
            .disallowUnknownFields()
            .required();

    public static final Field.Required<ServerConfig> SERVER =
        Field.builder("server")
            .classField(ServerConfig::fromBson)
            .disallowUnknownFields()
            .required();

    public static final Field.Optional<MetricsConfig> METRICS =
        Field.builder("metrics")
            .classField(MetricsConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<HealthCheckConfig> HEALTH_CHECK =
        Field.builder("healthCheck")
            .classField(HealthCheckConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<LoggingConfig> LOGGING =
        Field.builder("logging")
            .classField(LoggingConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<EmbeddingConfig> EMBEDDING =
        Field.builder("embedding")
            .classField(EmbeddingConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<AdvancedConfigs> ADVANCED_CONFIGS =
        Field.builder("advancedConfigs")
            .classField(AdvancedConfigs::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<TracingConfig> TRACING =
        Field.builder("tracing")
            .classField(TracingConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<CommunityDurabilityConfig> DURABILITY =
        Field.builder("durability")
            .classField(CommunityDurabilityConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  public record ParsedCommunityConfig(
      CommunityConfig config, List<BsonParseException> unknownFieldExceptions) {}

  public static ParsedCommunityConfig readFromFile(
      Path configPath, Optional<DeploymentEnvironment> deploymentEnvironment)
      throws IOException, BsonParseException {
    return readFromFile(configPath, Map.of(), deploymentEnvironment);
  }

  /**
   * Reads the config from {@code configPath} and merges CLI-supplied values into it.
   *
   * <p>See {@link ConfigSpliceUtil#splice} for the merge and conflict semantics.
   *
   * @param configPath the path of the YAML config file.
   * @param cliValues the CLI-supplied values keyed by dot-delimited YAML path.
   * @param deploymentEnvironment Edition of binary ru
   * @throws IOException if the config file cannot be read.
   * @throws BsonParseException if a config value is set both in the config file and on the command
   *     line, or the config cannot be parsed.
   */
  public static ParsedCommunityConfig readFromFile(
      Path configPath,
      Map<String, BsonValue> cliValues,
      Optional<DeploymentEnvironment> deploymentEnvironment)
      throws IOException, BsonParseException {
    LOG.atInfo().addKeyValue("configPath", configPath).log("Reading config from file");
    String yaml = Files.readString(configPath);
    BsonDocument bson = YamlCodec.fromYaml(yaml);
    ConfigSpliceUtil.splice(bson, cliValues);
    return CommunityConfig.fromBson(bson, deploymentEnvironment);
  }

  private static ParsedCommunityConfig fromBson(
      BsonDocument document, Optional<DeploymentEnvironment> deploymentEnvironment)
      throws BsonParseException {
    // tracing.otlp and logging.otlp are parsed strictly: unlike the local-only blocks, they are
    // outbound sinks and their optional caCertificateFile is security-relevant, so a misspelled key
    // would silently fall back to the JVM trust store instead of the operator's CA. Unknown keys
    // elsewhere only warn (forward compatibility). These passes only detect unknown keys;
    // endpoint/CA
    TracingConfig.validateStrictOtlp(document);
    OtlpTracingConfig.validateStrict(document, "logging");
    // Use a permissive parse context so we can collect any unknown fields.
    PermissiveBsonParseContext context = PermissiveBsonParseContext.root(deploymentEnvironment);
    CommunityConfig config;
    try (var parser = BsonDocumentParser.withContext(context, document).build()) {
      config = CommunityConfig.fromBson(parser);
    }
    return new ParsedCommunityConfig(config, context.getUnknownFieldExceptions());
  }

  public static CommunityConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new CommunityConfig(
        parser.getField(Fields.SYNC_SOURCE).unwrap(),
        parser.getField(Fields.STORAGE).unwrap(),
        parser.getField(Fields.SERVER).unwrap(),
        parser.getField(Fields.METRICS).unwrap(),
        parser.getField(Fields.HEALTH_CHECK).unwrap(),
        parser.getField(Fields.LOGGING).unwrap(),
        parser.getField(Fields.EMBEDDING).unwrap(),
        parser.getField(Fields.ADVANCED_CONFIGS).unwrap(),
        parser.getField(Fields.TRACING).unwrap(),
        parser.getField(Fields.DURABILITY).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.SYNC_SOURCE, this.syncSourceConfig)
        .field(Fields.STORAGE, this.storageConfig)
        .field(Fields.SERVER, this.serverConfig)
        .field(Fields.METRICS, this.metricsConfig)
        .field(Fields.HEALTH_CHECK, this.healthCheckConfig)
        .field(Fields.LOGGING, this.loggingConfig)
        .field(Fields.EMBEDDING, this.embeddingConfig)
        .field(Fields.ADVANCED_CONFIGS, this.advancedConfigs)
        .field(Fields.TRACING, this.tracingConfig)
        .field(Fields.DURABILITY, this.durabilityConfig)
        .build();
  }

  /**
   * Returns the disk-monitor thresholds from {@code advancedConfigs.diskMonitor}, falling back to
   * {@link DiskMonitorConfig#getDefault()} when the block is omitted.
   */
  public DiskMonitorConfig diskMonitorConfig() {
    return this.advancedConfigs
        .flatMap(AdvancedConfigs::diskMonitorConfig)
        .orElseGet(DiskMonitorConfig::getDefault);
  }

  /**
   * Returns the FTDC config from {@code advancedConfigs.ftdc}, falling back to {@link
   * FtdcCommunityConfig#getDefault()} when the block is omitted.
   */
  public FtdcCommunityConfig ftdcConfig() {
    return this.advancedConfigs
        .flatMap(AdvancedConfigs::ftdcConfig)
        .orElseGet(FtdcCommunityConfig::getDefault);
  }
}
