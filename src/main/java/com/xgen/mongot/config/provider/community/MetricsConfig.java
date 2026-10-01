package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;
import picocli.CommandLine;

public record MetricsConfig(boolean enabled, String address, Optional<MetricsConfig.Tls> tls)
    implements DocumentEncodable {
  private static class Fields {
    public static final Field.Required<Boolean> ENABLED =
        Field.builder("enabled").booleanField().edition(DeploymentEnvironment.COMMUNITY).required();
    public static final Field.WithDefault<String> ADDRESS =
        Field.builder("address")
            .stringField()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault("localhost:9946");
    public static final Field.Optional<MetricsConfig.Tls> TLS =
        Field.builder("tls")
            .classField(MetricsConfig.Tls::fromBson)
            .disallowUnknownFields()
            .optional()
            .edition(DeploymentEnvironment.COMMUNITY)
            .noDefault();
  }

  public static final class MetricsConfigCliOptionRegistry extends CliOptionRegistry {

    @CommandLine.Option(
        names = "--enableMetrics",
        description = "Enables the Prometheus metrics endpoint")
    public Optional<Boolean> enabled = Optional.empty();

    @CommandLine.Option(
        names = "--metricsAddress",
        description = "Address on which the Prometheus /metrics endpoint is exposed")
    public Optional<String> address = Optional.empty();

    @CommandLine.Mixin
    public MetricsConfig.Tls.TlsCliOptionRegistry tlsCliOptionRegistry =
        new MetricsConfig.Tls.TlsCliOptionRegistry();

    public MetricsConfigCliOptionRegistry() {
      addChildRegistry(Fields.TLS, this.tlsCliOptionRegistry);
      addBooleanLeafValue(Fields.ENABLED, () -> this.enabled);
      addStringLeafValue(Fields.ADDRESS, () -> this.address);
    }
  }

  public static MetricsConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new MetricsConfig(
        parser.getField(Fields.ENABLED).unwrap(),
        parser.getField(Fields.ADDRESS).unwrap(),
        parser.getField(Fields.TLS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ENABLED, this.enabled)
        .field(Fields.ADDRESS, this.address)
        .field(Fields.TLS, this.tls)
        .build();
  }

  /** Opt-in mTLS settings for the Prometheus {@code /metrics} endpoint. */
  public record Tls(
      Path certificateKeyFile, Optional<Path> certificateKeyFilePasswordFile, Path caFile)
      implements DocumentEncodable {
    private static class Fields {
      public static final Field.Required<Path> CERTIFICATE_KEY_FILE =
          Field.builder("certificateKeyFile")
              .classField(PathField.PARSER, PathField.ENCODER)
              .edition(DeploymentEnvironment.COMMUNITY)
              .required();
      public static final Field.Optional<Path> CERTIFICATE_KEY_FILE_PASSWORD_FILE =
          Field.builder("certificateKeyFilePasswordFile")
              .classField(PathField.PARSER, PathField.ENCODER)
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();
      public static final Field.Required<Path> CA_FILE =
          Field.builder("caFile")
              .classField(PathField.PARSER, PathField.ENCODER)
              .edition(DeploymentEnvironment.COMMUNITY)
              .required();
    }

    public static final class TlsCliOptionRegistry extends CliOptionRegistry {

      @CommandLine.Option(
          names = "--metricsTlsCertificateKeyFile",
          description =
              "Combined PEM with the metrics server private key, leaf certificate, and any "
                  + "intermediate certificates, used when serving /metrics over TLS")
      public Optional<Path> certificateKeyFile = Optional.empty();

      @CommandLine.Option(
          names = "--metricsTlsCertificateKeyFilePasswordFile",
          description =
              "Path to the file with the password to decrypt the metrics TLS private key, when it "
                  + "is encrypted")
      public Optional<Path> certificateKeyFilePasswordFile = Optional.empty();

      @CommandLine.Option(
          names = "--metricsTlsCaFile",
          description =
              "PEM CA bundle used to verify client certificates presented to /metrics over TLS")
      public Optional<Path> caFile = Optional.empty();

      public TlsCliOptionRegistry() {
        addPathLeafValue(Fields.CERTIFICATE_KEY_FILE, () -> this.certificateKeyFile);
        addPathLeafValue(
            Fields.CERTIFICATE_KEY_FILE_PASSWORD_FILE, () -> this.certificateKeyFilePasswordFile);
        addPathLeafValue(Fields.CA_FILE, () -> this.caFile);
      }
    }

    public static Tls fromBson(DocumentParser parser) throws BsonParseException {
      Path certificateKeyFile = parser.getField(Fields.CERTIFICATE_KEY_FILE).unwrap();
      Path caFile = parser.getField(Fields.CA_FILE).unwrap();
      Optional<Path> certificateKeyFilePasswordFile =
          parser.getField(Fields.CERTIFICATE_KEY_FILE_PASSWORD_FILE).unwrap();
      if (certificateKeyFile.toString().isBlank()) {
        parser.getContext().handleSemanticError("certificateKeyFile must not be empty");
      }
      if (caFile.toString().isBlank()) {
        parser.getContext().handleSemanticError("caFile must not be empty");
      }
      return new Tls(certificateKeyFile, certificateKeyFilePasswordFile, caFile);
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.CERTIFICATE_KEY_FILE, this.certificateKeyFile)
          .field(Fields.CERTIFICATE_KEY_FILE_PASSWORD_FILE, this.certificateKeyFilePasswordFile)
          .field(Fields.CA_FILE, this.caFile)
          .build();
    }
  }
}
