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

public record TlsConfig(
    boolean enabled,
    Optional<Path> tlsCertificateKeyFile,
    Optional<Path> tlsCertificateKeyFilePasswordFile,
    Optional<Path> caFile)
    implements DocumentEncodable {

  public static TlsConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new TlsConfig(
        parser.getField(Fields.ENABLED).unwrap(),
        parser.getField(Fields.TLS_CERTIFICATE_KEY_FILE).unwrap(),
        parser.getField(Fields.TLS_CERTIFICATE_KEY_FILE_PASSWORD_FILE).unwrap(),
        parser.getField(Fields.TLS_CA_FILE).unwrap());
  }

  public void validate(DocumentParser parser) throws BsonParseException {
    // Note: A CA file is not required when TLS is enabled. If a CA file is not provided the driver
    // itself will validate against the default JVM trust
    // store - https://www.mongodb.com/docs/drivers/java/sync/current/security/tls/
    if (this.tlsCertificateKeyFile.isPresent() && !this.enabled) {
      parser
          .getContext()
          .handleSemanticError("tls must be enabled when tlsCertificateKeyFile is configured");
    } else if (this.caFile.isPresent() && !this.enabled) {
      parser.getContext().handleSemanticError("tls must be enabled when caFile is configured");
    }

    if (this.tlsCertificateKeyFilePasswordFile().isPresent()
        && this.tlsCertificateKeyFile().isEmpty()) {
      parser
          .getContext()
          .handleSemanticError(
              "tlsCertificateKeyFile is required when tlsCertificateKeyFilePasswordFile "
                  + "is provided");
    }
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ENABLED, this.enabled)
        .field(Fields.TLS_CERTIFICATE_KEY_FILE, this.tlsCertificateKeyFile)
        .field(
            Fields.TLS_CERTIFICATE_KEY_FILE_PASSWORD_FILE, this.tlsCertificateKeyFilePasswordFile)
        .field(Fields.TLS_CA_FILE, this.caFile)
        .build();
  }

  public static class Fields {
    public static final Field.WithDefault<Boolean> ENABLED =
        Field.builder("enabled")
            .booleanField()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault(false);

    public static final Field.Optional<Path> TLS_CERTIFICATE_KEY_FILE =
        Field.builder("tlsCertificateKeyFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();

    public static final Field.Optional<Path> TLS_CERTIFICATE_KEY_FILE_PASSWORD_FILE =
        Field.builder("tlsCertificateKeyFilePasswordFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();

    public static final Field.Optional<Path> TLS_CA_FILE =
        Field.builder("caFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();
  }

  /** The {@link CliOptionRegistry} for the tls block within syncSource.replicaSet. */
  public static final class ReplicaSetTlsConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Option(
        names = "--replicaSetScramAuthTlsEnabled",
        description = "Flag to enable TLS authentication. Set to true to enable TLS authentication")
    public Optional<Boolean> enabled = Optional.empty();

    @CommandLine.Option(
        names = "--replicaSetScramAuthTlsCertificateKeyFile",
        description =
            "Path to the PEM file that contains the X.509 certificate and private key for "
                + "authenticating mongot with mongod.")
    public Optional<Path> tlsCertificateKeyFile = Optional.empty();

    @CommandLine.Option(
        names = "--replicaSetScramAuthTlsCertificateKeyFilePasswordFile",
        description =
            "Path to the file that contains the password to decrypt the certificate key file "
                + "specified in replicaSetScramAuthTlsCertificateKeyFile.")
    public Optional<Path> tlsCertificateKeyFilePasswordFile = Optional.empty();

    @CommandLine.Option(
        names = "--replicaSetScramAuthTlsCaFile",
        description =
            "Certificate Authority (CA) file that contains trusted certificates for verifying the "
                + "certificate presented by mongod.")
    public Optional<Path> caFile = Optional.empty();

    public ReplicaSetTlsConfigCliOptionRegistry() {
      addBooleanLeafValue(Fields.ENABLED, () -> this.enabled);
      addPathLeafValue(Fields.TLS_CERTIFICATE_KEY_FILE, () -> this.tlsCertificateKeyFile);
      addPathLeafValue(
          Fields.TLS_CERTIFICATE_KEY_FILE_PASSWORD_FILE,
          () -> this.tlsCertificateKeyFilePasswordFile);
      addPathLeafValue(Fields.TLS_CA_FILE, () -> this.caFile);
    }
  }

  /** The {@link CliOptionRegistry} for the tls block within syncSource.router. */
  public static final class RouterTlsConfigCliOptionRegistry extends CliOptionRegistry {

    @CommandLine.Option(
        names = "--routerScramAuthTlsEnabled",
        description = "Flag to enable TLS authentication. Set to true to enable TLS authentication")
    public Optional<Boolean> enabled = Optional.empty();

    @CommandLine.Option(
        names = "--routerScramAuthTlsCertificateKeyFile",
        description =
            "Path to the PEM file that contains the X.509 certificate and private key for "
                + "authenticating mongot with mongos")
    public Optional<Path> tlsCertificateKeyFile = Optional.empty();

    @CommandLine.Option(
        names = "--routerScramAuthTlsCertificateKeyFilePasswordFile",
        description =
            "Path to the file that contains the password to decrypt the certificate key file "
                + "specified in routerScramAuthTlsCertificateKeyFile")
    public Optional<Path> tlsCertificateKeyFilePasswordFile = Optional.empty();

    @CommandLine.Option(
        names = "--routerScramAuthTlsCaFile",
        description =
            "Certificate Authority (CA) file that contains trusted certificates for verifying "
                + "the certificate presented by mongos")
    public Optional<Path> caFile = Optional.empty();

    public RouterTlsConfigCliOptionRegistry() {
      addBooleanLeafValue(Fields.ENABLED, () -> this.enabled);
      addPathLeafValue(Fields.TLS_CERTIFICATE_KEY_FILE, () -> this.tlsCertificateKeyFile);
      addPathLeafValue(
          Fields.TLS_CERTIFICATE_KEY_FILE_PASSWORD_FILE,
          () -> this.tlsCertificateKeyFilePasswordFile);
      addPathLeafValue(Fields.TLS_CA_FILE, () -> this.caFile);
    }
  }
}
