package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;
import picocli.CommandLine;

public record X509Config(TlsConfig tlsConfig) implements DocumentEncodable {
  /** The {@link CliOptionRegistry} for the x509 block within syncSource.replicaSet. */
  public static final class ReplicaSetX509ConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Option(
        names = "--replicaSetX509TlsCertificateKeyFile",
        description =
            "Path to the PEM file containing the X.509 certificate and private key for "
                + "authenticating mongot with mongod")
    public Optional<Path> tlsCertificateKeyFile = Optional.empty();

    @CommandLine.Option(
        names = "--replicaSetX509TlsCertificateKeyFilePasswordFile",
        description =
            "Path to the file that contains the password to decrypt the certificate key file "
                + "specified in replicaSetX509TlsCertificateKeyFile")
    public Optional<Path> tlsCertificateKeyFilePasswordFile = Optional.empty();

    @CommandLine.Option(
        names = "--replicaSetX509CaFile",
        description =
            "Certificate Authority (CA) file that contains trusted X.509 certificates for "
                + "verifying the certificate presented by mongod.")
    public Optional<Path> caFile = Optional.empty();

    public ReplicaSetX509ConfigCliOptionRegistry() {
      addPathLeafValue(TlsConfig.Fields.TLS_CERTIFICATE_KEY_FILE, () -> this.tlsCertificateKeyFile);
      addPathLeafValue(
          TlsConfig.Fields.TLS_CERTIFICATE_KEY_FILE_PASSWORD_FILE,
          () -> this.tlsCertificateKeyFilePasswordFile);
      addPathLeafValue(TlsConfig.Fields.TLS_CA_FILE, () -> this.caFile);
    }
  }

  /** The {@link CliOptionRegistry} for the x509 block within syncSource.router. */
  public static final class RouterX509ConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Option(
        names = "--routerX509TlsCertificateKeyFile",
        description =
            "Path to the PEM file that contains the X.509 certificate and private key "
                + "for authenticating mongot with mongos")
    public Optional<Path> tlsCertificateKeyFile = Optional.empty();

    @CommandLine.Option(
        names = "--routerX509TlsCertificateKeyFilePasswordFile",
        description =
            "Path to the file that contains the password to decrypt the certificate key file"
                + " specified in routerX509TlsCertificateKeyFile")
    public Optional<Path> tlsCertificateKeyFilePasswordFile = Optional.empty();

    @CommandLine.Option(
        names = "--routerX509CaFile",
        description =
            "Certificate Authority (CA) file that contains trusted X509 certificates for "
                + "verifying the certificate presented by mongos")
    public Optional<Path> caFile = Optional.empty();

    public RouterX509ConfigCliOptionRegistry() {
      addPathLeafValue(TlsConfig.Fields.TLS_CERTIFICATE_KEY_FILE, () -> this.tlsCertificateKeyFile);
      addPathLeafValue(
          TlsConfig.Fields.TLS_CERTIFICATE_KEY_FILE_PASSWORD_FILE,
          () -> this.tlsCertificateKeyFilePasswordFile);
      addPathLeafValue(TlsConfig.Fields.TLS_CA_FILE, () -> this.caFile);
    }
  }

  /**
   * Validates legal member combinations when using x509 authentication mechanisms
   *
   * @param parser the {@link DocumentParser} to read from.
   * @throws BsonParseException - Throws in cases of malformed configurations
   */
  public void validate(DocumentParser parser) throws BsonParseException {
    // x509 uses a private PKI, so caFile is required — the private CA that signs client certs is
    // not in the JVM default trust store.
    if (this.tlsConfig.caFile().isEmpty()) {
      parser.getContext().handleSemanticError("caFile must be set within x509 config");
    }

    // x509 authentication is certificate-based, so a client certificate is always required.
    if (this.tlsConfig.tlsCertificateKeyFile().isEmpty()) {
      parser.getContext().handleSemanticError("tlsCertificateKeyFile is required using x509 auth");
    }

    this.tlsConfig.validate(parser);
  }

  public static X509Config fromBson(DocumentParser parser) throws BsonParseException {
    TlsConfig parsedTlsConfig =
        new TlsConfig(
            true,
            parser.getField(TlsConfig.Fields.TLS_CERTIFICATE_KEY_FILE).unwrap(),
            parser.getField(TlsConfig.Fields.TLS_CERTIFICATE_KEY_FILE_PASSWORD_FILE).unwrap(),
            parser.getField(TlsConfig.Fields.TLS_CA_FILE).unwrap());

    return new X509Config(parsedTlsConfig);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(TlsConfig.Fields.TLS_CERTIFICATE_KEY_FILE, this.tlsConfig.tlsCertificateKeyFile())
        .field(
            TlsConfig.Fields.TLS_CERTIFICATE_KEY_FILE_PASSWORD_FILE,
            this.tlsConfig.tlsCertificateKeyFilePasswordFile())
        .field(TlsConfig.Fields.TLS_CA_FILE, this.tlsConfig.caFile())
        .build();
  }
}
