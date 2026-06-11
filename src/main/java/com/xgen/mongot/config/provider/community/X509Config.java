package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;
import org.bson.BsonDocument;

public record X509Config(TlsConfig tlsConfig) implements DocumentEncodable {

  /**
   * Validates legal member combinations when using x509 authentication mechanisms
   *
   * @param parser the {@link DocumentParser} to read from.
   * @param caFile - legacy reference to parent CA member in sync source
   * @throws BsonParseException - Throws in cases of malformed configurations
   */
  public void validate(DocumentParser parser, Optional<Path> caFile) throws BsonParseException {
    // x509 uses a private PKI, so caFile is required — the private CA that signs client certs is
    // not in the JVM default trust store. Exactly one source is allowed (inline or legacy parent)
    // to avoid ambiguity.
    long presentCount =
        Stream.of(caFile, this.tlsConfig.caFile()).filter(Optional::isPresent).count();
    if (presentCount != 1) {
      parser
          .getContext()
          .handleSemanticError(
              "caFile must be set either within x509 config or parent sync source.");
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
