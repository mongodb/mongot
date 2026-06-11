package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;

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
        Field.builder("enabled").booleanField().optional().withDefault(false);

    public static final Field.Optional<Path> TLS_CERTIFICATE_KEY_FILE =
        Field.builder("tlsCertificateKeyFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .optional()
            .noDefault();

    public static final Field.Optional<Path> TLS_CERTIFICATE_KEY_FILE_PASSWORD_FILE =
        Field.builder("tlsCertificateKeyFilePasswordFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .optional()
            .noDefault();

    public static final Field.Optional<Path> TLS_CA_FILE =
        Field.builder("caFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .optional()
            .noDefault();
  }
}
