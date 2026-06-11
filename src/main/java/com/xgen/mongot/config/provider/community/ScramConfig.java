package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.mongodb.Databases;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;

public record ScramConfig(String authSource, String username, Path passwordFile, TlsConfig tls)
    implements DocumentEncodable {

  private static class Fields {
    public static final Field.WithDefault<String> AUTH_SOURCE =
        Field.builder("authSource")
            .stringField()
            .mustNotBeEmpty()
            .optional()
            .withDefault(Databases.ADMIN);

    public static final Field.Required<String> USERNAME =
        Field.builder("username").stringField().mustNotBeEmpty().required();

    public static final Field.Required<Path> PASSWORD_FILE =
        Field.builder("passwordFile").classField(PathField.PARSER, PathField.ENCODER).required();

    public static final Field.WithDefault<TlsConfig> TLS =
        Field.builder("tls")
            .classField(TlsConfig::fromBson, TlsConfig::toBson)
            .disallowUnknownFields()
            .optional()
            .withDefault(
                new TlsConfig(false, Optional.empty(), Optional.empty(), Optional.empty()));
  }

  public static ScramConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new ScramConfig(
        parser.getField(Fields.AUTH_SOURCE).unwrap(),
        parser.getField(Fields.USERNAME).unwrap(),
        parser.getField(Fields.PASSWORD_FILE).unwrap(),
        parser.getField(Fields.TLS).unwrap());
  }

  /**
   * Validates this SCRAM config, enforcing constraints on TLS fields and inherited CA file usage.
   */
  public void validate(DocumentParser parser, Optional<Path> parentCaFile)
      throws BsonParseException {
    // TODO(CLOUDP-395903): Remove this check after parent caFile deprecated
    if (parentCaFile.isPresent()) {
      parser
          .getContext()
          .handleSemanticError(
              "CA file must be defined within SCRAM's TLS config."
                  + " CA file not supported within sync source definition.");
    }

    // caFile is intentionally not required when tlsCertificateKeyFile is set: SCRAM authenticates
    // via password, so when no caFile is provided the driver falls back to the JVM default trust
    // store for server certificate verification.

    this.tls.validate(parser);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.AUTH_SOURCE, this.authSource)
        .field(Fields.USERNAME, this.username)
        .field(Fields.PASSWORD_FILE, this.passwordFile)
        .field(Fields.TLS, this.tls)
        .build();
  }
}
