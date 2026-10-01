package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * The optional, startup-only, additive {@code <parent>.otlp} block shared by the {@code tracing}
 * and {@code logging} sections: an OTLP/gRPC collector sink alongside the existing sinks. The
 * endpoint is mandatory and validated; {@code caCertificateFile} is optional and validated while
 * the config is read, so errors fail startup even when the sink is disabled.
 */
public record OtlpTracingConfig(String endpoint, Optional<Path> caCertificateFile)
    implements DocumentEncodable {

  static class Fields {
    static final Field.Required<String> ENDPOINT =
        Field.builder("endpoint")
            .stringField()
            .validate(OtlpExporterFactory::validateEndpoint)
            .edition(DeploymentEnvironment.COMMUNITY)
            .required();

    static final Field.Optional<Path> CA_CERTIFICATE_FILE =
        Field.builder("caCertificateFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .validate(OtlpExporterFactory::validateCaCertificateFile)
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();
  }

  public static OtlpTracingConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new OtlpTracingConfig(
        parser.getField(Fields.ENDPOINT).unwrap(),
        parser.getField(Fields.CA_CERTIFICATE_FILE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ENDPOINT, this.endpoint)
        .field(Fields.CA_CERTIFICATE_FILE, this.caCertificateFile)
        .build();
  }

  /**
   * Fails startup when {@code <parentKey>.otlp} contains an unknown field. Unlike the local-only
   * blocks this is an outbound sink whose optional {@code caCertificateFile} is security-relevant,
   * so a misspelled key must not silently fall back to the JVM trust store. Witness-only: endpoint
   * and CA values are validated once by the permissive parse and are not re-checked here.
   */
  public static void validateStrict(BsonDocument root, String parentKey) throws BsonParseException {
    BsonValue parent = root.get(parentKey);
    if (parent == null || !parent.isDocument()) {
      return;
    }
    BsonValue otlp = parent.asDocument().get("otlp");
    if (otlp == null || !otlp.isDocument()) {
      return;
    }
    try (BsonDocumentParser strict = BsonDocumentParser.fromRoot(otlp.asDocument()).build()) {
      strict.getField(Field.builder("endpoint").unparsedValueField().required());
      strict.getField(
          Field.builder("caCertificateFile").unparsedValueField().optional().noDefault());
    }
  }
}
