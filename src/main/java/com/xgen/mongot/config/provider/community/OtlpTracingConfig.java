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

/**
 * The optional, startup-only, additive {@code tracing.otlp} block: an OTLP/gRPC collector sink
 * alongside the log exporter. The endpoint is mandatory and validated; {@code caCertificateFile} is
 * optional and validated while the config is read, so errors fail startup even when tracing is
 * disabled.
 */
public record OtlpTracingConfig(String endpoint, Optional<Path> caCertificateFile)
    implements DocumentEncodable {

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

  static class Fields {
    static final Field.Required<String> ENDPOINT =
        Field.builder("endpoint")
            .stringField()
            .validate(OtlpExporterFactory::validateEndpoint)
            .required();

    static final Field.Optional<Path> CA_CERTIFICATE_FILE =
        Field.builder("caCertificateFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .validate(OtlpExporterFactory::validateCaCertificateFile)
            .optional()
            .noDefault();
  }
}
