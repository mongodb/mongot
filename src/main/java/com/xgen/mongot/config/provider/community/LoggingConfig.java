package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;
import org.slf4j.event.Level;

public record LoggingConfig(String verbosity, Optional<String> logPath)
    implements DocumentEncodable {
  private static class Fields {
    public static final Field.WithDefault<String> VERBOSITY =
        Field.builder("verbosity").stringField().optional().withDefault(Level.INFO.toString());
    public static final Field.Optional<String> LOG_PATH =
        Field.builder("logPath").stringField().optional().noDefault();
  }

  public static LoggingConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new LoggingConfig(
        parser.getField(Fields.VERBOSITY).unwrap(), parser.getField(Fields.LOG_PATH).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.VERBOSITY, this.verbosity)
        .field(Fields.LOG_PATH, this.logPath)
        .build();
  }
}
