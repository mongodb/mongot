package com.xgen.mongot.config.util;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record HysteresisConfig(double openThreshold, double closeThreshold)
    implements DocumentEncodable {
  public static class Fields {
    public static final Field.Required<Double> OPEN_THRESHOLD =
        Field.builder("openThreshold").doubleField().required();
    public static final Field.Required<Double> CLOSE_THRESHOLD =
        Field.builder("closeThreshold").doubleField().required();
  }

  public static HysteresisConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new HysteresisConfig(
        parser.getField(Fields.OPEN_THRESHOLD).unwrap(),
        parser.getField(Fields.CLOSE_THRESHOLD).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.OPEN_THRESHOLD, this.openThreshold)
        .field(Fields.CLOSE_THRESHOLD, this.closeThreshold)
        .build();
  }
}
