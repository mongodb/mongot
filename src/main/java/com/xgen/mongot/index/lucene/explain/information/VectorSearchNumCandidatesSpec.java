package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonValue;

public record VectorSearchNumCandidatesSpec(Mode mode, int queryLevelValue) implements Encodable {

  public enum Mode {
    AUTO,
    EXPLICIT
  }

  static class Fields {
    static final Field.Required<Mode> MODE =
        Field.builder("mode").enumField(Mode.class).asCamelCase().required();
    static final Field.Required<Integer> QUERY_LEVEL_VALUE =
        Field.builder("queryLevelValue").intField().required();
  }

  public static VectorSearchNumCandidatesSpec fromBson(DocumentParser parser)
      throws BsonParseException {
    return new VectorSearchNumCandidatesSpec(
        parser.getField(Fields.MODE).unwrap(), parser.getField(Fields.QUERY_LEVEL_VALUE).unwrap());
  }

  @Override
  public BsonValue toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MODE, this.mode)
        .field(Fields.QUERY_LEVEL_VALUE, this.queryLevelValue)
        .build();
  }
}
