package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

public record LuceneMetadataExplainInformation(
    Optional<Integer> totalSegments, Optional<Integer> totalDocs) implements DocumentEncodable {
  static class Fields {
    static final Field.Optional<Integer> TOTAL_SEGMENTS =
        Field.builder("totalSegments").intField().optional().noDefault();

    static final Field.Optional<Integer> TOTAL_DOCS =
        Field.builder("totalDocs").intField().optional().noDefault();
  }

  public static LuceneMetadataExplainInformation fromBson(DocumentParser parser)
      throws BsonParseException {
    return new LuceneMetadataExplainInformation(
        parser.getField(Fields.TOTAL_SEGMENTS).unwrap(),
        parser.getField(Fields.TOTAL_DOCS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.TOTAL_SEGMENTS, this.totalSegments)
        .field(Fields.TOTAL_DOCS, this.totalDocs)
        .build();
  }
}
