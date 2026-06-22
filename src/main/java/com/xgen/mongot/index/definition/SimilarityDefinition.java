package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

/** Represents a similarity model configuration for a search field. */
public record SimilarityDefinition(SimilarityType type) implements DocumentEncodable {

  public static final SimilarityDefinition BOOLEAN =
      new SimilarityDefinition(SimilarityType.BOOLEAN);

  public static final SimilarityDefinition BM25 = new SimilarityDefinition(SimilarityType.BM25);

  public static final SimilarityDefinition STABLE_TFL =
      new SimilarityDefinition(SimilarityType.STABLE_TFL);

  static class Fields {
    static final Field.Required<SimilarityType> TYPE =
        Field.builder("type").enumField(SimilarityType.class).asCamelCase().required();
  }

  public enum SimilarityType {
    /** Lucene Default -- Best for general purpose searching, but scores depend on corpus stats. */
    BM25,
    /** Custom metric that attempts to approximate BM25 without corpus stats. */
    STABLE_TFL,
    /**
     * Lucene metric that counts the number of query terms with at least one match in the document.
     */
    BOOLEAN
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.TYPE, this.type).build();
  }

  public static SimilarityDefinition fromBson(DocumentParser parser) throws BsonParseException {
    SimilarityType type = parser.getField(Fields.TYPE).unwrap();
    return new SimilarityDefinition(type);
  }
}
