package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

public class ShingleTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.AnyToGraph {
  static class Fields {
    // MIN_SHINGLE_SIZE must be >= 2.
    static final Field.Required<Integer> MIN_SHINGLE_SIZE =
        Field.builder("minShingleSize")
            .intField()
            .mustBeWithinBounds(Range.of(2, Integer.MAX_VALUE))
            .required();

    // MAX_SHINGLE_SIZE must be >= MIN_SHINGLE_SIZE.
    static final Field.Required<Integer> MAX_SHINGLE_SIZE =
        Field.builder("maxShingleSize")
            .intField()
            .mustBeWithinBounds(Range.of(2, Integer.MAX_VALUE))
            .required();
  }

  public final int minShingleSize;
  public final int maxShingleSize;

  public ShingleTokenFilterDefinition(int minShingleSize, int maxShingleSize) {
    this.minShingleSize = minShingleSize;
    this.maxShingleSize = maxShingleSize;
  }

  public static ShingleTokenFilterDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    int minShingleSize = parser.getField(Fields.MIN_SHINGLE_SIZE).unwrap();
    int maxShingleSize = parser.getField(Fields.MAX_SHINGLE_SIZE).unwrap();

    if (minShingleSize > maxShingleSize) {
      parser
          .getContext()
          .handleSemanticError("minShingleSize must not be greater than maxShingleSize");
    }

    return new ShingleTokenFilterDefinition(minShingleSize, maxShingleSize);
  }

  @Override
  public Type getType() {
    return Type.SHINGLE;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MIN_SHINGLE_SIZE, this.minShingleSize)
        .field(Fields.MAX_SHINGLE_SIZE, this.maxShingleSize)
        .build();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof ShingleTokenFilterDefinition otherDefinition)) {
      return false;
    }
    return (this.minShingleSize == otherDefinition.minShingleSize)
        && (this.maxShingleSize == otherDefinition.maxShingleSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.minShingleSize, this.maxShingleSize);
  }
}
