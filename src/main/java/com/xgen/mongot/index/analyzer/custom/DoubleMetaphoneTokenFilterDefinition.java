package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

public class DoubleMetaphoneTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.AnyToGraph {

  static class Fields {
    static final Field.WithDefault<OriginalTokens> ORIGINAL_TOKENS =
        Field.builder("originalTokens")
            .enumField(OriginalTokens.class)
            .asCamelCase()
            .optional()
            .withDefault(OriginalTokens.INCLUDE);
    static final int MAX_CODE_LENGTH_LIMIT = 8;
    static final Field.Optional<Integer> MAX_CODE_LENGTH =
        Field.builder("maxCodeLength")
            .intField()
            .mustBeWithinBounds(Range.of(1, MAX_CODE_LENGTH_LIMIT))
            .optional()
            .noDefault();
  }

  public final OriginalTokens originalTokens;
  public final Optional<Integer> maxCodeLength;

  public DoubleMetaphoneTokenFilterDefinition(
      OriginalTokens originalTokens, Optional<Integer> maxCodeLength) {
    this.originalTokens = originalTokens;
    this.maxCodeLength = maxCodeLength;
  }

  public static DoubleMetaphoneTokenFilterDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new DoubleMetaphoneTokenFilterDefinition(
        parser.getField(Fields.ORIGINAL_TOKENS).unwrap(),
        parser.getField(Fields.MAX_CODE_LENGTH).unwrap());
  }

  @Override
  public Type getType() {
    return Type.DOUBLE_METAPHONE;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ORIGINAL_TOKENS, this.originalTokens)
        .field(Fields.MAX_CODE_LENGTH, this.maxCodeLength)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DoubleMetaphoneTokenFilterDefinition that = (DoubleMetaphoneTokenFilterDefinition) o;
    return this.originalTokens == that.originalTokens
        && Objects.equals(this.maxCodeLength, that.maxCodeLength);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.originalTokens, this.maxCodeLength);
  }
}
