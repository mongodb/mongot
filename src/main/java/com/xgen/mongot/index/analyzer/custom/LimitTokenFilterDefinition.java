package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.bson.BsonDocument;

/**
 * A token filter that limits the number of tokens produced by an analyzer. This is useful for
 * preventing integer overflow errors in Lucene when using high-volume tokenizers like n-grams.
 *
 * <p>Example index definition: { "type": "limitTokenCount", "maxTokenCount": 1000 }
 */
public class LimitTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.OutputsSameTypeAsInput {

  private static final Field.Required<Integer> MAX_TOKEN_COUNT =
      Field.builder("maxTokenCount").intField().mustBePositive().required();

  private final int maxTokenCount;

  public LimitTokenFilterDefinition(int maxTokenCount) {
    if (maxTokenCount <= 0) {
      throw new IllegalArgumentException("maxTokenCount must be > 0");
    }
    this.maxTokenCount = maxTokenCount;
  }

  public int getMaxTokenCount() {
    return this.maxTokenCount;
  }

  @Override
  public Type getType() {
    return Type.LIMIT_TOKEN_COUNT;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return BsonDocumentBuilder.builder().field(MAX_TOKEN_COUNT, this.maxTokenCount).build();
  }

  public static LimitTokenFilterDefinition fromBson(DocumentParser parser)
      throws BsonParseException {

    int maxTokenCount = parser.getField(MAX_TOKEN_COUNT).unwrap();
    return new LimitTokenFilterDefinition(maxTokenCount);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof LimitTokenFilterDefinition)) {
      return false;
    }
    LimitTokenFilterDefinition that = (LimitTokenFilterDefinition) other;
    return this.maxTokenCount == that.maxTokenCount;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), this.maxTokenCount);
  }
}
