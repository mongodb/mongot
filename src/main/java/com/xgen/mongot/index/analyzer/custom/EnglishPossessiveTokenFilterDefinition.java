package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import java.util.Objects;
import org.bson.BsonDocument;

public class EnglishPossessiveTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.OutputsSameTypeAsInput {

  @Override
  public Type getType() {
    return Type.ENGLISH_POSSESSIVE;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return new BsonDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof EnglishPossessiveTokenFilterDefinition;
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }
}
