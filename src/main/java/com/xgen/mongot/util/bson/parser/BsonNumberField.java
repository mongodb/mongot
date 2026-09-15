package com.xgen.mongot.util.bson.parser;

import org.bson.BsonNumber;
import org.bson.BsonType;

public class BsonNumberField {

  private static final ValueEncoder<BsonNumber> ENCODER = x -> x;
  private static final ValueParser<BsonNumber> PARSER =
      (context, val) -> {
        if (!val.isNumber() || val.getBsonType() == BsonType.DECIMAL128) {
          context.handleUnexpectedType(TypeDescription.NUMBER_NOT_DECIMAL, val.getBsonType());
        }
        return val.asNumber();
      };

  public static class FieldBuilder extends Field.TypedBuilder<BsonNumber, FieldBuilder> {

    FieldBuilder(String name) {
      this(name, ENCODER, PARSER);
    }

    private FieldBuilder(
        String name, ValueEncoder<BsonNumber> encoder, ValueParser<BsonNumber> parser) {
      super(name, encoder, parser);
    }

    @Override
    BuilderFactory<BsonNumber, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends Value.TypedBuilder<BsonNumber, ValueBuilder> {

    ValueBuilder() {
      this(ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<BsonNumber> encoder, ValueParser<BsonNumber> parser) {
      super(encoder, parser);
    }

    @Override
    BuilderFactory<BsonNumber, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }
}
