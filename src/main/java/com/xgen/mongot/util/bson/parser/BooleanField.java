package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonType;
import org.bson.BsonValue;

/** Contains the TypeBuilders and ValueParser for built in Boolean parsing. */
public class BooleanField {

  private static final Encoder ENCODER = new Encoder();
  private static final Parser PARSER = new Parser();

  public static class FieldBuilder extends Field.TypedBuilder<Boolean, FieldBuilder> {

    FieldBuilder(String name) {
      this(name, ENCODER, PARSER, Optional.empty());
    }

    private FieldBuilder(
        String name,
        ValueEncoder<Boolean> encoder,
        ValueParser<Boolean> parser,
        Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    @Override
    BuilderFactory<Boolean, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends Value.TypedBuilder<Boolean, ValueBuilder> {

    ValueBuilder() {
      this(ENCODER, PARSER);
    }

    ValueBuilder(ValueEncoder<Boolean> encoder, ValueParser<Boolean> parser) {
      super(encoder, parser);
    }

    @Override
    BuilderFactory<Boolean, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private static class Encoder implements ValueEncoder<Boolean> {

    @Override
    public BsonValue encode(Boolean value) {
      return new BsonBoolean(value);
    }
  }

  private static class Parser implements ValueParser<Boolean> {

    @Override
    public Boolean parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      if (value.getBsonType() != BsonType.BOOLEAN) {
        context.handleUnexpectedType(TypeDescription.BOOLEAN, value.getBsonType());
      }

      return value.asBoolean().getValue();
    }
  }
}
