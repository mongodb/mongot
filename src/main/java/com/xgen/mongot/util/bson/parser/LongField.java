package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import org.bson.BsonInt64;
import org.bson.BsonValue;

public class LongField {

  private static final Encoder ENCODER = new Encoder();
  private static final Parser PARSER = new Parser();

  public static class FieldBuilder extends NumericField.FieldBuilder<Long, FieldBuilder> {

    FieldBuilder(String name) {
      this(name, ENCODER, PARSER, java.util.Optional.empty());
    }

    private FieldBuilder(
        String name,
        ValueEncoder<Long> encoder,
        ValueParser<Long> parser,
        java.util.Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    @Override
    BuilderFactory<Long, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends NumericField.ValueBuilder<Long, LongField.ValueBuilder> {

    ValueBuilder() {
      this(ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<Long> encoder, ValueParser<Long> parser) {
      super(encoder, parser);
    }

    @Override
    BuilderFactory<Long, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private static class Encoder implements ValueEncoder<Long> {

    @Override
    public BsonValue encode(Long value) {
      return new BsonInt64(value);
    }
  }

  private static class Parser implements ValueParser<Long> {

    @Override
    public Long parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      switch (value.getBsonType()) {
        case INT32 -> {
          return value.asInt32().longValue();
        }
        case INT64 -> {
          return value.asInt64().longValue();
        }
        case DOUBLE -> {
          double doubleValue = value.asDouble().doubleValue();
          if (doubleValue > Long.MAX_VALUE) {
            context.handleOverflow(TypeDescription.INT_64);
          } else if (doubleValue < Long.MIN_VALUE) {
            context.handleUnderflow(TypeDescription.INT_64);
          }

          // values < 2^52 could contain decimal points and therefore this check is required.
          // values >= 2^52 will be integers therefore there is no need to special case them
          // and will implicitly pass this remainder check and same thing applies for the
          // -2^52 and below.
          if (doubleValue % 1 != 0) {
            context.handleSemanticError("must be a long");
          }

          return (long) doubleValue;
        }
        default -> {
          return context.handleUnexpectedType(TypeDescription.NUMBER, value.getBsonType());
        }
      }
    }
  }
}
