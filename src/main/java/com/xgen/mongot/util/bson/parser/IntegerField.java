package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonValue;

/** Contains the TypeBuilders and ValueParser for built in Integer parsing. */
public class IntegerField {

  private static final Parser PARSER = new Parser();
  private static final Encoder DOUBLE_ENCODER = new Encoder(Encoding.DOUBLE);
  private static final Encoder INT_32_ENCODER = new Encoder(Encoding.INT_32);

  private enum Encoding {
    DOUBLE,
    INT_32,
  }

  public static class FieldBuilder extends NumericField.FieldBuilder<Integer, FieldBuilder> {

    FieldBuilder(String name) {
      this(name, INT_32_ENCODER, PARSER, java.util.Optional.empty());
    }

    private FieldBuilder(
        String name,
        ValueEncoder<Integer> encoder,
        ValueParser<Integer> parser,
        java.util.Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    public FieldBuilder encodeAsDouble() {
      return new FieldBuilder(this.name, DOUBLE_ENCODER, this.parser, this.edition);
    }

    @Override
    BuilderFactory<Integer, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends NumericField.ValueBuilder<Integer, ValueBuilder> {

    ValueBuilder() {
      this(INT_32_ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<Integer> encoder, ValueParser<Integer> parser) {
      super(encoder, parser);
    }

    ValueBuilder encodeAsDouble() {
      return new ValueBuilder(DOUBLE_ENCODER, this.parser);
    }

    @Override
    BuilderFactory<Integer, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private static class Encoder implements ValueEncoder<Integer> {

    private final Encoding encoding;

    Encoder(Encoding encoding) {
      this.encoding = encoding;
    }

    @Override
    public BsonValue encode(Integer value) {
      return switch (this.encoding) {
        case DOUBLE -> new BsonDouble(value);
        case INT_32 -> new BsonInt32(value);
      };
    }
  }

  static class Parser implements ValueParser<Integer> {

    @Override
    public Integer parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      switch (value.getBsonType()) {
        case INT32 -> {
          return value.asInt32().getValue();
        }
        case INT64 -> {
          long longValue = value.asInt64().longValue();
          if (longValue > Integer.MAX_VALUE) {
            context.handleOverflow(TypeDescription.INT_32);
          } else if (longValue < Integer.MIN_VALUE) {
            context.handleUnderflow(TypeDescription.INT_32);
          }

          return (int) longValue;
        }
        case DOUBLE -> {
          double doubleValue = value.asDouble().doubleValue();
          if (doubleValue > Integer.MAX_VALUE) {
            context.handleOverflow(TypeDescription.INT_32);
          } else if (doubleValue < Integer.MIN_VALUE) {
            context.handleUnderflow(TypeDescription.INT_32);
          }

          // Integers are representable to 2^53 (which is > 2^31), so we can simply check for a
          // remainder without worrying about precision.
          if (doubleValue % 1 != 0) {
            context.handleSemanticError("must be an integer");
          }

          return (int) doubleValue;
        }
        default -> {
          return context.handleUnexpectedType(TypeDescription.INTEGER, value.getBsonType());
        }
      }
    }
  }
}
