package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.Optional;
import org.bson.BsonDouble;
import org.bson.BsonValue;

/** Contains the TypeBuilders and ValueParser for built in Float parsing. */
public class FloatField {

  private static final Encoder ENCODER = new Encoder();
  private static final Parser PARSER = new Parser();

  public static class FieldBuilder extends NumericField.FieldBuilder<Float, FieldBuilder> {

    FieldBuilder(String name) {
      this(name, ENCODER, PARSER, Optional.empty());
    }

    private FieldBuilder(
        String name,
        ValueEncoder<Float> encoder,
        ValueParser<Float> parser,
        Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    public FieldBuilder mustBeFinite() {
      ValueParser<Float> parser = FloatField.mustBeFinite(this.parser);
      return new FieldBuilder(this.name, this.encoder, parser, this.edition);
    }

    @Override
    BuilderFactory<Float, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends NumericField.ValueBuilder<Float, ValueBuilder> {

    ValueBuilder() {
      super(ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<Float> encoder, ValueParser<Float> parser) {
      super(encoder, parser);
    }

    public ValueBuilder mustBeFinite() {
      ValueParser<Float> parser = FloatField.mustBeFinite(this.parser);
      return new ValueBuilder(this.encoder, parser);
    }

    @Override
    BuilderFactory<Float, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private static ValueParser<Float> mustBeFinite(ValueParser<Float> parser) {
    return new ValidatingValueParser<>(
        parser, f -> Float.isFinite(f) ? Optional.empty() : Optional.of("must be finite"));
  }

  private static class Encoder implements ValueEncoder<Float> {

    @Override
    public BsonValue encode(Float value) {
      return new BsonDouble(value);
    }
  }

  static class Parser implements ValueParser<Float> {

    private static final float MAX_FLOAT = Float.MAX_VALUE;
    private static final float MIN_FLOAT = -1 * Float.MAX_VALUE;

    @Override
    public Float parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      switch (value.getBsonType()) {
        case INT32 -> {
          return (float) value.asInt32().getValue();
        }
        case INT64 -> {
          return (float) value.asInt64().getValue();
        }
        case DOUBLE -> {
          double doubleValue = value.asDouble().doubleValue();

          // Explicitly handle special cases before checking for over-/underflow.
          if (doubleValue == Double.NEGATIVE_INFINITY) {
            return Float.NEGATIVE_INFINITY;
          } else if (doubleValue == Double.POSITIVE_INFINITY) {
            return Float.POSITIVE_INFINITY;
          } else if (Double.isNaN(doubleValue)) {
            return Float.NaN;
          }

          if (doubleValue > MAX_FLOAT) {
            context.handleOverflow(TypeDescription.FLOAT);
          } else if (doubleValue < MIN_FLOAT) {
            context.handleUnderflow(TypeDescription.FLOAT);
          }

          return (float) doubleValue;
        }
        default -> {
          return context.handleUnexpectedType(TypeDescription.NUMBER, value.getBsonType());
        }
      }
    }
  }
}
