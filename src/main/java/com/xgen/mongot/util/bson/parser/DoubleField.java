package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.Optional;
import org.bson.BsonDouble;
import org.bson.BsonValue;

public class DoubleField {

  private static final Encoder ENCODER = new Encoder();
  private static final Parser PARSER = new Parser();

  public static class FieldBuilder extends NumericField.FieldBuilder<Double, FieldBuilder> {

    FieldBuilder(String name) {
      this(name, ENCODER, PARSER, Optional.empty());
    }

    private FieldBuilder(
        String name,
        ValueEncoder<Double> encoder,
        ValueParser<Double> parser,
        Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    public FieldBuilder mustBeFinite() {
      ValueParser<Double> parser = DoubleField.mustBeFinite(this.parser);
      return new FieldBuilder(this.name, this.encoder, parser, this.edition);
    }

    @Override
    BuilderFactory<Double, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends NumericField.ValueBuilder<Double, ValueBuilder> {

    ValueBuilder() {
      this(ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<Double> encoder, ValueParser<Double> parser) {
      super(encoder, parser);
    }

    public ValueBuilder mustBeFinite() {
      ValueParser<Double> parser = DoubleField.mustBeFinite(this.parser);
      return new ValueBuilder(this.encoder, parser);
    }

    @Override
    BuilderFactory<Double, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private static ValueParser<Double> mustBeFinite(ValueParser<Double> parser) {
    return new ValidatingValueParser<>(
        parser, d -> Double.isFinite(d) ? Optional.empty() : Optional.of("must be finite"));
  }

  private static class Encoder implements ValueEncoder<Double> {

    @Override
    public BsonValue encode(Double value) {
      return new BsonDouble(value);
    }
  }

  private static class Parser implements ValueParser<Double> {

    @Override
    public Double parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      return switch (value.getBsonType()) {
        case DOUBLE, INT32, INT64 -> value.asNumber().doubleValue();
        default -> context.handleUnexpectedType(TypeDescription.NUMBER, value.getBsonType());
      };
    }
  }
}
