package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.Optional;
import org.apache.commons.lang3.Range;

/** Contains TypeBuilders for built in parsing of Number types. */
class NumericField {

  abstract static class FieldBuilder<N extends Number, B extends FieldBuilder<N, B>>
      extends Field.TypedBuilder<N, B> {

    FieldBuilder(
        String name,
        ValueEncoder<N> encoder,
        ValueParser<N> parser,
        Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    public B mustBeNonNegative() {
      ValueParser<N> parser = NumericField.mustBeNonNegative(this.parser);
      return getBuilderFactory().getBuilder(this.name, this.encoder, parser, this.edition);
    }

    public B mustBePositive() {
      ValueParser<N> parser = NumericField.mustBePositive(this.parser);
      return getBuilderFactory().getBuilder(this.name, this.encoder, parser, this.edition);
    }

    public B mustBeWithinBounds(Range<N> bounds) {
      ValueParser<N> parser = NumericField.mustBeWithinBounds(this.parser, bounds);
      return getBuilderFactory().getBuilder(this.name, this.encoder, parser, this.edition);
    }
  }

  abstract static class ValueBuilder<N extends Number, B extends ValueBuilder<N, B>>
      extends Value.TypedBuilder<N, B> {

    ValueBuilder(ValueEncoder<N> encoder, ValueParser<N> parser) {
      super(encoder, parser);
    }

    public B mustBeNonNegative() {
      ValueParser<N> parser = NumericField.mustBeNonNegative(this.parser);
      return getBuilderFactory().getBuilder(this.encoder, parser);
    }

    public B mustBePositive() {
      ValueParser<N> parser = NumericField.mustBePositive(this.parser);
      return getBuilderFactory().getBuilder(this.encoder, parser);
    }

    public B mustBeWithinBounds(Range<N> bounds) {
      ValueParser<N> parser = NumericField.mustBeWithinBounds(this.parser, bounds);
      return getBuilderFactory().getBuilder(this.encoder, parser);
    }
  }

  private static <N extends Number> ValueParser<N> mustBeNonNegative(ValueParser<N> parser) {
    return new ValidatingValueParser<>(
        parser, n -> n.doubleValue() < 0 ? Optional.of("cannot be negative") : Optional.empty());
  }

  private static <N extends Number> ValueParser<N> mustBePositive(ValueParser<N> parser) {
    return new ValidatingValueParser<>(
        parser, n -> n.doubleValue() <= 0 ? Optional.of("must be positive") : Optional.empty());
  }

  private static <N extends Number> ValueParser<N> mustBeWithinBounds(
      ValueParser<N> parser, Range<N> bounds) {
    return new ValidatingValueParser<>(
        parser,
        n ->
            bounds.contains(n) ? Optional.empty() : Optional.of("must be within bounds " + bounds));
  }
}
