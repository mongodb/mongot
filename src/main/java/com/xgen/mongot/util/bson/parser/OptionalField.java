package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.Optional;
import org.bson.BsonNull;
import org.bson.BsonValue;

/** Contains builders and ValueParser for optional fields. */
public class OptionalField {

  static final BsonValue MISSING_VALUE = new BsonNull();

  public static class FieldBuilder<T> {

    private final String name;
    private final Encoder<T> encoder;
    private final ValueParser<T> parser;
    private final boolean preserveNulls;
    private final Optional<DeploymentEnvironment> edition;

    private FieldBuilder(
        String name,
        Encoder<T> encoder,
        ValueParser<T> parser,
        boolean preserveNulls,
        Optional<DeploymentEnvironment> edition) {
      this.name = name;
      this.encoder = encoder;
      this.parser = parser;
      this.preserveNulls = preserveNulls;
      this.edition = edition;
    }

    static <T> FieldBuilder<T> create(
        String name,
        ValueEncoder<T> encoder,
        ValueParser<T> parser,
        Optional<DeploymentEnvironment> edition) {
      return new FieldBuilder<>(name, new Encoder<>(encoder), parser, false, edition);
    }

    private ValueParser<Optional<T>> getParser() {
      return this.preserveNulls ? new NullableParser<>(this.parser) : new Parser<>(this.parser);
    }

    public Field.WithDefault<T> withDefault(T value) {
      return new Field.WithDefault<>(this.name, this.encoder, getParser(), value, this.edition);
    }

    public Field.Optional<T> noDefault() {
      return new Field.Optional<>(this.name, this.encoder, getParser(), this.edition);
    }

    /**
     * Restricts this field to the given on-prem edition. Only meaningful for on-prem
     * (Community/Enterprise) configuration fields; Atlas configurations do not set an edition.
     */
    public FieldBuilder<T> edition(DeploymentEnvironment edition) {
      return new FieldBuilder<>(
          this.name, this.encoder, this.parser, this.preserveNulls, Optional.of(edition));
    }

    /**
     * Configures this FieldBuilder to preserve BSON null values when parsing.
     *
     * <p>By default, BSON null values are treated as missing and parsed as {@code
     * Optional.empty()}. When this method is called, BSON nulls are preserved and parsed as {@code
     * Optional.of(null)}, allowing you to distinguish between a missing field and a field
     * explicitly set to null in BSON.
     *
     * <p>Use this method when you need to differentiate between fields that are absent and fields
     * that are present but set to null in BSON.
     *
     * @return a new FieldBuilder instance that preserves BSON null values during parsing
     */
    public FieldBuilder<T> preserveBsonNull() {
      return new FieldBuilder<>(this.name, this.encoder, this.parser, true, this.edition);
    }
  }

  public static class ValueBuilder<T> {

    private final Encoder<T> encoder;
    private final Parser<T> parser;

    ValueBuilder(ValueEncoder<T> encoder, ValueParser<T> parser) {
      this.encoder = new Encoder<>(encoder);
      this.parser = new Parser<>(parser);
    }

    public Value.WithDefault<T> withDefault(T value) {
      return new Value.WithDefault<>(this.encoder, this.parser, value);
    }

    public Value.Optional<T> noDefault() {
      return new Value.Optional<>(this.encoder, this.parser);
    }
  }

  private static class Encoder<T> implements ValueEncoder<Optional<T>> {

    private final ValueEncoder<T> wrapped;

    Encoder(ValueEncoder<T> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public BsonValue encode(Optional<T> value) {
      return value.map(this.wrapped::encode).orElse(BsonNull.VALUE);
    }
  }

  private static class Parser<T> implements ValueParser<Optional<T>> {

    private final ValueParser<T> wrapped;

    Parser(ValueParser<T> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public Optional<T> parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      if (value.isNull()) {
        return Optional.empty();
      }

      return Optional.of(this.wrapped.parse(context, value));
    }
  }

  /**
   * A ValueParser that treats a specific {@code MISSING_VALUE} (a {@link BsonNull}) as missing and
   * returns {@link Optional#empty()}.
   *
   * <p>Uses reference equality to avoid matching any other BSON null value. This is useful when
   * distinguishing between fields that are absent (represented by {@code MISSING_VALUE}) and fields
   * that are explicitly set to BSON null.
   */
  private record NullableParser<T>(ValueParser<T> wrapped) implements ValueParser<Optional<T>> {

    @Override
    public Optional<T> parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      if (value == MISSING_VALUE) {
        return Optional.empty();
      }
      return Optional.of(this.wrapped.parse(context, value));
    }
  }
}
