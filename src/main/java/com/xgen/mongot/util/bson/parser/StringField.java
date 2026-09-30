package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.Optional;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;

public class StringField {

  private static final Encoder ENCODER = new Encoder();
  private static final Parser PARSER = new Parser();

  public static class FieldBuilder extends Field.TypedBuilder<String, FieldBuilder> {

    FieldBuilder(String name) {
      this(name, ENCODER, PARSER, Optional.empty());
    }

    private FieldBuilder(
        String name,
        ValueEncoder<String> encoder,
        ValueParser<String> parser,
        Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    public FieldBuilder mustNotBeEmpty() {
      ValueParser<String> parser = StringField.mustNotBeEmpty(this.parser);
      return new FieldBuilder(this.name, this.encoder, parser, this.edition);
    }

    public FieldBuilder mustNotBeBlank() {
      ValueParser<String> parser = StringField.mustNotBeBlank(this.parser);
      return new FieldBuilder(this.name, this.encoder, parser, this.edition);
    }

    public FieldBuilder mustNotBeginWith(String prefix) {
      ValueParser<String> parser = StringField.mustNotBeginWith(this.parser, prefix);
      return new FieldBuilder(this.name, this.encoder, parser, this.edition);
    }

    @Override
    BuilderFactory<String, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends Value.TypedBuilder<String, ValueBuilder> {

    ValueBuilder() {
      this(ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<String> encoder, ValueParser<String> parser) {
      super(encoder, parser);
    }

    public ValueBuilder mustNotBeEmpty() {
      ValueParser<String> parser = StringField.mustNotBeEmpty(this.parser);
      return new ValueBuilder(this.encoder, parser);
    }

    @Override
    BuilderFactory<String, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private static ValueParser<String> mustNotBeEmpty(ValueParser<String> parser) {
    return new ValidatingValueParser<>(
        parser, s -> s.isEmpty() ? Optional.of("cannot be empty") : Optional.empty());
  }

  private static ValueParser<String> mustNotBeBlank(ValueParser<String> parser) {
    return new ValidatingValueParser<>(
        parser, s -> s.isBlank() ? Optional.of("cannot be blank") : Optional.empty());
  }

  private static ValueParser<String> mustNotBeginWith(ValueParser<String> parser, String prefix) {
    return new ValidatingValueParser<>(
        parser,
        s ->
            s.startsWith(prefix)
                ? Optional.of(String.format("cannot begin with %s", prefix))
                : Optional.empty());
  }

  private static class Encoder implements ValueEncoder<String> {

    @Override
    public BsonValue encode(String value) {
      return new BsonString(value);
    }
  }

  private static class Parser implements ValueParser<String> {

    @Override
    public String parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      if (value.getBsonType() != BsonType.STRING) {
        context.handleUnexpectedType(TypeDescription.STRING, value.getBsonType());
      }

      return value.asString().getValue();
    }
  }
}
