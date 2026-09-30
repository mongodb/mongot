package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;

/** Contains the TypeBuilders and ValueParser for built in UUID parsing. */
public class UuidField {

  private static final Parser PARSER = new Parser();
  private static final Encoder BINARY_ENCODER = new Encoder(Encoding.BINARY);
  private static final Encoder STRING_ENCODER = new Encoder(Encoding.STRING);

  private enum Encoding {
    BINARY,
    STRING,
  }

  public static class FieldBuilder extends Field.TypedBuilder<UUID, FieldBuilder> {

    FieldBuilder(String name) {
      this(name, BINARY_ENCODER, PARSER, java.util.Optional.empty());
    }

    private FieldBuilder(
        String name,
        ValueEncoder<UUID> encoder,
        ValueParser<UUID> parser,
        java.util.Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    public FieldBuilder encodeAsString() {
      return new FieldBuilder(this.name, STRING_ENCODER, this.parser, this.edition);
    }

    @Override
    BuilderFactory<UUID, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends Value.TypedBuilder<UUID, ValueBuilder> {

    ValueBuilder() {
      super(BINARY_ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<UUID> encoder, ValueParser<UUID> parser) {
      super(encoder, parser);
    }

    public ValueBuilder encodeAsString() {
      return new ValueBuilder(STRING_ENCODER, this.parser);
    }

    @Override
    BuilderFactory<UUID, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private static class Encoder implements ValueEncoder<UUID> {

    private final Encoding encoding;

    Encoder(Encoding encoding) {
      this.encoding = encoding;
    }

    @Override
    public BsonValue encode(UUID value) {
      return switch (this.encoding) {
        case BINARY -> new BsonBinary(value);
        case STRING -> new BsonString(value.toString());
      };
    }
  }

  private static class Parser implements ValueParser<UUID> {

    @Override
    public UUID parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      switch (value.getBsonType()) {
        case BINARY -> {
          var binary = value.asBinary();
          if (binary.getType() != BsonBinarySubType.UUID_STANDARD.getValue()) {
            context.handleUnexpectedType(TypeDescription.UUID, BsonType.BINARY);
          }

          return binary.asUuid();
        }
        case STRING -> {
          var string = value.asString().getValue();
          return fromString(context, string);
        }
        default -> {
          return context.handleUnexpectedType(TypeDescription.UUID, value.getBsonType());
        }
      }
    }

    private static UUID fromString(BsonParseContext context, String string)
        throws BsonParseException {
      try {
        return UUID.fromString(string);
      } catch (IllegalArgumentException e) {
        return context.handleSemanticError(e.getMessage());
      }
    }
  }
}
