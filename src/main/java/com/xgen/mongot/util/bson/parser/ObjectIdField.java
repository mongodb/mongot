package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

/** Contains the TypeBuilders and ValueParser for built in ObjectId parsing. */
public class ObjectIdField {

  private static final Parser PARSER = new Parser();
  private static final Encoder OBJECT_ID_ENCODER = new Encoder(Encoding.OBJECT_ID);
  private static final Encoder STRING_ENCODER = new Encoder(Encoding.STRING);

  private enum Encoding {
    OBJECT_ID,
    STRING,
  }

  public static class FieldBuilder extends Field.TypedBuilder<ObjectId, FieldBuilder> {

    FieldBuilder(String name) {
      this(name, OBJECT_ID_ENCODER, PARSER, java.util.Optional.empty());
    }

    private FieldBuilder(
        String name,
        ValueEncoder<ObjectId> encoder,
        ValueParser<ObjectId> parser,
        java.util.Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    public FieldBuilder encodeAsString() {
      return new FieldBuilder(this.name, STRING_ENCODER, this.parser, this.edition);
    }

    @Override
    BuilderFactory<ObjectId, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends Value.TypedBuilder<ObjectId, ValueBuilder> {

    ValueBuilder() {
      this(OBJECT_ID_ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<ObjectId> encoder, ValueParser<ObjectId> parser) {
      super(encoder, parser);
    }

    ValueBuilder encodeAsString() {
      return new ValueBuilder(STRING_ENCODER, this.parser);
    }

    @Override
    BuilderFactory<ObjectId, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private static class Encoder implements ValueEncoder<ObjectId> {

    private final Encoding encoding;

    Encoder(Encoding encoding) {
      this.encoding = encoding;
    }

    @Override
    public BsonValue encode(ObjectId value) {
      return switch (this.encoding) {
        case OBJECT_ID -> new BsonObjectId(value);
        case STRING -> new BsonString(value.toHexString());
      };
    }
  }

  private static class Parser implements ValueParser<ObjectId> {

    @Override
    public ObjectId parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      switch (value.getBsonType()) {
        case OBJECT_ID -> {
          return value.asObjectId().getValue();
        }
        case STRING -> {
          String stringValue = value.asString().getValue();
          if (!ObjectId.isValid(stringValue)) {
            return context.handleSemanticError("is invalid ObjectId format");
          }

          return new ObjectId(stringValue);
        }
        default -> {
          return context.handleUnexpectedType(TypeDescription.OBJECT_ID, value.getBsonType());
        }
      }
    }
  }
}
