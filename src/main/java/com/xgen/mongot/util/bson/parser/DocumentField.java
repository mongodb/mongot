package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/** Contains the TypeBuilders and ValueParser for built in BsonDocument parsing. */
public class DocumentField {

  private static final Encoder ENCODER = new Encoder();
  private static final Parser PARSER = new Parser();

  public static class FieldBuilder extends Field.TypedBuilder<BsonDocument, FieldBuilder> {

    FieldBuilder(String name) {
      this(name, ENCODER, PARSER, Optional.empty());
    }

    private FieldBuilder(
        String name,
        ValueEncoder<BsonDocument> encoder,
        ValueParser<BsonDocument> parser,
        Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    @Override
    BuilderFactory<BsonDocument, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends Value.TypedBuilder<BsonDocument, ValueBuilder> {

    ValueBuilder() {
      this(ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<BsonDocument> encoder, ValueParser<BsonDocument> parser) {
      super(encoder, parser);
    }

    @Override
    BuilderFactory<BsonDocument, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private static class Encoder implements ValueEncoder<BsonDocument> {

    @Override
    public BsonValue encode(BsonDocument value) {
      return value;
    }
  }

  private static class Parser implements ValueParser<BsonDocument> {

    @Override
    public BsonDocument parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      if (!value.isDocument()) {
        context.handleUnexpectedType(TypeDescription.DOCUMENT, value.getBsonType());
      }

      return value.asDocument();
    }
  }
}
