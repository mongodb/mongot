package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.proto.BsonMessage;
import com.xgen.proto.BsonProtoParseException;
import org.bson.BsonDocumentReader;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;

public class ProtoField {
  @FunctionalInterface
  public interface MessageParser<T> {
    T parse(BsonReader reader, boolean allowUnknownFields) throws BsonProtoParseException;
  }

  /**
   * Wraps a FieldBuilder that contains a DocumentParser in order to enforce whether unknown fields
   * are accepted or not.
   */
  public static class DocumentFieldBuilder<T extends BsonMessage> {
    private final String name;
    private final MessageParser<T> parser;

    DocumentFieldBuilder(String name, MessageParser<T> parser) {
      this.name = name;
      this.parser = parser;
    }

    public FieldBuilder<T> disallowUnknownFields() {
      return new FieldBuilder<>(this.name, T::toBson, new ProtoValueParser<>(this.parser, false));
    }

    public FieldBuilder<T> allowUnknownFields() {
      return new FieldBuilder<>(this.name, T::toBson, new ProtoValueParser<>(this.parser, true));
    }
  }

  public static class FieldBuilder<T> extends Field.TypedBuilder<T, FieldBuilder<T>> {

    private FieldBuilder(
        String name,
        ValueEncoder<T> encoder,
        ValueParser<T> parser,
        java.util.Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    private FieldBuilder(String name, ValueEncoder<T> encoder, ValueParser<T> parser) {
      this(name, encoder, parser, java.util.Optional.empty());
    }

    @Override
    BuilderFactory<T, FieldBuilder<T>> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  private static class ProtoValueParser<T> implements ValueParser<T> {
    private final MessageParser<T> parser;
    private final boolean allowUnknownFields;

    private ProtoValueParser(MessageParser<T> parser, boolean allowUnknownFields) {
      this.parser = parser;
      this.allowUnknownFields = allowUnknownFields;
    }

    @Override
    public T parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      if (value.getBsonType() != BsonType.DOCUMENT) {
        context.handleUnexpectedType(TypeDescription.DOCUMENT, value.getBsonType());
      }

      try {
        return this.parser.parse(
            new BsonDocumentReader(value.asDocument()), this.allowUnknownFields);
      } catch (BsonProtoParseException e) {
        throw new BsonParseException(e);
      }
    }
  }
}
