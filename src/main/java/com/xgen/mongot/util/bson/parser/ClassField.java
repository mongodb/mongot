package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import org.bson.BsonType;
import org.bson.BsonValue;

/**
 * Contains classes, builders, and ValueParsers for parsing Java classes that do not have built in
 * support, or need more custom parsing.
 */
public class ClassField {

  /** FromDocumentParser represents a method that can produce a T from a DocumentParser. */
  @FunctionalInterface
  public interface FromDocumentParser<T> {

    T parse(DocumentParser parser) throws BsonParseException;
  }

  /** FromValueParser represents a method that can produce a T from a raw BsonValue. */
  @FunctionalInterface
  public interface FromValueParser<T> {

    T parse(BsonParseContext context, BsonValue value) throws BsonParseException;
  }

  /**
   * Wraps a FieldBuilder that contains a DocumentParser in order to enforce whether unknown fields
   * are accepted or not.
   */
  public static class DocumentFieldBuilder<T> {
    private final String name;
    private final ValueEncoder<T> encoder;
    private final FromDocumentParser<T> parser;

    DocumentFieldBuilder(String name, ValueEncoder<T> encoder, FromDocumentParser<T> parser) {
      this.name = name;
      this.encoder = encoder;
      this.parser = parser;
    }

    public FieldBuilder<T> allowUnknownFields() {
      return new FieldBuilder<>(
          this.name,
          this.encoder,
          new DocumentValueParser<>(this.parser, true),
          java.util.Optional.empty());
    }

    public FieldBuilder<T> disallowUnknownFields() {
      return new FieldBuilder<>(
          this.name,
          this.encoder,
          new DocumentValueParser<>(this.parser, false),
          java.util.Optional.empty());
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

    FieldBuilder(String name, ValueEncoder<T> encoder, FromValueParser<T> parser) {
      this(name, encoder, new ValueValueParser<>(parser), java.util.Optional.empty());
    }

    @Override
    BuilderFactory<T, FieldBuilder<T>> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  /**
   * Wraps a ValueBuilder that contains a DocumentParser in order to enforce whether unknown fields
   * are accepted or not.
   */
  public static class DocumentValueBuilder<T> {
    private final ValueEncoder<T> encoder;
    private final FromDocumentParser<T> parser;

    DocumentValueBuilder(ValueEncoder<T> encoder, FromDocumentParser<T> parser) {
      this.encoder = encoder;
      this.parser = parser;
    }

    public ValueBuilder<T> allowUnknownFields() {
      return new ValueBuilder<>(this.encoder, new DocumentValueParser<>(this.parser, true));
    }

    public ValueBuilder<T> disallowUnknownFields() {
      return new ValueBuilder<>(this.encoder, new DocumentValueParser<>(this.parser, false));
    }
  }

  public static class ValueBuilder<T> extends Value.TypedBuilder<T, ValueBuilder<T>> {

    private ValueBuilder(ValueEncoder<T> encoder, ValueParser<T> parser) {
      super(encoder, parser);
    }

    ValueBuilder(ValueEncoder<T> encoder, FromValueParser<T> parser) {
      this(encoder, new ValueValueParser<>(parser));
    }

    @Override
    BuilderFactory<T, ValueBuilder<T>> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private static class DocumentValueParser<T> implements ValueParser<T> {

    private final FromDocumentParser<T> fromDocumentParser;
    private final boolean allowUnknownFields;

    private DocumentValueParser(
        FromDocumentParser<T> fromDocumentParser, boolean allowUnknownFields) {
      this.fromDocumentParser = fromDocumentParser;
      this.allowUnknownFields = allowUnknownFields;
    }

    @Override
    public T parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      if (value.getBsonType() != BsonType.DOCUMENT) {
        context.handleUnexpectedType(TypeDescription.DOCUMENT, value.getBsonType());
      }

      try (DocumentParser parser =
          BsonDocumentParser.withContext(context, value.asDocument())
              .allowUnknownFields(this.allowUnknownFields)
              .build()) {
        return this.fromDocumentParser.parse(parser);
      }
    }
  }

  private static class ValueValueParser<T> implements ValueParser<T> {

    private final FromValueParser<T> parser;

    private ValueValueParser(FromValueParser<T> parser) {
      this.parser = parser;
    }

    @Override
    public T parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      return this.parser.parse(context, value);
    }
  }
}
