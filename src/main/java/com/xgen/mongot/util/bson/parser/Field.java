package com.xgen.mongot.util.bson.parser;

import com.xgen.proto.BsonMessage;
import org.bson.BsonValue;

/**
 * Field contains classes and methods for creating field specifications. A field combines both a
 * field name and a ValueParser to specify how to deserialize a value from a BSON document.
 */
public class Field {

  /**
   * Implemented by {@link Field} specifications that know the YAML name of the field they
   * deserialize.
   */
  public interface Nameable {
    String getName();
  }

  public static class Required<T> implements Nameable {

    private final String name;
    private final ValueEncoder<T> encoder;
    private final ValueParser<T> parser;

    Required(String name, ValueEncoder<T> encoder, ValueParser<T> parser) {
      this.name = name;
      this.encoder = encoder;
      this.parser = parser;
    }

    public String getName() {
      return this.name;
    }

    public BsonValue encode(T value) {
      return this.encoder.encode(value);
    }

    ValueParser<T> getParser() {
      return this.parser;
    }
  }

  public static class Optional<T> implements Nameable {

    private final String name;
    private final ValueEncoder<java.util.Optional<T>> encoder;
    private final ValueParser<java.util.Optional<T>> parser;

    Optional(
        String name,
        ValueEncoder<java.util.Optional<T>> encoder,
        ValueParser<java.util.Optional<T>> parser) {
      this.name = name;
      this.encoder = encoder;
      this.parser = parser;
    }

    public String getName() {
      return this.name;
    }

    public BsonValue encode(java.util.Optional<T> value) {
      return this.encoder.encode(value);
    }

    ValueParser<java.util.Optional<T>> getParser() {
      return this.parser;
    }
  }

  public static class WithDefault<T> implements Nameable {

    private final String name;
    private final ValueEncoder<java.util.Optional<T>> encoder;
    private final ValueParser<java.util.Optional<T>> parser;
    private final T defaultValue;

    WithDefault(
        String name,
        ValueEncoder<java.util.Optional<T>> encode,
        ValueParser<java.util.Optional<T>> parser,
        T defaultValue) {
      this.name = name;
      this.encoder = encode;
      this.parser = parser;
      this.defaultValue = defaultValue;
    }

    public String getName() {
      return this.name;
    }

    public T getDefaultValue() {
      return this.defaultValue;
    }

    public BsonValue encode(T value) {
      return this.encoder.encode(java.util.Optional.of(value));
    }

    ValueParser<java.util.Optional<T>> getParser() {
      return this.parser;
    }
  }

  public static TypeBuilder builder(String name) {
    return new TypeBuilder(name);
  }

  /** A builder stage that requires the type being parsed to be specified. */
  public static class TypeBuilder {

    private final String name;

    TypeBuilder(String name) {
      this.name = name;
    }

    public BinaryField.FieldBuilder binaryField() {
      return new BinaryField.FieldBuilder(this.name);
    }

    public BooleanField.FieldBuilder booleanField() {
      return new BooleanField.FieldBuilder(this.name);
    }

    public <T extends Encodable> ClassField.DocumentFieldBuilder<T> classField(
        ClassField.FromDocumentParser<T> parser) {
      return new ClassField.DocumentFieldBuilder<>(this.name, Encodable::toBson, parser);
    }

    public <T extends Encodable> ClassField.FieldBuilder<T> classField(
        ClassField.FromValueParser<T> parser) {
      return new ClassField.FieldBuilder<>(this.name, Encodable::toBson, parser);
    }

    public <T> ClassField.DocumentFieldBuilder<T> classField(
        ClassField.FromDocumentParser<T> parser, ValueEncoder<T> encoder) {
      return new ClassField.DocumentFieldBuilder<>(this.name, encoder, parser);
    }

    public <T> ClassField.FieldBuilder<T> classField(
        ClassField.FromValueParser<T> parser, ValueEncoder<T> encoder) {
      return new ClassField.FieldBuilder<>(this.name, encoder, parser);
    }

    public <T extends BsonMessage> ProtoField.DocumentFieldBuilder<T> protoField(
        ProtoField.MessageParser<T> parser) {
      return new ProtoField.DocumentFieldBuilder<>(this.name, parser);
    }

    public DocumentField.FieldBuilder documentField() {
      return new DocumentField.FieldBuilder(this.name);
    }

    public BsonDateTimeField.FieldBuilder bsonDateTimeField() {
      return new BsonDateTimeField.FieldBuilder(this.name);
    }

    public BsonNumberField.FieldBuilder bsonNumberField() {
      return new BsonNumberField.FieldBuilder(this.name);
    }

    public BsonTimestampField.FieldBuilder bsonTimestampField() {
      return new BsonTimestampField.FieldBuilder(this.name);
    }

    public DoubleField.FieldBuilder doubleField() {
      return new DoubleField.FieldBuilder(this.name);
    }

    public <T extends Enum<T>> EnumField.FieldCaseSelector<T> enumField(Class<T> enumClass) {
      return new EnumField.FieldCaseSelector<>(this.name, enumClass);
    }

    public FloatField.FieldBuilder floatField() {
      return new FloatField.FieldBuilder(this.name);
    }

    public IntegerField.FieldBuilder intField() {
      return new IntegerField.FieldBuilder(this.name);
    }

    public LongField.FieldBuilder longField() {
      return new LongField.FieldBuilder(this.name);
    }

    public ObjectIdField.FieldBuilder objectIdField() {
      return new ObjectIdField.FieldBuilder(this.name);
    }

    public StringField.FieldBuilder stringField() {
      return new StringField.FieldBuilder(this.name);
    }

    public UnparsedValueField.FieldBuilder unparsedValueField() {
      return new UnparsedValueField.FieldBuilder(this.name);
    }

    public UuidField.FieldBuilder uuidField() {
      return new UuidField.FieldBuilder(this.name);
    }

    public <T> ListField.FieldBuilder<T> listOf(Value.Required<T> element) {
      return new ListField.FieldBuilder<>(
          this.name,
          element.getEncoder(),
          element.getParser(),
          ListField.SingleValuePresence.MUST_BE_LIST);
    }

    public <T> ListField.FieldBuilder<java.util.Optional<T>> listOf(Value.Optional<T> element) {
      return new ListField.FieldBuilder<>(
          this.name,
          element.getEncoder(),
          element.getParser(),
          ListField.SingleValuePresence.MUST_BE_LIST);
    }

    public <T> ListField.FieldBuilder<java.util.Optional<T>> listOf(Value.WithDefault<T> element) {
      return new ListField.FieldBuilder<>(
          this.name,
          element.getEncoder(),
          element.getParser(),
          ListField.SingleValuePresence.MUST_BE_LIST);
    }

    /**
     * Creates a list of the supplied element, allowing the parsed value to be a single value of the
     * element type.
     */
    public <T> ListField.FieldBuilder<T> singleValueOrListOf(Value.Required<T> element) {
      return new ListField.FieldBuilder<>(
          this.name,
          element.getEncoder(),
          element.getParser(),
          ListField.SingleValuePresence.SINGLE_VALUE_ALLOWED);
    }

    /**
     * Creates a list of the supplied element, allowing the parsed value to be a single value of the
     * element type.
     */
    public <T> ListField.FieldBuilder<java.util.Optional<T>> singleValueOrListOf(
        Value.Optional<T> element) {
      return new ListField.FieldBuilder<>(
          this.name,
          element.getEncoder(),
          element.getParser(),
          ListField.SingleValuePresence.SINGLE_VALUE_ALLOWED);
    }

    public <T> MapField.FieldBuilder<T> mapOf(Value.Required<T> value) {
      return MapField.FieldBuilder.create(this.name, value.getEncoder(), value.getParser());
    }

    public <T> MapField.FieldBuilder<java.util.Optional<T>> mapOf(Value.Optional<T> value) {
      return MapField.FieldBuilder.create(this.name, value.getEncoder(), value.getParser());
    }

    public <T> MapField.FieldBuilder<java.util.Optional<T>> mapOf(Value.WithDefault<T> value) {
      return MapField.FieldBuilder.create(this.name, value.getEncoder(), value.getParser());
    }
  }

  /**
   * Each type with built in support for parsing should supply a builder which can be returned by
   * the TypeBuilder that supports type-specific validations, as well as the methods below.
   *
   * <p>Each type should extend TypeBuilder, where T is the type of field that will be built from
   * it.
   *
   * <p>B is the actual class that extends TypeBuilder. By having B specified, TypeBuilder can
   * return that specific class B rather than just a TypeBuilder over T, allowing for the
   * type-specific validation methods to be visible.
   */
  abstract static class TypedBuilder<T, B extends TypedBuilder<T, B>> {

    final String name;
    final ValueEncoder<T> encoder;
    final ValueParser<T> parser;

    TypedBuilder(String name, ValueEncoder<T> encoder, ValueParser<T> parser) {
      this.name = name;
      this.encoder = encoder;
      this.parser = parser;
    }

    interface BuilderFactory<T, B> {
      B getBuilder(String name, ValueEncoder<T> encoder, ValueParser<T> parser);
    }

    abstract BuilderFactory<T, B> getBuilderFactory();

    public B validate(FieldValidator<T> validator) {
      ValueParser<T> parser = new ValidatingValueParser<>(this.parser, validator);
      return getBuilderFactory().getBuilder(this.name, this.encoder, parser);
    }

    public Field.Required<T> required() {
      return new Field.Required<>(this.name, this.encoder, this.parser);
    }

    public OptionalField.FieldBuilder<T> optional() {
      return new OptionalField.FieldBuilder<>(this.name, this.encoder, this.parser);
    }

    public ListField.FieldBuilder<T> asList() {
      return new ListField.FieldBuilder<>(
          this.name, this.encoder, this.parser, ListField.SingleValuePresence.MUST_BE_LIST);
    }

    public ListField.FieldBuilder<T> asSingleValueOrList() {
      return new ListField.FieldBuilder<>(
          this.name, this.encoder, this.parser, ListField.SingleValuePresence.SINGLE_VALUE_ALLOWED);
    }

    public MapField.FieldBuilder<T> asMap() {
      return MapField.FieldBuilder.create(this.name, this.encoder, this.parser);
    }

    /**
     * Like {@link #asMap()}, but the parsed map preserves the BSON document's declared field order.
     * Use for order-sensitive consumers (e.g. aggregation facet definitions).
     */
    public MapField.FieldBuilder<T> asOrderedMap() {
      return MapField.FieldBuilder.createOrdered(this.name, this.encoder, this.parser);
    }
  }
}
