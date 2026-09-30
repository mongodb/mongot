package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonType;
import org.bson.BsonValue;

/** Contains the TypeBuilders and ValueParser for built in Map parsing. */
public class MapField {

  public static class FieldBuilder<T> extends Field.TypedBuilder<Map<String, T>, FieldBuilder<T>> {

    private FieldBuilder(
        String name,
        ValueEncoder<Map<String, T>> encoder,
        ValueParser<Map<String, T>> parser,
        Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    /**
     * Creates a new FieldBuilder. Must be a static factory method because a constructor would have
     * the same type erasure as the required private constructor.
     */
    static <T> FieldBuilder<T> create(
        String name,
        ValueEncoder<T> valueEncoder,
        ValueParser<T> valueParser,
        Optional<DeploymentEnvironment> edition) {
      return new FieldBuilder<>(
          name, new Encoder<>(valueEncoder), new Parser<>(valueParser, HashMap::new), edition);
    }

    /**
     * Like {@link #create}, but the parsed map preserves the BSON document's field order
     * (BsonDocument::entrySet is insertion-ordered). Opt-in for order-sensitive consumers such as
     * aggregation facet definitions; the default stays a plain HashMap so existing call sites keep
     * their iteration behavior.
     */
    static <T> FieldBuilder<T> createOrdered(
        String name,
        ValueEncoder<T> valueEncoder,
        ValueParser<T> valueParser,
        Optional<DeploymentEnvironment> edition) {
      return new FieldBuilder<>(
          name,
          new Encoder<>(valueEncoder),
          new Parser<>(valueParser, LinkedHashMap::new),
          edition);
    }

    public FieldBuilder<T> mustNotBeEmpty() {
      ValueParser<Map<String, T>> parser = MapField.mustNotBeEmpty(this.parser);
      return new FieldBuilder<>(this.name, this.encoder, parser, this.edition);
    }

    public FieldBuilder<T> mustNotContainEmptyStringAsKey() {
      ValueParser<Map<String, T>> parser = MapField.mustNotContainEmptyStringAsKey(this.parser);
      return new FieldBuilder<>(this.name, this.encoder, parser, this.edition);
    }

    public FieldBuilder<T> validateKeys(FieldValidator<String> keyValidator) {
      ValueParser<Map<String, T>> parser = MapField.validateKeys(this.parser, keyValidator);
      return new FieldBuilder<>(this.name, this.encoder, parser, this.edition);
    }

    @Override
    BuilderFactory<Map<String, T>, FieldBuilder<T>> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder<T> extends Value.TypedBuilder<Map<String, T>, ValueBuilder<T>> {

    private ValueBuilder(ValueEncoder<Map<String, T>> encoder, ValueParser<Map<String, T>> parser) {
      super(encoder, parser);
    }

    /**
     * Creates a new ValueBuilder. Must be a static factory method because a constructor would have
     * the same type erasure as the required private constructor.
     */
    static <T> ValueBuilder<T> create(ValueEncoder<T> valueEncoder, ValueParser<T> valueParser) {
      return new ValueBuilder<>(
          new Encoder<>(valueEncoder), new Parser<>(valueParser, HashMap::new));
    }

    public ValueBuilder<T> mustNotBeEmpty() {
      ValueParser<Map<String, T>> parser = MapField.mustNotBeEmpty(this.parser);
      return new ValueBuilder<>(this.encoder, parser);
    }

    public ValueBuilder<T> mustNotContainEmptyStringAsKey() {
      ValueParser<Map<String, T>> parser = MapField.mustNotContainEmptyStringAsKey(this.parser);
      return new ValueBuilder<>(this.encoder, parser);
    }

    public ValueBuilder<T> validateKeys(FieldValidator<String> keyValidator) {
      ValueParser<Map<String, T>> parser = MapField.validateKeys(this.parser, keyValidator);
      return new ValueBuilder<>(this.encoder, parser);
    }

    @Override
    BuilderFactory<Map<String, T>, ValueBuilder<T>> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private static <T> ValueParser<Map<String, T>> mustNotBeEmpty(
      ValueParser<Map<String, T>> parser) {
    return new ValidatingValueParser<>(
        parser, l -> l.isEmpty() ? Optional.of("cannot be empty") : Optional.empty());
  }

  private static <T> ValueParser<Map<String, T>> mustNotContainEmptyStringAsKey(
      ValueParser<Map<String, T>> parser) {
    return new ValidatingValueParser<>(
        parser,
        l ->
            l.containsKey("")
                ? Optional.of("cannot contain the empty string as a key")
                : Optional.empty());
  }

  private static <T> ValueParser<Map<String, T>> validateKeys(
      ValueParser<Map<String, T>> parser, FieldValidator<String> keyValidator) {
    return (context, value) -> {
      Map<String, T> map = parser.parse(context, value);

      for (String field : map.keySet()) {
        Optional<String> optionalError = keyValidator.validate(field);
        if (optionalError.isEmpty()) {
          continue;
        }

        String error = optionalError.get();
        BsonParseContext childContext = context.child(field);
        childContext.handleSemanticError(error);
      }

      return map;
    };
  }

  private static class Encoder<T> implements ValueEncoder<Map<String, T>> {

    private final ValueEncoder<T> elementEncoder;

    Encoder(ValueEncoder<T> elementEncoder) {
      this.elementEncoder = elementEncoder;
    }

    @Override
    public BsonValue encode(Map<String, T> value) {
      List<BsonElement> elements =
          value.entrySet().stream()
              .map(
                  entry ->
                      new BsonElement(entry.getKey(), this.elementEncoder.encode(entry.getValue())))
              .collect(Collectors.toList());

      return new BsonDocument(elements);
    }
  }

  private static class Parser<T> implements ValueParser<Map<String, T>> {

    private final ValueParser<T> valueParser;
    private final Supplier<Map<String, T>> mapFactory;

    Parser(ValueParser<T> valueParser, Supplier<Map<String, T>> mapFactory) {
      this.valueParser = valueParser;
      this.mapFactory = mapFactory;
    }

    @Override
    public Map<String, T> parse(BsonParseContext context, BsonValue value)
        throws BsonParseException {
      if (value.getBsonType() != BsonType.DOCUMENT) {
        context.handleUnexpectedType(TypeDescription.DOCUMENT, value.getBsonType());
      }

      BsonDocument document = value.asDocument();
      Map<String, T> map = this.mapFactory.get();

      for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
        String field = entry.getKey();

        BsonParseContext childContext = context.child(field);
        T childValue = this.valueParser.parse(childContext, entry.getValue());

        map.put(field, childValue);
      }

      return map;
    }
  }
}
