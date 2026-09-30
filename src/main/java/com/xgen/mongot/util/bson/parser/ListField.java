package com.xgen.mongot.util.bson.parser;

import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Range;
import org.bson.BsonArray;
import org.bson.BsonValue;

/** Contains the TypeBuilders and ValueParser for built in List parsing. */
public class ListField {

  enum SingleValuePresence {
    SINGLE_VALUE_ALLOWED,
    MUST_BE_LIST,
  }

  public static class FieldBuilder<T> extends Field.TypedBuilder<List<T>, FieldBuilder<T>> {
    private final Parser<T> listParser;

    FieldBuilder(
        String name,
        ValueEncoder<T> valueEncoder,
        ValueParser<T> elementParser,
        SingleValuePresence singleValuePresence,
        Optional<DeploymentEnvironment> edition) {
      this(
          name,
          new Encoder<>(valueEncoder, singleValuePresence),
          new Parser<>(elementParser, singleValuePresence),
          edition);
    }

    private FieldBuilder(
        String name,
        ValueEncoder<List<T>> encoder,
        Parser<T> parser,
        Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
      this.listParser = parser;
    }

    private FieldBuilder(
        String name,
        ValueEncoder<List<T>> encoder,
        ValueParser<List<T>> wrappedParser,
        Parser<T> baseListParser,
        Optional<DeploymentEnvironment> edition) {
      super(name, encoder, wrappedParser, edition);
      this.listParser = baseListParser;
    }

    /**
     * Configures the FieldBuilder to skip elements that cannot be parsed successfully.
     *
     * <p>When this is enabled, if an element in the BSON array fails to parse, it will be omitted
     * from the resulting list, and parsing will continue with the next element. If not enabled, a
     * parsing failure on any element will cause an exception to be thrown for the entire list field
     * at top level.
     *
     * @return a new {@link FieldBuilder} with the skip-invalid-elements behavior enabled.
     */
    public FieldBuilder<T> skipInvalidElements() {
      this.listParser.skipInvalidElements();
      return new FieldBuilder<>(
          this.name, this.encoder, this.parser, this.listParser, this.edition);
    }

    public FieldBuilder<T> mustNotBeEmpty() {
      ValueParser<List<T>> parser = ListField.mustNotBeEmpty(this.parser);
      return new FieldBuilder<>(this.name, this.encoder, parser, this.listParser, this.edition);
    }

    public FieldBuilder<T> mustBeUnique() {
      ValueParser<List<T>> parser = ListField.mustBeUnique(this.parser);
      return new FieldBuilder<>(this.name, this.encoder, parser, this.listParser, this.edition);
    }

    /**
     * Ensure R-typed attributes of T-typed list elements are unique. Extract attributes via
     * attributeGetter.
     */
    public <R> FieldBuilder<T> mustHaveUniqueAttribute(
        String attributeName, Function<T, R> attributeGetter) {
      ValueParser<List<T>> parser =
          ListField.mustHaveUniqueAttribute(this.parser, attributeName, attributeGetter);
      return new FieldBuilder<>(this.name, this.encoder, parser, this.listParser, this.edition);
    }

    public FieldBuilder<T> sizeMustBeWithinBounds(Range<Integer> bounds) {
      ValueParser<List<T>> parser = ListField.sizeMustBeWithinBounds(this.parser, bounds);
      return new FieldBuilder<>(this.name, this.encoder, parser, this.listParser, this.edition);
    }

    @Override
    BuilderFactory<List<T>, FieldBuilder<T>> getBuilderFactory() {
      return (name, encoder, parser, edition) ->
          new FieldBuilder<>(name, encoder, parser, this.listParser, edition);
    }
  }

  public static class ValueBuilder<T> extends Value.TypedBuilder<List<T>, ValueBuilder<T>> {

    ValueBuilder(
        ValueEncoder<T> elementEncoder,
        ValueParser<T> elementParser,
        SingleValuePresence singleValuePresence) {
      this(
          new Encoder<>(elementEncoder, singleValuePresence),
          new Parser<>(elementParser, singleValuePresence));
    }

    private ValueBuilder(ValueEncoder<List<T>> encoder, ValueParser<List<T>> parser) {
      super(encoder, parser);
    }

    public ValueBuilder<T> mustNotBeEmpty() {
      ValueParser<List<T>> parser = ListField.mustNotBeEmpty(this.parser);
      return new ValueBuilder<>(this.encoder, parser);
    }

    public ValueBuilder<T> mustBeUnique() {
      ValueParser<List<T>> parser = ListField.mustBeUnique(this.parser);
      return new ValueBuilder<>(this.encoder, parser);
    }

    @Override
    BuilderFactory<List<T>, ValueBuilder<T>> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private static <T> ValueParser<List<T>> sizeMustBeWithinBounds(
      ValueParser<List<T>> parser, Range<Integer> bounds) {
    return new ValidatingValueParser<>(
        parser,
        l ->
            !bounds.contains(l.size())
                ? Optional.of("size must be within bounds: " + bounds)
                : Optional.empty());
  }

  private static <T> ValueParser<List<T>> mustNotBeEmpty(ValueParser<List<T>> parser) {
    return new ValidatingValueParser<>(
        parser, l -> l.isEmpty() ? Optional.of("cannot be empty") : Optional.empty());
  }

  private static <T> ValueParser<List<T>> mustBeUnique(ValueParser<List<T>> parser) {
    return new ValidatingValueParser<>(
        parser,
        l ->
            Sets.newHashSet(l).size() == l.size()
                ? Optional.empty()
                : Optional.of("cannot contain duplicate elements"));
  }

  private static <T, R> ValueParser<List<T>> mustHaveUniqueAttribute(
      ValueParser<List<T>> parser, String attributeName, Function<T, R> getter) {
    return new ValidatingValueParser<>(
        parser,
        l ->
            l.stream().map(getter).collect(Collectors.toSet()).size() == l.size()
                ? Optional.empty()
                : Optional.of(
                    String.format("cannot contain elements with the same %s", attributeName)));
  }

  private record Encoder<T>(ValueEncoder<T> elementEncoder, SingleValuePresence singleValuePresence)
      implements ValueEncoder<List<T>> {

    @Override
    public BsonValue encode(List<T> value) {
      if (this.singleValuePresence == SingleValuePresence.SINGLE_VALUE_ALLOWED
          && value.size() == 1) {
        return this.elementEncoder.encode(value.get(0));
      }

      List<BsonValue> values =
          value.stream().map(this.elementEncoder::encode).collect(Collectors.toList());
      return new BsonArray(values);
    }
  }

  static class Parser<T> implements ValueParser<List<T>> {

    private final ValueParser<T> elementParser;
    private final SingleValuePresence singleValuePresence;
    private boolean skipInvalidElements = false;

    Parser(ValueParser<T> elementParser, SingleValuePresence singleValuePresence) {
      this.elementParser = elementParser;
      this.singleValuePresence = singleValuePresence;
    }

    /**
     * Sets `skipInvalidElements`. When it's set, parser will skip a problematic element instead of
     * throwing an exception. This is helpful when user is providing invalid input (e.g. unknown
     * elements). Note that this will take effect at top level, meaning, we will not partially
     * preserve valid elements in deeply nested lists when the parents themselves are problematic in
     * the upper level.
     */
    public void skipInvalidElements() {
      this.skipInvalidElements = true;
    }

    @Override
    public List<T> parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      switch (value.getBsonType()) {
        case ARRAY -> {
          // beware of using BsonArray::get or other random access methods.
          // It could be a RawBsonArray which will deserialize all the array up to index
          BsonArray array = value.asArray();
          List<T> result = new ArrayList<>();
          @Var int idx = 0;
          for (BsonValue element : array) {
            BsonParseContext elementContext = context.arrayElement(idx);
            try {
              result.add(this.elementParser.parse(elementContext, element));
            } catch (BsonParseException e) {
              if (!this.skipInvalidElements) {
                throw e;
              }
            }
            idx++;
          }
          return result;
        }
        default -> {
          if (this.singleValuePresence == SingleValuePresence.MUST_BE_LIST) {
            context.handleUnexpectedType(TypeDescription.ARRAY, value.getBsonType());
          }

          return Collections.singletonList(this.elementParser.parse(context, value));
        }
      }
    }
  }
}
