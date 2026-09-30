package com.xgen.mongot.util.bson.parser;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.Enums;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Contains the TypeBuilders and ValueParser for built in enum parsing. */
public class EnumField {
  private static final Logger LOG = LoggerFactory.getLogger(EnumField.class);

  // Utility method to normalize name based on a common strategy
  private static String normalizeName(String value) {
    return value.toUpperCase(); // uniformly handle case insensitivity
  }

  /** A builder stage that forces a choice on style and case sensitivity of input values. */
  public static class FieldCaseSelector<T extends Enum<T>> {

    private final String name;
    private final Class<T> enumClass;
    private final Optional<T> fallbackValue;

    FieldCaseSelector(String name, Class<T> enumClass) {
      this.name = name;
      this.enumClass = enumClass;
      this.fallbackValue = Optional.empty();
    }

    FieldCaseSelector(String name, Class<T> enumClass, T fallback) {
      this.name = name;
      this.enumClass = enumClass;
      this.fallbackValue = Optional.ofNullable(fallback);
    }

    /**
     * Allows the EnumParser to return a fallback enum rather than throw an exception when parsing
     * fails.
     *
     * <p>This is helpful in the case that upstream enum changes (e.g., from MMS) are made while
     * this code is not yet aware of them.
     *
     * @param fallback the enum value to use if parsing fails
     * @return a new {@link FieldCaseSelector} with the fallback value configured
     */
    public FieldCaseSelector<T> withFallback(T fallback) {
      return new FieldCaseSelector<>(this.name, this.enumClass, fallback);
    }

    public FieldBuilder<T> asCamelCase() {
      return new FieldBuilder<>(
          this.name, this.enumClass, Optional.of(CaseFormat.LOWER_CAMEL), this.fallbackValue);
    }

    public FieldBuilder<T> asUpperCamelCase() {
      return new FieldBuilder<>(
          this.name, this.enumClass, Optional.of(CaseFormat.UPPER_CAMEL), this.fallbackValue);
    }

    public FieldBuilder<T> asUpperUnderscore() {
      return new FieldBuilder<>(
          this.name, this.enumClass, Optional.of(CaseFormat.UPPER_UNDERSCORE), this.fallbackValue);
    }

    public FieldBuilder<T> asCaseInsensitive() {
      return new FieldBuilder<>(this.name, this.enumClass, Optional.empty(), this.fallbackValue);
    }
  }

  /** A builder stage that forces a choice on style and case sensitivity of input values. */
  public static class ValueCaseSelector<T extends Enum<T>> {

    private final Class<T> enumClass;
    private Optional<T> fallbackValue = Optional.empty();

    ValueCaseSelector(Class<T> enumClass) {
      this.enumClass = enumClass;
    }

    ValueCaseSelector(Class<T> enumClass, T fallback) {
      this.enumClass = enumClass;
      this.fallbackValue = Optional.ofNullable(fallback);
    }

    /**
     * Allows the EnumParser to return a fallback enum rather than throw an exception when parsing
     * fails.
     *
     * <p>This is helpful in the case that upstream enum changes (e.g., from MMS) are made while
     * this code is not yet aware of them.
     *
     * @param fallback the enum value to use if parsing fails
     * @return a new {@link ValueCaseSelector} with the fallback value configured
     */
    public ValueCaseSelector<T> withFallback(T fallback) {
      return new ValueCaseSelector<>(this.enumClass, fallback);
    }

    public ValueBuilder<T> asCamelCase() {
      return new ValueBuilder<>(
          this.enumClass, Optional.of(CaseFormat.LOWER_CAMEL), this.fallbackValue);
    }

    public ValueBuilder<T> asUpperCamelCase() {
      return new ValueBuilder<>(
          this.enumClass, Optional.of(CaseFormat.UPPER_CAMEL), this.fallbackValue);
    }

    public ValueBuilder<T> asUpperUnderscore() {
      return new ValueBuilder<>(
          this.enumClass, Optional.of(CaseFormat.UPPER_UNDERSCORE), this.fallbackValue);
    }

    public ValueBuilder<T> asCaseInsensitive() {
      return new ValueBuilder<>(this.enumClass, Optional.empty(), this.fallbackValue);
    }
  }

  public static class FieldBuilder<T extends Enum<T>>
      extends Field.TypedBuilder<T, FieldBuilder<T>> {

    FieldBuilder(
        String name,
        Class<T> enumClass,
        Optional<CaseFormat> caseFormat,
        Optional<T> fallbackValue) {
      this(
          name,
          new Encoder<>(caseFormat),
          new EnumParser<>(enumClass, caseFormat, fallbackValue),
          Optional.empty());
    }

    FieldBuilder(String name, Class<T> enumClass, Optional<CaseFormat> caseFormat) {
      this(
          name,
          new Encoder<>(caseFormat),
          new EnumParser<>(enumClass, caseFormat, Optional.empty()),
          Optional.empty());
    }

    private FieldBuilder(
        String name,
        ValueEncoder<T> encoder,
        ValueParser<T> parser,
        Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    @Override
    BuilderFactory<T, FieldBuilder<T>> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder<T extends Enum<T>>
      extends Value.TypedBuilder<T, ValueBuilder<T>> {

    ValueBuilder(Class<T> enumClass, Optional<CaseFormat> caseFormat, Optional<T> fallbackValue) {
      this(new Encoder<>(caseFormat), new EnumParser<>(enumClass, caseFormat, fallbackValue));
    }

    private ValueBuilder(ValueEncoder<T> encoder, ValueParser<T> parser) {
      super(encoder, parser);
    }

    @Override
    BuilderFactory<T, ValueBuilder<T>> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }

  private record Encoder<T extends Enum<T>>(Optional<CaseFormat> caseFormat)
      implements ValueEncoder<T> {

    @Override
    public BsonValue encode(T value) {
      String name =
          this.caseFormat
              .map(cf -> Enums.convertNameTo(cf, value))
              .orElse(normalizeName(value.name()));
      return new BsonString(name);
    }
  }

  /** Parses enums from their lower camelcase equivalent. */
  private static class EnumParser<T extends Enum<T>> implements ValueParser<T> {

    private final Map<String, T> fromNames;
    private final boolean shouldNormalizeName;
    private final Optional<T> fallbackValue;

    private EnumParser(
        Class<T> enumClass, Optional<CaseFormat> caseFormat, Optional<T> fallbackValue) {
      this.shouldNormalizeName = caseFormat.isEmpty();
      this.fromNames =
          Arrays.stream(enumClass.getEnumConstants())
              .collect(
                  CollectionUtils.toUnmodifiableMapUnsafe(
                      m ->
                          caseFormat
                              .map(cf -> Enums.convertNameTo(cf, m))
                              .orElse(normalizeName(m.name())),
                      m -> m));
      this.fallbackValue = fallbackValue;
    }

    private static <E extends Enum<E>> boolean isDocumented(E value) {
      try {
        // Fields marked @Deprecated will not appear in error messages to avoid steering users
        // toward them
        return !value
            .getDeclaringClass()
            .getField(value.name())
            .isAnnotationPresent(Deprecated.class);
      } catch (NoSuchFieldException e) {
        LOG.atWarn()
            .addKeyValue("enum", value.getDeclaringClass().getSimpleName())
            .addKeyValue("value", value)
            .log("Cannot access value from enum definition");
        return false;
      }
    }

    @Override
    public T parse(BsonParseContext context, BsonValue value) throws BsonParseException {
      if (value.getBsonType() != BsonType.STRING) {
        context.handleUnexpectedType(TypeDescription.STRING, value.getBsonType());
      }

      String name = value.asString().getValue();
      String normalized = this.shouldNormalizeName ? normalizeName(name) : name;

      T result = this.fromNames.get(normalized);
      if (result == null) {
        if (this.fallbackValue.isPresent()) {
          return this.fallbackValue.get();
        }
        String names =
            this.fromNames.entrySet().stream()
                .filter(e -> isDocumented(e.getValue()))
                .map(Map.Entry::getKey)
                .sorted()
                .collect(Collectors.joining(", "));
        return context.handleSemanticError(String.format("must be one of [%s]", names));
      }

      return result;
    }
  }
}
