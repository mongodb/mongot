package com.xgen.mongot.util.bson.parser;

import static com.xgen.mongot.util.bson.parser.OptionalField.MISSING_VALUE;

import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BsonDocumentParser implements DocumentParser {

  public enum UnknownFieldBehavior {
    ALLOW,
    DISALLOW,
    WARN,
  }

  private static final Logger LOG = LoggerFactory.getLogger(BsonDocumentParser.class);

  private final BsonParseContext context;
  private final BsonDocument wrapped;
  private final UnknownFieldBehavior unknownFieldBehavior;
  private final Set<String> witnessedFields;

  BsonDocumentParser(
      BsonParseContext context, BsonDocument wrapped, UnknownFieldBehavior unknownFieldBehavior) {
    this.context = context;
    this.wrapped = wrapped;
    this.unknownFieldBehavior = unknownFieldBehavior;
    this.witnessedFields = new HashSet<>();
  }

  public static class Builder {

    private final BsonParseContext context;
    private final BsonDocument wrapped;
    private UnknownFieldBehavior unknownFieldBehavior;

    private Builder(BsonParseContext context, BsonDocument wrapped) {
      this.context = context;
      this.wrapped = wrapped;
      this.unknownFieldBehavior = UnknownFieldBehavior.DISALLOW;
    }

    public Builder allowUnknownFields(boolean allowUnknownFields) {
      this.unknownFieldBehavior =
          allowUnknownFields ? UnknownFieldBehavior.ALLOW : UnknownFieldBehavior.DISALLOW;
      return this;
    }

    public Builder warnOnUnknownFields() {
      this.unknownFieldBehavior = UnknownFieldBehavior.WARN;
      return this;
    }

    public BsonDocumentParser build() {
      return new BsonDocumentParser(this.context, this.wrapped, this.unknownFieldBehavior);
    }
  }

  public static Builder fromRoot(BsonDocument document) {
    return new Builder(BsonParseContext.root(), document);
  }

  public static Builder withContext(BsonParseContext context, BsonDocument wrapped) {
    return new Builder(context, wrapped);
  }

  @Override
  public <T> ParsedField.Required<T> getField(Field.Required<T> field) throws BsonParseException {
    String name = field.getName();
    T value = getFieldValue(name, field.getParser(), true);

    return new ParsedField.Required<>(field.getName(), value);
  }

  @Override
  public <T> ParsedField.Optional<T> getField(Field.Optional<T> field) throws BsonParseException {
    String name = field.getName();
    Optional<T> value = getFieldValue(name, field.getParser(), false);

    return new ParsedField.Optional<>(field.getName(), value);
  }

  @Override
  public <T> ParsedField.WithDefault<T> getField(Field.WithDefault<T> field)
      throws BsonParseException {
    String name = field.getName();
    Optional<T> value = getFieldValue(name, field.getParser(), false);

    return new ParsedField.WithDefault<>(name, value, field.getDefaultValue());
  }

  @Override
  public boolean hasField(Field.Optional<?> field) throws BsonParseException {
    String name = field.getName();
    BsonParseContext context = this.context.child(name);
    BsonValue value = getValue(name, context, false);
    return !value.equals(MISSING_VALUE);
  }

  private <T> T getFieldValue(String name, ValueParser<T> parser, boolean isRequired)
      throws BsonParseException {
    // Add the name to our list of witnessed fields.
    // There's no risk in adding a field name we don't actually see (e.g. an optional field that
    // doesn't exist) since we're just trying to find fields that are in the document but were not
    // requested.
    this.witnessedFields.add(name);

    BsonParseContext context = this.context.child(name);
    BsonValue value = getValue(name, context, isRequired);

    return parser.parse(context, value);
  }

  BsonValue getValue(String name, BsonParseContext context, boolean isRequired)
      throws BsonParseException {

    Optional<BsonValue> value = Optional.ofNullable(this.wrapped.get(name));

    if (value.isEmpty()) {
      // The field is missing. If it's required, we should throw, otherwise treat it as null so that
      // the ValueParser can handle everything in a uniform fashion.
      if (isRequired) {
        throw context.deserializationException("is required");
      }
      return MISSING_VALUE;
    }
    return value.get();
  }

  @Override
  public ParsedFieldGroup getGroup() {
    return new ParsedFieldGroup(this.context);
  }

  @Override
  public BsonParseContext getContext() {
    return this.context;
  }

  @Override
  public void close() throws BsonParseException {
    switch (this.unknownFieldBehavior) {
      case ALLOW -> {
        // No action needed if allowing unknown fields
      }
      case DISALLOW, WARN -> {
        Sets.SetView<String> fieldsNotRequested =
            Sets.difference(this.wrapped.keySet(), this.witnessedFields);

        boolean allFieldsRequested = fieldsNotRequested.isEmpty();
        if (allFieldsRequested) {
          return;
        }

        // If we reach this part of the method, there are some unknown (unconsumed) fields
        try {
          this.context.handleUnexpectedFields(fieldsNotRequested);
        } catch (BsonParseException e) {
          if (this.unknownFieldBehavior == UnknownFieldBehavior.DISALLOW) {
            throw e;
          }

          LOG.atWarn().setCause(e).log("Ignoring unknown fields found during parsing");
        }
      }
    }
  }
}
