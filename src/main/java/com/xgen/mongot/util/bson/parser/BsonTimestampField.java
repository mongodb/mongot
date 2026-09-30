package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.Optional;
import org.bson.BsonTimestamp;

public class BsonTimestampField {
  private static final ValueEncoder<BsonTimestamp> ENCODER = x -> x;
  private static final ValueParser<BsonTimestamp> PARSER =
      (context, val) -> {
        if (!val.isTimestamp()) {
          context.handleUnexpectedType(TypeDescription.TIMESTAMP, val.getBsonType());
        }
        return val.asTimestamp();
      };

  public static class FieldBuilder
      extends Field.TypedBuilder<BsonTimestamp, BsonTimestampField.FieldBuilder> {

    FieldBuilder(String name) {
      this(name, ENCODER, PARSER, Optional.empty());
    }

    private FieldBuilder(
        String name,
        ValueEncoder<BsonTimestamp> encoder,
        ValueParser<BsonTimestamp> parser,
        Optional<DeploymentEnvironment> edition) {
      super(name, encoder, parser, edition);
    }

    @Override
    BuilderFactory<BsonTimestamp, BsonTimestampField.FieldBuilder> getBuilderFactory() {
      return BsonTimestampField.FieldBuilder::new;
    }
  }

  public static class ValueBuilder
      extends Value.TypedBuilder<BsonTimestamp, BsonTimestampField.ValueBuilder> {

    ValueBuilder() {
      this(ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<BsonTimestamp> encoder, ValueParser<BsonTimestamp> parser) {
      super(encoder, parser);
    }

    @Override
    BuilderFactory<BsonTimestamp, BsonTimestampField.ValueBuilder> getBuilderFactory() {
      return BsonTimestampField.ValueBuilder::new;
    }
  }
}
