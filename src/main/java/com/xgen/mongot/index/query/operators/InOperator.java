package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.type.NonNullValueType;
import com.xgen.mongot.index.query.operators.value.NonNullValue;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public record InOperator(
    Score score,
    List<FieldPath> paths,
    List<NonNullValue> values,
    Optional<List<String>> doesNotAffect)
    implements Operator {

  public static class Fields {

    public static final Field.Required<List<NonNullValue>> VALUE =
        Field.builder("value")
            .singleValueOrListOf(
                com.xgen.mongot.util.bson.parser.Value.builder()
                    .classValue(NonNullValue::fromBson)
                    .required())
            .mustNotBeEmpty()
            .validate(
                input ->
                    Value.containsDistinctType(input)
                        ? Optional.of("must have elements of the same type")
                        : Optional.empty())
            .required();

    public static final Field.Optional<List<String>> DOES_NOT_AFFECT =
        Field.builder("doesNotAffect")
            .singleValueOrListOf(
                com.xgen.mongot.util.bson.parser.Value.builder()
                    .stringValue()
                    .mustNotBeEmpty()
                    .required())
            .optional()
            .noDefault();
  }

  public InOperator(
      Score score,
      List<FieldPath> paths,
      List<NonNullValue> values,
      Optional<List<String>> doesNotAffect) {
    this.score = score;
    this.paths = paths;
    this.values = values;
    this.doesNotAffect = doesNotAffect;
    Check.argNotEmpty(values, "value");
  }

  public boolean doesNotAffectDefined() {
    return this.doesNotAffect.isPresent() && !this.doesNotAffect.get().isEmpty();
  }

  @Override
  public Type getType() {
    return Type.IN;
  }

  public NonNullValueType getValueType() {
    // array cannot be empty, values are of the same type
    return this.values.getFirst().getNonNullType();
  }

  public static InOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new InOperator(
        Operators.parseScore(parser),
        Operators.parseFieldPath(parser),
        parser.getField(Fields.VALUE).unwrap(),
        parser.getField(Fields.DOES_NOT_AFFECT).unwrap());
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score(), this.paths)
        .field(Fields.VALUE, this.values)
        .field(Fields.DOES_NOT_AFFECT, this.doesNotAffect)
        .build();
  }
}
