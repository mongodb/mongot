package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public record EqualsOperator(
    Score score, FieldPath path, Value value, Optional<List<String>> doesNotAffect)
    implements Operator {

  public static class Fields {
    public static final Field.Required<String> PATH =
        Field.builder("path").stringField().required();

    public static final Field.Required<Value> VALUE =
        Field.builder("value").classField(Value::fromBson).required();

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

  public boolean doesNotAffectDefined() {
    return this.doesNotAffect.isPresent() && !this.doesNotAffect.get().isEmpty();
  }

  @Override
  public Type getType() {
    return Type.EQUALS;
  }

  public static EqualsOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new EqualsOperator(
        Operators.parseScore(parser),
        FieldPath.parse(parser.getField(Fields.PATH).unwrap()),
        parser.getField(Fields.VALUE).unwrap(),
        parser.getField(Fields.DOES_NOT_AFFECT).unwrap());
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score())
        .field(Fields.PATH, this.path.toString())
        .field(Fields.VALUE, this.value)
        .field(Fields.DOES_NOT_AFFECT, this.doesNotAffect)
        .build();
  }
}
