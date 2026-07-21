package com.xgen.mongot.index.query.operators;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonValue;

public record EmbeddedDocumentOperator(Score score, FieldPath path, Operator operator)
    implements Operator {

  static class Fields {
    static final Field.Required<String> PATH =
        Field.builder("path").stringField().mustNotBeEmpty().required();
    static final Field.Required<Operator> OPERATOR =
        Field.builder("operator")
            .classField(Operator::exactlyOneFromBson)
            .disallowUnknownFields()
            .required();
    public static final Field.WithDefault<Score> SCORE =
        Field.builder("score")
            .classField(Score::fromBsonAllowEmbedded)
            .disallowUnknownFields()
            .optional()
            .withDefault(Score.defaultScore());
  }

  @Override
  public Type getType() {
    return Type.EMBEDDED_DOCUMENT;
  }

  static EmbeddedDocumentOperator fromBson(DocumentParser parser) throws BsonParseException {
    FieldPath path = FieldPath.parse(parser.getField(Fields.PATH).unwrap());
    Operator operator = parser.getField(Fields.OPERATOR).unwrap();
    // TODO(CLOUDP-327217): Move validation into OperatorValidator.
    validateOperatorPaths(path, operator, parser.getContext());
    return new EmbeddedDocumentOperator(parser.getField(Fields.SCORE).unwrap(), path, operator);
  }

  public static void validateOperatorPaths(
      FieldPath path, Operator operator, BsonParseContext context) throws BsonParseException {
    // TODO(CLOUDP-327217): delegate error reporting to OperatorEmbeddedRootValidator and remove
    // this method.
    ImmutableList<FieldPath> childPaths = Operators.getAdjacentChildPaths(operator);
    for (FieldPath child : childPaths) {
      if (!child.isChildOf(path)) {
        context.handleSemanticError(String.format("%s is not a subfield of %s", child, path));
      }
    }
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score())
        .field(Fields.PATH, this.path.toString())
        .field(Fields.OPERATOR, this.operator)
        .build();
  }
}
