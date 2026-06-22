package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonNumber;
import org.bson.BsonValue;

/**
 * A predicate which scores based on selected vector {@link VectorSimilarity} in the indexed vector
 * space.
 *
 * @param score score specification
 * @param paths path(s) to consider values at
 * @param vector query vector of the configured dimensionality
 * @param filter optional, can be used to pre-filter the scope of the vector search
 * @param k the number of nearest neighbors to return
 */
public record KnnBetaOperator(
    Score score, List<FieldPath> paths, List<BsonNumber> vector, Optional<Operator> filter, int k)
    implements Operator {
  public static class Fields {
    public static final Field.Required<List<BsonNumber>> VECTOR =
        Field.builder("vector")
            .listOf(Value.builder().bsonNumberField().required())
            .mustNotBeEmpty()
            .required();

    public static final Field.Optional<Operator> FILTER =
        Field.builder("filter")
            .classField(Operator::exactlyOneFromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Required<Integer> K =
        Field.builder("k").intField().mustBeWithinBounds(Range.of(1, Integer.MAX_VALUE)).required();
  }

  @Override
  public Type getType() {
    return Type.KNN_BETA;
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score())
        .field(Operators.Fields.FIELD_PATH, this.paths)
        .field(Fields.VECTOR, this.vector)
        .field(Fields.FILTER, this.filter)
        .field(Fields.K, this.k)
        .build();
  }

  public static KnnBetaOperator fromBson(DocumentParser parser) throws BsonParseException {

    if (parser.getContext().getHierarchy().size() > 1) {
      parser.getContext().handleSemanticError("knnBeta is not allowed to be nested");
    }

    return new KnnBetaOperator(
        Operators.parseScore(parser),
        parser.getField(Operators.Fields.FIELD_PATH).unwrap(),
        parser.getField(Fields.VECTOR).unwrap(),
        parser.getField(Fields.FILTER).unwrap(),
        parser.getField(Fields.K).unwrap());
  }
}
