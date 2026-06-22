package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.ObjectIdRangeBound;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.operators.bound.StringRangeBound;
import com.xgen.mongot.index.query.operators.bound.UuidRangeBound;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.NumericPoint;
import com.xgen.mongot.index.query.points.ObjectIdPoint;
import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.index.query.points.StringPoint;
import com.xgen.mongot.index.query.points.UuidPoint;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.Optionals;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonValue;

/**
 * Predicate that filters values at path within specified bounds. Bounds may be inclusive or
 * exclusive.
 *
 * @param score score specification.
 * @param paths path(s) for this predicate.
 * @param bounds bounds of this range.
 */
public record RangeOperator(
    Score score, List<FieldPath> paths, RangeBound<?> bounds, Optional<List<String>> doesNotAffect)
    implements Operator {

  public static class Fields {
    private static Field.Optional<Point> pointField(String name) {
      return Field.builder(name).classField(Point::fromBson).optional().noDefault();
    }

    public static final Field.Optional<Point> LT = pointField("lt");
    public static final Field.Optional<Point> LTE = pointField("lte");
    public static final Field.Optional<Point> GT = pointField("gt");
    public static final Field.Optional<Point> GTE = pointField("gte");

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
    return Type.RANGE;
  }

  /** Deserializes a RangeOperator from the supplied DocumentParser. */
  public static RangeOperator fromBson(DocumentParser parser) throws BsonParseException {
    var lt = parser.getField(Fields.LT);
    var lte = parser.getField(Fields.LTE);
    var gt = parser.getField(Fields.GT);
    var gte = parser.getField(Fields.GTE);

    var lower = parser.getGroup().atMostOneOf(gt, gte);
    var upper = parser.getGroup().atMostOneOf(lt, lte);
    var lowerInclusive = gte.unwrap().isPresent();
    var upperInclusive = lte.unwrap().isPresent();

    var bounds =
        boundsFromDefinition(parser.getContext(), lower, upper, lowerInclusive, upperInclusive);

    return new RangeOperator(
        Operators.parseScore(parser),
        Operators.parseFieldPath(parser),
        bounds,
        parser.getField(Fields.DOES_NOT_AFFECT).unwrap());
  }

  @Override
  public BsonValue operatorToBson() {
    BsonDocumentBuilder builder = Operators.documentBuilder(score(), this.paths);

    Optional<Point> lower = this.bounds.getLower().map(p -> p);
    Optional<Point> upper = this.bounds.getUpper().map(p -> p);

    var lowerField = this.bounds.lowerInclusive() ? Fields.GTE : Fields.GT;
    var upperField = this.bounds.upperInclusive() ? Fields.LTE : Fields.LT;

    return builder
        .field(lowerField, lower)
        .field(upperField, upper)
        .field(Fields.DOES_NOT_AFFECT, this.doesNotAffect)
        .build();
  }

  private static RangeBound<?> boundsFromDefinition(
      BsonParseContext context,
      Optional<Point> lower,
      Optional<Point> upper,
      boolean lowerInclusive,
      boolean upperInclusive)
      throws BsonParseException {
    Set<Point.Type> pointTypes =
        Optionals.present(Stream.of(lower, upper))
            .map(Point::getType)
            .collect(Collectors.toUnmodifiableSet());

    if (lower.isEmpty() && upper.isEmpty()) {
      context.handleSemanticError("must specify at least one of [lt, lte, gt, gte]");
    }

    if (pointTypes.size() != 1) {
      context.handleSemanticError("bounds must be of the same type");
    }

    Point.Type pointType = pointTypes.iterator().next();
    return switch (pointType) {
      case DATE ->
          DateRangeBound.create(
              context,
              lower.map(p -> (DatePoint) p),
              upper.map(p -> (DatePoint) p),
              lowerInclusive,
              upperInclusive);
      case NUMBER ->
          NumericRangeBound.create(
              context,
              lower.map(p -> (NumericPoint) p),
              upper.map(p -> (NumericPoint) p),
              lowerInclusive,
              upperInclusive);
      case OBJECT_ID ->
          ObjectIdRangeBound.create(
              context,
              lower.map(p -> (ObjectIdPoint) p),
              upper.map(p -> (ObjectIdPoint) p),
              lowerInclusive,
              upperInclusive);
      case STRING ->
          StringRangeBound.create(
              context,
              lower.map(p -> (StringPoint) p),
              upper.map(p -> (StringPoint) p),
              lowerInclusive,
              upperInclusive);
      case UUID ->
          UuidRangeBound.create(
              context,
              lower.map(p -> (UuidPoint) p),
              upper.map(p -> (UuidPoint) p),
              lowerInclusive,
              upperInclusive);
      case BOOLEAN ->
          context.handleSemanticError(
              String.format("range operator does not support %s point type", Point.Type.BOOLEAN));
      case GEO ->
          context.handleSemanticError(
              String.format("range operator does not support %s point type", Point.Type.GEO));
    };
  }
}
