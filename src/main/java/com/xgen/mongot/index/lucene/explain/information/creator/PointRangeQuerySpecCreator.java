package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.PointRangeQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.Representation;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Date;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PointRangeQuerySpecCreator {
  private static final Logger logger = LoggerFactory.getLogger(PointRangeQuerySpecCreator.class);

  static PointRangeQuerySpec fromQuery(org.apache.lucene.search.PointRangeQuery query) {
    String path = query.getField();
    Optional<Point.Type> pointType = typeFromPath(path);
    Representation representation = representationFromPath(path);

    long lower = org.apache.lucene.document.LongPoint.decodeDimension(query.getLowerPoint(), 0);
    long upper = org.apache.lucene.document.LongPoint.decodeDimension(query.getUpperPoint(), 0);

    Optional<Representation> representationToShow =
        pointType.map(t -> t == Point.Type.DATE).orElse(false)
            ? Optional.empty()
            : Optional.of(representation);

    return new PointRangeQuerySpec(
        FieldPath.parse(FieldName.stripAnyPrefixFromLuceneFieldName(path)),
        representationToShow,
        pointFrom(lower, representation, pointType, true),
        pointFrom(upper, representation, pointType, false));
  }

  private static Optional<Point> pointFrom(
      long value, Representation representation, Optional<Point.Type> pointType, boolean isLower) {
    if (pointType.isEmpty() || isOpenBound(value, representation, isLower)) {
      return Optional.empty();
    }

    return switch (pointType.get()) {
      case DATE -> Optional.of(new DatePoint(new Date(value)));
      case NUMBER ->
          switch (representation) {
            case INT_64 -> Optional.of(new LongPoint(value));
            case DOUBLE ->
                Optional.of(new DoublePoint(LuceneDoubleConversionUtils.fromLong(value)));
          };
      case GEO -> Optional.empty();
      case STRING, BOOLEAN, UUID, OBJECT_ID -> {
        logger.error("Unexpected IndexedType for numeric range: {}", pointType.get());
        yield Check.unreachable("Unexpected IndexedType for numeric range");
      }
    };
  }

  private static boolean isOpenBound(long value, Representation representation, boolean isLower) {
    if (isLower) {
      boolean isUnspecifiedIntLower =
          representation == Representation.INT_64 && value == Long.MIN_VALUE;
      boolean isUnspecifiedDoubleLower =
          representation == Representation.DOUBLE
              && value == LuceneDoubleConversionUtils.toLong(-1.0 * Double.MAX_VALUE);

      return isUnspecifiedIntLower || isUnspecifiedDoubleLower;
    }

    boolean isUnspecifiedIntUpper =
        representation == Representation.INT_64 && value == Long.MAX_VALUE;

    boolean isUnspecifiedDoubleUpper =
        representation == Representation.DOUBLE
            && value == LuceneDoubleConversionUtils.toLong(Double.MAX_VALUE);

    return isUnspecifiedIntUpper || isUnspecifiedDoubleUpper;
  }

  private static Optional<Point.Type> typeFromPath(String path) {
    if (FieldName.TypeField.NUMBER_DOUBLE
        .isType()
        .or(FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.isType())
        .or(FieldName.TypeField.NUMBER_INT64.isType())
        .or(FieldName.TypeField.NUMBER_INT64_MULTIPLE.isType())
        .test(path)) {
      return Optional.of(Point.Type.NUMBER);
    }
    if (FieldName.TypeField.GEO_SHAPE
        .isType()
        .or(FieldName.TypeField.GEO_POINT.isType())
        .test(path)) {
      return Optional.of(Point.Type.GEO);
    }
    if (FieldName.TypeField.DATE
        .isType()
        .or(FieldName.TypeField.DATE_MULTIPLE.isType())
        .or(FieldName.TypeField.DATE_V2.isType())
        .test(path)) {
      return Optional.of(Point.Type.DATE);
    }

    return Optional.empty();
  }

  private static Representation representationFromPath(String path) {
    // Double numbers are encoded in double precision; dates and longs use int64 precision encoding.
    if (FieldName.TypeField.NUMBER_DOUBLE.isTypeOf(path)
        || FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.isTypeOf(path)) {
      return Representation.DOUBLE;
    }

    return Representation.INT_64;
  }
}
