package com.xgen.mongot.index.lucene.query.pushdown.match;

import static java.util.stream.Collectors.toList;

import com.google.errorprone.annotations.ThreadSafe;
import com.xgen.mongot.index.lucene.query.pushdown.MqlComparator;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.operators.mql.AndClause;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.ClosedRangeBoundComparisonOperator;
import com.xgen.mongot.index.query.operators.mql.EqOperator;
import com.xgen.mongot.index.query.operators.mql.ExistsOperator;
import com.xgen.mongot.index.query.operators.mql.GtOperator;
import com.xgen.mongot.index.query.operators.mql.GteOperator;
import com.xgen.mongot.index.query.operators.mql.InOperator;
import com.xgen.mongot.index.query.operators.mql.LtOperator;
import com.xgen.mongot.index.query.operators.mql.LteOperator;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.index.query.operators.mql.NeOperator;
import com.xgen.mongot.index.query.operators.mql.NinOperator;
import com.xgen.mongot.index.query.operators.mql.NorClause;
import com.xgen.mongot.index.query.operators.mql.NotOperator;
import com.xgen.mongot.index.query.operators.mql.OrClause;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.util.FieldPath;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * This class creates a Predicate that tests a {@link RawBsonDocument} against a {@link Clause}
 * using semantics identical to the $match stage in the aggregation pipeline. This will be used in
 * the fallback path for pushdown filters. <br>
 * <br>
 * Whereas {@link DocumentMatcher} can evaluate a single predicate against a single {@link
 * FieldPath}, this class will construct a compound {@link Predicate} referring to multiple fields,
 * and will evaluate the boolean expression with short-circuiting. The document may be traversed
 * multiple times, but it only needs to be read from disk once.
 */
@ThreadSafe
class MatchStage implements Predicate<RawBsonDocument> {

  private static final Comparator<BsonValue> CMP = MqlComparator.LEXICOGRAPHIC_COMPARATOR;

  private final Predicate<RawBsonDocument> predicate;

  /**
   * Builds a Document Predicate from a {@link Clause}. This may be an expensive operation so that
   * returned MatchStage should be built once per query and re-used for each document hit.
   */
  public static MatchStage build(Clause clause) {
    // TODO(CLOUDP-215531): Decouple representation from public VectorSearch API
    return new MatchStage(toPredicate(clause));
  }

  public MatchStage(Predicate<RawBsonDocument> predicate) {
    this.predicate = predicate;
  }

  public boolean test(BytesRef bytes) {
    // TODO(CLOUDP-280897): If the number of clauses is large, consider eagerly materializing
    // document into the LinkedHashMap format if we can handle repeated keys correctly.
    return test(new RawBsonDocument(bytes.bytes, bytes.offset, bytes.length));
  }

  @Override
  public boolean test(RawBsonDocument document) {
    return this.predicate.test(document);
  }

  private static boolean isSameTypeBracket(BsonValue left, BsonValue right) {
    BsonType ltype = left.getBsonType();
    BsonType rtype = right.getBsonType();
    return MqlComparator.getBracketPriority(ltype) == MqlComparator.getBracketPriority(rtype);
  }

  private static Predicate<RawBsonDocument> toPredicate(MqlFilterOperator op, FieldPath path) {
    BsonValue target;
    List<BsonValue> targets;
    return switch (op) {
      case GtOperator gtOperator -> {
        target = gtOperator.getValue().toBson();
        yield d ->
            DocumentMatcher.matches(
                d, v -> isSameTypeBracket(v, target) && CMP.compare(v, target) > 0, path);
      }
      case GteOperator gteOperator -> {
        target = gteOperator.getValue().toBson();
        yield d ->
            DocumentMatcher.matches(
                d, v -> isSameTypeBracket(v, target) && CMP.compare(v, target) >= 0, path);
      }
      case LtOperator ltOperator -> {
        target = ltOperator.getValue().toBson();
        yield d ->
            DocumentMatcher.matches(
                d, v -> isSameTypeBracket(v, target) && CMP.compare(v, target) < 0, path);
      }
      case LteOperator lteOperator -> {
        target = lteOperator.getValue().toBson();
        yield d ->
            DocumentMatcher.matches(
                d, v -> isSameTypeBracket(v, target) && CMP.compare(v, target) <= 0, path);
      }
      case EqOperator eqOperator -> {
        target = eqOperator.value().toBson();
        yield d -> DocumentMatcher.matches(d, v -> CMP.compare(v, target) == 0, path);
      }
      case NeOperator neOperator -> {
        target = neOperator.value().toBson();
        yield d -> DocumentMatcher.matches(d, v -> CMP.compare(v, target) != 0, path);
      }
      case InOperator inOperator -> {
        targets = inOperator.values().stream().map(Value::toBson).collect(toList());
        yield d ->
            DocumentMatcher.matches(
                d, v -> targets.stream().anyMatch(t -> CMP.compare(v, t) == 0), path);
      }
      case NinOperator ninOperator -> {
        targets = ninOperator.values().stream().map(Value::toBson).collect(toList());
        yield d ->
            DocumentMatcher.matches(
                d, v -> targets.stream().noneMatch(t -> CMP.compare(v, t) == 0), path);
      }
      case NotOperator notOperator ->
          notOperator.negateValues().mqlFilterOperators().stream()
              .map(operator -> toPredicate(operator, path))
              .reduce(Predicate::and)
              .orElseThrow();

      case ExistsOperator existsOperator ->
          d ->
              DocumentMatcher.matches(
                  d, (value) -> existsOperator.value() == !value.isNull(), path);
      case ClosedRangeBoundComparisonOperator closedRange ->
          closedRangePredicate(closedRange, path);
    };
  }

  private static Predicate<RawBsonDocument> toPredicate(Clause clause) {
    return switch (clause) {
      case SimpleClause simpleClause ->
          simpleClause.mqlFilterOperators().stream()
              .map(op -> toPredicate(op, simpleClause.path()))
              .reduce(Predicate::and)
              .orElseThrow();
      case OrClause orClause ->
          orClause.getClauses().stream()
              .map(MatchStage::toPredicate)
              .reduce(Predicate::or)
              .orElseThrow();
      case AndClause andClause ->
          andClause.getClauses().stream()
              .map(MatchStage::toPredicate)
              .reduce(Predicate::and)
              .orElseThrow();
      case NorClause norClause ->
          norClause.getClauses().stream()
              .map(MatchStage::toPredicate)
              .reduce(Predicate::or)
              .orElseThrow()
              .negate();
    };
  }

  private static Predicate<RawBsonDocument> closedRangePredicate(
      ClosedRangeBoundComparisonOperator closedRange, FieldPath path) {
    RangeBound<? extends Comparable<?>> bounds = closedRange.bounds();
    // Both endpoints are present: the ClosedRangeBoundComparisonOperator constructor enforces it.
    BsonValue lower = ((Point) bounds.getLower().orElseThrow()).toBson();
    BsonValue upper = ((Point) bounds.getUpper().orElseThrow()).toBson();
    boolean lowerInclusive = bounds.lowerInclusive();
    boolean upperInclusive = bounds.upperInclusive();
    Predicate<RawBsonDocument> lowerPredicate =
        d ->
            DocumentMatcher.matches(
                d,
                v ->
                    isSameTypeBracket(v, lower)
                        && (lowerInclusive
                            ? CMP.compare(v, lower) >= 0
                            : CMP.compare(v, lower) > 0),
                path);
    Predicate<RawBsonDocument> upperPredicate =
        d ->
            DocumentMatcher.matches(
                d,
                v ->
                    isSameTypeBracket(v, upper)
                        && (upperInclusive
                            ? CMP.compare(v, upper) <= 0
                            : CMP.compare(v, upper) < 0),
                path);
    return lowerPredicate.and(upperPredicate);
  }
}
