package com.xgen.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.Point;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Synthetic {@link ComparisonOperator} that represents a fully-closed range. Produced only by
 * {@link com.xgen.mongot.index.query.optimizer.QueryOptimizer} from two open ranges such as {@code
 * $gt} + {@code $lt}. Never emitted by the user query parser.
 *
 * <p>If a single document contains array data then this operator is not valid. Translation in
 * {@code VectorSearchFilterQueryFactory} verifies the index actually supports this operator and
 * falls back to the equivalent AND-of-two-opens shape when it does not. The optimizer is therefore
 * allowed to be optimistic about data shape; correctness is enforced at the factory boundary.
 *
 * <p>{@link #toBson()} emits a {@code $$closedRange} sigil document so the operator is
 * unambiguously distinguishable in explain output and never round-trips through the user query
 * parser, which rejects {@code $$}-prefixed keys.
 */
public record ClosedRangeBoundComparisonOperator(RangeBound<? extends Comparable<?>> bounds)
    implements ComparisonOperator {

  /** Sigil key emitted by {@link #toBson()}. Never accepted as parser input. */
  public static final String SIGIL = "$$closedRange";

  public ClosedRangeBoundComparisonOperator {
    if (!(bounds instanceof NumericRangeBound || bounds instanceof DateRangeBound)) {
      throw new IllegalArgumentException(
          "closed range requires numeric or date bounds, got: "
              + bounds.getClass().getSimpleName());
    }
    if (bounds.getLower().isEmpty() || bounds.getUpper().isEmpty()) {
      throw new IllegalArgumentException(
          "closed range requires both lower and upper bounds, got: " + bounds);
    }
    checkEndpointOrdering(bounds);
  }

  /**
   * The endpoints must denote a non-empty closed interval. This mirrors the parse-path validation
   * in {@link RangeBound}, which direct {@link RangeBound} construction — the only way to build a
   * fully-closed bound outside the parser — does not run.
   */
  private static <P extends Point & Comparable<P>> void checkEndpointOrdering(
      RangeBound<P> bounds) {
    P lower = bounds.getLower().orElseThrow();
    P upper = bounds.getUpper().orElseThrow();
    int comparison = lower.compareTo(upper);
    if (comparison > 0) {
      throw new IllegalArgumentException("closed range requires lower <= upper, got: " + bounds);
    }
    if (comparison == 0 && !(bounds.lowerInclusive() && bounds.upperInclusive())) {
      // A NaN endpoint compares equal to values it is not actually equal to; such a false
      // equality between a NaN and a non-NaN endpoint is not an empty range.
      if (lower.equals(new DoublePoint(Double.NaN)) == upper.equals(new DoublePoint(Double.NaN))) {
        throw new IllegalArgumentException(
            "closed range with equal bounds requires both bounds inclusive, got: " + bounds);
      }
    }
  }

  @Override
  public Category getCategory() {
    return Category.CLOSED_RANGE;
  }

  @Override
  public ValueType getValueType() {
    return this.bounds.getType();
  }

  @Override
  public BsonValue operatorToBson() {
    return toSigilDocument();
  }

  @Override
  public BsonDocument toBson() {
    return new BsonDocument(SIGIL, toSigilDocument());
  }

  private BsonDocument toSigilDocument() {
    return new BsonDocument()
        .append("lower", ((Point) this.bounds.getLower().orElseThrow()).toBson())
        .append("lowerInclusive", BsonBoolean.valueOf(this.bounds.lowerInclusive()))
        .append("upper", ((Point) this.bounds.getUpper().orElseThrow()).toBson())
        .append("upperInclusive", BsonBoolean.valueOf(this.bounds.upperInclusive()));
  }
}
