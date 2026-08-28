package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.lucene.query.range.RangeQueryUtils;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.bound.BooleanRangeBound;
import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.ObjectIdRangeBound;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.operators.bound.StringRangeBound;
import com.xgen.mongot.index.query.operators.bound.UuidRangeBound;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/** Distributes queries to their appropriate query factory. */
class RangeQueryFactory {

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private final QueryFactoryContext queryFactoryContext;
  private final EqualsQueryFactory equalsQueryFactory;
  private final IndexCapabilities indexCapabilities;
  private final DateRangeQueryFactory dateRangeQueryFactory;

  public RangeQueryFactory(
      QueryFactoryContext queryFactoryContext, EqualsQueryFactory equalsQueryFactory) {
    this(
        queryFactoryContext,
        equalsQueryFactory,
        new DateRangeQueryFactory(
            queryFactoryContext.getIndexCapabilities(), DynamicFeatureFlagRegistry.empty()));
  }

  public RangeQueryFactory(
      QueryFactoryContext queryFactoryContext,
      EqualsQueryFactory equalsQueryFactory,
      DateRangeQueryFactory dateRangeQueryFactory) {
    this.queryFactoryContext = queryFactoryContext;
    this.equalsQueryFactory = equalsQueryFactory;
    this.indexCapabilities = queryFactoryContext.getIndexCapabilities();
    this.dateRangeQueryFactory = dateRangeQueryFactory;
  }

  Query fromOperator(RangeOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    if (operator.fusedByOptimizer()) {
      return fromFusedBounds(operator.bounds(), operator.paths(), singleQueryContext);
    }
    return fromBounds(operator.bounds(), operator.paths(), singleQueryContext);
  }

  /**
   * Identical to fromBounds(), unless canEmitFusedClosedRange()/hasNoArrayData() fails, in which
   * case fallback is required, and we break the one closed range into two half-open ranges.
   */
  Query fromFusedBounds(
      RangeBound<?> bounds, List<FieldPath> queryPaths, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    if (canEmitFusedClosedRange(bounds, queryPaths, singleQueryContext)) {
      return fromBounds(bounds, queryPaths, singleQueryContext);
    }
    return new BooleanQuery.Builder()
        .add(
            fromBounds(lowerHalf(bounds), queryPaths, singleQueryContext), BooleanClause.Occur.MUST)
        .add(
            fromBounds(upperHalf(bounds), queryPaths, singleQueryContext), BooleanClause.Occur.MUST)
        .build();
  }

  private static boolean canEmitFusedClosedRange(
      RangeBound<?> bounds, List<FieldPath> queryPaths, SingleQueryContext singleQueryContext) {
    try {
      return RangeQueryUtils.hasNoArrayData(
          singleQueryContext.getIndexReader(),
          bounds,
          queryPaths,
          singleQueryContext.getEmbeddedRoot());
    } catch (IOException e) {
      FLOGGER.atWarning().atMostEvery(5, TimeUnit.MINUTES).withCause(e).log(
          "Falling back to AND-of-two-opens for fused closed-range translation; could not read "
              + "PointValues for paths=%s",
          queryPaths);
      return false;
    }
  }

  private static RangeBound<?> lowerHalf(RangeBound<?> bounds) {
    return switch (bounds) {
      case NumericRangeBound numeric ->
          new NumericRangeBound(
              numeric.getLower(), Optional.empty(), numeric.lowerInclusive(), false);
      case DateRangeBound date ->
          new DateRangeBound(date.getLower(), Optional.empty(), date.lowerInclusive(), false);
      // Unreachable: every optimizer fusion site emits only the two bound types above.
      case BooleanRangeBound ignored -> throw unsupportedFusedBoundType(bounds);
      case ObjectIdRangeBound ignored -> throw unsupportedFusedBoundType(bounds);
      case StringRangeBound ignored -> throw unsupportedFusedBoundType(bounds);
      case UuidRangeBound ignored -> throw unsupportedFusedBoundType(bounds);
    };
  }

  private static RangeBound<?> upperHalf(RangeBound<?> bounds) {
    return switch (bounds) {
      case NumericRangeBound numeric ->
          new NumericRangeBound(
              Optional.empty(), numeric.getUpper(), false, numeric.upperInclusive());
      case DateRangeBound date ->
          new DateRangeBound(Optional.empty(), date.getUpper(), false, date.upperInclusive());
      // Unreachable: every optimizer fusion site emits only the two bound types above.
      case BooleanRangeBound ignored -> throw unsupportedFusedBoundType(bounds);
      case ObjectIdRangeBound ignored -> throw unsupportedFusedBoundType(bounds);
      case StringRangeBound ignored -> throw unsupportedFusedBoundType(bounds);
      case UuidRangeBound ignored -> throw unsupportedFusedBoundType(bounds);
    };
  }

  private static IllegalStateException unsupportedFusedBoundType(RangeBound<?> bounds) {
    return new IllegalStateException(
        "unsupported fused bound type: " + bounds.getClass().getSimpleName());
  }

  Query fromBounds(
      RangeBound<?> bounds, List<FieldPath> queryPaths, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    checkState(
        bounds.getLower().isPresent() || bounds.getUpper().isPresent(),
        "RangeOperator::getUpper() or " + "RangeOperator::getLower() must be present");
    BooleanComposer.StreamUtils<FieldPath> paths = BooleanComposer.StreamUtils.from(queryPaths);

    return switch (bounds) {
      case DateRangeBound dateRangeBound ->
          paths.mapWithBoundSecondArgument(
              singleQueryContext.getEmbeddedRoot(),
              this.dateRangeQueryFactory.fromBounds(dateRangeBound),
              BooleanClause.Occur.SHOULD);
      case NumericRangeBound numericRangeBound ->
          paths.mapWithBoundSecondArgument(
              singleQueryContext.getEmbeddedRoot(),
              NumericRangeQueryFactory.fromBounds(numericRangeBound),
              BooleanClause.Occur.SHOULD);
      case ObjectIdRangeBound objectIdRangeBound ->
          paths.mapWithBoundSecondArgument(
              singleQueryContext.getEmbeddedRoot(),
              ObjectIdRangeQueryFactory.fromBounds(objectIdRangeBound, this.indexCapabilities),
              BooleanClause.Occur.SHOULD);
      case StringRangeBound stringRangeBound ->
          paths.mapWithBoundSecondArgument(
              singleQueryContext.getEmbeddedRoot(),
              new StringRangeQueryFactory(this.queryFactoryContext).fromBounds(stringRangeBound),
              BooleanClause.Occur.SHOULD);
      case BooleanRangeBound booleanRangeBound ->
          paths.mapWithBoundSecondArgument(
              singleQueryContext.getEmbeddedRoot(),
              new BooleanRangeQueryFactory(this.equalsQueryFactory).fromBounds(booleanRangeBound),
              BooleanClause.Occur.SHOULD);
      case UuidRangeBound uuidRangeBound ->
          paths.mapWithBoundSecondArgument(
              singleQueryContext.getEmbeddedRoot(),
              UuidRangeQueryFactory.fromBounds(uuidRangeBound),
              BooleanClause.Occur.SHOULD);
    };
  }
}
