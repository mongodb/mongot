package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
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
import java.util.List;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

/** Distributes queries to their appropriate query factory. */
class RangeQueryFactory {

  private final QueryFactoryContext queryFactoryContext;
  private final EqualsQueryFactory equalsQueryFactory;
  private final IndexCapabilities indexCapabilities;

  public RangeQueryFactory(
      QueryFactoryContext queryFactoryContext,
      EqualsQueryFactory equalsQueryFactory) {
    this.queryFactoryContext = queryFactoryContext;
    this.equalsQueryFactory = equalsQueryFactory;
    this.indexCapabilities = queryFactoryContext.getIndexCapabilities();
  }

  Query fromOperator(RangeOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    return fromBounds(operator.bounds(), operator.paths(), singleQueryContext);
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
              DateRangeQueryFactory.fromBounds(dateRangeBound),
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
