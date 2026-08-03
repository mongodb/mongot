package com.xgen.mongot.index.lucene.query;

import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.range.RangeQueryUtils;
import com.xgen.mongot.index.lucene.query.util.MappingCompatibilityValidator;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.bound.BooleanRangeBound;
import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.ObjectIdRangeBound;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.operators.bound.StringRangeBound;
import com.xgen.mongot.index.query.operators.bound.UuidRangeBound;
import com.xgen.mongot.index.query.operators.mql.AndClause;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.ClosedRangeBoundComparisonOperator;
import com.xgen.mongot.index.query.operators.mql.ComparisonOperator;
import com.xgen.mongot.index.query.operators.mql.CompoundClause;
import com.xgen.mongot.index.query.operators.mql.EqOperator;
import com.xgen.mongot.index.query.operators.mql.ExistsOperator;
import com.xgen.mongot.index.query.operators.mql.InOperator;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.index.query.operators.mql.NeOperator;
import com.xgen.mongot.index.query.operators.mql.NinOperator;
import com.xgen.mongot.index.query.operators.mql.NorClause;
import com.xgen.mongot.index.query.operators.mql.NotOperator;
import com.xgen.mongot.index.query.operators.mql.OpenRangeBoundComparisonOperator;
import com.xgen.mongot.index.query.operators.mql.OrClause;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.index.query.optimizer.QueryOptimizer;
import com.xgen.mongot.index.query.optimizer.rules.DefaultRuleRegistry;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

public class VectorSearchFilterQueryFactory {

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private final QueryFactoryContext factoryContext;
  private final RangeQueryFactory rangeQueryFactory;
  private final InQueryFactory inQueryFactory;
  private final ExistsQueryFactory existsQueryFactory;
  private final EqualsQueryFactory equalsQueryFactory;
  private final QueryOptimizer queryOptimizer;

  public static VectorSearchFilterQueryFactory create(VectorQueryFactoryContext factoryContext) {
    return create(factoryContext, DynamicFeatureFlagRegistry.empty());
  }

  public static VectorSearchFilterQueryFactory create(
      VectorQueryFactoryContext factoryContext,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    var dateRangeQueryFactory =
        new DateRangeQueryFactory(
            factoryContext.getIndexCapabilities(), dynamicFeatureFlagRegistry);
    var equalsQueryFactory = new EqualsQueryFactory(factoryContext, dateRangeQueryFactory);
    var existsQueryFactory = new ExistsQueryFactory(factoryContext);
    var rangeQueryFactory =
        new RangeQueryFactory(factoryContext, equalsQueryFactory, dateRangeQueryFactory);
    var inQueryFactory = new InQueryFactory(factoryContext, dynamicFeatureFlagRegistry);
    var queryOptimizer =
        new QueryOptimizer(DefaultRuleRegistry.get(), factoryContext, dynamicFeatureFlagRegistry);
    return new VectorSearchFilterQueryFactory(
        factoryContext,
        rangeQueryFactory,
        inQueryFactory,
        existsQueryFactory,
        equalsQueryFactory,
        queryOptimizer);
  }

  VectorSearchFilterQueryFactory(
      QueryFactoryContext factoryContext,
      RangeQueryFactory rangeQueryFactory,
      InQueryFactory inQueryFactory,
      ExistsQueryFactory existsQueryFactory,
      EqualsQueryFactory equalsQueryFactory,
      QueryOptimizer queryOptimizer) {
    this.factoryContext = factoryContext;
    this.rangeQueryFactory = rangeQueryFactory;
    this.inQueryFactory = inQueryFactory;
    this.existsQueryFactory = existsQueryFactory;
    this.equalsQueryFactory = equalsQueryFactory;
    this.queryOptimizer = queryOptimizer;
  }

  /**
   * Converts a {@link VectorSearchFilter} into a Lucene {@link Query} suitable for execution
   * against the given index reader.
   */
  public Query createLuceneFilter(VectorSearchFilter filter, IndexReader indexReader)
      throws InvalidQueryException, IOException {
    return createLuceneFilter(filter, SingleQueryContext.createQueryRoot(indexReader));
  }

  Query createLuceneFilter(VectorSearchFilter filter, SingleQueryContext singleQueryContext)
      throws InvalidQueryException, IOException {
    VectorSearchFilter optimized =
        this.queryOptimizer.optimize(filter, singleQueryContext.getIndexReader()).tree();
    return switch (optimized) {
      case VectorSearchFilter.ClauseFilter clauseFilter ->
          createLuceneFilterFromClause(clauseFilter.clause(), singleQueryContext);
      case VectorSearchFilter.OperatorFilter operatorFilter ->
          createLuceneFilterFromOperator(operatorFilter.operator(), singleQueryContext);
    };
  }

  protected Query createLuceneFilterFromOperator(
      Operator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException, IOException {
    throw new UnsupportedOperationException(
        "Regular VectorSearchFilterQueryFactory cannot create filter query from operator");
  }

  protected Query createLuceneFilterFromClause(Clause clause, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    return switch (clause) {
      case CompoundClause compoundClause ->
          createLuceneFilterForCompoundClause(compoundClause, singleQueryContext);
      case SimpleClause simpleClause ->
          createLuceneFilterForSimpleClause(simpleClause, singleQueryContext);
    };
  }

  // No overloaded class names are used here to avoid the confusions of resolving to Clause or to
  // its subclasses.
  private Query createLuceneFilterForCompoundClause(
      CompoundClause compoundClause, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    List<Clause> clauses = compoundClause.getClauses();

    // If the clauses are empty, it must be an implicit AndClause (guaranteed by the constructors'
    // assertions). Implicit AndClause with an empty filter list matches all documents. We don't
    // need BooleanClause.Occur.FILTER here because this returned query will be used as the filter
    // in the vector query, so no scoring will be done anyway.
    if (clauses.isEmpty()) {
      if (this.factoryContext.isIndexWithEmbeddedFields()) {
        // If the index contains embedded documents, use a parent filter to prevent matching
        // unrelated embedded documents.
        return EmbeddedDocumentQueryFactory.parentFilter(singleQueryContext.getEmbeddedRoot());
      } else {
        return new MatchAllDocsQuery();
      }
    }
    var builder = new BooleanQuery.Builder();
    if (compoundClause.getOperator() == CompoundClause.Operator.NOR) {
      // if the NOR operator is used, we call MatchAllDocsQuery to get all the documents first
      // before executing a standalone negative query such as NOR since Lucene starts with no
      // documents matched initially
      builder.add(new BooleanClause(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD));
    }
    BooleanClause.Occur occur = toOccur(compoundClause);
    for (Clause clause : clauses) {
      builder.add(createLuceneFilterFromClause(clause, singleQueryContext), occur);
    }
    return builder.build();
  }

  private static BooleanClause.Occur toOccur(CompoundClause clause) {
    return switch (clause) {
      case AndClause andClause -> BooleanClause.Occur.MUST;
      case OrClause orClause -> BooleanClause.Occur.SHOULD;
      case NorClause norClause -> BooleanClause.Occur.MUST_NOT;
    };
  }

  private Query createLuceneFilterForSimpleClause(
      SimpleClause simpleClause, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    var builder = new BooleanQuery.Builder();
    for (MqlFilterOperator op : simpleClause.mqlFilterOperators()) {
      builder.add(
          createLuceneFilterForField(simpleClause.path(), op, singleQueryContext),
          BooleanClause.Occur.MUST);
    }
    return builder.build();
  }

  private Query createLuceneFilterForField(
      FieldPath path, MqlFilterOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {

    // Validate that auto-embedding fields are not used in filters
    if (this.factoryContext instanceof VectorQueryFactoryContext vectorContext) {
      if (vectorContext.isAutoEmbedField(path)) {
        throw new InvalidQueryException(
            String.format(
                "Field '%s' is an auto-embedding field and cannot be used in filters. "
                    + "Please use a separate field for filtering.",
                path));
      }
    }

    if (operator instanceof ComparisonOperator comparisonOperator) {
      MappingCompatibilityValidator.validate(
          path,
          singleQueryContext.getEmbeddedRoot(),
          this.factoryContext.getQueryTimeMappingChecks(),
          comparisonOperator.getValueType());
    } else if (operator instanceof ExistsOperator) {
      MappingCompatibilityValidator.validateExists(
          path,
          singleQueryContext.getEmbeddedRoot(),
          this.factoryContext.getQueryTimeMappingChecks());
    }

    return switch (operator) {
      case OpenRangeBoundComparisonOperator openRangeBoundComparisonOperator ->
          this.rangeQueryFactory.fromBounds(
              openRangeBoundComparisonOperator.getBounds(), List.of(path), singleQueryContext);
      case ClosedRangeBoundComparisonOperator closedRangeBoundComparisonOperator ->
          createLuceneFilterForClosedRange(
              path, closedRangeBoundComparisonOperator, singleQueryContext);
      case EqOperator eqOperator ->
          this.equalsQueryFactory.fromValue(
              eqOperator.value(), path, singleQueryContext.getEmbeddedRoot());
      case NeOperator neOperator ->
          CompoundQueryFactory.negate(
              List.of(
                  this.equalsQueryFactory.fromValue(
                      neOperator.value(), path, singleQueryContext.getEmbeddedRoot())),
              this.factoryContext,
              singleQueryContext);
      case InOperator inOperator ->
          this.inQueryFactory.fromValues(
              inOperator.values(),
              inOperator.getNonNullValueType(),
              List.of(path),
              singleQueryContext);
      case NinOperator ninOperator ->
          CompoundQueryFactory.negate(
              List.of(
                  this.inQueryFactory.fromValues(
                      ninOperator.values(),
                      ninOperator.getNonNullValueType(),
                      List.of(path),
                      singleQueryContext)),
              this.factoryContext,
              singleQueryContext);
      case NotOperator notOperator -> {
        var builder = new BooleanQuery.Builder();
        for (MqlFilterOperator op : notOperator.negateValues().mqlFilterOperators()) {
          builder.add(
              createLuceneFilterForField(path, op, singleQueryContext), BooleanClause.Occur.FILTER);
        }
        yield CompoundQueryFactory.negate(
            List.of(builder.build()), this.factoryContext, singleQueryContext);
      }
      case ExistsOperator existsOperator ->
          this.existsQueryFactory.existsQuery(
              path.toString(), existsOperator.value(), singleQueryContext);
    };
  }

  private Query createLuceneFilterForClosedRange(
      FieldPath path,
      ClosedRangeBoundComparisonOperator closedRange,
      SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    RangeBound<? extends Comparable<?>> bounds = closedRange.bounds();
    if (canEmitFusedClosedRange(
        bounds, path, singleQueryContext.getEmbeddedRoot(), singleQueryContext.getIndexReader())) {
      return this.rangeQueryFactory.fromBounds(bounds, List.of(path), singleQueryContext);
    }
    // Fall back to unoptimized filter with two open ranges.
    return andOfTwoOpens(bounds, path, singleQueryContext);
  }

  private Query andOfTwoOpens(
      RangeBound<? extends Comparable<?>> bounds,
      FieldPath path,
      SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    RangeBound<? extends Comparable<?>> lowerHalf = lowerHalf(bounds);
    RangeBound<? extends Comparable<?>> upperHalf = upperHalf(bounds);
    Query lowerQuery =
        this.rangeQueryFactory.fromBounds(lowerHalf, List.of(path), singleQueryContext);
    Query upperQuery =
        this.rangeQueryFactory.fromBounds(upperHalf, List.of(path), singleQueryContext);
    return new BooleanQuery.Builder()
        .add(lowerQuery, BooleanClause.Occur.MUST)
        .add(upperQuery, BooleanClause.Occur.MUST)
        .build();
  }

  private static RangeBound<? extends Comparable<?>> lowerHalf(
      RangeBound<? extends Comparable<?>> bounds) {
    return switch (bounds) {
      case NumericRangeBound numeric ->
          new NumericRangeBound(
              numeric.getLower(), Optional.empty(), numeric.lowerInclusive(), false);
      case DateRangeBound date ->
          new DateRangeBound(date.getLower(), Optional.empty(), date.lowerInclusive(), false);
      // Unreachable: the ClosedRangeBoundComparisonOperator constructor admits only the two bound
      // types above.
      case BooleanRangeBound ignored -> throw unsupportedClosedRangeBoundType(bounds);
      case ObjectIdRangeBound ignored -> throw unsupportedClosedRangeBoundType(bounds);
      case StringRangeBound ignored -> throw unsupportedClosedRangeBoundType(bounds);
      case UuidRangeBound ignored -> throw unsupportedClosedRangeBoundType(bounds);
    };
  }

  private static RangeBound<? extends Comparable<?>> upperHalf(
      RangeBound<? extends Comparable<?>> bounds) {
    return switch (bounds) {
      case NumericRangeBound numeric ->
          new NumericRangeBound(
              Optional.empty(), numeric.getUpper(), false, numeric.upperInclusive());
      case DateRangeBound date ->
          new DateRangeBound(Optional.empty(), date.getUpper(), false, date.upperInclusive());
      // Unreachable: the ClosedRangeBoundComparisonOperator constructor admits only the two bound
      // types above.
      case BooleanRangeBound ignored -> throw unsupportedClosedRangeBoundType(bounds);
      case ObjectIdRangeBound ignored -> throw unsupportedClosedRangeBoundType(bounds);
      case StringRangeBound ignored -> throw unsupportedClosedRangeBoundType(bounds);
      case UuidRangeBound ignored -> throw unsupportedClosedRangeBoundType(bounds);
    };
  }

  private static IllegalStateException unsupportedClosedRangeBoundType(RangeBound<?> bounds) {
    return new IllegalStateException(
        "unsupported bound type: " + bounds.getClass().getSimpleName());
  }

  private static boolean canEmitFusedClosedRange(
      RangeBound<?> bounds, FieldPath path, Optional<FieldPath> embeddedRoot, IndexReader reader) {
    try {
      return RangeQueryUtils.hasNoArrayData(reader, bounds, List.of(path), embeddedRoot);
    } catch (IOException e) {
      FLOGGER.atWarning().atMostEvery(5, TimeUnit.MINUTES).withCause(e).log(
          "Falling back to AND-of-two-opens for closed-range translation; could not read "
              + "PointValues for path=%s",
          path);
      return false;
    }
  }
}
