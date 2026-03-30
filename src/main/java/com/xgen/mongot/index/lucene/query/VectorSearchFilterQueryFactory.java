package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.MappingCompatibilityValidator;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.mql.AndClause;
import com.xgen.mongot.index.query.operators.mql.Clause;
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
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

public class VectorSearchFilterQueryFactory {

  private final QueryFactoryContext factoryContext;
  private final RangeQueryFactory rangeQueryFactory;
  private final InQueryFactory inQueryFactory;
  private final ExistsQueryFactory existsQueryFactory;
  private final EqualsQueryFactory equalsQueryFactory;

  public static VectorSearchFilterQueryFactory create(VectorQueryFactoryContext factoryContext) {
    var equalsQueryFactory = new EqualsQueryFactory(factoryContext);
    var existsQueryFactory = new ExistsQueryFactory(factoryContext);
    var rangeQueryFactory = new RangeQueryFactory(factoryContext, equalsQueryFactory);
    var inQueryFactory = new InQueryFactory(factoryContext);
    return new VectorSearchFilterQueryFactory(
        factoryContext, rangeQueryFactory, inQueryFactory, existsQueryFactory, equalsQueryFactory);
  }

  VectorSearchFilterQueryFactory(
      QueryFactoryContext factoryContext,
      RangeQueryFactory rangeQueryFactory,
      InQueryFactory inQueryFactory,
      ExistsQueryFactory existsQueryFactory,
      EqualsQueryFactory equalsQueryFactory) {
    this.factoryContext = factoryContext;
    this.rangeQueryFactory = rangeQueryFactory;
    this.inQueryFactory = inQueryFactory;
    this.existsQueryFactory = existsQueryFactory;
    this.equalsQueryFactory = equalsQueryFactory;
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
    return switch (filter) {
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
    }

    return switch (operator) {
      case OpenRangeBoundComparisonOperator openRangeBoundComparisonOperator ->
          this.rangeQueryFactory.fromBounds(
              openRangeBoundComparisonOperator.getBounds(), List.of(path), singleQueryContext);
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
              inOperator.values(), inOperator.getNonNullValueType(), List.of(path),
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
}
