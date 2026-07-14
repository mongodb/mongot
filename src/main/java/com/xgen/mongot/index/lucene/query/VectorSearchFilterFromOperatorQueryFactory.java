package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.optimizer.QueryOptimizer;
import java.io.IOException;
import org.apache.lucene.search.Query;

class VectorSearchFilterFromOperatorQueryFactory extends VectorSearchFilterQueryFactory {

  private final QueryCreator queryCreator;

  VectorSearchFilterFromOperatorQueryFactory(
      SearchQueryFactoryContext queryFactoryContext,
      RangeQueryFactory rangeQueryFactory,
      InQueryFactory inQueryFactory,
      ExistsQueryFactory existsQueryFactory,
      EqualsQueryFactory equalsQueryFactory,
      QueryOptimizer queryOptimizer,
      QueryCreator queryCreator) {
    super(
        queryFactoryContext,
        rangeQueryFactory,
        inQueryFactory,
        existsQueryFactory,
        equalsQueryFactory,
        queryOptimizer);
    this.queryCreator = queryCreator;
  }

  @Override
  protected Query createLuceneFilterFromOperator(
      Operator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException, IOException {
    return this.queryCreator.create(operator, singleQueryContext);
  }
}
