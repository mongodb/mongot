package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.definition.VectorFieldDefinitionResolver;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

/** This class is used for $vectorSearch queries running against vector index type. */
public class LuceneVectorQueryFactoryDistributor {

  private final VectorSearchQueryFactory vectorSearchQueryFactory;
  private final VectorFieldDefinitionResolver definitionResolver;

  private LuceneVectorQueryFactoryDistributor(
      VectorSearchQueryFactory vectorSearchQueryFactory, VectorQueryFactoryContext context) {
    this.vectorSearchQueryFactory = vectorSearchQueryFactory;
    this.definitionResolver = context.getFieldDefinitionResolver();
  }

  public static LuceneVectorQueryFactoryDistributor create(
      VectorQueryFactoryContext factoryContext) {
    var equalsQueryFactory = new EqualsQueryFactory(factoryContext);
    var existsQueryFactory = new ExistsQueryFactory(factoryContext);
    var rangeQueryFactory = new RangeQueryFactory(factoryContext, equalsQueryFactory);
    var inQueryFactory = new InQueryFactory(factoryContext);
    return new LuceneVectorQueryFactoryDistributor(
        new VectorSearchQueryFactory(
            factoryContext,
            new VectorSearchFilterQueryFactory(
                factoryContext,
                rangeQueryFactory,
                inQueryFactory,
                existsQueryFactory,
                equalsQueryFactory)),
        factoryContext);
  }

  public Query createQuery(MaterializedVectorSearchQuery query, IndexReader indexReader)
      throws InvalidQueryException, IOException {
    return this.vectorSearchQueryFactory.fromQuery(
        query.materializedCriteria(), SingleQueryContext.createQueryRoot(indexReader));
  }

  /**
   * Creates a WrappedKnnQuery from an operator to be used for explain queries.
   *
   * @param materializedVectorQuery vector search query
   * @param indexReader index reader
   * @return Lucene Query
   * @throws InvalidQueryException represents a parsing exception
   */
  public Query createExplainQuery(
      MaterializedVectorSearchQuery materializedVectorQuery, IndexReader indexReader)
      throws InvalidQueryException, IOException {
    var singleQueryContext = SingleQueryContext.createExplainRoot(indexReader);
    return new WrappedKnnQuery(
        this.vectorSearchQueryFactory.fromQuery(
            materializedVectorQuery.materializedCriteria(), singleQueryContext));
  }

  public VectorFieldDefinitionResolver getDefinitionResolver() {
    return this.definitionResolver;
  }
}
