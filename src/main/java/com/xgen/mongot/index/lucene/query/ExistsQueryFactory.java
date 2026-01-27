package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.ExistsOperator;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class ExistsQueryFactory {
  private final QueryFactoryContext queryFactoryContext;

  public ExistsQueryFactory(QueryFactoryContext queryFactoryContext) {
    this.queryFactoryContext = queryFactoryContext;
  }

  /**
   * Creates a Lucene {@link Query} from an {@link ExistsOperator} within the given {@link
   * SingleQueryContext}. This method assumes {@code exists = true}, i.e., the query will match
   * documents where the field exists.
   *
   * @throws InvalidQueryException if the query cannot be constructed
   */
  Query fromExistsOperator(ExistsOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    String path = operator.path();
    return existsQuery(path, true, singleQueryContext);
  }

  /**
   * Creates a Lucene {@link Query} that checks whether a field at the given path exists or not,
   * based on the provided boolean flag.
   *
   * @param path the field path to check
   * @param value if {@code true}, query will match documents where the field exists; if {@code
   *     false}, query will match documents where the field is missing
   * @param singleQueryContext the query context used to build the Lucene query
   * @return a Lucene {@link Query} representing the field existence condition
   * @throws InvalidQueryException if the query cannot be constructed
   */
  Query existsQuery(String path, boolean value, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {

    if (!this.queryFactoryContext.getIndexCapabilities().supportsFieldExistsQuery()) {
      throw new IllegalStateException("Index format version is not supported");
    }

    if (path.length() > LuceneConfig.MAX_TERM_CHAR_LENGTH) {
      throw new InvalidQueryException(
          String.format(
              "Field name length in exists query must not exceed %s characters",
              LuceneConfig.MAX_TERM_CHAR_LENGTH));
    }

    Query fieldNameExistsQuery =
        new TermQuery(new Term(FieldName.MetaField.FIELD_NAMES.getLuceneFieldName(), path));

    Query existsQuery =
        value
            ? fieldNameExistsQuery
            : new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                .add(fieldNameExistsQuery, BooleanClause.Occur.MUST_NOT)
                .build();

    // If this exists operator is being run over an index that does not contain embedded documents
    // we do not need to check that this field is indexed in an embedded document at the appropriate
    // nesting level.
    if (!this.queryFactoryContext.isIndexWithEmbeddedFields()) {
      // Wrap in ConstantScoreQuery since TF-IDF scoring produces unintuitive results for exists
      // queries.
      return new ConstantScoreQuery(existsQuery);
    }

    // If this exists operator is being run over an index that contains embedded documents, make
    // sure we're only matching fields that are in embedded documents at the nesting level of this
    // query.
    return new ConstantScoreQuery(
        new BooleanQuery.Builder()
            .add(
                EmbeddedDocumentQueryFactory.parentFilter(singleQueryContext.getEmbeddedRoot()),
                BooleanClause.Occur.FILTER)
            .add(existsQuery, BooleanClause.Occur.FILTER)
            .build());
  }
}
