package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.query.util.WrappedToChildBlockJoinQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.HasAncestorOperator;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;

public class HasAncestorQueryFactory {

  /**
   * A {@link Query} that identifies root Lucene documents in an index. Use this to join matching
   * embedded document(s) to their corresponding root Lucene document in embedded queries.
   */
  private static final TermQuery ROOT_DOCUMENTS_QUERY =
      new TermQuery(
          new Term(
              FieldName.MetaField.EMBEDDED_ROOT.getLuceneFieldName(),
              FieldValue.EMBEDDED_ROOT_FIELD_VALUE));

  private static final QueryBitSetProducer ROOT_BIT_SET_PRODUCER =
      new QueryBitSetProducer(ROOT_DOCUMENTS_QUERY);

  private final SearchQueryFactoryContext context;

  HasAncestorQueryFactory(SearchQueryFactoryContext context) {
    this.context = context;
  }

  /**
   * Create an hasAncestor {@link Query} from {@link HasAncestorOperator}, a {@link
   * SingleQueryContext}, and {@link LuceneSearchQueryFactoryDistributor} which creates query for
   * child documents.
   *
   * <p>It first creates query child documents of embedded documents from {@link
   * HasAncestorOperator#operator()}, then creating a {@link ToChildBlockJoinQuery} that joins the
   * matching children to their parent documents for matching parent documents.
   */
  Query fromHasAncestor(
      HasAncestorOperator operator,
      SingleQueryContext singleQueryContext,
      LuceneSearchQueryFactoryDistributor luceneQueryFactory)
      throws InvalidQueryException, IOException {

    InvalidQueryException.validate(
        this.context.getFeatureFlags().isEnabled(Feature.NEW_EMBEDDED_SEARCH_CAPABILITIES),
        "HasAncestor operator is not supported in the current configuration. "
            + "Please enable 'enableNewEmbeddedSearchCapabilities' feature flag to use it.");

    // there are three sources of embedded documents:
    // 1. returnScope at the top level of the query;
    // 2. path of EmbeddedDocumentOperator;
    // 3. ancestorPath of HasAncestorOperator;
    // for case 2, 3, in their query factory we have already validated that their paths are
    // indexed as embedded documents. For case 1, we need additional validation here.
    if (singleQueryContext.getEmbeddedRoot().isPresent()) {
      InvalidQueryException.validate(
          this.context
              .getQueryTimeMappingChecks()
              .isIndexedAsEmbeddedDocumentsField(singleQueryContext.getEmbeddedRoot().get()),
          "hasAncestor requires %s to be indexed as 'embeddedDocuments'",
          singleQueryContext
              .getEmbeddedRoot()
              .map(embeddedRoot -> String.format("returnScope '%s'", embeddedRoot))
              .orElse("document root"));
    }

    InvalidQueryException.validate(
        this.context
            .getQueryTimeMappingChecks()
            .isIndexedAsEmbeddedDocumentsField(operator.ancestorPath()),
        "hasAncestor requires path '%s' to be indexed as 'embeddedDocuments'",
        operator.ancestorPath());

    // Create QueryContext of HasAncestorOperator's operator querying embedded docs of ancestorPath.
    var hasAncestorContext = singleQueryContext.withEmbeddedRoot(operator.ancestorPath());

    // Create query from hasAncestor operator's child operator to find matching embedded docs under
    // ancestorPath.
    Query hasAncestorQuery =
        luceneQueryFactory.createQuery(operator.operator(), hasAncestorContext);

    // Create a QueryBitSetProducer that identifies embedded parent documents matching the ancestor
    // path.
    QueryBitSetProducer ancestorBitset =
        ancestorBitSetProducer(hasAncestorContext.getEmbeddedRoot());

    // Query matching parent documents joining from parent to child documents and
    // fetch documents whose embeddedPath is exact the same as embeddedRoot.
    return BooleanComposer.must(
        new WrappedToChildBlockJoinQuery(hasAncestorQuery, ancestorBitset),
        childFilter(singleQueryContext.getEmbeddedRoot()));
  }

  /**
   * Create a filter {@link QueryBitSetProducer} that identifies documents whose {@link
   * FieldName.MetaField#EMBEDDED_PATH} field matches the given parent embedded document path.
   */
  static QueryBitSetProducer ancestorBitSetProducer(
      Optional<FieldPath> parentEmbeddedDocumentPath) {
    return parentEmbeddedDocumentPath
        .map(
            fieldPath ->
                new QueryBitSetProducer(
                    new TermQuery(
                        new Term(
                            FieldName.MetaField.EMBEDDED_PATH.getLuceneFieldName(),
                            fieldPath.toString()))))
        .orElse(ROOT_BIT_SET_PRODUCER);
  }

  /**
   * Create a filter {@link Query} that identifies child documents whose {@link
   * FieldName.MetaField#EMBEDDED_PATH} field is exactly the same as embeddedRoot.
   */
  static Query childFilter(Optional<FieldPath> returnScope) {
    return new ConstantScoreQuery(
        returnScope
            .map(
                fieldPath ->
                    new TermQuery(
                        new Term(
                            FieldName.MetaField.EMBEDDED_PATH.getLuceneFieldName(),
                            fieldPath.toString())))
            .orElse(ROOT_DOCUMENTS_QUERY));
  }
}
