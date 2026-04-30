package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.DisableBulkScorerQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.scores.EmbeddedScore;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;

class EmbeddedDocumentQueryFactory {

  /**
   * A {@link Query} that identifies root Lucene documents in an index. Use this to join matching
   * embedded document(s) to their corresponding root Lucene document in embedded queries.
   */
  public static final TermQuery ROOT_DOCUMENTS_QUERY =
      new TermQuery(
          new Term(
              FieldName.MetaField.EMBEDDED_ROOT.getLuceneFieldName(),
              FieldValue.EMBEDDED_ROOT_FIELD_VALUE));

  /**
   * A {@link BitSetProducer} that identifies root Lucene documents in an index. Use this to group
   * embedded queries to root documents.
   *
   * <p>This uses the query defined in {@link EmbeddedDocumentQueryFactory#ROOT_DOCUMENTS_QUERY},
   * and only exists as a performance optimization. {@link QueryBitSetProducer} populates a cache
   * throughout its lifetime, and is more efficient if allowed to be reused.
   */
  private static final BitSetProducer ROOT_BIT_SET_PRODUCER =
      new QueryBitSetProducer(parentFilter(Optional.empty()));

  private final SearchQueryFactoryContext context;
  private final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;

  EmbeddedDocumentQueryFactory(
      SearchQueryFactoryContext context, DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    this.context = context;
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
  }

  /**
   * Create an embedded document {@link Query}, given an {@link EmbeddedDocumentOperator}, a {@link
   * SingleQueryContext}, and a function that can be used to create a Lucene Query for the child of
   * this embedded documents operator.
   */
  Query fromEmbeddedDocument(
      EmbeddedDocumentOperator embeddedDocumentOperator,
      SingleQueryContext singleQueryContext,
      LuceneSearchQueryFactoryDistributor luceneQueryFactory)
      throws InvalidQueryException, IOException {
    // Check that the path specified in the embeddedDocument operator is indeed the path of an
    // embeddedDocuments field definition in the index.
    InvalidQueryException.validate(
        this.context
            .getQueryTimeMappingChecks()
            .isIndexedAsEmbeddedDocumentsField(embeddedDocumentOperator.path()),
        "embeddedDocument requires path '%s' to be indexed as 'embeddedDocuments'",
        embeddedDocumentOperator.path().toString());

    // The query to match to individual embedded documents.
    Query innerChildQuery =
        luceneQueryFactory.createQuery(
            embeddedDocumentOperator.operator(),
            singleQueryContext.withEmbeddedRoot(embeddedDocumentOperator.path()));
    Query childQuery =
        this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
                DynamicFeatureFlags.DISABLE_BULK_SCORER_QUERY_FOR_EMBEDDED_DOCUMENT_CHILD)
            ? new DisableBulkScorerQuery(innerChildQuery)
            : innerChildQuery;

    // delegate to child query since a join is not needed for the same query context root and
    // embedded root.
    if (singleQueryContext.getEmbeddedRoot().isPresent()
        && singleQueryContext.getEmbeddedRoot().get().equals(embeddedDocumentOperator.path())) {
      return childQuery;
    }

    // The parentFilter identifies parent documents of embedded documents matched by the child
    // query. This is used to join embedded documents to their parent document.
    BitSetProducer parentFilter =
        singleQueryContext.getEmbeddedRoot().isEmpty()
            ? ROOT_BIT_SET_PRODUCER
            : new QueryBitSetProducer(parentFilter(singleQueryContext.getEmbeddedRoot()));

    return new WrappedToParentBlockJoinQuery(
        childQuery, parentFilter, scoreMode(embeddedDocumentOperator));
  }

  /**
   * The parent filter for an embedded query identifies documents that could be parents of a
   * matching embedded document. Parent documents are identified by a special $meta field that is
   * specific to the level of embedding of the parent document.
   */
  static Query parentFilter(Optional<FieldPath> parentEmbeddedDocumentPath) {
    if (parentEmbeddedDocumentPath.isEmpty()) {
      return ROOT_DOCUMENTS_QUERY;
    }

    return new TermQuery(
        new Term(
            FieldName.MetaField.EMBEDDED_PATH.getLuceneFieldName(),
            parentEmbeddedDocumentPath.get().toString()));
  }

  private static ScoreMode scoreMode(EmbeddedDocumentOperator operator) {
    if (operator.score() instanceof EmbeddedScore score) {
      return scoreMode(score.aggregate());
    } else {
      return scoreMode(EmbeddedScore.Fields.AGGREGATE.getDefaultValue());
    }
  }

  private static ScoreMode scoreMode(EmbeddedScore.Aggregate aggregate) {
    return switch (aggregate) {
      case SUM -> ScoreMode.Total;
      case MAXIMUM -> ScoreMode.Max;
      case MINIMUM -> ScoreMode.Min;
      case MEAN -> ScoreMode.Avg;
    };
  }
}
