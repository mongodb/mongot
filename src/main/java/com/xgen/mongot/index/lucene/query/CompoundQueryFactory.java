package com.xgen.mongot.index.lucene.query;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.DisableBulkScorerQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.CompoundClause;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.scores.DismaxScore;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

public class CompoundQueryFactory {

  private final SearchQueryFactoryContext queryFactoryContext;
  private final QueryCreator queryCreator;
  private final QueryCreator explainQueryCreator;
  private final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;

  CompoundQueryFactory(
      SearchQueryFactoryContext queryFactoryContext,
      QueryCreator queryCreator,
      QueryCreator explainQueryCreator,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    this.queryFactoryContext = queryFactoryContext;
    this.queryCreator = queryCreator;
    this.explainQueryCreator = explainQueryCreator;
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
  }

  public BooleanQuery fromCompound(CompoundOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException, IOException {
    boolean onlyMustNot =
        operator.must().isEmpty() && operator.should().isEmpty() && operator.filter().isEmpty();
    if (onlyMustNot) {
      return maybeForceDefaultBulkScorerForReqExcl(handleOnlyMustNot(operator, singleQueryContext));
    }

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    createLuceneQueries(operator.filter(), BooleanClause.Occur.FILTER, singleQueryContext)
        .forEach(q -> builder.add(q, BooleanClause.Occur.FILTER));
    createLuceneQueries(operator.must(), BooleanClause.Occur.MUST, singleQueryContext)
        .forEach(q -> builder.add(q, BooleanClause.Occur.MUST));
    createLuceneQueries(operator.mustNot(), BooleanClause.Occur.MUST_NOT, singleQueryContext)
        .forEach(q -> builder.add(q, BooleanClause.Occur.MUST_NOT));

    return maybeForceDefaultBulkScorerForReqExcl(
        handleShouldClause(operator, builder, singleQueryContext));
  }

  /**
   * Mitigates a Lucene 10.4 {@code DenseConjunctionBulkScorer} regression on {@code +required
   * -mustNot} compounds. When {@code query} has exactly one required ({@code FILTER} or {@code
   * MUST}) clause, at least one {@code MUST_NOT} clause, and no optional clauses, Lucene's {@code
   * BooleanScorerSupplier} builds a {@code ReqExclBulkScorer} over the required clause's own {@link
   * org.apache.lucene.search.BulkScorer}. For a single dense required clause that bulk scorer is a
   * {@code DenseConjunctionBulkScorer}, whose materialize-to-bitset round trip is pure overhead (no
   * second clause to AND-intersect). Wrapping the required clause in {@link DisableBulkScorerQuery}
   * forces {@code Weight.DefaultBulkScorer}, restoring the Lucene 10.1 direct postings iteration.
   * Gated by {@link DynamicFeatureFlags#DISABLE_BULK_SCORER_QUERY_FOR_COMPOUND_REQ_EXCL}.
   *
   * <p>Compounds with two or more required clauses are left untouched: there the {@code
   * BooleanScorerSupplier} builds its own conjunction from the clauses' scorers (wrapping a leaf
   * would have no effect) and the bitset AND-intersection is a genuine optimization.
   */
  private BooleanQuery maybeForceDefaultBulkScorerForReqExcl(BooleanQuery query) {
    if (!this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
        DynamicFeatureFlags.DISABLE_BULK_SCORER_QUERY_FOR_COMPOUND_REQ_EXCL)) {
      return query;
    }
    if (query.getMinimumNumberShouldMatch() != 0) {
      return query;
    }

    @Var BooleanClause requiredClause = null;
    @Var int requiredCount = 0;
    @Var int mustNotCount = 0;
    for (BooleanClause clause : query.clauses()) {
      switch (clause.occur()) {
        case FILTER, MUST -> {
          requiredClause = clause;
          requiredCount++;
        }
        case MUST_NOT -> mustNotCount++;
        case SHOULD -> {
          // An optional clause changes scorer selection away from the ReqExcl shape.
          return query;
        }
      }
    }

    if (requiredCount != 1 || mustNotCount < 1) {
      return query;
    }

    BooleanQuery.Builder rebuilt = new BooleanQuery.Builder();
    rebuilt.setMinimumNumberShouldMatch(query.getMinimumNumberShouldMatch());
    for (BooleanClause clause : query.clauses()) {
      if (clause == requiredClause) {
        rebuilt.add(new DisableBulkScorerQuery(clause.query()), clause.occur());
      } else {
        rebuilt.add(clause);
      }
    }
    return rebuilt.build();
  }

  static BooleanQuery negate(
      List<Query> queries,
      QueryFactoryContext queryFactoryContext,
      SingleQueryContext singleQueryContext) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();

    // Match all docs so MUST_NOT has a predicate to act upon
    // Uses FILTER so we don't affect the score
    if (queryFactoryContext.isIndexWithEmbeddedFields()) {
      // If the index contains embedded documents, add a FILTER clause that matches all parent
      // embedded documents or root documents. This is independent of whether any children of this
      // compound operator are themselves embeddedDocument operators; this filter must only select
      // documents that are parents of this compound operator, and may not select unrelated
      // embedded documents that are present in the index.
      builder.add(
          EmbeddedDocumentQueryFactory.parentFilter(singleQueryContext.getEmbeddedRoot()),
          BooleanClause.Occur.FILTER);
    } else {
      // If the index does not contain any embeddedDocuments, the filter clause here may select
      // all documents; a MatchAllDocsQuery will not select unrelated embedded documents, because
      // no embedded documents are present in this index.
      builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER);
    }

    queries.forEach(query -> builder.add(query, BooleanClause.Occur.MUST_NOT));

    return builder.build();
  }

  private BooleanQuery handleOnlyMustNot(
      CompoundOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException, IOException {

    List<Query> queries =
        createLuceneQueries(operator.mustNot(), BooleanClause.Occur.MUST_NOT, singleQueryContext);
    return negate(queries, this.queryFactoryContext, singleQueryContext);
  }

  private BooleanQuery handleShouldClause(
      CompoundOperator operator,
      BooleanQuery.Builder builder,
      SingleQueryContext singleQueryContext)
      throws InvalidQueryException, IOException {
    CompoundClause should = operator.should();
    if (should.isPresent()) {
      builder.setMinimumNumberShouldMatch(operator.minimumShouldMatch());
    }

    List<Query> shouldQueries =
        createLuceneQueries(should, BooleanClause.Occur.SHOULD, singleQueryContext);
    if (operator.score() instanceof DismaxScore(float tieBreakerScore)
        && shouldQueries.size() > 1) {
      builder.add(
          new DisjunctionMaxQuery(shouldQueries, tieBreakerScore), BooleanClause.Occur.SHOULD);
    } else {
      shouldQueries.forEach(q -> builder.add(q, BooleanClause.Occur.SHOULD));
    }

    return builder.build();
  }

  private List<Query> createLuceneQueries(
      CompoundClause clause, BooleanClause.Occur occur, SingleQueryContext singleQueryContext)
      throws InvalidQueryException, IOException {
    return switch (singleQueryContext.getQueryAssociation()) {
      case NONE -> {
        List<Query> result = new ArrayList<>(clause.operators().size());
        for (Operator operator : clause.operators()) {
          result.add(this.queryCreator.create(operator, singleQueryContext));
        }
        yield result;
      }
      case WITH_OPERATOR -> {
        List<? extends Operator> operators = clause.operators();

        // Special case when just one clause - no need to include an array index in the new child
        // path (e.g. new child will be "compound.occur" instead of "compound.occur[0]"). If there
        // are multiple clause for this occur, include the array index at which a clause occurs in
        // the operator path.
        Function<Integer, String> operatorPathFactory =
            operators.size() == 1
                ? ignored -> pathComponentFor(occur)
                : arrayIdx -> String.format("%s[%s]", pathComponentFor(occur), arrayIdx);

        List<Query> queries = new ArrayList<>(operators.size());
        for (int i = 0; i != operators.size(); i++) {
          Optional<FieldPath> childOperatorPath =
              Optional.of(
                  singleQueryContext
                      .getOperatorPath()
                      .map(path -> path.newChild("compound"))
                      .orElse(FieldPath.parse("compound"))
                      .newChild(operatorPathFactory.apply(i)));
          queries.add(
              i,
              this.explainQueryCreator.create(
                  operators.get(i), singleQueryContext.withOperatorPath(childOperatorPath)));
        }
        yield queries;
      }
    };
  }

  private String pathComponentFor(BooleanClause.Occur occur) {
    return switch (occur) {
      case MUST -> "must";
      case FILTER -> "filter";
      case SHOULD -> "should";
      case MUST_NOT -> "mustNot";
    };
  }
}
