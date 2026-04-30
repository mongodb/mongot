package com.xgen.mongot.index.lucene.query;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.lucene.query.util.DisableBulkScorerQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToChildBlockJoinQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.mongot.index.lucene.query.weights.WrappedExplainWeight;
import java.io.IOException;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

public class ScoreDetailsWrappedQuery extends Query {
  private final Query luceneQuery;

  @VisibleForTesting
  public ScoreDetailsWrappedQuery(Query luceneQuery) {
    this.luceneQuery = luceneQuery;
  }

  /**
   * A query that can nest other queries should be wrapped for score details if its weight's
   * explain() method calls its nested weights' explain() methods too. This is because if a nested
   * query weight's explain() method returns null, like <a
   * href=https://tinyurl.com/ucpjc93x>TermAutomatonQuery.TermAutomatonWeight's does</a>, then this
   * could cause a NullPointerException to occur if the query is not wrapped. Wrapping such queries
   * inside-out ensures that we can handle such exceptions safely like we do in {@link
   * WrappedExplainWeight#rewriteExplanation(LeafReaderContext, int)}.
   */
  public static ScoreDetailsWrappedQuery wrap(Query query) {
    return switch (query) {
      case BooleanQuery booleanQuery -> {
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        for (var clause : booleanQuery.clauses()) {
          booleanQueryBuilder.add(wrap(clause.query()), clause.occur());
        }
        booleanQueryBuilder.setMinimumNumberShouldMatch(booleanQuery.getMinimumNumberShouldMatch());
        yield new ScoreDetailsWrappedQuery(booleanQueryBuilder.build());
      }
      case WrappedToParentBlockJoinQuery wrappedToParentBlockJoinQuery ->
          new ScoreDetailsWrappedQuery(
              wrappedToParentBlockJoinQuery.withWrappedChildQuery(
                  wrap(wrappedToParentBlockJoinQuery.getChildQuery())));
      case WrappedToChildBlockJoinQuery wrappedToChildBlockJoinQuery ->
          new ScoreDetailsWrappedQuery(
              wrappedToChildBlockJoinQuery.withWrappedParentQuery(
                  wrap(wrappedToChildBlockJoinQuery.getParentQuery())));
      case DisjunctionMaxQuery disjunctionMaxQuery ->
          new ScoreDetailsWrappedQuery(
              new DisjunctionMaxQuery(
                  disjunctionMaxQuery.getDisjuncts().stream()
                      .map(ScoreDetailsWrappedQuery::wrap)
                      .collect(Collectors.toUnmodifiableList()),
                  disjunctionMaxQuery.getTieBreakerMultiplier()));
      case DisableBulkScorerQuery disableBulkScorerQuery ->
          // Execution-only wrapper; score details / explain should match the inner query alone.
          wrap(disableBulkScorerQuery.innerQuery());
      default -> new ScoreDetailsWrappedQuery(query);
    };
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode mode, float boost)
      throws IOException {
    return WrappedExplainWeight.create(this.luceneQuery, searcher, mode, boost);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    this.luceneQuery.visit(visitor);
  }

  @Override
  public Query rewrite(IndexSearcher reader) throws IOException {
    Query rewrittenQuery = this.luceneQuery.rewrite(reader);
    if (!Objects.equals(this.luceneQuery, rewrittenQuery)) {
      return new ScoreDetailsWrappedQuery(rewrittenQuery);
    }
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ScoreDetailsWrappedQuery that)) {
      return false;
    }
    return this.luceneQuery.equals(that.luceneQuery);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.luceneQuery);
  }

  @Override
  public String toString(String field) {
    return "ScoreDetailsWrapped (" + this.luceneQuery.toString() + ")";
  }
}
