package com.xgen.mongot.index.lucene.query.util;

import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

/**
 * Wraps a {@link Query} so Lucene does not use specialized {@link BulkScorer} implementations (for
 * example {@code ConjunctionBulkScorer}) for the wrapped subtree. Instead, {@link
 * Weight#bulkScorer} uses {@link Weight.DefaultBulkScorer} over the normal {@link Scorer}, matching
 * the approach of Lucene's test-only {@code DisablingBulkScorerQuery}.
 *
 * <p>Intended for experiments and mitigations when a Lucene upgrade shifts hot paths from {@code
 * ConjunctionDISI} to bulk conjunction scoring and that change regresses specific workloads (for
 * example dense {@code embeddedDocument} child queries).
 *
 * <p>See Apache Lucene {@code org.apache.lucene.tests.search.DisablingBulkScorerQuery} (ASL 2.0).
 */
public final class DisableBulkScorerQuery extends Query {

  private final Query query;

  public DisableBulkScorerQuery(Query query) {
    this.query = Objects.requireNonNull(query, "query");
  }

  /** The wrapped query; exposed for {@code ScoreDetailsWrappedQuery} and tests. */
  public Query innerQuery() {
    return this.query;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query rewritten = this.query.rewrite(indexSearcher);
    if (this.query != rewritten) {
      return new DisableBulkScorerQuery(rewritten);
    }
    return this;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    Weight inner = this.query.createWeight(searcher, scoreMode, boost);
    return new FilterWeight(inner) {
      @Override
      @Nullable
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        ScorerSupplier delegate = super.scorerSupplier(context);
        if (delegate == null) {
          return null;
        }
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            return delegate.get(leadCost);
          }

          @Override
          public long cost() {
            return delegate.cost();
          }

          @Override
          public void setTopLevelScoringClause() throws IOException {
            delegate.setTopLevelScoringClause();
          }

          @Override
          public BulkScorer bulkScorer() throws IOException {
            return new Weight.DefaultBulkScorer(get(Long.MAX_VALUE));
          }
        };
      }
    };
  }

  @Override
  public String toString(String field) {
    return "DisableBulkScorerQuery(" + this.query.toString(field) + ")";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    this.query.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
  }

  @Override
  public boolean equals(Object obj) {
    return sameClassAs(obj) && this.query.equals(((DisableBulkScorerQuery) obj).query);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + this.query.hashCode();
  }
}
