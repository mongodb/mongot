package com.xgen.testing.mongot.index.lucene.explain.query;

import com.xgen.mongot.index.lucene.explain.query.RewrittenChildClauses;
import com.xgen.mongot.index.lucene.explain.query.RewrittenQueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

public class RewrittenQueryExecutionContextNodeBuilder {
  private Optional<Query> query = Optional.empty();
  private Optional<ExplainTimings> timings = Optional.of(ExplainTimings.builder().build());
  private Optional<RewrittenChildClauses> childClauses = Optional.empty();
  private Optional<BooleanClause.Occur> clauseOccur = Optional.empty();

  public static RewrittenQueryExecutionContextNodeBuilder builder() {
    return new RewrittenQueryExecutionContextNodeBuilder();
  }

  public RewrittenQueryExecutionContextNodeBuilder query(Query query) {
    this.query = Optional.of(query);
    return this;
  }

  public RewrittenQueryExecutionContextNodeBuilder timings(ExplainTimings timings) {
    this.timings = Optional.of(timings);
    return this;
  }

  public RewrittenQueryExecutionContextNodeBuilder childClauses(
      RewrittenChildClauses childClauses) {
    this.childClauses = Optional.of(childClauses);
    return this;
  }

  public RewrittenQueryExecutionContextNodeBuilder clauseOccur(BooleanClause.Occur occur) {
    this.clauseOccur = Optional.of(occur);
    return this;
  }

  public RewrittenQueryExecutionContextNode build() {
    Check.isPresent(this.query, "query");

    return new RewrittenQueryExecutionContextNode(
        this.query.get(), this.timings.get(), this.childClauses, this.clauseOccur);
  }
}
