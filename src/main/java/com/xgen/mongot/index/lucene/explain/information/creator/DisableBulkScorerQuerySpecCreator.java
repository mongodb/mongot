package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.DisableBulkScorerQuerySpec;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;

public class DisableBulkScorerQuerySpecCreator {
  static DisableBulkScorerQuerySpec fromQuery(
      QueryExecutionContextNode child, Explain.Verbosity verbosity) {
    return new DisableBulkScorerQuerySpec(
        QueryExplainInformationCreator.fromNode(child, verbosity));
  }
}
