package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.NullnessAwareIndexSortRangeQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class NullnessAwareIndexSortRangeQuerySpecCreator {
  static <T extends Optional<? extends QueryChildren<?>>>
      NullnessAwareIndexSortRangeQuerySpec fromQuery(
          T children, boolean useIndexSort, Explain.Verbosity verbosity) {
    // Not getOnlyChild(): IndexSortSortedNumericDocValuesRangeQuery.visit() registers itself as a
    // leaf and then visits its fallback query with the same visitor, so the fallback's
    // IndexOrDocValuesQuery node is a sibling rather than a child. Only must() is read because
    // NullnessAwareIndexSortRangeQuery.visit() requests a MUST sub-visitor exclusively.
    List<QueryExplainInformation> childInfos =
        children
            .map(QueryChildren::must)
            .map(
                clauses ->
                    clauses.stream()
                        .map(
                            contextNode ->
                                QueryExplainInformationCreator.fromNode(contextNode, verbosity))
                        .collect(Collectors.toList()))
            .orElse(List.of());
    return new NullnessAwareIndexSortRangeQuerySpec(useIndexSort, childInfos);
  }
}
