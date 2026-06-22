package com.xgen.mongot.index.lucene.explain.information.creator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimingBreakdown;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.lucene.query.custom.WrappedQuery;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.timers.TimingData;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.lucene.search.Query;

public class QueryExplainInformationCreator {
  /**
   * Create {@link QueryExplainInformation} from a {@link QueryExecutionContextNode}. This is the
   * entry point for outside callers to create {@code SearchExplainInformation} from the root {@code
   * QueryExecutionContextNode}.
   */
  public static QueryExplainInformation fromNode(
      QueryExecutionContextNode node, Explain.Verbosity verbosity) {
    return from(node, Optional.empty(), Optional.empty(), verbosity);
  }

  // package-private for testing.
  static QueryExplainInformation from(
      QueryExecutionContextNode node,
      Optional<FieldPath> operatorPath,
      Optional<Map<ExplainTimings.Type, TimingData>> timings,
      Explain.Verbosity verbosity) {
    Query query = node.getQuery();

    @Var
    ImmutableMap<ExplainTimings.Type, TimingData> explainTimings =
        timings
            .map(stats -> ExplainTimings.combineTimingData(stats, node.getTimings()))
            .orElseGet(() -> node.getTimings().extractTimingData());

    Optional<WrappedKnnQuery> maybeWrappedKnnQuery = WrappedKnnQuery.asWrapped(query);
    if (maybeWrappedKnnQuery.isPresent()) {
      explainTimings = mergeKnnQueryTiming(node, explainTimings);
    }
    Optional<WrappedQuery> maybeWrappedQuery = WrappedQuery.asWrapped(query);
    // If query is a regular Lucene query or a WrappedKnnQuery (not a WrappedQuery), simply return a
    // new LuceneExplainInformation for it.
    if (maybeWrappedQuery.isEmpty() || maybeWrappedKnnQuery.isPresent()) {
      LuceneQuerySpecification args = LuceneQuerySpecificationCreator.querySpecFor(node, verbosity);

      return new QueryExplainInformation(
          operatorPath,
          args.getType(),
          Optional.empty(),
          args,
          timingsFor(explainTimings, verbosity));
    }

    // query is a WrappedQuery.
    WrappedQuery wrappingQuery = maybeWrappedQuery.get();
    Optional<QueryExecutionContextNode> wrappedNode = unwrapChildNodeIfSingleChild(node);

    // If query has exactly one child, make a new LuceneExplainInformation for that single child.
    // Pass down the operator and operator path metadata stored in WrappedQuery to be associated
    // with that child.
    if (wrappedNode.isPresent()) {
      return from(
          wrappedNode.get(),
          wrappingQuery.getOperatorPath(),
          Optional.of(explainTimings),
          verbosity);
    }

    // query is a WrappedQuery, but doesn't have exactly one child. We don't know how to proceed, so
    // return a LuceneExplainInformation with a LuceneQuerySpecification created from a
    // WrappedQuery.
    LuceneQuerySpecification wrappingArgs =
        LuceneQuerySpecificationCreator.querySpecFor(node, verbosity);

    return new QueryExplainInformation(
        wrappingQuery.getOperatorPath(),
        wrappingArgs.getType(),
        Optional.empty(),
        wrappingArgs,
        timingsFor(explainTimings, verbosity));
  }

  private static Optional<ExplainTimingBreakdown> timingsFor(
      Map<ExplainTimings.Type, TimingData> timings, Explain.Verbosity verbosity) {
    return switch (verbosity) {
      case QUERY_PLANNER -> Optional.empty();
      case EXECUTION_STATS, ALL_PLANS_EXECUTION ->
          Optional.of(ExplainTimingBreakdown.fromExecutionStats(timings));
    };
  }

  private static ImmutableMap<ExplainTimings.Type, TimingData> mergeKnnQueryTiming(
      QueryExecutionContextNode node, Map<ExplainTimings.Type, TimingData> originalTimings) {
    var mustChildren = node.getChildren().map(QueryChildren::must);
    var filterChildren = node.getChildren().map(QueryChildren::filter);
    return Streams.concat(
            originalTimings.entrySet().stream(),
            Stream.of(mustChildren, filterChildren)
                .flatMap(optChild -> optChild.map(l -> l.stream()).orElse(Stream.empty()))
                .map(QueryExecutionContextNode::getTimings)
                .flatMap(ExplainTimings::stream))
        .collect(ExplainTimings.toExplainTimingData());
  }

  private static Optional<QueryExecutionContextNode> unwrapChildNodeIfSingleChild(
      QueryExecutionContextNode node) {
    Optional<Collection<? extends QueryExecutionContextNode>> wrappedMustChildren =
        node.getChildren().map(QueryChildren::must);
    // Each WrappedQuery must have at least one child.
    var children = Check.isPresent(wrappedMustChildren, "MustChildren");

    // If there is only one child, return it instead of the wrapped query.
    if (children.size() == 1) {
      return Optional.of(children.stream().findFirst().get());
    }

    // If there are two or more children, we can't descend.
    return Optional.empty();
  }
}
