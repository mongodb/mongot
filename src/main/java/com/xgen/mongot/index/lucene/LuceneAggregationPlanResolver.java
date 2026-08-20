package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.aggregation.AggregationFieldResolver;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry;
import com.xgen.mongot.index.query.aggregation.TopAggregationSpec;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.AggregationSpecBuilder;
import java.util.Optional;

/**
 * Resolves the aggregation spec tree and metric field paths before Lucene execution so invalid
 * queries surface as {@link InvalidQueryException} to callers.
 */
public final class LuceneAggregationPlanResolver {

  public record AggregationPlan(
      TopAggregationSpec rootSpec, AggregationFieldResolver fieldResolver) {}

  private LuceneAggregationPlanResolver() {}

  /**
   * Resolves the aggregation spec tree and its metric field paths.
   *
   * @param returnStoredSource the query's {@code returnStoredSource}, which decides where
   *     token-valued metrics read their values from
   */
  public static AggregationPlan resolveAggregationPlan(
      LuceneFacetContext facetContext,
      AggregationCollector aggregationCollector,
      boolean returnStoredSource)
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver contextFieldResolver =
        new LuceneAggregationContextFieldResolver(facetContext);
    TopAggregationSpec rootSpec =
        AggregationSpecBuilder.toTopLevelSpec(
            aggregationCollector,
            AggregationRegistry.getInstance(),
            Optional.of(boundaryFieldResolver(facetContext)));
    AggregationFieldResolver fieldResolver =
        contextFieldResolver.withResolvedFields(rootSpec, returnStoredSource);
    return new AggregationPlan(rootSpec, fieldResolver);
  }

  static AggregationSpecBuilder.BoundaryFieldResolver boundaryFieldResolver(
      LuceneFacetContext facetContext) {
    return new FacetContextBoundaryResolver(
        new LuceneAggregationContextFieldResolver(facetContext));
  }
}
