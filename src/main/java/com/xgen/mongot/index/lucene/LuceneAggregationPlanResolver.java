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

  public static AggregationPlan resolveAggregationPlan(
      LuceneFacetContext facetContext, AggregationCollector aggregationCollector)
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver contextFieldResolver =
        new LuceneAggregationContextFieldResolver(facetContext);
    TopAggregationSpec rootSpec =
        AggregationSpecBuilder.toTopLevelSpec(
            aggregationCollector,
            AggregationRegistry.getInstance(),
            Optional.of(new FacetContextBoundaryResolver(contextFieldResolver)));
    AggregationFieldResolver fieldResolver = contextFieldResolver.withResolvedFields(rootSpec);
    return new AggregationPlan(rootSpec, fieldResolver);
  }
}
