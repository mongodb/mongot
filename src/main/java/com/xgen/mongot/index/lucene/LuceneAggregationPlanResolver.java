package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.aggregation.AggregationFieldResolver;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry;
import com.xgen.mongot.index.query.aggregation.TopAggregationSpec;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.AggregationSpecBuilder;
import com.xgen.mongot.index.version.IndexFormatVersion;
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
            Optional.of(boundaryFieldResolver(facetContext)));
    AggregationFieldResolver fieldResolver = contextFieldResolver.withResolvedFields(rootSpec);
    return new AggregationPlan(rootSpec, fieldResolver);
  }

  /**
   * Builds a {@link AggregationSpecBuilder.BoundaryFieldResolver} for shard merge planning from
   * index definition metadata (no live Lucene reader required).
   */
  public static AggregationSpecBuilder.BoundaryFieldResolver boundaryFieldResolverForPlanning(
      SearchIndexDefinition indexDefinition, IndexFormatVersion indexFormatVersion) {
    SearchFieldDefinitionResolver fieldResolver =
        indexDefinition.createFieldDefinitionResolver(indexFormatVersion);
    LuceneFacetContext facetContext =
        new LuceneFacetContext(
            fieldResolver, indexDefinition.getIndexCapabilities(indexFormatVersion));
    return boundaryFieldResolver(facetContext);
  }

  static AggregationSpecBuilder.BoundaryFieldResolver boundaryFieldResolver(
      LuceneFacetContext facetContext) {
    return new FacetContextBoundaryResolver(
        new LuceneAggregationContextFieldResolver(facetContext));
  }
}
