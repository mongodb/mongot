package com.xgen.mongot.index.lucene;

import com.google.common.collect.Sets;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

class OptimizedDrillSidewaysFactory {

  static LuceneDrillSideways from(
      Query baseQuery,
      Map<String, Query> drillDownQueries,
      FacetCollector collector,
      LuceneIndexSearcher searcher,
      LuceneFacetContext facetContext,
      Optional<ReturnScope> returnScope,
      boolean concurrentQuery,
      Optional<NamedExecutorService> executor,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {

    DrillDownQuery drillDownQuery = getLuceneDrillDownQuery(baseQuery, drillDownQueries, collector);
    MongotDrillSideways drillSideways =
        getOptimizedDrillSideways(
            collector,
            searcher,
            facetContext,
            returnScope.map(ReturnScope::path),
            concurrentQuery,
            executor,
            dynamicFeatureFlagRegistry);

    return new LuceneDrillSideways(drillSideways, drillDownQuery);
  }

  private static DrillDownQuery getLuceneDrillDownQuery(
      Query baseQuery, Map<String, Query> drillDownQueries, FacetCollector collector) {

    BooleanQuery baseQueryAsShould =
        new BooleanQuery.Builder()
            .add(new BooleanClause(baseQuery, BooleanClause.Occur.SHOULD))
            .build();

    // FacetsConfig is set to null since manual dimensions override its functionality
    DrillDownQuery drillDownQuery = new DrillDownQuery(null, baseQueryAsShould);

    // Add each active facet bucket as a drill-down dimension
    for (var entry : drillDownQueries.entrySet()) {
      drillDownQuery.add(entry.getKey(), entry.getValue());
    }

    // Get each non-active facet and add it as a drill-down dimension
    Set<String> nonActiveFacetDims =
        Sets.difference(collector.facetDefinitions().keySet(), drillDownQueries.keySet());
    for (String nonActiveFacetDim : nonActiveFacetDims) {
      drillDownQuery.add(nonActiveFacetDim, new MatchAllDocsQuery());
    }

    return drillDownQuery;
  }

  private static MongotDrillSideways getOptimizedDrillSideways(
      FacetCollector collector,
      LuceneIndexSearcher searcher,
      LuceneFacetContext facetContext,
      Optional<FieldPath> returnScope,
      boolean concurrentQuery,
      Optional<NamedExecutorService> executor,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {

    boolean concurrencyEnabled =
        concurrentQuery
            && dynamicFeatureFlagRegistry.evaluateClusterInvariant(
                DynamicFeatureFlags.DRILL_SIDEWAYS_CONCURRENCY.getName(),
                DynamicFeatureFlags.DRILL_SIDEWAYS_CONCURRENCY.getFallback());

    Optional<NamedExecutorService> validExecutor = concurrencyEnabled ? executor : Optional.empty();

    // Return DrillSideways instance
    return new MongotDrillSideways(
        searcher,
        LuceneFacetContext.FACETS_CONFIG,
        collector,
        facetContext,
        returnScope,
        searcher.getFacetsState(),
        searcher.getTokenFacetsStateCache(),
        validExecutor);
  }
}
