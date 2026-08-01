package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.index.lucene.LuceneFacetOptimizedDrillSidewaysSearchManager.OptimizedDrillSidewaysFacetCollectorQueryInfo;
import static com.xgen.mongot.index.lucene.LuceneSearchManager.QueryInfo;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.FieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.LuceneAggregationPlanResolver.AggregationPlan;
import com.xgen.mongot.index.lucene.LuceneAggregationSearchManager.AggregationQueryInfo;
import com.xgen.mongot.index.lucene.LuceneFacetCollectorSearchManager.FacetCollectorQueryInfo;
import com.xgen.mongot.index.lucene.LuceneFacetGenericDrillSidewaysSearchManager.GenericDrillSidewaysResultFacetCollectorQueryInfo;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedVectorRescorer;
import com.xgen.mongot.index.lucene.query.NestedAvgVectorRescorer;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.index.query.operators.VectorEmbeddedOptions;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

class LuceneSearchManagerFactory {

  private final FieldDefinitionResolver fieldDefinitionResolver;
  private final BinaryQuantizedVectorRescorer rescorer;
  private final IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater;

  LuceneSearchManagerFactory(
      FieldDefinitionResolver fieldDefinitionResolver,
      BinaryQuantizedVectorRescorer rescorer,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater) {
    this.fieldDefinitionResolver = fieldDefinitionResolver;
    this.rescorer = rescorer;
    this.metricsUpdater = metricsUpdater;
  }

  LuceneSearchManager<FacetCollectorQueryInfo> newFacetCollectorManager(
      Query query, Optional<Sort> luceneSort, Optional<SequenceToken> searchAfter) {
    return new MeteredLuceneSearchManager<>(
        this.metricsUpdater, new LuceneFacetCollectorSearchManager(query, luceneSort, searchAfter));
  }

  LuceneSearchManager<AggregationQueryInfo> newAggregationManager(
      Query query,
      Optional<Sort> luceneSort,
      Optional<Sort> invertedLuceneSort,
      Optional<SequenceToken> searchAfter,
      AggregationPlan aggregationPlan,
      int indexPartitionId) {
    return new MeteredLuceneSearchManager<>(
        this.metricsUpdater,
        new LuceneAggregationSearchManager(
            query, luceneSort, invertedLuceneSort, searchAfter, aggregationPlan, indexPartitionId));
  }

  LuceneSearchManager<QueryInfo> newOperatorManager(
      OperatorQuery operatorQuery,
      Query luceneQuery,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter) {

    Count count = operatorQuery.count();
    if (operatorQuery.operator() instanceof VectorSearchOperator vectorSearchOperator) {
      return newCachingVectorQueryManager(luceneQuery, vectorSearchOperator.criteria());
    }

    return new MeteredLuceneSearchManager<>(
        this.metricsUpdater,
        new LuceneOperatorSearchManager(luceneQuery, count, luceneSort, searchAfter));
  }

  private LuceneSearchManager<QueryInfo> newCachingVectorQueryManager(
      Query luceneQuery, VectorSearchCriteria criteria) {

    VectorQuantization quantization = this.resolveQuantization(criteria.path());
    boolean requiresRescoring = requiresRescoring(criteria, quantization);
    boolean requiresNestedAvgRescoring = requiresNestedAvgRescoring(criteria);
    LuceneSearchManager<QueryInfo> innerSearchManager =
        new LuceneCachingVectorSearchManager(
            luceneQuery,
            criteria,
            requiresRescoring ? Optional.of(this.rescorer) : Optional.empty(),
            requiresNestedAvgRescoring
                ? Optional.of(new NestedAvgVectorRescorer())
                : Optional.empty());
    return new MeteredLuceneVectorSearchManager<>(
        this.metricsUpdater, innerSearchManager, criteria, quantization);
  }

  LuceneSearchManager<QueryInfo> newVectorQueryManager(
      Query luceneQuery, VectorSearchCriteria criteria) {

    VectorQuantization quantization = this.resolveQuantization(criteria.path());
    boolean requiresRescoring = requiresRescoring(criteria, quantization);
    boolean requiresNestedAvgRescoring = requiresNestedAvgRescoring(criteria);
    LuceneSearchManager<QueryInfo> innerSearchManager =
        new LuceneVectorSearchManager(
            luceneQuery,
            criteria,
            requiresRescoring ? Optional.of(this.rescorer) : Optional.empty(),
            requiresNestedAvgRescoring
                ? Optional.of(new NestedAvgVectorRescorer())
                : Optional.empty());
    return new MeteredLuceneVectorSearchManager<>(
        this.metricsUpdater, innerSearchManager, criteria, quantization);
  }

  private boolean requiresRescoring(
      VectorSearchCriteria criteria, VectorQuantization quantization) {
    Vector vector =
        Check.isPresent(criteria.queryVector(), "criteria has to be materialized up to this point");
    return quantization == VectorQuantization.BINARY
        && (vector.getVectorType() == Vector.VectorType.FLOAT
            || vector.getVectorType() == Vector.VectorType.BYTE)
        && criteria.getVectorSearchType() == VectorSearchCriteria.Type.APPROXIMATE;
  }

  private static boolean requiresNestedAvgRescoring(VectorSearchCriteria criteria) {
    return criteria.getVectorSearchType() != VectorSearchCriteria.Type.EXACT
        && criteria
            .embeddedOptions()
            .map(opts -> opts.scoreMode() == VectorEmbeddedOptions.ScoreMode.AVG)
            .orElse(false);
  }

  private VectorQuantization resolveQuantization(FieldPath path) {
    return this.fieldDefinitionResolver
        .getVectorFieldSpecification(path)
        .map(VectorFieldSpecification::quantization)
        .orElse(VectorQuantization.NONE);
  }

  LuceneSearchManager<GenericDrillSidewaysResultFacetCollectorQueryInfo>
      newFacetGenericDrillSidewaysCollectorManager(
          Query query,
          Map<String, LuceneDrillSideways> facetToDrillSidewaysFacetQueries,
          Optional<Sort> luceneSort,
          Optional<SequenceToken> searchAfter,
          boolean concurrentQuery,
          Optional<NamedExecutorService> concurrentSearchExecutor,
          DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {

    boolean concurrencyEnabled =
        concurrentQuery
            && dynamicFeatureFlagRegistry.evaluateClusterInvariant(
                DynamicFeatureFlags.DRILL_SIDEWAYS_CONCURRENCY.getName(),
                DynamicFeatureFlags.DRILL_SIDEWAYS_CONCURRENCY.getFallback());

    Optional<NamedExecutorService> validExecutor =
        concurrencyEnabled ? concurrentSearchExecutor : Optional.empty();

    return new MeteredLuceneSearchManager<>(
        this.metricsUpdater,
        new LuceneFacetGenericDrillSidewaysSearchManager(
            query, facetToDrillSidewaysFacetQueries, luceneSort, searchAfter, validExecutor));
  }

  LuceneSearchManager<OptimizedDrillSidewaysFacetCollectorQueryInfo>
      newOptimizedDrillSidewaysCollectorManager(
          Query query,
          LuceneDrillSideways drillSideways,
          Optional<Sort> luceneSort,
          Optional<SequenceToken> searchAfter) {
    return new MeteredLuceneSearchManager<>(
        this.metricsUpdater,
        new LuceneFacetOptimizedDrillSidewaysSearchManager(
            query, drillSideways, luceneSort, searchAfter));
  }
}
