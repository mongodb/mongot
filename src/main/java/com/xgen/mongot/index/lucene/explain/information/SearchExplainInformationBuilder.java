package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;

public class SearchExplainInformationBuilder {
  private Optional<List<QueryExplainInformation>> queryInfos = Optional.empty();
  private Optional<QueryExecutionArea> allCollectorStats = Optional.empty();
  private Optional<ResourceUsageOutput> resourceUsage = Optional.empty();
  private Optional<List<VectorSearchTracingSpec>> vectorTracingInfos = Optional.empty();
  private Optional<List<VectorSearchSegmentStatsSpec>> vectorSearchSegmentStats = Optional.empty();
  private Optional<BsonDocument> vectorliteStats = Optional.empty();
  private Optional<BsonDocument> vectorliteTrace = Optional.empty();
  private Optional<VectorSearchNumCandidatesSpec> numCandidates = Optional.empty();
  private Optional<SortStats> sortStats = Optional.empty();
  private final List<SearchExplainInformation> indexPartitionExplainInformations =
      new ArrayList<>();

  private Optional<QueryExecutionArea> facetCollectorStats = Optional.empty();
  private Optional<QueryExecutionArea> createFacetCountsStats = Optional.empty();
  private Optional<Map<String, Integer>> queriedStringFacetCardinalities = Optional.empty();
  private Optional<Map<String, Integer>> totalStringFacetCardinalities = Optional.empty();
  private Optional<List<FeatureFlagEvaluationSpec>> dynamicFeatureFlags = Optional.empty();
  private Optional<List<String>> optimizerRulesFired = Optional.empty();

  public Optional<HighlightStats> highlightStats = Optional.empty();

  public Optional<ResultMaterializationStats> resultMaterializationStats = Optional.empty();

  private Optional<MetadataExplainInformation> metadata = Optional.empty();

  private Optional<AggregationStats> aggregationStats = Optional.empty();

  public static SearchExplainInformationBuilder newBuilder() {
    return new SearchExplainInformationBuilder();
  }

  public SearchExplainInformationBuilder addIndexPartitionExplainInformation(
      SearchExplainInformation explainInformation) {
    this.indexPartitionExplainInformations.add(explainInformation);
    return this;
  }

  public SearchExplainInformationBuilder queryExplainInfos(List<QueryExplainInformation> query) {
    this.queryInfos = Optional.of(query);
    return this;
  }

  public SearchExplainInformationBuilder allCollectorStats(QueryExecutionArea allCollectorStats) {
    this.allCollectorStats = Optional.of(allCollectorStats);
    return this;
  }

  public SearchExplainInformationBuilder facetCollectorStats(
      QueryExecutionArea facetCollectorStats) {
    this.facetCollectorStats = Optional.of(facetCollectorStats);
    return this;
  }

  public SearchExplainInformationBuilder createFacetCountsStats(
      QueryExecutionArea createFacetCountsStats) {
    this.createFacetCountsStats = Optional.of(createFacetCountsStats);
    return this;
  }

  public SearchExplainInformationBuilder queriedStringFacetCardinalities(
      Map<String, Integer> queriedStringFacetCardinalities) {
    this.queriedStringFacetCardinalities = Optional.of(queriedStringFacetCardinalities);
    return this;
  }

  public SearchExplainInformationBuilder totalStringFacetCardinalities(
      Map<String, Integer> totalStringFacetCardinalities) {
    this.totalStringFacetCardinalities = Optional.of(totalStringFacetCardinalities);
    return this;
  }

  public SearchExplainInformationBuilder resourceUsage(ResourceUsageOutput resourceUsage) {
    this.resourceUsage = Optional.of(resourceUsage);
    return this;
  }

  public SearchExplainInformationBuilder sortStats(SortStats sortStats) {
    this.sortStats = Optional.of(sortStats);
    return this;
  }

  public SearchExplainInformationBuilder highlightStats(HighlightStats highlightStats) {
    this.highlightStats = Optional.of(highlightStats);
    return this;
  }

  public SearchExplainInformationBuilder vectorSearchTracingInfos(
      List<VectorSearchTracingSpec> vectorTracingInfos) {
    this.vectorTracingInfos = Optional.of(vectorTracingInfos);
    return this;
  }

  public SearchExplainInformationBuilder vectorSearchSegmentStats(
      List<VectorSearchSegmentStatsSpec> vectorSearchSegmentStats) {
    this.vectorSearchSegmentStats = Optional.of(vectorSearchSegmentStats);
    return this;
  }

  public SearchExplainInformationBuilder vectorliteStats(BsonDocument vectorliteStats) {
    this.vectorliteStats = Optional.of(vectorliteStats);
    return this;
  }

  public SearchExplainInformationBuilder vectorliteTrace(BsonDocument vectorliteTrace) {
    this.vectorliteTrace = Optional.of(vectorliteTrace);
    return this;
  }

  public SearchExplainInformationBuilder numCandidates(
      VectorSearchNumCandidatesSpec numCandidates) {
    this.numCandidates = Optional.of(numCandidates);
    return this;
  }

  public SearchExplainInformationBuilder dynamicFeatureFlags(
      List<FeatureFlagEvaluationSpec> dynamicFeatureFlags) {
    this.dynamicFeatureFlags = Optional.of(dynamicFeatureFlags);
    return this;
  }

  public SearchExplainInformationBuilder optimizerRulesFired(List<String> optimizerRulesFired) {
    this.optimizerRulesFired = Optional.of(optimizerRulesFired);
    return this;
  }

  public SearchExplainInformationBuilder resultMaterializationStats(
      ResultMaterializationStats resultMaterializationStats) {
    this.resultMaterializationStats = Optional.of(resultMaterializationStats);
    return this;
  }

  public SearchExplainInformationBuilder aggregationStats(AggregationStats aggregationStats) {
    this.aggregationStats = Optional.of(aggregationStats);
    return this;
  }

  public SearchExplainInformationBuilder metadata(MetadataExplainInformation metadata) {
    this.metadata = Optional.of(metadata);
    return this;
  }

  /** Builds SearchExplainInformation from an SearchExplainInformationBuilder. */
  public SearchExplainInformation build() {
    if (this.indexPartitionExplainInformations.isEmpty()) {
      Optional<CollectorExplainInformation> collectStats =
          CollectorExplainInformation.create(
              this.allCollectorStats,
              this.facetCollectorStats,
              this.createFacetCountsStats,
              this.queriedStringFacetCardinalities,
              this.totalStringFacetCardinalities,
              this.sortStats);

      return new SearchExplainInformation(
          this.queryInfos,
          collectStats,
          this.highlightStats,
          this.resultMaterializationStats,
          this.aggregationStats,
          this.metadata,
          this.resourceUsage,
          this.vectorTracingInfos,
          this.vectorSearchSegmentStats,
          this.vectorliteStats,
          this.vectorliteTrace,
          this.numCandidates,
          Optional.empty(),
          this.dynamicFeatureFlags,
          this.optimizerRulesFired);
    }

    // Query/collect/highlight land in indexPartitionExplain. aggregation stays top-level because
    // partitionMerge is a cross-partition phase recorded after the per-partition fan-out joins.
    // vectorliteStats and vectorliteTrace also stay top-level: vectorlite indexes are
    // single-partition.
    return new SearchExplainInformation(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        this.aggregationStats,
        this.metadata,
        this.resourceUsage,
        Optional.empty(),
        Optional.empty(),
        this.vectorliteStats,
        this.vectorliteTrace,
        Optional.empty(),
        Optional.of(this.indexPartitionExplainInformations),
        this.dynamicFeatureFlags,
        this.optimizerRulesFired);
  }
}
