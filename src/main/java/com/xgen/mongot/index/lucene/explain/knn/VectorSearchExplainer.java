package com.xgen.mongot.index.lucene.explain.knn;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.information.VectorSearchNumCandidatesSpec;
import com.xgen.mongot.index.lucene.explain.information.VectorSearchSegmentStatsSpec;
import com.xgen.mongot.index.lucene.explain.information.VectorSearchTracingSpec;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.search.KnnCollector;
import org.bson.BsonValue;

public class VectorSearchExplainer implements FeatureExplainer {

  private final Map<Object, SegmentStatistics> segmentStats = new ConcurrentHashMap<>();

  private Optional<VectorSearchNumCandidatesSpec> numCandidates = Optional.empty();

  private final ImmutableMap<Integer, TracingInformation> tracingInfos;

  public VectorSearchExplainer() {
    this(List.of());
  }

  public VectorSearchExplainer(List<TracingTarget> tracingTargets) {
    this.tracingInfos =
        tracingTargets.stream()
            .collect(toImmutableMap((target) -> target.luceneDocId, TracingInformation::new));
  }

  public SegmentStatistics getSegmentStats(Object contextId) {
    return this.segmentStats.computeIfAbsent(contextId, unused -> new SegmentStatistics());
  }

  public void setNumCandidates(VectorSearchNumCandidatesSpec numCandidates) {
    this.numCandidates = Optional.of(numCandidates);
  }

  public Collection<TracingInformation> getTracingInformationWithVectors() {
    return this.tracingInfos.values().stream().filter(info -> !info.target.hasNoVector()).toList();
  }

  public Optional<TracingInformation> getTracingInformation(int targetLuceneDocId) {
    return Optional.ofNullable(this.tracingInfos.get(targetLuceneDocId));
  }

  @Override
  public void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    // The resolved numCandidates is a query-planning decision (computed before execution), so
    // surface it even at queryPlanner verbosity. It is only set in auto mode (see
    // VectorSearchQueryFactory#setAutoNumCandidatesExplain), so explicit-numCandidates queries emit
    // nothing here. Segment stats and tracing below are execution-level and stay behind the guard.
    this.numCandidates.ifPresent(builder::numCandidates);

    if (!verbosity.isGreaterThan(Explain.Verbosity.QUERY_PLANNER)) {
      return;
    }

    List<VectorSearchSegmentStatsSpec> vectorSearchSegmentStats = convertSegmentStatisticsToSpec();
    if (!vectorSearchSegmentStats.isEmpty()) {
      builder.vectorSearchSegmentStats(vectorSearchSegmentStats);
    }

    List<VectorSearchTracingSpec> tracingInfos =
        this.tracingInfos.values().stream()
            .map(
                tracingInfo ->
                    new VectorSearchTracingSpec(
                        tracingInfo.target.docId,
                        tracingInfo.unreachable,
                        tracingInfo.target.hasNoVector,
                        tracingInfo.visited,
                        tracingInfo.score,
                        tracingInfo.segmentId,
                        tracingInfo.dropReason))
            .toList();

    if (!tracingInfos.isEmpty()) {
      builder.vectorSearchTracingInfos(tracingInfos);
    }
  }

  private List<VectorSearchSegmentStatsSpec> convertSegmentStatisticsToSpec() {
    return this.segmentStats.values().stream()
        .map(
            (stats) -> {
              Optional<QueryExecutionArea> approximateExecutionArea =
                  QueryExecutionArea.notEmptyAreaForType(
                      ExplainTimings.Type.VECTOR_SEARCH_APPROXIMATE,
                      stats.timings.extractTimingData());
              Optional<QueryExecutionArea> exactExecutionArea =
                  QueryExecutionArea.notEmptyAreaForType(
                      ExplainTimings.Type.VECTOR_SEARCH_EXACT, stats.timings.extractTimingData());
              Optional<QueryExecutionArea> fullScanHeuristicExecutionArea =
                  QueryExecutionArea.notEmptyAreaForType(
                      ExplainTimings.Type.VECTOR_SEARCH_FULL_SCAN_HEURISTIC,
                      stats.timings.extractTimingData());

              VectorSearchSegmentStatsSpec.SegmentExecutionType executionType =
                  getExecutionType(
                      approximateExecutionArea, exactExecutionArea, fullScanHeuristicExecutionArea);

              return new VectorSearchSegmentStatsSpec(
                  stats.getSegmentId(),
                  executionType,
                  stats.getDocCount(),
                  stats.getAccumulatedVisitedDocs(),
                  approximateExecutionArea,
                  exactExecutionArea,
                  stats.getFilterMatchedDocsCount());
            })
        .toList();
  }

  private VectorSearchSegmentStatsSpec.SegmentExecutionType getExecutionType(
      Optional<QueryExecutionArea> approximateExecutionArea,
      Optional<QueryExecutionArea> exactExecutionArea,
      Optional<QueryExecutionArea> fullScanHeuristicExecutionArea) {
    boolean isApproximateFallback =
        approximateExecutionArea.isPresent() && exactExecutionArea.isPresent();
    boolean isFullScanHeuristic = fullScanHeuristicExecutionArea.isPresent();

    if (isFullScanHeuristic) {
      return VectorSearchSegmentStatsSpec.SegmentExecutionType.FULL_SCAN_HEURISTIC;
    } else if (isApproximateFallback) {
      return VectorSearchSegmentStatsSpec.SegmentExecutionType.APPROXIMATE_FALLBACK_TO_EXACT;
    } else if (exactExecutionArea.isPresent()) {
      return VectorSearchSegmentStatsSpec.SegmentExecutionType.EXACT;
    } else {
      return VectorSearchSegmentStatsSpec.SegmentExecutionType.APPROXIMATE;
    }
  }

  public static class SegmentStatistics {
    private Optional<String> segmentId = Optional.empty();
    private int docCount;
    private Optional<Integer> filterMatchedDocsCount = Optional.empty();
    private ExplainTimings timings = ExplainTimings.builder().ignoreInvocationCounts().build();
    private long accumulatedVisitedDocs = 0;
    private Optional<KnnCollector> knnCollector = Optional.empty();

    public Optional<String> getSegmentId() {
      return this.segmentId;
    }

    public SegmentStatistics setSegmentId(Optional<String> segmentId) {
      this.segmentId = segmentId;
      return this;
    }

    public ExplainTimings getTimings() {
      return this.timings;
    }

    @VisibleForTesting
    void setTimings(ExplainTimings timings) {
      this.timings = timings;
    }

    public SegmentStatistics setFilterMatchedDocsCount(int filterMatchedDocsCount) {
      this.filterMatchedDocsCount = Optional.of(filterMatchedDocsCount);
      return this;
    }

    Optional<Integer> getFilterMatchedDocsCount() {
      return this.filterMatchedDocsCount;
    }

    public SegmentStatistics setDocCount(int docCount) {
      this.docCount = docCount;
      return this;
    }

    @VisibleForTesting
    int getDocCount() {
      return this.docCount;
    }

    public long getAccumulatedVisitedDocs() {
      return this.accumulatedVisitedDocs
          + this.knnCollector.map(KnnCollector::visitedCount).orElse(0L);
    }

    public SegmentStatistics replaceCollectorAndMergeStats(KnnCollector collector) {
      this.knnCollector.ifPresent(value -> this.accumulatedVisitedDocs += value.visitedCount());
      this.knnCollector = Optional.of(collector);
      return this;
    }
  }

  public record TracingTarget(BsonValue docId, int luceneDocId, boolean hasNoVector) {}

  public static class TracingInformation {
    private final TracingTarget target;
    private boolean visited;
    private Optional<VectorSearchTracingSpec.DropReason> dropReason = Optional.empty();
    private Optional<Float> score = Optional.empty();
    private Optional<String> segmentId = Optional.empty();
    private boolean unreachable;
    private Optional<VectorSearchSegmentStatsSpec.SegmentExecutionType> executionType =
        Optional.empty();

    public TracingInformation(TracingTarget target) {
      this.target = target;
    }

    public TracingTarget getTarget() {
      return this.target;
    }

    public void setVisited(boolean visited) {
      this.visited = visited;
    }

    public boolean isVisited() {
      return this.visited;
    }

    public void setScore(float score) {
      this.score = Optional.of(score);
    }

    public void setDropReason(VectorSearchTracingSpec.DropReason dropReason) {
      this.dropReason = Optional.of(dropReason);
    }

    public Optional<VectorSearchTracingSpec.DropReason> getDropReason() {
      return this.dropReason;
    }

    public void setSegmentId(Optional<String> segmentId) {
      this.segmentId = segmentId;
    }

    public void markAsUnreachable() {
      this.unreachable = true;
    }

    public boolean isUnreachable() {
      return this.unreachable;
    }

    public Optional<VectorSearchSegmentStatsSpec.SegmentExecutionType> getExecutionType() {
      return this.executionType;
    }

    public void setExecutionType(VectorSearchSegmentStatsSpec.SegmentExecutionType executionType) {
      this.executionType = Optional.of(executionType);
    }
  }
}
