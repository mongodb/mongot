package com.xgen.mongot.index.lucene.explain.knn;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Ticker;
import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.information.VectorSearchNumCandidatesSpec;
import com.xgen.mongot.index.lucene.explain.information.VectorSearchSegmentStatsSpec;
import com.xgen.mongot.index.lucene.explain.information.VectorSearchTracingSpec;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.util.timers.ThreadSafeInvocationCountingTimer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.search.KnnCollector;
import org.bson.BsonInt32;
import org.junit.Test;

public class VectorSearchExplainerTest {

  @Test
  public void emitExplanation_runsWithApproximateSegment_explainOutputIsSetCorrectly() {
    int docCount = 10;
    long visitedDocCount = 6L;
    long approximateNanos = 10000000;

    VectorSearchExplainer vectorSearchExplainer = new VectorSearchExplainer();
    vectorSearchExplainer
        .getSegmentStats("id")
        .setSegmentId(Optional.of("_0"))
        .setDocCount(docCount)
        .replaceCollectorAndMergeStats(getMockKnnCollector(visitedDocCount))
        .setTimings(
            getMockedExplainTimings(
                Map.of(ExplainTimings.Type.VECTOR_SEARCH_APPROXIMATE, approximateNanos)));
    SearchExplainInformationBuilder builder = SearchExplainInformationBuilder.newBuilder();
    vectorSearchExplainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

    List<VectorSearchSegmentStatsSpec> vectorSearchSegmentStatsMap =
        builder.build().vectorSearchSegmentStats().get();

    var expected =
        new VectorSearchSegmentStatsSpec(
            Optional.of("_0"),
            VectorSearchSegmentStatsSpec.SegmentExecutionType.APPROXIMATE,
            docCount,
            visitedDocCount,
            Optional.of(
                new QueryExecutionArea((double) approximateNanos / 1000000, Optional.empty())),
            Optional.empty(),
            Optional.empty());
    Truth.assertWithMessage("Expected %s in %s", expected, vectorSearchSegmentStatsMap)
        .that(vectorSearchSegmentStatsMap)
        .containsExactly(expected);
  }

  @Test
  public void emitExplanation_runsWithExactSegmentAndFilter_explainOutputIsSetCorrectly() {
    int docCount = 10;
    long visitedDocCount = 6L;
    int filterMatchedDocsCount = 4;
    long exactNanos = 10000000;

    VectorSearchExplainer vectorSearchExplainer = new VectorSearchExplainer();
    vectorSearchExplainer
        .getSegmentStats("id")
        .setSegmentId(Optional.of("_0"))
        .setDocCount(docCount)
        .replaceCollectorAndMergeStats(getMockKnnCollector(visitedDocCount))
        .setFilterMatchedDocsCount(filterMatchedDocsCount)
        .setTimings(
            getMockedExplainTimings(Map.of(ExplainTimings.Type.VECTOR_SEARCH_EXACT, exactNanos)));

    SearchExplainInformationBuilder builder = SearchExplainInformationBuilder.newBuilder();
    vectorSearchExplainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

    List<VectorSearchSegmentStatsSpec> vectorSearchSegmentStatsMap =
        builder.build().vectorSearchSegmentStats().get();

    var expected =
        new VectorSearchSegmentStatsSpec(
            Optional.of("_0"),
            VectorSearchSegmentStatsSpec.SegmentExecutionType.EXACT,
            docCount,
            visitedDocCount,
            Optional.empty(),
            Optional.of(new QueryExecutionArea((double) exactNanos / 1000000, Optional.empty())),
            Optional.of(filterMatchedDocsCount));
    Truth.assertWithMessage("Expected %s in %s", expected, vectorSearchSegmentStatsMap)
        .that(vectorSearchSegmentStatsMap)
        .containsExactly(expected);
  }

  @Test
  public void
      emitExplanation_runsWithApproximateFallbackToExactSegmentAndFilter_outputIsSetCorrectly() {
    int docCount = 10;
    long visitedDocCount = 6L;
    int filterMatchedDocsCount = 4;
    long approximateNanos = 10000000;
    long exactNanos = 10000000;

    VectorSearchExplainer vectorSearchExplainer = new VectorSearchExplainer();
    vectorSearchExplainer
        .getSegmentStats("id")
        .setSegmentId(Optional.of("_0"))
        .setDocCount(docCount)
        .replaceCollectorAndMergeStats(getMockKnnCollector(visitedDocCount))
        .setFilterMatchedDocsCount(filterMatchedDocsCount)
        .setTimings(
            getMockedExplainTimings(
                Map.of(
                    ExplainTimings.Type.VECTOR_SEARCH_APPROXIMATE, approximateNanos,
                    ExplainTimings.Type.VECTOR_SEARCH_EXACT, exactNanos)));

    SearchExplainInformationBuilder builder = SearchExplainInformationBuilder.newBuilder();
    vectorSearchExplainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

    List<VectorSearchSegmentStatsSpec> vectorSearchSegmentStatsMap =
        builder.build().vectorSearchSegmentStats().get();

    var expected =
        new VectorSearchSegmentStatsSpec(
            Optional.of("_0"),
            VectorSearchSegmentStatsSpec.SegmentExecutionType.APPROXIMATE_FALLBACK_TO_EXACT,
            docCount,
            visitedDocCount,
            Optional.of(
                new QueryExecutionArea((double) approximateNanos / 1000000, Optional.empty())),
            Optional.of(new QueryExecutionArea((double) exactNanos / 1000000, Optional.empty())),
            Optional.of(filterMatchedDocsCount));
    Truth.assertWithMessage("Expected %s in %s", expected, vectorSearchSegmentStatsMap)
        .that(vectorSearchSegmentStatsMap)
        .containsExactly(expected);
  }

  @Test
  public void emitExplanation_runsWithReachableAndUnreachableDoc_tracingOutputIsSetCorrectly() {
    Optional<String> segmentId = Optional.of("_0");

    BsonInt32 docIdOne = new BsonInt32(1);
    VectorSearchExplainer.TracingTarget targetOne =
        new VectorSearchExplainer.TracingTarget(docIdOne, 1, false);

    BsonInt32 docIdTwo = new BsonInt32(2);
    VectorSearchExplainer.TracingTarget targetTwo =
        new VectorSearchExplainer.TracingTarget(docIdTwo, 2, false);
    VectorSearchExplainer vectorSearchExplainer =
        new VectorSearchExplainer(List.of(targetOne, targetTwo));

    VectorSearchExplainer.TracingInformation tracingInformationOne =
        vectorSearchExplainer.getTracingInformation(1).get();
    tracingInformationOne.setSegmentId(segmentId);
    float docOneScore = 0.5f;
    tracingInformationOne.setScore(docOneScore);
    tracingInformationOne.setVisited(true);
    tracingInformationOne.setDropReason(VectorSearchTracingSpec.DropReason.MERGE);

    VectorSearchExplainer.TracingInformation tracingInformationTwo =
        vectorSearchExplainer.getTracingInformation(2).get();
    tracingInformationTwo.setSegmentId(segmentId);
    tracingInformationTwo.setVisited(false);
    tracingInformationTwo.markAsUnreachable();

    SearchExplainInformationBuilder builder = SearchExplainInformationBuilder.newBuilder();
    vectorSearchExplainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

    List<VectorSearchTracingSpec> tracingSpecs = builder.build().vectorSearchTracingInfo().get();

    var expected =
        List.of(
            new VectorSearchTracingSpec(
                docIdOne,
                false,
                false,
                true,
                Optional.of(docOneScore),
                segmentId,
                Optional.of(VectorSearchTracingSpec.DropReason.MERGE)),
            new VectorSearchTracingSpec(
                docIdTwo, true, false, false, Optional.empty(), segmentId, Optional.empty()));
    Truth.assertWithMessage("Expected %s in %s", expected, tracingSpecs)
        .that(tracingSpecs)
        .isEqualTo(expected);
  }

  @Test
  public void emitExplanation_runsForDocumentWithNoVector_tracingOutputIsSetCorrectly() {
    Optional<String> segmentId = Optional.of("_0");

    BsonInt32 docIdOne = new BsonInt32(1);
    VectorSearchExplainer.TracingTarget targetOne =
        new VectorSearchExplainer.TracingTarget(docIdOne, 1, true);

    VectorSearchExplainer vectorSearchExplainer = new VectorSearchExplainer(List.of(targetOne));

    VectorSearchExplainer.TracingInformation tracingInformation =
        vectorSearchExplainer.getTracingInformation(1).get();
    tracingInformation.setSegmentId(segmentId);
    float docOneScore = 0.5f;
    tracingInformation.setScore(docOneScore);

    SearchExplainInformationBuilder builder = SearchExplainInformationBuilder.newBuilder();
    vectorSearchExplainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

    List<VectorSearchTracingSpec> tracingSpecs = builder.build().vectorSearchTracingInfo().get();

    var expected =
        List.of(
            new VectorSearchTracingSpec(
                docIdOne,
                false,
                true,
                false,
                Optional.of(docOneScore),
                segmentId,
                Optional.empty()));
    Truth.assertWithMessage("Expected %s in %s", expected, tracingSpecs)
        .that(tracingSpecs)
        .isEqualTo(expected);
  }

  @Test
  public void emitExplanation_explanationIsNotEmittedIfVerbosityIsQueryPlanner_outputIsEmpty() {
    int docCount = 10;
    int filterMatchedDocsCount = 4;
    long approximateNanos = 10000000;
    long exactNanos = 10000000;

    VectorSearchExplainer vectorSearchExplainer = new VectorSearchExplainer();
    vectorSearchExplainer
        .getSegmentStats("id")
        .setSegmentId(Optional.of("_0"))
        .setDocCount(docCount)
        .setFilterMatchedDocsCount(filterMatchedDocsCount)
        .setTimings(
            getMockedExplainTimings(
                Map.of(
                    ExplainTimings.Type.VECTOR_SEARCH_APPROXIMATE, approximateNanos,
                    ExplainTimings.Type.VECTOR_SEARCH_EXACT, exactNanos)));

    SearchExplainInformationBuilder builder = SearchExplainInformationBuilder.newBuilder();
    vectorSearchExplainer.emitExplanation(Explain.Verbosity.QUERY_PLANNER, builder);

    Optional<List<VectorSearchSegmentStatsSpec>> vectorSearchSegmentStatsMap =
        builder.build().vectorSearchSegmentStats();

    Truth.assertWithMessage("%s has to be empty", vectorSearchSegmentStatsMap)
        .that(vectorSearchSegmentStatsMap)
        .isEmpty();
  }

  @Test
  public void emitExplanation_autoNumCandidatesSetAtQueryPlanner_numCandidatesIsEmitted() {
    // numCandidates is a plan-time decision, so it must appear even at queryPlanner verbosity.
    // Execution-level segment stats stay suppressed at this verbosity.
    VectorSearchExplainer vectorSearchExplainer = new VectorSearchExplainer();
    vectorSearchExplainer.setNumCandidates(
        new VectorSearchNumCandidatesSpec(VectorSearchNumCandidatesSpec.Mode.AUTO, 400));
    vectorSearchExplainer.getSegmentStats("id").setSegmentId(Optional.of("_0")).setDocCount(10);

    SearchExplainInformationBuilder builder = SearchExplainInformationBuilder.newBuilder();
    vectorSearchExplainer.emitExplanation(Explain.Verbosity.QUERY_PLANNER, builder);
    var explain = builder.build();

    Truth.assertThat(explain.numCandidates())
        .hasValue(new VectorSearchNumCandidatesSpec(VectorSearchNumCandidatesSpec.Mode.AUTO, 400));
    Truth.assertWithMessage("segment stats must stay suppressed at queryPlanner")
        .that(explain.vectorSearchSegmentStats())
        .isEmpty();
  }

  @Test
  public void emitExplanation_numCandidatesNotSet_numCandidatesIsAbsentAtQueryPlanner() {
    // Explicit-numCandidates queries never set the spec, so nothing is emitted.
    VectorSearchExplainer vectorSearchExplainer = new VectorSearchExplainer();
    vectorSearchExplainer.getSegmentStats("id").setSegmentId(Optional.of("_0")).setDocCount(10);

    SearchExplainInformationBuilder builder = SearchExplainInformationBuilder.newBuilder();
    vectorSearchExplainer.emitExplanation(Explain.Verbosity.QUERY_PLANNER, builder);

    Truth.assertThat(builder.build().numCandidates()).isEmpty();
  }

  private static KnnCollector getMockKnnCollector(long visitedCount) {
    KnnCollector mockCollector = mock(KnnCollector.class);
    when(mockCollector.visitedCount()).thenReturn(visitedCount);
    return mockCollector;
  }

  private static ExplainTimings getMockedExplainTimings(Map<ExplainTimings.Type, Long> fixtures) {
    return new ExplainTimings(
        (type) ->
            fixtures.containsKey(type)
                ? new ThreadSafeInvocationCountingTimer(
                    Ticker.systemTicker(), fixtures.get(type), 0)
                : new ThreadSafeInvocationCountingTimer(Ticker.systemTicker()));
  }
}
