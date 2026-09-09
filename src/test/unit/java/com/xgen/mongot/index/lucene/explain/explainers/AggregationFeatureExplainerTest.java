package com.xgen.mongot.index.lucene.explain.explainers;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.information.AggregationNodeStatsSpec;
import com.xgen.mongot.index.lucene.explain.information.AggregationQueryStats;
import com.xgen.mongot.index.lucene.explain.information.AggregationStats;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.index.query.aggregation.AggregationNodeStats;
import com.xgen.mongot.util.Equality;
import io.opentelemetry.context.Context;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class AggregationFeatureExplainerTest {

  private static AggregationFeatureExplainer explainerWithTimings(ExplainTimings.Type... types)
      throws InterruptedException {
    var explainer = new AggregationFeatureExplainer();
    for (var type : types) {
      try (var unused = explainer.getPhaseProfile().split(type)) {
        Thread.sleep(1);
      }
    }
    return explainer;
  }

  @Test
  public void bothPhasesRecorded_emitsBothAreas() throws InterruptedException {
    var explainer =
        explainerWithTimings(
            ExplainTimings.Type.AGGREGATION_COLLECT, ExplainTimings.Type.AGGREGATION_MERGE);

    var builder = new SearchExplainInformationBuilder();
    explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

    var stats = builder.build().aggregation().get();
    Truth.assertThat(stats.collect().get().invocationCounts().get())
        .containsExactly(ExplainTimings.Type.AGGREGATION_COLLECT.getName(), 1L);
    Truth.assertThat(stats.partitionMerge().get().invocationCounts().get())
        .containsExactly(ExplainTimings.Type.AGGREGATION_MERGE.getName(), 1L);
    Truth.assertThat(stats.accumulatorCollect()).isEmpty();
    Truth.assertThat(stats.tree()).isEmpty();
    Truth.assertThat(stats.query()).isEmpty();
  }

  @Test
  public void onlyCollectRecorded_omitsMergePhase() throws InterruptedException {
    var explainer = explainerWithTimings(ExplainTimings.Type.AGGREGATION_COLLECT);

    var builder = new SearchExplainInformationBuilder();
    explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

    var stats = builder.build().aggregation().get();
    Truth.assertThat(stats.collect()).isPresent();
    Truth.assertThat(stats.accumulatorCollect()).isEmpty();
    Truth.assertThat(stats.partitionMerge()).isEmpty();
  }

  @Test
  public void accumulatorCollectRecorded_emitsItsOwnArea() throws InterruptedException {
    // The accumulator collector is profiled through ProfileLeafCollector, which records the generic
    // COLLECT type into its own timings instance rather than an AGGREGATION_* type.
    var explainer = new AggregationFeatureExplainer();
    try (var unused = explainer.getAccumulatorProfile().split(ExplainTimings.Type.COLLECT)) {
      Thread.sleep(1);
    }

    var builder = new SearchExplainInformationBuilder();
    explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

    var stats = builder.build().aggregation().get();
    Truth.assertThat(stats.accumulatorCollect().get().invocationCounts().get())
        .containsExactly(ExplainTimings.Type.COLLECT.getName(), 1L);
    Truth.assertThat(stats.collect()).isEmpty();
  }

  @Test
  public void noPhaseRecorded_omitsSectionEntirely() {
    var builder = new SearchExplainInformationBuilder();
    new AggregationFeatureExplainer().emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

    Truth.assertThat(builder.build().aggregation()).isEmpty();
  }

  @Test
  public void queryPlannerVerbosity_omitsSection() throws InterruptedException {
    var explainer =
        explainerWithTimings(
            ExplainTimings.Type.AGGREGATION_COLLECT, ExplainTimings.Type.AGGREGATION_MERGE);

    var builder = new SearchExplainInformationBuilder();
    explainer.emitExplanation(Explain.Verbosity.QUERY_PLANNER, builder);

    Truth.assertThat(builder.build().aggregation()).isEmpty();
  }

  /**
   * {@code tree} and {@code query} compare strictly: an expected section that omits them does not
   * match a runtime section that includes them.
   */
  @Test
  public void timingEquator_requiresTreeAndQueryWhenActualEmitsThem() {
    var tree =
        new AggregationNodeStatsSpec(
            "_top", Optional.empty(), Optional.empty(), Optional.empty(), 0L, 0L, Optional.empty());
    var query = new AggregationQueryStats(Optional.empty(), Optional.of(1), 1, Map.of(), 0L);
    var expected =
        new AggregationStats(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    var actual =
        new AggregationStats(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(tree),
            Optional.of(query));

    Truth.assertThat(expected.equals(actual, Equality.alwaysEqualEquator())).isFalse();
  }

  /**
   * Integration JSON fixtures omit {@code cursorId} because it is non-deterministic. The equator
   * still matches a runtime header that includes one.
   */
  @Test
  public void timingEquator_ignoresCursorIdWhenExpectedOmitsIt() {
    var tree =
        new AggregationNodeStatsSpec(
            "_top", Optional.empty(), Optional.empty(), Optional.empty(), 0L, 0L, Optional.empty());
    var expectedQuery =
        new AggregationQueryStats(Optional.empty(), Optional.of(1), 1, Map.of(), 0L);
    var actualQuery =
        new AggregationQueryStats(Optional.of(7431299184L), Optional.of(1), 1, Map.of(), 0L);
    var expected =
        new AggregationStats(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(tree),
            Optional.of(expectedQuery));
    var actual =
        new AggregationStats(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(tree),
            Optional.of(actualQuery));

    Truth.assertThat(expected.equals(actual, Equality.alwaysEqualEquator())).isTrue();
  }

  @Test
  public void timingEquator_comparesTreeWhenExpectedIncludesIt() {
    var expectedTree =
        new AggregationNodeStatsSpec(
            "_top", Optional.empty(), Optional.empty(), Optional.empty(), 0L, 0L, Optional.empty());
    var otherTree =
        new AggregationNodeStatsSpec(
            "other",
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            0L,
            0L,
            Optional.empty());
    var expected =
        new AggregationStats(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(expectedTree),
            Optional.empty());
    var actual =
        new AggregationStats(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(otherTree),
            Optional.empty());

    Truth.assertThat(expected.equals(actual, Equality.alwaysEqualEquator())).isFalse();
  }

  /**
   * Partition trees live on per-partition QueryInfos. After fan-out the current QueryInfo is the
   * root, so {@link AggregationFeatureExplainer#recordCursorId} has to walk those sub-contexts
   * rather than stamping only the root explainer.
   */
  @Test
  public void recordCursorId_stampsPartitionExplainers() throws Exception {
    var state =
        new ExplainQueryState(
            new Explain.QueryInfo(Explain.Verbosity.EXECUTION_STATS), /* numPartitions= */ 2);
    try (var unused = Context.current().with(state).makeCurrent()) {
      var tree = new AggregationNodeStats("root", List.of());
      for (int i = 0; i < 2; i++) {
        try (var unusedPartition = state.enterIndexPartitionQueryContext(i)) {
          AggregationFeatureExplainer.recordNodeStats(tree);
        }
      }

      AggregationFeatureExplainer.recordCursorId(7431299184L);

      var partitions =
          Explain.collect().orElseThrow().indexPartitionExplainInformation().orElseThrow();
      Truth.assertThat(partitions).hasSize(2);
      for (var partition : partitions) {
        Truth.assertThat(partition.aggregation().orElseThrow().query().orElseThrow().cursorId())
            .hasValue(7431299184L);
      }
    }
  }
}
