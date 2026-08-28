package com.xgen.mongot.index.lucene.explain.explainers;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
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
}
