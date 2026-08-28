package com.xgen.mongot.index.lucene.explain.explainers;

import com.xgen.mongot.index.lucene.explain.information.AggregationStats;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;
import java.util.Optional;

/**
 * Collects search aggregation phase timings for a single query, emitted as a dedicated {@code
 * aggregation} explain section rather than as part of the collector section.
 *
 * <p>Collection is recorded on whichever {@link Explain.QueryInfo} is current, so on a partitioned
 * index {@code collect} and {@code accumulatorCollect} land in that partition's explain
 * sub-document. {@code partitionMerge} is recorded on the parent query after the per-partition
 * fan-out has joined, so it is emitted at the top level.
 */
public class AggregationFeatureExplainer implements FeatureExplainer {
  /**
   * Tracks {@code IndexSearcher.search} ({@link AggregationStats#collect}) and the whole
   * partition-merge fold including {@code build()} ({@link AggregationStats#partitionMerge}).
   */
  private final ExplainTimings phaseProfile;

  /**
   * Held separately from {@link #phaseProfile} because the accumulator collector is profiled
   * through the shared {@link com.xgen.mongot.index.lucene.explain.profiler.ProfileLeafCollector},
   * which records the generic {@link ExplainTimings.Type#COLLECT}: a second instance keeps it from
   * landing in the same bucket as the query's other collectors.
   */
  private final ExplainTimings accumulatorProfile;

  public AggregationFeatureExplainer() {
    this.phaseProfile = ExplainTimings.builder().build();
    this.accumulatorProfile = ExplainTimings.builder().build();
  }

  /**
   * Phase profile for {@code AggregationStats.collect} ({@code IndexSearcher.search}) and {@code
   * AggregationStats.partitionMerge} (the whole {@code merge} fold, including {@code build()}).
   */
  public ExplainTimings getPhaseProfile() {
    return this.phaseProfile;
  }

  public ExplainTimings getAccumulatorProfile() {
    return this.accumulatorProfile;
  }

  /**
   * Profile for the accumulator collector of the aggregation on the current thread, or empty when
   * the query is not being explained. Callers wrap their {@code CollectorManager} in a {@link
   * com.xgen.mongot.index.lucene.explain.profiler.ProfileCollectorManager} with these.
   */
  public static Optional<ExplainTimings> currentAccumulatorProfile() {
    return Explain.getQueryInfo()
        .map(
            queryInfo ->
                queryInfo
                    .getFeatureExplainer(
                        AggregationFeatureExplainer.class, AggregationFeatureExplainer::new)
                    .getAccumulatorProfile());
  }

  /**
   * Profile for the aggregation phases running on the current thread, or empty when the query is
   * not being explained. Callers time a phase with {@code profile.map(t -> t.split(type))} wrapped
   * in an {@link com.xgen.mongot.util.timers.InvocationCountingTimer.AutocloseableOptional}.
   */
  public static Optional<ExplainTimings> currentPhaseProfile() {
    return Explain.getQueryInfo()
        .map(
            queryInfo ->
                queryInfo
                    .getFeatureExplainer(
                        AggregationFeatureExplainer.class, AggregationFeatureExplainer::new)
                    .getPhaseProfile());
  }

  @Override
  public void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    if (verbosity.equals(Explain.Verbosity.QUERY_PLANNER)) {
      return;
    }

    var timingData = this.phaseProfile.extractTimingData();
    AggregationStats.create(
            QueryExecutionArea.notEmptyAreaForType(
                ExplainTimings.Type.AGGREGATION_COLLECT, timingData),
            this.accumulatorProfile.allTimingDataIsEmpty()
                ? Optional.empty()
                : Optional.of(
                    QueryExecutionArea.collectAreaFor(this.accumulatorProfile.extractTimingData())),
            QueryExecutionArea.notEmptyAreaForType(
                ExplainTimings.Type.AGGREGATION_MERGE, timingData))
        .ifPresent(builder::aggregationStats);
  }
}
