package com.xgen.mongot.index.lucene.explain.explainers;

import com.xgen.mongot.index.lucene.explain.information.AggregationNodeStatsSpec;
import com.xgen.mongot.index.lucene.explain.information.AggregationQueryStats;
import com.xgen.mongot.index.lucene.explain.information.AggregationStats;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;
import com.xgen.mongot.index.query.aggregation.AggregationNodeStats;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Optional;

/**
 * Collects search aggregation phase timings and the per-node tree for a single query, emitted as a
 * dedicated {@code aggregation} explain section rather than as part of the collector section.
 *
 * <p>Collection is recorded on whichever {@link Explain.QueryInfo} is current, so on a partitioned
 * index {@code collect}, {@code accumulatorCollect} and {@code tree} land in that partition's
 * explain sub-document. {@code partitionMerge} is recorded on the parent query after the
 * per-partition fan-out has joined, so it is emitted at the top level.
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

  /** Nested per-node tree, recorded after the slice reduce. */
  private volatile Optional<AggregationNodeStatsSpec> tree = Optional.empty();

  /** The flat query-level header, assembled alongside the tree. */
  private volatile Optional<AggregationQueryStats> queryStats = Optional.empty();

  /** Set late, once the cursor exists; see {@link #recordCursorId}. */
  private volatile Optional<Long> cursorId = Optional.empty();

  /**
   * Set after collection from the partition's heap accountant; see {@link
   * #recordObservedHeapBytes}.
   */
  private volatile Optional<Long> observedHeapBytes = Optional.empty();

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

  /**
   * Records the merged per-node stats as the nested {@code aggregation.tree} and the flat {@code
   * aggregation.query} header. No-op when the query is not being explained.
   *
   * <p>Recorded on whichever {@link Explain.QueryInfo} is current, so on a partitioned index each
   * partition's explain sub-document gets its own tree -- which is correct, since the accumulator
   * trees are per partition search too.
   */
  public static void recordNodeStats(AggregationNodeStats nodeStats) {
    Optional<Explain.QueryInfo> queryInfo = Explain.getQueryInfo();
    if (queryInfo.isEmpty()) {
      return;
    }
    AggregationFeatureExplainer explainer =
        queryInfo
            .get()
            .getFeatureExplainer(
                AggregationFeatureExplainer.class, AggregationFeatureExplainer::new);
    Optional<Integer> partitionCount =
        Explain.getExplainQueryState()
            .map(ExplainQueryState::getNumPartitions)
            .filter(count -> count > 0);
    explainer.tree = Optional.of(toSpec(nodeStats));
    explainer.queryStats =
        Optional.of(
            new AggregationQueryStats(
                Optional.empty(),
                partitionCount,
                nodeStats.depth(),
                nodeStats.nodeCountsByType(),
                nodeStats.totalObservedBuckets(),
                Optional.empty()));
  }

  /**
   * Records the accumulator heap charged for this partition search onto the current explain query,
   * so {@code aggregation.query.observedHeapBytes} matches the process histogram.
   *
   * <p>No-op when the query is not being explained. Recorded separately from {@link
   * #recordNodeStats} because the accountant total is known only after {@code searcher.search}
   * returns.
   */
  public static void recordObservedHeapBytes(long observedHeapBytes) {
    Explain.getQueryInfo()
        .ifPresent(
            queryInfo ->
                queryInfo.getFeatureExplainer(
                            AggregationFeatureExplainer.class, AggregationFeatureExplainer::new)
                        .observedHeapBytes =
                    Optional.of(observedHeapBytes));
  }

  /**
   * Correlates this query's explain output with its log lines, kill errors and traces.
   *
   * <p>Recorded separately from the rest because the cursor does not exist until after the query
   * has run, and getMore batches record under the same id. Deliberately explain-only: one metric
   * series per query id would be unbounded cardinality, so nothing is ever tagged by it.
   */
  public static void recordCursorId(long cursorId) {
    Explain.getExplainQueryState()
        .ifPresent(
            state -> {
              // After partition fan-out the current QueryInfo is the root. Trees were recorded
              // inside each partition context, so stamp those explainers too -- without a factory,
              // so a partition that never aggregated does not grow an empty aggregation section.
              stampCursorId(state.getRootQueryInfo(), cursorId);
              for (Explain.QueryInfo partitionInfo : state.getIndexPartitionQueryInfos()) {
                stampCursorId(partitionInfo, cursorId);
              }
            });
  }

  private static void stampCursorId(Explain.QueryInfo queryInfo, long cursorId) {
    queryInfo
        .getFeatureExplainer(AggregationFeatureExplainer.class)
        .ifPresent(explainer -> explainer.cursorId = Optional.of(cursorId));
  }

  private static AggregationNodeStatsSpec toSpec(AggregationNodeStats stats) {
    List<AggregationNodeStatsSpec> children =
        stats.children().stream().map(AggregationFeatureExplainer::toSpec).toList();
    return new AggregationNodeStatsSpec(
        stats.name(),
        stats.type(),
        stats.path(),
        stats.requestedBuckets(),
        stats.observedBuckets(),
        stats.collectInvocations(),
        children.isEmpty() ? Optional.empty() : Optional.of(children));
  }

  @Override
  public void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    if (verbosity.equals(Explain.Verbosity.QUERY_PLANNER)) {
      return;
    }

    if (this.queryStats.isPresent()) {
      Check.checkState(
          this.cursorId.isPresent(), "aggregation.query.cursorId must be recorded before emit");
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
                ExplainTimings.Type.AGGREGATION_MERGE, timingData),
            this.tree,
            // The cursor id and heap bytes arrive after the tree, so stitch them on at emit time.
            this.queryStats.map(
                stats ->
                    stats
                        .withCursorId(this.cursorId)
                        .withObservedHeapBytes(this.observedHeapBytes)))
        .ifPresent(builder::aggregationStats);
  }
}
