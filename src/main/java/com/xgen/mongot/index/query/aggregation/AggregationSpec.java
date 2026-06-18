package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import java.util.List;

/**
 * Common interface for every node in an aggregation spec tree.
 *
 * <p>The framework defines three implementing flavors that compose into a tree:
 *
 * <ul>
 *   <li>{@link TopAggregationSpec} — the root container; holds top-level facets
 *   <li>{@link GroupAggregationSpec} — bucket nodes (e.g. terms); have children
 *   <li>{@link OperatorAggregationSpec} — leaf metric nodes (e.g. avg, sum); no children
 * </ul>
 */
public sealed interface AggregationSpec
    permits TopAggregationSpec, GroupAggregationSpec, OperatorAggregationSpec {

  /** User-assigned name for this aggregation. */
  String name();

  /** Child aggregation specs (empty for metrics, populated for bucket aggs). */
  List<AggregationSpec> children();

  /**
   * Create the runtime Accumulator for this spec.
   *
   * <p>Accumulator trees are built bottom-up: implementations build their children's accumulators
   * first and pass them into their own accumulator's constructor, which wires itself as the
   * children's parent backpointer.
   *
   * @param context provides index reader, index definition, etc.
   */
  Accumulator buildAccumulator(AggregationContext context) throws IOException;

  /**
   * Contribute this spec's merge logic to the shared pipeline builder. Called by
   * ShardedSearchPlanner at plan time. Bucket specs add their grouping key and recurse into
   * children, metric specs add their accumulator fields and finalization projections.
   */
  void buildShardMergePipeline(ShardMergePipelineBuilder builder);
}
