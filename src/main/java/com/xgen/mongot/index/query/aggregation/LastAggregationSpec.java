package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.io.IOException;

/**
 * Production leaf spec for the {@code last} aggregation operator.
 *
 * <p>{@code last} returns the metric path's value from a single document per bucket. When the query
 * defines a sort, that is the document that sorts latest under it. When no sort is defined, it is
 * the last document in encountered order.
 */
public final class LastAggregationSpec extends OperatorAggregationSpec {
  private final FieldPath path;

  public LastAggregationSpec(String name, FieldPath path) {
    super(Operator.LAST, name);
    this.path = path;
  }

  public FieldPath getPath() {
    return this.path;
  }

  @Override
  public Accumulator buildAccumulator(AggregationContext context) throws IOException {
    // TODO(CLOUDP-425978 follow-up, after CLOUDP-426648): fetch the metric path's DocValue per
    // bucket together with the doc's global-sort key, keeping the value from the doc that loses
    // under the sort (or the last value in encountered order when no sort is present).
    throw new UnsupportedOperationException("Last accumulation is not implemented yet");
  }

  @Override
  public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    // TODO(CLOUDP-425978 follow-up): merge per-shard candidates with MQL's $bottom accumulator,
    // sorted by the sort-key partial each shard carries in its intermediate BSON. Single-level
    // multi-shard merge lands with value fetching, once the accumulator fixes that intermediate
    // format; only multi-level merge waits on a separate design.
    // IllegalStateException, not UnsupportedOperationException: ShardedSearchPlanner only
    // wraps IllegalArgumentException / IllegalStateException into ShardedSearchPlannerException,
    // so anything else escapes planning as an uncaught internal error.
    throw new IllegalStateException("Sharded merge for 'last' aggregations is not implemented yet");
  }
}
