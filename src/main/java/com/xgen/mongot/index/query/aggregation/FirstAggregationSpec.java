package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.io.IOException;

/**
 * Production leaf spec for the {@code first} aggregation operator.
 *
 * <p>{@code first} returns the metric path's value from a single document per bucket. When the
 * query defines a sort, that is the document that sorts earliest under it. When no sort is defined,
 * it is the first document in encountered order.
 */
public final class FirstAggregationSpec extends OperatorAggregationSpec {
  private final FieldPath path;

  public FirstAggregationSpec(String name, FieldPath path) {
    super(Operator.FIRST, name);
    this.path = path;
  }

  public FieldPath getPath() {
    return this.path;
  }

  @Override
  public Accumulator buildAccumulator(AggregationContext context) throws IOException {
    // TODO(CLOUDP-425978 follow-up, after CLOUDP-426648): fetch the metric path's DocValue per
    // bucket together with the doc's global-sort key, keeping the value from the doc that wins
    // under the sort (or the first value in encountered order when no sort is present).
    throw new UnsupportedOperationException("First accumulation is not implemented yet");
  }

  @Override
  public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    // TODO(CLOUDP-425978 follow-up): merge per-shard candidates with MQL's $top accumulator,
    // sorted by the sort-key partial each shard carries in its intermediate BSON. Single-level
    // multi-shard merge lands with value fetching, once the accumulator fixes that intermediate
    // format; only multi-level merge waits on a separate design.
    // IllegalStateException, not UnsupportedOperationException: ShardedSearchPlanner only
    // wraps IllegalArgumentException / IllegalStateException into ShardedSearchPlannerException,
    // so anything else escapes planning as an uncaught internal error.
    throw new IllegalStateException(
        "Sharded merge for 'first' aggregations is not implemented yet");
  }
}
