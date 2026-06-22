package com.xgen.mongot.index.query.aggregation;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;

/** Intermediate level spec for sorted set doc values fields. */
public final class TermsGroupAggregationSpec extends GroupAggregationSpec {
  // Whether this is at the root level.
  private final boolean isRoot;
  // Max number of buckets to return per parent bucket, ordered by (doc count desc, term asc).
  private final int limit;

  public TermsGroupAggregationSpec(
      String name, FieldPath path, List<AggregationSpec> children, boolean isRoot, int limit) {
    super(path, name, children);
    checkArg(limit > 0, "limit must be positive, got: %s", limit);
    this.isRoot = isRoot;
    this.limit = limit;
  }

  public int getLimit() {
    return this.limit;
  }

  @Override
  protected Accumulator createAccumulator(
      AggregationContext context, List<Accumulator> childAccumulators) throws IOException {
    return TermsAccumulator.create(this, context, childAccumulators, this.isRoot);
  }
}
