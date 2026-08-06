package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.io.IOException;

/**
 * Production leaf spec for the {@code last} aggregation operator.
 *
 * <p>Picks the value of {@code path} from the document that sorts last under the query's global
 * sort, or the last document in encountered order when the query has no sort.
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
    return new LastAccumulator(this.name, this.path, context);
  }

  @Override
  public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    FirstLastShardMerge.buildShardMergePipeline(builder, this.operator, this.name);
  }
}
