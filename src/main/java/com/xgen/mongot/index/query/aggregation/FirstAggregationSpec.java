package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.io.IOException;

/**
 * Production leaf spec for the {@code first} aggregation operator.
 *
 * <p>Picks the value of {@code path} from the document that sorts first under the query's global
 * sort, or the first document in encountered order when the query has no sort.
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
    return new FirstAccumulator(this.name, this.path, context);
  }

  @Override
  public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    FirstLastShardMerge.buildShardMergePipeline(builder, this.operator, this.name);
  }
}
