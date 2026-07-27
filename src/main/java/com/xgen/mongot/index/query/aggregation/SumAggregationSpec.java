package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import org.bson.BsonDocument;
import org.bson.BsonString;

/** Production leaf spec for the {@code sum} aggregation operator. */
public final class SumAggregationSpec extends OperatorAggregationSpec {
  private final FieldPath path;

  public SumAggregationSpec(String name, FieldPath path) {
    super(Operator.SUM, name);
    this.path = path;
  }

  public FieldPath getPath() {
    return this.path;
  }

  @Override
  public Accumulator buildAccumulator(AggregationContext context) throws IOException {
    ResolvedNumericMetricField metricField =
        context
            .getFieldResolver()
            .orElseThrow(
                () -> new IllegalStateException("Aggregation field resolver is not configured"))
            .resolveNumeric(this.path);
    return new SumAccumulator(this.name, metricField, context);
  }

  @Override
  public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    builder.addMetricAccumulator(
        "sum_" + this.name, new BsonDocument("$sum", new BsonString("$sum_" + this.name)));
    builder.addMetricProjection(this.name, new BsonString("$$b.sum_" + this.name));
  }
}
