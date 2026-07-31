package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import org.bson.BsonDocument;
import org.bson.BsonString;

/**
 * Production leaf spec for the {@code min} aggregation operator.
 *
 * <p>Min resolves its path to a {@link ResolvedNumericMetricField} and reads {@code NUMBER_*_V2}
 * doc values, so it is supported only on {@code number} fields; other field types are rejected at
 * field-resolution time.
 */
public final class MinAggregationSpec extends OperatorAggregationSpec {
  private final FieldPath path;

  public MinAggregationSpec(String name, FieldPath path) {
    super(Operator.MIN, name);
    this.path = path;
  }

  public FieldPath getPath() {
    return this.path;
  }

  @Override
  public Accumulator buildAccumulator(AggregationContext context) {
    ResolvedNumericMetricField metricField =
        context
            .getFieldResolver()
            .orElseThrow(
                () -> new IllegalStateException("Aggregation field resolver is not configured"))
            .resolveNumeric(this.path);
    return new MinMaxAccumulator(this.name, metricField, MinMaxAccumulator.Extremum.MIN, context);
  }

  @Override
  public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    String partial = MinAggregationResult.partialFieldName(this.name);
    builder.addMetricAccumulator(partial, new BsonDocument("$min", new BsonString("$" + partial)));
    builder.addMetricProjection(this.name, new BsonString("$$b." + partial));
  }
}
