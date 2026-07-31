package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import org.bson.BsonDocument;
import org.bson.BsonString;

/**
 * Production leaf spec for the {@code max} aggregation operator.
 *
 * <p>Max resolves its path to a {@link ResolvedNumericMetricField} and reads {@code NUMBER_*_V2}
 * doc values, so it is supported only on {@code number} fields; other field types are rejected at
 * field-resolution time.
 */
public final class MaxAggregationSpec extends OperatorAggregationSpec {
  private final FieldPath path;

  public MaxAggregationSpec(String name, FieldPath path) {
    super(Operator.MAX, name);
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
    return new MinMaxAccumulator(this.name, metricField, MinMaxAccumulator.Extremum.MAX, context);
  }

  @Override
  public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    String partial = MaxAggregationResult.partialFieldName(this.name);
    builder.addMetricAccumulator(partial, new BsonDocument("$max", new BsonString("$" + partial)));
    builder.addMetricProjection(this.name, new BsonString("$$b." + partial));
  }
}
