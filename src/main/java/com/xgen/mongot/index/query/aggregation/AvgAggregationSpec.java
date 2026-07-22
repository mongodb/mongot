package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import org.bson.BsonDocument;
import org.bson.BsonString;

/** Production leaf spec for the {@code avg} aggregation operator. */
public final class AvgAggregationSpec extends OperatorAggregationSpec {
  private final FieldPath path;

  public AvgAggregationSpec(String name, FieldPath path) {
    super(Operator.AVG, name);
    this.path = path;
  }

  public FieldPath getPath() {
    return this.path;
  }

  @Override
  public Accumulator buildAccumulator(AggregationContext context) throws IOException {
    throw new UnsupportedOperationException("Average accumulation is not implemented yet");
  }

  @Override
  public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    builder.addMetricAccumulator(
        "avg_sum_" + this.name, new BsonDocument("$sum", new BsonString("$avg_sum_" + this.name)));
    builder.addMetricAccumulator(
        "avg_count_" + this.name,
        new BsonDocument("$sum", new BsonString("$avg_count_" + this.name)));
    // $divide is invalid when merged count is 0; return null to match MongoDB $avg.
    builder.addMetricProjection(
        this.name,
        BsonDocument.parse(
            """
            {"$cond": {
              "if": {"$eq": ["$$b.avg_count_%s", 0]},
              "then": null,
              "else": {"$divide": ["$$b.avg_sum_%s", "$$b.avg_count_%s"]}
            }}
            """
                .formatted(this.name, this.name, this.name)));
  }
}
