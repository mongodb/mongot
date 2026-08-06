package com.xgen.mongot.index.query.aggregation;

import org.bson.BsonDocument;
import org.bson.BsonString;

/**
 * Production leaf spec for the {@code count} aggregation operator.
 *
 * <p>The only operator that takes no path: it counts the documents collected into its owning bucket
 * rather than reading a field, so {@code AggregationRegistry.OperatorSupplier} is handed {@link
 * java.util.Optional#empty()} for the path and this spec has none to resolve.
 */
public final class CountAggregationSpec extends OperatorAggregationSpec {

  public CountAggregationSpec(String name) {
    super(Operator.COUNT, name);
  }

  @Override
  public Accumulator buildAccumulator(AggregationContext context) {
    return new CountAccumulator(this.name, context);
  }

  @Override
  public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
    // Per-shard counts are disjoint document sets, so the cross-shard merge is a plain sum of the
    // partials -- the same shape sum uses.
    String partial = CountAggregationResult.partialFieldName(this.name);
    builder.addMetricAccumulator(partial, new BsonDocument("$sum", new BsonString("$" + partial)));
    builder.addMetricProjection(this.name, new BsonString("$$b." + partial));
  }
}
