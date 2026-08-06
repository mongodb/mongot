package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;

/** Production leaf result for the {@code count} aggregation operator. */
public final class CountAggregationResult extends MetricAggregationResult {
  private static final String COUNT_PREFIX = "count_";

  private final long value;

  public CountAggregationResult(String name, long value) {
    super(name);
    this.value = value;
  }

  public long getValue() {
    return this.value;
  }

  /**
   * Wire field the per-bucket partial is carried under.
   *
   * <p>Prefixed, so it never collides with {@link
   * IntermediateAggregationMetaField#BUCKET_DOC_COUNT} -- the unprefixed {@code count} that every
   * intermediate row already carries for its grouping level, and which {@code
   * IntermediateAggregationMetaField} rejects a metric for setting.
   */
  static String partialFieldName(String name) {
    return COUNT_PREFIX + name;
  }

  @Override
  public AggregationResultBuilder toBuilder() {
    return new Builder(getName());
  }

  @Override
  public AggregationResult applyLimit() {
    return this;
  }

  @Override
  public BsonValue toStandaloneBson() {
    // Int64 rather than Int32: a bucket count is bounded by the collection size, and the legacy
    // facet count is also a long.
    return new BsonInt64(this.value);
  }

  @Override
  public BsonDocument toIntermediateMetricPartials() {
    return new BsonDocument(partialFieldName(getName()), new BsonInt64(this.value));
  }

  private static final class Builder implements AggregationResultBuilder {
    private final String name;
    private long count;

    Builder(String name) {
      this.name = name;
    }

    @Override
    public void add(AggregationResult other) {
      // Segments and partitions hold disjoint documents, so merging is addition -- the same reason
      // the cross-shard merge is a $sum.
      this.count += Check.instanceOf(other, CountAggregationResult.class).value;
    }

    @Override
    public AggregationResult build() {
      return new CountAggregationResult(this.name, this.count);
    }
  }
}
