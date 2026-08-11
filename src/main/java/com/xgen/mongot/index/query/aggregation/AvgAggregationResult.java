package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonValue;

/** Production leaf result for the {@code avg} aggregation operator. */
public final class AvgAggregationResult extends MetricAggregationResult {
  private static final String AVG_SUM_PREFIX = "avg_sum_";
  private static final String AVG_COUNT_PREFIX = "avg_count_";

  static String wireSumPartialName(String name) {
    return AVG_SUM_PREFIX + name;
  }

  static String wireCountPartialName(String name) {
    return AVG_COUNT_PREFIX + name;
  }

  private final double sum;
  private final long count;

  public AvgAggregationResult(String name, double sum, long count) {
    super(name);
    this.sum = sum;
    this.count = count;
  }

  public double getSum() {
    return this.sum;
  }

  public long getCount() {
    return this.count;
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
    // MongoDB $avg returns null when no values contribute to the average (count is 0).
    return this.count == 0 ? BsonNull.VALUE : new BsonDouble(this.sum / this.count);
  }

  @Override
  public BsonDocument toIntermediateMetricPartials() {
    return new BsonDocument()
        .append(wireSumPartialName(getName()), new BsonDouble(this.sum))
        .append(wireCountPartialName(getName()), new BsonInt64(this.count));
  }

  private static final class Builder implements AggregationResultBuilder {
    private final String name;
    private double sum;
    private long count;

    Builder(String name) {
      this.name = name;
    }

    @Override
    public void add(AggregationResult other) {
      AvgAggregationResult that = Check.instanceOf(other, AvgAggregationResult.class);
      this.sum += that.sum;
      this.count += that.count;
    }

    @Override
    public AggregationResult build() {
      return new AvgAggregationResult(this.name, this.sum, this.count);
    }
  }
}
