package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonValue;

/** Production leaf result for the {@code sum} aggregation operator. */
public final class SumAggregationResult extends MetricAggregationResult {
  private static final String SUM_PREFIX = "sum_";

  private final double value;

  public SumAggregationResult(String name, double value) {
    super(name);
    this.value = value;
  }

  public double getValue() {
    return this.value;
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
    return new BsonDouble(this.value);
  }

  @Override
  public BsonDocument toIntermediateMetricPartials() {
    return new BsonDocument(SUM_PREFIX + getName(), new BsonDouble(this.value));
  }

  private static final class Builder implements AggregationResultBuilder {
    private final String name;
    private double sum;

    Builder(String name) {
      this.name = name;
    }

    @Override
    public void add(AggregationResult other) {
      this.sum += Check.instanceOf(other, SumAggregationResult.class).value;
    }

    @Override
    public AggregationResult build() {
      return new SumAggregationResult(this.name, this.sum);
    }
  }
}
