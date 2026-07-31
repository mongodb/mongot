package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;

/**
 * Production leaf result for the {@code max} aggregation operator.
 *
 * <p>{@code value} is the running maximum BSON value, or empty when no doc in the bucket had an
 * eligible value for the field (bucket-level "missing"). Selection follows {@link MinMaxSelection}
 * so standalone and sharded results agree.
 */
public final class MaxAggregationResult extends MetricAggregationResult {
  private final Optional<BsonValue> value;

  public MaxAggregationResult(String name, Optional<BsonValue> value) {
    super(name);
    this.value = value;
  }

  public Optional<BsonValue> getValue() {
    return this.value;
  }

  /** The intermediate partial field carrying this metric across the shard-merge pipeline. */
  static String partialFieldName(String name) {
    return "max_" + name;
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
    return this.value.orElse(BsonNull.VALUE);
  }

  @Override
  public BsonDocument toIntermediateMetricPartials() {
    // Omit the partial when this shard/partition saw no value, so the cross-shard $max skips it
    // (matching MongoDB $max's ignore-missing semantics); a bucket with no values anywhere yields
    // null after the merge's $max over zero inputs.
    return this.value
        .map(v -> new BsonDocument(partialFieldName(getName()), v))
        .orElseGet(BsonDocument::new);
  }

  private static final class Builder implements AggregationResultBuilder {
    private final String name;
    private Optional<BsonValue> value = Optional.empty();

    Builder(String name) {
      this.name = name;
    }

    @Override
    public void add(AggregationResult other) {
      Optional<BsonValue> candidate = Check.instanceOf(other, MaxAggregationResult.class).value;
      if (candidate.isEmpty()) {
        return;
      }
      if (this.value.isEmpty() || MinMaxSelection.isLarger(candidate.get(), this.value.get())) {
        this.value = candidate;
      }
    }

    @Override
    public AggregationResult build() {
      return new MaxAggregationResult(this.name, this.value);
    }
  }
}
