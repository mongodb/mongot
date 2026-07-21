package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;

/**
 * Shared inline operator specs for aggregation unit tests.
 *
 * <p>Supports both Lucene accumulator tests ({@link #SumSpec(String, String)} with a raw doc-values
 * field name) and shard merge-pipeline / planner tests ({@link #SumSpec(String, FieldPath)} and
 * {@link #AvgSpec}).
 */
public final class TestOperatorSpecs {

  private TestOperatorSpecs() {}

  public static void installRegistry() {
    SearchAggregationBootstrap.installRegistry(
        Map.of(
            "sum",
            (name, path) -> new SumSpec("sum_price", path.orElse(FieldPath.parse("price"))),
            "avg",
            (name, path) -> new AvgSpec("avg_price", path.orElse(FieldPath.parse("price"))),
            "count",
            (name, path) -> new CountSpec(name)));
  }

  /** Inline leaf result carrying a single double value. */
  public static record SumResult(String name, double value) implements AggregationResult {
    @Override
    public String getName() {
      return this.name;
    }

    @Override
    public AggregationResult mergePartitionResult(AggregationResult other) {
      return new SumResult(this.name, this.value + ((SumResult) other).value);
    }

    @Override
    public BsonValue toStandaloneBson() {
      return new org.bson.BsonDouble(this.value);
    }

    @Override
    public BsonArray toIntermediateBson() {
      return new BsonArray();
    }

    @Override
    public BsonDocument toIntermediateMetricPartials() {
      return new BsonDocument("sum_" + this.name, new org.bson.BsonDouble(this.value));
    }
  }

  public static final class SumSpec extends OperatorAggregationSpec {
    @Nullable private final String luceneFieldName;

    public SumSpec(String name, FieldPath path) {
      super(Operator.SUM, name);
      this.luceneFieldName = null;
    }

    public SumSpec(String name, String luceneFieldName) {
      super(Operator.SUM, name);
      this.luceneFieldName = luceneFieldName;
    }

    @Override
    public Accumulator buildAccumulator(AggregationContext context) {
      if (this.luceneFieldName == null) {
        throw new UnsupportedOperationException("not exercised in planner tests");
      }
      return new SumAccumulator(this.name, this.luceneFieldName, context);
    }

    @Override
    public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
      builder.addMetricAccumulator(
          "sum_" + this.name, new BsonDocument("$sum", new BsonString("$sum_" + this.name)));
      builder.addMetricProjection(this.name, new BsonString("$$b.sum_" + this.name));
    }
  }

  /** Inline count spec that keeps the registry-supplied name, for spec-lowering assertions. */
  public static final class CountSpec extends OperatorAggregationSpec {
    public CountSpec(String name) {
      super(Operator.COUNT, name);
    }

    @Override
    public Accumulator buildAccumulator(AggregationContext context) {
      throw new UnsupportedOperationException("not exercised in planner tests");
    }

    @Override
    public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
      builder.addMetricAccumulator(
          "count_" + this.name, new BsonDocument("$sum", new BsonString("$count_" + this.name)));
      builder.addMetricProjection(this.name, new BsonString("$$b.count_" + this.name));
    }
  }

  public static final class AvgSpec extends OperatorAggregationSpec {
    @SuppressWarnings("unused")
    @Nullable
    private final FieldPath path;

    public AvgSpec(String name, FieldPath path) {
      super(Operator.AVG, name);
      this.path = path;
    }

    @Override
    public Accumulator buildAccumulator(AggregationContext context) {
      throw new UnsupportedOperationException("not exercised in planner tests");
    }

    @Override
    public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
      builder.addMetricAccumulator(
          "avg_sum_" + this.name,
          new BsonDocument("$sum", new BsonString("$avg_sum_" + this.name)));
      builder.addMetricAccumulator(
          "avg_count_" + this.name,
          new BsonDocument("$sum", new BsonString("$avg_count_" + this.name)));
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

  /** Inline leaf metric accumulator summing a numeric field per owning bucket ordinal. */
  static final class SumAccumulator extends Accumulator {
    private final String luceneFieldName;
    private final DoubleArray sums;

    SumAccumulator(String name, String luceneFieldName, AggregationContext context) {
      super(name, List.of(), context);
      this.luceneFieldName = luceneFieldName;
      this.sums = new DoubleArray(8);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationContext context) throws IOException {
      SortedNumericDocValues values =
          DocValues.getSortedNumeric(context.getLeafReaderContext().reader(), this.luceneFieldName);
      return new LeafBucketCollector() {
        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
          if (values.advanceExact(doc)) {
            int bucketOrd = (int) owningBucketOrd;
            int count = values.docValueCount();
            for (int i = 0; i < count; i++) {
              SumAccumulator.this.sums.increment(bucketOrd, (double) values.nextValue());
            }
          }
        }
      };
    }

    @Override
    protected List<AggregationResult> buildResults(LongArray ordsToCollect) {
      List<AggregationResult> results = new ArrayList<>();
      for (int i = 0; i < ordsToCollect.size(); i++) {
        int bucketOrd = (int) ordsToCollect.get(i);
        double sum = bucketOrd < this.sums.size() ? this.sums.get(bucketOrd) : 0.0;
        results.add(new SumResult(this.name, sum));
      }
      return results;
    }
  }
}
