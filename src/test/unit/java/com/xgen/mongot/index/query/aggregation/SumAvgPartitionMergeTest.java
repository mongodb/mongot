package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.Test;

/**
 * Validates production sum/avg leaf results merge correctly across shard partitions (multi-shard
 * path) and emit intermediate BSON partials compatible with the shard merge pipeline.
 */
public class SumAvgPartitionMergeTest extends AggregationTestSupport {

  private static final FieldPath CITY = FieldPath.parse("city");

  @Test
  public void sumPartitionMerge_combinesShardPartials() {
    TermsResult left =
        new TermsResult(
            "by_city",
            CITY,
            List.of(
                new TermsResult.TermsBucket(
                    "NYC",
                    AggregationResults.from(List.of(new SumAggregationResult("sum_price", 30.0)))),
                new TermsResult.TermsBucket(
                    "SF",
                    AggregationResults.from(
                        List.of(new SumAggregationResult("sum_price", 250.0))))));
    TermsResult right =
        new TermsResult(
            "by_city",
            CITY,
            List.of(
                new TermsResult.TermsBucket(
                    "NYC",
                    AggregationResults.from(List.of(new SumAggregationResult("sum_price", 20.0)))),
                new TermsResult.TermsBucket(
                    "LA",
                    AggregationResults.from(
                        List.of(new SumAggregationResult("sum_price", 350.0))))));

    TermsResult merged = (TermsResult) mergePartitionResults(left, right);
    assertEquals(50.0, sumFor(merged, "NYC"), 0.0001);
    assertEquals(250.0, sumFor(merged, "SF"), 0.0001);
    assertEquals(350.0, sumFor(merged, "LA"), 0.0001);
  }

  @Test
  public void avgPartitionMerge_combinesSumAndCountPartials() {
    TermsResult left = termsBucket("NYC", new AvgAggregationResult("avg_price", 30.0, 2));
    TermsResult right = termsBucket("NYC", new AvgAggregationResult("avg_price", 20.0, 1));

    TermsResult merged = (TermsResult) mergePartitionResults(left, right);
    AvgAggregationResult avg = avgFor(merged, "NYC");
    assertEquals(50.0, avg.getSum(), 0.0001);
    assertEquals(3, avg.getCount());
    assertEquals(new org.bson.BsonDouble(50.0 / 3.0), avg.toStandaloneBson());
  }

  @Test
  public void avgPartitionMerge_zeroCountAcrossShards_returnsNull() {
    TermsResult left = termsBucket("NYC", new AvgAggregationResult("avg_price", 0.0, 0));
    TermsResult right = termsBucket("NYC", new AvgAggregationResult("avg_price", 0.0, 0));

    TermsResult merged = (TermsResult) mergePartitionResults(left, right);
    assertSame(BsonNull.VALUE, avgFor(merged, "NYC").toStandaloneBson());
  }

  @Test
  public void sumIntermediateBson_emitsMergeablePartialsPerShard() {
    TermsResult shardA = termsBucket("NYC", new SumAggregationResult("sum_price", 30.0));
    TermsResult shardB = termsBucket("NYC", new SumAggregationResult("sum_price", 20.0));

    BsonArray partialsA = new TopLevelAggregationResult(List.of(shardA)).toIntermediateBson();
    BsonArray partialsB = new TopLevelAggregationResult(List.of(shardB)).toIntermediateBson();

    BsonDocument rowA = partialsA.get(0).asDocument();
    BsonDocument rowB = partialsB.get(0).asDocument();
    assertEquals(30.0, rowA.getDouble("sum_sum_price").doubleValue(), 0.0001);
    assertEquals(20.0, rowB.getDouble("sum_sum_price").doubleValue(), 0.0001);

    SumAggregationResult merged =
        (SumAggregationResult)
            mergePartitionResults(
                new SumAggregationResult("sum_price", 30.0),
                new SumAggregationResult("sum_price", 20.0));
    assertEquals(50.0, merged.getValue(), 0.0001);
  }

  @Test
  public void avgIntermediateBson_emitsSumAndCountPartialsPerShard() {
    TermsResult shardA = termsBucket("NYC", new AvgAggregationResult("avg_price", 30.0, 2));
    TermsResult shardB = termsBucket("NYC", new AvgAggregationResult("avg_price", 20.0, 1));

    BsonDocument rowA =
        new TopLevelAggregationResult(List.of(shardA)).toIntermediateBson().get(0).asDocument();
    BsonDocument rowB =
        new TopLevelAggregationResult(List.of(shardB)).toIntermediateBson().get(0).asDocument();

    assertEquals(30.0, rowA.getDouble("avg_sum_avg_price").doubleValue(), 0.0001);
    assertEquals(2, rowA.getInt64("avg_count_avg_price").longValue());
    assertEquals(20.0, rowB.getDouble("avg_sum_avg_price").doubleValue(), 0.0001);
    assertEquals(1, rowB.getInt64("avg_count_avg_price").longValue());
  }

  private static TermsResult termsBucket(String city, AggregationResult... metrics) {
    return new TermsResult(
        "by_city",
        CITY,
        List.of(new TermsResult.TermsBucket(city, AggregationResults.from(List.of(metrics)))));
  }

  private static double sumFor(TermsResult result, String city) {
    return ((SumAggregationResult) bucket(result, city).getChildResults().get("sum_price"))
        .getValue();
  }

  private static AvgAggregationResult avgFor(TermsResult result, String city) {
    return (AvgAggregationResult) bucket(result, city).getChildResults().get("avg_price");
  }

  private static TermsResult.TermsBucket bucket(TermsResult result, String city) {
    return result.getBuckets(BucketOrder.KEY_ASC).stream()
        .filter(b -> b.getKey().getValue().equals(city))
        .findFirst()
        .orElseThrow();
  }

  private static final class TermsResult extends MultiBucketAggregationResult {
    private final String name;
    private final List<TermsBucket> buckets;

    TermsResult(String name, FieldPath path, List<TermsBucket> buckets) {
      super(path);
      this.name = name;
      this.buckets = buckets;
    }

    @Override
    public String getName() {
      return this.name;
    }

    @Override
    public List<TermsBucket> getBuckets(BucketOrder order) {
      return this.buckets;
    }

    @Override
    public AggregationResultBuilder toBuilder() {
      return new Builder(this);
    }

    @Override
    public AggregationResult applyLimit() {
      List<TermsBucket> finalized = new ArrayList<>(this.buckets.size());
      for (TermsBucket b : this.buckets) {
        finalized.add(new TermsBucket(b.key, b.getChildResults().applyLimit()));
      }
      return new TermsResult(this.name, this.path, finalized);
    }

    static final class TermsBucket implements Bucket {
      private final String key;
      private final AggregationResults childResults;

      TermsBucket(String key, AggregationResults childResults) {
        this.key = key;
        this.childResults = childResults;
      }

      @Override
      public BsonString getKey() {
        return new BsonString(this.key);
      }

      @Override
      public AggregationResults getChildResults() {
        return this.childResults;
      }

      static TermsBucket merge(TermsBucket a, TermsBucket b) {
        AggregationResults.Builder builder = new AggregationResults.Builder();
        builder.add(a.childResults);
        builder.add(b.childResults);
        return new TermsBucket(a.key, builder.build());
      }
    }

    private static final class Builder implements AggregationResultBuilder {
      private final String name;
      private final FieldPath path;
      private final Map<String, TermsBucket> bucketsByKey = new LinkedHashMap<>();

      Builder(TermsResult shape) {
        this.name = shape.name;
        this.path = shape.path;
      }

      @Override
      public void add(AggregationResult other) {
        TermsResult that = Check.instanceOf(other, TermsResult.class);
        for (TermsBucket bucket : that.buckets) {
          this.bucketsByKey.merge(bucket.getKey().getValue(), bucket, TermsBucket::merge);
        }
      }

      @Override
      public AggregationResult build() {
        return new TermsResult(this.name, this.path, new ArrayList<>(this.bucketsByKey.values()));
      }
    }
  }
}
