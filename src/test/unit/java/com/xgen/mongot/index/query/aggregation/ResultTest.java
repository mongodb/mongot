package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

public class ResultTest {

  private static final FieldPath CITY = FieldPath.parse("city");

  @Test
  public void aggregationResultsByName() {
    AggregationResults results =
        AggregationResults.from(List.of(new SumResult("a", 1.0), new SumResult("b", 2.0)));

    assertEquals(1.0, ((SumResult) results.get("a")).value(), 0.0001);
    assertEquals(2.0, ((SumResult) results.get("b")).value(), 0.0001);
    assertThrows(IllegalStateException.class, () -> results.get("missing"));
  }

  @Test
  public void aggregationResultsRejectsDuplicateName() {
    assertThrows(
        IllegalStateException.class,
        () ->
            AggregationResults.from(List.of(new SumResult("dup", 1.0), new SumResult("dup", 2.0))));
  }

  @Test
  public void leafResultMergeSumsValues() {
    SumResult a = new SumResult("sum_price", 10.0);
    SumResult b = new SumResult("sum_price", 25.0);
    SumResult merged = (SumResult) a.mergePartitionResult(b);
    assertEquals(35.0, merged.value(), 0.0001);
    assertEquals("sum_price", merged.getName());
  }

  @Test
  public void groupResultMergeUnionsBucketsByKey() {
    TermsResult left =
        new TermsResult(
            "by_city",
            CITY,
            List.of(
                new TermsResult.TermsBucket(
                    "NYC", AggregationResults.from(List.of(new SumResult("sum", 10.0)))),
                new TermsResult.TermsBucket(
                    "LA", AggregationResults.from(List.of(new SumResult("sum", 5.0))))));

    TermsResult right =
        new TermsResult(
            "by_city",
            CITY,
            List.of(
                new TermsResult.TermsBucket(
                    "NYC", AggregationResults.from(List.of(new SumResult("sum", 20.0)))),
                new TermsResult.TermsBucket(
                    "SF", AggregationResults.from(List.of(new SumResult("sum", 7.0))))));

    TermsResult merged = (TermsResult) left.mergePartitionResult(right);
    assertEquals(3, merged.getBuckets().size());

    for (TermsResult.TermsBucket bucket : merged.getBuckets()) {
      double sum = ((SumResult) bucket.getChildResults().get("sum")).value();
      switch (bucket.getKey().getValue()) {
        case "NYC" -> assertEquals(30.0, sum, 0.0001);
        case "LA" -> assertEquals(5.0, sum, 0.0001);
        case "SF" -> assertEquals(7.0, sum, 0.0001);
        default -> throw new AssertionError("unexpected bucket: " + bucket.getKey());
      }
    }
  }

  @Test
  public void topLevelResultMergesChildrenByName() {
    TopLevelAggregationResult left =
        new TopLevelAggregationResult(List.of(new SumResult("a", 1.0), new SumResult("b", 2.0)));
    TopLevelAggregationResult right =
        new TopLevelAggregationResult(List.of(new SumResult("a", 10.0), new SumResult("b", 20.0)));

    TopLevelAggregationResult merged = (TopLevelAggregationResult) left.mergePartitionResult(right);

    assertEquals(2, merged.childResults().size());
    assertThrows(UnsupportedOperationException.class, merged::getName);
    assertEquals(11.0, ((SumResult) merged.childResults().get(0)).value(), 0.0001);
    assertEquals(22.0, ((SumResult) merged.childResults().get(1)).value(), 0.0001);
  }

  @Test
  public void aggregationResultIsIterable() {
    AggregationResults results =
        AggregationResults.from(List.of(new SumResult("x", 1.0), new SumResult("y", 2.0)));

    @com.google.errorprone.annotations.Var int count = 0;
    for (AggregationResult ignored : results) {
      count++;
    }
    assertEquals(2, count);
  }

  // ===== inline result fixtures =====

  private record SumResult(String name, double value) implements AggregationResult {
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
      return null;
    }

    @Override
    public BsonArray toIntermediateBson() {
      return new BsonArray();
    }

    @Override
    public BsonDocument toIntermediateMetricPartials() {
      return new org.bson.BsonDocument("sum_" + this.name, new org.bson.BsonDouble(this.value));
    }
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
    public List<TermsBucket> getBuckets() {
      return this.buckets;
    }

    @Override
    public AggregationResult mergePartitionResult(AggregationResult other) {
      TermsResult that = Check.instanceOf(other, TermsResult.class);
      Map<String, TermsBucket> merged =
          this.buckets.stream()
              .collect(
                  Collectors.toMap(
                      b -> b.getKey().getValue(),
                      Function.identity(),
                      (a, b) -> {
                        throw new IllegalStateException("dup: " + a.getKey());
                      },
                      LinkedHashMap::new));
      for (TermsBucket otherBucket : that.buckets) {
        merged.merge(otherBucket.getKey().getValue(), otherBucket, TermsBucket::merge);
      }
      return new TermsResult(this.name, this.path, new ArrayList<>(merged.values()));
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
        List<AggregationResult> merged = new ArrayList<>();
        for (AggregationResult lhs : a.childResults) {
          merged.add(lhs.mergePartitionResult(b.childResults.get(lhs.getName())));
        }
        return new TermsBucket(a.key, AggregationResults.from(merged));
      }
    }
  }
}
