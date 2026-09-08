package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonString;
import org.junit.Before;
import org.junit.Test;

/** Integration test driving the accumulator framework against an in-memory Lucene index. */
public class AccumulatorTest extends AggregationTestSupport {

  @Before
  public void setUp() throws Exception {
    buildIndex(
        List.of(
            new TestDoc("NYC", "x", 10),
            new TestDoc("NYC", "x", 20),
            new TestDoc("NYC", "x", 30),
            new TestDoc("LA", "x", 5),
            new TestDoc("LA", "x", 15)),
        Integer.MAX_VALUE); // single segment
  }

  @Test
  public void groupByCityWithSumPrice() throws Exception {
    AggregationSpec sumSpec = new TestOperatorSpecs.SumSpec("sum_price", PRICE_LUCENE_FIELD);
    AggregationSpec termsSpec =
        new TermsSpec("by_city", FieldPath.parse(CITY_FIELD), CITY_LUCENE_FIELD, List.of(sumSpec));
    TopAggregationSpec topSpec = new TopAggregationSpec(List.of(termsSpec));

    Accumulator topAccumulator = collectAll(topSpec, newContext());

    AggregationResult topResult = topAccumulator.buildTopLevelResult();
    assertNotNull(topResult);

    TopLevelAggregationResult top = (TopLevelAggregationResult) topResult;
    assertEquals(1, top.childResults().size());

    TermsResult byCity = (TermsResult) top.childResults().get(0);
    assertEquals("by_city", byCity.getName());
    assertEquals(2, byCity.getBuckets(BucketOrder.KEY_ASC).size());

    Map<String, Double> sumByCity = new HashMap<>();
    for (TermsResult.TermsBucket bucket : byCity.getBuckets(BucketOrder.KEY_ASC)) {
      TestOperatorSpecs.SumResult sum =
          (TestOperatorSpecs.SumResult) bucket.getChildResults().get("sum_price");
      sumByCity.put(bucket.getKey().getValue(), sum.value());
    }

    assertEquals(60.0, sumByCity.get("NYC"), 0.0001);
    assertEquals(20.0, sumByCity.get("LA"), 0.0001);
  }

  // ===== inline test-only fixtures: a local-ord terms accumulator + result, used to exercise the
  // framework wiring (BucketsAccumulator / buildSubAggsForBuckets) independently of the real
  // terms accumulators. =====

  /** Inline bucket result for term-style grouping. */
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
      return TestOperatorSpecs.folding(
          (a, b) -> ((TermsResult) a).mergeWith(Check.instanceOf(b, TermsResult.class)));
    }

    private TermsResult mergeWith(TermsResult that) {
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

    @Override
    public AggregationResult applyLimit() {
      // Test double: no numBuckets limit is modeled here, so nothing is truncated. Recurse so any
      // child results are finalized, matching the container contract.
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

      @Override
      public long getDocCount() {
        return 1L;
      }

      static TermsBucket merge(TermsBucket a, TermsBucket b) {
        List<AggregationResult> merged = new ArrayList<>();
        for (AggregationResult lhs : a.childResults) {
          AggregationResultBuilder builder = lhs.toBuilder();
          builder.add(lhs);
          builder.add(b.childResults.get(lhs.getName()));
          merged.add(builder.build());
        }
        return new TermsBucket(a.key, AggregationResults.from(merged));
      }
    }
  }

  /** Inline terms spec -- uses raw Lucene field name. */
  private static final class TermsSpec extends GroupAggregationSpec {
    private final String luceneFieldName;

    TermsSpec(String name, FieldPath path, String luceneFieldName, List<AggregationSpec> children) {
      super(path, name, children);
      this.luceneFieldName = luceneFieldName;
    }

    @Override
    public int groupingBucketCount() {
      return 1;
    }

    @Override
    protected Accumulator createAccumulator(
        AggregationContext context, List<Accumulator> childAccumulators) {
      return new LocalOrdTermsAccumulator(
          this.name, this.path, this.luceneFieldName, childAccumulators, context);
    }
  }

  /**
   * Inline terms accumulator -- simple per-segment local-ord strategy. Intended for single-segment
   * tests; reads the field by Lucene field name directly to avoid relying on field-type resolution.
   */
  private static final class LocalOrdTermsAccumulator extends BucketsAccumulator {
    private final FieldPath path;
    private final String luceneFieldName;
    private final Map<String, Integer> termToBucketOrd = new HashMap<>();
    private final List<String> bucketOrdToTerm = new ArrayList<>();

    LocalOrdTermsAccumulator(
        String name,
        FieldPath path,
        String luceneFieldName,
        List<Accumulator> childAccumulators,
        AggregationContext context) {
      super(name, childAccumulators, context);
      this.path = path;
      this.luceneFieldName = luceneFieldName;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(
        AggregationContext context, LeafBucketCollector children) throws IOException {
      SortedSetDocValues docValues =
          DocValues.getSortedSet(context.getLeafReaderContext().reader(), this.luceneFieldName);

      return new LeafBucketCollector() {
        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
          if (!docValues.advanceExact(doc)) {
            return;
          }
          int count = docValues.docValueCount();
          for (int i = 0; i < count; i++) {
            long segmentOrd = docValues.nextOrd();
            BytesRef termBytes = docValues.lookupOrd(segmentOrd);
            String term = termBytes.utf8ToString();
            int bucketOrd =
                LocalOrdTermsAccumulator.this.termToBucketOrd.computeIfAbsent(
                    term,
                    k -> {
                      int ord = LocalOrdTermsAccumulator.this.bucketOrdToTerm.size();
                      LocalOrdTermsAccumulator.this.bucketOrdToTerm.add(k);
                      return ord;
                    });
            children.collect(doc, bucketOrd);
          }
        }
      };
    }

    @Override
    protected List<AggregationResult> buildResults(LongArray ordsToCollect) throws IOException {
      int totalBuckets = this.bucketOrdToTerm.size();
      try (LongArray allBucketOrds = new LongArray(totalBuckets)) {
        for (int i = 0; i < totalBuckets; i++) {
          allBucketOrds.set(i, i);
        }
        IntFunction<AggregationResults> subAggsByOrd = buildSubAggsForBuckets(allBucketOrds);

        List<AggregationResult> results = new ArrayList<>();
        for (int i = 0; i < ordsToCollect.size(); i++) {
          List<TermsResult.TermsBucket> buckets = new ArrayList<>();
          for (int ord = 0; ord < totalBuckets; ord++) {
            buckets.add(
                new TermsResult.TermsBucket(
                    this.bucketOrdToTerm.get(ord), subAggsByOrd.apply(ord)));
          }
          results.add(new TermsResult(getName(), this.path, buckets));
        }
        return results;
      }
    }
  }
}
