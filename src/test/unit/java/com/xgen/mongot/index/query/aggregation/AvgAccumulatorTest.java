package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.Test;

public class AvgAccumulatorTest extends AggregationTestSupport {

  @Test
  public void groupByCityWithAvgPrice() throws Exception {
    buildIndex(
        List.of(
            new TestDoc("NYC", "x", 10),
            new TestDoc("NYC", "x", 20),
            new TestDoc("NYC", "x", 30),
            new TestDoc("LA", "x", 5),
            new TestDoc("LA", "x", 15)),
        Integer.MAX_VALUE);

    AggregationSpec avgSpec = new AvgAggregationSpec("avg_price", FieldPath.parse(PRICE_FIELD));
    AggregationSpec termsSpec =
        new TermsSpec("by_city", FieldPath.parse(CITY_FIELD), CITY_LUCENE_FIELD, List.of(avgSpec));
    TopAggregationSpec topSpec = new TopAggregationSpec(List.of(termsSpec));

    Accumulator topAccumulator = collectAll(topSpec, newContextWithFieldResolver());
    TopLevelAggregationResult top =
        (TopLevelAggregationResult) topAccumulator.buildTopLevelResult();

    TermsResult byCity = (TermsResult) top.childResults().get(0);
    Map<String, Double> avgByCity = new HashMap<>();
    for (TermsResult.TermsBucket bucket : byCity.getBuckets(BucketOrder.KEY_ASC)) {
      AvgAggregationResult avg = (AvgAggregationResult) bucket.getChildResults().get("avg_price");
      avgByCity.put(
          bucket.getKey().getValue(), ((org.bson.BsonDouble) avg.toStandaloneBson()).getValue());
    }

    assertEquals(20.0, avgByCity.get("NYC"), 0.0001);
    assertEquals(10.0, avgByCity.get("LA"), 0.0001);
  }

  @Test
  public void multiValueDoc_skipped() throws Exception {
    buildIndexWithMultiValuePriceDoc();

    AggregationSpec avgSpec = new AvgAggregationSpec("avg_price", FieldPath.parse(PRICE_FIELD));
    AggregationSpec termsSpec =
        new TermsSpec("by_city", FieldPath.parse(CITY_FIELD), CITY_LUCENE_FIELD, List.of(avgSpec));
    TopAggregationSpec topSpec = new TopAggregationSpec(List.of(termsSpec));

    Accumulator topAccumulator = collectAll(topSpec, newContextWithFieldResolver());
    TopLevelAggregationResult top =
        (TopLevelAggregationResult) topAccumulator.buildTopLevelResult();

    TermsResult byCity = (TermsResult) top.childResults().get(0);
    AvgAggregationResult avg =
        (AvgAggregationResult)
            byCity.getBuckets(BucketOrder.KEY_ASC).get(0).getChildResults().get("avg_price");
    assertEquals(0, avg.getCount());
    assertSame(BsonNull.VALUE, avg.toStandaloneBson());
  }

  @Test
  public void multiSegmentIndex_producesSameAverages() throws Exception {
    buildIndex(
        List.of(
            new TestDoc("NYC", "x", 10), new TestDoc("NYC", "x", 30), new TestDoc("LA", "x", 20)),
        1);

    AggregationSpec avgSpec = new AvgAggregationSpec("avg_price", FieldPath.parse(PRICE_FIELD));
    AggregationSpec termsSpec =
        new TermsSpec("by_city", FieldPath.parse(CITY_FIELD), CITY_LUCENE_FIELD, List.of(avgSpec));
    TopAggregationSpec topSpec = new TopAggregationSpec(List.of(termsSpec));

    Accumulator topAccumulator = collectAll(topSpec, newContextWithFieldResolver());
    TermsResult byCity =
        (TermsResult)
            ((TopLevelAggregationResult) topAccumulator.buildTopLevelResult())
                .childResults()
                .get(0);

    AvgAggregationResult nyc =
        (AvgAggregationResult)
            byCity.getBuckets(BucketOrder.KEY_ASC).get(0).getChildResults().get("avg_price");
    assertEquals(20.0, ((org.bson.BsonDouble) nyc.toStandaloneBson()).getValue(), 0.0001);
  }

  private void buildIndexWithMultiValuePriceDoc() throws IOException {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      Document luceneDoc = new Document();
      luceneDoc.add(new SortedSetDocValuesField(CITY_LUCENE_FIELD, new BytesRef("NYC")));
      luceneDoc.add(new SortedNumericDocValuesField(PRICE_LUCENE_FIELD, 10));
      luceneDoc.add(new SortedNumericDocValuesField(PRICE_LUCENE_FIELD, 20));
      writer.addDocument(luceneDoc);
    }
    this.reader = DirectoryReader.open(this.directory);
    this.searcher = new org.apache.lucene.search.IndexSearcher(this.reader);
  }

  private static final class TermsSpec extends GroupAggregationSpec {
    private final String luceneFieldName;

    TermsSpec(String name, FieldPath path, String luceneFieldName, List<AggregationSpec> children) {
      super(path, name, children);
      this.luceneFieldName = luceneFieldName;
    }

    @Override
    protected Accumulator createAccumulator(
        AggregationContext context, List<Accumulator> childAccumulators) {
      return new LocalOrdTermsAccumulator(
          this.name, this.path, this.luceneFieldName, childAccumulators, context);
    }
  }

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
