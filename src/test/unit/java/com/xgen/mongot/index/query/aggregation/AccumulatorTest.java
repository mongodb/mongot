package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Integration test driving the accumulator framework against an in-memory Lucene index. */
public class AccumulatorTest {

  private static final String CITY_FIELD = "city";
  private static final String PRICE_FIELD = "price";

  private Directory directory;
  private IndexReader reader;
  private IndexSearcher searcher;

  @Before
  public void setUp() throws Exception {
    this.directory = new ByteBuffersDirectory();
    try (IndexWriter writer = new IndexWriter(this.directory, new IndexWriterConfig())) {
      addDoc(writer, "NYC", 10);
      addDoc(writer, "NYC", 20);
      addDoc(writer, "NYC", 30);
      addDoc(writer, "LA", 5);
      addDoc(writer, "LA", 15);
    }
    this.reader = DirectoryReader.open(this.directory);
    this.searcher = new IndexSearcher(this.reader);
  }

  @After
  public void tearDown() throws Exception {
    if (this.reader != null) {
      this.reader.close();
    }
    if (this.directory != null) {
      this.directory.close();
    }
  }

  private static void addDoc(IndexWriter writer, String city, long price) throws Exception {
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField(CITY_FIELD, new BytesRef(city)));
    doc.add(new SortedNumericDocValuesField(PRICE_FIELD, price));
    writer.addDocument(doc);
  }

  @Test
  public void groupByCityWithSumPrice() throws Exception {
    AggregationSpec sumSpec = new SumSpec("sum_price", FieldPath.parse(PRICE_FIELD), PRICE_FIELD);
    AggregationSpec termsSpec =
        new TermsSpec("by_city", FieldPath.parse(CITY_FIELD), CITY_FIELD, List.of(sumSpec));
    TopAggregationSpec topSpec = new TopAggregationSpec(List.of(termsSpec));

    AggregationContext context = new TestContext(this.reader, AggregationRegistry.INSTANCE);

    Accumulator topAccumulator = topSpec.buildAccumulator(context, Optional.empty());
    Collector luceneCollector = topAccumulator.asCollector(context);
    this.searcher.search(
        new MatchAllDocsQuery(),
        new CollectorManager<Collector, Void>() {
          @Override
          public Collector newCollector() {
            // Reuse the same accumulator-backed collector across slices; this test is
            // single-threaded so a per-slice instance is unnecessary.
            return luceneCollector;
          }

          @Override
          public Void reduce(Collection<Collector> collectors) {
            return null;
          }
        });

    AggregationResult topResult = topAccumulator.buildTopLevelResult();
    assertNotNull(topResult);

    TopLevelAggregationResult top = (TopLevelAggregationResult) topResult;
    assertEquals(1, top.childResults().size());

    TermsResult byCity = (TermsResult) top.childResults().get(0);
    assertEquals("by_city", byCity.getName());
    assertEquals(2, byCity.getBuckets().size());

    Map<String, Double> sumByCity = new HashMap<>();
    for (TermsResult.TermsBucket bucket : byCity.getBuckets()) {
      SumResult sum = (SumResult) bucket.getChildResults().get("sum_price");
      sumByCity.put(bucket.getKey(), sum.value());
    }

    assertEquals(60.0, sumByCity.get("NYC"), 0.0001);
    assertEquals(20.0, sumByCity.get("LA"), 0.0001);
  }

  // ===== inline test-only spec / accumulator / result fixtures =====

  /** Inline leaf result carrying a single double value. */
  private record SumResult(String name, double value) implements AggregationResult {
    @Override
    public String getName() {
      return this.name;
    }

    @Override
    public AggregationResult mergePartitionResult(AggregationResult other) {
      return new SumResult(this.name, this.value + ((SumResult) other).value);
    }
  }

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
                      TermsBucket::getKey,
                      Function.identity(),
                      (a, b) -> {
                        throw new IllegalStateException("dup: " + a.getKey());
                      },
                      LinkedHashMap::new));
      for (TermsBucket otherBucket : that.buckets) {
        merged.merge(otherBucket.getKey(), otherBucket, TermsBucket::merge);
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
      public String getKey() {
        return this.key;
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

  /** Inline sum spec — uses raw Lucene field name (bypasses field-type resolution). */
  private static final class SumSpec extends OperatorAggregationSpec {
    @SuppressWarnings("unused")
    private final FieldPath path;

    private final String luceneFieldName;

    SumSpec(String name, FieldPath path, String luceneFieldName) {
      super(Operator.SUM, name);
      this.path = path;
      this.luceneFieldName = luceneFieldName;
    }

    @Override
    public Accumulator buildAccumulator(AggregationContext context, Optional<Accumulator> parent) {
      return new SumAccumulator(this.name, this.luceneFieldName, context, parent);
    }

    @Override
    public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
      throw new UnsupportedOperationException("not exercised in AccumulatorTest");
    }
  }

  /** Inline terms spec — uses raw Lucene field name. */
  private static final class TermsSpec extends GroupAggregationSpec {
    private final String luceneFieldName;

    TermsSpec(String name, FieldPath path, String luceneFieldName, List<AggregationSpec> children) {
      super(path, name, children);
      this.luceneFieldName = luceneFieldName;
    }

    @Override
    protected Accumulator createAccumulator(
        AggregationContext context, Optional<Accumulator> parent) {
      return new TermsAccumulator(
          this.name, this.path, this.luceneFieldName, this.children, context, parent);
    }
  }

  /** Inline leaf metric accumulator. */
  private static final class SumAccumulator extends Accumulator {
    private final String luceneFieldName;
    private final DoubleArray sums;

    SumAccumulator(
        String name,
        String luceneFieldName,
        AggregationContext context,
        Optional<Accumulator> parent) {
      super(name, parent, List.of(), context);
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

        @Override
        public void setScorer(Scorable scorer) throws IOException {}
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

  /**
   * Inline terms accumulator -- simple per-segment local-ord strategy. Intended for single-segment
   * tests; reads the field by Lucene field name directly to avoid relying on field-type resolution.
   */
  private static final class TermsAccumulator extends BucketsAccumulator {
    private final FieldPath path;
    private final String luceneFieldName;
    private final Map<String, Integer> termToBucketOrd = new HashMap<>();
    private final List<String> bucketOrdToTerm = new ArrayList<>();

    TermsAccumulator(
        String name,
        FieldPath path,
        String luceneFieldName,
        List<AggregationSpec> childrenSpecs,
        AggregationContext context,
        Optional<Accumulator> parent) {
      super(name, parent, childrenSpecs, context);
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
                TermsAccumulator.this.termToBucketOrd.computeIfAbsent(
                    term,
                    k -> {
                      int ord = TermsAccumulator.this.bucketOrdToTerm.size();
                      TermsAccumulator.this.bucketOrdToTerm.add(k);
                      return ord;
                    });
            children.collect(doc, bucketOrd);
          }
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {}
      };
    }

    @Override
    protected List<AggregationResult> buildResults(LongArray ordsToCollect) {
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

  /**
   * Minimal inline AggregationContext for the integration test. Threads an IndexReader and the
   * current LeafReaderContext through to the framework; the field-type resolver is not used here
   * because the inline specs read Lucene field names directly.
   */
  private static final class TestContext implements AggregationContext {
    private final IndexReader indexReader;
    private final AggregationRegistry registry;
    @Nullable private final LeafReaderContext leafReaderContext;

    TestContext(IndexReader indexReader, AggregationRegistry registry) {
      this(indexReader, registry, null);
    }

    private TestContext(
        IndexReader indexReader,
        AggregationRegistry registry,
        @Nullable LeafReaderContext leafReaderContext) {
      this.indexReader = indexReader;
      this.registry = registry;
      this.leafReaderContext = leafReaderContext;
    }

    @Override
    public AggregationContext withLeafReaderContext(LeafReaderContext context) {
      return new TestContext(this.indexReader, this.registry, context);
    }

    @Override
    public FieldTypeDefinition resolveFieldDefinition(FieldPath path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AggregationRegistry getRegistry() {
      return this.registry;
    }

    @Override
    public LeafReaderContext getLeafReaderContext() {
      if (this.leafReaderContext == null) {
        throw new IllegalStateException("LeafReaderContext not bound");
      }
      return this.leafReaderContext;
    }

    @Override
    public IndexReader getIndexReader() {
      return this.indexReader;
    }
  }
}
