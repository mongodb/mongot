package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.TermsAggregationResult.TermsBucket;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;
import org.junit.After;

/**
 * Shared scaffolding for aggregation accumulator tests: an in-memory Lucene index with token and
 * numeric fields, a minimal {@link AggregationContext}, a reusable {@code $sum} leaf fixture, and
 * helpers for driving a query and reading {@link TermsAggregationResult} buckets.
 */
public abstract class AggregationTestSupport {

  protected static final String CITY_FIELD = "city";
  protected static final String COLOR_FIELD = "color";
  protected static final String PRICE_FIELD = "price";

  // Prefixed Lucene field names matching the TypeField encoding the accumulators use.
  protected static final String CITY_LUCENE_FIELD =
      TypeField.TOKEN.getLuceneFieldName(FieldPath.parse(CITY_FIELD), Optional.empty());
  protected static final String COLOR_LUCENE_FIELD =
      TypeField.TOKEN.getLuceneFieldName(FieldPath.parse(COLOR_FIELD), Optional.empty());
  protected static final String PRICE_LUCENE_FIELD =
      TypeField.NUMBER_INT64_V2.getLuceneFieldName(FieldPath.parse(PRICE_FIELD), Optional.empty());

  protected Directory directory;
  protected IndexReader reader;
  protected IndexSearcher searcher;

  /** One indexed document: a city + color token field and a numeric price. */
  protected record TestDoc(String city, String color, long price) {}

  @After
  public void tearDownIndex() throws IOException {
    if (this.reader != null) {
      this.reader.close();
    }
    if (this.directory != null) {
      this.directory.close();
    }
  }

  /**
   * Build an in-memory index from {@code docs}, committing every {@code docsPerSegment} documents
   * so the test can control the segment count (e.g. force the multi-segment global-ordinal path).
   * NoMergePolicy keeps the committed segments from being merged back together.
   */
  protected void buildIndex(List<TestDoc> docs, int docsPerSegment) throws IOException {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      for (int i = 0; i < docs.size(); i++) {
        TestDoc doc = docs.get(i);
        Document luceneDoc = new Document();
        luceneDoc.add(new SortedSetDocValuesField(CITY_LUCENE_FIELD, new BytesRef(doc.city())));
        luceneDoc.add(new SortedSetDocValuesField(COLOR_LUCENE_FIELD, new BytesRef(doc.color())));
        luceneDoc.add(new SortedNumericDocValuesField(PRICE_LUCENE_FIELD, doc.price()));
        writer.addDocument(luceneDoc);
        if ((i + 1) % docsPerSegment == 0) {
          writer.commit();
        }
      }
    }
    this.reader = DirectoryReader.open(this.directory);
    this.searcher = new IndexSearcher(this.reader);
  }

  /** Build the accumulator tree for {@code spec}, run {@code query}, and return the root. */
  protected Accumulator collect(TopAggregationSpec spec, AggregationContext context, Query query)
      throws IOException {
    Accumulator top = spec.buildAccumulator(context);
    Collector collector = top.asCollector(context);
    this.searcher.search(
        query,
        new CollectorManager<Collector, Void>() {
          @Override
          public Collector newCollector() {
            // Single-threaded test: reuse one accumulator-backed collector across slices.
            return collector;
          }

          @Override
          public Void reduce(Collection<Collector> collectors) {
            return null;
          }
        });
    return top;
  }

  /** Convenience over {@link #collect} with a match-all query. */
  protected Accumulator collectAll(TopAggregationSpec spec, AggregationContext context)
      throws IOException {
    return collect(spec, context, new MatchAllDocsQuery());
  }

  protected AggregationContext newContext() {
    return new TestContext(this.reader);
  }

  // ===== assertion helpers =====

  protected static List<String> bucketKeysInOrder(TermsAggregationResult result) {
    return result.getBuckets().stream()
        .map(bucket -> ((BsonString) bucket.getKey()).getValue())
        .collect(Collectors.toList());
  }

  protected static Set<String> bucketKeys(TermsAggregationResult result) {
    return result.getBuckets().stream()
        .map(bucket -> ((BsonString) bucket.getKey()).getValue())
        .collect(Collectors.toSet());
  }

  protected static TermsBucket bucket(TermsAggregationResult result, String key) {
    return result.getBuckets().stream()
        .map(b -> (TermsBucket) b)
        .filter(b -> b.getKey().getValue().equals(key))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no bucket for key: " + key));
  }

  protected static double sumOf(TermsBucket bucket, String childName) {
    return ((SumResult) bucket.getChildResults().get(childName)).value();
  }

  protected static TermsAggregationResult childTerms(TermsBucket bucket, String childName) {
    return (TermsAggregationResult) bucket.getChildResults().get(childName);
  }

  // ===== shared $sum leaf fixture (reads PRICE_FIELD by raw Lucene name) =====

  /** Inline leaf result carrying a single double value. */
  protected record SumResult(String name, double value) implements AggregationResult {
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
  }

  /** Inline sum spec -- uses the raw Lucene field name (bypasses field-type resolution). */
  protected static final class SumSpec extends OperatorAggregationSpec {
    private final String luceneFieldName;

    protected SumSpec(String name, String luceneFieldName) {
      super(Operator.SUM, name);
      this.luceneFieldName = luceneFieldName;
    }

    @Override
    public Accumulator buildAccumulator(AggregationContext context) {
      return new SumAccumulator(this.name, this.luceneFieldName, context);
    }

    @Override
    public void buildShardMergePipeline(ShardMergePipelineBuilder builder) {
      throw new UnsupportedOperationException("not exercised in accumulator tests");
    }
  }

  /** Inline leaf metric accumulator summing a numeric field per owning bucket ordinal. */
  protected static final class SumAccumulator extends Accumulator {
    private final String luceneFieldName;
    private final DoubleArray sums;

    protected SumAccumulator(String name, String luceneFieldName, AggregationContext context) {
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

  // ===== shared AggregationContext =====

  /**
   * Minimal {@link AggregationContext} threading an IndexReader and the current LeafReaderContext.
   */
  protected static final class TestContext implements AggregationContext {
    private final IndexReader indexReader;
    @Nullable private final LeafReaderContext leafReaderContext;

    protected TestContext(IndexReader indexReader) {
      this(indexReader, null);
    }

    private TestContext(IndexReader indexReader, @Nullable LeafReaderContext leafReaderContext) {
      this.indexReader = indexReader;
      this.leafReaderContext = leafReaderContext;
    }

    @Override
    public AggregationContext withLeafReaderContext(LeafReaderContext context) {
      return new TestContext(this.indexReader, context);
    }

    @Override
    public AggregationRegistry getRegistry() {
      return AggregationRegistry.getInstance();
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
