package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.TermsAggregationResult.TermsBucket;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;
import org.junit.After;

/**
 * Shared scaffolding for aggregation accumulator tests: an in-memory Lucene index with token and
 * numeric fields, a minimal {@link AggregationContext}, reusable {@link TestOperatorSpecs} leaf
 * fixtures, and helpers for driving a query and reading {@link TermsAggregationResult} buckets.
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
    return ((TestOperatorSpecs.SumResult) bucket.getChildResults().get(childName)).value();
  }

  protected static TermsAggregationResult childTerms(TermsBucket bucket, String childName) {
    return (TermsAggregationResult) bucket.getChildResults().get(childName);
  }

  // ===== shared $sum leaf fixture (reads PRICE_FIELD by raw Lucene name) =====

  // ===== shared result-tree fixtures (until official aggregator results land) =====

  /** Inline avg leaf result for intermediate BSON and merge tests. */
  protected static record AvgResult(String name, double sum, long count)
      implements AggregationResult {
    @Override
    public String getName() {
      return this.name;
    }

    @Override
    public AggregationResult mergePartitionResult(AggregationResult other) {
      AvgResult that = Check.instanceOf(other, AvgResult.class);
      return new AvgResult(this.name, this.sum + that.sum, this.count + that.count);
    }

    @Override
    public AggregationResult applyLimit() {
      // Leaf metric: no buckets to truncate.
      return this;
    }

    @Override
    public BsonValue toStandaloneBson() {
      return new BsonDocument(
          this.name, this.count == 0 ? new BsonDouble(0) : new BsonDouble(this.sum / this.count));
    }

    @Override
    public BsonArray toIntermediateBson() {
      return new BsonArray();
    }

    @Override
    public BsonDocument toIntermediateMetricPartials() {
      return new BsonDocument()
          .append("avg_sum_" + this.name, new BsonDouble(this.sum))
          .append("avg_count_" + this.name, new BsonInt64(this.count));
    }
  }

  /** Inline terms group result for intermediate BSON and merge tests. */
  protected static final class TermsResult extends MultiBucketAggregationResult {
    private final String name;
    private final List<TermsBucket> buckets;

    protected TermsResult(String name, FieldPath path, List<TermsBucket> buckets) {
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

    protected static final class TermsBucket implements Bucket {
      private final String key;
      private final AggregationResults childResults;

      protected TermsBucket(String key, AggregationResults childResults) {
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

      private static TermsBucket merge(TermsBucket a, TermsBucket b) {
        List<AggregationResult> merged = new ArrayList<>();
        for (AggregationResult lhs : a.childResults) {
          merged.add(lhs.mergePartitionResult(b.childResults.get(lhs.getName())));
        }
        return new TermsBucket(a.key, AggregationResults.from(merged));
      }
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
