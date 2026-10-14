package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.lucene.field.FieldName;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
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
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
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
  protected static final String DOUBLE_PRICE_LUCENE_FIELD =
      TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(FieldPath.parse(PRICE_FIELD), Optional.empty());

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
        // Token-valued metrics (first/last) read stored source, so every fixture document carries
        // one alongside its doc values.
        addStoredSource(
            luceneDoc,
            new BsonDocument()
                .append(CITY_FIELD, new BsonString(doc.city()))
                .append(COLOR_FIELD, new BsonString(doc.color()))
                .append(PRICE_FIELD, new BsonInt64(doc.price())));
        writer.addDocument(luceneDoc);
        if ((i + 1) % docsPerSegment == 0) {
          writer.commit();
        }
      }
    }
    this.reader = DirectoryReader.open(this.directory);
    this.searcher = new IndexSearcher(this.reader);
  }

  /** One doc with multiple group-key tokens and a single metric value. */
  protected void buildIndexWithMultiValueCityDoc(long price) throws IOException {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      Document luceneDoc = new Document();
      luceneDoc.add(new SortedSetDocValuesField(CITY_LUCENE_FIELD, new BytesRef("NYC")));
      luceneDoc.add(new SortedSetDocValuesField(CITY_LUCENE_FIELD, new BytesRef("LA")));
      luceneDoc.add(new SortedNumericDocValuesField(PRICE_LUCENE_FIELD, price));
      addStoredSource(
          luceneDoc,
          new BsonDocument()
              .append(
                  CITY_FIELD, new BsonArray(List.of(new BsonString("NYC"), new BsonString("LA"))))
              .append(PRICE_FIELD, new BsonInt64(price)));
      writer.addDocument(luceneDoc);
    }
    this.reader = DirectoryReader.open(this.directory);
    this.searcher = new IndexSearcher(this.reader);
  }

  /**
   * Writes {@code source} as the document's stored source, which is where token-valued metrics read
   * their values from.
   */
  protected static void addStoredSource(Document doc, BsonDocument source) {
    // Encode straight to BSON rather than round-tripping through JSON, which would widen or narrow
    // numeric types (e.g. an int64 price coming back as an int32).
    doc.add(
        new StoredField(
            FieldName.StaticField.STORED_SOURCE.getLuceneFieldName(),
            new RawBsonDocument(source, new BsonDocumentCodec()).getByteBuffer().array()));
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

  /** Like {@link #newContext} but records per-node explain counters. */
  protected AggregationContext newExplainContext() {
    return new TestContext(this.reader, true);
  }

  protected static AggregationResult mergePartitionResults(
      AggregationResult left, AggregationResult right) {
    AggregationResultBuilder builder = left.toBuilder();
    builder.add(left);
    builder.add(right);
    return builder.build();
  }

  protected AggregationContext newContextWithHeapAccountant(
      AggregationHeapAccountant heapAccountant) {
    return AggregationContext.create(
        this.reader,
        Optional.empty(),
        luceneField -> Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        0,
        false,
        heapAccountant);
  }

  protected AggregationContext newContextWithFieldResolver() {
    return AggregationContext.create(
        this.reader,
        Optional.of(
            new AggregationFieldResolver() {
              @Override
              public ResolvedNumericMetricField resolveNumeric(FieldPath path) {
                if (path.equals(FieldPath.parse(PRICE_FIELD))) {
                  return new ResolvedNumericMetricField(
                      PRICE_LUCENE_FIELD, NumericFieldOptions.Representation.INT64);
                }
                throw new IllegalStateException("unexpected path: " + path);
              }

              @Override
              public ResolvedTokenField resolveToken(FieldPath path) {
                throw new IllegalStateException("unexpected path: " + path);
              }
            }),
        luceneField -> Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        0,
        false,
        AggregationHeapAccountant.disabled());
  }

  /** Like {@link #newContextWithFieldResolver} but records per-node explain counters. */
  protected AggregationContext newExplainContextWithFieldResolver() {
    return AggregationContext.create(
        this.reader,
        Optional.of(
            new AggregationFieldResolver() {
              @Override
              public ResolvedNumericMetricField resolveNumeric(FieldPath path) {
                if (path.equals(FieldPath.parse(PRICE_FIELD))) {
                  return new ResolvedNumericMetricField(
                      PRICE_LUCENE_FIELD, NumericFieldOptions.Representation.INT64);
                }
                throw new IllegalStateException("unexpected path: " + path);
              }

              @Override
              public ResolvedTokenField resolveToken(FieldPath path) {
                throw new IllegalStateException("unexpected path: " + path);
              }
            }),
        luceneField -> Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        0,
        true,
        AggregationHeapAccountant.disabled());
  }

  protected AggregationContext newContextWithDoubleFieldResolver() {
    return AggregationContext.create(
        this.reader,
        Optional.of(
            new AggregationFieldResolver() {
              @Override
              public ResolvedNumericMetricField resolveNumeric(FieldPath path) {
                if (path.equals(FieldPath.parse(PRICE_FIELD))) {
                  return new ResolvedNumericMetricField(
                      DOUBLE_PRICE_LUCENE_FIELD, NumericFieldOptions.Representation.DOUBLE);
                }
                throw new IllegalStateException("unexpected path: " + path);
              }

              @Override
              public ResolvedTokenField resolveToken(FieldPath path) {
                throw new IllegalStateException("unexpected path: " + path);
              }
            }),
        luceneField -> Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        0,
        false,
        AggregationHeapAccountant.disabled());
  }

  // ===== assertion helpers =====

  /** Two-result convenience over the seed-add-build {@link AggregationResultBuilder} protocol. */
  protected static AggregationResult merge(AggregationResult a, AggregationResult b) {
    AggregationResultBuilder builder = a.toBuilder();
    builder.add(a);
    builder.add(b);
    return builder.build();
  }

  protected static List<String> bucketKeysInOrder(TermsAggregationResult result) {
    return bucketKeysInOrder(result, BucketOrder.KEY_ASC);
  }

  /** Bucket keys in the requested read-time order (e.g. COUNT_DESC for top-N presentation). */
  protected static List<String> bucketKeysInOrder(
      TermsAggregationResult result, BucketOrder order) {
    return result.getBuckets(order).stream()
        .map(bucket -> ((BsonString) bucket.getKey()).getValue())
        .collect(Collectors.toList());
  }

  protected static Set<String> bucketKeys(TermsAggregationResult result) {
    return result.getBuckets(BucketOrder.KEY_ASC).stream()
        .map(bucket -> ((BsonString) bucket.getKey()).getValue())
        .collect(Collectors.toSet());
  }

  protected static TermsBucket bucket(TermsAggregationResult result, String key) {
    return result.getBuckets(BucketOrder.KEY_ASC).stream()
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
    public AggregationResultBuilder toBuilder() {
      return TestOperatorSpecs.folding(
          (a, b) -> {
            AvgResult left = (AvgResult) a;
            AvgResult right = Check.instanceOf(b, AvgResult.class);
            return new AvgResult(left.name, left.sum + right.sum, left.count + right.count);
          });
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
    public List<TermsBucket> getBuckets(BucketOrder order) {
      // Test fake: insertion order regardless of the requested order.
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
        finalized.add(new TermsBucket(b.key, b.docCount, b.getChildResults().applyLimit()));
      }
      return new TermsResult(this.name, this.path, finalized);
    }

    protected static final class TermsBucket implements Bucket {
      private final String key;
      private final long docCount;
      private final AggregationResults childResults;

      protected TermsBucket(String key, AggregationResults childResults) {
        this(key, 1L, childResults);
      }

      protected TermsBucket(String key, long docCount, AggregationResults childResults) {
        this.key = key;
        this.docCount = docCount;
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
        return this.docCount;
      }

      private static TermsBucket merge(TermsBucket a, TermsBucket b) {
        List<AggregationResult> merged = new ArrayList<>();
        for (AggregationResult lhs : a.childResults) {
          AggregationResultBuilder builder = lhs.toBuilder();
          builder.add(lhs);
          builder.add(b.childResults.get(lhs.getName()));
          merged.add(builder.build());
        }
        return new TermsBucket(a.key, a.docCount + b.docCount, AggregationResults.from(merged));
      }
    }
  }

  // ===== shared AggregationContext =====

  /**
   * Minimal {@link AggregationContext} threading an IndexReader and the current LeafReaderContext.
   */
  protected static final class TestContext implements AggregationContext {
    private final IndexReader indexReader;
    private final CachedGlobalValueLookup cachedGlobalValueLookup;
    private final Optional<LeafReaderContext> leafReaderContext;
    private final Optional<MultiValueDocMarker> multiValueDocMarker;
    private final Optional<AggregationFieldResolver> fieldResolver;
    private final Optional<Sort> sort;
    private final Optional<Sort> invertedSort;
    private final int partitionId;
    private final AtomicLong observedBuckets;
    private final boolean recordExplainNodeStats;
    private final AggregationHeapAccountant heapAccountant;

    protected TestContext(IndexReader indexReader) {
      this(indexReader, false);
    }

    protected TestContext(IndexReader indexReader, boolean recordExplainNodeStats) {
      this(
          indexReader,
          luceneField -> Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          0,
          new AtomicLong(),
          recordExplainNodeStats,
          AggregationHeapAccountant.disabled());
    }

    protected TestContext(
        IndexReader indexReader, CachedGlobalValueLookup cachedGlobalValueLookup) {
      this(
          indexReader,
          cachedGlobalValueLookup,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());
    }

    /**
     * Context wired for metric operators that resolve a field and may borrow a global sort, in the
     * two orientations {@code first} and {@code last} collect under.
     */
    protected TestContext(
        IndexReader indexReader,
        Optional<AggregationFieldResolver> fieldResolver,
        Optional<Sort> sort,
        Optional<Sort> invertedSort) {
      this(indexReader, fieldResolver, sort, invertedSort, 0);
    }

    /** As above, standing in for a specific index partition. */
    protected TestContext(
        IndexReader indexReader,
        Optional<AggregationFieldResolver> fieldResolver,
        Optional<Sort> sort,
        Optional<Sort> invertedSort,
        int partitionId) {
      this(
          indexReader,
          luceneField -> Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          fieldResolver,
          sort,
          invertedSort,
          partitionId);
    }

    private TestContext(
        IndexReader indexReader,
        CachedGlobalValueLookup cachedGlobalValueLookup,
        Optional<LeafReaderContext> leafReaderContext,
        Optional<MultiValueDocMarker> multiValueDocMarker,
        Optional<AggregationFieldResolver> fieldResolver,
        Optional<Sort> sort,
        Optional<Sort> invertedSort) {
      this(
          indexReader,
          cachedGlobalValueLookup,
          leafReaderContext,
          multiValueDocMarker,
          fieldResolver,
          sort,
          invertedSort,
          0);
    }

    private TestContext(
        IndexReader indexReader,
        CachedGlobalValueLookup cachedGlobalValueLookup,
        Optional<LeafReaderContext> leafReaderContext,
        Optional<MultiValueDocMarker> multiValueDocMarker,
        Optional<AggregationFieldResolver> fieldResolver,
        Optional<Sort> sort,
        Optional<Sort> invertedSort,
        int partitionId) {
      this(
          indexReader,
          cachedGlobalValueLookup,
          leafReaderContext,
          multiValueDocMarker,
          fieldResolver,
          sort,
          invertedSort,
          partitionId,
          new AtomicLong());
    }

    private TestContext(
        IndexReader indexReader,
        CachedGlobalValueLookup cachedGlobalValueLookup,
        Optional<LeafReaderContext> leafReaderContext,
        Optional<MultiValueDocMarker> multiValueDocMarker,
        Optional<AggregationFieldResolver> fieldResolver,
        Optional<Sort> sort,
        Optional<Sort> invertedSort,
        int partitionId,
        AtomicLong observedBuckets) {
      this(
          indexReader,
          cachedGlobalValueLookup,
          leafReaderContext,
          multiValueDocMarker,
          fieldResolver,
          sort,
          invertedSort,
          partitionId,
          observedBuckets,
          false,
          AggregationHeapAccountant.disabled());
    }

    private TestContext(
        IndexReader indexReader,
        CachedGlobalValueLookup cachedGlobalValueLookup,
        Optional<LeafReaderContext> leafReaderContext,
        Optional<MultiValueDocMarker> multiValueDocMarker,
        Optional<AggregationFieldResolver> fieldResolver,
        Optional<Sort> sort,
        Optional<Sort> invertedSort,
        int partitionId,
        AtomicLong observedBuckets,
        boolean recordExplainNodeStats,
        AggregationHeapAccountant heapAccountant) {
      this.partitionId = partitionId;
      this.indexReader = indexReader;
      this.cachedGlobalValueLookup = cachedGlobalValueLookup;
      this.leafReaderContext = leafReaderContext;
      this.multiValueDocMarker = multiValueDocMarker;
      this.fieldResolver = fieldResolver;
      this.sort = sort;
      this.invertedSort = invertedSort;
      this.observedBuckets = observedBuckets;
      this.recordExplainNodeStats = recordExplainNodeStats;
      this.heapAccountant = heapAccountant;
    }

    @Override
    public AggregationContext withLeafReaderContext(LeafReaderContext context) {
      this.multiValueDocMarker.ifPresent(MultiValueDocMarker::reset);
      return new TestContext(
          this.indexReader,
          this.cachedGlobalValueLookup,
          Optional.of(context),
          this.multiValueDocMarker,
          this.fieldResolver,
          this.sort,
          this.invertedSort,
          this.partitionId,
          this.observedBuckets,
          this.recordExplainNodeStats,
          this.heapAccountant);
    }

    @Override
    public AggregationContext withMultiValueDocMarker(MultiValueDocMarker marker) {
      return new TestContext(
          this.indexReader,
          this.cachedGlobalValueLookup,
          this.leafReaderContext,
          Optional.of(marker),
          this.fieldResolver,
          this.sort,
          this.invertedSort,
          this.partitionId,
          this.observedBuckets,
          this.recordExplainNodeStats,
          this.heapAccountant);
    }

    @Override
    public AggregationHeapAccountant getHeapAccountant() {
      return this.heapAccountant;
    }

    @Override
    public AggregationRegistry getRegistry() {
      return AggregationRegistry.getInstance();
    }

    @Override
    public CachedGlobalValueLookup getCachedGlobalValueLookup() {
      return this.cachedGlobalValueLookup;
    }

    @Override
    public LeafReaderContext getLeafReaderContext() {
      return this.leafReaderContext.orElseThrow(
          () -> new IllegalStateException("LeafReaderContext not bound"));
    }

    @Override
    public IndexReader getIndexReader() {
      return this.indexReader;
    }

    @Override
    public Optional<AggregationFieldResolver> getFieldResolver() {
      return this.fieldResolver;
    }

    @Override
    public Optional<Sort> getSort() {
      return this.sort;
    }

    @Override
    public Optional<Sort> getInvertedSort() {
      return this.invertedSort;
    }

    @Override
    public int getPartitionId() {
      return this.partitionId;
    }

    @Override
    public Optional<MultiValueDocMarker> getMultiValueDocMarker() {
      return this.multiValueDocMarker;
    }

    @Override
    public void addObservedBuckets(long count) {
      this.observedBuckets.addAndGet(count);
    }

    @Override
    public long observedBuckets() {
      return this.observedBuckets.get();
    }

    @Override
    public boolean recordExplainNodeStats() {
      return this.recordExplainNodeStats;
    }
  }
}
