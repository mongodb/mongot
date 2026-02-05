package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.index.IntermediateFacetBucket;
import com.xgen.mongot.index.definition.StringFacetFieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.LuceneIndexRule;
import com.xgen.testing.mongot.cursor.batch.BatchCursorOptionsBuilder;
import com.xgen.testing.mongot.index.query.CollectorQueryBuilder;
import com.xgen.testing.mongot.index.query.collectors.FacetCollectorBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LuceneFacetDrillSidewaysMetaBatchProducerFactoryTest {

  private static final String STRING_FACET_NAME = "colors";
  private static final String NUMERIC_FACET_NAME = "sizes";
  private static final String DATE_FACET_NAME = "releaseDates";

  private static final String STRING_FACET_PATH = "color";
  private static final String NUMERIC_FACET_PATH = "size";
  private static final String DATE_FACET_PATH = "releaseDate";

  private static final FacetDefinition.NumericFacetDefinition NUMERIC_FACET_DEFINITION =
      new FacetDefinition.NumericFacetDefinition(
          NUMERIC_FACET_PATH,
          Optional.empty(),
          List.of(new BsonInt64(0L), new BsonInt64(50L), new BsonInt64(100L)));

  private static final FacetDefinition.DateFacetDefinition DATE_FACET_DEFINITION =
      new FacetDefinition.DateFacetDefinition(
          DATE_FACET_PATH,
          Optional.empty(),
          List.of(new BsonDateTime(1000L), new BsonDateTime(2000L), new BsonDateTime(3000L)));

  private static final FacetDefinition.StringFacetDefinition STRING_FACET_DEFINITION =
      new FacetDefinition.StringFacetDefinition(STRING_FACET_PATH, 3);

  private static final TopDocs TOP_DOCS_WITH_NON_EXACT_COUNT =
      new TopDocs(
          new TotalHits(100L, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), new ScoreDoc[0]);
  private static final int NUM_BUCKETS = 2;
  private static final int NUM_COUNT_DOCS = 1;

  private static final BsonDocument COUNT_BUCKET_DOC =
      new BsonDocument("type", new BsonString("count")).append("count", new BsonInt64(1));

  private static final TopDocs TOP_DOCS =
      new TopDocs(new TotalHits(100, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);

  @Mock private SortedSetDocValuesReaderState facetState;
  @Mock private LuceneIndexSearcherReference searcherReference;
  @Mock private LuceneFacetContext facetContext;
  @Mock private LuceneIndexSearcher searcher;

  private Directory directory;
  private IndexWriter indexWriter;
  private FacetsConfig facetsConfig;

  private Queue<LuceneMetaBucketProducer> bucketProducers;
  private LuceneFacetCollectorMetaBatchProducer batchProducer;
  private Map<String, FacetDefinition> facetNameToDefinition;
  private CollectorQuery collectorQuery;
  private FacetCollector facetCollector;

  @Before
  public void setUp() throws Exception {
    this.bucketProducers = new LinkedList<>();

    this.directory = new ByteBuffersDirectory();
    this.indexWriter = new IndexWriter(this.directory, LuceneIndexRule.getIndexWriterConfig());
    this.facetsConfig = new FacetsConfig();

    when(this.searcherReference.getIndexSearcher()).thenReturn(this.searcher);
    when(this.searcher.getFacetsState()).thenReturn(Optional.of(this.facetState));
    // Return the actual path from the facet definition so it matches what we index
    when(this.facetContext.getBoundaryFacetPath(any(), any()))
        .thenAnswer(
            invocation -> {
              FacetDefinition.BoundaryFacetDefinition<?> def = invocation.getArgument(0);
              return def.path();
            });
    when(this.facetContext.getStringFacetFieldDefinition(any(), any()))
        .thenReturn(new StringFacetFieldDefinition());
    this.facetNameToDefinition = new HashMap<>();
    this.facetCollector =
        FacetCollectorBuilder.facet()
            .operator(OperatorBuilder.exists().path("_id").build())
            .facetDefinitions(this.facetNameToDefinition)
            .build();
    this.collectorQuery =
        CollectorQueryBuilder.builder()
            .collector(this.facetCollector)
            .index(SearchIndex.MOCK_INDEX_NAME)
            .returnScope(new ReturnScope(FieldPath.parse("custom.scope")))
            .returnStoredSource(false)
            .build();
  }

  @After
  public void tearDown() throws Exception {
    this.indexWriter.close();
    this.directory.close();
  }

  @Test(expected = IllegalStateException.class)
  public void create_nonExactCount_throwsException() throws Exception {
    Map<String, DrillSidewaysResult> facetToDrillSidewaysResult = Map.of();

    LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
        this.collectorQuery,
        this.facetContext,
        this.searcherReference,
        TOP_DOCS_WITH_NON_EXACT_COUNT,
        facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)),
        Optional.empty());
  }

  @Test
  public void create_emptyFacetDefinitions_returnsExhaustedProducer() throws Exception {
    Map<String, DrillSidewaysResult> facetToDrillSidewaysResult = Map.of();

    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)),
            Optional.empty());

    assertNotNull(result);
    assertTrue(result.isExhausted());
  }

  @Test
  public void create_genericDrillSidewaysResult_numberFacets_succeeds() throws Exception {
    this.facetNameToDefinition.put(NUMERIC_FACET_NAME, NUMERIC_FACET_DEFINITION);
    Map<String, DrillSidewaysResult> facetToDrillSidewaysResult =
        createDrillSidewaysResults(List.of(NUMERIC_FACET_NAME), List.of(NUMERIC_FACET_PATH));

    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)),
            Optional.empty());

    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void create_genericDrillSidewaysResult_dateFacets_succeeds() throws Exception {
    this.facetNameToDefinition.put(DATE_FACET_NAME, DATE_FACET_DEFINITION);
    Map<String, DrillSidewaysResult> facetToDrillSidewaysResult =
        createDrillSidewaysResults(List.of(DATE_FACET_NAME), List.of(DATE_FACET_PATH));

    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)),
            Optional.empty());

    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void create_genericDrillSidewaysResult_stringFacets_succeeds() throws Exception {
    this.facetNameToDefinition.put(STRING_FACET_NAME, STRING_FACET_DEFINITION);
    Map<String, DrillSidewaysResult> facetToDrillSidewaysResult =
        createDrillSidewaysResults(List.of(STRING_FACET_NAME), List.of(STRING_FACET_PATH));

    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)),
            Optional.empty());

    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void create_genericDrillSidewaysResult_mixedFacetTypes_succeeds() throws Exception {
    this.facetNameToDefinition.put(STRING_FACET_NAME, STRING_FACET_DEFINITION);
    this.facetNameToDefinition.put(NUMERIC_FACET_NAME, NUMERIC_FACET_DEFINITION);
    this.facetNameToDefinition.put(DATE_FACET_NAME, DATE_FACET_DEFINITION);
    Map<String, DrillSidewaysResult> facetToDrillSidewaysResult =
        createDrillSidewaysResults(
            List.of(STRING_FACET_NAME, DATE_FACET_NAME, NUMERIC_FACET_NAME),
            List.of(STRING_FACET_PATH, DATE_FACET_PATH, NUMERIC_FACET_PATH));

    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)),
            Optional.empty());

    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void create_optimizedDrillSidewaysResult_numberFacets_succeeds() throws Exception {
    this.facetNameToDefinition.put(NUMERIC_FACET_NAME, NUMERIC_FACET_DEFINITION);

    DrillSidewaysResult singleSharedResult =
        createOptimizedDrillSidewaysResult(List.of(NUMERIC_FACET_PATH));

    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetKey -> Optional.of(singleSharedResult),
            Optional.empty());

    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void create_optimizedDrillSidewaysResult_dateFacets_succeeds() throws Exception {
    this.facetNameToDefinition.put(DATE_FACET_NAME, DATE_FACET_DEFINITION);

    DrillSidewaysResult singleSharedResult =
        createOptimizedDrillSidewaysResult(List.of(DATE_FACET_PATH));

    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetKey -> Optional.of(singleSharedResult),
            Optional.empty());

    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void create_optimizedDrillSidewaysResult_stringFacets_succeeds() throws Exception {
    this.facetNameToDefinition.put(STRING_FACET_NAME, STRING_FACET_DEFINITION);

    DrillSidewaysResult singleSharedResult =
        createOptimizedDrillSidewaysResult(List.of(STRING_FACET_PATH));

    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetKey -> Optional.of(singleSharedResult),
            Optional.empty());

    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void create_optimizedDrillSidewaysResult_mixedFacetTypes_succeeds() throws Exception {
    this.facetNameToDefinition.put(STRING_FACET_NAME, STRING_FACET_DEFINITION);
    this.facetNameToDefinition.put(NUMERIC_FACET_NAME, NUMERIC_FACET_DEFINITION);
    this.facetNameToDefinition.put(DATE_FACET_NAME, DATE_FACET_DEFINITION);

    DrillSidewaysResult singleSharedResult =
        createOptimizedDrillSidewaysResult(
            List.of(STRING_FACET_PATH, NUMERIC_FACET_PATH, DATE_FACET_PATH));

    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetKey -> Optional.of(singleSharedResult),
            Optional.empty());

    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void create_optimizedDrillSidewaysResult_noFacets_returnsExhaustedProducer()
      throws Exception {
    DrillSidewaysResult singleSharedResult =
        createOptimizedDrillSidewaysResult(List.of("mock_path"));

    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetKey -> Optional.of(singleSharedResult),
            Optional.empty());

    assertNotNull(result);
    assertTrue(result.isExhausted());
  }

  @Test
  public void create_tokenFacets_missingFirstDrillSidewaysResult_doesNotSkipSecondFacet()
      throws Exception {

    when(this.facetContext.getStringFacetFieldDefinition(any(), any()))
        .thenReturn(new com.xgen.mongot.index.definition.TokenFieldDefinition(Optional.empty()));

    com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache tokenCache =
        mock(com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache.class);
    when(this.searcher.getTokenFacetsStateCache()).thenReturn(Optional.of(tokenCache));

    com.xgen.mongot.index.lucene.facet.TokenSsdvFacetState tokenState =
        mock(com.xgen.mongot.index.lucene.facet.TokenSsdvFacetState.class);
    when(tokenCache.get(anyString())).thenReturn(Optional.of(tokenState));

    String teamFacetName = "teamFacet";
    String leagueFacetName = "leagueFacet";
    FieldPath returnScopePath = FieldPath.parse("custom.scope");

    FacetDefinition.StringFacetDefinition teamDef =
        new FacetDefinition.StringFacetDefinition("team", 10);
    FacetDefinition.StringFacetDefinition leagueDef =
        new FacetDefinition.StringFacetDefinition("league", 10);

    this.facetNameToDefinition.clear();
    this.facetNameToDefinition.put(teamFacetName, teamDef);
    this.facetNameToDefinition.put(leagueFacetName, leagueDef);

    this.facetCollector =
        FacetCollectorBuilder.facet()
            .operator(OperatorBuilder.exists().path("_id").build())
            .facetDefinitions(this.facetNameToDefinition)
            .build();

    this.collectorQuery =
        CollectorQueryBuilder.builder()
            .collector(this.facetCollector)
            .index(SearchIndex.MOCK_INDEX_NAME)
            .returnScope(new ReturnScope(returnScopePath))
            .returnStoredSource(false)
            .build();

    Optional<FieldPath> returnScope = Optional.of(returnScopePath);
    String lucenePath =
        FieldName.TypeField.TOKEN.getLuceneFieldName(FieldPath.parse("league"), returnScope);

    DrillSidewaysResult dsResult = createOptimizedDrillSidewaysResult(List.of(lucenePath));

    Map<String, DrillSidewaysResult> facetToResult = new HashMap<>();
    facetToResult.put(leagueFacetName, dsResult);

    LuceneFacetCollectorMetaBatchProducer producer =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetName -> Optional.ofNullable(facetToResult.get(facetName)),
            Optional.empty());

    producer.execute(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
    BsonArray batch = producer.getNextBatch(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    boolean sawLeagueFacetBucket =
        batch.getValues().stream()
            .filter(v -> v.isDocument())
            .map(v -> v.asDocument())
            .anyMatch(
                doc ->
                    doc.containsKey("type")
                        && "facet".equals(doc.getString("type").getValue())
                        && doc.containsKey("tag")
                        && leagueFacetName.equals(doc.getString("tag").getValue()));

    assertTrue(
        "Expected leagueFacet buckets present even if teamFacet DrillSidewaysResult missing",
        sawLeagueFacetBucket);
  }

  @Test
  public void getNextBatch_singleProducer_returnsSingleBatch() throws Exception {
    this.bucketProducers.add(new MockProducer(NUM_BUCKETS));
    this.batchProducer =
        new LuceneFacetCollectorMetaBatchProducer(
            NUM_BUCKETS + NUM_COUNT_DOCS, this.bucketProducers, this.facetCollector);

    assertBatchesProduced(
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, NUM_BUCKETS + NUM_COUNT_DOCS, 1);
  }

  @Test
  public void getNextBatch_multipleProducers_returnsSingleBatch() throws Exception {
    this.bucketProducers.add(new MockProducer(NUM_BUCKETS));
    this.bucketProducers.add(new MockProducer(NUM_BUCKETS));

    this.batchProducer =
        new LuceneFacetCollectorMetaBatchProducer(
            2 * NUM_BUCKETS + NUM_COUNT_DOCS, this.bucketProducers, this.facetCollector);

    assertBatchesProduced(
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, 2 * NUM_BUCKETS + NUM_COUNT_DOCS, 1);
  }

  @Test
  public void getNextBatch_singleProducer_smallSizeLimit_returnsMultipleBatches() throws Exception {
    this.bucketProducers.add(new MockProducer(NUM_BUCKETS));

    this.batchProducer =
        new LuceneFacetCollectorMetaBatchProducer(
            NUM_BUCKETS + NUM_COUNT_DOCS, this.bucketProducers, this.facetCollector);

    var bsonArray = new BsonArray();
    bsonArray.add(COUNT_BUCKET_DOC);
    bsonArray.add(getBucket().toBson());
    Bytes sizeLimitForOneBucket = BsonUtils.bsonValueSerializedBytes(bsonArray);

    assertBatchesProduced(sizeLimitForOneBucket, NUM_BUCKETS + NUM_COUNT_DOCS, 2);
  }

  @Test
  public void getNextBatch_multipleProducers_smallSizeLimit_returnsMultipleBatches()
      throws Exception {
    this.bucketProducers.add(new MockProducer(NUM_BUCKETS));
    this.bucketProducers.add(new MockProducer(NUM_BUCKETS));

    this.batchProducer =
        new LuceneFacetCollectorMetaBatchProducer(
            2 * NUM_BUCKETS + NUM_COUNT_DOCS, this.bucketProducers, this.facetCollector);

    var bsonArray = new BsonArray();
    bsonArray.add(COUNT_BUCKET_DOC);
    bsonArray.add(getBucket().toBson());
    bsonArray.add(getBucket().toBson());
    Bytes sizeLimitForTwoBuckets = BsonUtils.bsonValueSerializedBytes(bsonArray);

    assertBatchesProduced(sizeLimitForTwoBuckets, 2 * NUM_BUCKETS + NUM_COUNT_DOCS, 2);
  }

  private Facets createFacets(List<String> facetPaths, List<List<String>> facetValues)
      throws IOException {
    for (int i = 0; i < facetPaths.size(); i++) {
      String path = facetPaths.get(i);
      for (String value : facetValues.get(i)) {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesFacetField(path, value));
        this.indexWriter.addDocument(this.facetsConfig.build(doc));
      }
    }
    this.indexWriter.commit();

    DirectoryReader reader = DirectoryReader.open(this.directory);
    SortedSetDocValuesReaderState state =
        new DefaultSortedSetDocValuesReaderState(reader, this.facetsConfig);
    return new SortedSetDocValuesFacetCounts(state);
  }

  private Map<String, DrillSidewaysResult> createDrillSidewaysResults(
      List<String> facetNames, List<String> facetPaths) throws IOException {
    List<List<String>> facetValues = new ArrayList<>();
    for (int i = 0; i < facetPaths.size(); i++) {
      List<String> values = new ArrayList<>();
      values.addAll(Collections.nCopies(10, "bucket1"));
      values.addAll(Collections.nCopies(20, "bucket2"));
      facetValues.add(values);
    }

    Facets facets = createFacets(facetPaths, facetValues);
    DrillSidewaysResult result = new DrillSidewaysResult(facets, null, null, null, null);

    Map<String, DrillSidewaysResult> drillSidewaysResults = new HashMap<>();
    for (String facetName : facetNames) {
      drillSidewaysResults.put(facetName, result);
    }
    return drillSidewaysResults;
  }

  private DrillSidewaysResult createOptimizedDrillSidewaysResult(List<String> facetPaths)
      throws IOException {
    List<List<String>> facetValues = new ArrayList<>();
    for (int i = 0; i < facetPaths.size(); i++) {
      List<String> values = new ArrayList<>();
      values.addAll(Collections.nCopies(10, "bucket1"));
      values.addAll(Collections.nCopies(20, "bucket2"));
      facetValues.add(values);
    }

    Facets facets = createFacets(facetPaths, facetValues);
    return new DrillSidewaysResult(facets, null, null, null, null);
  }

  private void assertBatchesProduced(
      Bytes resultsSizeLimit, int expectedCount, int expectedNumBatches) throws Exception {
    @Var boolean countChecked = false;
    @Var int docCount = 0;
    @Var int batchCount = 0;
    while (!this.batchProducer.isExhausted()) {
      this.batchProducer.execute(resultsSizeLimit, BatchCursorOptionsBuilder.empty());
      BsonArray batch = this.batchProducer.getNextBatch(resultsSizeLimit);
      if (!countChecked) {
        assertEquals(new BsonString("count"), batch.getValues().get(0).asDocument().get("type"));
        assertEquals(
            new BsonInt64(expectedCount), batch.getValues().get(0).asDocument().get("count"));
        countChecked = true;
      }

      batchCount++;
      docCount += batch.getValues().size();
    }

    assertEquals(expectedCount, docCount);
    assertEquals(expectedNumBatches, batchCount);
  }

  private static IntermediateFacetBucket getBucket() {
    return new IntermediateFacetBucket(
        IntermediateFacetBucket.Type.FACET, "tag", new BsonInt64(1), 100);
  }

  private static class MockProducer implements LuceneMetaBucketProducer {
    private final int numBuckets;

    private int position;

    public MockProducer(int numBuckets) {
      this.numBuckets = numBuckets;

      this.position = 0;
    }

    @Override
    public IntermediateFacetBucket peek() {
      checkState(this.position < this.numBuckets, "Producer is exhausted.");
      return getBucket();
    }

    @Override
    public void acceptAndAdvance() {
      checkState(this.position < this.numBuckets, "Producer is exhausted.");

      this.position++;
    }

    @Override
    public boolean isExhausted() {
      return this.position >= this.numBuckets;
    }
  }
}
