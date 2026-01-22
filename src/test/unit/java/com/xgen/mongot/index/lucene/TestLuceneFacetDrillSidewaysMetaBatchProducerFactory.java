package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.index.IntermediateFacetBucket;
import com.xgen.mongot.index.definition.StringFacetFieldDefinition;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.cursor.batch.BatchCursorOptionsBuilder;
import com.xgen.testing.mongot.index.query.CollectorQueryBuilder;
import com.xgen.testing.mongot.index.query.collectors.FacetCollectorBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestLuceneFacetDrillSidewaysMetaBatchProducerFactory {

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
  @Mock private Facets facets;

  private Queue<LuceneMetaBucketProducer> bucketProducers;
  private LuceneFacetCollectorMetaBatchProducer batchProducer;
  private Map<String, FacetDefinition> facetNameToDefinition;
  private CollectorQuery collectorQuery;
  private FacetCollector facetCollector;

  @Before
  public void setUp() throws Exception {
    this.bucketProducers = new LinkedList<>();

    when(this.searcherReference.getIndexSearcher()).thenReturn(this.searcher);
    when(this.searcher.getFacetsState()).thenReturn(Optional.of(this.facetState));
    when(this.facetContext.getBoundaryFacetPath(any(), any())).thenReturn("mock_facet_path");
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

  @Test(expected = IllegalStateException.class)
  public void testCreate_WithNonExactCount_ThrowsException() throws Exception {
    // Arrange
    Map<String, DrillSidewaysResult> facetToDrillSidewaysResult = Map.of();

    LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
        this.collectorQuery,
        this.facetContext,
        this.searcherReference,
        TOP_DOCS_WITH_NON_EXACT_COUNT,
        facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)));
  }

  @Test
  public void testCreate_WithEmptyFacetDefinitions() throws Exception {
    // Arrange
    Map<String, DrillSidewaysResult> facetToDrillSidewaysResult = Map.of();

    // Act
    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)));

    // Assert
    assertNotNull(result);
    assertTrue(result.isExhausted()); // Should be exhausted with no facets
  }

  @Test
  public void testCreate_WithGenericDrillSidewaysResult_NumberFacets() throws Exception {
    // Arrange
    this.facetNameToDefinition.put(NUMERIC_FACET_NAME, NUMERIC_FACET_DEFINITION);
    Map<String, DrillSidewaysResult> facetToDrillSidewaysResult =
        createMockGenericDrillSidewaysResults(
            List.of(NUMERIC_FACET_NAME), List.of(NUMERIC_FACET_PATH));

    // Act
    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)));

    // Assert
    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void testCreate_WithGenericDrillSidewaysResult_DateFacets() throws Exception {
    // Arrange
    this.facetNameToDefinition.put(DATE_FACET_NAME, DATE_FACET_DEFINITION);
    Map<String, DrillSidewaysResult> facetToDrillSidewaysResult =
        createMockGenericDrillSidewaysResults(List.of(DATE_FACET_NAME), List.of(DATE_FACET_PATH));

    // Act
    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)));

    // Assert
    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void testCreate_WithGenericDrillSidewaysResult_StringFacets() throws Exception {
    // Arrange
    this.facetNameToDefinition.put(STRING_FACET_NAME, STRING_FACET_DEFINITION);
    Map<String, DrillSidewaysResult> facetToDrillSidewaysResult =
        createMockGenericDrillSidewaysResults(
            List.of(STRING_FACET_NAME), List.of(STRING_FACET_PATH));

    // Act
    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)));

    // Assert
    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void testCreate_WithGenericDrillSidewaysResultMixedFacetTypes() throws Exception {
    // Arrange
    this.facetNameToDefinition.put(STRING_FACET_NAME, STRING_FACET_DEFINITION);
    this.facetNameToDefinition.put(NUMERIC_FACET_NAME, NUMERIC_FACET_DEFINITION);
    this.facetNameToDefinition.put(DATE_FACET_NAME, DATE_FACET_DEFINITION);
    Map<String, DrillSidewaysResult> facetToDrillSidewaysResult =
        createMockGenericDrillSidewaysResults(
            List.of(STRING_FACET_NAME, DATE_FACET_NAME, NUMERIC_FACET_NAME),
            List.of(STRING_FACET_PATH, DATE_FACET_PATH, NUMERIC_FACET_PATH));

    // Act
    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)));

    // Assert
    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void testCreate_WithOptimizedDrillSidewaysResult_NumberFacets() throws Exception {
    // Arrange
    this.facetNameToDefinition.put(NUMERIC_FACET_NAME, NUMERIC_FACET_DEFINITION);

    // Creating a single DrillSidewaysResult
    DrillSidewaysResult singleSharedResult =
        createMockOptimizedDrillSidewaysResult(NUMERIC_FACET_PATH);

    // Act
    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetKey -> Optional.of(singleSharedResult));

    // Assert
    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void testCreate_WithOptimizedDrillSidewaysResult_DateFacets() throws Exception {
    // Arrange
    this.facetNameToDefinition.put(DATE_FACET_NAME, DATE_FACET_DEFINITION);

    // Creating a single optimized DrillSidewaysResult
    DrillSidewaysResult singleSharedResult =
        createMockOptimizedDrillSidewaysResult(DATE_FACET_PATH);

    // Act
    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetKey -> Optional.of(singleSharedResult));

    // Assert
    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void testCreate_WithOptimizedDrillSidewaysResult_StringFacets() throws Exception {
    // Arrange
    this.facetNameToDefinition.put(STRING_FACET_NAME, STRING_FACET_DEFINITION);

    // Creating a single optimized DrillSidewaysResult
    DrillSidewaysResult singleSharedResult =
        createMockOptimizedDrillSidewaysResult(STRING_FACET_PATH);

    // Act
    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetKey -> Optional.of(singleSharedResult));

    // Assert
    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void testCreate_WithOptimizedDrillSidewaysResult_MixedFacetTypes() throws Exception {
    // Arrange
    this.facetNameToDefinition.put(STRING_FACET_NAME, STRING_FACET_DEFINITION);
    this.facetNameToDefinition.put(NUMERIC_FACET_NAME, NUMERIC_FACET_DEFINITION);
    this.facetNameToDefinition.put(DATE_FACET_NAME, DATE_FACET_DEFINITION);

    // Creating a single optimized DrillSidewaysResult
    DrillSidewaysResult singleSharedResult = createMockOptimizedDrillSidewaysResult("mock_path");

    // Act
    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetKey -> Optional.of(singleSharedResult));

    // Assert
    assertNotNull(result);
    assertEquals(100L, result.getTotalHits());
    assertFalse(result.isExhausted());
  }

  @Test
  public void testCreate_WithNoFacets_OptimizedDrillSidewaysResult() throws Exception {
    // Arrange
    DrillSidewaysResult singleSharedResult = createMockOptimizedDrillSidewaysResult("mock_path");

    // Act
    LuceneFacetCollectorMetaBatchProducer result =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetKey -> Optional.of(singleSharedResult));

    // Assert
    assertNotNull(result);
    assertTrue(result.isExhausted()); // Should be exhausted since there are no facets
  }

  @Test
  public void testCreate_TokenFacets_MissingFirstDrillSidewaysResult_DoesNotSkipSecondFacet()
      throws Exception {

    // Route facetable string definitions to TOKEN path (no mocking sealed types)
    when(this.facetContext.getStringFacetFieldDefinition(any(), any()))
        .thenReturn(new com.xgen.mongot.index.definition.TokenFieldDefinition(Optional.empty()));

    // Ensure token facets state cache is present so token producer path runs
    com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache tokenCache =
        mock(com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache.class);
    when(this.searcher.getTokenFacetsStateCache()).thenReturn(Optional.of(tokenCache));

    com.xgen.mongot.index.lucene.facet.TokenSsdvFacetState tokenState =
        mock(com.xgen.mongot.index.lucene.facet.TokenSsdvFacetState.class);
    when(tokenCache.get(anyString())).thenReturn(Optional.of(tokenState));

    // Two token-backed string facets
    String teamFacetName = "teamFacet";
    String leagueFacetName = "leagueFacet";

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
            .returnScope(new ReturnScope(FieldPath.parse("custom.scope")))
            .returnStoredSource(false)
            .build();

    // DrillSidewaysResult present ONLY for leagueFacet
    FacetResult facetResult = newFacetResult("ignored_path");
    when(this.facets.getAllChildren(anyString())).thenReturn(facetResult);

    DrillSidewaysResult dsResult = new DrillSidewaysResult(this.facets, null, null, null, null);

    Map<String, DrillSidewaysResult> facetToResult = new HashMap<>();
    facetToResult.put(leagueFacetName, dsResult); // teamFacet intentionally missing

    // Act: build meta batch producer
    LuceneFacetCollectorMetaBatchProducer producer =
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            this.collectorQuery,
            this.facetContext,
            this.searcherReference,
            TOP_DOCS,
            facetName -> Optional.ofNullable(facetToResult.get(facetName)));

    // Pull one batch (large enough)
    producer.execute(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
    BsonArray batch = producer.getNextBatch(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    // Assert: leagueFacet bucket docs must appear.
    // With old logic (return producers), we'd bail on teamFacet and never add leagueFacet
    // producers.
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
  public void testGetNextBatch_SingleProducer() throws Exception {

    this.bucketProducers.add(new MockProducer(NUM_BUCKETS));
    this.batchProducer =
        new LuceneFacetCollectorMetaBatchProducer(
            NUM_BUCKETS + NUM_COUNT_DOCS, this.bucketProducers, this.facetCollector);

    assertBatchesProduced(
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, NUM_BUCKETS + NUM_COUNT_DOCS, 1);
  }

  @Test
  public void testGetNextBatch_MultipleProducers() throws Exception {

    this.bucketProducers.add(new MockProducer(NUM_BUCKETS));
    this.bucketProducers.add(new MockProducer(NUM_BUCKETS));

    this.batchProducer =
        new LuceneFacetCollectorMetaBatchProducer(
            2 * NUM_BUCKETS + NUM_COUNT_DOCS, this.bucketProducers, this.facetCollector);

    assertBatchesProduced(
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, 2 * NUM_BUCKETS + NUM_COUNT_DOCS, 1);
  }

  @Test
  public void testGetNextBatch_SingleProducerMultipleBatches() throws Exception {

    this.bucketProducers.add(new MockProducer(NUM_BUCKETS));

    this.batchProducer =
        new LuceneFacetCollectorMetaBatchProducer(
            NUM_BUCKETS + NUM_COUNT_DOCS, this.bucketProducers, this.facetCollector);

    var bsonArray = new BsonArray();
    bsonArray.add(COUNT_BUCKET_DOC);
    bsonArray.add(getBucket().toBson());
    Bytes sizeLimitForOneBucket = BsonUtils.bsonValueSerializedBytes(bsonArray);

    // The sizeLimit can contain 1 bucket, but there are 2 buckets in total.
    assertBatchesProduced(sizeLimitForOneBucket, NUM_BUCKETS + NUM_COUNT_DOCS, 2);
  }

  @Test
  public void testGetNextBatch_MultipleProducersMultipleBatches() throws Exception {

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

    // The sizeLimit can contain 2 buckets, but there are 4 buckets in total.
    assertBatchesProduced(sizeLimitForTwoBuckets, 2 * NUM_BUCKETS + NUM_COUNT_DOCS, 2);
  }

  private Map<String, DrillSidewaysResult> createMockGenericDrillSidewaysResults(
      List<String> facetNames, List<String> facetPaths) throws IOException {
    DrillSidewaysResult result = new DrillSidewaysResult(this.facets, null, null, null, null);
    Map<String, DrillSidewaysResult> mockDrillSidewaysResults = new HashMap<>();

    for (int i = 0; i < facetPaths.size(); i++) {
      FacetResult facetResult = newFacetResult(facetPaths.get(i));
      when(this.facets.getTopChildren(anyInt(), anyString())).thenReturn(facetResult);
      when(this.facets.getAllChildren(anyString())).thenReturn(facetResult);

      mockDrillSidewaysResults.put(facetNames.get(i), result);
    }
    return mockDrillSidewaysResults;
  }

  private DrillSidewaysResult createMockOptimizedDrillSidewaysResult(String facetPath)
      throws IOException {
    DrillSidewaysResult result = new DrillSidewaysResult(this.facets, null, null, null, null);

    // Mock behavior for the single optimized DrillSidewaysResult
    FacetResult facetResult = newFacetResult(facetPath);
    when(this.facets.getTopChildren(anyInt(), anyString())).thenReturn(facetResult);
    when(this.facets.getAllChildren(anyString())).thenReturn(facetResult);

    return result;
  }

  private FacetResult newFacetResult(String facetPath) {
    LabelAndValue[] labelAndValues = {
        new LabelAndValue("bucket1", 10), new LabelAndValue("bucket2", 20)
    };
    return new FacetResult(facetPath, new String[0], 30, labelAndValues, 2);
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
