package com.xgen.mongot.index.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways.ConcurrentDrillSidewaysResult;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LuceneFacetOptimizedDrillSidewaysSearchManagerTest {

  private static final String FACET_1 = "categoryFacet";

  @Mock private MongotDrillSideways mockOptimizedDrillSideways;
  @Mock private ConcurrentDrillSidewaysResult mockResult;

  private LuceneIndexSearcherReference searcherReference;
  private IndexWriter indexWriter;
  private Directory directory;
  private Query testQuery;

  private Map<String, LuceneDrillSideways> facetQueries;
  private LuceneDrillSideways mockLuceneDrillSideways;

  @Before
  public void setUp() throws Exception {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config =
        new IndexWriterConfig(
            LuceneAnalyzer.indexAnalyzer(
                SearchIndex.MOCK_INDEX_DEFINITION, AnalyzerRegistryBuilder.empty()));
    config.setCodec(new LuceneCodec());
    this.indexWriter = new IndexWriter(this.directory, config);

    addTestDocuments(true);

    LuceneSearcherManager searcherManager =
        LuceneSearcherManager.create(
            this.indexWriter,
            new LuceneSearcherFactory(
                SearchIndex.MOCK_INDEX_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
            SearchIndex.mockMetricsFactory(),
            () -> false);

    this.searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH),
            FeatureFlags.getDefault());

    this.testQuery = new MatchAllDocsQuery();
    this.facetQueries = new HashMap<>();

    DrillDownQuery mockDrillDownQuery = new DrillDownQuery(null, this.testQuery);
    this.mockLuceneDrillSideways =
        new LuceneDrillSideways(this.mockOptimizedDrillSideways, mockDrillDownQuery);

    ScoreDoc[] hits = new ScoreDoc[] {new ScoreDoc(0, 1.0f), new ScoreDoc(1, 0.9f)};

    // Create TopDocs with the TotalHits and ScoreDoc[]
    TotalHits totalHits = new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO);
    TopDocs mockedTopDocs = new TopDocs(totalHits, hits);

    java.lang.reflect.Field collectorField =
        ConcurrentDrillSidewaysResult.class.getDeclaredField("collectorResult");
    collectorField.setAccessible(true);
    collectorField.set(this.mockResult, mockedTopDocs);

    when(this.mockOptimizedDrillSideways.searchSafe(
            any(DrillDownQuery.class), any(CollectorManager.class)))
        .thenReturn(this.mockResult);
  }

  @After
  public void tearDown() throws IOException {
    this.indexWriter.close();
    this.directory.close();
    this.searcherReference.close();
  }

  @Test
  public void testInitialSearch() throws IOException, InvalidQueryException {
    // Arrange
    this.facetQueries.put(FACET_1, this.mockLuceneDrillSideways);

    LuceneFacetOptimizedDrillSidewaysSearchManager searchManager =
        new LuceneFacetOptimizedDrillSidewaysSearchManager(
            this.testQuery, this.facetQueries.get(FACET_1), Optional.empty(), Optional.empty());

    // Act
    LuceneFacetOptimizedDrillSidewaysSearchManager.OptimizedDrillSidewaysFacetCollectorQueryInfo
        result = searchManager.initialSearch(this.searcherReference, 10);

    // Assert
    assertNotNull(result);
    assertNotNull(result.topDocs);
    assertEquals(2, result.topDocs.scoreDocs.length);
    assertNotNull(result.drillSidewaysResult);
    assertTrue(result.luceneExhausted);
  }

  @Test
  public void testInitialSearch_WithEmptyFacets_Optimized()
      throws IOException, InvalidQueryException {
    // Arrange
    LuceneFacetOptimizedDrillSidewaysSearchManager searchManager =
        new LuceneFacetOptimizedDrillSidewaysSearchManager(
            this.testQuery, this.mockLuceneDrillSideways, Optional.empty(), Optional.empty());

    // Act
    LuceneFacetOptimizedDrillSidewaysSearchManager.OptimizedDrillSidewaysFacetCollectorQueryInfo
        result = searchManager.initialSearch(this.searcherReference, 10);

    // Assert
    assertNotNull(result);
    assertNotNull(result.topDocs);
    assertEquals(2, result.topDocs.scoreDocs.length);
    assertNotNull(result.drillSidewaysResult);
    assertTrue(result.luceneExhausted);
  }

  @Test
  public void testInitialSearch_WithLargeBatchSize() throws IOException, InvalidQueryException {
    // Arrange
    this.facetQueries.put(FACET_1, this.mockLuceneDrillSideways);

    LuceneFacetOptimizedDrillSidewaysSearchManager searchManager =
        new LuceneFacetOptimizedDrillSidewaysSearchManager(
            this.testQuery, this.facetQueries.get(FACET_1), Optional.empty(), Optional.empty());

    // Act
    LuceneFacetOptimizedDrillSidewaysSearchManager.OptimizedDrillSidewaysFacetCollectorQueryInfo
        result = searchManager.initialSearch(this.searcherReference, 1000);

    // Assert
    assertNotNull(result);
    assertNotNull(result.topDocs);
    assertEquals(2, result.topDocs.scoreDocs.length);
    assertNotNull(result.drillSidewaysResult);
    assertTrue(result.luceneExhausted);
  }

  @Test
  public void testInitialSearch_WithSmallBatchSize() throws IOException, InvalidQueryException {
    // Arrange
    this.facetQueries.put(FACET_1, this.mockLuceneDrillSideways);

    LuceneFacetOptimizedDrillSidewaysSearchManager searchManager =
        new LuceneFacetOptimizedDrillSidewaysSearchManager(
            this.testQuery, this.facetQueries.get(FACET_1), Optional.empty(), Optional.empty());

    // Act
    LuceneFacetOptimizedDrillSidewaysSearchManager.OptimizedDrillSidewaysFacetCollectorQueryInfo
        result = searchManager.initialSearch(this.searcherReference, 2);

    // Assert
    assertNotNull(result);
    assertNotNull(result.topDocs);
    assertEquals(2, result.topDocs.scoreDocs.length);
    assertNotNull(result.drillSidewaysResult);
    assertFalse(result.luceneExhausted); // Should not be exhausted with batch size < total docs
  }

  @Test
  public void testInitialSearch_WithSort() throws IOException, InvalidQueryException {
    // Arrange
    Sort sort = new Sort(new SortField("category", SortField.Type.STRING));

    this.facetQueries.put(FACET_1, this.mockLuceneDrillSideways);

    LuceneFacetOptimizedDrillSidewaysSearchManager searchManager =
        new LuceneFacetOptimizedDrillSidewaysSearchManager(
            this.testQuery, this.facetQueries.get(FACET_1), Optional.of(sort), Optional.empty());

    // Act
    LuceneFacetOptimizedDrillSidewaysSearchManager.OptimizedDrillSidewaysFacetCollectorQueryInfo
        result = searchManager.initialSearch(this.searcherReference, 10);

    // Assert
    assertNotNull(result);
    assertNotNull(result.topDocs);
    assertEquals(2, result.topDocs.scoreDocs.length);
    assertNotNull(result.drillSidewaysResult);
  }

  private void addTestDocuments(boolean createSorted) throws IOException {
    Document doc1 = new Document();
    doc1.add(new StringField("id", "1", Field.Store.YES)); // Add identifier field
    doc1.add(newCategoryField("electronics", createSorted)); // Add category field
    this.indexWriter.addDocument(doc1);

    Document doc2 = new Document();
    doc2.add(new StringField("id", "2", Field.Store.YES));
    doc2.add(newCategoryField("books", createSorted)); // Add another category field
    this.indexWriter.addDocument(doc2);

    Document doc3 = new Document();
    doc3.add(new StringField("id", "3", Field.Store.YES));
    doc3.add(newCategoryField("electronics", createSorted)); // Same category as doc1
    this.indexWriter.addDocument(doc3);

    this.indexWriter.commit(); // Commit changes to simulate an indexed dataset
  }

  private Field newCategoryField(String categoryValue, boolean createSorted) {
    return createSorted
        ? new SortedDocValuesField("category", new BytesRef(categoryValue)) // Facet compatibility
        : new StringField("category", categoryValue, Field.Store.NO); // Regular string field
  }

  @Test
  public void testGetMoreTopDocs() throws IOException {
    // Arrange
    this.facetQueries.put(FACET_1, this.mockLuceneDrillSideways);

    LuceneFacetOptimizedDrillSidewaysSearchManager searchManager =
        new LuceneFacetOptimizedDrillSidewaysSearchManager(
            this.testQuery, this.facetQueries.get(FACET_1), Optional.empty(), Optional.empty());

    ScoreDoc lastScoreDoc = new ScoreDoc(0, 1.0f);

    // Act
    TopDocs result = searchManager.getMoreTopDocs(this.searcherReference, lastScoreDoc, 5);

    // Assert
    assertNotNull(result);
    assertNotNull(result.scoreDocs);
  }
}
