package com.xgen.mongot.index.lucene;

import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.BatchSizeStrategySelector;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.MeteredIndexWriter;
import com.xgen.mongot.index.SearchIndexReader.SearchProducerAndMetaResults;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryFactory;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedVectorRescorer;
import com.xgen.mongot.index.lucene.query.LuceneSearchQueryFactoryDistributor;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.DrillSidewaysInfo;
import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.DrillSidewaysInfo.QueryOptimizationStatus;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.testing.ConcurrencyTestUtils;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import com.xgen.testing.mongot.index.lucene.synonym.SynonymRegistryBuilder;
import com.xgen.testing.mongot.index.query.OperatorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import com.xgen.testing.mongot.mock.index.IndexGeneration;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@RunWith(Theories.class)
public class LuceneSearchIndexReaderTest {

  private LuceneSearchIndexReader readerWithDrillSidewaysEnabled;

  private static final OperatorQuery QUERY_DEFINITION =
      OperatorQueryBuilder.builder()
          .operator(
              OperatorBuilder.exists()
                  .path("foo")
                  .score(ScoreBuilder.constant().value(1).build())
                  .build())
          .index(MOCK_INDEX_NAME)
          .returnStoredSource(false)
          .build();

  private static final OperatorQuery INVALID_QUERY_DEFINITION =
      OperatorQueryBuilder.builder()
          .operator(
              OperatorBuilder.wildcard().path("foo").query("q*").allowAnalyzedField(false).build())
          .index(MOCK_INDEX_NAME)
          .returnStoredSource(false)
          .build();
  private LuceneSearcherFactory searcherFactory;

  @DataPoints("allQueryTypes")
  public static Set<QueryType> intermediateOrRegularQuery() {
    return Set.of(QueryType.INTERMEDIATE, QueryType.NON_INTERMEDIATE);
  }

  private enum QueryType {
    INTERMEDIATE,
    NON_INTERMEDIATE,
  }

  private LuceneSearchIndexReader reader;
  private IndexWriter writer;
  private LuceneSearchQueryFactoryDistributor queryFactory;
  private NamedExecutorService concurrentSearchExecutor;
  private NamedExecutorService concurrentRescoringExecutor;

  /** Set up resources for test. */
  @Before
  public void setUp() throws Exception {
    var folder = TestUtils.getTempFolder();
    var path = folder.getRoot().toPath();
    var indexPath = path.resolve("indexGeneration");
    var metadataPath = path.resolve("indexMapping/indexGeneration");
    var luceneConfig = LuceneConfigBuilder.builder().dataPath(indexPath).build();
    var directoryFactory =
        new IndexDirectoryFactory(indexPath, metadataPath, luceneConfig, 1, Optional.empty());
    var featureFlags =
        FeatureFlags.withDefaults().enable(Feature.ACCURATE_NUM_EMBEDDED_ROOT_DOCS_METRIC).build();
    var metricsFactory = SearchIndex.mockMetricsFactory();
    IndexMetricsUpdater indexMetricsUpdater =
        IndexMetricsUpdaterBuilder.builder()
            .metricsFactory(metricsFactory)
            .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
            .build();

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    this.concurrentSearchExecutor =
        spy(Executors.fixedSizeThreadScheduledExecutor("test", 1, meterRegistry));
    this.concurrentRescoringExecutor =
        spy(Executors.fixedSizeThreadScheduledExecutor("test", 1, new SimpleMeterRegistry()));

    LuceneSearchIndex index =
        LuceneSearchIndex.createDiskBacked(
            indexPath,
            metadataPath,
            LuceneConfigBuilder.builder().dataPath(indexPath).build(),
            featureFlags,
            new InstrumentedConcurrentMergeScheduler(meterRegistry),
            new TieredMergePolicy(),
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            mock(NamedScheduledExecutorService.class),
            Optional.of(this.concurrentSearchExecutor),
            Optional.of(this.concurrentRescoringExecutor),
            MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
            AnalyzerRegistryBuilder.empty(),
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            metricsFactory,
            Runnable::run);
    this.queryFactory =
        spy(
            LuceneSearchQueryFactoryDistributor.create(
                MOCK_INDEX_DEFINITION,
                MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
                AnalyzerRegistryBuilder.empty(),
                SynonymRegistryBuilder.empty(),
                indexMetricsUpdater.getQueryingMetricsUpdater(),
                false,
                featureFlags));
    InitializedLuceneSearchIndex initializedIndex =
        InitializedLuceneSearchIndex.create(
            index,
            MOCK_INDEX_DEFINITION_GENERATION.getGenerationId(),
            directoryFactory,
            mock(IndexDirectoryHelper.class),
            Optional.empty(),
            featureFlags,
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty()),
            () -> false);
    this.writer =
        ((SingleLuceneIndexWriter) ((MeteredIndexWriter) initializedIndex.getWriter()).getWrapped())
            .getLuceneWriter();

    var fieldDefinitionResolver =
        MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
            MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion);

    this.searcherFactory =
        spy(
            new LuceneSearcherFactory(
                MOCK_INDEX_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)));
    LuceneSearcherManager searcherManager =
        LuceneSearcherManager.create(
            this.writer, this.searcherFactory, SearchIndex.mockMetricsFactory());
    Analyzer analyzer =
        LuceneAnalyzer.indexAnalyzer(MOCK_INDEX_DEFINITION, AnalyzerRegistryBuilder.empty());
    LuceneHighlighterContext highlighterContext =
        new LuceneHighlighterContext(fieldDefinitionResolver, analyzer);
    LuceneFacetContext facetContext =
        new LuceneFacetContext(
            fieldDefinitionResolver,
            index
                .getDefinition()
                .getIndexCapabilities(index.getSearchIndexProperties().indexFormatVersion));
    this.reader =
        LuceneSearchIndexReader.create(
            this.queryFactory,
            searcherManager,
            MOCK_INDEX_DEFINITION,
            highlighterContext,
            facetContext,
            indexMetricsUpdater.getQueryingMetricsUpdater(),
            new LuceneSearchManagerFactory(
                fieldDefinitionResolver,
                new BinaryQuantizedVectorRescorer(Optional.of(this.concurrentRescoringExecutor)),
                indexMetricsUpdater.getQueryingMetricsUpdater()),
            Optional.of(this.concurrentSearchExecutor),
            0,
            featureFlags,
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty()));
    this.readerWithDrillSidewaysEnabled =
        LuceneSearchIndexReader.create(
            this.queryFactory,
            searcherManager,
            MOCK_INDEX_DEFINITION,
            highlighterContext,
            facetContext,
            indexMetricsUpdater.getQueryingMetricsUpdater(),
            new LuceneSearchManagerFactory(
                fieldDefinitionResolver,
                new BinaryQuantizedVectorRescorer(Optional.of(this.concurrentRescoringExecutor)),
                indexMetricsUpdater.getQueryingMetricsUpdater()),
            Optional.of(this.concurrentSearchExecutor),
            0,
            featureFlags,
            drillSidewaysEnabledRegistry());
  }

  private static DynamicFeatureFlagRegistry drillSidewaysEnabledRegistry() {
    DynamicFeatureFlagRegistry registry = mock(DynamicFeatureFlagRegistry.class);

    // Everything else: return default
    Mockito.when(registry.evaluateClusterInvariant(any(), anyBoolean()))
        .thenAnswer(invocation -> invocation.getArgument(1));

    // Always enable drill-sideways, regardless of default
    Mockito.when(registry.evaluateClusterInvariant(
            eq(DynamicFeatureFlags.DRILL_SIDEWAYS_FACETING.getName()),
            anyBoolean()))
        .thenReturn(true);

    return registry;
  }

  @Theory
  public void testCanQueryMultipleTimesSimultaneously(
      @FromDataPoints("allQueryTypes") QueryType queryType) throws Exception {
    // Use an invocation of LuceneQueryFactory::createQuery as a signal that we're within the
    // critical section in query().
    ConcurrencyTestUtils.assertCanBeInvokedConcurrently(
        this.queryFactory,
        factory -> factory.createQuery(any(), any(), any(), any()),
        () -> executeQuery(queryType));
  }

  @Theory
  public void testWhenQueryCallerClosesProducerReaderReferencesAreReleased(
      @FromDataPoints("allQueryTypes") QueryType queryType) throws Exception {
    // this acquires a references to a lucene reader under the hood
    var reader = getInternalReader();
    // one reference for management
    assertEquals(1, reader.getRefCount());

    // new queries increase the ref count
    BatchProducer producer1 = executeQuery(queryType);
    assertEquals(2, reader.getRefCount());
    BatchProducer producer2 = executeQuery(queryType);
    assertEquals(3, reader.getRefCount());

    // If our well-behaved client closes the producer, we should go back to 1 reference
    producer2.close();
    producer1.close();
    assertEquals(1, reader.getRefCount());
  }

  @Theory
  public void testSemanticallyInvalidQueryReaderReferencesAreReleased(
      @FromDataPoints("allQueryTypes") QueryType queryType) throws Exception {
    IndexReader reader = getInternalReader();
    assertEquals(1, reader.getRefCount());

    for (int i = 0; i < 3; i++) {
      executeInvalidQuery(queryType);
    }
    // one reference for management purposes
    assertEquals(1, reader.getRefCount());
  }

  @Theory
  public void testCannotCloseWhileQuery(@FromDataPoints("allQueryTypes") QueryType queryType)
      throws Exception {
    // Use an invocation of LuceneQueryFactory::createQuery as a signal that we're within the
    // critical section in query().
    ConcurrencyTestUtils.assertCannotBeInvokedConcurrently(
        this.queryFactory,
        factory -> factory.createQuery(any(), any(), any(), any()),
        () -> executeQuery(queryType),
        this.reader::close);
  }

  @Test
  public void testFailsOnReturnStoredSourceForNotConfiguredIndex() {
    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.exists().path("foo").build())
            .index(MOCK_INDEX_NAME)
            .returnStoredSource(true)
            .build();
    TestUtils.assertThrows(
        "storedSource is not configured",
        InvalidQueryException.class,
        () ->
            this.reader.query(
                query,
                QueryCursorOptions.empty(),
                BatchSizeStrategySelector.forQuery(query, QueryCursorOptions.empty()),
                QueryOptimizationFlags.DEFAULT_OPTIONS));
  }

  @Test
  public void testIndexReaderObeysConcurrentFlag() throws Exception {

    // import a bunch of documents to make sure Lucene splits them
    // in multiple slices
    for (int i = 0; i < 10; i++) {
      BsonDocument bson = new BsonDocument().append("_id", new BsonInt32(i));
      Document doc = new Document();
      doc.add(
          LuceneDocumentIdEncoder.documentIdField(LuceneDocumentIdEncoder.encodeDocumentId(bson)));
      doc.add(new StringField("name", "test", Field.Store.NO));

      this.writer.addDocument(doc);
      this.writer.commit();
    }

    this.reader.refresh();

    Query sequentialQuery =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.exists().path("foo").build())
            .index(MOCK_INDEX_NAME)
            .returnStoredSource(false)
            .concurrent(false)
            .build();

    this.reader.query(
        sequentialQuery,
        QueryCursorOptions.empty(),
        BatchSizeStrategySelector.forQuery(sequentialQuery, QueryCursorOptions.empty()),
        QueryOptimizationFlags.DEFAULT_OPTIONS);
    verify(this.concurrentSearchExecutor, Mockito.never()).execute(any());

    Query concurrentQuery =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.exists().path("foo").build())
            .index(MOCK_INDEX_NAME)
            .returnStoredSource(false)
            .concurrent(true)
            .build();

    this.reader.query(
        concurrentQuery,
        QueryCursorOptions.empty(),
        BatchSizeStrategySelector.forQuery(concurrentQuery, QueryCursorOptions.empty()),
        QueryOptimizationFlags.DEFAULT_OPTIONS);
    verify(this.concurrentSearchExecutor, Mockito.atLeast(1)).execute(any());
  }

  @Test
  public void testGetNumEmbeddedRootDocuments() throws Exception {
    BsonDocument bsonDoc1 = new BsonDocument().append("_id", new BsonInt32(1));
    Document doc1 = new Document();
    byte[] id1 = LuceneDocumentIdEncoder.encodeDocumentId(bsonDoc1);
    doc1.add(LuceneDocumentIdEncoder.documentIdField(id1));
    doc1.add(
        new StringField(
            FieldName.MetaField.EMBEDDED_ROOT.getLuceneFieldName(), "T", Field.Store.NO));
    doc1.add(new StringField("name", "East High", Field.Store.NO));

    this.writer.addDocument(doc1);
    this.writer.commit();

    this.reader.refresh();
    assertEquals(1, this.reader.getNumEmbeddedRootDocuments());

    // add 1 more embedded document
    BsonDocument bsonDoc2 = new BsonDocument().append("_id", new BsonInt32(2));
    Document doc2 = new Document();
    byte[] id2 = LuceneDocumentIdEncoder.encodeDocumentId(bsonDoc2);
    doc2.add(LuceneDocumentIdEncoder.documentIdField(id2));
    doc2.add(
        new StringField(
            FieldName.MetaField.EMBEDDED_ROOT.getLuceneFieldName(), "T", Field.Store.NO));
    doc2.add(new StringField("name", "East High School", Field.Store.NO));

    this.writer.addDocument(doc2);
    this.writer.commit();
    this.reader.refresh();
    assertEquals(2, this.reader.getNumEmbeddedRootDocuments());

    // update document to be non-embedded
    Document doc1v2 = new Document();
    doc1v2.add(LuceneDocumentIdEncoder.documentIdField(id1));
    doc1v2.add(new StringField("hello", "moon", Field.Store.YES));

    this.writer.updateDocument(LuceneDocumentIdEncoder.documentIdTerm(id1), doc1v2);
    this.writer.commit();

    this.reader.refresh();
    assertEquals(1, this.reader.getNumEmbeddedRootDocuments());

    // delete doc2, make sure 0 documents doesn't error
    this.writer.deleteDocuments(LuceneDocumentIdEncoder.documentIdTerm(id2));
    this.writer.commit();

    this.reader.refresh();
    assertEquals(0, this.reader.getNumEmbeddedRootDocuments());
  }

  @Test
  public void testGetNumEmbeddedRootDocsDoesNotCountUnreclaimedDeletes() throws Exception {
    this.writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    BsonDocument bsonDoc1 = new BsonDocument().append("_id", new BsonInt32(1));
    Document doc1 = new Document();
    byte[] id1 = LuceneDocumentIdEncoder.encodeDocumentId(bsonDoc1);
    doc1.add(LuceneDocumentIdEncoder.documentIdField(id1));
    doc1.add(
        new StringField(
            FieldName.MetaField.EMBEDDED_ROOT.getLuceneFieldName(), "T", Field.Store.NO));

    // add 1 more embedded document
    BsonDocument bsonDoc2 = new BsonDocument().append("_id", new BsonInt32(2));
    Document doc2 = new Document();
    byte[] id2 = LuceneDocumentIdEncoder.encodeDocumentId(bsonDoc2);
    doc2.add(LuceneDocumentIdEncoder.documentIdField(id2));
    doc2.add(
        new StringField(
            FieldName.MetaField.EMBEDDED_ROOT.getLuceneFieldName(), "T", Field.Store.NO));

    this.writer.addDocument(doc1);
    this.writer.addDocument(doc2);
    this.writer.commit();

    this.reader.refresh();
    assertEquals(2, this.reader.getNumEmbeddedRootDocuments());

    // deleting a document with no merge policy means the document is never physically deleted
    this.writer.deleteDocuments(LuceneDocumentIdEncoder.documentIdTerm(id2));
    this.writer.commit();

    this.reader.refresh();
    assertEquals(1, this.reader.getNumEmbeddedRootDocuments());
  }

  @Test
  public void testQuery_ExecutesFacetCollectorWhenDrillSidewaysInfoHasNonDrillSidewaysStatus()
      throws Exception {
    Optional<DrillSidewaysInfo> drillSidewaysInfo =
        Optional.of(
            new DrillSidewaysInfo(
                Map.of(), QueryOptimizationStatus.NON_DRILL_SIDEWAYS, Optional.empty()));
    assertExecutesFacetCollectorFlow(drillSidewaysInfo);
  }

  @Test
  public void testQuery_ExecutesFacetCollectorWhenNoDrillSideways() throws Exception {
    assertExecutesFacetCollectorFlow(Optional.empty());
  }

  private void assertExecutesFacetCollectorFlow(Optional<DrillSidewaysInfo> drillSidewaysInfo)
      throws IOException, InvalidQueryException, InterruptedException {

    LuceneSearchIndexReader readerSpy = spy(this.reader);

    doThrow(new AssertionError("unexpected method call"))
        .when(readerSpy)
        .genericDrillSidewaysQuery(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

    doThrow(new AssertionError("unexpected method call"))
        .when(readerSpy)
        .optimizedDrillSidewaysFacetCollectorQuery(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

    doReturn(mock(SearchProducerAndMetaResults.class))
        .when(readerSpy)
        .collectorQuery(any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

    executeCollectorQueryOnSpy(drillSidewaysInfo, readerSpy);
  }

  @Test
  public void testQuery_ExecutesGenericDrillSidewaysFlow() throws Exception {

    Optional<DrillSidewaysInfo> drillSidewaysInfo =
        Optional.of(
            new DrillSidewaysInfo(Map.of(), QueryOptimizationStatus.GENERIC, Optional.empty()));

    LuceneSearchIndexReader readerSpy = spy(this.readerWithDrillSidewaysEnabled);

    doReturn(mock(SearchProducerAndMetaResults.class))
        .when(readerSpy)
        .genericDrillSidewaysQuery(
            any(), any(), any(), any(), any(), any(), any(),
            any(), any(), any(), any(), any());

    doThrow(new AssertionError("unexpected method call"))
        .when(readerSpy)
        .facetCollectorQuery(any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

    executeCollectorQueryOnSpy(drillSidewaysInfo, readerSpy);
  }

  @Test
  public void testQuery_ExecutesOptimizableDrillSidewaysFlow() throws Exception {

    Optional<DrillSidewaysInfo> drillSidewaysInfo =
        Optional.of(
            new DrillSidewaysInfo(Map.of(), QueryOptimizationStatus.OPTIMIZABLE, Optional.empty()));

    LuceneSearchIndexReader readerSpy = spy(this.readerWithDrillSidewaysEnabled);

    doReturn(mock(SearchProducerAndMetaResults.class))
        .when(readerSpy)
        .optimizedDrillSidewaysFacetCollectorQuery(
            any(), any(), any(), any(), any(), any(),
            any(), any(), any(), any(), any(), any());

    doThrow(new AssertionError("unexpected method call"))
        .when(readerSpy)
        .facetCollectorQuery(any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

    executeCollectorQueryOnSpy(drillSidewaysInfo, readerSpy);
  }

  @Test
  public void testQuery_WhenDrillSidewaysInfoGenericButFlagDisabled_ExecutesFacetCollectorFlow()
      throws Exception {
    Optional<DrillSidewaysInfo> drillSidewaysInfo =
        Optional.of(new DrillSidewaysInfo(Map.of(), QueryOptimizationStatus.GENERIC,
            Optional.empty()));

    LuceneSearchIndexReader readerSpy = spy(this.reader); // flag disabled by default

    doThrow(new AssertionError("unexpected method call"))
        .when(readerSpy)
        .genericDrillSidewaysQuery(any(), any(), any(), any(), any(), any(), any(), any(), any(),
            any(), any(), any());

    doThrow(new AssertionError("unexpected method call"))
        .when(readerSpy)
        .optimizedDrillSidewaysFacetCollectorQuery(any(), any(), any(), any(), any(), any(), any(),
            any(), any(), any(), any(), any());

    doReturn(mock(SearchProducerAndMetaResults.class))
        .when(readerSpy)
        .facetCollectorQuery(any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

    executeCollectorQueryOnSpy(drillSidewaysInfo, readerSpy);
  }

  @Test
  public void testQuery_WhenDrillSidewaysInfoOptimizableButFlagDisabled_ExecutesFacetCollectorFlow()
      throws Exception {

    Optional<DrillSidewaysInfo> drillSidewaysInfo =
        Optional.of(
            new DrillSidewaysInfo(
                Map.of(), QueryOptimizationStatus.OPTIMIZABLE, Optional.empty()));

    // Use reader with default registry (drill-sideways flag disabled)
    LuceneSearchIndexReader readerSpy = spy(this.reader);

    // These must NOT be called when flag is disabled
    doThrow(new AssertionError("unexpected method call"))
        .when(readerSpy)
        .genericDrillSidewaysQuery(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

    doThrow(new AssertionError("unexpected method call"))
        .when(readerSpy)
        .optimizedDrillSidewaysFacetCollectorQuery(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

    // Facet collector path should be taken
    doReturn(mock(SearchProducerAndMetaResults.class))
        .when(readerSpy)
        .facetCollectorQuery(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

    executeCollectorQueryOnSpy(drillSidewaysInfo, readerSpy);
  }

  private static void executeCollectorQueryOnSpy(
      Optional<DrillSidewaysInfo> drillSidewaysInfo, LuceneSearchIndexReader readerSpy)
      throws IOException, InvalidQueryException, InterruptedException {
    FacetCollector facetCollector = new FacetCollector(null, Map.of(), drillSidewaysInfo);
    CollectorQuery collectorQuery =
        new CollectorQuery(
            facetCollector,
            "indexName",
            null,
            Optional.empty(),
            Optional.empty(),
            false,
            false,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    readerSpy.collectorQuery(
        collectorQuery,
        mock(org.apache.lucene.search.Query.class),
        mock(LuceneIndexSearcherReference.class),
        mock(BatchSizeStrategy.class),
        Optional.of(mock(LuceneUnifiedHighlighter.class)),
        Optional.of(mock(LuceneScoreDetailsManager.class)),
        Optional.empty(),
        Optional.empty(),
        mock(QueryCursorOptions.class),
        new QueryOptimizationFlags(false));
  }

  private BatchProducer executeQuery(QueryType queryType)
      throws IOException, InvalidQueryException, InterruptedException {
    return switch (queryType) {
      case INTERMEDIATE -> intermediateQuery(QUERY_DEFINITION);
      case NON_INTERMEDIATE -> query(QUERY_DEFINITION);
    };
  }

  private void executeInvalidQuery(QueryType queryType) {
    switch (queryType) {
      case INTERMEDIATE ->
          assertThrows(
              InvalidQueryException.class, () -> intermediateQuery(INVALID_QUERY_DEFINITION));
      case NON_INTERMEDIATE ->
          assertThrows(InvalidQueryException.class, () -> query(INVALID_QUERY_DEFINITION));
    }
  }

  /** must be called in the beginning of the test. */
  private IndexReader getInternalReader() throws IOException {
    ArgumentCaptor<IndexReader> getReader = ArgumentCaptor.forClass(IndexReader.class);
    verify(this.searcherFactory, times(1)).newSearcher(getReader.capture(), any());
    return getReader.getValue();
  }

  private BatchProducer query(SearchQuery queryDefinition)
      throws IOException, InvalidQueryException, InterruptedException {
    return this.reader.query(
            queryDefinition,
            QueryCursorOptions.empty(),
            BatchSizeStrategySelector.forQuery(queryDefinition, QueryCursorOptions.empty()),
            QueryOptimizationFlags.DEFAULT_OPTIONS)
        .searchBatchProducer;
  }

  private BatchProducer intermediateQuery(SearchQuery queryDefinition)
      throws IOException, InvalidQueryException, InterruptedException {
    return this.reader.intermediateQuery(
            queryDefinition,
            QueryCursorOptions.empty(),
            BatchSizeStrategySelector.forQuery(queryDefinition, QueryCursorOptions.empty()),
            QueryOptimizationFlags.DEFAULT_OPTIONS)
        .searchBatchProducer;
  }

  @Test
  public void testGetMaxStringFacetCardinality_featureFlagDisabled() {
    // When feature flag is disabled, should return 0
    assertEquals(0, this.reader.getMaxStringFacetCardinality());
  }

  @Test
  public void testGetMaxStringFacetCardinality_featureFlagEnabled_noFacets() throws Exception {
    // Create a new reader with the feature flag enabled
    var featureFlagsWithMetric =
        FeatureFlags.withDefaults()
            .enable(Feature.ACCURATE_NUM_EMBEDDED_ROOT_DOCS_METRIC)
            .enable(Feature.MAX_STRING_FACET_CARDINALITY_METRIC)
            .build();

    LuceneSearchIndexReader readerWithFlag =
        createReaderWithFeatureFlags(
            MOCK_INDEX_DEFINITION_GENERATION, null, featureFlagsWithMetric);

    // With no facets indexed, should return 0
    assertEquals(0, readerWithFlag.getMaxStringFacetCardinality());
  }

  @Test
  public void testGetMaxStringFacetCardinality_featureFlagEnabled_closedReader() throws Exception {
    // Create a new reader with the feature flag enabled
    var featureFlagsWithMetric =
        FeatureFlags.withDefaults()
            .enable(Feature.ACCURATE_NUM_EMBEDDED_ROOT_DOCS_METRIC)
            .enable(Feature.MAX_STRING_FACET_CARDINALITY_METRIC)
            .build();

    LuceneSearchIndexReader readerWithFlag =
        createReaderWithFeatureFlags(
            MOCK_INDEX_DEFINITION_GENERATION, null, featureFlagsWithMetric);

    // Close the reader
    readerWithFlag.close();

    // Closed reader should return 0
    assertEquals(0, readerWithFlag.getMaxStringFacetCardinality());
  }

  @Test
  public void testGetMaxStringFacetCardinality_withSingleFacetField() throws Exception {
    // Test with actual facet data - single field with cardinality 3
    // Add documents with facet field "color" having 3 unique values
    addFacetDocument(this.writer, "color", "red");
    addFacetDocument(this.writer, "color", "blue");
    addFacetDocument(this.writer, "color", "green");
    addFacetDocument(this.writer, "color", "red"); // duplicate
    this.writer.commit();

    // Use the mock facet index definition which has string facets enabled
    var mockIndexWithFacets =
        IndexGeneration.mockDefinitionGeneration(SearchIndex.MOCK_FACET_INDEX_DEFINITION);

    // Create searcher factory with string facets enabled
    var searcherFactory =
        new LuceneSearcherFactory(
            mockIndexWithFacets.getIndexDefinition(),
            false, // enableFacetingOverTokenFields (we're testing string facets, not token facets)
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            Optional.empty(),
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH));

    var featureFlagsWithMetric =
        FeatureFlags.withDefaults().enable(Feature.MAX_STRING_FACET_CARDINALITY_METRIC).build();

    LuceneSearchIndexReader readerWithFacets =
        createReaderWithFeatureFlags(mockIndexWithFacets, searcherFactory, featureFlagsWithMetric);

    // Should return cardinality of 3 (red, blue, green)
    assertEquals(3, readerWithFacets.getMaxStringFacetCardinality());
  }

  @Test
  public void testGetMaxStringFacetCardinality_withMultipleFacetFields() throws Exception {
    // Test with multiple facet fields - should return the maximum cardinality
    // Add documents with multiple facet fields
    // Field "color" has cardinality 3
    addFacetDocument(this.writer, "color", "red");
    addFacetDocument(this.writer, "color", "blue");
    addFacetDocument(this.writer, "color", "green");

    // Field "size" has cardinality 5 (this should be the max)
    addFacetDocument(this.writer, "size", "small");
    addFacetDocument(this.writer, "size", "medium");
    addFacetDocument(this.writer, "size", "large");
    addFacetDocument(this.writer, "size", "xlarge");
    addFacetDocument(this.writer, "size", "xxlarge");

    // Field "brand" has cardinality 2
    addFacetDocument(this.writer, "brand", "nike");
    addFacetDocument(this.writer, "brand", "adidas");

    this.writer.commit();

    // Use the mock facet index definition which has string facets enabled
    var mockIndexWithFacets =
        IndexGeneration.mockDefinitionGeneration(SearchIndex.MOCK_FACET_INDEX_DEFINITION);

    // Create searcher factory with string facets enabled
    var searcherFactory =
        new LuceneSearcherFactory(
            mockIndexWithFacets.getIndexDefinition(),
            false, // enableFacetingOverTokenFields (we're testing string facets, not token facets)
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            Optional.empty(),
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH));

    var featureFlagsWithMetric =
        FeatureFlags.withDefaults().enable(Feature.MAX_STRING_FACET_CARDINALITY_METRIC).build();

    LuceneSearchIndexReader readerWithFacets =
        createReaderWithFeatureFlags(mockIndexWithFacets, searcherFactory, featureFlagsWithMetric);

    // Should return 5 (max cardinality from "size" field)
    assertEquals(5, readerWithFacets.getMaxStringFacetCardinality());
  }

  private LuceneSearchIndexReader createReaderWithFeatureFlags(
      SearchIndexDefinitionGeneration indexDefinitionGeneration,
      LuceneSearcherFactory searcherFactory,
      FeatureFlags featureFlags)
      throws Exception {
    var metricsFactory = SearchIndex.mockMetricsFactory();
    IndexMetricsUpdater indexMetricsUpdater =
        IndexMetricsUpdaterBuilder.builder()
            .metricsFactory(metricsFactory)
            .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
            .build();

    LuceneSearcherManager searcherManager =
        LuceneSearcherManager.create(
            this.writer,
            searcherFactory != null ? searcherFactory : this.searcherFactory,
            SearchIndex.mockMetricsFactory());

    Analyzer analyzer =
        LuceneAnalyzer.indexAnalyzer(
            indexDefinitionGeneration.getIndexDefinition(), AnalyzerRegistryBuilder.empty());
    LuceneHighlighterContext highlighterContext =
        new LuceneHighlighterContext(
            indexDefinitionGeneration
                .getIndexDefinition()
                .createFieldDefinitionResolver(
                    indexDefinitionGeneration.generation().indexFormatVersion),
            analyzer);
    LuceneFacetContext facetContext =
        new LuceneFacetContext(
            indexDefinitionGeneration
                .getIndexDefinition()
                .createFieldDefinitionResolver(
                    indexDefinitionGeneration.generation().indexFormatVersion),
            indexDefinitionGeneration
                .getIndexDefinition()
                .getIndexCapabilities(indexDefinitionGeneration.generation().indexFormatVersion));

    return LuceneSearchIndexReader.create(
        this.queryFactory,
        searcherManager,
        indexDefinitionGeneration.getIndexDefinition(),
        highlighterContext,
        facetContext,
        indexMetricsUpdater.getQueryingMetricsUpdater(),
        new LuceneSearchManagerFactory(
            indexDefinitionGeneration
                .getIndexDefinition()
                .createFieldDefinitionResolver(
                    indexDefinitionGeneration.generation().indexFormatVersion),
            new BinaryQuantizedVectorRescorer(Optional.of(this.concurrentRescoringExecutor)),
            indexMetricsUpdater.getQueryingMetricsUpdater()),
        Optional.of(this.concurrentSearchExecutor),
        0,
        featureFlags,
        new DynamicFeatureFlagRegistry(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
  }

  private void addFacetDocument(IndexWriter writer, String dimension, String value)
      throws IOException {
    Document doc = new Document();
    doc.add(new SortedSetDocValuesFacetField(dimension, value));
    FacetsConfig config = new FacetsConfig();
    writer.addDocument(config.build(doc));
  }
}
