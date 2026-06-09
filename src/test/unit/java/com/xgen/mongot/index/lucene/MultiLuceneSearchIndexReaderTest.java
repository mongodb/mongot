package com.xgen.mongot.index.lucene;

import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_DYNAMIC_MULTI_FACET_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_DYNAMIC_MULTI_FACET_INDEX_PARTITION_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_MULTI_INDEX_PARTITION_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.NUM_PARTITIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.BatchSizeStrategySelector;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.FacetBucket;
import com.xgen.mongot.index.FacetInfo;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.MeteredSearchIndexReader;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.SearchIndexReader.SearchProducerAndMetaProducer;
import com.xgen.mongot.index.SearchIndexReader.SearchProducerAndMetaResults;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryFactory;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.lucene.explain.information.CollectorExplainInformation;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.cursor.batch.BatchCursorOptionsBuilder;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.DateFacetFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DateFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFacetFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import com.xgen.testing.mongot.index.query.CollectorQueryBuilder;
import com.xgen.testing.mongot.index.query.OperatorQueryBuilder;
import com.xgen.testing.mongot.index.query.collectors.CollectorBuilder;
import com.xgen.testing.mongot.index.query.collectors.FacetDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import com.xgen.testing.mongot.mock.index.IndexGeneration;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.lucene.index.TieredMergePolicy;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNumber;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(Theories.class)
public class MultiLuceneSearchIndexReaderTest {

  private static final FeatureFlags FEATURE_FLAGS =
      FeatureFlags.withDefaults()
          .enable(Feature.FACETING_OVER_TOKEN_FIELDS)
          .enable(Feature.CONCURRENT_INDEX_PARTITION_SEARCH)
          .build();
  private static final FeatureFlags FEATURE_FLAGS_CONCURRENT_DISABLED =
      FeatureFlags.withDefaults().enable(Feature.FACETING_OVER_TOKEN_FIELDS).build();
  private static final Operator EXISTS_OPERATOR =
      OperatorBuilder.exists()
          .path("field1")
          .score(ScoreBuilder.constant().value(1).build())
          .build();
  private static final OperatorQuery QUERY_DEFINITION =
      OperatorQueryBuilder.builder()
          .operator(EXISTS_OPERATOR)
          .index(MOCK_MULTI_INDEX_PARTITION_DEFINITION_GENERATION.getIndexDefinition().getName())
          .returnStoredSource(false)
          .concurrent(true)
          .build();
  private static final OperatorQuery NON_CONCURRENT_QUERY_DEFINITION =
      OperatorQueryBuilder.builder()
          .operator(
              OperatorBuilder.exists()
                  .path("field1")
                  .score(ScoreBuilder.constant().value(1).build())
                  .build())
          .index(MOCK_MULTI_INDEX_PARTITION_DEFINITION_GENERATION.getIndexDefinition().getName())
          .returnStoredSource(false)
          .concurrent(false)
          .build();
  private static final CollectorQuery COLLECTOR_QUERY_DEF =
      CollectorQueryBuilder.builder()
          .collector(
              CollectorBuilder.facet()
                  .operator(EXISTS_OPERATOR)
                  .facetDefinitions(
                      Map.of("facet", FacetDefinitionBuilder.string().path("f").build()))
                  .build())
          .index(
              MOCK_DYNAMIC_MULTI_FACET_INDEX_PARTITION_DEFINITION_GENERATION
                  .getIndexDefinition()
                  .getName())
          .returnStoredSource(false)
          .build();
  private static final CollectorQuery CONCURRENT_FACET_QUERY_DEF =
      CollectorQueryBuilder.builder()
          .collector(
              CollectorBuilder.facet()
                  .operator(EXISTS_OPERATOR)
                  .facetDefinitions(
                      Map.of("facet", FacetDefinitionBuilder.string().path("f").build()))
                  .build())
          .index(
              MOCK_DYNAMIC_MULTI_FACET_INDEX_PARTITION_DEFINITION_GENERATION
                  .getIndexDefinition()
                  .getName())
          .returnStoredSource(false)
          .concurrent(true)
          .build();
  private static final CollectorQuery NON_CONCURRENT_FACET_QUERY_DEF =
      CollectorQueryBuilder.builder()
          .collector(
              CollectorBuilder.facet()
                  .operator(EXISTS_OPERATOR)
                  .facetDefinitions(
                      Map.of("facet", FacetDefinitionBuilder.string().path("f").build()))
                  .build())
          .index(
              MOCK_DYNAMIC_MULTI_FACET_INDEX_PARTITION_DEFINITION_GENERATION
                  .getIndexDefinition()
                  .getName())
          .returnStoredSource(false)
          .concurrent(false)
          .build();
  private static final Date DATE_1 = date(3);
  private static final Date DATE_2 = date(4);
  private static final Date DATE_3 = date(5);
  private static final Date DATE_4 = date(6);
  private InitializedSearchIndex searchIndexWithPartitions;
  private InitializedSearchIndex searchIndexNoPartition;
  private NamedExecutorService concurrentSearchExecutor;

  @DataPoints("allQueryTypes")
  public static Set<QueryType> intermediateOrRegularQuery() {
    return Set.of(QueryType.INTERMEDIATE, QueryType.NON_INTERMEDIATE);
  }

  private static DynamicFeatureFlagRegistry drillSidewaysEnabledRegistry() {
    DynamicFeatureFlagRegistry registry = mock(DynamicFeatureFlagRegistry.class);

    // Everything else: return default
    Mockito.when(registry.evaluateClusterInvariant(any(), anyBoolean()))
        .thenAnswer(invocation -> invocation.getArgument(1));

    // Always enable drill-sideways, regardless of default
    Mockito.when(
            registry.evaluateClusterInvariant(
                eq(DynamicFeatureFlags.DRILL_SIDEWAYS_FACETING.getName()), anyBoolean()))
        .thenReturn(true);
    Mockito.when(
            registry.evaluateClusterInvariant(
                eq(DynamicFeatureFlags.COLLECT_MULTI_PARTITION_EMPTY_SEARCH_PRODUCER.getName()),
                anyBoolean()))
        .thenReturn(true);

    return registry;
  }

  private static InitializedSearchIndex createSearchIndex(
      SearchIndexDefinitionGeneration defGeneration,
      NamedExecutorService concurrentSearchExecutor,
      FeatureFlags featureFlags)
      throws IOException {
    var folder = TestUtils.getTempFolder();
    var path = folder.getRoot().toPath();
    var indexPath = path.resolve("indexGeneration");
    var metadataPath = path.resolve("indexMapping/indexGeneration");
    var luceneConfig = LuceneConfigBuilder.builder().dataPath(indexPath).build();
    var numPartitions = defGeneration.getIndexDefinition().getNumPartitions();
    var directoryFactory =
        new IndexDirectoryFactory(
            indexPath, metadataPath, luceneConfig, numPartitions, Optional.empty());
    var metricsFactory = SearchIndex.mockMetricsFactory();

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    var concurrentRescoringExecutor =
        spy(Executors.fixedSizeThreadScheduledExecutor("test", 1, meterRegistry));
    var mergeScheduler = new InstrumentedConcurrentMergeScheduler(meterRegistry);
    mergeScheduler.setMaxMergesAndThreads(10, 10);
    var index =
        LuceneSearchIndex.createDiskBacked(
            indexPath,
            metadataPath,
            luceneConfig,
            featureFlags,
            mergeScheduler,
            new TieredMergePolicy(),
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            mock(NamedScheduledExecutorService.class),
            Optional.of(concurrentSearchExecutor),
            Optional.of(concurrentRescoringExecutor),
            defGeneration.getIndexDefinition(),
            defGeneration.generation().indexFormatVersion,
            AnalyzerRegistryBuilder.empty(),
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            metricsFactory,
            Runnable::run);
    return InitializedLuceneSearchIndex.create(
        index,
        defGeneration.getGenerationId(),
        directoryFactory,
        mock(IndexDirectoryHelper.class),
        Optional.empty(),
        featureFlags,
        drillSidewaysEnabledRegistry(),
        () -> false);
  }

  private static void assertStringBucket(
      List<FacetBucket> facetBuckets, int index, String expectedBucketId, long expectedBucketSize) {
    assertEquals(expectedBucketId, facetBuckets.get(index).getId().asString().getValue());
    assertEquals(expectedBucketSize, facetBuckets.get(index).getCount());
  }

  private static void assertNumericBucket(
      List<FacetBucket> facetBuckets, int index, int expectedBucketId, long expectedBucketSize) {
    assertEquals(expectedBucketId, facetBuckets.get(index).getId().asInt32().getValue());
    assertEquals(expectedBucketSize, facetBuckets.get(index).getCount());
  }

  private static void assertDateBucket(
      List<FacetBucket> facetBuckets, int index, long expectedBucketId, long expectedBucketSize) {
    assertEquals(expectedBucketId, facetBuckets.get(index).getId().asDateTime().getValue());
    assertEquals(expectedBucketSize, facetBuckets.get(index).getCount());
  }

  private static List<BsonDocument> getResults(int docsRequested, BatchProducer searchProducer)
      throws IOException {
    BatchCursorOptions cursorOptions =
        BatchCursorOptionsBuilder.builder().docsRequested(docsRequested).build();
    Bytes sizeLimit = CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT;
    List<BsonDocument> result = new ArrayList<>();
    while (!searchProducer.isExhausted()) {
      searchProducer.execute(sizeLimit, cursorOptions);
      BsonArray array = searchProducer.getNextBatch(sizeLimit);
      for (var value : array.getValues()) {
        result.add(value.asDocument());
      }
    }
    // After exhausted, calling an extra getNextBatch() will return empty.
    searchProducer.execute(sizeLimit, cursorOptions);
    assertTrue(searchProducer.getNextBatch(sizeLimit).isEmpty());
    searchProducer.close();
    return result;
  }

  private static SearchProducerAndMetaResults query(
      InitializedSearchIndex index, SearchQuery queryDefinition)
      throws IOException, InvalidQueryException, InterruptedException {
    return index
        .getReader()
        .query(
            queryDefinition,
            QueryCursorOptions.empty(),
            BatchSizeStrategySelector.forQuery(queryDefinition, QueryCursorOptions.empty()),
            QueryOptimizationFlags.DEFAULT_OPTIONS);
  }

  private static SearchProducerAndMetaProducer intermediateQuery(
      InitializedSearchIndex index, SearchQuery queryDefinition)
      throws IOException, InvalidQueryException, InterruptedException {
    return index
        .getReader()
        .intermediateQuery(
            queryDefinition,
            QueryCursorOptions.empty(),
            BatchSizeStrategySelector.forQuery(queryDefinition, QueryCursorOptions.empty()),
            QueryOptimizationFlags.DEFAULT_OPTIONS);
  }

  private static SearchIndexDefinitionGeneration getSearchIndexDefinitionGeneration(
      DocumentFieldDefinitionBuilder facetIndexMappingsDrillSideways) {
    return IndexGeneration.mockDefinitionGeneration(
        SearchIndex.mockDefinitionBuilder(facetIndexMappingsDrillSideways.dynamic(true).build())
            .numPartitions(SearchIndex.NUM_PARTITIONS)
            .build());
  }

  private static Date date(int dayOfMonth) {
    return new Calendar.Builder()
        .setDate(2025, Calendar.NOVEMBER, dayOfMonth)
        .setTimeOfDay(0, 0, 0, 0)
        .setTimeZone(TimeZone.getTimeZone("GMT"))
        .build()
        .getTime();
  }

  @SuppressWarnings("unchecked")
  private static List<LuceneSearchIndexReader> getUnderlyingReaders(
      MultiLuceneSearchIndexReader multiReader) throws Exception {
    Field readersField = MultiLuceneSearchIndexReader.class.getDeclaredField("readers");
    readersField.setAccessible(true);
    return (List<LuceneSearchIndexReader>) readersField.get(multiReader);
  }

  private static int getCurrentSearcherRefCount(LuceneSearchIndexReader reader) throws Exception {
    Field managerField = LuceneSearchIndexReader.class.getDeclaredField("searcherManager");
    managerField.setAccessible(true);
    Object manager = managerField.get(reader);
    Field currentField = manager.getClass().getSuperclass().getDeclaredField("current");
    currentField.setAccessible(true);
    LuceneIndexSearcher searcher = (LuceneIndexSearcher) currentField.get(manager);
    return searcher.getIndexReader().getRefCount();
  }

  /** Set up resources for test. */
  @Before
  public void setUp() throws Exception {
    // Need more threads than NUM_PARTITIONS (8) to avoid deadlock:
    // - 8 threads for partition queries
    // - Additional threads for nested facet counting (ConcurrentSortedSetDocValuesFacetCounts
    //   uses the same executor for parallel facet counting within each partition)
    this.concurrentSearchExecutor =
        spy(
            Executors.fixedSizeThreadScheduledExecutor(
                "testConcurrentSearchExecutor", 16, new SimpleMeterRegistry()));
    setupIndexWithPartitions(MOCK_DYNAMIC_MULTI_FACET_INDEX_PARTITION_DEFINITION_GENERATION);

    this.searchIndexNoPartition =
        createSearchIndex(
            MOCK_DYNAMIC_MULTI_FACET_DEFINITION_GENERATION,
            this.concurrentSearchExecutor,
            FEATURE_FLAGS);
  }

  private void setupIndexWithPartitions(
      SearchIndexDefinitionGeneration partitionDefinitionGeneration) throws IOException {
    this.searchIndexWithPartitions =
        createSearchIndex(
            partitionDefinitionGeneration, this.concurrentSearchExecutor, FEATURE_FLAGS);
    assertTrue(this.searchIndexWithPartitions.getReader() instanceof MeteredSearchIndexReader);
    assertTrue(
        ((MeteredSearchIndexReader) this.searchIndexWithPartitions.getReader()).unwrap()
            instanceof MultiLuceneSearchIndexReader);
    var multiReader =
        (MultiLuceneSearchIndexReader)
            ((MeteredSearchIndexReader) this.searchIndexWithPartitions.getReader()).unwrap();
    assertEquals(NUM_PARTITIONS, multiReader.numUnderlyingReaders());
  }

  @After
  public void tearDown() throws Exception {
    this.searchIndexWithPartitions.close();
    this.searchIndexNoPartition.close();
    this.concurrentSearchExecutor.shutdown();
  }

  @Theory
  public void testEmptyIndex(@FromDataPoints("allQueryTypes") QueryType queryType)
      throws Exception {
    assertEquals(0, this.searchIndexWithPartitions.getWriter().getNumDocs());
    assertEquals(new ArrayList<BsonDocument>(), queryIndexWithPartitions(queryType));
  }

  @Theory
  public void testIndexHasNoHit(@FromDataPoints("allQueryTypes") QueryType queryType)
      throws Exception {
    BsonDocument bsonDocument = BsonDocument.parse("{_id: 99, a: 'mongo'}");
    addDocument(bsonDocument);
    assertEquals(1, this.searchIndexWithPartitions.getWriter().getNumDocs());
    assertEquals(new ArrayList<BsonDocument>(), queryIndexWithPartitions(queryType));
    checkResultsAreIdenticalWithAndWithoutPartition(queryType);
  }

  @Theory
  public void testIndexHasOneHit(@FromDataPoints("allQueryTypes") QueryType queryType)
      throws Exception {
    BsonDocument bsonDocument = BsonDocument.parse("{_id: 99, field1: 'mongo'}");
    addDocument(bsonDocument);
    assertEquals(1, this.searchIndexWithPartitions.getWriter().getNumDocs());
    List<BsonDocument> expectedResult = List.of(BsonDocument.parse("{_id: 99, $searchScore: 1.0}"));
    assertEquals(expectedResult, queryIndexWithPartitions(queryType));
    checkResultsAreIdenticalWithAndWithoutPartition(queryType);
  }

  @Theory
  public void testIndexHasManyHits(@FromDataPoints("allQueryTypes") QueryType queryType)
      throws Exception {
    int numDocs = 1;
    for (int i = 0; i < numDocs; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d'}", i, i));
      addDocument(bsonDocument);
    }
    assertEquals(numDocs, this.searchIndexWithPartitions.getWriter().getNumDocs());
    assertEquals(numDocs, queryIndexWithPartitions(queryType).size());
    checkResultsAreIdenticalWithAndWithoutPartition(queryType);
  }

  @Test
  public void testFacetExplain() throws Exception {
    @Var int id = 0;
    for (int i = 0; i < 1_000; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'foo'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }

    for (int i = 0; i < 1_000; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'bar'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }

    for (int i = 0; i < 1_000; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'baz'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }

    this.searchIndexWithPartitions.getReader().refresh();

    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.EXECUTION_STATS),
            Optional.of(
                MOCK_DYNAMIC_MULTI_FACET_INDEX_PARTITION_DEFINITION_GENERATION
                    .getIndexDefinition()
                    .getNumPartitions()))) {

      intermediateQuery(this.searchIndexWithPartitions, COLLECTOR_QUERY_DEF);

      var explainResult = Explain.collect().get();
      assertTrue(explainResult.collectStats().isEmpty() && explainResult.query().isEmpty());
      assertTrue(explainResult.indexPartitionExplainInformation().isPresent());

      for (var info : explainResult.indexPartitionExplainInformation().get()) {
        assertTrue(info.collectStats().map(CollectorExplainInformation::facetStats).isPresent());
      }
    }
  }

  @Theory
  public void testDrillSidewaysGeneric_filterFieldToken(
      @FromDataPoints("allQueryTypes") QueryType queryType) throws Exception {
    FieldDefinition filterFieldDefinition =
        FieldDefinitionBuilder.builder()
            .token(TokenFieldDefinitionBuilder.builder().build())
            .build();
    testDrillSidewaysGenericForStringType(queryType, filterFieldDefinition);
  }

  @Theory
  public void testDrillSidewaysGeneric_filterFieldTokenAndStringFacet(
      @FromDataPoints("allQueryTypes") QueryType queryType) throws Exception {
    FieldDefinition filterFieldDefinition =
        FieldDefinitionBuilder.builder()
            .token(TokenFieldDefinitionBuilder.builder().build())
            .stringFacet(StringFacetFieldDefinitionBuilder.builder().build())
            .build();
    testDrillSidewaysGenericForStringType(queryType, filterFieldDefinition);
  }

  @Theory
  public void testDrillSidewaysGeneric_filterFieldNumber(
      @FromDataPoints("allQueryTypes") QueryType queryType) throws Exception {
    FieldDefinition filterFieldDefinition =
        FieldDefinitionBuilder.builder()
            .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
            .build();
    testDrillSidewaysGenericForNumericType(queryType, filterFieldDefinition);
  }

  @Theory
  public void testDrillSidewaysGeneric_filterFieldNumberAndNumberFacet(
      @FromDataPoints("allQueryTypes") QueryType queryType) throws Exception {
    FieldDefinition filterFieldDefinition =
        FieldDefinitionBuilder.builder()
            .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
            .numberFacet(NumericFieldDefinitionBuilder.builder().buildNumberFacetField())
            .build();
    testDrillSidewaysGenericForNumericType(queryType, filterFieldDefinition);
  }

  @Theory
  public void testDrillSidewaysGeneric_filterFieldDate(
      @FromDataPoints("allQueryTypes") QueryType queryType) throws Exception {
    FieldDefinition filterFieldDefinition =
        FieldDefinitionBuilder.builder().date(DateFieldDefinitionBuilder.builder().build()).build();
    testDrillSidewaysGenericForDateType(queryType, filterFieldDefinition);
  }

  @Theory
  public void testDrillSidewaysGeneric_filterFieldDateAndDateFacet(
      @FromDataPoints("allQueryTypes") QueryType queryType) throws Exception {
    FieldDefinition filterFieldDefinition =
        FieldDefinitionBuilder.builder()
            .date(DateFieldDefinitionBuilder.builder().build())
            .dateFacet(DateFacetFieldDefinitionBuilder.builder().build())
            .build();
    testDrillSidewaysGenericForDateType(queryType, filterFieldDefinition);
  }

  private void testDrillSidewaysGenericForStringType(
      QueryType queryType, FieldDefinition filterFieldDefinition) throws Exception {
    DocumentFieldDefinitionBuilder facetIndexMappingsDrillSideways =
        DocumentFieldDefinitionBuilder.builder()
            .field("f1", filterFieldDefinition)
            .field(
                "f2",
                FieldDefinitionBuilder.builder()
                    .token(TokenFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "f3",
                FieldDefinitionBuilder.builder()
                    .stringFacet(StringFacetFieldDefinitionBuilder.builder().build())
                    .build());
    CompoundOperator compoundOperatorGenericDrillSideways =
        OperatorBuilder.compound()
            .should(EXISTS_OPERATOR)
            .must(
                OperatorBuilder.equals()
                    .path("f1")
                    .value("bar")
                    .doesNotAffect(List.of("facet2", "facet3"))
                    .build())
            .build();

    SearchIndexDefinitionGeneration partitionedIndexDefinitionGenerationDrillSideways =
        getSearchIndexDefinitionGeneration(facetIndexMappingsDrillSideways);

    CollectorQuery drillSidewaysCollectorQueryDef =
        CollectorQueryBuilder.builder()
            .collector(
                CollectorBuilder.facet()
                    .operator(compoundOperatorGenericDrillSideways)
                    .facetDefinitions(
                        Map.of(
                            "facet1", FacetDefinitionBuilder.string().path("f1").build(),
                            "facet2", FacetDefinitionBuilder.string().path("f2").build(),
                            "facet3", FacetDefinitionBuilder.string().path("f3").build()))
                    .withDrillSideways()
                    .build())
            .index(partitionedIndexDefinitionGenerationDrillSideways.getIndexDefinition().getName())
            .returnStoredSource(false)
            .build();

    setupIndexWithPartitions(partitionedIndexDefinitionGenerationDrillSideways);
    @Var int numDocs = 0;
    for (int i = 0; i < 1_000; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(
              String.format(
                  "{_id: %d, field1: 'mongo_%d', f: 'foo', f1: 'foo', f2: 'foo', f3: 'foo'}",
                  numDocs, numDocs));
      addDocument(bsonDocument);
      numDocs += 1;
    }
    for (int i = 0; i < 2_000; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(
              String.format(
                  "{_id: %d, field1: 'mongo_%d', f: 'bar', f1: 'bar', f2: 'bar', f3: 'bar'}",
                  numDocs, numDocs));
      addDocument(bsonDocument);
      numDocs += 1;
    }
    for (int i = 0; i < 3_000; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(
              String.format(
                  "{_id: %d, field1: 'mongo_%d', f: 'baz', f1: 'baz', f2: 'baz', f3: 'baz'}",
                  numDocs, numDocs));
      addDocument(bsonDocument);
      numDocs += 1;
    }

    MetaResults metaResults =
        executeQueryAndGetMetaResults(
            this.searchIndexWithPartitions, queryType, drillSidewaysCollectorQueryDef);

    assertEquals(2000L, metaResults.count().getTotalOrLower().longValue());

    Map<String, FacetInfo> facets = metaResults.facet().get();
    assertEquals(3, facets.size());

    List<FacetBucket> facet1Buckets = facets.get("facet1").buckets();
    assertEquals(1, facet1Buckets.size());
    assertStringBucket(facet1Buckets, 0, "bar", 2000L);

    List<FacetBucket> facet2Buckets = facets.get("facet2").buckets();
    assertEquals(3, facet2Buckets.size());
    assertStringBucket(facet2Buckets, 0, "baz", 3000L);
    assertStringBucket(facet2Buckets, 1, "bar", 2000L);
    assertStringBucket(facet2Buckets, 2, "foo", 1000L);

    List<FacetBucket> facet3Buckets = facets.get("facet3").buckets();
    assertEquals(3, facet3Buckets.size());
    assertStringBucket(facet3Buckets, 0, "baz", 3000L);
    assertStringBucket(facet3Buckets, 1, "bar", 2000L);
    assertStringBucket(facet3Buckets, 2, "foo", 1000L);
  }

  private void testDrillSidewaysGenericForNumericType(
      QueryType queryType, FieldDefinition filterFieldDefinition) throws Exception {
    DocumentFieldDefinitionBuilder facetIndexMappingsDrillSideways =
        DocumentFieldDefinitionBuilder.builder()
            .field("f1", filterFieldDefinition)
            .field(
                "f2",
                FieldDefinitionBuilder.builder()
                    .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
                    .build())
            .field(
                "f3",
                FieldDefinitionBuilder.builder()
                    .numberFacet(NumericFieldDefinitionBuilder.builder().buildNumberFacetField())
                    .build());
    CompoundOperator compoundOperatorGenericDrillSideways =
        OperatorBuilder.compound()
            .should(EXISTS_OPERATOR)
            .must(
                OperatorBuilder.range()
                    .path("f1")
                    .numericBounds(
                        Optional.of(new LongPoint(4L)), Optional.of(new LongPoint(5L)), true, false)
                    .doesNotAffect(List.of("facet2", "facet3"))
                    .build())
            .build();

    SearchIndexDefinitionGeneration partitionedIndexDefinitionGenerationDrillSideways =
        getSearchIndexDefinitionGeneration(facetIndexMappingsDrillSideways);

    List<BsonNumber> boundaries =
        List.of(new BsonInt32(3), new BsonInt32(4), new BsonInt32(5), new BsonInt32(6));
    CollectorQuery drillSidewaysCollectorQueryDef =
        CollectorQueryBuilder.builder()
            .collector(
                CollectorBuilder.facet()
                    .operator(compoundOperatorGenericDrillSideways)
                    .facetDefinitions(
                        Map.of(
                            "facet1",
                                FacetDefinitionBuilder.numeric()
                                    .boundaries(boundaries)
                                    .path("f1")
                                    .build(),
                            "facet2",
                                FacetDefinitionBuilder.numeric()
                                    .boundaries(boundaries)
                                    .path("f2")
                                    .build(),
                            "facet3",
                                FacetDefinitionBuilder.numeric()
                                    .boundaries(boundaries)
                                    .path("f3")
                                    .build()))
                    .withDrillSideways()
                    .build())
            .index(partitionedIndexDefinitionGenerationDrillSideways.getIndexDefinition().getName())
            .returnStoredSource(false)
            .build();

    setupIndexWithPartitions(partitionedIndexDefinitionGenerationDrillSideways);
    @Var int numDocs = 0;
    for (int i = 0; i < 1_000; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(
              String.format(
                  "{_id: %d, field1: 'mongo_%d', f: 3, f1: 3, f2: 3, f3: 3}", numDocs, numDocs));
      addDocument(bsonDocument);
      numDocs += 1;
    }
    for (int i = 0; i < 2_000; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(
              String.format(
                  "{_id: %d, field1: 'mongo_%d', f: 4, f1: 4, f2: 4, f3: 4}", numDocs, numDocs));
      addDocument(bsonDocument);
      numDocs += 1;
    }
    for (int i = 0; i < 3_000; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(
              String.format(
                  "{_id: %d, field1: 'mongo_%d', f: 5, f1: 5, f2: 5, f3: 5}", numDocs, numDocs));
      addDocument(bsonDocument);
      numDocs += 1;
    }

    MetaResults metaResults =
        executeQueryAndGetMetaResults(
            this.searchIndexWithPartitions, queryType, drillSidewaysCollectorQueryDef);

    assertEquals(2000L, metaResults.count().getTotalOrLower().longValue());

    Map<String, FacetInfo> facets = metaResults.facet().get();
    assertEquals(3, facets.size());

    List<FacetBucket> facet1Buckets = facets.get("facet1").buckets();
    assertEquals(3, facet1Buckets.size());
    assertNumericBucket(facet1Buckets, 0, 3, 0L);
    assertNumericBucket(facet1Buckets, 1, 4, 2000L);
    assertNumericBucket(facet1Buckets, 2, 5, 0L);

    List<FacetBucket> facet2Buckets = facets.get("facet2").buckets();
    assertEquals(3, facet2Buckets.size());
    assertNumericBucket(facet2Buckets, 0, 3, 1000L);
    assertNumericBucket(facet2Buckets, 1, 4, 2000L);
    assertNumericBucket(facet2Buckets, 2, 5, 3000L);

    List<FacetBucket> facet3Buckets = facets.get("facet3").buckets();
    assertEquals(3, facet3Buckets.size());
    assertNumericBucket(facet3Buckets, 0, 3, 1000L);
    assertNumericBucket(facet3Buckets, 1, 4, 2000L);
    assertNumericBucket(facet3Buckets, 2, 5, 3000L);
  }

  private void testDrillSidewaysGenericForDateType(
      QueryType queryType, FieldDefinition filterFieldDefinition) throws Exception {
    DocumentFieldDefinitionBuilder facetIndexMappingsDrillSideways =
        DocumentFieldDefinitionBuilder.builder()
            .field("f1", filterFieldDefinition)
            .field(
                "f2",
                FieldDefinitionBuilder.builder()
                    .date(DateFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "f3",
                FieldDefinitionBuilder.builder()
                    .dateFacet(DateFacetFieldDefinitionBuilder.builder().build())
                    .build());
    CompoundOperator compoundOperatorGenericDrillSideways =
        OperatorBuilder.compound()
            .should(EXISTS_OPERATOR)
            .must(
                OperatorBuilder.range()
                    .path("f1")
                    .dateBounds(
                        Optional.of(new DatePoint(DATE_2)),
                        Optional.of(new DatePoint(DATE_3)),
                        true,
                        false)
                    .doesNotAffect(List.of("facet2", "facet3"))
                    .build())
            .build();

    SearchIndexDefinitionGeneration partitionedIndexDefinitionGenerationDrillSideways =
        getSearchIndexDefinitionGeneration(facetIndexMappingsDrillSideways);

    List<BsonDateTime> boundaries =
        List.of(
            new BsonDateTime(DATE_1.getTime()),
            new BsonDateTime(DATE_2.getTime()),
            new BsonDateTime(DATE_3.getTime()),
            new BsonDateTime(DATE_4.getTime()));
    CollectorQuery drillSidewaysCollectorQueryDef =
        CollectorQueryBuilder.builder()
            .collector(
                CollectorBuilder.facet()
                    .operator(compoundOperatorGenericDrillSideways)
                    .facetDefinitions(
                        Map.of(
                            "facet1",
                            FacetDefinitionBuilder.date().boundaries(boundaries).path("f1").build(),
                            "facet2",
                            FacetDefinitionBuilder.date().boundaries(boundaries).path("f2").build(),
                            "facet3",
                            FacetDefinitionBuilder.date()
                                .boundaries(boundaries)
                                .path("f3")
                                .build()))
                    .withDrillSideways()
                    .build())
            .index(partitionedIndexDefinitionGenerationDrillSideways.getIndexDefinition().getName())
            .returnStoredSource(false)
            .build();

    setupIndexWithPartitions(partitionedIndexDefinitionGenerationDrillSideways);
    @Var int numDocs = 0;
    for (int i = 0; i < 1_000; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(
              String.format(
                  """
                  {
                    _id: %d,
                    field1: 'mongo_%d',
                    f:  ISODate('2025-11-03T00:00:00Z'),
                    f1: ISODate('2025-11-03T00:00:00Z'),
                    f2: ISODate('2025-11-03T00:00:00Z'),
                    f3: ISODate('2025-11-03T00:00:00Z')
                  }
                  """,
                  numDocs, numDocs));
      addDocument(bsonDocument);
      numDocs += 1;
    }
    for (int i = 0; i < 2_000; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(
              String.format(
                  """
                  {
                    _id: %d,
                    field1: 'mongo_%d',
                    f:  ISODate('2025-11-04T00:00:00Z'),
                    f1: ISODate('2025-11-04T00:00:00Z'),
                    f2: ISODate('2025-11-04T00:00:00Z'),
                    f3: ISODate('2025-11-04T00:00:00Z')
                  }
                  """,
                  numDocs, numDocs));
      addDocument(bsonDocument);
      numDocs += 1;
    }
    for (int i = 0; i < 3_000; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(
              String.format(
                  """
                  {
                    _id: %d,
                    field1: 'mongo_%d',
                    f:  ISODate('2025-11-05T00:00:00Z'),
                    f1: ISODate('2025-11-05T00:00:00Z'),
                    f2: ISODate('2025-11-05T00:00:00Z'),
                    f3: ISODate('2025-11-05T00:00:00Z')
                  }
                  """,
                  numDocs, numDocs));
      addDocument(bsonDocument);
      numDocs += 1;
    }

    MetaResults metaResults =
        executeQueryAndGetMetaResults(
            this.searchIndexWithPartitions, queryType, drillSidewaysCollectorQueryDef);

    assertEquals(2000L, metaResults.count().getTotalOrLower().longValue());

    Map<String, FacetInfo> facets = metaResults.facet().get();
    assertEquals(3, facets.size());

    List<FacetBucket> facet1Buckets = facets.get("facet1").buckets();
    assertEquals(3, facet1Buckets.size());
    assertDateBucket(facet1Buckets, 0, DATE_1.getTime(), 0L);
    assertDateBucket(facet1Buckets, 1, DATE_2.getTime(), 2000L);
    assertDateBucket(facet1Buckets, 2, DATE_3.getTime(), 0L);

    List<FacetBucket> facet2Buckets = facets.get("facet2").buckets();
    assertEquals(3, facet2Buckets.size());
    assertDateBucket(facet2Buckets, 0, DATE_1.getTime(), 1000L);
    assertDateBucket(facet2Buckets, 1, DATE_2.getTime(), 2000L);
    assertDateBucket(facet2Buckets, 2, DATE_3.getTime(), 3000L);

    List<FacetBucket> facet3Buckets = facets.get("facet3").buckets();
    assertEquals(3, facet3Buckets.size());
    assertDateBucket(facet3Buckets, 0, DATE_1.getTime(), 1000L);
    assertDateBucket(facet3Buckets, 1, DATE_2.getTime(), 2000L);
    assertDateBucket(facet3Buckets, 2, DATE_3.getTime(), 3000L);
  }

  /** Adds documents to both indexes. */
  private void addDocument(BsonDocument bsonDocument)
      throws IOException, FieldExceededLimitsException {
    ObjectId indexId = new ObjectId();
    bsonDocument.append(indexId.toString(), new BsonDocument("_id", bsonDocument.get("_id")));
    RawBsonDocument document = BsonUtils.documentToRaw(bsonDocument);
    var docEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
    this.searchIndexWithPartitions.getWriter().updateIndex(docEvent);
    this.searchIndexNoPartition.getWriter().updateIndex(docEvent);
  }

  private MetaResults executeQueryAndGetMetaResults(
      InitializedSearchIndex index, QueryType queryType, SearchQuery queryDefinition)
      throws IOException, InvalidQueryException, InterruptedException, ReaderClosedException {
    // Ensure that all the documents updates will be visible in the tests.
    index.getReader().refresh();
    MetaResults metaResults =
        switch (queryType) {
          case INTERMEDIATE -> {
            SearchProducerAndMetaProducer searchProducerAndMetaProducer =
                intermediateQuery(index, queryDefinition);
            FacetMergingBatchProducer metaBatchProducer =
                (FacetMergingBatchProducer) searchProducerAndMetaProducer.metaBatchProducer;
            yield metaBatchProducer.getMetaResultsAndClose(queryDefinition.count().type());
          }
          case NON_INTERMEDIATE -> query(index, queryDefinition).metaResults;
        };
    Check.argNotNull(metaResults, "metaResults");
    return metaResults;
  }

  private List<BsonDocument> executeQueryAndGetResults(
      InitializedSearchIndex index, QueryType queryType)
      throws IOException, InvalidQueryException, InterruptedException, ReaderClosedException {
    // Ensure that all the documents updates will be visible in the tests.
    index.getReader().refresh();
    BatchProducer searchProducer =
        switch (queryType) {
          case INTERMEDIATE -> intermediateQuery(index, QUERY_DEFINITION).searchBatchProducer;
          case NON_INTERMEDIATE -> query(index, QUERY_DEFINITION).searchBatchProducer;
        };
    Check.argNotNull(searchProducer, "searchProducer");
    int docsRequested = 27;
    return getResults(docsRequested, searchProducer);
  }

  private void checkResultsAreIdenticalWithAndWithoutPartition(QueryType queryType)
      throws IOException, InvalidQueryException, InterruptedException, ReaderClosedException {
    List<BsonDocument> resultsWithPartition =
        executeQueryAndGetResults(this.searchIndexWithPartitions, queryType);
    List<BsonDocument> resultsNoPartition =
        executeQueryAndGetResults(this.searchIndexNoPartition, queryType);
    assertEquals(resultsNoPartition, resultsWithPartition);
  }

  private List<BsonDocument> queryIndexWithPartitions(QueryType queryType)
      throws IOException, InvalidQueryException, InterruptedException, ReaderClosedException {
    return executeQueryAndGetResults(this.searchIndexWithPartitions, queryType);
  }

  @Test
  public void testOmitResultsClosesSearcherReferencesForAllPartitions() throws Exception {
    @Var SearchIndexReader reader = this.searchIndexWithPartitions.getReader();
    if (reader instanceof MeteredSearchIndexReader meteredReader) {
      reader = meteredReader.unwrap();
    }
    assertTrue(reader instanceof MultiLuceneSearchIndexReader);
    MultiLuceneSearchIndexReader multiReader = (MultiLuceneSearchIndexReader) reader;

    List<LuceneSearchIndexReader> underlyingReaders = getUnderlyingReaders(multiReader);
    List<Integer> refCountsBefore = new ArrayList<>(underlyingReaders.size());
    for (LuceneSearchIndexReader partitionReader : underlyingReaders) {
      refCountsBefore.add(getCurrentSearcherRefCount(partitionReader));
    }

    SearchProducerAndMetaResults result =
        multiReader.query(
            QUERY_DEFINITION,
            QueryCursorOptions.empty(),
            BatchSizeStrategySelector.forQuery(QUERY_DEFINITION, QueryCursorOptions.empty()),
            new QueryOptimizationFlags(true));
    result.searchBatchProducer.close();

    for (int i = 0; i < underlyingReaders.size(); i++) {
      int refCountAfter = getCurrentSearcherRefCount(underlyingReaders.get(i));
      assertEquals((int) refCountsBefore.get(i), refCountAfter);
    }
  }

  @Test
  public void testConcurrentQueryUsesExecutor() throws Exception {
    // Add some documents to ensure there are results
    int numDocs = 5;
    for (int i = 0; i < numDocs; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'test_%d'}", i, i));
      addDocument(bsonDocument);
    }
    this.searchIndexWithPartitions.getReader().refresh();

    // Execute a concurrent query and get results
    List<BsonDocument> results =
        queryWithDefinition(this.searchIndexWithPartitions, QUERY_DEFINITION);

    // Verify the executor was used (execute is called by TaskExecutor for parallel execution)
    verify(this.concurrentSearchExecutor, atLeast(1)).execute(any());

    // Verify all documents were returned
    assertEquals(numDocs, results.size());

    // Verify each result has expected structure (document ID and search score)
    for (BsonDocument result : results) {
      assertTrue("Result should contain _id", result.containsKey("_id"));
      assertTrue("Result should contain $searchScore", result.containsKey("$searchScore"));
      assertEquals(
          "Score should be 1.0 (constant score)",
          1.0,
          result.getDouble("$searchScore").getValue(),
          0.001);
    }
  }

  @Test
  public void testNonConcurrentQueryDoesNotUseExecutor() throws Exception {
    // Add some documents to ensure there are results
    int numDocs = 5;
    for (int i = 0; i < numDocs; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'test_%d'}", i, i));
      addDocument(bsonDocument);
    }
    this.searchIndexWithPartitions.getReader().refresh();

    // Execute a non-concurrent query and get results
    List<BsonDocument> results =
        queryWithDefinition(this.searchIndexWithPartitions, NON_CONCURRENT_QUERY_DEFINITION);

    // Verify the executor was NOT used for parallel partition execution
    verify(this.concurrentSearchExecutor, never()).execute(any());

    // Verify all documents were returned
    assertEquals(numDocs, results.size());

    // Verify each result has expected structure
    for (BsonDocument result : results) {
      assertTrue("Result should contain _id", result.containsKey("_id"));
      assertTrue("Result should contain $searchScore", result.containsKey("$searchScore"));
    }
  }

  @Test
  public void testNonConcurrentQueryReturnsCorrectResults() throws Exception {
    // Add some documents
    int numDocs = 10;
    for (int i = 0; i < numDocs; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d'}", i, i));
      addDocument(bsonDocument);
    }
    this.searchIndexWithPartitions.getReader().refresh();

    // Query with concurrent=false and collect all results
    List<BsonDocument> results =
        queryWithDefinition(this.searchIndexWithPartitions, NON_CONCURRENT_QUERY_DEFINITION);

    // Verify correct number of results
    assertEquals(numDocs, results.size());

    // Verify all document IDs are present (0 through numDocs-1)
    Set<Integer> returnedIds =
        results.stream()
            .map(doc -> doc.getInt32("_id").getValue())
            .collect(Collectors.toSet());
    for (int i = 0; i < numDocs; i++) {
      assertTrue("Result should contain document with _id " + i, returnedIds.contains(i));
    }
  }

  @Test
  public void testConcurrentAndNonConcurrentQueryReturnSameResults() throws Exception {
    // Add documents
    int numDocs = 10;
    for (int i = 0; i < numDocs; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d'}", i, i));
      addDocument(bsonDocument);
    }
    this.searchIndexWithPartitions.getReader().refresh();

    // Query with concurrent=true
    List<BsonDocument> concurrentResults =
        queryWithDefinition(this.searchIndexWithPartitions, QUERY_DEFINITION);

    // Query with concurrent=false
    List<BsonDocument> sequentialResults =
        queryWithDefinition(this.searchIndexWithPartitions, NON_CONCURRENT_QUERY_DEFINITION);

    // Both should return the same number of documents
    assertEquals(concurrentResults.size(), sequentialResults.size());
    assertEquals(numDocs, concurrentResults.size());

    // Extract and compare document IDs from both result sets
    Set<Integer> concurrentIds =
        concurrentResults.stream()
            .map(doc -> doc.getInt32("_id").getValue())
            .collect(Collectors.toSet());
    Set<Integer> sequentialIds =
        sequentialResults.stream()
            .map(doc -> doc.getInt32("_id").getValue())
            .collect(Collectors.toSet());

    // Both should contain the same document IDs
    assertEquals(
        "Concurrent and sequential queries should return the same document IDs",
        concurrentIds,
        sequentialIds);

    // Verify all expected IDs are present
    for (int i = 0; i < numDocs; i++) {
      assertTrue("Both results should contain document with _id " + i, concurrentIds.contains(i));
    }
  }

  @Test
  public void testConcurrentQueryWithFeatureFlagDisabledDoesNotHaveTaskExecutor() throws Exception {
    // Create a new executor
    NamedExecutorService testExecutor =
        Executors.fixedSizeThreadScheduledExecutor(
            "testFeatureFlagDisabledExecutor", 1, new SimpleMeterRegistry());

    // Create an index with the CONCURRENT_INDEX_PARTITION_SEARCH feature flag disabled
    InitializedSearchIndex indexWithFlagDisabled =
        createSearchIndex(
            MOCK_DYNAMIC_MULTI_FACET_INDEX_PARTITION_DEFINITION_GENERATION,
            testExecutor,
            FEATURE_FLAGS_CONCURRENT_DISABLED);

    try {
      // Verify the MultiReader does NOT have a TaskExecutor when feature flag is disabled
      @Var SearchIndexReader reader = indexWithFlagDisabled.getReader();
      if (reader instanceof MeteredSearchIndexReader meteredReader) {
        reader = meteredReader.unwrap();
      }
      assertTrue(
          "Reader should be MultiLuceneSearchIndexReader for partitioned index",
          reader instanceof MultiLuceneSearchIndexReader);
      MultiLuceneSearchIndexReader multiReader = (MultiLuceneSearchIndexReader) reader;

      // Use reflection to verify the taskExecutor field is empty
      var taskExecutorField = MultiLuceneSearchIndexReader.class.getDeclaredField("taskExecutor");
      taskExecutorField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Optional<Object> taskExecutor = (Optional<Object>) taskExecutorField.get(multiReader);
      assertTrue(
          "TaskExecutor should be empty when feature flag is disabled", taskExecutor.isEmpty());

      // Add some documents and verify queries still work (sequential execution)
      int numDocs = 5;
      for (int i = 0; i < numDocs; i++) {
        BsonDocument bsonDocument =
            BsonDocument.parse(String.format("{_id: %d, field1: 'test_%d'}", i, i));
        addDocumentToIndex(bsonDocument, indexWithFlagDisabled);
      }
      indexWithFlagDisabled.getReader().refresh();

      // Execute a query with concurrent=true
      List<BsonDocument> results = queryWithDefinition(indexWithFlagDisabled, QUERY_DEFINITION);

      // Verify all documents were still returned (sequential execution works)
      assertEquals(numDocs, results.size());
      for (BsonDocument result : results) {
        assertTrue("Result should contain _id", result.containsKey("_id"));
        assertTrue("Result should contain $searchScore", result.containsKey("$searchScore"));
      }
    } finally {
      indexWithFlagDisabled.close();
      testExecutor.shutdown();
    }
  }

  @Test
  public void testConcurrentQueryWithFeatureFlagEnabledHasTaskExecutor() throws Exception {
    // Create a new executor
    NamedExecutorService testExecutor =
        Executors.fixedSizeThreadScheduledExecutor(
            "testFeatureFlagEnabledExecutor", 1, new SimpleMeterRegistry());

    // Create an index with the CONCURRENT_INDEX_PARTITION_SEARCH feature flag enabled
    InitializedSearchIndex indexWithFlagEnabled =
        createSearchIndex(
            MOCK_DYNAMIC_MULTI_FACET_INDEX_PARTITION_DEFINITION_GENERATION,
            testExecutor,
            FEATURE_FLAGS);

    try {
      // Verify the MultiReader HAS a TaskExecutor when feature flag is enabled
      @Var SearchIndexReader reader = indexWithFlagEnabled.getReader();
      if (reader instanceof MeteredSearchIndexReader meteredReader) {
        reader = meteredReader.unwrap();
      }
      assertTrue(
          "Reader should be MultiLuceneSearchIndexReader for partitioned index",
          reader instanceof MultiLuceneSearchIndexReader);
      MultiLuceneSearchIndexReader multiReader = (MultiLuceneSearchIndexReader) reader;

      // Use reflection to verify the taskExecutor field is present
      var taskExecutorField = MultiLuceneSearchIndexReader.class.getDeclaredField("taskExecutor");
      taskExecutorField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Optional<Object> taskExecutor = (Optional<Object>) taskExecutorField.get(multiReader);
      assertTrue(
          "TaskExecutor should be present when feature flag is enabled", taskExecutor.isPresent());

      // Add some documents and verify queries work
      int numDocs = 5;
      for (int i = 0; i < numDocs; i++) {
        BsonDocument bsonDocument =
            BsonDocument.parse(String.format("{_id: %d, field1: 'test_%d'}", i, i));
        addDocumentToIndex(bsonDocument, indexWithFlagEnabled);
      }
      indexWithFlagEnabled.getReader().refresh();

      // Execute a query with concurrent=true
      List<BsonDocument> results = queryWithDefinition(indexWithFlagEnabled, QUERY_DEFINITION);

      // Verify all documents were returned
      assertEquals(numDocs, results.size());
      for (BsonDocument result : results) {
        assertTrue("Result should contain _id", result.containsKey("_id"));
        assertTrue("Result should contain $searchScore", result.containsKey("$searchScore"));
      }
    } finally {
      indexWithFlagEnabled.close();
      testExecutor.shutdown();
    }
  }

  /** Adds a document to a specific index. */
  private void addDocumentToIndex(BsonDocument bsonDocument, InitializedSearchIndex index)
      throws IOException, FieldExceededLimitsException {
    ObjectId indexId = new ObjectId();
    bsonDocument.append(indexId.toString(), new BsonDocument("_id", bsonDocument.get("_id")));
    RawBsonDocument document = BsonUtils.documentToRaw(bsonDocument);
    var docEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
    index.getWriter().updateIndex(docEvent);
  }

  /**
   * Helper method to execute a query with a specific query definition and return all results.
   */
  private List<BsonDocument> queryWithDefinition(
      InitializedSearchIndex index, OperatorQuery queryDefinition)
      throws IOException, InvalidQueryException, InterruptedException, ReaderClosedException {
    index.getReader().refresh();
    BatchProducer searchProducer =
        index
            .getReader()
            .query(
                queryDefinition,
                QueryCursorOptions.empty(),
                BatchSizeStrategySelector.forQuery(queryDefinition, QueryCursorOptions.empty()),
                QueryOptimizationFlags.DEFAULT_OPTIONS)
            .searchBatchProducer;
    Check.argNotNull(searchProducer, "searchProducer");

    int docsRequested = 100;
    var cursorOptions = BatchCursorOptionsBuilder.builder().docsRequested(docsRequested).build();
    Bytes sizeLimit = CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT;
    List<BsonDocument> results = new ArrayList<>();

    while (!searchProducer.isExhausted()) {
      searchProducer.execute(sizeLimit, cursorOptions);
      BsonArray array = searchProducer.getNextBatch(sizeLimit);
      for (var value : array.getValues()) {
        results.add(value.asDocument());
      }
    }
    searchProducer.close();
    return results;
  }

  private enum QueryType {
    INTERMEDIATE,
    NON_INTERMEDIATE,
  }
  
  @Test
  public void testConcurrentFacetQueryUsesExecutor() throws Exception {
    // Add some documents with facet values to ensure there are results
    @Var int id = 0;
    for (int i = 0; i < 100; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'foo'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }
    for (int i = 0; i < 100; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'bar'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }
    this.searchIndexWithPartitions.getReader().refresh();

    // Execute a concurrent facet query and get meta results
    MetaResults metaResults =
        executeQueryAndGetMetaResults(
            this.searchIndexWithPartitions, QueryType.NON_INTERMEDIATE, CONCURRENT_FACET_QUERY_DEF);

    // Verify the executor was used (execute is called by TaskExecutor for parallel execution)
    verify(this.concurrentSearchExecutor, atLeast(1)).execute(any());

    // Verify facet results are present and correct
    assertTrue("Meta results should have facet info", metaResults.facet().isPresent());
    Map<String, FacetInfo> facets = metaResults.facet().get();
    assertEquals(1, facets.size());
    assertTrue("Should have 'facet' key", facets.containsKey("facet"));

    List<FacetBucket> buckets = facets.get("facet").buckets();
    assertEquals(2, buckets.size());
    assertStringBucket(buckets, 0, "bar", 100L);
    assertStringBucket(buckets, 1, "foo", 100L);
  }

  @Test
  public void testNonConcurrentFacetQueryDoesNotUseExecutor() throws Exception {
    // Add some documents with facet values
    @Var int id = 0;
    for (int i = 0; i < 100; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'foo'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }
    for (int i = 0; i < 100; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'bar'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }
    this.searchIndexWithPartitions.getReader().refresh();

    // Execute a non-concurrent facet query and get meta results
    MetaResults metaResults =
        executeQueryAndGetMetaResults(
            this.searchIndexWithPartitions,
            QueryType.NON_INTERMEDIATE,
            NON_CONCURRENT_FACET_QUERY_DEF);

    // Verify the executor was NOT used for parallel partition execution
    verify(this.concurrentSearchExecutor, never()).execute(any());

    // Verify facet results are present and correct
    assertTrue("Meta results should have facet info", metaResults.facet().isPresent());
    Map<String, FacetInfo> facets = metaResults.facet().get();
    assertEquals(1, facets.size());

    List<FacetBucket> buckets = facets.get("facet").buckets();
    assertEquals(2, buckets.size());
    assertStringBucket(buckets, 0, "bar", 100L);
    assertStringBucket(buckets, 1, "foo", 100L);
  }

  @Test
  public void testConcurrentAndNonConcurrentFacetQueryReturnSameResults() throws Exception {
    // Add documents with multiple facet values
    @Var int id = 0;
    for (int i = 0; i < 500; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'foo'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }
    for (int i = 0; i < 300; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'bar'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }
    for (int i = 0; i < 200; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'baz'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }
    this.searchIndexWithPartitions.getReader().refresh();

    // Query with concurrent=true
    MetaResults concurrentResults =
        executeQueryAndGetMetaResults(
            this.searchIndexWithPartitions, QueryType.NON_INTERMEDIATE, CONCURRENT_FACET_QUERY_DEF);

    // Query with concurrent=false
    MetaResults sequentialResults =
        executeQueryAndGetMetaResults(
            this.searchIndexWithPartitions,
            QueryType.NON_INTERMEDIATE,
            NON_CONCURRENT_FACET_QUERY_DEF);

    // Both should return the same count
    assertEquals(
        concurrentResults.count().getTotalOrLower(),
        sequentialResults.count().getTotalOrLower());
    assertEquals(1000L, concurrentResults.count().getTotalOrLower().longValue());

    // Both should return the same facet buckets
    assertTrue(concurrentResults.facet().isPresent());
    assertTrue(sequentialResults.facet().isPresent());

    Map<String, FacetInfo> concurrentFacets = concurrentResults.facet().get();
    Map<String, FacetInfo> sequentialFacets = sequentialResults.facet().get();

    assertEquals(concurrentFacets.size(), sequentialFacets.size());

    List<FacetBucket> concurrentBuckets = concurrentFacets.get("facet").buckets();
    List<FacetBucket> sequentialBuckets = sequentialFacets.get("facet").buckets();

    assertEquals(concurrentBuckets.size(), sequentialBuckets.size());
    assertEquals(3, concurrentBuckets.size());

    // Verify same bucket values and counts (buckets should be sorted by count descending)
    for (int i = 0; i < concurrentBuckets.size(); i++) {
      assertEquals(
          "Bucket IDs should match",
          concurrentBuckets.get(i).getId(),
          sequentialBuckets.get(i).getId());
      assertEquals(
          "Bucket counts should match",
          concurrentBuckets.get(i).getCount(),
          sequentialBuckets.get(i).getCount());
    }

    // Verify expected bucket values
    assertStringBucket(concurrentBuckets, 0, "foo", 500L);
    assertStringBucket(concurrentBuckets, 1, "bar", 300L);
    assertStringBucket(concurrentBuckets, 2, "baz", 200L);
  }

  @Test
  public void testConcurrentFacetIntermediateQueryUsesExecutor() throws Exception {
    // Add some documents with facet values
    @Var int id = 0;
    for (int i = 0; i < 100; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'foo'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }
    for (int i = 0; i < 100; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'bar'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }
    this.searchIndexWithPartitions.getReader().refresh();

    // Execute a concurrent facet query using intermediate query path
    MetaResults metaResults =
        executeQueryAndGetMetaResults(
            this.searchIndexWithPartitions, QueryType.INTERMEDIATE, CONCURRENT_FACET_QUERY_DEF);

    // Verify the executor was used
    verify(this.concurrentSearchExecutor, atLeast(1)).execute(any());

    // Verify facet results are present and correct
    assertTrue("Meta results should have facet info", metaResults.facet().isPresent());
    Map<String, FacetInfo> facets = metaResults.facet().get();
    assertEquals(1, facets.size());

    List<FacetBucket> buckets = facets.get("facet").buckets();
    assertEquals(2, buckets.size());
  }

  @Test
  public void testNonConcurrentFacetIntermediateQueryDoesNotUseExecutor() throws Exception {
    // Add some documents with facet values
    @Var int id = 0;
    for (int i = 0; i < 100; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'foo'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }
    for (int i = 0; i < 100; i++) {
      BsonDocument bsonDocument =
          BsonDocument.parse(String.format("{_id: %d, field1: 'mongo_%d', f: 'bar'}", id, id));
      addDocument(bsonDocument);
      id += 1;
    }
    this.searchIndexWithPartitions.getReader().refresh();

    // Execute a non-concurrent facet query using intermediate query path
    MetaResults metaResults =
        executeQueryAndGetMetaResults(
            this.searchIndexWithPartitions, QueryType.INTERMEDIATE, NON_CONCURRENT_FACET_QUERY_DEF);

    // Verify the executor was NOT used
    verify(this.concurrentSearchExecutor, never()).execute(any());

    // Verify facet results are present and correct
    assertTrue("Meta results should have facet info", metaResults.facet().isPresent());
    Map<String, FacetInfo> facets = metaResults.facet().get();
    assertEquals(1, facets.size());

    List<FacetBucket> buckets = facets.get("facet").buckets();
    assertEquals(2, buckets.size());
  }

  // ============================================================================
  // Exception Handling Tests for Concurrent Partition Execution
  // ============================================================================

  /**
   * Tests that IOException thrown by a partition reader is correctly propagated through the
   * concurrent query path without being wrapped or modified.
   */
  @Test
  public void testConcurrentQueryPropagatesIoException() throws Exception {
    IOException originalException = new IOException("Test IO exception from partition");
    LuceneSearchIndexReader mockReader1 = mock(LuceneSearchIndexReader.class);
    LuceneSearchIndexReader mockReader2 = mock(LuceneSearchIndexReader.class);

    // First reader throws IOException
    Mockito.when(mockReader1.query(any(), any(), any(), any())).thenThrow(originalException);
    // Second reader would succeed but exception should propagate first
    Mockito.when(mockReader2.query(any(), any(), any(), any()))
        .thenReturn(
            new SearchProducerAndMetaResults(mock(BatchProducer.class), MetaResults.EMPTY));

    MultiLuceneSearchIndexReader multiReader =
        new MultiLuceneSearchIndexReader(
            List.of(mockReader1, mockReader2),
            drillSidewaysEnabledRegistry(),
            Optional.of(this.concurrentSearchExecutor));

    BatchSizeStrategy batchSizeStrategy =
        BatchSizeStrategySelector.forQuery(QUERY_DEFINITION, QueryCursorOptions.empty());

    IOException thrown =
        Assert.assertThrows(
            IOException.class,
            () ->
                multiReader.query(
                    QUERY_DEFINITION,
                    QueryCursorOptions.empty(),
                    batchSizeStrategy,
                    QueryOptimizationFlags.DEFAULT_OPTIONS));

    Assert.assertSame(originalException, thrown);
  }

  /**
   * Tests that InvalidQueryException thrown by a partition reader is correctly propagated through
   * the concurrent query path without being wrapped or modified.
   */
  @Test
  public void testConcurrentQueryPropagatesInvalidQueryException() throws Exception {
    InvalidQueryException originalException = new InvalidQueryException("Test invalid query");
    LuceneSearchIndexReader mockReader1 = mock(LuceneSearchIndexReader.class);
    LuceneSearchIndexReader mockReader2 = mock(LuceneSearchIndexReader.class);

    Mockito.when(mockReader1.query(any(), any(), any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.query(any(), any(), any(), any()))
        .thenReturn(
            new SearchProducerAndMetaResults(mock(BatchProducer.class), MetaResults.EMPTY));

    MultiLuceneSearchIndexReader multiReader =
        new MultiLuceneSearchIndexReader(
            List.of(mockReader1, mockReader2),
            drillSidewaysEnabledRegistry(),
            Optional.of(this.concurrentSearchExecutor));

    BatchSizeStrategy batchSizeStrategy =
        BatchSizeStrategySelector.forQuery(QUERY_DEFINITION, QueryCursorOptions.empty());

    InvalidQueryException thrown =
        Assert.assertThrows(
            InvalidQueryException.class,
            () ->
                multiReader.query(
                    QUERY_DEFINITION,
                    QueryCursorOptions.empty(),
                    batchSizeStrategy,
                    QueryOptimizationFlags.DEFAULT_OPTIONS));

    Assert.assertSame(originalException, thrown);
  }

  /**
   * Tests that InterruptedException thrown by a partition reader is correctly propagated through
   * the concurrent query path without being wrapped or modified.
   */
  @Test
  public void testConcurrentQueryPropagatesInterruptedException() throws Exception {
    InterruptedException originalException = new InterruptedException("Test interrupted");
    LuceneSearchIndexReader mockReader1 = mock(LuceneSearchIndexReader.class);
    LuceneSearchIndexReader mockReader2 = mock(LuceneSearchIndexReader.class);

    Mockito.when(mockReader1.query(any(), any(), any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.query(any(), any(), any(), any()))
        .thenReturn(
            new SearchProducerAndMetaResults(mock(BatchProducer.class), MetaResults.EMPTY));

    MultiLuceneSearchIndexReader multiReader =
        new MultiLuceneSearchIndexReader(
            List.of(mockReader1, mockReader2),
            drillSidewaysEnabledRegistry(),
            Optional.of(this.concurrentSearchExecutor));

    BatchSizeStrategy batchSizeStrategy =
        BatchSizeStrategySelector.forQuery(QUERY_DEFINITION, QueryCursorOptions.empty());

    InterruptedException thrown =
        Assert.assertThrows(
            InterruptedException.class,
            () ->
                multiReader.query(
                    QUERY_DEFINITION,
                    QueryCursorOptions.empty(),
                    batchSizeStrategy,
                    QueryOptimizationFlags.DEFAULT_OPTIONS));

    Assert.assertSame(originalException, thrown);
  }

  /**
   * Tests that when no partition reader throws an exception, the concurrent query completes
   * successfully.
   */
  @Test
  public void testConcurrentQuerySucceedsWithNoExceptions() throws Exception {
    LuceneSearchIndexReader mockReader1 = mock(LuceneSearchIndexReader.class);
    LuceneSearchIndexReader mockReader2 = mock(LuceneSearchIndexReader.class);

    SearchProducerAndMetaResults mockResult =
        new SearchProducerAndMetaResults(mock(BatchProducer.class), MetaResults.EMPTY);
    Mockito.when(mockReader1.query(any(), any(), any(), any())).thenReturn(mockResult);
    Mockito.when(mockReader2.query(any(), any(), any(), any())).thenReturn(mockResult);

    MultiLuceneSearchIndexReader multiReader =
        new MultiLuceneSearchIndexReader(
            List.of(mockReader1, mockReader2),
            drillSidewaysEnabledRegistry(),
            Optional.of(this.concurrentSearchExecutor));

    // Should not throw any exception
    multiReader.query(
        QUERY_DEFINITION,
        QueryCursorOptions.empty(),
        BatchSizeStrategySelector.forQuery(QUERY_DEFINITION, QueryCursorOptions.empty()),
        QueryOptimizationFlags.DEFAULT_OPTIONS);

    // Verify both readers were queried
    verify(mockReader1).query(any(), any(), any(), any());
    verify(mockReader2).query(any(), any(), any(), any());
  }

  /**
   * Tests the behavior when a partition reader throws a RuntimeException during concurrent query.
   *
   * <p>The current implementation catches all exceptions in the Callable and rethrows any
   * non-checked exception as RuntimeException from executeOnAllPartitions().
   */
  @Test
  public void testConcurrentQueryWithRuntimeException() throws Exception {
    RuntimeException originalException = new RuntimeException("Test runtime exception");
    LuceneSearchIndexReader mockReader1 = mock(LuceneSearchIndexReader.class);
    LuceneSearchIndexReader mockReader2 = mock(LuceneSearchIndexReader.class);

    Mockito.when(mockReader1.query(any(), any(), any(), any())).thenThrow(originalException);
    SearchProducerAndMetaResults mockResult =
        new SearchProducerAndMetaResults(mock(BatchProducer.class), MetaResults.EMPTY);
    Mockito.when(mockReader2.query(any(), any(), any(), any())).thenReturn(mockResult);

    MultiLuceneSearchIndexReader multiReader =
        new MultiLuceneSearchIndexReader(
            List.of(mockReader1, mockReader2),
            drillSidewaysEnabledRegistry(),
            Optional.of(this.concurrentSearchExecutor));

    RuntimeException thrown =
        Assert.assertThrows(
            RuntimeException.class,
            () ->
                multiReader.query(
                    QUERY_DEFINITION,
                    QueryCursorOptions.empty(),
                    BatchSizeStrategySelector.forQuery(
                        QUERY_DEFINITION, QueryCursorOptions.empty()),
                    QueryOptimizationFlags.DEFAULT_OPTIONS));
    Assert.assertSame(originalException, thrown.getCause());
  }

  // ============================================================================
  // Exception Handling Tests for Sequential Partition Execution
  // ============================================================================

  /**
   * Tests that IOException thrown by a partition reader is correctly propagated through the
   * sequential query path (when no executor is provided).
   */
  @Test
  public void testSequentialQueryPropagatesIoException() throws Exception {
    IOException originalException = new IOException("Test IO exception from partition");
    LuceneSearchIndexReader mockReader1 = mock(LuceneSearchIndexReader.class);
    LuceneSearchIndexReader mockReader2 = mock(LuceneSearchIndexReader.class);

    Mockito.when(mockReader1.query(any(), any(), any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.query(any(), any(), any(), any()))
        .thenReturn(
            new SearchProducerAndMetaResults(mock(BatchProducer.class), MetaResults.EMPTY));

    // Pass Optional.empty() for executor to get sequential processing
    MultiLuceneSearchIndexReader multiReader =
        new MultiLuceneSearchIndexReader(
            List.of(mockReader1, mockReader2), drillSidewaysEnabledRegistry(), Optional.empty());

    BatchSizeStrategy batchSizeStrategy =
        BatchSizeStrategySelector.forQuery(QUERY_DEFINITION, QueryCursorOptions.empty());

    IOException thrown =
        Assert.assertThrows(
            IOException.class,
            () ->
                multiReader.query(
                    QUERY_DEFINITION,
                    QueryCursorOptions.empty(),
                    batchSizeStrategy,
                    QueryOptimizationFlags.DEFAULT_OPTIONS));

    Assert.assertSame(originalException, thrown);
  }

  /**
   * Tests that InvalidQueryException thrown by a partition reader is correctly propagated through
   * the sequential query path (when no executor is provided).
   */
  @Test
  public void testSequentialQueryPropagatesInvalidQueryException() throws Exception {
    InvalidQueryException originalException = new InvalidQueryException("Test invalid query");
    LuceneSearchIndexReader mockReader1 = mock(LuceneSearchIndexReader.class);
    LuceneSearchIndexReader mockReader2 = mock(LuceneSearchIndexReader.class);

    Mockito.when(mockReader1.query(any(), any(), any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.query(any(), any(), any(), any()))
        .thenReturn(
            new SearchProducerAndMetaResults(mock(BatchProducer.class), MetaResults.EMPTY));

    MultiLuceneSearchIndexReader multiReader =
        new MultiLuceneSearchIndexReader(
            List.of(mockReader1, mockReader2), drillSidewaysEnabledRegistry(), Optional.empty());

    BatchSizeStrategy batchSizeStrategy =
        BatchSizeStrategySelector.forQuery(QUERY_DEFINITION, QueryCursorOptions.empty());

    InvalidQueryException thrown =
        Assert.assertThrows(
            InvalidQueryException.class,
            () ->
                multiReader.query(
                    QUERY_DEFINITION,
                    QueryCursorOptions.empty(),
                    batchSizeStrategy,
                    QueryOptimizationFlags.DEFAULT_OPTIONS));

    Assert.assertSame(originalException, thrown);
  }

  /**
   * Tests that InterruptedException thrown by a partition reader is correctly propagated through
   * the sequential query path (when no executor is provided).
   */
  @Test
  public void testSequentialQueryPropagatesInterruptedException() throws Exception {
    InterruptedException originalException = new InterruptedException("Test interrupted");
    LuceneSearchIndexReader mockReader1 = mock(LuceneSearchIndexReader.class);
    LuceneSearchIndexReader mockReader2 = mock(LuceneSearchIndexReader.class);

    Mockito.when(mockReader1.query(any(), any(), any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.query(any(), any(), any(), any()))
        .thenReturn(
            new SearchProducerAndMetaResults(mock(BatchProducer.class), MetaResults.EMPTY));

    MultiLuceneSearchIndexReader multiReader =
        new MultiLuceneSearchIndexReader(
            List.of(mockReader1, mockReader2), drillSidewaysEnabledRegistry(), Optional.empty());

    BatchSizeStrategy batchSizeStrategy =
        BatchSizeStrategySelector.forQuery(QUERY_DEFINITION, QueryCursorOptions.empty());

    InterruptedException thrown =
        Assert.assertThrows(
            InterruptedException.class,
            () ->
                multiReader.query(
                    QUERY_DEFINITION,
                    QueryCursorOptions.empty(),
                    batchSizeStrategy,
                    QueryOptimizationFlags.DEFAULT_OPTIONS));

    Assert.assertSame(originalException, thrown);
  }

  /**
   * Tests that RuntimeException thrown by a partition reader is correctly propagated through the
   * sequential query path. Unlike the concurrent path where RuntimeException is swallowed, the
   * sequential path propagates it directly since there's no catch block in the simple for loop.
   */
  @Test
  public void testSequentialQueryPropagatesRuntimeException() throws Exception {
    RuntimeException originalException = new RuntimeException("Test runtime exception");
    LuceneSearchIndexReader mockReader1 = mock(LuceneSearchIndexReader.class);
    LuceneSearchIndexReader mockReader2 = mock(LuceneSearchIndexReader.class);

    Mockito.when(mockReader1.query(any(), any(), any(), any())).thenThrow(originalException);
    Mockito.when(mockReader2.query(any(), any(), any(), any()))
        .thenReturn(
            new SearchProducerAndMetaResults(mock(BatchProducer.class), MetaResults.EMPTY));

    MultiLuceneSearchIndexReader multiReader =
        new MultiLuceneSearchIndexReader(
            List.of(mockReader1, mockReader2), drillSidewaysEnabledRegistry(), Optional.empty());

    BatchSizeStrategy batchSizeStrategy =
        BatchSizeStrategySelector.forQuery(QUERY_DEFINITION, QueryCursorOptions.empty());

    RuntimeException thrown =
        Assert.assertThrows(
            RuntimeException.class,
            () ->
                multiReader.query(
                    QUERY_DEFINITION,
                    QueryCursorOptions.empty(),
                    batchSizeStrategy,
                    QueryOptimizationFlags.DEFAULT_OPTIONS));

    Assert.assertSame(originalException, thrown);
  }

}
