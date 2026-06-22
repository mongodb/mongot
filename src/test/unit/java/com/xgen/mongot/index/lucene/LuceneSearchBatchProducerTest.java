package com.xgen.mongot.index.lucene;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.truth.Truth;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.batch.AdjustableBatchSizeStrategy;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.ConstantBatchSizeStrategy;
import com.xgen.mongot.cursor.batch.ExponentiallyIncreasingBatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.ResultFactory;
import com.xgen.mongot.index.SearchResult;
import com.xgen.mongot.index.VectorSearchResult;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.lucene.LuceneSearchManager.QueryInfo;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.pushdown.project.ProjectFactory;
import com.xgen.mongot.index.lucene.query.pushdown.project.ProjectSpec;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.testing.fakes.FakeStoredFields;
import com.xgen.testing.mongot.cursor.batch.BatchCursorOptionsBuilder;
import com.xgen.testing.mongot.cursor.batch.QueryCursorOptionsBuilder;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

@RunWith(Theories.class)
public class LuceneSearchBatchProducerTest {

  @DataPoints
  public static ResultFactory[] testData = {SearchResult::new, VectorSearchResult::create};

  @Theory
  public void testPartialBatch(ResultFactory resultFactory) throws Exception {
    int numDocs = 3;
    var producer =
        createProducer(
            new MockSearchManager(numDocs),
            new ConstantBatchSizeStrategy(Integer.MAX_VALUE),
            resultFactory);

    assertCorrectCounts(producer, numDocs, 1);
  }

  @Theory
  public void testMultipleBatches(ResultFactory resultFactory) throws Exception {
    int numDocs = 3;
    var producer =
        createProducer(
            new MockSearchManager(numDocs), new ConstantBatchSizeStrategy(2), resultFactory);

    assertCorrectCounts(producer, numDocs, 2);
  }

  @Theory
  public void testFirstBatchGetsAllDocsAndSecondBatchIsEmpty(ResultFactory resultFactory)
      throws Exception {
    int numDocs = 3;
    var producer =
        createProducer(
            new MockSearchManager(numDocs), new ConstantBatchSizeStrategy(3), resultFactory);

    @Var int docCount = 0;
    @Var int batchCount = 0;
    List<Integer> docsInBatches = new ArrayList<>();
    while (!producer.isExhausted()) {
      var batch = executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);
      docsInBatches.add(batch.size());
      docCount += batch.size();
      batchCount++;
    }
    // After exhausted, calling an extra getNextBatch() will return empty.
    assertEmptyNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);
    Assert.assertEquals(numDocs, docCount);
    Assert.assertEquals(2, batchCount);
    Integer[] expectedDocsInBatches = {3, 0};
    Assert.assertEquals(Arrays.asList(expectedDocsInBatches), docsInBatches);
  }

  @Theory
  public void testExplain(ResultFactory resultFactory) throws Exception {
    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.EXECUTION_STATS),
            Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {

      int numDocs = 3;
      var producer =
          createProducer(
              new MockSearchManager(numDocs), new ConstantBatchSizeStrategy(3), resultFactory);

      executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

      var resultMaterializationStats =
          Explain.collect().get().resultMaterialization().get().stats();
      Truth.assertThat(resultMaterializationStats.invocationCounts().get()).hasSize(1);
      Truth.assertThat(resultMaterializationStats.invocationCounts().get())
          .containsKey(ExplainTimings.Type.RETRIEVE_AND_SERIALIZE.getName());
    }
  }

  @Theory
  public void testZeroHit(ResultFactory resultFactory) throws Exception {
    int numDocs = 0;
    var producer =
        createProducer(
            new MockSearchManager(numDocs), new ConstantBatchSizeStrategy(3), resultFactory);

    List<Integer> docsInBatches = new ArrayList<>();
    while (!producer.isExhausted()) {
      var batch = executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);
      docsInBatches.add(batch.size());
    }
    // After exhausted, calling an extra getNextBatch() will return empty.
    assertEmptyNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);
    Assert.assertTrue(docsInBatches.isEmpty());
  }

  @Theory
  public void testPartialBatchWithIterator(ResultFactory resultFactory) throws Exception {
    int numDocs = 3;
    var producer =
        createProducer(
            new MockSearchManager(numDocs),
            new ConstantBatchSizeStrategy(Integer.MAX_VALUE),
            resultFactory);

    assertCorrectCountsWithIterator(producer, numDocs, 1);
  }

  @Theory
  public void testMultipleBatchesWithIterator(ResultFactory resultFactory) throws Exception {
    int numDocs = 3;
    var producer =
        createProducer(
            new MockSearchManager(numDocs), new ConstantBatchSizeStrategy(2), resultFactory);

    assertCorrectCountsWithIterator(producer, numDocs, 2);
  }

  @Theory
  public void testResultsLimitedByBatchSize(ResultFactory resultFactory) throws Exception {
    for (int byteSizeLimit = 101; byteSizeLimit < 2000; byteSizeLimit += 100) {
      var producer =
          createProducer(
              new MockSearchManager(50000), new ConstantBatchSizeStrategy(2), resultFactory);

      BsonArray batch = executeAndGetNextBatch(producer, Bytes.ofBytes(byteSizeLimit));

      // check that results are limited by the batch size
      Assert.assertTrue(BsonUtils.bsonValueSerializedBytes(batch).toBytes() < byteSizeLimit);

      // check that the cursor is not considered exhausted because we were limited by batch size
      Assert.assertFalse(
          executeAndGetNextBatch(producer, Bytes.ofBytes(byteSizeLimit)).getValues().isEmpty());
    }
  }

  @Theory
  public void testCursorExhaust(ResultFactory resultFactory) throws Exception {
    var producer =
        createProducer(
            new MockSearchManager(1000),
            new ExponentiallyIncreasingBatchSizeStrategy(),
            resultFactory);
    @Var var count = 0;

    while (!producer.isExhausted()) {
      var batch = executeAndGetNextBatch(producer, Bytes.ofMebi(16));
      count += batch.size();
    }
    // After exhausted, calling an extra getNextBatch() will return empty.
    Assert.assertTrue(
        executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT).isEmpty());

    Assert.assertEquals(1000, count);
  }

  @Theory
  public void testDocumentCannotFitInBatch(ResultFactory resultFactory) throws Exception {
    var producer =
        createProducer(new MockSearchManager(1), new ConstantBatchSizeStrategy(1), resultFactory);
    Assert.assertThrows(
        IllegalStateException.class, () -> executeAndGetNextBatch(producer, Bytes.ofBytes(7)));
  }

  @Theory
  public void testEmptyBatchWhenNoResultsFound(ResultFactory resultFactory) throws Exception {
    var producer =
        createProducer(new MockSearchManager(0), new ConstantBatchSizeStrategy(100), resultFactory);
    Assert.assertEquals(0, executeAndGetNextBatch(producer, Bytes.ofMebi(16)).size());
  }

  @Theory
  public void testLuceneTopDocsLimitIsAdjustedBasedOnAvgDocumentSizeAndResultSizeLimit(
      ResultFactory resultFactory) throws IOException, InvalidQueryException {
    var batchByteSizeLimit = Bytes.ofKibi(1);
    var batchDocsSizeLimit = 1000;
    var manager = spy(new MockSearchManager(10_000));
    var producer =
        spy(
            createProducer(
                manager, new ConstantBatchSizeStrategy(batchDocsSizeLimit), resultFactory));

    // batch size should have been restricted by byte size
    var initialBatch = executeAndGetNextBatch(producer, batchByteSizeLimit);
    Assert.assertTrue(initialBatch.size() < batchDocsSizeLimit);

    // but Lucene limit should be equal to the requested by batch size strategy, since there is
    // no doc size statistics on the very first batch
    var initialLimit = ArgumentCaptor.forClass(Integer.class);
    verify(manager).initialSearch(any(), initialLimit.capture());
    Assert.assertEquals(batchDocsSizeLimit, initialLimit.getValue().intValue());

    // call getNextBatch again
    executeAndGetNextBatch(producer, batchByteSizeLimit);

    // all subsequent Lucene calls should be limited based on avg document size from previous
    // batches, so we don't request more than what we can fit into the batch
    var subsequentLimit = ArgumentCaptor.forClass(Integer.class);
    verify(manager).getMoreTopDocs(any(), any(), subsequentLimit.capture());
    Assert.assertTrue(subsequentLimit.getValue() < batchDocsSizeLimit);
  }

  @Theory
  public void testLuceneConstantBatchSizeStrategy(ResultFactory resultFactory)
      throws IOException, InvalidQueryException {
    var batchDocsSizeLimit = 123;
    var producer =
        createProducer(
            new MockSearchManager(10_000),
            new ConstantBatchSizeStrategy(batchDocsSizeLimit),
            resultFactory);

    while (!producer.isExhausted()) {
      var batch = executeAndGetNextBatch(producer, Bytes.ofMebi(16));
      Assert.assertTrue(batchDocsSizeLimit == batch.size() || producer.isExhausted());
    }
    // After exhausted, calling an extra getNextBatch() will return empty.
    assertEmptyNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);
  }

  @Theory
  public void testLuceneIncreasingBatchSizeStrategy(ResultFactory resultFactory)
      throws IOException, InvalidQueryException {
    BatchSizeStrategy batchSizeStrategy = new ExponentiallyIncreasingBatchSizeStrategy();
    BatchCursorOptions cursorOptions = BatchCursorOptionsBuilder.builder().build();
    var producer = createProducer(new MockSearchManager(10_000), batchSizeStrategy, resultFactory);

    @Var int previousBatchSize = 0;

    while (!producer.isExhausted()) {
      var batch = executeAndGetNextBatch(producer, Bytes.ofMebi(16));
      Assert.assertTrue(previousBatchSize < batch.size() || producer.isExhausted());
      previousBatchSize = batch.size();
      batchSizeStrategy.adjust(cursorOptions);
    }
    // After exhausted, calling an extra getNextBatch() will return empty.
    assertEmptyNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);
  }

  @Theory
  public void testLuceneExtractableLimitBatchSizeStrategy(ResultFactory resultFactory)
      throws Exception {
    int docsRequested = 25;
    int docsExpected = 27;
    var producer =
        createProducer(
            new MockSearchManager(10_000),
            AdjustableBatchSizeStrategy.create(
                BatchCursorOptionsBuilder.builder().docsRequested(docsRequested).build(), false),
            resultFactory);

    Assert.assertEquals(
        docsExpected,
        executeAndGetNextBatch(
                producer,
                Bytes.ofMebi(16),
                BatchCursorOptionsBuilder.builder().docsRequested(docsRequested).build())
            .size());
  }

  @Theory
  public void testOversubscriptionFailedMetrics(ResultFactory resultFactory) throws IOException {
    int docsRequested = 25;
    int docsExpected = 27;
    var updater = IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
    var cursorOptions = QueryCursorOptionsBuilder.builder().docsRequested(docsRequested).build();
    LuceneIndexSearcherReference searcherRef = getSearcherRef();
    var manager = new MockSearchManager(10_000);
    var batchSizeStrategy = AdjustableBatchSizeStrategy.create(cursorOptions, false);
    QueryInfo queryInfo =
        manager.initialSearch(searcherRef, batchSizeStrategy.adviseNextBatchSize());
    var producer =
        new LuceneSearchBatchProducer(
            searcherRef,
            manager,
            queryInfo.topDocs,
            queryInfo.luceneExhausted,
            Optional.empty(),
            Optional.empty(),
            batchSizeStrategy,
            ProjectFactory.build(
                new ProjectSpec(false, StoredSourceDefinition.createIncludeAll()),
                searcherRef.getIndexSearcher().getIndexReader()),
            updater,
            cursorOptions,
            false,
            Optional.empty(),
            false,
            resultFactory,
            0,
            false);
    // On the first batch, we register only that we received a limit hint
    executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, cursorOptions);
    Assert.assertEquals(1, updater.getExtractableLimitQueryCounter().count(), 0);
    Assert.assertEquals(0, updater.getExtractableLimitQuerySecondBatchCounter().count(), 0);
    Assert.assertEquals(0, updater.getOrphanedDeletedDocsRatio().count());

    int secondBatchDocsRequested = 15;
    executeAndGetNextBatch(
        producer,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        BatchCursorOptionsBuilder.builder().docsRequested(secondBatchDocsRequested).build());
    Assert.assertEquals(1, updater.getExtractableLimitQueryCounter().count(), 0);
    // After the limit hint, we still need a second batch
    Assert.assertEquals(1, updater.getExtractableLimitQuerySecondBatchCounter().count(), 0);
    Assert.assertEquals(1, updater.getOrphanedDeletedDocsRatio().count());
    // 12 documents orphaned/deleted (oversubscribed by 2 but still need an additional 10) out of 27
    // docs
    Assert.assertEquals(
        (docsExpected - docsRequested + secondBatchDocsRequested) / 27.0,
        updater.getOrphanedDeletedDocsRatio().mean(),
        0.001);

    // no metrics should be logged on further batches
    executeAndGetNextBatch(
        producer,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        BatchCursorOptionsBuilder.builder().docsRequested(10).build());
    Assert.assertEquals(1, updater.getExtractableLimitQueryCounter().count(), 0);
    Assert.assertEquals(1, updater.getExtractableLimitQuerySecondBatchCounter().count(), 0);
    Assert.assertEquals(1, updater.getOrphanedDeletedDocsRatio().count());
  }

  @Test
  public void testBatchWithTiesMetricIsEmptyWhenNoPagination() throws IOException {
    int docsRequested = 25;
    var cursorOptions = BatchCursorOptionsBuilder.builder().docsRequested(docsRequested).build();
    var manager = new ConstantScoreMockSearchManager(10_000, 4);

    var queryCursorOptions = QueryCursorOptions.empty();
    var updater = IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
    var producer =
        batchWithTiesMetricVerificationProducer(
            cursorOptions, queryCursorOptions, manager, updater, Optional.empty());
    executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, cursorOptions);
    Assert.assertEquals(0, updater.getBatchWithTiesCounter().count(), 0);

    executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, cursorOptions);
    Assert.assertEquals(0, updater.getBatchWithTiesCounter().count(), 0);
  }

  @Test
  public void testBatchWithTiesMetricIsEmptyWhenNoTies() throws IOException {
    int docsRequested = 25;
    var cursorOptions = BatchCursorOptionsBuilder.builder().docsRequested(docsRequested).build();
    var manager = new MockSearchManager(10_000);

    var queryCursorOptions = new QueryCursorOptions(Optional.empty(), Optional.empty(), true);
    var updater = IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
    var producer =
        batchWithTiesMetricVerificationProducer(
            cursorOptions, queryCursorOptions, manager, updater, Optional.empty());
    executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, cursorOptions);
    Assert.assertEquals(0, updater.getBatchWithTiesCounter().count(), 0);

    executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, cursorOptions);
    Assert.assertEquals(0, updater.getBatchWithTiesCounter().count(), 0);
  }

  @Test
  public void testBatchWithTiesMetricIsReportedWhenSeqTokenRequested() throws IOException {
    int docsRequested = 25;
    var cursorOptions = BatchCursorOptionsBuilder.builder().docsRequested(docsRequested).build();
    var manager = new ConstantScoreMockSearchManager(10_000, 4);

    var queryCursorOptions = new QueryCursorOptions(Optional.empty(), Optional.empty(), true);

    var updater = IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
    var producer =
        batchWithTiesMetricVerificationProducer(
            cursorOptions, queryCursorOptions, manager, updater, Optional.empty());
    executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, cursorOptions);
    Assert.assertEquals(1, updater.getBatchWithTiesCounter().count(), 0);

    executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, cursorOptions);
    Assert.assertEquals(2, updater.getBatchWithTiesCounter().count(), 0);
  }

  @Test
  public void testBatchWithTiesMetricIsReportedWhenSequenceTokenIsPresent() throws IOException {
    int docsRequested = 25;
    var cursorOptions = BatchCursorOptionsBuilder.builder().docsRequested(docsRequested).build();
    var manager = new ConstantScoreMockSearchManager(10_000, 4);

    var queryCursorOptions = new QueryCursorOptions(Optional.empty(), Optional.empty(), true);
    var seqToken = SequenceToken.of(new BsonString("doesn't matter"), new ScoreDoc(5, 4));

    var updater = IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
    var producer =
        batchWithTiesMetricVerificationProducer(
            cursorOptions, queryCursorOptions, manager, updater, Optional.of(seqToken));
    executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, cursorOptions);
    Assert.assertEquals(1, updater.getBatchWithTiesCounter().count(), 0);

    executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, cursorOptions);
    Assert.assertEquals(2, updater.getBatchWithTiesCounter().count(), 0);
  }

  @Theory
  public void testRepeatedBatchesCounterDoesNotProduceFalsePositives(ResultFactory resultFactory)
      throws IOException, InvalidQueryException {
    int docsRequested = 100;
    var batchDocsSizeLimit = 25;

    var updater = IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
    var producer =
        createProducer(
            new MockSearchManager(docsRequested),
            new ConstantBatchSizeStrategy(batchDocsSizeLimit),
            resultFactory,
            updater);

    while (!producer.isExhausted()) {
      var batch = executeAndGetNextBatch(producer, Bytes.ofMebi(16));
      Assert.assertTrue(batchDocsSizeLimit == batch.size() || producer.isExhausted());
    }
    Assert.assertEquals(0, updater.getNoProgressBatchCounter().count(), 0);
  }

  @Theory
  public void testRepeatedBatchesAreDetectedCounterWorks(ResultFactory resultFactory)
      throws IOException, InvalidQueryException {
    int docsRequested = 100;
    var batchDocsSizeLimit = 25;

    var updater = IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
    var producer =
        createProducer(
            new RepeatedBatchMockSearchManager(docsRequested, 24),
            new ConstantBatchSizeStrategy(batchDocsSizeLimit),
            resultFactory,
            updater);

    @Var int batchCount = 0;
    while (!producer.isExhausted()) {
      var batch = executeAndGetNextBatch(producer, Bytes.ofMebi(16));
      Assert.assertTrue(batchDocsSizeLimit == batch.size() || producer.isExhausted());
      batchCount++;
    }
    Assert.assertEquals(5, batchCount);
    // It is 3 because 1 batch is new, 2, 3, 4 batches are identical and 5 is empty
    Assert.assertEquals(3, updater.getNoProgressBatchCounter().count(), 0);
  }

  @Theory
  public void testNoProgressBatchCounterCounterHandleZeroHitQueries(ResultFactory resultFactory)
      throws Exception {
    int numDocs = 0;
    var updater = IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
    var producer =
        createProducer(
            new MockSearchManager(numDocs),
            new ConstantBatchSizeStrategy(3),
            resultFactory,
            updater);

    while (!producer.isExhausted()) {
      var unused = executeAndGetNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);
    }
    Assert.assertEquals(0, updater.getNoProgressBatchCounter().count(), 0);
  }

  private LuceneSearchBatchProducer batchWithTiesMetricVerificationProducer(
      BatchCursorOptions batchCursorOptions,
      QueryCursorOptions queryCursorOptions,
      MockSearchManager manager,
      IndexMetricsUpdater.QueryingMetricsUpdater updater,
      Optional<SequenceToken> seqToken)
      throws IOException {
    var batchSizeStrategy = AdjustableBatchSizeStrategy.create(batchCursorOptions, false);
    var searcherRef = getSearcherRef();

    QueryInfo queryInfo =
        manager.initialSearch(searcherRef, batchSizeStrategy.adviseNextBatchSize());
    return new LuceneSearchBatchProducer(
        searcherRef,
        manager,
        queryInfo.topDocs,
        queryInfo.luceneExhausted,
        Optional.empty(),
        Optional.empty(),
        batchSizeStrategy,
        ProjectFactory.build(
            new ProjectSpec(false, StoredSourceDefinition.createIncludeAll()),
            searcherRef.getIndexSearcher().getIndexReader()),
        updater,
        queryCursorOptions,
        false,
        seqToken,
        false,
        SearchResult::new,
        0,
        false);
  }

  @Theory
  public void testIterator(ResultFactory resultFactory) throws Exception {
    int docsRequested = 25;
    var producer =
        createProducer(
            new MockSearchManager(10_000),
            new AdjustableBatchSizeStrategy(docsRequested, false),
            resultFactory);
    producer.execute(
        Bytes.ofMebi(16), BatchCursorOptionsBuilder.builder().docsRequested(docsRequested).build());
    var iter = producer.getSearchResultsIter();

    @Var int docsReturned = 0;
    while (iter.peek().isPresent()) {
      docsReturned++;
      iter.acceptAndAdvance();
    }
    Assert.assertEquals(docsRequested, docsReturned);
  }

  @Test
  public void testStoredSourceWithReturnScopePopulatesRootId() throws Exception {
    int docsRequested = 1;

    Map<String, IndexableField> storedFields =
        Map.of(
            FieldName.MetaField.ID.getLuceneFieldName(),
            LuceneDocumentIdEncoder.documentIdField(
                LuceneDocumentIdEncoder.encodeDocumentId(new BsonString("1"))));

    var producer =
        mockProducerWithStoredSourceAndReturnScope(
            new MockSearchManager(1),
            new AdjustableBatchSizeStrategy(true),
            SearchResult::new,
            getStoredSourceSearcherRef(storedFields));
    BsonArray nextBatch =
        executeAndGetNextBatch(
            producer,
            Bytes.ofMebi(16),
            BatchCursorOptionsBuilder.builder().docsRequested(docsRequested).build());

    Truth.assertThat(nextBatch.size()).isEqualTo(1);
    BsonValue result = nextBatch.getFirst();
    Truth.assertThat(result.asDocument().get("$searchRootDocumentId"))
        .isEqualTo(new BsonString("1"));
  }

  private static BsonArray executeAndGetNextBatch(
      LuceneSearchBatchProducer producer, Bytes resultSizeLimit) throws IOException {
    return executeAndGetNextBatch(producer, resultSizeLimit, BatchCursorOptionsBuilder.empty());
  }

  private static BsonArray executeAndGetNextBatch(
      LuceneSearchBatchProducer producer, Bytes resultSizeLimit, BatchCursorOptions cursorOptions)
      throws IOException {
    producer.execute(resultSizeLimit, cursorOptions);
    return producer.getNextBatch(resultSizeLimit);
  }

  private static LuceneIndexSearcherReference getStoredSourceSearcherRef(
      Map<String, IndexableField> fields) throws IOException {
    var searcher = mock(LuceneIndexSearcher.class);

    Supplier<Document> docGenerator =
        () -> {
          var doc = new Document();
          for (var field : fields.entrySet()) {
            doc.add(field.getValue());
          }
          return doc;
        };

    StoredFields mockStoredFields = new FakeStoredFields(id -> docGenerator.get());
    IndexReader mockReader = mock(LeafReader.class);
    when(mockReader.storedFields()).thenReturn(mockStoredFields);

    when(searcher.storedFields()).thenReturn(mockStoredFields);
    when(searcher.getIndexReader()).thenReturn(mockReader);

    LuceneIndexSearcherReference ref = mock(LuceneIndexSearcherReference.class);
    when(ref.getIndexSearcher()).thenReturn(searcher);
    return ref;
  }

  private void assertCorrectCounts(
      LuceneSearchBatchProducer producer, int expectedDocs, int expectedBatches) throws Exception {
    @Var int docCount = 0;
    @Var int batchCount = 0;
    while (!producer.isExhausted()) {
      var batch =
          executeAndGetNextBatch(
              producer,
              CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
              BatchCursorOptionsBuilder.empty());
      docCount += batch.getValues().size();
      batchCount++;
    }
    // After exhausted, calling an extra getNextBatch() will return empty.
    assertEmptyNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Assert.assertEquals(expectedDocs, docCount);
    Assert.assertEquals(expectedBatches, batchCount);
  }

  private void assertCorrectCountsWithIterator(
      LuceneSearchBatchProducer producer, int expectedDocs, int expectedBatches) throws Exception {
    @Var int docCount = 0;
    @Var int batchCount = 0;
    while (!producer.isExhausted()) {
      producer.execute(
          CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
      var iter = producer.getSearchResultsIter();

      while (iter.peek().isPresent()) {
        docCount += 1;
        iter.acceptAndAdvance();
      }
      batchCount++;
    }
    // After exhausted, calling an extra getNextBatch() will return empty.
    assertEmptyNextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);
    Assert.assertEquals(expectedDocs, docCount);
    Assert.assertEquals(expectedBatches, batchCount);
  }

  private void assertEmptyNextBatch(LuceneSearchBatchProducer producer, Bytes resultSizeLimit)
      throws IOException {
    Assert.assertTrue(executeAndGetNextBatch(producer, resultSizeLimit).isEmpty());
  }

  private static LuceneSearchBatchProducer createProducer(
      LuceneSearchManager<QueryInfo> manager,
      BatchSizeStrategy batchSizeStrategy,
      ResultFactory resultFactory,
      IndexMetricsUpdater.QueryingMetricsUpdater updater,
      int indexPartitionId)
      throws IOException, InvalidQueryException {
    var searcherRef = getSearcherRef();
    var queryInfo = manager.initialSearch(searcherRef, batchSizeStrategy.adviseNextBatchSize());
    // TODO(CLOUDP-280897): Add test for the isGaSort=true codepath.
    return new LuceneSearchBatchProducer(
        searcherRef,
        manager,
        queryInfo.topDocs,
        queryInfo.luceneExhausted,
        Optional.empty(),
        Optional.empty(),
        batchSizeStrategy,
        ProjectFactory.build(
            new ProjectSpec(false, StoredSourceDefinition.createIncludeAll()),
            searcherRef.getIndexSearcher().getIndexReader()),
        updater,
        QueryCursorOptions.empty(),
        false,
        Optional.empty(),
        false,
        resultFactory,
        indexPartitionId,
        false);
  }

  // Convenient method when indexPartitionId is 0.
  private static LuceneSearchBatchProducer createProducer(
      LuceneSearchManager<QueryInfo> manager,
      BatchSizeStrategy batchSizeStrategy,
      ResultFactory resultFactory)
      throws IOException, InvalidQueryException {
    var updater = IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty();
    return createProducer(manager, batchSizeStrategy, resultFactory, updater, 0);
  }

  private static LuceneSearchBatchProducer createProducer(
      LuceneSearchManager<QueryInfo> manager,
      BatchSizeStrategy batchSizeStrategy,
      ResultFactory resultFactory,
      IndexMetricsUpdater.QueryingMetricsUpdater updater)
      throws IOException, InvalidQueryException {
    return createProducer(manager, batchSizeStrategy, resultFactory, updater, 0);
  }

  private static LuceneSearchBatchProducer mockProducerWithStoredSourceAndReturnScope(
      LuceneSearchManager<QueryInfo> manager,
      BatchSizeStrategy batchSizeStrategy,
      ResultFactory resultFactory,
      LuceneIndexSearcherReference searcherRef)
      throws IOException, InvalidQueryException {
    QueryInfo queryInfo =
        manager.initialSearch(searcherRef, batchSizeStrategy.adviseNextBatchSize());
    return new LuceneSearchBatchProducer(
        searcherRef,
        manager,
        queryInfo.topDocs,
        queryInfo.luceneExhausted,
        Optional.empty(),
        Optional.empty(),
        batchSizeStrategy,
        ProjectFactory.build(
            new ProjectSpec(true, StoredSourceDefinition.createIncludeAll()),
            searcherRef.getIndexSearcher().getIndexReader()),
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty(),
        QueryCursorOptions.empty(),
        false,
        Optional.empty(),
        true,
        resultFactory,
        0,
        false);
  }

  private static LuceneIndexSearcherReference getSearcherRef() throws IOException {
    var searcher = mock(LuceneIndexSearcher.class);

    IntFunction<Document> docGenerator =
        docId -> {
          var doc = new Document();
          var id = LuceneDocumentIdEncoder.encodeDocumentId(new BsonString(String.valueOf(docId)));
          doc.add(LuceneDocumentIdEncoder.documentIdField(id));
          return doc;
        };

    StoredFields mockStoredFields = new FakeStoredFields(docGenerator);
    IndexReader mockReader = mock(LeafReader.class);
    when(mockReader.storedFields()).thenReturn(mockStoredFields);

    when(searcher.storedFields()).thenReturn(mockStoredFields);
    when(searcher.getIndexReader()).thenReturn(mockReader);

    LuceneIndexSearcherReference ref = mock(LuceneIndexSearcherReference.class);
    when(ref.getIndexSearcher()).thenReturn(searcher);
    return ref;
  }

  private static class MockSearchManager implements LuceneSearchManager<QueryInfo> {

    private final int numDocs;
    private final float scoreDelta;
    @Var private int servedDocs = 0;

    private MockSearchManager(int numDocs) {
      this.numDocs = numDocs;
      this.scoreDelta = 0.5f / numDocs;
    }

    protected float generateDocScore(int docId) {
      float ret = 1.0f - docId * this.scoreDelta;
      Check.argIsPositive(ret, "docScore");
      return ret;
    }

    protected ScoreDoc getScoreDoc(int i) {
      return new ScoreDoc(i, generateDocScore(i));
    }

    @Override
    public QueryInfo initialSearch(LuceneIndexSearcherReference searcherReference, int batchSize) {
      ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(batchSize, this.numDocs)];
      for (int i = 0; i < scoreDocs.length; i++) {
        scoreDocs[i] = getScoreDoc(i);
      }
      this.servedDocs += scoreDocs.length;
      return new QueryInfo(
          new TopDocs(new TotalHits(this.numDocs, TotalHits.Relation.EQUAL_TO), scoreDocs),
          scoreDocs.length < batchSize);
    }

    @Override
    public TopDocs getMoreTopDocs(
        LuceneIndexSearcherReference searcherReference, ScoreDoc lastScoreDoc, int batchSize) {
      ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(batchSize, this.numDocs - this.servedDocs)];
      for (int i = lastScoreDoc.doc + 1; i < lastScoreDoc.doc + 1 + scoreDocs.length; i++) {
        scoreDocs[i - lastScoreDoc.doc - 1] = getScoreDoc(i);
      }
      this.servedDocs += scoreDocs.length;
      return new TopDocs(new TotalHits(this.numDocs, TotalHits.Relation.EQUAL_TO), scoreDocs);
    }
  }

  private static class ConstantScoreMockSearchManager extends MockSearchManager {
    private final float constantScore;

    private ConstantScoreMockSearchManager(int numDocs, float score) {
      super(numDocs);
      this.constantScore = score;
    }

    @Override
    protected float generateDocScore(int docId) {
      return this.constantScore;
    }
  }

  private static class RepeatedBatchMockSearchManager extends MockSearchManager {
    private final int repeatAfter;

    private RepeatedBatchMockSearchManager(int numDocs, int repeatAfter) {
      super(numDocs);
      this.repeatAfter = repeatAfter;
    }

    @Override
    protected ScoreDoc getScoreDoc(int docId) {
      return docId > this.repeatAfter
          ? super.getScoreDoc(this.repeatAfter)
          : super.getScoreDoc(docId);
    }
  }

  @Test
  public void getNextBatch_withNullnessExpandedFields_stripsNullnessValues() throws Exception {
    SortField nullnessField = new SortField("$meta/nullness/age", SortField.Type.LONG);
    SortField valueField = new SortField("$type:int64/v2/age", SortField.Type.LONG);
    SortField[] sortFields = {nullnessField, valueField};

    int numDocs = 2;
    ScoreDoc[] scoreDocs = new ScoreDoc[numDocs];
    scoreDocs[0] = new FieldDoc(0, 1.0f, new Object[] {new BsonInt64(0), new BsonInt64(10)});
    scoreDocs[1] = new FieldDoc(1, 0.9f, new Object[] {new BsonInt64(0), new BsonInt64(20)});

    TopFieldDocs topDocs =
        new TopFieldDocs(
            new TotalHits(numDocs, TotalHits.Relation.EQUAL_TO), scoreDocs, sortFields);

    var searcherRef = getSearcherRef();
    @SuppressWarnings("unchecked")
    LuceneSearchManager<QueryInfo> noOpManager = mock(LuceneSearchManager.class);
    var producer =
        new LuceneSearchBatchProducer(
            searcherRef,
            noOpManager,
            topDocs,
            true,
            Optional.empty(),
            Optional.empty(),
            new ConstantBatchSizeStrategy(Integer.MAX_VALUE),
            ProjectFactory.build(
                new ProjectSpec(false, StoredSourceDefinition.createIncludeAll()),
                searcherRef.getIndexSearcher().getIndexReader()),
            IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty(),
            QueryCursorOptions.empty(),
            true,
            Optional.empty(),
            false,
            SearchResult::new,
            0,
            true);

    BsonArray batch = executeAndGetNextBatch(producer, Bytes.ofMebi(16));
    Assert.assertEquals(2, batch.size());

    BsonDocument sortValues0 = batch.get(0).asDocument().getDocument("$searchSortValues");
    Assert.assertEquals(
        "Nullness value should be stripped, leaving only the actual value field",
        1,
        sortValues0.size());
    Assert.assertTrue(sortValues0.containsKey("_0"));
    Assert.assertEquals(10L, sortValues0.get("_0").asInt64().getValue());

    BsonDocument sortValues1 = batch.get(1).asDocument().getDocument("$searchSortValues");
    Assert.assertEquals(1, sortValues1.size());
    Assert.assertTrue(sortValues1.containsKey("_0"));
    Assert.assertEquals(20L, sortValues1.get("_0").asInt64().getValue());
  }

  @Test
  public void getNextBatch_withoutNullnessExpansion_sortValuesUnchanged() throws Exception {
    SortField valueField = new SortField("$type:int64/v2/age", SortField.Type.LONG);
    SortField[] sortFields = {valueField};

    int numDocs = 1;
    ScoreDoc[] scoreDocs = {new FieldDoc(0, 1.0f, new Object[] {new BsonInt64(42)})};

    TopFieldDocs topDocs =
        new TopFieldDocs(
            new TotalHits(numDocs, TotalHits.Relation.EQUAL_TO), scoreDocs, sortFields);

    var searcherRef = getSearcherRef();
    @SuppressWarnings("unchecked")
    LuceneSearchManager<QueryInfo> noOpManager = mock(LuceneSearchManager.class);
    var producer =
        new LuceneSearchBatchProducer(
            searcherRef,
            noOpManager,
            topDocs,
            true,
            Optional.empty(),
            Optional.empty(),
            new ConstantBatchSizeStrategy(Integer.MAX_VALUE),
            ProjectFactory.build(
                new ProjectSpec(false, StoredSourceDefinition.createIncludeAll()),
                searcherRef.getIndexSearcher().getIndexReader()),
            IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty(),
            QueryCursorOptions.empty(),
            true,
            Optional.empty(),
            false,
            SearchResult::new,
            0,
            true);

    BsonArray batch = executeAndGetNextBatch(producer, Bytes.ofMebi(16));
    Assert.assertEquals(1, batch.size());

    BsonDocument sortValues = batch.get(0).asDocument().getDocument("$searchSortValues");
    Assert.assertEquals(1, sortValues.size());
    Assert.assertTrue(sortValues.containsKey("_0"));
    Assert.assertEquals(42L, sortValues.get("_0").asInt64().getValue());
  }

  @Test
  public void getNextBatch_multipleNullnessFields_allStripped() throws Exception {
    SortField[] sortFields = {
      new SortField("$meta/nullness/age", SortField.Type.LONG),
      new SortField("$type:int64/v2/age", SortField.Type.LONG),
      new SortField("$meta/nullness/score", SortField.Type.LONG),
      new SortField("$type:int64/v2/score", SortField.Type.LONG),
    };

    ScoreDoc[] scoreDocs = {
      new FieldDoc(
          0,
          1.0f,
          new Object[] {new BsonInt64(0), new BsonInt64(25), new BsonInt64(0), new BsonInt64(99)})
    };

    TopFieldDocs topDocs =
        new TopFieldDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), scoreDocs, sortFields);

    var searcherRef = getSearcherRef();
    @SuppressWarnings("unchecked")
    LuceneSearchManager<QueryInfo> noOpManager = mock(LuceneSearchManager.class);
    var producer =
        new LuceneSearchBatchProducer(
            searcherRef,
            noOpManager,
            topDocs,
            true,
            Optional.empty(),
            Optional.empty(),
            new ConstantBatchSizeStrategy(Integer.MAX_VALUE),
            ProjectFactory.build(
                new ProjectSpec(false, StoredSourceDefinition.createIncludeAll()),
                searcherRef.getIndexSearcher().getIndexReader()),
            IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.empty(),
            QueryCursorOptions.empty(),
            true,
            Optional.empty(),
            false,
            SearchResult::new,
            0,
            true);

    BsonArray batch = executeAndGetNextBatch(producer, Bytes.ofMebi(16));
    Assert.assertEquals(1, batch.size());

    BsonDocument sortValues = batch.get(0).asDocument().getDocument("$searchSortValues");
    Assert.assertEquals(
        "Both nullness fields should be stripped, leaving only 2 value fields",
        2,
        sortValues.size());
    Assert.assertEquals(25L, sortValues.get("_0").asInt64().getValue());
    Assert.assertEquals(99L, sortValues.get("_1").asInt64().getValue());
  }
}
