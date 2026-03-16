package com.xgen.mongot.cursor;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockInitializedIndex;
import static com.xgen.testing.mongot.mock.index.SearchIndexReader.mockIndexReader;
import static com.xgen.testing.mongot.mock.index.query.Query.mockQuery;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.truth.Truth;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.concurrent.OneShotSingleThreadExecutor;
import com.xgen.testing.mongot.cursor.batch.BatchCursorOptionsBuilder;
import com.xgen.testing.mongot.mock.index.BatchProducer;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class IndexCursorManagerImplTest {

  private static final int INTERMEDIATE_PROTOCOL_VERSION = 1;

  private InitializedSearchIndex index;
  private IndexCursorManagerImpl manager;
  private String namespace;
  private Optional<MongotCursor> lastCreatedSearchCursor;
  private Optional<MongotCursor> lastCreatedMetaCursor;

  @Before
  public void setUp() throws Exception {
    DefaultIndexCatalog indexCatalog = new DefaultIndexCatalog();
    IndexGeneration indexGeneration = mockIndexGeneration();
    this.index = mockInitializedIndex(indexGeneration, 3).asSearchIndex();
    this.namespace =
        String.format(
            "%s.%s",
            this.index.getDefinition().getDatabase(),
            this.index.getDefinition().getLastObservedCollectionName());
    indexCatalog.addIndex(indexGeneration);

    // create a cursor factory where cursors are being recorded and spied upon
    CursorFactory factory = spy(new CursorFactory(CursorIdSupplier.createDefault()));
    Answer<CursorFactory.CursorAndMetaResults> wrapNewCursors =
        invocationOnMock -> {
          CursorFactory.CursorAndMetaResults cursorAndMeta =
              (CursorFactory.CursorAndMetaResults) invocationOnMock.callRealMethod();

          MongotCursor mockCursor = spy(cursorAndMeta.cursor);
          this.lastCreatedSearchCursor = Optional.of(mockCursor);
          return new CursorFactory.CursorAndMetaResults(
              mockCursor, cursorAndMeta.metaResults);
        };
    Mockito.doAnswer(wrapNewCursors).when(factory).createCursor(any(), any(), any(), any(), any());

    Answer<CursorFactory.SearchCursorAndMetaCursor> wrapNewIntermediateCursors =
        invocationOnMock -> {
          CursorFactory.SearchCursorAndMetaCursor cursors =
              (CursorFactory.SearchCursorAndMetaCursor) invocationOnMock.callRealMethod();

          MongotCursor searchCursor = spy(cursors.searchCursor);
          this.lastCreatedSearchCursor = Optional.of(searchCursor);
          MongotCursor metaCursor = spy(cursors.metaCursor);
          this.lastCreatedMetaCursor = Optional.of(metaCursor);
          return new CursorFactory.SearchCursorAndMetaCursor(searchCursor, metaCursor);
        };
    Mockito.doAnswer(wrapNewIntermediateCursors)
        .when(factory)
        .createIntermediateCursors(any(), any(), any(), any(), any());

    this.manager = new IndexCursorManagerImpl(this.index, factory);
  }

  private long createCursor() throws Exception {
    return this.manager.createCursor(
            this.namespace,
            mockQuery(),
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS)
        .cursorId;
  }

  private IntermediateSearchCursorInfo createIntermediateCursors() throws Exception {
    return this.manager.createIntermediateCursors(
        this.namespace,
        mockQuery(),
        INTERMEDIATE_PROTOCOL_VERSION,
        QueryCursorOptions.empty(),
        QueryOptimizationFlags.DEFAULT_OPTIONS);
  }

  @Test
  public void testMetaResults() throws Exception {
    setIndexStatus(IndexStatus.steady());
    SearchCursorInfo cursorInfo =
        this.manager.createCursor(
            this.namespace,
            mockQuery(),
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS);

    @Var long docCount = 0;
    @Var boolean exhausted = false;
    @Var long getMoreCount = 0;
    while (!exhausted) {
      MongotCursorResultInfo getMoreInfo =
          this.manager.getNextBatch(
              cursorInfo.cursorId,
              CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
              BatchCursorOptionsBuilder.empty());
      exhausted = getMoreInfo.exhausted;
      docCount += getMoreInfo.batch.asArray().size();
      getMoreCount++;
      if (!exhausted) {
        Assert.assertEquals(
            docCount,
            this.manager
                .cursorAndStats
                .get(cursorInfo.cursorId)
                .stats()
                .docsReturned()
                .longValue());
        Assert.assertEquals(
            this.manager.cursorAndStats.get(cursorInfo.cursorId).stats().getMoreCount().longValue(),
            getMoreCount);
      }
    }

    Assert.assertEquals(BatchProducer.EXPECTED_DEFAULT_DOC_COUNT, docCount);
    Assert.assertEquals(docCount, cursorInfo.metaResults.count().getTotalOrLower().longValue());
  }

  @Test
  public void testCursorExplainPresentOnCreate() throws Exception {
    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.QUERY_PLANNER),
            Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
      setIndexStatus(IndexStatus.steady());
      var cursor = createCursor();
      Truth.assertThat(this.manager.getExplainQueryState(cursor)).isPresent();
    }
  }

  @Test
  public void testIntermediateCursors() throws Exception {
    setIndexStatus(IndexStatus.steady());
    var cursors = createIntermediateCursors();

    @Var long docCount = 0;
    @Var boolean exhausted = false;
    @Var long getMoreCount = 0;
    while (!exhausted) {
      MongotCursorResultInfo getMoreInfo =
          this.manager.getNextBatch(
              cursors.searchCursorId,
              CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
              BatchCursorOptionsBuilder.empty());
      exhausted = getMoreInfo.exhausted;
      docCount += getMoreInfo.batch.asArray().size();
      getMoreCount++;
      if (!exhausted) {
        Assert.assertEquals(
            this.manager
                .cursorAndStats
                .get(cursors.searchCursorId)
                .stats()
                .docsReturned()
                .longValue(),
            docCount);
        Assert.assertEquals(
            this.manager
                .cursorAndStats
                .get(cursors.searchCursorId)
                .stats()
                .getMoreCount()
                .longValue(),
            getMoreCount);
      }
    }

    Assert.assertEquals(BatchProducer.EXPECTED_DEFAULT_DOC_COUNT, docCount);
    // only search cursor is exhausted and closed.
    Assert.assertEquals(1, this.manager.cursorAndStats.size());
    assertMetaCursorCount(cursors.metaCursorId, BatchProducer.EXPECTED_DEFAULT_DOC_COUNT);
  }

  @Test
  public void testCanIterateIntermediateMetaCursor() throws Exception {
    setIndexStatus(IndexStatus.steady());
    var cursors = createIntermediateCursors();

    BsonArray batch = new BsonArray(List.of(new BsonDocument("foo", new BsonString("bar"))));

    // Hack the meta cursor to output multiple batches
    doAnswer((invocationOnMock) -> new MongotCursorResultInfo(false, batch, this.namespace))
        .doCallRealMethod()
        .when(this.lastCreatedMetaCursor.get())
        .getNextBatch(any(), any());

    MongotCursorResultInfo metaResultInfo =
        this.manager.getNextBatch(
            cursors.metaCursorId,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            BatchCursorOptionsBuilder.empty());
    Assert.assertEquals(metaResultInfo.batch, batch);
    Assert.assertEquals(
        metaResultInfo.batch.asArray().size(),
        this.manager.cursorAndStats.get(cursors.metaCursorId).stats().docsReturned().longValue());

    // Check that we can get the second batch, with the count
    assertMetaCursorCount(cursors.metaCursorId, BatchProducer.EXPECTED_DEFAULT_DOC_COUNT);

    // Assert that the cursor is now closed
    assertCursorDoesNotExist(cursors.metaCursorId);
  }

  @Test
  public void testSteadyStateIndexCreatesValidCursor() throws Exception {
    setIndexStatus(IndexStatus.steady());
    var cursorId = createCursor();
    assertSearchCursorExists(cursorId);
  }

  @Test
  public void testIndexDoesNotExistReturnsEmptyCursor() throws Exception {
    setIndexStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    var cursorId = createCursor();
    assertEmptyCursor(cursorId);
  }

  @Test
  public void testIntermediateIndexDoesNotExistReturnsEmptyCursor() throws Exception {
    setIndexStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    var cursors = createIntermediateCursors();
    assertEmptyCursor(cursors.searchCursorId);
    assertMetaCursorCount(cursors.metaCursorId, 0);
  }

  @Test
  public void testIndexStatesThrowsUnavailableException() throws IndexUnavailableException {
    Mockito.doThrow(new IndexUnavailableException("test"))
        .when(this.index)
        .throwIfUnavailableForQuerying();
    Assert.assertThrows(IndexUnavailableException.class, this::createCursor);
  }

  @Test
  public void testIntermediateIndexStatesThrowsUnavailableException()
      throws IndexUnavailableException {
    Mockito.doThrow(new IndexUnavailableException("test"))
        .when(this.index)
        .throwIfUnavailableForQuerying();
    Assert.assertThrows(IndexUnavailableException.class, this::createIntermediateCursors);
  }

  @Test
  public void testKillCursorRemovesCursor() throws Exception {
    var cursorId = createCursor();
    verify(this.lastCreatedSearchCursor.orElseThrow(), times(0)).close();

    this.manager.killCursor(cursorId);
    assertCursorDoesNotExist(cursorId);
    // make sure the cursor was closed
    verify(this.lastCreatedSearchCursor.orElseThrow()).close();
  }

  @Test
  public void testKillCursorRemovesIntermediateCursors() throws Exception {
    var cursors = createIntermediateCursors();
    verify(this.lastCreatedSearchCursor.orElseThrow(), times(0)).close();
    verify(this.lastCreatedMetaCursor.orElseThrow(), times(0)).close();

    this.manager.killCursor(cursors.searchCursorId);
    assertCursorDoesNotExist(cursors.searchCursorId);

    this.manager.killCursor(cursors.metaCursorId);
    assertCursorDoesNotExist(cursors.metaCursorId);

    // make sure the cursor was closed
    verify(this.lastCreatedSearchCursor.orElseThrow()).close();
    verify(this.lastCreatedMetaCursor.orElseThrow()).close();
  }

  @Test
  public void testKillAllCursorsKillsCursor() throws Exception {
    var cursorId = createCursor();
    verify(this.lastCreatedSearchCursor.orElseThrow(), times(0)).close();

    this.manager.killAll();
    // make sure this specific cursor was killed
    assertCursorDoesNotExist(cursorId);
    // make sure the cursor was closed
    verify(this.lastCreatedSearchCursor.orElseThrow()).close();
  }

  @Test
  public void testKillAllCursorsKillsIntermediateCursors() throws Exception {
    var cursors = createIntermediateCursors();
    verify(this.lastCreatedSearchCursor.orElseThrow(), times(0)).close();
    verify(this.lastCreatedMetaCursor.orElseThrow(), times(0)).close();

    this.manager.killAll();

    // make sure this specific cursor was killed
    assertCursorDoesNotExist(cursors.searchCursorId);
    assertCursorDoesNotExist(cursors.metaCursorId);

    // make sure the cursor was closed
    verify(this.lastCreatedSearchCursor.orElseThrow()).close();
    verify(this.lastCreatedMetaCursor.orElseThrow()).close();
  }

  @Test
  public void testMetaCursorAfterKilledSearchCursor() throws Exception {
    var cursors = createIntermediateCursors();
    verify(this.lastCreatedSearchCursor.orElseThrow(), times(0)).close();
    verify(this.lastCreatedMetaCursor.orElseThrow(), times(0)).close();

    this.manager.killCursor(cursors.searchCursorId);
    assertCursorDoesNotExist(cursors.searchCursorId);

    assertMetaCursorCount(cursors.metaCursorId, BatchProducer.EXPECTED_DEFAULT_DOC_COUNT);
  }

  @Test
  public void testSearchCursorAfterKilledMetaCursor() throws Exception {
    var cursors = createIntermediateCursors();
    verify(this.lastCreatedSearchCursor.orElseThrow(), times(0)).close();
    verify(this.lastCreatedMetaCursor.orElseThrow(), times(0)).close();

    this.manager.killCursor(cursors.metaCursorId);
    assertCursorDoesNotExist(cursors.metaCursorId);

    assertSearchCursorExists(cursors.searchCursorId);
  }

  @Test
  public void testKillIdleCursorKillsIdleCursor() throws Exception {
    var cursorId = createCursor();

    // The cursor is inactive at this point, so it should be considered idle
    var killSince = Instant.now();

    var killed = this.manager.killIdleCursorsSince(killSince);

    // idleCursor was not used since idleTime, so it is idle
    assertCursorDoesNotExist(cursorId);
    Assert.assertEquals(List.of(cursorId), killed);
  }

  @Test
  public void testKillIdleCursorKillsIdleIntermediateCursors() throws Exception {
    var cursors = createIntermediateCursors();

    // The cursor is inactive at this point, so it should be considered idle
    var killSince = Instant.now();
    var killed = this.manager.killIdleCursorsSince(killSince);

    // The order of the returned cursors is not guaranteed, so make it a set to check equivalence
    // with the expected cursor values.
    var killedSet = new HashSet<>(killed);
    Assert.assertEquals(killed.size(), killedSet.size());

    // idleCursor was not used since idleTime, so it is idle
    assertCursorDoesNotExist(cursors.searchCursorId);
    assertCursorDoesNotExist(cursors.metaCursorId);
    Assert.assertEquals(Set.of(cursors.searchCursorId, cursors.metaCursorId), killedSet);
  }

  @Test
  public void testKillIdleCursorDoesNotKillActiveCursor() throws Exception {
    var killSince = Instant.now();

    // The cursor was created since idle time, so it should be considered active, and not killed
    var cursorId = createCursor();

    var killed = this.manager.killIdleCursorsSince(killSince);

    assertSearchCursorExists(cursorId);
    Assert.assertEquals(Collections.emptyList(), killed);
  }

  @Test
  public void testKillIdleCursorDoesNotKillActiveIntermediateCursors() throws Exception {
    var killSince = Instant.now();

    // The cursors were created since idle time, so they should be considered active, and not killed
    var cursors = createIntermediateCursors();

    var killed = this.manager.killIdleCursorsSince(killSince);

    assertSearchCursorExists(cursors.searchCursorId);
    assertMetaCursorCount(cursors.metaCursorId, BatchProducer.EXPECTED_DEFAULT_DOC_COUNT);
    Assert.assertEquals(Collections.emptyList(), killed);
  }

  /**
   * Make sure that calls to getMore are recorded as activity on the cursor and it is not considered
   * idle.
   */
  @Test
  public void testGetMoreCallOnCursorConsideredAsActiveCursor() throws Exception {
    var cursorId = createCursor();

    var killSince = Instant.now();

    this.manager.getNextBatch(
        cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());

    // we called get more on the cursor, so it was active since killSince
    var killed = this.manager.killIdleCursorsSince(killSince);

    assertSearchCursorExists(cursorId);
    Assert.assertEquals(Collections.emptyList(), killed);
  }

  /**
   * Make sure that calls to getMore are recorded as activity on the cursor and it is not considered
   * idle.
   */
  @Test
  public void testGetMoreCallOnIntermediateCursorsConsideredAsActiveCursors() throws Exception {
    var cursors = createIntermediateCursors();

    doAnswer(
            (invocationOnMock) -> {
              // Call the real method so the MongotCursor will set its last operation time.
              MongotCursorResultInfo resultInfo =
                  (MongotCursorResultInfo) invocationOnMock.callRealMethod();
              // Return a batch with exhausted=false, so this cursor is not killed
              return new MongotCursorResultInfo(false, resultInfo.batch, resultInfo.namespace);
            })
        // Return an empty, exhausted batch so this cursor will be killed on the next getMore.
        .doAnswer(
            (invocationOnMock) -> new MongotCursorResultInfo(true, new BsonArray(), this.namespace))
        .when(this.lastCreatedMetaCursor.get())
        .getNextBatch(any(), any());

    var killSince = Instant.now();

    this.manager.getNextBatch(
        cursors.searchCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        BatchCursorOptionsBuilder.empty());
    this.manager.getNextBatch(
        cursors.metaCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        BatchCursorOptionsBuilder.empty());

    // we called get more on the cursor, so it was active since killSince
    var killed = this.manager.killIdleCursorsSince(killSince);

    assertSearchCursorExists(cursors.searchCursorId);
    // Check that we can still access the cursor, even though we've mocked it to return an empty
    // batch
    this.manager.getNextBatch(
        cursors.metaCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        BatchCursorOptionsBuilder.empty());
    Assert.assertEquals(Collections.emptyList(), killed);
  }

  /**
   * This method tests that an idle cursor can be reaped due to a timeout concurrently with it being
   * killed explicitly. It is very unlikely that this should happen.
   */
  @Test
  public void testIdleCursorKilledConcurrently() throws Exception {
    var cursorId = createCursor();

    var killSince = Instant.now();

    // this cursor is now idle
    MongotCursor cursor = this.lastCreatedSearchCursor.orElseThrow();

    var twoLatch = new Phaser(2);
    Answer<Void> waitForTwoCalls =
        invocation -> {
          twoLatch.arriveAndAwaitAdvance();
          invocation.callRealMethod();
          return null;
        };
    doAnswer(waitForTwoCalls).when(cursor).close();

    // kill the cursor and time it out concurrently
    ExecutorService executorService = Executors.newCachedThreadPool();
    Future<List<Long>> killedIdFuture =
        executorService.submit(() -> this.manager.killIdleCursorsSince(killSince));
    var killFuture = executorService.submit(() -> this.manager.killCursor(cursorId));

    // in case we deadlock, the test will fail on a timeout
    killFuture.get();

    assertCursorDoesNotExist(cursorId);
    Assert.assertEquals(List.of(cursorId), killedIdFuture.get());

    executorService.shutdownNow();
  }

  @Test
  public void testKillCursorIsIdempotent() throws Exception {
    var cursorId = createCursor();
    this.manager.killCursor(cursorId);
    this.manager.killCursor(cursorId); // should not throw
    assertCursorDoesNotExist(cursorId);
  }

  @Test
  public void testSearchNascentCursor() throws Exception {
    Executor executor = new OneShotSingleThreadExecutor("testSearchInIsOpenCursors");
    CountDownLatch enterSearch = new CountDownLatch(1);
    CountDownLatch exitHasOpenCursors = new CountDownLatch(1);
    CountDownLatch exitSearch = new CountDownLatch(1);

    // Use index.getReader to mark critical section inside search (before cursor id is put into
    // map). Happens in CursorFactory::createCursor.
    when(this.index.getReader())
        .thenAnswer(
            invocation -> {
              enterSearch.countDown();
              exitHasOpenCursors.await();
              return mockIndexReader();
            });

    Assert.assertFalse("hasOpenCursors false before search", this.manager.hasOpenCursors());

    FutureUtils.checkedRunAsync(
        () -> {
          this.manager.createCursor(
              "a", mockQuery(), QueryCursorOptions.empty(), QueryOptimizationFlags.DEFAULT_OPTIONS);
          exitSearch.countDown();
        },
        executor,
        Exception.class);

    // Wait for search to enter critical section and test hasOpenCursors() == true.
    enterSearch.await(1, TimeUnit.SECONDS);
    Assert.assertTrue("hasOpenCursors true during search", this.manager.hasOpenCursors());
  }

  @Test
  public void testNascentIntermediateCursor() throws Exception {
    Executor executor = new OneShotSingleThreadExecutor("testSearchInIsOpenCursors");
    CountDownLatch enterSearch = new CountDownLatch(1);
    CountDownLatch exitHasOpenCursors = new CountDownLatch(1);
    CountDownLatch exitSearch = new CountDownLatch(1);

    // Use index.getReader to mark critical section inside search (before cursor id is put into
    // map). Happens in CursorFactory::createCursor.
    when(this.index.getReader())
        .thenAnswer(
            invocation -> {
              enterSearch.countDown();
              exitHasOpenCursors.await();
              return mockIndexReader();
            });

    Assert.assertFalse("hasOpenCursors false before search", this.manager.hasOpenCursors());

    FutureUtils.checkedRunAsync(
        () -> {
          this.manager.createIntermediateCursors(
              "a",
              mockQuery(),
              INTERMEDIATE_PROTOCOL_VERSION,
              QueryCursorOptions.empty(),
              QueryOptimizationFlags.DEFAULT_OPTIONS);
          exitSearch.countDown();
        },
        executor,
        Exception.class);

    // Wait for search to enter critical section and test hasOpenCursors() == true.
    enterSearch.await(1, TimeUnit.SECONDS);
    Assert.assertTrue("hasOpenCursors true during search", this.manager.hasOpenCursors());
  }

  private void setIndexStatus(IndexStatus status) {
    this.index.setStatus(status);
  }

  private void assertSearchCursorExists(long cursorId)
      throws IOException, MongotCursorNotFoundException {
    // should not throw:
    this.manager.getNextBatch(
        cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
    this.manager.getNextBatch(
        cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());

    this.manager.getExplainQueryState(cursorId);
  }

  private void assertCursorDoesNotExist(long cursorId) {
    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            this.manager.getNextBatch(
                cursorId,
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
                BatchCursorOptionsBuilder.empty()));

    Assert.assertThrows(
        MongotCursorNotFoundException.class, () -> this.manager.getExplainQueryState(cursorId));
  }

  private void assertEmptyCursor(long cursorId) throws Exception {
    // Should return an empty batch
    MongotCursorResultInfo batch =
        this.manager.getNextBatch(
            cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
    Assert.assertEquals(0, batch.batch.asArray().size());
    Assert.assertTrue(batch.exhausted);

    try {
      this.manager.getNextBatch(
          cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());
    } catch (MongotCursorNotFoundException expected) {
      // expected
    }
  }

  private void assertMetaCursorCount(long cursorId, long expectedCount) throws Exception {
    MongotCursorResultInfo metaResultInfo =
        this.manager.getNextBatch(
            cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptionsBuilder.empty());

    Assert.assertTrue(metaResultInfo.exhausted);
    Assert.assertEquals(1, metaResultInfo.batch.asArray().getValues().size());
    Assert.assertEquals(
        expectedCount,
        metaResultInfo.batch.asArray().get(0).asDocument().get("count").asInt64().getValue());
  }
}
