package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils.POST_BATCH_RESUME_TOKEN;
import static com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils.toRawBsonDocuments;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.ChangeStreamEventCheckException;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException.Type;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamIndexManager.BatchInfo;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamIndexManager.DocumentMetricsUpdater;
import com.xgen.mongot.util.Condition;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

@RunWith(Enclosed.class)
public class ChangeStreamIndexManagerTest {

  private enum TestParameter {
    DECODING_SCHEDULER
  }

  private static final IndexMetricsUpdater IGNORE_METRICS =
      IndexMetricsUpdaterBuilder.builder()
          .metricsFactory(SearchIndex.mockMetricsFactory())
          .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
          .build();

  private static final DocumentMetricsUpdater NOOP_DOCUMENT_METRICS_UPDATER = (x, y, z) -> {};

  private static final Timer NOOP_TIMER =
      Timer.builder("dummy").register(new SimpleMeterRegistry());

  @RunWith(Parameterized.class)
  public static class SharedTests {

    @Parameters(name = "{0}")
    public static List<TestParameter> params() {
      return List.of(TestParameter.DECODING_SCHEDULER);
    }

    @Parameter() public TestParameter testParameter;

    private Optional<DecodingWorkScheduler> decodingScheduler;

    private ChangeStreamIndexManagerFactory getChangeStreamIndexManagerFactory() {
      var scheduler = spy(DecodingWorkScheduler.create(2, new SimpleMeterRegistry()));
      this.decodingScheduler = Optional.of(scheduler);
      return ChangeStreamManager.indexManagerFactoryWithDecodingScheduler(scheduler);
    }

    @Test
    public void testEmptyBatchEnqueuesIndexingButUpdatesResumeInfoAndCompletesFuture()
        throws Exception {
      IndexingWorkScheduler scheduler = mock(IndexingWorkScheduler.class);
      when(scheduler.schedule(any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));
      AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();
      CompletableFuture<Void> future = new CompletableFuture<>();

      DocumentIndexer indexer = indexer();

      var metricsFactory = SearchIndex.mockMetricsFactory();
      IndexMetricsUpdater metricsUpdater =
          IndexMetricsUpdaterBuilder.builder()
              .metricsFactory(metricsFactory)
              .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
              .build();

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  MOCK_INDEX_DEFINITION,
                  scheduler,
                  indexer,
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  resumeInfoReference::set,
                  metricsUpdater,
                  future,
                  GenerationIdBuilder.create());

      CompletableFuture<Void> indexFuture =
          manager.indexBatch(
                  new ChangeStreamBatch(
                      Collections.emptyList(),
                      ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                      new BsonTimestamp()),
                  NOOP_DOCUMENT_METRICS_UPDATER,
                  NOOP_TIMER)
              .indexingFuture;

      verifyNoMoreInteractions(indexer);
      indexFuture.get(300, TimeUnit.MILLISECONDS);
      assertTrue(indexFuture.isDone());
      Assert.assertFalse(indexFuture.isCompletedExceptionally());
      Assert.assertFalse(future.isDone());

      Assert.assertEquals(ChangeStreamUtils.POST_BATCH_RESUME_INFO, resumeInfoReference.get());
      Assert.assertEquals(
          ResumeTokenUtils.opTimeFromResumeToken(resumeInfoReference.get().getResumeToken())
              .getValue(),
          metricsUpdater
              .getIndexingMetricsUpdater()
              .getMetricsFactory()
              .get("replicationOpTime")
              .gauge()
              .value(),
          0);
    }

    @Test
    public void testBatchEnqueuesIndexingThenUpdatesResumeInfoAndCompletesFuture()
        throws Exception {
      IndexingWorkScheduler scheduler = mock(IndexingWorkScheduler.class);
      AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();
      CompletableFuture<Void> future = new CompletableFuture<>();

      CompletableFuture<Void> schedulerFuture = new CompletableFuture<>();
      when(scheduler.schedule(any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(schedulerFuture);

      DocumentIndexer indexer = indexer();

      var metricsFactory = SearchIndex.mockMetricsFactory();
      IndexMetricsUpdater metricsUpdater =
          IndexMetricsUpdaterBuilder.builder()
              .metricsFactory(metricsFactory)
              .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
              .build();

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  MOCK_INDEX_DEFINITION,
                  scheduler,
                  indexer,
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  resumeInfoReference::set,
                  metricsUpdater,
                  future,
                  GenerationIdBuilder.create());

      CompletableFuture<Void> indexFuture =
          manager.indexBatch(
                  new ChangeStreamBatch(
                      toRawBsonDocuments(
                          Collections.singletonList(
                              ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION))),
                      ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                      new BsonTimestamp()),
                  NOOP_DOCUMENT_METRICS_UPDATER,
                  NOOP_TIMER)
              .indexingFuture;

      Assert.assertThrows(
          TimeoutException.class, () -> indexFuture.get(250, TimeUnit.MILLISECONDS));
      assertNull(resumeInfoReference.get());

      Assert.assertTrue(
          metricsUpdater
              .getIndexingMetricsUpdater()
              .getReplicationOpTimeInfo()
              .snapshot()
              .isEmpty());

      schedulerFuture.complete(null);

      indexFuture.get(5, TimeUnit.SECONDS);
      Assert.assertEquals(ChangeStreamUtils.POST_BATCH_RESUME_INFO, resumeInfoReference.get());
      Assert.assertEquals(
          ResumeTokenUtils.opTimeFromResumeToken(resumeInfoReference.get().getResumeToken())
              .getValue(),
          metricsUpdater
              .getIndexingMetricsUpdater()
              .getReplicationOpTimeInfo()
              .snapshotOrThrow()
              .replicationOpTime(),
          0);

      Assert.assertFalse(future.isDone());
    }

    @Test
    public void testExceptionalBatchIndexingFailsIndexingFuture() throws Exception {
      IndexingWorkScheduler scheduler = mock(IndexingWorkScheduler.class);
      AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();
      CompletableFuture<Void> future = new CompletableFuture<>();

      when(scheduler.schedule(any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

      DocumentIndexer indexer = indexer();

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  MOCK_INDEX_DEFINITION,
                  scheduler,
                  indexer,
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  resumeInfoReference::set,
                  IGNORE_METRICS,
                  future,
                  GenerationIdBuilder.create());

      CompletableFuture<Void> indexFuture =
          manager.indexBatch(
                  new ChangeStreamBatch(
                      toRawBsonDocuments(
                          Collections.singletonList(
                              ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION))),
                      ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                      new BsonTimestamp()),
                  NOOP_DOCUMENT_METRICS_UPDATER,
                  NOOP_TIMER)
              .indexingFuture;

      FutureUtils.swallowedFuture(indexFuture).get();
      Assert.assertFalse(future.isDone());
    }

    @Test
    public void testRenameBatchNoInvalidateUsesPriorEventsResumeInfo() throws Exception {
      ChangeStreamDocument<RawBsonDocument> insertEvent =
          ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION);
      testInvalidatingEventSplitBatchProperlyUpdatesResumeToken(
          Arrays.asList(insertEvent, ChangeStreamUtils.renameEvent(1)),
          Optional.of(insertEvent.getResumeToken()));
    }

    @Test
    public void testRenameBatchNoInvalidateNoPriorEventDoesNotUpdateResumeInfo() throws Exception {
      testInvalidatingEventSplitBatchProperlyUpdatesResumeToken(
          Collections.singletonList(ChangeStreamUtils.renameEvent(0)), Optional.empty());
    }

    @Test
    public void testDropBatchNoInvalidateUsesPriorEventsResumeInfo() throws Exception {
      ChangeStreamDocument<RawBsonDocument> insertEvent =
          ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION);
      testInvalidatingEventSplitBatchProperlyUpdatesResumeToken(
          Arrays.asList(insertEvent, ChangeStreamUtils.dropEvent(1)),
          Optional.of(insertEvent.getResumeToken()));
    }

    @Test
    public void testDropBatchNoInvalidateNoPriorEventDoesNotUpdateResumeInfo() throws Exception {
      testInvalidatingEventSplitBatchProperlyUpdatesResumeToken(
          Collections.singletonList(ChangeStreamUtils.dropEvent(0)), Optional.empty());
    }

    @Test
    public void testDropDatabaseBatchNoInvalidateUsesPriorEventsResumeInfo() throws Exception {
      ChangeStreamDocument<RawBsonDocument> insertEvent =
          ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION);
      testInvalidatingEventSplitBatchProperlyUpdatesResumeToken(
          Arrays.asList(insertEvent, ChangeStreamUtils.dropDatabaseEvent(1)),
          Optional.of(insertEvent.getResumeToken()));
    }

    @Test
    public void testDropDatabaseBatchNoInvalidateNoPriorEventDoesNotUpdateResumeInfo()
        throws Exception {
      testInvalidatingEventSplitBatchProperlyUpdatesResumeToken(
          Collections.singletonList(ChangeStreamUtils.dropDatabaseEvent(0)), Optional.empty());
    }

    @Test
    public void testShutDownCompletesFutureAndDoesNotEnqueueNextBatch() {
      CompletableFuture<Void> future = new CompletableFuture<>();
      IndexingWorkScheduler scheduler = mock(IndexingWorkScheduler.class);
      when(scheduler.cancel(any(), any(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  MOCK_INDEX_DEFINITION,
                  scheduler,
                  indexer(),
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  ignoredResumeInfo -> {},
                  IGNORE_METRICS,
                  future,
                  GenerationIdBuilder.create());

      CompletableFuture<Void> shutdownFuture = manager.shutdown();

      assertTrue(shutdownFuture.isDone());
      Assert.assertFalse(shutdownFuture.isCompletedExceptionally());
      assertTrue(future.isCompletedExceptionally());
      assertFutureCompletesWithSteadyStateException(future, SteadyStateException.Type.SHUT_DOWN);

      // Offering a batch after shutting down should ignore the batch.
      CompletableFuture<Void> indexFuture =
          manager.indexBatch(
                  new ChangeStreamBatch(
                      toRawBsonDocuments(
                          Collections.singletonList(
                              ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION))),
                      ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                      new BsonTimestamp()),
                  NOOP_DOCUMENT_METRICS_UPDATER,
                  NOOP_TIMER)
              .indexingFuture;

      verify(scheduler, never()).schedule(any(), any(), any(), any(), any(), any(), any());
      this.decodingScheduler.ifPresent(
          ds -> verify(ds, never()).schedule(any(), any(), any(), any(), any(), any()));
      assertFutureCompletesWithSteadyStateException(
          indexFuture, SteadyStateException.Type.SHUT_DOWN);
    }

    @Test
    public void testShutDownCompletesOnlyAfterInFlightBatchesAreProcessed() {
      CompletableFuture<Void> future = new CompletableFuture<>();
      IndexingWorkScheduler scheduler = mock(IndexingWorkScheduler.class);

      CompletableFuture<Void> inFlightBatchFuture = new CompletableFuture<>();
      when(scheduler.cancel(any(), any(), any())).thenReturn(inFlightBatchFuture);

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  MOCK_INDEX_DEFINITION,
                  scheduler,
                  indexer(),
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  ignoreResumeInfo -> {},
                  IGNORE_METRICS,
                  future,
                  GenerationIdBuilder.create());

      CompletableFuture<Void> shutdownFuture = manager.shutdown();

      // check that manager is not shut down while batches are processing
      assertFalse(shutdownFuture.isDone());

      // complete processing and check that manager is shut down
      inFlightBatchFuture.complete(null);
      assertTrue(shutdownFuture.isDone());
    }

    @Test
    public void testShutDownDuringIndexingCompletesShutdownFutureWhenDone() throws Exception {
      IndexingWorkScheduler scheduler = scheduler();
      CompletableFuture<Void> future = new CompletableFuture<>();

      CountDownLatch finishIndexing = new CountDownLatch(1);
      Answer<Void> hang =
          invocation -> {
            finishIndexing.await();
            return null;
          };

      DocumentIndexer indexer = indexer();
      doAnswer(hang).when(indexer).indexDocumentEvent(any());

      var metricsFactory = SearchIndex.mockMetricsFactory();
      IndexMetricsUpdater metricsUpdater =
          IndexMetricsUpdaterBuilder.builder()
              .metricsFactory(metricsFactory)
              .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
              .build();

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  MOCK_INDEX_DEFINITION,
                  scheduler,
                  indexer,
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  ignoreResumeInfo -> {},
                  metricsUpdater,
                  future,
                  GenerationIdBuilder.create());

      CompletableFuture<Void> indexFuture =
          manager.indexBatch(
                  new ChangeStreamBatch(
                      toRawBsonDocuments(
                          Collections.singletonList(
                              ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION))),
                      ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                      new BsonTimestamp()),
                  NOOP_DOCUMENT_METRICS_UPDATER,
                  NOOP_TIMER)
              .indexingFuture;

      // Ensure schedule is called before shutting down the manager. Needed for async decoding flow.
      this.decodingScheduler.ifPresent(
          ds ->
              verify(ds, timeout(5000).times(1))
                  .schedule(any(), any(), any(), any(), any(), any()));
      testScheduleCallsWithTimeout(scheduler, 1, 5000);

      // Shut down the manager. The future should not complete until after indexing completes.
      CompletableFuture<Void> shutdownFuture = manager.shutdown();

      Assert.assertThrows(
          TimeoutException.class, () -> indexFuture.get(250, TimeUnit.MILLISECONDS));
      Assert.assertTrue(
          metricsUpdater
              .getIndexingMetricsUpdater()
              .getReplicationOpTimeInfo()
              .snapshot()
              .isEmpty());
      Assert.assertFalse(shutdownFuture.isDone());
      Assert.assertFalse(future.isDone());

      finishIndexing.countDown();
      indexFuture.get(5, TimeUnit.SECONDS);

      // Shutdown future should complete successfully.
      shutdownFuture.get(5, TimeUnit.SECONDS);

      // Future should complete with a SHUT_DOWN SteadyStateException.
      assertFutureCompletesWithSteadyStateException(future, SteadyStateException.Type.SHUT_DOWN);
    }

    private static void assertFutureCompletesWithSteadyStateException(
        CompletableFuture<Void> future, SteadyStateException.Type type) {
      ExecutionException executionException =
          Assert.assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
      assertThat(executionException.getCause()).isInstanceOf(SteadyStateException.class);
      Assert.assertSame(type, ((SteadyStateException) executionException.getCause()).getType());
    }

    private void testInvalidatingEventSplitBatchProperlyUpdatesResumeToken(
        List<ChangeStreamDocument<RawBsonDocument>> events,
        Optional<BsonDocument> expectedResumeToken)
        throws Exception {
      AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();

      CompletableFuture<Void> future = new CompletableFuture<>();

      IndexMetricsUpdater metricsUpdater =
          IndexMetricsUpdaterBuilder.builder()
              .metricsFactory(SearchIndex.mockMetricsFactory())
              .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
              .build();

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  MOCK_INDEX_DEFINITION,
                  scheduler(),
                  indexer(),
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  resumeInfoReference::set,
                  metricsUpdater,
                  future,
                  GenerationIdBuilder.create());

      CompletableFuture<Void> indexFuture =
          manager.indexBatch(
                  new ChangeStreamBatch(
                      toRawBsonDocuments(events),
                      ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                      new BsonTimestamp()),
                  NOOP_DOCUMENT_METRICS_UPDATER,
                  NOOP_TIMER)
              .indexingFuture;

      try {
        indexFuture.get(5, TimeUnit.SECONDS);
      } catch (ExecutionException ex) {
        throw new AssertionError("indexFuture completed exceptionally");
      }

      // Ensure that the ChangeStreamResumeInfo was updated properly.
      Assert.assertEquals(
          expectedResumeToken
              .map(
                  resumeToken ->
                      ChangeStreamResumeInfo.create(
                          ChangeStreamUtils.POST_BATCH_RESUME_INFO.getNamespace(), resumeToken))
              .orElse(null),
          resumeInfoReference.get());
    }

    @Test
    public void testHandlesDropEvent() {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.dropEvent(1),
              ChangeStreamUtils.invalidateEvent(2));
      testThrowsLifecycleException(events, SteadyStateException.Type.DROPPED);
    }

    /**
     * Our collection may be dropped when a different collection is renamed to our namespace with
     * dropTarget=true, the change-stream event in this case will be a rename and not a drop for the
     * dropped collection! This may happen as a result of an $out stage too:
     * https://docs.mongodb.com/v4.2/reference/operator/aggregation/out/index.html#replace-existing-collection
     */
    @Test
    public void testCollectionDroppedDueToRenameEventHandledAsDrop() {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.renameEvent(
                  1, ChangeStreamUtils.OTHER_NAMESPACE, ChangeStreamUtils.NAMESPACE),
              ChangeStreamUtils.invalidateEvent(2));
      testThrowsLifecycleException(events, SteadyStateException.Type.DROPPED);
    }

    @Test
    public void testNonInvalidatingResyncExceptionThrownDueToRenameEventWithNoDestination() {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.renameEvent(1, ChangeStreamUtils.OTHER_NAMESPACE, null),
              ChangeStreamUtils.invalidateEvent(2));
      testThrowsLifecycleException(events, Type.NON_INVALIDATING_RESYNC);
    }

    @Test
    public void testHandlesDropDatabaseEvent() {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.dropDatabaseEvent(1),
              ChangeStreamUtils.invalidateEvent(2));
      testThrowsLifecycleException(events, SteadyStateException.Type.DROPPED);
    }

    @Test
    public void testHandlesRenameEvent() {

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.renameEvent(1),
              ChangeStreamUtils.invalidateEvent(2));
      var rename = testThrowsLifecycleException(events, SteadyStateException.Type.RENAMED);
      Assert.assertEquals(ChangeStreamUtils.OTHER_NAMESPACE, rename.getResumeInfo().getNamespace());
    }

    @Test
    public void testHandlesRenameEventWithCorrectResumeInfo_3Events() throws Exception {
      IndexingWorkScheduler scheduler = mock(IndexingWorkScheduler.class);
      when(scheduler.schedule(any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));
      AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();
      CompletableFuture<Void> future = new CompletableFuture<>();

      DocumentIndexer indexer = indexer();

      var metricsFactory = SearchIndex.mockMetricsFactory();
      IndexMetricsUpdater metricsUpdater =
          IndexMetricsUpdaterBuilder.builder()
              .metricsFactory(metricsFactory)
              .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
              .build();

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  MOCK_INDEX_DEFINITION,
                  scheduler,
                  indexer,
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  resumeInfoReference::set,
                  metricsUpdater,
                  future,
                  GenerationIdBuilder.create());

      CompletableFuture<Void> indexFuture =
          manager.indexBatch(
                  new ChangeStreamBatch(
                      toRawBsonDocuments(
                          List.of(
                              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
                              ChangeStreamUtils.renameEvent(1),
                              ChangeStreamUtils.invalidateEvent(2))),
                      ChangeStreamUtils.PRE_BATCH_RESUME_TOKEN,
                      new BsonTimestamp()),
                  NOOP_DOCUMENT_METRICS_UPDATER,
                  NOOP_TIMER)
              .indexingFuture;

      verifyNoMoreInteractions(indexer);
      indexFuture.get(300, TimeUnit.MILLISECONDS);
      assertTrue(indexFuture.isDone());
      assertTrue(future.isCompletedExceptionally());

      // Should be the resume token of the insert event
      Assert.assertEquals(
          ChangeStreamUtils.resumeToken(new BsonTimestamp(1, 0)),
          resumeInfoReference.get().getResumeToken());
    }

    @Test
    public void testHandlesRenameEventWithCorrectResumeInfo_2Events() throws Exception {
      IndexingWorkScheduler scheduler = mock(IndexingWorkScheduler.class);
      when(scheduler.schedule(any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));
      AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();
      CompletableFuture<Void> future = new CompletableFuture<>();

      DocumentIndexer indexer = indexer();

      var metricsFactory = SearchIndex.mockMetricsFactory();
      IndexMetricsUpdater metricsUpdater =
          IndexMetricsUpdaterBuilder.builder()
              .metricsFactory(metricsFactory)
              .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
              .build();

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  MOCK_INDEX_DEFINITION,
                  scheduler,
                  indexer,
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  resumeInfoReference::set,
                  metricsUpdater,
                  future,
                  GenerationIdBuilder.create());

      CompletableFuture<Void> indexFuture =
          manager.indexBatch(
                  new ChangeStreamBatch(
                      toRawBsonDocuments(
                          List.of(
                              ChangeStreamUtils.renameEvent(1),
                              ChangeStreamUtils.invalidateEvent(2))),
                      ChangeStreamUtils.PRE_BATCH_RESUME_TOKEN,
                      new BsonTimestamp()),
                  NOOP_DOCUMENT_METRICS_UPDATER,
                  NOOP_TIMER)
              .indexingFuture;

      verifyNoMoreInteractions(indexer);
      indexFuture.get(300, TimeUnit.MILLISECONDS);
      assertTrue(indexFuture.isDone());
      assertTrue(future.isCompletedExceptionally());

      // Should be the resume info before the rename (pre-batch), so empty resume info
      assertNull(resumeInfoReference.get());
    }

    @Test
    public void testHandlesRenameEventNotFollowedByInvalidate() {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.renameEvent(1),
              ChangeStreamUtils.insertEvent(2, MOCK_INDEX_DEFINITION));

      // Lifecycle events at the *middle* of a change stream batch (unexpected) are detected while
      // preparing the batch for indexing. This leads to the indexing future failing exceptionally,
      // which will fail the lifecycle future in ChangeStreamManager exception handling stage.
      testIndexingFutureEndsExceptionally(events, SteadyStateException.Type.REQUIRES_RESYNC);
    }

    @Test
    public void testHandlesRenameEventSplitFromInvalidate() throws Exception {
      List<RawBsonDocument> events =
          toRawBsonDocuments(
              Arrays.asList(
                  ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.renameEvent(1)));

      AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();

      CompletableFuture<Void> future = new CompletableFuture<>();

      IndexingWorkScheduler scheduler = mock(IndexingWorkScheduler.class);
      when(scheduler.schedule(any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  MOCK_INDEX_DEFINITION,
                  scheduler,
                  indexer(),
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  resumeInfoReference::set,
                  IGNORE_METRICS,
                  future,
                  GenerationIdBuilder.create());

      CompletableFuture<Void> indexFuture =
          manager.indexBatch(
                  new ChangeStreamBatch(
                      events, ChangeStreamUtils.POST_BATCH_RESUME_TOKEN, new BsonTimestamp()),
                  NOOP_DOCUMENT_METRICS_UPDATER,
                  NOOP_TIMER)
              .indexingFuture;

      indexFuture.get(5, TimeUnit.SECONDS);

      ChangeStreamDocument<RawBsonDocument> invalidateEvent = ChangeStreamUtils.invalidateEvent(2);
      manager.indexBatch(
          new ChangeStreamBatch(
              toRawBsonDocuments(List.of(invalidateEvent)),
              invalidateEvent.getResumeToken(),
              new BsonTimestamp()),
          NOOP_DOCUMENT_METRICS_UPDATER,
          NOOP_TIMER);

      try {
        future.get(5, TimeUnit.SECONDS);
      } catch (ExecutionException ex) {
        assertThat(ex.getCause()).isInstanceOf(SteadyStateException.class);
        SteadyStateException steady = (SteadyStateException) ex.getCause();
        assertSame(SteadyStateException.Type.RENAMED, steady.getType());
        return;
      }

      throw new AssertionError("Expected an exception of type %s, but nothing was thrown!");
    }

    @Test
    public void testHandlesInvalidateEvent() {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.invalidateEvent(1));
      var invalidate = testThrowsLifecycleException(events, SteadyStateException.Type.INVALIDATED);
      Assert.assertEquals(ChangeStreamUtils.NAMESPACE, invalidate.getResumeInfo().getNamespace());
    }

    /**
     * When the work scheduler throws a {@link ExceededLimitsException}, it should be wrapped with
     * an FIELD_EXCEEDED SteadyStateException.
     */
    @Test
    public void testIndexingLimitExceededThrowsExceededSteadyStateException() {
      CompletableFuture<Void> lifecycleFuture = new CompletableFuture<>();

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  MOCK_INDEX_DEFINITION,
                  scheduler(),
                  com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
                      .mockFieldLimitsExceeded(),
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  ignoreResumeInfo -> {},
                  IGNORE_METRICS,
                  lifecycleFuture,
                  GenerationIdBuilder.create());

      CompletableFuture<Void> indexFuture =
          manager.indexBatch(
                  new ChangeStreamBatch(
                      Collections.emptyList(),
                      ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                      new BsonTimestamp()),
                  NOOP_DOCUMENT_METRICS_UPDATER,
                  NOOP_TIMER)
              .indexingFuture;

      var ex =
          Assert.assertThrows(ExecutionException.class, () -> indexFuture.get(5, TimeUnit.SECONDS));
      assertThat(ex.getCause()).isInstanceOf(SteadyStateException.class);
      assertSame(
          SteadyStateException.Type.FIELD_EXCEEDED,
          ((SteadyStateException) ex.getCause()).getType());
    }

    @Test
    public void testHandlesOther() {

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.otherEvent(1));
      testThrowsLifecycleException(events, SteadyStateException.Type.REQUIRES_RESYNC);
    }

    @Test
    public void testHandlesOtherOnView() {

      IndexDefinition indexDefinition =
          SearchIndex.mockDefinitionBuilder()
              .view(ViewDefinition.existing("test", List.of()))
              .build();

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, indexDefinition), ChangeStreamUtils.otherEvent(1));
      testThrowsLifecycleException(events, SteadyStateException.Type.REQUIRES_RESYNC);
    }

    @Test
    public void testHandlesInsert() throws Exception {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION));
      RawBsonDocument document = events.get(0).getFullDocument();
      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromOriginalDocument(Optional.of(document)), document)),
          MOCK_INDEX_DEFINITION);
    }

    @Test
    public void testHandlesInsertOnView() throws Exception {

      IndexDefinition indexDefinition =
          SearchIndex.mockDefinitionBuilder()
              .view(ViewDefinition.existing("test", List.of()))
              .build();

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(ChangeStreamUtils.insertEvent(0, indexDefinition));
      RawBsonDocument document = events.get(0).getFullDocument();
      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(
                      Optional.of(document), indexDefinition.getIndexId()),
                  document)),
          indexDefinition);
    }

    @Test
    public void testHandlesUpdate() throws Exception {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.updateEvent(
                  0, new UpdateDescription(List.of("a"), null), MOCK_INDEX_DEFINITION));
      RawBsonDocument document = events.get(0).getFullDocument();
      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createUpdate(
                  DocumentMetadata.fromOriginalDocument(Optional.of(document)), document)),
          MOCK_INDEX_DEFINITION);
    }

    @Test
    public void testHandlesUpdateOnView() throws Exception {

      IndexDefinition indexDefinition =
          SearchIndex.mockDefinitionBuilder()
              .view(ViewDefinition.existing("test", List.of()))
              .build();

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.updateEvent(
                  0, new UpdateDescription(List.of("a"), null), indexDefinition));
      RawBsonDocument document = events.get(0).getFullDocument();
      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createUpdate(
                  DocumentMetadata.fromMetadataNamespace(
                      Optional.of(document), indexDefinition.getIndexId()),
                  document)),
          indexDefinition);
    }

    @Test
    public void testHandlesUpdateNoData() throws Exception {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.updateNoDataEvent(0, new UpdateDescription(List.of("a"), null)));
      var document = events.get(0);

      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(DocumentMetadata.extractId(document.getDocumentKey()))),
          MOCK_INDEX_DEFINITION);
    }

    @Test
    public void testHandlesUpdateNoDataOnView() throws Exception {

      IndexDefinition indexDefinition =
          SearchIndex.mockDefinitionBuilder()
              .view(ViewDefinition.existing("test", List.of()))
              .build();

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.updateNoDataEvent(0, new UpdateDescription(List.of("a"), null)));
      var document = events.get(0);

      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(DocumentMetadata.extractId(document.getDocumentKey()))),
          indexDefinition);
    }

    @Test
    public void testHandlesUpdateWithEmptyMetadata() throws Exception {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.updateEventWithEmptyMetadata(
                  0, new UpdateDescription(List.of("a"), null), MOCK_INDEX_DEFINITION));
      var document = events.get(0);

      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(DocumentMetadata.extractId(document.getDocumentKey()))),
          MOCK_INDEX_DEFINITION);
    }

    @Test
    public void testHandlesUpdateWithEmptyMetadataOnView() throws Exception {

      IndexDefinition indexDefinition =
          SearchIndex.mockDefinitionBuilder()
              .view(ViewDefinition.existing("test", List.of()))
              .build();

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.updateEventWithEmptyMetadata(
                  0, new UpdateDescription(List.of("a"), null), indexDefinition));
      var document = events.get(0);

      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(DocumentMetadata.extractId(document.getDocumentKey()))),
          indexDefinition);
    }

    @Test
    public void testHandlesUpdateWithNoMetadata() throws Exception {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.updateEventWithNoMetadata(
                  0, new UpdateDescription(List.of("a"), null)));
      var document = events.get(0);

      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(DocumentMetadata.extractId(document.getDocumentKey()))),
          MOCK_INDEX_DEFINITION);
    }

    @Test
    public void testHandlesUpdateWithNoMetadataOnView() throws Exception {

      IndexDefinition indexDefinition =
          SearchIndex.mockDefinitionBuilder()
              .view(ViewDefinition.existing("test", List.of()))
              .build();

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.updateEventWithNoMetadata(
                  0, new UpdateDescription(List.of("a"), null)));
      var document = events.get(0);

      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(DocumentMetadata.extractId(document.getDocumentKey()))),
          indexDefinition);
    }

    @Test
    public void testHandlesInsertWithNoMetadata() throws Exception {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(ChangeStreamUtils.insertEventWithNoMetadata(0));
      RawBsonDocument document = events.get(0).getFullDocument();
      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromOriginalDocument(Optional.of(document)), document)),
          MOCK_INDEX_DEFINITION);
    }

    @Test
    public void testHandlesInsertWithNoMetadataOnView() throws Exception {

      IndexDefinition indexDefinition =
          SearchIndex.mockDefinitionBuilder()
              .view(ViewDefinition.existing("test", List.of()))
              .build();

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.dataEvent(
                  OperationType.INSERT,
                  0,
                  null,
                  new BsonDocument()
                      .append("_id", new BsonInt32(0))
                      .append("field", new BsonString("test"))));
      var document = events.get(0);
      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(DocumentMetadata.extractId(document.getDocumentKey()))),
          indexDefinition);
    }

    @Test
    public void testHandlesInsertWithVirtualDeleteOnView() throws Exception {

      IndexDefinition indexDefinition =
          SearchIndex.mockDefinitionBuilder()
              .view(ViewDefinition.existing("test", List.of()))
              .build();

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.dataEvent(
                  OperationType.INSERT,
                  0,
                  null,
                  new BsonDocument()
                      .append("_id", new BsonInt32(0))
                      .append(
                          indexDefinition.getIndexId().toString(),
                          new BsonDocument()
                              .append("_id", new BsonInt32(0))
                              .append("deleted", new BsonBoolean(true)))));
      var document = events.get(0);

      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(DocumentMetadata.extractId(document.getDocumentKey()))),
          indexDefinition);
    }

    @Test
    public void testHandlesUpdateWithVirtualDeleteOnView() throws Exception {

      IndexDefinition indexDefinition =
          SearchIndex.mockDefinitionBuilder()
              .view(ViewDefinition.existing("test", List.of()))
              .build();

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.dataEvent(
                  OperationType.UPDATE,
                  0,
                  null,
                  new BsonDocument()
                      .append("_id", new BsonInt32(0))
                      .append(
                          indexDefinition.getIndexId().toString(),
                          new BsonDocument()
                              .append("_id", new BsonInt32(0))
                              .append("deleted", new BsonBoolean(true)))));
      var document = events.get(0);

      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(DocumentMetadata.extractId(document.getDocumentKey()))),
          indexDefinition);
    }

    @Test
    public void testHandlesReplaceWithVirtualDeleteOnView() throws Exception {

      IndexDefinition indexDefinition =
          SearchIndex.mockDefinitionBuilder()
              .view(ViewDefinition.existing("test", List.of()))
              .build();

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.dataEvent(
                  OperationType.REPLACE,
                  0,
                  null,
                  new BsonDocument()
                      .append("_id", new BsonInt32(0))
                      .append(
                          indexDefinition.getIndexId().toString(),
                          new BsonDocument()
                              .append("_id", new BsonInt32(0))
                              .append("deleted", new BsonBoolean(true)))));
      var document = events.get(0);

      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(DocumentMetadata.extractId(document.getDocumentKey()))),
          indexDefinition);
    }

    @Test
    public void testVirtualDeleteIsIgnoredForLifecycleEvents() throws Exception {

      IndexDefinition indexDefinition =
          SearchIndex.mockDefinitionBuilder()
              .view(ViewDefinition.existing("test", List.of()))
              .build();

      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.dataEvent(
                  OperationType.REPLACE,
                  0,
                  null,
                  new BsonDocument()
                      .append("_id", new BsonInt32(0))
                      .append(
                          indexDefinition.getIndexId().toString(),
                          new BsonDocument()
                              .append("_id", new BsonInt32(0))
                              .append("deleted", new BsonBoolean(true)))));
      var document = events.get(0);

      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(DocumentMetadata.extractId(document.getDocumentKey()))),
          indexDefinition);
    }

    @Test
    public void testFiltersUpdates() throws Exception {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.updateEvent(
                  0, new UpdateDescription(List.of(), null), MOCK_INDEX_DEFINITION));

      // no changes, as our only change does not apply to our index
      testSuccessfulBatch(events, List.of(), MOCK_INDEX_DEFINITION);
    }

    @Test
    public void testFiltersUpdatesWithOthers() throws Exception {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.updateEvent(
                  0, new UpdateDescription(List.of(), null), MOCK_INDEX_DEFINITION));
      RawBsonDocument document = events.get(0).getFullDocument();
      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromOriginalDocument(Optional.of(document)), document)),
          MOCK_INDEX_DEFINITION);
    }

    @Test
    public void testHandlesReplace() throws Exception {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(ChangeStreamUtils.replaceEvent(0, MOCK_INDEX_DEFINITION));
      RawBsonDocument document = events.get(0).getFullDocument();
      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createUpdate(
                  DocumentMetadata.fromOriginalDocument(Optional.of(document)), document)),
          MOCK_INDEX_DEFINITION);
    }

    @Test
    public void testHandlesDelete() throws Exception {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(ChangeStreamUtils.deleteEvent(0, MOCK_INDEX_DEFINITION));
      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(
                  DocumentMetadata.extractId(events.get(0).getDocumentKey()))),
          MOCK_INDEX_DEFINITION);
    }

    @Test
    public void testLastEventPerDocument() throws Exception {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.updateEvent(
                  0, new UpdateDescription(List.of("a"), null), MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.deleteEvent(1, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.updateEvent(
                  2, new UpdateDescription(List.of(), null), MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.updateEvent(
                  2, new UpdateDescription(List.of(), null), MOCK_INDEX_DEFINITION));
      testSuccessfulBatch(
          events,
          List.of(
              DocumentEvent.createDelete(
                  DocumentMetadata.extractId(events.get(2).getDocumentKey())),
              DocumentEvent.createUpdate(
                  DocumentMetadata.fromOriginalDocument(
                      Optional.of(events.get(1).getFullDocument())),
                  events.get(1).getFullDocument())),
          MOCK_INDEX_DEFINITION);
    }

    private SteadyStateException testThrowsLifecycleException(
        List<ChangeStreamDocument<RawBsonDocument>> events, SteadyStateException.Type expected) {
      AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();

      CompletableFuture<Void> future = new CompletableFuture<>();

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  MOCK_INDEX_DEFINITION,
                  scheduler(),
                  indexer(),
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  resumeInfoReference::set,
                  IGNORE_METRICS,
                  future,
                  GenerationIdBuilder.create());

      manager.indexBatch(
          new ChangeStreamBatch(
              toRawBsonDocuments(events),
              ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
              new BsonTimestamp()),
          NOOP_DOCUMENT_METRICS_UPDATER,
          NOOP_TIMER);

      ExecutionException ex =
          Assert.assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
      assertTrue(ex.getCause() instanceof SteadyStateException);
      SteadyStateException steady = (SteadyStateException) ex.getCause();
      assertSame(expected, steady.getType());
      return steady;
    }

    private void testSuccessfulBatch(
        List<ChangeStreamDocument<RawBsonDocument>> events,
        List<DocumentEvent> batch,
        IndexDefinition indexDefinition)
        throws Exception {
      AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();

      CompletableFuture<Void> future = new CompletableFuture<>();

      IndexingWorkScheduler scheduler = mock(IndexingWorkScheduler.class);
      when(scheduler.schedule(any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));

      ChangeStreamIndexManager manager =
          getChangeStreamIndexManagerFactory()
              .create(
                  indexDefinition,
                  scheduler,
                  indexer(),
                  ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
                  resumeInfoReference::set,
                  IGNORE_METRICS,
                  future,
                  GenerationIdBuilder.create());

      CompletableFuture<Void> indexFuture =
          manager.indexBatch(
                  new ChangeStreamBatch(
                      toRawBsonDocuments(events),
                      ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                      new BsonTimestamp()),
                  NOOP_DOCUMENT_METRICS_UPDATER,
                  NOOP_TIMER)
              .indexingFuture;

      indexFuture.get(5, TimeUnit.SECONDS);

      verify(scheduler, times(1))
          .schedule(argThat(batch::equals), any(), any(), any(), any(), any(), any());
    }
  }

  public static class DecodingWorkSchedulerTests {

    @Test
    public void testHandlesRenameEventNotFollowedByInvalidate() {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.dataEvent(OperationType.INSERT, 0, null, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.renameEvent(1),
              ChangeStreamUtils.dataEvent(OperationType.INSERT, 2, null, MOCK_INDEX_DEFINITION));
      testIndexingFutureEndsExceptionally(events, SteadyStateException.Type.REQUIRES_RESYNC);
    }

    @Test
    public void testHandlesOtherInBatch() {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.renameEvent(1),
              ChangeStreamUtils.insertEvent(2, MOCK_INDEX_DEFINITION));
      testIndexingFutureEndsExceptionally(events, SteadyStateException.Type.REQUIRES_RESYNC);
    }

    @Test
    public void testHandlesInvalidateInBatch() {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.invalidateEvent(1),
              ChangeStreamUtils.insertEvent(2, MOCK_INDEX_DEFINITION));
      testIndexingFutureEndsExceptionally(events, SteadyStateException.Type.REQUIRES_RESYNC);
    }

    @Test
    public void testHandlesDropInBatch() {
      List<ChangeStreamDocument<RawBsonDocument>> events =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.dropEvent(1),
              ChangeStreamUtils.insertEvent(2, MOCK_INDEX_DEFINITION));
      testIndexingFutureEndsExceptionally(events, Type.DROPPED);
    }

    @Test
    public void testGetIndexableChangeStreamEvents() {
      DecodingExecutorChangeStreamIndexManager indexManager = createDecodingIndexManager();

      List<ChangeStreamDocument<RawBsonDocument>> allIndexable =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.updateEvent(2, null, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.deleteEvent(3, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.replaceEvent(4, MOCK_INDEX_DEFINITION));

      {
        var decodedEventsView =
            indexManager.getIndexableChangeStreamEvents(
                ChangeStreamUtils.toRawBsonDocuments(allIndexable), false);

        Assert.assertEquals(allIndexable, decodedEventsView);
      }

      {
        // Split rename at end
        List<ChangeStreamDocument<RawBsonDocument>> renameAtEnd = new ArrayList<>(allIndexable);
        renameAtEnd.add(ChangeStreamUtils.renameEvent(5));

        Assert.assertEquals(
            allIndexable,
            indexManager.getIndexableChangeStreamEvents(
                ChangeStreamUtils.toRawBsonDocuments(renameAtEnd), true));

        Assert.assertEquals(
            List.of(),
            indexManager.getIndexableChangeStreamEvents(
                ChangeStreamUtils.toRawBsonDocuments(List.of(ChangeStreamUtils.invalidateEvent(6))),
                true));
      }

      {
        // RENAME + INVALIDATE at end
        List<ChangeStreamDocument<RawBsonDocument>> renameAtEnd = new ArrayList<>(allIndexable);
        renameAtEnd.addAll(
            List.of(ChangeStreamUtils.renameEvent(5), ChangeStreamUtils.invalidateEvent(6)));

        Assert.assertEquals(
            allIndexable,
            indexManager.getIndexableChangeStreamEvents(
                ChangeStreamUtils.toRawBsonDocuments(renameAtEnd), true));
      }

      {
        // empty list with lifecycle event
        Assert.assertEquals(
            List.of(), indexManager.getIndexableChangeStreamEvents(List.of(), true));
      }

      {
        // empty list without lifecycle event
        Assert.assertEquals(
            List.of(), indexManager.getIndexableChangeStreamEvents(List.of(), false));
      }
    }

    @Test
    public void testGetIndexableChangeStreamEvents_throwsOnOtherEvent() {
      List<ChangeStreamDocument<RawBsonDocument>> otherInMiddle =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.updateEvent(2, null, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.otherEvent(3),
              ChangeStreamUtils.deleteEvent(4, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.replaceEvent(5, MOCK_INDEX_DEFINITION));

      testDecodedListViewValidation(otherInMiddle, Type.REQUIRES_RESYNC);
    }

    @Test
    public void testGetIndexableChangeStreamEvents_throwsOnRenameEvent() {
      List<ChangeStreamDocument<RawBsonDocument>> otherInMiddle =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.updateEvent(2, null, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.renameEvent(3),
              ChangeStreamUtils.deleteEvent(4, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.replaceEvent(5, MOCK_INDEX_DEFINITION));

      testDecodedListViewValidation(otherInMiddle, Type.REQUIRES_RESYNC);
    }

    @Test
    public void testGetIndexableChangeStreamEvents_throwsOnInvalidateEvent() {
      List<ChangeStreamDocument<RawBsonDocument>> otherInMiddle =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.updateEvent(2, null, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.invalidateEvent(3),
              ChangeStreamUtils.deleteEvent(4, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.replaceEvent(5, MOCK_INDEX_DEFINITION));

      testDecodedListViewValidation(otherInMiddle, Type.REQUIRES_RESYNC);
    }

    @Test
    public void testGetIndexableChangeStreamEvents_throwsOnDropEvent() {
      List<ChangeStreamDocument<RawBsonDocument>> otherInMiddle =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.updateEvent(2, null, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.dropEvent(3),
              ChangeStreamUtils.deleteEvent(4, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.replaceEvent(5, MOCK_INDEX_DEFINITION));

      testDecodedListViewValidation(otherInMiddle, Type.DROPPED);
    }

    @Test
    public void testGetIndexableChangeStreamEvents_throwsOnDropDatabaseEvent() {
      List<ChangeStreamDocument<RawBsonDocument>> otherInMiddle =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.updateEvent(2, null, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.dropDatabaseEvent(3),
              ChangeStreamUtils.deleteEvent(4, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.replaceEvent(5, MOCK_INDEX_DEFINITION));

      testDecodedListViewValidation(otherInMiddle, Type.DROPPED);
    }

    @Test
    public void testBatchesAreDecodedAndEnqueuedForIndexingInOrder() throws Exception {
      CompletableFuture<Void> future = new CompletableFuture<>();
      AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();

      IndexingWorkScheduler scheduler = mock(IndexingWorkScheduler.class);
      when(scheduler.schedule(any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));

      var decodingScheduler = spy(DecodingWorkScheduler.create(4, new SimpleMeterRegistry()));

      ChangeStreamIndexManager manager =
          DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
              MOCK_INDEX_DEFINITION,
              scheduler,
              indexer(),
              ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
              resumeInfoReference::set,
              IGNORE_METRICS,
              future,
              MOCK_INDEX_GENERATION_ID,
              decodingScheduler);

      var preBatchBarrier = new CountDownLatch(1);
      var postBatchBarrier = new CountDownLatch(1);

      List<ChangeStreamDocument<RawBsonDocument>> firstBatchEvents =
          List.of(
              ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION));

      // Inject pause in getRawEvents() just before async decoding, allowing indexManager to look
      // up for lifecycle events prior to decoding.
      AtomicBoolean blockFirstBatch = new AtomicBoolean(false);
      doAnswer(
              invocation -> {
                blockFirstBatch.set(true);
                return invocation.callRealMethod();
              })
          .when(decodingScheduler)
          .schedule(any(), any(), any(), any(), any(), any());

      var firstBatch = mock(ChangeStreamBatch.class);
      when(firstBatch.getCommandOperationTime()).thenReturn(new BsonTimestamp());
      when(firstBatch.getPostBatchResumeToken())
          .thenReturn(ChangeStreamUtils.resumeToken(new BsonTimestamp(3, 0)));
      when(firstBatch.getRawEvents())
          .thenAnswer(
              ignored -> {
                if (blockFirstBatch.get()) {
                  preBatchBarrier.countDown();
                  postBatchBarrier.await();
                }
                return toRawBsonDocuments(firstBatchEvents);
              });

      List<ChangeStreamDocument<RawBsonDocument>> secondBatchEvents =
          List.of(
              ChangeStreamUtils.insertEvent(2, MOCK_INDEX_DEFINITION),
              ChangeStreamUtils.insertEvent(3, MOCK_INDEX_DEFINITION));

      var secondBatch =
          new ChangeStreamBatch(
              toRawBsonDocuments(secondBatchEvents),
              ChangeStreamUtils.resumeToken(new BsonTimestamp(5, 0)),
              new BsonTimestamp());

      // Call index on first batch
      CompletableFuture<Void> indexFuture1 =
          manager.indexBatch(firstBatch, NOOP_DOCUMENT_METRICS_UPDATER, NOOP_TIMER).indexingFuture;

      // Wait for decoding on first batch to start and ensure is blocked
      verify(decodingScheduler)
          .schedule(eq(MOCK_INDEX_GENERATION_ID), any(), any(), any(), any(), any());
      Assert.assertTrue(preBatchBarrier.await(5, TimeUnit.SECONDS));

      // Schedule a second batch for indexing
      CompletableFuture<Void> indexFuture2 =
          manager.indexBatch(secondBatch, NOOP_DOCUMENT_METRICS_UPDATER, NOOP_TIMER).indexingFuture;
      verify(decodingScheduler, times(2))
          .schedule(eq(MOCK_INDEX_GENERATION_ID), any(), any(), any(), any(), any());

      // Wait for 5 seconds to ensure 1. the first index is not complete and 2. the second index
      // is not scheduled for indexing.
      Assert.assertThrows(TimeoutException.class, () -> indexFuture1.get(5, TimeUnit.SECONDS));
      verify(scheduler, never()).schedule(any(), any(), any(), any(), any(), any(), any());

      // Release the first batch. Verify it got scheduled.
      var inOrder = Mockito.inOrder(scheduler);
      postBatchBarrier.countDown();
      indexFuture1.get(5, TimeUnit.SECONDS);
      inOrder
          .verify(scheduler, times(1))
          .schedule(
              argThat(
                  (scheduledEvents) ->
                      scheduledEvents.equals(
                          Lists.reverse(
                              firstBatchEvents.stream()
                                  .map(
                                      e ->
                                          DocumentEvent.createInsert(
                                              DocumentMetadata.fromMetadataNamespace(
                                                  Optional.of(e.getFullDocument()),
                                                  MOCK_INDEX_DEFINITION.getIndexId()),
                                              e.getFullDocument()))
                                  .collect(Collectors.toList())))),
              any(),
              any(),
              any(),
              any(),
              any(),
              any());

      // Verify the second batch is scheduled.
      indexFuture2.get(5, TimeUnit.SECONDS);
      inOrder
          .verify(scheduler, times(1))
          .schedule(
              argThat(
                  (scheduledEvents) ->
                      scheduledEvents.equals(
                          Lists.reverse(
                              secondBatchEvents.stream()
                                  .map(
                                      e ->
                                          DocumentEvent.createInsert(
                                              DocumentMetadata.fromMetadataNamespace(
                                                  Optional.of(e.getFullDocument()),
                                                  MOCK_INDEX_DEFINITION.getIndexId()),
                                              e.getFullDocument()))
                                  .collect(Collectors.toList())))),
              any(),
              any(),
              any(),
              any(),
              any(),
              any());
    }

    @Test
    public void testExceptionalBatchDecodingFailsIndexingFuture() throws Exception {
      IndexingWorkScheduler scheduler = mock(IndexingWorkScheduler.class);
      DecodingWorkScheduler decodingScheduler = mock(DecodingWorkScheduler.class);

      AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();
      CompletableFuture<Void> future = new CompletableFuture<>();

      when(decodingScheduler.schedule(any(), any(), any(), any(), any(), any()))
          .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

      DocumentIndexer indexer = indexer();

      DecodingExecutorChangeStreamIndexManager indexManager =
          DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
              MOCK_INDEX_DEFINITION,
              scheduler,
              indexer,
              ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
              resumeInfoReference::set,
              IGNORE_METRICS,
              future,
              GenerationIdBuilder.create(),
              decodingScheduler);

      CompletableFuture<Void> indexFuture =
          indexManager.indexBatch(
                  new ChangeStreamBatch(
                      toRawBsonDocuments(
                          Collections.singletonList(
                              ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION))),
                      ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                      new BsonTimestamp()),
                  NOOP_DOCUMENT_METRICS_UPDATER,
                  NOOP_TIMER)
              .indexingFuture;

      FutureUtils.swallowedFuture(indexFuture).get();
      Assert.assertFalse(future.isDone());
    }

    @Test
    public void testShutdownWaitsUntilAllScheduledBatchesComplete() throws Exception {
      SimpleMeterRegistry registry = new SimpleMeterRegistry();
      IndexingWorkScheduler scheduler = scheduler();
      DecodingWorkScheduler decodingScheduler = spy(DecodingWorkScheduler.create(2, registry));

      CyclicBarrier finishIndexing = new CyclicBarrier(2);
      CyclicBarrier finishDecoding = new CyclicBarrier(2);

      DocumentIndexer indexer = mock(DocumentIndexer.class);

      // allow block indexing
      doAnswer(
              invocation -> {
                finishIndexing.await();
                return null;
              })
          .when(indexer)
          .indexDocumentEvent(any());

      // create an hanging change-stream for second batch
      ChangeStreamBatch hangingBatch = mock(ChangeStreamBatch.class);
      when(hangingBatch.getCommandOperationTime()).thenReturn(new BsonTimestamp());
      when(hangingBatch.getPostBatchResumeToken())
          .thenReturn(ChangeStreamUtils.resumeToken(new BsonTimestamp(3, 0)));

      // allow calls `getRawEvents()` in pre-processing by this thread (total 6 calls), after hang.
      AtomicInteger blockDecoding = new AtomicInteger(-6);
      when(hangingBatch.getRawEvents())
          .thenAnswer(
              invocation -> {
                if (blockDecoding.getAndIncrement() >= 0) {
                  finishDecoding.await();
                }
                return ChangeStreamUtils.toRawBsonDocuments(
                    List.of(ChangeStreamUtils.insertEvent(9, MOCK_INDEX_DEFINITION)));
              });

      CompletableFuture<Void> lifecycleFuture = new CompletableFuture<>();

      ChangeStreamIndexManager manager =
          DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
              MOCK_INDEX_DEFINITION,
              scheduler,
              indexer,
              ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
              ignoreResumeInfo -> {},
              IGNORE_METRICS,
              lifecycleFuture,
              GenerationIdBuilder.create(),
              decodingScheduler);

      BatchInfo firstBatchInfo =
          manager.indexBatch(
              new ChangeStreamBatch(
                  ChangeStreamUtils.toRawBsonDocuments(
                      List.of(ChangeStreamUtils.insertEvent(6, MOCK_INDEX_DEFINITION))),
                  POST_BATCH_RESUME_TOKEN,
                  new BsonTimestamp()),
              NOOP_DOCUMENT_METRICS_UPDATER,
              NOOP_TIMER);

      BatchInfo secondBatchInfo =
          manager.indexBatch(hangingBatch, NOOP_DOCUMENT_METRICS_UPDATER, NOOP_TIMER);

      verify(decodingScheduler, times(2)).schedule(any(), any(), any(), any(), any(), any());
      testScheduleCallsWithTimeout(scheduler, 1, 500);
      verify(indexer, timeout(500).times(1)).indexDocumentEvent(any());

      // call shutdown
      CompletableFuture<Void> shutdownFuture = manager.shutdown();

      // finish indexing of first batch and decoding of second
      finishIndexing.await(5, TimeUnit.SECONDS);
      finishDecoding.await(5, TimeUnit.SECONDS);

      // wait for the second batch to be scheduled for indexing
      testScheduleCallsWithTimeout(scheduler, 2, 500);
      verify(indexer, timeout(500).times(2)).indexDocumentEvent(any());

      // check that manager is not shut down while batches are processing
      assertFalse(shutdownFuture.isDone());

      // finish indexing of second batch
      finishIndexing.await(5, TimeUnit.SECONDS);
      shutdownFuture.get(5, TimeUnit.SECONDS);
      assertTrue(shutdownFuture.isDone());

      // verify both batches completed successfully
      firstBatchInfo.indexingFuture.get(5, TimeUnit.SECONDS);
      secondBatchInfo.indexingFuture.get(5, TimeUnit.SECONDS);

      // should end with shutdown steady-state exception
      assertTrue(lifecycleFuture.isCompletedExceptionally());
    }

    @Test
    public void testFailLifecycleCompletesInFlightIndexing() throws Exception {
      SimpleMeterRegistry registry = new SimpleMeterRegistry();
      IndexingWorkScheduler scheduler = scheduler();
      DecodingWorkScheduler decodingScheduler = spy(DecodingWorkScheduler.create(2, registry));

      Semaphore finishIndexing = new Semaphore(0);
      Semaphore finishDecoding = new Semaphore(0);

      DocumentIndexer indexer = mock(DocumentIndexer.class);

      // allow block indexing
      doAnswer(
              invocation -> {
                finishIndexing.acquire();
                return null;
              })
          .when(indexer)
          .indexDocumentEvent(any());

      // create an hanging change-stream for second batch
      ChangeStreamBatch hangingBatch = mock(ChangeStreamBatch.class);
      when(hangingBatch.getCommandOperationTime()).thenReturn(new BsonTimestamp());
      when(hangingBatch.getPostBatchResumeToken())
          .thenReturn(ChangeStreamUtils.resumeToken(new BsonTimestamp(3, 0)));

      // allow calls `getRawEvents()` in pre-processing by this thread (total 6 calls), after hang.
      AtomicInteger blockDecoding = new AtomicInteger(-6);
      when(hangingBatch.getRawEvents())
          .thenAnswer(
              invocation -> {
                if (blockDecoding.getAndIncrement() >= 0) {
                  finishDecoding.acquire();
                }
                return ChangeStreamUtils.toRawBsonDocuments(
                    List.of(ChangeStreamUtils.insertEvent(9, MOCK_INDEX_DEFINITION)));
              });

      CompletableFuture<Void> lifecycleFuture = new CompletableFuture<>();

      ChangeStreamIndexManager manager =
          DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
              MOCK_INDEX_DEFINITION,
              scheduler,
              indexer,
              ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
              ignoreResumeInfo -> {},
              IGNORE_METRICS,
              lifecycleFuture,
              GenerationIdBuilder.create(),
              decodingScheduler);

      BatchInfo firstBatchInfo =
          manager.indexBatch(
              new ChangeStreamBatch(
                  ChangeStreamUtils.toRawBsonDocuments(
                      List.of(ChangeStreamUtils.insertEvent(6, MOCK_INDEX_DEFINITION))),
                  POST_BATCH_RESUME_TOKEN,
                  new BsonTimestamp()),
              NOOP_DOCUMENT_METRICS_UPDATER,
              NOOP_TIMER);

      BatchInfo secondBatchInfo =
          manager.indexBatch(hangingBatch, NOOP_DOCUMENT_METRICS_UPDATER, NOOP_TIMER);

      verify(decodingScheduler, times(2)).schedule(any(), any(), any(), any(), any(), any());
      testScheduleCallsWithTimeout(scheduler, 1, 500);
      verify(indexer, timeout(500).times(1)).indexDocumentEvent(any());

      // terminate indexing
      Throwable lifecycleException = new RuntimeException();
      manager.failLifecycle(lifecycleException);

      // finish indexing of first batch and decoding of second
      finishIndexing.release();
      finishDecoding.release();

      Assert.assertThrows(ExecutionException.class, () -> lifecycleFuture.get(5, TimeUnit.SECONDS));

      // verify first completes successfully, second should get canceled with `lifecycleException`
      firstBatchInfo.indexingFuture.get(5, TimeUnit.SECONDS);
      Assert.assertThrows(
          ExecutionException.class, () -> secondBatchInfo.indexingFuture.get(5, TimeUnit.SECONDS));

      // second batch should not get scheduled for indexing and fail with `lifecycleException`
      assertThat(scheduler.getEnqueueCount()).isEqualTo(1);
      verify(indexer, times(1)).indexDocumentEvent(any());
    }

    @Test
    public void testDecodingPerIndexNotBlockedOnIndexing() throws Exception {
      SimpleMeterRegistry registry = new SimpleMeterRegistry();
      IndexingWorkScheduler scheduler = scheduler();
      DecodingWorkScheduler decodingScheduler = spy(DecodingWorkScheduler.create(2, registry));

      CountDownLatch finishIndexing = new CountDownLatch(1);
      DocumentIndexer hangingIndexer = mock(DocumentIndexer.class);
      doAnswer(
              invocation -> {
                finishIndexing.await();
                return null;
              })
          .when(hangingIndexer)
          .indexDocumentEvent(any());

      CompletableFuture<Void> lifecycleFuture = new CompletableFuture<>();

      ChangeStreamIndexManager manager =
          DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
              MOCK_INDEX_DEFINITION,
              scheduler,
              hangingIndexer,
              ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
              ignoreResumeInfo -> {},
              IGNORE_METRICS,
              lifecycleFuture,
              GenerationIdBuilder.create(),
              decodingScheduler);

      BatchInfo firstBatchInfo =
          manager.indexBatch(
              new ChangeStreamBatch(
                  ChangeStreamUtils.toRawBsonDocuments(
                      List.of(ChangeStreamUtils.insertEvent(6, MOCK_INDEX_DEFINITION))),
                  POST_BATCH_RESUME_TOKEN,
                  new BsonTimestamp()),
              NOOP_DOCUMENT_METRICS_UPDATER,
              NOOP_TIMER);

      BatchInfo secondBatchInfo =
          manager.indexBatch(
              new ChangeStreamBatch(
                  ChangeStreamUtils.toRawBsonDocuments(
                      List.of(ChangeStreamUtils.insertEvent(6, MOCK_INDEX_DEFINITION))),
                  POST_BATCH_RESUME_TOKEN,
                  new BsonTimestamp()),
              NOOP_DOCUMENT_METRICS_UPDATER,
              NOOP_TIMER);

      // verify that both batches were scheduled for decoding
      verify(decodingScheduler, times(2)).schedule(any(), any(), any(), any(), any(), any());

      // check that the first batch started indexing
      verify(hangingIndexer, timeout(500).times(1)).indexDocumentEvent(any());

      // indexing of first batch is blocked, verify that we can decode and schedule the second batch
      // while first batch is being indexed
      testScheduleCallsWithTimeout(scheduler, 2, 500);

      // sanity check that both indexing futures did not complete
      Assert.assertThrows(
          TimeoutException.class, () -> firstBatchInfo.indexingFuture.get(5, TimeUnit.SECONDS));
      Assert.assertThrows(
          TimeoutException.class, () -> secondBatchInfo.indexingFuture.get(5, TimeUnit.SECONDS));

      // allow indexing for first batch to complete
      finishIndexing.countDown();

      // check that the second batch started indexing
      verify(hangingIndexer, timeout(500).times(2)).indexDocumentEvent(any());

      // check that both indexing futures eventually completes
      finishIndexing.countDown();
      firstBatchInfo.indexingFuture.get(5, TimeUnit.SECONDS);
      secondBatchInfo.indexingFuture.get(5, TimeUnit.SECONDS);
    }

    private void testDecodedListViewValidation(
        List<ChangeStreamDocument<RawBsonDocument>> events, SteadyStateException.Type expected) {
      DecodingExecutorChangeStreamIndexManager indexManager = createDecodingIndexManager();

      var decodedEventsView =
          indexManager.getIndexableChangeStreamEvents(
              ChangeStreamUtils.toRawBsonDocuments(events), false);

      var ex =
          Assert.assertThrows(
              ChangeStreamEventCheckException.class,
              () -> {
                for (var event : decodedEventsView) {
                  Assert.assertNotNull(event);
                }
              });

      assertNotNull(ex.getCause());
      assertTrue(ex.getCause() instanceof SteadyStateException);
      var steady = (SteadyStateException) ex.getCause();
      assertEquals(expected, steady.getType());
    }

    private DecodingExecutorChangeStreamIndexManager createDecodingIndexManager() {
      return DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
          MOCK_INDEX_DEFINITION,
          mock(IndexingWorkScheduler.class),
          com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
              .mockFieldLimitsExceeded(),
          ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
          ignoreResumeInfo -> {},
          IGNORE_METRICS,
          new CompletableFuture<>(),
          GenerationIdBuilder.create(),
          mock(DecodingWorkScheduler.class));
    }
  }

  private static void testIndexingFutureEndsExceptionally(
      List<ChangeStreamDocument<RawBsonDocument>> events, SteadyStateException.Type expected) {
    AtomicReference<ChangeStreamResumeInfo> resumeInfoReference = new AtomicReference<>();

    CompletableFuture<Void> lifecycleFuture = new CompletableFuture<>();

    var decodingScheduler = DecodingWorkScheduler.create(4, new SimpleMeterRegistry());

    try {
      GenerationId generationId = GenerationIdBuilder.create();
      ChangeStreamIndexManager manager =
          DecodingExecutorChangeStreamIndexManager.createWithDecodingScheduler(
              MOCK_INDEX_DEFINITION,
              scheduler(),
              indexer(),
              ChangeStreamUtils.PRE_BATCH_RESUME_INFO.getNamespace(),
              resumeInfoReference::set,
              IGNORE_METRICS,
              lifecycleFuture,
              generationId,
              decodingScheduler);

      BatchInfo info =
          manager.indexBatch(
              new ChangeStreamBatch(
                  toRawBsonDocuments(events),
                  ChangeStreamUtils.POST_BATCH_RESUME_TOKEN,
                  new BsonTimestamp()),
              NOOP_DOCUMENT_METRICS_UPDATER,
              NOOP_TIMER);

      CompletableFuture<Void> indexingFuture = info.indexingFuture;

      ExecutionException indexingEx =
          Assert.assertThrows(
              ExecutionException.class, () -> indexingFuture.get(5, TimeUnit.SECONDS));
      assertTrue(indexingEx.getCause() instanceof SteadyStateException);
      SteadyStateException indexingSteadyStateEx = (SteadyStateException) indexingEx.getCause();
      assertSame(expected, indexingSteadyStateEx.getType());
      assertFalse(lifecycleFuture.isDone());

      // cancel failed decoding batch before shutdown
      manager.failLifecycle(indexingSteadyStateEx);
      ExecutionException lifecycleEx =
          Assert.assertThrows(
              ExecutionException.class, () -> lifecycleFuture.get(5, TimeUnit.SECONDS));
      assertTrue(lifecycleEx.getCause() instanceof SteadyStateException);
      assertSame(expected, ((SteadyStateException) lifecycleEx.getCause()).getType());
      assertTrue(lifecycleFuture.isDone());
    } finally {
      decodingScheduler.shutdown();
    }
  }

  private static void testScheduleCallsWithTimeout(
      IndexingWorkScheduler scheduler, int numCalls, int timeoutMillis) {
    Condition.await()
        .atMost(Duration.ofMillis(timeoutMillis))
        .withTimeoutCause(
            new Throwable(
                "IndexingWorkScheduler did not call schedule() "
                    + numCalls
                    + " times before "
                    + timeoutMillis
                    + "ms"))
        .until(() -> scheduler.getEnqueueCount() == numCalls);
  }

  private static DocumentIndexer indexer() {
    return com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
        .mockDocumentIndexer();
  }

  private static IndexingWorkScheduler scheduler() {
    IndexingWorkSchedulerFactory dispatcher =
        IndexingWorkSchedulerFactory.create(2, mock(Supplier.class), new SimpleMeterRegistry());
    return dispatcher.getIndexingWorkScheduler(MOCK_INDEX_DEFINITION);
  }
}
