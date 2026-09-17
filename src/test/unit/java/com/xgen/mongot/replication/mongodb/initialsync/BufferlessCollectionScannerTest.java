package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.testing.mongot.mock.index.SearchIndex.IGNORE_METRICS;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.mongodb.common.AggregateCommandCollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.BufferlessIdOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.BufferlessNaturalOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.mongodb.CollectionScanAggregateCommand;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.mock.index.IndexGeneration;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class BufferlessCollectionScannerTest {

  private static final BsonTimestamp START_OPTIME = new BsonTimestamp(0, 1);
  private static final BsonValue MIN_VALID_LAST_SCANNED_ID = BsonUtils.MIN_KEY;
  private static final BsonTimestamp MIN_VALID_OPTIME = new BsonTimestamp(5, 1);
  private static final String AFTER_CLUSTER_TIME_FIELD = "afterClusterTime";
  private static final Duration DEFAULT_SCAN_TIME = Duration.ofMinutes(5);

  private static final BsonDocument POST_BATCH_RESUME_TOKEN =
      new BsonDocument().append("$recordId", new BsonInt64(1));

  @Parameterized.Parameter public static boolean enableNaturalOrderScan;

  @Parameterized.Parameters
  public static List<Boolean> data() {
    return Arrays.asList(true, false);
  }

  private static class Mocks {

    private final InitialSyncContext context;
    private final InitialSyncMongoClient mongoClient;
    private final CollectionScanMongoClient<InitialSyncException> findCommandMongoClient;
    private final DocumentIndexer documentIndexer;
    private final IndexDefinitionGeneration indexDefinitionGeneration;
    private final IndexingWorkScheduler indexingWorkScheduler;

    private final BufferlessCollectionScanner collectionScanner;

    private Mocks(
        InitialSyncMongoClient mongoClient,
        CollectionScanMongoClient<InitialSyncException> collectionScanCommandMongoClient,
        Optional<BsonValue> lastScannedId,
        IndexDefinitionGeneration generation,
        IndexMetricsUpdater indexMetricsUpdater,
        Gate initialSyncGate) {
      this.mongoClient = mongoClient;
      this.findCommandMongoClient = collectionScanCommandMongoClient;
      this.documentIndexer =
          com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
              .mockDocumentIndexer();
      this.indexDefinitionGeneration = generation;
      this.indexingWorkScheduler = mock(IndexingWorkScheduler.class);
      this.context =
          spy(
              InitialSyncContext.create(
                      this.indexDefinitionGeneration,
                      this.indexingWorkScheduler,
                      this.documentIndexer,
                      indexMetricsUpdater,
                      Optional.empty(),
                      false,
                      enableNaturalOrderScan,
                      false,
                      1,
                      initialSyncGate)
                  .withProgress(START_OPTIME));

      this.collectionScanner =
          new BufferlessCollectionScanner(
              Clock.systemUTC(),
              this.context,
              this.mongoClient,
              enableNaturalOrderScan
                  ? lastScannedId.orElse(new BsonDocument())
                  : lastScannedId.orElse(BsonUtils.MIN_KEY),
              new MetricsFactory("test", new SimpleMeterRegistry()),
              enableNaturalOrderScan);
    }

    static Mocks noDocuments() throws Exception {
      Mocks mocks = createMocks();

      when(mocks.findCommandMongoClient.hasNext()).thenReturn(false);
      when(mocks.findCommandMongoClient.getNext()).thenReturn(List.of());
      when(mocks.findCommandMongoClient.getPostBatchResumeToken())
          .thenReturn(Optional.ofNullable(POST_BATCH_RESUME_TOKEN));

      return mocks;
    }

    static Mocks threeDocuments() throws Exception {
      return threeDocuments(Optional.empty());
    }

    static Mocks threeDocuments(Optional<BsonValue> lastScannedId) throws Exception {
      Mocks mocks = createMocks(lastScannedId, MOCK_INDEX_DEFINITION_GENERATION);

      when(mocks.findCommandMongoClient.hasNext()).thenReturn(true).thenReturn(false);
      when(mocks.findCommandMongoClient.getNext())
          .thenReturn(
              List.of(
                  BsonUtils.documentToRaw(
                      new BsonDocument("_id", new BsonInt32(1))
                          .append(
                              MOCK_INDEX_DEFINITION_GENERATION.getIndexId().toString(),
                              new BsonDocument("_id", new BsonInt32(1)))),
                  BsonUtils.documentToRaw(
                      new BsonDocument("_id", new BsonInt32(2))
                          .append(
                              MOCK_INDEX_DEFINITION_GENERATION.getIndexId().toString(),
                              new BsonDocument("_id", new BsonInt32(2)))),
                  BsonUtils.documentToRaw(
                      new BsonDocument("_id", new BsonInt32(3))
                          .append(
                              MOCK_INDEX_DEFINITION_GENERATION.getIndexId().toString(),
                              new BsonDocument("_id", new BsonInt32(3))))));
      when(mocks.findCommandMongoClient.getPostBatchResumeToken())
          .thenReturn(Optional.ofNullable(POST_BATCH_RESUME_TOKEN));
      return mocks;
    }

    static Mocks infiniteDocuments() throws Exception {
      Mocks mocks = createMocks();

      when(mocks.findCommandMongoClient.hasNext()).thenReturn(true);

      BsonString id = new BsonString(UUID.randomUUID().toString());
      when(mocks.findCommandMongoClient.getNext())
          .thenReturn(
              List.of(
                  BsonUtils.documentToRaw(
                      new BsonDocument("_id", id)
                          .append(
                              MOCK_INDEX_DEFINITION_GENERATION.getIndexId().toString(),
                              new BsonDocument("_id", id)))));
      when(mocks.findCommandMongoClient.getPostBatchResumeToken())
          .thenReturn(Optional.ofNullable(POST_BATCH_RESUME_TOKEN));
      return mocks;
    }

    static Mocks documentThenThrowsRequiresResync() throws Exception {
      Mocks mocks = createMocks();

      when(mocks.findCommandMongoClient.hasNext()).thenReturn(true);
      BsonString id = new BsonString(UUID.randomUUID().toString());
      when(mocks.findCommandMongoClient.getNext())
          .thenReturn(
              List.of(
                  BsonUtils.documentToRaw(
                      new BsonDocument("_id", id)
                          .append(
                              MOCK_INDEX_DEFINITION_GENERATION.getIndexId().toString(),
                              new BsonDocument("_id", id)))))
          .thenThrow(InitialSyncException.createRequiresResync("mocked error"));
      when(mocks.findCommandMongoClient.getPostBatchResumeToken())
          .thenReturn(Optional.ofNullable(POST_BATCH_RESUME_TOKEN));
      return mocks;
    }

    static Mocks indexingWorkSchedulerCompletesExceptionally(Throwable exception) throws Exception {
      Mocks mocks = infiniteDocuments();
      var future = CompletableFuture.failedFuture(exception);
      doReturn(future)
          .when(mocks.indexingWorkScheduler)
          .schedule(any(), any(), any(), any(), any(), any(), any());

      return mocks;
    }

    static Mocks indexingWorkSchedulerSucceedsOnceThenCompletesExceptionally(
        IndexMetricsUpdater indexMetricsUpdater, Throwable exception) throws Exception {
      Mocks mocks = infiniteDocumentsWithMetricsUpdater(indexMetricsUpdater);
      var future = CompletableFuture.failedFuture(exception);
      doReturn(CompletableFuture.completedFuture(null))
          .doReturn(future)
          .when(mocks.indexingWorkScheduler)
          .schedule(any(), any(), any(), any(), any(), any(), any());
      return mocks;
    }

    static Mocks infiniteDocumentsWithMetricsUpdater(IndexMetricsUpdater indexMetricsUpdater)
        throws Exception {
      Mocks mocks =
          createMocks(
              Optional.empty(),
              MOCK_INDEX_DEFINITION_GENERATION,
              indexMetricsUpdater,
              ToggleGate.opened());

      when(mocks.findCommandMongoClient.hasNext()).thenReturn(true);

      BsonString id = new BsonString(UUID.randomUUID().toString());
      when(mocks.findCommandMongoClient.getNext())
          .thenReturn(
              List.of(
                  BsonUtils.documentToRaw(
                      new BsonDocument("_id", id)
                          .append(
                              MOCK_INDEX_DEFINITION_GENERATION.getIndexId().toString(),
                              new BsonDocument("_id", id)))));
      when(mocks.findCommandMongoClient.getPostBatchResumeToken())
          .thenReturn(Optional.ofNullable(POST_BATCH_RESUME_TOKEN));
      return mocks;
    }

    private static Mocks createMocks() throws Exception {
      return createMocks(Optional.empty(), MOCK_INDEX_DEFINITION_GENERATION);
    }

    private static Mocks createMocks(IndexDefinitionGeneration generation) throws Exception {
      return createMocks(Optional.empty(), generation);
    }

    private static Mocks createMocks(
        Optional<BsonValue> lastScannedId, IndexDefinitionGeneration generation) throws Exception {
      return createMocks(lastScannedId, generation, IGNORE_METRICS, ToggleGate.opened());
    }

    private static Mocks createMocks(
        Optional<BsonValue> lastScannedId,
        IndexDefinitionGeneration generation,
        IndexMetricsUpdater indexMetricsUpdater,
        Gate initialSyncGate)
        throws Exception {

      InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);

      CollectionScanMongoClient<InitialSyncException> aggregateCommandMongoClient =
          mock(AggregateCommandCollectionScanMongoClient.class);
      when(aggregateCommandMongoClient.getMinValidOpTime()).thenReturn(MIN_VALID_OPTIME);
      when(mongoClient.getCollectionAggregateCommandMongoClient(any(), any(), any(), any()))
          .thenReturn(aggregateCommandMongoClient);
      when(mongoClient.getSyncSourceHost()).thenReturn("testHost");
      Mocks mock =
          new Mocks(
              mongoClient,
              aggregateCommandMongoClient,
              lastScannedId,
              generation,
              indexMetricsUpdater,
              initialSyncGate);

      when(mock.indexingWorkScheduler.schedule(any(), any(), any(), any(), any(), any(), any()))
          .thenAnswer(
              invocation -> {
                List<DocumentEvent> batch = invocation.getArgument(0);
                CompletableFuture<Void> future = new CompletableFuture<>();
                // Simulate processing each DocumentEvent
                new Thread(
                        () -> {
                          try {
                            for (DocumentEvent event : batch) {
                              mock.documentIndexer.indexDocumentEvent(event);
                            }
                            Optional<IndexCommitUserData> commitUserData =
                                invocation.getArgument(5);
                            commitUserData.ifPresent(mock.documentIndexer::updateCommitUserData);
                            future.complete(null);
                          } catch (Exception e) {
                            future.completeExceptionally(e);
                          }
                        })
                    .start();
                return future;
              });
      when(mock.indexingWorkScheduler.cancel(any(), any(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));

      return mock;
    }
  }

  private CollectionScanAggregateCommand aggregateCommandWithAfterClusterTime() {
    return argThat(
        aggregateCommand ->
            aggregateCommand.getReadConcernDocument().isPresent()
                && aggregateCommand
                    .getReadConcernDocument()
                    .get()
                    .getTimestamp(AFTER_CLUSTER_TIME_FIELD)
                    .equals(START_OPTIME));
  }

  @Test
  public void testScanBubblesException() throws Exception {
    Mocks mocks = Mocks.documentThenThrowsRequiresResync();

    try {
      mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME);
      Assert.fail(
          "scanner did not throw InitialSyncException when MongoClient threw MongoException");
    } catch (InitialSyncException e) {
      Assert.assertTrue(
          "InitialSyncException was not REQUIRES_RESYNC when MongoClient threw MongoException",
          e.isRequiresResync());
    }

    // Should have updated the index once as well.
    verify(mocks.documentIndexer).indexDocumentEvent(any());
  }

  @Test
  public void testScanNoDocuments() throws Exception {
    Mocks mocks = Mocks.noDocuments();

    BufferlessCollectionScanner.Result result =
        mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME);
    verify(mocks.mongoClient)
        .getCollectionAggregateCommandMongoClient(
            aggregateCommandWithAfterClusterTime(), any(), any(), any());
    verify(mocks.documentIndexer, times(0)).indexDocumentEvent(any());
    BsonValue expected = enableNaturalOrderScan ? new BsonDocument() : MIN_VALID_LAST_SCANNED_ID;
    Assert.assertEquals(expected, result.getLastScannedToken());
    Assert.assertFalse(result.getContinueSync());
  }

  @Test
  public void testScanCompletes() throws Exception {
    Mocks mocks = Mocks.createMocks(MOCK_INDEX_DEFINITION_GENERATION);

    when(mocks.findCommandMongoClient.hasNext()).thenReturn(true).thenReturn(false);
    when(mocks.findCommandMongoClient.getNext())
        .thenReturn(
            List.of(
                // one document does not have metadata namespace, which should not affect indexes
                // created on a collection
                BsonUtils.documentToRaw(new BsonDocument("_id", new BsonInt32(1))),
                BsonUtils.documentToRaw(
                    new BsonDocument("_id", new BsonInt32(2))
                        .append(
                            MOCK_INDEX_DEFINITION_GENERATION.getIndexId().toString(),
                            new BsonDocument("_id", new BsonInt32(2)))),
                BsonUtils.documentToRaw(
                    new BsonDocument("_id", new BsonInt32(3))
                        .append(
                            MOCK_INDEX_DEFINITION_GENERATION.getIndexId().toString(),
                            new BsonDocument("_id", new BsonInt32(3))))));

    BsonDocument resumeToken = new BsonDocument().append("$recordId", new BsonInt64(3));
    when(mocks.findCommandMongoClient.getPostBatchResumeToken())
        .thenReturn(Optional.ofNullable(resumeToken));

    BufferlessCollectionScanner.Result result =
        mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME);
    verify(mocks.mongoClient)
        .getCollectionAggregateCommandMongoClient(
            aggregateCommandWithAfterClusterTime(), any(), any(), any());
    verify(mocks.documentIndexer, times(3)).indexDocumentEvent(any());
    verify(mocks.indexingWorkScheduler).schedule(any(), any(), any(), any(), any(), any(), any());
    BsonValue expected = enableNaturalOrderScan ? resumeToken : new BsonInt32(3);
    Assert.assertEquals(expected, result.getLastScannedToken());
    Assert.assertFalse(result.getContinueSync());
  }

  @Test
  public void testEndOfScanNullRecordIdKeepsPreviousToken() throws Exception {
    // Only the natural-order scan carries post-batch resume tokens; the _id scan tracks the last
    // document id and never sees a null $recordId.
    Assume.assumeTrue(enableNaturalOrderScan);

    Mocks mocks = Mocks.createMocks(MOCK_INDEX_DEFINITION_GENERATION);

    // Two batches: the first ends on a resumable $recordId, the second finishes the scan and comes
    // back with a null $recordId (mongod 8.2.1+), which mongod refuses to resume from.
    when(mocks.findCommandMongoClient.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);
    when(mocks.findCommandMongoClient.getNext())
        .thenReturn(
            List.of(
                BsonUtils.documentToRaw(
                    new BsonDocument("_id", new BsonInt32(1))
                        .append(
                            MOCK_INDEX_DEFINITION_GENERATION.getIndexId().toString(),
                            new BsonDocument("_id", new BsonInt32(1))))))
        .thenReturn(
            List.of(
                BsonUtils.documentToRaw(
                    new BsonDocument("_id", new BsonInt32(2))
                        .append(
                            MOCK_INDEX_DEFINITION_GENERATION.getIndexId().toString(),
                            new BsonDocument("_id", new BsonInt32(2))))));

    BsonDocument resumableToken = new BsonDocument().append("$recordId", new BsonInt64(1));
    BsonDocument endOfScanToken = new BsonDocument().append("$recordId", BsonNull.VALUE);
    when(mocks.findCommandMongoClient.getPostBatchResumeToken())
        .thenReturn(Optional.of(resumableToken))
        .thenReturn(Optional.of(endOfScanToken));

    BufferlessCollectionScanner.Result result =
        mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME);

    verify(mocks.documentIndexer, times(2)).indexDocumentEvent(any());
    // The scan finished, but the last resumable position is kept instead of the unresumable
    // null-$recordId token, so a restart in the end-of-scan window rescans rather than resyncs.
    Assert.assertEquals(resumableToken, result.getLastScannedToken());
    Assert.assertFalse(result.getContinueSync());
  }

  @Test
  public void testDocumentIsSkippedWhenMetadataIsEmptyAndIndexIsCreatedOnView() throws Exception {

    SearchIndexDefinition indexDefinition =
        SearchIndex.mockDefinitionBuilder()
            .view(ViewDefinition.existing("test", List.of()))
            .build();

    Mocks mocks = Mocks.createMocks(IndexGeneration.mockDefinitionGeneration(indexDefinition));

    when(mocks.findCommandMongoClient.hasNext()).thenReturn(true).thenReturn(false);
    when(mocks.findCommandMongoClient.getNext())
        .thenReturn(
            List.of(
                // first document is invalid (no metadata namespace), expected to be ignored
                BsonUtils.documentToRaw(new BsonDocument("_id", new BsonInt32(1))),
                // second document is valid, expected to be processed
                BsonUtils.documentToRaw(
                    new BsonDocument("_id", new BsonInt32(2))
                        .append(
                            MOCK_INDEX_DEFINITION_GENERATION.getIndexId().toString(),
                            new BsonDocument("_id", new BsonInt32(2))))));

    BsonDocument resumeToken = new BsonDocument().append("$recordId", new BsonInt64(2));
    when(mocks.findCommandMongoClient.getPostBatchResumeToken())
        .thenReturn(Optional.ofNullable(resumeToken));

    BufferlessCollectionScanner.Result result =
        mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME);
    verify(mocks.mongoClient)
        .getCollectionAggregateCommandMongoClient(
            aggregateCommandWithAfterClusterTime(), any(), any(), any());
    verify(mocks.documentIndexer, times(1)).indexDocumentEvent(any());
    verify(mocks.indexingWorkScheduler).schedule(any(), any(), any(), any(), any(), any(), any());
    BsonValue expected = enableNaturalOrderScan ? resumeToken : new BsonInt32(2);
    Assert.assertEquals(expected, result.getLastScannedToken());
    Assert.assertFalse(result.getContinueSync());
  }

  @Test
  public void testScanWithLastScannedId() throws Exception {
    BsonValue lastScannedId = enableNaturalOrderScan ? POST_BATCH_RESUME_TOKEN : new BsonObjectId();
    Mocks mocks = Mocks.threeDocuments(Optional.of(lastScannedId));

    BufferlessCollectionScanner.Result result =
        mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME);

    verify(mocks.mongoClient)
        .getCollectionAggregateCommandMongoClient(
            argThat(
                aggregateCommand ->
                    enableNaturalOrderScan
                        ? aggregateCommand.getStartAt().isPresent()
                            && aggregateCommand.getStartAt().get().equals(lastScannedId)
                        : aggregateCommand.getLastScannedId().isPresent()
                            && aggregateCommand.getLastScannedId().get().equals(lastScannedId)),
            any(),
            any(),
            any());

    verify(mocks.documentIndexer, times(3)).indexDocumentEvent(any());
    verify(mocks.indexingWorkScheduler).schedule(any(), any(), any(), any(), any(), any(), any());
    BsonValue expected = enableNaturalOrderScan ? POST_BATCH_RESUME_TOKEN : new BsonInt32(3);
    Assert.assertEquals(expected, result.getLastScannedToken());
    Assert.assertFalse(result.getContinueSync());
  }

  @Test
  public void testScanPartial() throws Exception {
    Mocks mocks = Mocks.infiniteDocuments();

    BufferlessCollectionScanner.Result result =
        mocks.collectionScanner.scanWithTimeLimit(Duration.ofSeconds(2));
    verify(mocks.mongoClient)
        .getCollectionAggregateCommandMongoClient(
            aggregateCommandWithAfterClusterTime(), any(), any(), any());

    verify(mocks.indexingWorkScheduler, atLeastOnce())
        .schedule(any(), any(), any(), any(), any(), any(), any());
    Assert.assertTrue(result.getContinueSync());
  }

  @Test
  public void testShutdownBeforeStart() throws Exception {
    Mocks mocks = Mocks.infiniteDocuments();

    // Start scanning documents in a separate thread.
    AtomicReference<InitialSyncException> exceptionReference = new AtomicReference<>();
    Thread scannerThread =
        new Thread(
            () -> {
              try {
                mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME);
              } catch (InitialSyncException e) {
                exceptionReference.set(e);
              }
            });

    mocks.collectionScanner.signalShutdown();

    scannerThread.start();

    // Wait until we've started the scan, then shut down the scanner.
    verify(mocks.documentIndexer, times(0)).indexDocumentEvent(any());

    scannerThread.join(2500);

    // Verify cancel was called on the scheduler.
    verify(mocks.indexingWorkScheduler, times(1))
        .cancel(eq(mocks.indexDefinitionGeneration.getGenerationId()), any(), any());

    Assert.assertNotNull(
        "BufferlessCollectionScanner did not throw InitialSyncException", exceptionReference.get());
    Assert.assertTrue(
        "BufferlessCollectionScanner did not throw SHUT_DOWN InitialSyncException",
        exceptionReference.get().isShutdown());
  }

  @Test
  public void testShutdownRightBeforeIndexingError() throws Exception {
    Mocks mocks = Mocks.infiniteDocuments();

    CountDownLatch indexingStarted = new CountDownLatch(1);
    CountDownLatch shutdownSignalled = new CountDownLatch(1);
    doAnswer(
            (unused) -> {
              indexingStarted.countDown();
              // Block until the main thread signals that shutdown has been called
              shutdownSignalled.await();
              // Then, immediately fail
              throw new IllegalArgumentException("Simulated indexing failure");
            })
        .when(mocks.documentIndexer)
        .indexDocumentEvent(any());

    // Use a generic Throwable reference to capture whichever exception wins the race.
    AtomicReference<Throwable> exceptionReference = new AtomicReference<>();
    Thread scannerThread =
        new Thread(
            () -> {
              try {
                mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME);
              } catch (Throwable t) { // Catch any terminal exception or error
                exceptionReference.set(t);
              }
            });
    scannerThread.start();

    indexingStarted.await();

    mocks.collectionScanner.signalShutdown();

    shutdownSignalled.countDown();
    scannerThread.join(2500);

    // Verify cancel was called on the scheduler.
    verify(mocks.indexingWorkScheduler, atLeastOnce())
        .cancel(eq(mocks.indexDefinitionGeneration.getGenerationId()), any(), any());

    Throwable caughtException = exceptionReference.get();
    Assert.assertNotNull(
        "Scanner thread should have terminated with an exception", caughtException);

    // The exception can be one of two valid types depending on the race condition's outcome.
    // Either the indexing failure propagated, or the shutdown signal was processed first.
    // We assert that the outcome is one of these two possibilities.
    boolean isExpectedException =
        caughtException instanceof IllegalArgumentException
            || (caughtException instanceof InitialSyncException
                && ((InitialSyncException) caughtException).isShutdown());

    Assert.assertTrue(
        "Exception was not of an expected type. Got: " + caughtException.getClass().getName(),
        isExpectedException);
  }

  @Test
  public void testScanIsInterruptable() throws Exception {
    Mocks mocks = Mocks.infiniteDocuments();

    // Start scanning documents in a separate thread.
    AtomicReference<InitialSyncException> exceptionReference = new AtomicReference<>();
    Thread scannerThread =
        new Thread(
            () -> {
              try {
                mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME);
              } catch (InitialSyncException e) {
                exceptionReference.set(e);
              }
            });
    scannerThread.start();

    // Wait until we've started the scan, then shut down the scanner.
    verify(mocks.documentIndexer, timeout(2500).atLeastOnce()).indexDocumentEvent(any());

    mocks.collectionScanner.signalShutdown();
    scannerThread.join(2500);

    // verify cancel was called on the scheduler
    verify(mocks.indexingWorkScheduler, times(1))
        .cancel(eq(mocks.indexDefinitionGeneration.getGenerationId()), any(), any());

    Assert.assertNotNull(
        "BufferlessCollectionScanner did not throw InitialSyncException", exceptionReference.get());
    Assert.assertTrue(
        "BufferlessCollectionScanner did not throw SHUT_DOWN InitialSyncException",
        exceptionReference.get().isShutdown());
  }

  @Test
  public void testScanWrapsFieldLimitExceededException() throws Exception {
    Mocks mocks =
        Mocks.indexingWorkSchedulerCompletesExceptionally(
            new FieldExceededLimitsException("exceeded"));

    InitialSyncException e =
        Assert.assertThrows(
            InitialSyncException.class,
            () -> mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME));
    Assert.assertEquals(InitialSyncException.Type.FIELD_EXCEEDED, e.getType());
  }

  @Test
  public void testScanCancelsIndexSchedulingOnExceptionAndWaitsForBatchCompletion()
      throws Exception {
    Mocks mocks =
        Mocks.indexingWorkSchedulerCompletesExceptionally(new IllegalStateException("unexpected"));

    CompletableFuture<Void> inFlightBatch = new CompletableFuture<>();

    doReturn(inFlightBatch)
        .when(mocks.indexingWorkScheduler)
        .cancel(eq(mocks.indexDefinitionGeneration.getGenerationId()), any(), any());

    var latch = new CountDownLatch(1);
    Thread scannerThread =
        new Thread(
            () -> {
              Assert.assertThrows(
                  IllegalStateException.class,
                  () -> mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME));
              latch.countDown();
            });
    scannerThread.start();

    // Expect scanner to wait until the in-flight batch is completed.
    Assert.assertFalse(latch.await(500, TimeUnit.MILLISECONDS));

    // Complete the in-flight batch.
    inFlightBatch.complete(null);
    scannerThread.join();

    // Check that scanner has finished.
    Assert.assertEquals(0, latch.getCount());
  }

  @Test
  public void testScanNeverCancelsIndexSchedulingOnSuccessfulRun() throws Exception {
    Mocks mocks = Mocks.threeDocuments();
    mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME);
    verify(mocks.indexingWorkScheduler, never()).cancel(any(), any(), any());
  }

  @Test
  public void testScanSetsCommitUserDataOnEveryBatch() throws Exception {
    Mocks mocks = Mocks.threeDocuments();

    BufferlessCollectionScanner.Result result =
        mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME);
    verify(mocks.documentIndexer, times(3)).indexDocumentEvent(any());
    InitialSyncResumeInfo resumeInfo =
        enableNaturalOrderScan
            ? new BufferlessNaturalOrderInitialSyncResumeInfo(
                START_OPTIME,
                POST_BATCH_RESUME_TOKEN,
                enableNaturalOrderScan ? Optional.of("testHost") : Optional.empty())
            : new BufferlessIdOrderInitialSyncResumeInfo(START_OPTIME, new BsonInt32(3));
    verify(mocks.documentIndexer)
        .updateCommitUserData(
            IndexCommitUserData.createInitialSyncResume(
                mocks.context.getGenerationId().generation.formatVersion.versionNumber(),
                resumeInfo));
    BsonValue expected = enableNaturalOrderScan ? POST_BATCH_RESUME_TOKEN : new BsonInt32(3);
    verify(mocks.indexingWorkScheduler).schedule(any(), any(), any(), any(), any(), any(), any());
    Assert.assertEquals(expected, result.getLastScannedToken());
    Assert.assertFalse(result.getContinueSync());
  }

  @Test
  public void testMetricsUpdateOnlyWhenBatchCompletes() throws Exception {
    IndexMetricsUpdater indexMetricsUpdater =
        IndexMetricsUpdaterBuilder.builder()
            .metricsFactory(SearchIndex.mockMetricsFactory())
            .indexMetricsSupplier(mock(IndexMetricValuesSupplier.class))
            .build();

    Mocks mocks =
        Mocks.indexingWorkSchedulerSucceedsOnceThenCompletesExceptionally(
            indexMetricsUpdater, new IllegalStateException("unexpected"));

    Assert.assertThrows(
        IllegalStateException.class,
        () -> mocks.collectionScanner.scanWithTimeLimit(DEFAULT_SCAN_TIME));

    // Check metrics are not updated when exception is thrown.
    Assert.assertEquals(
        1,
        indexMetricsUpdater
            .getReplicationMetricsUpdater()
            .getInitialSyncMetrics()
            .getCollectionScanBatchTotalApplicableDocuments()
            .count());

    Assert.assertTrue(
        indexMetricsUpdater
                .getReplicationMetricsUpdater()
                .getInitialSyncMetrics()
                .getTotalApplicableBytes()
                .count()
            > 0);
  }
}
