package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.testing.mongot.mock.index.SearchIndex.IGNORE_METRICS;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils.toRawBsonDocuments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

@RunWith(Parameterized.class)
public class BufferlessChangeStreamApplierTest {

  private static final BsonTimestamp OP_TIME = new BsonTimestamp(0, 0);
  private static final BsonTimestamp STOP_AFTER_OP_TIME = new BsonTimestamp(1, 0);
  private static final BsonValue LAST_SCANNED_ID = new BsonInt32(100);

  private static final BsonDocument POST_BATCH_RESUME_TOKEN =
      new BsonDocument().append("$recordId", new BsonInt64(100));

  private static final ChangeStreamBatch EMPTY_BATCH =
      new ChangeStreamBatch(
          Collections.emptyList(), ChangeStreamUtils.resumeToken(OP_TIME), OP_TIME);

  @Parameterized.Parameter public static boolean enableNaturalOrderScan;

  @Parameterized.Parameters
  public static List<Boolean> data() {
    return Arrays.asList(true, false);
  }

  /** Tests that an exception thrown by a change stream is bubbled up. */
  @Test
  public void testChangeStreamThrows() throws Exception {
    try (Mocks mocks = Mocks.changeStreamThrowsImmediately()) {
      assertThrowsInitialSyncException(mocks, InitialSyncException.Type.DROPPED);
    }

    try (Mocks mocks = Mocks.changeStreamThrowsDelayed()) {
      assertThrowsInitialSyncException(mocks, InitialSyncException.Type.DROPPED);
    }
  }

  /** Tests that change stream events with different types are handled properly. */
  @Test
  public void testChangeStreamHandledEventTypes() throws Exception {
    Map<ChangeStreamDocument<RawBsonDocument>, InitialSyncException.Type> handledTypes =
        new HashMap<>();
    handledTypes.put(ChangeStreamUtils.dropEvent(0), InitialSyncException.Type.DROPPED);
    handledTypes.put(ChangeStreamUtils.dropDatabaseEvent(0), InitialSyncException.Type.DROPPED);
    handledTypes.put(ChangeStreamUtils.renameEvent(0), InitialSyncException.Type.REQUIRES_RESYNC);
    handledTypes.put(ChangeStreamUtils.otherEvent(0), InitialSyncException.Type.REQUIRES_RESYNC);
    handledTypes.put(
        ChangeStreamUtils.invalidateEvent(0), InitialSyncException.Type.REQUIRES_RESYNC);

    for (var entry : handledTypes.entrySet()) {
      ChangeStreamDocument<RawBsonDocument> event = entry.getKey();
      InitialSyncException.Type exceptionType = entry.getValue();

      try (Mocks mocks = Mocks.changeStreamHasEventImmediately(event)) {
        assertThrowsInitialSyncException(mocks, exceptionType);
      }

      try (Mocks mocks = Mocks.changeStreamHasEventDelayed(event)) {
        assertThrowsInitialSyncException(mocks, exceptionType);
      }
    }
  }

  /** Tests that metrics are updated correctly. */
  @Test
  public void testChangeStreamMetricsUpdate() throws Exception {
    IndexMetricsUpdater indexMetricsUpdater =
        IndexMetricsUpdaterBuilder.builder()
            .metricsFactory(SearchIndex.mockMetricsFactory())
            .indexMetricsSupplier(mock(IndexMetricValuesSupplier.class))
            .build();

    try (Mocks mocks =
        Mocks.changeStreamHasEventThenThrows(
            ChangeStreamUtils.insertEvent(6, MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition()),
            indexMetricsUpdater)) {
      assertThrowsInitialSyncException(mocks, InitialSyncException.Type.FIELD_EXCEEDED);
      // Check metrics are not updated when exception is thrown.
      Assert.assertEquals(
          1,
          indexMetricsUpdater
              .getReplicationMetricsUpdater()
              .getInitialSyncMetrics()
              .getChangeStreamBatchTotalApplicableDocuments()
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

  // Test that for a fresh start (isFreshStart=true), the change stream starts at highWaterMark + 1.
  // This is safe because the collection scan already captured everything at highWaterMark.
  @Test
  public void testChangeStreamStartsAtHighWaterMarkPlusOneForFreshStart() throws Exception {
    BsonTimestamp highWaterMark = new BsonTimestamp(1, 0);
    BsonTimestamp expectedStartOpTime = new BsonTimestamp(1, 1); // highWaterMark + 1
    IndexDefinition indexDefinition = MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition();

    // Create events at the expectedStartOpTime (after highWaterMark).
    ChangeStreamDocument<RawBsonDocument> event1 =
        createEventWithOpTime(1, expectedStartOpTime, indexDefinition);
    ChangeStreamDocument<RawBsonDocument> event2 =
        createEventWithOpTime(2, expectedStartOpTime, indexDefinition);

    BsonDocument postBatchResumeToken = ChangeStreamUtils.resumeToken(expectedStartOpTime);

    // isFreshStart=true means we use highWaterMark + 1
    try (Mocks mocks =
        Mocks.createMocksWithOptime(
            highWaterMark, Optional.empty(), IGNORE_METRICS, /* isFreshStart= */ true)) {
      ChangeStreamBatch batch1 =
          new ChangeStreamBatch(
              toRawBsonDocuments(Arrays.asList(event1, event2)),
              postBatchResumeToken,
              expectedStartOpTime);

      ChangeStreamBatch batch2 =
          new ChangeStreamBatch(Collections.emptyList(), postBatchResumeToken, STOP_AFTER_OP_TIME);

      when(mocks.changeStreamMongoClient.getNext()).thenReturn(batch1, batch2);

      BsonValue lastScannedToken =
          enableNaturalOrderScan ? POST_BATCH_RESUME_TOKEN : LAST_SCANNED_ID;
      BsonTimestamp result =
          mocks.applier.applyEvents(
              lastScannedToken, STOP_AFTER_OP_TIME, /* continue_sync= */ false);
      Assert.assertEquals(expectedStartOpTime, result);

      // Verify that the command issued to open the change stream starts at highWaterMark + 1.
      ChangeStreamAggregateCommand command = mocks.commandCaptor.getValue();
      Optional<BsonTimestamp> startOpTime = command.getStartAtOperationTime();

      Assert.assertTrue("start operation time should be present", startOpTime.isPresent());
      Assert.assertEquals(
          "For fresh start, change stream should start at highWaterMark + 1",
          expectedStartOpTime,
          startOpTime.get());
    }
  }

  // Test that for a resume (isFreshStart=false), the change stream starts at highWaterMark
  // (inclusive) to ensure no events are missed from multi-document transactions.
  @Test
  public void testChangeStreamStartsAtHighWaterMarkForResume() throws Exception {
    BsonTimestamp highWaterMark = new BsonTimestamp(1, 0);
    IndexDefinition indexDefinition = MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition();

    // Create events at the highWaterMark.
    ChangeStreamDocument<RawBsonDocument> event1 =
        createEventWithOpTime(1, highWaterMark, indexDefinition);
    ChangeStreamDocument<RawBsonDocument> event2 =
        createEventWithOpTime(2, highWaterMark, indexDefinition);

    BsonDocument postBatchResumeToken = ChangeStreamUtils.resumeToken(highWaterMark);

    // isFreshStart=false means we use highWaterMark (inclusive)
    try (Mocks mocks =
        Mocks.createMocksWithOptime(
            highWaterMark, Optional.empty(), IGNORE_METRICS, /* isFreshStart= */ false)) {
      ChangeStreamBatch batch1 =
          new ChangeStreamBatch(
              toRawBsonDocuments(Arrays.asList(event1, event2)),
              postBatchResumeToken,
              highWaterMark);

      ChangeStreamBatch batch2 =
          new ChangeStreamBatch(Collections.emptyList(), postBatchResumeToken, STOP_AFTER_OP_TIME);

      when(mocks.changeStreamMongoClient.getNext()).thenReturn(batch1, batch2);

      BsonValue lastScannedToken =
          enableNaturalOrderScan ? POST_BATCH_RESUME_TOKEN : LAST_SCANNED_ID;
      BsonTimestamp result =
          mocks.applier.applyEvents(
              lastScannedToken, STOP_AFTER_OP_TIME, /* continue_sync= */ false);
      Assert.assertEquals(highWaterMark, result);

      // Verify that the command issued to open the change stream starts at the highWaterMark.
      ChangeStreamAggregateCommand command = mocks.commandCaptor.getValue();
      Optional<BsonTimestamp> startOpTime = command.getStartAtOperationTime();

      Assert.assertTrue("start operation time should be present", startOpTime.isPresent());
      Assert.assertEquals(
          "For resume, change stream should start at highWaterMark (inclusive)",
          highWaterMark,
          startOpTime.get());
    }
  }

  // Utility to create a change stream event with a specific optime.
  private static ChangeStreamDocument<RawBsonDocument> createEventWithOpTime(
      int id, BsonTimestamp opTime, IndexDefinition indexDefinition) {
    return new ChangeStreamDocument<>(
        OperationType.INSERT,
        ChangeStreamUtils.resumeToken(opTime),
        null,
        null,
        BsonUtils.documentToRaw(
            new BsonDocument()
                .append("_id", new BsonInt32(id))
                .append("foo", new BsonString("bar"))
                .append(
                    indexDefinition.getIndexId().toString(), // metadata namespace
                    new BsonDocument("_id", new BsonInt32(id)))),
        new BsonDocument().append("_id", new BsonInt32(id)),
        opTime,
        /*update description*/ null,
        /*txn number*/ null,
        null);
  }

  private static void assertThrowsInitialSyncException(
      Mocks mocks, InitialSyncException.Type type) {
    BsonValue lastScannedToken = enableNaturalOrderScan ? POST_BATCH_RESUME_TOKEN : LAST_SCANNED_ID;
    try {
      mocks.applier.applyEvents(lastScannedToken, STOP_AFTER_OP_TIME, true);
    } catch (InitialSyncException e) {
      Assert.assertEquals(type, e.getType());
    }
  }

  private static class Mocks implements Closeable {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final ChangeStreamMongoClient<InitialSyncException> changeStreamMongoClient;
    private final InitialSyncContext context;

    private final BufferlessChangeStreamApplier applier;

    private final ArgumentCaptor<ChangeStreamAggregateCommand> commandCaptor =
        ArgumentCaptor.forClass(ChangeStreamAggregateCommand.class);

    private Mocks(
        InitialSyncMongoClient mongoClient,
        ChangeStreamMongoClient<InitialSyncException> changeStreamMongoClient,
        IndexDefinitionGeneration indexDefinitionGeneration,
        BsonTimestamp highWaterMark,
        MetricsFactory metricsFactory,
        IndexMetricsUpdater indexMetricsUpdater,
        boolean isFreshStart) {
      this.changeStreamMongoClient = changeStreamMongoClient;

      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
          spy(
              IndexingWorkSchedulerFactory.create(
                  2, mock(Supplier.class), new SimpleMeterRegistry()));
      DocumentIndexer indexer =
          com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
              .mockDocumentIndexer();
      this.context =
          spy(
              InitialSyncContext.create(
                      SearchIndex.MOCK_INDEX_DEFINITION_GENERATION,
                      indexingWorkSchedulerFactory.getIndexingWorkScheduler(
                          indexDefinitionGeneration.getIndexDefinition()),
                      indexer,
                      indexMetricsUpdater,
                      Optional.empty(),
                      false,
                      enableNaturalOrderScan,
                      ToggleGate.opened())
                  .withProgress(OP_TIME));
      this.applier =
          spy(
              new BufferlessChangeStreamApplier(
                  Clock.systemUTC(),
                  Duration.ofMinutes(5),
                  Duration.ofMinutes(1),
                  List.of(),
                  false,
                  false,
                  this.context,
                  mongoClient,
                  indexDefinitionGeneration.getIndexDefinition().getLastObservedNamespace(),
                  highWaterMark,
                  metricsFactory,
                  enableNaturalOrderScan,
                  isFreshStart));
    }

    @Override
    public void close() {
      this.executor.shutdown();
      try {
        this.executor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Returns Mocks whose ChangeStreamMongoClient throws a DROPPED InitialSyncException when
     * getNext() is called.
     */
    private static Mocks changeStreamThrowsImmediately() throws Exception {
      Mocks mocks = createMocks();
      when(mocks.changeStreamMongoClient.getNext()).thenThrow(InitialSyncException.createDropped());
      return mocks;
    }

    /**
     * Returns Mocks whose ChangeStreamMongoClient throws a DROPPED InitialSyncException when
     * getNext() is called for the second time.
     */
    private static Mocks changeStreamThrowsDelayed() throws Exception {
      Mocks mocks = createMocks();
      when(mocks.changeStreamMongoClient.getNext())
          .thenReturn(EMPTY_BATCH)
          .thenThrow(InitialSyncException.createDropped());
      return mocks;
    }

    /** Returns Mocks whose changeStreamMongoClient returns the supplied event type. */
    private static Mocks changeStreamHasEventImmediately(
        ChangeStreamDocument<RawBsonDocument> event) throws Exception {
      Mocks mocks = createMocks();
      when(mocks.changeStreamMongoClient.getNext())
          .thenReturn(
              new ChangeStreamBatch(
                  toRawBsonDocuments(Collections.singletonList(event)),
                  ChangeStreamUtils.resumeToken(OP_TIME),
                  OP_TIME));
      return mocks;
    }

    /**
     * Returns Mocks whose changeStreamMongoClient returns the supplied event type when tryNext() is
     * called for the second time.
     */
    private static Mocks changeStreamHasEventDelayed(ChangeStreamDocument<RawBsonDocument> event)
        throws Exception {
      Mocks mocks = createMocks();
      when(mocks.changeStreamMongoClient.getNext())
          .thenReturn(EMPTY_BATCH)
          .thenReturn(
              new ChangeStreamBatch(
                  toRawBsonDocuments(Collections.singletonList(event)),
                  ChangeStreamUtils.resumeToken(OP_TIME),
                  OP_TIME));
      return mocks;
    }

    /**
     * Returns Mocks whose ChangeStreamMongoClient throws a DROPPED InitialSyncException when
     * getNext() is called for the second time.
     */
    private static Mocks changeStreamHasEventThenThrows(
        ChangeStreamDocument<RawBsonDocument> event, IndexMetricsUpdater indexMetricsUpdater)
        throws Exception {
      Mocks mocks = createMocks(Optional.empty(), indexMetricsUpdater);
      ChangeStreamBatch changeStreamBatch =
          new ChangeStreamBatch(
              toRawBsonDocuments(Collections.singletonList(event)),
              ChangeStreamUtils.resumeToken(OP_TIME),
              OP_TIME);
      when(mocks.changeStreamMongoClient.getNext())
          .thenReturn(changeStreamBatch, changeStreamBatch);
      doReturn(CompletableFuture.completedFuture(null))
          .doReturn(CompletableFuture.failedFuture(new FieldExceededLimitsException("unexpected")))
          .when(mocks.context)
          .schedule(any(), any(), any());
      return mocks;
    }

    private static Mocks createMocks() throws Exception {
      return createMocks(Optional.empty());
    }

    private static Mocks createMocks(Optional<IndexDefinitionGeneration> definitionGeneration)
        throws Exception {
      return createMocks(definitionGeneration, IGNORE_METRICS);
    }

    private static Mocks createMocks(
        Optional<IndexDefinitionGeneration> definitionGeneration,
        IndexMetricsUpdater indexMetricsUpdater)
        throws Exception {
      // Default to isFreshStart=false for existing tests (resume behavior)
      return createMocks(definitionGeneration, indexMetricsUpdater, false);
    }

    private static Mocks createMocks(
        Optional<IndexDefinitionGeneration> definitionGeneration,
        IndexMetricsUpdater indexMetricsUpdater,
        boolean isFreshStart)
        throws Exception {
      ChangeStreamMongoClient<InitialSyncException> changeStreamClient =
          mock(ChangeStreamMongoClient.class);
      when(changeStreamClient.getNext()).thenReturn(EMPTY_BATCH);

      InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
      when(mongoClient.getChangeStreamMongoClient(any(), any(), any(), any(), any()))
          .thenReturn(changeStreamClient);
      when(mongoClient.getSyncSourceHost()).thenReturn("testHost");

      Mocks mocks =
          new Mocks(
              mongoClient,
              changeStreamClient,
              definitionGeneration.orElse(MOCK_INDEX_DEFINITION_GENERATION),
              OP_TIME,
              new MetricsFactory("test", new SimpleMeterRegistry()),
              indexMetricsUpdater,
              isFreshStart);

      return mocks;
    }

    private static Mocks createMocksWithOptime(
        BsonTimestamp optime,
        Optional<IndexDefinitionGeneration> definitionGeneration,
        IndexMetricsUpdater indexMetricsUpdater,
        boolean isFreshStart)
        throws Exception {
      ChangeStreamMongoClient<InitialSyncException> changeStreamClient =
          mock(ChangeStreamMongoClient.class);
      when(changeStreamClient.getNext()).thenReturn(EMPTY_BATCH);

      InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
      when(mongoClient.getChangeStreamMongoClient(any(), any(), any(), any(), any()))
          .thenReturn(changeStreamClient);
      when(mongoClient.getSyncSourceHost()).thenReturn("testHost");

      Mocks mocks =
          new Mocks(
              mongoClient,
              changeStreamClient,
              definitionGeneration.orElse(MOCK_INDEX_DEFINITION_GENERATION),
              optime,
              new MetricsFactory("test", new SimpleMeterRegistry()),
              indexMetricsUpdater,
              isFreshStart);

      when(mongoClient.getChangeStreamMongoClient(
              mocks.commandCaptor.capture(), any(), any(), any(), any()))
          .thenReturn(changeStreamClient);
      return mocks;
    }
  }
}
