package com.xgen.mongot.replication.mongodb.initialsync;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.IndexTypeData.INDEX_TYPE_TAG_NAME;
import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH_AUTO_EMBEDDING;
import static com.xgen.testing.mongot.mock.index.SearchIndex.IGNORE_METRICS;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.mockAutoEmbeddingVectorDefinition;
import static com.xgen.testing.mongot.mock.index.VectorIndex.mockMaterializedViewAutoEmbeddingVectorDefinition;
import static com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils.toRawBsonDocuments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.DocumentEvent;
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
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.mock.index.IndexGeneration;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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
import org.bson.types.ObjectId;
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

  /** Distinct from OP_TIME so a batch's high water mark is distinguishable from the run's start. */
  private static final BsonTimestamp INITIAL_HIGH_WATER_MARK = new BsonTimestamp(3, 1);

  private static final BsonTimestamp BATCH_OP_TIME = new BsonTimestamp(5, 1);

  private static final BsonTimestamp SECOND_BATCH_OP_TIME = new BsonTimestamp(7, 1);

  /**
   * Sits between the two batch opTimes, which is what makes the apply loop read exactly two: it
   * keeps going while the stop point is ahead of the last batch's opTime.
   */
  private static final BsonTimestamp STOP_AFTER_FIRST_BATCH = new BsonTimestamp(6, 0);

  private static final BsonDocument POST_BATCH_RESUME_TOKEN =
      new BsonDocument().append("$recordId", new BsonInt64(100));

  private static final ChangeStreamBatch EMPTY_BATCH =
      new ChangeStreamBatch(
          Collections.emptyList(), ChangeStreamUtils.resumeToken(OP_TIME), Optional.of(OP_TIME));

  @Parameterized.Parameter public static boolean enableNaturalOrderScan;

  @Parameterized.Parameters(name = "enableNaturalOrderScan={0}")
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

  @Test
  public void partialEmbedMetrics_onlyRegisterForMaterializedViewAutoEmbeddingIndexes()
      throws Exception {
    SimpleMeterRegistry regularRegistry = new SimpleMeterRegistry();
    try (Mocks ignored =
        Mocks.createMocks(
            Optional.empty(), IGNORE_METRICS, false, new MetricsFactory("test", regularRegistry))) {
      assertNoAutoEmbeddingMetrics(regularRegistry);
    }

    SimpleMeterRegistry legacyAutoEmbeddingRegistry = new SimpleMeterRegistry();
    try (Mocks ignored =
        Mocks.createMocks(
            Optional.of(
                IndexGeneration.mockDefinitionGeneration(
                    mockAutoEmbeddingVectorDefinition(new ObjectId()))),
            IGNORE_METRICS,
            false,
            new MetricsFactory("test", legacyAutoEmbeddingRegistry))) {
      assertNoAutoEmbeddingMetrics(legacyAutoEmbeddingRegistry);
    }

    SimpleMeterRegistry materializedViewAutoEmbeddingRegistry = new SimpleMeterRegistry();
    try (Mocks ignored =
        Mocks.createMocks(
            Optional.of(
                IndexGeneration.mockDefinitionGeneration(
                    mockMaterializedViewAutoEmbeddingVectorDefinition(new ObjectId()))),
            IGNORE_METRICS,
            false,
            new MetricsFactory("test", materializedViewAutoEmbeddingRegistry))) {
      assertThat(
              materializedViewAutoEmbeddingRegistry
                  .find("test.partialEmbedApplied")
                  .tag(INDEX_TYPE_TAG_NAME, TAG_VECTOR_SEARCH_AUTO_EMBEDDING.tagValue)
                  .counter())
          .isNotNull();
      assertThat(
              materializedViewAutoEmbeddingRegistry
                  .find("test.viewFieldDependenciesDecision")
                  .tag(INDEX_TYPE_TAG_NAME, TAG_VECTOR_SEARCH_AUTO_EMBEDDING.tagValue)
                  .meters())
          .isNotEmpty();
    }
  }

  @Test
  public void applyEvents_searchAutoEmbedPathUpdate_schedulesOnlyApplicableUpdate()
      throws Exception {
    var indexDefinition = SearchIndex.MOCK_SEARCH_AUTO_EMBEDDING_INDEX_DEFINITION;
    var definitionGeneration = IndexGeneration.mockDefinitionGeneration(indexDefinition);
    var sourceUpdateDescription =
        new UpdateDescription(null, new BsonDocument("content", new BsonString("updated")));
    var sourceUpdate =
        ChangeStreamUtils.dataEvent(
            OperationType.UPDATE,
            0,
            sourceUpdateDescription,
            new BsonDocument()
                .append("_id", new BsonInt32(0))
                .append("content", new BsonString("updated"))
                .append(
                    indexDefinition.getIndexId().toString(),
                    new BsonDocument("_id", new BsonInt32(0))));
    var unrelatedUpdateDescription =
        new UpdateDescription(null, new BsonDocument("unrelated", new BsonString("updated")));
    var unrelatedUpdate =
        ChangeStreamUtils.updateEvent(1, unrelatedUpdateDescription, indexDefinition);

    try (Mocks mocks = Mocks.createMocks(Optional.of(definitionGeneration))) {
      when(mocks.changeStreamMongoClient.getNext())
          .thenReturn(
              new ChangeStreamBatch(
                  toRawBsonDocuments(List.of(sourceUpdate, unrelatedUpdate)),
                  ChangeStreamUtils.resumeToken(OP_TIME),
                  Optional.of(OP_TIME)));
      doReturn(CompletableFuture.completedFuture(null))
          .when(mocks.context)
          .schedule(any(), any(), any());

      BsonValue lastScannedToken =
          enableNaturalOrderScan ? POST_BATCH_RESUME_TOKEN : LAST_SCANNED_ID;
      ArgumentCaptor<List<DocumentEvent>> scheduledBatch = ArgumentCaptor.captor();
      ArgumentCaptor<SchedulerQueue.Priority> scheduledPriority = ArgumentCaptor.captor();

      mocks.applier.applyEvents(lastScannedToken, OP_TIME, false);

      verify(mocks.context).schedule(scheduledBatch.capture(), scheduledPriority.capture(), any());
      Assert.assertEquals(
          SchedulerQueue.Priority.INITIAL_SYNC_CHANGE_STREAM, scheduledPriority.getValue());
      Assert.assertEquals(1, scheduledBatch.getValue().size());
      DocumentEvent scheduledEvent = scheduledBatch.getValue().getFirst();
      Assert.assertEquals(DocumentEvent.EventType.UPDATE, scheduledEvent.getEventType());
      Assert.assertEquals(
          sourceUpdate.getFullDocument(), scheduledEvent.getDocument().orElseThrow());
      Assert.assertEquals(
          "updated", scheduledEvent.getChangedEmbedTexts().get(FieldPath.parse("content")));
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
              Optional.of(expectedStartOpTime));

      ChangeStreamBatch batch2 =
          new ChangeStreamBatch(
              Collections.emptyList(), postBatchResumeToken, Optional.of(STOP_AFTER_OP_TIME));

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
              Optional.of(highWaterMark));

      ChangeStreamBatch batch2 =
          new ChangeStreamBatch(
              Collections.emptyList(), postBatchResumeToken, Optional.of(STOP_AFTER_OP_TIME));

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
        OperationType.INSERT.getValue(),
        ChangeStreamUtils.resumeToken(opTime),
        null,
        null,
        null,
        BsonUtils.documentToRaw(
            new BsonDocument()
                .append("_id", new BsonInt32(id))
                .append("foo", new BsonString("bar"))
                .append(
                    indexDefinition.getIndexId().toString(), // metadata namespace
                    new BsonDocument("_id", new BsonInt32(id)))),
        null,
        new BsonDocument().append("_id", new BsonInt32(id)),
        opTime,
        /*update description*/ null,
        /*txn number*/ null,
        null,
        null,
        null,
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

  @Test
  public void applyEvents_parallelScan_handsOverHighWaterMarkInsteadOfStagingABlob()
      throws Exception {
    ParallelCollectionScanResumeState resumeState = parallelResumeState();

    try (Mocks mocks = Mocks.createMocksWithParallelResumeState(resumeState)) {
      when(mocks.changeStreamMongoClient.getNext()).thenReturn(batchAt(BATCH_OP_TIME));
      doReturn(CompletableFuture.completedFuture(null))
          .when(mocks.context)
          .scheduleWithoutCommitUserData(any(), any());

      mocks.applier.applyEvents(lastScannedToken(), OP_TIME, false);

      verify(mocks.context, never()).schedule(any(), any(), any());
      verify(mocks.context).scheduleWithoutCommitUserData(any(), any());
      assertThat(resumeState.getHighWaterMark()).isEqualTo(BATCH_OP_TIME);
    }
  }

  /**
   * Takes two batches because one cannot tell a mark advanced once per batch from one advanced once
   * at the end of the call. Reading the mark as each batch is scheduled pins the ordering the scan
   * windows depend on: a batch's position is already published by the time the next batch is
   * scheduled, so the applier never hands back a future that has run ahead of the mark.
   */
  @Test
  public void applyEvents_parallelScan_advancesTheHighWaterMarkOncePerBatch() throws Exception {
    ParallelCollectionScanResumeState resumeState = parallelResumeState();

    try (Mocks mocks = Mocks.createMocksWithParallelResumeState(resumeState)) {
      when(mocks.changeStreamMongoClient.getNext())
          .thenReturn(batchAt(BATCH_OP_TIME), batchAt(SECOND_BATCH_OP_TIME));

      List<BsonTimestamp> marksWhenScheduled = new ArrayList<>();
      doAnswer(
              invocation -> {
                marksWhenScheduled.add(resumeState.getHighWaterMark());
                return CompletableFuture.completedFuture(null);
              })
          .when(mocks.context)
          .scheduleWithoutCommitUserData(any(), any());

      mocks.applier.applyEvents(lastScannedToken(), STOP_AFTER_FIRST_BATCH, false);

      assertThat(marksWhenScheduled)
          .containsExactly(INITIAL_HIGH_WATER_MARK, BATCH_OP_TIME)
          .inOrder();
      assertThat(resumeState.getHighWaterMark()).isEqualTo(SECOND_BATCH_OP_TIME);
    }
  }

  @Test
  public void applyEvents_parallelScan_leavesHighWaterMarkAloneWhenIndexingFails()
      throws Exception {
    ParallelCollectionScanResumeState resumeState = parallelResumeState();

    try (Mocks mocks = Mocks.createMocksWithParallelResumeState(resumeState)) {
      when(mocks.changeStreamMongoClient.getNext()).thenReturn(batchAt(BATCH_OP_TIME));
      doReturn(CompletableFuture.failedFuture(new FieldExceededLimitsException("unexpected")))
          .when(mocks.context)
          .scheduleWithoutCommitUserData(any(), any());

      Assert.assertThrows(
          InitialSyncException.class,
          () -> mocks.applier.applyEvents(lastScannedToken(), OP_TIME, false));

      assertThat(resumeState.getHighWaterMark()).isEqualTo(INITIAL_HIGH_WATER_MARK);
    }
  }

  private static BsonValue lastScannedToken() {
    return enableNaturalOrderScan ? POST_BATCH_RESUME_TOKEN : LAST_SCANNED_ID;
  }

  private static ChangeStreamBatch batchAt(BsonTimestamp opTime) {
    return new ChangeStreamBatch(
        Collections.emptyList(), ChangeStreamUtils.resumeToken(opTime), Optional.of(opTime));
  }

  private static ParallelCollectionScanResumeState parallelResumeState() {
    return new ParallelCollectionScanResumeState(
        MOCK_INDEX_DEFINITION_GENERATION.generation().formatVersion.versionNumber(),
        UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee"),
        Optional.of("testHost"),
        INITIAL_HIGH_WATER_MARK,
        List.of(
            new ParallelCollectionScanPartitionState(
                0, Optional.empty(), Optional.empty(), Optional.empty(), false)));
  }

  private static void assertNoAutoEmbeddingMetrics(SimpleMeterRegistry meterRegistry) {
    assertThat(meterRegistry.find("test.partialEmbedApplied").meters()).isEmpty();
    assertThat(meterRegistry.find("test.viewFieldDependenciesDecision").meters()).isEmpty();
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
        boolean isFreshStart,
        Optional<ParallelCollectionScanResumeState> parallelResumeState) {
      this.changeStreamMongoClient = changeStreamMongoClient;

      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
          spy(
              IndexingWorkSchedulerFactory.create(
                  2, mock(Supplier.class), new SimpleMeterRegistry(), true));
      DocumentIndexer indexer =
          com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
              .mockDocumentIndexer();
      this.context =
          spy(
              InitialSyncContext.create(
                      indexDefinitionGeneration,
                      indexingWorkSchedulerFactory.getIndexingWorkScheduler(
                          indexDefinitionGeneration.getIndexDefinition()),
                      indexer,
                      indexMetricsUpdater,
                      Optional.empty(),
                      false,
                      enableNaturalOrderScan,
                      false,
                      1,
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
                  isFreshStart,
                  parallelResumeState));
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
                  Optional.of(OP_TIME)));
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
                  Optional.of(OP_TIME)));
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
              Optional.of(OP_TIME));
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
      return createMocks(
          definitionGeneration,
          indexMetricsUpdater,
          isFreshStart,
          new MetricsFactory("test", new SimpleMeterRegistry()));
    }

    private static Mocks createMocks(
        Optional<IndexDefinitionGeneration> definitionGeneration,
        IndexMetricsUpdater indexMetricsUpdater,
        boolean isFreshStart,
        MetricsFactory metricsFactory)
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
              metricsFactory,
              indexMetricsUpdater,
              isFreshStart,
              Optional.empty());

      return mocks;
    }

    /** Returns Mocks for an applier running alongside a parallel collection scan. */
    private static Mocks createMocksWithParallelResumeState(
        ParallelCollectionScanResumeState parallelResumeState) throws Exception {
      ChangeStreamMongoClient<InitialSyncException> changeStreamClient =
          mock(ChangeStreamMongoClient.class);
      when(changeStreamClient.getNext()).thenReturn(EMPTY_BATCH);

      InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
      when(mongoClient.getChangeStreamMongoClient(any(), any(), any(), any(), any()))
          .thenReturn(changeStreamClient);
      when(mongoClient.getSyncSourceHost()).thenReturn("testHost");

      return new Mocks(
          mongoClient,
          changeStreamClient,
          MOCK_INDEX_DEFINITION_GENERATION,
          OP_TIME,
          new MetricsFactory("test", new SimpleMeterRegistry()),
          IGNORE_METRICS,
          false,
          Optional.of(parallelResumeState));
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
              isFreshStart,
              Optional.empty());

      when(mongoClient.getChangeStreamMongoClient(
              mocks.commandCaptor.capture(), any(), any(), any(), any()))
          .thenReturn(changeStreamClient);
      return mocks;
    }
  }
}
