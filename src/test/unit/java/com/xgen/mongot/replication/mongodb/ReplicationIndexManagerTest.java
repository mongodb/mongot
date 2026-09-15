package com.xgen.mongot.replication.mongodb;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.status.StaleStatusReason.UNEXPECTED_ERROR;
import static com.xgen.mongot.replication.mongodb.ReplicationIndexManager.EXCEEDED_LIMIT_REASON_PREFIX;
import static com.xgen.mongot.util.Condition.await;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockIndex;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockInitializedIndex;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockMetricsFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Keep;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoQueryException;
import com.mongodb.ServerAddress;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.Reason;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.replication.mongodb.IndexManager.State;
import com.xgen.mongot.replication.mongodb.ReplicationIndexManager.FailedIndexAction;
import com.xgen.mongot.replication.mongodb.common.BufferlessIdOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexStateInfo;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymManager;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymMappingHighWaterMark;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymMappingManager;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymMappingManagerFactory;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class ReplicationIndexManagerTest {

  @Test
  public void testNoCommitUserDataEnqueuesInitialSync() throws Exception {
    try (Mocks mocks = Mocks.noCommitUserData()) {
      verify(mocks.initialSyncQueue, timeout(1000))
          .enqueue(
              any(),
              eq(MOCK_INDEX_DEFINITION_GENERATION),
              any(),
              any(),
              anyBoolean(),
              anyBoolean());

      verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.INITIAL_SYNC));
    }
  }

  @Test
  public void testMismatchingIndexVersionHalts() throws Exception {
    try (Mocks mocks = Mocks.mismatchingBackendIndexVersion()) {
      var inOrder = inOrder(mocks.index, mocks.documentIndexer);

      // Production behavior is to call Crash.now() and terminate the process.
      //
      // In tests, a JVM security manager ensures the JVM is not halted, and throws an exception.
      // This exception is caught by the replication manager, and the index is marked as failed
      // without halting the JVM.
      //
      // So here, we verify that the index is marked as failed and dropped.

      // Check the only interaction with the IndexWriter was a call to getCommitUserData.
      inOrder.verify(mocks.index, timeout(1000)).getWriter();
      verify(mocks.index.getWriter(), timeout(1000)).getCommitUserData();
      verifyNoMoreInteractions(mocks.index.getWriter());
      verifyNoMoreInteractions(mocks.documentIndexer);

      // Then, the index should have been marked failed and dropped.
      inOrder
          .verify(mocks.index, timeout(2000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED, IndexStatus.Reason.INITIALIZATION_FAILED));
      inOrder.verify(mocks.index, timeout(1000)).close();
      inOrder.verify(mocks.index, timeout(1000)).drop();
      assertThat(mocks.index.getStatus().getMessage().orElseThrow())
          .startsWith("replication failed (action=drop): ");
      verifyNoInteractions(mocks.initialSyncQueue, mocks.steadyStateManager);
      mocks.assertStateTransitions(IndexManager.State.INITIALIZING, IndexManager.State.FAILED);

      mocks.assertDropMetric(Reason.INITIALIZATION_FAILED);
    }
  }

  @Test
  public void testBufferlessInitialSyncResumesWithCurrentIndexVersion() throws Exception {
    try (Mocks mocks =
        Mocks.initialSyncResumeCurrentIndexVersion(Mocks.BUFFERLESS_INITIAL_SYNC_RESUME_INFO)) {
      verify(mocks.initialSyncQueue, timeout(1000))
          .enqueueResume(
              any(),
              eq(MOCK_INDEX_DEFINITION_GENERATION),
              any(),
              eq(Mocks.BUFFERLESS_INITIAL_SYNC_RESUME_INFO),
              any(),
              anyBoolean(),
              anyBoolean());

      verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.INITIAL_SYNC));
      mocks.assertStateTransitions(
          IndexManager.State.INITIALIZING, IndexManager.State.INITIAL_SYNC);
    }
  }

  @Test
  public void testInitialSyncHaltsWithMismatchingIndexVersion() throws Exception {
    try (Mocks mocks = Mocks.initialSyncResumeMismatchingIndexVersion()) {
      var inOrder = inOrder(mocks.index, mocks.documentIndexer);

      // Production behavior is to call Crash.now() and terminate the process.
      //
      // In tests, a JVM security manager ensures the JVM is not halted, and throws an exception.
      // This exception is caught by the replication manager, and the index is marked as failed
      // without halting the JVM.
      //
      // So here, we verify that the index is marked as failed and dropped.
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED, IndexStatus.Reason.INITIALIZATION_FAILED));
      inOrder.verify(mocks.index, timeout(1000)).close();
      inOrder.verify(mocks.index, timeout(1000)).drop();
      inOrder.verifyNoMoreInteractions();

      verifyNoInteractions(mocks.initialSyncQueue, mocks.steadyStateManager);
      mocks.assertStateTransitions(IndexManager.State.INITIALIZING, IndexManager.State.FAILED);
    }
  }

  @Test
  public void testResumingInitialSyncDoesNotResumeAfterRequiresResyncException() throws Exception {
    var exception = new MongoException("A NonRetryableException");
    try (Mocks mocks =
        Mocks.exceptionalInitialSyncWithResumeInfo(
            InitialSyncException.createRequiresResync(exception, "err"))) {
      var inOrder = Mockito.inOrder(mocks.initialSyncQueue, mocks.index);

      // Verify that initial sync was resumed once as we create it with resume info
      inOrder
          .verify(mocks.initialSyncQueue, timeout(1000))
          .enqueueResume(
              any(),
              eq(MOCK_INDEX_DEFINITION_GENERATION),
              any(),
              eq(Mocks.BUFFERLESS_INITIAL_SYNC_RESUME_INFO),
              any(),
              anyBoolean(),
              anyBoolean());
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.INITIAL_SYNC));

      // Verify that the initial sync is restarted as a new resync because it threw
      // a REQUIRES_RESYNC exception
      inOrder
          .verify(mocks.initialSyncQueue, timeout(1000))
          .enqueue(
              any(),
              eq(MOCK_INDEX_DEFINITION_GENERATION),
              any(),
              any(),
              anyBoolean(),
              anyBoolean());
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.INITIAL_SYNC));

      inOrder.verifyNoMoreInteractions();
      mocks.assertStateTransitions(
          IndexManager.State.INITIAL_SYNC, IndexManager.State.INITIAL_SYNC_BACKOFF);
      mocks.assertMetric(
          InitialSyncException.class,
          InitialSyncException.Type.REQUIRES_RESYNC.name(),
          ReplicationIndexManager.exceptionClassWithErrorCode(exception),
          "mongod");
    }
  }

  @Test
  public void testInitialSyncDoesResumeAfterResumableTransientException() throws Exception {
    var causeException = new MongoException(9001, "exception");
    try (Mocks mocks =
        Mocks.exceptionalInitialSyncWithResumeInfo(
            InitialSyncException.createResumableTransient(causeException))) {
      var inOrder = Mockito.inOrder(mocks.initialSyncQueue, mocks.index);

      // Verify that initial sync was resumed once, then resumed again.
      for (int times = 0; times < 2; times++) {
        inOrder
            .verify(mocks.initialSyncQueue, timeout(1000))
            .enqueueResume(
                any(),
                eq(MOCK_INDEX_DEFINITION_GENERATION),
                any(),
                eq(Mocks.BUFFERLESS_INITIAL_SYNC_RESUME_INFO),
                any(),
                anyBoolean(),
                anyBoolean());
        inOrder
            .verify(mocks.index, timeout(1000))
            .setStatus(indexStatusWithCode(IndexStatus.StatusCode.INITIAL_SYNC));
      }

      inOrder.verifyNoMoreInteractions();
      mocks.assertStateTransitions(
          IndexManager.State.INITIALIZING, IndexManager.State.INITIAL_SYNC);
      mocks.assertMetric(
          InitialSyncException.class,
          InitialSyncException.Type.RESUMABLE_TRANSIENT.name(),
          ReplicationIndexManager.exceptionClassWithErrorCode(causeException),
          "mongod");
    }
  }

  @Test
  public void testInitialSyncResyncsAfterTransientViewErrorAndKeepsTheWarningMessageInStatus()
      throws Exception {
    var cause =
        new MongoQueryException(
            new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(123))
                .append("errmsg", new BsonString("PlanExecutor error")),
            new ServerAddress("host"));
    try (Mocks mocks =
        Mocks.exceptionalInitialSyncOnView(InitialSyncException.createRequiresResync(cause))) {

      verify(mocks.initialSyncQueue, timeout(10000).times(2))
          .enqueue(any(), any(), any(), any(), anyBoolean(), anyBoolean());

      mocks.assertStateTransitions(
          IndexManager.State.INITIAL_SYNC, IndexManager.State.INITIAL_SYNC_BACKOFF);

      InOrder inOrder = inOrder(mocks.index);

      // initial status
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(
              argThat(
                  status ->
                      status.getStatusCode() == IndexStatus.StatusCode.INITIAL_SYNC
                          && status.getMessage().isEmpty()));

      // status set on error
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(
              argThat(
                  status ->
                      status.getStatusCode() == IndexStatus.StatusCode.INITIAL_SYNC
                          && status.getMessage().orElseThrow().contains("PlanExecutor error")));

      // status during re-start after the backoff period
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(
              argThat(status -> status.getStatusCode() == IndexStatus.StatusCode.NOT_STARTED));

      // status set on re-sync initialization
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(
              argThat(
                  status ->
                      status.getStatusCode() == IndexStatus.StatusCode.INITIAL_SYNC
                          && status.getMessage().orElseThrow().contains("PlanExecutor error")));
    }
  }

  @Test
  public void testInitialSyncResyncsAfterQueryRejectedBySettingsAndKeepsTheErrorMessageInStatus()
      throws Exception {
    var cause =
        new MongoQueryException(
            new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(411))
                .append("errmsg", new BsonString("Query rejected by admin settings")),
            new ServerAddress("host"));
    try (Mocks mocks =
        Mocks.exceptionalInitialSync(InitialSyncException.createRequiresResync(cause))) {

      // The index keeps resyncing (enqueued at least twice) rather than failing.
      verify(mocks.initialSyncQueue, timeout(10000).atLeast(2))
          .enqueue(any(), any(), any(), any(), anyBoolean(), anyBoolean());

      mocks.assertStateTransitions(
          IndexManager.State.INITIAL_SYNC, IndexManager.State.INITIAL_SYNC_BACKOFF);

      InOrder inOrder = inOrder(mocks.index);

      // initial status, no message
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(
              argThat(
                  status ->
                      status.getStatusCode() == IndexStatus.StatusCode.INITIAL_SYNC
                          && status.getMessage().isEmpty()));

      // status set on error, carrying the query-settings rejection guidance and driver detail
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(
              argThat(
                  status ->
                      status.getStatusCode() == IndexStatus.StatusCode.INITIAL_SYNC
                          && status.getMessage().orElse("").contains("removeQuerySettings")
                          && status
                              .getMessage()
                              .orElse("")
                              .contains("Query rejected by admin settings")));
    }
  }

  @Test
  public void testInitialSyncDoesRetryAfterDoesNotExistException() throws Exception {
    try (Mocks mocks =
        Mocks.exceptionalInitialSync(
            InitialSyncException.createDoesNotExist("collection does not exist locally"))) {
      var inOrder = Mockito.inOrder(mocks.initialSyncQueue, mocks.index);

      // Verify that initial sync was attempted and retried after encountering the exception.
      inOrder
          .verify(mocks.initialSyncQueue, timeout(1000))
          .enqueue(
              any(),
              eq(MOCK_INDEX_DEFINITION_GENERATION),
              any(),
              any(),
              anyBoolean(),
              anyBoolean());
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.DOES_NOT_EXIST, IndexStatus.Reason.COLLECTION_NOT_FOUND));
      Assert.assertFalse(mocks.index.getStatus().isIndexDropped());
      inOrder
          .verify(mocks.initialSyncQueue, timeout(1000))
          .enqueue(
              any(),
              eq(MOCK_INDEX_DEFINITION_GENERATION),
              any(),
              any(),
              anyBoolean(),
              anyBoolean());
      mocks.assertStateTransitions(
          IndexManager.State.INITIALIZING, IndexManager.State.INITIAL_SYNC);

      mocks.assertMetric(
          InitialSyncException.class,
          InitialSyncException.Type.DOES_NOT_EXIST.name(),
          "None",
          "None");
    }
  }

  @Test
  public void testInitialSyncShutsDownAfterDoesNotExistExceptionWithFeatureFlagEnabled()
      throws Exception {
    InitialSyncException exception =
        InitialSyncException.createDoesNotExist("collection does not exist locally");
    // shutDownReplicationWhenCollectionNotFound should be true
    FeatureFlags featureFlags =
        FeatureFlags.withDefaults()
            .enable(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)
            .build();
    IndexStateInfo indexStateInfo =
        IndexStateInfo.create(StatusCode.DOES_NOT_EXIST, Reason.COLLECTION_NOT_FOUND);
    IndexCommitUserData indexCommitUserData =
        IndexCommitUserData.createFromIndexStateInfo(indexStateInfo);
    try (Mocks mocks = Mocks.exceptionalInitialSyncWithFeatureFlag(exception, featureFlags)) {
      var inOrder = Mockito.inOrder(mocks.initialSyncQueue, mocks.index, mocks.documentIndexer);

      // Verify that initial sync was attempted and index status set
      inOrder
          .verify(mocks.initialSyncQueue, timeout(1000))
          .enqueue(
              any(),
              eq(MOCK_INDEX_DEFINITION_GENERATION),
              any(),
              any(),
              anyBoolean(),
              anyBoolean());
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.DOES_NOT_EXIST, IndexStatus.Reason.COLLECTION_NOT_FOUND));

      verifyNoMoreInteractions(mocks.initialSyncQueue);
      // Verify that the indexStateInfo was committed

      inOrder
          .verify(mocks.documentIndexer, timeout(1000))
          .updateCommitUserData(indexCommitUserData);

      inOrder.verify(mocks.documentIndexer, timeout(1000)).commit();

      mocks.assertStateTransitions(State.INITIAL_SYNC, State.SHUT_DOWN);

      mocks.assertMetric(
          InitialSyncException.class,
          InitialSyncException.Type.DOES_NOT_EXIST.name(),
          "None",
          "None");
    }
  }

  @Test
  public void testReadersRefreshedBeforeIndexSteady() throws Exception {
    try (Mocks mocks = Mocks.successfulInitialSync()) {
      InOrder inOrder = Mockito.inOrder(mocks.index, mocks.index.getReader());

      // Reader should be refreshed before the index is marked as steady
      inOrder.verify(mocks.index.getReader(), timeout(1000)).refresh();
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.STEADY));
      mocks.assertStateTransitions(
          IndexManager.State.INITIAL_SYNC, IndexManager.State.STEADY_STATE);
    }
  }

  @Test
  public void testShutdownCancelsInitialSync() throws Exception {
    try (Mocks mocks = Mocks.noCommitUserData()) {
      verify(mocks.initialSyncQueue, timeout(1000))
          .enqueue(
              any(),
              eq(MOCK_INDEX_DEFINITION_GENERATION),
              any(),
              any(),
              anyBoolean(),
              anyBoolean());

      mocks.manager.shutdown();
      verify(mocks.initialSyncQueue).cancel(eq(MOCK_INDEX_GENERATION_ID));
      verify(mocks.synonymMappingManager).shutdown();
      mocks.assertStateTransitions(IndexManager.State.INITIAL_SYNC, IndexManager.State.SHUT_DOWN);
    }
  }

  @Test
  public void testShutdownWhileWaitingForInitialSyncRequeue() throws Exception {
    try (Mocks mocks =
        Mocks.exceptionalInitialSync(
            InitialSyncException.createFieldLimitExceeded("something wrong"), Duration.ofDays(1))) {
      // Wait for the index to be enqueued and failed.
      verify(mocks.initialSyncQueue, timeout(Duration.ofSeconds(5).toMillis()))
          .enqueue(
              any(),
              eq(MOCK_INDEX_DEFINITION_GENERATION),
              any(),
              any(),
              anyBoolean(),
              anyBoolean());
      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED, IndexStatus.Reason.EXCEED_MAX_LIMIT));

      // Shutting down the manager after having the initial sync fail should not try to cancel the
      // enqueue.
      assertSuccess(mocks.manager.shutdown());
      verify(mocks.initialSyncQueue, times(0)).cancel(eq(MOCK_INDEX_GENERATION_ID));
      verify(mocks.synonymMappingManager).shutdown();
      mocks.assertStateTransitions(
          IndexManager.State.FAILED_EXCEEDED, IndexManager.State.SHUT_DOWN);
      mocks.assertStateTransitions(
          IndexManager.State.INITIALIZING, IndexManager.State.INITIAL_SYNC);
      mocks.assertMetric(
          InitialSyncException.class,
          InitialSyncException.Type.FIELD_EXCEEDED.name(),
          "None",
          "None");
    }
  }

  @Test
  public void testShutdownAfterIndexExceededDoesNothing() throws Exception {
    try (Mocks mocks =
        Mocks.exceptionalInitialSync(
            InitialSyncException.createFieldLimitExceeded("exceeded"), Duration.ofDays(1))) {
      assertShutdownAfterIndexFailDoesNothing(mocks);
    }
  }

  @Test
  public void testShutdownAfterIndexIsFailedDoesNothing() throws Exception {
    try (Mocks mocks =
        Mocks.exceptionalInitialSync(
            InitialSyncException.createFailed(new RuntimeException("unexpected")),
            Duration.ofDays(1))) {
      assertShutdownAfterIndexFailDoesNothing(mocks);

      mocks.assertMetric(
          InitialSyncException.class,
          InitialSyncException.Type.FAILED.name(),
          RuntimeException.class.getSimpleName(),
          "None");
    }
  }

  private void assertShutdownAfterIndexFailDoesNothing(Mocks mocks) throws Exception {
    // wait for index to fail
    verify(mocks.index, timeout(1000))
        .setStatus(indexStatusWithCode(IndexStatus.StatusCode.FAILED));

    clearInvocations(mocks.index, mocks.initialSyncQueue, mocks.steadyStateManager);

    // Shutting down the manager after having the exceeded fail doesn't need to cancel or do
    // anything particular on the index.
    assertSuccess(mocks.manager.shutdown());

    verify(mocks.initialSyncQueue, times(0)).cancel(any());
    verify(mocks.steadyStateManager, times(0)).stop(any());
    Assert.assertSame(IndexStatus.StatusCode.FAILED, mocks.index.getStatus().getStatusCode());
  }

  @Test
  public void testShutdownAfterOplogFallOffDoesNothing() throws Exception {
    var causeException = new MongoException(286, "ChangeStreamHistoryLost");
    try (Mocks mocks =
        Mocks.exceptionalSteadyState(
            SteadyStateException.createNonInvalidatingResync(causeException))) {
      // wait for index to go recovering
      verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT));

      clearInvocations(mocks.index, mocks.initialSyncQueue, mocks.steadyStateManager);

      assertSuccess(mocks.manager.shutdown());
      verify(mocks.steadyStateManager, times(0)).stop(any());
      Assert.assertSame(
          IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT, mocks.index.getStatus().getStatusCode());

      mocks.assertMetric(
          SteadyStateException.class,
          SteadyStateException.Type.NON_INVALIDATING_RESYNC.name(),
          ReplicationIndexManager.exceptionClassWithErrorCode(causeException),
          "mongod");
    }
  }

  @Test
  public void testSuccessfulInitialSyncResumesSteadyState() throws Exception {
    try (Mocks mocks = Mocks.successfulInitialSync()) {
      // Wait for the index to be put into STEADY.
      verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.STEADY));

      // Ensure that the following events happen in order:
      //  - the index is enqueued to initial sync
      //  - the index is put into the INITIAL_SYNC state
      //  - the index is put into the STEADY state
      //  - the SteadyStateIndexManager for the index is created
      InOrder inOrder =
          Mockito.inOrder(mocks.initialSyncQueue, mocks.steadyStateManager, mocks.index);

      inOrder
          .verify(mocks.initialSyncQueue)
          .enqueue(
              any(),
              eq(MOCK_INDEX_DEFINITION_GENERATION),
              any(),
              any(),
              anyBoolean(),
              anyBoolean());

      inOrder
          .verify(mocks.index)
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.INITIAL_SYNC));

      inOrder.verify(mocks.index).setStatus(indexStatusWithCode(IndexStatus.StatusCode.STEADY));

      inOrder
          .verify(mocks.steadyStateManager, timeout(1000))
          .add(any(), any(), any(), any(), any(), anyBoolean());

      assertEquals(IndexStatus.StatusCode.STEADY, mocks.index.getStatus().getStatusCode());
      mocks.assertStateTransitions(
          IndexManager.State.INITIAL_SYNC, IndexManager.State.STEADY_STATE);
      assertThat(
              mocks
                  .index
                  .getMetricsUpdater()
                  .getIndexingMetricsUpdater()
                  .getReplicationOpTimeInfo()
                  .snapshot())
          .isPresent();
    }
  }

  @Test
  public void testForceMergeAfterInitialSyncEnabledBySystemProperty() throws Exception {
    System.setProperty(ReplicationIndexManager.FORCE_MERGE_MAX_NUM_SEGMENTS_PROPERTY, "1");
    try (Mocks mocks = Mocks.successfulInitialSync()) {
      IndexWriter writer = mocks.index.getWriter();

      InOrder inOrder = Mockito.inOrder(mocks.index, writer, mocks.documentIndexer);
      inOrder.verify(writer, timeout(1000)).forceMerge(1);
      inOrder.verify(mocks.documentIndexer, timeout(1000)).commit();
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.STEADY));

      mocks.assertStateTransitions(
          IndexManager.State.INITIAL_SYNC, IndexManager.State.STEADY_STATE);
    } finally {
      System.clearProperty(ReplicationIndexManager.FORCE_MERGE_MAX_NUM_SEGMENTS_PROPERTY);
    }
  }

  @Test
  public void testForceMergeAfterInitialSyncUsesConfiguredSegmentCount() throws Exception {
    System.setProperty(ReplicationIndexManager.FORCE_MERGE_MAX_NUM_SEGMENTS_PROPERTY, "4");
    try (Mocks mocks = Mocks.successfulInitialSync()) {
      IndexWriter writer = mocks.index.getWriter();

      InOrder inOrder = Mockito.inOrder(mocks.index, writer, mocks.documentIndexer);
      inOrder.verify(writer, timeout(1000)).forceMerge(4);
      inOrder.verify(mocks.documentIndexer, timeout(1000)).commit();
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.STEADY));

      mocks.assertStateTransitions(
          IndexManager.State.INITIAL_SYNC, IndexManager.State.STEADY_STATE);
    } finally {
      System.clearProperty(ReplicationIndexManager.FORCE_MERGE_MAX_NUM_SEGMENTS_PROPERTY);
    }
  }

  @Test
  public void testNoForceMergeAfterInitialSyncWhenPropertyUnset() throws Exception {
    try (Mocks mocks = Mocks.successfulInitialSync()) {
      IndexWriter writer = mocks.index.getWriter();

      verify(mocks.steadyStateManager, timeout(1000))
          .add(any(), any(), any(), any(), any(), anyBoolean());
      verify(writer, never()).forceMerge(anyInt());

      mocks.assertStateTransitions(
          IndexManager.State.INITIAL_SYNC, IndexManager.State.STEADY_STATE);
    }
  }

  @Test
  public void testNoForceMergeWhenResumingSteadyStateFromRestart() throws Exception {
    System.setProperty(ReplicationIndexManager.FORCE_MERGE_MAX_NUM_SEGMENTS_PROPERTY, "1");
    try (Mocks mocks = Mocks.validUserData(FeatureFlags.withDefaults().build())) {
      IndexWriter writer = mocks.index.getWriter();

      verify(mocks.steadyStateManager, timeout(5000))
          .add(eq(MOCK_INDEX_GENERATION_ID), any(), any(), any(), any(), anyBoolean());
      verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.STEADY));
      // This resume did not come from initial sync, so no force-merge should have occurred.
      verify(writer, never()).forceMerge(anyInt());

      mocks.assertStateTransitions(
          IndexManager.State.INITIALIZING, IndexManager.State.STEADY_STATE);
    } finally {
      System.clearProperty(ReplicationIndexManager.FORCE_MERGE_MAX_NUM_SEGMENTS_PROPERTY);
    }
  }

  @Test
  public void testDroppedInitialSyncException() throws Exception {
    try (Mocks mocks = Mocks.exceptionalInitialSync(InitialSyncException.createDropped())) {
      verify(mocks.index, timeout(5000)).drop();

      verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.DOES_NOT_EXIST));

      mocks.assertStateTransitions(IndexManager.State.INITIAL_SYNC, IndexManager.State.SHUT_DOWN);

      mocks.assertDropMetric(Reason.INDEX_DROPPED);
    }
  }

  @Test
  public void testFailedInitialSyncException() throws Exception {
    try (Mocks mocks =
        Mocks.exceptionalInitialSync(InitialSyncException.createFailed(new RuntimeException()))) {
      verify(mocks.index, timeout(5000)).drop();

      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED,
                  IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED));
      assertThat(mocks.index.getStatus().getMessage().orElseThrow())
          .startsWith("replication failed (action=drop): ");
      mocks.assertStateTransitions(IndexManager.State.INITIAL_SYNC, IndexManager.State.FAILED);

      mocks.assertDropMetric(Reason.INITIAL_SYNC_REPLICATION_FAILED);
      mocks.assertFailedIndexMetric(
          FailedIndexAction.DROP, State.INITIAL_SYNC, InitialSyncException.class);
    }
  }

  @Test
  public void testTransientInitialSyncException() throws Exception {
    try (Mocks mocks =
        Mocks.exceptionalInitialSync(InitialSyncException.createRequiresResync(""))) {
      verify(mocks.initialSyncQueue, timeout(10000).times(2))
          .enqueue(any(), any(), any(), any(), anyBoolean(), anyBoolean());

      mocks.assertStateTransitions(
          IndexManager.State.INITIAL_SYNC, IndexManager.State.INITIAL_SYNC_BACKOFF);
    }
  }

  @Test
  public void testRuntimeInitialSyncException() throws Exception {
    try (Mocks mocks = Mocks.exceptionalInitialSync(new RuntimeException())) {
      verify(mocks.index, timeout(5000)).drop();

      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED,
                  IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED));

      mocks.assertStateTransitions(IndexManager.State.INITIAL_SYNC, IndexManager.State.FAILED);
      mocks.assertFailedIndexMetric(
          FailedIndexAction.DROP, State.INITIAL_SYNC, RuntimeException.class);
    }
  }

  @Test
  public void testFailedInitialSyncExceptionRetainInitialSyncDataOnDiskFlagEnabled_closeIndex()
      throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults().enable(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK).build();

    // Exception type is InitialSyncException.
    try (Mocks mocks =
        Mocks.exceptionalInitialSyncWithFeatureFlag(
            InitialSyncException.createFailed(new RuntimeException()), featureFlags)) {

      // Verify index is closed, but NOT dropped
      verify(mocks.index, timeout(5000)).close();
      verify(mocks.index, never()).drop();

      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED,
                  IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED));
      assertThat(mocks.index.getStatus().getMessage().orElseThrow())
          .startsWith("replication failed (action=close): ");
      mocks.assertStateTransitions(IndexManager.State.INITIAL_SYNC, IndexManager.State.FAILED);
      mocks.assertFailedIndexMetric(
          FailedIndexAction.CLOSE, State.INITIAL_SYNC, InitialSyncException.class);
    }
  }

  @Test
  public void testFailedInitialSyncExceptionRetainInitialSyncDataOnDiskFlagDisabled_dropIndex()
      throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults()
            .disable(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)
            .build();

    // Exception type is InitialSyncException.
    try (Mocks mocks =
        Mocks.exceptionalInitialSyncWithFeatureFlag(
            InitialSyncException.createFailed(new RuntimeException()), featureFlags)) {

      verify(mocks.index, timeout(5000)).close();
      verify(mocks.index, timeout(5000)).drop();

      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED,
                  IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED));
      assertThat(mocks.index.getStatus().getMessage().orElseThrow())
          .startsWith("replication failed (action=drop): ");
      mocks.assertStateTransitions(IndexManager.State.INITIAL_SYNC, IndexManager.State.FAILED);
      mocks.assertFailedIndexMetric(
          FailedIndexAction.DROP, State.INITIAL_SYNC, InitialSyncException.class);
    }
  }

  @Test
  public void testUnexpectedExceptionRetainInitialSyncDataOnDiskFlagEnabled_closeIndex()
      throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults().enable(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK).build();

    // Exception type is not InitialSyncException.
    try (Mocks mocks =
        Mocks.exceptionalInitialSyncWithFeatureFlag(
            new RuntimeException("unexpected"), featureFlags)) {
      // Verify index is closed, but NOT dropped
      verify(mocks.index, timeout(5000)).close();
      verify(mocks.index, never()).drop();

      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED,
                  IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED));

      mocks.assertStateTransitions(IndexManager.State.INITIAL_SYNC, IndexManager.State.FAILED);
      mocks.assertFailedIndexMetric(
          FailedIndexAction.CLOSE, State.INITIAL_SYNC, RuntimeException.class);
    }
  }

  @Test
  public void testUnexpectedExceptionRetainInitialSyncDataOnDiskFlagDisabled_dropIndex()
      throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults()
            .disable(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)
            .build();

    // Exception type is not InitialSyncException.
    try (Mocks mocks =
        Mocks.exceptionalInitialSyncWithFeatureFlag(
            new RuntimeException("unexpected"), featureFlags)) {
      verify(mocks.index, timeout(5000)).close();
      verify(mocks.index, timeout(5000)).drop();

      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED,
                  IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED));

      mocks.assertStateTransitions(IndexManager.State.INITIAL_SYNC, IndexManager.State.FAILED);
      mocks.assertFailedIndexMetric(
          FailedIndexAction.DROP, State.INITIAL_SYNC, RuntimeException.class);
    }
  }

  @Test
  public void testExceededInitialSyncExceptionFailsIndex() throws Exception {
    String reason = "exceeded limits";
    try (Mocks mocks =
        Mocks.exceptionalInitialSync(InitialSyncException.createFieldLimitExceeded(reason))) {
      IndexWriter writer = mocks.index.getWriter();
      var indexCommitUserData = IndexCommitUserData.createExceeded(reason);

      InOrder inOrder =
          Mockito.inOrder(
              mocks.initialSyncQueue,
              writer,
              mocks.index,
              mocks.index.getReader(),
              mocks.documentIndexer,
              mocks.periodicCommitter);
      // Timeout needed for all assertions as they happen sequentially and concurrently to this
      // test.
      inOrder
          .verify(mocks.initialSyncQueue, timeout(1000))
          .enqueue(any(), any(), any(), any(), anyBoolean(), anyBoolean());
      // index has exceeded its limits, we fail it, clear it, and persist the reason
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithMessage(IndexStatus.StatusCode.FAILED, EXCEEDED_LIMIT_REASON_PREFIX));
      inOrder.verify(mocks.documentIndexer, timeout(1000)).clearIndex(indexCommitUserData);
      mocks.assertStateTransitions(
          IndexManager.State.INITIAL_SYNC, IndexManager.State.FAILED_EXCEEDED);
    }
  }

  @Test
  public void testErrorInitialSyncException() throws Exception {
    try (Mocks mocks = Mocks.exceptionalInitialSync(new AssertionError())) {
      verify(mocks.index, timeout(5000)).drop();

      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED,
                  IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED));

      mocks.assertStateTransitions(IndexManager.State.INITIAL_SYNC, IndexManager.State.FAILED);
      mocks.assertFailedIndexMetric(
          FailedIndexAction.DROP, State.INITIAL_SYNC, AssertionError.class);
    }
  }

  @Test
  public void testExceededUserDataInitializesFailedIndex() throws Exception {
    try (Mocks mocks = Mocks.exceededUserData()) {
      // index transitions to Fail
      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithMessage(IndexStatus.StatusCode.FAILED, EXCEEDED_LIMIT_REASON_PREFIX));
      verifyNoInteractions(mocks.initialSyncQueue);
      verifyNoInteractions(mocks.steadyStateManager);
      verifyNoInteractions(mocks.synonymManager);
      mocks.assertStateTransitions(
          IndexManager.State.INITIALIZING, IndexManager.State.FAILED_EXCEEDED);
    }
  }

  @Test
  public void testValidCommitUserDataResumesSteadyState() throws Exception {
    try (Mocks mocks = Mocks.validUserData()) {
      verify(mocks.steadyStateManager, timeout(5000))
          .add(eq(MOCK_INDEX_GENERATION_ID), any(), any(), any(), any(), anyBoolean());

      verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.STEADY));

      mocks.assertStateTransitions(
          IndexManager.State.INITIALIZING, IndexManager.State.STEADY_STATE);
    }
  }

  @Test
  public void testStaleInternalSearchNamespaceResyncs() throws Exception {
    // Committed change-stream namespace database is the un-prefixed internal-search base, while the
    // resolved definition database is tenant-prefixed. This is the CLOUDP-416232 wedge: a pre-fix
    // resolution persisted the wrong internal database in the checkpoint. Expect the stale
    // checkpoint to be discarded and a fresh initial sync enqueued, not a steady-state resume.
    try (Mocks mocks =
        Mocks.internalSearchNamespace(
            Mocks.PREFIXED_INTERNAL_SEARCH_DB, Mocks.UNPREFIXED_INTERNAL_SEARCH_DB)) {
      verify(mocks.initialSyncQueue, timeout(5000))
          .enqueue(any(), any(), any(), any(), anyBoolean(), anyBoolean());
      verify(mocks.steadyStateManager, never())
          .add(any(), any(), any(), any(), any(), anyBoolean());
    }
  }

  @Test
  public void testStaleInternalSearchNamespaceResyncDisabledByFeatureFlag() throws Exception {
    // Same wedged state as testStaleInternalSearchNamespaceResyncs, but with the kill-switch
    // disabled: the guard is bypassed and the (stale) checkpoint is resumed as-is, so a normal
    // steady-state resume happens and no fresh initial sync is enqueued.
    FeatureFlags featureFlags =
        FeatureFlags.withDefaults().disable(Feature.RESET_STALE_INTERNAL_SEARCH_CHECKPOINT).build();
    try (Mocks mocks =
        Mocks.internalSearchNamespace(
            Mocks.PREFIXED_INTERNAL_SEARCH_DB,
            Mocks.UNPREFIXED_INTERNAL_SEARCH_DB,
            Optional.of(featureFlags))) {
      verify(mocks.steadyStateManager, timeout(5000))
          .add(any(), any(), any(), any(), any(), anyBoolean());
      verify(mocks.initialSyncQueue, never())
          .enqueue(any(), any(), any(), any(), anyBoolean(), anyBoolean());
    }
  }

  @Test
  public void testMatchingInternalSearchNamespaceResumes() throws Exception {
    // Committed namespace database matches the resolved (tenant-prefixed) definition database, so
    // the checkpoint is valid: resume steady state normally, no false-positive resync.
    try (Mocks mocks =
        Mocks.internalSearchNamespace(
            Mocks.PREFIXED_INTERNAL_SEARCH_DB, Mocks.PREFIXED_INTERNAL_SEARCH_DB)) {
      verify(mocks.steadyStateManager, timeout(5000))
          .add(any(), any(), any(), any(), any(), anyBoolean());
      verify(mocks.initialSyncQueue, never())
          .enqueue(any(), any(), any(), any(), anyBoolean(), anyBoolean());
    }
  }

  @Test
  public void testShutdownStopsSteadyStateManager() throws Exception {
    try (Mocks mocks = Mocks.validUserData()) {
      verify(mocks.steadyStateManager, timeout(5000))
          .add(eq(MOCK_INDEX_GENERATION_ID), any(), any(), any(), any(), anyBoolean());
      mocks.manager.shutdown();
      verify(mocks.steadyStateManager).stop(eq(MOCK_INDEX_GENERATION_ID));

      mocks.assertStateTransitions(IndexManager.State.STEADY_STATE, IndexManager.State.SHUT_DOWN);
    }
  }

  @Test
  public void testDroppedSteadyStateException() throws Exception {
    try (Mocks mocks = Mocks.exceptionalSteadyState(SteadyStateException.createDropped())) {
      verify(mocks.index, timeout(5000)).drop();

      verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.DOES_NOT_EXIST));

      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE, IndexManager.State.STEADY_STATE_SHUT_DOWN);
      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE_SHUT_DOWN, IndexManager.State.SHUT_DOWN);

      mocks.assertMetric(
          SteadyStateException.class, SteadyStateException.Type.DROPPED.toString(), "None", "None");

      mocks.assertDropMetric(Reason.INDEX_DROPPED);
    }
  }

  @Test
  public void testRequiresResyncSteadyStateException() throws Exception {
    var causeException = new RuntimeException("unexpected");
    try (Mocks mocks =
        Mocks.exceptionalSteadyState(SteadyStateException.createRequiresResync(causeException))) {
      // Wait for the index to be enqueued to resync.
      verify(mocks.initialSyncQueue, timeout(5000))
          .enqueue(
              any(),
              eq(MOCK_INDEX_DEFINITION_GENERATION),
              any(),
              any(),
              anyBoolean(),
              anyBoolean());

      // Ensure the index is cleared.
      verify(mocks.documentIndexer, timeout(1000)).clearIndex(any());

      // Ensure that the following events happen in order:
      //  - the index is put into the STEADY state
      //  - the SteadyStateIndexManager is created
      //  - the SteadyStateIndexManager is shut down
      //  - the index is enqueued onto the InitialSyncQueue
      InOrder inOrder =
          Mockito.inOrder(mocks.initialSyncQueue, mocks.steadyStateManager, mocks.index);

      inOrder.verify(mocks.index).setStatus(indexStatusWithCode(IndexStatus.StatusCode.STEADY));

      inOrder.verify(mocks.steadyStateManager).add(any(), any(), any(), any(), any(), anyBoolean());

      inOrder.verify(mocks.steadyStateManager).stop(MOCK_INDEX_GENERATION_ID);

      inOrder
          .verify(mocks.initialSyncQueue)
          .enqueue(
              any(),
              eq(MOCK_INDEX_DEFINITION_GENERATION),
              any(),
              any(),
              anyBoolean(),
              anyBoolean());

      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE_SHUT_DOWN, IndexManager.State.INITIAL_SYNC);
      mocks.assertMetric(
          SteadyStateException.class,
          SteadyStateException.Type.REQUIRES_RESYNC.toString(),
          ReplicationIndexManager.exceptionClassWithErrorCode(causeException),
          "None");
    }
  }

  @Test
  public void testTransientSteadyStateException() throws Exception {
    var causeException = new Exception("");
    try (Mocks mocks =
        Mocks.exceptionalSteadyState(SteadyStateException.createTransient(causeException))) {
      verify(mocks.steadyStateManager, timeout(1000).times(2))
          .add(any(), any(), any(), any(), any(), anyBoolean());

      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE, IndexManager.State.STEADY_STATE_SHUT_DOWN);
      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE_SHUT_DOWN, IndexManager.State.STEADY_STATE);
      mocks.assertMetric(
          SteadyStateException.class,
          SteadyStateException.Type.TRANSIENT.toString(),
          ReplicationIndexManager.exceptionClassWithErrorCode(causeException),
          "None");
    }
  }

  @Test
  public void testOplogFalloffSteadyStateException() throws Exception {
    var causeException = new MongoException(286, "ChangeStreamHistoryLost");
    try (Mocks mocks =
        Mocks.exceptionalSteadyState(
            SteadyStateException.createNonInvalidatingResync(causeException))) {
      verify(mocks.steadyStateManager, timeout(1000))
          .add(any(), any(), any(), any(), any(), anyBoolean());
      verify(mocks.documentIndexer, timeout(1000)).commit();
      verify(mocks.index, timeout(1000))
          .setStatus(indexStatusWithCode(IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT));

      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE, IndexManager.State.STEADY_STATE_SHUT_DOWN);

      mocks.assertMetric(
          SteadyStateException.class,
          SteadyStateException.Type.NON_INVALIDATING_RESYNC.name(),
          ReplicationIndexManager.exceptionClassWithErrorCode(causeException),
          "mongod");
    }
  }

  @Test
  public void testExceededSteadyStateExceptionFailsIndex() throws Exception {
    String reason = "exceeded reason";
    try (Mocks mocks =
        Mocks.exceptionalSteadyState(SteadyStateException.createFieldExceeded(reason))) {
      var writer = mocks.index.getWriter();
      var indexCommitUserData = IndexCommitUserData.createExceeded(reason);

      verify(mocks.steadyStateManager, timeout(1000).times(1))
          .add(any(), any(), any(), any(), any(), anyBoolean());

      InOrder inOrder =
          Mockito.inOrder(
              writer,
              mocks.index,
              mocks.index.getReader(),
              mocks.documentIndexer,
              mocks.periodicCommitter);
      // index has exceeded its limits, we fail it, clear it, and persist the reason
      inOrder
          .verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithMessage(IndexStatus.StatusCode.FAILED, EXCEEDED_LIMIT_REASON_PREFIX));
      inOrder.verify(mocks.documentIndexer, timeout(1000)).clearIndex(indexCommitUserData);

      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE, IndexManager.State.STEADY_STATE_SHUT_DOWN);
      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE_SHUT_DOWN, IndexManager.State.FAILED_EXCEEDED);
      mocks.assertMetric(
          SteadyStateException.class,
          SteadyStateException.Type.FIELD_EXCEEDED.name(),
          "None",
          "None");
    }
  }

  @Test
  public void testRenamedSteadyStateException() throws Exception {
    ChangeStreamResumeInfo renamedResumeInfo =
        ChangeStreamResumeInfo.create(
            new MongoNamespace("database", "collection"),
            ChangeStreamUtils.POST_BATCH_RESUME_TOKEN);
    try (Mocks mocks =
        Mocks.exceptionalSteadyState(SteadyStateException.createRenamed(renamedResumeInfo))) {
      verify(mocks.steadyStateManager, timeout(10000).times(2))
          .add(any(), any(), any(), any(), any(), anyBoolean());

      // Ensure that first the change stream is resumed with the commit user data resume info, then
      // after that resumed with the resume info from the SteadyStateException.
      InOrder inOrder = Mockito.inOrder(mocks.steadyStateManager);
      inOrder
          .verify(mocks.steadyStateManager)
          .add(any(), any(), any(), any(), eq(Mocks.RESUME_INFO), anyBoolean());
      inOrder
          .verify(mocks.steadyStateManager)
          .add(any(), any(), any(), any(), eq(renamedResumeInfo), anyBoolean());

      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE, IndexManager.State.STEADY_STATE_SHUT_DOWN);
      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE_SHUT_DOWN, IndexManager.State.STEADY_STATE);
      mocks.assertMetric(
          SteadyStateException.class, SteadyStateException.Type.RENAMED.name(), "None", "None");
    }
  }

  @Test
  public void testInvalidatedSteadyStateException() throws Exception {
    ChangeStreamResumeInfo resumeInfo =
        ChangeStreamResumeInfo.create(
            new MongoNamespace("database", "collection"),
            ChangeStreamUtils.POST_BATCH_RESUME_TOKEN);
    try (Mocks mocks =
        Mocks.exceptionalSteadyState(SteadyStateException.createInvalidated(resumeInfo))) {
      verify(mocks.steadyStateManager, timeout(10000).times(2))
          .add(any(), any(), any(), any(), any(), anyBoolean());

      // Ensure that first the change stream is resumed with the commit user data resume info, then
      // after that resumed with the resume info from the SteadyStateException.
      InOrder inOrder = Mockito.inOrder(mocks.steadyStateManager);
      inOrder
          .verify(mocks.steadyStateManager)
          .add(any(), any(), any(), any(), eq(Mocks.RESUME_INFO), anyBoolean());
      inOrder
          .verify(mocks.steadyStateManager)
          .add(any(), any(), any(), any(), eq(resumeInfo), anyBoolean());

      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE, IndexManager.State.STEADY_STATE_SHUT_DOWN);
      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE_SHUT_DOWN, IndexManager.State.STEADY_STATE);
      mocks.assertMetric(
          SteadyStateException.class, SteadyStateException.Type.INVALIDATED.name(), "None", "None");
    }
  }

  @Test
  public void testRuntimeSteadyStateException() throws Exception {
    try (Mocks mocks = Mocks.exceptionalSteadyState(new RuntimeException())) {
      verify(mocks.index, timeout(5000)).drop();

      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED,
                  IndexStatus.Reason.STEADY_STATE_REPLICATION_FAILED));

      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE, IndexManager.State.STEADY_STATE_SHUT_DOWN);
      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE_SHUT_DOWN, IndexManager.State.FAILED);
      mocks.assertMetric(RuntimeException.class, "None", "None", "None");
      mocks.assertFailedIndexMetric(
          FailedIndexAction.DROP, State.STEADY_STATE_SHUT_DOWN, RuntimeException.class);
    }
  }

  @Test
  public void testErrorSteadyStateException() throws Exception {
    try (Mocks mocks = Mocks.exceptionalSteadyState(new AssertionError())) {
      verify(mocks.index, timeout(5000)).drop();

      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED,
                  IndexStatus.Reason.STEADY_STATE_REPLICATION_FAILED));

      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE, IndexManager.State.STEADY_STATE_SHUT_DOWN);
      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE_SHUT_DOWN, IndexManager.State.FAILED);
      mocks.assertMetric(AssertionError.class, "None", "None", "None");
      mocks.assertFailedIndexMetric(
          FailedIndexAction.DROP, State.STEADY_STATE_SHUT_DOWN, AssertionError.class);
    }
  }

  @Test
  public void testUnexpectedExceptionStalesIndex() throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults()
            .enable(Feature.STALE_STATE_TRANSITION)
            .enable(Feature.RETAIN_FAILED_INDEX_DATA_ON_DISK)
            .enable(Feature.REMOVE_ABSENT_INDEXES_BEFORE_INITIALIZATION)
            .build();
    try (Mocks mocks =
        Mocks.exceptionalSteadyState(
            new IllegalArgumentException("invalid field name"), featureFlags)) {
      verify(mocks.steadyStateManager, timeout(1000).times(1))
          .add(any(), any(), any(), any(), any(), anyBoolean());

      // index becomes stale due to unknown errors
      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithMessage(
                  IndexStatus.StatusCode.STALE, UNEXPECTED_ERROR.formatMessage("")));

      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE, IndexManager.State.STEADY_STATE_SHUT_DOWN);
      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE_SHUT_DOWN, IndexManager.State.SHUT_DOWN);

      // ensure stale transition sets last updated optime
      assertThat(
              mocks
                  .index
                  .getMetricsUpdater()
                  .getIndexingMetricsUpdater()
                  .getReplicationOpTimeInfo()
                  .snapshot())
          .isPresent();
    }
  }

  @Test
  public void testUnexpectedExceptionFailedIndex_closeIndex() throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults()
            .enable(Feature.RETAIN_FAILED_INDEX_DATA_ON_DISK)
            .enable(Feature.REMOVE_ABSENT_INDEXES_BEFORE_INITIALIZATION)
            .build();
    try (Mocks mocks =
        Mocks.exceptionalSteadyState(
            new IllegalArgumentException("invalid field name"), featureFlags)) {
      verify(mocks.steadyStateManager, timeout(1000).times(1))
          .add(any(), any(), any(), any(), any(), anyBoolean());

      // index becomes failed due to unknown errors
      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithMessage(
                  IndexStatus.StatusCode.FAILED, "replication failed (action=close): "));

      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE, IndexManager.State.STEADY_STATE_SHUT_DOWN);
      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE_SHUT_DOWN, IndexManager.State.FAILED);
      verify(mocks.index, timeout(1000)).close();
      verify(mocks.index, never()).drop();
      mocks.assertFailedIndexMetric(
          FailedIndexAction.CLOSE, State.STEADY_STATE_SHUT_DOWN, IllegalArgumentException.class);
    }
  }

  @Test
  public void testUnexpectedExceptionFailedIndex_dropIndex() throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults()
            .enable(Feature.REMOVE_ABSENT_INDEXES_BEFORE_INITIALIZATION)
            .build();
    try (Mocks mocks =
        Mocks.exceptionalSteadyState(
            new IllegalArgumentException("invalid field name"), featureFlags)) {
      verify(mocks.steadyStateManager, timeout(1000).times(1))
          .add(any(), any(), any(), any(), any(), anyBoolean());

      // index becomes failed due to unknown errors
      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithMessage(
                  IndexStatus.StatusCode.FAILED, "replication failed (action=drop): "));

      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE, IndexManager.State.STEADY_STATE_SHUT_DOWN);
      mocks.assertStateTransitions(
          IndexManager.State.STEADY_STATE_SHUT_DOWN, IndexManager.State.FAILED);
      verify(mocks.index, timeout(1000)).close();
      verify(mocks.index, timeout(1000)).drop();

      mocks.assertDropMetric(Reason.STEADY_STATE_REPLICATION_FAILED);
      mocks.assertFailedIndexMetric(
          FailedIndexAction.DROP, State.STEADY_STATE_SHUT_DOWN, IllegalArgumentException.class);
    }
  }

  @Test
  public void testInitialSyncExceptionCounter() throws Exception {
    // The exceptional initial sync queue running on a separate thread could cause the counter to
    // increment before we can assert that it's 0.
    try (Mocks mocks = Mocks.hangingExceptionalInitialSync(InitialSyncException.createDropped())) {
      // The latch currently has the initial sync queue paused.
      assertEquals(
          0,
          mocks
              .index
              .getMetricsUpdater()
              .getIndexingMetricsUpdater()
              .getInitialSyncExceptionCounter()
              .count(),
          0);

      mocks.latch.orElseThrow(AssertionError::new).countDown();
      verify(mocks.index, timeout(5000)).drop();

      assertEquals(
          1,
          mocks
              .index
              .getMetricsUpdater()
              .getIndexingMetricsUpdater()
              .getInitialSyncExceptionCounter()
              .count(),
          0);
    }
  }

  @Test
  public void testSteadyStateExceptionCounter() throws Exception {
    // In the method below, the exceptional steady state manager running on a separate thread could
    // cause the counter to increment before we can assert that it's 0.
    try (Mocks mocks = Mocks.hangingExceptionalSteadyState(SteadyStateException.createDropped())) {

      // The latch currently has the steady state manager paused.
      assertEquals(
          0,
          mocks
              .index
              .getMetricsUpdater()
              .getIndexingMetricsUpdater()
              .getSteadyStateExceptionCounter()
              .count(),
          0);

      mocks.latch.orElseThrow(AssertionError::new).countDown();

      verify(mocks.index, timeout(5000)).drop();

      assertEquals(
          1,
          mocks
              .index
              .getMetricsUpdater()
              .getIndexingMetricsUpdater()
              .getSteadyStateExceptionCounter()
              .count(),
          0);
    }
  }

  @Test
  public void testShutdownShutsDownSynonymMappingManager() throws Exception {
    try (Mocks mocks = Mocks.validUserData()) {
      mocks.manager.shutdown();

      verify(mocks.synonymMappingManager).shutdown();
      verifyNoMoreInteractions(mocks.synonymMappingManager);
    }
  }

  @Test
  public void testShutdownClosesPeriodicCommitter() throws Exception {
    try (Mocks mocks = Mocks.validUserData()) {
      mocks.manager.shutdown();

      verify(mocks.periodicCommitter).close();
      verifyNoMoreInteractions(mocks.periodicCommitter);
    }
  }

  @Test
  public void testFailedIndexShutsDownSynonymMappingManager() throws Exception {
    try (Mocks mocks = Mocks.exceptionalInitialSync(new AssertionError())) {
      verify(mocks.index, timeout(5000)).drop();

      verify(mocks.synonymMappingManager, timeout(1000)).shutdown();
      verifyNoMoreInteractions(mocks.synonymMappingManager);
    }
  }

  @Test
  public void testDropShutsDownSynonymMappingManager() throws Exception {
    try (Mocks mocks = Mocks.exceptionalInitialSync(InitialSyncException.createDropped())) {
      verify(mocks.index, timeout(5000)).drop();

      verify(mocks.synonymMappingManager, timeout(1000)).shutdown();
      verifyNoMoreInteractions(mocks.synonymMappingManager);
    }
  }

  @Test
  public void testUnsupportedIndexFormatVersion() throws Exception {
    try (Mocks mocks = Mocks.unsupportedIndexFormatVersionSteadyState()) {
      verify(mocks.index, timeout(1000))
          .setStatus(
              indexStatusWithReason(
                  IndexStatus.StatusCode.FAILED, IndexStatus.Reason.INITIALIZATION_FAILED));

      mocks.assertStateTransitions(IndexManager.State.INITIALIZING, IndexManager.State.FAILED);

      mocks.assertDropMetric(Reason.INITIALIZATION_FAILED);
    }
  }

  @Test
  public void testFailAndCloseIndex() throws Exception {
    try (Mocks mocks = Mocks.validUserData()) {
      // Wait for the manager to reach STEADY_STATE before forcing state via reflection,
      // otherwise the background init thread may overwrite our state change.
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> mocks.manager.getState() == State.STEADY_STATE);

      Method method =
          ReplicationIndexManager.class.getDeclaredMethod(
              "failAndCloseIndex", Throwable.class, IndexStatus.Reason.class);
      method.setAccessible(true);
      Throwable throwable = new Exception("Failed init exception");
      method.invoke(mocks.manager, throwable, IndexStatus.Reason.INITIALIZATION_FAILED);

      Assert.assertSame(IndexStatus.StatusCode.FAILED, mocks.index.getStatus().getStatusCode());
      Assert.assertSame(
          IndexStatus.Reason.INITIALIZATION_FAILED,
          mocks.index.getStatus().getReason().orElseThrow());
      mocks.assertFailedIndexMetric(FailedIndexAction.CLOSE, State.STEADY_STATE, Exception.class);
    }
  }

  @Test
  public void testFailAndRetainIndex() throws Exception {
    try (Mocks mocks = Mocks.validUserData()) {
      // Wait for the manager to reach STEADY_STATE before invoking failAndRetainIndex,
      // otherwise the background init thread may overwrite our state change.
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> mocks.manager.getState() == State.STEADY_STATE);

      Throwable throwable = new Exception("initial sync failed");
      mocks.manager.failAndRetainIndex(throwable);

      // Index data should be kept on disk.
      verify(mocks.index, timeout(1000)).close();
      verify(mocks.index, never()).drop();

      // Status should be FAILED with retryable reason.
      Assert.assertSame(IndexStatus.StatusCode.FAILED, mocks.index.getStatus().getStatusCode());
      Assert.assertSame(
          IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY,
          mocks.index.getStatus().getReason().orElseThrow());
      assertThat(mocks.index.getStatus().getMessage().orElseThrow())
          .startsWith("replication failed (action=retain): ");

      // Metric should record a RETAIN action.
      mocks.assertFailedIndexMetric(FailedIndexAction.RETAIN, State.STEADY_STATE, Exception.class);
    }
  }

  @Test
  public void testFailRetainOrResync_flagEnabled_retainsIndex() throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults().enable(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK).build();
    try (Mocks mocks = Mocks.validUserData(featureFlags)) {
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> mocks.manager.getState() == State.STEADY_STATE);

      Throwable throwable = new Exception("initial sync failed");
      mocks.manager.failRetainOrResync(throwable, IndexStatus.initialSync());

      // Index should be closed (data retained) but NOT dropped.
      verify(mocks.index, timeout(1000)).close();
      verify(mocks.index, never()).drop();

      Assert.assertSame(IndexStatus.StatusCode.FAILED, mocks.index.getStatus().getStatusCode());
      Assert.assertSame(
          Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY,
          mocks.index.getStatus().getReason().orElseThrow());
      assertThat(mocks.index.getStatus().getMessage().orElseThrow())
          .startsWith("replication failed (action=retain): ");
      mocks.assertFailedIndexMetric(FailedIndexAction.RETAIN, State.STEADY_STATE, Exception.class);
    }
  }

  @Test
  public void testFailRetainOrResync_flagDisabled_schedulesResync() throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults()
            .disable(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)
            .build();
    try (Mocks mocks = Mocks.validUserData(featureFlags)) {
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> mocks.manager.getState() == State.STEADY_STATE);

      Throwable throwable = new Exception("initial sync failed");
      mocks.manager.failRetainOrResync(throwable, IndexStatus.initialSync());

      // Resync should be scheduled: index is re-enqueued for initial sync.
      verify(mocks.initialSyncQueue, timeout(5000))
          .enqueue(any(), any(), any(), any(), anyBoolean(), anyBoolean());

      // Index should NOT be closed or dropped.
      verify(mocks.index, never()).close();
      verify(mocks.index, never()).drop();

      mocks.assertStateTransitions(State.STEADY_STATE, State.INITIAL_SYNC_BACKOFF);
    }
  }

  @Test
  public void testFailAndClearIndex() throws Exception {
    try (Mocks mocks = Mocks.validUserData()) {
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> mocks.manager.getState() == State.STEADY_STATE);

      Throwable throwable = new Exception("bson too large");
      mocks.manager.failAndClearIndex(throwable);

      // Index should be cleared but NOT dropped or closed.
      verify(mocks.documentIndexer, timeout(1000)).clearIndex(IndexCommitUserData.EMPTY);
      verify(mocks.index, never()).drop();

      // Status should be FAILED with retryable reason.
      Assert.assertSame(IndexStatus.StatusCode.FAILED, mocks.index.getStatus().getStatusCode());
      Assert.assertSame(
          IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY,
          mocks.index.getStatus().getReason().orElseThrow());
      assertThat(mocks.index.getStatus().getMessage().orElseThrow())
          .startsWith("replication failed (action=clear): ");

      // Metric should record a CLEAR action.
      mocks.assertFailedIndexMetric(FailedIndexAction.CLEAR, State.STEADY_STATE, Exception.class);
    }
  }

  @Test
  public void testFailClearOrResync_flagEnabled_clearsIndex() throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults().enable(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK).build();
    try (Mocks mocks = Mocks.validUserData(featureFlags)) {
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> mocks.manager.getState() == State.STEADY_STATE);

      Throwable throwable = new Exception("bson too large");
      mocks.manager.failClearOrResync(throwable, IndexStatus.initialSync());

      // Index should be cleared but NOT dropped.
      verify(mocks.documentIndexer, timeout(1000)).clearIndex(IndexCommitUserData.EMPTY);
      verify(mocks.index, never()).drop();

      Assert.assertSame(IndexStatus.StatusCode.FAILED, mocks.index.getStatus().getStatusCode());
      Assert.assertSame(
          Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY,
          mocks.index.getStatus().getReason().orElseThrow());
      assertThat(mocks.index.getStatus().getMessage().orElseThrow())
          .startsWith("replication failed (action=clear): ");
      mocks.assertFailedIndexMetric(FailedIndexAction.CLEAR, State.STEADY_STATE, Exception.class);
    }
  }

  @Test
  public void testFailClearOrResync_flagDisabled_schedulesResync() throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults()
            .disable(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)
            .build();
    try (Mocks mocks = Mocks.validUserData(featureFlags)) {
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> mocks.manager.getState() == State.STEADY_STATE);

      Throwable throwable = new Exception("bson too large");
      mocks.manager.failClearOrResync(throwable, IndexStatus.initialSync());

      // Resync should be scheduled: index is re-enqueued for initial sync.
      verify(mocks.initialSyncQueue, timeout(5000))
          .enqueue(any(), any(), any(), any(), anyBoolean(), anyBoolean());

      // Index should NOT be closed or dropped.
      verify(mocks.index, never()).close();
      verify(mocks.index, never()).drop();

      mocks.assertStateTransitions(State.STEADY_STATE, State.INITIAL_SYNC_BACKOFF);
    }
  }

  @Test
  public void testFailCloseOrResync_flagEnabled_closesIndex() throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults().enable(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK).build();
    try (Mocks mocks = Mocks.validUserData(featureFlags)) {
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> mocks.manager.getState() == State.STEADY_STATE);

      Throwable throwable = new Exception("no query execution plans");
      mocks.manager.failCloseOrResync(throwable, IndexStatus.initialSync());

      // Index should be closed but NOT dropped.
      verify(mocks.index, timeout(1000)).close();
      verify(mocks.index, never()).drop();

      Assert.assertSame(IndexStatus.StatusCode.FAILED, mocks.index.getStatus().getStatusCode());
      Assert.assertSame(
          Reason.INITIAL_SYNC_REPLICATION_FAILED,
          mocks.index.getStatus().getReason().orElseThrow());
      assertThat(mocks.index.getStatus().getMessage().orElseThrow())
          .startsWith("replication failed (action=close): ");
      mocks.assertFailedIndexMetric(FailedIndexAction.CLOSE, State.STEADY_STATE, Exception.class);
    }
  }

  @Test
  public void testFailCloseOrResync_flagDisabled_schedulesResync() throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults()
            .disable(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)
            .build();
    try (Mocks mocks = Mocks.validUserData(featureFlags)) {
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> mocks.manager.getState() == State.STEADY_STATE);

      Throwable throwable = new Exception("no query execution plans");
      mocks.manager.failCloseOrResync(throwable, IndexStatus.initialSync());

      // Resync should be scheduled: index is re-enqueued for initial sync.
      verify(mocks.initialSyncQueue, timeout(5000))
          .enqueue(any(), any(), any(), any(), anyBoolean(), anyBoolean());

      // Index should NOT be closed or dropped.
      verify(mocks.index, never()).close();
      verify(mocks.index, never()).drop();

      mocks.assertStateTransitions(State.STEADY_STATE, State.INITIAL_SYNC_BACKOFF);
    }
  }

  @Test
  public void testValidateReasonForStatus()
      throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
    IndexStatus indexStatusInstance = new IndexStatus(IndexStatus.StatusCode.STEADY);
    Method validateMethod =
        IndexStatus.class.getDeclaredMethod(
            "validateReasonForStatus", IndexStatus.StatusCode.class, Optional.class);
    validateMethod.setAccessible(true);

    // verify the validation doesn't throw any exceptions
    validateMethod.invoke(
        indexStatusInstance,
        IndexStatus.StatusCode.FAILED,
        Optional.of(IndexStatus.Reason.INITIALIZATION_FAILED));

    validateMethod.invoke(indexStatusInstance, IndexStatus.StatusCode.FAILED, Optional.empty());

    InvocationTargetException invocationException =
        assertThrows(
            InvocationTargetException.class,
            () ->
                validateMethod.invoke(
                    indexStatusInstance,
                    IndexStatus.StatusCode.STALE,
                    Optional.of(IndexStatus.Reason.INITIALIZATION_FAILED)));
    // Verify that the cause of the InvocationTargetException is IllegalArgumentException
    Throwable cause = invocationException.getCause(); // Unwrap the underlying exception
    assertThat(cause).isInstanceOf(IllegalArgumentException.class);
    assertThat(cause.getMessage())
        .contains("Reason INITIALIZATION_FAILED is not valid for status STALE");
  }

  @Test
  public void testMaybeCrashOnUnexpectedThrowable() {
    ReplicationIndexManager.maybeCrashOnUnexpectedThrowable(new Exception());
    ReplicationIndexManager.maybeCrashOnUnexpectedThrowable(new Exception(new Exception()));

    Assert.assertThrows(
        SecurityException.class,
        () -> ReplicationIndexManager.maybeCrashOnUnexpectedThrowable(new OutOfMemoryError()));

    Assert.assertThrows(
        SecurityException.class,
        () ->
            ReplicationIndexManager.maybeCrashOnUnexpectedThrowable(
                new Exception(new OutOfMemoryError())));
  }

  @Test
  public void testIncrementConsecutiveInitialSyncResyncExceptionsGauge() throws Exception {
    // Gauge should increment when an InitialSyncException that requires resync occurs.
    try (Mocks mocks =
        Mocks.hangingExceptionalInitialSync(InitialSyncException.createRequiresResync(""))) {
      assertEquals(
          0,
          mocks
              .index
              .getMetricsUpdater()
              .getIndexingMetricsUpdater()
              .getConsecutiveInitialSyncResyncExceptions()
              .get());
      mocks.latch.orElseThrow(AssertionError::new).countDown();
      verify(mocks.initialSyncQueue, timeout(10000).times(2))
          .enqueue(any(), any(), any(), any(), anyBoolean(), anyBoolean());
      mocks.assertStateTransitions(
          IndexManager.State.INITIAL_SYNC, IndexManager.State.INITIAL_SYNC_BACKOFF);
      assertEquals(
          1,
          mocks
              .index
              .getMetricsUpdater()
              .getIndexingMetricsUpdater()
              .getConsecutiveInitialSyncResyncExceptions()
              .get());
    }
  }

  @Test
  public void testResetConsecutiveInitialSyncResyncExceptionsGauge() throws Exception {
    // Gauge should be 0 when initial sync succeeds.
    try (Mocks mocks = Mocks.successfulInitialSync()) {
      await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> mocks.manager.getState() == State.STEADY_STATE);
      assertEquals(
          0,
          mocks
              .index
              .getMetricsUpdater()
              .getIndexingMetricsUpdater()
              .getConsecutiveInitialSyncResyncExceptions()
              .get());
    }
  }

  @Test
  public void testIdFieldTypeGaugeResetsWhenLeavingInitialSync() throws Exception {
    try (Mocks mocks = Mocks.noCommitUserData()) {
      // The hanging initial sync queue keeps the index parked in INITIAL_SYNC.
      await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> mocks.manager.getState() == State.INITIAL_SYNC);

      // Simulate the observing indexer reporting the _id type of the first indexed document.
      mocks
          .index
          .getMetricsUpdater()
          .getReplicationMetricsUpdater()
          .getInitialSyncMetrics()
          .reportIdKeyFieldType("OBJECT_ID");
      assertEquals(1.0, initialSyncIdFieldTypeGauge(mocks, "OBJECT_ID"), 0.0);

      // Leaving INITIAL_SYNC (here via shutdown) must flip the gauge back to 0.
      assertSuccess(mocks.manager.shutdown());
      await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> initialSyncIdFieldTypeGauge(mocks, "OBJECT_ID") == 0.0);
    }
  }

  @Test
  public void testIdFieldTypeGaugeResetsWhenLeavingSteadyState() throws Exception {
    try (Mocks mocks = Mocks.successfulInitialSync()) {
      // The hanging steady state manager keeps the index parked in STEADY_STATE.
      await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> mocks.manager.getState() == State.STEADY_STATE);

      // Simulate the observing indexer reporting the _id type of the first indexed document.
      mocks
          .index
          .getMetricsUpdater()
          .getReplicationMetricsUpdater()
          .getSteadyStateMetrics()
          .reportIdKeyFieldType("OBJECT_ID");
      assertEquals(1.0, steadyStateIdFieldTypeGauge(mocks, "OBJECT_ID"), 0.0);

      // Leaving STEADY_STATE (here via shutdown) must flip the gauge back to 0.
      assertSuccess(mocks.manager.shutdown());
      await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> steadyStateIdFieldTypeGauge(mocks, "OBJECT_ID") == 0.0);
    }
  }

  private static double initialSyncIdFieldTypeGauge(Mocks mocks, String bsonType) {
    return idFieldTypeGauge(
        mocks,
        IndexMetricsUpdater.ReplicationMetricsUpdater.InitialSyncMetrics.NAMESPACE,
        bsonType);
  }

  private static double steadyStateIdFieldTypeGauge(Mocks mocks, String bsonType) {
    return idFieldTypeGauge(
        mocks,
        IndexMetricsUpdater.ReplicationMetricsUpdater.SteadyStateMetrics.NAMESPACE,
        bsonType);
  }

  private static double idFieldTypeGauge(Mocks mocks, String phaseNamespace, String bsonType) {
    return mocks
        .metricsFactory
        .childMetricsFactory(IndexMetricsUpdater.ReplicationMetricsUpdater.NAMESPACE)
        .childMetricsFactory(phaseNamespace)
        .get("idFieldType", Tags.of("bsonType", bsonType))
        .gauge()
        .value();
  }

  private void assertSuccess(CompletableFuture<Void> future) throws Exception {
    future.get();
  }

  private IndexStatus indexStatusWithCode(IndexStatus.StatusCode expected) {
    return argThat(new IndexStatusCodeMatcher(expected));
  }

  private IndexStatus indexStatusWithMessage(IndexStatus.StatusCode status, String messagePrefix) {
    return argThat(
        actual ->
            status.equals(actual.getStatusCode())
                && actual.getMessage().orElseThrow().startsWith(messagePrefix));
  }

  private IndexStatus indexStatusWithReason(
      IndexStatus.StatusCode status, IndexStatus.Reason reason) {
    return argThat(
        actual ->
            status.equals(actual.getStatusCode())
                && reason.equals(actual.getReason().orElseThrow()));
  }

  private static class IndexStatusCodeMatcher implements ArgumentMatcher<IndexStatus> {

    private final IndexStatus.StatusCode expected;

    private IndexStatusCodeMatcher(IndexStatus.StatusCode expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(IndexStatus right) {
      return right != null && this.expected.equals(right.getStatusCode());
    }

    @Override
    public String toString() {
      return String.format("StatusCode.%s", this.expected.name());
    }
  }

  private static class Mocks implements Closeable {

    static final ChangeStreamResumeInfo RESUME_INFO =
        ChangeStreamResumeInfo.create(
            new MongoNamespace("database", "collection"),
            ChangeStreamUtils.POST_BATCH_RESUME_TOKEN);

    static final InitialSyncResumeInfo BUFFERLESS_INITIAL_SYNC_RESUME_INFO =
        new BufferlessIdOrderInitialSyncResumeInfo(new BsonTimestamp(0, 0), new BsonString("key"));

    final ExecutorService executor;
    @Keep final MongotCursorManager cursorManager;
    final InitialSyncQueue initialSyncQueue;
    final SteadyStateManager steadyStateManager;
    final SynonymManager synonymManager;
    final InitializedIndex index;
    final DocumentIndexer documentIndexer;
    final PeriodicIndexCommitter periodicCommitter;
    final ReplicationIndexManager manager;
    final SynonymMappingManager synonymMappingManager;
    @Keep final PerIndexMetricsFactory metricsFactory;
    final MeterRegistry meterRegistry;

    final Optional<CountDownLatch> latch;
    @Keep final com.xgen.mongot.index.IndexGeneration indexGeneration;

    private Mocks(
        ExecutorService executor,
        MongotCursorManager cursorManager,
        InitialSyncQueue initialSyncQueue,
        SteadyStateManager steadyStateManager,
        SynonymManager synonymManager,
        InitializedIndex index,
        DocumentIndexer documentIndexer,
        PeriodicIndexCommitter periodicCommitter,
        ReplicationIndexManager manager,
        SynonymMappingManager synonymMappingManager,
        PerIndexMetricsFactory metricsFactory,
        MeterRegistry meterRegistry,
        com.xgen.mongot.index.IndexGeneration indexGeneration,
        Optional<CountDownLatch> latch) {
      this.executor = executor;
      this.cursorManager = cursorManager;
      this.initialSyncQueue = initialSyncQueue;
      this.steadyStateManager = steadyStateManager;
      this.synonymManager = synonymManager;
      this.index = index;
      this.documentIndexer = documentIndexer;
      this.periodicCommitter = periodicCommitter;
      this.manager = manager;
      this.synonymMappingManager = synonymMappingManager;
      this.metricsFactory = metricsFactory;
      this.indexGeneration = indexGeneration;
      this.meterRegistry = meterRegistry;

      this.latch = latch;
    }

    @Override
    public void close() {
      this.executor.shutdown();
      try {
        this.executor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    static Mocks noCommitUserData() throws Exception {
      return create(
          Optional.empty(),
          HANGING_INITIAL_SYNC_QUEUE,
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.empty());
    }

    @Keep
    static Mocks initializeIndexAlwaysFails() throws Exception {
      var index = mockIndex(SearchIndex.MOCK_INDEX_DEFINITION);
      // TODO(CLOUDP-280897): Throw exception when initializing index.
      return create(
          Optional.empty(),
          HANGING_INITIAL_SYNC_QUEUE,
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          mockIndexGeneration(index),
          Optional.empty(),
          Optional.empty());
    }

    @Keep
    static Mocks initializeIndexSucceeds() throws Exception {
      var index = mockIndex(SearchIndex.MOCK_INDEX_DEFINITION);
      return create(
          Optional.empty(),
          HANGING_INITIAL_SYNC_QUEUE,
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          mockIndexGeneration(index),
          Optional.empty(),
          Optional.empty());
    }

    static Mocks mismatchingBackendIndexVersion() throws Exception {
      return create(
          Optional.of(
              IndexCommitUserData.createChangeStreamResume(
                  RESUME_INFO,
                  IndexFormatVersion.create(IndexFormatVersion.CURRENT.versionNumber - 1))),
          HANGING_INITIAL_SYNC_QUEUE,
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.empty());
    }

    static Mocks unsupportedIndexFormatVersionSteadyState() throws Exception {
      var unsupportedDefinitionGeneration =
          mockDefinitionGeneration(
              new GenerationId(
                  new ObjectId(),
                  new Generation(UserIndexVersion.FIRST, IndexFormatVersion.create(1))));
      return create(
          Optional.of(
              IndexCommitUserData.createChangeStreamResume(
                  RESUME_INFO, IndexFormatVersion.create(1))),
          HANGING_INITIAL_SYNC_QUEUE,
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          mockIndexGeneration(unsupportedDefinitionGeneration),
          Optional.empty(),
          Optional.empty());
    }

    static Mocks validUserData() throws Exception {
      return create(
          Optional.of(
              IndexCommitUserData.createChangeStreamResume(
                  RESUME_INFO, IndexFormatVersion.CURRENT)),
          HANGING_INITIAL_SYNC_QUEUE,
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.empty());
    }

    static Mocks validUserData(FeatureFlags featureFlags) throws Exception {
      return create(
          Optional.of(
              IndexCommitUserData.createChangeStreamResume(
                  RESUME_INFO, IndexFormatVersion.CURRENT)),
          HANGING_INITIAL_SYNC_QUEUE,
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.empty(),
          Optional.of(featureFlags));
    }

    static final String PREFIXED_INTERNAL_SEARCH_DB =
        "6a1d8bee3c6725e1b057660f___mdb_internal_search";
    static final String UNPREFIXED_INTERNAL_SEARCH_DB = "__mdb_internal_search";

    // Builds a manager whose derived-index definition lives in an internal-search database
    // (defDatabase) but whose committed change-stream resume namespace was persisted under a
    // different internal-search database (committedNamespaceDatabase). Used to exercise the
    // stale-checkpoint namespace guard in determineInitAction.
    static Mocks internalSearchNamespace(String defDatabase, String committedNamespaceDatabase)
        throws Exception {
      return internalSearchNamespace(defDatabase, committedNamespaceDatabase, Optional.empty());
    }

    static Mocks internalSearchNamespace(
        String defDatabase, String committedNamespaceDatabase, Optional<FeatureFlags> featureFlags)
        throws Exception {
      SearchIndexDefinition definition =
          SearchIndexDefinitionBuilder.builder()
              .indexId(SearchIndex.MOCK_INDEX_ID)
              .name(SearchIndex.MOCK_INDEX_NAME)
              .database(defDatabase)
              .lastObservedCollectionName(SearchIndex.MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
              .collectionUuid(SearchIndex.MOCK_INDEX_COLLECTION_UUID)
              .mappings(SearchIndex.MOCK_INDEX_MAPPINGS)
              .build();
      ChangeStreamResumeInfo committedResume =
          ChangeStreamResumeInfo.create(
              new MongoNamespace(committedNamespaceDatabase, "collection"),
              ChangeStreamUtils.POST_BATCH_RESUME_TOKEN);
      return create(
          Optional.of(
              IndexCommitUserData.createChangeStreamResume(
                  committedResume, IndexFormatVersion.CURRENT)),
          HANGING_INITIAL_SYNC_QUEUE,
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          mockIndexGeneration(mockIndex(definition)),
          Optional.empty(),
          featureFlags);
    }

    static Mocks initialSyncResumeCurrentIndexVersion(InitialSyncResumeInfo initialSyncResumeInfo)
        throws Exception {
      return create(
          Optional.of(
              IndexCommitUserData.createInitialSyncResume(
                  IndexFormatVersion.CURRENT, initialSyncResumeInfo)),
          HANGING_INITIAL_SYNC_QUEUE,
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.empty());
    }

    static Mocks initialSyncResumeMismatchingIndexVersion() throws Exception {
      return create(
          Optional.of(
              IndexCommitUserData.createInitialSyncResume(
                  IndexFormatVersion.create(IndexFormatVersion.CURRENT.versionNumber - 1),
                  BUFFERLESS_INITIAL_SYNC_RESUME_INFO)),
          HANGING_INITIAL_SYNC_QUEUE,
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.empty());
    }

    static Mocks exceededUserData() throws Exception {
      return create(
          Optional.of(IndexCommitUserData.createExceeded("exceeded reason")),
          HANGING_INITIAL_SYNC_QUEUE,
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.empty());
    }

    static Mocks successfulInitialSync() throws Exception {
      return create(
          Optional.empty(),
          SUCCESSFUL_INITIAL_SYNC_QUEUE,
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.empty());
    }

    static Mocks exceptionalInitialSync(Throwable t) throws Exception {
      return create(
          Optional.empty(),
          exceptionalInitialSyncQueue(t),
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.empty());
    }

    static Mocks exceptionalInitialSync(Throwable t, Duration resyncBackoff) throws Exception {
      return create(
          Optional.empty(),
          exceptionalInitialSyncQueue(t),
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.of(resyncBackoff),
          Optional.empty());
    }

    static Mocks exceptionalInitialSyncWithFeatureFlag(Throwable t, FeatureFlags featureFlags)
        throws Exception {
      return create(
          Optional.empty(),
          exceptionalInitialSyncQueue(t),
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
          Optional.empty(),
          Optional.of(featureFlags));
    }

    static Mocks hangingExceptionalInitialSync(Throwable t) throws Exception {
      CountDownLatch initialSyncQueueLatch = new CountDownLatch(1);
      return create(
          Optional.empty(),
          hangingExceptionalnitialSyncQueue(t, initialSyncQueueLatch),
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.of(initialSyncQueueLatch));
    }

    static Mocks exceptionalInitialSyncWithResumeInfo(Throwable t) throws Exception {
      return create(
          Optional.of(
              IndexCommitUserData.createInitialSyncResume(
                  IndexFormatVersion.CURRENT, BUFFERLESS_INITIAL_SYNC_RESUME_INFO)),
          exceptionalInitialSyncQueue(t),
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.empty());
    }

    static Mocks exceptionalInitialSyncOnView(Throwable t) throws Exception {
      var definition =
          SearchIndex.mockDefinitionBuilder()
              .view(ViewDefinition.existing("test", List.of()))
              .build();
      return create(
          Optional.empty(),
          exceptionalInitialSyncQueue(t),
          HANGING_STEADY_STATE_MANAGER,
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          mockIndexGeneration(mockDefinitionGeneration(definition)),
          Optional.empty(),
          Optional.empty());
    }

    static Mocks exceptionalSteadyState(Throwable t) throws Exception {
      return create(
          Optional.of(
              IndexCommitUserData.createChangeStreamResume(
                  RESUME_INFO, IndexFormatVersion.CURRENT)),
          HANGING_INITIAL_SYNC_QUEUE,
          exceptionalSteadyStateManager(t),
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.empty(),
          Optional.empty());
    }

    static Mocks exceptionalSteadyState(Throwable t, FeatureFlags featureFlags) throws Exception {
      return create(
          Optional.of(
              IndexCommitUserData.createChangeStreamResume(
                  RESUME_INFO, IndexFormatVersion.CURRENT)),
          HANGING_INITIAL_SYNC_QUEUE,
          exceptionalSteadyStateManager(t),
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.empty(),
          Optional.of(featureFlags));
    }

    static Mocks hangingExceptionalSteadyState(Throwable t) throws Exception {
      CountDownLatch steadyStateManagerLatch = new CountDownLatch(1);
      return create(
          Optional.of(
              IndexCommitUserData.createChangeStreamResume(
                  RESUME_INFO, IndexFormatVersion.CURRENT)),
          HANGING_INITIAL_SYNC_QUEUE,
          hangingExceptionalSteadyStateManager(t, steadyStateManagerLatch),
          HANGING_SYNONYM_MANAGER,
          Optional.empty(),
          Optional.of(steadyStateManagerLatch));
    }

    private static final Answer<CompletableFuture<ChangeStreamResumeInfo>>
        HANGING_INITIAL_SYNC_QUEUE =
            invocation -> {
              Runnable callback = invocation.getArgument(2);
              callback.run();
              return new CompletableFuture<>();
            };

    private static final Answer<CompletableFuture<ChangeStreamResumeInfo>>
        SUCCESSFUL_INITIAL_SYNC_QUEUE =
            invocation -> {
              Runnable callback = invocation.getArgument(2);
              callback.run();
              return CompletableFuture.completedFuture(RESUME_INFO);
            };

    private static Answer<CompletableFuture<ChangeStreamResumeInfo>> exceptionalInitialSyncQueue(
        Throwable t) {
      AtomicBoolean firstTime = new AtomicBoolean(true);
      return invocation -> {
        Runnable callback = invocation.getArgument(2);
        // Sets the index's status to INITIAL_SYNC
        callback.run();

        return firstTime.getAndSet(false)
            ? CompletableFuture.failedFuture(t)
            : new CompletableFuture<>();
      };
    }

    private static Answer<CompletableFuture<ChangeStreamResumeInfo>>
        hangingExceptionalnitialSyncQueue(Throwable t, CountDownLatch latch) {
      AtomicBoolean firstTime = new AtomicBoolean(true);
      return invocation -> {
        Runnable callback = invocation.getArgument(2);
        callback.run();

        return firstTime.getAndSet(false) ? firstTimeFuture(t, latch) : new CompletableFuture<>();
      };
    }

    private static final Answer<CompletableFuture<Void>> HANGING_STEADY_STATE_MANAGER =
        ignored -> new CompletableFuture<>();

    private static Answer<CompletableFuture<Void>> exceptionalSteadyStateManager(Throwable t) {
      AtomicBoolean firstTime = new AtomicBoolean(true);
      return invocation ->
          firstTime.getAndSet(false)
              ? CompletableFuture.failedFuture(t)
              : new CompletableFuture<>();
    }

    private static Answer<CompletableFuture<Void>> hangingExceptionalSteadyStateManager(
        Throwable t, CountDownLatch latch) {
      AtomicBoolean firstTime = new AtomicBoolean(true);
      return invocation ->
          firstTime.getAndSet(false) ? firstTimeFuture(t, latch) : new CompletableFuture<>();
    }

    private static <T> CompletableFuture<T> firstTimeFuture(Throwable t, CountDownLatch latch)
        throws InterruptedException {
      latch.await();
      return CompletableFuture.failedFuture(t);
    }

    private static final Answer<CompletableFuture<SynonymMappingHighWaterMark>>
        HANGING_SYNONYM_MANAGER =
            invocation -> {
              Runnable callback = invocation.getArgument(3);
              callback.run();
              return new CompletableFuture<>();
            };

    private static Mocks create(
        Optional<IndexCommitUserData> commitUserData,
        Answer<CompletableFuture<ChangeStreamResumeInfo>> initialSyncQueueAnswer,
        Answer<CompletableFuture<Void>> steadyStateManagerAnswer,
        Answer<CompletableFuture<SynonymMappingHighWaterMark>> synonymManagerAnswer,
        Optional<Duration> resyncBackoff,
        Optional<CountDownLatch> latch)
        throws Exception {
      return create(
          commitUserData,
          initialSyncQueueAnswer,
          steadyStateManagerAnswer,
          synonymManagerAnswer,
          resyncBackoff,
          mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
          latch,
          Optional.empty());
    }

    private static Mocks create(
        Optional<IndexCommitUserData> commitUserData,
        Answer<CompletableFuture<ChangeStreamResumeInfo>> initialSyncQueueAnswer,
        Answer<CompletableFuture<Void>> steadyStateManagerAnswer,
        Answer<CompletableFuture<SynonymMappingHighWaterMark>> synonymManagerAnswer,
        Optional<Duration> resyncBackoff,
        Optional<CountDownLatch> latch,
        Optional<FeatureFlags> featureFlags)
        throws Exception {
      return create(
          commitUserData,
          initialSyncQueueAnswer,
          steadyStateManagerAnswer,
          synonymManagerAnswer,
          resyncBackoff,
          mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
          latch,
          featureFlags);
    }

    private static Mocks create(
        Optional<IndexCommitUserData> commitUserData,
        Answer<CompletableFuture<ChangeStreamResumeInfo>> initialSyncQueueAnswer,
        Answer<CompletableFuture<Void>> steadyStateManagerAnswer,
        Answer<CompletableFuture<SynonymMappingHighWaterMark>> synonymManagerAnswer,
        Optional<Duration> resyncBackoff,
        com.xgen.mongot.index.IndexGeneration indexGeneration,
        Optional<CountDownLatch> latch,
        Optional<FeatureFlags> featureFlags)
        throws Exception {
      ExecutorService executorService = Executors.newCachedThreadPool();
      MongotCursorManager cursorManager = mock(MongotCursorManager.class);

      InitialSyncQueue initialSyncQueue = mock(InitialSyncQueue.class);
      doAnswer(initialSyncQueueAnswer)
          .when(initialSyncQueue)
          .enqueue(any(), any(), any(), any(), anyBoolean(), anyBoolean());
      doAnswer(initialSyncQueueAnswer)
          .when(initialSyncQueue)
          .enqueueResume(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean());
      when(initialSyncQueue.cancel(any())).thenReturn(CompletableFuture.completedFuture(null));

      SteadyStateManager steadyStateManager = mock(SteadyStateManager.class);
      when(steadyStateManager.stop(any()))
          .thenReturn(CompletableFuture.completedFuture(RESUME_INFO));
      when(steadyStateManager.add(any(), any(), any(), any(), any(), anyBoolean()))
          .then(steadyStateManagerAnswer);

      SynonymManager synonymManager = mock(SynonymManager.class);
      when(synonymManager.cancel(any())).thenReturn(CompletableFuture.completedFuture(null));
      doAnswer(synonymManagerAnswer)
          .when(synonymManager)
          .enqueueCollectionScan(any(), any(), any(), any());

      SynonymMappingManager synonymMappingManager = mock(SynonymMappingManager.class);
      when(synonymMappingManager.shutdown()).thenReturn(CompletableFuture.completedFuture(null));

      SynonymMappingManagerFactory synonymFactory =
          (ignored0, ignored1, ignored2) -> List.of(synonymMappingManager);

      var index = mockInitializedIndex(indexGeneration);
      when(index.getWriter().getCommitUserData())
          .then(
              ignored ->
                  commitUserData
                      .map(IndexCommitUserData::toEncodedData)
                      .orElseGet(IndexCommitUserData.EMPTY::toEncodedData));
      clearInvocations(index);

      var metricsFactory = mockMetricsFactory();
      IndexMetricValuesSupplier indexMetricValuesSupplier =
          IndexMetricsSupplier.mockEmptyIndexMetricsSupplier();
      doReturn(
              IndexMetricsUpdaterBuilder.builder()
                  .metricsFactory(metricsFactory)
                  .indexMetricsSupplier(indexMetricValuesSupplier)
                  .build())
          .when(index)
          .getMetricsUpdater();

      DocumentIndexer documentIndexer =
          com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
              .mockDocumentIndexer();

      MeterRegistry meterRegistry = new SimpleMeterRegistry();
      PeriodicIndexCommitter periodicCommitter = mock(PeriodicIndexCommitter.class);
      ReplicationIndexManager manager =
          ReplicationIndexManager.create(
              executorService,
              cursorManager,
              initialSyncQueue,
              steadyStateManager,
              Optional.of(synonymManager),
              indexGeneration,
              index,
              documentIndexer,
              periodicCommitter,
              synonymFactory,
              meterRegistry,
              featureFlags.orElseGet(FeatureFlags::getDefault),
              resyncBackoff.orElseGet(() -> Duration.ofMillis(10)),
              Duration.ofMillis(10),
              Duration.ofMillis(10),
              false);

      return new Mocks(
          executorService,
          cursorManager,
          initialSyncQueue,
          steadyStateManager,
          synonymManager,
          index,
          documentIndexer,
          periodicCommitter,
          manager,
          synonymMappingManager,
          metricsFactory,
          meterRegistry,
          indexGeneration,
          latch);
    }

    private void assertStateTransitions(IndexManager.State from, IndexManager.State to) {
      assertEquals(
          1.0,
          this.meterRegistry
              .counter(
                  "replicationIndexManager.transitionState",
                  Tags.of("fromState", from.name(), "toState", to.name()))
              .count(),
          0.01);
    }

    public <T> void assertMetric(Class<T> clazz, String type, String cause, String causeCategory) {
      assertEquals(
          1.0,
          this.meterRegistry
              .counter(
                  "replicationIndexManager.exceptions",
                  Tags.of(
                      "clazz",
                      clazz.getSimpleName(),
                      "type",
                      type,
                      "cause",
                      cause,
                      "causeCategory",
                      causeCategory))
              .count(),
          0.01);
    }

    public void assertFailedIndexMetric(
        FailedIndexAction action, State state, Class<?> errorClass) {
      assertFailedIndexMetric(action, state, errorClass.getSimpleName());
    }

    public void assertFailedIndexMetric(FailedIndexAction action, State state, String error) {
      assertEquals(
          1.0,
          this.meterRegistry
              .counter(
                  "replicationIndexManager.failed_index",
                  Tags.of(
                      "action", action.name(),
                      "state", state.name(),
                      "error", error))
              .count(),
          0.01);
    }

    public <T> void assertDropMetric(Reason reason) {
      assertEquals(
          1.0,
          this.meterRegistry
              .counter(
                  "replicationIndexManager.indexesDroppedCounter",
                  Tags.of("reason", reason.name().toLowerCase()))
              .count(),
          0.01);
    }
  }
}
