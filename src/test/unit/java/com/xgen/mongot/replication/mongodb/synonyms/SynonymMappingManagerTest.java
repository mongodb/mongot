package com.xgen.mongot.replication.mongodb.synonyms;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SYNONYM_MAPPING_DEFINITION_NAME;
import static com.xgen.testing.mongot.mock.index.VectorIndex.mockVectorDefinition;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Keep;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.synonym.SynonymMappingException;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class SynonymMappingManagerTest {
  /** Upper bound (exclusive) on the jittered backoff used for throttled syncs. */
  private static final Duration THROTTLED_BACKOFF_LIMIT = Duration.ofMillis(100);

  private static final BsonTimestamp SCAN_OPERATION_TIME = new BsonTimestamp(12345L);
  private static final SynonymMappingHighWaterMark SYNONYM_SCAN_HIGH_WATER_MARK =
      SynonymMappingHighWaterMark.create(SCAN_OPERATION_TIME);

  private static final BsonTimestamp SCAN_OPERATION_TIME_T1 = new BsonTimestamp(23456L);
  private static final SynonymMappingHighWaterMark SYNONYM_SCAN_HIGH_WATER_MARK_T1 =
      SynonymMappingHighWaterMark.create(SCAN_OPERATION_TIME_T1);

  private static final BsonTimestamp SCAN_OPERATION_TIME_T2 = new BsonTimestamp(34567L);
  private static final SynonymMappingHighWaterMark SYNONYM_SCAN_HIGH_WATER_MARK_T2 =
      SynonymMappingHighWaterMark.create(SCAN_OPERATION_TIME_T2);

  @Test
  public void testSuccessfulSynonymSync() throws Exception {
    try (Mocks mocks = Mocks.successfulSynonymSync()) {
      Thread.sleep(500);
      Assert.assertEquals(
          SynonymMappingManager.State.READY, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());
    }
  }

  @Test
  public void testInvalidSynonymSyncExceptionSynonymSync() throws Exception {
    Throwable throwable =
        SynonymSyncException.createInvalid(
            SynonymMappingException.invalidSynonymDocument(new Exception("failed document")));
    assertStateAfterException(SynonymMappingManager.State.INVALID, throwable);
  }

  @Test
  public void testExceededSynonymSyncExceptionSynonymSync() throws Exception {
    Throwable throwable =
        SynonymSyncException.createFieldExceeded(new FieldExceededLimitsException("exceeded"));
    assertStateAfterException(SynonymMappingManager.State.INVALID, throwable);
  }

  @Test
  public void testDroppedSynonymSyncExceptionSynonymSync() throws Exception {
    Throwable throwable = SynonymSyncException.createDropped();
    assertStateAfterException(SynonymMappingManager.State.READY, throwable);
  }

  @Test
  public void testShutdownSynonymSyncExceptionSynonymSync() throws Exception {
    Throwable throwable = SynonymSyncException.createShutDown();
    assertStateAfterException(SynonymMappingManager.State.SHUTDOWN, throwable);
  }

  @Test
  public void testShutdownBeforeSynonymScanHandleResult() throws Exception {
    CountDownLatch syncStarted = new CountDownLatch(1);
    CompletableFuture<SynonymMappingHighWaterMark> firstFuture = new CompletableFuture<>();

    try (Mocks mocks =
        Mocks.createWithCollectionScanAnswer(
            invocation -> {
              Runnable callback = invocation.getArgument(3, Runnable.class);
              callback.run();
              syncStarted.countDown();
              return firstFuture;
            })) {
      // Should be in initial sync before shutdown and first scan completes.
      Assert.assertTrue(syncStarted.await(5, TimeUnit.SECONDS));
      Assert.assertEquals(
          SynonymMappingManager.State.INITIAL_SYNC, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      mocks.synonymMappingManager.shutdown().get(5, TimeUnit.SECONDS);

      CompletableFuture<?> completeFuture =
          CompletableFuture.runAsync(
              () -> firstFuture.complete(SYNONYM_SCAN_HIGH_WATER_MARK), mocks.lifecycleExecutor);
      completeFuture.get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testFailedSynonymSyncExceptionSynonymSync() throws Exception {
    Throwable throwable =
        SynonymSyncException.createFailed("unknown exception during synonym mapping creation");
    try (Mocks mocks = Mocks.exceptionalSynonymSync(throwable)) {
      Thread.sleep(500);
      Assert.assertEquals(
          SynonymMappingManager.State.FAILED, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());
    }
  }

  @Test
  public void testDroppedSynonymSyncAdvancesHighWaterMark() throws Exception {
    Throwable invalid =
        SynonymSyncException.createInvalid(
            SynonymMappingException.invalidSynonymDocument(new IOException()),
            Optional.of(SCAN_OPERATION_TIME));
    Throwable dropped = SynonymSyncException.createDropped();

    CompletableFuture<SynonymMappingHighWaterMark> firstFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> secondFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> thirdFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> fourthFuture = new CompletableFuture<>();

    List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> collectionScanAnswers =
        List.of(
            invocation -> {
              Runnable callback = invocation.getArgument(3, Runnable.class);
              callback.run();
              return firstFuture;
            },
            invocation -> {
              Runnable callback = invocation.getArgument(3, Runnable.class);
              callback.run();
              return thirdFuture;
            });

    List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> changeStreamAnswers =
        List.of(invocation -> secondFuture, invocation -> fourthFuture);

    Mocks mocks = Mocks.create(collectionScanAnswers, changeStreamAnswers);
    try (mocks) {
      Thread.sleep(500);
      Assert.assertEquals(
          SynonymMappingManager.State.INITIAL_SYNC, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // See an invalid document when performing initial sync.
      CompletableFuture.runAsync(() -> firstFuture.completeExceptionally(invalid));

      Thread.sleep(500);
      Assert.assertEquals(
          SynonymMappingManager.State.INVALID, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());

      // Witness a drop when listening for changes.
      CompletableFuture.runAsync(() -> secondFuture.completeExceptionally(dropped));

      Thread.sleep(500);
      // Should transition to READY_UPDATING.
      Assert.assertEquals(
          SynonymMappingManager.State.READY_UPDATING, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // Successful collection scan from empty collection.
      CompletableFuture.runAsync(() -> thirdFuture.complete(SYNONYM_SCAN_HIGH_WATER_MARK_T1));

      Thread.sleep(500);
      // After sync, should be READY with operation time of last sync.
      Assert.assertEquals(
          SynonymMappingManager.State.READY, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK_T1,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());

      // Collection scan completes without seeing change events.
      CompletableFuture.runAsync(() -> fourthFuture.complete(SYNONYM_SCAN_HIGH_WATER_MARK_T2));

      Thread.sleep(500);
      // Should stay READY and advance high water mark because no changes were witnessed.
      Assert.assertEquals(
          SynonymMappingManager.State.READY, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK_T2,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());
    }
  }

  @Test
  public void testInvalidDocumentWithOperationTime() throws Exception {
    Throwable invalid =
        SynonymSyncException.createInvalid(
            SynonymMappingException.invalidSynonymDocument(new IOException()),
            Optional.of(SCAN_OPERATION_TIME));

    CompletableFuture<SynonymMappingHighWaterMark> firstFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> secondFuture = new CompletableFuture<>();
    List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> collectionScanAnswers =
        List.of(
            invocation -> {
              Runnable callback = invocation.getArgument(3, Runnable.class);
              callback.run();
              return firstFuture;
            });

    List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> changeStreamAnswers =
        List.of(invocation -> secondFuture);

    Mocks mocks = Mocks.create(collectionScanAnswers, changeStreamAnswers);
    try (mocks) {
      Thread.sleep(500);
      Assert.assertEquals(
          SynonymMappingManager.State.INITIAL_SYNC, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // See an invalid document when performing initial sync.
      CompletableFuture.runAsync(() -> firstFuture.completeExceptionally(invalid));

      Thread.sleep(500);
      // Should be INVALID state.
      Assert.assertEquals(
          SynonymMappingManager.State.INVALID, mocks.synonymMappingManager.getState());
      // Should have operation time set from invalid exception.
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());

      // Watching for invalid events, continue to advance the high water mark.
      CompletableFuture.runAsync(() -> secondFuture.complete(SYNONYM_SCAN_HIGH_WATER_MARK_T1));

      Thread.sleep(500);
      // Remain INVALID, update high water mark.
      Assert.assertEquals(
          SynonymMappingManager.State.INVALID, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK_T1,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());
    }
  }

  @Test
  public void testInvalidDocumentRecoversOnChangeAndSuccessfulScan() throws Exception {
    Throwable invalid =
        SynonymSyncException.createInvalid(
            SynonymMappingException.invalidSynonymDocument(new IOException()),
            Optional.of(SCAN_OPERATION_TIME));

    CompletableFuture<SynonymMappingHighWaterMark> firstFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> secondFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> thirdFuture = new CompletableFuture<>();
    List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> collectionScanAnswers =
        List.of(
            invocation -> {
              Runnable callback = invocation.getArgument(3, Runnable.class);
              callback.run();
              return firstFuture;
            },
            invocation -> {
              Runnable callback = invocation.getArgument(3, Runnable.class);
              callback.run();
              return thirdFuture;
            });

    List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> changeStreamAnswers =
        List.of(invocation -> secondFuture);

    Mocks mocks = Mocks.create(collectionScanAnswers, changeStreamAnswers);
    try (mocks) {
      Thread.sleep(500);
      Assert.assertEquals(
          SynonymMappingManager.State.INITIAL_SYNC, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // See an invalid document when performing initial sync.
      CompletableFuture.runAsync(() -> firstFuture.completeExceptionally(invalid));

      Thread.sleep(500);
      // Should be INVALID state.
      Assert.assertEquals(
          SynonymMappingManager.State.INVALID, mocks.synonymMappingManager.getState());
      // Should have operation time set from invalid exception.
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());

      // Watching for events in invalid state, notice a change and return an empty high water mark.
      CompletableFuture.runAsync(
          () -> secondFuture.complete(SynonymMappingHighWaterMark.createEmpty()));

      Thread.sleep(500);
      // Transition to INITIAL_SYNC, try collection scan again.
      Assert.assertEquals(
          SynonymMappingManager.State.INITIAL_SYNC, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // Complete scan successfully.
      CompletableFuture.runAsync(() -> thirdFuture.complete(SYNONYM_SCAN_HIGH_WATER_MARK_T1));

      Thread.sleep(500);
      // Transition to INITIAL_SYNC, try collection scan again.
      Assert.assertEquals(
          SynonymMappingManager.State.READY, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK_T1,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());
    }
  }

  @Test
  public void testInvalidDocumentWithoutOperationTime() throws Exception {
    Throwable invalid =
        SynonymSyncException.createInvalid(
            SynonymMappingException.invalidSynonymDocument(new IOException()));
    Throwable invalidWithTime =
        SynonymSyncException.createInvalid(
            SynonymMappingException.invalidSynonymDocument(new IOException()),
            Optional.of(SCAN_OPERATION_TIME));

    CompletableFuture<SynonymMappingHighWaterMark> firstFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> secondFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> thirdFuture = new CompletableFuture<>();
    List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> collectionScanAnswers =
        List.of(
            invocation -> {
              Runnable callback = invocation.getArgument(3, Runnable.class);
              callback.run();
              return firstFuture;
            },
            invocation -> {
              Runnable callback = invocation.getArgument(3, Runnable.class);
              callback.run();
              return secondFuture;
            });

    List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> changeStreamAnswers =
        List.of(invocation -> thirdFuture);

    Mocks mocks = Mocks.create(collectionScanAnswers, changeStreamAnswers);
    try (mocks) {
      Thread.sleep(500);
      Assert.assertEquals(
          SynonymMappingManager.State.INITIAL_SYNC, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // See an invalid document when performing initial sync.
      CompletableFuture.runAsync(() -> firstFuture.completeExceptionally(invalid));

      Thread.sleep(500);
      // Without an operation time, remain in INITIAL_SYNC state.
      Assert.assertEquals(
          SynonymMappingManager.State.INITIAL_SYNC, mocks.synonymMappingManager.getState());
      // Should have empty high water mark.
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // Without a mark to start a change stream from, a mapping must try a collection scan again
      // from scratch.
      CompletableFuture.runAsync(() -> secondFuture.completeExceptionally(invalidWithTime));

      Thread.sleep(500);
      // Remain INVALID, but this time set high water mark.
      Assert.assertEquals(
          SynonymMappingManager.State.INVALID, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());

      // Watching for invalid events, continue to advance the high water mark.
      CompletableFuture.runAsync(() -> thirdFuture.complete(SYNONYM_SCAN_HIGH_WATER_MARK_T1));

      Thread.sleep(500);
      // Remain INVALID, update high water mark.
      Assert.assertEquals(
          SynonymMappingManager.State.INVALID, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK_T1,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());
    }
  }

  @Test
  public void testTransientSynonymSyncExceptionSynonymSync() throws Exception {
    Throwable throwable =
        SynonymSyncException.createTransient(
            SynonymMappingException.failSynonymMapBuild(
                new Exception("analyzed mapping to empty string")));

    CompletableFuture<SynonymMappingHighWaterMark> firstFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> secondFuture = new CompletableFuture<>();
    Mocks mocks =
        Mocks.create(
            List.of(
                invocation -> {
                  Runnable callback = invocation.getArgument(3, Runnable.class);
                  callback.run();
                  return firstFuture;
                },
                invocation -> {
                  Runnable callback = invocation.getArgument(3, Runnable.class);
                  callback.run();
                  return secondFuture;
                }),
            List.of());

    try (mocks) {
      Thread.sleep(500);
      // Should be in initial sync before first scan completes.
      Assert.assertEquals(
          SynonymMappingManager.State.INITIAL_SYNC, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // Encounter transient error on first run, remain in INITIAL_SYNC state.
      CompletableFuture.runAsync(
          () -> firstFuture.completeExceptionally(throwable), mocks.lifecycleExecutor);

      Thread.sleep(500);
      // Should be in initial sync after transient error and re-enqueueing.
      Assert.assertEquals(
          SynonymMappingManager.State.INITIAL_SYNC, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // Complete sync on second try.
      CompletableFuture.runAsync(() -> secondFuture.complete(SYNONYM_SCAN_HIGH_WATER_MARK));

      Thread.sleep(500);
      // Should be ready after successful second initial sync attempt.
      Assert.assertEquals(
          SynonymMappingManager.State.READY, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());
    }
  }

  @Test
  public void testTransientSynonymSyncExceptionFromCollectionScan() throws Exception {
    Throwable throwable =
        SynonymSyncException.createTransient(
            SynonymMappingException.failSynonymMapBuild(
                new Exception("analyzed mapping to empty string")));

    CompletableFuture<SynonymMappingHighWaterMark> firstFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> secondFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> thirdFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> fourthFuture = new CompletableFuture<>();

    List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> collectionScanAnswers =
        List.of(
            invocation -> {
              Runnable callback = invocation.getArgument(3, Runnable.class);
              callback.run();
              return firstFuture;
            },
            invocation -> {
              Runnable callback = invocation.getArgument(3, Runnable.class);
              callback.run();
              return thirdFuture;
            },
            invocation -> {
              Runnable callback = invocation.getArgument(3, Runnable.class);
              callback.run();
              return fourthFuture;
            });

    List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> changeStreamAnswers =
        List.of(invocation -> secondFuture);

    Mocks mocks = Mocks.create(collectionScanAnswers, changeStreamAnswers);

    try (mocks) {
      Thread.sleep(500);
      Assert.assertEquals(
          SynonymMappingManager.State.INITIAL_SYNC, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // Complete sync on first try.
      CompletableFuture.runAsync(() -> firstFuture.complete(SYNONYM_SCAN_HIGH_WATER_MARK));

      Thread.sleep(500);
      Assert.assertEquals(
          SynonymMappingManager.State.READY, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());

      // Notice change in change stream.
      CompletableFuture.runAsync(
          () -> secondFuture.complete(SynonymMappingHighWaterMark.createEmpty()));

      Thread.sleep(500);
      // Should be READY_UPDATING on change.
      Assert.assertEquals(
          SynonymMappingManager.State.READY_UPDATING, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // Transient error on sync.
      CompletableFuture.runAsync(
          () -> thirdFuture.completeExceptionally(throwable), mocks.lifecycleExecutor);

      Thread.sleep(500);
      // Should still be READY_UPDATING after transient error; should try again.
      Assert.assertEquals(
          SynonymMappingManager.State.READY_UPDATING, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // Complete sync on fourth sync.
      CompletableFuture.runAsync(() -> fourthFuture.complete(SYNONYM_SCAN_HIGH_WATER_MARK_T1));

      Thread.sleep(500);
      // Transition to ready state on completion.
      Assert.assertEquals(
          SynonymMappingManager.State.READY, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK_T1,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());
    }
  }

  @Test
  public void testCollectionScanKeepsUpdatingHighWaterMark() throws Exception {
    CompletableFuture<SynonymMappingHighWaterMark> firstFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> secondFuture = new CompletableFuture<>();
    CompletableFuture<SynonymMappingHighWaterMark> thirdFuture = new CompletableFuture<>();

    List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> collectionScanAnswers =
        List.of(
            invocation -> {
              Runnable callback = invocation.getArgument(3, Runnable.class);
              callback.run();
              return firstFuture;
            });

    List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> changeStreamAnswers =
        List.of(invocation -> secondFuture, invocation -> thirdFuture);

    Mocks mocks = Mocks.create(collectionScanAnswers, changeStreamAnswers);

    try (mocks) {
      Thread.sleep(500);
      Assert.assertEquals(
          SynonymMappingManager.State.INITIAL_SYNC, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());

      // Complete sync on first try.
      CompletableFuture.runAsync(() -> firstFuture.complete(SYNONYM_SCAN_HIGH_WATER_MARK));

      Thread.sleep(500);
      // Transition to ready state on completion.
      Assert.assertEquals(
          SynonymMappingManager.State.READY, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());

      // No changes in change stream, update high water mark time.
      CompletableFuture.runAsync(() -> secondFuture.complete(SYNONYM_SCAN_HIGH_WATER_MARK_T1));

      Thread.sleep(500);
      // Should still be READY.
      Assert.assertEquals(
          SynonymMappingManager.State.READY, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK_T1,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());

      // No changes in change stream, update high water mark time.
      CompletableFuture.runAsync(() -> thirdFuture.complete(SYNONYM_SCAN_HIGH_WATER_MARK_T2));

      Thread.sleep(500);
      // Should still be READY.
      Assert.assertEquals(
          SynonymMappingManager.State.READY, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          SYNONYM_SCAN_HIGH_WATER_MARK_T2,
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark());
    }
  }

  @Ignore // TODO(CLOUDP-280897): vector search - need VectorIndex mock
  @Test
  public void testCreateForVectorIndexReturnsEmptyList() {
    VectorIndexDefinitionGeneration definitionGeneration =
        new VectorIndexDefinitionGeneration(
            mockVectorDefinition(new ObjectId()), Generation.CURRENT);
    Collection<SynonymMappingManager> result =
        SynonymMappingManager.create(
            mock(SynonymManager.class),
            mock(Executor.class),
            new IndexGeneration(VectorIndex.mockIndex(definitionGeneration), definitionGeneration));
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void backoffFor_throttledTransientException_usesShortJitteredBackoff() throws Exception {
    Duration transientBackoff = Duration.ofSeconds(30);
    SynonymSyncException throttled =
        SynonymSyncException.createThrottled(
            new MongoCommandException(
                new BsonDocument("ok", new BsonInt32(0))
                    .append("code", new BsonInt32(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code))
                    .append("errmsg", new BsonString("throttled")),
                new ServerAddress()),
            Optional.empty());

    try (Mocks mocks =
        Mocks.create(
            List.of(
                invocation -> {
                  Runnable callback = invocation.getArgument(3, Runnable.class);
                  callback.run();
                  return CompletableFuture.completedFuture(SYNONYM_SCAN_HIGH_WATER_MARK);
                }),
            Collections.emptyList(),
            transientBackoff)) {
      Assert.assertTrue(throttled.isThrottled());
      Assert.assertTrue(
          mocks.synonymMappingManager.backoffFor(throttled).compareTo(THROTTLED_BACKOFF_LIMIT) < 0);
    }
  }

  @Test
  public void backoffFor_plainTransientException_usesFlatTransientBackoff() throws Exception {
    Duration transientBackoff = Duration.ofSeconds(30);
    SynonymSyncException transientException = SynonymSyncException.createTransient("transient");

    try (Mocks mocks =
        Mocks.create(
            List.of(
                invocation -> {
                  Runnable callback = invocation.getArgument(3, Runnable.class);
                  callback.run();
                  return CompletableFuture.completedFuture(SYNONYM_SCAN_HIGH_WATER_MARK);
                }),
            Collections.emptyList(),
            transientBackoff)) {
      Assert.assertFalse(transientException.isThrottled());
      Assert.assertEquals(
          transientBackoff, mocks.synonymMappingManager.backoffFor(transientException));
    }
  }

  private static void assertStateAfterException(
      SynonymMappingManager.State state, Throwable throwable) throws Exception {
    try (Mocks mocks = Mocks.exceptionalSynonymSync(throwable)) {
      Thread.sleep(500);
      Assert.assertEquals(state, mocks.synonymMappingManager.getState());
      Assert.assertEquals(
          mocks.synonymMappingManager.getSynonymMappingHighWaterMark(),
          SynonymMappingHighWaterMark.createEmpty());
    }
  }

  private static class Mocks implements Closeable {

    private static Mocks successfulSynonymSync() throws Exception {
      return createWithCollectionScanAnswer(
          invocation -> {
            // run onComplete callback
            Runnable callback = invocation.getArgument(3, Runnable.class);
            callback.run();
            return CompletableFuture.completedFuture(SYNONYM_SCAN_HIGH_WATER_MARK);
          });
    }

    private static Mocks exceptionalSynonymSync(Throwable throwable) throws Exception {
      return createWithCollectionScanAnswer(
          invocation -> {
            // run onComplete callback
            Runnable callback = invocation.getArgument(3, Runnable.class);
            callback.run();
            return CompletableFuture.failedFuture(throwable);
          });
    }

    @Keep final SynonymManager synonymManager;
    final ExecutorService lifecycleExecutor;
    @Keep final SynonymDocumentIndexerFactory documentIndexerFactory;
    @Keep final SynonymMappingDefinition definition;
    @Keep final IndexGeneration indexGeneration;

    final SynonymMappingManager synonymMappingManager;

    private Mocks(
        SynonymManager synonymManager,
        ExecutorService lifecycleExecutor,
        SynonymDocumentIndexerFactory documentIndexerFactory,
        SynonymMappingDefinition definition,
        IndexGeneration indexGeneration,
        SynonymMappingManager synonymMappingManager) {
      this.synonymManager = synonymManager;
      this.lifecycleExecutor = lifecycleExecutor;
      this.documentIndexerFactory = documentIndexerFactory;
      this.definition = definition;
      this.indexGeneration = indexGeneration;
      this.synonymMappingManager = synonymMappingManager;
    }

    static Mocks createWithCollectionScanAnswer(
        Answer<CompletableFuture<SynonymMappingHighWaterMark>> synonymManagerAnswer)
        throws Exception {
      return create(List.of(synonymManagerAnswer), Collections.emptyList());
    }

    static Mocks create(
        List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> collectionScanAnswers,
        List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> changeStreamAnswers)
        throws Exception {
      return create(collectionScanAnswers, changeStreamAnswers, Duration.ZERO);
    }

    static Mocks create(
        List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> collectionScanAnswers,
        List<Answer<CompletableFuture<SynonymMappingHighWaterMark>>> changeStreamAnswers,
        Duration transientBackoff)
        throws Exception {
      Answer<CompletableFuture<SynonymMappingHighWaterMark>> collScanAnswer =
          new Answer<>() {
            private int invocationCount = 0;

            @Override
            public CompletableFuture<SynonymMappingHighWaterMark> answer(
                InvocationOnMock invocation) throws Throwable {
              return collectionScanAnswers.get(this.invocationCount++).answer(invocation);
            }
          };

      Answer<CompletableFuture<SynonymMappingHighWaterMark>> chgStreamAnswer =
          new Answer<>() {
            private int invocationCount = 0;

            @Override
            public CompletableFuture<SynonymMappingHighWaterMark> answer(
                InvocationOnMock invocation) throws Throwable {
              return changeStreamAnswers.get(this.invocationCount++).answer(invocation);
            }
          };

      ExecutorService lifecycleExecutor = Executors.newCachedThreadPool();

      SynonymDocumentIndexer synonymDocumentIndexer = mock(SynonymDocumentIndexer.class);

      SynonymDocumentIndexerFactory synonymDocumentIndexerFactory =
          (ignored0, ignored2) -> synonymDocumentIndexer;

      IndexGeneration indexGeneration = mockIndexGeneration();

      SynonymMappingDefinition synonymMappingDefinition =
          indexGeneration
              .getDefinition()
              .asSearchDefinition()
              .getSynonymMap()
              .get(MOCK_SYNONYM_MAPPING_DEFINITION_NAME);

      SynonymManager synonymManager = mock(SynonymManager.class);

      when(synonymManager.cancel(any())).thenReturn(CompletableFuture.completedFuture(null));
      when(synonymManager.enqueueCollectionScan(any(), any(), any(), any()))
          .thenAnswer(collScanAnswer);
      when(synonymManager.enqueueChangeStream(any(), any(), any())).thenAnswer(chgStreamAnswer);

      SynonymMappingManager manager =
          SynonymMappingManager.create(
              synonymManager,
              lifecycleExecutor,
              synonymDocumentIndexerFactory,
              synonymMappingDefinition,
              indexGeneration,
              Duration.ZERO,
              transientBackoff);

      return new Mocks(
          synonymManager,
          lifecycleExecutor,
          synonymDocumentIndexerFactory,
          synonymMappingDefinition,
          indexGeneration,
          manager);
    }

    @Override
    public void close() {
      this.lifecycleExecutor.shutdown();
      try {
        this.lifecycleExecutor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
