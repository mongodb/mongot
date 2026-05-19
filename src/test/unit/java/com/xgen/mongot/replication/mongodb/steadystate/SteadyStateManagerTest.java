package com.xgen.mongot.replication.mongodb.steadystate;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IdTypeObservingDocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamManager;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SteadyStateManagerTest {

  private static final ChangeStreamResumeInfo RESUME_INFO =
      ChangeStreamResumeInfo.create(
          new MongoNamespace("database", "collection"),
          new BsonDocument("resumeToken", new BsonBoolean(true)));

  private static final IndexMetricsUpdater IGNORE_METRICS =
      IndexMetricsUpdaterBuilder.builder()
          .metricsFactory(SearchIndex.mockMetricsFactory())
          .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
          .build();

  @Test
  public void testShutdown() {
    ChangeStreamManager changeStreamManager = mock(ChangeStreamManager.class);
    when(changeStreamManager.shutdown()).thenReturn(FutureUtils.COMPLETED_FUTURE);

    SteadyStateManager manager = new SteadyStateManager(changeStreamManager, new HashMap<>());

    CompletableFuture<Void> future = manager.shutdown();
    Assert.assertTrue(future.isDone());
    verify(changeStreamManager).shutdown();
  }

  @Test
  public void testAdd() throws Exception {
    ChangeStreamManager changeStreamManager = mock(ChangeStreamManager.class);
    when(changeStreamManager.add(any(), any(), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(new CompletableFuture<>());

    SteadyStateManager manager = new SteadyStateManager(changeStreamManager, new HashMap<>());

    DocumentIndexer documentIndexer =
        com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
            .mockDocumentIndexer();
    manager.add(
        MOCK_INDEX_GENERATION_ID,
        documentIndexer,
        MOCK_INDEX_DEFINITION,
        IGNORE_METRICS,
        RESUME_INFO,
        false);

    ArgumentCaptor<DocumentIndexer> indexerCaptor =
        ArgumentCaptor.forClass(DocumentIndexer.class);
    verify(changeStreamManager)
        .add(
            eq(MOCK_INDEX_GENERATION_ID),
            indexerCaptor.capture(),
            eq(MOCK_INDEX_DEFINITION),
            eq(RESUME_INFO),
            any(),
            any(),
            eq(false));
    assertThat(indexerCaptor.getValue()).isInstanceOf(IdTypeObservingDocumentIndexer.class);
    assertThat(((IdTypeObservingDocumentIndexer) indexerCaptor.getValue()).getDelegate())
        .isEqualTo(documentIndexer);

    // Add an index with the same indexId, but new generation should work
    clearInvocations(changeStreamManager);
    GenerationId incrementedGeneration =
        GenerationIdBuilder.incrementUser(MOCK_INDEX_GENERATION_ID);
    manager.add(
        incrementedGeneration,
        documentIndexer,
        MOCK_INDEX_DEFINITION,
        IGNORE_METRICS,
        RESUME_INFO,
        false);

    ArgumentCaptor<DocumentIndexer> indexerCaptor2 =
        ArgumentCaptor.forClass(DocumentIndexer.class);
    verify(changeStreamManager)
        .add(
            eq(incrementedGeneration),
            indexerCaptor2.capture(),
            eq(MOCK_INDEX_DEFINITION),
            eq(RESUME_INFO),
            any(),
            any(),
            eq(false));
    assertThat(indexerCaptor2.getValue()).isInstanceOf(IdTypeObservingDocumentIndexer.class);
    assertThat(((IdTypeObservingDocumentIndexer) indexerCaptor2.getValue()).getDelegate())
        .isEqualTo(documentIndexer);
  }

  @Test
  public void testAddFutureCompletesWhenChangeStreamFutureCompletesExceptionally()
      throws Exception {
    ChangeStreamManager changeStreamManager = mock(ChangeStreamManager.class);
    CompletableFuture<Void> changeStreamFuture = new CompletableFuture<>();
    when(changeStreamManager.add(any(), any(), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(changeStreamFuture);

    SteadyStateManager manager = new SteadyStateManager(changeStreamManager, new HashMap<>());

    CompletableFuture<Void> future =
        manager.add(
            MOCK_INDEX_GENERATION_ID,
            com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
                .mockDocumentIndexer(),
            MOCK_INDEX_DEFINITION,
            IGNORE_METRICS,
            RESUME_INFO,
            false);
    Assert.assertFalse(future.isDone());

    changeStreamFuture.completeExceptionally(SteadyStateException.createDropped());
    Assert.assertTrue(future.isCompletedExceptionally());
    assertCompletesWithDropped(future);
  }

  @Test
  public void testAddAlreadyExisting() throws Exception {
    ChangeStreamManager changeStreamManager = mock(ChangeStreamManager.class);
    when(changeStreamManager.add(any(), any(), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(new CompletableFuture<>());

    SteadyStateManager manager = new SteadyStateManager(changeStreamManager, new HashMap<>());

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
            .mockDocumentIndexer(),
        MOCK_INDEX_DEFINITION,
        IGNORE_METRICS,
        RESUME_INFO,
        false);

    Assert.assertThrows(
        IllegalStateException.class,
        () ->
            manager.add(
                MOCK_INDEX_GENERATION_ID,
                com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
                    .mockDocumentIndexer(),
                MOCK_INDEX_DEFINITION,
                IGNORE_METRICS,
                RESUME_INFO,
                false));
  }

  @Test
  public void testAddAfterShutdownThrows() {
    ChangeStreamManager changeStreamManager = mock(ChangeStreamManager.class);
    when(changeStreamManager.shutdown()).thenReturn(FutureUtils.COMPLETED_FUTURE);

    SteadyStateManager manager = new SteadyStateManager(changeStreamManager, new HashMap<>());
    manager.shutdown();

    SteadyStateException e =
        Assert.assertThrows(
            SteadyStateException.class,
            () ->
                manager.add(
                    MOCK_INDEX_GENERATION_ID,
                    com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
                        .mockDocumentIndexer(),
                    MOCK_INDEX_DEFINITION,
                    IGNORE_METRICS,
                    RESUME_INFO,
                    false));
    Assert.assertTrue(e.isShutdown());
  }

  @Test
  public void testStop() throws Exception {
    ChangeStreamManager changeStreamManager = mock(ChangeStreamManager.class);
    when(changeStreamManager.add(any(), any(), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(new CompletableFuture<>());
    when(changeStreamManager.stop(any())).thenReturn(FutureUtils.COMPLETED_FUTURE);

    SteadyStateManager manager = new SteadyStateManager(changeStreamManager, new HashMap<>());

    manager.add(
        MOCK_INDEX_GENERATION_ID,
        com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
            .mockDocumentIndexer(),
        MOCK_INDEX_DEFINITION,
        IGNORE_METRICS,
        RESUME_INFO,
        false);

    CompletableFuture<ChangeStreamResumeInfo> future = manager.stop(MOCK_INDEX_GENERATION_ID);
    ChangeStreamResumeInfo resumeInfo = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(RESUME_INFO, resumeInfo);
  }

  @Test
  public void testStopDoesNotExist() {
    ChangeStreamManager changeStreamManager = mock(ChangeStreamManager.class);
    SteadyStateManager manager = new SteadyStateManager(changeStreamManager, new HashMap<>());

    try {
      manager.stop(MOCK_INDEX_GENERATION_ID);
      Assert.fail("SteadyStateManager did not throw exception");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  private static void assertCompletesWithDropped(CompletableFuture<Void> future) throws Exception {
    future
        .handle(
            (ignored, throwable) -> {
              Assert.assertNull(ignored);
              Assert.assertNotNull(throwable);
              assertThat(throwable).isInstanceOf(SteadyStateException.class);
              Assert.assertEquals(
                  SteadyStateException.Type.DROPPED, ((SteadyStateException) throwable).getType());
              return null;
            })
        .get(5, TimeUnit.SECONDS);
  }
}
