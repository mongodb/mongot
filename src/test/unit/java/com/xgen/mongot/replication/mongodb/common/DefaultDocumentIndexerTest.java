package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.DocumentEvent.EventType.DELETE;
import static com.xgen.testing.mongot.mock.index.IndexWriter.mockStagingIndexWriter;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockInitializedIndex;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.CommitDataStagingWriter;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.regex.RegexInterruptedException;
import com.xgen.mongot.util.regex.RegexLimitExceededException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.junit.Test;
import org.mockito.InOrder;

public class DefaultDocumentIndexerTest {

  @Test
  public void clearIndex_delayedCheckpoint_preservesAnalysisFailure() throws Exception {
    assertDelayedCheckpointCannotReplaceFailure(DefaultDocumentIndexer::updateCommitUserData);
  }

  @Test
  public void clearIndex_delayedCheckpointSupplier_preservesAnalysisFailure() throws Exception {
    assertDelayedCheckpointCannotReplaceFailure(
        (indexer, checkpoint) -> indexer.updateCommitUserDataSupplier(() -> checkpoint));
  }

  private static void assertDelayedCheckpointCannotReplaceFailure(
      BiConsumer<DefaultDocumentIndexer, IndexCommitUserData> publishCheckpoint) throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    var writer = index.getWriter();
    var indexer = DefaultDocumentIndexer.create(index);
    var checkpoint = commitUserData(index, 1);
    var failure = new RegexLimitExceededException();
    var failed = IndexCommitUserData.createExceeded(failure.getMessage());
    AtomicReference<EncodedUserData> persisted = new AtomicReference<>();
    doAnswer(
            call -> {
              persisted.set(call.getArgument(0));
              return null;
            })
        .when(writer)
        .commit(any());
    doAnswer(
            call -> {
              persisted.set(call.getArgument(0));
              return null;
            })
        .when(index)
        .clear(any());
    doThrow(failure).when(writer).updateIndex(any());
    CountDownLatch finalizerPaused = new CountDownLatch(1);
    CountDownLatch releaseFinalizer = new CountDownLatch(1);

    try (var executor = Executors.newSingleThreadExecutor()) {
      var finalizer =
          executor.submit(
              () -> {
                // An earlier batch can be descheduled after releasing the next batch in
                // SchedulerQueue,
                // but before publishing its checkpoint. Resume it only after the failed clear is
                // durable.
                finalizerPaused.countDown();
                assertThat(releaseFinalizer.await(5, TimeUnit.SECONDS)).isTrue();
                publishCheckpoint.accept(indexer, checkpoint);
                return null;
              });
      try {
        assertThat(finalizerPaused.await(5, TimeUnit.SECONDS)).isTrue();
        assertThrows(
            RegexLimitExceededException.class,
            () -> indexer.indexDocumentEvent(DocumentEvent.createDelete(ID)));
        indexer.clearIndex(failed);
        releaseFinalizer.countDown();
        finalizer.get(5, TimeUnit.SECONDS);
        // A periodic commit that already passed its status check must retain the terminal marker.
        indexer.commit();

        assertThat(persisted.get().asString()).isEqualTo(failed.toEncodedData().asString());
      } finally {
        releaseFinalizer.countDown();
      }
    }
  }

  @Test
  public void clearIndex_overlappingCommit_persistsFailureAfterCommitFinishes() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    var writer = index.getWriter();
    var indexer = DefaultDocumentIndexer.create(index);
    var checkpoint = commitUserData(index, 1);
    var failed = IndexCommitUserData.createExceeded(new RegexLimitExceededException().getMessage());
    indexer.updateCommitUserData(checkpoint);
    CountDownLatch commitEntered = new CountDownLatch(1);
    CountDownLatch releaseCommit = new CountDownLatch(1);
    CountDownLatch clearRequested = new CountDownLatch(1);
    AtomicReference<EncodedUserData> persisted = new AtomicReference<>();
    doAnswer(
            call -> {
              commitEntered.countDown();
              assertThat(releaseCommit.await(5, TimeUnit.SECONDS)).isTrue();
              persisted.set(call.getArgument(0));
              return null;
            })
        .when(writer)
        .commit(any());
    doAnswer(
            call -> {
              persisted.set(call.getArgument(0));
              return null;
            })
        .when(index)
        .clear(any());
    doThrow(new RegexLimitExceededException()).when(writer).updateIndex(any());

    try (var executor = Executors.newFixedThreadPool(2)) {
      var commit =
          executor.submit(
              () -> {
                indexer.commit();
                return null;
              });
      try {
        assertThat(commitEntered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThrows(
            RegexLimitExceededException.class,
            () -> indexer.indexDocumentEvent(DocumentEvent.createDelete(ID)));
        var clear =
            executor.submit(
                () -> {
                  clearRequested.countDown();
                  indexer.clearIndex(failed);
                });
        assertThat(clearRequested.await(5, TimeUnit.SECONDS)).isTrue();
        releaseCommit.countDown();
        commit.get(5, TimeUnit.SECONDS);
        clear.get(5, TimeUnit.SECONDS);

        assertThat(persisted.get().asString()).isEqualTo(failed.toEncodedData().asString());
      } finally {
        releaseCommit.countDown();
      }
    }
  }

  @Test
  public void indexDocumentEvent_regexLimit_stopsSubsequentDocumentsForThisIndexer()
      throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);
    DocumentEvent event = DocumentEvent.createDelete(ID);
    var writer = index.getWriter();
    doThrow(new RegexLimitExceededException()).when(writer).updateIndex(event);

    assertThrows(RegexLimitExceededException.class, () -> indexer.indexDocumentEvent(event));
    assertThrows(RegexLimitExceededException.class, () -> indexer.indexDocumentEvent(event));

    verify(index.getWriter(), times(1)).updateIndex(event);
  }

  @Test
  public void indexDocumentEvent_interrupted_doesNotLatchIndexFailure() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);
    DocumentEvent event = DocumentEvent.createDelete(ID);
    var writer = index.getWriter();
    doThrow(new RegexInterruptedException()).doNothing().when(writer).updateIndex(event);

    assertThrows(RegexInterruptedException.class, () -> indexer.indexDocumentEvent(event));
    indexer.indexDocumentEvent(event);

    verify(index.getWriter(), times(2)).updateIndex(event);
  }

  private static final BsonValue ID = new BsonInt32(0);
  private static final RawBsonDocument DOCUMENT =
      BsonUtils.documentToRaw(
          new BsonDocument(MOCK_INDEX_ID.toString(), new BsonDocument("_id", ID)));

  @Test
  public void testIndexInsertDocumentEvent() throws Exception {
    testIndexDocumentEvent(DocumentEvent.EventType.INSERT);
  }

  @Test
  public void testIndexUpdateDocumentEvent() throws Exception {
    testIndexDocumentEvent(DocumentEvent.EventType.UPDATE);
  }

  @Test
  public void testIndexDeleteDocumentEvent() throws Exception {
    testIndexDocumentEvent(DELETE);
  }

  @Test
  public void testCommit() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);
    EncodedUserData userData =
        EncodedUserData.fromString("{\"backendIndexVersion\": {\"$numberInt\": \"123\"}}");
    indexer.updateCommitUserData(
        IndexCommitUserData.fromEncodedData(userData, Optional.of(index.getGenerationId())));
    indexer.commit();
    verify(index.getWriter()).commit(userData);
  }

  @Test
  public void testCommitStagesUserDataOnAStagingWriterBeforeCommitting() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    var stagingWriter = mockStagingIndexWriter();
    when(index.getWriter()).thenReturn(stagingWriter);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);
    EncodedUserData userData =
        EncodedUserData.fromString("{\"backendIndexVersion\": {\"$numberInt\": \"123\"}}");
    indexer.updateCommitUserData(
        IndexCommitUserData.fromEncodedData(userData, Optional.of(index.getGenerationId())));

    indexer.commit();

    InOrder inOrder = inOrder(stagingWriter);
    inOrder.verify((CommitDataStagingWriter) stagingWriter).stageUserData(userData);
    inOrder.verify(stagingWriter).commit(userData);
  }

  @Test
  public void testStageCommitUserDataStagesWithoutCommitting() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    var stagingWriter = mockStagingIndexWriter();
    when(index.getWriter()).thenReturn(stagingWriter);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);
    EncodedUserData userData =
        EncodedUserData.fromString("{\"backendIndexVersion\": {\"$numberInt\": \"123\"}}");
    indexer.updateCommitUserData(
        IndexCommitUserData.fromEncodedData(userData, Optional.of(index.getGenerationId())));

    indexer.stageCommitUserData();

    verify((CommitDataStagingWriter) stagingWriter).stageUserData(userData);
    verify(stagingWriter, never()).commit(any());
  }

  @Test
  public void testStageCommitUserDataIsANoOpForNonStagingWriters() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);
    indexer.updateCommitUserData(commitUserData(index, 1));

    indexer.stageCommitUserData();

    verify(index.getWriter(), never()).commit(any());
  }

  @Test
  public void testStageCommitUserDataIncludesAutoEmbeddingMetadata() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    var stagingWriter = mockStagingIndexWriter();
    when(index.getWriter()).thenReturn(stagingWriter);
    DefaultDocumentIndexer indexer =
        DefaultDocumentIndexer.create(index, () -> new AutoEmbeddingCommitMetadata(true));
    EncodedUserData base =
        EncodedUserData.fromString("{\"backendIndexVersion\": {\"$numberInt\": \"123\"}}");
    indexer.updateCommitUserData(
        IndexCommitUserData.fromEncodedData(base, Optional.of(index.getGenerationId())));

    indexer.stageCommitUserData();

    verify((CommitDataStagingWriter) stagingWriter)
        .stageUserData(
            argThat(
                encoded ->
                    IndexCommitUserData.fromEncodedData(encoded, Optional.empty())
                        .autoEmbeddingMetadata()
                        .equals(Optional.of(new AutoEmbeddingCommitMetadata(true)))));
  }

  @Test
  public void testClearIndex() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    MeterRegistry registry = mock(MeterRegistry.class);
    when(registry.counter(any(), any(Iterable.class))).thenReturn(mock(Counter.class));
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);

    IndexCommitUserData userData = IndexCommitUserData.createExceeded("exceeded");
    indexer.clearIndex(userData);
    verify(index).clear(userData.toEncodedData());
    indexer.commit();
    verify(index.getWriter()).commit(userData.toEncodedData());
  }

  @Test
  public void testCommitPersistsAutoEmbeddingMetadataWhenSupplierPresent() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer =
        DefaultDocumentIndexer.create(index, () -> new AutoEmbeddingCommitMetadata(true));
    EncodedUserData base =
        EncodedUserData.fromString("{\"backendIndexVersion\": {\"$numberInt\": \"123\"}}");
    indexer.updateCommitUserData(
        IndexCommitUserData.fromEncodedData(base, Optional.of(index.getGenerationId())));
    indexer.commit();
    verify(index.getWriter())
        .commit(
            argThat(
                encoded ->
                    IndexCommitUserData.fromEncodedData(encoded, Optional.empty())
                        .autoEmbeddingMetadata()
                        .equals(Optional.of(new AutoEmbeddingCommitMetadata(true)))));
  }

  @Test
  public void testCommitPersistsFalseWhenSupplierReturnsFalse() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer =
        DefaultDocumentIndexer.create(index, () -> new AutoEmbeddingCommitMetadata(false));
    EncodedUserData base =
        EncodedUserData.fromString("{\"backendIndexVersion\": {\"$numberInt\": \"123\"}}");
    indexer.updateCommitUserData(
        IndexCommitUserData.fromEncodedData(base, Optional.of(index.getGenerationId())));
    indexer.commit();
    verify(index.getWriter())
        .commit(
            argThat(
                encoded ->
                    IndexCommitUserData.fromEncodedData(encoded, Optional.empty())
                        .autoEmbeddingMetadata()
                        .equals(Optional.of(new AutoEmbeddingCommitMetadata(false)))));
  }

  @Test
  public void testCommitOmitsAutoEmbeddingMetadataWhenNoSupplier() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);
    EncodedUserData base =
        EncodedUserData.fromString("{\"backendIndexVersion\": {\"$numberInt\": \"123\"}}");
    indexer.updateCommitUserData(
        IndexCommitUserData.fromEncodedData(base, Optional.of(index.getGenerationId())));
    indexer.commit();
    verify(index.getWriter())
        .commit(
            argThat(
                encoded ->
                    IndexCommitUserData.fromEncodedData(encoded, Optional.empty())
                        .autoEmbeddingMetadata()
                        .isEmpty()));
  }

  @Test
  public void testCommitWithPreexistingCommitUserData() throws Exception {
    EncodedUserData userData =
        EncodedUserData.fromString("{\"backendIndexVersion\": {\"$numberInt\": \"123\"}}");
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    when(index.getWriter().getCommitUserData()).thenReturn(userData);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);
    indexer.commit();
    verify(index.getWriter()).commit(userData);
  }

  @Test
  public void testCommitReadsSupplierAtCommitTime() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);
    AtomicReference<IndexCommitUserData> live = new AtomicReference<>(commitUserData(index, 1));
    indexer.updateCommitUserDataSupplier(live::get);

    // The whole point of the supplier: partitions keep advancing after it is installed, and the
    // commit has to persist where they are now rather than where they were.
    live.set(commitUserData(index, 2));
    indexer.commit();

    verify(index.getWriter()).commit(encodedUserData(2));
  }

  @Test
  public void testStagedCommitUserDataReplacesSupplier() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);

    indexer.updateCommitUserDataSupplier(() -> commitUserData(index, 1));
    indexer.updateCommitUserData(commitUserData(index, 2));
    indexer.commit();

    verify(index.getWriter()).commit(encodedUserData(2));
  }

  @Test
  public void testSupplierReplacesStagedCommitUserData() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);

    indexer.updateCommitUserData(commitUserData(index, 1));
    indexer.updateCommitUserDataSupplier(() -> commitUserData(index, 2));
    indexer.commit();

    verify(index.getWriter()).commit(encodedUserData(2));
  }

  @Test
  public void testClearIndexReplacesSupplier() throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);
    indexer.updateCommitUserDataSupplier(() -> commitUserData(index, 1));

    // A clear drops the documents the supplied position points past, so it has to win.
    IndexCommitUserData cleared = IndexCommitUserData.createExceeded("exceeded");
    indexer.clearIndex(cleared);
    indexer.commit();

    verify(index.getWriter()).commit(cleared.toEncodedData());
  }

  private static EncodedUserData encodedUserData(int backendIndexVersion) {
    return EncodedUserData.fromString(
        String.format("{\"backendIndexVersion\": {\"$numberInt\": \"%d\"}}", backendIndexVersion));
  }

  private static IndexCommitUserData commitUserData(
      InitializedIndex index, int backendIndexVersion) {
    return IndexCommitUserData.fromEncodedData(
        encodedUserData(backendIndexVersion), Optional.of(index.getGenerationId()));
  }

  private static void testIndexDocumentEvent(DocumentEvent.EventType eventType) throws Exception {
    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);

    switch (eventType) {
      case INSERT:
        indexer.indexDocumentEvent(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(
                    Optional.of(DOCUMENT), index.getDefinition().getIndexId()),
                DOCUMENT));
        break;
      case UPDATE:
        indexer.indexDocumentEvent(
            DocumentEvent.createUpdate(
                DocumentMetadata.fromMetadataNamespace(
                    Optional.of(DOCUMENT), index.getDefinition().getIndexId()),
                DOCUMENT));
        break;
      case DELETE:
        indexer.indexDocumentEvent(DocumentEvent.createDelete(ID));
    }

    verify(index.getWriter())
        .updateIndex(
            argThat(
                docEvent -> {
                  if (docEvent.getEventType() != eventType) {
                    return false;
                  }
                  if (docEvent.getEventType() == DELETE) {
                    return docEvent.getDocumentId() == ID;
                  }
                  return docEvent.getDocument().equals(Optional.of(DOCUMENT));
                }));
  }
}
