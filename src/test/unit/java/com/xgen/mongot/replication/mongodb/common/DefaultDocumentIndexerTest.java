package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.index.DocumentEvent.EventType.DELETE;
import static com.xgen.testing.mongot.mock.index.IndexWriter.mockStagingIndexWriter;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockInitializedIndex;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.CommitDataStagingWriter;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.util.BsonUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.junit.Test;
import org.mockito.InOrder;

public class DefaultDocumentIndexerTest {

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
                        .getAutoEmbeddingMetadata()
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
                        .getAutoEmbeddingMetadata()
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
                        .getAutoEmbeddingMetadata()
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
                        .getAutoEmbeddingMetadata()
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
