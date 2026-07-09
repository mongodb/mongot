package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.index.DocumentEvent.EventType.DELETE;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockInitializedIndex;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.util.BsonUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.junit.Test;

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
