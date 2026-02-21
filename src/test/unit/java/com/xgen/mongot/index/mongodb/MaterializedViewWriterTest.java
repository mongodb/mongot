package com.xgen.mongot.index.mongodb;

import static com.xgen.mongot.index.mongodb.MaterializedViewWriter.MV_DATABASE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexClosedException;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.BsonUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MaterializedViewWriterTest {
  private static final MetricsFactory METRICS_FACTORY =
      new MetricsFactory("matviewwritertest", new SimpleMeterRegistry());
  private static final String MV_COLLECTION_NAME = "matviewwritertest";
  private static final MongoNamespace MV_NAMESPACE =
      new MongoNamespace(MV_DATABASE_NAME, MV_COLLECTION_NAME);
  private static final MaterializedViewGenerationId GENERATION_ID =
      new MaterializedViewGenerationId(
          new ObjectId(), new MaterializedViewGeneration(Generation.FIRST));
  private static final UUID COLLECTION_UUID = UUID.randomUUID();

  private MongoClient mockMongoClient;
  private MongoDatabase mockDatabase;
  private MongoCollection mockCollection;
  private LeaseManager mockLeaseManager;

  @Before
  public void setup() {
    this.mockMongoClient = Mockito.mock(MongoClient.class);
    this.mockDatabase = Mockito.mock(MongoDatabase.class);
    this.mockCollection = Mockito.mock(MongoCollection.class);

    this.mockLeaseManager = Mockito.mock(LeaseManager.class);
    when(this.mockDatabase.getCollection(MV_NAMESPACE.getCollectionName(), RawBsonDocument.class))
        .thenReturn(this.mockCollection);
    when(this.mockMongoClient.getDatabase(MV_DATABASE_NAME)).thenReturn(this.mockDatabase);
  }

  @After
  public void reset() {
    Mockito.reset(this.mockMongoClient);
  }

  @Test
  public void testUpdateAndCommit() throws IOException, FieldExceededLimitsException {
    var matViewWriter =
        new MaterializedViewWriter(
            this.mockMongoClient,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID);
    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));
    DocumentEvent insertDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
    DocumentEvent updateDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
    DocumentEvent deleteDocument = DocumentEvent.createDelete(new BsonInt32(1));
    matViewWriter.updateIndex(insertDocument);
    matViewWriter.updateIndex(updateDocument);
    matViewWriter.updateIndex(deleteDocument);

    matViewWriter.commit(EncodedUserData.EMPTY);

    verify(this.mockCollection).bulkWrite(argThat(list -> list.size() == 3));
  }

  @Test
  public void testCommitSingleDocumentExceedingLimitThrowsNonTransientException()
      throws IOException, FieldExceededLimitsException {
    when(this.mockCollection.bulkWrite(any()))
        .thenThrow(new BsonMaximumSizeExceededException("mocked error"));
    var matViewWriter =
        new MaterializedViewWriter(
            this.mockMongoClient,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID);

    Assert.assertThrows(
        MaterializedViewNonTransientException.class, () -> updateAndCommit(1, matViewWriter));
  }

  @Test
  public void testCommitLargeBatchGetsRetriedWithSmallerBatches()
      throws IOException, FieldExceededLimitsException {
    when(this.mockCollection.bulkWrite(any()))
        .thenThrow(new BsonMaximumSizeExceededException("mocked error"))
        .thenReturn(null);
    var matViewWriter =
        new MaterializedViewWriter(
            this.mockMongoClient,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID);

    // Insert two documents
    updateAndCommit(2, matViewWriter);

    // should see two separate bulk writes, each with a single document
    verify(this.mockCollection, times(2)).bulkWrite(argThat(list -> list.size() == 1));
  }

  @Test
  public void testCommitPartialFailureWithNonRetryableErrorThrowsNonTransientException()
      throws IOException, FieldExceededLimitsException {
    // Error code 9 is FailedToParse - not retry-able
    BulkWriteError bulkWriteError = new BulkWriteError(9, "mocked error", new BsonDocument(), 0);
    MongoBulkWriteException bulkWriteException = Mockito.mock(MongoBulkWriteException.class);
    when(bulkWriteException.getWriteErrors()).thenReturn(List.of(bulkWriteError));
    when(this.mockCollection.bulkWrite(any())).thenThrow(bulkWriteException);
    var matViewWriter =
        new MaterializedViewWriter(
            this.mockMongoClient,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID);

    Assert.assertThrows(
        MaterializedViewNonTransientException.class, () -> updateAndCommit(1, matViewWriter));
  }

  @Test
  public void testCommitPartialFailureWithRetryableErrorGetsRetried()
      throws IOException, FieldExceededLimitsException {
    // Error code 6 is HostUnreachable - retry-able
    BulkWriteError bulkWriteError = new BulkWriteError(6, "mocked error", new BsonDocument(), 0);
    MongoBulkWriteException bulkWriteException = Mockito.mock(MongoBulkWriteException.class);
    when(bulkWriteException.getWriteErrors()).thenReturn(List.of(bulkWriteError));
    when(this.mockCollection.bulkWrite(any())).thenThrow(bulkWriteException).thenReturn(null);
    var matViewWriter =
        new MaterializedViewWriter(
            this.mockMongoClient,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID);
    updateAndCommit(1, matViewWriter);

    verify(this.mockCollection, times(2)).bulkWrite(argThat(list -> list.size() == 1));
  }

  @Test
  public void testUpdateClosedIndex() throws IOException {
    var matViewWriter =
        new MaterializedViewWriter(
            this.mockMongoClient,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID);
    matViewWriter.close();
    ObjectId indexId = new ObjectId();
    Assert.assertThrows(
        IndexClosedException.class,
        () -> matViewWriter.updateIndex(createDocumentEvent(indexId, 1)));
  }

  @Test
  public void testCommitClosedIndex() throws IOException {
    var matViewWriter =
        new MaterializedViewWriter(
            this.mockMongoClient,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID);
    matViewWriter.close();
    Assert.assertThrows(
        IndexClosedException.class, () -> matViewWriter.commit(EncodedUserData.EMPTY));
  }

  @Test
  public void testDropMaterializedViewCollection() throws Exception {
    var matViewWriter =
        new MaterializedViewWriter(
            this.mockMongoClient,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID);
    CompletableFuture<Void> future = matViewWriter.dropMaterializedViewCollection();
    future.get();
    verify(this.mockCollection).drop();
  }

  private void updateAndCommit(int numDocs, MaterializedViewWriter matViewWriter)
      throws IOException, FieldExceededLimitsException {
    ObjectId indexId = new ObjectId();
    for (int i = 0; i < numDocs; ++i) {
      matViewWriter.updateIndex(createDocumentEvent(indexId, i));
    }
    matViewWriter.commit(EncodedUserData.EMPTY);
  }

  @Test
  public void testFilterOnlyUpdateUsesUpdateOneModel()
      throws IOException, FieldExceededLimitsException {
    var matViewWriter =
        new MaterializedViewWriter(
            this.mockMongoClient,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID);

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1)))
                .append("filterField", new BsonString("value")));

    // Create a filter-only update event with filterFieldUpdates
    BsonDocument filterFieldUpdates = new BsonDocument("filterField", new BsonString("newValue"));
    DocumentEvent filterOnlyUpdateEvent =
        DocumentEvent.createFilterOnlyUpdate(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId),
            document,
            filterFieldUpdates);

    matViewWriter.updateIndex(filterOnlyUpdateEvent);
    matViewWriter.commit(EncodedUserData.EMPTY);

    // Verify that bulkWrite was called with an UpdateOneModel (not ReplaceOneModel)
    verify(this.mockCollection)
        .bulkWrite(
            argThat(
                list -> {
                  if (list.size() != 1) {
                    return false;
                  }
                  // Check that it's an UpdateOneModel, not ReplaceOneModel
                  return list.get(0) instanceof com.mongodb.client.model.UpdateOneModel;
                }));
  }

  @Test
  public void testRegularUpdateUsesReplaceOneModel()
      throws IOException, FieldExceededLimitsException {
    var matViewWriter =
        new MaterializedViewWriter(
            this.mockMongoClient,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID);

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));

    // Create a regular update event (no filterFieldUpdates)
    DocumentEvent regularUpdateEvent =
        DocumentEvent.createUpdate(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);

    matViewWriter.updateIndex(regularUpdateEvent);
    matViewWriter.commit(EncodedUserData.EMPTY);

    // Verify that bulkWrite was called with a ReplaceOneModel
    verify(this.mockCollection)
        .bulkWrite(
            argThat(
                list -> {
                  if (list.size() != 1) {
                    return false;
                  }
                  // Check that it's a ReplaceOneModel, not UpdateOneModel
                  return list.get(0) instanceof com.mongodb.client.model.ReplaceOneModel;
                }));
  }

  private DocumentEvent createDocumentEvent(ObjectId indexId, int docId) {
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(docId))));
    return DocumentEvent.createInsert(
        DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
  }
}
