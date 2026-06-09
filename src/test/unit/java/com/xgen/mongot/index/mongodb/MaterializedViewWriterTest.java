package com.xgen.mongot.index.mongodb;

import static com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator.createAutoEmbedMapping;
import static com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping;
import static com.xgen.mongot.index.mongodb.MaterializedViewWriter.MV_DATABASE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.RateLimiter;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils;
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
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
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

  private AutoEmbeddingMongoClient autoEmbeddingMongoClient;
  private MongoClient mockMongoClient;
  private MongoDatabase mockDatabase;
  private MongoCollection mockCollection;
  private LeaseManager mockLeaseManager;

  @Before
  public void setup() {
    this.mockMongoClient = mock(MongoClient.class);
    this.autoEmbeddingMongoClient =
        new AutoEmbeddingMongoClient(
            mock(SyncSourceConfig.class),
            this.mockMongoClient,
            this.mockMongoClient,
            this.mockMongoClient,
            new SimpleMeterRegistry());

    this.mockDatabase = mock(MongoDatabase.class);
    this.mockCollection = mock(MongoCollection.class);

    this.mockLeaseManager = mock(LeaseManager.class);
    when(this.mockLeaseManager.getLeaseVersion(GENERATION_ID)).thenReturn(1L);
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
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());
    ObjectId indexId = new ObjectId();
    RawBsonDocument document1 =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));
    RawBsonDocument document2 =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(2))));
    DocumentEvent insertDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document1), indexId), document1);
    DocumentEvent updateDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document2), indexId), document2);
    DocumentEvent deleteDocument = DocumentEvent.createDelete(new BsonInt32(3));
    matViewWriter.updateIndex(insertDocument);
    matViewWriter.updateIndex(updateDocument);
    matViewWriter.updateIndex(deleteDocument);

    matViewWriter.commit(EncodedUserData.EMPTY);

    verify(this.mockCollection)
        .bulkWrite(
            argThat(list -> list.size() == 3), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  @Test
  public void testCommitSingleDocumentExceedingLimitThrowsNonTransientException()
      throws IOException, FieldExceededLimitsException {
    when(this.mockCollection.bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS)))
        .thenThrow(new BsonMaximumSizeExceededException("mocked error"));
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());

    Assert.assertThrows(
        MaterializedViewNonTransientException.class, () -> updateAndCommit(1, matViewWriter));
  }

  @Test
  public void testCommitLargeBatchGetsRetriedWithSmallerBatches()
      throws IOException, FieldExceededLimitsException {
    when(this.mockCollection.bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS)))
        .thenThrow(new BsonMaximumSizeExceededException("mocked error"))
        .thenReturn(null);
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());

    // Insert two documents
    updateAndCommit(2, matViewWriter);

    // should see two separate bulk writes, each with a single document
    verify(this.mockCollection, times(2))
        .bulkWrite(
            argThat(list -> list.size() == 1), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  @Test
  public void testCommitPartialFailureWithNonRetryableErrorThrowsNonTransientException()
      throws IOException, FieldExceededLimitsException {
    // Error code 9 is FailedToParse - not retry-able
    BulkWriteError bulkWriteError = new BulkWriteError(9, "mocked error", new BsonDocument(), 0);
    MongoBulkWriteException bulkWriteException = mock(MongoBulkWriteException.class);
    when(bulkWriteException.getWriteErrors()).thenReturn(List.of(bulkWriteError));
    when(this.mockCollection.bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS)))
        .thenThrow(bulkWriteException);
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());

    Assert.assertThrows(
        MaterializedViewNonTransientException.class, () -> updateAndCommit(1, matViewWriter));
  }

  @Test
  public void testCommitPartialFailureWithRetryableErrorGetsRetried()
      throws IOException, FieldExceededLimitsException {
    // Error code 6 is HostUnreachable - retry-able
    BulkWriteError bulkWriteError = new BulkWriteError(6, "mocked error", new BsonDocument(), 0);
    MongoBulkWriteException bulkWriteException = mock(MongoBulkWriteException.class);
    when(bulkWriteException.getWriteErrors()).thenReturn(List.of(bulkWriteError));
    when(this.mockCollection.bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS)))
        .thenThrow(bulkWriteException)
        .thenReturn(null);
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());
    updateAndCommit(1, matViewWriter);

    verify(this.mockCollection, times(2))
        .bulkWrite(
            argThat(list -> list.size() == 1), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  @Test
  public void testCommitBulkWriteExceptionWithNoWriteErrorsRetriesFullBatch()
      throws IOException, FieldExceededLimitsException {
    MongoBulkWriteException bulkWriteException = mock(MongoBulkWriteException.class);
    when(bulkWriteException.getWriteErrors()).thenReturn(List.of());
    when(this.mockCollection.bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS)))
        .thenThrow(bulkWriteException)
        .thenReturn(null);
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());

    updateAndCommit(2, matViewWriter);

    verify(this.mockCollection, times(2))
        .bulkWrite(
            argThat(list -> list.size() == 2), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  @Test
  public void testUpdateClosedIndex() throws IOException {
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());
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
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());
    matViewWriter.close();
    Assert.assertThrows(
        IndexClosedException.class, () -> matViewWriter.commit(EncodedUserData.EMPTY));
  }

  @Test
  public void testDropMaterializedViewCollection() throws Exception {
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());
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
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1)))
                .append("filterField", new BsonString("value")));

    // Create a filter-only update event with filterFieldUpdates
    BsonDocument filterFieldUpdates = new BsonDocument("filterField", new BsonString("newValue"));
    DocumentEvent filterOnlyUpdateEvent =
        DocumentEvent.createPartialUpdate(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId),
            document,
            filterFieldUpdates,
            com.google.common.collect.ImmutableMap.of());

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
                }),
            same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  @Test
  public void testRegularUpdateUsesReplaceOneModel()
      throws IOException, FieldExceededLimitsException {
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());

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
                }),
            same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  @Test
  public void testCommitWithoutRateLimiter_proceedsNormally()
      throws IOException, FieldExceededLimitsException {
    var writer =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());
    updateAndCommit(1, writer);
    verify(this.mockCollection)
        .bulkWrite(
            argThat(list -> list.size() == 1), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  @Test
  public void testCommitWithRateLimiter_permitsAvailable_proceedsWithoutDelay()
      throws IOException, FieldExceededLimitsException {
    RateLimiter limiter = mock(RateLimiter.class);
    when(limiter.acquire()).thenReturn(0.0);
    var writer =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.of(limiter));
    updateAndCommit(1, writer);
    verify(limiter).acquire();
    verify(this.mockCollection)
        .bulkWrite(
            argThat(list -> list.size() == 1), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  @Test
  public void testCommitWithRateLimiter_throttlesWhenRateExceeded()
      throws IOException, FieldExceededLimitsException {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("throttleTest", registry);
    RateLimiter limiter = mock(RateLimiter.class);
    when(limiter.acquire()).thenReturn(0.0).thenReturn(0.5);
    var writer =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            metricsFactory,
            COLLECTION_UUID,
            Optional.of(limiter));
    updateAndCommit(1, writer);
    updateAndCommit(1, writer);
    verify(limiter, times(2)).acquire();
    Counter throttleCount = registry.find("throttleTest.mvWriteThrottleCount").counter();
    Assert.assertNotNull(throttleCount);
    Assert.assertEquals("Second commit should be throttled", 1, (int) throttleCount.count());
  }

  @Test
  public void testCommitEmptyBuffer_skipsRateLimiter() throws IOException {
    RateLimiter limiter = mock(RateLimiter.class);
    var writer =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.of(limiter));
    writer.commit(EncodedUserData.EMPTY);
    Mockito.verify(limiter, Mockito.never()).acquire();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFactory_sharedRateLimiterAndNoRateLimiter() throws Exception {
    RateLimiter sharedLimiter = RateLimiter.create(50);
    var writer1 =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            "col1",
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            UUID.randomUUID(),
            Optional.of(sharedLimiter));
    var writer2 =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            "col2",
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            UUID.randomUUID(),
            Optional.of(sharedLimiter));

    java.lang.reflect.Field rlField = MaterializedViewWriter.class.getDeclaredField("rateLimiter");
    rlField.setAccessible(true);
    Optional<RateLimiter> rl1 = (Optional<RateLimiter>) rlField.get(writer1);
    Optional<RateLimiter> rl2 = (Optional<RateLimiter>) rlField.get(writer2);
    Assert.assertTrue("Writer1 should have a rate limiter", rl1.isPresent());
    Assert.assertTrue("Writer2 should have a rate limiter", rl2.isPresent());
    Assert.assertSame(
        "Both writers should share the same RateLimiter instance", rl1.get(), rl2.get());

    var writer3 =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            "col3",
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            UUID.randomUUID(),
            Optional.empty());
    Optional<RateLimiter> rl3 = (Optional<RateLimiter>) rlField.get(writer3);
    Assert.assertFalse("Writer should not have a rate limiter", rl3.isPresent());
  }

  @Test
  public void testRateLimiterMetrics_throttleCountAndWaitTime()
      throws IOException, FieldExceededLimitsException {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("testMetrics", registry);
    RateLimiter limiter = mock(RateLimiter.class);
    when(limiter.acquire()).thenReturn(0.0).thenReturn(0.5);
    var writer =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            metricsFactory,
            COLLECTION_UUID,
            Optional.of(limiter));

    Counter throttleCount = registry.find("testMetrics.mvWriteThrottleCount").counter();
    Timer throttleWaitTime = registry.find("testMetrics.mvWriteThrottleWaitTime").timer();
    Assert.assertNotNull("Throttle count metric should be registered", throttleCount);
    Assert.assertNotNull("Throttle wait time metric should be registered", throttleWaitTime);

    updateAndCommit(1, writer);
    Assert.assertEquals("First commit should not be throttled", 0, (int) throttleCount.count());
    Assert.assertEquals("No wait time recorded for first commit", 0, throttleWaitTime.count());

    updateAndCommit(1, writer);
    Assert.assertEquals("Second commit should be throttled", 1, (int) throttleCount.count());
    Assert.assertEquals("Wait time should be recorded once", 1, throttleWaitTime.count());
    Assert.assertEquals(
        "Total wait time should reflect acquire() return value",
        500.0,
        throttleWaitTime.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS),
        50.0);
  }

  // ==================== addFencingToWriteModels unit tests ====================

  @Test
  public void testAddFencing_replaceModel_addsLeaseVersionAndFencingFilter() {
    BsonDocument originalDoc =
        new BsonDocument("_id", new BsonInt32(1)).append("data", new BsonString("hello"));
    RawBsonDocument rawDoc = BsonUtils.documentToRaw(originalDoc);
    ReplaceOneModel<RawBsonDocument> replaceModel =
        new ReplaceOneModel<>(
            new BsonDocument("_id", new BsonInt32(1)), rawDoc, new ReplaceOptions().upsert(true));

    List<WriteModel<RawBsonDocument>> result =
        MaterializedViewWriter.addFencingToWriteModels(List.of(replaceModel), 42L);

    Assert.assertEquals("Should produce same number of models", 1, result.size());
    Assert.assertTrue("Should be a ReplaceOneModel", result.get(0) instanceof ReplaceOneModel);

    ReplaceOneModel<RawBsonDocument> fencedModel = (ReplaceOneModel<RawBsonDocument>) result.get(0);

    // Verify _autoEmbed._leaseVersion was added as nested doc {_autoEmbed: {_leaseVersion: 42}}
    BsonDocument fencedDoc = fencedModel.getReplacement().toBsonDocument();
    Assert.assertTrue(
        "Replacement doc should contain _autoEmbed", fencedDoc.containsKey("_autoEmbed"));
    Assert.assertEquals(
        42L, fencedDoc.getDocument("_autoEmbed").getInt64("_leaseVersion").getValue());

    // Verify original data is preserved
    Assert.assertEquals("hello", fencedDoc.getString("data").getValue());

    // Verify fencing filter is present (rendered to BsonDocument for inspection)
    BsonDocument filterDoc = fencedModel.getFilter().toBsonDocument();
    Assert.assertTrue("Filter should contain $and", filterDoc.containsKey("$and"));

    // Verify original RawBsonDocument was not mutated
    BsonDocument afterDoc = rawDoc.toBsonDocument();
    Assert.assertFalse(
        "Original document should not be modified", afterDoc.containsKey("_autoEmbed"));
  }

  @Test
  public void testAddFencing_updateModel_addsLeaseVersionToSetAndFencingFilter() {
    BsonDocument setFields = new BsonDocument("filterField", new BsonString("newValue"));
    BsonDocument update = new BsonDocument("$set", setFields);
    UpdateOneModel<RawBsonDocument> updateModel =
        new UpdateOneModel<>(new BsonDocument("_id", new BsonInt32(2)), update);

    List<WriteModel<RawBsonDocument>> result =
        MaterializedViewWriter.addFencingToWriteModels(List.of(updateModel), 99L);

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(0) instanceof UpdateOneModel);

    UpdateOneModel<RawBsonDocument> fencedModel = (UpdateOneModel<RawBsonDocument>) result.get(0);

    // Verify __leaseVersion was added to $set
    BsonDocument fencedUpdate = fencedModel.getUpdate().toBsonDocument();
    BsonDocument fencedSet = fencedUpdate.getDocument("$set");
    Assert.assertTrue(
        "$set should contain __leaseVersion",
        fencedSet.containsKey(MaterializedViewWriter.LEASE_VERSION_FIELD));
    Assert.assertEquals(
        99L, fencedSet.getInt64(MaterializedViewWriter.LEASE_VERSION_FIELD).getValue());

    // Verify original $set fields are preserved
    Assert.assertEquals("newValue", fencedSet.getString("filterField").getValue());

    // Verify fencing filter
    BsonDocument filterDoc = fencedModel.getFilter().toBsonDocument();
    Assert.assertTrue("Filter should contain $and", filterDoc.containsKey("$and"));
  }

  @Test
  public void testAddFencing_deleteModel_addsFencingFilter() {
    DeleteOneModel<RawBsonDocument> deleteModel =
        new DeleteOneModel<>(new BsonDocument("_id", new BsonInt32(3)));

    List<WriteModel<RawBsonDocument>> result =
        MaterializedViewWriter.addFencingToWriteModels(List.of(deleteModel), 10L);

    Assert.assertEquals(1, result.size());
    Assert.assertTrue("Should be a DeleteOneModel", result.get(0) instanceof DeleteOneModel);

    // Should be a new instance with fencing filter, not the original
    Assert.assertNotSame(
        "DeleteOneModel should be replaced with fenced version", deleteModel, result.get(0));

    // Verify fencing filter is present
    DeleteOneModel<RawBsonDocument> fencedModel = (DeleteOneModel<RawBsonDocument>) result.get(0);
    BsonDocument filterDoc = fencedModel.getFilter().toBsonDocument();
    Assert.assertTrue("Filter should contain $and", filterDoc.containsKey("$and"));
  }

  @Test
  public void testAddFencing_preservesListOrderAndSize() {
    List<WriteModel<RawBsonDocument>> models =
        List.of(
            new ReplaceOneModel<>(
                new BsonDocument("_id", new BsonInt32(1)),
                BsonUtils.documentToRaw(
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(1))),
                new ReplaceOptions().upsert(true)),
            new DeleteOneModel<>(new BsonDocument("_id", new BsonInt32(99))),
            new UpdateOneModel<>(
                new BsonDocument("_id", new BsonInt32(2)),
                new BsonDocument("$set", new BsonDocument("f", new BsonString("v")))));

    List<WriteModel<RawBsonDocument>> result =
        MaterializedViewWriter.addFencingToWriteModels(models, 5L);

    Assert.assertEquals("Must preserve list size (1:1 invariant)", 3, result.size());
    Assert.assertTrue(
        "Index 0 should be ReplaceOneModel", result.get(0) instanceof ReplaceOneModel);
    Assert.assertTrue("Index 1 should be DeleteOneModel", result.get(1) instanceof DeleteOneModel);
    Assert.assertTrue("Index 2 should be UpdateOneModel", result.get(2) instanceof UpdateOneModel);
  }

  @Test
  public void testAddFencing_skipsWhenLeaseVersionIsMaxValue() {
    BsonDocument doc = new BsonDocument("_id", new BsonInt32(1));
    ReplaceOneModel<RawBsonDocument> model =
        new ReplaceOneModel<>(
            new BsonDocument("_id", new BsonInt32(1)),
            BsonUtils.documentToRaw(doc),
            new ReplaceOptions().upsert(true));

    List<WriteModel<RawBsonDocument>> input = List.of(model);
    List<WriteModel<RawBsonDocument>> result =
        MaterializedViewWriter.addFencingToWriteModels(input, Long.MAX_VALUE);

    Assert.assertSame(
        "Should return original list unchanged for static leader sentinel", input, result);
  }

  @Test
  public void testCommit_negativeLeaseVersion_throwsNonTransientException()
      throws IOException, FieldExceededLimitsException {
    when(this.mockLeaseManager.getLeaseVersion(GENERATION_ID)).thenReturn(-1L);
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());

    Assert.assertThrows(
        MaterializedViewNonTransientException.class, () -> updateAndCommit(1, matViewWriter));

    verify(this.mockCollection, times(0))
        .bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  // ==================== Fencing + partial-failure retry tests ====================

  @Test
  public void testCommitFencingRejection_dupKey11000_throwsNonTransientException()
      throws IOException, FieldExceededLimitsException {
    // Error code 11000 is DuplicateKey — this is the fencing signal when a fenced upsert's
    // filter doesn't match (doc has higher __leaseVersion) and the upsert collides on _id.
    BulkWriteError bulkWriteError =
        new BulkWriteError(11000, "duplicate key error", new BsonDocument(), 0);
    MongoBulkWriteException bulkWriteException = Mockito.mock(MongoBulkWriteException.class);
    when(bulkWriteException.getWriteErrors()).thenReturn(List.of(bulkWriteError));
    when(this.mockCollection.bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS)))
        .thenThrow(bulkWriteException);
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());

    Assert.assertThrows(
        MaterializedViewNonTransientException.class, () -> updateAndCommit(1, matViewWriter));

    // Should not retry — only one bulkWrite call
    verify(this.mockCollection, times(1))
        .bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  @Test
  public void testCommitPartialFailureRetry_retriesOnlyErroredOps()
      throws IOException, FieldExceededLimitsException {
    BulkWriteError bulkWriteError = new BulkWriteError(6, "mocked error", new BsonDocument(), 1);
    MongoBulkWriteException bulkWriteException = Mockito.mock(MongoBulkWriteException.class);
    when(bulkWriteException.getWriteErrors()).thenReturn(List.of(bulkWriteError));
    when(this.mockCollection.bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS)))
        .thenThrow(bulkWriteException)
        .thenReturn(null);
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());

    updateAndCommit(3, matViewWriter);

    verify(this.mockCollection)
        .bulkWrite(
            argThat(list -> list.size() == 3), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
    verify(this.mockCollection)
        .bulkWrite(
            argThat(list -> list.size() == 1), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  @Test
  public void testCommitFencingRejection_prioritizedOverRetryableErrors()
      throws IOException, FieldExceededLimitsException {
    // Defensive test: if errors contain both a fencing dup key and a retryable error, fencing wins.
    BulkWriteError retryableError =
        new BulkWriteError(6, "host unreachable", new BsonDocument(), 0);
    BulkWriteError fencingError = new BulkWriteError(11000, "duplicate key", new BsonDocument(), 1);
    MongoBulkWriteException bulkWriteException = Mockito.mock(MongoBulkWriteException.class);
    when(bulkWriteException.getWriteErrors()).thenReturn(List.of(retryableError, fencingError));
    when(this.mockCollection.bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS)))
        .thenThrow(bulkWriteException);
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());

    Assert.assertThrows(
        MaterializedViewNonTransientException.class, () -> updateAndCommit(2, matViewWriter));

    // Should not retry — fencing rejection takes priority
    verify(this.mockCollection, times(1))
        .bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  @Test
  public void testTenantPrefixedDatabaseName_usesCorrectNamespace()
      throws IOException, FieldExceededLimitsException {
    String tenantDb = "acme___mdb_internal_search";
    MongoDatabase tenantMockDatabase = mock(MongoDatabase.class);
    when(this.mockMongoClient.getDatabase(tenantDb)).thenReturn(tenantMockDatabase);
    when(tenantMockDatabase.getCollection(MV_COLLECTION_NAME, RawBsonDocument.class))
        .thenReturn(this.mockCollection);

    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            tenantDb,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());

    Assert.assertEquals(
        new MongoNamespace(tenantDb, MV_COLLECTION_NAME), matViewWriter.getNamespace());

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));
    matViewWriter.updateIndex(
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document));
    matViewWriter.commit(EncodedUserData.EMPTY);

    verify(this.mockMongoClient).getDatabase(tenantDb);
    verify(this.mockCollection)
        .bulkWrite(
            argThat(list -> list.size() == 1), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS));
  }

  /**
   * Enforcer test: verifies that MV documents produced by fencing do not cause spurious re-indexing
   * in {@link com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils#compareDocuments}. This
   * test automatically catches regressions when new metadata fields are added to the writer but not
   * registered in {@link
   * com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils#MV_METADATA_FIELDS}.
   */
  @Test
  public void testFencedDocumentDoesNotTriggerSpuriousReIndexing() throws java.io.IOException {
    // Build a MV document via the normal auto-embedding path.
    // Use schema version 1 so embeddings are stored under _autoEmbed.* (e.g., _autoEmbed.a,
    // _autoEmbed._hash.a). This causes compareDocuments to traverse the _autoEmbed subtree and
    // encounter _autoEmbed._leaseVersion as an "extra field". With version 0, embeddings are
    // stored as top-level fields and _autoEmbed is not traversed, so the test would pass vacuously
    // even without the MV_METADATA_FIELDS exclusion.
    var vectorIndexDefinition =
        com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("a")
            .withAutoEmbedField("b")
            .withFilterPath("color")
            .build();
    var schemaMetadata =
        new com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata
            .MaterializedViewSchemaMetadata(
            1,
            java.util.Map.of(
                com.xgen.mongot.util.FieldPath.parse("a"),
                com.xgen.mongot.util.FieldPath.parse("_autoEmbed.a"),
                com.xgen.mongot.util.FieldPath.parse("b"),
                com.xgen.mongot.util.FieldPath.parse("_autoEmbed.b")));

    com.xgen.mongot.util.bson.Vector vector1 =
        com.xgen.mongot.util.bson.Vector.fromFloats(
            new float[] {1.0f, 2.0f}, com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE);
    com.xgen.mongot.util.bson.Vector vector2 =
        com.xgen.mongot.util.bson.Vector.fromFloats(
            new float[] {5.0f, 6.0f}, com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE);

    com.google.common.collect.ImmutableMap<String, com.xgen.mongot.util.bson.Vector> embeddings =
        com.google.common.collect.ImmutableMap.of("aString", vector1, "bString", vector2);

    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("a", new BsonString("aString"))
            .append("b", new BsonString("bString"))
            .append("color", new BsonString("red"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    com.google.common.collect.ImmutableMap.Builder<
            com.xgen.mongot.util.FieldPath,
            com.google.common.collect.ImmutableMap<String, com.xgen.mongot.util.bson.Vector>>
        embeddingsPerFieldBuilder = com.google.common.collect.ImmutableMap.builder();
    for (com.xgen.mongot.util.FieldPath fieldPath :
        vectorIndexDefinition.getMappings().fieldMap().keySet()) {
      embeddingsPerFieldBuilder.put(fieldPath, embeddings);
    }

    DocumentEvent mvDocEvent =
        com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils
            .buildMaterializedViewDocumentEvent(
                rawDocumentEvent,
                createAutoEmbedMapping(vectorIndexDefinition),
                embeddingsPerFieldBuilder.build(),
                schemaMetadata);

    // Run fencing — this injects _autoEmbed._leaseVersion (and potentially future metadata).
    RawBsonDocument mvRawDoc = mvDocEvent.getDocument().get();
    var replaceModel =
        new ReplaceOneModel<>(
            new BsonDocument("_id", new BsonString("anId")),
            mvRawDoc,
            new ReplaceOptions().upsert(true));

    List<WriteModel<RawBsonDocument>> fencedModels =
        MaterializedViewWriter.addFencingToWriteModels(List.of(replaceModel), 42L);

    ReplaceOneModel<RawBsonDocument> fencedModel =
        (ReplaceOneModel<RawBsonDocument>) fencedModels.get(0);
    RawBsonDocument fencedDoc = fencedModel.getReplacement();

    // Now compareDocuments against the fenced MV doc — it should NOT trigger re-indexing.
    var comparisonResult =
        AutoEmbeddingDocumentUtils.compareDocuments(
            rawDocumentEvent.getDocument().get(),
            fencedDoc,
            createAutoEmbedMapping(vectorIndexDefinition),
            createMatViewAutoEmbedMapping(vectorIndexDefinition, schemaMetadata),
            schemaMetadata);

    Assert.assertFalse(
        "Fenced MV document should not trigger spurious re-indexing. "
            + "If this fails, a metadata field added by MaterializedViewWriter is not registered "
            + "in AutoEmbeddingDocumentUtils.MV_METADATA_FIELDS.",
        comparisonResult.needsReIndexing());
    Assert.assertEquals(2, comparisonResult.reusableEmbeddings().size());
  }

  @Test
  public void testCommitWithInvalidLeaseThrowsNonTransientWithInvalidLeaseReason() {
    when(this.mockLeaseManager.getLeaseVersion(GENERATION_ID)).thenReturn(0L);
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());
    MaterializedViewNonTransientException ex =
        Assert.assertThrows(
            MaterializedViewNonTransientException.class, () -> updateAndCommit(1, matViewWriter));
    Assert.assertEquals(MaterializedViewNonTransientException.Reason.INVALID_LEASE, ex.getReason());
  }

  @Test
  public void testCommitSingleDocExceedingLimitHasDocumentTooLargeReason() {
    when(this.mockCollection.bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS)))
        .thenThrow(new BsonMaximumSizeExceededException("mocked error"));
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());
    MaterializedViewNonTransientException ex =
        Assert.assertThrows(
            MaterializedViewNonTransientException.class, () -> updateAndCommit(1, matViewWriter));
    Assert.assertEquals(
        MaterializedViewNonTransientException.Reason.DOCUMENT_TOO_LARGE, ex.getReason());
  }

  @Test
  public void testCommitNonRetryableErrorHasNonRetryableErrorReason() {
    // Error code 9 is FailedToParse - not retryable
    BulkWriteError bulkWriteError = new BulkWriteError(9, "mocked error", new BsonDocument(), 0);
    MongoBulkWriteException bulkWriteException = mock(MongoBulkWriteException.class);
    when(bulkWriteException.getWriteErrors()).thenReturn(List.of(bulkWriteError));
    when(this.mockCollection.bulkWrite(any(), same(MaterializedViewWriter.MV_BULK_WRITE_OPTIONS)))
        .thenThrow(bulkWriteException);
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());
    MaterializedViewNonTransientException ex =
        Assert.assertThrows(
            MaterializedViewNonTransientException.class, () -> updateAndCommit(1, matViewWriter));
    Assert.assertEquals(
        MaterializedViewNonTransientException.Reason.NON_RETRYABLE_ERROR, ex.getReason());
  }

  @Test
  public void testGetCommitUserDataThrowsTransientWithReadLeaseFailedReason() throws IOException {
    when(this.mockLeaseManager.getCommitInfo(GENERATION_ID))
        .thenThrow(new IOException("mocked lease read failure"));
    var matViewWriter =
        new MaterializedViewWriter(
            this.autoEmbeddingMongoClient,
            MV_DATABASE_NAME,
            MV_COLLECTION_NAME,
            GENERATION_ID,
            this.mockLeaseManager,
            METRICS_FACTORY,
            COLLECTION_UUID,
            Optional.empty());
    MaterializedViewTransientException ex =
        Assert.assertThrows(
            MaterializedViewTransientException.class, matViewWriter::getCommitUserData);
    Assert.assertEquals(
        MaterializedViewTransientException.Reason.READ_LEASE_FAILED, ex.getReason());
  }

  private DocumentEvent createDocumentEvent(ObjectId indexId, int docId) {
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(docId))));
    return DocumentEvent.createInsert(
        DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
  }
}
