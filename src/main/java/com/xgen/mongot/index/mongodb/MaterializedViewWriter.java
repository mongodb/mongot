package com.xgen.mongot.index.mongodb;

import static com.xgen.mongot.embedding.utils.EmbeddingConnectionStringUtils.disableDirectConnection;

import com.google.common.util.concurrent.RateLimiter;
import com.google.errorprone.annotations.Var;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.utils.MongoClientOperationExecutor;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexClosedException;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.WriterClosedException;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.concurrent.LockGuard;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.mongot.util.mongodb.MongoClientBuilder;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.bson.BsonDocument;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer for the auto embedding index mat view. The primary differences between this and the Lucene
 * based writer are described below:
 *
 * <ul>
 *   <li>1. This writer writes to a MongoDB collection and not a Lucene index
 *   <li>2. This writer uses an in-memory buffer to batch writes and only flushes to MongoDB on a
 *       commit as opposed to every write as that would be inefficient. We expect to, however,
 *       commit more frequently than the Lucene writer.
 * </ul>
 */
public class MaterializedViewWriter implements IndexWriter {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewWriter.class);

  // TODO(CLOUDP-356242): make this configurable
  public static final String MV_DATABASE_NAME = "__mdb_internal_search";

  private static final int MAX_SUB_RETRY_ATTEMPTS = 3;

  private final MongoClient mongoClient;
  private final MongoNamespace namespace;
  private final AtomicReference<ConcurrentLinkedQueue<WriteModel<RawBsonDocument>>>
      bulkOperationsRef;
  private final MongoClientOperationExecutor operationExecutor;
  private final UUID collectionUuid;
  private final MaterializedViewGenerationId generationId;
  private final LeaseManager leaseManager;
  private final Optional<RateLimiter> rateLimiter;

  // Metrics
  private final Counter payloadTooLargeErrors;
  private final Counter partialBulkWriteErrors;
  private final Counter mvWriteThrottleCount;
  private final Timer mvWriteThrottleWaitTime;
  private final DistributionSummary bulkWriteNumDocs;
  private final DistributionSummary bulkWritePayloadSize;

  /**
   * A pair of read and write locks which are used to synchronize access to the materialized view
   * collection and the in-memory buffer. The read lock allows concurrent access to the in-memory
   * buffer, but the write lock ensures that the buffer is flushed atomically without losing any
   * updates. The write lock is also used to coordinate the close/shutdown of the writer.
   */
  private final ReentrantReadWriteLock.WriteLock shutdownAndCommitExclusiveLock;

  private final ReentrantReadWriteLock.ReadLock shutdownAndCommitSharedLock;

  /**
   * Whether the writer is closed. You need to hold the above-mentioned read lock to read the value
   * and the write lock to update the value.
   */
  private boolean closed;

  public MaterializedViewWriter(
      MongoClient mongoClient,
      String matViewName,
      MaterializedViewGenerationId generationId,
      LeaseManager leaseManager,
      MetricsFactory metricsFactory,
      UUID collectionUuid,
      Optional<RateLimiter> rateLimiter) {
    this.mongoClient = mongoClient;
    this.namespace = new MongoNamespace(MV_DATABASE_NAME, matViewName);
    this.generationId = generationId;
    this.leaseManager = leaseManager;
    this.bulkOperationsRef = new AtomicReference<>(new ConcurrentLinkedQueue<>());
    this.payloadTooLargeErrors = metricsFactory.counter("payloadTooLargeErrors");
    this.partialBulkWriteErrors = metricsFactory.counter("partialBulkWriteErrors");
    this.mvWriteThrottleCount = metricsFactory.counter("mvWriteThrottleCount");
    this.mvWriteThrottleWaitTime = metricsFactory.timer("mvWriteThrottleWaitTime");
    this.bulkWriteNumDocs = metricsFactory.summary("bulkWriteNumDocs");
    this.bulkWritePayloadSize = metricsFactory.summary("bulkWritePayloadSize");
    this.operationExecutor =
        new MongoClientOperationExecutor(metricsFactory, "materializedViewCollection");
    this.collectionUuid = collectionUuid;
    this.rateLimiter = rateLimiter;
    ReentrantReadWriteLock shutdownLock = new ReentrantReadWriteLock(true);
    this.shutdownAndCommitExclusiveLock = shutdownLock.writeLock();
    this.shutdownAndCommitSharedLock = shutdownLock.readLock();
    this.closed = false;
  }

  @Override
  public void updateIndex(DocumentEvent event) throws IOException, FieldExceededLimitsException {
    try (LockGuard ignored = LockGuard.with(this.shutdownAndCommitSharedLock)) {
      ensureOpen("updateIndex");
      var bulkOperations = this.bulkOperationsRef.get();
      switch (event.getEventType()) {
        case INSERT ->
            bulkOperations.add(
                new ReplaceOneModel<>(
                    new BsonDocument("_id", event.getDocumentId()),
                    event.getDocument().get(), // only returns empty when event is a delete
                    new ReplaceOptions().upsert(true)));
        case UPDATE -> {
          // For filter-only updates, use $set to update only the filter fields.
          // This preserves existing embeddings in the materialized view document.
          if (event.getFilterFieldUpdates().isPresent()) {
            bulkOperations.add(
                new UpdateOneModel<>(
                    new BsonDocument("_id", event.getDocumentId()),
                    new BsonDocument("$set", event.getFilterFieldUpdates().get())));
          } else {
            bulkOperations.add(
                new ReplaceOneModel<>(
                    new BsonDocument("_id", event.getDocumentId()),
                    event.getDocument().get(), // only returns empty when event is a delete
                    new ReplaceOptions().upsert(true)));
          }
        }
        case DELETE ->
            bulkOperations.add(
                new DeleteOneModel<>(new BsonDocument("_id", event.getDocumentId())));
      }
    }
  }

  @Override
  public void commit(EncodedUserData userData) throws IOException {
    ConcurrentLinkedQueue<WriteModel<RawBsonDocument>> bulkOperations;
    try (LockGuard ignored = LockGuard.with(this.shutdownAndCommitExclusiveLock)) {
      ensureOpen("commit");
      bulkOperations = this.bulkOperationsRef.getAndSet(new ConcurrentLinkedQueue<>());
    }
    if (!bulkOperations.isEmpty()) {
      // Throttle MV writes if ratelimiter is configured. acquire() blocks until
      // a permit is available, smoothing write bursts.
      this.rateLimiter.ifPresent(
          limiter -> {
            double waitSeconds = limiter.acquire();
            if (waitSeconds > 0) {
              this.mvWriteThrottleCount.increment();
              this.mvWriteThrottleWaitTime.record(
                  (long) (waitSeconds * 1000), TimeUnit.MILLISECONDS);
            }
          });

      // TODO(CLOUDP-360778): if commit throws an exception, we currently crash the JVM (see
      // PeriodicIndexCommitter::crashWithException). We will likely need to implement our own
      // periodic committer or not have a periodic committer at all since we call commit from the
      // indexing work scheduler directly.
      try {
        bulkWrite(bulkOperations.stream().toList(), 0);
      } catch (MaterializedViewNonTransientException e) {
        throw e;
      } catch (Exception e) {
        throw new MaterializedViewTransientException(e);
      }
    }
    try {
      this.leaseManager.updateCommitInfo(this.generationId, userData);
    } catch (MaterializedViewTransientException | MaterializedViewNonTransientException ex) {
      LOG.atWarn()
          .addKeyValue("generationId", this.generationId)
          .setCause(ex)
          .log("Fails to commit lease documents");
      throw ex;
    }
  }

  @Override
  public EncodedUserData getCommitUserData() {
    try {
      return this.leaseManager.getCommitInfo(this.generationId);
    } catch (IOException e) {
      // TODO(CLOUDP-360778): Throwing an exception while retrieving the checkpoint can currently
      // crash the JVM (see ReplicationIndexManager::create)
      throw new MaterializedViewTransientException(e);
    }
  }

  @Override
  public Optional<ExceededLimitsException> exceededLimits() {
    return Optional.empty();
  }

  @Override
  public int getNumFields() throws WriterClosedException {
    return 0;
  }

  @Override
  public void deleteAll(EncodedUserData userData) throws IOException {}

  @Override
  public void close() {
    try (LockGuard ignored = LockGuard.with(this.shutdownAndCommitExclusiveLock)) {
      if (this.closed) {
        return;
      }
      this.closed = true;
    }
  }

  @Override
  public long getNumDocs() throws WriterClosedException {
    return 0;
  }

  public UUID getCollectionUuid() {
    return this.collectionUuid;
  }

  public MongoClient getMongoClient() {
    return this.mongoClient;
  }

  public MongoNamespace getNamespace() {
    return this.namespace;
  }

  /**
   * Drop the materialized view collection. This is a temporary solution for garbage collection and
   * is called from MaterializedViewManager when we detect an index deletion event.
   *
   * @return a future that completes when the materialized view collection has been dropped.
   */
  public CompletableFuture<Void> dropMaterializedViewCollection() {
    this.close();
    return CompletableFuture.runAsync(
        () -> {
          this.mongoClient
              .getDatabase(this.namespace.getDatabaseName())
              .getCollection(this.namespace.getCollectionName(), RawBsonDocument.class)
              .drop();
        });
  }

  public static class Factory implements Closeable {
    // TODO(CLOUDP-360606): make it configurable. Use 2 for now, as we follows the formula used in
    // MongoDbReplicationManager::getSyncMongoClient but without counting connections for synonym,
    // sessionRefresh and changeStreamModeSelection as this client is for stateless writing only, we
    // will adjust it later.
    private static final int DEFAULT_MAX_CONNECTIONS = 2;
    private final MongoClient materializedViewMongoClient;
    private final MetricsFactory metricsFactory;
    private final Optional<RateLimiter> rateLimiter;

    public Factory(
        SyncSourceConfig syncSourceConfig,
        MeterRegistry meterRegistry,
        Optional<Integer> mvWriteRateLimitRps) {
      // TODO(CLOUDP-360542): Investigate whether we need to change this when primary mongod node is
      // not discoverable in original seedAddresses
      this.materializedViewMongoClient =
          createMaterializedViewMongoClient(syncSourceConfig, meterRegistry);
      this.metricsFactory = new MetricsFactory("materializedViewWriter", meterRegistry);
      this.rateLimiter =
          mvWriteRateLimitRps.map(
              rps -> {
                LOG.info("MV write rate limiter configured at {} RPS", rps);
                return RateLimiter.create(rps);
              });
    }

    public MaterializedViewWriter create(
        String matViewColName,
        MaterializedViewGenerationId generationId,
        LeaseManager leaseManager,
        UUID collectionUuid) {
      return new MaterializedViewWriter(
          this.materializedViewMongoClient,
          matViewColName,
          generationId,
          leaseManager,
          this.metricsFactory,
          collectionUuid,
          this.rateLimiter);
    }

    @Override
    public void close() {
      // Close materializedViewMongoClient in factory as this materializedViewMongoClient is shared.
      this.materializedViewMongoClient.close();
    }

    private MongoClient createMaterializedViewMongoClient(
        SyncSourceConfig syncSourceConfig, MeterRegistry meterRegistry) {
      // Use mongosUri if available, otherwise fall back to mongodUri. This allows the MongoDB
      // driver to automatically discover replica set topology and route writes to the primary,
      // avoiding NotWritablePrimary errors after failovers.
      var originalConnectionString = syncSourceConfig.mongosUri.orElse(syncSourceConfig.mongodUri);
      // Replace directConnection=true with directConnection=false to enable topology discovery.
      // MMS provides connection strings with directConnection=true which forces the driver to
      // connect only to the specified host. For write operations, we need to route to the primary,
      // so we must enable topology discovery by setting directConnection=false.
      // TODO(CLOUDP-360542): have mms return connection strings with directConnection=false.
      var connectionString = disableDirectConnection(originalConnectionString);
      LOG.atInfo()
          .addKeyValue("hosts", connectionString.getHosts())
          .addKeyValue("directConnection", connectionString.isDirectConnection())
          .log("Creating MongoClient for MaterializedViewWriter");
      return MongoClientBuilder.buildNonReplicationWithDefaults(
          connectionString,
          "AutoEmbedding Materialized View Writer",
          DEFAULT_MAX_CONNECTIONS,
          syncSourceConfig.sslContext,
          meterRegistry);
    }
  }

  /**
   * Write a batch of documents to the materialized view.
   *
   * @param documents list of documents to write
   * @param subRetryAttempt number of times we have attempted to retry a partial batch
   * @throws Exception throws a non-transient exception in case of a known non-retryable error like
   *     a document being too large. Otherwise throws a transient exception.
   */
  private void bulkWrite(List<WriteModel<RawBsonDocument>> documents, int subRetryAttempt)
      throws Exception {
    // limit to avoid unbounded recursive calls
    if (subRetryAttempt >= MAX_SUB_RETRY_ATTEMPTS) {
      throw new MaterializedViewNonTransientException(
          "Failed to write to materialized view due to too many sub-retry attempts");
    }
    // Record bulk write metrics - this metric is at an attempt level, so it is possible to
    // double count when retrying. This is ok for now as we are mainly using these metrics to
    // detect general trends and not for precise accounting.
    this.bulkWriteNumDocs.record(documents.size());
    this.bulkWritePayloadSize.record(calculatePayloadSize(documents));

    try {
      this.operationExecutor.execute(
          "materializedViewBulkWrite",
          () -> {
            this.mongoClient
                .getDatabase(this.namespace.getDatabaseName())
                .getCollection(this.namespace.getCollectionName(), RawBsonDocument.class)
                .bulkWrite(documents);
          });
    } catch (MongoBulkWriteException e) {
      this.partialBulkWriteErrors.increment();
      var failedOperations = filterFailedOperations(documents, e.getWriteErrors());
      LOG.warn(
          "{} out of {} operations failed in bulk write, retrying failed operations",
          failedOperations.size(),
          documents.size());
      bulkWrite(failedOperations, subRetryAttempt + 1);
    } catch (BsonMaximumSizeExceededException | MongoCommandException e) {
      if (isPayloadTooLarge(e)) {
        this.payloadTooLargeErrors.increment();
        // unlikely but possible scenario where a single document is larger than 16MiB after
        // replacing the text with vectors. In this case, retrying doesn't help so bailing out. To
        // handle this, we might need to split up the document  into multiple smaller chunks and
        // insert.
        if (documents.size() == 1) {
          throw new MaterializedViewNonTransientException(
              "Failed to write to materialized view due to single document exceeding 16MiB limit");
        }
        LOG.warn(
            "Bulk write failed due to large batch size, retrying with smaller batches. "
                + "Current batch size is {}",
            documents.size());
        int mid = documents.size() / 2;
        bulkWrite(documents.subList(0, mid), subRetryAttempt + 1);
        bulkWrite(documents.subList(mid, documents.size()), subRetryAttempt + 1);
      } else {
        throw new MaterializedViewTransientException(e);
      }
    }
  }

  /**
   * Returns true if the write failed due to the bulk payload exceeding 16 MiB.
   *
   * @param ex the exception thrown by the client
   * @return true if the exception matches the criteria
   */
  private boolean isPayloadTooLarge(Throwable ex) {
    // Docs aren't clear as to which exception is thrown, so checking both.
    return ex instanceof BsonMaximumSizeExceededException
        || (ex instanceof MongoCommandException
            && ((MongoCommandException) ex).getErrorCode() == 10334);
  }

  /**
   * Filter failed operations using BulkWriteError and identify operations to retry.
   *
   * @param bulkOperations The original list of bulk write operations
   * @param writeErrors BulkWriteErrors returned from MongoBulkWriteException
   * @return A filtered list containing only the failed operations
   */
  private static List<WriteModel<RawBsonDocument>> filterFailedOperations(
      List<WriteModel<RawBsonDocument>> bulkOperations, List<BulkWriteError> writeErrors) {
    List<WriteModel<RawBsonDocument>> retryOperations = new ArrayList<>();

    for (BulkWriteError error : writeErrors) {
      if (!Errors.RETRYABLE_ERROR_CODES.contains(error.getCode())) {
        // Since we cannot skip any document events, we need to fail the entire batch if we
        // encounter even a single non-retryable error.
        throw new MaterializedViewNonTransientException(
            "Failed to write to materialized view due to non-retryable error: "
                + error.getMessage());
      }
      int failedIndex = error.getIndex();
      retryOperations.add(bulkOperations.get(failedIndex));
    }

    return retryOperations;
  }

  /**
   * Calculate the total payload size of all documents in the bulk write operation.
   *
   * @param documents list of write models to calculate size for
   * @return total size in bytes
   */
  private static long calculatePayloadSize(List<WriteModel<RawBsonDocument>> documents) {
    @Var long totalSize = 0;
    for (WriteModel<RawBsonDocument> writeModel : documents) {
      if (writeModel instanceof ReplaceOneModel<RawBsonDocument> replaceModel) {
        totalSize += replaceModel.getReplacement().getByteBuffer().remaining();
      }
      // UpdateOneModel is only used for filter-field-only updates (a small $set of a few fields),
      // so its payload is negligible relative to full-document ReplaceOneModel payloads which carry
      // embeddings. Measuring it would require serializing a BsonDocument on the hot path, so it
      // is intentionally excluded here.
      // DeleteOneModel doesn't have a document payload to measure.
    }
    return totalSize;
  }

  private void ensureOpen(String methodName) {
    if (this.closed) {
      String message = String.format("Cannot call %s() after to close()", methodName);
      throw new IndexClosedException(message);
    }
  }

  @Override
  public void cancelMerges() throws IOException {
    // No-op: MaterializedViewWriter writes to MongoDB, not Lucene, so there are no merges to cancel
    LOG.debug("cancelMerges() called on MaterializedViewWriter - no action needed");
  }
}
