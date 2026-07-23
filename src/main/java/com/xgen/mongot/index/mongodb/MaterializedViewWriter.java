package com.xgen.mongot.index.mongodb;

import static com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE;

import com.google.common.util.concurrent.RateLimiter;
import com.google.errorprone.annotations.Var;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
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
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.concurrent.LockGuard;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.mongot.util.retry.ExponentialBackoffPolicy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
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
 *
 * <p>MV commits always use <b>unordered</b> {@link com.mongodb.client.MongoCollection#bulkWrite}
 * for the materialized view collection (commit and partial-failure retry paths only), which does
 * not follow list order. So we need deduplication operations to avoid non-deterministic behavior.
 * Deduplication operations are applied in {@link
 * com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils}. {@link
 * #dropMaterializedViewCollection()} uses {@code drop()} and is unaffected.
 */
public class MaterializedViewWriter implements IndexWriter {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewWriter.class);

  /**
   * Options for MV {@code bulkWrite}; unordered so retry logic can use per-index write errors.
   *
   * <p>Treat as immutable: this instance is shared — do not call fluent setters ({@code ordered},
   * etc.) on it.
   */
  static final BulkWriteOptions MV_BULK_WRITE_OPTIONS = new BulkWriteOptions().ordered(false);

  // TODO(CLOUDP-356242): make this configurable
  public static final String MV_DATABASE_NAME = "__mdb_internal_search";

  // Nested under _autoEmbed to avoid collisions with user fields.
  // Written as nested doc: {_autoEmbed: {_leaseVersion: N}}.
  // Queried via dot notation: "_autoEmbed._leaseVersion".
  static final String LEASE_VERSION_FIELD = "_autoEmbed._leaseVersion";
  private static final String LEASE_VERSION_BSON_PARENT = "_autoEmbed";
  private static final String LEASE_VERSION_BSON_KEY = "_leaseVersion";
  private static final int MAX_SUB_RETRY_ATTEMPTS = 3;

  private final MongoNamespace namespace;
  private final AtomicReference<ConcurrentLinkedQueue<WriteModel<RawBsonDocument>>>
      bulkOperationsRef;
  private final MongoClientOperationExecutor operationExecutor;
  private final UUID collectionUuid;
  private final MaterializedViewGenerationId generationId;
  private final LeaseManager leaseManager;
  private final Optional<RateLimiter> rateLimiter;

  // Metrics
  private final Counter partialBulkWriteErrors;
  private final Counter mvWriteThrottleCount;
  private final Timer mvWriteThrottleWaitTime;
  private final DistributionSummary bulkWriteNumDocs;
  private final DistributionSummary bulkWritePayloadSize;
  private final Counter mvUnmatchedFilterUpdates;

  /**
   * A pair of read and write locks which are used to synchronize access to the materialized view
   * collection and the in-memory buffer. The read lock allows concurrent access to the in-memory
   * buffer, but the write lock ensures that the buffer is flushed atomically without losing any
   * updates. The write lock is also used to coordinate the close/shutdown of the writer.
   */
  private final ReentrantReadWriteLock.WriteLock shutdownAndCommitExclusiveLock;

  private final ReentrantReadWriteLock.ReadLock shutdownAndCommitSharedLock;

  private final AutoEmbeddingMongoClient autoEmbeddingMongoClient;
  private final MetricsFactory metricsFactory;

  /**
   * Whether the writer is closed. You need to hold the above-mentioned read lock to read the value
   * and the write lock to update the value.
   */
  private boolean closed;

  /** Builds Materialized View Writer for an auto embedding index. */
  public MaterializedViewWriter(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      String matViewDatabaseName,
      String matViewName,
      MaterializedViewGenerationId generationId,
      LeaseManager leaseManager,
      MetricsFactory metricsFactory,
      UUID collectionUuid,
      Optional<RateLimiter> rateLimiter) {
    this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
    this.namespace = new MongoNamespace(matViewDatabaseName, matViewName);
    this.generationId = generationId;
    this.leaseManager = leaseManager;
    this.metricsFactory = metricsFactory;
    this.bulkOperationsRef = new AtomicReference<>(new ConcurrentLinkedQueue<>());
    this.partialBulkWriteErrors = metricsFactory.counter("partialBulkWriteErrors");
    this.mvWriteThrottleCount = metricsFactory.counter("mvWriteThrottleCount");
    this.mvWriteThrottleWaitTime = metricsFactory.timerSec("mvWriteThrottleWaitTime");
    this.bulkWriteNumDocs = metricsFactory.summary("bulkWriteNumDocs");
    this.bulkWritePayloadSize = metricsFactory.summary("bulkWritePayloadSize");
    this.mvUnmatchedFilterUpdates = metricsFactory.counter("mvUnmatchedFilterUpdates");
    this.operationExecutor =
        new MongoClientOperationExecutor(
            metricsFactory,
            "materializedViewCollection",
            ExponentialBackoffPolicy.builder()
                .initialDelay(Duration.ofSeconds(1))
                .backoffFactor(4)
                .maxDelay(Duration.ofMinutes(1))
                .maxRetries(5)
                .jitter(0.1)
                .build(),
            () -> this.closed);
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
        long leaseVersion = this.leaseManager.getLeaseVersion(this.generationId);
        if (leaseVersion <= 0) {
          recordMvErrorMetric(
              MaterializedViewNonTransientException.class,
              MaterializedViewNonTransientException.Reason.INVALID_LEASE);
          throw new MaterializedViewNonTransientException(
              "Cannot write to MV without a valid lease (leaseVersion=" + leaseVersion + ")",
              MaterializedViewNonTransientException.Reason.INVALID_LEASE);
        }
        List<WriteModel<RawBsonDocument>> bulkList = bulkOperations.stream().toList();
        // Apply fencing once here, then pass already-fenced documents to bulkWrite.
        // This ensures retry paths (partial failure, payload split) use the same fenced
        // documents without re-fencing, and error indices align directly.
        List<WriteModel<RawBsonDocument>> fencedOps =
            addFencingToWriteModels(bulkList, leaseVersion);
        bulkWrite(fencedOps, 0);
      } catch (MaterializedViewNonTransientException e) {
        throw e;
      } catch (MaterializedViewTransientException e) {
        throw e;
      } catch (Exception e) {
        recordMvErrorMetric(
            MaterializedViewTransientException.class,
            MaterializedViewTransientException.Reason.BULK_WRITE_ERROR);
        throw new MaterializedViewTransientException(
            String.valueOf(e.getMessage()),
            e,
            MaterializedViewTransientException.Reason.BULK_WRITE_ERROR);
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
      recordMvErrorMetric(
          MaterializedViewTransientException.class,
          MaterializedViewTransientException.Reason.READ_LEASE_FAILED);
      throw new MaterializedViewTransientException(
          String.valueOf(e.getMessage()),
          e,
          MaterializedViewTransientException.Reason.READ_LEASE_FAILED);
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

  public AutoEmbeddingMongoClient getMongoClient() {
    return this.autoEmbeddingMongoClient;
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
    LOG.atInfo()
        .addKeyValue("generationId", this.generationId)
        .addKeyValue("namespace", this.namespace)
        .log("Dropping materialized view collection");
    this.close();
    var mongoClientOpt = this.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient();
    if (mongoClientOpt.isEmpty()) {
      recordMvErrorMetric(
          MaterializedViewTransientException.class,
          MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE);
      // This won't crash Mongot when IndexAction::dropIndex triggers it.
      return CompletableFuture.failedFuture(
          new MaterializedViewTransientException(
              "Materialized view writer client is not available at this time.",
              MONGO_CLIENT_NOT_AVAILABLE));
    }
    return CompletableFuture.runAsync(
        () ->
            mongoClientOpt
                .get()
                .getDatabase(this.namespace.getDatabaseName())
                .getCollection(this.namespace.getCollectionName(), RawBsonDocument.class)
                .drop());
  }

  public static class Factory implements Closeable {
    private final AutoEmbeddingMongoClient autoEmbeddingMongoClient;
    private final MetricsFactory metricsFactory;
    private final Optional<RateLimiter> rateLimiter;

    public Factory(
        AutoEmbeddingMongoClient autoEmbeddingMongoClient,
        MeterRegistry meterRegistry,
        Optional<Integer> mvWriteRateLimitRps) {
      // TODO(CLOUDP-360542): Investigate whether we need to change this when primary mongod node is
      // not discoverable in original seedAddresses
      this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
      this.metricsFactory = new MetricsFactory("materializedViewWriter", meterRegistry);
      this.rateLimiter =
          mvWriteRateLimitRps.map(
              rps -> {
                LOG.info("MV write rate limiter configured at {} RPS", rps);
                return RateLimiter.create(rps);
              });
    }

    public MaterializedViewWriter create(
        String matViewDatabaseName,
        String matViewColName,
        MaterializedViewGenerationId generationId,
        LeaseManager leaseManager,
        UUID collectionUuid) {
      return new MaterializedViewWriter(
          this.autoEmbeddingMongoClient,
          matViewDatabaseName,
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
      this.autoEmbeddingMongoClient.close();
    }
  }

  /**
   * Write a batch of documents to the materialized view. Documents are expected to already have
   * fencing applied via {@link #addFencingToWriteModels} before being passed here.
   *
   * @param documents list of (already fenced) documents to write
   * @param subRetryAttempt number of times we have attempted to retry a partial batch
   * @throws Exception throws a non-transient exception in case of a known non-retryable error like
   *     a document being too large. Otherwise throws a transient exception.
   */
  private void bulkWrite(List<WriteModel<RawBsonDocument>> documents, int subRetryAttempt)
      throws Exception {
    // limit to avoid unbounded recursive calls
    if (subRetryAttempt >= MAX_SUB_RETRY_ATTEMPTS) {
      recordMvErrorMetric(
          MaterializedViewTransientException.class,
          MaterializedViewTransientException.Reason.BULK_WRITE_ERROR);
      throw new MaterializedViewTransientException(
          "Failed to write to materialized view due to too many sub-retry attempts,"
              + " will retry the whole batch.",
          MaterializedViewTransientException.Reason.BULK_WRITE_ERROR);
    }
    var mongoClientOpt = this.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient();
    if (mongoClientOpt.isEmpty()) {
      // This shouldn't happen since we block replication when sync source is missing in
      // IndexActions
      recordMvErrorMetric(
          MaterializedViewTransientException.class,
          MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE);
      throw new MaterializedViewTransientException(
          "Materialized view writer client is not available at this time."
              + " Will retry the whole batch.",
          MONGO_CLIENT_NOT_AVAILABLE);
    }
    // Record bulk write metrics - this metric is at an attempt level, so it is possible to
    // double count when retrying. This is ok for now as we are mainly using these metrics to
    // detect general trends and not for precise accounting.
    int docCount = documents.size();
    long payloadSizeBytes = calculatePayloadSize(documents);
    this.bulkWriteNumDocs.record(docCount);
    this.bulkWritePayloadSize.record(payloadSizeBytes);
    if (docCount > 0) {
      LOG.debug(
          "Materialized view bulk write: generationId={} docCount={} payloadSizeBytes={}",
          this.generationId,
          docCount,
          payloadSizeBytes);
    }
    // MV writes use the default write concern (w:1) rather than w:majority. This is safe because
    // commit() calls updateCommitInfo() after bulkWrite(), and updateCommitInfo() uses w:majority
    // on the lease collection. Since both write to the same mongod primary, MV writes precede the
    // checkpoint in the oplog. MongoDB secondaries apply oplog entries in order, so w:majority on
    // the checkpoint implicitly guarantees all preceding MV writes are also majority-replicated.
    // If the checkpoint write fails (e.g., primary crash before it completes), the checkpoint does
    // not advance, and the next leader re-processes the batch.
    try {
      // Block-return lambda (not an expression lambda) so this binds to the value-returning
      // execute(String, CheckedSupplier) overload rather than the void CheckedRunnable one.
      BulkWriteResult bulkWriteResult =
          this.operationExecutor.execute(
              "materializedViewBulkWrite",
              () -> {
                return mongoClientOpt
                    .get()
                    .getDatabase(this.namespace.getDatabaseName())
                    .getCollection(this.namespace.getCollectionName(), RawBsonDocument.class)
                    .bulkWrite(documents, MV_BULK_WRITE_OPTIONS);
              });
      recordWriteEffectConservation(documents, bulkWriteResult);
    } catch (MongoBulkWriteException e) {
      // Unordered bulk may report multiple writeErrors (one per failed op).
      // Check if the error is a fencing rejection (DuplicateKey from a fenced upsert).
      // DuplicateKey occurs when the fencing filter doesn't match (existing doc has a higher
      // _autoEmbed._leaseVersion) and the upsert attempts an insert that conflicts on _id.
      // This means we're a zombie leader — fail the entire batch immediately.
      boolean hasFencingRejection =
          e.getWriteErrors().stream()
              .anyMatch(err -> err.getCategory() == ErrorCategory.DUPLICATE_KEY);
      if (hasFencingRejection) {
        recordMvErrorMetric(
            MaterializedViewNonTransientException.class,
            MaterializedViewNonTransientException.Reason.FENCING_REJECTION);
        LOG.warn(
            "Fencing rejection detected — this instance is a stale leader. "
                + "A document in the MV has a higher _autoEmbed._leaseVersion. Failing batch.");
        throw new MaterializedViewNonTransientException(
            "MV write rejected by fencing: a newer leader has written to this document",
            MaterializedViewNonTransientException.Reason.FENCING_REJECTION);
      }
      // Not a fencing rejection — retry the failed operations
      // Disk-full and write-blocked cases are detected and wrapped as transient by
      // MongoClientOperationExecutor before reaching here.
      // filterFailedOperations may throw MaterializedViewNonTransientException
      // for non-retryable errors; record the metric before it propagates.
      this.partialBulkWriteErrors.increment();
      List<WriteModel<RawBsonDocument>> failedOperations;
      try {
        failedOperations = filterFailedOperations(documents, e.getWriteErrors());
      } catch (MaterializedViewNonTransientException nonTransient) {
        recordMvErrorMetric(MaterializedViewNonTransientException.class, nonTransient.getReason());
        throw nonTransient;
      }
      if (failedOperations.isEmpty()) {
        // e.g. writeConcernError with no per-operation writeErrors — we have no indices for a
        // partial retry, and bulkWrite(List.of()) would throw IllegalArgumentException.
        //
        // Tradeoff: we retry the entire batch, which may re-apply ops that already succeeded on the
        // server (ambiguous ack / partial success). MV writes are replace/update/delete by _id with
        // fencing; replaying the same batch is treated as acceptable idempotence at document scope
        // versus failing closed or crashing on an empty retry list.
        LOG.warn(
            "Bulk write failed with no per-operation write errors; retrying full batch of {}"
                + " operations",
            documents.size());
        // TODO(CLOUDP-397327): avoid retrying the whole batch.
        bulkWrite(documents, subRetryAttempt + 1);
        return;
      }
      LOG.warn(
          "{} out of {} operations failed in bulk write, retrying failed operations",
          failedOperations.size(),
          documents.size());
      bulkWrite(failedOperations, subRetryAttempt + 1);
    } catch (BsonMaximumSizeExceededException | MongoCommandException e) {
      if (isPayloadTooLarge(e)) {
        // unlikely but possible scenario where a single document is larger than 16MiB after
        // replacing the text with vectors. In this case, retrying doesn't help so bailing out. To
        // handle this, we might need to split up the document  into multiple smaller chunks and
        // insert.
        if (documents.size() == 1) {
          recordMvErrorMetric(
              MaterializedViewNonTransientException.class,
              MaterializedViewNonTransientException.Reason.DOCUMENT_TOO_LARGE);
          throw new MaterializedViewNonTransientException(
              "Failed to write to materialized view due to single document exceeding 16MiB limit",
              MaterializedViewNonTransientException.Reason.DOCUMENT_TOO_LARGE);
        }
        LOG.warn(
            "Bulk write failed due to large batch size, retrying with smaller batches. "
                + "Current batch size is {}",
            documents.size());
        int mid = documents.size() / 2;
        bulkWrite(documents.subList(0, mid), subRetryAttempt + 1);
        bulkWrite(documents.subList(mid, documents.size()), subRetryAttempt + 1);
      } else {
        recordMvErrorMetric(
            MaterializedViewTransientException.class,
            MaterializedViewTransientException.Reason.BULK_WRITE_ERROR);
        throw new MaterializedViewTransientException(
            String.valueOf(e.getMessage()),
            e,
            MaterializedViewTransientException.Reason.BULK_WRITE_ERROR);
      }
    }
  }

  /**
   * Write-effect conservation tripwire. On a successful bulk write, every {@link ReplaceOneModel}
   * is an {@code upsert=true}, so it always either matches an existing document or upserts a new
   * one — contributing exactly one to {@code matchedCount + upsertedCount}. A {@link
   * DeleteOneModel} does not affect either count. Only the {@link UpdateOneModel} used for
   * filter-only / partial-embed {@code $set} updates is {@code upsert=false} and can therefore
   * match zero documents. The shortfall between the writes we expected to land and those that
   * actually did equals the number of such zero-match updates.
   *
   * <p><b>Interpretation — this is not by itself MV/source drift.</b> An {@link UpdateOneModel}
   * matches zero documents for two distinct reasons:
   *
   * <ol>
   *   <li><b>Missing MV doc (drift):</b> the target document has not been ingested into the MV yet
   *       (e.g. a change-stream update arriving before initial sync has upserted the doc) — a
   *       silent no-op that genuinely drops the intended write.
   *   <li><b>Fenced out (correct):</b> {@code UpdateOneModel}s carry a fencing predicate ({@code
   *       Filters.and(idFilter, fencingCondition)}), so the update also matches nothing when a
   *       newer leader has written a higher {@code _autoEmbed._leaseVersion}. Here the zero match
   *       is the intended behavior of fencing, not lost data. (A batch containing an upserting
   *       {@link ReplaceOneModel} would instead surface fencing as a {@code DUPLICATE_KEY}
   *       rejection on the throw path; an all-{@code UpdateOneModel} batch on a fenced-out writer
   *       can reach here silently.)
   * </ol>
   *
   * <p>The metric is therefore named for the observable effect (unmatched filter updates), not a
   * cause; alerting should correlate it with leadership/fencing signals before treating a rise as
   * drift.
   *
   * <p>This is a cheap, always-on signal, not precise accounting: it is evaluated only on the clean
   * success path (a {@code bulkWrite} that throws is handled by the retry logic above and its
   * partial effects are not counted), mirroring the "detect general trends" approach of the {@code
   * bulkWriteNumDocs} metric.
   */
  private void recordWriteEffectConservation(
      List<WriteModel<RawBsonDocument>> documents, BulkWriteResult bulkWriteResult) {
    if (bulkWriteResult == null || !bulkWriteResult.wasAcknowledged()) {
      return;
    }
    long expectedMatchedOrUpserted =
        documents.stream()
            .filter(
                model -> model instanceof ReplaceOneModel<?> || model instanceof UpdateOneModel<?>)
            .count();
    long actualMatchedOrUpserted =
        (long) bulkWriteResult.getMatchedCount() + bulkWriteResult.getUpserts().size();
    long unmatchedFilterUpdates = expectedMatchedOrUpserted - actualMatchedOrUpserted;
    if (unmatchedFilterUpdates > 0) {
      this.mvUnmatchedFilterUpdates.increment(unmatchedFilterUpdates);
      LOG.warn(
          "Materialized view bulk write had {} filter-only/partial-embed update(s) that matched no"
              + " document: either the MV doc is not yet ingested (dropped write) or this writer"
              + " was fenced out by a newer leader (correct no-op). generationId={}",
          unmatchedFilterUpdates,
          this.generationId);
    }
  }

  private void recordMvErrorMetric(Class<? extends Exception> exceptionType, Enum<?> reason) {
    this.metricsFactory
        .counter(
            "mvErrors",
            Tags.of(
                "exceptionType", exceptionType.getSimpleName(),
                "reason", reason.name().toLowerCase(Locale.ROOT)))
        .increment();
  }

  /**
   * Adds per-document fencing to write models so that stale leaders cannot overwrite documents
   * written by a newer leader with a higher leaseVersion.
   *
   * <p>All write model types receive a fencing filter that rejects the operation when the existing
   * document already has a higher leaseVersion. In addition, {@link ReplaceOneModel} embeds the
   * leaseVersion in the replacement document, and {@link UpdateOneModel} adds it via {@code $set}.
   * {@link DeleteOneModel} gets only the filter — without it a zombie could delete a doc written by
   * the new leader, then reinsert stale data via a subsequent upsert.
   *
   * @param documents the original write models
   * @param leaseVersion the current lease version to embed as a fencing token
   * @return a new list of write models with fencing applied
   */
  static List<WriteModel<RawBsonDocument>> addFencingToWriteModels(
      List<WriteModel<RawBsonDocument>> documents, long leaseVersion) {
    // Skip fencing for static leaders (sentinel value) to avoid poisoning MV documents
    // with Long.MAX_VALUE, which would block any future dynamic leader writes.
    if (leaseVersion == Long.MAX_VALUE) {
      return documents;
    }
    // Fencing condition: only write if existing doc has leaseVersion <= ours, or no field yet.
    Bson fencingCondition =
        Filters.or(
            Filters.lte(LEASE_VERSION_FIELD, leaseVersion),
            Filters.exists(LEASE_VERSION_FIELD, false));

    List<WriteModel<RawBsonDocument>> fenced = new ArrayList<>(documents.size());
    for (WriteModel<RawBsonDocument> writeModel : documents) {
      if (writeModel instanceof ReplaceOneModel<RawBsonDocument> replaceModel) {
        // Embed leaseVersion as a nested document {_autoEmbed: {_leaseVersion: N}}.
        // RawBsonDocument is immutable, so decode into a mutable BsonDocument copy first.
        // TODO(CLOUDP-392847): Revisit efficiency — keep documents as BsonDocument in the queue
        // and defer RawBsonDocument encoding to after fencing is applied.
        // Note: doc.put("_autoEmbed._leaseVersion", ...) would create a literal dotted key,
        // but Filters/queries interpret dots as nested paths — so we must write as nested doc.
        BsonDocument doc = new BsonDocument();
        doc.putAll(replaceModel.getReplacement().toBsonDocument());
        BsonDocument autoEmbedDoc;
        if (doc.containsKey(LEASE_VERSION_BSON_PARENT)
            && doc.get(LEASE_VERSION_BSON_PARENT).isDocument()) {
          autoEmbedDoc = doc.getDocument(LEASE_VERSION_BSON_PARENT);
        } else {
          autoEmbedDoc = new BsonDocument();
        }
        autoEmbedDoc.put(LEASE_VERSION_BSON_KEY, new BsonInt64(leaseVersion));
        doc.put(LEASE_VERSION_BSON_PARENT, autoEmbedDoc);

        fenced.add(
            new ReplaceOneModel<>(
                Filters.and(replaceModel.getFilter(), fencingCondition),
                BsonUtils.documentToRaw(doc),
                replaceModel.getReplaceOptions()));
      } else if (writeModel instanceof UpdateOneModel<RawBsonDocument> updateModel) {
        // Add $set for leaseVersion to the update. Clone to avoid modifying the original.
        // getUpdate() is null when the agg pipeline constructor is used; skip fencing the
        // update body in that case (the fencing filter on the query still protects the write).
        BsonDocument updateDoc = new BsonDocument();
        if (updateModel.getUpdate() != null) {
          updateDoc.putAll(updateModel.getUpdate().toBsonDocument());
        }
        BsonDocument setDoc = new BsonDocument();
        if (updateDoc.containsKey("$set")) {
          setDoc.putAll(updateDoc.getDocument("$set"));
        }
        setDoc.put(LEASE_VERSION_FIELD, new BsonInt64(leaseVersion));
        updateDoc.put("$set", setDoc);

        fenced.add(
            new UpdateOneModel<>(
                Filters.and(updateModel.getFilter(), fencingCondition),
                updateDoc,
                updateModel.getOptions()));
      } else if (writeModel instanceof DeleteOneModel<RawBsonDocument> deleteModel) {
        fenced.add(new DeleteOneModel<>(Filters.and(deleteModel.getFilter(), fencingCondition)));
      } else {
        fenced.add(writeModel);
      }
    }
    return fenced;
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
   * MV {@code bulkWrite} is always unordered: the server may attempt every operation in the batch.
   * Operations that succeeded must not be retried. Returns one entry per distinct failing index in
   * {@code writeErrors} (sorted by index).
   */
  private static List<WriteModel<RawBsonDocument>> filterFailedOperations(
      List<WriteModel<RawBsonDocument>> bulkOperations, List<BulkWriteError> writeErrors) {
    TreeSet<Integer> failedIndices = new TreeSet<>();
    for (BulkWriteError error : writeErrors) {
      if (!Errors.RETRYABLE_ERROR_CODES.contains(error.getCode())) {
        throw new MaterializedViewNonTransientException(
            "Failed to write to materialized view due to non-retryable error: "
                + error.getMessage(),
            MaterializedViewNonTransientException.Reason.NON_RETRYABLE_ERROR);
      }
      failedIndices.add(error.getIndex());
    }
    List<WriteModel<RawBsonDocument>> retryOperations = new ArrayList<>(failedIndices.size());
    for (int idx : failedIndices) {
      retryOperations.add(bulkOperations.get(idx));
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
