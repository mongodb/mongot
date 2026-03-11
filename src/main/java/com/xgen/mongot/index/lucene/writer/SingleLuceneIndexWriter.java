package com.xgen.mongot.index.lucene.writer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.DocsExceededLimitsException;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexClosedException;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.WriterClosedException;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.ingestion.BsonDocumentProcessor;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.commit.LuceneCommitData;
import com.xgen.mongot.index.lucene.document.DefaultIndexingPolicy;
import com.xgen.mongot.index.lucene.document.LuceneIndexingPolicy;
import com.xgen.mongot.index.lucene.document.builder.DocumentBlockBuilder;
import com.xgen.mongot.index.lucene.document.context.IndexingPolicyBuilderContext;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.similarity.LuceneSimilarity;
import com.xgen.mongot.index.lucene.sort.LuceneIndexSortCompatibilityValidator;
import com.xgen.mongot.index.lucene.sort.LuceneIndexSortFactory;
import com.xgen.mongot.index.lucene.util.LuceneCodecUtils;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.LoggableIdUtils;
import com.xgen.mongot.util.concurrent.LockGuard;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;

/** LuceneIndexWriter is an IndexWriter that indexes documents into a Lucene index. */
@SuppressWarnings("GuardedBy") // Uses LockGuard instead
public class SingleLuceneIndexWriter implements LuceneIndexWriter {

  private final DefaultKeyValueLogger logger;

  /**
   * These exclusive/shared locks allow for many different threads to use the LuceneIndexWriter
   * concurrently (locking in the shared mode), but locks out all other threads if a thread is
   * close()-ing the LuceneIndexReader.
   *
   * <p>This enables good performance during the common case (i.e. LuceneIndexWriter is open, many
   * threads accessing it), while preventing race conditions while shutting down (e.g. returning
   * from close() (and subsequently having the Directory closed) while documents are being indexed
   * on a different thread, resulting in the indexing trying to use a closed SearcherManager.
   */
  private final ReentrantReadWriteLock.WriteLock shutdownExclusiveLock;

  private final ReentrantReadWriteLock.ReadLock shutdownSharedLock;

  /**
   * In order to do anything with the org.apache.lucene.index.IndexWriter, you must acquire the
   * shutdownSharedLock to ensure that the LuceneIndexWriter (and subsequently the
   * org.apache.lucene.index.IndexWriter) will not be closed() while processing.
   */
  @GuardedBy("shutdownSharedLock")
  private final org.apache.lucene.index.IndexWriter luceneWriter;

  /**
   * Factory to create {@link DocumentBlockBuilder} instances, that in turn builds Lucene documents.
   * This factory creates a single {@link DocumentBlockBuilder} instance per ingested BSON document,
   * and produces an array of Lucene Documents to be indexed as a block.
   */
  private final LuceneIndexingPolicy indexingPolicy;

  /**
   * An optional upper limit for fields in an index. To support different indexing patterns, this
   * limit is not enforced by LuceneIndexWriter, instead, callers must separately call
   * exceededLimits() and act on the result.
   */
  private final Optional<Integer> fieldLimit;

  /**
   * A soft upper limit for the number of documents in an index. It must be lower than the Lucene
   * hard limit that is equal to ~2B (Integer.MAX_VALUE - 128).
   *
   * <p>If an index exceeds this limit then it should transition to STALE status.
   *
   * <p>See {@link org.apache.lucene.index.IndexWriter#MAX_DOCS}
   */
  private final Optional<Integer> docsLimit;

  /**
   * In order to read closed's value you must acquire at least the shutdownSharedLock.
   *
   * <p>In order to flip closed's value you must acquire the shutdownExclusiveLock.
   *
   * <p>Note that @GuardedBy is not capable of expressing something being guarded by either of two
   * locks, so we do not annotate this variable.
   */
  private boolean closed;

  private final IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater;

  private final Set<FieldPath> fieldPathsToFilterOut;

  private final FeatureFlags featureFlags;

  // Rate limiter for indexing failure logging to avoid excessive ID decoding and logging overhead
  private static final RateLimiter INDEXING_FAILURE_LOG_RATE_LIMITER =
      RateLimiter.create(1.0 / 60.0); // Allow 1 log per 60 seconds (same as atMostEvery 1 minute)

  private SingleLuceneIndexWriter(
      org.apache.lucene.index.IndexWriter luceneWriter,
      LuceneIndexingPolicy indexingPolicy,
      ObjectId indexId,
      Optional<Integer> fieldLimit,
      Optional<Integer> docsLimit,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater,
      Set<FieldPath> fieldPathsToFilterOut,
      FeatureFlags featureFlags) {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", indexId);
    defaultKeyValues.put("indexFormatVersion", indexCapabilities);
    this.logger = DefaultKeyValueLogger.getLogger(SingleLuceneIndexWriter.class, defaultKeyValues);

    this.luceneWriter = luceneWriter;
    this.indexingPolicy = indexingPolicy;
    this.fieldLimit = fieldLimit;
    this.docsLimit = docsLimit;

    ReentrantReadWriteLock shutdownLock = new ReentrantReadWriteLock(true);
    this.shutdownExclusiveLock = shutdownLock.writeLock();
    this.shutdownSharedLock = shutdownLock.readLock();

    this.indexingMetricsUpdater = indexingMetricsUpdater;
    this.fieldPathsToFilterOut = fieldPathsToFilterOut;
    this.featureFlags = featureFlags;

    this.closed = false;
  }

  /** Creates a writer for a Lucene-native vector index. */
  public static SingleLuceneIndexWriter createForVectorIndex(
      Directory directory,
      InstrumentedConcurrentMergeScheduler.PerIndexPartitionMergeScheduler mergeScheduler,
      MergePolicy mergePolicy,
      double ramBufferSizeMb,
      Optional<Integer> fieldLimit,
      Optional<Integer> docsLimit,
      VectorIndexDefinition indexDefinition,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater,
      Optional<IndexDeletionPolicy> indexDeletionPolicy,
      FeatureFlags featureFlags)
      throws IOException {

    Map<FieldPath, VectorFieldSpecification> vectorFields =
        LuceneCodecUtils.extractVectorFieldsFromVectorIndex(indexDefinition.getFields());

    IndexWriterConfig indexWriterConfig =
        new org.apache.lucene.index.IndexWriterConfig()
            .setCodec(new LuceneCodec(vectorFields))
            .setMergePolicy(mergePolicy)
            .setRAMBufferSizeMB(ramBufferSizeMb)
            .setMergeScheduler(mergeScheduler)
            .setCommitOnClose(false);
    indexDeletionPolicy.ifPresent(indexWriterConfig::setIndexDeletionPolicy);
    // initializes the corresponding PerIndexMergeScheduler
    mergeScheduler.setInfoStream(indexWriterConfig.getInfoStream());

    org.apache.lucene.index.IndexWriter luceneIndexWriter =
        new org.apache.lucene.index.IndexWriter(directory, indexWriterConfig);

    LuceneIndexingPolicy luceneIndexingPolicy =
        DefaultIndexingPolicy.create(indexDefinition, indexCapabilities, indexingMetricsUpdater);

    Set<FieldPath> customVectorEngineFields =
        vectorFields.entrySet().stream()
            .filter(entry -> entry.getValue().isCustomVectorEngine())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    SingleLuceneIndexWriter writer =
        new SingleLuceneIndexWriter(
            luceneIndexWriter,
            luceneIndexingPolicy,
            indexDefinition.getIndexId(),
            fieldLimit,
            docsLimit,
            indexCapabilities,
            indexingMetricsUpdater,
            customVectorEngineFields,
            featureFlags);

    writer
        .logger
        .atInfo()
        .addKeyValue("indexId", indexDefinition.getIndexId())
        .addKeyValue(
            "numFields",
            Crash.because("Failed to get number of fields from index writer")
                .ifThrows(writer::getNumFields))
        .log("Index writer initialized");
    return writer;
  }

  /** Creates a writer for a Lucene search index. */
  public static SingleLuceneIndexWriter createForSearchIndex(
      Directory directory,
      InstrumentedConcurrentMergeScheduler.PerIndexPartitionMergeScheduler mergeScheduler,
      MergePolicy mergePolicy,
      double ramBufferSizeMb,
      Optional<Integer> fieldLimit,
      Optional<Integer> docsLimit,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater,
      Optional<IndexDeletionPolicy> indexDeletionPolicy,
      FeatureFlags featureFlags)
      throws IOException {

    Similarity similarity = LuceneSimilarity.from(resolver.indexDefinition);

    Map<FieldPath, VectorFieldSpecification> pathToField =
        LuceneCodecUtils.extractVectorFieldsFromSearchMappings(resolver.getFields());

    IndexWriterConfig indexWriterConfig =
        new org.apache.lucene.index.IndexWriterConfig(indexAnalyzer)
            .setCodec(new LuceneCodec(pathToField))
            .setSimilarity(similarity)
            .setMergePolicy(mergePolicy)
            .setRAMBufferSizeMB(ramBufferSizeMb)
            .setMergeScheduler(mergeScheduler)
            .setCommitOnClose(false)
            .setIndexDeletionPolicy(
                indexDeletionPolicy.orElse(new KeepOnlyLastCommitDeletionPolicy()));
    // initializes the corresponding PerIndexMergeScheduler
    mergeScheduler.setInfoStream(indexWriterConfig.getInfoStream());

    if (resolver.indexDefinition.getSort().isPresent()
        && featureFlags.isEnabled(Feature.SORTED_INDEX)) {
      org.apache.lucene.search.Sort newIndexSort =
          new LuceneIndexSortFactory(resolver)
              .createIndexSort(resolver.indexDefinition.getSort().get());

      LuceneIndexSortCompatibilityValidator.validate(directory, newIndexSort);

      indexWriterConfig.setIndexSort(newIndexSort);
      // Parent field is set to sort parent-child documents as a unit.
      indexWriterConfig.setParentField(FieldName.MetaField.PARENT_FIELD.getLuceneFieldName());
    }

    org.apache.lucene.index.IndexWriter luceneIndexWriter =
        new org.apache.lucene.index.IndexWriter(directory, indexWriterConfig);

    LuceneIndexingPolicy luceneIndexingPolicy =
        DefaultIndexingPolicy.create(indexAnalyzer, resolver, indexingMetricsUpdater);

    SingleLuceneIndexWriter writer =
        new SingleLuceneIndexWriter(
            luceneIndexWriter,
            luceneIndexingPolicy,
            resolver.indexDefinition.getIndexId(),
            fieldLimit,
            docsLimit,
            resolver.indexCapabilities,
            indexingMetricsUpdater,
            Set.of(),
            featureFlags);

    writer
        .logger
        .atInfo()
        .addKeyValue("indexId", resolver.indexDefinition.getIndexId())
        .addKeyValue(
            "numFields",
            Crash.because("Failed to get number of fields from index writer")
                .ifThrows(writer::getNumFields))
        .log("Index writer initialized");
    return writer;
  }

  public org.apache.lucene.index.IndexWriter getLuceneWriter() {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("getLuceneWriter");
      return this.luceneWriter;
    }
  }

  /** Updates the index to reflect the changed document. */
  @Override
  public void updateIndex(DocumentEvent event) throws IOException, FieldExceededLimitsException {
    updateIndex(LuceneDocumentIdEncoder.encodeDocumentId(event.getDocumentId()), event);
  }

  public void updateIndex(byte[] encodedDocumentId, DocumentEvent event)
      throws IOException, FieldExceededLimitsException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("updateIndex");

      switch (event.getEventType()) {
        case DELETE -> handleDelete(encodedDocumentId, event);
        case INSERT, UPDATE -> handleInsertOrUpdate(encodedDocumentId, event);
      }
    }
  }

  @Override
  public void commit(EncodedUserData userData) throws IOException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("commit");
      LuceneCommitData commitData =
          new LuceneCommitData(LuceneCommitData.IndexWriterData.EMPTY, userData);
      this.luceneWriter.setLiveCommitData(commitData.toDataMapEntries());
      try {
        this.luceneWriter.commit();
      } catch (Exception e) {
        recordIndexingFailure(e, "commit");
        throw e;
      }
    }
  }

  public LuceneCommitData.IndexWriterData getInternalWriterData() {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("getInternalWriterData");
      LuceneCommitData commitData =
          LuceneCommitData.fromDataMap(Optional.ofNullable(this.luceneWriter.getLiveCommitData()));
      return commitData.getIndexWriterData();
    }
  }

  @Override
  public EncodedUserData getCommitUserData() {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("getCommitUserData");
      LuceneCommitData commitData =
          LuceneCommitData.fromDataMap(Optional.ofNullable(this.luceneWriter.getLiveCommitData()));
      return commitData.getEncodedUserData();
    }
  }

  @Override
  public void deleteAll(EncodedUserData userData) throws IOException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      LuceneCommitData commitData =
          new LuceneCommitData(new LuceneCommitData.IndexWriterData(true), userData);
      this.luceneWriter.setLiveCommitData(commitData.toDataMapEntries());
      try {
        this.luceneWriter.deleteAll();
      } catch (Exception e) {
        recordIndexingFailure(e, "deleteAll");
        throw e;
      }
      try {
        this.luceneWriter.commit();
      } catch (Exception e) {
        recordIndexingFailure(e, "commit");
        throw e;
      }
    }
  }

  @Override
  public void close() throws IOException {
    try (LockGuard ignored = LockGuard.with(this.shutdownExclusiveLock)) {
      if (this.closed) {
        return;
      }

      try {
        // Cancel running merges and block new merge scheduling before closing to avoid
        // blocking on long-running merges (e.g., HNSW graph building).
        // Note: Pending merges are not explicitly aborted; they will be discarded when
        // the IndexWriter closes. We call cancelMergesInternal() instead of cancelMerges()
        // to avoid the ensureOpen() check, since we're in the process of closing.
        // This behavior is controlled by the CANCEL_MERGE feature flag.
        if (this.featureFlags.isEnabled(Feature.CANCEL_MERGE)) {
          try {
            cancelMergesInternal();
          } catch (Exception e) {
            this.logger
                .atWarn()
                .setCause(e)
                .log("Failed to cancel merges during close, continuing with close anyway");
            // Continue with close even if cancelMerges fails
          }
        }

        this.luceneWriter.close();
      } catch (AlreadyClosedException e) {
        recordIndexingFailure(e, "close");
        this.logger.warn("Caught AlreadyClosedException when closing, ignoring.");
      } catch (Exception e) {
        recordIndexingFailure(e, "close");
        throw e;
      }
      this.closed = true;
    }
  }

  // The total number of docs in this index. Note that buffered deletions are not detracted from
  // this count until they are flushed to disk, so this number should be considered approximate.
  @Override
  public long getNumDocs() throws WriterClosedException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      if (this.closed) {
        throw WriterClosedException.create("getNumDocs");
      }

      return this.luceneWriter.getDocStats().numDocs;
    }
  }

  @Override
  public long getNumLuceneMaxDocs() throws WriterClosedException {
    return getMaxLuceneMaxDocs();
  }

  @Override
  public int getMaxLuceneMaxDocs() throws WriterClosedException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      if (this.closed) {
        throw WriterClosedException.create("getMaxLuceneMaxDocs");
      }

      return this.luceneWriter.getDocStats().maxDoc;
    }
  }

  @Override
  public Optional<ExceededLimitsException> exceededLimits() {
    // Check the field limit first as it is more restrictive than the document limit.
    // It also helps to release disk space because we mark such indexes as failed and eventually
    // drop them.
    // After that check for the docs limit exceeding. If exceeds then it the index transitions to
    // the stale state. Replication will be stopped but the index will be queryable.
    return exceededFieldLimits()
        .map(Function.<ExceededLimitsException>identity())
        .or(this::exceededDocsLimits);
  }

  public Optional<FieldExceededLimitsException> exceededFieldLimits() {
    if (this.fieldLimit.isEmpty()) {
      return Optional.empty();
    }

    int numberOfFields;
    try {
      numberOfFields = getNumFields();
    } catch (WriterClosedException e) {
      return Optional.empty();
    }

    int fieldLimit = this.fieldLimit.get();

    if (numberOfFields > fieldLimit) {
      String message = String.format("Field limit exceeded: %d > %d", numberOfFields, fieldLimit);
      return Optional.of(new FieldExceededLimitsException(message));
    }

    return Optional.empty();
  }

  public Optional<DocsExceededLimitsException> exceededDocsLimits() {
    return this.docsLimit.flatMap(
        limit -> {
          var numDocs = this.luceneWriter.getDocStats().maxDoc;
          if (numDocs > limit) {
            var message = String.format("Documents limit exceeded: %d > %d", numDocs, limit);
            return Optional.of(new DocsExceededLimitsException(message));
          }
          return Optional.empty();
        });
  }

  @Override
  public int getNumFields() throws WriterClosedException {
    return getFieldNames().size();
  }

  public Set<String> getFieldNames() throws WriterClosedException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      if (this.closed) {
        throw WriterClosedException.create("getFieldNames");
      }

      return this.luceneWriter.getFieldNames();
    }
  }

  @GuardedBy("shutdownSharedLock")
  private void handleDelete(byte[] encodedDocumentId, DocumentEvent event) throws IOException {
    if (encodedDocumentId.length > org.apache.lucene.index.IndexWriter.MAX_TERM_LENGTH) {
      this.logger
          .atWarn()
          .addKeyValue("maxTermLength", org.apache.lucene.index.IndexWriter.MAX_TERM_LENGTH)
          .log(
              "Unable to delete document due to its _id being larger "
                  + "than the maximum allowed byte length");
      return;
    }
    this.logger
        .atTrace()
        .addKeyValue("eventDocumentId", event.getDocumentId())
        .log("deleting document");
    try {
      this.luceneWriter.deleteDocuments(LuceneDocumentIdEncoder.documentIdTerm(encodedDocumentId));
    } catch (Exception e) {
      recordIndexingFailure(e, event.getEventType().name());
      throw e;
    }
  }

  @GuardedBy("shutdownSharedLock")
  private void handleInsertOrUpdate(byte[] encodedDocumentId, DocumentEvent event)
      throws IOException, FieldExceededLimitsException {
    RawBsonDocument document = Check.isPresent(event.getDocument(), "document");
    if (encodedDocumentId.length > org.apache.lucene.index.IndexWriter.MAX_TERM_LENGTH) {
      this.logger
          .atWarn()
          .addKeyValue("maxTermLength", org.apache.lucene.index.IndexWriter.MAX_TERM_LENGTH)
          .log(
              "Unable to delete document due to its _id being larger "
                  + "than the maximum allowed byte length");
      return;
    }

    this.logger
        .atTrace()
        .addKeyValue("eventDocumentId", event.getDocumentId())
        .log("indexing document");

    try {
      IndexingPolicyBuilderContext context =
          IndexingPolicyBuilderContext.builder()
              .autoEmbeddings(event.getAutoEmbeddings())
              .customVectorEngineId(event.getCustomVectorEngineId())
              .fieldPathsToFilterOut(this.fieldPathsToFilterOut)
              .build();
      DocumentBlockBuilder builder = this.indexingPolicy.createBuilder(encodedDocumentId, context);
      BsonDocumentProcessor.process(document, builder);

      List<Document> documentBlock = builder.buildBlock();

      if (this.fieldLimit.isPresent()) {
        // Try to fail early so we don't exceed field limits.
        // This condition is sufficient but not necessary to exceed field limits (imagine a document
        // passing this test but pushing the index over the limit). See exceededLimits().
        throwIfDocumentBlockExceedsFieldLimit(documentBlock, this.fieldLimit.get());
      }
      // It's possible that the document event we're seeing is an insert from the change stream
      // for a document that we already inserted during the initial sync collection scan. Also,
      // the same might happen after restart during ongoing indexing, because async background
      // commits might include events from a new batch, while resume token in CommitUserData
      // is from the previous (fully processed) batch. If we were simply to call addDocument for an
      // insert, in this situation we would end up having multiple documents in lucene with the same
      // _id. One future optimization could be to denote in the DocumentEvent whether or not it's
      // possible that this is a re-insert (i.e. we're still not up to minValidOpTime), and then
      // call addDocument when we know it's safe.
      try {
        this.luceneWriter.updateDocuments(
            LuceneDocumentIdEncoder.documentIdTerm(encodedDocumentId), documentBlock);
      } catch (Exception e) {
        recordIndexingFailure(e, event.getEventType().name());
        throw e;
      }
    } catch (Throwable e) {
      // Rate limiting the exception logging with `_id` inside the log message to help debugging.
      logIndexingFailureWithRateLimited(e, event.getEventType().name(), encodedDocumentId);
      throw e;
    }
  }

  private void throwIfDocumentBlockExceedsFieldLimit(List<Document> documentBlock, int fieldLimit)
      throws FieldExceededLimitsException {
    long fieldsIndexed =
        documentBlock.stream()
            .flatMap(d -> d.getFields().stream())
            .map(IndexableField::name)
            .distinct()
            .count();
    if (fieldsIndexed > fieldLimit) {
      String msg =
          String.format(
              "Number of fields exceeded limit in a document: %s > %s", fieldsIndexed, fieldLimit);
      throw new FieldExceededLimitsException(msg);
    }
  }

  private void ensureOpen(String methodName) {
    if (this.closed) {
      String message = String.format("Cannot call %s() after to close()", methodName);
      throw new IndexClosedException(message);
    }
  }

  private void recordIndexingFailure(Exception e, String operation) {
    this.indexingMetricsUpdater
        .getMetricsFactory()
        .counter(
            "IndexingInternalFailures",
            Tags.of("exceptionName", e.getClass().getSimpleName(), "operation", operation))
        .increment();
  }

  /**
   * Logs an indexing failure at a limited rate.
   *
   * <p>If the document ID cannot be decoded, it returns "unknown" to avoid PII leakage.
   *
   * <p>The entire method (including ID decoding) is rate limited to avoid unnecessary CPU overhead
   * when rate limit is triggered.
   *
   * @param e - the throwable that occurred
   * @param operation - the operation that failed
   * @param encodedDocumentId - the encoded document ID
   */
  private void logIndexingFailureWithRateLimited(
      Throwable e, String operation, byte[] encodedDocumentId) {
    // Rate limit the entire method to avoid ID decoding when rate limit is triggered
    if (!INDEXING_FAILURE_LOG_RATE_LIMITER.tryAcquire()) {
      return;
    }

    @Var String documentIdString;
    try {
      BsonValue documentId = LuceneDocumentIdEncoder.decodeDocumentId(encodedDocumentId);
      documentIdString = LoggableIdUtils.getLoggableId(documentId);
    } catch (Exception decodeException) {
      // If decoding fails, return "unknown" to avoid PII leakage
      documentIdString = LoggableIdUtils.UNKNOWN_LOGGABLE_ID;
    }
    doLogIndexingFailure(e, operation, documentIdString);
  }

  /**
   * Performs the actual logging of an indexing failure to allow testing.
   *
   * <p>This method is separated out to allow testing without rate limiting concerns.
   *
   * @param e - the throwable that occurred
   * @param operation - the operation that failed
   * @param documentIdString - the string representation of the document ID
   */
  @VisibleForTesting
  public void doLogIndexingFailure(Throwable e, String operation, String documentIdString) {
    this.logger
        .atError()
        .addKeyValue("operation", operation)
        .addKeyValue("documentId", documentIdString)
        .addKeyValue("exceptionType", e.getClass().getSimpleName())
        .log(
            "Indexing failure during %s operation for document with _id: %s",
            operation, documentIdString);
  }

  @Override
  public void cancelMerges() throws IOException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("cancelMerges");
      cancelMergesInternal();
    }
  }

  /**
   * Internal method to cancel merges without checking if the writer is open.
   * This is used by close() to cancel merges during the close process.
   */
  private void cancelMergesInternal() throws IOException {
    this.logger
        .atInfo()
        .log("Cancelling running merges and blocking new merge scheduling");

    try {
      // Get the per-index merge scheduler
      var mergeScheduler = this.luceneWriter.getConfig().getMergeScheduler();

      // The merge scheduler should be a PerIndexPartitionMergeScheduler
      if (mergeScheduler
          instanceof InstrumentedConcurrentMergeScheduler.PerIndexPartitionMergeScheduler) {
        var perIndexScheduler =
            (InstrumentedConcurrentMergeScheduler.PerIndexPartitionMergeScheduler) mergeScheduler;

        // Cancel merges only for this index partition
        // This will:
        // 1. Block new merges from being scheduled for this index
        // 2. Mark all running merges for this index as aborted
        // 3. Wait for running merge threads to detect the abort signal and stop
        // Note: Pending merges (queued in IndexWriter but not yet started) are not explicitly
        // aborted; they will be discarded when the IndexWriter is closed.
        boolean allMergesTerminated = perIndexScheduler.cancelMerges();
        if (allMergesTerminated) {
          this.logger
              .atInfo()
              .log("Successfully cancelled all merges");
        } else {
          this.logger
              .atWarn()
              .log(
                  "Merge cancellation completed but some merge threads are still running after "
                      + "timeout; index close may be slow");
        }
      } else {
        // Fallback: close() waits for current merges but does NOT block future merge scheduling.
        // This means the cancelMerges() contract is not fully upheld in this path - callers
        // should not rely on cancellation guarantees when using an unexpected scheduler type.
        // This fallback exists only for defensive coding; in practice, we always expect
        // PerIndexPartitionMergeScheduler.
        this.logger
            .atWarn()
            .addKeyValue("schedulerType", mergeScheduler.getClass().getName())
            .log(
                "Unexpected merge scheduler type, falling back to close() which waits for "
                    + "current merges but does not block future merge scheduling");
        mergeScheduler.close();
      }
    } catch (Exception e) {
      this.logger
          .atWarn()
          .setCause(e)
          .log("Error during merge cancellation");
      recordIndexingFailure(e, "cancelMerges");
      throw e;
    }
  }
}
