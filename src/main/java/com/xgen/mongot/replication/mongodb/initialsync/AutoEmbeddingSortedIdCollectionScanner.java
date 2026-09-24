package com.xgen.mongot.replication.mongodb.initialsync;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.ibm.icu.text.RuleBasedCollator;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator;
import com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.query.pushdown.ArrayComparator;
import com.xgen.mongot.index.lucene.query.pushdown.MqlComparator;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.LoggableIdUtils;
import com.xgen.mongot.util.mongodb.CollectionScanFindCommand;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

/**
 * This class is responsible for implementing the initial sync for an auto-embedding materialized
 * view collection. This implementation does a co-ordinated sorted _id based full collection scan of
 * both the source collection and mat view collection, and performs a sort merge to generate the
 * final set of document events to index.
 */
public class AutoEmbeddingSortedIdCollectionScanner extends BufferlessCollectionScanner {

  private static final FluentLogger LOG = FluentLogger.forEnclosingClass();

  private final DefaultKeyValueLogger logger;
  private final MongoNamespace matViewNamespace;
  private final AutoEmbedFieldMapping autoEmbedMapping;
  private final AutoEmbedFieldMapping matViewAutoEmbedMapping;
  private final MaterializedViewCollectionMetadata matViewCollectionMetadata;
  private final Optional<FieldPath> sourceNestedRoot;

  private final Timer preprocessingBatchTimer;
  private final Counter skippedDocumentsWithoutMetadataNamespaceCounter;
  private final Counter resyncMissingDocuments;
  private final Counter resyncSurplusDocuments;
  private final Counter resyncStaleDocuments;
  private final Optional<RuleBasedCollator> stringCollator;
  private final Counter resyncIdOrderInversions;
  private TripwireMode tripwireMode = TripwireMode.REPORT;
  private boolean tripwireModeResolved;
  private final List<BsonValue> sourceBatchIds = new ArrayList<>();
  private Optional<BsonValue> previousBatchLastId = Optional.empty();
  private boolean firstPage = true;

  /** Builds an AutoEmbeddingSortedIdCollectionScanner. */
  @VisibleForTesting
  public AutoEmbeddingSortedIdCollectionScanner(
      Clock clock,
      InitialSyncContext context,
      InitialSyncMongoClient mongoClient,
      BsonValue lastScannedToken,
      MaterializedViewCollectionMetadataCatalog matViewCollectionMetadataCatalog,
      MetricsFactory metricsFactory) {
    super(clock, context, mongoClient, lastScannedToken, metricsFactory, false);
    this.preprocessingBatchTimer =
        metricsFactory.timerSec(CollectionScanBatchPreprocessor.PREPROCESSING_BATCH_DURATIONS);
    this.skippedDocumentsWithoutMetadataNamespaceCounter =
        metricsFactory.counter(
            CollectionScanBatchPreprocessor.SKIPPED_DOCUMENTS_WITHOUT_METADATA_NAMESPACE);
    this.resyncMissingDocuments = metricsFactory.counter("resyncMissingDocuments");
    this.resyncSurplusDocuments = metricsFactory.counter("resyncSurplusDocuments");
    this.resyncStaleDocuments = metricsFactory.counter("resyncStaleDocuments");
    this.resyncIdOrderInversions = metricsFactory.counter("resyncIdOrderInversions");
    Check.checkState(
        !context.useNaturalOrderScan(),
        "AutoEmbeddingSortedIdCollectionScanner should not be used with natural order scan");
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", context.getIndexId());
    defaultKeyValues.put("generationId", context.getGenerationId());
    this.logger =
        DefaultKeyValueLogger.getLogger(
            AutoEmbeddingSortedIdCollectionScanner.class, defaultKeyValues);
    // TODO(CLOUDP-363914): matViewSchemaMetadata can be different if we want to reuse different MV
    // collection to build new MV collection
    this.matViewCollectionMetadata =
        Check.isPresent(
            matViewCollectionMetadataCatalog.getMetadataIfPresent(context.getGenerationId()),
            "matViewCollectionMetadata");
    // TODO(CLOUDP-363914): collectionName can be different if we want to reuse different MV
    // collection to build new MV collection
    this.matViewNamespace =
        new MongoNamespace(
            matViewCollectionMetadataCatalog.getDatabaseName(context.getGenerationId()),
            this.matViewCollectionMetadata.collectionName());

    IndexDefinition indexDef = context.getIndexDefinition();
    this.autoEmbedMapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDef);
    this.matViewAutoEmbedMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            indexDef, this.matViewCollectionMetadata.schemaMetadata());
    this.sourceNestedRoot =
        indexDef instanceof VectorIndexDefinition vectorIndexDefinition
            ? vectorIndexDefinition.getNestedRoot()
            : Optional.empty();
    // The mat view's collation is carried on the metadata (set deterministically by the resolver
    // from the mat view's collection info), so there is no network read or best-effort fallback
    // here. A simple/absent collation yields empty: the binary (UTF-8) comparator already matches.
    this.stringCollator =
        this.matViewCollectionMetadata.collation().flatMap(CollationUtils::toIcuCollator);
  }

  // Compares two _id values for the sort-merge. With a non-simple source collation both the source
  // cursor and the mat-view window are served in collation order, so string _ids must be compared
  // with the ICU collator to interleave them correctly. Every other case (simple/absent collation,
  // or
  // non-string values) uses the binary comparator, which matches a simple-collation _id index.
  private int compareIds(BsonValue sourceDocId, BsonValue matViewDocId) {
    return compareIds(sourceDocId, matViewDocId, this.stringCollator);
  }

  // Shared by the sort-merge and the id-order tripwire: order string-like _ids by the ICU collator
  // when a non-simple source collation is in scope, else the binary comparator. The tripwire
  // compares
  // source batch ids under this same comparator, so it flags when the server's order diverges from
  // the merge's — an ICU parity gap (e.g. th) or an unresolvable collation that fell back to
  // binary.
  private static int compareIds(
      BsonValue sourceDocId, BsonValue matViewDocId, Optional<RuleBasedCollator> stringCollator) {
    if (stringCollator.isPresent() && isStringLike(sourceDocId) && isStringLike(matViewDocId)) {
      String sourceValue = stringValue(sourceDocId);
      String matViewValue = stringValue(matViewDocId);
      // Fast path for the common in-sync resync: identical strings are equal without the ICU
      // collator, so we skip the tie-break below (which runs on every 0 comparison).
      if (sourceValue.equals(matViewValue)) {
        return 0;
      }
      int cmp = stringCollator.get().compare(sourceValue, matViewValue);
      if (cmp != 0) {
        return cmp;
      }
      // Safety net rather than a real case: MongoDB enforces collation-unique _id, so two
      // byte-distinct but collation-equal string _ids cannot coexist in one collection (the
      // duplicate-key error rejects the second insert). If a collator still rates two distinct
      // _ids equal, do not collapse them into "the same doc" — tie-break by code point byte order
      // (the order a simple collation uses), so the merge always treats distinct _ids as distinct.
      return compareByCodePoint(sourceValue, matViewValue);
    }
    return MqlComparator.compareValues(sourceDocId, matViewDocId, ArrayComparator.MIN);
  }

  // A collection collator orders both STRING and SYMBOL (legacy) _ids; falling back to byte order
  // for
  // a SYMBOL _id would diverge from the collated mat-view index.
  private static boolean isStringLike(BsonValue value) {
    BsonType type = value.getBsonType();
    return type == BsonType.STRING || type == BsonType.SYMBOL;
  }

  private static String stringValue(BsonValue value) {
    return value.getBsonType() == BsonType.SYMBOL
        ? value.asSymbol().getSymbol()
        : value.asString().getValue();
  }

  // Compares strings by Unicode code point, which is byte-identical to UTF-8 byte order (UTF-8
  // preserves code-point order) without materializing a byte[] per comparison. Used only as the
  // tie-break when the ICU collator rates two distinct _ids equal (impossible in a valid collated
  // collection, since MongoDB enforces collation-unique _id).
  private static int compareByCodePoint(String left, String right) {
    @Var int leftIndex = 0;
    @Var int rightIndex = 0;
    while (leftIndex < left.length() && rightIndex < right.length()) {
      int leftCodePoint = left.codePointAt(leftIndex);
      int rightCodePoint = right.codePointAt(rightIndex);
      if (leftCodePoint != rightCodePoint) {
        return Integer.compare(leftCodePoint, rightCodePoint);
      }
      leftIndex += Character.charCount(leftCodePoint);
      rightIndex += Character.charCount(rightCodePoint);
    }
    return Integer.compare(left.length() - leftIndex, right.length() - rightIndex);
  }

  @Override
  protected List<DocumentEvent> bufferNextBatch(
      CollectionScanMongoClient<InitialSyncException> mongoClient) throws InitialSyncException {
    List<RawBsonDocument> batch = mongoClient.getNext();
    this.sourceBatchIds.clear();

    // The last scanned doc's id bounds both the mat-view query and the resume token; see
    // lastScannedIdOf for which id (metadata namespace vs root) is used and why.
    Optional<BsonValue> lastScannedId =
        batch.isEmpty() ? Optional.empty() : lastScannedIdOf(batch.getLast());
    Optional<BsonValue> upperBound = mongoClient.hasNext() ? lastScannedId : Optional.empty();

    // For autoEmbedding indexes, the preprocessing timer measures the time to fetch the relevant
    // docs from the mat view collection and do the sort merge.
    var preprocessingTimer = Stopwatch.createStarted();
    List<RawBsonDocument> matViewBatch = getMatViewDocsInRange(upperBound);
    List<DocumentEvent> documentEvents = new ArrayList<>(sortMergeEvents(batch, matViewBatch));
    // Include the previous batch's last id so the boundary pair that straddles a getMore boundary
    // (batch sizes are arbitrary) is checked too, not just pairs within one batch.
    runIdOrderTripwire(
        findFirstInversion(
            withPreviousBatchLastId(this.previousBatchLastId, this.sourceBatchIds),
            this.stringCollator));
    this.previousBatchLastId =
        this.sourceBatchIds.isEmpty()
            ? this.previousBatchLastId
            : Optional.of(this.sourceBatchIds.get(this.sourceBatchIds.size() - 1));
    this.preprocessingBatchTimer.record(preprocessingTimer.stop().elapsed());
    this.logger.debug(
        "Detected "
            + documentEvents.size()
            + " events that need to be resynced to the MaterializedView.");

    // BufferlessCollectionScanner updates lastScannedToken only in the presence of non-empty
    // document event batches. In auto-embedding resync case, we can have empty document event
    // batches if the mat view is fully in sync or partial document event batches if the mat view is
    // partially in sync. In such cases, we still want to update the last scanned token based on the
    // scanned batch to ensure resync makes progress across scan iterations. We thus use the last
    // scanned token from the scanned batch instead of the document event batch in such scenarios.
    lastScannedId.ifPresent(id -> this.lastScannedToken = id);
    this.firstPage = false;

    return documentEvents;
  }

  // CLOUDP-447390 / CLOUDP-444644: detect when a source batch's _id order is not monotonic under
  // the comparator the merge uses (compareIds -> the ICU collator when a non-simple source
  // collation
  // is in scope, else binary). A non-monotonic batch means the server's order disagrees with the
  // merge's — an ICU parity gap (e.g. th) or an unresolvable collation that fell back to binary —
  // which can make overlapping windows silently drop documents. Runs for any string-like or mixed
  // _id (compareIds handles STRING, SYMBOL and mixed types); the previous batch's last id is
  // carried
  // across getMore boundaries via withPreviousBatchLastId. Always runs in REPORT mode (record a
  // metric + warn and continue); the FAIL_FAST feature flag escalates to aborting the resync.
  //
  // Source ids are collected during sortMergeEvents (which already extracts each source id), so the
  // check reuses the merge's iteration instead of a second pass.
  private void runIdOrderTripwire(Optional<IdOrderInversion> inversion)
      throws InitialSyncException {
    if (inversion.isEmpty()) {
      return;
    }
    TripwireMode mode = tripwireMode();
    IdOrderInversion found = inversion.get();
    this.resyncIdOrderInversions.increment();
    LOG.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
        "AE resync source batch _id order is not monotonic under the MV-window comparator "
            + "(id-order mismatch, CLOUDP-444644): previous=%s, current=%s at index %d, mode=%s.",
        LoggableIdUtils.getLoggableId(found.previous()),
        LoggableIdUtils.getLoggableId(found.current()),
        found.index(),
        mode);
    if (mode == TripwireMode.FAIL) {
      // Message is intentionally generic (no raw _id): with retain-on-failure it surfaces as the
      // index status message to customers, so only the index + cause hint are safe to expose.
      throw InitialSyncException.createRequiresResync(
          "AE resync aborted by the id-order tripwire: source _id order is not monotonic "
              + "under the MV-window comparator at index "
              + found.index()
              + ". The source collection likely has a non-simple collation; see CLOUDP-444644.");
    }
  }

  // Resolved lazily so the existing scanner unit tests (which build a scanner with a mocked context
  // that does not stub getFeatureFlags()) do not surface a null on the path they never exercise.
  // The
  // context's FeatureFlags are fixed for the process, so the mode cannot change mid-scan.
  private TripwireMode tripwireMode() {
    if (!this.tripwireModeResolved) {
      this.tripwireMode =
          this.context.getFeatureFlags().isEnabled(Feature.AE_RESYNC_ID_ORDER_TRIPWIRE_FAIL_FAST)
              ? TripwireMode.FAIL
              : TripwireMode.REPORT;
      this.tripwireModeResolved = true;
    }
    return this.tripwireMode;
  }

  /**
   * First source batch position where the previous id sorts after next id under the comparator the
   * merge uses (the ICU collator when a non-simple source collation is in scope, else binary). A
   * non-monotonic source batch means the server's order disagrees with the merge's comparator — an
   * ICU parity gap (e.g. th) or an unresolvable collation that fell back to binary.
   */
  @VisibleForTesting
  static Optional<IdOrderInversion> findFirstInversion(
      List<BsonValue> ids, Optional<RuleBasedCollator> stringCollator) {
    for (int i = 1; i < ids.size(); i++) {
      BsonValue previous = ids.get(i - 1);
      BsonValue current = ids.get(i);
      if (compareIds(previous, current, stringCollator) > 0) {
        return Optional.of(new IdOrderInversion(previous, current, i));
      }
    }
    return Optional.empty();
  }

  // Collects a source id for the id-order tripwire. Every source id is collected (compareIds
  // handles STRING, SYMBOL and mixed types), so findFirstInversion can check the full ordered run.
  private void recordSourceId(Optional<BsonValue> id) {
    id.ifPresent(this.sourceBatchIds::add);
  }

  // Prepends the previous batch's last id so findFirstInversion also checks the adjacent pair that
  // straddles a getMore boundary (batch sizes are arbitrary, so the mismatch could land there).
  @VisibleForTesting
  static List<BsonValue> withPreviousBatchLastId(
      Optional<BsonValue> previousBatchLastId, List<BsonValue> currentBatchIds) {
    if (previousBatchLastId.isEmpty() || currentBatchIds.isEmpty()) {
      return currentBatchIds;
    }
    List<BsonValue> result = new ArrayList<>(currentBatchIds.size() + 1);
    result.add(previousBatchLastId.get());
    result.addAll(currentBatchIds);
    return result;
  }

  @VisibleForTesting
  record IdOrderInversion(BsonValue previous, BsonValue current, int index) {}

  /** How the id-order tripwire acts on a detected inversion: REPORT (default) or FAIL. */
  enum TripwireMode {
    REPORT,
    FAIL
  }

  @Override
  protected void updateLastScannedToken(
      List<DocumentEvent> batch, Optional<BsonDocument> postBatchResumeToken) {
    // No-op. We update the last scanned token based on the scanned batch in bufferNextBatch.
  }

  // Returns the documents from the mat view collection in the range (lastScannedToken, upperBound)
  private List<RawBsonDocument> getMatViewDocsInRange(Optional<BsonValue> upperBound)
      throws InitialSyncException {
    var findClient = getFindCommandClient(upperBound);
    List<RawBsonDocument> matViewBatch = new ArrayList<>();
    while (findClient.hasNext()) {
      matViewBatch.addAll(findClient.getNext());
    }
    findClient.close();
    return matViewBatch;
  }

  // Performs a co-ordinated iteration of both the source collection and mat view collection
  // documents and identifies the differences. Conceptually similar to the merge sort algorithm.
  @VisibleForTesting
  List<DocumentEvent> sortMergeEvents(
      List<RawBsonDocument> baseBatch, List<RawBsonDocument> matViewBatch) {
    @Var int baseIdx = 0;
    @Var int matViewIdx = 0;
    List<DocumentEvent> operations = new ArrayList<>();
    while (baseIdx < baseBatch.size() && matViewIdx < matViewBatch.size()) {
      RawBsonDocument sourceDoc = baseBatch.get(baseIdx);
      RawBsonDocument matViewDoc = matViewBatch.get(matViewIdx);
      DocumentMetadata sourceDocMetadata = getDocumentMetadata(sourceDoc);
      // Documents in the mat view are already created with the view definition if present,
      // so we can use them as is.
      DocumentMetadata matViewDocMetadata =
          DocumentMetadata.fromOriginalDocument(Optional.of(matViewDoc));

      // Replicating the logic from BufferlessCollectionScanner to skip documents without an ID
      // field (e.g. a missing metadata namespace caused by duplicate fields, HELP-60413).
      // Only baseIdx advances (not matViewIdx), so the skipped doc's mat-view entry is left
      // unmatched and gets deleted as a surplus doc — a source doc we can't read is dropped from
      // the index, matching how the steady-state change-stream path treats it.
      if (sourceDocMetadata.getId().isEmpty()) {
        recordSkippedDocumentWithoutMetadataNamespace(sourceDoc);
        baseIdx++;
        continue;
      }
      BsonValue sourceDocId = sourceDocMetadata.getId().get();
      recordSourceId(Optional.of(sourceDocId));

      BsonValue matViewDocId = matViewDocMetadata.getId().get();

      // compare BsonValues without being aware of the underlying type, but order string _ids by the
      // source collection's collation when it has a non-simple collation (see compareIds).
      int comparison = compareIds(sourceDocId, matViewDocId);

      if (comparison < 0) {
        // Doc is in the source collection but not mat view.
        operations.add(DocumentEvent.createInsert(sourceDocMetadata, sourceDoc));
        baseIdx++;
      } else if (comparison > 0) {
        // Doc is in the mat view but not the source collection.
        operations.add(DocumentEvent.createDelete(matViewDocId));
        matViewIdx++;
      } else {
        // Doc is in both source collection and mat view.

        var comparisonResult =
            AutoEmbeddingDocumentUtils.compareDocuments(
                sourceDoc,
                matViewDoc,
                this.autoEmbedMapping,
                this.matViewAutoEmbedMapping,
                this.matViewCollectionMetadata.schemaMetadata(),
                this.sourceNestedRoot);
        if (comparisonResult.needsReIndexing()) {
          var rawDocumentEvent = DocumentEvent.createUpdate(sourceDocMetadata, sourceDoc);
          // Check if we have any re-usable embeddings to pass in.
          if (comparisonResult.reusableEmbeddings().isEmpty()) {
            operations.add(rawDocumentEvent);
          } else {
            operations.add(
                DocumentEvent.createFromDocumentEventAndVectors(
                    rawDocumentEvent, comparisonResult.reusableEmbeddings()));
          }
        }
        baseIdx++;
        matViewIdx++;
      }
    }

    // Any remaining items in baseBatch are inserts.
    while (baseIdx < baseBatch.size()) {
      RawBsonDocument sourceDoc = baseBatch.get(baseIdx);
      DocumentMetadata metadata = getDocumentMetadata(sourceDoc);
      // A source doc whose metadata namespace is missing (duplicate fields break mongod's $project,
      // HELP-60413) has an empty id. Skip it here the same way the main loop does: otherwise
      // createInsert below calls Check.isPresent(getId()) and throws, aborting the whole resync.
      if (metadata.getId().isEmpty()) {
        recordSkippedDocumentWithoutMetadataNamespace(sourceDoc);
        baseIdx++;
        continue;
      }
      recordSourceId(metadata.getId());
      operations.add(DocumentEvent.createInsert(metadata, sourceDoc));
      baseIdx++;
    }

    // Any remaining items in the matViewBatch are deletes. Read the _id with fromOriginalDocument,
    // NOT getDocumentMetadata: mat-view docs are keyed by _id at the root and carry no <indexId>
    // metadata namespace, so for a view-based index getDocumentMetadata would dispatch to
    // fromMetadataNamespace and throw NoSuchElementException on the absent namespace. This matches
    // how the main loop above reads mat-view docs.
    while (matViewIdx < matViewBatch.size()) {
      DocumentMetadata metadata =
          DocumentMetadata.fromOriginalDocument(Optional.of(matViewBatch.get(matViewIdx)));
      operations.add(DocumentEvent.createDelete(metadata.getId().get()));
      matViewIdx++;
    }
    recordDivergenceMetrics(operations);
    return operations;
  }

  /**
   * Makes the otherwise-silent self-heal observable by counting the divergences this sort-merge
   * detected between the source collection and the materialized view, by class:
   *
   * <ul>
   *   <li>{@code missing} — in the source but not the MV (an {@link DocumentEvent.EventType#INSERT}
   *       heal action);
   *   <li>{@code surplus} — an MV entry that the scan deletes (a {@link
   *       DocumentEvent.EventType#DELETE}). This is usually "in the MV but not the source", but it
   *       also covers the MV entry of a source doc that was <em>skipped as unreadable</em> (missing
   *       metadata namespace, e.g. HELP-60413 duplicate fields): such a doc exists in the source
   *       yet is dropped from the index, so {@code surplus} is not strictly "not in source";
   *   <li>{@code stale} — present in both but with content that needs re-indexing (an {@link
   *       DocumentEvent.EventType#UPDATE}).
   * </ul>
   *
   * <p>On a healthy, already-populated MV a resync emits none of these. Note {@code missing} is
   * dominated by expected back-fill during the first full build of an MV (the MV starts empty, so
   * every source document is an insert); it is meaningful as a drift signal when scanning an
   * already-populated MV (e.g. a resync after oplog fall-off). {@code stale} is ~zero on a first
   * build and is an unambiguous drift signal; {@code surplus} is ~zero on a first build too but,
   * per above, also tracks skipped-unreadable-doc deletes, so a non-zero value is not by itself
   * proof of MV/source drift (correlate with the skipped-document counter).
   */
  private void recordDivergenceMetrics(List<DocumentEvent> operations) {
    @Var long missing = 0;
    @Var long surplus = 0;
    @Var long stale = 0;
    for (DocumentEvent event : operations) {
      switch (event.getEventType()) {
        case INSERT -> missing++;
        case DELETE -> surplus++;
        case UPDATE -> stale++;
      }
    }
    if (missing > 0) {
      this.resyncMissingDocuments.increment(missing);
    }
    if (surplus > 0) {
      this.resyncSurplusDocuments.increment(surplus);
    }
    if (stale > 0) {
      this.resyncStaleDocuments.increment(stale);
    }
  }

  // Records that a document lacking its metadata namespace (e.g. duplicate fields, HELP-60413) was
  // skipped during the sort-merge, mirroring CollectionScanBatchPreprocessor.
  // TO-DO: CollectionScanBatchPreprocessor has an equivalent skip block; route this through it to
  // remove the cross-class duplication.
  private void recordSkippedDocumentWithoutMetadataNamespace(RawBsonDocument doc) {
    this.skippedDocumentsWithoutMetadataNamespaceCounter.increment();
    LOG.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
        "Document %s did not contain metadata namespace, likely due to "
            + "presence of duplicate fields, skipping indexing this document",
        Optional.ofNullable(doc).map(DocumentMetadata::extractId));
  }

  private CollectionScanMongoClient<InitialSyncException> getFindCommandClient(
      Optional<BsonValue> upperBound) throws InitialSyncException {

    var highWaterMark = this.context.getChangeStreamResumeOperationTime();

    var builder =
        new CollectionScanFindCommand.Builder(this.matViewNamespace.getCollectionName())
            .readConcern(ReadConcern.MAJORITY, highWaterMark)
            .sort(Sorts.ascending(ID_KEY))
            .hint(Indexes.ascending(ID_KEY));

    Bson lowerBoundFilter;
    // This logic around the first page is to mimic the logic of the source collection scan. We
    // always use the inclusive bound for the first page and exclusive bound for subsequent pages.
    // This is handled automatically with the source collection scan since it just uses an open
    // cursor, but we need to manage it explicitly as our find commands are stateless and do not
    // rely on the cursor.
    if (this.firstPage) {
      lowerBoundFilter = typeUnbracketedRangeFilter("$gte", this.lastScannedToken);
    } else {
      lowerBoundFilter = typeUnbracketedRangeFilter("$gt", this.lastScannedToken);
    }
    Bson filter;
    if (upperBound.isEmpty()) {
      // if no upper bound is provided, it means we've exhausted the base collection and thus
      // need to read till the end of the MV.
      filter = lowerBoundFilter;
    } else {
      // Otherwise, read up to the upper bound of the base collection batch
      Bson upperBoundFilter = typeUnbracketedRangeFilter("$lte", upperBound.get());
      filter = Filters.and(lowerBoundFilter, upperBoundFilter);
    }

    builder.filter(filter);

    return this.mongoClient.getAutoEmbeddingResyncMongoClient(
        builder.build(),
        this.context.getIndexDefinition(),
        this.matViewNamespace,
        this.context.getInitialSyncMetricsUpdater());
  }

  private static Bson typeUnbracketedRangeFilter(String operator, BsonValue bound) {
    // Mirrors the source scan's $match/$expr range (see CollectionScanAggregateCommand): plain $gt,
    // $gte and $lte are type-bracketed, so a window that straddles a mixed-type _id boundary
    // returns
    // no MV docs (every source doc re-inserted, other-type surplus docs never deleted). $expr
    // avoids
    // type bracketing. The _id hint below forces the index to bound the query.
    return new BsonDocument(
        "$expr", new BsonDocument(operator, new BsonArray(List.of(new BsonString("$_id"), bound))));
  }

  // Resolves the id of the last scanned doc, used to bound the next scan iteration (the mat-view
  // query bound and the resume token). Prefer the metadata-namespace id: for a view index that is
  // the original pre-view _id, which is the space the scan sort/resume and the mat view are keyed
  // in. The view pipeline may rewrite the root _id (e.g. a $set on a dotted _id path), so only fall
  // back to the root _id when the metadata namespace is absent (HELP-60413 duplicate fields), to
  // let a namespace-less last doc advance the scan instead of throwing.
  @VisibleForTesting
  Optional<BsonValue> lastScannedIdOf(RawBsonDocument doc) {
    return getDocumentMetadata(doc)
        .getId()
        .or(() -> DocumentMetadata.fromOriginalDocument(Optional.of(doc)).getId());
  }

  private DocumentMetadata getDocumentMetadata(RawBsonDocument doc) {
    if (this.context.getIndexDefinition().getView().isPresent()) {
      return DocumentMetadata.fromMetadataNamespace(
          Optional.ofNullable(doc), this.context.getIndexId());
    } else {
      return DocumentMetadata.fromOriginalDocument(Optional.ofNullable(doc));
    }
  }
}
