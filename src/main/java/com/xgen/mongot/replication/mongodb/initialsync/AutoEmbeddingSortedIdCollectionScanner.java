package com.xgen.mongot.replication.mongodb.initialsync;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
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
import com.xgen.mongot.util.mongodb.CollectionScanFindCommand;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
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
    this.preprocessingBatchTimer = metricsFactory.timerSec(PREPROCESSING_BATCH_DURATIONS);
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
  }

  @Override
  protected List<DocumentEvent> bufferNextBatch(
      CollectionScanMongoClient<InitialSyncException> mongoClient) throws InitialSyncException {
    List<RawBsonDocument> batch = mongoClient.getNext();

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

      BsonValue matViewDocId = matViewDocMetadata.getId().get();

      // the below util method allows us to compare BsonValues without being aware of the underlying
      // type.
      int comparison = MqlComparator.compareValues(sourceDocId, matViewDocId, ArrayComparator.MIN);

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
    return operations;
  }

  // Records that a document lacking its metadata namespace (e.g. duplicate fields, HELP-60413) was
  // skipped during the sort-merge, mirroring BufferlessCollectionScanner.
  // TO-DO: BufferlessCollectionScanner.bufferNextBatch still has an equivalent skip block; hoist
  // this into the shared superclass to remove the cross-class duplication.
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
      lowerBoundFilter = Filters.gte(ID_KEY, this.lastScannedToken);
    } else {
      lowerBoundFilter = Filters.gt(ID_KEY, this.lastScannedToken);
    }
    Bson filter;
    if (upperBound.isEmpty()) {
      // if no upper bound is provided, it means we've exhausted the base collection and thus
      // need to read till the end of the MV.
      filter = lowerBoundFilter;
    } else {
      // Otherwise, read up to the upper bound of the base collection batch
      Bson upperBoundFilter = Filters.lte(ID_KEY, upperBound.get());
      filter = Filters.and(lowerBoundFilter, upperBoundFilter);
    }

    builder.filter(filter);

    return this.mongoClient.getAutoEmbeddingResyncMongoClient(
        builder.build(),
        this.context.getIndexDefinition(),
        this.matViewNamespace,
        this.context.getInitialSyncMetricsUpdater());
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
