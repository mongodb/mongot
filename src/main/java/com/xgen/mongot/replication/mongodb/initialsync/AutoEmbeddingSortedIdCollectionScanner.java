package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.mongot.index.mongodb.MaterializedViewWriter.MV_DATABASE_NAME;

import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils;
import com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.lucene.query.pushdown.ArrayComparator;
import com.xgen.mongot.index.lucene.query.pushdown.MqlComparator;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.CollectionScanFindCommand;
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
  private final VectorIndexFieldMapping matViewFieldMappingWithHashes;
  private final MaterializedViewCollectionMetadata matViewCollectionMetadata;

  private boolean firstPage = true;

  /** Builds an AutoEmbeddingSortedIdCollectionScanner. */
  public AutoEmbeddingSortedIdCollectionScanner(
      Clock clock,
      InitialSyncContext context,
      InitialSyncMongoClient mongoClient,
      BsonValue lastScannedToken,
      MaterializedViewCollectionMetadataCatalog matViewCollectionMetadataCatalog,
      MetricsFactory metricsFactory) {
    super(clock, context, mongoClient, lastScannedToken, metricsFactory, false);
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
        new MongoNamespace(MV_DATABASE_NAME, this.matViewCollectionMetadata.collectionName());

    this.matViewFieldMappingWithHashes =
        AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
            resolveFieldMapping(context),
            this.matViewCollectionMetadata.schemaMetadata());
  }

  @Override
  protected List<DocumentEvent> bufferNextBatch(
      CollectionScanMongoClient<InitialSyncException> mongoClient) throws InitialSyncException {
    List<RawBsonDocument> batch = mongoClient.getNext();

    Optional<BsonValue> upperBound;
    if (mongoClient.hasNext()) {
      DocumentMetadata metadata = getDocumentMetadata(batch.getLast());
      upperBound = metadata.getId();
    } else {
      upperBound = Optional.empty();
    }

    List<RawBsonDocument> matViewBatch = getMatViewDocsInRange(upperBound);
    List<DocumentEvent> documentEvents = new ArrayList<>(sortMergeEvents(batch, matViewBatch));
    this.logger.info(
        "Detected "
            + documentEvents.size()
            + " events that need to be resynced to the MaterializedView.");

    // BufferlessCollectionScanner updates lastScannedToken only in the presence of non-empty
    // document event batches. In auto-embedding resync case, we can have empty document event
    // batches if the mat view is fully in sync or partial document event batches if the mat view is
    // partially in sync. In such cases, we still want to update the last scanned token based on the
    // scanned batch to ensure resync makes progress across scan iterations. We thus use the last
    // scanned token from the scanned batch instead of the document event batch in such scenarios.
    if (!batch.isEmpty()) {
      DocumentMetadata metadata = getDocumentMetadata(batch.getLast());
      this.lastScannedToken = metadata.getId().get();
    }
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
  private List<DocumentEvent> sortMergeEvents(
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
      // field.
      // TO-DO: look into pulling this logic into a common place to avoid code duplication.
      BsonValue sourceDocId = sourceDocMetadata.getId().get();
      if (sourceDocMetadata.getId().isEmpty()) {
        this.skippedDocumentsWithoutMetadataNamespaceCounter.increment();
        LOG.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
            "Document %s did not contain metadata namespace, likely due to "
                + "presence of duplicate fields, skipping indexing this document",
            Optional.ofNullable(sourceDoc).map(DocumentMetadata::extractId));
        baseIdx++;
        continue;
      }

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
                resolveFieldMapping(this.context),
                this.matViewFieldMappingWithHashes,
                this.matViewCollectionMetadata.schemaMetadata());
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
      DocumentMetadata metadata = getDocumentMetadata(baseBatch.get(baseIdx));
      operations.add(DocumentEvent.createInsert(metadata, baseBatch.get(baseIdx)));
      baseIdx++;
    }

    // Any remaining items in the matViewBatch are deletes.
    while (matViewIdx < matViewBatch.size()) {
      DocumentMetadata metadata = getDocumentMetadata(matViewBatch.get(matViewIdx));
      operations.add(DocumentEvent.createDelete(metadata.getId().get()));
      matViewIdx++;
    }
    return operations;
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

  // Helper method to extract the document metadata from the given document with the appropriate
  // handling for views.
  private static VectorIndexFieldMapping resolveFieldMapping(InitialSyncContext context) {
    return switch (context.getIndexDefinition()) {
      case VectorIndexDefinition vectorDef -> vectorDef.getMappings();
      case SearchIndexDefinition ignored ->
          // TODO(CLOUDP-353553): Support search auto-embedding in sorted ID collection scanner
          throw new UnsupportedOperationException(
              "Search auto-embedding not yet supported in sorted ID collection scanner");
    };
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
