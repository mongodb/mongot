package com.xgen.mongot.replication.mongodb.common;

import com.google.common.collect.Lists;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.definition.FieldDefinitionResolver;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.LazyTransformationList;
import com.xgen.mongot.util.LazyTransformationList.Transformer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeStreamDocumentUtils {

  private static final CodecRegistry REGISTRY = MongoClientSettings.getDefaultCodecRegistry();

  private static final Codec<ChangeStreamDocument<RawBsonDocument>> CHANGE_STREAM_DOCUMENT_CODEC =
      ChangeStreamDocument.createCodec(RawBsonDocument.class, REGISTRY);

  private static final FluentLogger LOG = FluentLogger.forEnclosingClass();
  private static final Logger logger = LoggerFactory.getLogger(ChangeStreamDocumentUtils.class);

  public static class DocumentEventBatch {
    public final List<DocumentEvent> finalChangeEvents;
    public final int updatesWitnessed;
    public final int updatesApplicable;
    public final int skippedDocumentsWithoutMetadataNamespace;
    public final int applicableDocumentsTotal;
    public final long applicableDocumentsTotalBytes;

    private DocumentEventBatch(
        List<DocumentEvent> finalChangeEvents,
        int updatesWitnessed,
        int updatesApplicable,
        int skippedDocumentsWithoutMetadataNamespace,
        int applicableDocumentsTotal,
        long applicableDocumentsTotalBytes) {
      this.finalChangeEvents = finalChangeEvents;
      this.updatesWitnessed = updatesWitnessed;
      this.updatesApplicable = updatesApplicable;
      this.skippedDocumentsWithoutMetadataNamespace = skippedDocumentsWithoutMetadataNamespace;
      this.applicableDocumentsTotal = applicableDocumentsTotal;
      this.applicableDocumentsTotalBytes = applicableDocumentsTotalBytes;
    }
  }

  private static class ChangeStreamBatchMetrics {
    private int witnessedUpdatesCounter;
    private int applicableUpdatesCounter;
    private int skippedDocumentWithoutMetadataNamespaceCounter;
    private int applicableDocumentsTotalCounter;
    private long applicableDocumentsTotalBytesCounter;

    public ChangeStreamBatchMetrics() {
      this.witnessedUpdatesCounter = 0;
      this.applicableUpdatesCounter = 0;
      this.skippedDocumentWithoutMetadataNamespaceCounter = 0;
      this.applicableDocumentsTotalCounter = 0;
      this.applicableDocumentsTotalBytesCounter = 0;
    }

    public void updateWitnessed() {
      this.witnessedUpdatesCounter++;
    }

    public void updateApplicable() {
      this.applicableUpdatesCounter++;
    }

    public void updateSkippedDocumentWithoutMetadata() {
      this.skippedDocumentWithoutMetadataNamespaceCounter++;
    }

    public void applicableDocumentsTotal() {
      this.applicableDocumentsTotalCounter++;
    }

    public void applicableDocumentsTotalBytes(long bytes) {
      this.applicableDocumentsTotalBytesCounter += bytes;
    }

    public int getWitnessedUpdates() {
      return this.witnessedUpdatesCounter;
    }

    public int getApplicableUpdates() {
      return this.applicableUpdatesCounter;
    }

    public int getSkippedDocumentsWithoutMetadataNamespace() {
      return this.skippedDocumentWithoutMetadataNamespaceCounter;
    }

    public int getApplicableDocumentsTotal() {
      return this.applicableDocumentsTotalCounter;
    }

    public long getApplicableDocumentsTotalBytes() {
      return this.applicableDocumentsTotalBytesCounter;
    }
  }

  /** Returns the opTime from the postBatchResumeToken of a change stream batch. */
  public static BsonTimestamp opTimeFromBatch(ChangeStreamBatch batch) {
    BsonDocument resume = batch.getPostBatchResumeToken();
    return Crash.because("failed to parse optime out of resume token")
        .ifThrows(() -> ResumeTokenUtils.opTimeFromResumeToken(resume));
  }

  /** Converts a ChangeStreamDocument into it's Bson representation. */
  public static RawBsonDocument changeStreamDocumentToBsonDocument(
      ChangeStreamDocument<RawBsonDocument> changeStreamDocument) {
    // We implement this by serializing a ChangeStreamDocument into a RawBsonDocument.
    try (BasicOutputBuffer buffer = new BasicOutputBuffer()) {
      try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
        CHANGE_STREAM_DOCUMENT_CODEC.encode(
            writer, changeStreamDocument, BsonUtils.DEFAULT_FAST_CONTEXT);

        return new RawBsonDocument(buffer.getInternalBuffer());
      }
    }
  }

  /**
   * Converts a bsonDocument that represents a ChangeStreamDocument back into the
   * ChangeStreamDocument.
   */
  public static ChangeStreamDocument<RawBsonDocument> bsonDocumentToChangeStreamDocument(
      BsonDocument bsonDocument) {
    return CHANGE_STREAM_DOCUMENT_CODEC.decode(
        bsonDocument.asBsonReader(), DecoderContext.builder().build());
  }

  /**
   * Converts a list of {@link RawBsonDocument} into a list of {@link ChangeStreamDocument}. Each
   * object is lazily decoded on traversal.
   *
   * <p>Note:Elements are decoded on each get. Therefore, using this list could be expensive if list
   * is fully traversed more than once.
   */
  public static List<ChangeStreamDocument<RawBsonDocument>> asLazyDecodableChangeStreamDocuments(
      List<RawBsonDocument> rawChangeStreamEvents) {
    return LazyTransformationList.create(
        rawChangeStreamEvents, (raw) -> raw.decode(CHANGE_STREAM_DOCUMENT_CODEC));
  }

  /**
   * Similar to function above. In addition, allows callers to pass a validation function to execute
   * on each element as the list is traversed. Errors will be thrown per element.
   */
  public static <E extends Exception>
      List<ChangeStreamDocument<RawBsonDocument>>
          asLazyDecodableChangeStreamDocumentsWithEventValidation(
              List<RawBsonDocument> rawChangeStreamEvents,
              ChangeStreamEventChecker<E> eventChecker) {
    return LazyTransformationList.create(
        rawChangeStreamEvents,
        eventValidationTransformer(
            (raw) -> raw.decode(CHANGE_STREAM_DOCUMENT_CODEC), eventChecker));
  }

  public static void recordChangeStreamEventSizes(
      List<RawBsonDocument> batch, Consumer<Integer> eventSizeRecorder) {
    for (RawBsonDocument rawDoc : batch) {
      int size = rawDoc.getByteBuffer().remaining();
      eventSizeRecorder.accept(size);
    }
  }

  /**
   * Look backwards through the batch, which should only contain document events. Return a list of
   * {@link DocumentEvent} representing the final valid document event in this batch for each
   * documentId in the batch.
   *
   * <p>We will skip updates that do not touch fields that are in this index definition.
   */
  public static DocumentEventBatch handleDocumentEvents(
      List<ChangeStreamDocument<RawBsonDocument>> batch,
      IndexDefinition indexDefinition,
      FieldDefinitionResolver fieldDefinitionResolver,
      // TODO(CLOUDP-324852): Remove this parameter once initial sync and steady state logic are
      //  aligned.
      boolean areUpdateEventsPrefiltered) {

    ChangeStreamBatchMetrics metrics = new ChangeStreamBatchMetrics();
    Set<BsonDocument> processedDocs = new HashSet<>();

    List<DocumentEvent> finalChangeEvents = new ArrayList<>();

    for (var event : Lists.reverse(batch)) {

      Check.stateNotNull(
          event.getDocumentKey(), "Found event with no key in handleDocumentEvents.");

      // If we haven't already created a DocumentEvent for the latest valid change to this document.
      if (!processedDocs.contains(event.getDocumentKey())) {

        Optional<DocumentEvent> docEvent =
            getDocumentEvent(
                event,
                indexDefinition,
                fieldDefinitionResolver,
                metrics,
                areUpdateEventsPrefiltered);
        if (docEvent.isPresent()) {
          finalChangeEvents.add(docEvent.get());

          processedDocs.add(event.getDocumentKey());
        }
      }
    }

    return new DocumentEventBatch(
        finalChangeEvents,
        metrics.getWitnessedUpdates(),
        metrics.getApplicableUpdates(),
        metrics.getSkippedDocumentsWithoutMetadataNamespace(),
        metrics.getApplicableDocumentsTotal(),
        metrics.getApplicableDocumentsTotalBytes());
  }

  /**
   * Returns the corresponding {@link DocumentEvent} to be indexed, based on the {@link
   * ChangeStreamDocument}.
   */
  private static Optional<DocumentEvent> getDocumentEvent(
      ChangeStreamDocument<RawBsonDocument> event,
      IndexDefinition indexDefinition,
      FieldDefinitionResolver fieldDefinitionResolver,
      ChangeStreamBatchMetrics metrics,
      boolean areUpdateEventsPrefiltered) {

    DocumentMetadata metadata = createMetadata(event, indexDefinition);

    if (indexDefinition.getView().isPresent() && !metadata.isMetadataNamespacePresent()) {
      // missing metadata namespace means that mongod did not properly execute $project, likely due
      // to the undefined behavior when duplicate fields are present in the original document, see
      // HELP-60413. other fields can also be corrupted, depending on the number of duplicates.
      // we cannot proceed, so treating this as a DELETE event.
      BsonValue documentId = DocumentMetadata.extractId(event.getDocumentKey());
      metrics.updateSkippedDocumentWithoutMetadata();
      LOG.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
          "Document %s did not contain metadata namespace, likely due to presence "
              + "of duplicate fields, skipping indexing this document",
          documentId);
      return Optional.of(DocumentEvent.createDelete(documentId));
    }

    OperationType operation =
        metadata.isDeleted() ? OperationType.DELETE : event.getOperationType();

    switch (operation) {
      case INSERT -> {
        incrementApplicableDocumentMetrics(metrics, event);
        return Optional.of(DocumentEvent.createInsert(metadata, event.getFullDocument()));
      }
      case UPDATE -> {
        if (metadata.getId().isEmpty()) {
          // when updateLookup happens after the doc is already deleted, we can get an update event
          // that does not contain fullDocument._id. we should treat such documents as a
          // DELETE event
          return Optional.of(
              DocumentEvent.createDelete(DocumentMetadata.extractId(event.getDocumentKey())));
        }

        metrics.updateWitnessed();

        // We can skip the updateDescriptionAppliesToIndex check if the updates have already been
        // filtered at an earlier iteration.
        if (areUpdateEventsPrefiltered
            || ChangeStreams.updateDescriptionAppliesToIndex(
                event.getUpdateDescription(), fieldDefinitionResolver, indexDefinition.getView())) {
          metrics.updateApplicable();
          incrementApplicableDocumentMetrics(metrics, event);

          // For materialized view based auto-embedding indexes (AUTO_EMBED fields, version >= 2),
          // check if embedding generation is required. If not (only filter fields changed),
          // we can skip embedding and use partial updates. Old EMBEDDING strategy (TEXT fields,
          // version 1) does not support partial updates as it writes directly to Lucene.
          if (indexDefinition instanceof VectorIndexDefinition vectorDef
              && MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(
                  indexDefinition)) {
            VectorIndexFieldMapping fieldMapping = vectorDef.getMappings();
            if (!AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(
                event.getUpdateDescription(), fieldMapping)) {
              BsonDocument filterFieldUpdates =
                  AutoEmbeddingDocumentUtils.extractFilterFieldValues(
                      event.getFullDocument(), fieldMapping);
              // Use partial update if filter fields were extracted successfully.
              // If empty (extraction failed or only non-indexed fields changed), fall back to full
              // document replacement to ensure data correctness.
              if (!filterFieldUpdates.isEmpty()) {
                return Optional.of(
                    DocumentEvent.createFilterOnlyUpdate(
                        metadata, event.getFullDocument(), filterFieldUpdates));
              }
            }
          }
          // Full document replacement
          return Optional.of(DocumentEvent.createUpdate(metadata, event.getFullDocument()));
        } else {
          return Optional.empty();
        }

        // We can skip the updateDescriptionAppliesToIndex check if the updates have already been
        // filtered at an earlier iteration.
      }
      case REPLACE -> {
        incrementApplicableDocumentMetrics(metrics, event);
        return Optional.of(DocumentEvent.createUpdate(metadata, event.getFullDocument()));
      }
      case DELETE -> {
        return Optional.of(
            DocumentEvent.createDelete(DocumentMetadata.extractId(event.getDocumentKey())));
      }
      case INVALIDATE, DROP, DROP_DATABASE, RENAME, OTHER -> {
        logger.error("Unexpected OperationType: {}", event.getOperationType());
        return Check.unreachable("Unexpected OperationType");
      }
    }
    logger.error("Unhandled OperationType: {}", event.getOperationType());
    return Check.unreachable("Unhandled OperationType");
  }

  private static void incrementApplicableDocumentMetrics(
      ChangeStreamBatchMetrics metrics, ChangeStreamDocument<RawBsonDocument> event) {
    metrics.applicableDocumentsTotal();
    metrics.applicableDocumentsTotalBytes(
        Optional.ofNullable(event.getFullDocument())
            .map(rawDocument -> rawDocument.getByteBuffer().remaining())
            .orElse(0));
  }

  private static DocumentMetadata createMetadata(
      ChangeStreamDocument<RawBsonDocument> event, IndexDefinition indexDefinition) {

    // for views, we use the metadata namespace to get the deleted flag and unmodified _id
    if (indexDefinition.getView().isPresent()) {
      return DocumentMetadata.fromMetadataNamespace(
          Optional.ofNullable(event.getFullDocument()), indexDefinition.getIndexId());
    }

    // for collections, we only need _id from the root of the document
    return DocumentMetadata.fromOriginalDocument(Optional.ofNullable(event.getFullDocument()));
  }

  /**
   * Traverses the given list of raw {@link ChangeStreamDocument} and returns the index of the
   * left-most non-indexable event (INVALIDATE, RENAME, DROP, OTHER) from the end of list. In case
   * the list is empty returns 0, if no such elements found (or present in middle of list) returns
   * {@link List#size()}.
   */
  public static int indexOfLifecycleEvent(List<RawBsonDocument> events) {
    @Var int lastIndex = events.size();

    while (lastIndex > 0) {

      switch (bsonDocumentToChangeStreamDocument(events.get(lastIndex - 1)).getOperationType()) {
        case INSERT, UPDATE, REPLACE, DELETE -> {
          return lastIndex;
        }
        case DROP, DROP_DATABASE, RENAME, INVALIDATE, OTHER -> lastIndex--;
      }
    }

    return lastIndex;
  }

  @FunctionalInterface
  public interface ChangeStreamEventChecker<E extends Exception> {

    Optional<E> check(ChangeStreamDocument<RawBsonDocument> event);
  }

  public static class ChangeStreamEventCheckException extends RuntimeException {

    public ChangeStreamEventCheckException(Throwable cause) {
      super(cause);
    }

    public static ChangeStreamEventCheckException create(Throwable cause) {
      return new ChangeStreamEventCheckException(cause);
    }
  }

  /**
   * Decorates a given change stream document transformer with validation check per event. In case
   * of non-valid event this transformer will throw a {@link ChangeStreamEventCheckException}.
   *
   * @return a decoded change steam event.
   * @throws ChangeStreamEventCheckException extends {@link RuntimeException}.
   */
  private static <E extends Exception>
      Transformer<RawBsonDocument, ChangeStreamDocument<RawBsonDocument>>
          eventValidationTransformer(
              Transformer<RawBsonDocument, ChangeStreamDocument<RawBsonDocument>> wrapped,
              ChangeStreamEventChecker<E> eventChecker) {

    return (raw) -> {
      var decoded = wrapped.transform(raw);
      Optional<E> lifecycleEvent = eventChecker.check(decoded);
      if (lifecycleEvent.isPresent()) {
        throw ChangeStreamEventCheckException.create(lifecycleEvent.get());
      }
      return decoded;
    };
  }
}
