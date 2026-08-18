package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator;
import com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils;
import com.xgen.mongot.embedding.utils.PartialEmbedClassification;
import com.xgen.mongot.embedding.utils.PartialEmbedFallbackReason;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.definition.FieldDefinitionResolver;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.replication.mongodb.common.ViewFieldDependencies.ViewDerivationFallbackReason;
import com.xgen.mongot.replication.mongodb.common.ViewFieldDependencies.ViewFullProcessingReason;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.LazyTransformationList;
import com.xgen.mongot.util.LazyTransformationList.Transformer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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
    // Partial-embed observability (CLOUDP-412617): partial-embeds applied, and per-reason
    // full-re-index fallback counts.
    public final int partialEmbedApplied;
    public final Map<PartialEmbedFallbackReason, Integer> partialEmbedFallbacks;
    // Non-view auto-embed observability: filter-only partials applied (the non-view analog of
    // viewFilterOnlyPartialApplied below).
    public final int filterOnlyPartialApplied;
    // View auto-embed observability: per-decision reasons, view filter-only partials applied, and
    // (first batch per index only) the derivation fallback reason.
    public final Map<ViewFullProcessingReason, Integer> viewDecisions;
    public final int viewFilterOnlyPartialApplied;
    public final Optional<ViewDerivationFallbackReason> viewDerivationFallbackReason;

    private DocumentEventBatch(
        List<DocumentEvent> finalChangeEvents,
        int updatesWitnessed,
        int updatesApplicable,
        int skippedDocumentsWithoutMetadataNamespace,
        int applicableDocumentsTotal,
        long applicableDocumentsTotalBytes,
        int partialEmbedApplied,
        Map<PartialEmbedFallbackReason, Integer> partialEmbedFallbacks,
        int filterOnlyPartialApplied,
        Map<ViewFullProcessingReason, Integer> viewDecisions,
        int viewFilterOnlyPartialApplied,
        Optional<ViewDerivationFallbackReason> viewDerivationFallbackReason) {
      this.finalChangeEvents = finalChangeEvents;
      this.updatesWitnessed = updatesWitnessed;
      this.updatesApplicable = updatesApplicable;
      this.skippedDocumentsWithoutMetadataNamespace = skippedDocumentsWithoutMetadataNamespace;
      this.applicableDocumentsTotal = applicableDocumentsTotal;
      this.applicableDocumentsTotalBytes = applicableDocumentsTotalBytes;
      this.partialEmbedApplied = partialEmbedApplied;
      this.partialEmbedFallbacks = partialEmbedFallbacks;
      this.filterOnlyPartialApplied = filterOnlyPartialApplied;
      this.viewDecisions = viewDecisions;
      this.viewFilterOnlyPartialApplied = viewFilterOnlyPartialApplied;
      this.viewDerivationFallbackReason = viewDerivationFallbackReason;
    }
  }

  private static class ChangeStreamBatchMetrics {
    private int witnessedUpdatesCounter;
    private int applicableUpdatesCounter;
    private int skippedDocumentWithoutMetadataNamespaceCounter;
    private int applicableDocumentsTotalCounter;
    private long applicableDocumentsTotalBytesCounter;
    private int partialEmbedAppliedCounter;
    private final EnumMap<PartialEmbedFallbackReason, Integer> partialEmbedFallbackCounters =
        new EnumMap<>(PartialEmbedFallbackReason.class);
    private int filterOnlyPartialAppliedCounter;
    private final EnumMap<ViewFullProcessingReason, Integer> viewDecisionCounters =
        new EnumMap<>(ViewFullProcessingReason.class);
    private int viewFilterOnlyPartialAppliedCounter;
    private Optional<ViewDerivationFallbackReason> viewDerivationFallbackReason = Optional.empty();

    public ChangeStreamBatchMetrics() {
      this.witnessedUpdatesCounter = 0;
      this.applicableUpdatesCounter = 0;
      this.skippedDocumentWithoutMetadataNamespaceCounter = 0;
      this.applicableDocumentsTotalCounter = 0;
      this.applicableDocumentsTotalBytesCounter = 0;
      this.partialEmbedAppliedCounter = 0;
    }

    public void updateWitnessed() {
      this.witnessedUpdatesCounter++;
    }

    /** An UPDATE was classified partial-embed-safe and handled without a full re-index. */
    public void partialEmbedApplied() {
      this.partialEmbedAppliedCounter++;
    }

    /** A change fell back to a full document re-index, tagged with why. */
    public void partialEmbedFallback(PartialEmbedFallbackReason reason) {
      this.partialEmbedFallbackCounters.merge(reason, 1, Integer::sum);
    }

    /** A view-based UPDATE was classified partial|full, tagged with the reason. */
    public void viewDecision(ViewFullProcessingReason reason) {
      this.viewDecisionCounters.merge(reason, 1, Integer::sum);
    }

    /** A non-view UPDATE took the filter-only partial path (no embed field changed). */
    public void filterOnlyPartialApplied() {
      this.filterOnlyPartialAppliedCounter++;
    }

    /** A view-based UPDATE took the filter-only partial path. */
    public void viewFilterOnlyPartialApplied() {
      this.viewFilterOnlyPartialAppliedCounter++;
    }

    /** Records (once per index) why derivation fell back to full processing. */
    public void recordViewDerivationFallback(ViewDerivationFallbackReason reason) {
      this.viewDerivationFallbackReason = Optional.of(reason);
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

    public int getPartialEmbedApplied() {
      return this.partialEmbedAppliedCounter;
    }

    public Map<PartialEmbedFallbackReason, Integer> getPartialEmbedFallbacks() {
      return this.partialEmbedFallbackCounters;
    }

    public int getFilterOnlyPartialApplied() {
      return this.filterOnlyPartialAppliedCounter;
    }

    public Map<ViewFullProcessingReason, Integer> getViewDecisions() {
      return this.viewDecisionCounters;
    }

    public int getViewFilterOnlyPartialApplied() {
      return this.viewFilterOnlyPartialAppliedCounter;
    }

    public Optional<ViewDerivationFallbackReason> getViewDerivationFallbackReason() {
      return this.viewDerivationFallbackReason;
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

  /**
   * Records the on-the-wire size of every raw event in a change-stream batch via {@code
   * eventSizeRecorder} and returns the batch's total raw size in bytes, traversing the batch once.
   * Callers that need both the per-event sizes (e.g. large-event thresholds) and the batch total
   * (e.g. batch-fullness classification) thus pay for only a single pass (CLOUDP-353521,
   * CLOUDP-425971).
   *
   * <p>The returned total is the sum of per-event raw payload bytes — roughly what counts against
   * the server's ~16MB response cap — as opposed to the post-filter applicable byte count.
   */
  public static long recordChangeStreamEventSizes(
      List<RawBsonDocument> batch, Consumer<Integer> eventSizeRecorder) {
    @Var long total = 0;
    for (RawBsonDocument rawDoc : batch) {
      int size = rawDoc.getByteBuffer().remaining();
      eventSizeRecorder.accept(size);
      total += size;
    }
    return total;
  }

  /**
   * Look backwards through the batch, which should only contain document events. Return a list of
   * {@link DocumentEvent} representing the final valid document event in this batch for each
   * documentId in the batch.
   *
   * <p>We will skip updates that do not touch fields that are in this index definition.
   */
  @VisibleForTesting
  static DocumentEventBatch handleDocumentEvents(
      List<ChangeStreamDocument<RawBsonDocument>> batch,
      IndexDefinition indexDefinition,
      FieldDefinitionResolver fieldDefinitionResolver,
      boolean areUpdateEventsPrefiltered,
      Optional<ViewFieldDependencies> viewFieldDependencies) {
    // Convenience overload: no runtime config, never reports the derivation fallback (tests).
    return handleDocumentEvents(
        batch,
        indexDefinition,
        fieldDefinitionResolver,
        areUpdateEventsPrefiltered,
        viewFieldDependencies,
        Optional.empty(),
        false);
  }

  /** As above, with the auto-embedding runtime config; does not report the derivation fallback. */
  public static DocumentEventBatch handleDocumentEvents(
      List<ChangeStreamDocument<RawBsonDocument>> batch,
      IndexDefinition indexDefinition,
      FieldDefinitionResolver fieldDefinitionResolver,
      boolean areUpdateEventsPrefiltered,
      Optional<ViewFieldDependencies> viewFieldDependencies,
      Optional<AutoEmbeddingMaterializedViewConfig> autoEmbeddingMaterializedViewConfig) {
    return handleDocumentEvents(
        batch,
        indexDefinition,
        fieldDefinitionResolver,
        areUpdateEventsPrefiltered,
        viewFieldDependencies,
        autoEmbeddingMaterializedViewConfig,
        false);
  }

  /**
   * Handles a document-event batch. {@code reportDerivationFallback} is set by the caller on the
   * first batch per index so the view derivation fallback is counted once.
   */
  public static DocumentEventBatch handleDocumentEvents(
      List<ChangeStreamDocument<RawBsonDocument>> batch,
      IndexDefinition indexDefinition,
      FieldDefinitionResolver fieldDefinitionResolver,
      // TODO(CLOUDP-324852): Remove this parameter once initial sync and steady state logic are
      //  aligned.
      boolean areUpdateEventsPrefiltered,
      // Derived once per index definition by the caller (see deriveViewFieldDependencies); empty
      // for non-view or non-auto-embedding indexes.
      Optional<ViewFieldDependencies> viewFieldDependencies,
      Optional<AutoEmbeddingMaterializedViewConfig> autoEmbeddingMaterializedViewConfig,
      // True on the first batch per index only, so the derivation fallback is counted once.
      boolean reportDerivationFallback) {

    ChangeStreamBatchMetrics metrics = new ChangeStreamBatchMetrics();
    if (reportDerivationFallback) {
      viewFieldDependencies
          .flatMap(ViewFieldDependencies::derivationFallbackReason)
          .ifPresent(metrics::recordViewDerivationFallback);
    }
    Set<BsonDocument> processedDocs = new HashSet<>();

    // TODO(CLOUDP-424650): Split full-update eligibility from this vector-only partial-update
    // mapping so search autoEmbedVector indexes are covered without enabling partial updates.
    Optional<AutoEmbedFieldMapping> autoEmbedMapping =
        (indexDefinition instanceof VectorIndexDefinition vectorDef
                && MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(
                    indexDefinition))
            ? Optional.of(AutoEmbedFieldMappingCreator.createAutoEmbedMapping(vectorDef))
            : Optional.empty();
    boolean forceFullUpdate =
        autoEmbedMapping.isPresent()
            && autoEmbeddingMaterializedViewConfig
                .map(AutoEmbeddingMaterializedViewConfig::forceChangeStreamFullUpdate)
                .orElse(false);

    // Identify docs whose collapsed-away predecessors required full processing.
    Set<BsonDocument> autoEmbedDocsRequiringFullUpdate = new HashSet<>();
    if (autoEmbedMapping.isPresent() && !forceFullUpdate) {
      Set<BsonDocument> seenSurvivor = new HashSet<>();
      for (var event : Lists.reverse(batch)) {
        BsonDocument key = event.getDocumentKey();
        if (seenSurvivor.add(key)) {
          continue;
        }
        if (eventRequiresAutoEmbedFullUpdate(
            event, indexDefinition, autoEmbedMapping.get(), viewFieldDependencies)) {
          autoEmbedDocsRequiringFullUpdate.add(key);
        }
      }
    }

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
                areUpdateEventsPrefiltered,
                autoEmbedMapping,
                viewFieldDependencies,
                autoEmbedDocsRequiringFullUpdate.contains(event.getDocumentKey()),
                forceFullUpdate);
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
        metrics.getApplicableDocumentsTotalBytes(),
        metrics.getPartialEmbedApplied(),
        metrics.getPartialEmbedFallbacks(),
        metrics.getFilterOnlyPartialApplied(),
        metrics.getViewDecisions(),
        metrics.getViewFilterOnlyPartialApplied(),
        metrics.getViewDerivationFallbackReason());
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
      boolean areUpdateEventsPrefiltered,
      Optional<AutoEmbedFieldMapping> autoEmbedMapping,
      Optional<ViewFieldDependencies> viewFieldDependencies,
      boolean docRequiresFullUpdate,
      boolean forceFullUpdate) {

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

        // A collapsed-away predecessor required full processing; the survivor must too.
        if (docRequiresFullUpdate) {
          metrics.updateApplicable();
          incrementApplicableDocumentMetrics(metrics, event);
          metrics.partialEmbedFallback(
              PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL);
          return Optional.of(DocumentEvent.createUpdate(metadata, event.getFullDocument()));
        }

        // We can skip the updateDescriptionAppliesToIndex check if the updates have already been
        // filtered at an earlier iteration.
        if (areUpdateEventsPrefiltered
            || ChangeStreams.updateDescriptionAppliesToIndex(
                event.getUpdateDescription(), fieldDefinitionResolver, indexDefinition.getView())) {
          metrics.updateApplicable();
          incrementApplicableDocumentMetrics(metrics, event);

          if (autoEmbedMapping.isPresent() && forceFullUpdate) {
            metrics.partialEmbedFallback(
                PartialEmbedFallbackReason.FEATURE_FLAG_FORCED_FULL_UPDATE);
            return Optional.of(DocumentEvent.createUpdate(metadata, event.getFullDocument()));
          }

          // For materialized view based auto-embedding indexes (AUTO_EMBED fields, version >= 2),
          // decide between a partial $set and a full ReplaceOne.
          //
          // For NON-VIEW indexes: classify the update (CLOUDP-412617). When only filter fields
          // changed, emit a filter-only $set; when only a subset of embed fields changed, emit a
          // partial-embed $set carrying just the changed embed texts; otherwise fall back to a
          // full re-index. Partial-embed metrics are recorded at the *committed* outcome below.
          //
          // For VIEW-BASED indexes: ViewFieldDependencies statically derives, per embed source,
          // the base fields it depends on plus the membership/computed-filter triggers
          // (CLOUDP-412469). A change touching only a subset of embed feeders re-embeds just that
          // subset (read from the view-resolved fullDocument); a membership/computed-filter change
          // takes the full path (HELP-94834: partial updates silently dropped documents
          // transitioning into the view). Unknown dependencies fall back to full processing.
          //
          // The old EMBEDDING strategy (TEXT fields, version 1) writes directly to Lucene and
          // does not support partial updates.
          if (autoEmbedMapping.isPresent()) {
            AutoEmbedFieldMapping mapping = autoEmbedMapping.get();
            if (indexDefinition.getView().isPresent()) {
              // Classify the view update: re-embed only the embed sources whose base feeders
              // changed (the view-resolved fullDocument already carries the recomputed text), or
              // emit a filter-only $set when no embed source changed. Absent dependencies or a
              // membership/computed-filter change fall through to the full (embed + upsert) path.
              if (viewFieldDependencies.isPresent()) {
                ViewFieldDependencies deps = viewFieldDependencies.get();
                ViewFieldDependencies.ViewUpdateClassification decision =
                    deps.classifyViewUpdate(event.getUpdateDescription());
                // Decision metric: NONE when a partial path is taken, else the force-full reason.
                metrics.viewDecision(
                    decision.requiresFull()
                        ? deps.classifyUpdate(event.getUpdateDescription())
                        : ViewFullProcessingReason.NONE);
                if (!decision.requiresFull()) {
                  boolean embedChanged = !decision.changedEmbedPaths().isEmpty();
                  @Var ImmutableMap<FieldPath, String> changedEmbedTexts = ImmutableMap.of();
                  @Var Optional<PartialEmbedFallbackReason> fallbackReason = Optional.empty();
                  if (embedChanged) {
                    PartialEmbedClassification classification =
                        AutoEmbeddingDocumentUtils.extractChangedEmbedTexts(
                            decision.changedEmbedPaths(), event.getFullDocument(), mapping);
                    changedEmbedTexts = classification.changedTexts().orElse(ImmutableMap.of());
                    fallbackReason = classification.fallbackReason();
                  }
                  if (fallbackReason.isEmpty()) {
                    Optional<BsonDocument> filterFieldUpdates =
                        AutoEmbeddingDocumentUtils.extractFilterFieldValues(
                            event.getFullDocument(), mapping);
                    // A base $set overwriting a filter's ancestor (e.g. {meta: null} dropping
                    // meta.category) or destructively touching a filter can't be mirrored by a
                    // partial $set built from the post-image; force a full replace (CLOUDP-402522).
                    Optional<PartialEmbedFallbackReason> filterOnlyUnsafe =
                        indexDefinition instanceof VectorIndexDefinition vectorDef
                            ? AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
                                event.getUpdateDescription(), mapping, vectorDef.getNestedRoot())
                            : Optional.empty();
                    if (filterOnlyUnsafe.isEmpty()
                        && filterFieldUpdates.isPresent()
                        && (!changedEmbedTexts.isEmpty() || !filterFieldUpdates.get().isEmpty())) {
                      if (embedChanged) {
                        metrics.partialEmbedApplied();
                      } else {
                        metrics.viewFilterOnlyPartialApplied();
                      }
                      return Optional.of(
                          DocumentEvent.createPartialUpdate(
                              metadata,
                              event.getFullDocument(),
                              filterFieldUpdates.get(),
                              changedEmbedTexts));
                    }
                    if (filterOnlyUnsafe.isPresent()) {
                      fallbackReason = filterOnlyUnsafe;
                    } else if (embedChanged) {
                      fallbackReason =
                          Optional.of(PartialEmbedFallbackReason.FILTER_EXTRACTION_FAILED);
                    }
                  }
                  fallbackReason.ifPresent(metrics::partialEmbedFallback);
                }
              }
            } else {
              boolean embedFieldChanged =
                  AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(
                      event.getUpdateDescription(), mapping);
              @Var ImmutableMap<FieldPath, String> changedEmbedTexts = ImmutableMap.of();
              @Var Optional<PartialEmbedFallbackReason> fallbackReason = Optional.empty();
              if (embedFieldChanged) {
                PartialEmbedClassification classification =
                    AutoEmbeddingDocumentUtils.computePartialEmbedChangedTexts(
                        event.getUpdateDescription(), event.getFullDocument(), mapping);
                changedEmbedTexts = classification.changedTexts().orElse(ImmutableMap.of());
                fallbackReason = classification.fallbackReason();
              }
              if (fallbackReason.isEmpty()) {
                // Empty Optional = extraction failed; fall back to a full replace rather than
                // issue a partial $set that would silently drop filter fields. A present-but-empty
                // body is valid (index has no filter fields) and still supports a partial embed.
                // The filter-only path additionally needs a non-empty body to be worth a partial
                // update.
                Optional<BsonDocument> filterFieldUpdates =
                    AutoEmbeddingDocumentUtils.extractFilterFieldValues(
                        event.getFullDocument(), mapping);
                // A partial $set can't clear a filter removed ($unset/$pop/$slice) or dropped by a
                // $set overwriting its parent; those fall back to a full replace (CLOUDP-402522).
                Optional<PartialEmbedFallbackReason> filterOnlyUnsafe =
                    indexDefinition instanceof VectorIndexDefinition vectorDef
                        ? AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
                            event.getUpdateDescription(), mapping, vectorDef.getNestedRoot())
                        : Optional.empty();
                if (filterOnlyUnsafe.isEmpty()
                    && filterFieldUpdates.isPresent()
                    && (!changedEmbedTexts.isEmpty() || !filterFieldUpdates.get().isEmpty())) {
                  if (embedFieldChanged) {
                    metrics.partialEmbedApplied();
                  } else {
                    metrics.filterOnlyPartialApplied();
                  }
                  return Optional.of(
                      DocumentEvent.createPartialUpdate(
                          metadata,
                          event.getFullDocument(),
                          filterFieldUpdates.get(),
                          changedEmbedTexts));
                }
                // Couldn't issue a partial $set: a filter it can't maintain forces a full replace;
                // for an embed-changing update the only other cause is a filter-extraction failure.
                if (filterOnlyUnsafe.isPresent()) {
                  fallbackReason = filterOnlyUnsafe;
                } else if (embedFieldChanged) {
                  fallbackReason = Optional.of(PartialEmbedFallbackReason.FILTER_EXTRACTION_FAILED);
                }
              }
              fallbackReason.ifPresent(metrics::partialEmbedFallback);
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
        // A REPLACE re-embeds the whole document; count it as a full-re-index fallback
        // (CLOUDP-412617), gated to the same MV-backed non-view population as the UPDATE path.
        if (autoEmbedMapping.isPresent() && indexDefinition.getView().isEmpty()) {
          metrics.partialEmbedFallback(PartialEmbedFallbackReason.REPLACE_EVENT);
        }
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

  /**
   * Returns true if the given change-stream event, considered alone, must take the full (embed +
   * upsert) write path on a materialized-view auto-embedding index. Used by the batch-aware
   * pre-pass in {@link #handleDocumentEvents} to detect events that would be lost behind a
   * filter-only survivor after intra-batch dedup.
   *
   * <p>INSERT and REPLACE always require full processing (they are full writes by definition). An
   * UPDATE requires full processing when it would destructively drop a filter ({@link
   * AutoEmbeddingDocumentUtils#filterOnlyPartialUpdateFallbackReason}) — otherwise a collapsed
   * filter-dropping event would leave a stale filter behind a partial-update survivor (views and
   * non-views alike). View-based indexes additionally consult {@link
   * ViewFieldDependencies#requiresFullProcessing}. Non-view UPDATEs additionally consult {@link
   * AutoEmbeddingDocumentUtils#requiresEmbeddingGeneration}. Other operation types (DELETE,
   * INVALIDATE, etc.) do not interact with the partial-update path.
   */
  private static boolean eventRequiresAutoEmbedFullUpdate(
      ChangeStreamDocument<RawBsonDocument> event,
      IndexDefinition indexDefinition,
      AutoEmbedFieldMapping autoEmbedMapping,
      Optional<ViewFieldDependencies> viewFieldDependencies) {
    return switch (event.getOperationType()) {
      case INSERT, REPLACE -> true;
      case UPDATE -> {
        Optional<PartialEmbedFallbackReason> filterOnlyUnsafe =
            indexDefinition instanceof VectorIndexDefinition vectorDef
                ? AutoEmbeddingDocumentUtils.filterOnlyPartialUpdateFallbackReason(
                    event.getUpdateDescription(), autoEmbedMapping, vectorDef.getNestedRoot())
                : Optional.empty();
        if (indexDefinition.getView().isPresent()) {
          boolean requiresFull =
              viewFieldDependencies
                  .map(deps -> deps.requiresFullProcessing(event.getUpdateDescription()))
                  .orElse(true);
          yield requiresFull || filterOnlyUnsafe.isPresent();
        }
        yield AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(
                event.getUpdateDescription(), autoEmbedMapping)
            || filterOnlyUnsafe.isPresent();
      }
      default -> false;
    };
  }

  private static void incrementApplicableDocumentMetrics(
      ChangeStreamBatchMetrics metrics, ChangeStreamDocument<RawBsonDocument> event) {
    metrics.applicableDocumentsTotal();
    metrics.applicableDocumentsTotalBytes(
        Optional.ofNullable(event.getFullDocument())
            .map(rawDocument -> rawDocument.getByteBuffer().remaining())
            .orElse(0));
  }

  /**
   * Derives the view-pipeline field dependencies for a materialized-view auto-embedding index, or
   * empty for any other index. This is a pure function of the immutable index definition, so
   * callers should compute it once per index (e.g. memoized) rather than per change-stream batch.
   */
  public static Optional<ViewFieldDependencies> deriveViewFieldDependencies(
      IndexDefinition indexDefinition) {
    if (!(indexDefinition instanceof VectorIndexDefinition)
        || !MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(
            indexDefinition)) {
      return Optional.empty();
    }
    return indexDefinition
        .getView()
        .flatMap(view -> view.getEffectivePipeline())
        .map(
            pipeline ->
                ViewFieldDependencies.derive(
                    pipeline,
                    embedSourcePaths(indexDefinition),
                    fieldPathsOfType(indexDefinition, VectorIndexFieldDefinition.Type.FILTER)));
  }

  /**
   * Returns the source paths of the index's embed fields. Both AUTO_EMBED and TEXT fields are embed
   * sources (see AutoEmbedFieldMappingCreator), so both must be tracked — omitting TEXT would
   * under-collect and serve stale embeddings on a mixed index.
   */
  private static Set<FieldPath> embedSourcePaths(IndexDefinition indexDefinition) {
    // TODO(CLOUDP-353553): handle SearchIndexDefinition once auto-embedding is supported there.
    if (!(indexDefinition instanceof VectorIndexDefinition vectorDef)) {
      return Set.of();
    }
    return vectorDef.getFields().stream()
        .filter(VectorIndexFieldDefinition::isAutoEmbedField)
        .map(VectorIndexFieldDefinition::getPath)
        .collect(Collectors.toSet());
  }

  /** Returns the paths of the index's fields of the given type. */
  private static Set<FieldPath> fieldPathsOfType(
      IndexDefinition indexDefinition, VectorIndexFieldDefinition.Type type) {
    if (!(indexDefinition instanceof VectorIndexDefinition vectorDef)) {
      return Set.of();
    }
    return vectorDef.getFields().stream()
        .filter(field -> field.getType() == type)
        .map(VectorIndexFieldDefinition::getPath)
        .collect(Collectors.toSet());
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
