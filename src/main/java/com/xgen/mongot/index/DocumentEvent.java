package com.xgen.mongot.index;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * Contains a Document along with information about the type of event that occurred on the Document.
 */
public class DocumentEvent {

  public enum EventType {
    INSERT,
    UPDATE,
    DELETE,
  }

  private final EventType eventType;
  private final BsonValue documentId;
  private final Optional<RawBsonDocument> document;
  // Auto-generated embeddings associated to this document, used for auto-embedding vector index.
  // e.g., {eventType:INSERT, documentId:123, document: {text_path_a: "text to be vectorized"},
  // autoEmbeddings: {text_path_a:{"text to be vectorized": [1f, 2f, ...]}}}
  private final ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings;
  // The $set BODY for partial updates (fields-to-set only — MaterializedViewWriter wraps this in
  // $set itself, callers must NOT pre-wrap). For filter-only updates this holds only filter-field
  // values. For partial-embed updates it starts the same way and gets augmented with vector + hash
  // entries for the changed embed paths once the embedding service responds (see the embedding
  // scheduler). Either way, when present, the writer issues a $set instead of a full document
  // replacement.
  // Example (filter-only):     {"string_filter": "new_value"}
  // Example (partial-embed, post-embedding):
  //   {"string_filter": "new_value", "_autoEmbed.ae1": <vec>, "_autoEmbed._hash.ae1": "abc..."}
  private final Optional<BsonDocument> filterFieldUpdates;
  // For partial-embed updates: declared embed paths whose embeddings need to be regenerated,
  // mapped to the chosen non-empty source text for each path. Populated by the change-stream
  // classifier so the embedding scheduler and $set builder don't have to re-walk the source
  // document. Empty for filter-only updates, full replacements, and post-augmentation events.
  private final ImmutableMap<FieldPath, String> changedEmbedTexts;
  // Custom vector engine id assigned during write to the native index. Carried through to
  // the Lucene indexing policy so the id-to-_id mapping document can be written.
  private final Optional<Long> customVectorEngineId;

  private DocumentEvent(
      DocumentEvent rawDocumentEventWithoutVector,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    this.eventType = rawDocumentEventWithoutVector.getEventType();
    this.documentId = rawDocumentEventWithoutVector.getDocumentId();
    this.document = rawDocumentEventWithoutVector.document;
    this.autoEmbeddings = autoEmbeddings;
    this.filterFieldUpdates = rawDocumentEventWithoutVector.filterFieldUpdates;
    this.changedEmbedTexts = rawDocumentEventWithoutVector.changedEmbedTexts;
    this.customVectorEngineId = rawDocumentEventWithoutVector.customVectorEngineId;
  }

  private DocumentEvent(
      EventType eventType, BsonValue documentId, Optional<RawBsonDocument> document) {
    this.eventType = eventType;
    this.documentId = documentId;
    this.document = document;
    this.autoEmbeddings = ImmutableMap.of();
    this.filterFieldUpdates = Optional.empty();
    this.changedEmbedTexts = ImmutableMap.of();
    this.customVectorEngineId = Optional.empty();
  }

  private DocumentEvent(
      EventType eventType,
      BsonValue documentId,
      Optional<RawBsonDocument> document,
      BsonDocument filterFieldUpdates,
      ImmutableMap<FieldPath, String> changedEmbedTexts) {
    this.eventType = eventType;
    this.documentId = documentId;
    this.document = document;
    this.autoEmbeddings = ImmutableMap.of();
    this.filterFieldUpdates = Optional.of(filterFieldUpdates);
    this.changedEmbedTexts = changedEmbedTexts;
    this.customVectorEngineId = Optional.empty();
  }

  private DocumentEvent(
      EventType eventType,
      BsonValue documentId,
      Optional<RawBsonDocument> document,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings,
      Optional<BsonDocument> filterFieldUpdates,
      ImmutableMap<FieldPath, String> changedEmbedTexts,
      Optional<Long> customVectorEngineId) {
    this.eventType = eventType;
    this.documentId = documentId;
    this.document = document;
    this.autoEmbeddings = autoEmbeddings;
    this.filterFieldUpdates = filterFieldUpdates;
    this.changedEmbedTexts = changedEmbedTexts;
    this.customVectorEngineId = customVectorEngineId;
  }

  public static DocumentEvent createFromDocumentEvent(
      DocumentEvent event, RawBsonDocument document) {
    return new DocumentEvent(event.getEventType(), event.getDocumentId(), Optional.of(document));
  }

  public static DocumentEvent createInsert(DocumentMetadata metadata, RawBsonDocument document) {
    return new DocumentEvent(
        EventType.INSERT, Check.isPresent(metadata.getId(), "id"), Optional.of(document));
  }

  public static DocumentEvent createUpdate(DocumentMetadata metadata, RawBsonDocument document) {
    return new DocumentEvent(
        EventType.UPDATE, Check.isPresent(metadata.getId(), "id"), Optional.of(document));
  }

  public static DocumentEvent createDelete(BsonValue documentId) {
    return new DocumentEvent(EventType.DELETE, documentId, Optional.empty());
  }

  /**
   * Creates an UPDATE event whose MV write is a partial {@code $set} rather than a full document
   * replacement.
   *
   * <p>{@code changedEmbedTexts} carries the source-side declared embed paths whose embeddings
   * need to be regenerated and the chosen non-empty text for each. An empty map describes a
   * filter-only update (no embedding work pending); a non-empty map describes a partial-embed
   * update that the embedding scheduler will augment via {@link #withFilterFieldUpdates} once it
   * has the vectors.
   */
  public static DocumentEvent createPartialUpdate(
      DocumentMetadata metadata,
      RawBsonDocument document,
      BsonDocument filterFieldUpdates,
      ImmutableMap<FieldPath, String> changedEmbedTexts) {
    return new DocumentEvent(
        EventType.UPDATE,
        Check.isPresent(metadata.getId(), "id"),
        Optional.of(document),
        filterFieldUpdates,
        changedEmbedTexts);
  }

  /**
   * Returns a copy of this partial-embed event with the {@code $set} body replaced by {@code
   * augmentedFilterFieldUpdates} and {@code changedEmbedTexts} cleared. Used by the embedding
   * scheduler after merging vector + hash entries for the changed embed paths into the original
   * filter-field {@code $set} body — once that augmentation completes, the event has no further
   * embedding work to do, so it should be indistinguishable from a filter-only update to
   * downstream code.
   */
  public DocumentEvent withFilterFieldUpdates(BsonDocument augmentedFilterFieldUpdates) {
    return new DocumentEvent(
        this.eventType,
        this.documentId,
        this.document,
        this.autoEmbeddings,
        Optional.of(augmentedFilterFieldUpdates),
        ImmutableMap.of(),
        this.customVectorEngineId);
  }

  public static DocumentEvent createFromDocumentEventAndVectors(
      DocumentEvent rawDocumentEventWithoutVector,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    return new DocumentEvent(rawDocumentEventWithoutVector, autoEmbeddings);
  }

  /** Returns a new DocumentEvent with the given custom vector engine id. */
  public DocumentEvent withCustomVectorEngineId(long customVectorEngineId) {
    return new DocumentEvent(
        this.eventType,
        this.documentId,
        this.document,
        this.autoEmbeddings,
        this.filterFieldUpdates,
        this.changedEmbedTexts,
        Optional.of(customVectorEngineId));
  }

  public Optional<Long> getCustomVectorEngineId() {
    return this.customVectorEngineId;
  }

  /** Returns {@link Optional#empty()} only when the event type is {@link EventType#DELETE} */
  public Optional<RawBsonDocument> getDocument() {
    return this.document;
  }

  public BsonValue getDocumentId() {
    return this.documentId;
  }

  public EventType getEventType() {
    return this.eventType;
  }

  public ImmutableMap<FieldPath, ImmutableMap<String, Vector>> getAutoEmbeddings() {
    return this.autoEmbeddings;
  }

  /**
   * Returns the $set body for partial updates. When present, indicates this event should use a
   * partial update instead of full document replacement in MaterializedViewWriter. The body is
   * filter-fields-only for filter-only updates, and filter-fields + new vector/hash entries for
   * partial-embed updates after embeddings are applied.
   *
   * <p>Presence is <b>not</b> a reliable proxy for "filter-only" — both filter-only updates and
   * partial-embed updates carry a body. Callers needing to distinguish "no embedding work
   * pending" should use {@link #isFilterOnlyUpdate()} instead.
   */
  public Optional<BsonDocument> getFilterFieldUpdates() {
    return this.filterFieldUpdates;
  }

  /**
   * Returns the chosen non-empty source text per declared auto-embed path whose embedding still
   * needs to be regenerated and merged into {@link #getFilterFieldUpdates}. Non-empty only for
   * partial-embed updates that have not yet been processed by the embedding scheduler.
   */
  public ImmutableMap<FieldPath, String> getChangedEmbedTexts() {
    return this.changedEmbedTexts;
  }

  /**
   * Returns {@code true} if this is a filter-only update — i.e. it carries a {@code $set} body to
   * apply verbatim and needs no embedding work. Partial-embed updates also carry a {@code $set}
   * body but still need embeddings for {@link #getChangedEmbedTexts()} and therefore are
   * <b>not</b> filter-only.
   */
  public boolean isFilterOnlyUpdate() {
    return this.filterFieldUpdates.isPresent() && this.changedEmbedTexts.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DocumentEvent that = (DocumentEvent) o;
    return this.documentId.equals(that.documentId)
        && this.document.equals(that.document)
        && this.eventType == that.eventType
        && this.autoEmbeddings.equals(that.autoEmbeddings)
        && this.filterFieldUpdates.equals(that.filterFieldUpdates)
        && this.changedEmbedTexts.equals(that.changedEmbedTexts)
        && this.customVectorEngineId.equals(that.customVectorEngineId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.documentId,
        this.document,
        this.eventType,
        this.autoEmbeddings,
        this.filterFieldUpdates,
        this.changedEmbedTexts,
        this.customVectorEngineId);
  }

  @Override
  public String toString() {
    return "DocumentEvent{"
        + "eventType="
        + this.eventType
        + ", documentId="
        + this.documentId
        + ", document="
        + this.document
        + ", autoEmbeddings="
        + this.autoEmbeddings
        + ", filterFieldUpdates="
        + this.filterFieldUpdates
        + ", changedEmbedTexts="
        + this.changedEmbedTexts
        + ", customVectorEngineId="
        + this.customVectorEngineId
        + '}';
  }

}
