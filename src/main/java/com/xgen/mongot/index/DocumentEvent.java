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
  // For filter-only updates: contains the $set document with only filter field values.
  // When present, indicates this is a filter-only update that should skip embedding generation
  // and use partial update ($set) instead of full document replacement in MaterializedViewWriter.
  // Example: {"$set": {"string_filter": "new_value", "int_filter": 11}}
  private final Optional<BsonDocument> filterFieldUpdates;
  // Custom vector engine id assigned during write to the native index. Carried through to
  // the Lucene indexing policy so the id-to-_id mapping document can be written.
  private Optional<Long> customVectorEngineId;

  private DocumentEvent(
      DocumentEvent rawDocumentEventWithoutVector,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    this.eventType = rawDocumentEventWithoutVector.getEventType();
    this.documentId = rawDocumentEventWithoutVector.getDocumentId();
    this.document = rawDocumentEventWithoutVector.document;
    this.autoEmbeddings = autoEmbeddings;
    this.filterFieldUpdates = rawDocumentEventWithoutVector.filterFieldUpdates;
    this.customVectorEngineId = rawDocumentEventWithoutVector.customVectorEngineId;
  }

  private DocumentEvent(
      EventType eventType, BsonValue documentId, Optional<RawBsonDocument> document) {
    this.eventType = eventType;
    this.documentId = documentId;
    this.document = document;
    this.autoEmbeddings = ImmutableMap.of();
    this.filterFieldUpdates = Optional.empty();
    this.customVectorEngineId = Optional.empty();
  }

  private DocumentEvent(
      EventType eventType,
      BsonValue documentId,
      Optional<RawBsonDocument> document,
      BsonDocument filterFieldUpdates) {
    this.eventType = eventType;
    this.documentId = documentId;
    this.document = document;
    this.autoEmbeddings = ImmutableMap.of();
    this.filterFieldUpdates = Optional.of(filterFieldUpdates);
    this.customVectorEngineId = Optional.empty();
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
   * Creates an UPDATE event for filter-only updates. When only filter fields change (no AUTO_EMBED
   * or TEXT fields), we skip embedding generation and use partial update ($set) in
   * MaterializedViewWriter.
   *
   * @param metadata the document metadata
   * @param document the full document from the change stream
   * @param filterFieldUpdates the $set document containing only filter field values to update
   */
  public static DocumentEvent createFilterOnlyUpdate(
      DocumentMetadata metadata, RawBsonDocument document, BsonDocument filterFieldUpdates) {
    return new DocumentEvent(
        EventType.UPDATE,
        Check.isPresent(metadata.getId(), "id"),
        Optional.of(document),
        filterFieldUpdates);
  }

  public static DocumentEvent createFromDocumentEventAndVectors(
      DocumentEvent rawDocumentEventWithoutVector,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    return new DocumentEvent(rawDocumentEventWithoutVector, autoEmbeddings);
  }

  /** Sets custom vector engine id and returns the same event object. */
  public DocumentEvent withCustomVectorEngineId(long customVectorEngineId) {
    this.customVectorEngineId = Optional.of(customVectorEngineId);
    return this;
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
   * Returns the $set document for filter-only updates. When present, indicates this event should
   * use partial update instead of full document replacement in MaterializedViewWriter.
   */
  public Optional<BsonDocument> getFilterFieldUpdates() {
    return this.filterFieldUpdates;
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
        + ", customVectorEngineId="
        + this.customVectorEngineId
        + '}';
  }
}
