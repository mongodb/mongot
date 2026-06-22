package com.xgen.mongot.index.lucene.document.block;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.definition.VectorEmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.bson.BsonValue;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.Nullable;

/**
 * Delegates consuming indexed values to a wrapped {@link FieldValueHandler}. Determines when to
 * create new {@link VectorEmbeddedDocumentBuilder}s as configured by the FieldDefinition at a path.
 *
 * <p>Creates a new {@link VectorEmbeddedDocumentBuilder} when a new embedded document should be
 * created. See {@link #subDocumentHandler()} to see how this happens.
 */
class VectorEmbeddedFieldValueHandler implements FieldValueHandler {
  private final Optional<FieldValueHandler> wrappedFieldValueHandler;
  private final DocumentBlock documentBlock;
  // Null when this handler is for an intermediate ancestor path of the nestedRoot (not a leaf
  // field or the nestedRoot itself). In that case subDocumentHandler() must return a
  // VectorEmbeddedDocumentHandler so traversal continues with embedded-root awareness.
  @Nullable private final VectorIndexFieldDefinition fieldDefinition;
  private final VectorIndexFieldMapping mapping;
  private final IndexingMetricsUpdater indexingMetricsUpdater;
  private final IndexCapabilities indexCapabilities;
  private final FieldPath path;
  private final byte[] id;
  private final ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings;

  private VectorEmbeddedFieldValueHandler(
      Optional<FieldValueHandler> wrappedFieldValueHandler,
      DocumentBlock documentBlock,
      @Nullable VectorIndexFieldDefinition fieldDefinition,
      VectorIndexFieldMapping mapping,
      IndexingMetricsUpdater indexingMetricsUpdater,
      IndexCapabilities indexCapabilities,
      FieldPath path,
      byte[] id,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    this.wrappedFieldValueHandler = wrappedFieldValueHandler;
    this.documentBlock = documentBlock;
    this.fieldDefinition = fieldDefinition;
    this.mapping = mapping;
    this.indexingMetricsUpdater = indexingMetricsUpdater;
    this.indexCapabilities = indexCapabilities;
    this.path = path;
    this.id = id;
    this.autoEmbeddings = autoEmbeddings;
  }

  static FieldValueHandler create(
      Optional<FieldValueHandler> wrappedValueHandler,
      DocumentBlock documentBlock,
      @Nullable VectorIndexFieldDefinition fieldDefinition,
      VectorIndexFieldMapping mapping,
      IndexingMetricsUpdater indexingMetricsUpdater,
      IndexCapabilities indexCapabilities,
      FieldPath path,
      byte[] id,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    return new VectorEmbeddedFieldValueHandler(
        wrappedValueHandler,
        documentBlock,
        fieldDefinition,
        mapping,
        indexingMetricsUpdater,
        indexCapabilities,
        path,
        id,
        autoEmbeddings);
  }

  /**
   * Creates a {@link VectorEmbeddedFieldValueHandler} for an intermediate ancestor path of the
   * {@code nestedRoot} (e.g. {@code "sections"} when {@code nestedRoot} is {@code
   * "sections.paragraphs"}). No field definition is associated with this path; the handler's sole
   * purpose is to ensure that {@link #subDocumentHandler()} returns a {@link
   * VectorEmbeddedDocumentHandler} so that traversal continues with embedded-root awareness until
   * the actual {@code nestedRoot} is reached.
   */
  static FieldValueHandler createAncestor(
      Optional<FieldValueHandler> wrappedValueHandler,
      DocumentBlock documentBlock,
      VectorIndexFieldMapping mapping,
      IndexingMetricsUpdater indexingMetricsUpdater,
      IndexCapabilities indexCapabilities,
      FieldPath path,
      byte[] id,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    return new VectorEmbeddedFieldValueHandler(
        wrappedValueHandler,
        documentBlock,
        null, // ancestor path — no field definition
        mapping,
        indexingMetricsUpdater,
        indexCapabilities,
        path,
        id,
        autoEmbeddings);
  }

  @Override
  public void handleBinary(Supplier<Binary> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleBinary(supplier));
  }

  @Override
  public void handleBoolean(Supplier<Boolean> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleBoolean(supplier));
  }

  @Override
  public void handleDateTime(Supplier<Long> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleDateTime(supplier));
  }

  @Override
  public void handleDouble(Supplier<Double> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleDouble(supplier));
  }

  @Override
  public void handleGeometry(Supplier<Optional<Geometry>> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleGeometry(supplier));
  }

  @Override
  public void handleInt32(Supplier<Integer> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleInt32(supplier));
  }

  @Override
  public void handleInt64(Supplier<Long> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleInt64(supplier));
  }

  @Override
  public void handleKnnVector(Supplier<Optional<Vector>> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleKnnVector(supplier));
  }

  @Override
  public void handleNull() {
    this.wrappedFieldValueHandler.ifPresent(FieldValueHandler::handleNull);
  }

  @Override
  public void handleObjectId(Supplier<ObjectId> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleObjectId(supplier));
  }

  @Override
  public void handleString(Supplier<String> supplier) throws IOException {
    if (this.wrappedFieldValueHandler.isPresent()) {
      this.wrappedFieldValueHandler.get().handleString(supplier);
    }
  }

  @Override
  public void handleUuid(Supplier<Optional<UUID>> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleUuid(supplier));
  }

  @Override
  public void handleRawBsonValue(Supplier<BsonValue> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleRawBsonValue(supplier));
  }

  @Override
  public void markFieldNameExists() {
    this.wrappedFieldValueHandler.ifPresent(FieldValueHandler::markFieldNameExists);
  }

  @Override
  public Optional<FieldValueHandler> arrayFieldValueHandler() {
    return Optional.of(
        VectorEmbeddedFieldValueHandler.create(
            this.wrappedFieldValueHandler.flatMap(FieldValueHandler::arrayFieldValueHandler),
            this.documentBlock,
            this.fieldDefinition,
            this.mapping,
            this.indexingMetricsUpdater,
            this.indexCapabilities,
            this.path,
            this.id,
            this.autoEmbeddings));
  }

  /**
   * Creates a sub document handler for a field at this path.
   *
   * <p>If the field is configured to create embedded documents (i.e., it's a
   * VectorEmbeddedDocumentsFieldDefinition), create a new {@link VectorEmbeddedDocumentBuilder} to
   * build the new embedded document, and return it along with the wrapped sub document handler.
   *
   * <p>If this handler is for an intermediate ancestor path of the {@code nestedRoot} (i.e., {@code
   * fieldDefinition} is null), the sub-document handler is wrapped in a {@link
   * VectorEmbeddedDocumentHandler} so that traversal continues with embedded-root awareness.
   */
  @Override
  public Optional<DocumentHandler> subDocumentHandler() {
    Optional<DocumentHandler> wrappedHandler = wrappedSubDocumentHandler();

    // Check if this field is a VectorEmbeddedDocumentsFieldDefinition
    if (this.fieldDefinition instanceof VectorEmbeddedDocumentsFieldDefinition embeddedDefinition) {
      // Create a new embedded document builder for this embedded document
      VectorEmbeddedDocumentBuilder embeddedBuilder =
          VectorEmbeddedDocumentBuilder.create(
              this.documentBlock,
              embeddedDefinition,
              this.mapping,
              this.id,
              this.path,
              this.indexingMetricsUpdater,
              this.indexCapabilities,
              this.autoEmbeddings);

      // If there's also a wrapped handler, we need to handle both
      // For now, return the embedded builder (similar to search index pattern)
      return Optional.of(embeddedBuilder);
    }

    // No embedded document definition, return the wrapped handler
    return wrappedHandler;
  }

  /**
   * Get the sub-{@link DocumentHandler} for the wrapped {@link FieldValueHandler}.
   *
   * <p>When this handler is for an intermediate ancestor path of the {@code nestedRoot} (i.e.,
   * {@code fieldDefinition} is null), the sub-document handler must be wrapped in a {@link
   * VectorEmbeddedDocumentHandler} so that the {@code isNestedRoot} check fires at the correct
   * depth as the BSON processor descends further into the document tree.
   */
  private Optional<DocumentHandler> wrappedSubDocumentHandler() {
    Optional<DocumentHandler> wrappedSubDocumentHandler =
        this.wrappedFieldValueHandler.flatMap(FieldValueHandler::subDocumentHandler);

    // fieldDefinition is null only for ancestor paths of the nestedRoot (created via
    // createAncestor). In that case we must keep embedded-root awareness alive by wrapping the
    // sub-document handler in a VectorEmbeddedDocumentHandler.
    if (this.fieldDefinition == null) {
      return wrappedSubDocumentHandler.map(
          handler ->
              VectorEmbeddedDocumentHandler.create(
                  handler,
                  null, // no embedded definition yet — still descending toward nestedRoot
                  this.mapping,
                  this.indexingMetricsUpdater,
                  this.indexCapabilities,
                  this.documentBlock,
                  this.path,
                  this.id,
                  this.autoEmbeddings));
    }

    return wrappedSubDocumentHandler;
  }
}
