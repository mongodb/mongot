package com.xgen.mongot.index.lucene.document.block;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.definition.VectorEmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;
import org.jetbrains.annotations.Nullable;

/**
 * Similar to {@link VectorEmbeddedDocumentBuilder}, except not at the root of a document block.
 * Does not know how to build a {@code List<Document>} document block, and always has a FieldPath.
 *
 * <p>This class is responsible for creating field value handlers for embedded documents in vector
 * indexes, handling both regular fields and nested embedded documents.
 */
class VectorEmbeddedDocumentHandler implements DocumentHandler {
  final DocumentHandler wrappedFieldHandler;
  // Null when this handler is for an intermediate ancestor path of the nestedRoot (i.e., we are
  // still descending toward the nestedRoot and have not yet reached it).
  @Nullable final VectorEmbeddedDocumentsFieldDefinition fieldDefinition;
  final VectorIndexFieldMapping mapping;
  final DocumentBlock documentBlock;
  final FieldPath fieldPath;
  final byte[] id;
  final IndexingMetricsUpdater indexingMetricsUpdater;
  final IndexCapabilities indexCapabilities;
  final ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings;

  private VectorEmbeddedDocumentHandler(
      DocumentHandler wrappedFieldHandler,
      @Nullable VectorEmbeddedDocumentsFieldDefinition fieldDefinition,
      VectorIndexFieldMapping mapping,
      IndexingMetricsUpdater indexingMetricsUpdater,
      IndexCapabilities indexCapabilities,
      DocumentBlock documentBlock,
      FieldPath fieldPath,
      byte[] id,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    this.wrappedFieldHandler = wrappedFieldHandler;
    this.fieldDefinition = fieldDefinition;
    this.mapping = mapping;
    this.indexingMetricsUpdater = indexingMetricsUpdater;
    this.indexCapabilities = indexCapabilities;
    this.documentBlock = documentBlock;
    this.fieldPath = fieldPath;
    this.id = id;
    this.autoEmbeddings = autoEmbeddings;
  }

  static DocumentHandler create(
      DocumentHandler wrappedFieldHandler,
      @Nullable VectorEmbeddedDocumentsFieldDefinition fieldDefinition,
      VectorIndexFieldMapping mapping,
      IndexingMetricsUpdater indexingMetricsUpdater,
      IndexCapabilities indexCapabilities,
      DocumentBlock documentBlock,
      FieldPath fieldPath,
      byte[] id,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    return new VectorEmbeddedDocumentHandler(
        wrappedFieldHandler,
        fieldDefinition,
        mapping,
        indexingMetricsUpdater,
        indexCapabilities,
        documentBlock,
        fieldPath,
        id,
        autoEmbeddings);
  }

  /**
   * Create a {@link FieldValueHandler} for a value at a particular leaf path.
   *
   * <p>See {@link VectorEmbeddedDocumentHandler#valueHandler(DocumentHandler, DocumentBlock,
   * VectorEmbeddedDocumentsFieldDefinition, VectorIndexFieldMapping, IndexingMetricsUpdater,
   * IndexCapabilities, FieldPath, byte[], ImmutableMap)}.
   */
  @Override
  public Optional<FieldValueHandler> valueHandler(String leafPath) {
    return valueHandler(
        this.wrappedFieldHandler,
        this.documentBlock,
        this.fieldDefinition,
        this.mapping,
        this.indexingMetricsUpdater,
        this.indexCapabilities,
        this.fieldPath.newChild(leafPath),
        this.id,
        this.autoEmbeddings);
  }

  /**
   * Create a {@link FieldValueHandler} for a field at absolute path {@code
   * fieldValueHandlerAbsolutePath}.
   *
   * <p>If there is a statically-defined field definition for that field, return the {@link
   * FieldValueHandler} created from this class's wrapped {@link DocumentHandler} wrapped in a
   * {@link VectorEmbeddedFieldValueHandler}, because there may be some embedded field at or under
   * this field that we may want to create a new embedded document for.
   *
   * <p>If there is no statically-defined document field for this field, there cannot be an embedded
   * field defined at this field or in a subfield of a document at this field, so we can return the
   * {@link FieldValueHandler} from the wrapped {@link DocumentHandler} without wrapping it in a
   * {@link VectorEmbeddedFieldValueHandler}.
   */
  static Optional<FieldValueHandler> valueHandler(
      DocumentHandler documentHandler,
      DocumentBlock documentBlock,
      @Nullable VectorEmbeddedDocumentsFieldDefinition fieldDefinition,
      VectorIndexFieldMapping mapping,
      IndexingMetricsUpdater indexingMetricsUpdater,
      IndexCapabilities indexCapabilities,
      FieldPath fieldValueHandlerAbsolutePath,
      byte[] id,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    String leafPath = fieldValueHandlerAbsolutePath.getLeaf();

    // Check if this path is the nested root for embedded vectors
    if (mapping.isNestedRoot(fieldValueHandlerAbsolutePath)) {
      // Create a VectorEmbeddedDocumentsFieldDefinition for this embedded root
      VectorEmbeddedDocumentsFieldDefinition embeddedDefinition =
          VectorEmbeddedDocumentsFieldDefinition.create(fieldValueHandlerAbsolutePath, mapping);

      // Create a VectorEmbeddedFieldValueHandler that will handle the embedded documents
      return Optional.of(
              VectorEmbeddedFieldValueHandler.create(
                  documentHandler.valueHandler(leafPath),
                  documentBlock,
                  embeddedDefinition,
                  mapping,
                  indexingMetricsUpdater,
                  indexCapabilities,
                  fieldValueHandlerAbsolutePath,
                  id,
                  autoEmbeddings))
          .or(() -> documentHandler.valueHandler(leafPath));
    }

    // Check if this path is an intermediate ancestor of the nested root (e.g. "sections" when
    // nestedRoot is "sections.paragraphs"). We must keep descending with embedded-root awareness
    // so that the nestedRoot check above fires at the correct depth.
    if (mapping.isAncestorOfNestedRoot(fieldValueHandlerAbsolutePath)) {
      return Optional.of(
          VectorEmbeddedFieldValueHandler.createAncestor(
              documentHandler.valueHandler(leafPath),
              documentBlock,
              mapping,
              indexingMetricsUpdater,
              indexCapabilities,
              fieldValueHandlerAbsolutePath,
              id,
              autoEmbeddings));
    }

    // If fieldDefinition is null (at root level), check the mapping directly
    @Var VectorIndexFieldDefinition field = null;
    if (fieldDefinition != null) {
      field = fieldDefinition.fields().get(leafPath);
    } else {
      // At root level, get the field definition from the mapping
      field = mapping.getFieldDefinition(fieldValueHandlerAbsolutePath).orElse(null);
    }

    if (field != null) {
      return Optional.of(
              VectorEmbeddedFieldValueHandler.create(
                  documentHandler.valueHandler(leafPath),
                  documentBlock,
                  field,
                  mapping,
                  indexingMetricsUpdater,
                  indexCapabilities,
                  fieldValueHandlerAbsolutePath,
                  id,
                  autoEmbeddings))
          .or(() -> documentHandler.valueHandler(leafPath));
    }
    return documentHandler.valueHandler(leafPath);
  }
}
