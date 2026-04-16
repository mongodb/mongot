package com.xgen.mongot.embedding.utils;

import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.bson.BsonDocument;
import org.bson.BsonValue;

// TODO(CLOUDP-363302): Support stored source from vectorIndexDefinition.
/**
 * Walks a BSON document tree and copies passthrough (filter) field values into a target document.
 * Skips sub-trees that contain no passthrough fields.
 */
public class MaterializedViewDocumentHandler implements DocumentHandler {

  private final Predicate<FieldPath> shouldKeep;
  private final Predicate<FieldPath> shouldDescend;
  private final Optional<FieldPath> parentPath;
  private final BsonValue bsonValue;

  MaterializedViewDocumentHandler(
      Predicate<FieldPath> shouldKeep,
      Predicate<FieldPath> shouldDescend,
      Optional<FieldPath> parentPath,
      BsonValue bsonValue) {
    this.shouldKeep = shouldKeep;
    this.shouldDescend = shouldDescend;
    this.parentPath = parentPath;
    this.bsonValue = bsonValue;
  }

  /**
   * Creates a handler that copies passthrough fields from the given mapping into {@code bsonDoc}.
   */
  public static MaterializedViewDocumentHandler create(
      AutoEmbedFieldMapping mapping, Optional<FieldPath> parentPath, BsonDocument bsonDoc) {
    Set<FieldPath> passthroughAncestors = mapping.passthroughAncestorPaths();
    return new MaterializedViewDocumentHandler(
        mapping::isPassthrough, passthroughAncestors::contains, parentPath, bsonDoc);
  }

  @Override
  public Optional<FieldValueHandler> valueHandler(String field) {
    FieldPath fullPath = createChildFieldPath(field);
    if (!this.shouldKeep.test(fullPath) && !this.shouldDescend.test(fullPath)) {
      return Optional.empty();
    }
    return Optional.of(
        MaterializedViewFieldValueHandler.create(
            this.shouldKeep, this.shouldDescend, fullPath, this.bsonValue));
  }

  private FieldPath createChildFieldPath(String field) {
    return this.parentPath
        .map(parent -> parent.newChild(field))
        .orElseGet(() -> FieldPath.newRoot(field));
  }
}
