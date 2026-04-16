package com.xgen.mongot.embedding.utils;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.FieldPath;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Collects string values for vector text fields.
 *
 * <p>If the FieldPath, path, is found in the VectorIndexFieldMapping, and it is a vector text, then
 * the FieldHandler is called to add string values to the given set.
 *
 * <p>TODO(CLOUDP-366279): Deprecate this class and switch to CollectFieldValueDocumentHandler.
 */
public class CollectVectorTextStringsDocumentHandler implements DocumentHandler {

  private final AutoEmbedFieldMapping mapping;
  private final Optional<FieldPath> path;
  private final Map<FieldPath, Set<String>> vectorTextPathMap;

  CollectVectorTextStringsDocumentHandler(
      AutoEmbedFieldMapping mapping,
      Optional<FieldPath> path,
      Map<FieldPath, Set<String>> vectorTextPathMap) {
    this.mapping = mapping;
    this.path = path;
    this.vectorTextPathMap = vectorTextPathMap;
  }

  public static CollectVectorTextStringsDocumentHandler create(
      AutoEmbedFieldMapping mapping, Optional<FieldPath> path) {
    return new CollectVectorTextStringsDocumentHandler(mapping, path, new HashMap<>());
  }

  @Override
  public Optional<FieldValueHandler> valueHandler(String field) {
    FieldPath fullPath = createChildFieldPath(field);
    return this.mapping.childPathExists(fullPath)
        ? Optional.of(
            CollectVectorTextStringsFieldValueHandler.create(
                this.mapping, fullPath, this.vectorTextPathMap))
        : Optional.empty();
  }

  /** Returns an immutable set of vector text strings to be auto-embedded */
  public ImmutableMap<FieldPath, Set<String>> getVectorTextPathMap() {
    return ImmutableMap.copyOf(this.vectorTextPathMap);
  }

  private FieldPath createChildFieldPath(String field) {
    return this.path.map(path -> path.newChild(field)).orElseGet(() -> FieldPath.newRoot(field));
  }
}
