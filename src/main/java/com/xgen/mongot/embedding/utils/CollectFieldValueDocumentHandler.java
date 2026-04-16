package com.xgen.mongot.embedding.utils;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.FieldPath;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * A document handler that collects all field values from a given document. Callers can either
 * choose to collect all field values, or they can choose to provide an index field mapping and a
 * predicate, and only collect field values that are part of the index field mapping and match the
 * predicate.
 */
public class CollectFieldValueDocumentHandler implements DocumentHandler {

  private final AutoEmbedFieldMapping mapping;
  private final Optional<FieldPath> path;
  private final Map<FieldPath, List<BsonValue>> fieldPathValueMap;
  private final Set<FieldPath> arrayPaths;

  // TODO(CLOUDP-366279): Callers can currently provide both the predicate and set
  // onlyCollectFieldsinMapping to true, but we currently ignore the predicate in such cases. We
  // should refactor this to allow callers to only set one of the two.
  private final Predicate<FieldPath> fieldPredicate;
  private final boolean onlyCollectFieldsinMapping;

  CollectFieldValueDocumentHandler(
      AutoEmbedFieldMapping mapping,
      Optional<FieldPath> path,
      Map<FieldPath, List<BsonValue>> fieldPathValueMap,
      Set<FieldPath> arrayPaths,
      Predicate<FieldPath> fieldPredicate,
      boolean onlyCollectFieldsinMapping) {
    this.mapping = mapping;
    this.path = path;
    this.fieldPathValueMap = fieldPathValueMap;
    this.arrayPaths = arrayPaths;
    this.fieldPredicate = fieldPredicate;
    this.onlyCollectFieldsinMapping = onlyCollectFieldsinMapping;
  }

  public static CollectFieldValueDocumentHandler create(
      AutoEmbedFieldMapping mapping,
      Optional<FieldPath> path,
      Predicate<FieldPath> fieldPredicate,
      boolean onlyCollectFieldsinMapping) {
    return new CollectFieldValueDocumentHandler(
        mapping,
        path,
        new HashMap<>(),
        new HashSet<>(),
        fieldPredicate,
        onlyCollectFieldsinMapping);
  }

  @Override
  public Optional<FieldValueHandler> valueHandler(String field) {
    FieldPath fullPath = createChildFieldPath(field);
    if (this.onlyCollectFieldsinMapping && !this.mapping.childPathExists(fullPath)) {
      return Optional.empty();
    }
    return Optional.of(
        CollectFieldValueHandler.create(
            this.mapping,
            fullPath,
            this.fieldPathValueMap,
            this.arrayPaths,
            this.fieldPredicate,
            this.onlyCollectFieldsinMapping));
  }

  /** Returns an immutable map of field paths to collected values */
  public ImmutableMap<FieldPath, List<BsonValue>> getCollectedValues() {
    return ImmutableMap.copyOf(this.fieldPathValueMap);
  }

  /**
   * Returns collected field values as a BsonDocument ready for $set operations. Array fields are
   * reconstructed as BsonArray, scalar fields use the first collected value.
   */
  public BsonDocument toBsonDocument() {
    BsonDocument result = new BsonDocument();
    for (Map.Entry<FieldPath, List<BsonValue>> entry : this.fieldPathValueMap.entrySet()) {
      FieldPath path = entry.getKey();
      List<BsonValue> values = entry.getValue();
      if (!values.isEmpty()) {
        BsonValue reconstructedValue =
            this.arrayPaths.contains(path) ? new BsonArray(values) : values.get(0);
        result.put(path.toString(), reconstructedValue);
      }
    }
    return result;
  }

  private FieldPath createChildFieldPath(String field) {
    return this.path.map(path -> path.newChild(field)).orElseGet(() -> FieldPath.newRoot(field));
  }
}
