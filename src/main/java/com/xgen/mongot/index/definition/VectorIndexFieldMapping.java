package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;

public record VectorIndexFieldMapping(
    ImmutableMap<FieldPath, VectorIndexFieldDefinition> fieldMap,
    ImmutableSet<String> documentPaths,
    Optional<FieldPath> nestedRoot) {

  public Optional<VectorIndexFieldDefinition> getFieldDefinition(FieldPath path) {
    return Optional.ofNullable(this.fieldMap.get(path));
  }

  public Optional<VectorQuantization> getQuantizationForField(FieldPath path) {
    return getFieldDefinition(path)
        .filter(VectorIndexFieldDefinition::isVectorField)
        .map(VectorIndexFieldDefinition::asVectorField)
        .map(field -> field.specification().quantization());
  }

  public boolean childPathExists(FieldPath path) {
    return this.fieldMap.containsKey(path) || subDocumentExists(path);
  }

  public boolean subDocumentExists(FieldPath path) {
    return this.documentPaths.contains(path.toString());
  }

  public boolean isNestedRoot(FieldPath path) {
    return this.nestedRoot.isPresent() && this.nestedRoot.get().equals(path);
  }

  public boolean hasNestedRoot() {
    return this.nestedRoot.isPresent();
  }

  public static VectorIndexFieldMapping create(
      List<VectorIndexFieldDefinition> fields, Optional<FieldPath> nestedRoot)
      throws IllegalArgumentException {
    ImmutableSet<String> documentPaths = createDocumentPathMap(fields);
    VectorIndexFieldMapping mapping =
        new VectorIndexFieldMapping(createMap(fields), documentPaths, nestedRoot);

    if (nestedRoot.isPresent()
        && !mapping.subDocumentExists(nestedRoot.get())
        && !mapping.childPathExists(nestedRoot.get())) {
      throw new IllegalArgumentException(
          String.format("nestedRoot is set but no field path is under it: %s", nestedRoot.get()));
    }

    return mapping;
  }

  private static ImmutableMap<FieldPath, VectorIndexFieldDefinition> createMap(
      List<VectorIndexFieldDefinition> fields) {
    return fields.stream()
        .collect(
            ImmutableMap.toImmutableMap(
                VectorIndexFieldDefinition::getPath,
                field -> field,
                VectorIndexFieldDefinition::throwDuplicateException));
  }

  private static ImmutableSet<String> createDocumentPathMap(
      List<VectorIndexFieldDefinition> fields) {
    return fields.stream()
        .flatMap(field -> field.getPath().ancestorPaths())
        .map(FieldPath::toString)
        .collect(ImmutableSet.toImmutableSet());
  }
}
