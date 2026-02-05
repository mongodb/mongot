package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;

public record VectorIndexFieldMapping(
    ImmutableMap<FieldPath, VectorIndexFieldDefinition> fieldMap,
    ImmutableSet<String> documentPaths) {

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

  public static VectorIndexFieldMapping create(List<VectorIndexFieldDefinition> fields) {
    return new VectorIndexFieldMapping(createMap(fields), createDocumentPathMap(fields));
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
