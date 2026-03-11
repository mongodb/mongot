package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapping of vector index field definitions to their paths.
 *
 * <p>This class tracks:
 *
 * <ul>
 *   <li>Field definitions mapped by their paths
 *   <li>Document paths (ancestor paths of all fields)
 *   <li>Embedded vector roots (array fields containing subdocuments with vectors)
 * </ul>
 */
public record VectorIndexFieldMapping(
    ImmutableMap<FieldPath, VectorIndexFieldDefinition> fieldMap,
    ImmutableSet<String> documentPaths,
    Optional<FieldPath> nestedRoot) {

  private static final Logger LOG = LoggerFactory.getLogger(VectorIndexFieldMapping.class);

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
      List<VectorIndexFieldDefinition> fields, Optional<FieldPath> nestedRoot) {
    ImmutableSet<String> documentPaths = createDocumentPathMap(fields);
    @Var VectorIndexFieldMapping mapping =
        new VectorIndexFieldMapping(createMap(fields), documentPaths, nestedRoot);

    if (nestedRoot.isPresent()
        && !mapping.subDocumentExists(nestedRoot.get())
        && !mapping.childPathExists(nestedRoot.get())) {
      LOG.warn(
          "nestedRoot is set but no field path is under it: {}. "
              + "Treating as no nested root; fields will not be indexed as nested.",
          nestedRoot.get());
      mapping =
          new VectorIndexFieldMapping(createMap(fields), documentPaths, Optional.empty());
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
