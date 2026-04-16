package com.xgen.mongot.embedding;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping.AutoEmbedField;
import com.xgen.mongot.util.FieldPath;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Auto-embed field mapping for vector indexes.
 *
 * <p>Vector indexes have a fixed, known set of fields. AUTO_EMBED and TEXT fields become {@link
 * AutoEmbedField.EmbedField}, FILTER fields become {@link AutoEmbedField.PassthroughField}. VECTOR
 * and EMBEDDED_DOCUMENTS fields are omitted as they are not relevant to auto-embedding document
 * processing. Ancestor paths for tree traversal are inferred from the included field paths.
 */
public record VectorAutoEmbedFieldMapping(
    Map<FieldPath, AutoEmbedField> fieldMap,
    Set<FieldPath> ancestorPaths,
    Set<FieldPath> passthroughAncestorPaths,
    ImmutableMap<FieldPath, AutoEmbedField.EmbedField> embedFields)
    implements AutoEmbedFieldMapping {

  /** Creates a mapping that infers ancestor and passthrough ancestor paths from the field map. */
  public VectorAutoEmbedFieldMapping(Map<FieldPath, AutoEmbedField> fieldMap) {
    this(
        fieldMap,
        computeAncestorPaths(fieldMap),
        computePassthroughAncestorPaths(fieldMap),
        computeEmbedFields(fieldMap));
  }

  @Override
  public boolean childPathExists(FieldPath path) {
    return this.fieldMap.containsKey(path) || subDocumentExists(path);
  }

  @Override
  public boolean subDocumentExists(FieldPath path) {
    return this.ancestorPaths.contains(path);
  }

  @Override
  public Optional<AutoEmbedField> getField(FieldPath path) {
    return Optional.ofNullable(this.fieldMap.get(path));
  }

  @Override
  public boolean isEmbed(FieldPath path) {
    return getField(path).filter(AutoEmbedField.EmbedField.class::isInstance).isPresent();
  }

  @Override
  public boolean isPassthrough(FieldPath path) {
    return getField(path).filter(AutoEmbedField.PassthroughField.class::isInstance).isPresent();
  }

  private static ImmutableMap<FieldPath, AutoEmbedField.EmbedField> computeEmbedFields(
      Map<FieldPath, AutoEmbedField> fields) {
    return fields.entrySet().stream()
        .filter(e -> e.getValue() instanceof AutoEmbedField.EmbedField)
        .collect(
            ImmutableMap.toImmutableMap(
                Map.Entry::getKey, e -> (AutoEmbedField.EmbedField) e.getValue()));
  }

  private static ImmutableSet<FieldPath> computeAncestorPaths(
      Map<FieldPath, AutoEmbedField> fields) {
    return fields.keySet().stream()
        .flatMap(FieldPath::ancestorPaths)
        .collect(ImmutableSet.toImmutableSet());
  }

  private static ImmutableSet<FieldPath> computePassthroughAncestorPaths(
      Map<FieldPath, AutoEmbedField> fields) {
    return fields.entrySet().stream()
        .filter(e -> e.getValue() instanceof AutoEmbedField.PassthroughField)
        .flatMap(e -> e.getKey().ancestorPaths())
        .collect(ImmutableSet.toImmutableSet());
  }
}
