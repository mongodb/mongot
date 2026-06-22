package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import java.util.Objects;
import java.util.Optional;

public class VectorFieldDefinitionResolver implements FieldDefinitionResolver {

  private final VectorIndexDefinition definition;
  private final IndexCapabilities indexCapabilities;

  public VectorFieldDefinitionResolver(
      VectorIndexDefinition definition, IndexFormatVersion indexFormatVersion) {
    this.definition = definition;
    this.indexCapabilities = definition.getIndexCapabilities(indexFormatVersion);
  }

  @Override
  public boolean isUsed(FieldPath path) {
    return this.definition.getFields().stream()
        .map(VectorIndexFieldDefinition::getPath)
        .anyMatch(configuredPath -> configuredPath.getPathHierarchy().contains(path));
  }

  public IndexCapabilities getIndexCapabilities() {
    return this.indexCapabilities;
  }

  public boolean isIndexed(FieldPath path, VectorIndexFieldDefinition.Type type) {
    return this.definition.getFields().stream()
        .filter(definition -> definition.getType() == type)
        .map(VectorIndexFieldDefinition::getPath)
        .anyMatch(configuredPath -> Objects.equals(configuredPath, path));
  }

  /**
   * Checks if a field is indexed with the specified type, considering the embedded root context.
   *
   * <p>For filter fields in vector search queries, the field path in the query must match exactly
   * what's defined in the index definition, regardless of whether querying embedded vectors or not.
   * The embeddedRoot parameter is provided for context but does not affect path matching for
   * filters.
   *
   * <p>Examples: - Index has filter on "sections.section_name" - Query must use filter:
   * {"sections.section_name": "value"} (full path) - Query cannot use filter: {"section_name":
   * "value"} (relative path)
   *
   * @param path the field path to check (must be the full absolute path as defined in index)
   * @param embeddedRoot the embedded root context, if any (provided for context, not used for path
   *     matching)
   * @param type the field definition type to check for
   * @return true if the field is indexed with the specified type
   */
  public boolean isIndexed(
      FieldPath path, Optional<FieldPath> embeddedRoot, VectorIndexFieldDefinition.Type type) {
    // Always use exact path matching, regardless of embedded context
    // The query must specify the full path as defined in the index
    return isIndexed(path, type);
  }

  @Override
  public Optional<VectorFieldSpecification> getVectorFieldSpecification(FieldPath path) {
    return this.definition.getFields().stream()
        .filter(definition -> Objects.equals(definition.getPath(), path))
        .filter(VectorIndexFieldDefinition::isVectorField)
        .map(VectorIndexFieldDefinition::asVectorField)
        .map(VectorIndexVectorFieldDefinition::specification)
        .findFirst();
  }

  public ImmutableList<VectorIndexFieldDefinition> getFields() {
    return this.definition.getFields();
  }

  /**
   * Returns the nested root path for embedded vector documents, if specified at the index level.
   *
   * @return Optional containing the nested root path, or empty if not an embedded vector index
   */
  public Optional<FieldPath> getNestedRoot() {
    return this.definition.getNestedRoot();
  }
}
