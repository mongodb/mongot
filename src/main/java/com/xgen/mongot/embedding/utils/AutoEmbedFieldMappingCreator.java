package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.HASH_FIELD_SUFFIX;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping.AutoEmbedField;
import com.xgen.mongot.embedding.VectorAutoEmbedFieldMapping;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.util.FieldPath;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory methods for creating {@link AutoEmbedFieldMapping} instances from index definitions. */
public class AutoEmbedFieldMappingCreator {

  private static final Logger LOG = LoggerFactory.getLogger(AutoEmbedFieldMappingCreator.class);

  private static final String HASH_FIELD_PREFIX = "_autoEmbed._hash";

  /** Creates an {@code AutoEmbedFieldMapping} from a source {@link VectorIndexDefinition}. */
  public static AutoEmbedFieldMapping createAutoEmbedMapping(
      VectorIndexDefinition indexDefinition) {
    VectorIndexFieldMapping mappings = indexDefinition.getMappings();
    ImmutableMap.Builder<FieldPath, AutoEmbedField> builder = ImmutableMap.builder();
    for (Map.Entry<FieldPath, VectorIndexFieldDefinition> entry : mappings.fieldMap().entrySet()) {
      FieldPath path = entry.getKey();
      VectorIndexFieldDefinition field = entry.getValue();
      switch (field.getType()) {
        case AUTO_EMBED -> {
          var spec = field.asVectorAutoEmbedField().specification();
          builder.put(path, new AutoEmbedField.EmbedField(path, spec));
        }
        case TEXT -> {
          var spec = field.asVectorTextField().specification();
          builder.put(path, new AutoEmbedField.EmbedField(path, spec));
        }
        case FILTER -> builder.put(path, new AutoEmbedField.PassthroughField(path));
        case VECTOR -> {
          // Output field written by the embedding provider, not a source field.
        }
        case EMBEDDED_DOCUMENTS -> {
          // Structural array marker, not a leaf field to process.
        }
      }
    }
    return new VectorAutoEmbedFieldMapping(builder.build());
  }

  /**
   * Creates a new AutoEmbedFieldMapping from a VectorIndexDefinition by converting AUTO_EMBED
   * fields and adding hash fields for a materialized view.
   *
   * <p>For example: {'title': {'path': 'title', 'type': 'autoEmbed', 'model': 'voyage-4',
   * 'modality': 'text'}, 'plot': {'path': 'plot', 'type': 'filter'}}
   *
   * <p>will be converted to:
   *
   * <p>{'_autoEmbed.title': {'path': '_autoEmbed.title', 'type': 'autoEmb', 'numDimensions': 1024,
   * 'similarity': 'dotProduct'}, '_autoEmbed._hash.title': {'path': '_autoEmbed._hash.title',
   * 'type': 'filter'}, 'plot': {'path': 'plot', 'type': 'filter'}}
   *
   * <p>AUTO_EMBED field paths are remapped according to the schema metadata. FILTER and TEXT fields
   * are preserved as-is. VECTOR and EMBEDDED_DOCUMENTS fields are omitted. For each AUTO_EMBED
   * field, a corresponding hash field is added as a passthrough field.
   */
  public static AutoEmbedFieldMapping createMatViewAutoEmbedMapping(
      VectorIndexDefinition indexDefinition, MaterializedViewSchemaMetadata schemaMetadata) {
    VectorIndexFieldMapping mappings = indexDefinition.getMappings();
    ImmutableMap.Builder<FieldPath, AutoEmbedField> builder = ImmutableMap.builder();
    for (Map.Entry<FieldPath, VectorIndexFieldDefinition> entry : mappings.fieldMap().entrySet()) {
      FieldPath path = entry.getKey();
      VectorIndexFieldDefinition field = entry.getValue();
      switch (field.getType()) {
        case AUTO_EMBED -> {
          var spec = field.asVectorAutoEmbedField().specification();
          FieldPath matViewPath =
              getMatViewFieldPath(path, schemaMetadata.autoEmbeddingFieldsMapping());
          builder.put(matViewPath, new AutoEmbedField.EmbedField(matViewPath, spec));
          FieldPath hashPath =
              getHashFieldPath(path, schemaMetadata.materializedViewSchemaVersion());
          builder.put(hashPath, new AutoEmbedField.PassthroughField(hashPath));
        }
        case TEXT -> {
          var spec = field.asVectorTextField().specification();
          builder.put(path, new AutoEmbedField.EmbedField(path, spec));
        }
        case FILTER -> builder.put(path, new AutoEmbedField.PassthroughField(path));
        case VECTOR -> {
          // Output field written by the embedding provider, not a source field.
        }
        case EMBEDDED_DOCUMENTS -> {
          // Structural array marker, not a leaf field to process.
        }
      }
    }
    return new VectorAutoEmbedFieldMapping(builder.build());
  }

  /**
   * Returns the hash field path for the given field path by materialized view schema version.
   *
   * <p>For version 0, appends _hash to the leaf. For version 1+, prepends '_autoEmbed._hash.'.
   */
  public static FieldPath getHashFieldPath(FieldPath fieldPath, long matViewSchemaVersion) {
    // TODO(CLOUDP-363914): build hash field from MV schema metadata, not by version.
    if (matViewSchemaVersion == 0) {
      return fieldPath
          .getParent()
          .map(path -> path.newChild(fieldPath.getLeaf() + HASH_FIELD_SUFFIX))
          .orElse(FieldPath.newRoot(fieldPath.getLeaf() + HASH_FIELD_SUFFIX));
    }
    return FieldPath.parse(HASH_FIELD_PREFIX + FieldPath.DELIMITER + fieldPath.toString());
  }

  /**
   * Converts a source auto-embedding field path to the materialized view field path using the
   * schema fields mapping. Returns the original path if no mapping exists.
   */
  public static FieldPath getMatViewFieldPath(
      FieldPath sourceFieldPath, Map<FieldPath, FieldPath> schemaFieldsMapping) {
    if (schemaFieldsMapping.containsKey(sourceFieldPath)) {
      return schemaFieldsMapping.get(sourceFieldPath);
    }
    return sourceFieldPath;
  }

  /**
   * Converts a source {@code nestedRoot} to the materialized view {@code nestedRoot} by mirroring
   * the path rewrite applied to auto-embed fields that live under it. Returns the original
   * {@code nestedRoot} if the {@code schemaFieldsMapping} is empty or contains no entry whose
   * source path is a descendant of {@code nestedRoot}.
   *
   * <p>The auto-embed field-path remapping (see {@link
   * com.xgen.mongot.embedding.mongodb.MaterializedViewCollectionResolver}) prepends the MV
   * namespace (e.g. {@code _autoEmbed.}) to every source auto-embed path. Because {@code
   * nestedRoot} must be an ancestor of those source paths, the MV-equivalent {@code nestedRoot} is
   * the derived path with the per-entry "tail below the source {@code nestedRoot}" stripped off.
   *
   * <p>Example: with source {@code nestedRoot = "sections"} and mapping {@code
   * "sections.section_content" -> "_autoEmbed.sections.section_content"}, the derived {@code
   * nestedRoot} is {@code "_autoEmbed.sections"}.
   *
   * <p><b>Invariant:</b> all auto-embed fields under the same source {@code nestedRoot} are
   * expected to share the same MV-prefix, and therefore produce the same derived root. This method
   * returns the derived root computed from the first matching entry encountered. If later
   * matching entries disagree, a warning is logged and the lexicographically smallest derived root
   * is returned to ensure a deterministic result regardless of map iteration order.
   */
  public static Optional<FieldPath> getMatViewNestedRoot(
      Optional<FieldPath> sourceNestedRoot, Map<FieldPath, FieldPath> schemaFieldsMapping) {
    if (sourceNestedRoot.isEmpty()) {
      return Optional.empty();
    }
    FieldPath source = sourceNestedRoot.get();
    @Var Optional<FieldPath> result = Optional.empty();
    @Var boolean inconsistencyDetected = false;
    for (Map.Entry<FieldPath, FieldPath> entry : schemaFieldsMapping.entrySet()) {
      FieldPath sourcePath = entry.getKey();
      FieldPath derivedPath = entry.getValue();
      if (!sourcePath.isChildOf(source)) {
        continue;
      }
      FieldPath derivedRoot = stripLevelsBelowRoot(source, sourcePath, derivedPath);
      if (result.isEmpty()) {
        result = Optional.of(derivedRoot);
      } else if (!result.get().equals(derivedRoot)) {
        // Invariant violation: different auto-embed fields under the same nestedRoot disagree on
        // the MV-prefix. Pick the lexicographically smallest derived root to guarantee a
        // deterministic result regardless of map iteration order. Log rather than throw so we
        // fail loud in logs but don't break index derivation over a schema inconsistency.
        inconsistencyDetected = true;
        if (derivedRoot.toString().compareTo(result.get().toString()) < 0) {
          result = Optional.of(derivedRoot);
        }
      }
    }
    if (inconsistencyDetected) {
      LOG.warn(
          "Inconsistent derived nestedRoot across auto-embed mapping entries under source"
              + " nestedRoot={}. Using lexicographically smallest derived root: {}.",
          source,
          result.get());
    }
    return result.isPresent() ? result : sourceNestedRoot;
  }

  private static FieldPath stripLevelsBelowRoot(
      FieldPath sourceNestedRoot, FieldPath sourcePath, FieldPath derivedPath) {
    int levelsBelowRoot =
        sourcePath.getPathHierarchy().size() - sourceNestedRoot.getPathHierarchy().size();
    @Var FieldPath derivedRoot = derivedPath;
    for (int i = 0; i < levelsBelowRoot; i++) {
      derivedRoot =
          derivedRoot
              .getParent()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          String.format(
                              "Derived field path has fewer segments than its source path when"
                                  + " deriving materialized-view nestedRoot: source=%s,"
                                  + " derived=%s, sourceNestedRoot=%s",
                              sourcePath, derivedPath, sourceNestedRoot)));
    }
    return derivedRoot;
  }

  private AutoEmbedFieldMappingCreator() {}
}
