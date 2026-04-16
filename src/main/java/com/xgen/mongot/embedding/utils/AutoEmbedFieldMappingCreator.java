package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.HASH_FIELD_SUFFIX;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping.AutoEmbedField;
import com.xgen.mongot.embedding.VectorAutoEmbedFieldMapping;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.util.FieldPath;
import java.util.Map;

/** Factory methods for creating {@link AutoEmbedFieldMapping} instances from index definitions. */
public class AutoEmbedFieldMappingCreator {

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

  private AutoEmbedFieldMappingCreator() {}
}
