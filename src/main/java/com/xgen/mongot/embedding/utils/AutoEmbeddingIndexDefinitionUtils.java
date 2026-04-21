package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import static com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator.getMatViewFieldPath;
import static com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator.getMatViewNestedRoot;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.IllegalEmbeddedFieldException;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.util.FieldPath;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class AutoEmbeddingIndexDefinitionUtils {

  /**
   * Converts original raw VectorIndexDefinition in source collection to normalized
   * VectorIndexDefinition with following changes:
   *
   * <p>1. Converts VectorIndexFieldDefinition fields from AUTO_EMBED TO VECTOR type by input
   * materializedView schemaFieldsMapping. The mapping from VectorProviderQuantization to
   * VectorQuantization happens in VectorAutoEmbedFieldDefinition::fromBson, and the lucene
   * quantization type will be stored in autoEmbedField.specification().
   *
   * <p>For example: {'title': {'path': 'title', 'type': 'autoEmbed', 'model': 'voyage-4',
   * 'modality': 'text', 'quantization': 'float', numDimensions: 1024, similarity: 'dotProduct'},
   * 'plot': {'path': 'plot', 'type': 'filter'}}
   *
   * <p>will be converted to:
   *
   * <p>{'_autoEmbed.title': {'path': '_autoEmbed.title', 'type': 'vector', 'numDimensions': 1024,
   * 'similarity': 'dotProduct', 'quantization': 'none', numDimensions: 1024, similarity:
   * 'dotProduct'}, 'plot': {'path': 'plot', 'type': 'filter'}}
   *
   * <p>2. Replaces the collection UUID with the materialized view collection UUID, updates database
   * name.
   */
  public static VectorIndexDefinition getDerivedVectorIndexDefinition(
      VectorIndexDefinition rawDefinition,
      String databaseName,
      UUID materializedViewCollectionUuid,
      MaterializedViewSchemaMetadata schemaMetadata) {
    return new VectorIndexDefinition(
        rawDefinition.getIndexId(),
        rawDefinition.getName(),
        databaseName,
        rawDefinition.getIndexId().toHexString(),
        materializedViewCollectionUuid,
        Optional.empty(),
        rawDefinition.getNumPartitions(),
        getDerivedVectorIndexFields(rawDefinition.getFields(), schemaMetadata),
        rawDefinition.getParsedIndexFeatureVersion(),
        rawDefinition.getDefinitionVersion(),
        rawDefinition.getDefinitionVersionCreatedAt(),
        Optional.empty(), // TODO(CLOUDP-363302)
        // Remap nestedRoot so it lives in the same namespace as the (possibly remapped) vector
        // field paths on the materialized view. Without this, embedded-vector detection at query
        // time (VectorSearchQueryFactory#determineEmbeddedRoot) fails because the field path is
        // prefixed (e.g. "_autoEmbed.sections.section_content") while nestedRoot is not
        // ("sections"), causing the isChildOf check to return false. See CLOUDP-398738.
        getMatViewNestedRoot(
            rawDefinition.getNestedRoot(), schemaMetadata.autoEmbeddingFieldsMapping()),
        rawDefinition.getIndexIdAtCreationTime(),
        rawDefinition.getAutoEmbeddingDefinitionVersion(),
        rawDefinition.getMaterializedViewNameFormatVersion());
  }

  /**
   * Creates a derived search index definition where auto-embed fields are replaced with regular
   * vector fields. The derived definition points to the materialized view collection.
   */
  public static SearchIndexDefinition getDerivedSearchIndexDefinition(
      SearchIndexDefinition rawDefinition,
      String databaseName,
      UUID materializedViewCollectionUuid,
      MaterializedViewSchemaMetadata schemaMetadata) {
    DocumentFieldDefinition derivedMappings =
        getDerivedSearchMappings(rawDefinition.getMappings(), schemaMetadata);
    return SearchIndexDefinition.create(
        rawDefinition.getIndexId(),
        rawDefinition.getName(),
        databaseName,
        rawDefinition.getIndexId().toHexString(),
        materializedViewCollectionUuid,
        Optional.empty(),
        rawDefinition.getNumPartitions(),
        derivedMappings,
        rawDefinition.getAnalyzerName(),
        rawDefinition.getSearchAnalyzerName(),
        rawDefinition.getAnalyzers().isEmpty()
            ? Optional.empty()
            : Optional.of(rawDefinition.getAnalyzers()),
        rawDefinition.getParsedIndexFeatureVersion(),
        rawDefinition.getSynonyms(),
        Optional.empty(),
        rawDefinition.getTypeSets(),
        rawDefinition.getSort(),
        rawDefinition.getDefinitionVersion(),
        rawDefinition.getDefinitionVersionCreatedAt(),
        rawDefinition.getIndexIdAtCreationTime(),
        rawDefinition.getAutoEmbeddingDefinitionVersion(),
        rawDefinition.getMaterializedViewNameFormatVersion());
  }

  private static List<VectorIndexFieldDefinition> getDerivedVectorIndexFields(
      List<VectorIndexFieldDefinition> rawFields, MaterializedViewSchemaMetadata schemaMetadata) {
    return rawFields.stream()
        .map(
            field -> {
              if (field.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED) {
                VectorAutoEmbedFieldDefinition autoEmbedField = field.asVectorAutoEmbedField();
                return new VectorDataFieldDefinition(
                    getMatViewFieldPath(
                        field.getPath(), schemaMetadata.autoEmbeddingFieldsMapping()),
                    toVectorFieldSpecification(autoEmbedField.specification()));
              }
              return field;
            })
        .toList();
  }

  private static DocumentFieldDefinition getDerivedSearchMappings(
      DocumentFieldDefinition rawMappings, MaterializedViewSchemaMetadata schemaMetadata) {
    Map<String, FieldDefinition> derivedFields = new LinkedHashMap<>();
    for (Map.Entry<String, FieldDefinition> entry : rawMappings.fields().entrySet()) {
      FieldDefinition fieldDef = entry.getValue();
      if (fieldDef.searchAutoEmbedFieldDefinition().isPresent()) {
        SearchAutoEmbedFieldDefinition autoEmbed =
            fieldDef.searchAutoEmbedFieldDefinition().get();
        SearchIndexVectorFieldDefinition vectorField =
            new SearchIndexVectorFieldDefinition(autoEmbed.specification());
        // Remap the field key to the mat-view path based on sourceField
        FieldPath matViewPath =
            getMatViewFieldPath(
                autoEmbed.sourceField(), schemaMetadata.autoEmbeddingFieldsMapping());
        derivedFields.put(matViewPath.toString(), replaceWithVectorField(fieldDef, vectorField));
      } else {
        derivedFields.put(entry.getKey(), fieldDef);
      }
    }
    try {
      return DocumentFieldDefinition.create(rawMappings.dynamic(), derivedFields);
    } catch (IllegalEmbeddedFieldException e) {
      throw new IllegalStateException("Failed to create derived mappings", e);
    }
  }

  private static FieldDefinition replaceWithVectorField(
      FieldDefinition original, SearchIndexVectorFieldDefinition vectorField) {
    return new FieldDefinition(
        original.autocompleteFieldDefinition(),
        original.booleanFieldDefinition(),
        original.dateFieldDefinition(),
        original.dateFacetFieldDefinition(),
        original.documentFieldDefinition(),
        original.embeddedDocumentsFieldDefinition(),
        original.geoFieldDefinition(),
        Optional.empty(),
        Optional.of(vectorField),
        original.knnVectorFieldDefinition(),
        original.numberFieldDefinition(),
        original.numberFacetFieldDefinition(),
        original.objectIdFieldDefinition(),
        original.sortableDateBetaV1FieldDefinition(),
        original.sortableNumberBetaV1FieldDefinition(),
        original.sortableStringBetaV1FieldDefinition(),
        original.stringFieldDefinition(),
        original.stringFacetFieldDefinition(),
        original.tokenFieldDefinition(),
        original.uuidFieldDefinition());
  }

  public static VectorFieldSpecification toVectorFieldSpecification(
      VectorAutoEmbedFieldSpecification spec) {
    return new VectorFieldSpecification(
        spec.numDimensions(),
        spec.similarity(),
        spec.autoEmbedQuantization().toLuceneQuantization(),
        spec.indexingAlgorithm());
  }
}
