package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import static com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator.getMatViewFieldPath;
import static com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator.getMatViewNestedRoot;

import com.google.errorprone.annotations.Var;
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
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
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
        rawDefinition.getIndexNamespace(),
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
              if (field.getType() == VectorIndexFieldDefinition.Type.FILTER) {
                // CLOUDP-402522: filter fields under the source nestedRoot are remapped into the
                // MV namespace via schemaMetadata.autoEmbeddingFieldsMapping. The remapping is a
                // no-op for top-level filters (no entry in the mapping).
                FieldPath matViewPath =
                    getMatViewFieldPath(
                        field.getPath(), schemaMetadata.autoEmbeddingFieldsMapping());
                if (!matViewPath.equals(field.getPath())) {
                  return new VectorIndexFilterFieldDefinition(matViewPath);
                }
              }
              return field;
            })
        .toList();
  }

  private static DocumentFieldDefinition getDerivedSearchMappings(
      DocumentFieldDefinition rawMappings, MaterializedViewSchemaMetadata schemaMetadata) {
    Map<String, FieldDefinition> derivedFields = new LinkedHashMap<>();
    // Auto-embed fields (at any nesting depth) become top-level vector fields keyed by their flat
    // materialized-view path (e.g. "_autoEmbed.content"); collected here and merged in at the end.
    Map<String, FieldDefinition> hoistedVectorFields = new LinkedHashMap<>();
    collectDerivedSearchFields(
        rawMappings.fields(),
        schemaMetadata,
        rawMappings.dynamic().isEnabled(),
        derivedFields,
        hoistedVectorFields);
    derivedFields.putAll(hoistedVectorFields);
    try {
      return DocumentFieldDefinition.create(rawMappings.dynamic(), derivedFields);
    } catch (IllegalEmbeddedFieldException e) {
      throw new IllegalStateException("Failed to create derived mappings", e);
    }
  }

  /**
   * Rebuilds {@code fields} into {@code out}, hoisting each auto-embed declaration to a pure {@code
   * vector} field at its flat materialized-view path in {@code hoistedVectorFields}. A field's type
   * alternatives are processed independently (search mappings may declare several types per field):
   * the auto-embed alternative is stripped from the rebuilt field, a {@code document} alternative
   * containing auto-embed descendants is rebuilt recursively, and every other alternative is
   * preserved at the field's original path. A rebuilt document that becomes empty is dropped —
   * unless dynamic mapping governs it: a dynamic document must survive to dynamically index its
   * wholesale-copied MV subtree, and a static document under a dynamic mapping must survive to keep
   * suppressing dynamic indexing of its subtree. A field left with no type definitions is dropped.
   */
  private static void collectDerivedSearchFields(
      Map<String, FieldDefinition> fields,
      MaterializedViewSchemaMetadata schemaMetadata,
      boolean underDynamicMapping,
      Map<String, FieldDefinition> out,
      Map<String, FieldDefinition> hoistedVectorFields) {
    for (Map.Entry<String, FieldDefinition> entry : fields.entrySet()) {
      FieldDefinition fieldDef = entry.getValue();
      var autoEmbed = fieldDef.searchAutoEmbedFieldDefinition();
      var document = fieldDef.documentFieldDefinition();
      if (autoEmbed.isPresent()) {
        SearchAutoEmbedFieldDefinition autoEmbedDef = autoEmbed.get();
        // The derived vector field lives at the flat mat-view path regardless of nesting depth.
        // It carries only the vector definition: the field's other type alternatives describe the
        // declaration path, not the mat-view path holding the vector.
        FieldPath matViewPath =
            getMatViewFieldPath(
                autoEmbedDef.sourceField(), schemaMetadata.autoEmbeddingFieldsMapping());
        hoistedVectorFields.put(
            matViewPath.toString(),
            vectorOnlyField(
                new SearchIndexVectorFieldDefinition(
                    toVectorFieldSpecification(autoEmbedDef.specification()))));
      }

      @Var Optional<DocumentFieldDefinition> derivedDocument = document;
      if (document.isPresent() && document.get().containsSearchAutoEmbedField()) {
        DocumentFieldDefinition documentDef = document.get();
        Map<String, FieldDefinition> childFields = new LinkedHashMap<>();
        collectDerivedSearchFields(
            documentDef.fields(),
            schemaMetadata,
            underDynamicMapping || documentDef.dynamic().isEnabled(),
            childFields,
            hoistedVectorFields);
        derivedDocument =
            !childFields.isEmpty() || documentDef.dynamic().isEnabled() || underDynamicMapping
                ? Optional.of(createDerivedDocument(documentDef, childFields))
                : Optional.empty();
      }

      FieldDefinition derivedField = withoutAutoEmbedField(fieldDef, derivedDocument);
      if (derivedField.hasScalarFieldDefinitions()
          || derivedField.documentFieldDefinition().isPresent()) {
        out.put(entry.getKey(), derivedField);
      }
    }
  }

  private static DocumentFieldDefinition createDerivedDocument(
      DocumentFieldDefinition original, Map<String, FieldDefinition> fields) {
    try {
      return DocumentFieldDefinition.create(original.dynamic(), fields);
    } catch (IllegalEmbeddedFieldException e) {
      throw new IllegalStateException("Failed to create derived nested mappings", e);
    }
  }

  /**
   * Copies {@code original} with the auto-embed alternative removed and the {@code document}
   * alternative replaced by {@code newDocument}; every other type alternative is preserved.
   */
  private static FieldDefinition withoutAutoEmbedField(
      FieldDefinition original, Optional<DocumentFieldDefinition> newDocument) {
    return new FieldDefinition(
        original.autocompleteFieldDefinition(),
        original.booleanFieldDefinition(),
        original.dateFieldDefinition(),
        original.dateFacetFieldDefinition(),
        newDocument,
        original.embeddedDocumentsFieldDefinition(),
        original.geoFieldDefinition(),
        Optional.empty(),
        original.searchIndexVectorFieldDefinition(),
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

  private static FieldDefinition vectorOnlyField(SearchIndexVectorFieldDefinition vectorField) {
    return new FieldDefinition(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(vectorField),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
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
