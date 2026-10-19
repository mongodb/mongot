package com.xgen.mongot.config.provider.community.embedding;

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.InvalidIndexDefinitionException;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Validator for auto-embedding indexes on community.
 *
 * <p>These validations are ONLY applied to auto-embedding indexes (indexes that contain TEXT or
 * AUTO_EMBED field types).
 */
public class AutoEmbeddingIndexValidator {
  private static final String RESERVED_AUTO_EMBED_NAMESPACE = "_autoEmbed";

  /**
   * Runs validations for auto-embedding indexes on community. Works for both vector and search
   * index definitions.
   *
   * @throws InvalidIndexDefinitionException if the index violates restrictions
   * @throws IllegalArgumentException if called on a non-auto-embedding index
   */
  public static void validate(IndexDefinition indexDefinition)
      throws InvalidIndexDefinitionException {
    if (!indexDefinition.isAutoEmbeddingIndex()) {
      throw new IllegalArgumentException(
          "Unable to invoke validations on non auto-embedding indexes");
    }

    validateEmbeddingModelsAreRegistered(indexDefinition);
    switch (indexDefinition) {
      case VectorIndexDefinition vectorIndex -> {
        validateVectorReservedAutoEmbedNamespace(vectorIndex);
        validateNoMixedVectorTypes(vectorIndex);
      }
      case SearchIndexDefinition searchIndex -> {
        validateSearchReservedAutoEmbedNamespace(searchIndex);
        validateSearchNoMixedVectorTypes(searchIndex);
        validateSearchUniqueAutoEmbedPaths(searchIndex);
        validateNoConflictingAutoEmbedSourcePaths(searchIndex);
      }
    }
  }

  /**
   * Rejects user mappings in the root {@code _autoEmbed} namespace, which is reserved for generated
   * materialized-view fields.
   */
  public static void validateSearchReservedAutoEmbedNamespace(SearchIndexDefinition searchIndex)
      throws InvalidIndexDefinitionException {
    List<String> invalidPaths =
        searchIndex.getMappings().fields().keySet().stream()
            .filter(AutoEmbeddingIndexValidator::isReservedAutoEmbedPath)
            .sorted()
            .toList();
    if (!invalidPaths.isEmpty()) {
      throw new InvalidIndexDefinitionException(
          String.format(
              "Search auto-embedding mapping path(s) %s are invalid because \"%s\" is reserved for"
                  + " internal auto-embedding materialized-view fields",
              invalidPaths, RESERVED_AUTO_EMBED_NAMESPACE));
    }
  }

  /**
   * Rejects filter fields in the root {@code _autoEmbed} namespace, which is reserved for generated
   * materialized-view fields.
   */
  public static void validateVectorReservedAutoEmbedNamespace(VectorIndexDefinition vectorIndex)
      throws InvalidIndexDefinitionException {
    List<String> invalidPaths =
        vectorIndex.getFields().stream()
            .filter(field -> field.getType() == VectorIndexFieldDefinition.Type.FILTER)
            .map(field -> field.getPath().toString())
            .filter(AutoEmbeddingIndexValidator::isReservedAutoEmbedPath)
            .sorted()
            .toList();
    if (!invalidPaths.isEmpty()) {
      throw new InvalidIndexDefinitionException(
          String.format(
              "Vector auto-embedding filter path(s) %s are invalid because \"%s\" is reserved for"
                  + " internal auto-embedding materialized-view fields",
              invalidPaths, RESERVED_AUTO_EMBED_NAMESPACE));
    }
  }

  private static boolean isReservedAutoEmbedPath(String path) {
    return path.equals(RESERVED_AUTO_EMBED_NAMESPACE)
        || path.startsWith(RESERVED_AUTO_EMBED_NAMESPACE + FieldPath.DELIMITER);
  }

  /**
   * Validates that all embedding models referenced in the index are registered in the
   * EmbeddingModelCatalog.
   *
   * @param indexDefinition the index to validate
   * @throws InvalidIndexDefinitionException if any referenced model is not registered
   */
  private static void validateEmbeddingModelsAreRegistered(IndexDefinition indexDefinition)
      throws InvalidIndexDefinitionException {
    Set<String> modelNames = new HashSet<>(indexDefinition.getModelNamePerPath().values());
    List<String> unregisteredModels = new ArrayList<>();

    for (String modelName : modelNames) {
      if (!EmbeddingModelCatalog.isModelRegistered(modelName)) {
        unregisteredModels.add(modelName);
      }
    }

    if (!unregisteredModels.isEmpty()) {
      Set<String> registeredModels = EmbeddingModelCatalog.getAllSupportedModels();
      String supportedModelsList =
          registeredModels.isEmpty()
              ? "none (embedding service not configured)"
              : String.join(", ", registeredModels);
      throw new InvalidIndexDefinitionException(
          String.format(
              "The following embedding model(s) are not supported: %s. Supported models: %s.",
              String.join(", ", unregisteredModels), supportedModelsList));
    }
  }

  /**
   * Validates that an index update does not convert from auto-embedding to non-auto-embedding.
   * Works for both vector and search index definitions.
   */
  public static void validateNoAutoEmbeddingTypeConversion(
      VectorIndexDefinition oldIndex, VectorIndexDefinition newIndex)
      throws InvalidIndexDefinitionException {
    if (oldIndex.isAutoEmbeddingIndex() && !newIndex.isAutoEmbeddingIndex()) {
      throw new InvalidIndexDefinitionException(
          "For a vector search index definition, you cannot convert a "
              + "type:autoEmbed to a type:vector");
    }
  }

  /**
   * Collects auto-embed declarations at any {@code document} nesting depth, in field-hierarchy
   * order. A field's auto-embed and {@code document} type alternatives are processed independently,
   * mirroring the auto-embed mapping walk.
   */
  private static List<SearchAutoEmbedFieldDefinition> getAutoEmbeddingFieldDefinitions(
      SearchIndexDefinition newIndex) {
    List<SearchAutoEmbedFieldDefinition> autoEmbedFields = new ArrayList<>();
    collectAutoEmbedFields(newIndex.getMappings(), autoEmbedFields);
    return autoEmbedFields;
  }

  private static void collectAutoEmbedFields(
      DocumentFieldDefinition document, List<SearchAutoEmbedFieldDefinition> out) {
    for (FieldDefinition fieldDef : document.fields().values()) {
      fieldDef.searchAutoEmbedFieldDefinition().ifPresent(out::add);
      fieldDef.documentFieldDefinition().ifPresent(nested -> collectAutoEmbedFields(nested, out));
    }
  }

  private static void validateNoMixedVectorTypes(VectorIndexDefinition vectorIndex)
      throws InvalidIndexDefinitionException {
    @Var boolean hasRegularVectorField = false;
    @Var boolean hasAutoEmbedField = false;

    for (VectorIndexFieldDefinition field : vectorIndex.getFields()) {
      VectorIndexFieldDefinition.Type type = field.getType();

      if (type == VectorIndexFieldDefinition.Type.TEXT
          || type == VectorIndexFieldDefinition.Type.AUTO_EMBED) {
        hasAutoEmbedField = true;
      }

      if (type == VectorIndexFieldDefinition.Type.VECTOR) {
        hasRegularVectorField = true;
      }
    }

    throwIfMixedVectorTypes(hasRegularVectorField, hasAutoEmbedField);
  }

  private static void validateSearchNoMixedVectorTypes(SearchIndexDefinition searchIndex)
      throws InvalidIndexDefinitionException {
    throwIfMixedVectorTypes(
        containsRegularVectorField(searchIndex.getMappings()),
        searchIndex.getMappings().containsSearchAutoEmbedField());
  }

  /**
   * True if a {@code vector} or {@code knnVector} field is declared at any nesting depth,
   * descending through both {@code document} and {@code embeddedDocuments} fields (the latter is
   * the canonical shape for nested pre-computed vectors).
   */
  private static boolean containsRegularVectorField(DocumentFieldDefinition document) {
    return containsRegularVectorField(document.fields().values());
  }

  private static boolean containsRegularVectorField(Iterable<FieldDefinition> fields) {
    for (FieldDefinition fieldDef : fields) {
      if (fieldDef.searchIndexVectorFieldDefinition().isPresent()
          || fieldDef.knnVectorFieldDefinition().isPresent()) {
        return true;
      }
      if (fieldDef
          .documentFieldDefinition()
          .map(nested -> containsRegularVectorField(nested.fields().values()))
          .orElse(false)) {
        return true;
      }
      if (fieldDef
          .embeddedDocumentsFieldDefinition()
          .map(embedded -> containsRegularVectorField(embedded.fields().values()))
          .orElse(false)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Rejects duplicate auto-embed {@code path} declarations with a clean validation error. The
   * definition itself constructs fine (model resolution keeps the last declaration), so this
   * community-path check is the user-facing rejection for definitions built directly rather than
   * parsed from BSON.
   */
  private static void validateSearchUniqueAutoEmbedPaths(SearchIndexDefinition searchIndex)
      throws InvalidIndexDefinitionException {
    Set<FieldPath> seen = new HashSet<>();
    Set<FieldPath> duplicates = new LinkedHashSet<>();
    for (SearchAutoEmbedFieldDefinition autoEmbed : getAutoEmbeddingFieldDefinitions(searchIndex)) {
      if (!seen.add(autoEmbed.path())) {
        duplicates.add(autoEmbed.path());
      }
    }
    if (!duplicates.isEmpty()) {
      throw new InvalidIndexDefinitionException(
          String.format("Multiple autoEmbed fields declare the same path: %s", duplicates));
    }
  }

  /**
   * Rejects auto-embed {@code path}s that conflict with a document-shaped mapping: {@code geo}
   * paths, document-shaped alternatives on the same field, or an autoEmbed declared under {@code
   * embeddedDocuments}. A text-valued or scalar declaration on the same field is allowed (dual
   * role): the materialized view copies the raw value alongside the generated vector.
   */
  private static void validateNoConflictingAutoEmbedSourcePaths(SearchIndexDefinition searchIndex)
      throws InvalidIndexDefinitionException {
    ImmutableSet<FieldPath> conflicts =
        SearchIndexDefinition.autoEmbedPathMappingConflicts(searchIndex.getMappings());
    if (!conflicts.isEmpty()) {
      throw new InvalidIndexDefinitionException(
          String.format(
              "autoEmbed path(s) %s conflict with a document-shaped mapping: document,"
                  + " embeddedDocuments, and geo fields cannot be autoEmbed sources, and autoEmbed"
                  + " is not supported under embeddedDocuments",
              conflicts));
    }
  }

  private static void throwIfMixedVectorTypes(
      boolean hasRegularVectorField, boolean hasAutoEmbedField)
      throws InvalidIndexDefinitionException {
    if (hasRegularVectorField && hasAutoEmbedField) {
      throw new InvalidIndexDefinitionException(
          "Index cannot mix regular vector fields with auto-embedding fields. "
              + "Please use either pre-computed embeddings or auto-embedding, "
              + "but not both in the same index.");
    }
  }
}
