package com.xgen.mongot.config.provider.community.embedding;

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.HierarchicalFieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.InvalidIndexDefinitionException;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validator for auto-embedding index definitions, shared by both deployment surfaces.
 *
 * <p>Community/on-prem index management commands run these checks at create and update time. The
 * Atlas conf-call intake runs them on every definition of every conf call, so a rule added here
 * also applies to definitions that are already live on Atlas: a new rule that could reject a live
 * definition needs a rollout plan before it ships.
 *
 * <p>These validations are ONLY applied to auto-embedding indexes (indexes that contain TEXT or
 * AUTO_EMBED field types).
 */
public class AutoEmbeddingIndexValidator {
  private static final String RESERVED_AUTO_EMBED_NAMESPACE = "_autoEmbed";

  /**
   * Runs validations for auto-embedding indexes. Works for both vector and search definitions.
   *
   * @throws InvalidIndexDefinitionException if the index violates restrictions
   * @throws IllegalArgumentException if called on a non-auto-embedding index
   */
  public static void validate(IndexDefinition indexDefinition)
      throws InvalidIndexDefinitionException {
    validateEmbeddingModelsAreRegistered(indexDefinition);
    validateDefinitionShape(indexDefinition);
  }

  /**
   * Runs the auto-embedding mapping-shape validations without requiring the referenced models to be
   * registered. The Atlas conf-call path uses this so a lagging or partially populated model
   * catalog cannot invalidate a live index; an unregistered model surfaces as MODEL_NOT_REGISTERED
   * during replication, which retries once the catalog catches up.
   *
   * @throws InvalidIndexDefinitionException if the index violates restrictions
   * @throws IllegalArgumentException if called on a non-auto-embedding index
   */
  public static void validateDefinitionShape(IndexDefinition indexDefinition)
      throws InvalidIndexDefinitionException {
    if (!indexDefinition.isAutoEmbeddingIndex()) {
      throw new IllegalArgumentException(
          "Unable to invoke validations on non auto-embedding indexes");
    }

    switch (indexDefinition) {
      case VectorIndexDefinition vectorIndex -> {
        validateVectorReservedAutoEmbedNamespace(vectorIndex);
        validateNoMixedVectorTypes(vectorIndex);
      }
      case SearchIndexDefinition searchIndex -> {
        validateSearchReservedAutoEmbedNamespace(searchIndex);
        validateNoConflictingAutoEmbedSourcePaths(searchIndex);
        validateSearchNoDynamicMappings(searchIndex);
        validateSearchNoStoredSource(searchIndex);
      }
    }
  }

  /**
   * Rejects user mappings in the root {@code _autoEmbed} namespace, which is reserved for generated
   * materialized-view fields.
   */
  private static void validateSearchReservedAutoEmbedNamespace(SearchIndexDefinition searchIndex)
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
  private static void validateVectorReservedAutoEmbedNamespace(VectorIndexDefinition vectorIndex)
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

  /**
   * Rejects auto-embed {@code path}s that conflict with a document-shaped mapping: {@code geo}
   * paths or document-shaped alternatives on the same field. A text-valued or scalar declaration on
   * the same field is allowed (dual role): the materialized view copies the raw value alongside the
   * generated vector.
   */
  private static void validateNoConflictingAutoEmbedSourcePaths(SearchIndexDefinition searchIndex)
      throws InvalidIndexDefinitionException {
    ImmutableSet<FieldPath> conflicts =
        SearchIndexDefinition.autoEmbedPathMappingConflicts(searchIndex.getMappings());
    if (!conflicts.isEmpty()) {
      throw new InvalidIndexDefinitionException(
          String.format(
              "autoEmbed path(s) %s conflict with a document-shaped mapping: document,"
                  + " embeddedDocuments, and geo fields cannot be autoEmbed sources",
              conflicts));
    }
  }

  /**
   * Rejects {@code dynamic} mappings at any level when the index contains an auto-embed field.
   *
   * <p>Dynamic mappings are supported for ordinary Search indexes, but an auto-embedding index is
   * served from a generated materialized view: a dynamic mapping would also index the generated
   * {@code _autoEmbed} subtree and the raw source values that the embedding pipeline consumes.
   * Mirrors the MMS/Atlas validation so community definitions are rejected at create/update time
   * instead of producing a serving definition that differs from what the user declared.
   */
  private static void validateSearchNoDynamicMappings(SearchIndexDefinition searchIndex)
      throws InvalidIndexDefinitionException {
    if (containsEnabledDynamicMappings(searchIndex.getMappings())) {
      throw new InvalidIndexDefinitionException(
          String.format(
              "Search index: %s cannot use dynamic mappings at any level when it contains an"
                  + " autoEmbed field. Set dynamic to false or omit it.",
              searchIndex.getName()));
    }
  }

  /**
   * True if {@code dynamic} is enabled on the given mappings node or on any nested {@code
   * document}/{@code embeddedDocuments} field definition.
   */
  private static boolean containsEnabledDynamicMappings(
      HierarchicalFieldDefinition hierarchicalField) {
    if (hierarchicalField.dynamic().isEnabled()) {
      return true;
    }
    for (FieldDefinition fieldDef : hierarchicalField.fields().values()) {
      if (fieldDef
              .documentFieldDefinition()
              .map(AutoEmbeddingIndexValidator::containsEnabledDynamicMappings)
              .orElse(false)
          || fieldDef
              .embeddedDocumentsFieldDefinition()
              .map(AutoEmbeddingIndexValidator::containsEnabledDynamicMappings)
              .orElse(false)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Rejects a configured {@code storedSource} when the index contains an auto-embed field. The
   * derived serving definition drops stored source (CLOUDP-459648), so a stored source that stores
   * fields would be silently ignored and {@code returnStoredSource} queries would fail at query
   * time. An absent {@code storedSource} and explicit {@code false} (exclude-all) stay allowed.
   */
  private static void validateSearchNoStoredSource(SearchIndexDefinition searchIndex)
      throws InvalidIndexDefinitionException {
    if (!searchIndex.getStoredSource().isAllExcluded()) {
      throw new InvalidIndexDefinitionException(
          String.format(
              "Search index: %s cannot use storedSource when it contains an autoEmbed field."
                  + " Stored source is not yet compatible with automated embedding; remove"
                  + " storedSource or the autoEmbed field.",
              searchIndex.getName()));
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
