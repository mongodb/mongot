package com.xgen.mongot.config.provider.community.embedding;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.InvalidIndexDefinitionException;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Validator for auto-embedding vector indexes on community.
 *
 * <p>These validations are ONLY applied to auto-embedding indexes (indexes that contain TEXT or
 * AUTO_EMBED field types).
 */
public class AutoEmbeddingIndexValidator {

  /**
   * Runs validations for auto-embedding indexes on community. Works for both vector and search
   * index definitions.
   *
   * <p>Pass the raw definition BSON when available to reject explicit hnswOptions on community
   * vector indexes; pass {@link Optional#empty()} to skip the BSON check.
   *
   * @throws InvalidIndexDefinitionException if the index violates restrictions
   * @throws IllegalArgumentException if called on a non-auto-embedding index
   */
  public static void validate(
      IndexDefinition indexDefinition, Optional<BsonDocument> definitionBson)
      throws InvalidIndexDefinitionException {
    if (!indexDefinition.isAutoEmbeddingIndex()) {
      throw new IllegalArgumentException(
          "Unable to invoke validations on non auto-embedding indexes");
    }

    validateEmbeddingModelsAreRegistered(indexDefinition);
    switch (indexDefinition) {
      case VectorIndexDefinition vectorIndex -> {
        validateNoMixedVectorTypes(vectorIndex);
        if (definitionBson.isPresent()) {
          validateNoHnswOptionsInAutoEmbedFields(definitionBson.get());
        }
      }
      case SearchIndexDefinition searchIndex -> validateSearchNoMixedVectorTypes(searchIndex);
    }
  }

  /**
   * Validates that the raw definition BSON does not contain hnswOptions in any autoEmbed field.
   * Call this before parsing so that any explicit hnswOptions (including default values) is
   * rejected on community. We use a BSON walk here instead of parser-level rejection to avoid
   * touching the shared parser or introducing parse-context flags; once community allows
   * hnswOptions, this method will be removed.
   *
   * @param definitionBson the raw index definition document (e.g. has "fields" array)
   * @throws InvalidIndexDefinitionException if any autoEmbed field contains hnswOptions
   */
  @VisibleForTesting
  public static void validateNoHnswOptionsInAutoEmbedFields(BsonDocument definitionBson)
      throws InvalidIndexDefinitionException {
    BsonValue fieldsValue = definitionBson.get("fields");
    if (fieldsValue == null || !fieldsValue.isArray()) {
      return;
    }
    BsonArray fields = fieldsValue.asArray();
    for (BsonValue v : fields) {
      if (!v.isDocument()) {
        continue;
      }
      BsonDocument field = v.asDocument();
      if (!field.containsKey("type") || !field.get("type").isString()) {
        continue;
      }
      if ("autoEmbed".equalsIgnoreCase(field.getString("type").getValue())
          && field.containsKey("hnswOptions")) {
        throw new InvalidIndexDefinitionException(
            "hnswOptions is not supported for auto-embedding indexes on community. "
                + "Omit hnswOptions to use default HNSW settings.");
      }
    }
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

  public static void validateNoAutoEmbeddingFieldChanges(
      VectorIndexDefinition oldIndex, VectorIndexDefinition newIndex)
      throws InvalidIndexDefinitionException {
    checkBothAutoEmbedding(oldIndex, newIndex);
    throwIfFieldsChanged(
        getAutoEmbeddingFieldDefinitions(oldIndex), getAutoEmbeddingFieldDefinitions(newIndex));
  }

  public static void validateNoAutoEmbeddingFieldChanges(
      SearchIndexDefinition oldIndex, SearchIndexDefinition newIndex)
      throws InvalidIndexDefinitionException {
    checkBothAutoEmbedding(oldIndex, newIndex);
    throwIfFieldsChanged(
        getAutoEmbeddingFieldDefinitions(oldIndex), getAutoEmbeddingFieldDefinitions(newIndex));
  }

  private static List<VectorIndexFieldDefinition> getAutoEmbeddingFieldDefinitions(
      VectorIndexDefinition newIndex) {
    return newIndex.getFields().stream()
        .filter(
            field ->
                field.getType() == VectorIndexFieldDefinition.Type.TEXT
                    || field.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED)
        .toList();
  }

  private static List<SearchAutoEmbedFieldDefinition> getAutoEmbeddingFieldDefinitions(
      SearchIndexDefinition newIndex) {
    return newIndex.getMappings().fields().values().stream()
        .map(FieldDefinition::searchAutoEmbedFieldDefinition)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .toList();
  }

  private static void checkBothAutoEmbedding(IndexDefinition oldIndex, IndexDefinition newIndex) {
    if (!oldIndex.isAutoEmbeddingIndex() || !newIndex.isAutoEmbeddingIndex()) {
      throw new IllegalArgumentException(
          "Unable to invoke auto-embedding field change validation on non auto-embedding indexes");
    }
  }

  private static void throwIfFieldsChanged(List<?> oldFields, List<?> newFields)
      throws InvalidIndexDefinitionException {
    if (!oldFields.equals(newFields)) {
      throw new InvalidIndexDefinitionException(
          "Updates to auto-embedding fields are not allowed. "
              + "To modify auto-embedding fields, please drop and recreate the index.");
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
    @Var boolean hasRegularVectorField = false;
    @Var boolean hasAutoEmbedField = false;

    for (Map.Entry<String, FieldDefinition> entry : searchIndex.getMappings().fields().entrySet()) {
      FieldDefinition fieldDef = entry.getValue();
      if (fieldDef.searchAutoEmbedFieldDefinition().isPresent()) {
        hasAutoEmbedField = true;
      }
      if (fieldDef.searchIndexVectorFieldDefinition().isPresent()
          || fieldDef.knnVectorFieldDefinition().isPresent()) {
        hasRegularVectorField = true;
      }
    }

    throwIfMixedVectorTypes(hasRegularVectorField, hasAutoEmbedField);
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
