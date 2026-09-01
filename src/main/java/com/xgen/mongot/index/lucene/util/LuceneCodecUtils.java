package com.xgen.mongot.index.lucene.util;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.HierarchicalFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexVectorFieldDefinition;
import com.xgen.mongot.util.FieldPath;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Utility class for extracting vector field specifications for LuceneCodec construction. */
public final class LuceneCodecUtils {

  private LuceneCodecUtils() {
    // Do not instantiate
  }

  /**
   * Extracts vector field specifications from a list of vector index field definitions.
   *
   * @param fields list of vector index field definitions
   * @return map from field path to vector field specification
   */
  public static Map<FieldPath, VectorFieldSpecification> extractVectorFieldsFromVectorIndex(
      List<VectorIndexFieldDefinition> fields) {
    List<VectorIndexVectorFieldDefinition> vectorFields =
        fields.stream()
            .filter(VectorIndexFieldDefinition::isVectorField)
            .map(VectorIndexFieldDefinition::asVectorField)
            .toList();

    Map<FieldPath, VectorFieldSpecification> pathToField = new HashMap<>();
    for (VectorIndexVectorFieldDefinition vectorField : vectorFields) {
      pathToField.put(vectorField.getPath(), vectorField.specification());
    }
    return pathToField;
  }

  /**
   * Extracts vector field specifications from search index field mappings.
   *
   * <p>Search vectors may be nested under either {@code document} or {@code embeddedDocuments}
   * mappings. The returned paths match the full dotted paths used in Lucene field names.
   *
   * @param mappings root search index mappings
   * @return map from field path to vector field specification
   */
  public static Map<FieldPath, VectorFieldSpecification> extractVectorFieldsFromSearchMappings(
      DocumentFieldDefinition mappings) {
    Map<FieldPath, VectorFieldSpecification> pathToField = new HashMap<>();
    collectVectorFields(mappings, Optional.empty(), pathToField);
    return pathToField;
  }

  private static void collectVectorFields(
      HierarchicalFieldDefinition mappings,
      Optional<FieldPath> parentPath,
      Map<FieldPath, VectorFieldSpecification> pathToField) {
    for (Map.Entry<String, FieldDefinition> entry : mappings.fields().entrySet()) {
      FieldPath path =
          parentPath
              .map(parent -> parent.newChild(entry.getKey()))
              .orElseGet(() -> FieldPath.parse(entry.getKey()));
      FieldDefinition fieldDefinition = entry.getValue();
      fieldDefinition
          .vectorFieldSpecification()
          .ifPresent(specification -> pathToField.put(path, specification));
      fieldDefinition
          .documentFieldDefinition()
          .ifPresent(document -> collectVectorFields(document, Optional.of(path), pathToField));
      fieldDefinition
          .embeddedDocumentsFieldDefinition()
          .ifPresent(embedded -> collectVectorFields(embedded, Optional.of(path), pathToField));
    }
  }
}
