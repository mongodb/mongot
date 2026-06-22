package com.xgen.mongot.index.lucene.similarity;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SimilarityDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Constructs a mapping from Lucene field names to {@link Similarity} instances, based on similarity
 * definitions specified in a {@link SearchIndexDefinition}.
 *
 * <p>This builder recursively processes all fields in the index mapping, including:
 *
 * <ul>
 *   <li>Top-level string and autocomplete fields
 *   <li>Multi-field variants of string fields
 *   <li>Fields nested under embedded documents
 * </ul>
 *
 * <p>Each field that declares a {@link SimilarityDefinition} is associated with a corresponding
 * Lucene {@code Similarity} instance such as {@link BM25Similarity}, {@link BooleanSimilarity}, or
 * {@link StableTflSimilarity}.
 *
 * <p>If multiple fields map to the same Lucene field name (which is not expected in valid
 * mappings), the last entry will take precedence to avoid build failure.
 */
public class SimilarityWrapperBuilder {

  private static final BooleanSimilarity BOOLEAN_SIMILARITY = new BooleanSimilarity();

  private void addSimilarityToMap(
      Optional<SimilarityDefinition> similarityDefinition,
      String luceneFieldName,
      ImmutableMap.Builder<String, Similarity> similarityMap) {
    if (similarityDefinition.isEmpty()) {
      return;
    }

    SimilarityDefinition definition = similarityDefinition.get();
    switch (definition.type()) {
      case BM25 -> similarityMap.put(luceneFieldName, LuceneSimilarity.DEFAULT_BM25_SIMILARITY);
      case BOOLEAN -> similarityMap.put(luceneFieldName, BOOLEAN_SIMILARITY);
      case STABLE_TFL -> similarityMap.put(luceneFieldName, StableTflSimilarity.getInstance());
    }
  }

  private void addSimilarityForFieldDefinition(
      FieldDefinition field,
      FieldPath path,
      Optional<FieldPath> embeddedRoot,
      ImmutableMap.Builder<String, Similarity> similarityMap) {
    field
        .stringFieldDefinition()
        .ifPresent(
            stringField ->
                addSimilarityToMap(
                    stringField.similarity(),
                    FieldName.TypeField.STRING.getLuceneFieldName(path, embeddedRoot),
                    similarityMap));

    field
        .stringFieldDefinition()
        .map(StringFieldDefinition::multi)
        .map(ImmutableMap::entrySet)
        .stream()
        .flatMap(Collection::stream)
        .forEach(
            multiField ->
                addSimilarityToMap(
                    multiField.getValue().similarity(),
                    FieldName.MultiField.getLuceneFieldName(
                        path, multiField.getKey(), embeddedRoot),
                    similarityMap));

    field
        .autocompleteFieldDefinition()
        .ifPresent(
            autocompleteField ->
                addSimilarityToMap(
                    autocompleteField.getSimilarity(),
                    FieldName.TypeField.AUTOCOMPLETE.getLuceneFieldName(path, embeddedRoot),
                    similarityMap));
  }

  private void addSimilarityForMappings(
      Map<FieldPath, FieldDefinition> fields,
      Optional<FieldPath> embeddedRoot,
      ImmutableMap.Builder<String, Similarity> similarityMap) {
    fields.forEach(
        (path, fieldDefinition) ->
            addSimilarityForFieldDefinition(fieldDefinition, path, embeddedRoot, similarityMap));
  }

  ImmutableMap<String, Similarity> build(SearchIndexDefinition indexDefinition) {
    ImmutableMap.Builder<String, Similarity> similarities = ImmutableMap.builder();

    // Add similarities for all fields that are not children of an embeddedDocuments field
    // definition.
    addSimilarityForMappings(
        indexDefinition.getFieldHierarchyContext().getRootFields(), Optional.empty(), similarities);

    // Add similarities for all fields that are children of an embeddedDocuments field definition.
    indexDefinition
        .getFieldHierarchyContext()
        .getFieldsByEmbeddedRoot()
        .forEach(
            (embeddedRoot, embeddedFields) ->
                addSimilarityForMappings(embeddedFields, Optional.of(embeddedRoot), similarities));

    // NOTE: build() would throw an exception with duplicate keys. This shouldn't ever be the case,
    // but if a customer managed to upload an invalid index, throwing would cause a crash loop.
    return similarities.buildKeepingLast();
  }
}
