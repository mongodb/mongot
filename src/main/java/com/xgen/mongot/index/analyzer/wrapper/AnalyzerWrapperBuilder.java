package com.xgen.mongot.index.analyzer.wrapper;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.definition.DynamicDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.definition.TokenFieldDefinition;
import com.xgen.mongot.index.definition.TypeSetDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;

/**
 * This class is a template for creating per field analyzers from a given {@link
 * SearchIndexDefinition}.
 */
abstract class AnalyzerWrapperBuilder<T> {

  abstract Optional<T> getFieldNormalizer(
      AnalyzerRegistry analyzerRegistry, TokenFieldDefinition field);

  /**
   * Returns the name of the {@link Analyzer} that should be used for an indexed string field. This
   * should be a valid key in the {@link AnalyzerRegistry}
   */
  abstract Optional<T> getFieldAnalyzer(AnalyzerRegistry registry, StringFieldDefinition field);

  /**
   * Returns the name of the {@link Analyzer} that should be used for an autocomplete field. This
   * should be a valid key in the {@link AnalyzerRegistry}
   */
  abstract T getAutoCompleteAnalyzer(AnalyzerRegistry registry, AutocompleteFieldDefinition field);

  private void addAnalyzersForMappings(
      Map<FieldPath, FieldDefinition> fields,
      Optional<FieldPath> embeddedRoot,
      AnalyzerRegistry registry,
      ImmutableMap.Builder<String, T> analyzerMap,
      T defaultAnalyzer,
      T defaultNormalizer) {
    fields.forEach(
        (path, fieldDefinition) ->
            addAnalyzersForFieldDefinition(
                fieldDefinition,
                path,
                embeddedRoot,
                registry,
                analyzerMap,
                defaultAnalyzer,
                defaultNormalizer));
  }

  private void addAnalyzersForFieldDefinition(
      FieldDefinition field,
      FieldPath path,
      Optional<FieldPath> embeddedRoot,
      AnalyzerRegistry registry,
      ImmutableMap.Builder<String, T> analyzerMap,
      T defaultAnalyzer,
      T defaultNormalizer) {

    field
        .tokenFieldDefinition()
        .ifPresent(
            tokenField ->
                analyzerMap.put(
                    FieldName.TypeField.TOKEN.getLuceneFieldName(path, embeddedRoot),
                    this.getFieldNormalizer(registry, tokenField).orElse(defaultNormalizer)));

    field
        .stringFieldDefinition()
        .ifPresent(
            stringField ->
                analyzerMap.put(
                    FieldName.TypeField.STRING.getLuceneFieldName(path, embeddedRoot),
                    this.getFieldAnalyzer(registry, stringField).orElse(defaultAnalyzer)));

    field
        .stringFieldDefinition()
        .map(StringFieldDefinition::multi)
        .map(ImmutableMap::entrySet)
        .stream()
        .flatMap(Collection::stream)
        .forEach(
            multiField ->
                analyzerMap.put(
                    FieldName.MultiField.getLuceneFieldName(
                        path, multiField.getKey(), embeddedRoot),
                    this.getFieldAnalyzer(registry, multiField.getValue())
                        .orElse(defaultAnalyzer)));

    field
        .autocompleteFieldDefinition()
        .ifPresent(
            autocompleteField ->
                analyzerMap.put(
                    FieldName.TypeField.AUTOCOMPLETE.getLuceneFieldName(path, embeddedRoot),
                    getAutoCompleteAnalyzer(registry, autocompleteField)));
  }

  ImmutableMap<String, T> buildStaticMappings(
      SearchIndexDefinition indexDefinition,
      AnalyzerRegistry registry,
      T defaultAnalyzer,
      T defaultNormalizer) {
    ImmutableMap.Builder<String, T> analyzers = ImmutableMap.builder();

    // Add analyzers for all fields that are not children of an embeddedDocuments field definition.
    addAnalyzersForMappings(
        indexDefinition.getFieldHierarchyContext().getRootFields(),
        Optional.empty(),
        registry,
        analyzers,
        defaultAnalyzer,
        defaultNormalizer);

    // Add analyzers for all fields that are children of an embeddedDocuments field definition.
    indexDefinition
        .getFieldHierarchyContext()
        .getFieldsByEmbeddedRoot()
        .forEach(
            (embeddedRoot, embeddedFields) ->
                addAnalyzersForMappings(
                    embeddedFields,
                    Optional.of(embeddedRoot),
                    registry,
                    analyzers,
                    defaultAnalyzer,
                    defaultNormalizer));

    // NOTE: build() would throw an exception with duplicate keys. This shouldn't ever be the case,
    // but if a customer managed to upload an invalid index, throwing would cause a crash loop.
    return analyzers.buildKeepingLast();
  }

  Optional<DynamicTypeSetPrefixMap<T>> buildDynamicMappings(
      SearchIndexDefinition indexDefinition,
      AnalyzerRegistry registry,
      T defaultAnalyzer,
      T defaultNormalizer) {

    if (indexDefinition.getTypeSets().isEmpty()) {
      return Optional.empty();
    }

    ImmutableMap.Builder<String, T> dynamicAnalyzers = ImmutableMap.builder();

    addDynamicDefinitionToPrefixMap(
        FieldPath.parse(""),
        indexDefinition.getMappings().dynamic(),
        indexDefinition.getTypeSetsMap(),
        Optional.empty(),
        registry,
        dynamicAnalyzers,
        defaultAnalyzer,
        defaultNormalizer);

    indexDefinition
        .getFieldHierarchyContext()
        .getRootFields()
        .forEach(
            (path, fieldDefinition) -> {
              fieldDefinition
                  .documentFieldDefinition()
                  .ifPresent(
                      documentFieldDefinition ->
                          addDynamicDefinitionToPrefixMap(
                              path,
                              documentFieldDefinition.dynamic(),
                              indexDefinition.getTypeSetsMap(),
                              Optional.empty(),
                              registry,
                              dynamicAnalyzers,
                              defaultAnalyzer,
                              defaultNormalizer));

              fieldDefinition
                  .embeddedDocumentsFieldDefinition()
                  .ifPresent(
                      embeddedDocumentsFieldDefinition ->
                          addDynamicDefinitionToPrefixMap(
                              path,
                              embeddedDocumentsFieldDefinition.dynamic(),
                              indexDefinition.getTypeSetsMap(),
                              Optional.of(path),
                              registry,
                              dynamicAnalyzers,
                              defaultAnalyzer,
                              defaultNormalizer));
            });

    // Add analyzers for all fields that are children of an embeddedDocuments field definition.
    indexDefinition
        .getFieldHierarchyContext()
        .getFieldsByEmbeddedRoot()
        .forEach(
            (embeddedRoot, embeddedFields) ->
                embeddedFields.forEach(
                    (path, fieldDefinition) -> {
                      fieldDefinition
                          .documentFieldDefinition()
                          .ifPresent(
                              documentFieldDefinition ->
                                  addDynamicDefinitionToPrefixMap(
                                      path,
                                      documentFieldDefinition.dynamic(),
                                      indexDefinition.getTypeSetsMap(),
                                      Optional.of(embeddedRoot),
                                      registry,
                                      dynamicAnalyzers,
                                      defaultAnalyzer,
                                      defaultNormalizer));

                      fieldDefinition
                          .embeddedDocumentsFieldDefinition()
                          .ifPresent(
                              embeddedDocumentsFieldDefinition ->
                                  addDynamicDefinitionToPrefixMap(
                                      path,
                                      embeddedDocumentsFieldDefinition.dynamic(),
                                      indexDefinition.getTypeSetsMap(),
                                      Optional.of(path),
                                      registry,
                                      dynamicAnalyzers,
                                      defaultAnalyzer,
                                      defaultNormalizer));
                    }));

    ImmutableMap<String, T> prefixToAnalyzer = dynamicAnalyzers.buildKeepingLast();
    return prefixToAnalyzer.isEmpty()
        ? Optional.empty()
        : Optional.of(new DynamicTypeSetPrefixMap<>(prefixToAnalyzer));
  }

  private void addDynamicDefinitionToPrefixMap(
      FieldPath path,
      DynamicDefinition dynamic,
      Map<String, TypeSetDefinition> typeSetsMap,
      Optional<FieldPath> embeddedRoot,
      AnalyzerRegistry registry,
      ImmutableMap.Builder<String, T> dynamicAnalyzers,
      T defaultAnalyzer,
      T defaultNormalizer) {

    switch (dynamic) {
      case DynamicDefinition.Document docDefinition ->
          addAnalyzersForFieldDefinition(
              typeSetsMap.get(docDefinition.typeSet()).getFieldDefinition(),
              path,
              embeddedRoot,
              registry,
              dynamicAnalyzers,
              defaultAnalyzer,
              defaultNormalizer);
      case DynamicDefinition.Boolean ignored -> {}
    }
  }
}
