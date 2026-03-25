package com.xgen.mongot.config.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.index.analyzer.TokenStreamTypeProvider;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.InvalidIndexDefinitionException;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.definition.TypeSetDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.LoggableException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.ListUtils;
import org.bson.types.ObjectId;

/**
 * Contains helper methods for checking invariants that must hold for indexes, analyzers and
 * blobstore.
 */
public class Invariants {

  public static class InvariantException extends LoggableException {
    InvariantException(String message) {
      super(message);
    }
  }

  public static void validateInvariants(
      List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions,
      List<SearchIndexDefinition> searchDefinitions,
      List<VectorIndexDefinition> vectorDefinitions,
      IndexCatalog indexCatalog)
      throws InvariantException {
    validateInvariants(
        analyzerDefinitions,
        searchDefinitions,
        vectorDefinitions,
        IndexDefinitions.allIndexDefinitions(indexCatalog.getIndexes()));
  }

  public static void validateInvariants(
      List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions,
      List<SearchIndexDefinition> searchDefinitions,
      List<VectorIndexDefinition> vectorDefinitions,
      List<IndexDefinition> allExistingIndexes)
      throws InvariantException {
    Invariants.validateInvariants(
        analyzerDefinitions,
        searchDefinitions,
        allExistingIndexes.stream()
            .filter(SearchIndexDefinition.class::isInstance)
            .map(SearchIndexDefinition.class::cast)
            .toList());
    Invariants.validateInvariants(
        vectorDefinitions,
        allExistingIndexes.stream()
            .filter(VectorIndexDefinition.class::isInstance)
            .map(VectorIndexDefinition.class::cast)
            .toList());
    Invariants.validateCrossTypeInvariants(
        searchDefinitions, vectorDefinitions, allExistingIndexes);
  }

  /** Validates that vector definitions do not violate any invariants. */
  static void validateInvariants(
      List<VectorIndexDefinition> vectorDefinitions,
      List<VectorIndexDefinition> existingVectorDefinitions)
      throws InvariantException {
    validateIndexInvariants(vectorDefinitions, existingVectorDefinitions);
  }

  /** Validates that the analyzer and index definitions do not violate any invariants. */
  static void validateInvariants(
      List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions,
      List<SearchIndexDefinition> searchDefinitions,
      List<SearchIndexDefinition> existingSearchDefinitions)
      throws InvariantException {
    validateAnalyzerInvariants(searchDefinitions, analyzerDefinitions);
    validateIndexInvariants(searchDefinitions, existingSearchDefinitions);
  }

  /** Validate that invariants hold for different index states. */
  public static void validateGenerationalInvariants(
      List<IndexDefinitionGeneration> stagedIndexes,
      List<IndexDefinitionGeneration> liveIndexes,
      List<IndexDefinitionGeneration> deletedIndexes)
      throws InvariantException {
    IndexInvariants.validateGenerationalInvariants(stagedIndexes, liveIndexes, deletedIndexes);
    AnalyzerInvariants.validate(CollectionUtils.concat(stagedIndexes, liveIndexes, deletedIndexes));
  }

  /**
   * Validates that all vector index definitions with a nestedRoot have at least one field path
   * under that root. If the nestedRoot does not match any field path, the index definition is
   * invalid.
   */
  public static void validateVectorNestedRootReferences(
      List<VectorIndexDefinition> vectorDefinitions) throws InvalidIndexDefinitionException {
    for (VectorIndexDefinition vectorDef : vectorDefinitions) {
      Optional<FieldPath> rawNestedRoot = vectorDef.getRawNestedRoot();
      if (rawNestedRoot.isPresent()) {
        var mappings = vectorDef.getMappings();
        if (!mappings.subDocumentExists(rawNestedRoot.get())
            && !mappings.childPathExists(rawNestedRoot.get())) {
          throw new InvalidIndexDefinitionException(
              "nestedRoot \""
                  + rawNestedRoot.get()
                  + "\" does not match any field path in the index definition");
        }
      }
    }
  }

  /** Validates that the analyzer definitions do not violate any invariants. */
  static void validateAnalyzerInvariants(
      List<SearchIndexDefinition> indexDefinitions,
      List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions)
      throws InvariantException {
    AnalyzerInvariants.validate(indexDefinitions, analyzerDefinitions);
  }

  /** Validates that the index definitions do not violate any invariants. */
  static void validateIndexInvariants(
      List<? extends IndexDefinition> indexDefinitions,
      List<? extends IndexDefinition> existingDefinitions)
      throws InvariantException {
    IndexInvariants.validate(indexDefinitions, existingDefinitions);
  }

  /**
   * Validates that none of the supplied indexes have changed their type compared to the existing
   * index catalog and that search and vector definitions do not have cross-type id and namespace
   * duplication.
   */
  static void validateCrossTypeInvariants(
      List<SearchIndexDefinition> searchDefinitions,
      List<VectorIndexDefinition> vectorDefinitions,
      List<IndexDefinition> allExistingDefinitions)
      throws InvariantException {
    IndexInvariants.validateCrossTypeInvariants(
        searchDefinitions, vectorDefinitions, allExistingDefinitions);
  }

  public static class AnalyzerInvariants {
    /** Validates all analyzer invariants. */
    public static void validate(
        List<SearchIndexDefinition> indexDefinitions,
        List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions)
        throws InvariantException {
      // Ensure that all the analyzers have unique names.
      for (SearchIndexDefinition indexDefinition : indexDefinitions) {
        validateUniqueNames(indexDefinition, analyzerDefinitions);
      }
    }

    /** Validate analyzer invariants for definitions already bound to overridden analyzers. */
    static void validate(ImmutableList<IndexDefinitionGeneration> definitionGenerations)
        throws InvariantException {
      // Ensure that all the analyzers have unique names.
      for (var definitionGeneration : definitionGenerations) {
        if (definitionGeneration instanceof SearchIndexDefinitionGeneration searchGeneration) {
          validateUniqueNames(
              searchGeneration.definition().analyzerDefinitions(),
              searchGeneration.getIndexDefinition().getAnalyzers());
        }
      }
    }

    /** Validates that each of the supplied AnalyzerDefinitions have different names. */
    private static void validateUniqueNames(
        SearchIndexDefinition indexDefinition,
        List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions)
        throws InvariantException {
      List<CustomAnalyzerDefinition> customAnalyzers = indexDefinition.getAnalyzers();
      validateUniqueNames(analyzerDefinitions, customAnalyzers);
    }

    private static void validateUniqueNames(
        List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions,
        List<CustomAnalyzerDefinition> customAnalyzers)
        throws InvariantException {
      List<AnalyzerDefinition> analyzers =
          Stream.concat(customAnalyzers.stream(), analyzerDefinitions.stream())
              .collect(Collectors.toList());
      Set<String> duplicateAnalyzerNames = getDuplicateNames(analyzers);

      if (!duplicateAnalyzerNames.isEmpty()) {
        throw new InvariantException(
            String.format(
                "multiple analyzers had duplicate names: %s",
                String.join(", ", duplicateAnalyzerNames)));
      }
    }

    /** Returns a set of the analyzer names that were defined multiple times. */
    private static Set<String> getDuplicateNames(List<AnalyzerDefinition> analyzerDefinitions) {
      return getDuplicateAnalyzers(analyzerDefinitions).stream()
          .map(AnalyzerDefinition::name)
          .collect(Collectors.toSet());
    }

    /** Returns a list of duplicate analyzers. */
    public static <A extends AnalyzerDefinition> List<A> getDuplicateAnalyzers(
        List<A> analyzerDefinitions) {
      Map<String, List<A>> analyzersByName =
          analyzerDefinitions.stream().collect(Collectors.groupingBy(A::name, Collectors.toList()));

      return analyzersByName.values().stream()
          .filter(a -> a.size() > 1)
          .flatMap(Collection::stream)
          .collect(Collectors.toList());
    }

    /**
     * Unresolved analyzer names are analyzer names that are referenced in an index definition that
     * mongot can't find a correctly defined analyzer for. These may be names of improperly defined
     * overridden analyzers, or may be undefined entirely.
     */
    public static Set<String> getUnresolvedAnalyzerNames(
        SearchIndexDefinition indexDefinition, Set<String> overriddenValidAnalyzerNames) {
      return Sets.difference(
          indexDefinition.getNonStockAnalyzerNames(),
          Sets.union(indexDefinition.getAnalyzerMap().keySet(), overriddenValidAnalyzerNames));
    }

    public static void throwIfCustomAndOverriddenAnalyzersShareNames(
        SearchIndexDefinition indexDefinition, Set<String> overriddenValidAnalyzerNames)
        throws InvalidIndexDefinitionException {
      Set<String> nonUniqueAnalyzerNames =
          Sets.intersection(
              indexDefinition.getAnalyzerMap().keySet(), overriddenValidAnalyzerNames);

      if (!nonUniqueAnalyzerNames.isEmpty()) {
        throw new InvalidIndexDefinitionException(
            String.format(
                "defines custom analyzers with same names as overridden analyzers: %s",
                String.join(", ", nonUniqueAnalyzerNames)));
      }
    }

    public static void validateFieldAnalyzerReferences(
        SearchIndexDefinition indexDefinition,
        Set<String> overriddenValidAnalyzerNames,
        Set<String> overriddenInvalidAnalyzerNames)
        throws InvalidIndexDefinitionException {
      // Unresolved analyzer names are analyzer names that are referenced in an index definition
      // that mongot can't find a correctly defined analyzer for. These may be names of improperly
      // defined overridden analyzers, or may be undefined entirely.
      Set<String> unresolvedAnalyzerNames =
          getUnresolvedAnalyzerNames(indexDefinition, overriddenValidAnalyzerNames);

      // If there are no unresolved analyzer names, all referenced analyzer names point to a valid
      // analyzer - we have successfully validated the analyzer references of fields in this index.
      if (unresolvedAnalyzerNames.isEmpty()) {
        return;
      }

      // unresolvedAnalyzerNames is not empty (e.g. there are some analyzers referenced in this
      // index definition which mongot can't resolve to a valid analyzer).
      throwIfUnresolvedAnalyzerIsUndefined(unresolvedAnalyzerNames, overriddenInvalidAnalyzerNames);
    }

    private static void throwIfUnresolvedAnalyzerIsUndefined(
        Set<String> overriddenUnresolvedAnalyzerNames, Set<String> overriddenNamedInvalidAnalyzers)
        throws InvalidIndexDefinitionException {
      Set<String> nonExistentAnalyzers =
          Sets.difference(overriddenUnresolvedAnalyzerNames, overriddenNamedInvalidAnalyzers);
      if (!nonExistentAnalyzers.isEmpty()) {
        throw InvalidIndexDefinitionException.referencesUndefinedAnalyzers(nonExistentAnalyzers);
      }
    }

    /**
     * Validate synonym and autocomplete analyzer references.
     *
     * @param indexDefinition Index definition.
     * @param validOverriddenAnalyzers Overridden analyzers are deprecated, pass an empty map if not
     *     present.
     * @param maxSynonymMappingsPerIndex To check if synonym mapping limit has been exceeded.
     */
    public static void validateSynonymAndAutocompleteAnalyzerReferences(
        SearchIndexDefinition indexDefinition,
        Map<String, OverriddenBaseAnalyzerDefinition> validOverriddenAnalyzers,
        Optional<Integer> maxSynonymMappingsPerIndex)
        throws InvalidIndexDefinitionException {
      var tokenStreamTypeProvider =
          new TokenStreamTypeProvider(validOverriddenAnalyzers, indexDefinition.getAnalyzerMap());
      validateSynonymAnalyzerReferences(
          indexDefinition, tokenStreamTypeProvider, maxSynonymMappingsPerIndex);
      validateAutocompleteAnalyzerReferences(indexDefinition, tokenStreamTypeProvider);
    }

    private static void validateSynonymAnalyzerReferences(
        SearchIndexDefinition indexDefinition,
        TokenStreamTypeProvider tokenStreamTypeProvider,
        Optional<Integer> maxSynonymMappingsPerIndex)
        throws InvalidIndexDefinitionException {
      if (indexDefinition.getSynonyms().isEmpty()) {
        return;
      }

      int numSynonymMappings = indexDefinition.getSynonyms().get().size();
      if (maxSynonymMappingsPerIndex.isPresent()
          && numSynonymMappings > maxSynonymMappingsPerIndex.get()) {
        throw new InvalidIndexDefinitionException(
            String.format(
                "Synonym mappings limit exceeded: %s > %s",
                numSynonymMappings, maxSynonymMappingsPerIndex.get()));
      }

      Predicate<SynonymMappingDefinition> usesGraphTokenStreamAnalyzer =
          def -> tokenStreamTypeProvider.isGraphOrThrow(def.analyzer());

      Optional<SynonymMappingDefinition> invalidSynonymDefinition =
          indexDefinition.getSynonyms().get().stream()
              .filter(usesGraphTokenStreamAnalyzer)
              .findFirst();

      if (invalidSynonymDefinition.isPresent()) {
        throw InvalidIndexDefinitionException.usesGraphTokenProducingAnalyzerWithSynonyms(
            invalidSynonymDefinition.get());
      }
    }

    private static void validateAutocompleteAnalyzerReferences(
        SearchIndexDefinition indexDefinition, TokenStreamTypeProvider tokenStreamTypeProvider)
        throws InvalidIndexDefinitionException {
      Predicate<Map.Entry<String, AutocompleteFieldDefinition>> usesGraphTokenStreamAnalyzer =
          entry -> tokenStreamTypeProvider.isGraphOrThrow(entry.getValue().getAnalyzer());

      Optional<Map.Entry<String, AutocompleteFieldDefinition>> invalidAutocompleteFieldEntry =
          indexDefinition
              .getStaticFieldDefinitionsOfType(FieldDefinition::autocompleteFieldDefinition)
              .stream()
              .filter(usesGraphTokenStreamAnalyzer)
              .findFirst();

      if (invalidAutocompleteFieldEntry.isPresent()) {
        var invalidAutocomplete = invalidAutocompleteFieldEntry.get();
        throw InvalidIndexDefinitionException.usesGraphTokenProducingAnalyzerWithAutocomplete(
            invalidAutocomplete.getKey(), invalidAutocomplete.getValue());
      }

      // look inside embeddedDocument fields
      ImmutableMap<FieldPath, ImmutableMap<FieldPath, FieldDefinition>> fieldsByEmbeddedRoot =
          indexDefinition.getFieldHierarchyContext().getFieldsByEmbeddedRoot();
      Set<Map.Entry<String, AutocompleteFieldDefinition>> autocompleteDefinitionsInEmbDocFieldsSet =
          fieldsByEmbeddedRoot.values().stream()
              .flatMap(entries -> entries.entrySet().stream())
              .filter(entry -> entry.getValue().autocompleteFieldDefinition().isPresent())
              .map(
                  entry ->
                      Map.entry(
                          entry.getKey().toString(),
                          entry.getValue().autocompleteFieldDefinition().get()))
              .collect(Collectors.toSet());

      Optional<Map.Entry<String, AutocompleteFieldDefinition>> invalidAutocompleteEmbFieldEntry =
          autocompleteDefinitionsInEmbDocFieldsSet.stream()
              .filter(usesGraphTokenStreamAnalyzer)
              .findFirst();

      if (invalidAutocompleteEmbFieldEntry.isPresent()) {
        var invalidAutocomplete = invalidAutocompleteEmbFieldEntry.get();
        throw InvalidIndexDefinitionException.usesGraphTokenProducingAnalyzerWithAutocomplete(
            invalidAutocomplete.getKey(), invalidAutocomplete.getValue());
      }

      // Look inside typeSets
      Set<TypeSetDefinition> typeSetsWithInvalidAutocomplete =
          indexDefinition.getTypeSets().stream()
              .flatMap(Collection::stream)
              .filter(
                  typeSetDefinition ->
                      typeSetContainsInvalidAutocompleteDefinition(
                          typeSetDefinition, tokenStreamTypeProvider))
              .collect(Collectors.toSet());
      if (!typeSetsWithInvalidAutocomplete.isEmpty()) {
        throw new InvalidIndexDefinitionException(
            String.format(
                "autocomplete field in the following "
                    + "typeSets use graph token stream producing analyzers: %s",
                typeSetsWithInvalidAutocomplete.stream()
                    .map(TypeSetDefinition::name)
                    .collect(Collectors.toSet())));
      }
    }

    private static boolean typeSetContainsInvalidAutocompleteDefinition(
        TypeSetDefinition typeSetDefinition, TokenStreamTypeProvider tokenStreamTypeProvider) {
      return typeSetDefinition.types().stream()
          .anyMatch(
              fieldTypeDefinition ->
                  fieldTypeDefinition
                          instanceof AutocompleteFieldDefinition autocompleteFieldDefinition
                      && tokenStreamTypeProvider.isGraphOrThrow(
                          autocompleteFieldDefinition.getAnalyzer()));
    }
  }

  public static class IndexInvariants {
    /**
     * Validate invariants for each index stage separately, as well as ones that should hold for
     * indexes in multiple states.
     */
    public static void validateGenerationalInvariants(
        List<IndexDefinitionGeneration> stagedIndexes,
        List<IndexDefinitionGeneration> liveIndexes,
        List<IndexDefinitionGeneration> deletedIndexes)
        throws InvariantException {
      // Invariants must hold for the live indexes.
      IndexInvariants.validate(
          IndexDefinitions.indexDefinitions(liveIndexes), Collections.emptyList());

      // Invariants must hold for the staged indexes given the live ones.
      IndexInvariants.validate(
          IndexDefinitions.indexDefinitions(stagedIndexes),
          IndexDefinitions.indexDefinitions(liveIndexes));
      // all staged indexes must have a corresponding live index.
      IndexInvariants.validateStagedIndexesSubsetOfLive(stagedIndexes, liveIndexes);

      // Generation ids must be unique across all indexes.
      ImmutableList<IndexDefinitionGeneration> allIndexes =
          CollectionUtils.concat(stagedIndexes, liveIndexes, deletedIndexes);
      IndexInvariants.validateUniqueGenerationIds(allIndexes);
    }

    public static void validateCrossTypeInvariants(
        List<SearchIndexDefinition> desiredSearchDefinitions,
        List<VectorIndexDefinition> desiredVectorDefinitions,
        List<? extends IndexDefinition> existingDefinitions)
        throws InvariantException {
      List<IndexDefinition> combinedDefinitions =
          ListUtils.union(desiredSearchDefinitions, desiredVectorDefinitions);
      // Ensure that all the indexes have unique indexIds
      validateUniqueIndexIds(combinedDefinitions);
      // Ensure that each tuple of (database name, collection UUID, index name) is unique.
      validateUniqueNamespaceAndName(combinedDefinitions);
      // Ensure that none of the indexes have a type change
      validateImmutableType(combinedDefinitions, existingDefinitions);
    }

    /** Validates all index invariants. */
    public static void validate(
        List<? extends IndexDefinition> desiredDefinitions,
        List<? extends IndexDefinition> existingDefinitions)
        throws InvariantException {
      // Ensure that all the indexes have unique indexIds.
      validateUniqueIndexIds(desiredDefinitions);

      // Ensure that each tuple of (database name, collection UUID, index name) is unique.
      validateUniqueNamespaceAndName(desiredDefinitions);

      // Ensure that definitions that contain existing ids have not changed their database,
      // collectionUUID, or name.
      validateImmutableNamespace(desiredDefinitions, existingDefinitions);
    }

    /** Validates that each of the indexes have different ids. */
    public static void validateUniqueIndexIds(List<? extends IndexDefinition> indexDefinitions)
        throws InvariantException {
      Set<ObjectId> duplicateIds = getDuplicateIndexIds(indexDefinitions);

      if (!duplicateIds.isEmpty()) {
        Set<String> duplicateIdStrings =
            duplicateIds.stream().map(ObjectId::toHexString).collect(Collectors.toSet());
        throw new InvariantException(
            String.format("indexes had duplicate ids: %s", String.join(", ", duplicateIdStrings)));
      }
    }

    /** Validates that each of the indexes have different generation ids. */
    static void validateUniqueGenerationIds(List<IndexDefinitionGeneration> definitionGenerations)
        throws InvariantException {
      Set<GenerationId> duplicateIds =
          CollectionUtils.findDuplicates(
              definitionGenerations.stream()
                  .map(IndexDefinitionGeneration::getGenerationId)
                  .collect(Collectors.toList()));

      if (!duplicateIds.isEmpty()) {
        Set<String> duplicateIdStrings =
            duplicateIds.stream().map(GenerationId::uniqueString).collect(Collectors.toSet());
        throw new InvariantException(
            String.format(
                "indexes had duplicate generation ids: %s", String.join(", ", duplicateIdStrings)));
      }
    }

    /** Returns a set of index ids that are defined multiple times. */
    public static Set<ObjectId> getDuplicateIndexIds(
        List<? extends IndexDefinition> indexDefinitions) {
      Map<ObjectId, List<IndexDefinition>> indexesById =
          IndexDefinitions.groupedById(indexDefinitions);
      return indexesById.values().stream()
          .filter(i -> i.size() > 1)
          .map(i -> i.getFirst().getIndexId())
          .collect(Collectors.toSet());
    }

    /**
     * Validates that there are no indexes with the same name for the same collection UUID in a
     * database.
     */
    public static void validateUniqueNamespaceAndName(
        List<? extends IndexDefinition> indexDefinitions) throws InvariantException {
      Set<ObjectId> duplicateUniqueTuples = getDuplicateNamespaceAndNameIds(indexDefinitions);

      if (!duplicateUniqueTuples.isEmpty()) {
        Set<String> duplicateIdStrings =
            duplicateUniqueTuples.stream().map(ObjectId::toHexString).collect(Collectors.toSet());

        throw new InvariantException(
            String.format(
                "duplicate index names for the same collection: %s",
                String.join(", ", duplicateIdStrings)));
      }
    }

    /** Returns a set of index ids that share the same namespace and name. */
    public static Set<ObjectId> getDuplicateNamespaceAndNameIds(
        List<? extends IndexDefinition> indexDefinitions) {
      var indexesByUniqueTuple =
          indexDefinitions.stream()
              .collect(
                  Collectors.groupingBy(
                      i ->
                          String.format(
                              "(%s %s %s)", i.getDatabase(), i.getCollectionUuid(), i.getName()),
                      Collectors.toList()));

      return indexesByUniqueTuple.values().stream()
          .filter(i -> i.size() > 1)
          .map(i -> i.getFirst().getIndexId())
          .collect(Collectors.toSet());
    }

    /**
     * Validates that no indexes that already exist have now changed their database, collectionUUID
     * or name.
     */
    public static void validateImmutableNamespace(
        List<? extends IndexDefinition> desiredDefinitions,
        List<? extends IndexDefinition> existingDefinitions)
        throws InvariantException {

      Map<String, String> mutatedIndexes =
          getMutatedIndexes(desiredDefinitions, existingDefinitions);

      if (!mutatedIndexes.isEmpty()) {
        throw new InvariantException(
            String.format(
                "The following indexes had modified namespaces in their index definitions: %s.  %s",
                String.join(", ", mutatedIndexes.keySet()),
                String.join(".  ", mutatedIndexes.values())));
      }
    }

    private static void validateImmutableType(
        List<? extends IndexDefinition> desiredDefinitions,
        List<? extends IndexDefinition> existingDefinitions)
        throws InvariantException {
      var desiredById = IndexDefinitions.groupedById(desiredDefinitions);
      var existingById = IndexDefinitions.groupedById(existingDefinitions);
      var overlappingIds = Sets.intersection(desiredById.keySet(), existingById.keySet());

      var mutatedIds =
          overlappingIds.stream()
              .filter(
                  id -> {
                    var desired = desiredById.get(id).getFirst();
                    var existing = existingById.get(id).getFirst();
                    return desired.getType() != existing.getType();
                  })
              .map(ObjectId::toHexString)
              .collect(Collectors.toList());

      if (!mutatedIds.isEmpty()) {
        throw new InvariantException(
            String.format(
                "The following indexes had modified their type: %s",
                String.join(", ", mutatedIds)));
      }
    }

    private static Map<String, String> getMutatedIndexes(
        List<? extends IndexDefinition> desiredDefinitions,
        List<? extends IndexDefinition> existingDefinitions) {
      Map<ObjectId, List<IndexDefinition>> existingById =
          IndexDefinitions.groupedById(existingDefinitions);
      Map<ObjectId, List<IndexDefinition>> desiredById =
          IndexDefinitions.groupedById(desiredDefinitions);

      Sets.SetView<ObjectId> overlappingIds =
          Sets.intersection(desiredById.keySet(), existingById.keySet());
      Map<String, String> mutatedIndexes = new HashMap<>();
      for (ObjectId overlappingId : overlappingIds) {
        IndexDefinition desired = desiredById.get(overlappingId).getFirst();
        IndexDefinition existingDefinition = existingById.get(overlappingId).getFirst();

        if (!desired.getDatabase().equals(existingDefinition.getDatabase())) {
          mutatedIndexes.put(
              overlappingId.toHexString(),
              String.format(
                  "Index %s originally had database %s but now has %s",
                  overlappingId.toHexString(),
                  existingDefinition.getDatabase(),
                  desired.getDatabase()));
        } else if (!desired.getCollectionUuid().equals(existingDefinition.getCollectionUuid())) {
          mutatedIndexes.put(
              overlappingId.toHexString(),
              String.format(
                  "Index %s originally had collection UUID %s but now has %s",
                  overlappingId.toHexString(),
                  existingDefinition.getCollectionUuid(),
                  desired.getCollectionUuid()));
        } else if (!desired.getName().equals(existingDefinition.getName())) {
          mutatedIndexes.put(
              overlappingId.toHexString(),
              String.format(
                  "Index %s originally had name %s but now has %s",
                  overlappingId.toHexString(), existingDefinition.getName(), desired.getName()));
        } else if (!desired
            .getView()
            .map(ViewDefinition::getName)
            .equals(existingDefinition.getView().map(ViewDefinition::getName))) {
          mutatedIndexes.put(
              overlappingId.toHexString(),
              String.format(
                  "Index %s originally was created on view %s but now the view name is %s",
                  overlappingId.toHexString(),
                  existingDefinition.getView().map(ViewDefinition::getName),
                  desired.getView().map(ViewDefinition::getName)));
        }
      }

      return mutatedIndexes;
    }

    private static void validateStagedIndexesSubsetOfLive(
        List<IndexDefinitionGeneration> stagedIndexes, List<IndexDefinitionGeneration> liveIndexes)
        throws InvariantException {
      var stagedIds =
          stagedIndexes.stream()
              .map(def -> def.getGenerationId().indexId)
              .collect(Collectors.toUnmodifiableSet());
      var liveIds =
          liveIndexes.stream()
              .map(def -> def.getGenerationId().indexId)
              .collect(Collectors.toUnmodifiableSet());

      Sets.SetView<ObjectId> stagedWithoutLive = Sets.difference(stagedIds, liveIds);
      if (!stagedWithoutLive.isEmpty()) {
        throw new InvariantException(
            String.format(
                "staged indexes without corresponding live indexes: %s", stagedWithoutLive));
      }
    }
  }
}
