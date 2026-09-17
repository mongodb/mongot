package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.LoggableException;
import java.util.Set;
import org.jetbrains.annotations.Nullable;

/**
 * InvalidIndexDefinitionException is thrown when an IndexDefinition is attempted to be created with
 * invalid properties.
 */
public class InvalidIndexDefinitionException extends LoggableException {

  public InvalidIndexDefinitionException(String s) {
    super(s);
  }

  /**
   * Throw if an index definition references an analyzer name associated with a known, invalid
   * analyzer.
   */
  public static InvalidIndexDefinitionException referencesInvalidOverriddenAnalyzer(
      String analyzerName, @Nullable String analyzerError) {
    return new InvalidIndexDefinitionException(
        String.format(
            "references invalid analyzer \"%s\" that has the following error: %s",
            analyzerName, analyzerError));
  }

  /**
   * Throw if an index definition references an analyzer name not associated with any known
   * analyzers, valid or invalid.
   */
  public static InvalidIndexDefinitionException referencesUndefinedAnalyzers(
      Set<String> undefinedAnalyzerReferences) {
    return new InvalidIndexDefinitionException(
        String.format(
            "references non-existent analyzers: %s",
            String.join(", ", undefinedAnalyzerReferences.stream().sorted().toList())));
  }

  /** Throw if a SynonymMappingDefinition uses an analyzer that produces a graph token stream. */
  public static InvalidIndexDefinitionException usesGraphTokenProducingAnalyzerWithSynonyms(
      SynonymMappingDefinition definition) {
    return new InvalidIndexDefinitionException(
        String.format(
            "synonym mapping %s uses graph token stream producing analyzer %s",
            definition.name(), definition.analyzer()));
  }

  /**
   * Throw if a SynonymMappingDefinition names a stock analyzer that only exists as the base of an
   * overridden analyzer definition, and so is never instantiated into the analyzer registry.
   */
  public static InvalidIndexDefinitionException usesBaseOnlyAnalyzerWithSynonyms(
      SynonymMappingDefinition definition) {
    return new InvalidIndexDefinitionException(
        String.format(
            "synonym mapping %s references analyzer %s, which cannot be used on its own",
            definition.name(), definition.analyzer()));
  }

  /**
   * Throw if an AutocompleteFieldDefinition uses an analyzer that produces a graph token stream.
   */
  public static InvalidIndexDefinitionException usesGraphTokenProducingAnalyzerWithAutocomplete(
      String path, AutocompleteFieldDefinition definition) {
    return new InvalidIndexDefinitionException(
        String.format(
            "autocomplete field at path %s uses graph token stream producing analyzer %s",
            path, definition.getAnalyzer()));
  }
}
