package com.xgen.mongot.index.lucene.query.highlights;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.util.LucenePath;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringWildcardPath;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.SearchQueryTimeMappingChecks;
import com.xgen.mongot.index.query.highlights.Highlight;
import com.xgen.mongot.index.query.highlights.UnresolvedHighlight;
import com.xgen.mongot.index.query.operators.AutocompleteOperator;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.PhraseOperator;
import com.xgen.mongot.index.query.operators.RegexOperator;
import com.xgen.mongot.index.query.operators.SearchOperator;
import com.xgen.mongot.index.query.operators.SpanOperator;
import com.xgen.mongot.index.query.operators.TermOperator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.WildcardOperator;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.FieldPath;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexReader;

/**
 * HighlightResolver performs the necessary operations to turn an UnresolvedHighlight into a
 * Highlight.
 */
public class HighlightResolver {
  private static final Predicate<UnresolvedStringPath> IS_WILDCARD =
      path -> path instanceof UnresolvedStringWildcardPath;

  private final SearchQueryTimeMappingChecks queryTimeMappingChecks;

  @VisibleForTesting
  HighlightResolver(SearchQueryTimeMappingChecks queryTimeMappingChecks) {
    this.queryTimeMappingChecks = queryTimeMappingChecks;
  }

  public static HighlightResolver create(SearchFieldDefinitionResolver resolver) {
    SearchQueryTimeMappingChecks queryTimeMappingChecks =
        new SearchQueryTimeMappingChecks(resolver);
    return new HighlightResolver(queryTimeMappingChecks);
  }

  /** Turns an UnresolvedHighlight into a Highlight. */
  public Highlight resolveHighlight(
      UnresolvedHighlight unresolvedHighlight,
      IndexReader indexReader,
      Operator operator,
      Optional<FieldPath> returnScope)
      throws InvalidQueryException {
    List<UnresolvedStringPath> unresolvedPaths = unresolvedHighlight.paths();

    Stream<StringPath> resolvedWildcardPaths =
        resolveWildcardPaths(unresolvedPaths, indexReader, returnScope);

    Stream<StringPath> resolvedNonWildcardPaths =
        resolveNonWildcardPaths(unresolvedPaths, indexReader, returnScope);

    Map<StringPath, String> queriedPathsToLuceneFieldNames =
        getPathsToLuceneFieldNamesMap(indexReader, operator, returnScope);

    Stream<String> luceneFieldNamesForResolvedWildcardPaths =
        resolvedWildcardPaths
            .map(
                resolvedPath ->
                    validateWildcardPath(resolvedPath, queriedPathsToLuceneFieldNames, returnScope))
            .flatMap(Optional::stream); // resolved wildcard paths that fail validation are removed

    Stream<String> luceneFieldNamesForResolvedNonWildcardPaths =
        CheckedStream.from(resolvedNonWildcardPaths)
            .mapAndCollectChecked(
                resolvedPath ->
                    validateNonWildcardPath(
                        resolvedPath, queriedPathsToLuceneFieldNames, returnScope))
            .stream();

    List<String> resolvedValidatedLuceneFieldNames =
        Streams.concat(
                luceneFieldNamesForResolvedWildcardPaths,
                luceneFieldNamesForResolvedNonWildcardPaths)
            .distinct()
            .toList();

    Map<String, String> storedLuceneFieldNameMap =
        resolvedValidatedLuceneFieldNames.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(), this::getStoredLuceneField, (a, b) -> a, HashMap::new));

    return Highlight.create(storedLuceneFieldNameMap, unresolvedHighlight);
  }

  private static Stream<StringPath> resolveWildcardPaths(
      List<UnresolvedStringPath> unresolvedPaths,
      IndexReader indexReader,
      Optional<FieldPath> returnScope) {
    List<UnresolvedStringWildcardPath> wildcardPaths =
        unresolvedPaths.stream()
            .filter(IS_WILDCARD)
            .map(path -> (UnresolvedStringWildcardPath) path)
            .toList();

    // For now, do not consider highlighting fields that are indexed inside embedded documents.
    // Investigate embeddedDocument highlighting in https://jira.mongodb.org/browse/CLOUDP-110940
    return LucenePath.resolveWildcardPaths(indexReader, wildcardPaths, returnScope);
  }

  private static Stream<StringPath> resolveNonWildcardPaths(
      List<UnresolvedStringPath> unresolvedPaths,
      IndexReader indexReader,
      Optional<FieldPath> returnScope) {
    List<UnresolvedStringPath> nonWildcardPaths =
        unresolvedPaths.stream().filter(IS_WILDCARD.negate()).toList();

    // For now, do not consider highlighting fields that are indexed inside embedded documents.
    // Investigate embeddedDocument highlighting in https://jira.mongodb.org/browse/CLOUDP-110940
    return LucenePath.resolveStringPaths(indexReader, nonWildcardPaths, returnScope).stream();
  }

  /**
   * Constructs a map that maps each queried path to the path's appropriate indexed version of the
   * field that should be used for highlighting.
   */
  @VisibleForTesting
  static Map<StringPath, String> getPathsToLuceneFieldNamesMap(
      IndexReader indexReader, Operator operator, Optional<FieldPath> returnScope) {
    return switch (operator) {
      case AutocompleteOperator autocompleteOperator ->
          Map.of(
              new StringFieldPath(autocompleteOperator.path()),
              FieldName.TypeField.AUTOCOMPLETE.getLuceneFieldName(
                  autocompleteOperator.path(), returnScope));
      case CompoundOperator compoundOperator ->
          compoundOperator
              .getOperators()
              .map(op -> getPathsToLuceneFieldNamesMap(indexReader, op, returnScope))
              .map(Map::entrySet)
              .flatMap(Set::stream)
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      Map.Entry::getValue,
                      HighlightResolver::mergeLuceneFieldNamesForSamePath));
      case PhraseOperator phraseOperator ->
          LucenePath.resolveStringPaths(indexReader, phraseOperator.paths(), returnScope).stream()
              .map(
                  path ->
                      Map.entry(path, FieldName.getLuceneFieldNameForStringPath(path, returnScope)))
              .collect(CollectionUtils.toMapUnsafe(Map.Entry::getKey, Map.Entry::getValue));
      case RegexOperator regexOperator ->
          LucenePath.resolveStringPaths(indexReader, regexOperator.paths(), returnScope).stream()
              .map(
                  path ->
                      Map.entry(path, FieldName.getLuceneFieldNameForStringPath(path, returnScope)))
              .collect(CollectionUtils.toMapUnsafe(Map.Entry::getKey, Map.Entry::getValue));
      case SearchOperator searchOperator ->
          searchOperator.paths().stream()
              .distinct()
              .map(
                  path ->
                      Map.entry(path, FieldName.getLuceneFieldNameForStringPath(path, returnScope)))
              .collect(CollectionUtils.toMapUnsafe(Map.Entry::getKey, Map.Entry::getValue));
      case SpanOperator spanOperator ->
          spanOperator.getPaths().stream()
              .distinct()
              .map(
                  path ->
                      Map.entry(path, FieldName.getLuceneFieldNameForStringPath(path, returnScope)))
              .collect(CollectionUtils.toMapUnsafe(Map.Entry::getKey, Map.Entry::getValue));
      case TermOperator termOperator ->
          termOperator.paths().stream()
              .distinct()
              .map(
                  path ->
                      Map.entry(path, FieldName.getLuceneFieldNameForStringPath(path, returnScope)))
              .collect(CollectionUtils.toMapUnsafe(Map.Entry::getKey, Map.Entry::getValue));
      case TextOperator textOperator ->
          LucenePath.resolveStringPaths(indexReader, textOperator.paths(), returnScope).stream()
              .map(
                  path ->
                      Map.entry(path, FieldName.getLuceneFieldNameForStringPath(path, returnScope)))
              .collect(CollectionUtils.toMapUnsafe(Map.Entry::getKey, Map.Entry::getValue));
      case WildcardOperator wildcardOperator ->
          LucenePath.resolveStringPaths(indexReader, wildcardOperator.paths(), returnScope).stream()
              .map(
                  path ->
                      Map.entry(path, FieldName.getLuceneFieldNameForStringPath(path, returnScope)))
              .collect(CollectionUtils.toMapUnsafe(Map.Entry::getKey, Map.Entry::getValue));
      default -> Collections.emptyMap();
    };
  }

  /**
   * We highlight over the autocomplete version of a field if and only if the only operator querying
   * over the given path is the autocomplete operator.
   *
   * <p>If a given path is specified by an autocomplete and string-typed operator, then only the
   * string indexed version of the field will be highlighted.
   */
  private static String mergeLuceneFieldNamesForSamePath(
      String luceneFieldName1, String luceneFieldName2) {
    Predicate<String> isStringOrMulti =
        FieldName.TypeField.STRING.isType().or(FieldName.MultiField.isType());
    if (isStringOrMulti.test(luceneFieldName1)) {
      return luceneFieldName1;
    } else if (isStringOrMulti.test(luceneFieldName2)) {
      return luceneFieldName2;
    } else {
      // If neither lucene field name starts with a string or multi field prefix, then they both
      // must start with the autocomplete field prefix.
      return luceneFieldName1;
    }
  }

  @VisibleForTesting
  Optional<String> validateWildcardPath(
      StringPath resolvedPath,
      Map<StringPath, String> queriedPathsToLuceneFieldNames,
      Optional<FieldPath> returnScope) {
    if (queriedPathsToLuceneFieldNames.containsKey(resolvedPath)) {
      // If resolved wildcard highlight path is specified in query
      try {
        // If indexed field will support highlighting, return it.
        validateQueriedPathStorage(
            resolvedPath, queriedPathsToLuceneFieldNames.get(resolvedPath), returnScope);
        return Optional.of(queriedPathsToLuceneFieldNames.get(resolvedPath));
      } catch (InvalidQueryException e) {
        // Matched field did not satisfy highlighting requirements.
        return Optional.empty();
      }
    }
    // If resolved wildcard highlight path is not specified in query, highlight its string indexed
    // version if possible.
    if (this.queryTimeMappingChecks.isHighlightableStringField(resolvedPath, returnScope)) {
      return Optional.of(FieldName.getLuceneFieldNameForStringPath(resolvedPath, returnScope));
    }
    return Optional.empty();
  }

  /**
   * A resolved non-wildcard path specified by the user that fails validation will result in an
   * exception being thrown.
   */
  @VisibleForTesting
  String validateNonWildcardPath(
      StringPath resolvedPath,
      Map<StringPath, String> queriedPathsToLuceneFieldNames,
      Optional<FieldPath> returnScope)
      throws InvalidQueryException {
    if (queriedPathsToLuceneFieldNames.containsKey(resolvedPath)) {
      // If resolved non-wildcard highlight path is specified in query, validate that the indexed
      // field will support highlighting and then return it.
      String luceneFieldName = queriedPathsToLuceneFieldNames.get(resolvedPath);
      validateQueriedPathStorage(resolvedPath, luceneFieldName, returnScope);
      return luceneFieldName;
    }
    // If the path was not queried over, highlight its string indexed version if possible.
    this.queryTimeMappingChecks.validatePathStringStorage(resolvedPath, returnScope);
    return FieldName.getLuceneFieldNameForStringPath(resolvedPath, returnScope);
  }

  private void validateQueriedPathStorage(
      StringPath resolvedPath, String luceneFieldName, Optional<FieldPath> returnScope)
      throws InvalidQueryException {
    // No need to validate queried autocomplete paths since their validation is done during
    // autocomplete query construction.
    if (FieldName.TypeField.STRING.isTypeOf(luceneFieldName)
        || FieldName.MultiField.isTypeOf(luceneFieldName)) {
      this.queryTimeMappingChecks.validatePathStringStorage(resolvedPath, returnScope);
    }
  }

  /**
   * Returns the Lucene field name from which to load stored values for highlighting.
   *
   * <p>If the given field name is a multi-field, this method attempts to resolve and return the
   * corresponding base field, but only if the base field is stored. If the base field is not
   * stored, or if the input is not a multi-field, the original field name is returned unchanged.
   *
   * <p>Note: Since we do not consider highlighting fields that are indexed inside embedded
   * documents, this method does not explicitly handle embedded document paths.
   *
   * @param luceneFieldName the original Lucene field name, which may be a multi-field
   * @return the resolved base field name if it is stored; otherwise, the original field name
   */
  @VisibleForTesting
  String getStoredLuceneField(String luceneFieldName) {
    if (!FieldName.MultiField.isTypeOf(luceneFieldName)) {
      return luceneFieldName;
    }

    StringMultiFieldPath multiFieldPath = FieldName.MultiField.getFieldPath(luceneFieldName);
    FieldPath baseFieldPath = multiFieldPath.getFieldPath();

    return this.queryTimeMappingChecks.isHighlightableStringField(
            new StringFieldPath(baseFieldPath), Optional.empty())
        ? FieldName.TypeField.STRING.getLuceneFieldName(baseFieldPath, Optional.empty())
        : luceneFieldName;
  }
}
