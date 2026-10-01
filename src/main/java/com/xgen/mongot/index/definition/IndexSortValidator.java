package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.query.sort.MetaSortOptions;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;

public class IndexSortValidator {
  private static Trie<String, FieldDefinition> buildStaticFieldsIndex(
      DocumentFieldDefinition mappings) {
    Trie<String, FieldDefinition> fields = new PatriciaTrie<>();
    SearchIndexDefinition.registerFields(fields, Optional.empty(), mappings);
    return fields;
  }

  static void validateNoScoreFields(Sort sort) throws BsonParseException {
    Set<String> scoreFields =
        sort.getSortFields().stream()
            .filter(f -> f.options() instanceof MetaSortOptions)
            .map(f -> f.field().toString())
            .collect(Collectors.toSet());

    if (!scoreFields.isEmpty()) {
      throw new BsonParseException(
          String.format("Cannot sort on score for fields: %s", scoreFields), Optional.empty());
    }
  }

  /**
   * An {@code embeddedDocument} scope is a query-time concept (it restricts a sort to the matching
   * children of an array per query). It has no meaning on an index-level sort, where {@code
   * LuceneIndexSortFactory} builds an ordinary physical sort that ignores the scope, so reject it
   * here rather than silently accept-and-ignore it. This is independent of any query-side feature
   * gate.
   */
  static void validateNoEmbeddedScope(Sort sort) throws BsonParseException {
    Set<String> scopedFields =
        sort.getSortFields().stream()
            .filter(
                f ->
                    f.options() instanceof UserFieldSortOptions userOptions
                        && userOptions.embeddedScope().isPresent())
            .map(f -> f.field().toString())
            .collect(Collectors.toSet());

    if (!scopedFields.isEmpty()) {
      throw new BsonParseException(
          String.format(
              "embeddedDocument is not supported on an index sort for fields: %s", scopedFields),
          Optional.empty());
    }
  }

  static void validateSortFieldsStaticallyDefined(
      Sort sort, Trie<String, FieldDefinition> staticFields) throws BsonParseException {
    Set<String> undefinedSortFields =
        sort.getSortFields().stream()
            .map(f -> f.field().toString())
            .filter(f -> !staticFields.containsKey(f))
            .collect(Collectors.toSet());

    if (!undefinedSortFields.isEmpty()) {
      throw new BsonParseException(
          String.format("Sort fields: %s are not statically defined", undefinedSortFields),
          Optional.empty());
    }
  }

  /**
   * String-ish types that are not sortable on their own but are acceptable alongside TOKEN. When
   * TOKEN is present, STRING and AUTOCOMPLETE definitions are filtered out before validation.
   */
  private static final ImmutableSet<FieldTypeDefinition.Type> STRING_ISH_NON_SORTABLE_TYPES =
      ImmutableSet.of(FieldTypeDefinition.Type.STRING, FieldTypeDefinition.Type.AUTOCOMPLETE);

  /**
   * Validates that all sort fields have at least one sortable type definition, and that every type
   * definition (after string-ish deduplication) is in {@link
   * FieldDefinition#INDEXING_SORTABLE_TYPES}.
   *
   * <p>String-ish deduplication: if TOKEN is present, STRING and AUTOCOMPLETE definitions are
   * ignored because TOKEN subsumes them for sort purposes.
   */
  static void validateSortFieldsAreSortable(Sort sort, Trie<String, FieldDefinition> staticFields)
      throws BsonParseException {
    // LinkedHashMap preserves insertion order for deterministic error messages.
    Map<String, List<FieldTypeDefinition.Type>> unsortableFields = new LinkedHashMap<>();

    for (MongotSortField sortField : sort.getSortFields()) {
      FieldDefinition fieldDefinition =
          Check.isNotNull(staticFields.get(sortField.field().toString()), "fieldDefinition");

      List<FieldTypeDefinition.Type> allTypes =
          fieldDefinition
              .getAllDefinitions()
              .flatMap(Optional::stream)
              .map(FieldTypeDefinition::getType)
              .toList();

      List<FieldTypeDefinition.Type> effectiveTypes = filterStringishTypes(allTypes);

      if (effectiveTypes.isEmpty()) {
        unsortableFields.put(sortField.field().toString(), allTypes);
        continue;
      }

      boolean allSortable =
          effectiveTypes.stream().allMatch(FieldDefinition.INDEXING_SORTABLE_TYPES::contains);
      if (!allSortable) {
        unsortableFields.put(sortField.field().toString(), effectiveTypes);
      }
    }

    if (!unsortableFields.isEmpty()) {
      throw new BsonParseException(
          String.format(
              "Sort fields are not sortable: %s. Sortable types are: %s",
              unsortableFields, FieldDefinition.INDEXING_SORTABLE_TYPES),
          Optional.empty());
    }
  }

  static List<FieldTypeDefinition.Type> filterStringishTypes(List<FieldTypeDefinition.Type> types) {
    boolean hasToken = types.contains(FieldTypeDefinition.Type.TOKEN);
    if (hasToken) {
      return types.stream().filter(t -> !STRING_ISH_NON_SORTABLE_TYPES.contains(t)).toList();
    }
    return types;
  }

  public static void checkSortedIndexEnabled(
      SearchIndexDefinition index, FeatureFlags featureFlags) {
    if (index.getSort().isPresent() && !featureFlags.isEnabled(Feature.SORTED_INDEX)) {
      throw new IllegalArgumentException(
          "Sort configuration is not supported, " + "Feature flag SORTED_INDEX is not enabled.");
    }
  }

  /**
   * Validates that the sort configuration is compatible with the field mappings.
   *
   * @param optionalSort the sort configuration to validate
   * @param mappings the document field mappings
   * @throws BsonParseException if validation fails
   */
  public static void validateSort(Optional<Sort> optionalSort, DocumentFieldDefinition mappings)
      throws BsonParseException {
    if (optionalSort.isEmpty()) {
      return;
    }

    Trie<String, FieldDefinition> staticFields = buildStaticFieldsIndex(mappings);
    Sort sort = optionalSort.get();

    validateNoScoreFields(sort);
    validateNoEmbeddedScope(sort);
    validateSortFieldsStaticallyDefined(sort, staticFields);
    validateSortFieldsAreSortable(sort, staticFields);
  }
}
