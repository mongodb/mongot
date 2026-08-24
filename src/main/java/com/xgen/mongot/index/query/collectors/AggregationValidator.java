package com.xgen.mongot.index.query.collectors;

import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.query.aggregation.Operator;
import com.xgen.mongot.index.query.sort.MetaSortOptions;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.UserFacingException;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;

/**
 * Central home for validation of aggregation-capable {@code facet} requests: parse-time semantic
 * checks, the schema checks the query path runs per reader, and the aggregation feature gate.
 * Keeping these checks in one place makes it easy to scope future aggregation-syntax validation
 * here rather than scattering it across query parsing.
 */
public class AggregationValidator {

  /**
   * Signals a collector that parses but cannot run as issued -- either because aggregation is
   * disabled or because it does not fit the index schema.
   *
   * <p>This package cannot depend on {@code index.query} (which already depends on it), so it
   * cannot throw {@code InvalidQueryException} directly. Callers on the query path translate this
   * into one.
   */
  public static class AggregationValidationException extends Exception
      implements UserFacingException {
    public AggregationValidationException(String message) {
      super(message);
    }
  }

  private final BsonParseContext bsonParseContext;

  public AggregationValidator(BsonParseContext bsonParseContext) {
    this.bsonParseContext = bsonParseContext;
  }

  /**
   * Rejects a global {@code sort}/{@code sortBetaV1} on an aggregation collector when no facet tree
   * carries a {@code first}/{@code last}-dependent metric. The borrowed global sort is only
   * meaningful for ordering such metrics, so a sort with nothing to order is a caller error rather
   * than being silently ignored.
   *
   * <p>The converse is deliberately allowed: a {@code first}/{@code last} metric without a global
   * field sort picks the first/last value in encountered (natural) order, mirroring MQL {@code
   * $first}/{@code $last} semantics.
   *
   * <p>Only user-field sorts are accepted: a meta (score) sort component is rejected, because
   * {@code last} under a score sort would need the globally lowest-scoring matching documents --
   * exactly the ones score pruning and early termination skip. Score-sorted {@code first} could be
   * admitted later without a breaking change; starting field-sort-only keeps the rule loosenable.
   */
  public void validateSort(AggregationCollector collector, Optional<SortSpec> sortSpec)
      throws BsonParseException {
    if (sortSpec.isEmpty()) {
      return;
    }
    if (!collector.usesFirstOrLast()) {
      this.bsonParseContext.handleSemanticError(
          "sort (or sortBetaV1) is only supported when at least one 'first' or 'last' aggregation"
              + " metric is specified");
    }
    boolean hasMetaSort =
        sortSpec.get().getSortFields().stream()
            .anyMatch(sortField -> sortField.options() instanceof MetaSortOptions);
    if (hasMetaSort) {
      this.bsonParseContext.handleSemanticError(
          "'first' and 'last' aggregation metrics only support sorting by document fields; a $meta"
              + " sort component is not supported");
    }
  }

  /**
   * Validates an aggregation collector against the index schema it was issued against, rejecting it
   * up front rather than letting it fail deeper in boundary accumulator setup, where the failure
   * could not surface as a query error: spec building reaches fields through {@code
   * BoundaryFieldResolver}, whose method cannot declare a checked exception. {@code min}/{@code
   * max} metric paths are checked here for the same reason: they do not resolve to a single column,
   * so an unmapped or legacy-facet-only path would otherwise collect as empty rather than fail.
   *
   * <p>{@code aggregationEnabled} carries the {@code SEARCH_AGGREGATION} feature flag. It is
   * checked first so a query that is invalid on both counts reports a disabled feature rather than
   * a grouping-field error.
   *
   * <p>Callers on the Lucene query path run this per partition reader. The answer is the same for
   * every partition of an index -- they share one index definition -- but repeating a schema check
   * costs nothing next to the search itself, and it leaves each reader self-contained.
   */
  public static void validateAggregationQuery(
      AggregationCollector collector,
      SearchFieldDefinitionResolver fieldDefinitionResolver,
      Optional<FieldPath> returnScope,
      boolean aggregationEnabled)
      throws AggregationValidationException {
    if (!aggregationEnabled) {
      throw new AggregationValidationException(
          "Aggregation is not enabled. Please contact support.");
    }
    validateGroupingFieldDefinitions(collector, fieldDefinitionResolver, returnScope);
    validateMixedMetricFields(collector, fieldDefinitionResolver, returnScope);
  }

  /**
   * Rejects {@code min}/{@code max} paths that no sortable doc-values column backs, and rejects
   * those metrics under an embedded-document return scope.
   *
   * <p>Run here rather than during Lucene field resolution: {@link #validateAggregationQuery} is
   * the per-reader schema gate, so an invalid path fails once as a query error instead of being
   * re-checked while resolving the spec tree.
   */
  private static void validateMixedMetricFields(
      AggregationCollector collector,
      SearchFieldDefinitionResolver fieldDefinitionResolver,
      Optional<FieldPath> returnScope)
      throws AggregationValidationException {
    for (AggregationFacetDefinition facet : collector.facets().values()) {
      validateMixedMetricFields(facet, fieldDefinitionResolver, returnScope);
    }
  }

  private static void validateMixedMetricFields(
      AggregationFacetDefinition facet,
      SearchFieldDefinitionResolver fieldDefinitionResolver,
      Optional<FieldPath> returnScope)
      throws AggregationValidationException {
    for (MetricSpec metric : facet.aggregations().values()) {
      if (metric.operator() != Operator.MIN && metric.operator() != Operator.MAX) {
        continue;
      }
      // TODO(CLOUDP-426510): support min/max aggregation over embedded documents.
      if (returnScope.isPresent()) {
        throw new AggregationValidationException(
            "Multi-typed aggregation over embedded documents is not supported.");
      }
      FieldPath path = metric.path().orElseThrow();
      Optional<FieldDefinition> fieldDef =
          fieldDefinitionResolver.getFieldDefinition(path, returnScope);
      if (fieldDef.isEmpty()
          || !fieldDef.get().hasAnyFieldDefinitionOf(FieldDefinition.getIndexingSortableTypes())) {
        throw new AggregationValidationException(
            String.format(
                "Cannot use a min/max aggregation on field \"%s\" because it was not indexed as any"
                    + " of the following types: %s.",
                path, FieldDefinition.getIndexingSortableTypes()));
      }
    }
    for (AggregationFacetDefinition child : facet.facets().values()) {
      validateMixedMetricFields(child, fieldDefinitionResolver, returnScope);
    }
  }

  /**
   * Rejects grouping fields whose index field definition search aggregation cannot read.
   *
   * <p>Search aggregation groups on v2 {@code number} / {@code date} fields only. The legacy {@code
   * numberFacet} / {@code dateFacet} types remain supported by ordinary facet queries, so they are
   * rejected here rather than in shared facet field resolution.
   *
   * <p>Without this, a legacy-facet-only field resolves to a {@code *_FACET} Lucene type and then
   * fails deeper in {@code SearchAggregationBootstrap.boundary}, where {@code
   * numberFieldDefinition()} / {@code dateFieldDefinition()} are empty and {@code orElseThrow()}
   * raises a bare {@link java.util.NoSuchElementException} instead of a query error.
   *
   * <p>A grouping field indexed as neither is left alone: facet field resolution already reports
   * that as a not-indexed error with a message covering both the v2 and legacy types.
   */
  private static void validateGroupingFieldDefinitions(
      AggregationCollector collector,
      SearchFieldDefinitionResolver fieldDefinitionResolver,
      Optional<FieldPath> returnScope)
      throws AggregationValidationException {
    for (AggregationFacetDefinition facet : collector.facets().values()) {
      validateGroupingFieldDefinitions(facet, fieldDefinitionResolver, returnScope);
    }
  }

  private static void validateGroupingFieldDefinitions(
      AggregationFacetDefinition facet,
      SearchFieldDefinitionResolver fieldDefinitionResolver,
      Optional<FieldPath> returnScope)
      throws AggregationValidationException {
    if (facet.group() instanceof FacetDefinition.BoundaryFacetDefinition<?> boundary) {
      validateBoundaryGroupingField(boundary, facet.path(), fieldDefinitionResolver, returnScope);
    }
    for (AggregationFacetDefinition child : facet.facets().values()) {
      validateGroupingFieldDefinitions(child, fieldDefinitionResolver, returnScope);
    }
  }

  private static void validateBoundaryGroupingField(
      FacetDefinition.BoundaryFacetDefinition<?> boundary,
      FieldPath path,
      SearchFieldDefinitionResolver fieldDefinitionResolver,
      Optional<FieldPath> returnScope)
      throws AggregationValidationException {
    Optional<FieldDefinition> fieldDefinition =
        fieldDefinitionResolver.getFieldDefinition(path, returnScope);
    boolean legacyOnly =
        switch (boundary) {
          case FacetDefinition.NumericFacetDefinition ignored ->
              fieldDefinition.flatMap(FieldDefinition::numberFieldDefinition).isEmpty()
                  && fieldDefinition
                      .flatMap(FieldDefinition::numberFacetFieldDefinition)
                      .isPresent();
          case FacetDefinition.DateFacetDefinition ignored ->
              fieldDefinition.flatMap(FieldDefinition::dateFieldDefinition).isEmpty()
                  && fieldDefinition.flatMap(FieldDefinition::dateFacetFieldDefinition).isPresent();
        };
    if (legacyOnly) {
      throw new AggregationValidationException(
          String.format(
              "Cannot aggregate on field \"%s\" because it is indexed as a legacy \"numberFacet\""
                  + " or \"dateFacet\" field. Search aggregation requires a \"number\" or \"date\""
                  + " field.",
              path));
    }
  }
}
