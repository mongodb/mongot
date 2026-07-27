package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.NumberFieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.aggregation.AggregationFieldResolver;
import com.xgen.mongot.index.query.aggregation.NumericMetricFieldPaths;
import com.xgen.mongot.index.query.aggregation.ResolvedNumericMetricField;
import com.xgen.mongot.index.query.aggregation.ResolvedTokenField;
import com.xgen.mongot.index.query.aggregation.TopAggregationSpec;
import com.xgen.mongot.index.query.collectors.FacetDefinition.BoundaryFacetDefinition;
import com.xgen.mongot.util.FieldPath;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Single entry point for resolving search-aggregation fields through a {@link LuceneFacetContext}.
 *
 * <p>Centralizes field-definition lookup, Lucene field naming, and boundary facet typing so callers
 * (metric resolution, boundary spec building, etc.) do not reach into {@link LuceneFacetContext}
 * directly.
 */
public final class LuceneAggregationContextFieldResolver implements AggregationFieldResolver {

  private final LuceneFacetContext facetContext;
  private final Map<FieldPath, ResolvedNumericMetricField> numericFields;
  private final Map<FieldPath, ResolvedTokenField> tokenFields;

  public LuceneAggregationContextFieldResolver(LuceneFacetContext facetContext) {
    this(facetContext, Map.of(), Map.of());
  }

  private LuceneAggregationContextFieldResolver(
      LuceneFacetContext facetContext,
      Map<FieldPath, ResolvedNumericMetricField> numericFields,
      Map<FieldPath, ResolvedTokenField> tokenFields) {
    this.facetContext = facetContext;
    this.numericFields = numericFields;
    this.tokenFields = tokenFields;
  }

  public FieldDefinition getFieldDefinition(FieldPath path, Optional<FieldPath> returnScope)
      throws InvalidQueryException {
    return this.facetContext
        .getFieldDefinitionResolver()
        .getFieldDefinition(path, returnScope)
        .orElseThrow(
            () ->
                new InvalidQueryException(
                    String.format(
                        "Cannot facet on field \"%s\" \"%s\" because it was not indexed.",
                        path, returnScope.map(FieldPath::toString).orElse(""))));
  }

  public FieldName.TypeField getBoundaryFacetFieldType(
      BoundaryFacetDefinition<?> facetDefinition, Optional<FieldPath> returnScope)
      throws InvalidQueryException {
    return this.facetContext.getBoundaryFacetFieldType(facetDefinition, returnScope);
  }

  /**
   * Resolves all metric field paths referenced by the given spec tree for Lucene execution.
   *
   * <p>Call before Lucene execution so invalid queries surface as {@link InvalidQueryException}.
   */
  public LuceneAggregationContextFieldResolver withResolvedFields(TopAggregationSpec rootSpec)
      throws InvalidQueryException {
    Set<FieldPath> numericPaths = NumericMetricFieldPaths.collect(rootSpec);
    Map<FieldPath, ResolvedNumericMetricField> numeric = new LinkedHashMap<>();
    for (FieldPath path : numericPaths) {
      numeric.put(path, resolveNumericMetricField(path, Optional.empty()));
    }
    // TODO(CLOUDP-425978): collect token paths from first/last specs when execution lands.
    return new LuceneAggregationContextFieldResolver(
        this.facetContext, Collections.unmodifiableMap(numeric), Map.of());
  }

  /** Resolves a numeric metric path to its Lucene doc-values field and representation. */
  public ResolvedNumericMetricField resolveNumericMetricField(
      FieldPath path, Optional<FieldPath> returnScope) throws InvalidQueryException {
    NumberFieldDefinition numberFieldDef = getNumberFieldDefinition(path, returnScope);
    FieldName.TypeField typeField = getNumericTypeField(numberFieldDef);
    return new ResolvedNumericMetricField(
        typeField.getLuceneFieldName(path, returnScope), numberFieldDef.options().representation());
  }

  /** Resolves a token aggregation metric path to its Lucene doc-values field. */
  public ResolvedTokenField resolveTokenMetricField(FieldPath path, Optional<FieldPath> returnScope)
      throws InvalidQueryException {
    if (returnScope.isPresent()) {
      throw new InvalidQueryException(
          "Token aggregation over embedded documents is not supported.");
    }

    Optional<FieldDefinition> maybeFieldDef =
        this.facetContext.getFieldDefinitionResolver().getFieldDefinition(path, returnScope);
    if (maybeFieldDef.flatMap(FieldDefinition::tokenFieldDefinition).isEmpty()) {
      throwFieldNotIndexed(path.toString(), "token", returnScope);
    }

    return new ResolvedTokenField(FieldName.TypeField.TOKEN.getLuceneFieldName(path, returnScope));
  }

  @Override
  public ResolvedNumericMetricField resolveNumeric(FieldPath path) {
    return Optional.ofNullable(this.numericFields.get(path))
        .orElseThrow(() -> new IllegalStateException("Unresolved numeric metric path: " + path));
  }

  @Override
  public ResolvedTokenField resolveToken(FieldPath path) {
    return Optional.ofNullable(this.tokenFields.get(path))
        .orElseThrow(() -> new IllegalStateException("Unresolved token metric path: " + path));
  }

  private NumberFieldDefinition getNumberFieldDefinition(
      FieldPath path, Optional<FieldPath> returnScope) throws InvalidQueryException {
    // TODO(CLOUDP-426510): support numeric aggregation over embedded documents.
    if (returnScope.isPresent()) {
      throw new InvalidQueryException(
          "Numeric aggregation over embedded documents is not supported.");
    }

    Optional<FieldDefinition> maybeFieldDef =
        this.facetContext.getFieldDefinitionResolver().getFieldDefinition(path, returnScope);

    Optional<NumberFieldDefinition> numberFieldDef =
        maybeFieldDef.flatMap(FieldDefinition::numberFieldDefinition);

    if (numberFieldDef.isEmpty()) {
      throwFieldNotIndexed(path.toString(), "number", returnScope);
    }
    return numberFieldDef.get();
  }

  /**
   * Maps a {@link NumberFieldDefinition} to its Lucene doc-values type for search aggregation.
   *
   * <p>Search aggregation resolves only v2 {@link NumberFieldDefinition} types ({@link
   * FieldName.TypeField#NUMBER_INT64_V2} / {@link FieldName.TypeField#NUMBER_DOUBLE_V2}), not
   * legacy number facet fields used by non-search-agg facet queries.
   */
  private static FieldName.TypeField getNumericTypeField(NumberFieldDefinition numberFieldDef) {
    return switch (numberFieldDef.options().representation()) {
      case DOUBLE -> FieldName.TypeField.NUMBER_DOUBLE_V2;
      case INT64 -> FieldName.TypeField.NUMBER_INT64_V2;
    };
  }

  private static void throwFieldNotIndexed(
      String path, String fieldType, Optional<FieldPath> returnScope) throws InvalidQueryException {
    String returnScopeSubstring =
        returnScope.map(scope -> String.format("at returnScope %s", scope)).orElse("");
    throw new InvalidQueryException(
        String.format(
            "Cannot facet on field \"%s\" \"%s\" because it was not indexed as a \"%s\" field.",
            path, returnScopeSubstring, fieldType));
  }
}
