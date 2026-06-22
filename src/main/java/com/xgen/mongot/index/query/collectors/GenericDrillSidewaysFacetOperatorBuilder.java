package com.xgen.mongot.index.query.collectors;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.query.operators.AutocompleteOperator;
import com.xgen.mongot.index.query.operators.CompoundClause;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.operators.EqualsOperator;
import com.xgen.mongot.index.query.operators.ExistsOperator;
import com.xgen.mongot.index.query.operators.GeoShapeOperator;
import com.xgen.mongot.index.query.operators.GeoWithinOperator;
import com.xgen.mongot.index.query.operators.HasAncestorOperator;
import com.xgen.mongot.index.query.operators.HasRootOperator;
import com.xgen.mongot.index.query.operators.InOperator;
import com.xgen.mongot.index.query.operators.KnnBetaOperator;
import com.xgen.mongot.index.query.operators.MoreLikeThisOperator;
import com.xgen.mongot.index.query.operators.NearOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.PhraseOperator;
import com.xgen.mongot.index.query.operators.QueryStringOperator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.SearchOperator;
import com.xgen.mongot.index.query.operators.SpanOperator;
import com.xgen.mongot.index.query.operators.TermLevelOperator;
import com.xgen.mongot.index.query.operators.TermOperator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The {@code FacetOperatorBuilder} class is responsible for building an operator map for a generic
 * query. A generic query is one where the facet defined in doesNotAffect does not match the path
 * being queried (e.g. doesNotAffect specifies multiple facets), in which we must build operator
 * queries for each facet to use at query execution time. Resulting map contains facet names ->
 * corresponding query to obtain counts for this facet. This class performs a single traversal of a
 * generic operator tree in order to simultaneously build operators for all specified facets.
 */
public class GenericDrillSidewaysFacetOperatorBuilder {
  /**
   * Recursively traverses a generic query once, building operators for all facets simultaneously.
   * Returns a map of facet name to its corresponding operator.
   *
   * @param operator the root operator for traversal.
   * @param facetNames the set of facet names for which operators should be constructed.
   * @return a map of facet names to corresponding query to obtain counts for this facet.
   */
  static Map<String, Optional<Operator>> buildGenericQueryOperatorMap(
      Operator operator, Set<String> facetNames) {
    Map<String, Optional<Operator>> facetToOperatorMap =
        switch (operator) {
          case CompoundOperator compoundOp -> handleCompoundOperator(compoundOp, facetNames);
          case EmbeddedDocumentOperator embeddedOp ->
              handleEmbeddedOperator(embeddedOp, facetNames);
          case EqualsOperator equalsOp -> handleEqualsOperator(equalsOp, facetNames);
          case InOperator inOp -> handleInOperator(inOp, facetNames);
          case RangeOperator rangeOp -> handleRangeOperator(rangeOp, facetNames);

          // Add any new operators here with explicit handling for doesNotAffect
          case AllDocumentsOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case AutocompleteOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case ExistsOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case GeoShapeOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case GeoWithinOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case VectorSearchOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case KnnBetaOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case MoreLikeThisOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case NearOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case PhraseOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case QueryStringOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case SearchOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case SpanOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case TermOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case TermLevelOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case TextOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case HasAncestorOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
          case HasRootOperator ignored -> handleNonDoesNotAffectOperator(ignored, facetNames);
        };
    // Default missing facets to Optional.empty()
    facetNames.stream()
        .filter(facet -> !facetToOperatorMap.containsKey(facet))
        .forEach(facet -> facetToOperatorMap.put(facet, Optional.empty()));

    return facetToOperatorMap;
  }

  @VisibleForTesting
  static Map<String, Optional<Operator>> handleNonDoesNotAffectOperator(
      Operator operator, Set<String> facetNames) {
    Map<String, Optional<Operator>> result = new HashMap<>();
    facetNames.forEach(facet -> result.put(facet, Optional.of(operator)));
    return result;
  }

  /**
   * Processes an EmbeddedDocumentOperator and filters its inner operator entries based on the
   * specified facet names.
   */
  @VisibleForTesting
  static Map<String, Optional<Operator>> handleEmbeddedOperator(
      EmbeddedDocumentOperator embeddedOp, Set<String> facetNames) {
    Map<String, Optional<Operator>> filteredFacetOperatorsMap = new HashMap<>();
    Operator innerOperator = embeddedOp.operator();

    Map<String, Optional<Operator>> innerResult =
        buildGenericQueryOperatorMap(innerOperator, facetNames);
    for (String facetName : facetNames) {
      // Filter out entries in innerResult where doesNotAffect contains the current facetName
      Optional<Operator> filteredOp =
          innerResult.get(facetName).flatMap(op -> filterOperatorByDoesNotAffect(op, facetName));

      // Add the filtered operator to result if it's present and valid
      if (filteredOp.isPresent()) {
        EmbeddedDocumentOperator scopedSimpleEmbedded =
            new EmbeddedDocumentOperator(embeddedOp.score(), embeddedOp.path(), filteredOp.get());
        filteredFacetOperatorsMap.put(facetName, Optional.of(scopedSimpleEmbedded));
      } else {
        filteredFacetOperatorsMap.put(facetName, Optional.empty());
      }
    }
    return filteredFacetOperatorsMap;
  }

  /**
   * Recursively processes and filters a hierarchy of Operator objects based on whether their
   * doesNotAffect field contains a specified facetName. Excludes any operators that match the field
   * present within doesNotAffect.
   */
  @VisibleForTesting
  static Optional<Operator> filterOperatorByDoesNotAffect(Operator operator, String facetName) {
    return switch (operator) {
      case EqualsOperator equalsOperator ->
          shouldIncludeOperator(equalsOperator.doesNotAffect(), facetName)
              ? Optional.of(equalsOperator)
              : Optional.empty();
      case InOperator inOperator ->
          shouldIncludeOperator(inOperator.doesNotAffect(), facetName)
              ? Optional.of(inOperator)
              : Optional.empty();
      case RangeOperator rangeOperator ->
          shouldIncludeOperator(rangeOperator.doesNotAffect(), facetName)
              ? Optional.of(rangeOperator)
              : Optional.empty();
      case CompoundOperator compoundOperator -> {
        Map<String, Optional<Operator>> facetToOperatorMap =
            handleCompoundOperator(compoundOperator, Set.of(facetName));
        yield facetToOperatorMap.getOrDefault(facetName, Optional.empty());
      }

      // Cases where no filtering is needed; return the operator unchanged
      case AllDocumentsOperator ignored -> Optional.of(operator);
      case AutocompleteOperator ignored -> Optional.of(operator);
      case EmbeddedDocumentOperator ignored -> Optional.of(operator);
      case ExistsOperator ignored -> Optional.of(operator);
      case GeoShapeOperator ignored -> Optional.of(operator);
      case GeoWithinOperator ignored -> Optional.of(operator);
      case VectorSearchOperator ignored -> Optional.of(operator);
      case KnnBetaOperator ignored -> Optional.of(operator);
      case MoreLikeThisOperator ignored -> Optional.of(operator);
      case NearOperator ignored -> Optional.of(operator);
      case PhraseOperator ignored -> Optional.of(operator);
      case QueryStringOperator ignored -> Optional.of(operator);
      case SearchOperator ignored -> Optional.of(operator);
      case SpanOperator ignored -> Optional.of(operator);
      case TermOperator ignored -> Optional.of(operator);
      case TermLevelOperator ignored -> Optional.of(operator);
      case TextOperator ignored -> Optional.of(operator);
      case HasAncestorOperator ignored -> Optional.of(operator);
      case HasRootOperator ignored -> Optional.of(operator);
    };
  }

  /**
   * Processes a CompoundOperator by breaking its clauses (filter, must, mustNot, should) down into
   * facet-specific mappings. For each clause type, the operators are filtered based on relevance to
   * the provided facet names and recombined into new CompoundOperator instances.
   */
  @VisibleForTesting
  static Map<String, Optional<Operator>> handleCompoundOperator(
      CompoundOperator operator, Set<String> facetNames) {
    // Initialize result maps for each clause type
    Map<String, List<Operator>> filterClauses = new HashMap<>();
    Map<String, List<Operator>> mustClauses = new HashMap<>();
    Map<String, List<Operator>> mustNotClauses = new HashMap<>();
    Map<String, List<Operator>> shouldClauses = new HashMap<>();

    facetNames.forEach(
        facet -> {
          filterClauses.put(facet, new ArrayList<>());
          mustClauses.put(facet, new ArrayList<>());
          mustNotClauses.put(facet, new ArrayList<>());
          shouldClauses.put(facet, new ArrayList<>());
        });

    // Process each clause type
    handleClauseOperators(operator.filter().operators(), filterClauses, facetNames);
    handleClauseOperators(operator.must().operators(), mustClauses, facetNames);
    handleClauseOperators(operator.mustNot().operators(), mustNotClauses, facetNames);
    handleClauseOperators(operator.should().operators(), shouldClauses, facetNames);

    Map<String, Optional<Operator>> compoundOperatorsByFacet = new HashMap<>();
    for (String facet : facetNames) {
      // Only create compound operator if at least one clause has operators
      boolean hasOperators =
          !filterClauses.get(facet).isEmpty()
              || !mustClauses.get(facet).isEmpty()
              || !mustNotClauses.get(facet).isEmpty()
              || !shouldClauses.get(facet).isEmpty();

      if (hasOperators) {
        CompoundOperator facetOp =
            new CompoundOperator(
                operator.score(),
                wrapClause(filterClauses.get(facet)),
                wrapClause(mustClauses.get(facet)),
                wrapClause(mustNotClauses.get(facet)),
                wrapClause(shouldClauses.get(facet)),
                Math.min(operator.minimumShouldMatch(), shouldClauses.get(facet).size()),
                operator.doesNotAffect());
        compoundOperatorsByFacet.put(facet, Optional.of(facetOp));
      }
    }

    return compoundOperatorsByFacet;
  }

  @VisibleForTesting
  static void handleClauseOperators(
      List<? extends Operator> operators,
      Map<String, List<Operator>> clauseMap,
      Set<String> facetNames) {
    for (Operator op : operators) {
      Map<String, Optional<Operator>> facetOps = buildGenericQueryOperatorMap(op, facetNames);
      facetOps.forEach(
          (facet, operatorOpt) ->
              operatorOpt.ifPresent(operator -> clauseMap.get(facet).add(operator)));
    }
  }

  private static Optional<CompoundClause> wrapClause(List<Operator> operators) {
    return operators.isEmpty() ? Optional.empty() : Optional.of(new CompoundClause(operators));
  }

  @VisibleForTesting
  static Map<String, Optional<Operator>> handleEqualsOperator(
      EqualsOperator operator, Set<String> facetNames) {
    Map<String, Optional<Operator>> result = new HashMap<>();
    for (String facet : facetNames) {
      if (shouldIncludeOperator(operator.doesNotAffect(), facet)) {
        result.put(facet, Optional.of(operator));
      }
    }
    return result;
  }

  @VisibleForTesting
  static Map<String, Optional<Operator>> handleInOperator(
      InOperator operator, Set<String> facetNames) {
    Map<String, Optional<Operator>> result = new HashMap<>();
    for (String facet : facetNames) {
      if (shouldIncludeOperator(operator.doesNotAffect(), facet)) {
        result.put(facet, Optional.of(operator));
      }
    }
    return result;
  }

  @VisibleForTesting
  static Map<String, Optional<Operator>> handleRangeOperator(
      RangeOperator operator, Set<String> facetNames) {
    Map<String, Optional<Operator>> result = new HashMap<>();
    for (String facet : facetNames) {
      if (shouldIncludeOperator(operator.doesNotAffect(), facet)) {
        result.put(facet, Optional.of(operator));
      }
    }
    return result;
  }

  /**
   * Determines whether an operator should be included in a facet's query based on the doesNotAffect
   * list associated with this operator. The operator should be included if either:
   *
   * <ul>
   *   <li>doesNotAffect list does not contain the given facet name, or
   *   <li>doesNotAffect list is absent.
   * </ul>
   *
   * <p>If doesNotAffect specifies a facet, this indicates that the associated operator should not
   * affect the document set that the facet is computing its counts over.
   */
  @VisibleForTesting
  static boolean shouldIncludeOperator(Optional<List<String>> doesNotAffect, String facetName) {
    return doesNotAffect.map(list -> !list.contains(facetName)).orElse(true);
  }
}
