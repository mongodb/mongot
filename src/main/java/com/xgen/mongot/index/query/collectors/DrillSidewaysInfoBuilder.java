package com.xgen.mongot.index.query.collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Var;
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
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * DrillSidewaysInfoBuilder is responsible for constructing a {@link DrillSidewaysInfo}, which
 * stores information that will be later used during drill sideways query execution. This builder
 * traverses an operator, determines its optimization status (optimizable, generic, or non-drill
 * sideways) and constructs a {@code Map<String, Operator>} mapping facet name to its drill down
 * buckets for an optimizable query or mapping facet name to the query to obtain the facet counts
 * for a generic query. An optimizable query meets the following conditions:
 *
 * <ul>
 *   <li>the facet specified by doesNotAffect is the same as path being queried
 *   <li>series of "AND"s (must, filter clauses) representing facet bucket selections
 * </ul>
 *
 * <p>and can thus leverage a single call of the Lucene built-in DrillSideways API. Queries that
 * contain doesNotAffect but do not meet these conditions are considered generic, and we must
 * instead build operator queries for each facet to use at query execution time. If the operator
 * does not contain doesNotAffect anywhere within itself, then it is treated as a non-drill sideways
 * query. Key components:
 *
 * <ul>
 *   <li>DrillSidewaysInfo: Record holding facet operators, optimization status, & pre-filter
 *       operator.
 *   <li>QueryOptimizationStatus: Enum representing whether Operator is optimizable, generic, or non
 *       drill-sideways.
 *   <li>A utility class that maintains the state during operator traversal, including facetName to
 *       operator maps, pre-filter clauses, and path mappings.
 *   <li>CompoundClauseType: Enum on types of clauses (MUST, SHOULD, FILTER) for pre-filter
 *       operations.
 * </ul>
 */
public class DrillSidewaysInfoBuilder {
  public record DrillSidewaysInfo(
      Map<String, Optional<Operator>> facetOperators,
      QueryOptimizationStatus optimizationStatus,
      Optional<Operator> preFilter) {
    /** Enum that represents whether Operator is optimizable, generic, or non-drill sideways */
    public enum QueryOptimizationStatus {
      OPTIMIZABLE,
      GENERIC,
      NON_DRILL_SIDEWAYS
    }
  }

  enum CompoundClauseType {
    MUST,
    SHOULD,
    FILTER,
    MUST_NOT
  }

  private static final ImmutableSet<CompoundClauseType> NON_OPTIMIZABLE_COMPOUND_CLAUSE_TYPES =
      ImmutableSet.of(CompoundClauseType.MUST_NOT, CompoundClauseType.SHOULD);

  /**
   * BuildState is a utility that maintains the state of the operator during its traversal. Tracks
   * various components necessary for query execution.
   *
   * <ul>
   *   <li>optimizationStatus: Represents current optimization status, initially set to OPTIMIZABLE
   *   <li>containsDoesNotAffect: Boolean used to track the presence of "doesNotAffect" during
   *       operator traversal. If false by end of traversal, treat the operator as a
   *       'NON_DRILL_SIDEWAYS' query.
   *   <li>operatorsByFacet: facetName->active drill down operators
   *   <li>genericCaseFacetOperators: facetName->query to obtain facet counts
   *   <li>facetNamesFromParentDoesNotAffect: List of facetNames from "doesNotAffect" fields in
   *       parent operators, later propagated to downstream leaf operators.
   *   <li>pre-filter clauses by compound clause type
   *   <li>pathToFacetName: Mapping from query paths to facet names
   * </ul>
   */
  static class BuildState {
    DrillSidewaysInfo.QueryOptimizationStatus optimizationStatus =
        DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE;
    boolean containsDoesNotAffect = false;
    final Map<String, List<Operator>> operatorsByFacet = new HashMap<>();
    final Map<String, Optional<Operator>> facetToOperatorForGenericCase = new HashMap<>();
    Optional<List<String>> facetNamesFromParentDoesNotAffect = Optional.of(new ArrayList<>());

    List<Operator> preFilterMustClauses = new ArrayList<>();
    List<Operator> preFilterShouldClauses = new ArrayList<>();
    List<Operator> preFilterFilterClauses = new ArrayList<>();
    List<Operator> preFilterMustNotClauses = new ArrayList<>();

    final Map<String, List<String>> pathToFacetNames;

    // Fields for embedded context
    private Optional<FieldPath> embeddedPath = Optional.empty();
    private Optional<Score> embeddedScore = Optional.empty();

    BuildState(Map<String, List<String>> pathToFacetNames) {
      this.pathToFacetNames = pathToFacetNames;
    }

    /**
     * Retrieves the embedded path for the current context. This is primarily used downstream when
     * constructing the pre-filter for embedded operators.
     */
    public Optional<FieldPath> getEmbeddedPath() {
      return this.embeddedPath;
    }

    /**
     * Sets the embedded path for the current context. Used downstream for checking if an operator
     * is embedded in order to wrap prefilter in embedded doc operator.
     */
    public void setEmbeddedPath(FieldPath embeddedPath) {
      this.embeddedPath = Optional.ofNullable(embeddedPath);
    }

    public Optional<Score> getEmbeddedScore() {
      return this.embeddedScore;
    }

    public void setEmbeddedScore(Score embeddedScore) {
      this.embeddedScore = Optional.ofNullable(embeddedScore);
    }
  }

  public static BuildState create(Map<String, FacetDefinition> facetDefinitions) {
    Map<String, List<String>> pathToFacetNames = new HashMap<>();
    for (Map.Entry<String, FacetDefinition> entry : facetDefinitions.entrySet()) {
      String facetName = entry.getKey();
      String pathName = entry.getValue().path();
      if (pathToFacetNames.containsKey(pathName)) {
        pathToFacetNames.get(pathName).add(facetName);
      } else {
        pathToFacetNames.put(pathName, new ArrayList<>());
        pathToFacetNames.get(pathName).add(facetName);
      }
    }

    return new BuildState(pathToFacetNames);
  }

  /** Builds DrillSidewaysInfo() object for a given Operator operator. */
  public static DrillSidewaysInfo buildFacetOperators(
      Operator operator, Map<String, FacetDefinition> facetDefinitions) {
    BuildState state = create(facetDefinitions);

    // Traverse operator to determine if optimizable/generic and build facet->operators mapping
    traverseOperator(operator, state, Optional.empty());
    verifyQueryOptimizable(state);

    // If operator does not contain doesNotAffect anywhere in query, then return object
    // optimization status = NON_DRILL_SIDEWAYS so FacetCollector can treat it as such.
    if (!state.containsDoesNotAffect) {
      return new DrillSidewaysInfo(
          Collections.emptyMap(),
          DrillSidewaysInfo.QueryOptimizationStatus.NON_DRILL_SIDEWAYS,
          Optional.empty());
    }

    // If operator is optimizable, return DrillSidewaysInfo with Map<String, Operator>
    // containing mapping of facetName->corresponding active drill down operators
    if (state.optimizationStatus == DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE) {
      return new DrillSidewaysInfo(
          convertToFacetOperators(state.operatorsByFacet),
          DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE,
          buildPreFilter(state));
    }

    // Operator is generic, so build facetName->query map to obtain counts for each facet
    state.facetToOperatorForGenericCase.putAll(
        GenericDrillSidewaysFacetOperatorBuilder.buildGenericQueryOperatorMap(
            operator, facetDefinitions.keySet()));
    return new DrillSidewaysInfo(
        state.facetToOperatorForGenericCase,
        DrillSidewaysInfo.QueryOptimizationStatus.GENERIC,
        Optional.empty());
  }

  /**
   * Traverses the operator to organize operators for query execution. Evaluates the operator
   * structure recursively to determine if optimizable or generic. Builds mapping facetName->drill
   * down buckets if optimizable or exits if operator is generic
   *
   * @param operator The operator currently being examined and processed.
   * @param state The build state that holds current query optimization state and operator mappings.
   * @param compoundClauseType Specifies type of clause currently being processed, if applicable.
   */
  @VisibleForTesting
  static void traverseOperator(
      Operator operator, BuildState state, Optional<CompoundClauseType> compoundClauseType) {
    if (state.optimizationStatus == DrillSidewaysInfo.QueryOptimizationStatus.GENERIC) {
      return;
    }

    switch (operator) {
      case CompoundOperator compound ->
          processCompoundOperator(state, compoundClauseType, compound);
      case EmbeddedDocumentOperator embedded ->
          processEmbeddedOperator(state, embedded, compoundClauseType);

      // Handle supported leaf operators
      case RangeOperator rangeOp -> handleLeafOperator(rangeOp, state, compoundClauseType);
      case EqualsOperator equalsOp -> handleLeafOperator(equalsOp, state, compoundClauseType);
      case InOperator inOp -> handleLeafOperator(inOp, state, compoundClauseType);

      // If this is a root operator (no compoundClauseType), treat as a must clause for pre-filter
      case AllDocumentsOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case AutocompleteOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case ExistsOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case GeoShapeOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case GeoWithinOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case KnnBetaOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case VectorSearchOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case MoreLikeThisOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case NearOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case PhraseOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case QueryStringOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case SearchOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case SpanOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case TermOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case TermLevelOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case TextOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case HasAncestorOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
      case HasRootOperator ignored -> {
        CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
        addToPreFilter(operator, state, effectiveType);
      }
    }
  }

  /**
   * Processes a CompoundOperator by evaluating clause types and traversing its nested operators.
   * This method checks generic conditions that may terminate processing early, tracks the
   * "doesNotAffect" from the compound operator, and delegates traversal of its nested operators
   * based on their clause type.
   */
  private static void processCompoundOperator(
      BuildState state,
      Optional<CompoundClauseType> compoundClauseType,
      CompoundOperator compound) {
    if (genericConditionsMet(compound, state, compoundClauseType)) {
      return;
    }
    trackDoesNotAffectInParentOperators(compound, state);

    for (Operator op : compound.must().operators()) {
      traverseOperator(op, state, Optional.of(CompoundClauseType.MUST));
    }
    for (Operator op : compound.filter().operators()) {
      traverseOperator(op, state, Optional.of(CompoundClauseType.FILTER));
    }
    for (Operator op : compound.should().operators()) {
      traverseOperator(op, state, Optional.of(CompoundClauseType.SHOULD));
    }
    for (Operator op : compound.mustNot().operators()) {
      traverseOperator(op, state, Optional.of(CompoundClauseType.MUST_NOT));
    }
  }

  /**
   * Processes an EmbeddedDocumentOperator by recursively traversing its nested operator and
   * updating the build state. This handles compound operators and leaf operators within an embedded
   * document.
   */
  static void processEmbeddedOperator(
      BuildState state,
      EmbeddedDocumentOperator embedded,
      Optional<CompoundClauseType> clauseType) {
    // Set the embedded path and score in the state to mark this as an embedded document operator.
    // This will be utilized downstream when constructing the pre-filter.
    state.setEmbeddedPath(embedded.path());
    state.setEmbeddedScore(embedded.score());

    // Create a nested state to process the inner operator
    Operator nestedOperator = embedded.operator();
    BuildState nestedState = new BuildState(state.pathToFacetNames);
    traverseOperator(nestedOperator, nestedState, Optional.empty());

    // Propagate optimization status and doesNotAffect logic
    state.containsDoesNotAffect |= nestedState.containsDoesNotAffect;
    state.optimizationStatus = nestedState.optimizationStatus;

    // Early exit if the query is generic
    if (state.optimizationStatus == DrillSidewaysInfo.QueryOptimizationStatus.GENERIC) {
      return;
    }

    // Build prefilter for the inner operator
    Optional<Operator> nestedPreFilter = buildPreFilter(nestedState);

    // Distinguish between inner and outermost recursive context
    if (clauseType.isPresent()) {
      // Inner recursive case: wrap the nested operator
      if (nestedPreFilter.isPresent()) {
        EmbeddedDocumentOperator wrappedOuter =
            new EmbeddedDocumentOperator(embedded.score(), embedded.path(), nestedPreFilter.get());

        clauseType.ifPresentOrElse(
            type -> addToPreFilter(wrappedOuter, state, type),
            () -> addToPreFilter(wrappedOuter, state, CompoundClauseType.MUST));
      }
    } else {
      // Outermost recursive case. Only replace with single nested preFilter when we have nested
      // embedding (outer -> inner EmbeddedDocumentOperator -> ...); then we preserve both levels.
      // For single embedding (outer -> compound only), copy the nested state's clause lists.
      if (nestedOperator instanceof EmbeddedDocumentOperator && nestedPreFilter.isPresent()) {
        state.preFilterFilterClauses = new ArrayList<>();
        state.preFilterFilterClauses.add(nestedPreFilter.get());
        state.preFilterMustClauses = new ArrayList<>();
        state.preFilterMustNotClauses = new ArrayList<>();
        state.preFilterShouldClauses = new ArrayList<>();
      } else {
        state.preFilterFilterClauses = nestedState.preFilterFilterClauses;
        state.preFilterMustClauses = nestedState.preFilterMustClauses;
        state.preFilterMustNotClauses = nestedState.preFilterMustNotClauses;
        state.preFilterShouldClauses = nestedState.preFilterShouldClauses;
      }
    }

    // Propagate facet exclusions from nested state to parent state
    state.facetNamesFromParentDoesNotAffect =
        Optional.of(
            Stream.concat(
                    state
                        .facetNamesFromParentDoesNotAffect
                        .orElse(Collections.emptyList())
                        .stream(),
                    nestedState
                        .facetNamesFromParentDoesNotAffect
                        .orElse(Collections.emptyList())
                        .stream())
                .distinct()
                .toList());

    // Process operators from the nested state and add to operatorsByFacet
    propagateFacetOperatorsEmbedded(embedded, nestedState, state, nestedOperator);
  }

  /**
   * Builds CompoundOperator from operators in nested state, wraps in an EmbeddedDocumentOperator
   * and adds it to operatorsByFacet map.
   */
  private static void propagateFacetOperatorsEmbedded(
      EmbeddedDocumentOperator embedded,
      BuildState nestedState,
      BuildState state,
      Operator nestedOperator) {
    // Iterate over the nested state's operators mapped by facet name.
    facetLoop:
    for (Map.Entry<String, List<Operator>> entry : nestedState.operatorsByFacet.entrySet()) {
      String facetName = entry.getKey();
      List<Operator> operators = entry.getValue();

      switch (nestedOperator) {
        case CompoundOperator compound -> {
          List<Operator> filterOperators =
              extractOperatorsForEmbeddedDoc(operators, CompoundClauseType.FILTER);

          // Construct a new CompoundOperator using filtered operators.
          CompoundOperator scopedCompound =
              new CompoundOperator(
                  compound.score(),
                  filterOperators.isEmpty()
                      ? Optional.empty()
                      : Optional.of(new CompoundClause(filterOperators)),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  0,
                  compound.doesNotAffect());

          // Add the wrapped embedded operator to the state's operators map for the current facet.
          EmbeddedDocumentOperator wrappedEmbedded =
              new EmbeddedDocumentOperator(embedded.score(), embedded.path(), scopedCompound);
          state
              .operatorsByFacet
              .computeIfAbsent(facetName, k -> new ArrayList<>())
              .add(wrappedEmbedded);
        }
        case EmbeddedDocumentOperator embeddedDocumentOperator -> {
          // nestedState was already populated when the caller ran traverseOperator(nestedOperator,
          // nestedState, ...), so we only need to propagate to parent (do not call
          // processEmbeddedOperator again — that would mutate during iteration and duplicate).
          for (Map.Entry<String, List<Operator>> nestedFacetEntry :
              nestedState.operatorsByFacet.entrySet()) {
            String nestedFacetName = nestedFacetEntry.getKey();
            for (Operator nestedOperatorForFacet : nestedFacetEntry.getValue()) {
              EmbeddedDocumentOperator wrappedEmbedded =
                  new EmbeddedDocumentOperator(
                      embedded.score(), embedded.path(), nestedOperatorForFacet);
              state
                  .operatorsByFacet
                  .computeIfAbsent(nestedFacetName, k -> new ArrayList<>())
                  .add(wrappedEmbedded);
            }
          }
          break facetLoop;
        }
        case RangeOperator rangeOperator ->
            processLeafOperatorForEmbeddedDoc(rangeOperator, embedded.path(), facetName, state);
        case EqualsOperator equalsOperator ->
            processLeafOperatorForEmbeddedDoc(equalsOperator, embedded.path(), facetName, state);
        case InOperator inOperator ->
            processLeafOperatorForEmbeddedDoc(inOperator, embedded.path(), facetName, state);

        // These operators don't support doesNotAffect, so they do not need to be added to the facet
        // operator map. They've already been added to the prefilter during traverseOperator().
        case AllDocumentsOperator ignored -> {
          // Empty block
        }
        case AutocompleteOperator ignored -> {
          // Empty block
        }
        case ExistsOperator ignored -> {
          // Empty block
        }
        case GeoShapeOperator ignored -> {
          // Empty block
        }
        case GeoWithinOperator ignored -> {
          // Empty block
        }
        case VectorSearchOperator ignored -> {
          // Empty block
        }
        case KnnBetaOperator ignored -> {
          // Empty block
        }
        case MoreLikeThisOperator ignored -> {
          // Empty block
        }
        case NearOperator ignored -> {
          // Empty block
        }
        case PhraseOperator ignored -> {
          // Empty block
        }
        case QueryStringOperator ignored -> {
          // Empty block
        }
        case SearchOperator ignored -> {
          // Empty block
        }
        case SpanOperator ignored -> {
          // Empty block
        }
        case TermOperator ignored -> {
          // Empty block
        }
        case TermLevelOperator ignored -> {
          // Empty block
        }
        case TextOperator ignored -> {
          // Empty block
        }
        case HasAncestorOperator ignored -> {
          // Empty block
        }
        case HasRootOperator ignored -> {
          // Empty block
        }
      }
    }
  }

  /**
   * Extracts a stream of operators corresponding to the specified clause type (e.g., MUST, SHOULD).
   * This method works by analyzing both compound operators and individual leaf operators.
   */
  @VisibleForTesting
  static List<Operator> extractOperatorsForEmbeddedDoc(
      List<Operator> operators, CompoundClauseType clauseType) {
    return operators.stream()
        .flatMap(operator -> mapOperatorsForEmbeddedDoc(operator, clauseType))
        .toList();
  }

  /**
   * Maps an operator to a stream of corresponding clause type operators. For compound operators,
   * this extracts nested operators based on the requested clause type and includes the parent
   * operator if the clause contains any operators. Leaf operators are treated as directly
   * corresponding to the provided clause type.
   */
  private static Stream<Operator> mapOperatorsForEmbeddedDoc(
      Operator operator, CompoundClauseType clauseType) {
    return switch (operator) {
      case CompoundOperator compound -> {
        // Extract operators within the requested clause type.
        List<? extends Operator> nestedOperators =
            switch (clauseType) {
              case MUST -> compound.must().operators();
              case SHOULD -> compound.should().operators();
              case MUST_NOT -> compound.mustNot().operators();
              case FILTER -> compound.filter().operators();
            };

        // Include the parent CompoundOperator if the clause contains operators.
        yield Stream.concat(
            nestedOperators.stream(), Stream.of(compound).filter(op -> !nestedOperators.isEmpty()));
      }

      // Leaf operators treated as corresponding to the current clause type
      case EqualsOperator ignored -> Stream.of(operator);
      case InOperator ignored -> Stream.of(operator);
      case RangeOperator ignored -> Stream.of(operator);
      case AllDocumentsOperator ignored -> Stream.of(operator);
      case AutocompleteOperator ignored -> Stream.of(operator);
      case EmbeddedDocumentOperator ignored -> Stream.of(operator);
      case ExistsOperator ignored -> Stream.of(operator);
      case GeoShapeOperator ignored -> Stream.of(operator);
      case GeoWithinOperator ignored -> Stream.of(operator);
      case VectorSearchOperator ignored -> Stream.of(operator);
      case KnnBetaOperator ignored -> Stream.of(operator);
      case MoreLikeThisOperator ignored -> Stream.of(operator);
      case NearOperator ignored -> Stream.of(operator);
      case PhraseOperator ignored -> Stream.of(operator);
      case QueryStringOperator ignored -> Stream.of(operator);
      case SearchOperator ignored -> Stream.of(operator);
      case SpanOperator ignored -> Stream.of(operator);
      case TermOperator ignored -> Stream.of(operator);
      case TermLevelOperator ignored -> Stream.of(operator);
      case TextOperator ignored -> Stream.of(operator);
      case HasAncestorOperator ignored -> Stream.of(operator);
      case HasRootOperator ignored -> Stream.of(operator);
    };
  }

  /**
   * Processes a leaf operator for an embedded document. If the given leaf operator affects the
   * specified facet name, it wraps the operator in a EmbeddedDocumentOperator and adds it to the
   * operatorsByFacet in the BuildState.
   */
  static void processLeafOperatorForEmbeddedDoc(
      Operator operator, FieldPath embeddedPath, String facetName, BuildState state) {
    // Extract operator path from the specific operator type
    Optional<String> operatorPath = extractFacetNameFromOperator(operator);

    // Translate the operator path to a facet name using the pathToFacetName mapping
    Optional<List<String>> mappedFacetNames = operatorPath.map(state.pathToFacetNames::get);

    // Check if the mapped facet name matches the provided facet name
    if (mappedFacetNames.isPresent() && mappedFacetNames.get().contains(facetName)) {
      EmbeddedDocumentOperator wrappedEmbedded =
          new EmbeddedDocumentOperator(Score.defaultScore(), embeddedPath, operator);
      state
          .operatorsByFacet
          .computeIfAbsent(facetName, k -> new ArrayList<>())
          .add(wrappedEmbedded);
    }
  }

  private static Optional<String> extractFacetNameFromOperator(Operator operator) {
    return switch (operator) {
      case EqualsOperator equalsOperator -> Optional.of(equalsOperator.path().toString());
      case InOperator inOperator ->
          inOperator.paths().stream().map(FieldPath::toString).findFirst();
      case RangeOperator rangeOperator ->
          rangeOperator.paths().stream().map(FieldPath::toString).findFirst();
      default -> Optional.empty();
    };
  }

  /**
   * Verify if all operators in the query exclude only their own facet. If not, the query
   * optimizations status is updated to GENERIC.
   */
  @VisibleForTesting
  static void verifyQueryOptimizable(BuildState state) {
    if (state.optimizationStatus == DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE) {
      boolean isOptimizable =
          state.operatorsByFacet.entrySet().stream()
              .allMatch(
                  entry -> {
                    String expectedFacet = entry.getKey();
                    List<Operator> operators = entry.getValue();
                    Optional<List<String>> facetNamesFromParentDoesNotAffect =
                        state.facetNamesFromParentDoesNotAffect;

                    return allOperatorsExcludeOnlyOwnFacet(
                        operators, expectedFacet, facetNamesFromParentDoesNotAffect);
                  });

      if (!isOptimizable) {
        state.optimizationStatus = DrillSidewaysInfo.QueryOptimizationStatus.GENERIC;
      }
    }
  }

  /**
   * Validate whether compound operator has doesNotAffect field. Checks if doesNotAffect field is
   * within a mustNot or should clause, which means the query is generic.
   */
  @VisibleForTesting
  static boolean genericConditionsMet(
      CompoundOperator compoundOp,
      BuildState state,
      Optional<CompoundClauseType> compoundClauseType) {
    if (compoundOp.doesNotAffect().isPresent() && !compoundOp.doesNotAffect().get().isEmpty()) {
      // if compound operator contains doesNotAffect, update containsDoesNotAffect to true, so
      // we know later to not treat this query as NON_DRILL_SIDEWAYS
      state.containsDoesNotAffect = true;

      // Check if it is a non-optimizable clause (e.g. must, shouldNot)
      boolean isNonOptimizableClause =
          compoundClauseType.map(NON_OPTIMIZABLE_COMPOUND_CLAUSE_TYPES::contains).orElse(false);

      // if doesNotAffect defined in mustNot or should clause, query is generic
      if (isNonOptimizableClause) {
        state.optimizationStatus = DrillSidewaysInfo.QueryOptimizationStatus.GENERIC;
        return true;
      }
    }
    return false;
  }

  /**
   * Propagate facets specified by doesNotAffect from a parent operator downstream to child
   * operators.
   */
  @VisibleForTesting
  static void trackDoesNotAffectInParentOperators(CompoundOperator compoundOp, BuildState state) {
    // Get the existing list of facets from parent operators
    List<String> parentDoesNotAffectFacets =
        state.facetNamesFromParentDoesNotAffect.orElse(Collections.emptyList());

    // Get the list of facets from the current compound operator
    List<String> compoundDoesNotAffectFacets =
        compoundOp.doesNotAffect().orElse(Collections.emptyList());

    // Combine the two lists into a stream and remove duplicates.
    List<String> combinedFacets =
        Stream.concat(parentDoesNotAffectFacets.stream(), compoundDoesNotAffectFacets.stream())
            .distinct()
            .toList();

    // Wrap the combined list in an Optional, keeping track of all doesNotAffect fields so far.
    state.facetNamesFromParentDoesNotAffect =
        Optional.of(combinedFacets).filter(list -> !list.isEmpty());
  }

  /**
   * Adds leaf operator containing doesNotAffect to facetName-> operator map. If doesNotAffect
   * contained in a non-optimizable clause (e.g. must, shouldNot), then query is generic.
   */
  @VisibleForTesting
  static void handleLeafOperator(
      Operator operator, BuildState state, Optional<CompoundClauseType> compoundClauseType) {
    boolean doesNotAffect = leafOperatorContainsDoesNotAffect(operator, state);

    if (doesNotAffect) {
      // if doesNotAffect defined in mustNot or should clause, query is generic
      if (compoundClauseType.map(NON_OPTIMIZABLE_COMPOUND_CLAUSE_TYPES::contains).orElse(false)) {
        state.optimizationStatus = DrillSidewaysInfo.QueryOptimizationStatus.GENERIC;
        return;
      }
      addLeafOperatorToOperatorMap(operator, state);
    } else {
      CompoundClauseType effectiveType = compoundClauseType.orElse(CompoundClauseType.MUST);
      addToPreFilter(operator, state, effectiveType);
    }
  }

  /**
   * Check if leaf operator contains doesNotAffect - it either contains doesNotAffect itself or a
   * parent operator contains doesNotAffect field(s).
   */
  @VisibleForTesting
  static boolean leafOperatorContainsDoesNotAffect(Operator operator, BuildState state) {
    boolean operatorDefinesDoesNotAffect =
        getDoesNotAffect(operator).map(excludedFacets -> !excludedFacets.isEmpty()).orElse(false);

    // If leaf operator has doesNotAffect defined, update containsDoesNotAffect to true, so we
    // know later to not treat this as a non-drill sideways query.
    state.containsDoesNotAffect |= operatorDefinesDoesNotAffect;

    // doesNotAffect is true if either leaf operator has doesNotAffect defined or if there are any
    // parent operators with doesNotAffect defined
    return operatorDefinesDoesNotAffect
        || state
            .facetNamesFromParentDoesNotAffect
            .map(facetNamesFromParentDoesNotAffect -> !facetNamesFromParentDoesNotAffect.isEmpty())
            .orElse(false);
  }

  /** Add to the map containing facetName -> active drill-down operators. */
  @VisibleForTesting
  static void addLeafOperatorToOperatorMap(Operator operator, BuildState state) {
    List<FieldPath> paths = getLeafOperatorPaths(operator);
    for (FieldPath path : paths) {
      String pathName = path.toString();
      if (state.pathToFacetNames.containsKey(pathName)) {
        for (String facetName : state.pathToFacetNames.get(pathName)) {
          state.operatorsByFacet.computeIfAbsent(facetName, k -> new ArrayList<>()).add(operator);
        }
      }
    }
  }

  /** Add to operator to its appropriate prefilter clause. */
  private static void addToPreFilter(
      Operator operator, BuildState state, CompoundClauseType compoundClauseType) {
    switch (compoundClauseType) {
      case MUST -> state.preFilterMustClauses.add(operator);
      case SHOULD -> state.preFilterShouldClauses.add(operator);
      case FILTER -> state.preFilterFilterClauses.add(operator);
      case MUST_NOT -> state.preFilterMustNotClauses.add(operator);
    }
  }

  /** If query is optimizable, build prefilter for DrillSidewaysInfo object. */
  @VisibleForTesting
  static Optional<Operator> buildPreFilter(BuildState state) {
    // Check if any prefilter clauses exist
    boolean hasClauses =
        !state.preFilterMustClauses.isEmpty()
            || !state.preFilterShouldClauses.isEmpty()
            || !state.preFilterFilterClauses.isEmpty()
            || !state.preFilterMustNotClauses.isEmpty();

    if (!hasClauses) {
      return Optional.empty();
    }

    // Build the CompoundOperator for prefilter clauses
    CompoundOperator compoundOperator =
        new CompoundOperator(
            Score.defaultScore(),
            wrapClause(state.preFilterFilterClauses),
            wrapClause(state.preFilterMustClauses),
            wrapClause(state.preFilterMustNotClauses),
            wrapClause(state.preFilterShouldClauses),
            0,
            Optional.empty() // Prefilters exclude "doesNotAffect"
            );

    // Wrap in an EmbeddedDocumentOperator if embeddedPath is present
    if (state.getEmbeddedPath().isPresent()) {
      // If the only content is a single EmbeddedDocumentOperator (nested embedding), use directly
      // so the result is embeddedDoc(outer, embeddedDoc(inner, ...)) not
      // embeddedDoc(outer, compound([embeddedDoc(inner, ...)])).
      @Var Operator innerOperator = compoundOperator;
      if (state.preFilterFilterClauses.size() == 1
          && state.preFilterMustClauses.isEmpty()
          && state.preFilterMustNotClauses.isEmpty()
          && state.preFilterShouldClauses.isEmpty()
          && state.preFilterFilterClauses.get(0) instanceof EmbeddedDocumentOperator embedded) {
        innerOperator = embedded;
      }
      return Optional.of(
          new EmbeddedDocumentOperator(
              state.getEmbeddedScore().orElse(Score.defaultScore()),
              state.getEmbeddedPath().get(),
              innerOperator));
    }

    // Otherwise, return the CompoundOperator as-is
    return Optional.of(compoundOperator);
  }

  private static List<FieldPath> getLeafOperatorPaths(Operator operator) {
    return switch (operator) {
      case RangeOperator rangeOp -> rangeOp.paths();
      case EqualsOperator equalsOp -> List.of(equalsOp.path());
      case InOperator inOp -> inOp.paths();

      case AllDocumentsOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
      case AutocompleteOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
      case CompoundOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
      case EmbeddedDocumentOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
      case ExistsOperator ignored -> throw new IllegalArgumentException("Unexpected operator type");
      case GeoShapeOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
      case GeoWithinOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
      case VectorSearchOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
      case KnnBetaOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
      case MoreLikeThisOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
      case NearOperator ignored -> throw new IllegalArgumentException("Unexpected operator type");
      case PhraseOperator ignored -> throw new IllegalArgumentException("Unexpected operator type");
      case QueryStringOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
      case SearchOperator ignored -> throw new IllegalArgumentException("Unexpected operator type");
      case SpanOperator ignored -> throw new IllegalArgumentException("Unexpected operator type");
      case TermOperator ignored -> throw new IllegalArgumentException("Unexpected operator type");
      case TermLevelOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
      case TextOperator ignored -> throw new IllegalArgumentException("Unexpected operator type");
      case HasAncestorOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
      case HasRootOperator ignored ->
          throw new IllegalArgumentException("Unexpected operator type");
    };
  }

  private static Optional<CompoundClause> wrapClause(List<Operator> operators) {
    return operators.isEmpty() ? Optional.empty() : Optional.of(new CompoundClause(operators));
  }

  /**
   * For an operator to be optimizable, its doesNotAffect fields (including doesNotAffect fields
   * from any of its parent operators) must only exclude facet matching its own path.
   */
  @VisibleForTesting
  static boolean allOperatorsExcludeOnlyOwnFacet(
      List<Operator> operators,
      String expectedFacet,
      Optional<List<String>> facetNamesFromParentDoesNotAffect) {
    return operators.stream()
        .allMatch(
            operator -> {
              // Combine doesNotAffect lists from the operator itself and parent operators
              List<String> combinedDoesNotAffect =
                  Stream.concat(
                          getDoesNotAffect(operator).orElse(Collections.emptyList()).stream(),
                          facetNamesFromParentDoesNotAffect
                              .orElse(Collections.emptyList())
                              .stream())
                      .distinct()
                      .toList();

              // Validate: Combined exclusions must exclude only the expectedFacet
              return combinedDoesNotAffect.size() == 1
                  && combinedDoesNotAffect.contains(expectedFacet);
            });
  }

  private static Optional<List<String>> getDoesNotAffect(Operator operator) {
    return switch (operator) {
      case RangeOperator rangeOp -> rangeOp.doesNotAffect();
      case EqualsOperator equalsOp -> equalsOp.doesNotAffect();
      case InOperator inOp -> inOp.doesNotAffect();

      case CompoundOperator compoundOp -> getCompoundDoesNotAffect(compoundOp);
      case EmbeddedDocumentOperator embedded -> getDoesNotAffect(embedded.operator());

      case AllDocumentsOperator ignored -> Optional.empty();
      case AutocompleteOperator ignored -> Optional.empty();
      case ExistsOperator ignored -> Optional.empty();
      case GeoShapeOperator ignored -> Optional.empty();
      case GeoWithinOperator ignored -> Optional.empty();
      case VectorSearchOperator ignored -> Optional.empty();
      case KnnBetaOperator ignored -> Optional.empty();
      case MoreLikeThisOperator ignored -> Optional.empty();
      case NearOperator ignored -> Optional.empty();
      case PhraseOperator ignored -> Optional.empty();
      case QueryStringOperator ignored -> Optional.empty();
      case SearchOperator ignored -> Optional.empty();
      case SpanOperator ignored -> Optional.empty();
      case TermOperator ignored -> Optional.empty();
      case TermLevelOperator ignored -> Optional.empty();
      case TextOperator ignored -> Optional.empty();
      case HasAncestorOperator ignored -> Optional.empty();
      case HasRootOperator ignored -> Optional.empty();
    };
  }

  /**
   * Combines the doesNotAffect lists for a compound operator and its nested operators. This method
   * iterates over all nested operators within the compound operator's clauses and aggregates their
   * doesNotAffect fields.
   */
  private static Optional<List<String>> getCompoundDoesNotAffect(CompoundOperator compoundOp) {
    @Var
    List<String> combinedDoesNotAffect =
        new ArrayList<>(compoundOp.doesNotAffect().orElse(Collections.emptyList()));

    if (compoundOp.must().isPresent()) {
      for (Operator nested : compoundOp.must().operators()) {
        combinedDoesNotAffect.addAll(getDoesNotAffect(nested).orElse(Collections.emptyList()));
      }
    }
    if (compoundOp.filter().isPresent()) {
      for (Operator nested : compoundOp.filter().operators()) {
        combinedDoesNotAffect.addAll(getDoesNotAffect(nested).orElse(Collections.emptyList()));
      }
    }
    if (compoundOp.should().isPresent()) {
      for (Operator nested : compoundOp.should().operators()) {
        combinedDoesNotAffect.addAll(getDoesNotAffect(nested).orElse(Collections.emptyList()));
      }
    }
    if (compoundOp.mustNot().isPresent()) {
      for (Operator nested : compoundOp.mustNot().operators()) {
        combinedDoesNotAffect.addAll(getDoesNotAffect(nested).orElse(Collections.emptyList()));
      }
    }
    combinedDoesNotAffect = combinedDoesNotAffect.stream().distinct().toList();

    // Return the combined doesNotAffect list as an Optional
    return combinedDoesNotAffect.isEmpty() ? Optional.empty() : Optional.of(combinedDoesNotAffect);
  }

  /**
   * Given a Map of String facet name->list of operators queried at that path Convert to Map of
   * String facet name->Operator by wrapping list of operators via should clauses into a single
   * CompoundOperator
   */
  @VisibleForTesting
  static Map<String, Optional<Operator>> convertToFacetOperators(
      Map<String, List<Operator>> operatorsByPath) {
    Map<String, Optional<Operator>> result = new HashMap<>();

    for (Map.Entry<String, List<Operator>> entry : operatorsByPath.entrySet()) {
      String path = entry.getKey();
      List<Operator> operators = entry.getValue();

      if (operators.size() == 1) {
        // Single operator case - use it directly
        result.put(path, Optional.of(operators.get(0)));
      } else {
        // Multiple operators - create a compound with should clauses
        // This is in line with how Lucene specifies multiple drill-downs for each facet,
        // wrapping them all inside a single SHOULD clause
        result.put(
            path,
            Optional.of(
                new CompoundOperator(
                    Score.defaultScore(),
                    Optional.empty(), // filter
                    Optional.empty(), // must
                    Optional.empty(), // mustNot
                    Optional.of(new CompoundClause(operators)),
                    0,
                    Optional.empty())));
      }
    }
    return result;
  }
}
