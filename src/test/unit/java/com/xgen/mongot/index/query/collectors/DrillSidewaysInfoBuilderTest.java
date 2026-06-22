package com.xgen.mongot.index.query.collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.addLeafOperatorToOperatorMap;
import static com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.allOperatorsExcludeOnlyOwnFacet;
import static com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.buildPreFilter;
import static com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.convertToFacetOperators;
import static com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.extractOperatorsForEmbeddedDoc;
import static com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.genericConditionsMet;
import static com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.handleLeafOperator;
import static com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.processEmbeddedOperator;
import static com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.processLeafOperatorForEmbeddedDoc;
import static com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.trackDoesNotAffectInParentOperators;
import static com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.traverseOperator;
import static com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.verifyQueryOptimizable;

import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.DrillSidewaysInfo;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.operators.EqualsOperator;
import com.xgen.mongot.index.query.operators.ExistsOperator;
import com.xgen.mongot.index.query.operators.InOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.value.NonNullValue;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.points.NumericPoint;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDouble;
import org.junit.Test;

public class DrillSidewaysInfoBuilderTest {
  @Test
  public void handleLeafOperator_doesNotAffectNotDefined_addedToPrefilter() {
    // Set up RangeOperator without doesNotAffect explicitly defined
    Optional<NumericPoint> lower = Optional.of(new LongPoint(6L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(7L));

    RangeOperator operator =
        OperatorBuilder.range().path("size").numericBounds(lower, upper, true, false).build();

    // Initialize state
    Map<String, List<String>> pathToFacetName = Map.of("size", List.of("sizes"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    handleLeafOperator(
        operator, state, Optional.of(DrillSidewaysInfoBuilder.CompoundClauseType.FILTER));

    assertThat(state.containsDoesNotAffect).isFalse();
    assertThat(state.operatorsByFacet).isEmpty();
    assertThat(state.preFilterFilterClauses).isNotEmpty();
    assertThat(state.preFilterMustClauses).isEmpty();
    assertThat(state.preFilterShouldClauses).isEmpty();
    assertThat(state.preFilterMustNotClauses).isEmpty();
  }

  @Test
  public void handleLeafOperator_doesNotAffectInShouldClause_genericWithEarlyExit() {
    // Set up operators with doesNotAffect defined
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    RangeOperator operator =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(lower, upper, true, false)
            .doesNotAffect("colors")
            .build();

    // Define compoundClauseType that triggers early exit
    Optional<DrillSidewaysInfoBuilder.CompoundClauseType> clauseType =
        Optional.of(DrillSidewaysInfoBuilder.CompoundClauseType.SHOULD);

    // Initialize state
    Map<String, List<String>> pathToFacetName = Map.of("color", List.of("colors"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    handleLeafOperator(operator, state, clauseType);

    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC)
        .isEqualTo(state.optimizationStatus);
    assertThat(state.operatorsByFacet).isEmpty(); // No facets updated
    assertThat(state.containsDoesNotAffect).isTrue(); // Early exit - flag set to true
  }

  @Test
  public void handleLeafOperator_doesNotAffectTrue_optimizableWithNoEarlyExit() {
    // Set up a RangeOperator with doesNotAffect defined
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    RangeOperator operator =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(lower, upper, true, false)
            .doesNotAffect("colors")
            .build();

    // Define FILTER clause type -> no early exit
    Optional<DrillSidewaysInfoBuilder.CompoundClauseType> clauseType =
        Optional.of(DrillSidewaysInfoBuilder.CompoundClauseType.FILTER);

    // Initialize state
    Map<String, List<String>> pathToFacetName = Map.of("color", List.of("colors"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    handleLeafOperator(operator, state, clauseType);

    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE)
        .isEqualTo(state.optimizationStatus);
    assertThat(state.containsDoesNotAffect).isTrue();
    assertThat(state.operatorsByFacet).containsKey("colors");
    assertThat(state.operatorsByFacet.get("colors")).containsExactly(operator);
  }

  @Test
  public void handleLeafOperator_parentDoesNotAffectPropagatesToChild_optimizableWithNoEarlyExit() {
    // Set up RangeOperator without doesNotAffect explicitly defined
    Optional<NumericPoint> lower = Optional.of(new LongPoint(6L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(7L));

    RangeOperator operator =
        OperatorBuilder.range().path("size").numericBounds(lower, upper, true, false).build();

    // If parent operator has doesNotAffect defined and operator only excludes its own facet,
    // then the query is still optimizable.
    Map<String, List<String>> pathToFacetName = Map.of("size", List.of("sizes"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);
    state.facetNamesFromParentDoesNotAffect = Optional.of(List.of("sizes"));
    state.containsDoesNotAffect = true;

    handleLeafOperator(
        operator, state, Optional.of(DrillSidewaysInfoBuilder.CompoundClauseType.FILTER));

    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE)
        .isEqualTo(state.optimizationStatus);
    assertThat(state.containsDoesNotAffect).isTrue();
    assertThat(state.operatorsByFacet).containsKey("sizes");
    assertThat(state.operatorsByFacet.get("sizes")).containsExactly(operator);
  }

  @Test
  public void allOperatorsExcludeOnlyOwnFacet_allExcludeExpectedFacet() {
    // Create equals, in, and range operators that exclude only "colors"
    long value = 1;
    Operator operator1 =
        OperatorBuilder.equals().path("color").value(value).doesNotAffect("colors").build();

    Operator operator2 =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(
                Optional.of(new LongPoint(1)), Optional.of(new LongPoint(2)), true, false)
            .doesNotAffect("colors")
            .build();

    List<Long> values = new ArrayList<>();
    values.add((long) 1);
    values.add((long) 2);

    Operator operator3 =
        OperatorBuilder.in().path("color").longs(values).doesNotAffect("colors").build();

    List<Operator> operators = List.of(operator1, operator2, operator3);

    boolean result = allOperatorsExcludeOnlyOwnFacet(operators, "colors", Optional.empty());

    assertThat(result).isTrue(); // All operators exclude their own facet, matching "color"
  }

  @Test
  public void allOperatorsExcludeOnlyOwnFacet_excludeIncorrectFacet() {
    // Create an operator that excludes "size" instead of its own facet "color"
    Operator operator1 =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(
                Optional.of(new LongPoint(1)), Optional.of(new LongPoint(2)), true, false)
            .doesNotAffect("sizes") // Excludes a facet different from its own
            .build();

    Operator operator2 =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(
                Optional.of(new LongPoint(3)), Optional.of(new LongPoint(4)), true, false)
            .doesNotAffect("colors")
            .build();

    List<Operator> operators = List.of(operator1, operator2);

    boolean result = allOperatorsExcludeOnlyOwnFacet(operators, "colors", Optional.empty());

    assertThat(result).isFalse(); // One operator excludes a facet different from its own
  }

  @Test
  public void allOperatorsExcludeOnlyOwnFacet_excludeMultipleFacets() {
    // Create an operator that excludes multiple facets
    Operator operator1 =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(
                Optional.of(new LongPoint(1)), Optional.of(new LongPoint(2)), true, false)
            .doesNotAffect(List.of("colors", "sizes")) // Excludes multiple facets
            .build();

    Operator operator2 =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(
                Optional.of(new LongPoint(3)), Optional.of(new LongPoint(4)), true, false)
            .doesNotAffect("colors")
            .build();

    List<Operator> operators = List.of(operator1, operator2);

    boolean result = allOperatorsExcludeOnlyOwnFacet(operators, "colors", Optional.empty());

    assertThat(result).isFalse(); // One operator excludes multiple facets
  }

  @Test
  public void allOperatorsExcludeOnlyOwnFacet_parentExclusionsConflict() {
    // Create operators that exclude only "color"
    Operator operator1 =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(
                Optional.of(new LongPoint(1)), Optional.of(new LongPoint(2)), true, false)
            .doesNotAffect("colors")
            .build();

    Operator operator2 =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(
                Optional.of(new LongPoint(3)), Optional.of(new LongPoint(4)), true, false)
            .doesNotAffect("colors")
            .build();

    List<Operator> operators = List.of(operator1, operator2);

    // Define parent exclusions that add "size"
    Optional<List<String>> parentExclusions = Optional.of(List.of("colors", "size"));

    boolean result = allOperatorsExcludeOnlyOwnFacet(operators, "colors", parentExclusions);

    assertThat(result).isFalse(); // Parent exclusions introduce additional facets ("size").
  }

  @Test
  public void convertToFacetOperators_optimizableFacetOperator_doesNotContainDoesNotAffect() {
    RangeOperator optimizableOperator =
        OperatorBuilder.range()
            .path("color") // Facet path
            .numericBounds(
                Optional.of(new LongPoint(1)), Optional.of(new LongPoint(2)), true, false)
            .build();

    String path = "color";
    Map<String, List<Operator>> inputOperatorsByPath = Map.of(path, List.of(optimizableOperator));

    Map<String, Optional<Operator>> result = convertToFacetOperators(inputOperatorsByPath);

    assertThat(result).containsKey(path);
    RangeOperator operator =
        (RangeOperator)
            result
                .get(path)
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: " + path));

    // Ensure that the resulting operator does not contain 'doesNotAffect'
    assertThat(operator.doesNotAffect()).isEmpty();
  }

  @Test
  public void extractOperatorsByClauseType_withOperatorBuilder() {
    // Create a MUST clause operator
    Operator rangeOperator =
        OperatorBuilder.range()
            .path("age")
            .numericBounds(
                Optional.of(new LongPoint(20)), Optional.of(new LongPoint(40)), true, false)
            .doesNotAffect("ageFacet")
            .build();

    // Create a SHOULD clause operator
    Operator equalsOperator =
        OperatorBuilder.equals().path("name").value("John").doesNotAffect("nameFacet").build();

    // Create a FILTER clause operator
    Operator existsOperator = OperatorBuilder.exists().path("active").build();

    // Create a MUST_NOT clause operator
    Operator mustNotRangeOperator =
        OperatorBuilder.range()
            .path("score")
            .numericBounds(
                Optional.of(new LongPoint(50)), Optional.of(new LongPoint(90)), true, true)
            .doesNotAffect("scoreFacet")
            .build();

    // Create a CompoundOperator that wraps MUST, SHOULD, FILTER, and MUST_NOT clause operators.
    CompoundOperator compoundOperator =
        OperatorBuilder.compound()
            .must(rangeOperator)
            .should(equalsOperator)
            .filter(existsOperator)
            .mustNot(mustNotRangeOperator)
            .build();

    List<Operator> operators = List.of(compoundOperator);

    // Extract operators for MUST clause.
    List<Operator> mustOperators =
        extractOperatorsForEmbeddedDoc(operators, DrillSidewaysInfoBuilder.CompoundClauseType.MUST);
    assertThat(mustOperators.size()).isEqualTo(2); // RangeOperator + CompoundOperator
    assertThat(mustOperators).contains(rangeOperator);
    assertThat(mustOperators).contains(compoundOperator);

    // Extract operators for SHOULD clause.
    List<Operator> shouldOperators =
        extractOperatorsForEmbeddedDoc(
            operators, DrillSidewaysInfoBuilder.CompoundClauseType.SHOULD);
    assertThat(shouldOperators.size()).isEqualTo(2); // EqualsOperator + CompoundOperator
    assertThat(shouldOperators).contains(equalsOperator);
    assertThat(shouldOperators).contains(compoundOperator);

    // Extract operators for FILTER clause.
    List<Operator> filterOperators =
        extractOperatorsForEmbeddedDoc(
            operators, DrillSidewaysInfoBuilder.CompoundClauseType.FILTER);
    assertThat(filterOperators.size()).isEqualTo(2); // ExistsOperator + CompoundOperator
    assertThat(filterOperators).contains(existsOperator);
    assertThat(filterOperators).contains(compoundOperator);

    // Extract operators for MUST_NOT clause.
    List<Operator> mustNotOperators =
        extractOperatorsForEmbeddedDoc(
            operators, DrillSidewaysInfoBuilder.CompoundClauseType.MUST_NOT);
    assertThat(mustNotOperators.size()).isEqualTo(2); // RangeOperator + CompoundOperator
    assertThat(mustNotOperators).contains(mustNotRangeOperator);
    assertThat(mustNotOperators).contains(compoundOperator);
  }

  @Test
  public void processLeafOperator_affectsFacet_addsToOperatorsByFacet() {
    // Set up RangeOperator with a path that matches the facetName
    Optional<NumericPoint> lower = Optional.of(new LongPoint(10L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(20L));

    RangeOperator operator =
        OperatorBuilder.range().path("size").numericBounds(lower, upper, true, false).build();

    FieldPath embeddedPath = FieldPath.parse("a.b.c").withNewRoot("document");

    // Initialize BuildState with mapping between paths and facet names
    Map<String, List<String>> pathToFacetName = Map.of("size", List.of("sizes"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    processLeafOperatorForEmbeddedDoc(operator, embeddedPath, "sizes", state);

    // Verify that the EmbeddedDocumentOperator was added to the "sizes" facet
    assertThat(state.operatorsByFacet).containsKey("sizes");
    assertThat(state.operatorsByFacet.get("sizes")).hasSize(1);
    assertThat(state.operatorsByFacet.get("sizes").get(0))
        .isInstanceOf(EmbeddedDocumentOperator.class);

    // Verify the EmbeddedDocumentOperator is correctly wrapped
    EmbeddedDocumentOperator wrappedOperator =
        (EmbeddedDocumentOperator) state.operatorsByFacet.get("sizes").get(0);
    assertThat(wrappedOperator.operator()).isEqualTo(operator);
    assertThat(wrappedOperator.path()).isEqualTo(embeddedPath);
  }

  @Test
  public void processLeafOperator_doesNotAffectFacet_doesNotAddToOperatorsByFacet() {
    // Set up EqualsOperator with a path that does not match the facetName
    EqualsOperator operator =
        OperatorBuilder.equals()
            .path("size") // Path does not match "prices" facetName
            .value("small")
            .build();

    // Create embedded document path
    FieldPath embeddedPath = FieldPath.parse("a.b.c").withNewRoot("document");

    // Initialize BuildState with mapping between paths and facet names
    Map<String, List<String>> pathToFacetName = Map.of("size", List.of("sizes"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    processLeafOperatorForEmbeddedDoc(operator, embeddedPath, "prices", state);

    // Verify that no operators were added to the "prices" facet
    assertThat(state.operatorsByFacet).doesNotContainKey("prices");
  }

  /**
   * Regression test for extractFacetNameFromOperator with InOperator (CLOUDP-321297). The path
   * lookup must use the first path's string form, not paths().toString().
   */
  @Test
  public void processLeafOperator_InOperator_affectsFacet_addsToOperatorsByFacet() {
    List<Long> values = Arrays.asList(1L, 2L);
    InOperator operator = OperatorBuilder.in().path("color").longs(values).build();

    FieldPath embeddedPath = FieldPath.parse("a.b.c").withNewRoot("document");

    Map<String, List<String>> pathToFacetNames = Map.of("color", List.of("colors"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetNames);

    processLeafOperatorForEmbeddedDoc(operator, embeddedPath, "colors", state);

    assertThat(state.operatorsByFacet).containsKey("colors");
    assertThat(state.operatorsByFacet.get("colors")).hasSize(1);
    assertThat(state.operatorsByFacet.get("colors").get(0))
        .isInstanceOf(EmbeddedDocumentOperator.class);
    EmbeddedDocumentOperator wrappedOperator =
        (EmbeddedDocumentOperator) state.operatorsByFacet.get("colors").get(0);
    assertThat(wrappedOperator.operator()).isEqualTo(operator);
    assertThat(wrappedOperator.path()).isEqualTo(embeddedPath);
  }

  @Test
  public void traverseOperator_shouldClauseWithDoesNotAffect_genericWithEarlyExit() {
    // Set up a CompoundOperator with doesNotAffect defined
    CompoundOperator compoundOperator =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(
                        Optional.of(new LongPoint(1)), Optional.of(new LongPoint(2)), true, false)
                    .doesNotAffect("colors")
                    .build())
            .build();

    Map<String, List<String>> pathToFacetName = Map.of("color", List.of("colors"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    // Set a non-optimizable clause type
    Optional<DrillSidewaysInfoBuilder.CompoundClauseType> compoundClauseType =
        Optional.of(DrillSidewaysInfoBuilder.CompoundClauseType.SHOULD);

    traverseOperator(compoundOperator, state, compoundClauseType);

    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC);
    assertThat(state.containsDoesNotAffect).isTrue(); // doesNotAffect was encountered
    assertThat(state.operatorsByFacet).isEmpty(); // No operators processed beyond early exit
  }

  @Test
  public void traverseOperator_compoundOperatorWithNestedClauses_optimizableWithNoEarlyExit() {
    // Set up nested operators
    RangeOperator mustOperator =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(
                Optional.of(new LongPoint(1)), Optional.of(new LongPoint(10)), true, false)
            .doesNotAffect("colors")
            .build();

    EqualsOperator filterOperator =
        OperatorBuilder.equals().path("size").value(42).doesNotAffect("sizes").build();

    CompoundOperator compoundOperator =
        OperatorBuilder.compound().must(mustOperator).filter(filterOperator).build();

    Map<String, List<String>> pathToFacetName =
        Map.of("color", List.of("colors"), "size", List.of("sizes"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    traverseOperator(compoundOperator, state, Optional.empty());

    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);
    assertThat(state.containsDoesNotAffect).isTrue(); // doesNotAffect processed
    assertThat(state.operatorsByFacet).containsKey("colors");
    assertThat(state.operatorsByFacet).containsKey("sizes");
    assertThat(state.operatorsByFacet.get("colors")).containsExactly(mustOperator);
    assertThat(state.operatorsByFacet.get("sizes")).containsExactly(filterOperator);
  }

  @Test
  public void traverseOperator_initializedWithGeneric_genericWithExitEarly() {
    // Set up a RangeOperator
    RangeOperator rangeOperator =
        OperatorBuilder.range()
            .path("price")
            .numericBounds(
                Optional.of(new LongPoint(10)), Optional.of(new LongPoint(20)), true, false)
            .doesNotAffect("prices")
            .build();

    // Initialize state with GENERIC optimization status
    Map<String, List<String>> pathToFacetName = Map.of("price", List.of("prices"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);
    state.optimizationStatus = DrillSidewaysInfo.QueryOptimizationStatus.GENERIC;

    traverseOperator(rangeOperator, state, Optional.empty());

    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC);
    assertThat(state.operatorsByFacet).isEmpty(); // No operators processed after exit
  }

  @Test
  public void traverseOperator_doesNotAffectNotDefined_rootLevelOperatorAddedToPrefilter() {
    // Set up a RangeOperator
    RangeOperator rangeOperator =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(
                Optional.of(new LongPoint(5)), Optional.of(new LongPoint(8)), true, false)
            .build(); // No doesNotAffect defined

    // Initialize state
    Map<String, List<String>> pathToFacetName = Map.of("color", List.of("colors"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    traverseOperator(rangeOperator, state, Optional.empty());

    assertThat(state.preFilterMustClauses).containsExactly(rangeOperator);
    assertThat(state.containsDoesNotAffect).isFalse(); // No doesNotAffect processed
  }

  @Test
  public void traverseOperator_existsOperator_noCompoundClauseType_defaultsToMust() {
    // Set up ExistsOperator
    ExistsOperator existsOperator = OperatorBuilder.exists().path("active").build();

    Map<String, List<String>> pathToFacetName = Map.of("active", List.of("isActive"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    traverseOperator(existsOperator, state, Optional.empty());

    // Optimization status remains OPTIMIZABLE
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);

    // OperatorsByFacet correctly maps the "active" field to the "isActive" facet
    assertThat(state.operatorsByFacet).isEmpty();

    // GenericCaseFacetOperators remains empty since this operator isn't generic
    assertThat(state.facetToOperatorForGenericCase).isEmpty();

    // FacetNamesFromParentDoesNotAffect should remain unchanged and optional
    assertThat(state.facetNamesFromParentDoesNotAffect.get()).isEmpty();

    // PreFilter clauses
    // Since no compoundClauseType was provided, the default type MUST is applied
    assertThat(state.preFilterMustClauses).containsExactly(existsOperator);
    assertThat(state.preFilterShouldClauses).isEmpty();
    assertThat(state.preFilterFilterClauses).isEmpty();
    assertThat(state.preFilterMustNotClauses).isEmpty();
  }

  @Test
  public void processEmbeddedOperator_optimizable_doesNotExitEarly() {
    CompoundOperator compoundOperator =
        OperatorBuilder.compound()
            .must(
                OperatorBuilder.equals()
                    .path("teachers.first")
                    .value("John")
                    .doesNotAffect("teachersFirstNameFacet")
                    .build())
            .must(
                OperatorBuilder.equals()
                    .path("teachers.last")
                    .value("Smith")
                    .doesNotAffect("teachersLastNameFacet")
                    .build())
            .build();

    EmbeddedDocumentOperator embeddedOperator =
        OperatorBuilder.embeddedDocument().path("teachers").operator(compoundOperator).build();

    // Set up BuildState that is OPTIMIZABLE
    Map<String, List<String>> pathToFacetName =
        Map.of(
            "teachers.first", List.of("teachersFirstFacet"),
            "teachers.last", List.of("teachersLastFacet"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);
    state.optimizationStatus = DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE;

    processEmbeddedOperator(state, embeddedOperator, Optional.empty());

    // Ensure the optimization status remains OPTIMIZABLE
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);

    // Verify operators are added to the appropriate facets using facet mapping
    assertThat(state.operatorsByFacet.containsKey("teachersFirstFacet")).isTrue();
    assertThat(state.operatorsByFacet.containsKey("teachersLastFacet")).isTrue();

    // Verify the operators for "teachers.first" and "teachers.last"
    assertThat(state.operatorsByFacet.get("teachersFirstFacet").size() == 1).isTrue();

    assertThat(state.operatorsByFacet.get("teachersLastFacet").size() == 1).isTrue();

    Operator firstOperator = state.operatorsByFacet.get("teachersFirstFacet").get(0);
    assertThat(firstOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    Operator lastOperator = state.operatorsByFacet.get("teachersLastFacet").get(0);
    assertThat(lastOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    EmbeddedDocumentOperator teachersFirst = (EmbeddedDocumentOperator) firstOperator;
    EmbeddedDocumentOperator teachersLast = (EmbeddedDocumentOperator) lastOperator;

    assertThat(teachersFirst.operator()).isInstanceOf(CompoundOperator.class);

    assertThat(teachersLast.operator()).isInstanceOf(CompoundOperator.class);

    assertThat(teachersFirst.path().toString()).isEqualTo("teachers");
    assertThat(teachersLast.path().toString()).isEqualTo("teachers");

    assertThat(state.containsDoesNotAffect).isTrue();
  }

  @Test
  public void processEmbeddedOperator_generic_exitsEarly() {
    // Set up a compound operator with mustNot clauses
    CompoundOperator compoundOperator =
        OperatorBuilder.compound()
            .mustNot(
                OperatorBuilder.equals()
                    .path("teachers.first")
                    .value("John")
                    .doesNotAffect("teachersFirstNameFacet")
                    .build())
            .mustNot(
                OperatorBuilder.equals()
                    .path("teachers.last")
                    .value("Smith")
                    .doesNotAffect("teachersLastNameFacet")
                    .build())
            .build();

    // Wrap the compound operator in an EmbeddedDocumentOperator
    EmbeddedDocumentOperator embeddedOperator =
        OperatorBuilder.embeddedDocument().path("teachers").operator(compoundOperator).build();

    // Initialize BuildState with paths mapped to facets and a GENERIC optimization status
    Map<String, List<String>> pathToFacetName =
        Map.of(
            "teachers.first", List.of("teachersFirstFacet"),
            "teachers.last", List.of("teachersLastFacet"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);
    state.optimizationStatus = DrillSidewaysInfo.QueryOptimizationStatus.GENERIC;

    // Execute the method under test
    processEmbeddedOperator(state, embeddedOperator, Optional.empty());

    // Assertions: Optimization status remains GENERIC, and the operators are properly processed
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC);
    assertThat(state.containsDoesNotAffect).isTrue();
  }

  @Test
  public void processEmbeddedOperator_minimumShouldMatchAdjustedForFacetOperators() {
    // Set up a compound operator with multiple should clauses affecting different facets
    CompoundOperator compoundOperator =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.range()
                    .path("products.color")
                    .numericBounds(
                        Optional.of(new LongPoint(1)), Optional.of(new LongPoint(2)), true, false)
                    .doesNotAffect("productColorsFacet")
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("products.size")
                    .numericBounds(
                        Optional.of(new LongPoint(3)), Optional.of(new LongPoint(4)), true, false)
                    .doesNotAffect("productSizesFacet")
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("products.weight")
                    .numericBounds(
                        Optional.of(new LongPoint(5)), Optional.of(new LongPoint(6)), true, false)
                    .doesNotAffect("productWeightsFacet")
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("products.dimension")
                    .numericBounds(
                        Optional.of(new LongPoint(7)), Optional.of(new LongPoint(8)), true, false)
                    .doesNotAffect("productDimensionsFacet")
                    .build())
            .minimumShouldMatch(3) // Original minimumShouldMatch set to 3
            .build();

    // Wrap the compound operator in an EmbeddedDocumentOperator
    EmbeddedDocumentOperator embeddedOperator =
        OperatorBuilder.embeddedDocument().path("products").operator(compoundOperator).build();

    // Initialize BuildState with paths mapped to facets
    Map<String, List<String>> pathToFacetName =
        Map.of(
            "products.color", List.of("productColorsFacet"),
            "products.size", List.of("productSizesFacet"),
            "products.weight", List.of("productWeightsFacet"),
            "products.dimension", List.of("productDimensionsFacet"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    // Execute the method under test
    processEmbeddedOperator(state, embeddedOperator, Optional.empty());
    // Optimization status changes to GENERIC
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC);
  }

  @Test
  public void verifyQueryOptimizable_allOperatorsExcludeOwnFacet_optimizable() {
    long value = 1;
    // Setting up operators that exclude only their own facet
    Operator operatorColor =
        OperatorBuilder.equals().path("color").value(value).doesNotAffect("colors").build();

    Operator operatorSize =
        OperatorBuilder.equals().path("size").value(value).doesNotAffect("sizes").build();

    DrillSidewaysInfoBuilder.BuildState state = new DrillSidewaysInfoBuilder.BuildState(Map.of());
    state.operatorsByFacet.put("colors", List.of(operatorColor));
    state.operatorsByFacet.put("sizes", List.of(operatorSize));

    state.optimizationStatus = DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE;

    verifyQueryOptimizable(state);

    // Optimization status remains OPTIMIZABLE
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);
  }

  @Test
  public void verifyQueryOptimizable_anyOperatorExcludesOtherFacet_Generic() {
    long value = 1;
    // Setting up an operator that excludes facets other than its own
    Operator operatorColor =
        OperatorBuilder.equals().path("color").value(value).doesNotAffect("sizes").build();

    Operator operatorSize =
        OperatorBuilder.equals().path("size").value(value).doesNotAffect("sizes").build();

    DrillSidewaysInfoBuilder.BuildState state = new DrillSidewaysInfoBuilder.BuildState(Map.of());
    state.operatorsByFacet.put("colors", List.of(operatorColor));
    state.operatorsByFacet.put("sizes", List.of(operatorSize));

    state.optimizationStatus = DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE;

    verifyQueryOptimizable(state);

    // Optimization status changes to GENERIC
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC);
  }

  @Test
  public void verifyQueryOptimizable_emptyOperatorsByFacet_Optimizable() {
    // Setting up the state with an empty operatorsByFacet map
    DrillSidewaysInfoBuilder.BuildState state = new DrillSidewaysInfoBuilder.BuildState(Map.of());
    state.optimizationStatus = DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE;

    verifyQueryOptimizable(state);

    // Optimization status remains OPTIMIZABLE
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);
  }

  @Test
  public void verifyQueryOptimizable_optimizationStatusAlreadyGeneric_Generic() {
    // Setup state with optimizationStatus already GENERIC
    DrillSidewaysInfoBuilder.BuildState state = new DrillSidewaysInfoBuilder.BuildState(Map.of());
    state.optimizationStatus = DrillSidewaysInfo.QueryOptimizationStatus.GENERIC;

    verifyQueryOptimizable(state);

    // OptimizationStatus doesn't change
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC);
  }

  @Test
  public void genericConditionsMet_doesNotAffectInNonOptimizableClause_genericWithExitEarly() {
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    CompoundOperator compoundOp =
        OperatorBuilder.compound()
            .mustNot(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .doesNotAffect("colors")
            .build();

    // Non-optimizable clause type (e.g., MUST_NOT)
    Optional<DrillSidewaysInfoBuilder.CompoundClauseType> clauseType =
        Optional.of(DrillSidewaysInfoBuilder.CompoundClauseType.MUST_NOT);

    DrillSidewaysInfoBuilder.BuildState state = new DrillSidewaysInfoBuilder.BuildState(Map.of());

    boolean isGeneric = genericConditionsMet(compoundOp, state, clauseType);

    assertThat(isGeneric).isTrue();
    assertThat(state.containsDoesNotAffect).isTrue();
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC);
  }

  @Test
  public void genericConditionsMet_doesNotAffectInOptimizableClause_optimizableWithNoEarlyExit() {
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    CompoundOperator compoundOp =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .doesNotAffect("colors")
            .build();

    // Optimizable clause type (e.g., FILTER)
    Optional<DrillSidewaysInfoBuilder.CompoundClauseType> clauseType =
        Optional.of(DrillSidewaysInfoBuilder.CompoundClauseType.FILTER);

    DrillSidewaysInfoBuilder.BuildState state = new DrillSidewaysInfoBuilder.BuildState(Map.of());

    boolean isGeneric = genericConditionsMet(compoundOp, state, clauseType);

    assertThat(isGeneric).isFalse();
    assertThat(state.containsDoesNotAffect).isTrue();
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);
  }

  @Test
  public void genericConditionsMet_noDoesNotAffectInNonOptimizableClause_optimizable() {
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    CompoundOperator compoundOp =
        OperatorBuilder.compound()
            .mustNot(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .build();

    // Non-optimizable clause type but no doesNotAffect defined
    Optional<DrillSidewaysInfoBuilder.CompoundClauseType> clauseType =
        Optional.of(DrillSidewaysInfoBuilder.CompoundClauseType.MUST_NOT);

    DrillSidewaysInfoBuilder.BuildState state = new DrillSidewaysInfoBuilder.BuildState(Map.of());

    boolean isGeneric = genericConditionsMet(compoundOp, state, clauseType);

    assertThat(isGeneric).isFalse();
    assertThat(state.containsDoesNotAffect).isFalse();
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);
  }

  @Test
  public void genericConditionsMet_noDoesNotAffectInOptimizableClause_optimizableWithNoEarlyExit() {
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    CompoundOperator compoundOp =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .build();

    // Optimizable clause type
    Optional<DrillSidewaysInfoBuilder.CompoundClauseType> clauseType =
        Optional.of(DrillSidewaysInfoBuilder.CompoundClauseType.FILTER);

    DrillSidewaysInfoBuilder.BuildState state = new DrillSidewaysInfoBuilder.BuildState(Map.of());

    boolean isGeneric = genericConditionsMet(compoundOp, state, clauseType);

    assertThat(isGeneric).isFalse();
    assertThat(state.containsDoesNotAffect).isFalse();
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);
  }

  @Test
  public void genericConditionsMet_withNullCompoundClauseType_optimizableWithNoEarlyExit() {
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    CompoundOperator compoundOp =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .doesNotAffect("colors")
            .build();

    // null/empty compoundClauseType
    Optional<DrillSidewaysInfoBuilder.CompoundClauseType> clauseType = Optional.empty();

    DrillSidewaysInfoBuilder.BuildState state = new DrillSidewaysInfoBuilder.BuildState(Map.of());

    boolean isGeneric = genericConditionsMet(compoundOp, state, clauseType);

    assertThat(isGeneric).isFalse();
    assertThat(state.containsDoesNotAffect).isTrue();
    assertThat(state.optimizationStatus)
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);
  }

  @Test
  public void addLeafOperatorToOperatorMap_singlePath() {
    long value = 1;
    // Set up an operator with a single path
    Operator operator = OperatorBuilder.equals().path("color").value(value).build();

    Map<String, List<String>> pathToFacetName = Map.of("color", List.of("colors"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    addLeafOperatorToOperatorMap(operator, state);

    assertThat(state.operatorsByFacet).containsKey("colors");
    assertThat(state.operatorsByFacet.get("colors")).containsExactly(operator);
  }

  @Test
  public void addLeafOperatorToOperatorMap_multiplePaths() {
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    // Set up an operator with multiple paths
    Operator operator =
        OperatorBuilder.range()
            .path("color")
            .path("size")
            .numericBounds(lower, upper, true, true)
            .build();

    Map<String, List<String>> pathToFacetName =
        Map.of(
            "color", List.of("colors"),
            "size", List.of("sizes"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    addLeafOperatorToOperatorMap(operator, state);

    assertThat(state.operatorsByFacet.containsKey("colors")).isTrue();
    assertThat(state.operatorsByFacet.containsKey("sizes")).isTrue();
    assertThat(state.operatorsByFacet.get("colors")).containsExactly(operator);
    assertThat(state.operatorsByFacet.get("sizes")).containsExactly(operator);
  }

  @Test
  public void addLeafOperatorToOperatorMap_InOperator_pathMappedToFacet_addsToOperatorsByFacet() {
    List<Long> values = Arrays.asList(1L, 2L);
    Operator operator = OperatorBuilder.in().path("color").longs(values).build();

    Map<String, List<String>> pathToFacetName = Map.of("color", List.of("colors"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    addLeafOperatorToOperatorMap(operator, state);

    assertThat(state.operatorsByFacet).containsKey("colors");
    assertThat(state.operatorsByFacet.get("colors")).containsExactly(operator);
  }

  @Test
  public void addLeafOperatorToOperatorMap_pathNotMappedToFacetName_operatorsByFacetEmpty() {
    long value = 1;
    // Set up an operator with a path that does not map to any facet name
    Operator operator = OperatorBuilder.equals().path("unknownPath").value(value).build();

    // Initialize state with pathToFacetName that doesn't include "unknownPath"
    Map<String, List<String>> pathToFacetName = Map.of("color", List.of("colors"));
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    addLeafOperatorToOperatorMap(operator, state);

    // Verify operatorsByFacet remains empty (no mapping for "unknownPath")
    assertThat(state.operatorsByFacet).isEmpty();
  }

  @Test
  public void buildPreFilter_noPrefilterClauses_returnsEmpty() {
    // Initialize state with no clauses
    Map<String, List<String>> pathToFacetName = new HashMap<>();
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);

    Optional<Operator> preFilter = buildPreFilter(state);

    assertThat(preFilter).isEmpty();
  }

  @Test
  public void buildPreFilter_prefilterMustClauses_returnsCompoundOperator() {
    // Create must clause operators and add them to BuildState
    RangeOperator mustClause =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(Optional.of(new LongPoint(1)), Optional.of(new LongPoint(3)), true, true)
            .build();

    Map<String, List<String>> pathToFacetName = new HashMap<>();
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);
    state.preFilterMustClauses.add(mustClause);

    Optional<Operator> preFilter = buildPreFilter(state);
    assertThat(preFilter).isPresent();
    assertThat(preFilter.get()).isInstanceOf(CompoundOperator.class);

    CompoundOperator compoundOperator = (CompoundOperator) preFilter.get();
    assertThat(compoundOperator.must().isEmpty()).isFalse();
    assertThat(compoundOperator.filter().isEmpty()).isTrue();
    assertThat(compoundOperator.mustNot().isEmpty()).isTrue();
    assertThat(compoundOperator.should().isEmpty()).isTrue();
  }

  @Test
  public void buildPreFilter_allClauseTypesPopulated_returnsCompoundOperator() {
    // Create multiple range operators for different clause types
    RangeOperator mustClause =
        OperatorBuilder.range()
            .path("color")
            .numericBounds(Optional.of(new LongPoint(1)), Optional.of(new LongPoint(3)), true, true)
            .build();

    RangeOperator shouldClause =
        OperatorBuilder.range()
            .path("size")
            .numericBounds(Optional.of(new LongPoint(5)), Optional.of(new LongPoint(6)), true, true)
            .build();

    RangeOperator filterClause =
        OperatorBuilder.range()
            .path("size")
            .numericBounds(Optional.of(new LongPoint(3)), Optional.of(new LongPoint(4)), true, true)
            .build();

    RangeOperator mustNotClause =
        OperatorBuilder.range()
            .path("size")
            .numericBounds(Optional.of(new LongPoint(7)), Optional.of(new LongPoint(8)), true, true)
            .build();

    Map<String, List<String>> pathToFacetName = new HashMap<>();
    DrillSidewaysInfoBuilder.BuildState state =
        new DrillSidewaysInfoBuilder.BuildState(pathToFacetName);
    state.preFilterMustClauses.add(mustClause);
    state.preFilterShouldClauses.add(shouldClause);
    state.preFilterFilterClauses.add(filterClause);
    state.preFilterMustNotClauses.add(mustNotClause);

    Optional<Operator> preFilter = buildPreFilter(state);

    assertThat(preFilter).isPresent();
    assertThat(preFilter.get()).isInstanceOf(CompoundOperator.class);

    CompoundOperator compoundOperator = (CompoundOperator) preFilter.get();

    assertThat(compoundOperator.must().isEmpty()).isFalse();
    assertThat(compoundOperator.should().isEmpty()).isFalse();
    assertThat(compoundOperator.filter().isEmpty()).isFalse();
    assertThat(compoundOperator.mustNot().isEmpty()).isFalse();
  }

  @Test
  public void trackDoesNotAffectInParentOperators_addNewFields() {
    CompoundOperator compoundOp =
        OperatorBuilder.compound().doesNotAffect(List.of("color", "size")).build();

    DrillSidewaysInfoBuilder.BuildState state = new DrillSidewaysInfoBuilder.BuildState(Map.of());
    state.facetNamesFromParentDoesNotAffect = Optional.of(List.of("shape"));

    trackDoesNotAffectInParentOperators(compoundOp, state);

    List<String> expected = List.of("shape", "color", "size");
    List<String> actual = state.facetNamesFromParentDoesNotAffect.orElse(Collections.emptyList());

    // Assert lists are equal
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void buildFacetOperators_noDoesNotAffectShouldClause_nonDrillSideways() {
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(5L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(6L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower2, upper2, true, false)
                    .build())
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.NON_DRILL_SIDEWAYS)
        .isEqualTo(info.optimizationStatus());

    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).isEmpty();
  }

  @Test
  public void buildFacetOperators_duplicatePath_shouldHandleGracefullyForNonDrillSideways() {
    // Define numeric bounds for the ranges
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(3L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(4L));
    Optional<NumericPoint> lower3 = Optional.of(new LongPoint(6L));
    Optional<NumericPoint> upper3 = Optional.of(new LongPoint(7L));

    // Define the compound operator
    CompoundOperator operator =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .filter(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower2, upper2, true, false)
                    .build())
            .must(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower3, upper3, true, false)
                    .build())
            .build();

    // Define facet definitions
    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitionsWithDuplicates();

    // Build DrillSidewaysInfo
    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Verify that the optimization status is NON_DRILL_SIDEWAYS
    assertThat(info.optimizationStatus())
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.NON_DRILL_SIDEWAYS);

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).isEmpty();
  }

  @Test
  public void buildFacetOperators_operatorsExcludeOnlyOwnFacet_optimizable() {
    //  Requested query:
    //  {
    //      "compoundOperator": {
    //      "filter": [
    //      {
    //        "range": {
    //        "path": "color",
    //            "numericBounds": {
    //          "lower": 1,
    //              "upper": 2,
    //              "includeLower": true,
    //              "includeUpper": false
    //        },
    //        "doesNotAffect": "colors"
    //      }
    //      },
    //      {
    //        "range": {
    //        "path": "color",
    //            "numericBounds": {
    //          "lower": 3,
    //              "upper": 4,
    //              "includeLower": true,
    //              "includeUpper": false
    //        },
    //        "doesNotAffect": "colors"
    //      }
    //      }
    //    ],
    //      "must": [
    //      {
    //        "range": {
    //        "path": "size",
    //            "numericBounds": {
    //          "lower": 6,
    //              "upper": 7,
    //              "includeLower": true,
    //              "includeUpper": false
    //        },
    //        "doesNotAffect": "sizes"
    //      }
    //      }
    //    ]
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(3L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(4L));
    Optional<NumericPoint> lower3 = Optional.of(new LongPoint(6L));
    Optional<NumericPoint> upper3 = Optional.of(new LongPoint(7L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .doesNotAffect("colors")
                    .build())
            .filter(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower2, upper2, true, false)
                    .doesNotAffect("colors")
                    .build())
            .must(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower3, upper3, true, false)
                    .doesNotAffect("sizes")
                    .build())
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("colors");
    assertThat(facetOperators).containsKey("sizes");

    // Validate "colors" facet operator (wrapped via SHOULD clauses into single CompoundOperator)
    CompoundOperator colorsOperator =
        (CompoundOperator)
            facetOperators
                .get("colors")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: colors"));
    List<Operator> shouldOperators =
        colorsOperator.should().operators().stream().map(op -> (Operator) op).toList();
    assertThat(shouldOperators).hasSize(2);
    assertRangeOperator(shouldOperators.get(0), "color", 1L, 2L);
    assertRangeOperator(shouldOperators.get(1), "color", 3L, 4L);

    // Validate "sizes" facet operator (a RangeOperator)
    RangeOperator sizesOperator =
        (RangeOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));

    assertRangeOperator(sizesOperator, "size", 6L, 7L);
  }

  @Test
  public void buildFacetOperators_embeddedOperator_optimizable() {
    //  Facet Operator map:
    //  {
    //      "embeddedDocument": {
    //      "path": "teachers",
    //          "operator": {
    //        "compound": {
    //          "must": [
    //          {
    //            "equals": {
    //            "path": "teachers.first",
    //                "value": "John",
    //                "doesNotAffect": ["teachersFirstNameFacet"]
    //          }
    //          },
    //          {
    //            "equals": {
    //            "path": "teachers.last",
    //                "value": "Smith",
    //                "doesNotAffect": ["teachersLastNameFacet"]
    //          }
    //          }
    //        ]
    //        }
    //      }
    //    },
    //      "facets": {
    //      "teachersFirstNameFacet": {
    //        "type": "string",
    //            "path": "teachers.first",
    //            "numBuckets": 5
    //      },
    //      "teachersLastNameFacet": {
    //        "type": "string",
    //            "path": "teachers.last",
    //            "numBuckets": 5
    //      }
    //    }
    //    }

    CompoundOperator compoundOp =
        OperatorBuilder.compound()
            .must(
                OperatorBuilder.equals()
                    .path("teachers.first")
                    .value("John")
                    .doesNotAffect("teachersFirstNameFacet")
                    .build())
            .must(
                OperatorBuilder.equals()
                    .path("teachers.last")
                    .value("Smith")
                    .doesNotAffect("teachersLastNameFacet")
                    .build())
            .build();

    EmbeddedDocumentOperator operator =
        OperatorBuilder.embeddedDocument().path("teachers").operator(compoundOp).build();

    // Get facet definitions using the new method
    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitionsEmbedded();

    // Build DrillSidewaysInfo using buildFacetOperators method
    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Validate optimization status
    assertThat(info.optimizationStatus())
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);

    // Extract facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("teachersFirstNameFacet");
    assertThat(facetOperators).containsKey("teachersLastNameFacet");

    // Validate "teachersFirstNameFacet" operator
    Operator firstNameOperator =
        facetOperators
            .get("teachersFirstNameFacet")
            .orElseThrow(() -> new IllegalStateException("No operator present for facet."));
    assertThat(firstNameOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    EmbeddedDocumentOperator firstNameEmbeddedOp = (EmbeddedDocumentOperator) firstNameOperator;
    assertThat(firstNameEmbeddedOp.path().toString()).isEqualTo("teachers");

    assertThat(firstNameEmbeddedOp.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator firstNameCompoundOp = (CompoundOperator) firstNameEmbeddedOp.operator();
    assertThat(firstNameCompoundOp.filter().isPresent()).isTrue();

    List<? extends Operator> firstNameFilterClauses = firstNameCompoundOp.filter().operators();
    assertThat(firstNameFilterClauses).hasSize(1);
    assertThat(firstNameFilterClauses.get(0)).isInstanceOf(EqualsOperator.class);
    EqualsOperator firstNameClause = (EqualsOperator) firstNameFilterClauses.get(0);
    assertThat(firstNameClause.path().toString()).isEqualTo("teachers.first");
    assertThat(firstNameClause.value()).isEqualTo(ValueBuilder.string("John"));
    assertThat(firstNameClause.doesNotAffect().get()).contains("teachersFirstNameFacet");

    // Validate "teachersLastNameFacet" operator
    Operator lastNameOperator =
        facetOperators
            .get("teachersLastNameFacet")
            .orElseThrow(() -> new IllegalStateException("No operator present for facet."));
    assertThat(lastNameOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    EmbeddedDocumentOperator lastNameEmbeddedOp = (EmbeddedDocumentOperator) lastNameOperator;
    assertThat(lastNameEmbeddedOp.path().toString()).isEqualTo("teachers");

    assertThat(lastNameEmbeddedOp.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator lastNameCompoundOp = (CompoundOperator) lastNameEmbeddedOp.operator();
    assertThat(lastNameCompoundOp.filter().isPresent()).isTrue();

    List<? extends Operator> lastNameFilterClauses = lastNameCompoundOp.filter().operators();
    assertThat(lastNameFilterClauses).hasSize(1);
    assertThat(lastNameFilterClauses.get(0)).isInstanceOf(EqualsOperator.class);
    EqualsOperator lastNameClause = (EqualsOperator) lastNameFilterClauses.get(0);
    assertThat(lastNameClause.path().toString()).isEqualTo("teachers.last");
    assertThat(lastNameClause.value()).isEqualTo(ValueBuilder.string("Smith"));
    assertThat(lastNameClause.doesNotAffect().get()).contains("teachersLastNameFacet");
  }

  @Test
  public void buildFacetOperators_embeddedOperatorWithMustShouldClausesAndPreFilter_optimizable() {
    //  Facet Operator map:
    //  {
    //      "embeddedDocument": {
    //      "path": "teachers",
    //          "operator": {
    //        "compound": {
    //          "must": [
    //          {
    //            "equals": {
    //            "path": "teachers.first",
    //                "value": "John",
    //                "doesNotAffect": ["teachersFirstNameFacet"]
    //          }
    //          }
    //        ],
    //         "should" : [
    //         {
    //            "equals": {
    //            "path": "teachers.last",
    //                "value": "Smith",
    //          }
    //          }
    //        ]
    //        }
    //      }
    //    },
    //      "facets": {
    //      "teachersFirstNameFacet": {
    //        "type": "string",
    //            "path": "teachers.first",
    //            "numBuckets": 5
    //      },
    //      "teachersLastNameFacet": {
    //        "type": "string",
    //            "path": "teachers.last",
    //            "numBuckets": 5
    //      }
    //    }
    //    }

    CompoundOperator compoundOp =
        OperatorBuilder.compound()
            .must(
                OperatorBuilder.equals()
                    .path("teachers.first")
                    .value("John")
                    .doesNotAffect("teachersFirstNameFacet")
                    .build())
            .should(OperatorBuilder.equals().path("teachers.last").value("Smith").build())
            .build();

    EmbeddedDocumentOperator operator =
        OperatorBuilder.embeddedDocument().path("teachers").operator(compoundOp).build();

    // Get facet definitions using the new method
    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitionsEmbedded();

    // Build DrillSidewaysInfo using buildFacetOperators method
    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Validate optimization status
    assertThat(info.optimizationStatus())
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);

    // Extract facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("teachersFirstNameFacet");

    // Validate "teachersFirstNameFacet" operator
    Operator firstNameOperator =
        facetOperators
            .get("teachersFirstNameFacet")
            .orElseThrow(() -> new IllegalStateException("No operator present for facet."));
    assertThat(firstNameOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    EmbeddedDocumentOperator firstNameEmbeddedOp = (EmbeddedDocumentOperator) firstNameOperator;
    assertThat(firstNameEmbeddedOp.path().toString()).isEqualTo("teachers");

    assertThat(firstNameEmbeddedOp.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator firstNameCompoundOp = (CompoundOperator) firstNameEmbeddedOp.operator();
    assertThat(firstNameCompoundOp.filter().isPresent()).isTrue();

    List<? extends Operator> firstNameFilterClauses = firstNameCompoundOp.filter().operators();
    assertThat(firstNameFilterClauses).hasSize(1);
    assertThat(firstNameFilterClauses.get(0)).isInstanceOf(EqualsOperator.class);
    EqualsOperator firstNameClause = (EqualsOperator) firstNameFilterClauses.get(0);
    assertThat(firstNameClause.path().toString()).isEqualTo("teachers.first");
    assertThat(firstNameClause.value()).isEqualTo(ValueBuilder.string("John"));
    assertThat(firstNameClause.doesNotAffect().get()).contains("teachersFirstNameFacet");

    // Validate pre-filter operator
    Optional<Operator> preFilterClauses = info.preFilter();
    assertThat(preFilterClauses).isPresent();

    Operator preFilterShouldOperator = preFilterClauses.get();
    assertThat(preFilterShouldOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    // Unwrap the embedded operator to access the underlying CompoundOperator
    EmbeddedDocumentOperator preFilterEmbeddedOp =
        (EmbeddedDocumentOperator) preFilterShouldOperator;
    assertThat(preFilterEmbeddedOp.path().toString()).isEqualTo("teachers");

    assertThat(preFilterEmbeddedOp.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator preFilterCompoundOp = (CompoundOperator) preFilterEmbeddedOp.operator();
    assertThat(preFilterCompoundOp.should().isPresent()).isTrue();

    List<? extends Operator> preFilterShouldClausesList = preFilterCompoundOp.should().operators();
    assertThat(preFilterShouldClausesList).hasSize(1);

    EqualsOperator preFilterShouldClause = (EqualsOperator) preFilterShouldClausesList.get(0);
    assertThat(preFilterShouldClause.path().toString()).isEqualTo("teachers.last");
    assertThat(preFilterShouldClause.value()).isEqualTo(ValueBuilder.string("Smith"));
    // No doesNotAffect present for pre-filter operator
    assertThat(preFilterShouldClause.doesNotAffect()).isEmpty();
  }

  @Test
  public void buildFacetOperators_embeddedOperatorWithMustClausesAndPreFilter_optimizable() {
    // Set up a CompoundOperator with must clauses, one of which does not include "doesNotAffect"
    CompoundOperator compoundOp =
        OperatorBuilder.compound()
            .must(
                OperatorBuilder.equals()
                    .path("teachers.first")
                    .value("John")
                    .doesNotAffect("teachersFirstNameFacet")
                    .build())
            .must(
                OperatorBuilder.equals()
                    .path("teachers.last")
                    .value("Smith")
                    .doesNotAffect("teachersLastNameFacet")
                    .build())
            .must(
                OperatorBuilder.equals() // Operator without doesNotAffect
                    .path("teachers.middle")
                    .value("Alex")
                    .build())
            .build();

    // Wrap the CompoundOperator in an EmbeddedDocumentOperator
    EmbeddedDocumentOperator operator =
        OperatorBuilder.embeddedDocument().path("teachers").operator(compoundOp).build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitionsEmbedded();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Validate optimization status
    assertThat(info.optimizationStatus())
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);

    // Validate facet operators for "teachersFirstNameFacet"
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("teachersFirstNameFacet");
    assertThat(facetOperators).containsKey("teachersLastNameFacet");

    Operator firstNameOperator =
        facetOperators
            .get("teachersFirstNameFacet")
            .orElseThrow(() -> new IllegalStateException("No operator present for facet."));
    assertThat(firstNameOperator).isInstanceOf(EmbeddedDocumentOperator.class);
    EmbeddedDocumentOperator firstNameEmbeddedOp = (EmbeddedDocumentOperator) firstNameOperator;

    assertThat(firstNameEmbeddedOp.path().toString()).isEqualTo("teachers");
    assertThat(firstNameEmbeddedOp.operator()).isInstanceOf(CompoundOperator.class);

    CompoundOperator firstNameCompoundOp = (CompoundOperator) firstNameEmbeddedOp.operator();
    assertThat(firstNameCompoundOp.filter().isPresent()).isTrue();

    List<? extends Operator> firstNameFilterClauses = firstNameCompoundOp.filter().operators();
    assertThat(firstNameFilterClauses).hasSize(1);
    assertThat(firstNameFilterClauses.get(0)).isInstanceOf(EqualsOperator.class);

    EqualsOperator firstNameClause = (EqualsOperator) firstNameFilterClauses.get(0);
    assertThat(firstNameClause.path().toString()).isEqualTo("teachers.first");
    assertThat(firstNameClause.value()).isEqualTo(ValueBuilder.string("John"));
    assertThat(firstNameClause.doesNotAffect().get()).contains("teachersFirstNameFacet");

    // Validate facet operators for "teachersLastNameFacet"
    Operator lastNameOperator =
        facetOperators
            .get("teachersLastNameFacet")
            .orElseThrow(() -> new IllegalStateException("No operator present for facet."));
    assertThat(lastNameOperator).isInstanceOf(EmbeddedDocumentOperator.class);
    EmbeddedDocumentOperator lastNameEmbeddedOp = (EmbeddedDocumentOperator) lastNameOperator;

    assertThat(lastNameEmbeddedOp.path().toString()).isEqualTo("teachers");
    assertThat(lastNameEmbeddedOp.operator()).isInstanceOf(CompoundOperator.class);

    CompoundOperator lastNameCompoundOp = (CompoundOperator) lastNameEmbeddedOp.operator();
    assertThat(lastNameCompoundOp.filter().isPresent()).isTrue();

    List<? extends Operator> lastNameFilterClauses = lastNameCompoundOp.filter().operators();
    assertThat(lastNameFilterClauses).hasSize(1);
    assertThat(lastNameFilterClauses.get(0)).isInstanceOf(EqualsOperator.class);

    EqualsOperator lastNameClause = (EqualsOperator) lastNameFilterClauses.get(0);
    assertThat(lastNameClause.path().toString()).isEqualTo("teachers.last");
    assertThat(lastNameClause.value()).isEqualTo(ValueBuilder.string("Smith"));
    assertThat(lastNameClause.doesNotAffect().get()).contains("teachersLastNameFacet");

    // Validate pre-filter operator
    Optional<Operator> preFilterMustClauses = info.preFilter();
    assertThat(preFilterMustClauses).isPresent();

    Operator preFilterMustOperator = preFilterMustClauses.get();
    assertThat(preFilterMustOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    // Unwrap the EmbeddedDocumentOperator
    EmbeddedDocumentOperator preFilterEmbeddedOp = (EmbeddedDocumentOperator) preFilterMustOperator;
    assertThat(preFilterEmbeddedOp.path().toString()).isEqualTo("teachers");

    assertThat(preFilterEmbeddedOp.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator preFilterCompoundOp = (CompoundOperator) preFilterEmbeddedOp.operator();
    assertThat(preFilterCompoundOp.must().isPresent()).isTrue();

    // Validate pre-filter MUST clauses
    List<? extends Operator> preFilterMustClausesList = preFilterCompoundOp.must().operators();
    assertThat(preFilterMustClausesList).hasSize(1);

    EqualsOperator preFilterMustClause = (EqualsOperator) preFilterMustClausesList.get(0);
    assertThat(preFilterMustClause.path().toString()).isEqualTo("teachers.middle");
    assertThat(preFilterMustClause.value()).isEqualTo(ValueBuilder.string("Alex"));
    // No "doesNotAffect" present for pre-filter operator
    assertThat(preFilterMustClause.doesNotAffect()).isEmpty();
  }

  @Test
  public void buildFacetOperators_embeddedCompoundWithPreFilter_optimizable() {
    //  Requested query:
    //  {
    //      "embeddedDocumentOperator": {
    //      "path": "teachers",
    //          "operator": {
    //        "compoundOperator": {
    //          "must": [
    //          {
    //            "equals": {
    //            "path": "teachers.first",
    //                "value": "John",
    //                "doesNotAffect": "teachersFirstNameFacet"
    //          }
    //          },
    //          {
    //            "equals": {
    //            "path": "teachers.subject",
    //                "value": "Calculus"
    //          }
    //          }
    //        ],
    //          "filter": [
    //          {
    //            "equals": {
    //            "path": "teachers.last",
    //                "value": "Smith",
    //                "doesNotAffect": "teachersLastNameFacet"
    //          }
    //          },
    //          {
    //            "equals": {
    //            "path": "teachers.grade",
    //                "value": "ninth"
    //          }
    //          }
    //        ],
    //          "mustNot": [
    //          {
    //            "range": {
    //            "path": "teachers.salary",
    //                "numericBounds": {
    //              "lower": 1,
    //                  "upper": 2,
    //                  "includeLower": true,
    //                  "includeUpper": false
    //            }
    //          }
    //          }
    //        ],
    //          "should": [
    //          {
    //            "text": {
    //            "path": "teachers.skills",
    //                "query": "advanced mathematics"
    //          }
    //          },
    //          {
    //            "text": {
    //            "path": "teachers.skills",
    //                "query": "physics"
    //          }
    //          }
    //        ]
    //        }
    //      }
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    CompoundOperator compoundOp =
        OperatorBuilder.compound()
            .must(
                OperatorBuilder.equals() // MUST clause operator for "teachers.first"
                    .path("teachers.first")
                    .value("John")
                    .doesNotAffect("teachersFirstNameFacet")
                    .build())
            .filter(
                OperatorBuilder.equals() // FILTER clause operator for "teachers.last"
                    .path("teachers.last")
                    .value("Smith")
                    .doesNotAffect("teachersLastNameFacet")
                    .build())
            .must(
                OperatorBuilder.equals() // MUST clause operator for "teachers.subject"
                    .path("teachers.subject")
                    .value("Calculus")
                    .build())
            .filter(
                OperatorBuilder.equals() // FILTER clause operator for "teachers.grade"
                    .path("teachers.grade")
                    .value("ninth")
                    .build())
            .mustNot(
                OperatorBuilder.range() // MUST_NOT clause operator for "teachers.salary"
                    .path("teachers.salary")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .should(
                OperatorBuilder.text() // SHOULD clause operator for "teachers.skills"
                    .path("teachers.skills")
                    .query("advanced mathematics") // First SHOULD clause
                    .build())
            .should(
                OperatorBuilder.text()
                    .path("teachers.skills")
                    .query("physics") // Second SHOULD clause
                    .build())
            .build();

    // Wrap the CompoundOperator inside an EmbeddedDocumentOperator for the "teachers" path
    EmbeddedDocumentOperator embeddedOp =
        OperatorBuilder.embeddedDocument().path("teachers").operator(compoundOp).build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitionsEmbedded();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(embeddedOp, facetDefinitions);

    // Validate optimization status
    assertThat(info.optimizationStatus())
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators.keySet()).contains("teachersFirstNameFacet");

    // Validate "teachersFirstNameFacet" operator
    Operator firstNameOperator =
        facetOperators
            .get("teachersFirstNameFacet")
            .orElseThrow(
                () -> new IllegalStateException("No operator present for teachersFirstNameFacet."));
    assertThat(firstNameOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    EmbeddedDocumentOperator firstNameEmbeddedOp = (EmbeddedDocumentOperator) firstNameOperator;
    assertThat(firstNameEmbeddedOp.path().toString()).isEqualTo("teachers");

    assertThat(firstNameEmbeddedOp.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator firstNameCompoundOp = (CompoundOperator) firstNameEmbeddedOp.operator();
    assertThat(firstNameCompoundOp.filter().isPresent()).isTrue();

    List<? extends Operator> firstNameFilterClauses = firstNameCompoundOp.filter().operators();
    assertThat(firstNameFilterClauses).hasSize(1); // Validate one clause in FILTER
    assertThat(firstNameFilterClauses.get(0)).isInstanceOf(EqualsOperator.class);

    EqualsOperator firstNameClause = (EqualsOperator) firstNameFilterClauses.get(0);
    assertThat(firstNameClause.path().toString()).isEqualTo("teachers.first");
    assertThat(firstNameClause.value()).isEqualTo(ValueBuilder.string("John"));
    assertThat(firstNameClause.doesNotAffect().get()).contains("teachersFirstNameFacet");

    // Validate "teachersLastNameFacet" operator
    Operator lastNameOperator =
        facetOperators
            .get("teachersLastNameFacet")
            .orElseThrow(
                () -> new IllegalStateException("No operator present for teachersLastNameFacet."));
    assertThat(lastNameOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    EmbeddedDocumentOperator lastNameEmbeddedOp = (EmbeddedDocumentOperator) lastNameOperator;
    assertThat(lastNameEmbeddedOp.path().toString()).isEqualTo("teachers");

    assertThat(lastNameEmbeddedOp.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator lastNameCompoundOp = (CompoundOperator) lastNameEmbeddedOp.operator();
    assertThat(lastNameCompoundOp.filter().isPresent()).isTrue();

    List<? extends Operator> lastNameFilterClauses = lastNameCompoundOp.filter().operators();
    assertThat(lastNameFilterClauses).hasSize(1); // Validate one clause in FILTER
    assertThat(lastNameFilterClauses.get(0)).isInstanceOf(EqualsOperator.class);

    EqualsOperator lastNameClause = (EqualsOperator) lastNameFilterClauses.get(0);
    assertThat(lastNameClause.path().toString()).isEqualTo("teachers.last");
    assertThat(lastNameClause.value()).isEqualTo(ValueBuilder.string("Smith"));
    assertThat(lastNameClause.doesNotAffect().get()).contains("teachersLastNameFacet");

    // Validate pre-filter operator
    Optional<Operator> preFilterClauses = info.preFilter();
    assertThat(preFilterClauses).isPresent();

    Operator preFilterOperator = preFilterClauses.get();
    assertThat(preFilterOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    // Unwrap the EmbeddedDocumentOperator pre-filter
    EmbeddedDocumentOperator preFilterEmbeddedDocument =
        (EmbeddedDocumentOperator) preFilterOperator;
    assertThat(preFilterEmbeddedDocument.path().toString()).isEqualTo("teachers");

    assertThat(preFilterEmbeddedDocument.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator preFilterCompound = (CompoundOperator) preFilterEmbeddedDocument.operator();

    // Validate MUST clauses in pre-filter
    assertThat(preFilterCompound.must().isPresent()).isTrue();
    List<? extends Operator> preFilterMustClausesList = preFilterCompound.must().operators();
    assertThat(preFilterMustClausesList).hasSize(1);

    EqualsOperator preFilterMustClause = (EqualsOperator) preFilterMustClausesList.get(0);
    assertThat(preFilterMustClause.path().toString()).isEqualTo("teachers.subject");
    assertThat(preFilterMustClause.value()).isEqualTo(ValueBuilder.string("Calculus"));

    // Validate FILTER clauses in pre-filter
    assertThat(preFilterCompound.filter().isPresent()).isTrue();
    List<? extends Operator> preFilterFilterClausesList = preFilterCompound.filter().operators();
    assertThat(preFilterMustClausesList).hasSize(1);

    EqualsOperator preFilterFilterClause = (EqualsOperator) preFilterFilterClausesList.get(0);
    assertThat(preFilterFilterClause.path().toString()).isEqualTo("teachers.grade");
    assertThat(preFilterFilterClause.value()).isEqualTo(ValueBuilder.string("ninth"));

    // Validate MUST_NOT clauses in pre-filter
    assertThat(preFilterCompound.mustNot().isPresent()).isTrue();
    List<? extends Operator> preFilterMustNotClausesList = preFilterCompound.mustNot().operators();
    assertThat(preFilterMustNotClausesList).hasSize(1);

    RangeOperator preFilterMustNotClause = (RangeOperator) preFilterMustNotClausesList.get(0);
    assertThat(preFilterMustNotClause.paths().toString()).contains("teachers.salary");

    // Validate SHOULD clauses in pre-filter
    assertThat(preFilterCompound.should().isPresent()).isTrue();
    List<? extends Operator> preFilterShouldClausesList = preFilterCompound.should().operators();
    assertThat(preFilterShouldClausesList).hasSize(2);

    TextOperator preFilterShouldClause1 = (TextOperator) preFilterShouldClausesList.get(0);
    assertThat(preFilterShouldClause1.paths().toString()).contains("teachers.skills");
    assertThat(preFilterShouldClause1.query()).contains("advanced mathematics");

    TextOperator preFilterShouldClause2 = (TextOperator) preFilterShouldClausesList.get(1);
    assertThat(preFilterShouldClause2.paths().toString()).contains("teachers.skills");
    assertThat(preFilterShouldClause2.query()).contains("physics");
  }

  @Test
  public void buildFacetOperators_deeplyNestedEmbeddedCompoundWithPreFilters_structureValidation() {
    //  Requested query:
    //  {
    //      "embeddedDocument": {
    //      "path": "teachers.outer",
    //          "operator": {
    //        "compound": {
    //          "must": [
    //          {
    //            "embeddedDocument": {
    //            "path": "teachers.inner",
    //                "operator": {
    //              "compound": {
    //                "must": [
    //                {
    //                  "equals": {
    //                  "path": "teachers.first",
    //                      "value": "John",
    //                      "doesNotAffect": ["teachersFirstNameFacet"]
    //                }
    //                }
    //                  ],
    //                "filter": [
    //                {
    //                  "range": {
    //                  "path": "teachers.age",
    //                      "gte": 1,
    //                      "lte": 2,
    //                      "includeLower": true,
    //                      "includeUpper": true
    //                }
    //                }
    //                  ],
    //                "mustNot": [
    //                {
    //                  "exists": {
    //                  "path": "teachers.salary"
    //                }
    //                }
    //                  ],
    //                "should": [
    //                {
    //                  "text": {
    //                  "path": "teachers.notes",
    //                      "query": "certified"
    //                }
    //                },
    //                {
    //                  "text": {
    //                  "path": "teachers.skills",
    //                      "query": "physics"
    //                }
    //                }
    //                  ]
    //              }
    //            }
    //          }
    //          }
    //        ],
    //          "filter": [
    //          {
    //            "equals": {
    //            "path": "teachers.last",
    //                "value": "Smith",
    //                "doesNotAffect": ["teachersLastNameFacet"]
    //          }
    //          }
    //        ],
    //          "should": [
    //          {
    //            "equals": {
    //            "path": "teachers.city",
    //                "value": "New York"
    //          }
    //          }
    //        ],
    //          "mustNot": [
    //          {
    //            "range": {
    //            "path": "teachers.experience.years",
    //                "gte": 1,
    //                "lte": 2,
    //                "includeLower": true,
    //                "includeUpper": false
    //          }
    //          }
    //        ]
    //        }
    //      }
    //    }
    //    }

    // Pre-filter
    //    {
    //      "embeddedDocument": {
    //      "path": "teachers.outer",
    //          "operator": {
    //        "compound": {
    //          "must": [
    //          {
    //            "embeddedDocument": {
    //            "path": "teachers.inner",
    //                "operator": {
    //              "compound": {
    //                "filter": [
    //                {
    //                  "range": {
    //                  "path": "teachers.age",
    //                      "gte": 1,
    //                      "lte": 2,
    //                      "includeLower": true,
    //                      "includeUpper": true
    //                }
    //                }
    //                      ],
    //                "mustNot": [
    //                {
    //                  "exists": {
    //                  "path": "teachers.salary"
    //                }
    //                }
    //                      ],
    //                "should": [
    //                {
    //                  "text": {
    //                  "path": "teachers.notes",
    //                      "query": "certified"
    //                }
    //                },
    //                {
    //                  "text": {
    //                  "path": "teachers.skills",
    //                      "query": "physics"
    //                }
    //                }
    //                      ]
    //              }
    //            }
    //          }
    //          }
    //            ],
    //          "should": [
    //          {
    //            "equals": {
    //            "path": "teachers.city",
    //                "value": "New York"
    //          }
    //          }
    //            ],
    //          "mustNot": [
    //          {
    //            "range": {
    //            "path": "teachers.experience.years",
    //                "gte": 1,
    //                "lte": 2,
    //                "includeLower": true,
    //                "includeUpper": false
    //          }
    //          }
    //            ]
    //        }
    //      }
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    // Define the innermost CompoundOperator (inside deepest EmbeddedDocumentOperator)
    CompoundOperator innerCompoundOp =
        OperatorBuilder.compound()
            .must(
                OperatorBuilder.equals()
                    .path("teachers.first")
                    .value("John")
                    .doesNotAffect("teachersFirstNameFacet")
                    .build()) // MUST clause operator
            .filter(
                OperatorBuilder.range()
                    .path("teachers.age")
                    .numericBounds(lower, upper, true, true)
                    .build()) // FILTER clause operator
            .mustNot(
                OperatorBuilder.exists()
                    .path("teachers.salary")
                    .build()) // MUST_NOT clause operator
            .should(
                OperatorBuilder.text()
                    .path("teachers.notes")
                    .query("certified")
                    .build()) // First SHOULD clause
            .should(
                OperatorBuilder.text()
                    .path("teachers.skills")
                    .query("physics") // Second SHOULD clause
                    .build())
            .build();

    // Wrap the innermost CompoundOperator inside an EmbeddedDocumentOperator
    EmbeddedDocumentOperator innerEmbeddedOp =
        OperatorBuilder.embeddedDocument().path("teachers.inner").operator(innerCompoundOp).build();

    // Define the outer CompoundOperator
    CompoundOperator outerCompoundOp =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.equals()
                    .path("teachers.last")
                    .value("Smith")
                    .doesNotAffect("teachersLastNameFacet")
                    .build())
            .should(OperatorBuilder.equals().path("teachers.city").value("New York").build())
            .mustNot(
                OperatorBuilder.range()
                    .path("teachers.experience.years")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .must(innerEmbeddedOp)
            .build();

    // Wrap the outer compound operator inside an outer EmbeddedDocumentOperator
    EmbeddedDocumentOperator outerEmbeddedOp =
        OperatorBuilder.embeddedDocument().path("teachers.outer").operator(outerCompoundOp).build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitionsEmbedded();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(outerEmbeddedOp, facetDefinitions);

    // Validate optimization status
    assertThat(info.optimizationStatus())
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);

    // Validate facet operators for teachersFirstNameFacet and teachersLastNameFacet
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators.keySet()).contains("teachersFirstNameFacet");
    assertThat(facetOperators.keySet()).contains("teachersLastNameFacet");

    // Validate "teachersFirstNameFacet" operator
    Operator firstNameOperator =
        facetOperators
            .get("teachersFirstNameFacet")
            .orElseThrow(
                () -> new IllegalStateException("No operator present for teachersFirstNameFacet."));
    assertThat(firstNameOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    EmbeddedDocumentOperator firstNameEmbeddedOp = (EmbeddedDocumentOperator) firstNameOperator;
    assertThat(firstNameEmbeddedOp.path().toString()).isEqualTo("teachers.outer");

    assertThat(firstNameEmbeddedOp.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator firstNameCompoundOp = (CompoundOperator) firstNameEmbeddedOp.operator();
    assertThat(firstNameCompoundOp.filter().isPresent()).isTrue();

    List<? extends Operator> firstNameFilterClauses = firstNameCompoundOp.filter().operators();
    assertThat(firstNameFilterClauses).hasSize(1); // Validate one clause in FILTER
    assertThat(firstNameFilterClauses.get(0)).isInstanceOf(EmbeddedDocumentOperator.class);

    EmbeddedDocumentOperator firstNameClause =
        (EmbeddedDocumentOperator) firstNameFilterClauses.get(0);
    assertThat(firstNameClause.path().toString()).isEqualTo("teachers.inner");
    assertThat(firstNameClause.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator firstNameCompoundOperator = (CompoundOperator) firstNameClause.operator();
    assertThat(firstNameCompoundOperator.filter().isPresent()).isTrue();
    List<? extends Operator> firstNameFilterClauses2 =
        firstNameCompoundOperator.filter().operators();
    assertThat(firstNameFilterClauses2).hasSize(1);
    assertThat(firstNameFilterClauses2.get(0)).isInstanceOf(EqualsOperator.class);
    EqualsOperator firstNameEqualsOperator = (EqualsOperator) firstNameFilterClauses2.get(0);
    assertThat(firstNameEqualsOperator.path().toString()).isEqualTo("teachers.first");
    assertThat(firstNameEqualsOperator.doesNotAffect().get()).contains("teachersFirstNameFacet");

    // Validate "teachersLastNameFacet" operator
    Operator lastNameOperator =
        facetOperators
            .get("teachersLastNameFacet")
            .orElseThrow(
                () -> new IllegalStateException("No operator present for teachersLastNameFacet."));
    assertThat(lastNameOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    EmbeddedDocumentOperator lastNameEmbeddedOp = (EmbeddedDocumentOperator) lastNameOperator;
    assertThat(lastNameEmbeddedOp.path().toString()).isEqualTo("teachers.outer");

    assertThat(lastNameEmbeddedOp.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator lastNameCompoundOp = (CompoundOperator) lastNameEmbeddedOp.operator();
    assertThat(lastNameCompoundOp.filter().isPresent()).isTrue();

    List<? extends Operator> lastNameFilterClauses = lastNameCompoundOp.filter().operators();
    assertThat(lastNameFilterClauses).hasSize(1); // Validate one clause in FILTER
    assertThat(lastNameFilterClauses.get(0)).isInstanceOf(EqualsOperator.class);

    EqualsOperator lastNameClause = (EqualsOperator) lastNameFilterClauses.get(0);
    assertThat(lastNameClause.path().toString()).isEqualTo("teachers.last");
    assertThat(lastNameClause.value()).isEqualTo(ValueBuilder.string("Smith"));
    assertThat(lastNameClause.doesNotAffect().get()).contains("teachersLastNameFacet");

    // ------------------ Validate pre-filter structure ------------------
    Optional<Operator> preFilterClauses = info.preFilter();

    // Assert that the prefilter is present
    assertThat(preFilterClauses).isPresent();

    Operator preFilterOperator = preFilterClauses.get();

    // Assert that the outer-level prefilter operator is an EmbeddedDocumentOperator
    assertThat(preFilterOperator).isInstanceOf(EmbeddedDocumentOperator.class);

    // Extract the outer EmbeddedDocumentOperator and verify its path
    EmbeddedDocumentOperator preFilterEmbeddedDocument =
        (EmbeddedDocumentOperator) preFilterOperator;
    assertThat(preFilterEmbeddedDocument.path().toString()).isEqualTo("teachers.outer");

    // Assert that the operator inside the EmbeddedDocumentOperator is a CompoundOperator
    assertThat(preFilterEmbeddedDocument.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator preFilterCompound = (CompoundOperator) preFilterEmbeddedDocument.operator();

    // Validate the `MUST` clause of the top-level CompoundOperator
    assertThat(preFilterCompound.must().isPresent()).isTrue();
    List<? extends Operator> mustClauses = preFilterCompound.must().operators();
    assertThat(mustClauses).hasSize(1);

    // Extract the inner EmbeddedDocumentOperator (teachers.inner) and validate it
    EmbeddedDocumentOperator innerOperator = (EmbeddedDocumentOperator) mustClauses.get(0);
    assertThat(innerOperator.path().toString()).isEqualTo("teachers.inner");

    // Assert that the operator inside the inner EmbeddedDocumentOperator is a CompoundOperator
    assertThat(innerOperator.operator()).isInstanceOf(CompoundOperator.class);
    CompoundOperator innerCompound = (CompoundOperator) innerOperator.operator();

    // Validate the `MUST` clause of the inner CompoundOperator
    assertThat(innerCompound.must().isPresent()).isFalse();

    // Validate the `FILTER` clause of the inner CompoundOperator
    assertThat(innerCompound.filter().isPresent()).isTrue();
    List<? extends Operator> innerFilterClauses = innerCompound.filter().operators();
    assertThat(innerFilterClauses).hasSize(1);

    // Validate the RangeOperator inside the `FILTER` clause of the inner CompoundOperator
    RangeOperator rangeOperator = (RangeOperator) innerFilterClauses.get(0);
    assertThat(rangeOperator.paths().toString()).contains("teachers.age");

    // Validate the `MUST_NOT` clause of the inner CompoundOperator
    assertThat(innerCompound.mustNot().isPresent()).isTrue();
    List<? extends Operator> innerMustNotClauses = innerCompound.mustNot().operators();
    assertThat(innerMustNotClauses).hasSize(1);

    // Validate the ExistsOperator inside the `MUST_NOT` clause of the inner CompoundOperator
    ExistsOperator existsOperator = (ExistsOperator) innerMustNotClauses.get(0);
    assertThat(existsOperator.path().toString()).isEqualTo("teachers.salary");

    // Validate the `SHOULD` clause of the inner CompoundOperator
    assertThat(innerCompound.should().isPresent()).isTrue();
    List<? extends Operator> innerShouldClauses = innerCompound.should().operators();
    assertThat(innerShouldClauses).hasSize(2);

    // Validate the TextOperators inside the `SHOULD` clause of the inner CompoundOperator
    TextOperator notesOperator = (TextOperator) innerShouldClauses.get(0);
    assertThat(notesOperator.paths().toString()).contains("teachers.notes");
    assertThat(notesOperator.query()).contains("certified");

    TextOperator skillsOperator = (TextOperator) innerShouldClauses.get(1);
    assertThat(skillsOperator.paths().toString()).contains("teachers.skills");
    assertThat(skillsOperator.query()).contains("physics");

    // Validate the `FILTER` clause of the outer CompoundOperator DNE because doesNotAffect present
    assertThat(preFilterCompound.filter().isPresent()).isFalse();

    // Validate the `SHOULD` clause of the outer CompoundOperator
    assertThat(preFilterCompound.should().isPresent()).isTrue();
    List<? extends Operator> shouldClauses = preFilterCompound.should().operators();
    assertThat(shouldClauses).hasSize(1);

    // Validate the EqualsOperator inside the `SHOULD` clause of the outer CompoundOperator
    EqualsOperator cityOperator = (EqualsOperator) shouldClauses.get(0);
    assertThat(cityOperator.path().toString()).contains("teachers.city");
    assertThat(cityOperator.value()).isEqualTo(ValueBuilder.string("New York"));

    // Validate the `MUST_NOT` clause of the outer CompoundOperator
    assertThat(preFilterCompound.mustNot().isPresent()).isTrue();
    List<? extends Operator> mustNotClauses = preFilterCompound.mustNot().operators();
    assertThat(mustNotClauses).hasSize(1);

    // Validate the RangeOperator inside the `MUST_NOT` clause of the outer CompoundOperator
    RangeOperator experienceOperator = (RangeOperator) mustNotClauses.get(0);
    assertThat(experienceOperator.paths().toString()).contains("teachers.experience.years");
  }

  @Test
  public void buildFacetOperators_compoundOperatorExcludesOnlyOwnFacet_optimizable() {
    //  Requested query:
    //  {
    //      "compound": {
    //      "filter": [
    //      {
    //        "range": {
    //        "path": "size",
    //            "gte": 5,
    //            "lt": 6,
    //            "doesNotAffect": ["sizes"]
    //      }
    //      }
    //    ],
    //      "must": [
    //      {
    //        "range": {
    //        "path": "color",
    //            "gte": 1,
    //            "lt": 2,
    //            "doesNotAffect": ["colors"]
    //      }
    //      },
    //      {
    //        "range": {
    //        "path": "color",
    //            "gte": 3,
    //            "lt": 4,
    //            "doesNotAffect": ["colors"]
    //      }
    //      }
    //    ],
    //      "minimumShouldMatch": 0
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(3L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(4L));
    Optional<NumericPoint> lower3 = Optional.of(new LongPoint(5L));
    Optional<NumericPoint> upper3 = Optional.of(new LongPoint(6L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower3, upper3, true, false)
                    .doesNotAffect("sizes")
                    .build())
            .must(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .doesNotAffect("colors")
                    .build())
            .must(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower2, upper2, true, false)
                    .doesNotAffect("colors")
                    .build())
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("colors");
    assertThat(facetOperators).containsKey("sizes");

    // Validate "colors" facet operator
    CompoundOperator colorsOperator =
        (CompoundOperator)
            facetOperators
                .get("colors")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: colors"));
    List<Operator> operators = colorsOperator.getOperators().toList();
    assertThat(operators).hasSize(2);
    assertRangeOperator(operators.get(0), "color", 1L, 2L);
    assertRangeOperator(operators.get(1), "color", 3L, 4L);

    // Validate "sizes" facet operator
    RangeOperator sizesOperator =
        (RangeOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    assertRangeOperator(sizesOperator, "size", 5L, 6L);
  }

  @Test
  public void buildFacetOperator_doesNotAffectFilterOuterCompoundLevel_optimizable() {
    //   Requested query:
    //   {
    //      "compound": {
    //      "filter": [
    //      {
    //        "range": {
    //        "path": "size",
    //            "gte": lower2,
    //            "lt": upper2
    //      }
    //      },
    //      {
    //        "range": {
    //        "path": "size",
    //            "gte": lower,
    //            "lt": upper
    //      }
    //      }
    //    ],
    //      "doesNotAffect": ["sizes"]
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(5L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(6L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(7L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(8L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower2, upper2, true, false)
                    .build())
            .filter(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .doesNotAffect("sizes")
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("sizes");

    // Validate "sizes" facet operator
    CompoundOperator sizesOperator =
        (CompoundOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    ;
    List<Operator> operators = sizesOperator.getOperators().toList();
    assertThat(operators).hasSize(2);
    assertRangeOperator(operators.get(0), "size", 7L, 8L);
    assertRangeOperator(operators.get(1), "size", 5L, 6L);
  }

  @Test
  public void buildFacetOperator_doesNotAffectNestedCompoundOperators_optimizable() {
    // Requested query:
    // {
    //    "compound": {
    //      "filter": [
    //        {
    //          "compound": {
    //            "filter": [
    //              {
    //                "range": {
    //                  "path": "size",
    //                  "gte": lower2,
    //                  "lt": upper2
    //                }
    //              },
    //              {
    //                "range": {
    //                  "path": "size",
    //                  "gte": lower3,
    //                  "lt": upper3
    //                }
    //              }
    //            ]
    //          }
    //        }
    //      ],
    //      "doesNotAffect": ["sizes"]
    //    }
    // }

    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(7L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(8L));
    Optional<NumericPoint> lower3 = Optional.of(new LongPoint(10L));
    Optional<NumericPoint> upper3 = Optional.of(new LongPoint(12L));

    // Create the inner nested compound operator
    CompoundOperator innerCompoundOperator =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower2, upper2, true, false)
                    .build())
            .filter(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower3, upper3, true, false)
                    .build())
            .build();

    // Create the top-level compound operator with the doesNotAffect
    CompoundOperator topLevelCompoundOperator =
        OperatorBuilder.compound().filter(innerCompoundOperator).doesNotAffect("sizes").build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(topLevelCompoundOperator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("sizes");

    // Validate "sizes" facet operator
    CompoundOperator sizesOperator =
        (CompoundOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    List<Operator> operators = sizesOperator.getOperators().toList();
    assertThat(operators).hasSize(2);

    // Validate each range operator
    assertRangeOperator(operators.get(0), "size", 7L, 8L); // First range operator
    assertRangeOperator(operators.get(1), "size", 10L, 12L); // Second range operator
  }

  @Test
  public void buildFacetOperators_doesNotAffectShouldOuterCompoundLevel_generic() {
    //  Requested query:
    //  {
    //      "compound": {
    //      "should": [
    //      {
    //        "range": {
    //        "path": "size",
    //            "gte": lower2,
    //            "lt": upper2
    //      }
    //      },
    //      {
    //        "range": {
    //        "path": "size",
    //            "gte": lower,
    //            "lt": upper
    //      }
    //      }
    //    ],
    //      "minimumShouldMatch": 0,
    //          "doesNotAffect": ["sizes"]
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(5L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(6L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(7L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(8L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower2, upper2, true, false)
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .doesNotAffect("sizes")
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("colors");

    // Validate "sizes" facet operator
    CompoundOperator sizesOperator =
        (CompoundOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    ;
    List<Operator> operators = sizesOperator.getOperators().toList();
    assertThat(operators).hasSize(2);
    assertRangeOperator(operators.get(0), "size", 7L, 8L);
    assertRangeOperator(operators.get(1), "size", 5L, 6L);
  }

  @Test
  public void buildFacetOperators_doesNotAffectInteriorOperatorLevel_optimizable() {
    //  Requested query:
    //  {
    //      "compound": {
    //      "must": [
    //      {
    //        "range": {
    //        "path": "size",
    //            "gte": lower2,
    //            "lt": upper2,
    //            "doesNotAffect": ["sizes"]
    //      }
    //      },
    //      {
    //        "range": {
    //        "path": "size",
    //            "gte": lower,
    //            "lt": upper,
    //            "doesNotAffect": ["sizes"]
    //      }
    //      }
    //    ]
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(5L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(6L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(7L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(8L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .must(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower2, upper2, true, false)
                    .doesNotAffect("sizes")
                    .build())
            .must(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower, upper, true, false)
                    .doesNotAffect("sizes")
                    .build())
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("sizes");

    // Validate "sizes" facet operator
    CompoundOperator sizesOperator =
        (CompoundOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    ;
    List<Operator> operators = sizesOperator.getOperators().toList();
    assertThat(operators).hasSize(2);
    assertRangeOperator(operators.get(0), "size", 7L, 8L);
    assertRangeOperator(operators.get(1), "size", 5L, 6L);
  }

  @Test
  public void buildFacetOperators_multipleSelectionsSameFacet_optimizable() {
    //  Requested query:
    //  {
    //      "compound": {
    //      "filter": [
    //      {
    //        "range": {
    //        "path": "size",
    //            "gte": 5,
    //            "lt": 6,
    //            "doesNotAffect": ["sizes"]
    //      }
    //      },
    //      {
    //        "in": {
    //        "path": "color",
    //            "values": [1, 2],
    //        "doesNotAffect": ["colors"]
    //      }
    //      }
    //    ]
    //    }
    //    }

    List<Long> values = new ArrayList<>();
    values.add((long) 1);
    values.add((long) 2);

    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(5L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(6L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower2, upper2, true, false)
                    .doesNotAffect("sizes")
                    .build())
            .filter(
                OperatorBuilder.in().path("color").longs(values).doesNotAffect("colors").build())
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("colors");
    assertThat(facetOperators).containsKey("sizes");

    // Validate "colors" facet operator
    InOperator colorsOperator =
        (InOperator)
            facetOperators
                .get("colors")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: colors"));
    assertInOperator(
        colorsOperator,
        "color",
        Arrays.asList(ValueBuilder.longNumber(1), ValueBuilder.longNumber(2)));

    // Validate "sizes" facet operator
    RangeOperator sizesOperator =
        (RangeOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    assertRangeOperator(sizesOperator, "size", 5L, 6L);
  }

  @Test
  public void buildFacetOperators_interiorCompoundMatchesPath_Optimizable() {
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(3L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(4L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.compound()
                    .filter(
                        OperatorBuilder.range()
                            .path("color")
                            .numericBounds(lower, upper, true, false)
                            .doesNotAffect("colors")
                            .build())
                    .filter(
                        OperatorBuilder.range()
                            .path("color")
                            .numericBounds(lower2, upper2, true, false)
                            .doesNotAffect("colors")
                            .build())
                    .build())
            .doesNotAffect("colors")
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    // Facet name colors: corresponding active buckets
    assertThat(facetOperators).containsKey("colors");

    // Validate "colors" facet operator
    CompoundOperator colorsOperator =
        (CompoundOperator)
            facetOperators
                .get("colors")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: colors"));
    List<Operator> operators = colorsOperator.getOperators().toList();
    assertThat(operators).hasSize(2);
    assertRangeOperator(operators.get(0), "color", 1L, 2L);
    assertRangeOperator(operators.get(1), "color", 3L, 4L);
  }

  @Test
  public void buildFacetOperators_mustNotClauseWithoutDoesNotAffect_optimizable() {
    //   Requested query:
    //   {
    //      "compound": {
    //      "filter": [
    //      {
    //        "range": {
    //        "path": "size",
    //            "gte": lower3,
    //            "lt": upper3,
    //            "doesNotAffect": ["sizes"]
    //      }
    //      },
    //      {
    //        "range": {
    //        "path": "size",
    //            "gte": lower4,
    //            "lt": upper4,
    //            "doesNotAffect": ["sizes"]
    //      }
    //      }
    //    ],
    //      "mustNot": [
    //      {
    //        "range": {
    //        "path": "color",
    //            "gte": lower,
    //            "lt": upper
    //      }
    //      },
    //      {
    //        "range": {
    //        "path": "color",
    //            "gte": lower2,
    //            "lt": upper2
    //      }
    //      }
    //    ]
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(3L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(4L));
    Optional<NumericPoint> lower3 = Optional.of(new LongPoint(5L));
    Optional<NumericPoint> upper3 = Optional.of(new LongPoint(6L));
    Optional<NumericPoint> lower4 = Optional.of(new LongPoint(7L));
    Optional<NumericPoint> upper4 = Optional.of(new LongPoint(8L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower3, upper3, true, false)
                    .doesNotAffect("sizes")
                    .build())
            .filter(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower4, upper4, true, false)
                    .doesNotAffect("sizes")
                    .build())
            .mustNot(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .mustNot(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower2, upper2, true, false)
                    .build())
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("sizes");

    // Validate "sizes" facet operator
    CompoundOperator sizesOperator =
        (CompoundOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    List<Operator> operators = sizesOperator.getOperators().toList();
    assertThat(operators).hasSize(2);
    assertRangeOperator(operators.get(0), "size", 5L, 6L);
    assertRangeOperator(operators.get(1), "size", 7L, 8L);
  }

  @Test
  public void buildFacetOperators_shouldClauseWithoutDoesNotAffect_optimizable() {
    //  Requested query:
    //  {
    //      "compound": {
    //      "filter": [
    //      {
    //        "range": {
    //        "path": "color",
    //            "gte": lower,
    //            "lt": upper,
    //            "doesNotAffect": ["colors"]
    //      }
    //      },
    //      {
    //        "range": {
    //        "path": "color",
    //            "gte": lower2,
    //            "lt": upper2,
    //            "doesNotAffect": ["colors"]
    //      }
    //      }
    //    ],
    //      "should": [
    //      {
    //        "range": {
    //        "path": "color",
    //            "gte": lower,
    //            "lt": upper
    //      }
    //      },
    //      {
    //        "range": {
    //        "path": "color",
    //            "gte": lower2,
    //            "lt": upper2
    //      }
    //      }
    //    ],
    //      "minimumShouldMatch": 0
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(3L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(4L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .doesNotAffect("colors")
                    .build())
            .filter(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower2, upper2, true, false)
                    .doesNotAffect("colors")
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower2, upper2, true, false)
                    .build())
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("colors");

    // Validate "colors" facet operator
    CompoundOperator colorsOperator =
        (CompoundOperator)
            facetOperators
                .get("colors")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: colors"));
    List<Operator> operators = colorsOperator.getOperators().toList();
    assertThat(operators).hasSize(2);
    assertRangeOperator(operators.get(0), "color", 1L, 2L);
    assertRangeOperator(operators.get(1), "color", 3L, 4L);
  }

  @Test
  public void buildFacetOperators_pathMismatchDoesNotAffect_generic() {
    //  Requested query:
    //  {
    //      "compound": {
    //      "must": [
    //      {
    //        "equals": {
    //        "path": "color",
    //            "value": 1,
    //            "doesNotAffect": ["sizes"]
    //      }
    //      },
    //      {
    //        "range": {
    //        "path": "size",
    //            "gte": lower2,
    //            "lt": upper2,
    //            "doesNotAffect": ["colors"]
    //      }
    //      }
    //    ]
    //    }
    //    }

    long value = 1;
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(5L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(7L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .must(
                OperatorBuilder.equals().path("color").value(value).doesNotAffect("sizes").build())
            .must(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower2, upper2, true, false)
                    .doesNotAffect("colors")
                    .build())
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("colors");
    assertThat(facetOperators).containsKey("sizes");

    // Validate "colors" facet operator
    CompoundOperator colorsOperator =
        (CompoundOperator)
            facetOperators
                .get("colors")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: colors"));
    ;
    List<Operator> colorOperator = colorsOperator.getOperators().toList();
    assertThat(colorOperator).hasSize(1);
    assertEqualsOperator(colorOperator.get(0), "color", value);

    // Validate "sizes" facet operator
    CompoundOperator sizesOperator =
        (CompoundOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    List<Operator> operators = sizesOperator.getOperators().toList();
    assertThat(operators).hasSize(1);
    assertRangeOperator(operators.get(0), "size", 5L, 7L);
  }

  @Test
  public void buildFacetOperators_mustNotClauseWithDoesNotAffect_generic() {
    //  Requested query:
    //  {
    //      "compound": {
    //      "mustNot": [
    //      {
    //        "range": {
    //        "path": "color",
    //            "gte": lower,
    //            "lt": upper,
    //            "doesNotAffect": ["colors"]
    //      }
    //      },
    //      {
    //        "range": {
    //        "path": "color",
    //            "gte": lower2,
    //            "lt": upper2,
    //            "doesNotAffect": ["colors"]
    //      }
    //      }
    //    ]
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(3L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(4L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .mustNot(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .doesNotAffect("colors")
                    .build())
            .mustNot(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower2, upper2, true, false)
                    .doesNotAffect("colors")
                    .build())
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("sizes");
    assertThat(facetOperators).containsKey("colors");

    // Validate "sizes" facet operator
    CompoundOperator sizesOperator =
        (CompoundOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    ;
    List<Operator> operators = sizesOperator.getOperators().toList();
    assertThat(operators).hasSize(2);
    assertRangeOperator(operators.get(0), "color", 1L, 2L);
    assertRangeOperator(operators.get(1), "color", 3L, 4L);

    // Validate "colors" facet operator
    assertThat(facetOperators.get("colors")).isEmpty();
  }

  @Test
  public void buildFacetOperators_rangeOperatorMultiplePaths_generic() {
    //  Requested query:
    //  {
    //      "range": {
    //      "paths": ["color", "size"],
    //      "numericBounds": {
    //        "lower": 1,
    //            "upper": 2
    //      },
    //      "inclusiveLower": true,
    //          "exclusiveUpper": true,
    //          "doesNotAffect": ["colors"]
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    RangeOperator operator =
        OperatorBuilder.range()
            .path("color") // Specify multiple paths
            .path("size")
            .numericBounds(lower, upper, true, false)
            .doesNotAffect("colors")
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("colors");
    assertThat(facetOperators).containsKey("sizes");

    // Validate "sizes" facet operator
    RangeOperator sizesOperator =
        (RangeOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    assertRangeOperator(sizesOperator, "color", 1L, 2L);

    // Validate the "colors" facet operator
    assertThat(facetOperators.get("colors")).isEmpty();
  }

  @Test
  public void buildFacetOperators_shouldClauseWithDoesNotAffect_generic() {
    //  Requested query:
    //  {
    //      "compound": {
    //      "should": [
    //      {
    //        "range": {
    //        "path": "color",
    //            "gte": lower,
    //            "lt": upper,
    //            "doesNotAffect": ["colors"]
    //      }
    //      }
    //    ],
    //      "minimumShouldMatch": 0
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .doesNotAffect("colors")
                    .build())
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("sizes");
    assertThat(facetOperators).containsKey("colors");

    // Validate "sizes" facet operator
    CompoundOperator sizesOperator =
        (CompoundOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    List<Operator> operators2 = sizesOperator.getOperators().toList();
    assertThat(operators2).hasSize(1);
    assertRangeOperator(operators2.get(0), "color", 1L, 2L);

    // Validate the "colors" facet operator
    assertThat(facetOperators.get("colors")).isEmpty();
  }

  @Test
  public void buildFacetOperators_rangeOperator_generic() {
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    Operator operator =
        OperatorBuilder.range()
            .path("size")
            .numericBounds(lower, upper, true, false)
            .doesNotAffect("colors")
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("sizes");
    assertThat(facetOperators).containsKey("colors");

    // Validate "sizes" facet operator
    RangeOperator sizesOperator =
        (RangeOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    assertRangeOperator(sizesOperator, "size", 1L, 2L);

    // Validate the "colors" facet operator
    assertThat(facetOperators.get("colors")).isEmpty();
  }

  @Test
  public void buildFacetOperators_multipleDoesNotAffectFields_generic() {
    // Requested query:
    // {
    //      "compound": {
    //      "must": [
    //      {
    //        "equals": {
    //        "path": "color",
    //            "value": 1,
    //            "doesNotAffect": ["colors"]
    //      }
    //      },
    //      {
    //        "range": {
    //        "path": "size",
    //            "gte": lower2,
    //            "lt": upper2,
    //            "doesNotAffect": ["colors", "sizes"]
    //      }
    //      }
    //    ]
    //    }
    //    }

    long value = 1;
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(5L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(7L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .must(
                OperatorBuilder.equals().path("color").value(value).doesNotAffect("colors").build())
            .must(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(lower2, upper2, true, false)
                    .doesNotAffect(Arrays.asList("colors", "sizes"))
                    .build())
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("sizes");
    assertThat(facetOperators).containsKey("colors");

    // Validate "sizes" facet operator
    CompoundOperator sizesOperator =
        (CompoundOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    List<Operator> operators = sizesOperator.getOperators().toList();
    assertThat(operators).hasSize(1);
    assertEqualsOperator(operators.get(0), "color", value);

    // Validate the "colors" facet operator
    assertThat(facetOperators.get("colors")).isEmpty();
  }

  @Test
  public void buildFacetOperators_interiorCompoundOperatorMismatchPath_generic() {
    //  Requested query:  {
    //      "compound": {
    //      "filter": [
    //      {
    //        "compound": {
    //        "filter": [
    //        {
    //          "range": {
    //          "path": "color",
    //              "gte": lower,
    //              "lt": upper,
    //              "doesNotAffect": ["sizes"]
    //        }
    //        },
    //        {
    //          "range": {
    //          "path": "color",
    //              "gte": lower2,
    //              "lt": upper2,
    //              "doesNotAffect": ["sizes"]
    //        }
    //        }
    //          ]
    //      }
    //      }
    //    ],
    //      "doesNotAffect": ["colors"]
    //    }
    //    }

    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(3L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(4L));

    CompoundOperator operator =
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.compound()
                    .filter(
                        OperatorBuilder.range()
                            .path("color")
                            .numericBounds(lower, upper, true, false)
                            .doesNotAffect("sizes")
                            .build())
                    .filter(
                        OperatorBuilder.range()
                            .path("color")
                            .numericBounds(lower2, upper2, true, false)
                            .doesNotAffect("sizes")
                            .build())
                    .build())
            .doesNotAffect("colors")
            .build();

    Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

    DrillSidewaysInfoBuilder.DrillSidewaysInfo info =
        DrillSidewaysInfoBuilder.buildFacetOperators(operator, facetDefinitions);

    // Check the optimization status
    assertThat(DrillSidewaysInfo.QueryOptimizationStatus.GENERIC)
        .isEqualTo(info.optimizationStatus());

    // Validate facet operators
    Map<String, Optional<Operator>> facetOperators = info.facetOperators();
    assertThat(facetOperators).containsKey("colors");

    // Validate the structure of the "colors" CompoundOperator
    Operator colorsOperator =
        facetOperators
            .get("colors")
            .orElseThrow(() -> new IllegalStateException("No operator present for facet: colors"));
    assertThat(colorsOperator).isInstanceOf(CompoundOperator.class);
    CompoundOperator outerCompoundOperator = (CompoundOperator) colorsOperator;

    // Extract the nested CompoundOperator
    List<Operator> nestedOperators = outerCompoundOperator.getOperators().toList();
    assertThat(nestedOperators).hasSize(1);

    // Check that the nested operator itself is a CompoundOperator
    Operator innerCompoundOperator = nestedOperators.get(0);
    assertThat(innerCompoundOperator).isInstanceOf(CompoundOperator.class);
    CompoundOperator innerCompound = (CompoundOperator) innerCompoundOperator;

    // Extract RangeOperators from the inner CompoundOperator
    List<Operator> rangeOperators = innerCompound.getOperators().toList();
    assertThat(rangeOperators).hasSize(2);

    // Validate individual RangeOperators
    assertRangeOperator(rangeOperators.get(0), "color", 1L, 2L);
    assertRangeOperator(rangeOperators.get(1), "color", 3L, 4L);
  }

  private static Map<String, FacetDefinition> getFacetDefinitions() {
    Map<String, FacetDefinition> facetDefinitions = new HashMap<>();
    // Add color facet definition
    facetDefinitions.put(
        "colors",
        new FacetDefinition.NumericFacetDefinition(
            "color",
            Optional.empty(),
            List.of(
                new BsonDouble(1.0),
                new BsonDouble(2.0),
                new BsonDouble(3.0),
                new BsonDouble(4.0))));

    // Add size facet definition
    facetDefinitions.put(
        "sizes",
        new FacetDefinition.NumericFacetDefinition(
            "size",
            Optional.empty(),
            List.of(
                new BsonDouble(5.0),
                new BsonDouble(6.0),
                new BsonDouble(7.0),
                new BsonDouble(8.0))));
    return facetDefinitions;
  }

  private static Map<String, FacetDefinition> getFacetDefinitionsWithDuplicates() {
    Map<String, FacetDefinition> facetDefinitions = new HashMap<>();

    // Add color facet definition
    facetDefinitions.put(
        "colors",
        new FacetDefinition.NumericFacetDefinition(
            "color",
            Optional.empty(),
            List.of(
                new BsonDouble(1.0),
                new BsonDouble(2.0),
                new BsonDouble(3.0),
                new BsonDouble(4.0)))); // Path: "color"

    // Add duplicate facet definition on the same path ("color")
    facetDefinitions.put(
        "duplicateColors",
        new FacetDefinition.NumericFacetDefinition(
            "color",
            Optional.empty(),
            List.of(new BsonDouble(10.0), new BsonDouble(11.0)))); // Duplicate path: "color"

    // Add size facet definition
    facetDefinitions.put(
        "sizes",
        new FacetDefinition.NumericFacetDefinition(
            "size",
            Optional.empty(),
            List.of(
                new BsonDouble(5.0),
                new BsonDouble(6.0),
                new BsonDouble(7.0),
                new BsonDouble(8.0)))); // Path: "size"

    return facetDefinitions;
  }

  private static Map<String, FacetDefinition> getFacetDefinitionsEmbedded() {
    Map<String, FacetDefinition> facetDefinitions = new HashMap<>();

    // Add teachersFirstNameFacet definition
    facetDefinitions.put(
        "teachersFirstNameFacet",
        new FacetDefinition.StringFacetDefinition(
            "teachers.first", 5 // numBuckets
            ));

    // Add teachersLastNameFacet definition
    facetDefinitions.put(
        "teachersLastNameFacet",
        new FacetDefinition.StringFacetDefinition(
            "teachers.last", 5 // numBuckets
            ));

    return facetDefinitions;
  }

  private void assertEqualsOperator(Operator operator, String expectedPath, long value) {
    assertThat(operator).isInstanceOf(EqualsOperator.class);
    EqualsOperator equalsOperator = (EqualsOperator) operator;

    // Validate the path
    assertThat(equalsOperator.path().toString()).isEqualTo(expectedPath);

    // Validate the lower bound
    assertThat(equalsOperator.value()).isEqualTo(ValueBuilder.longNumber(value));
  }

  private void assertInOperator(Operator operator, String expectedPath, List<NonNullValue> values) {
    assertThat(operator).isInstanceOf(InOperator.class);
    InOperator inOperator = (InOperator) operator;

    // Validate the path
    assertThat(inOperator.paths().get(0).toString()).isEqualTo(expectedPath);

    // Validate the size of the lists
    List<NonNullValue> actualValues = inOperator.values();
    assertThat(actualValues).hasSize(values.size());

    // Validate the content
    for (NonNullValue expectedValue : values) {
      assertThat(actualValues).contains(expectedValue);
    }
  }

  @Test
  public void buildFacetOperators_directNestedEmbeddedDocs_propagatesFacetOperatorsAndPreFilter()
      throws Exception {
    // Nested embedded docs (outer -> outer.inner -> compound with doesNotAffect): facet operators
    // must be propagated to the parent state and the preFilter must preserve both embedding levels.
    FacetCollector collector =
        parseFacetCollector(
            """
            {
              "operator": {
                "embeddedDocument": {"path": "outer", "operator":
                    {"embeddedDocument": {"path": "outer.inner", "operator":
                        {"compound": {"filter": [
                            {"exists":  {"path": "outer.inner.notes"}},
                            {"equals":  {"path": "outer.inner.x", "value": "val",
                                         "doesNotAffect": "xFacet"}}
                        ]}}
                    }}
                }
              },
              "facets": {
                "xFacet": {"type": "string", "path": "outer.inner.x"}
              }
            }\
            """);

    DrillSidewaysInfo info = collector.drillSidewaysInfo().get();

    assertThat(info.optimizationStatus())
        .isEqualTo(DrillSidewaysInfo.QueryOptimizationStatus.OPTIMIZABLE);
    assertThat(info.facetOperators()).containsKey("xFacet");
    assertThat(info.facetOperators().get("xFacet")).isPresent();
    Operator facetOp = info.facetOperators().get("xFacet").get();
    assertThat(facetOp).isInstanceOf(EmbeddedDocumentOperator.class);
    EmbeddedDocumentOperator facetEmbedded = (EmbeddedDocumentOperator) facetOp;
    assertThat(facetEmbedded.path().toString()).isEqualTo("outer");

    // PreFilter must be wrapped at both levels: embeddedDoc("outer", embeddedDoc("outer.inner",
    // compound(filter:[exists]))).
    assertThat(info.preFilter()).isPresent();
    Operator preFilter = info.preFilter().get();
    assertThat(preFilter).isInstanceOf(EmbeddedDocumentOperator.class);
    EmbeddedDocumentOperator outerWrap = (EmbeddedDocumentOperator) preFilter;
    assertThat(outerWrap.path().toString()).isEqualTo("outer");
    assertThat(outerWrap.operator()).isInstanceOf(EmbeddedDocumentOperator.class);
    EmbeddedDocumentOperator innerWrap = (EmbeddedDocumentOperator) outerWrap.operator();
    assertThat(innerWrap.path().toString()).isEqualTo("outer.inner");
  }

  private static FacetCollector parseFacetCollector(String json) throws Exception {
    try (var parser =
        com.xgen.mongot.util.bson.parser.BsonDocumentParser.fromRoot(
                org.bson.BsonDocument.parse(json))
            .build()) {
      return FacetCollector.fromBson(parser);
    }
  }

  private void assertRangeOperator(
      Operator operator, String expectedPath, long expectedLower, long expectedUpper) {
    assertThat(operator).isInstanceOf(RangeOperator.class);
    RangeOperator rangeOperator = (RangeOperator) operator;

    // Validate the path
    assertThat(rangeOperator.paths().getFirst().toString()).isEqualTo(expectedPath);

    // Validate the lower bound
    assertThat(rangeOperator.bounds().getLower()).isPresent(); // Ensure lower bound is present
    assertThat(rangeOperator.bounds().getLower().get()).isEqualTo(new LongPoint(expectedLower));

    // Validate the upper bound
    assertThat(rangeOperator.bounds().getUpper()).isPresent(); // Ensure upper bound is present
    assertThat(rangeOperator.bounds().getUpper().get()).isEqualTo(new LongPoint(expectedUpper));
  }
}
