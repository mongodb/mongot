package com.xgen.mongot.index.query.collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.xgen.mongot.index.query.collectors.GenericDrillSidewaysFacetOperatorBuilder.buildGenericQueryOperatorMap;
import static com.xgen.mongot.index.query.collectors.GenericDrillSidewaysFacetOperatorBuilder.handleEmbeddedOperator;

import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.operators.EqualsOperator;
import com.xgen.mongot.index.query.operators.InOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.points.NumericPoint;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonDouble;
import org.junit.Before;
import org.junit.Test;

public class GenericDrillSidewaysFacetOperatorBuilderTest {
  private CompoundOperator sampleQuery;
  private Map<String, FacetDefinition> facetDefinitions;

  @Before
  public void setUp() {
    this.sampleQuery = buildSampleQuery();
    this.facetDefinitions = buildFacetDefinitions();
  }

  @Test
  public void handleNonDoesNotAffectOperator_validOperatorAndFacetNames() {
    Operator operator =
        OperatorBuilder.range()
            .path("category")
            .numericBounds(
                Optional.of(new LongPoint(10)), Optional.of(new LongPoint(20)), true, false)
            .build();

    Set<String> facetNames = new HashSet<>(Arrays.asList("facet1", "facet2", "facet3"));

    Map<String, Optional<Operator>> result =
        GenericDrillSidewaysFacetOperatorBuilder.handleNonDoesNotAffectOperator(
            operator, facetNames);

    // Verify all facet names are keys in the result and each key maps to the same operator
    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(facetNames.size());
    facetNames.forEach(facet -> assertThat(result.containsKey(facet)).isTrue());
    facetNames.forEach(facet -> assertThat(result.get(facet).orElse(null) == operator).isTrue());
  }

  @Test
  public void handleNonDoesNotAffectOperator_emptyFacetNames_returnsEmptyMap() {
    Operator operator =
        OperatorBuilder.range()
            .path("category")
            .numericBounds(Optional.of(new LongPoint(10)), Optional.empty(), true, false)
            .build();

    Set<String> facetNames = new HashSet<>();

    Map<String, Optional<Operator>> result =
        GenericDrillSidewaysFacetOperatorBuilder.handleNonDoesNotAffectOperator(
            operator, facetNames);

    assertThat(result).isNotNull();
    assertThat(result.isEmpty()).isTrue();
  }

  @Test
  public void handleNonDoesNotAffectOperator_singleFacetName_returnsMapWithSingleEntry() {
    Operator operator =
        OperatorBuilder.range()
            .path("age")
            .numericBounds(
                Optional.of(new LongPoint(18)), Optional.of(new LongPoint(65)), true, true)
            .build();

    Set<String> facetNames = new HashSet<>(Collections.singletonList("singleFacet"));

    Map<String, Optional<Operator>> result =
        GenericDrillSidewaysFacetOperatorBuilder.handleNonDoesNotAffectOperator(
            operator, facetNames);

    // Verify result contains one entry with the expected facet name and operator
    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.containsKey("singleFacet")).isTrue();
    assertThat(result.get("singleFacet")).isEqualTo(Optional.of(operator));
  }

  @Test
  public void handleCompoundOperator_minimumShouldMatchAdjustedForFacetOperators() {
    Set<String> facetNames = Set.of("colors", "sizes");

    // Valid CompoundOperator setup (minimumShouldMatch <= # should clauses)
    CompoundOperator compoundOperator =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(
                        Optional.of(new LongPoint(1)), Optional.of(new LongPoint(2)), true, false)
                    .doesNotAffect("colors")
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("shade")
                    .numericBounds(
                        Optional.of(new LongPoint(3)), Optional.of(new LongPoint(4)), true, false)
                    .doesNotAffect("colors")
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(
                        Optional.of(new LongPoint(5)), Optional.of(new LongPoint(6)), true, false)
                    .doesNotAffect("sizes")
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("dimension")
                    .numericBounds(
                        Optional.of(new LongPoint(7)), Optional.of(new LongPoint(8)), true, false)
                    .doesNotAffect("sizes")
                    .build())
            .minimumShouldMatch(3) // Original minimumShouldMatch is valid
            .build();

    Map<String, Optional<Operator>> result =
        GenericDrillSidewaysFacetOperatorBuilder.handleCompoundOperator(
            compoundOperator, facetNames);

    // Verify the operator for the "colors" facet
    CompoundOperator colorsFacetOperator =
        (CompoundOperator)
            result
                .get("colors")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: colors"));
    assertThat(colorsFacetOperator).isNotNull();
    assertThat(colorsFacetOperator.should().operators().size()).isEqualTo(2);
    assertThat(colorsFacetOperator.minimumShouldMatch()).isEqualTo(2);

    // Verify the operator for the "sizes" facet
    CompoundOperator sizesFacetOperator =
        (CompoundOperator)
            result
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));
    assertThat(sizesFacetOperator).isNotNull();
    assertThat(sizesFacetOperator.should().operators().size()).isEqualTo(2);
    assertThat(sizesFacetOperator.minimumShouldMatch()).isEqualTo(2);
  }

  @Test
  public void handleCompoundOperator_clausesMatchExactly_minimumShouldMatchUnchanged() {
    Set<String> facetNames = Set.of("colors", "sizes");

    CompoundOperator compoundOperator =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(
                        Optional.of(new LongPoint(1)), Optional.of(new LongPoint(2)), true, false)
                    .doesNotAffect("colors")
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(
                        Optional.of(new LongPoint(3)), Optional.of(new LongPoint(4)), true, false)
                    .doesNotAffect("sizes") // Doesn't affect unrelated "sizes" facet
                    .build())
            .minimumShouldMatch(2) // Matches the exact number of should clauses
            .build();

    Map<String, Optional<Operator>> result =
        GenericDrillSidewaysFacetOperatorBuilder.handleCompoundOperator(
            compoundOperator, facetNames);

    // Validate the resulting facet operator
    CompoundOperator resultFacetOperator =
        (CompoundOperator)
            result
                .get("colors")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: colors"));

    // Assertions
    assertThat(resultFacetOperator).isNotNull();
    assertThat(resultFacetOperator.should().operators().size()).isEqualTo(1);
    assertThat(resultFacetOperator.minimumShouldMatch()).isEqualTo(1);
  }

  @Test
  public void handleEmbeddedOperator_validFacetOperatorsWithDoesNotAffectFiltering() {
    CompoundOperator compoundOp =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.equals()
                    .path("teachers.first")
                    .value("John")
                    .doesNotAffect("teachersFirstNameFacet")
                    .build())
            .should(
                OperatorBuilder.equals()
                    .path("teachers.last")
                    .value("Smith")
                    .doesNotAffect("teachersLastNameFacet")
                    .build())
            .minimumShouldMatch(1)
            .build();

    EmbeddedDocumentOperator embeddedOperator =
        OperatorBuilder.embeddedDocument().path("teachers").operator(compoundOp).build();

    Set<String> facetNames = Set.of("teachersFirstNameFacet", "teachersLastNameFacet");

    Map<String, Optional<Operator>> result = handleEmbeddedOperator(embeddedOperator, facetNames);

    // Validate "teachersFirstNameFacet"
    assertThat(result).containsKey("teachersFirstNameFacet");
    assertThat(result.get("teachersFirstNameFacet")).isPresent();
    EmbeddedDocumentOperator firstNameFacetOperator =
        (EmbeddedDocumentOperator) result.get("teachersFirstNameFacet").get();

    // Validate the EmbeddedDocumentOperator's path
    assertThat(firstNameFacetOperator.path().toString()).isEqualTo("teachers");

    // Validate the CompoundOperator within the EmbeddedDocumentOperator
    CompoundOperator firstNameCompound = (CompoundOperator) firstNameFacetOperator.operator();
    List<? extends Operator> firstNameShouldOperators = firstNameCompound.should().operators();
    assertThat(firstNameShouldOperators.size()).isEqualTo(1);
    assertThat(firstNameCompound.minimumShouldMatch()).isEqualTo(1);

    // Validate the filtered EqualsOperator for "teachers.last"
    EqualsOperator firstNameEqualsOperator = (EqualsOperator) firstNameShouldOperators.get(0);
    assertThat(firstNameEqualsOperator.path().toString()).isEqualTo("teachers.last");
    assertThat(firstNameEqualsOperator.value()).isEqualTo(ValueBuilder.string("Smith"));
    assertThat(firstNameEqualsOperator.doesNotAffect().get().contains("teachersLastNameFacet"))
        .isTrue();

    // Validate "teachersLastNameFacet"
    assertThat(result).containsKey("teachersLastNameFacet");
    assertThat(result.get("teachersLastNameFacet")).isPresent();
    EmbeddedDocumentOperator lastNameFacetOperator =
        (EmbeddedDocumentOperator) result.get("teachersLastNameFacet").get();

    // Validate the EmbeddedDocumentOperator's path
    assertThat(lastNameFacetOperator.path().toString()).isEqualTo("teachers");

    // Validate the CompoundOperator within the EmbeddedDocumentOperator
    CompoundOperator lastNameCompound = (CompoundOperator) lastNameFacetOperator.operator();
    List<? extends Operator> lastNameShouldOperators = lastNameCompound.should().operators();
    assertThat(lastNameShouldOperators.size()).isEqualTo(1);
    assertThat(lastNameCompound.minimumShouldMatch()).isEqualTo(1);

    // Validate the filtered EqualsOperator for "teachers.first"
    EqualsOperator lastNameEqualsOperator = (EqualsOperator) lastNameShouldOperators.get(0);
    assertThat(lastNameEqualsOperator.path().toString()).isEqualTo("teachers.first");
    assertThat(lastNameEqualsOperator.value()).isEqualTo(ValueBuilder.string("John"));
    assertThat(lastNameEqualsOperator.doesNotAffect().get().contains("teachersFirstNameFacet"))
        .isTrue();
  }

  @Test
  public void handleClauseOperators_givenOperatorsAndFacetNames_populatesClauseMapCorrectly() {
    // Arrange: Define sample operators
    Operator operator1 =
        OperatorBuilder.range()
            .path("price")
            .numericBounds(
                Optional.of(new LongPoint(100)), Optional.of(new LongPoint(200)), true, false)
            .build();

    Operator operator2 = OperatorBuilder.equals().path("tags").value("discounted").build();

    List<Operator> operators = Arrays.asList(operator1, operator2);

    Set<String> facetNames = new HashSet<>(Arrays.asList("facet1", "facet2"));

    // Prepare the clauseMap with initial empty lists for each facet name
    Map<String, List<Operator>> clauseMap = new HashMap<>();
    facetNames.forEach(facet -> clauseMap.put(facet, new ArrayList<>()));

    GenericDrillSidewaysFacetOperatorBuilder.handleClauseOperators(
        operators, clauseMap, facetNames);

    assertThat(clauseMap).isNotNull();
    assertThat(clauseMap.size()).isEqualTo(facetNames.size());
    facetNames.forEach(
        facet -> {
          assertThat(clauseMap.get(facet)).isNotNull();
          assertThat(clauseMap.get(facet).size()).isEqualTo(operators.size());
          assertThat(clauseMap.get(facet)).containsExactly(operator1, operator2);
        });
  }

  @Test
  public void handleClauseOperators_givenEmptyOperators_doesNotModifyClauseMap() {
    List<Operator> operators = Collections.emptyList();

    Set<String> facetNames = new HashSet<>(Arrays.asList("facet1", "facet2"));

    // Prepare the clauseMap with initial empty lists for each facet name
    Map<String, List<Operator>> clauseMap = new HashMap<>();
    facetNames.forEach(facet -> clauseMap.put(facet, new ArrayList<>()));

    GenericDrillSidewaysFacetOperatorBuilder.handleClauseOperators(
        operators, clauseMap, facetNames);

    assertThat(clauseMap).isNotNull();
    assertThat(clauseMap.size()).isEqualTo(facetNames.size());
    facetNames.forEach(
        facet -> {
          assertThat(clauseMap.get(facet)).isNotNull();
          assertThat(clauseMap.get(facet).isEmpty()).isTrue();
        });
  }

  @Test
  public void handleClauseOperators_givenEmptyFacetNames_doesNotModifyClauseMap() {
    Operator operator1 =
        OperatorBuilder.range()
            .path("price")
            .numericBounds(
                Optional.of(new LongPoint(100)), Optional.of(new LongPoint(200)), true, false)
            .build();

    Operator operator2 = OperatorBuilder.equals().path("tags").value("discounted").build();

    List<Operator> operators = Arrays.asList(operator1, operator2);

    Set<String> facetNames = new HashSet<>();

    // Prepare the clauseMap with no initial entries
    Map<String, List<Operator>> clauseMap = new HashMap<>();

    GenericDrillSidewaysFacetOperatorBuilder.handleClauseOperators(
        operators, clauseMap, facetNames);

    assertThat(clauseMap).isNotNull();
    assertThat(clauseMap.isEmpty()).isTrue();
  }

  @Test
  public void handleClauseOperators_givenOperatorsAndSingleFacetName_populatesCorrectly() {
    // Arrange: Define sample operators
    Operator operator1 =
        OperatorBuilder.range()
            .path("quantity")
            .numericBounds(
                Optional.of(new LongPoint(50)), Optional.of(new LongPoint(100)), false, true)
            .build();

    Operator operator2 = OperatorBuilder.equals().path("color").value("red").build();

    List<Operator> operators = Arrays.asList(operator1, operator2);

    Set<String> facetNames = new HashSet<>(Collections.singletonList("singleFacet"));

    // Prepare the clauseMap with initial empty list for the single facet name
    Map<String, List<Operator>> clauseMap = new HashMap<>();
    clauseMap.put("singleFacet", new ArrayList<>());

    GenericDrillSidewaysFacetOperatorBuilder.handleClauseOperators(
        operators, clauseMap, facetNames);

    assertThat(clauseMap).isNotNull();
    assertThat(clauseMap.size()).isEqualTo(1);
    assertThat(clauseMap.get("singleFacet")).isNotNull();
    assertThat(clauseMap.get("singleFacet").size()).isEqualTo(operators.size());
    assertThat(clauseMap.get("singleFacet")).containsExactly(operator1, operator2);
  }

  @Test
  public void handleEqualsOperator_givenValidOperatorAndFacetNames_includesCorrectFacets() {
    EqualsOperator operator =
        OperatorBuilder.equals()
            .path("field")
            .value("value")
            .doesNotAffect("excludedFacet") // This facet will be excluded
            .build();

    Set<String> facetNames = new HashSet<>(Arrays.asList("facet1", "facet2", "excludedFacet"));

    Map<String, Optional<Operator>> result =
        GenericDrillSidewaysFacetOperatorBuilder.handleEqualsOperator(operator, facetNames);

    // Verify only facets that are NOT excluded are included in the result
    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(2); // "facet1" and "facet2" should be included
    assertThat(result.containsKey("facet1")).isTrue();
    assertThat(result.containsKey("facet2")).isTrue();
    assertThat(result.containsKey("excludedFacet")).isFalse(); // "excludedFacet" should be excluded
    assertThat(result.get("facet1")).isEqualTo(Optional.of(operator));
    assertThat(result.get("facet2")).isEqualTo(Optional.of(operator));
  }

  @Test
  public void handleEqualsOperator_givenEmptyFacetNames_returnsEmptyMap() {
    EqualsOperator operator = OperatorBuilder.equals().path("field").value("value").build();

    Set<String> facetNames = new HashSet<>(); // No facets provided

    Map<String, Optional<Operator>> result =
        GenericDrillSidewaysFacetOperatorBuilder.handleEqualsOperator(operator, facetNames);

    assertThat(result).isNotNull();
    assertThat(result.isEmpty()).isTrue();
  }

  @Test
  public void handleInOperator_givenValidOperatorAndFacetNames_includesCorrectFacets() {
    List<Long> values = new ArrayList<>();
    values.add((long) 1);
    values.add((long) 2);

    InOperator operator =
        OperatorBuilder.in().path("field").longs(values).doesNotAffect("excludedFacet").build();

    Set<String> facetNames = new HashSet<>(Arrays.asList("facet1", "facet2", "excludedFacet"));

    Map<String, Optional<Operator>> result =
        GenericDrillSidewaysFacetOperatorBuilder.handleInOperator(operator, facetNames);

    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(2); // "facet1" and "facet2" should be included
    assertThat(result.containsKey("facet1")).isTrue();
    assertThat(result.containsKey("facet2")).isTrue();
    assertThat(result.containsKey("excludedFacet")).isFalse();
    assertThat(result.get("facet1")).isEqualTo(Optional.of(operator));
    assertThat(result.get("facet2")).isEqualTo(Optional.of(operator));
  }

  @Test
  public void handleInOperator_givenEmptyFacetNames_returnsEmptyMap() {
    List<Long> values = new ArrayList<>();
    values.add((long) 1);
    values.add((long) 2);

    InOperator operator = OperatorBuilder.in().path("field").longs(values).build();

    Set<String> facetNames = new HashSet<>(); // No facets provided

    Map<String, Optional<Operator>> result =
        GenericDrillSidewaysFacetOperatorBuilder.handleInOperator(operator, facetNames);

    assertThat(result).isNotNull();
    assertThat(result.isEmpty()).isTrue();
  }

  @Test
  public void handleRangeOperator_givenValidOperatorAndFacetNames_includesCorrectFacets() {
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    RangeOperator operator =
        OperatorBuilder.range()
            .path("field")
            .numericBounds(lower, upper, true, false)
            .doesNotAffect("excludedFacet") // This facet will be excluded
            .build();

    Set<String> facetNames = new HashSet<>(Arrays.asList("facet1", "facet2", "excludedFacet"));

    Map<String, Optional<Operator>> result =
        GenericDrillSidewaysFacetOperatorBuilder.handleRangeOperator(operator, facetNames);

    // Verify only facets that are NOT excluded are included in the result
    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(2); // "facet1" and "facet2" should be included
    assertThat(result.containsKey("facet1")).isTrue();
    assertThat(result.containsKey("facet2")).isTrue();
    assertThat(result.containsKey("excludedFacet")).isFalse(); // "excludedFacet" should be excluded
    assertThat(result.get("facet1")).isEqualTo(Optional.of(operator));
    assertThat(result.get("facet2")).isEqualTo(Optional.of(operator));
  }

  @Test
  public void handleRangeOperator_givenEmptyFacetNames_returnsEmptyMap() {
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    RangeOperator operator =
        OperatorBuilder.range()
            .path("field")
            .numericBounds(lower, upper, true, false)
            .doesNotAffect("excludedFacet")
            .build();

    Set<String> facetNames = new HashSet<>(); // No facets provided

    Map<String, Optional<Operator>> result =
        GenericDrillSidewaysFacetOperatorBuilder.handleRangeOperator(operator, facetNames);

    assertThat(result).isNotNull();
    assertThat(result.isEmpty()).isTrue();
  }

  @Test
  public void shouldIncludeOperator_givenAbsentDoesNotAffect_returnsTrue() {
    Optional<List<String>> doesNotAffect = Optional.empty();
    String facetName = "facet1";

    boolean result =
        GenericDrillSidewaysFacetOperatorBuilder.shouldIncludeOperator(doesNotAffect, facetName);

    assertThat(result).isTrue();
  }

  @Test
  public void shouldIncludeOperator_givenDoesNotAffectWithoutFacetName_returnsTrue() {
    Optional<List<String>> doesNotAffect = Optional.of(Arrays.asList("facet2", "facet3"));
    String facetName = "facet1"; // Not present in doesNotAffect list

    boolean result =
        GenericDrillSidewaysFacetOperatorBuilder.shouldIncludeOperator(doesNotAffect, facetName);

    assertThat(result).isTrue();
  }

  @Test
  public void shouldIncludeOperator_givenDoesNotAffectWithFacetName_returnsFalse() {
    Optional<List<String>> doesNotAffect = Optional.of(Arrays.asList("facet1", "facet2"));
    String facetName = "facet1"; // Present in doesNotAffect list

    boolean result =
        GenericDrillSidewaysFacetOperatorBuilder.shouldIncludeOperator(doesNotAffect, facetName);

    assertThat(result).isFalse();
  }

  @Test
  public void buildGenericQueryOperatorMap_operatorWithoutDoesNotAffect() {
    //   Requested query:
    //   {
    //      "range": {
    //      "path": "size",
    //          "gte": 1,
    //          "lt": 2,
    //          "includesLower": true,
    //          "includesUpper": false,
    //          "doesNotAffect": ["colors"]
    //      }
    //    }
    // Define bounds
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));

    Operator operator =
        OperatorBuilder.range()
            .path("size")
            .numericBounds(lower, upper, true, false)
            .doesNotAffect("colors")
            .build();

    Set<String> facetNames = Set.of("colors", "sizes");

    Map<String, Optional<Operator>> facetOperators =
        buildGenericQueryOperatorMap(operator, facetNames);

    // Assert that all provided facets exist in the map
    assertThat(facetOperators).containsKey("colors");
    assertThat(facetOperators).containsKey("sizes");

    // Validate "sizes" is correctly mapped to the RangeOperator
    assertThat(facetOperators.get("sizes")).isPresent();
    Operator sizesOperator = facetOperators.get("sizes").get();
    assertThat(sizesOperator).isInstanceOf(RangeOperator.class);
    assertRangeOperator(sizesOperator, "size", 1L, 2L);

    // Validate "colors" facet is Optional.empty due to doesNotAffect
    assertThat(facetOperators.get("colors")).isEmpty();
  }

  @Test
  public void buildGenericQueryOperatorMap_operatorWithMustClauseAndShouldClause() {
    // Requested query:
    //    {
    //      "compound": {
    //      "must": [
    //      {
    //        "compound": {
    //        "should": [
    //        {
    //          "range": {
    //          "path": "color",
    //              "gte": 1,
    //              "lt": 2,
    //              "doesNotAffect": ["colors"]
    //        }
    //        },
    //        {
    //          "range": {
    //          "path": "color",
    //              "gte": 3,
    //              "lt": 4,
    //              "doesNotAffect": ["colors"]
    //        }
    //        }
    //          ],
    //        "minimumShouldMatch": 0
    //      }
    //      },
    //      {
    //        "compound": {
    //        "should": [
    //        {
    //          "range": {
    //          "path": "size",
    //              "gte": 5,
    //              "lt": 6,
    //              "doesNotAffect": ["sizes"]
    //        }
    //        },
    //        {
    //          "range": {
    //          "path": "size",
    //              "gte": 6,
    //              "lt": 7,
    //              "doesNotAffect": ["colors", "sizes"]
    //        }
    //        }
    //          ],
    //        "minimumShouldMatch": 0
    //      }
    //      }
    //    ]
    //    }
    //    }

    Map<String, Optional<Operator>> facetOperators =
        buildFacetOperators(this.sampleQuery, this.facetDefinitions);

    CompoundOperator colorFacetOperator =
        (CompoundOperator)
            facetOperators
                .get("colors")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: colors"));

    assertWithMessage("Colors facet operator should exist").that(colorFacetOperator).isNotNull();

    // First must clause should be present (contains size operators)
    List<? extends Operator> mustClauses = colorFacetOperator.must().operators();
    assertWithMessage("Should have one must clause").that(mustClauses.size()).isEqualTo(1);

    // Check the size range operators
    CompoundOperator sizeCompound = (CompoundOperator) mustClauses.get(0);
    List<? extends Operator> shouldClauses = sizeCompound.should().operators();
    assertWithMessage("Should only contain size operator not excluding colors")
        .that(shouldClauses.size())
        .isEqualTo(1);

    // Verify the remaining operator is the first size range operator
    RangeOperator rangeOp = (RangeOperator) shouldClauses.getFirst();
    assertWithMessage("Path should be size")
        .that(rangeOp.paths().getFirst().toString())
        .isEqualTo("size");

    NumericPoint lowerBound = (NumericPoint) rangeOp.bounds().getLower().get();
    assertWithMessage("Lower bound should be 5")
        .that(lowerBound.getLongValueRepresentation())
        .isWithin(1)
        .of((long) 5.0);

    NumericPoint upperBound = (NumericPoint) rangeOp.bounds().getUpper().get();
    assertWithMessage("Upper bound should be 6")
        .that(upperBound.getLongValueRepresentation())
        .isWithin(1)
        .of((long) 6.0);
  }

  @Test
  public void buildGenericQueryOperatorMap_embeddedOperator_generic() {
    //  Requested query :
    //   {
    //      "facet": {
    //      "operator": {
    //        "embeddedDocument": {
    //          "path": "teachers",
    //              "operator": {
    //            "compound": {
    //              "should": [
    //              {
    //                "equals": {
    //                "path": "teachers.first",
    //                    "value": "John",
    //                    "doesNotAffect": ["teachersFirstNameFacet"]
    //              }
    //              },
    //              {
    //                "equals": {
    //                "path": "teachers.last",
    //                    "value": "Smith",
    //                    "doesNotAffect": ["teachersLastNameFacet"]
    //              }
    //              }
    //            ]
    //            }
    //          }
    //        }
    //      },
    //      "facets": {
    //        "teachersFirstNameFacet": {
    //          "type": "string",
    //              "path": "teachers.first",
    //              "numBuckets": 5
    //        },
    //        "teachersLastNameFacet": {
    //          "type": "string",
    //              "path": "teachers.last",
    //              "numBuckets": 5
    //        }
    //      }
    //    }
    //    }

    //  Expected output FacetOperator map:
    //    {
    //      "teachersFirstNameFacet": {
    //      "embeddedDocument": {
    //        "path": "teachers",
    //            "operator": {
    //          "compound": {
    //            "should": [
    //            {
    //              "equals": {
    //              "path": "teachers.last",
    //                  "value": "Smith",
    //                  "doesNotAffect": ["teachersLastNameFacet"]
    //            }
    //            }
    //          ]
    //          }
    //        }
    //      }
    //    },
    //      "teachersLastNameFacet": {
    //      "embeddedDocument": {
    //        "path": "teachers",
    //            "operator": {
    //          "compound": {
    //            "should": [
    //            {
    //              "equals": {
    //              "path": "teachers.first",
    //                  "value": "John",
    //                  "doesNotAffect": ["teachersFirstNameFacet"]
    //            }
    //            }
    //          ]
    //          }
    //        }
    //      }
    //    }
    //    }

    CompoundOperator compoundOp =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.equals()
                    .path("teachers.first")
                    .value("John")
                    .doesNotAffect("teachersFirstNameFacet")
                    .build())
            .should(
                OperatorBuilder.equals()
                    .path("teachers.last")
                    .value("Smith")
                    .doesNotAffect("teachersLastNameFacet")
                    .build())
            .build();

    EmbeddedDocumentOperator operator =
        OperatorBuilder.embeddedDocument().path("teachers").operator(compoundOp).build();

    Set<String> facetNames = Set.of("teachersFirstNameFacet", "teachersLastNameFacet");

    Map<String, Optional<Operator>> facetOperators =
        buildGenericQueryOperatorMap(operator, facetNames);
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
    assertThat(firstNameCompoundOp.should().isPresent()).isTrue();

    // Validate the filtered should clauses for "teachersFirstNameFacet"
    List<? extends Operator> filteredFirstNameShouldClauses =
        firstNameCompoundOp.should().operators().stream()
            .filter(
                op ->
                    op instanceof EqualsOperator equalsOperator
                        && !equalsOperator.doesNotAffect().get().contains("teachersFirstNameFacet"))
            .toList();
    assertThat(filteredFirstNameShouldClauses).hasSize(1);
    assertThat(filteredFirstNameShouldClauses.get(0)).isInstanceOf(EqualsOperator.class);
    EqualsOperator firstNameClause = (EqualsOperator) filteredFirstNameShouldClauses.get(0);
    assertThat(firstNameClause.path().toString()).isEqualTo("teachers.last");
    assertThat(firstNameClause.value()).isEqualTo(ValueBuilder.string("Smith"));
    assertThat(firstNameClause.doesNotAffect().get()).contains("teachersLastNameFacet");

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
    assertThat(lastNameCompoundOp.should().isPresent()).isTrue();

    // Validate the filtered should clauses for "teachersLastNameFacet"
    List<? extends Operator> filteredLastNameShouldClauses =
        lastNameCompoundOp.should().operators().stream()
            .filter(
                op ->
                    op instanceof EqualsOperator equalsOperator
                        && !equalsOperator.doesNotAffect().get().contains("teachersLastNameFacet"))
            .toList();
    assertThat(filteredLastNameShouldClauses).hasSize(1);
    assertThat(filteredLastNameShouldClauses.get(0)).isInstanceOf(EqualsOperator.class);
    EqualsOperator lastNameClause = (EqualsOperator) filteredLastNameShouldClauses.get(0);
    assertThat(lastNameClause.path().toString()).isEqualTo("teachers.first");
    assertThat(lastNameClause.value()).isEqualTo(ValueBuilder.string("John"));
    assertThat(lastNameClause.doesNotAffect().get()).contains("teachersFirstNameFacet");
  }

  @Test
  public void buildGenericQueryOperatorMap_operatorWithMultipleMustClausesAndShouldClauses() {
    // Requested query:
    //    {
    //      "compound": {
    //      "must": [
    //      {
    //        "compound": {
    //        "should": [
    //        {
    //          "range": {
    //          "path": "color",
    //              "gte": 1,
    //              "lt": 2,
    //              "doesNotAffect": ["colors"]
    //        }
    //        },
    //        {
    //          "range": {
    //          "path": "color",
    //              "gte": 3,
    //              "lt": 4,
    //              "doesNotAffect": ["colors"]
    //        }
    //        }
    //          ],
    //        "minimumShouldMatch": 0
    //      }
    //      },
    //      {
    //        "compound": {
    //        "should": [
    //        {
    //          "range": {
    //          "path": "size",
    //              "gte": 5,
    //              "lt": 6,
    //              "doesNotAffect": ["sizes"]
    //        }
    //        },
    //        {
    //          "range": {
    //          "path": "size",
    //              "gte": 6,
    //              "lt": 7,
    //              "doesNotAffect": ["colors", "sizes"]
    //        }
    //        }
    //          ],
    //        "minimumShouldMatch": 0
    //      }
    //      }
    //    ]
    //    }
    //    }

    Map<String, Optional<Operator>> facetOperators =
        buildFacetOperators(this.sampleQuery, this.facetDefinitions);

    CompoundOperator sizeFacetOperator =
        (CompoundOperator)
            facetOperators
                .get("sizes")
                .orElseThrow(
                    () -> new IllegalStateException("No operator present for facet: sizes"));

    assertWithMessage("Sizes facet operator should exist").that(sizeFacetOperator).isNotNull();

    // Check must clauses (should contain only color compound)
    List<? extends Operator> mustClauses = sizeFacetOperator.must().operators();
    assertWithMessage("Should have one must clause").that(mustClauses.size()).isEqualTo(1);

    // Check the color range operators
    CompoundOperator colorCompound = (CompoundOperator) mustClauses.getFirst();
    List<? extends Operator> shouldClauses = colorCompound.should().operators();
    assertWithMessage("Should contain both color operators")
        .that(shouldClauses.size())
        .isEqualTo(2);

    // Verify first color range operator
    RangeOperator firstRange = (RangeOperator) shouldClauses.get(0);
    assertWithMessage("Path should be color")
        .that(firstRange.paths().getFirst().toString())
        .isEqualTo("color");

    NumericPoint lowerBound = (NumericPoint) firstRange.bounds().getLower().get();
    assertWithMessage("First range lower bound should be 1")
        .that(lowerBound.getLongValueRepresentation())
        .isWithin(1)
        .of((long) 1.0);

    NumericPoint upperBound = (NumericPoint) firstRange.bounds().getUpper().get();
    assertWithMessage("First range upper bound should be 2")
        .that(upperBound.getLongValueRepresentation())
        .isWithin(1)
        .of((long) 2.0);

    // Verify second color range operator
    RangeOperator secondRange = (RangeOperator) shouldClauses.get(1);
    assertWithMessage("Path should be color")
        .that(secondRange.paths().getFirst().toString())
        .isEqualTo("color");

    NumericPoint lowerBound2 = (NumericPoint) secondRange.bounds().getLower().get();
    assertWithMessage("Second range lower bound should be 3")
        .that(lowerBound2.getLongValueRepresentation())
        .isWithin(1)
        .of((long) 3.0);

    NumericPoint upperBound2 = (NumericPoint) secondRange.bounds().getUpper().get();
    assertWithMessage("Second range upper bound should be 4")
        .that(upperBound2.getLongValueRepresentation())
        .isWithin(1)
        .of((long) 4.0);
  }

  @Test
  public void buildGenericQueryOperatorMap_emptyFacetDefinitions() {
    // Requested query:
    //    {
    //      "compound": {
    //      "must": [
    //      {
    //        "compound": {
    //        "should": [
    //        {
    //          "range": {
    //          "path": "color",
    //              "gte": 1,
    //              "lt": 2,
    //              "doesNotAffect": ["colors"]
    //        }
    //        },
    //        {
    //          "range": {
    //          "path": "color",
    //              "gte": 3,
    //              "lt": 4,
    //              "doesNotAffect": ["colors"]
    //        }
    //        }
    //          ],
    //        "minimumShouldMatch": 0
    //      }
    //      },
    //      {
    //        "compound": {
    //        "should": [
    //        {
    //          "range": {
    //          "path": "size",
    //              "gte": 5,
    //              "lt": 6,
    //              "doesNotAffect": ["sizes"]
    //        }
    //        },
    //        {
    //          "range": {
    //          "path": "size",
    //              "gte": 6,
    //              "lt": 7,
    //              "doesNotAffect": ["colors", "sizes"]
    //        }
    //        }
    //          ],
    //        "minimumShouldMatch": 0
    //      }
    //      }
    //    ]
    //    }
    //    }

    Map<String, Optional<Operator>> facetOperators =
        buildFacetOperators(this.sampleQuery, Collections.emptyMap());
    assertWithMessage("Should return empty map for empty facet definitions")
        .that(facetOperators)
        .isEmpty();
  }

  private CompoundOperator buildSampleQuery() {
    Optional<NumericPoint> lower = Optional.of(new LongPoint(1L));
    Optional<NumericPoint> upper = Optional.of(new LongPoint(2L));
    Optional<NumericPoint> lower2 = Optional.of(new LongPoint(3L));
    Optional<NumericPoint> upper2 = Optional.of(new LongPoint(4L));

    // Build the inner compound operators for color clauses
    CompoundOperator colorCompound =
        OperatorBuilder.compound()
            .score(Score.defaultScore())
            .should(
                OperatorBuilder.range()
                    .score(Score.defaultScore())
                    .path("color")
                    .numericBounds(lower, upper, true, false)
                    .doesNotAffect("colors")
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("color")
                    .numericBounds(lower2, upper2, true, false)
                    .doesNotAffect("colors")
                    .build())
            .minimumShouldMatch(0)
            .build();

    Optional<NumericPoint> lower3 = Optional.of(new LongPoint(5L));
    Optional<NumericPoint> upper3 = Optional.of(new LongPoint(6L));
    Optional<NumericPoint> lower4 = Optional.of(new LongPoint(7L));

    // Build the inner compound operators for size clauses
    CompoundOperator sizeCompound =
        OperatorBuilder.compound()
            .score(Score.defaultScore())
            .should(
                OperatorBuilder.range()
                    .score(Score.defaultScore())
                    .path("size")
                    .numericBounds(lower3, upper3, true, false)
                    .doesNotAffect("sizes")
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("size")
                    .numericBounds(upper3, lower4, true, false)
                    .doesNotAffect(Arrays.asList("colors", "sizes"))
                    .build())
            .minimumShouldMatch(0)
            .build();

    // Build the outer compound operator
    return OperatorBuilder.compound()
        .score(Score.defaultScore())
        .must(colorCompound)
        .must(sizeCompound)
        .minimumShouldMatch(0)
        .build();
  }

  private Map<String, FacetDefinition> buildFacetDefinitions() {
    Map<String, FacetDefinition> definitions = new HashMap<>();

    // Add color facet definition
    definitions.put(
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
    definitions.put(
        "sizes",
        new FacetDefinition.NumericFacetDefinition(
            "size",
            Optional.empty(),
            List.of(
                new BsonDouble(5.0),
                new BsonDouble(6.0),
                new BsonDouble(7.0),
                new BsonDouble(8.0))));

    return definitions;
  }

  private static Map<String, Optional<Operator>> buildFacetOperators(
      Operator operator, Map<String, FacetDefinition> facetDefinitions) {
    Set<String> facetNames = facetDefinitions.keySet();
    return buildGenericQueryOperatorMap(operator, facetNames);
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
