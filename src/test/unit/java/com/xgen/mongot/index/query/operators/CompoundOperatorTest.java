package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CompoundOperatorTest {

  private static final String SUITE_NAME = "compound";
  private static final BsonDeserializationTestSuite<CompoundOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, CompoundOperator::fromBson);

  private final TestSpecWrapper<CompoundOperator> testSpec;

  public CompoundOperatorTest(TestSpecWrapper<CompoundOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<CompoundOperator>> data() {
    return TEST_SUITE.withExamples(
        doesNotAffectFilter(),
        doesNotAffectShould(),
        doesNotAffectMust(),
        doesNotAffectMustNot(),
        doesNotAffectEmptyArray(),
        doesNotAffectString(),
        doesNotAffectNested(),
        doesNotAffectNestedOuterLevel(),
        doesNotAffectBothLevels(),
        doesNotAffectMultiInterior(),
        doesNotAffectOuterCompoundLevel(),
        doesNotAffectInnerOperatorLevel(),
        singleFilter(),
        multipleFilters(),
        singleMust(),
        multipleMusts(),
        singleMustNot(),
        multipleMustNots(),
        singleShould(),
        multipleShoulds(),
        allClauses(),
        minimumShouldMatchZero(),
        minimumShouldMatchLessThanClauses(),
        minimumShouldMatchSameAsClauses(),
        withBoostScore(),
        withDismaxScore());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<CompoundOperator> doesNotAffectFilter() {
    return TestSpec.valid(
        "doesNotAffectFilter",
        OperatorBuilder.compound()
            .filter(OperatorBuilder.text().path("size").query("S").build())
            .doesNotAffect(List.of("points", "sizes"))
            .build());
  }

  private static ValidSpec<CompoundOperator> doesNotAffectShould() {
    return TestSpec.valid(
        "doesNotAffectShould",
        OperatorBuilder.compound()
            .should(OperatorBuilder.text().path("size").query("S").build())
            .doesNotAffect(List.of("points", "sizes"))
            .build());
  }

  private static ValidSpec<CompoundOperator> doesNotAffectMust() {
    return TestSpec.valid(
        "doesNotAffectMust",
        OperatorBuilder.compound()
            .must(OperatorBuilder.text().path("size").query("S").build())
            .doesNotAffect(List.of("points", "sizes"))
            .build());
  }

  private static ValidSpec<CompoundOperator> doesNotAffectMustNot() {
    return TestSpec.valid(
        "doesNotAffectMustNot",
        OperatorBuilder.compound()
            .mustNot(OperatorBuilder.text().path("size").query("S").build())
            .doesNotAffect(List.of("points", "sizes"))
            .build());
  }

  private static ValidSpec<CompoundOperator> doesNotAffectEmptyArray() {
    return TestSpec.valid(
        "doesNotAffectEmptyArray",
        OperatorBuilder.compound()
            .should(OperatorBuilder.text().path("size").query("S").build())
            .doesNotAffect(List.of())
            .build());
  }

  private static ValidSpec<CompoundOperator> doesNotAffectString() {
    return TestSpec.valid(
        "doesNotAffectString",
        OperatorBuilder.compound()
            .should(OperatorBuilder.text().path("size").query("S").build())
            .doesNotAffect("sizes")
            .build());
  }

  private static ValidSpec<CompoundOperator> doesNotAffectNested() {
    return TestSpec.valid(
        "doesNotAffectNested",
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.equals()
                    .path("size")
                    .value("S")
                    .doesNotAffect(List.of("points", "sizes"))
                    .build())
            .build());
  }

  private static ValidSpec<CompoundOperator> doesNotAffectNestedOuterLevel() {
    return TestSpec.valid(
        "doesNotAffectNestedOuterLevel",
        OperatorBuilder.compound()
            .should(OperatorBuilder.equals().path("size").value("S").build())
            .doesNotAffect(List.of("points", "sizes"))
            .build());
  }

  private static ValidSpec<CompoundOperator> doesNotAffectBothLevels() {
    return TestSpec.valid(
        "doesNotAffectBothLevels",
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.equals()
                    .path("size")
                    .value("S")
                    .doesNotAffect(List.of("sizes"))
                    .build())
            .doesNotAffect(List.of("points", "sizes"))
            .build());
  }

  private static ValidSpec<CompoundOperator> doesNotAffectMultiInterior() {
    return TestSpec.valid(
        "doesNotAffectMultiInterior",
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.range()
                    .path("rating")
                    .numericBounds(
                        Optional.of(new DoublePoint(4d)),
                        Optional.of(new DoublePoint(5d)),
                        true,
                        false)
                    .doesNotAffect(List.of("ratings"))
                    .build())
            .should(
                OperatorBuilder.range()
                    .path("rating")
                    .numericBounds(
                        Optional.of(new DoublePoint(6d)),
                        Optional.of(new DoublePoint(7d)),
                        true,
                        false)
                    .doesNotAffect(List.of("ratings"))
                    .build())
            .doesNotAffect(List.of("firmnesses"))
            .build());
  }

  private static ValidSpec<CompoundOperator> doesNotAffectOuterCompoundLevel() {
    return TestSpec.valid(
        "doesNotAffectOuterCompoundLevel",
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.compound()
                    .should(
                        OperatorBuilder.range()
                            .path("pricePerNight")
                            .numericBounds(
                                Optional.of(new DoublePoint(0d)),
                                Optional.of(new DoublePoint(100d)),
                                true,
                                false)
                            .build())
                    .should(
                        OperatorBuilder.range()
                            .path("pricePerNight")
                            .numericBounds(
                                Optional.of(new DoublePoint(300d)),
                                Optional.of(new DoublePoint(400d)),
                                true,
                                false)
                            .build())
                    .should(
                        OperatorBuilder.range()
                            .path("pricePerNight")
                            .numericBounds(
                                Optional.of(new DoublePoint(500d)),
                                Optional.of(new DoublePoint(1000d)),
                                true,
                                false)
                            .build())
                    .doesNotAffect(List.of("pricePerNightFacet"))
                    .minimumShouldMatch(1)
                    .build())
            .build());
  }

  private static ValidSpec<CompoundOperator> doesNotAffectInnerOperatorLevel() {
    return TestSpec.valid(
        "doesNotAffectInnerOperatorLevel",
        OperatorBuilder.compound()
            .filter(
                OperatorBuilder.compound()
                    .should(
                        OperatorBuilder.range()
                            .path("pricePerNight")
                            .numericBounds(
                                Optional.of(new DoublePoint(0d)),
                                Optional.of(new DoublePoint(100d)),
                                true,
                                false)
                            .doesNotAffect(List.of("pricePerNightFacet"))
                            .build())
                    .should(
                        OperatorBuilder.range()
                            .path("pricePerNight")
                            .numericBounds(
                                Optional.of(new DoublePoint(300d)),
                                Optional.of(new DoublePoint(400d)),
                                true,
                                false)
                            .doesNotAffect(List.of("pricePerNightFacet"))
                            .build())
                    .should(
                        OperatorBuilder.range()
                            .path("pricePerNight")
                            .numericBounds(
                                Optional.of(new DoublePoint(500d)),
                                Optional.of(new DoublePoint(1000d)),
                                true,
                                false)
                            .doesNotAffect(List.of("pricePerNightFacet"))
                            .build())
                    .minimumShouldMatch(1)
                    .build())
            .build());
  }

  private static ValidSpec<CompoundOperator> singleFilter() {
    return TestSpec.valid(
        "single filter",
        OperatorBuilder.compound()
            .filter(OperatorBuilder.text().path("title").query("godfather").build())
            .build());
  }

  private static ValidSpec<CompoundOperator> multipleFilters() {
    return TestSpec.valid(
        "multiple filters",
        OperatorBuilder.compound()
            .filter(OperatorBuilder.text().path("title").query("first").build())
            .filter(OperatorBuilder.text().path("title").query("second").build())
            .build());
  }

  private static ValidSpec<CompoundOperator> singleMust() {
    return TestSpec.valid(
        "single must",
        OperatorBuilder.compound()
            .must(OperatorBuilder.text().path("title").query("godfather").build())
            .build());
  }

  private static ValidSpec<CompoundOperator> multipleMusts() {
    return TestSpec.valid(
        "multiple musts",
        OperatorBuilder.compound()
            .must(OperatorBuilder.text().path("title").query("first").build())
            .must(OperatorBuilder.text().path("title").query("second").build())
            .build());
  }

  private static ValidSpec<CompoundOperator> singleMustNot() {
    return TestSpec.valid(
        "single mustNot",
        OperatorBuilder.compound()
            .mustNot(OperatorBuilder.text().path("title").query("godfather").build())
            .build());
  }

  private static ValidSpec<CompoundOperator> multipleMustNots() {
    return TestSpec.valid(
        "multiple mustNots",
        OperatorBuilder.compound()
            .mustNot(OperatorBuilder.text().path("title").query("first").build())
            .mustNot(OperatorBuilder.text().path("title").query("second").build())
            .build());
  }

  private static ValidSpec<CompoundOperator> singleShould() {
    return TestSpec.valid(
        "single should",
        OperatorBuilder.compound()
            .should(OperatorBuilder.text().path("title").query("godfather").build())
            .build());
  }

  private static ValidSpec<CompoundOperator> multipleShoulds() {
    return TestSpec.valid(
        "multiple shoulds",
        OperatorBuilder.compound()
            .should(OperatorBuilder.text().path("title").query("first").build())
            .should(OperatorBuilder.text().path("title").query("second").build())
            .build());
  }

  private static ValidSpec<CompoundOperator> allClauses() {
    return TestSpec.valid(
        "all clauses",
        OperatorBuilder.compound()
            .filter(OperatorBuilder.text().path("title").query("first").build())
            .must(OperatorBuilder.text().path("title").query("second").build())
            .mustNot(OperatorBuilder.text().path("title").query("third").build())
            .should(OperatorBuilder.text().path("title").query("fourth").build())
            .build());
  }

  private static ValidSpec<CompoundOperator> minimumShouldMatchZero() {
    return TestSpec.valid(
        "minimumShouldMatch zero",
        OperatorBuilder.compound()
            .should(OperatorBuilder.text().path("title").query("first").build())
            .should(OperatorBuilder.text().path("title").query("second").build())
            .should(OperatorBuilder.text().path("title").query("third").build())
            .minimumShouldMatch(0)
            .build());
  }

  private static ValidSpec<CompoundOperator> minimumShouldMatchLessThanClauses() {
    return TestSpec.valid(
        "minimumShouldMatch less than clauses",
        OperatorBuilder.compound()
            .should(OperatorBuilder.text().path("title").query("first").build())
            .should(OperatorBuilder.text().path("title").query("second").build())
            .should(OperatorBuilder.text().path("title").query("third").build())
            .minimumShouldMatch(2)
            .build());
  }

  private static ValidSpec<CompoundOperator> minimumShouldMatchSameAsClauses() {
    return TestSpec.valid(
        "minimumShouldMatch same as clauses",
        OperatorBuilder.compound()
            .should(OperatorBuilder.text().path("title").query("first").build())
            .should(OperatorBuilder.text().path("title").query("second").build())
            .should(OperatorBuilder.text().path("title").query("third").build())
            .minimumShouldMatch(3)
            .build());
  }

  private static ValidSpec<CompoundOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        OperatorBuilder.compound()
            .filter(OperatorBuilder.text().path("title").query("godfather").build())
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static ValidSpec<CompoundOperator> withDismaxScore() {
    return TestSpec.valid(
        "with dismax score",
        OperatorBuilder.compound()
            .should(OperatorBuilder.text().path("title").query("godfather").build())
            .score(ScoreBuilder.dismax().tieBreakerScore(0.5f).build())
            .build());
  }
}
