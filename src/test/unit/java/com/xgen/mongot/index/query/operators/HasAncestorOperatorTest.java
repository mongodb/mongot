package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import com.xgen.testing.mongot.index.query.scores.expressions.MultiplyExpressionBuilder;
import com.xgen.testing.mongot.index.query.scores.expressions.PathExpressionBuilder;
import com.xgen.testing.mongot.index.query.scores.expressions.ScoreExpressionBuilder;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class HasAncestorOperatorTest {
  private static final String SUITE_NAME = "hasAncestor";
  private static final BsonDeserializationTestSuite<HasAncestorOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, HasAncestorOperator::fromBson);

  private final TestSpecWrapper<HasAncestorOperator> testSpec;

  public HasAncestorOperatorTest(TestSpecWrapper<HasAncestorOperator> testSpec) {
    this.testSpec = testSpec;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<HasAncestorOperator>> data() {
    return TEST_SUITE.withExamples(
        simple(),
        withCompound(),
        withCompoundAndScoring(),
        withNestedEmbedded(),
        withNestedAndCompound(),
        nestedWithCompoundButUnmatchedAncestorPath(),
        withNestedHasRoot());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<HasAncestorOperator> simple() {
    return TestSpec.valid(
        "simple",
        OperatorBuilder.hasAncestor()
            .ancestorPath("movies")
            .operator(OperatorBuilder.text().path("movies.title").query("Split").build())
            .build());
  }

  private static ValidSpec<HasAncestorOperator> withCompound() {
    return TestSpec.valid(
        "withCompound",
        OperatorBuilder.hasAncestor()
            .ancestorPath("movies")
            .operator(
                OperatorBuilder.compound()
                    .must(OperatorBuilder.text().path("movies.title").query("Split").build())
                    .should(OperatorBuilder.equals().path("movies.year").value(1999).build())
                    .build())
            .build());
  }

  private static ValidSpec<HasAncestorOperator> withNestedEmbedded() {
    return TestSpec.valid(
        "withNestedEmbedded",
        OperatorBuilder.hasAncestor()
            .ancestorPath("movies")
            .operator(
                OperatorBuilder.embeddedDocument()
                    .path("movies.reviews")
                    .operator(
                        OperatorBuilder.hasAncestor()
                            .ancestorPath("movies")
                            .operator(
                                OperatorBuilder.embeddedDocument()
                                    .path("movies.reviews.comments")
                                    .operator(
                                        OperatorBuilder.hasAncestor()
                                            .ancestorPath("movies.reviews")
                                            .operator(
                                                OperatorBuilder.compound()
                                                    .must(
                                                        OperatorBuilder.text()
                                                            .path("movies.reviews.comments.author")
                                                            .query("DJT")
                                                            .build())
                                                    .should(
                                                        OperatorBuilder.equals()
                                                            .path("movies.reviews.comments.year")
                                                            .value(1999)
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build());
  }

  private static ValidSpec<HasAncestorOperator> withCompoundAndScoring() {
    return TestSpec.valid(
        "withCompoundAndScoring",
        OperatorBuilder.hasAncestor()
            .ancestorPath("movies")
            .operator(
                OperatorBuilder.compound()
                    .must(OperatorBuilder.text().path("movies.title").query("Split").build())
                    .should(OperatorBuilder.equals().path("movies.year").value(1999).build())
                    .build())
            .score(
                ScoreBuilder.function()
                    .expression(
                        MultiplyExpressionBuilder.builder()
                            .arg(ScoreExpressionBuilder.builder().build())
                            .arg(
                                PathExpressionBuilder.builder().value("movies.imdb.rating").build())
                            .build())
                    .build())
            .build());
  }

  private static ValidSpec<HasAncestorOperator> withNestedAndCompound() {
    return TestSpec.valid(
        "nestedWithCompound",
        OperatorBuilder.hasAncestor()
            .ancestorPath("movies")
            .operator(
                OperatorBuilder.compound()
                    .must(
                        OperatorBuilder.hasAncestor()
                            .ancestorPath("movies.reviews.content")
                            .operator(
                                OperatorBuilder.equals()
                                    .path("movies.reviews.content.date")
                                    .value(
                                        new Calendar.Builder()
                                            .setDate(2022, Calendar.DECEMBER, 6)
                                            .setTimeOfDay(13, 13, 13)
                                            .setTimeZone(TimeZone.getTimeZone("UTC"))
                                            .build()
                                            .getTime())
                                    .build())
                            .build())
                    .build())
            .build());
  }

  /*
   * This test is to ensure that HasAncestorOperator deserialization can handle a nested
   * compound operator where the ancestor path does not match the operator's path.
   * This is not a valid case, but we are not going to throw errors in the syntax validation phase.
   * Such error will be caught in the query factory.
   * */
  private static ValidSpec<HasAncestorOperator> nestedWithCompoundButUnmatchedAncestorPath() {
    return TestSpec.valid(
        "nestedWithCompoundButUnmatchedAncestorPath",
        OperatorBuilder.hasAncestor()
            .ancestorPath("movies.director")
            .operator(
                OperatorBuilder.compound()
                    .must(
                        OperatorBuilder.hasAncestor()
                            .ancestorPath("movies.reviews.content")
                            .operator(
                                OperatorBuilder.equals()
                                    .path("movies.reviews.content.date")
                                    .value(
                                        new Calendar.Builder()
                                            .setDate(2022, Calendar.DECEMBER, 6)
                                            .setTimeOfDay(13, 13, 13)
                                            .setTimeZone(TimeZone.getTimeZone("UTC"))
                                            .build()
                                            .getTime())
                                    .build())
                            .build())
                    .build())
            .build());
  }

  private static ValidSpec<HasAncestorOperator> withNestedHasRoot() {
    return TestSpec.valid(
        "withNestedHasRoot",
        OperatorBuilder.hasAncestor()
            .ancestorPath("movies")
            .operator(
                OperatorBuilder.hasRoot()
                    .operator(
                        OperatorBuilder.text()
                            .path("movies.reviews.content.comments.author.name")
                            .query("DJT")
                            .build())
                    .build())
            .build());
  }
}
