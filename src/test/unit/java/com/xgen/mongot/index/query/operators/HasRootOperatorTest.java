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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class HasRootOperatorTest {
  private static final String SUITE_NAME = "hasRoot";
  private static final BsonDeserializationTestSuite<HasRootOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, HasRootOperator::fromBson);

  private final TestSpecWrapper<HasRootOperator> testSpec;

  public HasRootOperatorTest(TestSpecWrapper<HasRootOperator> testSpec) {
    this.testSpec = testSpec;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<HasRootOperator>> data() {
    return TEST_SUITE.withExamples(simple(), withCompoundAndScoring(), withNestedEmbedded());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<HasRootOperator> simple() {
    return TestSpec.valid(
        "simple",
        OperatorBuilder.hasRoot()
            .operator(OperatorBuilder.text().path("movies.title").query("Split").build())
            .build());
  }

  private static ValidSpec<HasRootOperator> withCompoundAndScoring() {
    return TestSpec.valid(
        "withCompoundAndScoring",
        OperatorBuilder.hasRoot()
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

  private static ValidSpec<HasRootOperator> withNestedEmbedded() {
    return TestSpec.valid(
        "withNestedEmbedded",
        OperatorBuilder.hasRoot()
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
                                        OperatorBuilder.hasRoot()
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
}
