package com.xgen.mongot.index;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;

import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.OperatorQueryBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import org.junit.Test;

public class QueryShapeTest {

  @Test
  public void of_leafOperator_rendersOperatorType() {
    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.text().path("notes").query("hello").build())
            .returnStoredSource(false)
            .build();

    String shape = QueryShape.of(query);

    assertThat(shape).isEqualTo("text");
  }

  @Test
  public void of_compound_rendersEachClauseByName() {
    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.compound()
                    .must(OperatorBuilder.text().path("notes").query("hello").build())
                    .mustNot(OperatorBuilder.exists().path("archived").build())
                    .filter(OperatorBuilder.equals().path("token").value("abc").build())
                    .build())
            .returnStoredSource(false)
            .build();

    String shape = QueryShape.of(query);

    assertThat(shape).isEqualTo("compound[must:text, mustNot:exists, filter:equals]");
  }

  @Test
  public void of_severalOperatorsInOneClause_pipeSeparatesThem() {
    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.compound()
                    .should(OperatorBuilder.text().path("a").query("x").build())
                    .should(OperatorBuilder.term().path("b").query("y").build())
                    .build())
            .returnStoredSource(false)
            .build();

    String shape = QueryShape.of(query);

    assertThat(shape).isEqualTo("compound[should:text|term]");
  }

  @Test
  public void of_nestedCompounds_preservesNesting() {
    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.compound()
                    .should(
                        OperatorBuilder.compound()
                            .filter(OperatorBuilder.term().path("b").query("y").build())
                            .build())
                    .build())
            .returnStoredSource(false)
            .build();

    String shape = QueryShape.of(query);

    assertThat(shape).isEqualTo("compound[should:compound[filter:term]]");
  }

  @Test
  public void of_nestingBeyondMaxDepth_collapsesRemainder() {
    // Five levels of compound. The user controls nesting depth, so rendering has to stop somewhere
    // rather than let one query dictate how much log volume it produces. The innermost text
    // operator sits past the cap and is dropped.
    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.compound()
                    .must(
                        OperatorBuilder.compound()
                            .must(
                                OperatorBuilder.compound()
                                    .must(
                                        OperatorBuilder.compound()
                                            .must(
                                                OperatorBuilder.compound()
                                                    .must(
                                                        OperatorBuilder.text()
                                                            .path("notes")
                                                            .query("deep")
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .returnStoredSource(false)
            .build();

    String shape = QueryShape.of(query);

    assertThat(shape)
        .isEqualTo("compound[must:compound[must:compound[must:compound[must:compound[...]]]]]");
  }

  @Test
  public void of_clauseWiderThanMax_summarisesRemainder() {
    // Nine operators in one clause: eight rendered, the ninth summarised.
    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.compound()
                    .should(OperatorBuilder.text().path("p").query("q1").build())
                    .should(OperatorBuilder.text().path("p").query("q2").build())
                    .should(OperatorBuilder.text().path("p").query("q3").build())
                    .should(OperatorBuilder.text().path("p").query("q4").build())
                    .should(OperatorBuilder.text().path("p").query("q5").build())
                    .should(OperatorBuilder.text().path("p").query("q6").build())
                    .should(OperatorBuilder.text().path("p").query("q7").build())
                    .should(OperatorBuilder.text().path("p").query("q8").build())
                    .should(OperatorBuilder.text().path("p").query("q9").build())
                    .build())
            .returnStoredSource(false)
            .build();

    String shape = QueryShape.of(query);

    assertThat(shape).isEqualTo("compound[should:text|text|text|text|text|text|text|text|+1 more]");
  }

  @Test
  public void of_vectorQuery_rendersSearchType() {
    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("testIndex")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.newRoot("embedding"))
                    .queryVector(Vector.fromFloats(new float[] {1.f, 2.f}, NATIVE))
                    .numCandidates(100)
                    .limit(10)
                    .build())
            .build();

    String shape = QueryShape.of(query);

    assertThat(shape).isEqualTo("vectorSearch[approximate]");
  }

  @Test
  public void of_userSuppliedValues_omitsThem() {
    // The reason this class exists: the rendered shape is written to an operator-readable service
    // log, so it must never carry anything the customer supplied.
    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.compound()
                    .must(
                        OperatorBuilder.text()
                            .path("CANARY-medicalRecordNumber")
                            .query("CANARY-SSN-123-45-6789")
                            .build())
                    .filter(
                        OperatorBuilder.equals()
                            .path("CANARY-medicalRecordNumber")
                            .value("CANARY-patient@example.com")
                            .build())
                    .build())
            .returnStoredSource(false)
            .build();

    String shape = QueryShape.of(query);

    assertThat(shape).doesNotContain("CANARY");
    assertThat(shape).isEqualTo("compound[must:text, filter:equals]");
  }

  @Test
  public void of_vectorQuery_omitsQueryVectorAndPath() {
    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("testIndex")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.newRoot("embedding"))
                    .queryVector(Vector.fromFloats(new float[] {0.42f, 0.99f}, NATIVE))
                    .numCandidates(100)
                    .limit(10)
                    .build())
            .build();

    String shape = QueryShape.of(query);

    assertThat(shape).doesNotContain("0.42");
    assertThat(shape).doesNotContain("0.99");
    assertThat(shape).doesNotContain("embedding");
  }
}
