package com.xgen.mongot.server.command.search.definition.request;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.CursorOptionsDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.ExplainDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.VectorSearchCommandDefinitionBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class VectorSearchCommandDefinitionTest {
  private static final String SUITE_NAME = "vector-search";
  private static final BsonDeserializationTestSuite<VectorSearchCommandDefinition> TEST_SUITE =
      fromDocument(
          "src/test/unit/resources/server/command/search/definition/request",
          SUITE_NAME,
          doc -> VectorSearchCommandDefinition.fromBson(doc, true));

  private final BsonDeserializationTestSuite.TestSpecWrapper<VectorSearchCommandDefinition>
      testSpec;

  public VectorSearchCommandDefinitionTest(
      BsonDeserializationTestSuite.TestSpecWrapper<VectorSearchCommandDefinition> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<
          BsonDeserializationTestSuite.TestSpecWrapper<VectorSearchCommandDefinition>>
      data() throws BsonParseException {
    return TEST_SUITE.withExamples(
        vectorSearch(),
        vectorSearchOnView(),
        vectorSearchWithExplain(),
        vectorSearchWithGtFilter(),
        vectorSearchWithGtLtFilter(),
        vectorSearchWithDeadline());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  @Test
  public void testCursorOptionsRoundTripThroughBson() throws Exception {
    VectorSearchCommandDefinition definition =
        VectorSearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .vectorSearchQuery(
                VectorQueryBuilder.builder()
                    .index("vecSearch")
                    .criteria(
                        ApproximateVectorQueryCriteriaBuilder.builder()
                            .path(FieldPath.parse("vec"))
                            .limit(100)
                            .numCandidates(100)
                            .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                            .build())
                    .build())
            .cursorOptions(
                CursorOptionsDefinitionBuilder.builder()
                    .batchSize(25)
                    .requireSequenceTokens(false)
                    .build())
            .build();

    VectorSearchCommandDefinition roundTripped;
    try (var parser = BsonDocumentParser.fromRoot(definition.toBson()).build()) {
      roundTripped = VectorSearchCommandDefinition.fromBson(parser, true);
    }

    Assert.assertEquals(definition.cursorOptions(), roundTripped.cursorOptions());
    Assert.assertEquals(definition.toBson(), roundTripped.toBson());
  }

  @Test
  public void testExpectedModelRoundTripThroughBson() throws Exception {
    VectorSearchCommandDefinition definition =
        VectorSearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .vectorSearchQuery(
                VectorQueryBuilder.builder()
                    .index("vecSearch")
                    .criteria(
                        ApproximateVectorQueryCriteriaBuilder.builder()
                            .path(FieldPath.parse("vec"))
                            .limit(100)
                            .numCandidates(100)
                            .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                            .build())
                    .build())
            .expectedModel("voyage-3-large")
            .build();

    VectorSearchCommandDefinition roundTripped;
    try (var parser = BsonDocumentParser.fromRoot(definition.toBson()).build()) {
      roundTripped = VectorSearchCommandDefinition.fromBson(parser, true);
    }

    Assert.assertEquals("voyage-3-large", roundTripped.expectedModel().get());
    Assert.assertEquals(definition.toBson(), roundTripped.toBson());
  }

  private static BsonDeserializationTestSuite.ValidSpec<VectorSearchCommandDefinition>
      vectorSearch() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "vector-search",
        VectorSearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .vectorSearchQuery(
                VectorQueryBuilder.builder()
                    .index("vecSearch")
                    .criteria(
                        ApproximateVectorQueryCriteriaBuilder.builder()
                            .path(FieldPath.parse("vec"))
                            .limit(100)
                            .numCandidates(100)
                            .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                            .build())
                    .build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<VectorSearchCommandDefinition>
      vectorSearchOnView() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "vector-search-on-view",
        VectorSearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .viewName("my-view")
            .vectorSearchQuery(
                VectorQueryBuilder.builder()
                    .index("vecSearch")
                    .criteria(
                        ApproximateVectorQueryCriteriaBuilder.builder()
                            .path(FieldPath.parse("vec"))
                            .limit(100)
                            .numCandidates(100)
                            .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                            .build())
                    .build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<VectorSearchCommandDefinition>
      vectorSearchWithExplain() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "vector-search-with-explain",
        VectorSearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .vectorSearchQuery(
                VectorQueryBuilder.builder()
                    .index("vecSearch")
                    .criteria(
                        ApproximateVectorQueryCriteriaBuilder.builder()
                            .path(FieldPath.parse("vec"))
                            .limit(100)
                            .numCandidates(100)
                            .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                            .build())
                    .build())
            .explain(
                ExplainDefinitionBuilder.builder()
                    .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                    .build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<VectorSearchCommandDefinition>
      vectorSearchWithGtFilter() throws BsonParseException {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "vector-search-with-filter-gt",
        VectorSearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .vectorSearchQuery(
                VectorQueryBuilder.builder()
                    .index("vecSearch")
                    .criteria(
                        ApproximateVectorQueryCriteriaBuilder.builder()
                            .path(FieldPath.parse("vec"))
                            .limit(100)
                            .numCandidates(100)
                            .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                            .filter(
                                new VectorSearchFilter.ClauseFilter(
                                    ClauseBuilder.simpleClause()
                                        .path(FieldPath.parse("cost"))
                                        .addOperator(
                                            MqlFilterOperatorBuilder.gt()
                                                .value(ValueBuilder.intNumber(70))
                                                .build())
                                        .build()))
                            .build())
                    .build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<VectorSearchCommandDefinition>
      vectorSearchWithGtLtFilter() throws BsonParseException {

    List<MqlFilterOperator> operators = new ArrayList<>();
    operators.add(MqlFilterOperatorBuilder.gt().value(ValueBuilder.intNumber(70)).build());
    operators.add(MqlFilterOperatorBuilder.lt().value(ValueBuilder.intNumber(120)).build());

    return BsonDeserializationTestSuite.TestSpec.valid(
        "vector-search-with-filter-gt-lt",
        VectorSearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .vectorSearchQuery(
                VectorQueryBuilder.builder()
                    .index("vecSearch")
                    .criteria(
                        ApproximateVectorQueryCriteriaBuilder.builder()
                            .path(FieldPath.parse("vec"))
                            .limit(100)
                            .numCandidates(100)
                            .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                            .filter(
                                new VectorSearchFilter.ClauseFilter(
                                    ClauseBuilder.simpleClause()
                                        .path(FieldPath.parse("cost"))
                                        .operators(operators)
                                        .build()))
                            .build())
                    .build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<VectorSearchCommandDefinition>
      vectorSearchWithDeadline() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "vector-search-with-deadline",
        VectorSearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .vectorSearchQuery(
                VectorQueryBuilder.builder()
                    .index("vecSearch")
                    .criteria(
                        ApproximateVectorQueryCriteriaBuilder.builder()
                            .path(FieldPath.parse("vec"))
                            .limit(100)
                            .numCandidates(100)
                            .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                            .build())
                    .build())
            .deadlineTimestampMs(1234567890123L)
            .build());
  }
}
