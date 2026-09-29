package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.ExactVectorCriteriaBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      VectorSearchOperatorTest.DeserializationTest.class,
      VectorSearchOperatorTest.SerializationTest.class,
    })
public class VectorSearchOperatorTest {
  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "vectorSearch-deserialization";
    private static final BsonDeserializationTestSuite<VectorSearchOperator> TEST_SUITE =
        fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, VectorSearchOperator::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<VectorSearchOperator> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<VectorSearchOperator> testSpec) {
      this.testSpec = testSpec;
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<VectorSearchOperator>>
        data() {
      return TEST_SUITE.withExamples(simple(), filter(), exact(), autoNumCandidates());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchOperator> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          OperatorBuilder.vectorSearch()
              .criteria(
                  ApproximateVectorQueryCriteriaBuilder.builder()
                      .path("description")
                      .queryVector(
                          Vector.fromFloats(
                              new float[] {2f, 2f, 2f}, FloatVector.OriginalType.BSON))
                      .numCandidates(10)
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchOperator> filter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "filter",
          OperatorBuilder.vectorSearch()
              .criteria(
                  ApproximateVectorQueryCriteriaBuilder.builder()
                      .path("description")
                      .queryVector(
                          Vector.fromFloats(
                              new float[] {5f, 5f, 5f}, FloatVector.OriginalType.BSON))
                      .filter(
                          new VectorSearchFilter.OperatorFilter(
                              OperatorBuilder.equals().path("flag").value(true).build()))
                      .numCandidates(10)
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchOperator> exact() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "exact",
          OperatorBuilder.vectorSearch()
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path("description")
                      .queryVector(
                          Vector.fromFloats(
                              new float[] {5f, 5f, 5f}, FloatVector.OriginalType.BSON))
                      .filter(
                          new VectorSearchFilter.OperatorFilter(
                              OperatorBuilder.equals().path("flag").value(true).build()))
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchOperator>
        autoNumCandidates() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "auto-numCandidates",
          OperatorBuilder.vectorSearch()
              .criteria(
                  ApproximateVectorQueryCriteriaBuilder.builder()
                      .path("description")
                      .queryVector(
                          Vector.fromFloats(
                              new float[] {2f, 2f, 2f}, FloatVector.OriginalType.BSON))
                      .limit(5)
                      .build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    public static final String SUITE_NAME = "vectorSearch-serialization";
    public static final BsonSerializationTestSuite<VectorSearchOperator> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(DefinitionTests.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<VectorSearchOperator> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<VectorSearchOperator> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<VectorSearchOperator>> data() {
      return List.of(simple(), filter());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<VectorSearchOperator> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          OperatorBuilder.vectorSearch()
              .criteria(
                  ApproximateVectorQueryCriteriaBuilder.builder()
                      .path("description")
                      .queryVector(
                          Vector.fromFloats(
                              new float[] {2f, 2f, 2f}, FloatVector.OriginalType.BSON))
                      .numCandidates(10)
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorSearchOperator> filter() {
      return BsonSerializationTestSuite.TestSpec.create(
          "filter",
          OperatorBuilder.vectorSearch()
              .criteria(
                  ApproximateVectorQueryCriteriaBuilder.builder()
                      .path("description")
                      .queryVector(
                          Vector.fromFloats(
                              new float[] {5f, 5f, 5f}, FloatVector.OriginalType.NATIVE))
                      .filter(
                          new VectorSearchFilter.OperatorFilter(
                              OperatorBuilder.equals().path("flag").value(true).build()))
                      .numCandidates(10)
                      .limit(5)
                      .build())
              .build());
    }
  }
}
