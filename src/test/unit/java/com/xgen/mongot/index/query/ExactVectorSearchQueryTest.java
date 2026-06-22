package com.xgen.mongot.index.query;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.query.ExactVectorCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      ExactVectorSearchQueryTest.DeserializationTest.class,
      ExactVectorSearchQueryTest.SerializationTest.class,
    })
public class ExactVectorSearchQueryTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "exact-vector-search-deserialization";
    private static final BsonDeserializationTestSuite<VectorSearchQuery> TEST_SUITE =
        fromDocument(
            "src/test/unit/resources/index/query/",
            SUITE_NAME,
            doc -> VectorSearchQuery.fromBson(doc, true));

    private final BsonDeserializationTestSuite.TestSpecWrapper<VectorSearchQuery> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<VectorSearchQuery> testSpec) {
      this.testSpec = testSpec;
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<VectorSearchQuery>> data()
        throws BsonParseException {
      return TEST_SUITE.withExamples(
          simple(),
          filter(),
          parentFilter(),
          filterAndParentFilter(),
          bsonFloatVectorQuery(),
          bsonByteVectorQuery(),
          simpleAutoEmbeddingQuery(),
          simpleAutoEmbeddingQueryWithMultiModalFormat(),
          withSearchNodePreference(),
          autoEmbeddingQueryWithModel());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchQuery> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .queryVector(Vector.fromFloats(new float[] {2f, 2f, 2f}, NATIVE))
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchQuery> filter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with-filter",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .queryVector(Vector.fromFloats(new float[] {2f, 2f, 2f}, NATIVE))
                      .limit(10)
                      .filter(
                          new VectorSearchFilter.ClauseFilter(
                              ClauseBuilder.simpleClause()
                                  .path(FieldPath.newRoot("my-field"))
                                  .operators(
                                      List.of(
                                          MqlFilterOperatorBuilder.nin()
                                              .values(
                                                  List.of(
                                                      ValueBuilder.intNumber(1),
                                                      ValueBuilder.intNumber(2),
                                                      ValueBuilder.intNumber(3)))
                                              .build()))
                                  .build()))
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchQuery> parentFilter()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with-parentFilter",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .queryVector(Vector.fromFloats(new float[] {2f, 2f, 2f}, NATIVE))
                      .limit(10)
                      .parentFilter(
                          new VectorSearchFilter.ClauseFilter(
                              ClauseBuilder.simpleClause()
                                  .path(FieldPath.newRoot("rating"))
                                  .operators(
                                      List.of(
                                          MqlFilterOperatorBuilder.gte()
                                              .value(ValueBuilder.doubleNumber(8.0))
                                              .build()))
                                  .build()))
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchQuery> filterAndParentFilter()
        throws BsonParseException {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with-filter-and-parentFilter",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .queryVector(Vector.fromFloats(new float[] {2f, 2f, 2f}, NATIVE))
                      .limit(10)
                      .filter(
                          new VectorSearchFilter.ClauseFilter(
                              ClauseBuilder.simpleClause()
                                  .path(FieldPath.newRoot("my-field"))
                                  .operators(
                                      List.of(
                                          MqlFilterOperatorBuilder.eq()
                                              .value(ValueBuilder.string("action"))
                                              .build()))
                                  .build()))
                      .parentFilter(
                          new VectorSearchFilter.ClauseFilter(
                              ClauseBuilder.simpleClause()
                                  .path(FieldPath.newRoot("rating"))
                                  .operators(
                                      List.of(
                                          MqlFilterOperatorBuilder.gte()
                                              .value(ValueBuilder.doubleNumber(8.5))
                                              .build()))
                                  .build()))
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchQuery>
        bsonFloatVectorQuery() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "bson-float32-query-vector",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .queryVector(Vector.fromFloats(new float[] {2f, 2f, 2f}, NATIVE))
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchQuery> bsonByteVectorQuery() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "bson-int8-query-vector",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .queryVector(
                          Vector.fromBytes(new byte[] {(byte) 0x02, (byte) 0x02, (byte) 0x02}))
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchQuery>
        simpleAutoEmbeddingQuery() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple-auto-embedding-query",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .query("test query")
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchQuery>
        simpleAutoEmbeddingQueryWithMultiModalFormat() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple-auto-embedding-query-multimodal",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .query(new VectorSearchQueryInput.MultiModal(Optional.of("test query")))
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchQuery>
        withSearchNodePreference() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with-searchNodePreference",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .queryVector(Vector.fromFloats(new float[] {2f, 2f, 2f}, NATIVE))
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchQuery>
        autoEmbeddingQueryWithModel() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "auto-embedding-query-with-model",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .query(
                          new VectorSearchQueryInput.Text(
                              "test query", Optional.of("voyage-3-large")))
                      .limit(5)
                      .build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    public static final String SUITE_NAME = "exact-vector-search-serialization";
    public static final BsonSerializationTestSuite<VectorSearchQuery> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(
            "src/test/unit/resources/index/query/", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<VectorSearchQuery> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<VectorSearchQuery> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<VectorSearchQuery>> data()
        throws BsonParseException {
      return List.of(
          simple(),
          filter(),
          parentFilter(),
          filterAndParentFilter(),
          simpleAutoEmbeddingQuery(),
          simpleAutoEmbeddingQueryWithMultiModalFormat(),
          autoEmbeddingQueryWithModel());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<VectorSearchQuery> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .queryVector(Vector.fromFloats(new float[] {2f, 2f, 2f}, NATIVE))
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorSearchQuery> filter() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with-filter",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .queryVector(Vector.fromFloats(new float[] {2f, 2f, 2f}, NATIVE))
                      .limit(10)
                      .filter(
                          new VectorSearchFilter.ClauseFilter(
                              ClauseBuilder.simpleClause()
                                  .path(FieldPath.newRoot("my-field"))
                                  .operators(
                                      List.of(
                                          MqlFilterOperatorBuilder.nin()
                                              .values(
                                                  List.of(
                                                      ValueBuilder.string("one"),
                                                      ValueBuilder.string("two"),
                                                      ValueBuilder.string("three")))
                                              .build()))
                                  .build()))
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorSearchQuery> parentFilter()
        throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "with-parentFilter",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .queryVector(Vector.fromFloats(new float[] {2f, 2f, 2f}, NATIVE))
                      .limit(10)
                      .parentFilter(
                          new VectorSearchFilter.ClauseFilter(
                              ClauseBuilder.simpleClause()
                                  .path(FieldPath.newRoot("rating"))
                                  .operators(
                                      List.of(
                                          MqlFilterOperatorBuilder.gte()
                                              .value(ValueBuilder.doubleNumber(8.0))
                                              .build()))
                                  .build()))
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorSearchQuery> filterAndParentFilter()
        throws BsonParseException {
      return BsonSerializationTestSuite.TestSpec.create(
          "with-filter-and-parentFilter",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .queryVector(Vector.fromFloats(new float[] {2f, 2f, 2f}, NATIVE))
                      .limit(10)
                      .filter(
                          new VectorSearchFilter.ClauseFilter(
                              ClauseBuilder.simpleClause()
                                  .path(FieldPath.newRoot("my-field"))
                                  .operators(
                                      List.of(
                                          MqlFilterOperatorBuilder.eq()
                                              .value(ValueBuilder.string("action"))
                                              .build()))
                                  .build()))
                      .parentFilter(
                          new VectorSearchFilter.ClauseFilter(
                              ClauseBuilder.simpleClause()
                                  .path(FieldPath.newRoot("rating"))
                                  .operators(
                                      List.of(
                                          MqlFilterOperatorBuilder.gte()
                                              .value(ValueBuilder.doubleNumber(8.5))
                                              .build()))
                                  .build()))
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorSearchQuery>
        simpleAutoEmbeddingQuery() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple-auto-embedding-query",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .query("test query")
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorSearchQuery>
        simpleAutoEmbeddingQueryWithMultiModalFormat() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple-auto-embedding-query-multimodal",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .query(new VectorSearchQueryInput.MultiModal(Optional.of("test query")))
                      .limit(5)
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorSearchQuery>
        autoEmbeddingQueryWithModel() {
      return BsonSerializationTestSuite.TestSpec.create(
          "auto-embedding-query-with-model",
          VectorQueryBuilder.builder()
              .index("myVectorIndex")
              .criteria(
                  ExactVectorCriteriaBuilder.builder()
                      .path(FieldPath.newRoot("description"))
                      .query(
                          new VectorSearchQueryInput.Text(
                              "test query", Optional.of("voyage-3-large")))
                      .limit(5)
                      .build())
              .build());
    }
  }
}
