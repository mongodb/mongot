package com.xgen.mongot.index.lucene.explain.information;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.lucene.explain.information.BooleanQueryBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.QueryExplainInformationBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.TermQueryBuilder;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      QueryExplainInformationTest.DeserializationTest.class,
      QueryExplainInformationTest.SerializationTest.class,
    })
public class QueryExplainInformationTest {
  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "query-explain-info-deserialization";
    private static final BsonDeserializationTestSuite<QueryExplainInformation> TEST_SUITE =
        fromDocument(
            ExplainInformationTestUtil.RESOURCES_PATH,
            SUITE_NAME,
            QueryExplainInformation::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<QueryExplainInformation> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<QueryExplainInformation> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<QueryExplainInformation>>
        data() {
      return TEST_SUITE.withExamples(simple(), compoundOperator());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<QueryExplainInformation> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          QueryExplainInformationBuilder.builder()
              .analyzer("lucene.standard")
              .type(LuceneQuerySpecification.Type.TERM_QUERY)
              .args(TermQueryBuilder.builder().path("name").value("quie").build())
              .stats(ExplainInformationTestUtil.BASIC_STATS)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<QueryExplainInformation>
        compoundOperator() {
      QueryExplainInformation textExplainInfo =
          QueryExplainInformationBuilder.builder()
              .path("must[0]")
              .type(LuceneQuerySpecification.Type.TERM_QUERY)
              .args(TermQueryBuilder.builder().path("q").value("whopper").build())
              .build();

      return BsonDeserializationTestSuite.TestSpec.valid(
          "compound-operator",
          QueryExplainInformationBuilder.builder()
              .type(LuceneQuerySpecification.Type.BOOLEAN_QUERY)
              .args(BooleanQueryBuilder.builder().must(List.of(textExplainInfo)).build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "query-explain-info-serialization";
    private static final BsonSerializationTestSuite<QueryExplainInformation> TEST_SUITE =
        fromEncodable(ExplainInformationTestUtil.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<QueryExplainInformation> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<QueryExplainInformation> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<QueryExplainInformation>> data() {
      return Arrays.asList(simple(), compoundOperator());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<QueryExplainInformation> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          QueryExplainInformationBuilder.builder()
              .analyzer("lucene.standard")
              .type(LuceneQuerySpecification.Type.TERM_QUERY)
              .args(TermQueryBuilder.builder().path("name").value("quie").build())
              .stats(ExplainInformationTestUtil.BASIC_STATS)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<QueryExplainInformation> compoundOperator() {
      QueryExplainInformation textExplainInfo =
          QueryExplainInformationBuilder.builder()
              .path("must[0]")
              .type(LuceneQuerySpecification.Type.TERM_QUERY)
              .args(TermQueryBuilder.builder().path("q").value("whopper").build())
              .build();

      return BsonSerializationTestSuite.TestSpec.create(
          "compound-operator",
          QueryExplainInformationBuilder.builder()
              .type(LuceneQuerySpecification.Type.BOOLEAN_QUERY)
              .args(BooleanQueryBuilder.builder().must(List.of(textExplainInfo)).build())
              .build());
    }
  }
}
