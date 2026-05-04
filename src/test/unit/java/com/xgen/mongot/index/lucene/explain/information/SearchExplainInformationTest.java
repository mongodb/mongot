package com.xgen.mongot.index.lucene.explain.information;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.QueryExplainInformationBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.SortStatsBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.TermQueryBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.CursorOptionsDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.OptimizationFlagsDefinitionBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      SearchExplainInformationTest.DeserializationTest.class,
      SearchExplainInformationTest.SerializationTest.class,
    })
public class SearchExplainInformationTest {
  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "search-explain-info-deserialization";
    private static final BsonDeserializationTestSuite<SearchExplainInformation> TEST_SUITE =
        fromDocument(
            ExplainInformationTestUtil.RESOURCES_PATH,
            SUITE_NAME,
            SearchExplainInformation::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<SearchExplainInformation> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<SearchExplainInformation> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<SearchExplainInformation>>
        data() {
      return TEST_SUITE.withExamples(simple(), multipleQueryInfos(), withIndexPartition());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchExplainInformation>
        multipleQueryInfos() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "multiple query infos",
          SearchExplainInformationBuilder.newBuilder()
              .queryExplainInfos(
                  List.of(
                      QueryExplainInformationBuilder.builder()
                          .analyzer("lucene.standard")
                          .type(LuceneQuerySpecification.Type.TERM_QUERY)
                          .args(TermQueryBuilder.builder().path("name").value("quie").build())
                          .stats(ExplainInformationTestUtil.BASIC_STATS)
                          .build(),
                      QueryExplainInformationBuilder.builder()
                          .analyzer("lucene.standard")
                          .type(LuceneQuerySpecification.Type.TERM_QUERY)
                          .args(TermQueryBuilder.builder().path("foo").value("bar").build())
                          .stats(ExplainInformationTestUtil.BASIC_STATS)
                          .build()))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchExplainInformation> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          SearchExplainInformationBuilder.newBuilder()
              .queryExplainInfos(
                  List.of(
                      QueryExplainInformationBuilder.builder()
                          .analyzer("lucene.standard")
                          .type(LuceneQuerySpecification.Type.TERM_QUERY)
                          .args(TermQueryBuilder.builder().path("name").value("quie").build())
                          .stats(ExplainInformationTestUtil.BASIC_STATS)
                          .build()))
              .allCollectorStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .sortStats(
                  SortStatsBuilder.builder()
                      .profilingIteratorExecutionArea(
                          ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .sortExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .fieldInfos(
                          Map.of(
                              "foo", List.of(FieldName.TypeField.DATE, FieldName.TypeField.TOKEN)))
                      .build())
              .resourceUsage(new ResourceUsageOutput(1, 2, 3, 4, 1, 1))
              .highlightStats(
                  new HighlightStats(
                      List.of("foo"),
                      Map.of(),
                      Optional.of(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)))
              .resultMaterializationStats(
                  new ResultMaterializationStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA))
              .metadata(
                  new MetadataExplainInformation(
                      Optional.of("testVersion"),
                      Optional.of("localhost"),
                      Optional.of("default"),
                      Optional.of(
                          CursorOptionsDefinitionBuilder.builder()
                              .docsRequested(25)
                              .build()
                              .toBson()),
                      Optional.of(
                          OptimizationFlagsDefinitionBuilder.builder()
                              .omitSearchDocumentResults(true)
                              .build()
                              .toBson()),
                      Optional.of(
                          new LuceneMetadataExplainInformation(Optional.of(3), Optional.of(5)))))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchExplainInformation>
        withIndexPartition() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with index partition",
          SearchExplainInformationBuilder.newBuilder()
              .resourceUsage(new ResourceUsageOutput(1, 2, 3, 4, 1, 1))
              .addIndexPartitionExplainInformation(
                  SearchExplainInformationBuilder.newBuilder()
                      .queryExplainInfos(
                          List.of(
                              QueryExplainInformationBuilder.builder()
                                  .analyzer("lucene.standard")
                                  .type(LuceneQuerySpecification.Type.TERM_QUERY)
                                  .args(
                                      TermQueryBuilder.builder().path("name").value("quie").build())
                                  .stats(ExplainInformationTestUtil.BASIC_STATS)
                                  .build()))
                      .allCollectorStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .build())
              .addIndexPartitionExplainInformation(
                  SearchExplainInformationBuilder.newBuilder()
                      .queryExplainInfos(
                          List.of(
                              QueryExplainInformationBuilder.builder()
                                  .analyzer("lucene.english")
                                  .type(LuceneQuerySpecification.Type.TERM_QUERY)
                                  .args(TermQueryBuilder.builder().path("foo").value("bar").build())
                                  .stats(ExplainInformationTestUtil.BASIC_STATS)
                                  .build()))
                      .allCollectorStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "search-explain-info-serialization";
    private static final BsonSerializationTestSuite<SearchExplainInformation> TEST_SUITE =
        fromEncodable(ExplainInformationTestUtil.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<SearchExplainInformation> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<SearchExplainInformation> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<SearchExplainInformation>> data() {
      return Arrays.asList(
          simple(), multipleQueryInfos(), withIndexPartitionDoesNotSerializeQueryAndCollect());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<SearchExplainInformation> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          SearchExplainInformationBuilder.newBuilder()
              .queryExplainInfos(
                  List.of(
                      QueryExplainInformationBuilder.builder()
                          .analyzer("lucene.standard")
                          .type(LuceneQuerySpecification.Type.TERM_QUERY)
                          .args(TermQueryBuilder.builder().path("name").value("quie").build())
                          .stats(ExplainInformationTestUtil.BASIC_STATS)
                          .build()))
              .allCollectorStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
              .sortStats(
                  SortStatsBuilder.builder()
                      .profilingIteratorExecutionArea(
                          ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .sortExecutionArea(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .fieldInfos(
                          Map.of(
                              "foo", List.of(FieldName.TypeField.DATE, FieldName.TypeField.TOKEN)))
                      .build())
              .resourceUsage(new ResourceUsageOutput(1, 2, 3, 4, 1, 1))
              .highlightStats(
                  new HighlightStats(
                      List.of("foo"),
                      Map.of(),
                      Optional.of(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)))
              .resultMaterializationStats(
                  new ResultMaterializationStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA))
              .metadata(
                  new MetadataExplainInformation(
                      Optional.of("testVersion"),
                      Optional.of("localhost"),
                      Optional.of("default"),
                      Optional.of(
                          CursorOptionsDefinitionBuilder.builder()
                              .docsRequested(25)
                              .build()
                              .toBson()),
                      Optional.of(
                          OptimizationFlagsDefinitionBuilder.builder()
                              .omitSearchDocumentResults(true)
                              .build()
                              .toBson()),
                      Optional.of(
                          new LuceneMetadataExplainInformation(Optional.of(3), Optional.of(5)))))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchExplainInformation>
        multipleQueryInfos() {
      return BsonSerializationTestSuite.TestSpec.create(
          "multiple query infos",
          SearchExplainInformationBuilder.newBuilder()
              .queryExplainInfos(
                  List.of(
                      QueryExplainInformationBuilder.builder()
                          .analyzer("lucene.standard")
                          .type(LuceneQuerySpecification.Type.TERM_QUERY)
                          .args(TermQueryBuilder.builder().path("name").value("quie").build())
                          .stats(ExplainInformationTestUtil.BASIC_STATS)
                          .build(),
                      QueryExplainInformationBuilder.builder()
                          .analyzer("lucene.standard")
                          .type(LuceneQuerySpecification.Type.TERM_QUERY)
                          .args(TermQueryBuilder.builder().path("foo").value("bar").build())
                          .stats(ExplainInformationTestUtil.BASIC_STATS)
                          .build()))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchExplainInformation>
        withIndexPartitionDoesNotSerializeQueryAndCollect() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with index partition does not serialize query and collect",
          SearchExplainInformationBuilder.newBuilder()
              .resourceUsage(new ResourceUsageOutput(1, 2, 3, 4, 1, 1))
              .addIndexPartitionExplainInformation(
                  SearchExplainInformationBuilder.newBuilder()
                      .queryExplainInfos(
                          List.of(
                              QueryExplainInformationBuilder.builder()
                                  .analyzer("lucene.standard")
                                  .type(LuceneQuerySpecification.Type.TERM_QUERY)
                                  .args(
                                      TermQueryBuilder.builder().path("name").value("quie").build())
                                  .stats(ExplainInformationTestUtil.BASIC_STATS)
                                  .build()))
                      .allCollectorStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .build())
              .addIndexPartitionExplainInformation(
                  SearchExplainInformationBuilder.newBuilder()
                      .queryExplainInfos(
                          List.of(
                              QueryExplainInformationBuilder.builder()
                                  .analyzer("lucene.english")
                                  .type(LuceneQuerySpecification.Type.TERM_QUERY)
                                  .args(TermQueryBuilder.builder().path("foo").value("bar").build())
                                  .stats(ExplainInformationTestUtil.BASIC_STATS)
                                  .build()))
                      .allCollectorStats(ExplainInformationTestUtil.QUERY_EXECUTION_AREA)
                      .build())
              .build());
    }
  }
}
