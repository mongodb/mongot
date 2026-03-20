package com.xgen.mongot.cursor.serialization;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.index.IntermediateFacetBucket;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.ResourceUsageOutput;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.util.Bytes;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.cursor.serialization.MongotCursorBatchBuilder;
import com.xgen.testing.mongot.cursor.serialization.MongotCursorResultBuilder;
import com.xgen.testing.mongot.cursor.serialization.MongotIntermediateCursorBatchBuilder;
import com.xgen.testing.mongot.index.IntermediateFacetBucketBuilder;
import com.xgen.testing.mongot.index.SearchResultBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.QueryExplainInformationBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.TermQueryBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      MongotIntermediateCursorBatchTest.FunctionalityTest.class,
      MongotIntermediateCursorBatchTest.DeserializationTest.class,
      MongotIntermediateCursorBatchTest.SerializationTest.class
    })
public class MongotIntermediateCursorBatchTest {

  static final String RESOURCES_PATH = "src/test/unit/resources/cursor/serialization";

  private static List<BsonValue> makeBatch(int size) {
    List<BsonValue> results = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      results.add(
          SearchResultBuilder.builder().id(new BsonInt32(i)).score((float) i).build().toBson());
    }

    return results;
  }

  private static List<BsonValue> makeMetaBatch(int size) {
    List<BsonValue> results = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      results.add(
          IntermediateFacetBucketBuilder.builder()
              .type(IntermediateFacetBucket.Type.FACET)
              .tag("myFacet")
              .bucket(new BsonInt32(i))
              .count(Long.valueOf(i))
              .build()
              .toBson());
    }

    return results;
  }

  public static class FunctionalityTest {

    private static final String MAX_NAMESPACE = new String(new char[255]);

    @Test
    public void testEmptyBatchSizeCalculationWithVariables() {
      Bytes emptyBatchBytes = MongotIntermediateCursorBatch.calculateEmptyBatchSize();

      MongotCursorBatch searchBatch =
          new MongotCursorBatch(
              new MongotCursorResult(
                  Long.MAX_VALUE,
                  new BsonArray(),
                  MAX_NAMESPACE,
                  Optional.of(MongotCursorResult.Type.RESULTS)),
              Optional.empty());
      MongotCursorBatch metaBatch =
          new MongotCursorBatch(
              new MongotCursorResult(
                  Long.MAX_VALUE,
                  new BsonArray(),
                  MAX_NAMESPACE,
                  Optional.of(MongotCursorResult.Type.META)),
              Optional.empty());
      MongotIntermediateCursorBatch testBatch =
          new MongotIntermediateCursorBatch(metaBatch, searchBatch);

      long difference =
          testBatch.toRawBson().getByteBuffer().remaining() - emptyBatchBytes.toBytes();
      long bsonArraySize =
          new MongotCursorResult(Long.MAX_VALUE, new BsonArray(), MAX_NAMESPACE, Optional.empty())
                  .toRawBson()
                  .getByteBuffer()
                  .remaining()
              - new MongotCursorResult(
                      Long.MAX_VALUE, new BsonNull(), MAX_NAMESPACE, Optional.empty())
                  .toRawBson()
                  .getByteBuffer()
                  .remaining();
      Assert.assertEquals(2 * bsonArraySize, difference);
    }
  }

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "mongot-intermediate-cursor-batch-deserialization";

    private static final BsonDeserializationTestSuite<MongotIntermediateCursorBatch> TEST_SUITE =
        fromDocument(RESOURCES_PATH, SUITE_NAME, MongotIntermediateCursorBatch::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<MongotIntermediateCursorBatch>
        testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<MongotIntermediateCursorBatch> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<
            BsonDeserializationTestSuite.TestSpecWrapper<MongotIntermediateCursorBatch>>
        data() {
      return TEST_SUITE.withExamples(full());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<MongotIntermediateCursorBatch> full() {
      SearchExplainInformation explain =
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
              .resourceUsage(new ResourceUsageOutput(1, 2, 3, 4, 1, 1))
              .build();

      return BsonDeserializationTestSuite.TestSpec.valid(
          "full",
          MongotIntermediateCursorBatchBuilder.builder()
              .metaCursor(
                  MongotCursorBatchBuilder.builder()
                      .cursor(
                          MongotCursorResultBuilder.builder()
                              .id(1L)
                              .namespace("foo.bar")
                              .nextBatch(makeMetaBatch(2))
                              .type(MongotCursorResult.Type.META)
                              .build())
                      .explain(explain)
                      .build())
              .searchCursor(
                  MongotCursorBatchBuilder.builder()
                      .cursor(
                          MongotCursorResultBuilder.builder()
                              .id(2L)
                              .namespace("foo.bar")
                              .nextBatch(makeBatch(2))
                              .type(MongotCursorResult.Type.RESULTS)
                              .build())
                      .explain(explain)
                      .build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "mongot-intermediate-cursor-batch-serialization";
    private static final BsonSerializationTestSuite<MongotIntermediateCursorBatch> TEST_SUITE =
        fromEncodable(RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<MongotIntermediateCursorBatch> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<MongotIntermediateCursorBatch> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<MongotIntermediateCursorBatch>>
        data() {
      return Arrays.asList(full());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<MongotIntermediateCursorBatch> full() {
      SearchExplainInformation explain =
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
              .resourceUsage(new ResourceUsageOutput(1, 2, 3, 4, 1, 1))
              .build();

      return BsonSerializationTestSuite.TestSpec.create(
          "full",
          MongotIntermediateCursorBatchBuilder.builder()
              .metaCursor(
                  MongotCursorBatchBuilder.builder()
                      .cursor(
                          MongotCursorResultBuilder.builder()
                              .id(1L)
                              .namespace("foo.bar")
                              .nextBatch(makeMetaBatch(2))
                              .type(MongotCursorResult.Type.META)
                              .build())
                      .explain(explain)
                      .build())
              .searchCursor(
                  MongotCursorBatchBuilder.builder()
                      .cursor(
                          MongotCursorResultBuilder.builder()
                              .id(2L)
                              .namespace("foo.bar")
                              .nextBatch(makeBatch(2))
                              .type(MongotCursorResult.Type.RESULTS)
                              .build())
                      .explain(explain)
                      .build())
              .build());
    }
  }
}
