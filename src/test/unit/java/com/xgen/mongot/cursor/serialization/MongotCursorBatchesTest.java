package com.xgen.mongot.cursor.serialization;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.index.CountResult;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.Variables;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.ResourceUsageOutput;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.util.Bytes;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.cursor.serialization.MongotCursorBatchBuilder;
import com.xgen.testing.mongot.cursor.serialization.MongotCursorResultBuilder;
import com.xgen.testing.mongot.index.FacetBucketBuilder;
import com.xgen.testing.mongot.index.FacetInfoBuilder;
import com.xgen.testing.mongot.index.MetaResultsBuilder;
import com.xgen.testing.mongot.index.SearchResultBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.QueryExplainInformationBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.TermQueryBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      MongotCursorBatchesTest.FunctionalityTest.class,
      MongotCursorBatchesTest.DeserializationTest.class,
      MongotCursorBatchesTest.SerializationTest.class
    })
public class MongotCursorBatchesTest {

  static final String RESOURCES_PATH = "src/test/unit/resources/cursor/serialization";

  private static List<BsonValue> makeBatch(int size) {
    List<BsonValue> results = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      results.add(
          SearchResultBuilder.builder().id(new BsonInt32(i)).score((float) i).build().toBson());
    }

    return results;
  }

  public static class FunctionalityTest {

    private static final String MAX_NAMESPACE = new String(new char[255]);

    @Test
    public void testEmptyBatchSizeCalculationWithVariables() {
      Bytes emptyBatchBytes =
          MongotCursorBatch.calculateEmptyBatchSize(
              Optional.of(new Variables(MetaResults.EMPTY).toRawBson()), Optional.empty());

      MongotCursorResult testResult =
          new MongotCursorResult(Long.MAX_VALUE, new BsonArray(), MAX_NAMESPACE, Optional.empty());
      MongotCursorBatch testBatch =
          new MongotCursorBatch(
              testResult,
              Optional.empty(),
              Optional.of(new Variables(MetaResults.EMPTY).toRawBson()));

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
      Assert.assertEquals(bsonArraySize, difference);
    }

    @Test
    public void testEmptyBatchSizeCalculationWithoutVariables() {
      Bytes emptyBatchBytes =
          MongotCursorBatch.calculateEmptyBatchSize(Optional.empty(), Optional.empty());

      MongotCursorResult testResult =
          new MongotCursorResult(Long.MAX_VALUE, new BsonArray(), MAX_NAMESPACE, Optional.empty());
      MongotCursorBatch testBatch =
          new MongotCursorBatch(testResult, Optional.empty(), Optional.empty());

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
      Assert.assertEquals(bsonArraySize, difference);
    }
  }

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "mongot-cursor-batch-deserialization";

    private static final BsonDeserializationTestSuite<MongotCursorBatch> TEST_SUITE =
        fromDocument(RESOURCES_PATH, SUITE_NAME, MongotCursorBatch::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<MongotCursorBatch> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<MongotCursorBatch> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<MongotCursorBatch>> data() {
      return TEST_SUITE.withExamples(simple(), withMeta());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<MongotCursorBatch> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          MongotCursorBatchBuilder.builder()
              .cursor(
                  MongotCursorResultBuilder.builder()
                      .id(1L)
                      .namespace("foo.bar")
                      .nextBatch(makeBatch(2))
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<MongotCursorBatch> withMeta() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "withMeta",
          MongotCursorBatchBuilder.builder()
              .cursor(
                  MongotCursorResultBuilder.builder()
                      .id(1L)
                      .namespace("foo.bar")
                      .nextBatch(makeBatch(2))
                      .build())
              .variables(
                  new Variables(
                      MetaResultsBuilder.builder()
                          .count(CountResult.lowerBoundCount(1000))
                          .facet(
                              Map.of(
                                  "myFacet",
                                  FacetInfoBuilder.builder()
                                      .buckets(
                                          List.of(
                                              FacetBucketBuilder.builder()
                                                  .id(new BsonString("category"))
                                                  .count(2L)
                                                  .build()))
                                      .build()))
                          .build()))
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "mongot-cursor-batch-serialization";
    private static final BsonSerializationTestSuite<MongotCursorBatch> TEST_SUITE =
        fromEncodable(RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<MongotCursorBatch> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<MongotCursorBatch> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<MongotCursorBatch>> data() {
      return Arrays.asList(empty(), full(), last(), noMeta(), facet());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<MongotCursorBatch> empty() {
      return BsonSerializationTestSuite.TestSpec.create(
          "empty",
          MongotCursorBatchBuilder.builder()
              .cursor(
                  MongotCursorResultBuilder.builder()
                      .id(0L)
                      .nextBatch(Collections.emptyList())
                      .namespace("foo.bar")
                      .build())
              .variables(
                  new Variables(
                      MetaResultsBuilder.builder().count(CountResult.lowerBoundCount(0)).build()))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<MongotCursorBatch> full() {
      return BsonSerializationTestSuite.TestSpec.create(
          "full",
          MongotCursorBatchBuilder.builder()
              .cursor(
                  MongotCursorResultBuilder.builder()
                      .id(1L)
                      .nextBatch(makeBatch(4))
                      .namespace("foo.bar")
                      .build())
              .explain(
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
                      .resourceUsage(new ResourceUsageOutput(1, 2, 3, 4, 1, 1))
                      .build())
              .variables(
                  new Variables(
                      MetaResultsBuilder.builder().count(CountResult.lowerBoundCount(4)).build()))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<MongotCursorBatch> last() {
      return BsonSerializationTestSuite.TestSpec.create(
          "last",
          MongotCursorBatchBuilder.builder()
              .cursor(
                  MongotCursorResultBuilder.builder()
                      .id(0L)
                      .nextBatch(makeBatch(2))
                      .namespace("foo.bar")
                      .build())
              .variables(
                  new Variables(
                      MetaResultsBuilder.builder().count(CountResult.lowerBoundCount(6)).build()))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<MongotCursorBatch> noMeta() {
      return BsonSerializationTestSuite.TestSpec.create(
          "noMeta",
          MongotCursorBatchBuilder.builder()
              .cursor(
                  MongotCursorResultBuilder.builder()
                      .id(0L)
                      .nextBatch(Collections.emptyList())
                      .namespace("foo.bar")
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<MongotCursorBatch> facet() {
      return BsonSerializationTestSuite.TestSpec.create(
          "facet",
          MongotCursorBatchBuilder.builder()
              .cursor(
                  MongotCursorResultBuilder.builder()
                      .id(0L)
                      .nextBatch(makeBatch(2))
                      .namespace("foo.bar")
                      .build())
              .variables(
                  new Variables(
                      MetaResultsBuilder.builder()
                          .count(CountResult.lowerBoundCount(6))
                          .facet(
                              Map.of(
                                  "myFacet",
                                  FacetInfoBuilder.builder()
                                      .buckets(
                                          List.of(
                                              FacetBucketBuilder.builder()
                                                  .id(new BsonString("category"))
                                                  .count(2L)
                                                  .build()))
                                      .build()))
                          .build()))
              .build());
    }
  }
}
