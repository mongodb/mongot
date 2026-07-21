package com.xgen.mongot.index.query.collectors;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.query.collectors.CollectorBuilder;
import com.xgen.testing.mongot.index.query.collectors.FacetDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {CollectorTest.DeserializationTest.class, CollectorTest.SerializationTest.class})
public class CollectorTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "collector-deserialization";
    private static final BsonDeserializationTestSuite<Collector> TEST_SUITE =
        fromDocument(
            "src/test/unit/resources/index/query/collectors/",
            SUITE_NAME,
            parser ->
                Collector.atMostOneFromBson(
                        parser, /* allow10k= */ false, /* useAggregation= */ false)
                    .orElseThrow());

    private final BsonDeserializationTestSuite.TestSpecWrapper<Collector> testSpec;

    public DeserializationTest(BsonDeserializationTestSuite.TestSpecWrapper<Collector> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<Collector>> data() {
      return TEST_SUITE.withExamples(facets());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Collector> facets() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "facets",
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("review").query("good").build())
              .facetDefinitions(
                  Map.of(
                      "directorFacet",
                      FacetDefinitionBuilder.string().numBuckets(10).path("director").build(),
                      "ratingFacet",
                      FacetDefinitionBuilder.numeric()
                          .boundaries(List.of(new BsonDouble(1.12), new BsonDouble(2.64)))
                          .path("rating")
                          .build(),
                      "eventFacet",
                      FacetDefinitionBuilder.date()
                          .boundaries(
                              List.of(
                                  new BsonDateTime(
                                      new Calendar.Builder()
                                          .setDate(2020, Calendar.MAY, 8)
                                          .setTimeOfDay(12, 11, 0, 0)
                                          .setTimeZone(TimeZone.getTimeZone("UTC"))
                                          .build()
                                          .getTimeInMillis()),
                                  new BsonDateTime(
                                      new Calendar.Builder()
                                          .setDate(2021, Calendar.MAY, 8)
                                          .setTimeOfDay(12, 11, 0, 0)
                                          .setTimeZone(TimeZone.getTimeZone("UTC"))
                                          .build()
                                          .getTimeInMillis())))
                          .path("start")
                          .build()))
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "collector-serialization";
    private static final BsonSerializationTestSuite<Collector> TEST_SUITE =
        fromEncodable("src/test/unit/resources/index/query/collectors/", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<Collector> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<Collector> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<Collector>> data() {
      return List.of(compound());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<Collector> compound() {
      return BsonSerializationTestSuite.TestSpec.create(
          "facets",
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("review").query("good").build())
              .facetDefinitions(
                  Map.of(
                      "directorFacet",
                      FacetDefinitionBuilder.string().numBuckets(50).path("director").build(),
                      "ratingFacet",
                      FacetDefinitionBuilder.numeric()
                          .boundaries(List.of(new BsonDouble(1.12), new BsonDouble(2.64)))
                          .path("rating")
                          .build(),
                      "eventFacet",
                      FacetDefinitionBuilder.date()
                          .boundaries(
                              List.of(
                                  new BsonDateTime(
                                      new Calendar.Builder()
                                          .setDate(2020, Calendar.MAY, 8)
                                          .setTimeOfDay(12, 11, 0, 0)
                                          .setTimeZone(TimeZone.getTimeZone("UTC"))
                                          .build()
                                          .getTimeInMillis()),
                                  new BsonDateTime(
                                      new Calendar.Builder()
                                          .setDate(2021, Calendar.MAY, 8)
                                          .setTimeOfDay(12, 11, 0, 0)
                                          .setTimeZone(TimeZone.getTimeZone("UTC"))
                                          .build()
                                          .getTimeInMillis())))
                          .path("start")
                          .build()))
              .build());
    }
  }
}
