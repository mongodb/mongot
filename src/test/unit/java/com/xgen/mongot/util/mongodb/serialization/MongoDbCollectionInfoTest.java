package com.xgen.mongot.util.mongodb.serialization;

import static com.xgen.testing.BsonDeserializationTestSuite.fromRootDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {MongoDbCollectionInfoTest.DeserializationTest.class})
public class MongoDbCollectionInfoTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "collection-info-deserialization";
    private static final BsonDeserializationTestSuite<Optional<MongoDbCollectionInfo>> TEST_SUITE =
        fromRootDocument(
            "src/test/unit/resources/util/mongodb/serialization",
            SUITE_NAME,
            MongoDbCollectionInfo::fromBsonDocument);

    private final BsonDeserializationTestSuite.TestSpecWrapper<Optional<MongoDbCollectionInfo>>
        testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<Optional<MongoDbCollectionInfo>> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<
            BsonDeserializationTestSuite.TestSpecWrapper<Optional<MongoDbCollectionInfo>>>
        data() {
      return TEST_SUITE.withExamples(
          collection(),
          clusteredCollection(),
          collectionWithNoOptions(),
          collectionWithUnmodelledOptions(),
          collatedCollection(),
          view(),
          viewWithNoPipeline(),
          viewWithNoViewOn(),
          timeseries(),
          collectionWithInvalidUuid(),
          collectionWithNoUuid());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<MongoDbCollectionInfo>>
        collection() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "collection",
          Optional.of(
              new MongoDbCollectionInfo.Collection(
                  "testDatabaseWithCollectionAndView",
                  new MongoDbCollectionInfo.Collection.Info(
                      UUID.fromString("0eaa56f3-7cd1-4321-8b06-e47ef68503b9")),
                  MongoDbCollectionInfo.Collection.Options.NONE)));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<MongoDbCollectionInfo>>
        clusteredCollection() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "clustered-collection",
          Optional.of(
              new MongoDbCollectionInfo.Collection(
                  "clusteredCollection",
                  new MongoDbCollectionInfo.Collection.Info(
                      UUID.fromString("1fbb67a4-8de2-4432-9c17-f58fa79614ca")),
                  new MongoDbCollectionInfo.Collection.Options(true))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<MongoDbCollectionInfo>>
        collectionWithNoOptions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "collection-with-no-options",
          Optional.of(
              new MongoDbCollectionInfo.Collection(
                  "noOptionsCollection",
                  new MongoDbCollectionInfo.Collection.Info(
                      UUID.fromString("2fcc78b5-9ef3-4543-ad28-069fb8a725db")),
                  MongoDbCollectionInfo.Collection.Options.NONE)));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<MongoDbCollectionInfo>>
        collectionWithUnmodelledOptions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "collection-with-unmodelled-options",
          Optional.of(
              new MongoDbCollectionInfo.Collection(
                  "cappedCollection",
                  new MongoDbCollectionInfo.Collection.Info(
                      UUID.fromString("3add89c6-af04-4654-be39-17a0c9b836ec")),
                  MongoDbCollectionInfo.Collection.Options.NONE)));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<MongoDbCollectionInfo>>
        collatedCollection() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "collated-collection",
          Optional.of(
              new MongoDbCollectionInfo.Collection(
                  "collatedCollection",
                  new MongoDbCollectionInfo.Collection.Info(
                      UUID.fromString("4bee9ad7-b015-4765-8f4a-28b1dac947fd")),
                  new MongoDbCollectionInfo.Collection.Options(
                      false,
                      Optional.of(
                          new BsonDocument("locale", new BsonString("en"))
                              .append("caseLevel", BsonBoolean.FALSE)
                              .append("caseFirst", new BsonString("off"))
                              .append("strength", new BsonInt32(2))
                              .append("numericOrdering", BsonBoolean.FALSE)
                              .append("alternate", new BsonString("non-ignorable"))
                              .append("maxVariable", new BsonString("punct"))
                              .append("normalization", BsonBoolean.FALSE)
                              .append("backwards", BsonBoolean.FALSE)
                              .append("version", new BsonString("57.1")))))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<MongoDbCollectionInfo>> view() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "view",
          Optional.of(
              new MongoDbCollectionInfo.View(
                  "testDatabaseWithCollectionAndView-view",
                  new MongoDbCollectionInfo.View.Options(
                      "testDatabaseWithCollectionAndView",
                      List.of(
                          new BsonDocument(
                              "$set", new BsonDocument("cats", new BsonInt32(20))))))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<MongoDbCollectionInfo>>
        collectionWithInvalidUuid() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "collection-with-invalid-uuid", Optional.empty());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<MongoDbCollectionInfo>>
        collectionWithNoUuid() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "collection-with-no-uuid", Optional.empty());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<MongoDbCollectionInfo>>
        viewWithNoPipeline() {
      return BsonDeserializationTestSuite.TestSpec.valid("view-with-no-pipeline", Optional.empty());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<MongoDbCollectionInfo>>
        viewWithNoViewOn() {
      return BsonDeserializationTestSuite.TestSpec.valid("view-with-no-viewOn", Optional.empty());
    }

    private static BsonDeserializationTestSuite.ValidSpec<Optional<MongoDbCollectionInfo>>
        timeseries() {
      return BsonDeserializationTestSuite.TestSpec.valid("timeseries", Optional.empty());
    }
  }
}
