package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      ViewDefinitionTest.DeserializationTest.class,
      ViewDefinitionTest.SerializationTest.class,
      ViewDefinitionTest.DefinitionTest.class
    })
public class ViewDefinitionTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "view-definition-deserialization";
    private static final BsonDeserializationTestSuite<ViewDefinition> TEST_SUITE =
        fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, ViewDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<ViewDefinition> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<ViewDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<ViewDefinition>> data() {
      return TEST_SUITE.withExamples(simple(), emptyPipeline(), nonPresentView());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<ViewDefinition> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          ViewDefinition.existing(
              "myView",
              List.of(
                  new BsonDocument("$set", new BsonDocument("cats", new BsonInt32(20))),
                  new BsonDocument("$set", new BsonDocument("dogs", new BsonInt32(40))))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<ViewDefinition> emptyPipeline() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty pipeline", ViewDefinition.existing("myView", List.of()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<ViewDefinition> nonPresentView() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "non existent view", ViewDefinition.missing("myView"));
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "view-definition-serialization";
    private static final BsonSerializationTestSuite<ViewDefinition> TEST_SUITE =
        fromEncodable(DefinitionTests.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<ViewDefinition> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<ViewDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<ViewDefinition>> data() {
      return Arrays.asList(simple(), emptyPipeline(), nonPresentView());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<ViewDefinition> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          ViewDefinition.existing(
              "myView",
              List.of(
                  new BsonDocument("$set", new BsonDocument("cats", new BsonInt32(20))),
                  new BsonDocument("$set", new BsonDocument("dogs", new BsonInt32(40))))));
    }

    private static BsonSerializationTestSuite.TestSpec<ViewDefinition> emptyPipeline() {
      return BsonSerializationTestSuite.TestSpec.create(
          "empty pipeline", ViewDefinition.existing("myView", List.of()));
    }

    private static BsonSerializationTestSuite.TestSpec<ViewDefinition> nonPresentView() {
      return BsonSerializationTestSuite.TestSpec.create(
          "non existent view", ViewDefinition.missing("myView"));
    }
  }

  public static class DefinitionTest {

    @Test
    public void testSingleViewParent() {
      var viewInfo =
          new MongoDbCollectionInfo.View(
              "parrots",
              new MongoDbCollectionInfo.View.Options(
                  "birds",
                  List.of(
                      new BsonDocument(
                          "$match", new BsonDocument("class", new BsonString("parrots"))))));

      var parentCollectionUuid = UUID.randomUUID();
      var parentCollectionInfo =
          new MongoDbCollectionInfo.Collection(
              "birds", new MongoDbCollectionInfo.Collection.Info(parentCollectionUuid));

      var result =
          ViewDefinition.fromCollectionInfos(
              new MongoNamespace("animals", "parrots"),
              parentCollectionUuid,
              new MongoDbCollectionInfos(
                  ImmutableMap.of(
                      new MongoNamespace("animals", "birds"),
                      parentCollectionInfo,
                      new MongoNamespace("animals", "parrots"),
                      viewInfo)));

      Assert.assertTrue(result.exists());
      Assert.assertEquals("parrots", result.getName());
      Assert.assertEquals(result.getEffectivePipeline().get(), viewInfo.options().pipeline());
    }

    @Test
    public void testViewParentHierarchy() {

      var birdsCollectionUuid = UUID.randomUUID();
      var birdsCollectionInfo =
          new MongoDbCollectionInfo.Collection(
              "birds", new MongoDbCollectionInfo.Collection.Info(birdsCollectionUuid));

      var parrotsViewInfo =
          new MongoDbCollectionInfo.View(
              "parrots",
              new MongoDbCollectionInfo.View.Options(
                  "birds",
                  List.of(
                      new BsonDocument(
                          "$match", new BsonDocument("class", new BsonString("parrots"))))));

      var cockatooViewInfo =
          new MongoDbCollectionInfo.View(
              "cockatoo",
              new MongoDbCollectionInfo.View.Options(
                  "parrots",
                  List.of(
                      new BsonDocument(
                          "$match",
                          new BsonDocument("subfamily", new BsonString("cacatuoidea"))))));

      var cockatooEnrichedViewInfo =
          new MongoDbCollectionInfo.View(
              "cockatoo-enriched",
              new MongoDbCollectionInfo.View.Options(
                  "cockatoo",
                  List.of(
                      new BsonDocument(
                          "$addFields",
                          new BsonDocument(
                              "totalScore",
                              new BsonDocument(
                                  "$add",
                                  new BsonArray(
                                      List.of(
                                          new BsonString("$weight"),
                                          new BsonString("$speed")))))))));

      var namespace = new MongoNamespace("animals", "cockatoo-enriched");

      var result =
          ViewDefinition.fromCollectionInfos(
              namespace,
              birdsCollectionUuid,
              new MongoDbCollectionInfos(
                  ImmutableMap.of(
                      new MongoNamespace("animals", "birds"),
                      birdsCollectionInfo,
                      new MongoNamespace("animals", "parrots"),
                      parrotsViewInfo,
                      new MongoNamespace("animals", "cockatoo"),
                      cockatooViewInfo,
                      new MongoNamespace("animals", "cockatoo-enriched"),
                      cockatooEnrichedViewInfo)));

      Assert.assertTrue(result.exists());
      Assert.assertEquals("cockatoo-enriched", result.getName());
      Assert.assertEquals(
          result.getEffectivePipeline().get(),
          List.of(
              new BsonDocument("$match", new BsonDocument("class", new BsonString("parrots"))),
              new BsonDocument(
                  "$match", new BsonDocument("subfamily", new BsonString("cacatuoidea"))),
              new BsonDocument(
                  "$addFields",
                  new BsonDocument(
                      "totalScore",
                      new BsonDocument(
                          "$add",
                          new BsonArray(
                              List.of(new BsonString("$weight"), new BsonString("$speed"))))))));
    }

    @Test
    public void testEmptyPipelineInHierarchy() {
      var birdsCollectionUuid = UUID.randomUUID();
      var birdsCollectionInfo =
          new MongoDbCollectionInfo.Collection(
              "birds", new MongoDbCollectionInfo.Collection.Info(birdsCollectionUuid));

      var parrotsViewInfo =
          new MongoDbCollectionInfo.View(
              "parrots", new MongoDbCollectionInfo.View.Options("birds", List.of()));

      var cockatooViewInfo =
          new MongoDbCollectionInfo.View(
              "cockatoo",
              new MongoDbCollectionInfo.View.Options(
                  "parrots",
                  List.of(
                      new BsonDocument(
                          "$match",
                          new BsonDocument("subfamily", new BsonString("cacatuoidea"))))));

      var namespace = new MongoNamespace("animals", "cockatoo");

      var result =
          ViewDefinition.fromCollectionInfos(
              namespace,
              birdsCollectionUuid,
              new MongoDbCollectionInfos(
                  ImmutableMap.of(
                      new MongoNamespace("animals", "birds"),
                      birdsCollectionInfo,
                      new MongoNamespace("animals", "parrots"),
                      parrotsViewInfo,
                      new MongoNamespace("animals", "cockatoo"),
                      cockatooViewInfo)));

      Assert.assertTrue(result.exists());
      Assert.assertEquals("cockatoo", result.getName());
      Assert.assertEquals(
          result.getEffectivePipeline().get(),
          List.of(
              new BsonDocument(
                  "$match", new BsonDocument("subfamily", new BsonString("cacatuoidea")))));
    }

    @Test
    public void testCollectionInfosDoNotContainTargetView() {
      var viewInfo =
          new MongoDbCollectionInfo.View(
              "parrots",
              new MongoDbCollectionInfo.View.Options(
                  "birds",
                  List.of(
                      new BsonDocument(
                          "$match", new BsonDocument("class", new BsonString("parrots"))))));

      var parentCollectionInfo =
          new MongoDbCollectionInfo.Collection(
              "birds", new MongoDbCollectionInfo.Collection.Info(UUID.randomUUID()));

      var result =
          ViewDefinition.fromCollectionInfos(
              new MongoNamespace("animals", "name that does not exist"),
              parentCollectionInfo.info().uuid(),
              new MongoDbCollectionInfos(
                  ImmutableMap.of(
                      new MongoNamespace("animals", "birds"),
                      parentCollectionInfo,
                      new MongoNamespace("animals", "parrots"),
                      viewInfo)));

      Assert.assertFalse(result.exists());
      Assert.assertEquals("name that does not exist", result.getName());
      Assert.assertTrue(result.getEffectivePipeline().isEmpty());
    }

    @Test
    public void testMissingParentInHierarchy() {
      var viewInfo =
          new MongoDbCollectionInfo.View(
              "parrots",
              new MongoDbCollectionInfo.View.Options(
                  "birds",
                  List.of(
                      new BsonDocument(
                          "$match", new BsonDocument("class", new BsonString("parrots"))))));

      var result =
          ViewDefinition.fromCollectionInfos(
              new MongoNamespace("animals", "parrots"),
              UUID.randomUUID(),
              new MongoDbCollectionInfos(
                  ImmutableMap.of(new MongoNamespace("animals", "parrots"), viewInfo)));

      Assert.assertFalse(result.exists());
      Assert.assertEquals("parrots", result.getName());
      Assert.assertTrue(result.getEffectivePipeline().isEmpty());
    }

    @Test
    public void testGivenNamespaceRepresentsCollectionNotView() {
      var info =
          new MongoDbCollectionInfo.Collection(
              "birds", new MongoDbCollectionInfo.Collection.Info(UUID.randomUUID()));

      var result =
          ViewDefinition.fromCollectionInfos(
              new MongoNamespace("animals", "birds"),
              UUID.randomUUID(),
              new MongoDbCollectionInfos(
                  ImmutableMap.of(new MongoNamespace("animals", "birds"), info)));

      Assert.assertFalse(result.exists());
      Assert.assertEquals("birds", result.getName());
      Assert.assertTrue(result.getEffectivePipeline().isEmpty());
    }

    @Test
    public void testSourceCollectionUuidMismatch() {
      var viewInfo =
          new MongoDbCollectionInfo.View(
              "parrots",
              new MongoDbCollectionInfo.View.Options(
                  "birds",
                  List.of(
                      new BsonDocument(
                          "$match", new BsonDocument("class", new BsonString("parrots"))))));

      var parentCollectionUuid = UUID.randomUUID();
      var parentCollectionInfo =
          new MongoDbCollectionInfo.Collection(
              "birds", new MongoDbCollectionInfo.Collection.Info(parentCollectionUuid));

      var result =
          ViewDefinition.fromCollectionInfos(
              new MongoNamespace("animals", "parrots"),
              UUID.randomUUID(), // random UUID instead of parentCollectionUuid
              new MongoDbCollectionInfos(
                  ImmutableMap.of(
                      new MongoNamespace("animals", "birds"),
                      parentCollectionInfo,
                      new MongoNamespace("animals", "parrots"),
                      viewInfo)));

      Assert.assertFalse(result.exists());
      Assert.assertEquals("parrots", result.getName());
      Assert.assertFalse(result.getEffectivePipeline().isPresent());
    }
  }
}
