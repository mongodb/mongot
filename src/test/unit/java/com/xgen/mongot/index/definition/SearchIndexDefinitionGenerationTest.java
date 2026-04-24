package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.definition.AnalyzerBoundSearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import java.util.List;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      SearchIndexDefinitionGenerationTest.DeserializationTest.class,
      SearchIndexDefinitionGenerationTest.SerializationTest.class,
      SearchIndexDefinitionGenerationTest.ClassTest.class,
    })
public class SearchIndexDefinitionGenerationTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "index-definition-generation-deserialization";
    private static final BsonDeserializationTestSuite<IndexDefinitionGeneration> TEST_SUITE =
        fromDocument(
            DefinitionTests.RESOURCES_PATH, SUITE_NAME, SearchIndexDefinitionGeneration::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<IndexDefinitionGeneration> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<IndexDefinitionGeneration> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<IndexDefinitionGeneration>>
        data() {
      return TEST_SUITE.withExamples(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexDefinitionGeneration> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple", SearchIndexDefinitionGenerationTest.simple());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "index-definition-generation-serialization";
    private static final BsonSerializationTestSuite<IndexDefinitionGeneration> TEST_SUITE =
        fromEncodable(DefinitionTests.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<IndexDefinitionGeneration> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<IndexDefinitionGeneration> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<IndexDefinitionGeneration>> data() {
      return List.of(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexDefinitionGeneration> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple", SearchIndexDefinitionGenerationTest.simple());
    }
  }

  private static IndexDefinitionGeneration simple() {
    AnalyzerBoundSearchIndexDefinition definition =
        AnalyzerBoundSearchIndexDefinitionBuilder.builder()
            .index(
                SearchIndexDefinitionBuilder.builder()
                    .indexId(new ObjectId("507f191e810c19729de860ea"))
                    .name("index")
                    .database("database")
                    .lastObservedCollectionName("collection")
                    .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
                    .dynamicMapping()
                    .build())
            .build();
    return SearchIndexDefinitionGenerationBuilder.builder()
        .definition(definition)
        .generation(1, 2)
        .build();
  }

  public static class ClassTest {
    @Test
    public void testIncrementsUser() {
      var firstIndex =
          AnalyzerBoundSearchIndexDefinitionBuilder.builder()
              .index(
                  SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build())
              .build();
      var secondIndex =
          AnalyzerBoundSearchIndexDefinitionBuilder.builder()
              .index(
                  SearchIndexDefinitionBuilder.builder()
                      .defaultMetadata()
                      .dynamicMapping()
                      .analyzerName("lucene.cjk")
                      .build())
              .build();

      var first = new SearchIndexDefinitionGeneration(firstIndex, Generation.FIRST);
      var second = first.incrementUser(secondIndex);
      var expected =
          new SearchIndexDefinitionGeneration(
              secondIndex,
              new Generation(new UserIndexVersion(1), first.generation().indexFormatVersion));
      Assert.assertEquals(expected, second);
    }
  }
}
