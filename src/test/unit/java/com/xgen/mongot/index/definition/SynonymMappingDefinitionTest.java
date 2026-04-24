package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.SynonymMappingDefinitionBuilder;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      SynonymMappingDefinitionTest.DeserializationTest.class,
      SynonymMappingDefinitionTest.SerializationTest.class,
      SynonymMappingDefinitionTest.DefinitionTest.class,
    })
public class SynonymMappingDefinitionTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "synonym-deserialization";
    private static final BsonDeserializationTestSuite<SynonymMappingDefinition> TEST_SUITE =
        fromDocument(
            DefinitionTests.RESOURCES_PATH, SUITE_NAME, SynonymMappingDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<SynonymMappingDefinition> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<SynonymMappingDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<SynonymMappingDefinition>>
        data() {
      return TEST_SUITE.withExamples(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<SynonymMappingDefinition> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          SynonymMappingDefinitionBuilder.builder()
              .name("simple-synonyms")
              .synonymSourceDefinition("collection")
              .analyzer("lucene.standard")
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "synonym-serialization";
    private static final BsonSerializationTestSuite<SynonymMappingDefinition> TEST_SUITE =
        fromEncodable(DefinitionTests.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<SynonymMappingDefinition> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<SynonymMappingDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<SynonymMappingDefinition>> data() {
      return Arrays.asList(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<SynonymMappingDefinition> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          SynonymMappingDefinitionBuilder.builder()
              .name("simple")
              .synonymSourceDefinition("collection")
              .analyzer("lucene.english")
              .build());
    }
  }

  public static class DefinitionTest {
    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(
          () ->
              SynonymMappingDefinitionBuilder.builder()
                  .analyzer("lucene.standard")
                  .name("synonyms")
                  .synonymSourceDefinition("source")
                  .build(),
          () ->
              SynonymMappingDefinitionBuilder.builder()
                  .analyzer("newAnalyzer")
                  .name("synonyms")
                  .synonymSourceDefinition("source")
                  .build(),
          () ->
              SynonymMappingDefinitionBuilder.builder()
                  .analyzer("lucene.standard")
                  .name("differentName")
                  .synonymSourceDefinition("source")
                  .build(),
          () ->
              SynonymMappingDefinitionBuilder.builder()
                  .analyzer("lucene.standard")
                  .name("synonyms")
                  .synonymSourceDefinition("differentSource")
                  .build());
    }
  }
}
