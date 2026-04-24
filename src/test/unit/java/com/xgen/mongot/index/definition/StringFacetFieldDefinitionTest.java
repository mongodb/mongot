package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.load;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.StringFacetFieldDefinitionBuilder;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      StringFacetFieldDefinitionTest.DeserializationTest.class,
      StringFacetFieldDefinitionTest.SerializationTest.class,
      StringFacetFieldDefinitionTest.DefinitionTest.class
    })
public class StringFacetFieldDefinitionTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "stringFacet-deserialization";
    private static final BsonDeserializationTestSuite<StringFacetFieldDefinition> TEST_SUITE =
        fromDocument(
            DefinitionTests.RESOURCES_PATH, SUITE_NAME, StringFacetFieldDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<StringFacetFieldDefinition> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<StringFacetFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<StringFacetFieldDefinition>>
        data() {
      return TEST_SUITE.withExamples(empty());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<StringFacetFieldDefinition> empty() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty", StringFacetFieldDefinitionBuilder.builder().build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "stringFacet-serialization";
    private static final BsonSerializationTestSuite<StringFacetFieldDefinition> TEST_SUITE =
        load(
            DefinitionTests.RESOURCES_PATH,
            SUITE_NAME,
            StringFacetFieldDefinition::fieldTypeToBson);

    private final BsonSerializationTestSuite.TestSpec<StringFacetFieldDefinition> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<StringFacetFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<StringFacetFieldDefinition>> data() {
      return Arrays.asList(empty());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<StringFacetFieldDefinition> empty() {
      return BsonSerializationTestSuite.TestSpec.create(
          "empty", StringFacetFieldDefinitionBuilder.builder().build());
    }
  }

  public static class DefinitionTest {
    @Test
    public void testGetType() {
      var definition = StringFacetFieldDefinitionBuilder.builder().build();
      Assert.assertEquals(FieldTypeDefinition.Type.STRING_FACET, definition.getType());
    }

    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(() -> StringFacetFieldDefinitionBuilder.builder().build());
    }
  }
}
