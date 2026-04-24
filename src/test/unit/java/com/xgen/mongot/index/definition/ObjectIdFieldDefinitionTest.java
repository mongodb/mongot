package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.load;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.ObjectIdFieldDefinitionBuilder;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      ObjectIdFieldDefinitionTest.DeserializationTest.class,
      ObjectIdFieldDefinitionTest.SerializationTest.class,
      ObjectIdFieldDefinitionTest.DefinitionTest.class
    })
public class ObjectIdFieldDefinitionTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "objectId-deserialization";
    private static final BsonDeserializationTestSuite<ObjectIdFieldDefinition> TEST_SUITE =
        fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, ObjectIdFieldDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<ObjectIdFieldDefinition> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<ObjectIdFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<ObjectIdFieldDefinition>>
        data() {
      return TEST_SUITE.withExamples(empty());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<ObjectIdFieldDefinition> empty() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty", ObjectIdFieldDefinitionBuilder.builder().build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "objectId-serialization";
    private static final BsonSerializationTestSuite<ObjectIdFieldDefinition> TEST_SUITE =
        load(DefinitionTests.RESOURCES_PATH, SUITE_NAME, ObjectIdFieldDefinition::fieldTypeToBson);

    private final BsonSerializationTestSuite.TestSpec<ObjectIdFieldDefinition> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<ObjectIdFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<ObjectIdFieldDefinition>> data() {
      return Arrays.asList(empty());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<ObjectIdFieldDefinition> empty() {
      return BsonSerializationTestSuite.TestSpec.create(
          "empty", ObjectIdFieldDefinitionBuilder.builder().build());
    }
  }

  public static class DefinitionTest {
    @Test
    public void testGetType() {
      ObjectIdFieldDefinition definition = ObjectIdFieldDefinitionBuilder.builder().build();
      Assert.assertEquals(FieldTypeDefinition.Type.OBJECT_ID, definition.getType());
    }

    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(() -> ObjectIdFieldDefinitionBuilder.builder().build());
    }
  }
}
