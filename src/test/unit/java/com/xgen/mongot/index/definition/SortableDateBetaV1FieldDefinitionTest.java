package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.load;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.SortableDateBetaV1FieldDefinitionBuilder;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      SortableDateBetaV1FieldDefinitionTest.DeserializationTest.class,
      SortableDateBetaV1FieldDefinitionTest.SerializationTest.class,
      SortableDateBetaV1FieldDefinitionTest.DefinitionTest.class
    })
public class SortableDateBetaV1FieldDefinitionTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "sortable-date-beta-v1-deserialization";
    private static final BsonDeserializationTestSuite<SortableDateBetaV1FieldDefinition>
        TEST_SUITE =
            fromDocument(
                DefinitionTests.RESOURCES_PATH,
                SUITE_NAME,
                SortableDateBetaV1FieldDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<SortableDateBetaV1FieldDefinition>
        testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<SortableDateBetaV1FieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<
            BsonDeserializationTestSuite.TestSpecWrapper<SortableDateBetaV1FieldDefinition>>
        data() {
      return TEST_SUITE.withExamples(empty());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortableDateBetaV1FieldDefinition>
        empty() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty", SortableDateBetaV1FieldDefinitionBuilder.builder().build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "sortable-date-beta-v1-serialization";
    private static final BsonSerializationTestSuite<SortableDateBetaV1FieldDefinition> TEST_SUITE =
        load(
            DefinitionTests.RESOURCES_PATH,
            SUITE_NAME,
            SortableDateBetaV1FieldDefinition::fieldTypeToBson);

    private final BsonSerializationTestSuite.TestSpec<SortableDateBetaV1FieldDefinition> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<SortableDateBetaV1FieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<SortableDateBetaV1FieldDefinition>>
        data() {
      return Arrays.asList(empty());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<SortableDateBetaV1FieldDefinition> empty() {
      return BsonSerializationTestSuite.TestSpec.create(
          "empty", SortableDateBetaV1FieldDefinitionBuilder.builder().build());
    }
  }

  public static class DefinitionTest {

    @Test
    public void testGetType() {
      SortableDateBetaV1FieldDefinition definition =
          SortableDateBetaV1FieldDefinitionBuilder.builder().build();
      Assert.assertEquals(FieldTypeDefinition.Type.SORTABLE_DATE_BETA_V1, definition.getType());
    }

    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(
          () -> SortableDateBetaV1FieldDefinitionBuilder.builder().build());
    }
  }
}
