package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.load;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.definition.AutocompleteFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TypeSetDefinitionBuilder;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      TypeSetDefinitionTest.DeserializationTest.class,
      TypeSetDefinitionTest.SerializationTest.class,
    })
public class TypeSetDefinitionTest {
  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "typeSet-deserialization";
    private static final BsonDeserializationTestSuite<TypeSetDefinition> TEST_SUITE =
        fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, TypeSetDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<TypeSetDefinition> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<TypeSetDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<TypeSetDefinition>> data() {
      return TEST_SUITE.withExamples(simple(), withMulti());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<TypeSetDefinition> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          TypeSetDefinitionBuilder.builder()
              .name("foo")
              .addType(AutocompleteFieldDefinitionBuilder.builder().build())
              .addType(NumericFieldDefinitionBuilder.builder().buildNumberField())
              .addType(StringFieldDefinitionBuilder.builder().build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TypeSetDefinition> withMulti() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with multi",
          TypeSetDefinitionBuilder.builder()
              .name("foo")
              .addType(NumericFieldDefinitionBuilder.builder().buildNumberField())
              .addType(
                  StringFieldDefinitionBuilder.builder()
                      .multi(
                          "english",
                          StringFieldDefinitionBuilder.builder()
                              .analyzerName("lucene.english")
                              .build())
                      .multi(
                          "french",
                          StringFieldDefinitionBuilder.builder()
                              .analyzerName("lucene.french")
                              .build())
                      .build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "typeSet-serialization";
    private static final BsonSerializationTestSuite<TypeSetDefinition> TEST_SUITE =
        load(DefinitionTests.RESOURCES_PATH, SUITE_NAME, TypeSetDefinition::toBson);

    private final BsonSerializationTestSuite.TestSpec<TypeSetDefinition> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<TypeSetDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<TypeSetDefinition>> data() {
      return List.of(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<TypeSetDefinition> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          TypeSetDefinitionBuilder.builder()
              .name("foo")
              .addType(AutocompleteFieldDefinitionBuilder.builder().build())
              .addType(NumericFieldDefinitionBuilder.builder().buildNumberField())
              .addType(StringFieldDefinitionBuilder.builder().build())
              .build());
    }
  }
}
