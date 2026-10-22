package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.load;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.DateFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.GeoFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      DocumentFieldDefinitionTest.DeserializationTest.class,
      DocumentFieldDefinitionTest.SerializationTest.class,
      DocumentFieldDefinitionTest.DefinitionTest.class,
    })
public class DocumentFieldDefinitionTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "document-deserialization";
    private static final BsonDeserializationTestSuite<DocumentFieldDefinition> TEST_SUITE =
        fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, DocumentFieldDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<DocumentFieldDefinition> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<DocumentFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<DocumentFieldDefinition>>
        data() {
      return TEST_SUITE.withExamples(
          empty(),
          explicitDynamic(),
          explicitEmptyFields(),
          explicitPopulatedFields(),
          explicitEmptyArray());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<DocumentFieldDefinition> empty() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty", DocumentFieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<DocumentFieldDefinition>
        explicitDynamic() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit dynamic", DocumentFieldDefinitionBuilder.builder().dynamic(false).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<DocumentFieldDefinition>
        explicitEmptyFields() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit empty fields", DocumentFieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<DocumentFieldDefinition>
        explicitPopulatedFields() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit populated fields",
          DocumentFieldDefinitionBuilder.builder()
              .field(
                  "a",
                  FieldDefinitionBuilder.builder()
                      .string(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .field(
                  "b",
                  FieldDefinitionBuilder.builder()
                      .date(DateFieldDefinitionBuilder.builder().build())
                      .geo(GeoFieldDefinitionBuilder.builder().build())
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<DocumentFieldDefinition>
        explicitEmptyArray() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit field mapped to empty array",
          DocumentFieldDefinitionBuilder.builder()
              .field("a", FieldDefinitionBuilder.builder().build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "document-serialization";
    private static final BsonSerializationTestSuite<DocumentFieldDefinition> TEST_SUITE =
        load(DefinitionTests.RESOURCES_PATH, SUITE_NAME, DocumentFieldDefinition::fieldTypeToBson);

    private final BsonSerializationTestSuite.TestSpec<DocumentFieldDefinition> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<DocumentFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<DocumentFieldDefinition>> data() {
      return Arrays.asList(simple(), populatedFields());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<DocumentFieldDefinition> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple", DocumentFieldDefinitionBuilder.builder().dynamic(false).build());
    }

    private static BsonSerializationTestSuite.TestSpec<DocumentFieldDefinition> populatedFields() {
      return BsonSerializationTestSuite.TestSpec.create(
          "populated fields",
          DocumentFieldDefinitionBuilder.builder()
              .field(
                  "a",
                  FieldDefinitionBuilder.builder()
                      .string(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .field(
                  "b",
                  FieldDefinitionBuilder.builder()
                      .date(DateFieldDefinitionBuilder.builder().build())
                      .geo(GeoFieldDefinitionBuilder.builder().build())
                      .build())
              .build());
    }
  }

  public static class DefinitionTest {

    @Test
    public void testGetType() {
      DocumentFieldDefinition definition = DocumentFieldDefinitionBuilder.builder().build();
      Assert.assertEquals(FieldTypeDefinition.Type.DOCUMENT, definition.getType());
    }

    @Test
    public void testIsDynamic() {
      DocumentFieldDefinition definition =
          DocumentFieldDefinitionBuilder.builder().dynamic(true).build();
      Assert.assertEquals(definition.dynamic(), new DynamicDefinition.Boolean(true));
    }

    @Test
    public void testGetFields() {
      FieldDefinition a =
          FieldDefinitionBuilder.builder()
              .date(DateFieldDefinitionBuilder.builder().build())
              .document(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
              .string(
                  StringFieldDefinitionBuilder.builder()
                      .analyzerName("foo")
                      .searchAnalyzerName("bar")
                      .ignoreAbove(13)
                      .build())
              .build();

      FieldDefinition b =
          FieldDefinitionBuilder.builder()
              .document(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build();

      DocumentFieldDefinition definition =
          DocumentFieldDefinitionBuilder.builder().field("a", a).field("b", b).build();

      Assert.assertEquals("a", Optional.of(a), definition.getField("a"));
      Assert.assertEquals("b", Optional.of(b), definition.getField("b"));
      Assert.assertEquals("c", Optional.empty(), definition.getField("c"));

      var fields = Map.ofEntries(Map.entry("a", a), Map.entry("b", b));
      Assert.assertEquals("fields", fields, definition.fields());
    }

    @Test
    public void testContainsSearchAutoEmbedField() {
      FieldDefinition autoEmbed =
          FieldDefinitionBuilder.builder()
              .searchAutoEmbed(new SearchAutoEmbedFieldDefinition("voyage-3-large"))
              .build();
      FieldDefinition nestedAutoEmbed =
          FieldDefinitionBuilder.builder()
              .searchAutoEmbed(new SearchAutoEmbedFieldDefinition("voyage-3-large"))
              .build();
      FieldDefinition plain =
          FieldDefinitionBuilder.builder()
              .date(DateFieldDefinitionBuilder.builder().build())
              .build();

      DocumentFieldDefinition topLevel =
          DocumentFieldDefinitionBuilder.builder().field("content", autoEmbed).build();
      Assert.assertTrue("top-level auto-embed", topLevel.containsSearchAutoEmbedField());

      DocumentFieldDefinition nested =
          DocumentFieldDefinitionBuilder.builder()
              .field(
                  "parent",
                  FieldDefinitionBuilder.builder()
                      .document(
                          DocumentFieldDefinitionBuilder.builder()
                              .field("content", nestedAutoEmbed)
                              .build())
                      .build())
              .build();
      Assert.assertTrue("auto-embed nested under document", nested.containsSearchAutoEmbedField());

      DocumentFieldDefinition without =
          DocumentFieldDefinitionBuilder.builder()
              .field("title", plain)
              .field(
                  "parent",
                  FieldDefinitionBuilder.builder()
                      .document(
                          DocumentFieldDefinitionBuilder.builder().field("title", plain).build())
                      .build())
              .build();
      Assert.assertFalse("no auto-embed anywhere", without.containsSearchAutoEmbedField());
    }

    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(
          () -> DocumentFieldDefinitionBuilder.builder().dynamic(false).build(),
          () -> DocumentFieldDefinitionBuilder.builder().dynamic(true).build(),
          () ->
              DocumentFieldDefinitionBuilder.builder()
                  .field(
                      "field",
                      FieldDefinitionBuilder.builder()
                          .date(DateFieldDefinitionBuilder.builder().build())
                          .build())
                  .build(),
          () ->
              DocumentFieldDefinitionBuilder.builder()
                  .field(
                      "field",
                      FieldDefinitionBuilder.builder()
                          .geo(GeoFieldDefinitionBuilder.builder().build())
                          .build())
                  .build());
    }
  }
}
