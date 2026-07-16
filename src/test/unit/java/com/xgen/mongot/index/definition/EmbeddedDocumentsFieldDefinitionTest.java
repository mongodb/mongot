package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.load;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.DateFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.GeoFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.KnnVectorFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SortableDateBetaV1FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import java.util.Arrays;
import java.util.List;
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
      EmbeddedDocumentsFieldDefinitionTest.DefinitionTest.class,
      EmbeddedDocumentsFieldDefinitionTest.DeserializationTest.class,
      EmbeddedDocumentsFieldDefinitionTest.SerializationTest.class,
    })
public class EmbeddedDocumentsFieldDefinitionTest {

  public static class DefinitionTest {
    @Test
    public void testGetType() {
      EmbeddedDocumentsFieldDefinition definition =
          EmbeddedDocumentsFieldDefinitionBuilder.builder().build();
      Assert.assertEquals(FieldTypeDefinition.Type.EMBEDDED_DOCUMENTS, definition.getType());
    }

    @Test
    public void testIsDynamic() {
      EmbeddedDocumentsFieldDefinition definition =
          EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build();
      Assert.assertEquals(definition.dynamic(), new DynamicDefinition.Boolean(true));
    }

    @Test
    public void testStoredSourceDefinition() {
      EmbeddedDocumentsFieldDefinition definition =
          EmbeddedDocumentsFieldDefinitionBuilder.builder()
              .storedSource(
                  StoredSourceDefinition.create(
                      StoredSourceDefinition.Mode.INCLUSION, List.of("b")))
              .build();
      Assert.assertEquals(
          Optional.of(
              StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("b"))),
          definition.storedSourceDefinition());
    }

    @Test
    public void testGetFieldAndFields() {
      FieldDefinition a =
          FieldDefinitionBuilder.builder()
              .date(DateFieldDefinitionBuilder.builder().build())
              .embeddedDocuments(
                  EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
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
              .embeddedDocuments(
                  EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
              .build();

      EmbeddedDocumentsFieldDefinition definition =
          EmbeddedDocumentsFieldDefinitionBuilder.builder().field("a", a).field("b", b).build();

      Assert.assertEquals("a", Optional.of(a), definition.getField("a"));
      Assert.assertEquals("b", Optional.of(b), definition.getField("b"));
      Assert.assertEquals("c", Optional.empty(), definition.getField("c"));

      var fields = Map.ofEntries(Map.entry("a", a), Map.entry("b", b));
      Assert.assertEquals("fields", fields, definition.fields());
    }

    @Test
    public void testGetFieldHierarchyContext() {
      FieldDefinition a =
          FieldDefinitionBuilder.builder()
              .date(DateFieldDefinitionBuilder.builder().build())
              .embeddedDocuments(
                  EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
              .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
              .string(
                  StringFieldDefinitionBuilder.builder()
                      .analyzerName("foo")
                      .searchAnalyzerName("bar")
                      .ignoreAbove(13)
                      .build())
              .build();

      FieldDefinition fieldBDotC =
          FieldDefinitionBuilder.builder()
              .embeddedDocuments(
                  EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
              .build();

      FieldDefinition b =
          FieldDefinitionBuilder.builder()
              .document(
                  DocumentFieldDefinitionBuilder.builder()
                      .field("c", fieldBDotC)
                      .dynamic(true)
                      .build())
              .build();

      EmbeddedDocumentsFieldDefinition definition =
          EmbeddedDocumentsFieldDefinitionBuilder.builder().field("a", a).field("b", b).build();

      FieldHierarchyContext expected =
          new FieldHierarchyContext(
              ImmutableMap.of(
                  FieldPath.parse("a"),
                  a,
                  FieldPath.parse("b"),
                  b,
                  FieldPath.parse("b.c"),
                  fieldBDotC),
              ImmutableMap.of(
                  FieldPath.parse("a"),
                  ImmutableMap.of(),
                  FieldPath.parse("b.c"),
                  ImmutableMap.of()),
              ImmutableMap.of(FieldPath.parse("a"), definition, FieldPath.parse("b.c"), definition),
              2);
      Assert.assertEquals(
          "unexpected FieldHierarchyContext", expected, definition.fieldHierarchyContext());
    }

    @Test
    public void testSortableType() {
      FieldDefinition fields =
          FieldDefinitionBuilder.builder()
              .sortableDateBetaV1(SortableDateBetaV1FieldDefinitionBuilder.builder().build())
              .build();

      try {
        EmbeddedDocumentsFieldDefinition.create(
            new DynamicDefinition.Boolean(false), Map.of("a", fields), Optional.empty());
        Assert.fail("should throw");
      } catch (IllegalEmbeddedFieldException e) {
        Assert.assertEquals(
            "cannot define fields of type sortableDateBetaV1 inside an embeddedDocuments field",
            e.getMessage());
        return;
      }

      Assert.fail("unreachable");
    }

    @Test
    public void testKnnVectorType() {
      FieldDefinition fields =
          FieldDefinitionBuilder.builder()
              .knnVector(
                  KnnVectorFieldDefinitionBuilder.builder()
                      .dimensions(1024)
                      .similarity(VectorSimilarity.EUCLIDEAN)
                      .build())
              .build();

      try {
        EmbeddedDocumentsFieldDefinition.create(
            new DynamicDefinition.Boolean(false), Map.of("a", fields), Optional.empty());
        Assert.fail("should throw");
      } catch (IllegalEmbeddedFieldException e) {
        Assert.assertEquals(
            "cannot define fields of type knnVector inside an embeddedDocuments field",
            e.getMessage());
        return;
      }

      Assert.fail("unreachable");
    }

    @Test
    public void testAutoEmbedVectorType() {
      FieldDefinition fields =
          FieldDefinitionBuilder.builder()
              .searchAutoEmbed(
                  new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content")))
              .build();

      try {
        EmbeddedDocumentsFieldDefinition.create(
            new DynamicDefinition.Boolean(false), Map.of("a", fields), Optional.empty());
        Assert.fail("should throw");
      } catch (IllegalEmbeddedFieldException e) {
        Assert.assertEquals(
            "cannot define fields of type autoEmbedVector inside an embeddedDocuments field",
            e.getMessage());
        return;
      }

      Assert.fail("unreachable");
    }

    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(
          () -> EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(false).build(),
          () -> EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build(),
          () ->
              EmbeddedDocumentsFieldDefinitionBuilder.builder()
                  .field(
                      "field",
                      FieldDefinitionBuilder.builder()
                          .date(DateFieldDefinitionBuilder.builder().build())
                          .build())
                  .build(),
          () ->
              EmbeddedDocumentsFieldDefinitionBuilder.builder()
                  .field(
                      "field",
                      FieldDefinitionBuilder.builder()
                          .geo(GeoFieldDefinitionBuilder.builder().build())
                          .build())
                  .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "embeddedDocuments-deserialization";
    private static final BsonDeserializationTestSuite<EmbeddedDocumentsFieldDefinition> TEST_SUITE =
        fromDocument(
            DefinitionTests.RESOURCES_PATH, SUITE_NAME, EmbeddedDocumentsFieldDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<EmbeddedDocumentsFieldDefinition>
        testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<EmbeddedDocumentsFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<
            BsonDeserializationTestSuite.TestSpecWrapper<EmbeddedDocumentsFieldDefinition>>
        data() {
      return TEST_SUITE.withExamples(
          empty(),
          explicitDynamic(),
          explicitEmptyFields(),
          explicitPopulatedFields(),
          explicitEmptyArray(),
          storedSourceConfigured());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<EmbeddedDocumentsFieldDefinition>
        empty() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty", EmbeddedDocumentsFieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<EmbeddedDocumentsFieldDefinition>
        explicitDynamic() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit dynamic",
          EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(false).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<EmbeddedDocumentsFieldDefinition>
        explicitEmptyFields() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit empty fields", EmbeddedDocumentsFieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<EmbeddedDocumentsFieldDefinition>
        explicitPopulatedFields() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit populated fields",
          EmbeddedDocumentsFieldDefinitionBuilder.builder()
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

    private static BsonDeserializationTestSuite.ValidSpec<EmbeddedDocumentsFieldDefinition>
        explicitEmptyArray() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit field mapped to empty array",
          EmbeddedDocumentsFieldDefinitionBuilder.builder()
              .field("a", FieldDefinitionBuilder.builder().build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<EmbeddedDocumentsFieldDefinition>
        storedSourceConfigured() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "storedSource configured",
          EmbeddedDocumentsFieldDefinitionBuilder.builder()
              .dynamic(true)
              .storedSource(
                  StoredSourceDefinition.create(
                      StoredSourceDefinition.Mode.INCLUSION, List.of("b")))
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "embeddedDocuments-serialization";
    private static final BsonSerializationTestSuite<EmbeddedDocumentsFieldDefinition> TEST_SUITE =
        load(
            DefinitionTests.RESOURCES_PATH,
            SUITE_NAME,
            EmbeddedDocumentsFieldDefinition::fieldTypeToBson);

    private final BsonSerializationTestSuite.TestSpec<EmbeddedDocumentsFieldDefinition> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<EmbeddedDocumentsFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<EmbeddedDocumentsFieldDefinition>>
        data() {
      return Arrays.asList(simple(), populatedFields());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<EmbeddedDocumentsFieldDefinition> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple", EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(false).build());
    }

    private static BsonSerializationTestSuite.TestSpec<EmbeddedDocumentsFieldDefinition>
        populatedFields() {
      return BsonSerializationTestSuite.TestSpec.create(
          "populated fields",
          EmbeddedDocumentsFieldDefinitionBuilder.builder()
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
              .storedSource(
                  StoredSourceDefinition.create(
                      StoredSourceDefinition.Mode.INCLUSION, List.of("b")))
              .build());
    }
  }
}
