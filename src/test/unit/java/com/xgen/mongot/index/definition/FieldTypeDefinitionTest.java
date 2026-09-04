package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.load;

import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.AutocompleteFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.BooleanFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DateFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.GeoFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.KnnVectorFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.ObjectIdFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SortableDateBetaV1FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SortableNumberBetaV1FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SortableStringBetaV1FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFacetFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.UuidFieldDefinitionBuilder;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      FieldTypeDefinitionTest.DeserializationTest.class,
      FieldTypeDefinitionTest.SerializationTest.class,
      FieldTypeDefinitionTest.DefinitionTest.class,
    })
public class FieldTypeDefinitionTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "field-type-deserialization";
    private static final BsonDeserializationTestSuite<FieldTypeDefinition> TEST_SUITE =
        fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, FieldTypeDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<FieldTypeDefinition> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<FieldTypeDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<FieldTypeDefinition>>
        data() {
      return TEST_SUITE.withExamples(
          autocomplete(),
          autoEmbed(),
          bool(),
          date(),
          document(),
          embeddedDocuments(),
          geo(),
          knnVector(),
          number(),
          objectId(),
          sortableDateBetaV1(),
          sortableNumberBetaV1(),
          sortableStringBetaV1(),
          string(),
          stringFacet(),
          token(),
          uuid());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> autocomplete() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "autocomplete",
          AutocompleteFieldDefinitionBuilder.builder()
              .tokenizationStrategy(AutocompleteFieldDefinition.TokenizationStrategy.N_GRAM)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> autoEmbed() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "autoEmbed",
          new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("description")));
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> bool() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "boolean", BooleanFieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> date() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "date", DateFieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> document() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "document", DocumentFieldDefinitionBuilder.builder().dynamic(false).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> embeddedDocuments() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "embeddedDocuments",
          EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(false).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> geo() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "geo", GeoFieldDefinitionBuilder.builder().indexShapes(false).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> knnVector() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "knnVector",
          KnnVectorFieldDefinitionBuilder.builder()
              .similarity(VectorSimilarity.DOT_PRODUCT)
              .dimensions(100)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> number() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "number",
          NumericFieldDefinitionBuilder.builder()
              .representation(NumericFieldOptions.Representation.INT64)
              .buildNumberField());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> objectId() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "objectId", ObjectIdFieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition>
        sortableDateBetaV1() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "sortableDateBetaV1", SortableDateBetaV1FieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition>
        sortableNumberBetaV1() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "sortableNumberBetaV1", SortableNumberBetaV1FieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition>
        sortableStringBetaV1() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "sortableStringBetaV1", SortableStringBetaV1FieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> string() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "string", StringFieldDefinitionBuilder.builder().store(false).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> stringFacet() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "stringFacet", StringFacetFieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> token() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "token", TokenFieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FieldTypeDefinition> uuid() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "uuid", UuidFieldDefinitionBuilder.builder().build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "field-type-serialization";
    private static final BsonSerializationTestSuite<FieldTypeDefinition> TEST_SUITE =
        load(DefinitionTests.RESOURCES_PATH, SUITE_NAME, FieldTypeDefinition::toBson);

    private final BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<FieldTypeDefinition>> data() {
      return Arrays.asList(
          autocomplete(),
          autoEmbed(),
          bool(),
          date(),
          document(),
          embeddedDocuments(),
          geo(),
          knnVector(),
          number(),
          objectId(),
          sortableDateBetaV1(),
          sortableNumberBetaV1(),
          sortableStringBetaV1(),
          string(),
          stringFacet(),
          token(),
          uuid());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> autocomplete() {
      return BsonSerializationTestSuite.TestSpec.create(
          "autocomplete",
          AutocompleteFieldDefinitionBuilder.builder()
              .tokenizationStrategy(AutocompleteFieldDefinition.TokenizationStrategy.EDGE_GRAM)
              .minGrams(12)
              .maxGrams(19)
              .foldDiacritics(true)
              .analyzer("lucene.standard")
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> autoEmbed() {
      return BsonSerializationTestSuite.TestSpec.create(
          "autoEmbed",
          new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("description")));
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> bool() {
      return BsonSerializationTestSuite.TestSpec.create(
          "boolean", BooleanFieldDefinitionBuilder.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> date() {
      return BsonSerializationTestSuite.TestSpec.create(
          "date", DateFieldDefinitionBuilder.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> document() {
      return BsonSerializationTestSuite.TestSpec.create(
          "document", DocumentFieldDefinitionBuilder.builder().dynamic(false).build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> embeddedDocuments() {
      return BsonSerializationTestSuite.TestSpec.create(
          "embeddedDocuments",
          EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(false).build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> geo() {
      return BsonSerializationTestSuite.TestSpec.create(
          "geo", GeoFieldDefinitionBuilder.builder().indexShapes(false).build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> knnVector() {
      return BsonSerializationTestSuite.TestSpec.create(
          "knnVector",
          KnnVectorFieldDefinitionBuilder.builder()
              .similarity(VectorSimilarity.EUCLIDEAN)
              .dimensions(1000)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> number() {
      return BsonSerializationTestSuite.TestSpec.create(
          "number",
          NumericFieldDefinitionBuilder.builder()
              .representation(NumericFieldOptions.Representation.INT64)
              .buildNumberField());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> objectId() {
      return BsonSerializationTestSuite.TestSpec.create(
          "objectId", ObjectIdFieldDefinitionBuilder.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> sortableDateBetaV1() {
      return BsonSerializationTestSuite.TestSpec.create(
          "sortableDateBetaV1", SortableDateBetaV1FieldDefinitionBuilder.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> sortableNumberBetaV1() {
      return BsonSerializationTestSuite.TestSpec.create(
          "sortableNumberBetaV1", SortableNumberBetaV1FieldDefinitionBuilder.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> sortableStringBetaV1() {
      return BsonSerializationTestSuite.TestSpec.create(
          "sortableStringBetaV1", SortableStringBetaV1FieldDefinitionBuilder.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> string() {
      return BsonSerializationTestSuite.TestSpec.create(
          "string", StringFieldDefinitionBuilder.builder().store(false).build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> stringFacet() {
      return BsonSerializationTestSuite.TestSpec.create(
          "stringFacet", StringFacetFieldDefinitionBuilder.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> token() {
      return BsonSerializationTestSuite.TestSpec.create(
          "token", TokenFieldDefinitionBuilder.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<FieldTypeDefinition> uuid() {
      return BsonSerializationTestSuite.TestSpec.create(
          "uuid", UuidFieldDefinitionBuilder.builder().build());
    }
  }

  public static class DefinitionTest {

    @Test
    public void testEquality() {
      TestUtils.assertEqualityGroups(
          () -> AutocompleteFieldDefinitionBuilder.builder().build(),
          () -> BooleanFieldDefinitionBuilder.builder().build(),
          () -> DateFieldDefinitionBuilder.builder().build(),
          () -> DocumentFieldDefinitionBuilder.builder().build(),
          () -> EmbeddedDocumentsFieldDefinitionBuilder.builder().build(),
          () -> GeoFieldDefinitionBuilder.builder().build(),
          () -> NumericFieldDefinitionBuilder.builder().buildNumberField(),
          () -> ObjectIdFieldDefinitionBuilder.builder().build(),
          () -> SortableDateBetaV1FieldDefinitionBuilder.builder().build(),
          () -> SortableNumberBetaV1FieldDefinitionBuilder.builder().build(),
          () -> SortableStringBetaV1FieldDefinitionBuilder.builder().build(),
          () -> StringFieldDefinitionBuilder.builder().build(),
          () -> StringFacetFieldDefinitionBuilder.builder().build(),
          () -> TokenFieldDefinitionBuilder.builder().build(),
          () -> UuidFieldDefinitionBuilder.builder().build());
    }
  }
}
