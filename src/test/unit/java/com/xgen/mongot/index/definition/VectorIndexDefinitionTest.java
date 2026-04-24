package com.xgen.mongot.index.definition;

import static com.xgen.mongot.index.definition.StoredSourceDefinition.Mode.EXCLUSION;
import static com.xgen.mongot.index.definition.StoredSourceDefinition.Mode.INCLUSION;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.truth.Truth;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      VectorIndexDefinitionTest.DeserializationTest.class,
      VectorIndexDefinitionTest.SerializationTest.class,
      VectorIndexDefinitionTest.NestedRootTest.class
    })
public class VectorIndexDefinitionTest {

  /** Tests for VectorIndexDefinition with nestedRoot (flat fields under an array root). */
  @RunWith(org.junit.runners.JUnit4.class)
  public static class NestedRootTest {

    private static final VectorFieldSpecification SIMPLE_SPEC =
        new VectorFieldSpecification(
            128,
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm());

    @Test
    public void testGetNestedRoot_WhenPresent() {
      VectorIndexDefinition definition =
          VectorIndexDefinitionBuilder.builder()
              .nestedRoot("sections")
              .setFields(
                  List.of(
                      new VectorDataFieldDefinition(
                          FieldPath.parse("sections.embedding"), SIMPLE_SPEC),
                      new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.name"))))
              .build();

      Truth.assertThat(definition.getNestedRoot()).isPresent();
      Truth.assertThat(definition.getNestedRoot().get()).isEqualTo(FieldPath.parse("sections"));
    }

    @Test
    public void testGetNestedRoot_WhenAbsent() {
      VectorIndexDefinition definition =
          VectorIndexDefinitionBuilder.builder()
              .setFields(
                  List.of(new VectorDataFieldDefinition(FieldPath.parse("embedding"), SIMPLE_SPEC)))
              .build();

      Truth.assertThat(definition.getNestedRoot()).isEmpty();
    }

    @Test
    public void testRoundTrip_WithNestedRootAndFlatFields() throws Exception {
      VectorIndexDefinition original =
          VectorIndexDefinitionBuilder.builder()
              .nestedRoot("sections")
              .setFields(
                  List.of(
                      new VectorDataFieldDefinition(
                          FieldPath.parse("sections.embedding1"), SIMPLE_SPEC),
                      new VectorDataFieldDefinition(
                          FieldPath.parse("sections.embedding2"), SIMPLE_SPEC),
                      new VectorIndexFilterFieldDefinition(FieldPath.parse("title"))))
              .build();

      BsonDocument bson = original.toBson();
      VectorIndexDefinition parsed = VectorIndexDefinition.fromBson(bson);

      Truth.assertThat(parsed.getNestedRoot()).isPresent();
      Truth.assertThat(parsed.getNestedRoot().get()).isEqualTo(FieldPath.parse("sections"));
      assertEquals(original.getFields().size(), parsed.getFields().size());
      assertEquals(original, parsed);
    }

    @Test
    public void testMappings_HasNestedRootWhenDefinitionHasNestedRoot() {
      VectorIndexDefinition definition =
          VectorIndexDefinitionBuilder.builder()
              .nestedRoot("sections")
              .setFields(
                  List.of(
                      new VectorDataFieldDefinition(
                          FieldPath.parse("sections.embedding"), SIMPLE_SPEC)))
              .build();

      assertTrue(definition.getMappings().hasNestedRoot());
      assertTrue(definition.getMappings().isNestedRoot(FieldPath.parse("sections")));
      assertFalse(definition.getMappings().isNestedRoot(FieldPath.parse("other")));
    }

    @Test
    public void testToBson_IncludesNestedRoot() {
      VectorIndexDefinition definition =
          VectorIndexDefinitionBuilder.builder()
              .nestedRoot("reviews")
              .setFields(
                  List.of(
                      new VectorDataFieldDefinition(
                          FieldPath.parse("reviews.embedding"), SIMPLE_SPEC)))
              .build();

      BsonDocument bson = definition.toBson();

      Truth.assertThat(bson.containsKey("nestedRoot")).isTrue();
      assertEquals("reviews", bson.getString("nestedRoot").getValue());
    }

    @Test
    public void testFromBson_PreservesNestedRoot() throws Exception {
      VectorIndexDefinition original =
          VectorIndexDefinitionBuilder.builder()
              .nestedRoot("sections")
              .setFields(
                  List.of(
                      new VectorDataFieldDefinition(
                          FieldPath.parse("sections.embedding"), SIMPLE_SPEC)))
              .build();

      BsonDocument bson = original.toBson();
      VectorIndexDefinition parsed = VectorIndexDefinition.fromBson(bson);

      Truth.assertThat(parsed.getNestedRoot()).isPresent();
      Truth.assertThat(parsed.getNestedRoot().get()).isEqualTo(FieldPath.parse("sections"));
    }

    @Test
    public void testIndexIdAtCreationTime_whenSet_returnsValue() {
      var indexId = new ObjectId("507f191e810c19729de860ea");
      var indexIdAtCreationTime = new ObjectId("607f191e810c19729de860eb");

      VectorIndexDefinition definition =
          VectorIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .indexIdAtCreationTime(indexIdAtCreationTime)
              .withCosineVectorField("my.vector.field", 128)
              .build();

      Truth.assertThat(definition.getIndexIdAtCreationTime()).isPresent();
      Truth.assertThat(definition.getIndexIdAtCreationTime().get())
          .isEqualTo(indexIdAtCreationTime);
    }

    @Test
    public void testIndexIdAtCreationTime_whenNotSet_returnsEmpty() {
      VectorIndexDefinition definition =
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 128)
              .build();

      Truth.assertThat(definition.getIndexIdAtCreationTime()).isEmpty();
    }

    @Test
    public void testIndexIdAtCreationTime_sameAsIndexId() {
      var indexId = new ObjectId("507f191e810c19729de860ea");

      VectorIndexDefinition definition =
          VectorIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .indexIdAtCreationTime(indexId)
              .withCosineVectorField("my.vector.field", 128)
              .build();

      Truth.assertThat(definition.getIndexIdAtCreationTime()).isPresent();
      Truth.assertThat(definition.getIndexIdAtCreationTime().get()).isEqualTo(indexId);
      Truth.assertThat(definition.getIndexId()).isEqualTo(indexId);
    }

    @Test
    public void testIndexIdAtCreationTime_differentValue_affectsEquality() {
      var indexId = new ObjectId("507f191e810c19729de860ea");
      var indexIdAtCreationTime = new ObjectId("607f191e810c19729de860eb");

      VectorIndexDefinition definitionWithCreationTime =
          VectorIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .indexIdAtCreationTime(indexIdAtCreationTime)
              .withCosineVectorField("my.vector.field", 128)
              .build();

      VectorIndexDefinition definitionWithoutCreationTime =
          VectorIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .withCosineVectorField("my.vector.field", 128)
              .build();

      // Different because indexIdAtCreationTime (607f) != indexId (507f)
      assertFalse(definitionWithCreationTime.equals(definitionWithoutCreationTime));
      assertNotEquals(
          definitionWithCreationTime.hashCode(), definitionWithoutCreationTime.hashCode());
    }

    @Test
    public void testStoredSource_roundTrip_inclusion() throws Exception {
      VectorIndexDefinition original =
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .storedSource(StoredSourceDefinition.create(INCLUSION, List.of("a", "b.c")))
              .build();

      BsonDocument bson = original.toBson();
      VectorIndexDefinition parsed = VectorIndexDefinition.fromBson(bson);
      assertEquals(original, parsed);
    }

    @Test
    public void testStoredSource_roundTrip_exclusion() throws Exception {
      VectorIndexDefinition original =
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .storedSource(StoredSourceDefinition.create(EXCLUSION, List.of("x", "y.z")))
              .build();

      BsonDocument bson = original.toBson();
      VectorIndexDefinition parsed = VectorIndexDefinition.fromBson(bson);
      assertEquals(original, parsed);
    }

    @Test
    public void testIndexIdAtCreationTime_sameAsIndexId_doesNotAffectEquality() {
      var indexId = new ObjectId("507f191e810c19729de860ea");

      VectorIndexDefinition definitionWithCreationTimeSameAsIndexId =
          VectorIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .indexIdAtCreationTime(indexId) // same as indexId
              .withCosineVectorField("my.vector.field", 128)
              .build();

      VectorIndexDefinition definitionWithoutCreationTime =
          VectorIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .withCosineVectorField("my.vector.field", 128)
              .build();

      // Equal because indexIdAtCreationTime.orElse(indexId) == indexId for both
      assertEquals(definitionWithCreationTimeSameAsIndexId, definitionWithoutCreationTime);
      assertEquals(
          definitionWithCreationTimeSameAsIndexId.hashCode(),
          definitionWithoutCreationTime.hashCode());
    }

    @Test
    public void testIndexIdAtCreationTime_roundTrip() throws Exception {
      var indexIdAtCreationTime = new ObjectId("607f191e810c19729de860eb");

      VectorIndexDefinition original =
          VectorIndexDefinitionBuilder.builder()
              .indexIdAtCreationTime(indexIdAtCreationTime)
              .withCosineVectorField("my.vector.field", 128)
              .build();

      BsonDocument bson = original.toBson();
      VectorIndexDefinition parsed = VectorIndexDefinition.fromBson(bson);

      Truth.assertThat(parsed.getIndexIdAtCreationTime()).isPresent();
      Truth.assertThat(parsed.getIndexIdAtCreationTime().get()).isEqualTo(indexIdAtCreationTime);
      assertEquals(original, parsed);
    }
  }

  private static void setupRegistry() {
    EmbeddingModelCatalog.registerModelConfig(
        "voyage-3-large",
        EmbeddingModelConfig.create(
            "voyage-3-large",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.of("us-east-1"),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(512),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.START),
                    Optional.of(100),
                    Optional.of(1000)),
                new EmbeddingServiceConfig.ErrorHandlingConfig(50, 50L, 10L, 0.1),
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "token123", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty())));
    EmbeddingModelCatalog.registerModelConfig(
        "voyage-3.5",
        EmbeddingModelConfig.create(
            "voyage-3.5",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.of("us-east-1"),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(1024), Optional.empty(), Optional.of(100), Optional.of(1000)),
                new EmbeddingServiceConfig.ErrorHandlingConfig(50, 50L, 10L, 0.1),
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "token123", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty())));
  }

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "vector-index-deserialization";
    private static final BsonDeserializationTestSuite<VectorIndexDefinition> TEST_SUITE =
        fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, VectorIndexDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<VectorIndexDefinition> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<VectorIndexDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestSpecWrapper<VectorIndexDefinition>> data() {
      setupRegistry();
      return TEST_SUITE.withExamples(
          minDimensions(),
          maxDimensions(),
          allSimilarities(),
          withView(),
          withDefaultNumPartitions(),
          withMultipleNumPartitions(),
          withEmptyQuantizationField(),
          withCustomHnswOptions(),
          withCustomMaxEdges(),
          withCustomNumEdgeCandidates(),
          withStoredDocumentInclusion(),
          withStoredDocumentExclusion(),
          textEmbedding(),
          textEmbeddingUsingDotProduct(),
          textEmbeddingWithInvalidModelName(),
          textEmbeddingFullConfig(),
          withFlatIndexingAlgorithm(),
          withExplicitHnswIndexingAlgorithm(),
          withIndexIdAtCreationTime(),
          withIndexIdAtCreationTimeSameAsIndexId(),
          withAutoEmbeddingDefinitionVersion(),
          withMaterializedViewNameFormatVersion(),
          withBothAutoEmbeddingAndMaterializedViewVersions());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition> maxDimensions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "max dimensions",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .withFilterPath("my.filter.field")
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition> minDimensions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "min dimensions",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 1)
              .withFilterPath("my.filter.field")
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition> allSimilarities() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "all similarities",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.cosine.vector.field", 1)
              .withDotProductVectorField("my.dotproduct.vector.field", 2)
              .withEuclideanVectorField("my.euclidean.vector.field", 3)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition> withView() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with view",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.cosine.vector.field", 1)
              .withDotProductVectorField("my.dotproduct.vector.field", 2)
              .withEuclideanVectorField("my.euclidean.vector.field", 3)
              .view(
                  ViewDefinition.existing(
                      "myView",
                      List.of(
                          new BsonDocument("$set", new BsonDocument("cats", new BsonInt32(20))),
                          new BsonDocument("$set", new BsonDocument("dogs", new BsonInt32(40))))))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withDefaultNumPartitions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with default numPartitions",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .withFilterPath("my.filter.field")
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withMultipleNumPartitions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with multiple numPartitions",
          VectorIndexDefinitionBuilder.builder()
              .numPartitions(2)
              .withCosineVectorField("my.vector.field", 2048)
              .withFilterPath("my.filter.field")
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withEmptyQuantizationField() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with empty quantization field",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withCustomHnswOptions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with custom hnsw options",
          VectorIndexDefinitionBuilder.builder()
              .withVectorField(
                  "my.vector.field",
                  2048,
                  VectorSimilarity.COSINE,
                  VectorQuantization.NONE,
                  new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                      new VectorFieldSpecification.HnswOptions(32, 500)))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withCustomMaxEdges() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with custom hnsw options (only maxEdges)",
          VectorIndexDefinitionBuilder.builder()
              .withVectorField(
                  "my.vector.field",
                  2048,
                  VectorSimilarity.COSINE,
                  VectorQuantization.NONE,
                  new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                      new VectorFieldSpecification.HnswOptions(
                          32,
                          VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_HNSW_OPTIONS
                              .numEdgeCandidates())))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withCustomNumEdgeCandidates() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with custom hnsw options (only numEdgeCandidates)",
          VectorIndexDefinitionBuilder.builder()
              .withVectorField(
                  "my.vector.field",
                  2048,
                  VectorSimilarity.COSINE,
                  VectorQuantization.NONE,
                  new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                      new VectorFieldSpecification.HnswOptions(
                          VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_HNSW_OPTIONS
                              .maxEdges(),
                          500)))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withStoredDocumentInclusion() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with stored fields document inclusion",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .storedSource(StoredSourceDefinition.create(INCLUSION, List.of("a", "b.c", "b.d")))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withStoredDocumentExclusion() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with stored fields document exclusion",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .storedSource(StoredSourceDefinition.create(EXCLUSION, List.of("a", "b.c", "b.d")))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition> textEmbedding() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "text embedding",
          VectorIndexDefinitionBuilder.builder().withTextField("my.vector.text.field").build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        textEmbeddingUsingDotProduct() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "text embedding using dot product",
          VectorIndexDefinitionBuilder.builder()
              .withTextField("my.vector.text.field2", "voyage-3.5", VectorSimilarity.DOT_PRODUCT)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        textEmbeddingWithInvalidModelName() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "text embedding with invalid model name",
          VectorIndexDefinitionBuilder.builder()
              .withTextField("my.vector.text.field2", "voyage-3.5-xyz")
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        textEmbeddingFullConfig() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "text embedding full config",
          VectorIndexDefinitionBuilder.builder()
              .withTextField(
                  "my.vector.text.field2",
                  "voyage-3.5",
                  VectorSimilarity.DOT_PRODUCT,
                  VectorQuantization.SCALAR)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withFlatIndexingAlgorithm() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with flat indexing algorithm",
          VectorIndexDefinitionBuilder.builder()
              .withVectorField(
                  "my.vector.field",
                  1024,
                  VectorSimilarity.COSINE,
                  VectorQuantization.NONE,
                  new VectorIndexingAlgorithm.FlatIndexingAlgorithm())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withExplicitHnswIndexingAlgorithm() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with explicit hnsw indexing algorithm",
          VectorIndexDefinitionBuilder.builder()
              .withVectorField(
                  "my.vector.field",
                  1024,
                  VectorSimilarity.COSINE,
                  VectorQuantization.NONE,
                  new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                      new VectorFieldSpecification.HnswOptions(32, 500)))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withIndexIdAtCreationTime() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with indexIdAtCreationTime",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .indexIdAtCreationTime(new ObjectId("607f191e810c19729de860eb"))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withIndexIdAtCreationTimeSameAsIndexId() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with indexIdAtCreationTime same as indexId",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .indexIdAtCreationTime(new ObjectId("507f191e810c19729de860ea"))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withAutoEmbeddingDefinitionVersion() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with autoEmbeddingDefinitionVersion",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .autoEmbeddingDefinitionVersion(5L)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withMaterializedViewNameFormatVersion() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with materializedViewNameFormatVersion",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .materializedViewNameFormatVersion(2L)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorIndexDefinition>
        withBothAutoEmbeddingAndMaterializedViewVersions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with both autoEmbeddingDefinitionVersion and materializedViewNameFormatVersion",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .autoEmbeddingDefinitionVersion(5L)
              .materializedViewNameFormatVersion(2L)
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "vector-index-serialization";
    private static final BsonSerializationTestSuite<VectorIndexDefinition> TEST_SUITE =
        fromEncodable(DefinitionTests.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<VectorIndexDefinition> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<VectorIndexDefinition> testSpec) {
      this.testSpec = testSpec;
      setupRegistry();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<VectorIndexDefinition>> data() {
      return Arrays.asList(
          minDimensions(),
          maxDimensions(),
          allSimilarities(),
          withView(),
          multipleIndexPartitions(),
          customHnswOptions(),
          withStoredDocumentInclusion(),
          withStoredDocumentExclusion(),
          textEmbedding(),
          textEmbeddingUsingDotProduct(),
          textEmbeddingFullConfig(),
          flatIndexingAlgorithm(),
          withIndexIdAtCreationTime(),
          withAutoEmbeddingDefinitionVersion(),
          withMaterializedViewNameFormatVersion(),
          withBothAutoEmbeddingAndMaterializedViewVersions());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition> maxDimensions() {
      return BsonSerializationTestSuite.TestSpec.create(
          "max dimensions",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .withFilterPath("my.filter.field")
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition> minDimensions() {
      return BsonSerializationTestSuite.TestSpec.create(
          "min dimensions",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 1)
              .withFilterPath("my.filter.field")
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition> allSimilarities() {
      return BsonSerializationTestSuite.TestSpec.create(
          "all similarities",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.cosine.vector.field", 1)
              .withDotProductVectorField("my.dotproduct.vector.field", 2)
              .withEuclideanVectorField("my.euclidean.vector.field", 3)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition> withView() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with view",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.cosine.vector.field", 1)
              .withDotProductVectorField("my.dotproduct.vector.field", 2)
              .withEuclideanVectorField("my.euclidean.vector.field", 3)
              .view(
                  ViewDefinition.existing(
                      "myView",
                      List.of(
                          new BsonDocument("$set", new BsonDocument("cats", new BsonInt32(20))),
                          new BsonDocument("$set", new BsonDocument("dogs", new BsonInt32(40))))))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition>
        multipleIndexPartitions() {
      return BsonSerializationTestSuite.TestSpec.create(
          "multiple index-partitions",
          VectorIndexDefinitionBuilder.builder()
              .numPartitions(2)
              .withCosineVectorField("my.vector.field", 2048)
              .withFilterPath("my.filter.field")
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition> customHnswOptions() {
      return BsonSerializationTestSuite.TestSpec.create(
          "custom hnsw options",
          VectorIndexDefinitionBuilder.builder()
              .withVectorField(
                  "my.vector.field",
                  2048,
                  VectorSimilarity.COSINE,
                  VectorQuantization.NONE,
                  new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                      new VectorFieldSpecification.HnswOptions(32, 500)))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition>
        withStoredDocumentInclusion() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with stored fields document inclusion",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .storedSource(StoredSourceDefinition.create(INCLUSION, List.of("a", "b.c", "b.d")))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition>
        withStoredDocumentExclusion() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with stored fields document exclusion",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .storedSource(StoredSourceDefinition.create(EXCLUSION, List.of("a", "b.c", "b.d")))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition> textEmbedding() {
      return BsonSerializationTestSuite.TestSpec.create(
          "text embedding",
          VectorIndexDefinitionBuilder.builder().withTextField("my.vector.text.field").build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition>
        textEmbeddingUsingDotProduct() {
      return BsonSerializationTestSuite.TestSpec.create(
          "text embedding using dot product",
          VectorIndexDefinitionBuilder.builder()
              .withTextField("my.vector.text.field2", "voyage-3.5", VectorSimilarity.DOT_PRODUCT)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition>
        textEmbeddingFullConfig() {
      return BsonSerializationTestSuite.TestSpec.create(
          "text embedding full config",
          VectorIndexDefinitionBuilder.builder()
              .withTextField(
                  "my.vector.text.field2",
                  "voyage-3.5",
                  VectorSimilarity.DOT_PRODUCT,
                  VectorQuantization.SCALAR)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition>
        flatIndexingAlgorithm() {
      return BsonSerializationTestSuite.TestSpec.create(
          "flat indexing algorithm",
          VectorIndexDefinitionBuilder.builder()
              .withVectorField(
                  "my.vector.field",
                  1024,
                  VectorSimilarity.COSINE,
                  VectorQuantization.NONE,
                  new VectorIndexingAlgorithm.FlatIndexingAlgorithm())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition>
        withIndexIdAtCreationTime() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with indexIdAtCreationTime",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .indexIdAtCreationTime(new ObjectId("607f191e810c19729de860eb"))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition>
        withAutoEmbeddingDefinitionVersion() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with autoEmbeddingDefinitionVersion",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .autoEmbeddingDefinitionVersion(5L)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition>
        withMaterializedViewNameFormatVersion() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with materializedViewNameFormatVersion",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .materializedViewNameFormatVersion(2L)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<VectorIndexDefinition>
        withBothAutoEmbeddingAndMaterializedViewVersions() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with both autoEmbeddingDefinitionVersion and materializedViewNameFormatVersion",
          VectorIndexDefinitionBuilder.builder()
              .withCosineVectorField("my.vector.field", 2048)
              .autoEmbeddingDefinitionVersion(5L)
              .materializedViewNameFormatVersion(2L)
              .build());
    }
  }
}
