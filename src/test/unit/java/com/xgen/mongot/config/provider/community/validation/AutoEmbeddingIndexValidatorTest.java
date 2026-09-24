package com.xgen.mongot.config.provider.community.validation;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.config.provider.community.embedding.AutoEmbeddingIndexValidator;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingProvider;
import com.xgen.mongot.index.definition.BooleanFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.InvalidIndexDefinitionException;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.VectorTextFieldDefinition;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.KnnVectorFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

public class AutoEmbeddingIndexValidatorTest {

  private static final String REGISTERED_MODEL = "test-model";
  private static final String UNREGISTERED_MODEL = "unregistered-model";

  @Before
  public void setUp() {
    EmbeddingModelConfig testConfig =
        EmbeddingModelConfig.create(
            REGISTERED_MODEL,
            EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.of("us-east-1"),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(1024),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.START),
                    Optional.of(512),
                    Optional.of(100),
                    Optional.of("text"),
                    Optional.of(VectorAutoEmbedQuantization.FLOAT),
                    Optional.empty()),
                new EmbeddingServiceConfig.ErrorHandlingConfig(50, 50L, 10L, 0.1),
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "test-key", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty()));
    EmbeddingModelCatalog.registerModelConfig(REGISTERED_MODEL, testConfig);
  }

  @Test
  public void validateIndexWithRegisteredModel_success() throws InvalidIndexDefinitionException {
    VectorIndexDefinition indexWithRegisteredModel = createVectorIndexWithModel(REGISTERED_MODEL);

    AutoEmbeddingIndexValidator.validate(indexWithRegisteredModel);
  }

  @Test
  public void validateIndexWithUnregisteredModel_throwsException() {
    VectorIndexDefinition indexWithUnregisteredModel =
        createVectorIndexWithModel(UNREGISTERED_MODEL);

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(indexWithUnregisteredModel));

    assertThat(exception.getMessage()).contains("are not supported");
    assertThat(exception.getMessage()).contains(UNREGISTERED_MODEL);
  }

  @Test
  public void validateIndexWithNoEmbeddingServiceConfigured_throwsException() {
    String neverRegisteredModel = "never-registered-model-" + System.nanoTime();

    VectorIndexDefinition indexWithModel = createVectorIndexWithModel(neverRegisteredModel);

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(indexWithModel));

    assertThat(exception.getMessage()).contains("are not supported");
    assertThat(exception.getMessage()).contains(neverRegisteredModel);
  }

  @Test
  public void validateIndexWithSingleEmbeddingModel_succeeds()
      throws InvalidIndexDefinitionException {
    // Create index with two fields using the same model
    VectorTextFieldDefinition field1 =
        new VectorTextFieldDefinition(REGISTERED_MODEL, FieldPath.parse("field1"));
    VectorAutoEmbedFieldDefinition field2 =
        new VectorAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("field2"));

    VectorIndexDefinition indexWithSameModel = createVectorIndexWithFields(List.of(field1, field2));

    AutoEmbeddingIndexValidator.validate(indexWithSameModel);
  }

  @Test
  public void validateIndexWithMultipleEmbeddingModels_succeeds()
      throws InvalidIndexDefinitionException {
    // Register a second model
    EmbeddingModelConfig secondModelConfig =
        EmbeddingModelConfig.create(
            "second-model",
            EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.of("us-east-1"),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(1024),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.START),
                    Optional.of(512),
                    Optional.of(100)),
                new EmbeddingServiceConfig.ErrorHandlingConfig(50, 50L, 10L, 0.1),
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "test-key", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty()));
    EmbeddingModelCatalog.registerModelConfig("second-model", secondModelConfig);

    // Create index with two fields using different models
    VectorTextFieldDefinition field1 =
        new VectorTextFieldDefinition(REGISTERED_MODEL, FieldPath.parse("field1"));
    VectorAutoEmbedFieldDefinition field2 =
        new VectorAutoEmbedFieldDefinition("second-model", FieldPath.parse("field2"));

    VectorIndexDefinition indexWithMultipleModels =
        createVectorIndexWithFields(List.of(field1, field2));

    // Multi-model indexes are now supported - validation should pass
    AutoEmbeddingIndexValidator.validate(indexWithMultipleModels);
  }

  @Test
  public void validateIndexWithMixedVectorTypes_throwsException() {
    // Create index with both regular VECTOR field and auto-embedding TEXT field
    VectorDataFieldDefinition vectorField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(FieldPath.parse("embedding"))
            .numDimensions(3)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    VectorTextFieldDefinition textField =
        new VectorTextFieldDefinition(REGISTERED_MODEL, FieldPath.parse("textField"));

    VectorIndexDefinition indexWithMixedTypes =
        createVectorIndexWithFields(List.of(vectorField, textField));

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(indexWithMixedTypes));

    assertTrue(exception.getMessage().contains("cannot mix regular vector fields"));
  }

  @Test
  public void validate_vectorAutoEmbeddingWithReservedFilterPath_throwsException() {
    assertVectorReservedNamespaceRejected("_autoEmbed");
  }

  @Test
  public void validate_vectorAutoEmbeddingWithDottedReservedFilterPath_throwsException() {
    assertVectorReservedNamespaceRejected("_autoEmbed.foo");
  }

  @Test
  public void validate_vectorAutoEmbeddingOutsideReservedFilterNamespace_succeeds()
      throws InvalidIndexDefinitionException {
    VectorAutoEmbedFieldDefinition autoEmbedField =
        new VectorAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("foo"));
    VectorIndexDefinition vectorIndex =
        createVectorIndexWithFields(
            List.of(
                autoEmbedField,
                new VectorIndexFilterFieldDefinition(FieldPath.parse("_autoEmbedded")),
                new VectorIndexFilterFieldDefinition(FieldPath.parse("metadata._autoEmbed"))));

    AutoEmbeddingIndexValidator.validate(vectorIndex);
  }

  @Test
  public void validateNoAutoEmbeddingTypeConversion_autoEmbeddingToVector_throwsException() {
    // Old index is auto-embedding
    VectorAutoEmbedFieldDefinition autoEmbedField =
        new VectorAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("autoEmbedField"));
    VectorIndexDefinition oldIndex = createVectorIndexWithFields(List.of(autoEmbedField));

    // New index is regular vector
    VectorDataFieldDefinition vectorField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(FieldPath.parse("embedding"))
            .numDimensions(3)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    VectorIndexDefinition newIndex = createVectorIndexWithFields(List.of(vectorField));

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () ->
                AutoEmbeddingIndexValidator.validateNoAutoEmbeddingTypeConversion(
                    oldIndex, newIndex));

    assertThat(exception.getMessage())
        .contains("you cannot convert a type:autoEmbed to a type:vector");
  }

  @Test
  public void validateNoAutoEmbeddingTypeConversion_vectorToAutoEmbedding_succeeds()
      throws InvalidIndexDefinitionException {
    // Old index is regular vector
    VectorDataFieldDefinition vectorField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(FieldPath.parse("embedding"))
            .numDimensions(3)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    VectorIndexDefinition oldIndex = createVectorIndexWithFields(List.of(vectorField));

    // New index is auto-embedding
    VectorAutoEmbedFieldDefinition autoEmbedField =
        new VectorAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("autoEmbedField"));
    VectorIndexDefinition newIndex = createVectorIndexWithFields(List.of(autoEmbedField));

    // Should not throw when converting from vector to auto-embedding
    AutoEmbeddingIndexValidator.validateNoAutoEmbeddingTypeConversion(oldIndex, newIndex);
  }

  @Test
  public void validateNoAutoEmbeddingTypeConversion_bothAutoEmbedding_succeeds()
      throws InvalidIndexDefinitionException {
    VectorAutoEmbedFieldDefinition field =
        new VectorAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("autoEmbedField"));
    VectorIndexDefinition oldIndex = createVectorIndexWithFields(List.of(field));
    VectorIndexDefinition newIndex = createVectorIndexWithFields(List.of(field));

    // Should not throw
    AutoEmbeddingIndexValidator.validateNoAutoEmbeddingTypeConversion(oldIndex, newIndex);
  }

  @Test
  public void validateNoAutoEmbeddingTypeConversion_bothRegularVector_succeeds()
      throws InvalidIndexDefinitionException {
    VectorDataFieldDefinition vectorField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(FieldPath.parse("embedding"))
            .numDimensions(3)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    VectorIndexDefinition oldIndex = createVectorIndexWithFields(List.of(vectorField));
    VectorIndexDefinition newIndex = createVectorIndexWithFields(List.of(vectorField));

    // Should not throw
    AutoEmbeddingIndexValidator.validateNoAutoEmbeddingTypeConversion(oldIndex, newIndex);
  }

  // Regression guard: community must accept the advanced auto-embed parameters that Atlas accepts
  // (quantization, numDimensions, similarity, indexingMethod, hnswOptions). If a future change
  // re-introduces a community-only gate on any of these, these tests will fail.
  //
  // Two variants cover both HNSW-with-explicit-hnswOptions (which serializes the "hnswOptions"
  // key) and FLAT indexingMethod (which serializes the "indexingMethod" key); a single field
  // can't carry both because VectorAutoEmbedFieldSpecification.toBson() uses
  // fieldOmitDefaultValue for each, and FLAT carries no HnswOptions. Both tests also
  // round-trip through fromBson() so a regression in the shared parser would also fail.

  @Test
  public void validateAutoEmbedWithExplicitHnswOptions_succeeds() throws Exception {
    VectorAutoEmbedFieldDefinition field =
        new VectorAutoEmbedFieldDefinition(
            REGISTERED_MODEL,
            VectorAutoEmbedFieldSpecification.DEFAULT_MODALITY,
            FieldPath.parse("autoEmbedField"),
            512,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.SCALAR,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                new VectorFieldSpecification.HnswOptions(32, 200)));

    assertAutoEmbedFieldAcceptedAfterBsonRoundTrip(field);
  }

  @Test
  public void validateAutoEmbedWithFlatIndexingMethod_succeeds() throws Exception {
    VectorAutoEmbedFieldDefinition field =
        new VectorAutoEmbedFieldDefinition(
            REGISTERED_MODEL,
            VectorAutoEmbedFieldSpecification.DEFAULT_MODALITY,
            FieldPath.parse("autoEmbedField"),
            512,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.SCALAR,
            new VectorIndexingAlgorithm.FlatIndexingAlgorithm());

    assertAutoEmbedFieldAcceptedAfterBsonRoundTrip(field);
  }

  /**
   * Serializes the field, re-parses via the production {@code fromBson} (going through the
   * type-dispatching parser, just like the create command does), then runs the validator on the
   * resulting index. Exercises both parser and validator against a non-default field.
   */
  private void assertAutoEmbedFieldAcceptedAfterBsonRoundTrip(VectorAutoEmbedFieldDefinition field)
      throws Exception {
    BsonDocument fieldBson = field.toBson();
    try (var parser = BsonDocumentParser.fromRoot(fieldBson).build()) {
      VectorIndexFieldDefinition reparsed = VectorIndexFieldDefinition.fromBson(parser);
      AutoEmbeddingIndexValidator.validate(createVectorIndexWithFields(List.of(reparsed)));
    }
  }

  private VectorIndexDefinition createVectorIndexWithModel(String modelName) {
    VectorAutoEmbedFieldDefinition field =
        new VectorAutoEmbedFieldDefinition(modelName, FieldPath.parse("autoEmbedField"));
    return createVectorIndexWithFields(List.of(field));
  }

  private VectorIndexDefinition createVectorIndexWithFields(
      List<VectorIndexFieldDefinition> fields) {
    return new VectorIndexDefinition(
        new ObjectId(),
        "testIndex",
        "testDb",
        "testCollection",
        UUID.randomUUID(),
        Optional.empty(),
        1,
        fields,
        3,
        Optional.empty(),
        Optional.of(Instant.now()),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  private void assertVectorReservedNamespaceRejected(String invalidPath) {
    VectorAutoEmbedFieldDefinition autoEmbedField =
        new VectorAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("foo"));
    VectorIndexDefinition vectorIndex =
        createVectorIndexWithFields(
            List.of(
                autoEmbedField,
                new VectorIndexFilterFieldDefinition(FieldPath.parse(invalidPath))));

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(vectorIndex));

    assertThat(exception.getMessage()).contains(invalidPath);
    assertThat(exception.getMessage()).contains("_autoEmbed");
    assertThat(exception.getMessage()).contains("reserved");
  }

  // ---- Search index tests ----

  @Test
  public void validateSearchIndexWithRegisteredModel_succeeds()
      throws InvalidIndexDefinitionException {
    SearchIndexDefinition searchIndex = createSearchIndexWithAutoEmbed(REGISTERED_MODEL);
    AutoEmbeddingIndexValidator.validate(searchIndex);
  }

  @Test
  public void validate_searchAutoEmbeddingWithReservedScalarMapping_throwsException() {
    SearchIndexDefinition searchIndex =
        createSearchIndexWithAutoEmbedAndField(
            "_autoEmbed",
            FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build());

    assertReservedNamespaceRejected(searchIndex, "_autoEmbed");
  }

  @Test
  public void validate_searchAutoEmbeddingWithDottedReservedMapping_throwsException() {
    SearchIndexDefinition searchIndex =
        createSearchIndexWithAutoEmbedAndField(
            "_autoEmbed.metadata",
            FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build());

    assertReservedNamespaceRejected(searchIndex, "_autoEmbed.metadata");
  }

  @Test
  public void validate_searchAutoEmbeddingWithReservedDocumentMapping_throwsException() {
    var reservedDocument =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "metadata",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    SearchIndexDefinition searchIndex =
        createSearchIndexWithAutoEmbedAndField(
            "_autoEmbed", FieldDefinitionBuilder.builder().document(reservedDocument).build());

    assertReservedNamespaceRejected(searchIndex, "_autoEmbed");
  }

  @Test
  public void validate_searchAutoEmbeddingWithReservedEmbeddedMapping_throwsException() {
    var reservedEmbeddedDocument =
        EmbeddedDocumentsFieldDefinitionBuilder.builder()
            .field(
                "metadata",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    SearchIndexDefinition searchIndex =
        createSearchIndexWithAutoEmbedAndField(
            "_autoEmbed",
            FieldDefinitionBuilder.builder().embeddedDocuments(reservedEmbeddedDocument).build());

    assertReservedNamespaceRejected(searchIndex, "_autoEmbed");
  }

  @Test
  public void validate_searchAutoEmbeddingWithReservedMultiTypeMapping_throwsException() {
    var reservedDocument =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "metadata",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    SearchIndexDefinition searchIndex =
        createSearchIndexWithAutoEmbedAndField(
            "_autoEmbed",
            FieldDefinitionBuilder.builder()
                .bool(new BooleanFieldDefinition())
                .document(reservedDocument)
                .build());

    assertReservedNamespaceRejected(searchIndex, "_autoEmbed");
  }

  @Test
  public void validate_searchAutoEmbeddingOutsideReservedNamespace_succeeds()
      throws InvalidIndexDefinitionException {
    AutoEmbeddingIndexValidator.validate(
        createSearchIndexWithAutoEmbedAndField(
            "_autoEmbedded",
            FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build()));
    AutoEmbeddingIndexValidator.validate(
        createSearchIndexWithAutoEmbedAndField(
            "metadata._autoEmbed",
            FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build()));
  }

  @Test
  public void validateSearchIndexWithUnregisteredModel_throwsException() {
    VectorAutoEmbedFieldSpecification unregisteredSpecification =
        new VectorAutoEmbedFieldSpecification(
            1024,
            VectorSimilarity.DOT_PRODUCT,
            VectorAutoEmbedQuantization.FLOAT,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm(),
            UNREGISTERED_MODEL,
            VectorAutoEmbedFieldSpecification.DEFAULT_MODALITY);
    SearchIndexDefinition searchIndex =
        createSearchIndexWithAutoEmbedFields(
            new SearchAutoEmbedFieldDefinition(
                FieldPath.parse("content"), unregisteredSpecification));

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("are not supported");
    assertThat(exception.getMessage()).contains(UNREGISTERED_MODEL);
  }

  @Test
  public void validateSearchIndexWithMixedVectorTypes_throwsException() {
    SearchAutoEmbedFieldDefinition autoEmbedDef =
        new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("content"));
    var knnVectorDef =
        KnnVectorFieldDefinitionBuilder.builder()
            .dimensions(1024)
            .similarity(VectorSimilarity.DOT_PRODUCT)
            .build();
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "autoEmbedField",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedDef).build())
            .field("vectorField", FieldDefinitionBuilder.builder().knnVector(knnVectorDef).build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("cannot mix regular vector fields");
  }

  @Test
  public void validateSearchIndexWithDocumentVectorCollision_throwsException() {
    var vectorSpec =
        new VectorFieldSpecification(
            1024,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm());
    var generatedNamespace =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "content",
                FieldDefinitionBuilder.builder()
                    .searchIndexVectorField(new SearchIndexVectorFieldDefinition(vectorSpec))
                    .build())
            .build();
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            REGISTERED_MODEL, FieldPath.parse("content")))
                    .build())
            .field(
                "generated", FieldDefinitionBuilder.builder().document(generatedNamespace).build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("cannot mix regular vector fields");
  }

  @Test
  public void validateSearchIndexWithUserVectorInReservedNamespace_throwsException() {
    var vectorSpec =
        new VectorFieldSpecification(
            1024,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm());
    var reservedNamespace =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "content",
                FieldDefinitionBuilder.builder()
                    .searchIndexVectorField(new SearchIndexVectorFieldDefinition(vectorSpec))
                    .build())
            .build();
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            REGISTERED_MODEL, FieldPath.parse("content")))
                    .build())
            .field(
                "_autoEmbed", FieldDefinitionBuilder.builder().document(reservedNamespace).build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("reserved");
    assertThat(exception.getMessage()).contains("_autoEmbed");
  }

  @Test
  public void validateSearchIndexWithNestedAutoEmbedAndTopLevelVector_throwsException() {
    // Nested auto-embed + top-level regular vector: the mixed-type check must see auto-embed
    // declarations at any nesting depth.
    var knnVectorDef =
        KnnVectorFieldDefinitionBuilder.builder()
            .dimensions(1024)
            .similarity(VectorSimilarity.DOT_PRODUCT)
            .build();
    var nested =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            REGISTERED_MODEL, FieldPath.parse("text")))
                    .build())
            .build();
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(nested).build())
            .field("rawVector", FieldDefinitionBuilder.builder().knnVector(knnVectorDef).build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("cannot mix regular vector fields");
  }

  @Test
  public void validateSearchIndexWithAutoEmbedAndVectorUnderEmbeddedDocuments_throwsException() {
    // A regular vector inside an embeddedDocuments field (the canonical nested pre-computed
    // vector shape; knnVector is illegal there but the vector type is allowed) must trip the
    // mixed-type check alongside a top-level auto-embed.
    var vectorSpec =
        new VectorFieldSpecification(
            1024,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm());
    var embeddedDocs =
        EmbeddedDocumentsFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "vector",
                FieldDefinitionBuilder.builder()
                    .searchIndexVectorField(new SearchIndexVectorFieldDefinition(vectorSpec))
                    .build())
            .build();
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "autoEmbedField",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            REGISTERED_MODEL, FieldPath.parse("content")))
                    .build())
            .field(
                "sections",
                FieldDefinitionBuilder.builder().embeddedDocuments(embeddedDocs).build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("cannot mix regular vector fields");
  }

  @Test
  public void validateSearchIndexWithDuplicatePaths_throwsException() {
    // A nested and a top-level declaration sharing a path: construction keeps the last
    // model, and this community-path validation is the clean user-facing rejection.
    var nested =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            REGISTERED_MODEL, FieldPath.parse("content")))
                    .build())
            .build();
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(nested).build())
            .field(
                "top_embed",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            REGISTERED_MODEL, FieldPath.parse("content")))
                    .build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("same path");
    assertThat(exception.getMessage()).contains("content");
  }

  @Test
  public void validateSearchIndexWithPathOverlappingDeclaredField_throwsException() {
    // An embed path coinciding with a declared non-embed path: the source is consumed by
    // the embedding pipeline, so the overlapping declaration would silently index nothing.
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "content",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .field(
                "content_embed",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            REGISTERED_MODEL, FieldPath.parse("content")))
                    .build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("consumed by the embedding pipeline");
    assertThat(exception.getMessage()).contains("content");
  }

  private SearchIndexDefinition createSearchIndexWithAutoEmbed(String modelName) {
    return createSearchIndexWithAutoEmbedFields(
        new SearchAutoEmbedFieldDefinition(modelName, FieldPath.parse("content")));
  }

  private SearchIndexDefinition createSearchIndexWithAutoEmbedFields(
      SearchAutoEmbedFieldDefinition autoEmbedDef) {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding", FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedDef).build())
            .build();
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  private SearchIndexDefinition createSearchIndexWithAutoEmbedAndField(
      String fieldName, FieldDefinition fieldDefinition) {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            REGISTERED_MODEL, FieldPath.parse("content")))
                    .build())
            .field(fieldName, fieldDefinition)
            .build();
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  private static void assertReservedNamespaceRejected(
      SearchIndexDefinition searchIndex, String invalidPath) {
    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains(invalidPath);
    assertThat(exception.getMessage()).contains("_autoEmbed");
    assertThat(exception.getMessage()).contains("reserved");
  }
}
