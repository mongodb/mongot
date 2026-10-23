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
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.InvalidIndexDefinitionException;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
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
import com.xgen.testing.mongot.index.definition.GeoFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.KnnVectorFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TypeSetDefinitionBuilder;
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
  public void validateIndexWithNoModelsRegistered_namesEmptyCatalog() {
    // The rejection lists the supported models; with nothing configured it has to say so instead
    // of printing an empty list.
    EmbeddingModelCatalog.clear();
    VectorIndexDefinition indexWithModel = createVectorIndexWithModel(UNREGISTERED_MODEL);

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(indexWithModel));

    assertThat(exception.getMessage()).contains("none (embedding service not configured)");
  }

  @Test
  public void validateIndexWithoutAutoEmbedFields_throwsIllegalArgument() {
    // Callers gate on isAutoEmbeddingIndex(); the guard keeps a future caller honest.
    SearchIndexDefinition plainIndex =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(false).build())
            .build();

    assertThrows(
        IllegalArgumentException.class, () -> AutoEmbeddingIndexValidator.validate(plainIndex));
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
            new SearchAutoEmbedFieldDefinition(unregisteredSpecification));

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("are not supported");
    assertThat(exception.getMessage()).contains(UNREGISTERED_MODEL);
  }

  @Test
  public void validateDefinitionShape_skipsModelRegistrationCheck()
      throws InvalidIndexDefinitionException {
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
            new SearchAutoEmbedFieldDefinition(unregisteredSpecification));

    // The Atlas conf-call path validates shape only; an unregistered model surfaces during
    // replication as MODEL_NOT_REGISTERED instead of invalidating the definition.
    AutoEmbeddingIndexValidator.validateDefinitionShape(searchIndex);
  }

  @Test
  public void validateSearchIndexWithMixedVectorTypes_succeeds()
      throws InvalidIndexDefinitionException {
    SearchAutoEmbedFieldDefinition autoEmbedDef =
        new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL);
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

    AutoEmbeddingIndexValidator.validate(searchIndex);
    AutoEmbeddingIndexValidator.validateDefinitionShape(searchIndex);
  }

  @Test
  public void validateSearchIndexWithNestedRegularVector_succeeds()
      throws InvalidIndexDefinitionException {
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
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL))
                    .build())
            .field(
                "generated", FieldDefinitionBuilder.builder().document(generatedNamespace).build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbeddingIndexValidator.validate(searchIndex);
    AutoEmbeddingIndexValidator.validateDefinitionShape(searchIndex);
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
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL))
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
  public void validateSearchIndexWithNestedAutoEmbedAndTopLevelVector_succeeds()
      throws InvalidIndexDefinitionException {
    // Search indexes allow pre-computed vectors alongside nested auto-embedding fields.
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
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL))
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

    AutoEmbeddingIndexValidator.validate(searchIndex);
    AutoEmbeddingIndexValidator.validateDefinitionShape(searchIndex);
  }

  @Test
  public void validateSearchIndexWithAutoEmbedAndVectorUnderEmbeddedDocuments_succeeds()
      throws InvalidIndexDefinitionException {
    // Regular vectors under embeddedDocuments may coexist with auto-embedding outside that scope.
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
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL))
                    .build())
            .field(
                "sections",
                FieldDefinitionBuilder.builder().embeddedDocuments(embeddedDocs).build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbeddingIndexValidator.validate(searchIndex);
    AutoEmbeddingIndexValidator.validateDefinitionShape(searchIndex);
  }

  @Test
  public void validateSearchIndexWithDocumentAlternativeOnAutoEmbedField_throwsException() {
    // A document alternative cannot share its field with autoEmbed: the raw document value and
    // the generated vector cannot both live at the source path.
    var docField =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "text",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "description",
                FieldDefinitionBuilder.builder()
                    .document(docField)
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL))
                    .build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("document-shaped mapping");
    assertThat(exception.getMessage()).contains("description");
  }

  @Test
  public void validateSearchIndexWithGeoAutoEmbedSource_throwsException() {
    // Geo values are GeoJSON documents with no declared children to enumerate, so they cannot be
    // copied into the materialized view alongside a generated vector.
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "location",
                FieldDefinitionBuilder.builder()
                    .geo(GeoFieldDefinitionBuilder.builder().build())
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL))
                    .build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("document-shaped mapping");
    assertThat(exception.getMessage()).contains("location");
  }

  @Test
  public void validateSearchIndexWithPathOverlappingOwnScalarAlternative_succeeds()
      throws InvalidIndexDefinitionException {
    // Dual role: a text alternative on the auto-embed source key is allowed; the scalar and the
    // generated vector share the field key as their query path.
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "title",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL))
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbeddingIndexValidator.validate(searchIndex);
  }

  @Test
  public void validate_searchAutoEmbeddingWithNumberAndAutoEmbedOnSameField_isAllowed()
      throws InvalidIndexDefinitionException {
    // Scalar alternatives may share the path: numeric values are not embedded, string values are.
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "description",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL))
                    .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
                    .build())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbeddingIndexValidator.validate(searchIndex);
  }

  @Test
  public void validateSearchIndexWithRootDynamicMappings_throwsException() {
    // Dynamic mappings are supported for ordinary Search indexes, but an auto-embedding index is
    // served from a generated materialized view; MMS rejects dynamic at any level and the community
    // path must agree.
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field("embedding", autoEmbedField())
            .build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () ->
                AutoEmbeddingIndexValidator.validate(
                    searchIndexWithMappings("dynamic-root", mappings)));

    assertThat(exception.getMessage()).contains("cannot use dynamic mappings at any level");
    assertThat(exception.getMessage()).contains("dynamic-root");
  }

  @Test
  public void validateSearchIndexWithNestedDynamicMappings_throwsException() {
    var nested = DocumentFieldDefinitionBuilder.builder().dynamic(true).build();
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("embedding", autoEmbedField())
            .field("parent", FieldDefinitionBuilder.builder().document(nested).build())
            .build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () ->
                AutoEmbeddingIndexValidator.validate(
                    searchIndexWithMappings("dynamic-nested", mappings)));

    assertThat(exception.getMessage()).contains("cannot use dynamic mappings at any level");
  }

  @Test
  public void validateSearchIndexWithTypeSetDynamicMappings_throwsException() {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic("dynamicTypeSet")
            .field("embedding", autoEmbedField())
            .build();
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .name("dynamic-type-set")
            .mappings(mappings)
            .typeSets(
                TypeSetDefinitionBuilder.builder()
                    .name("dynamicTypeSet")
                    .addType(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("cannot use dynamic mappings at any level");
  }

  @Test
  public void validateSearchIndexWithDynamicMappingsUnderEmbeddedDocuments_throwsException() {
    var embedded = EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build();
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("embedding", autoEmbedField())
            .field("sections", FieldDefinitionBuilder.builder().embeddedDocuments(embedded).build())
            .build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () ->
                AutoEmbeddingIndexValidator.validate(
                    searchIndexWithMappings("dynamic-embedded", mappings)));

    assertThat(exception.getMessage()).contains("cannot use dynamic mappings at any level");
  }

  @Test
  public void validateSearchIndexWithStoredSourceTrue_throwsException() {
    assertStoredSourceRejected(StoredSourceDefinition.createIncludeAll(), "stored-source-true");
  }

  @Test
  public void validateSearchIndexWithStoredSourceIncludePaths_throwsException() {
    assertStoredSourceRejected(
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("title")),
        "stored-source-include");
  }

  @Test
  public void validateSearchIndexWithStoredSourceExcludePaths_throwsException() {
    assertStoredSourceRejected(
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("title")),
        "stored-source-exclude");
  }

  @Test
  public void validateSearchIndexWithStoredSourceFalse_succeeds()
      throws InvalidIndexDefinitionException {
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(searchAutoEmbedMappings())
            .storedSource(StoredSourceDefinition.createExcludeAll())
            .build();

    AutoEmbeddingIndexValidator.validate(searchIndex);
  }

  /**
   * Every rule rejects through {@link InvalidIndexDefinitionException}. The Atlas conf-call intake
   * catches that (and parse errors) per definition, so an unchecked exception here would take the
   * host down instead of quarantining the one index.
   */
  @Test
  public void validate_everyViolationThrowsCheckedException() {
    List<SearchIndexDefinition> violatingIndexes =
        List.of(
            searchIndexWithMappings(
                "dynamic",
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(true)
                    .field("embedding", autoEmbedField())
                    .build()),
            SearchIndexDefinitionBuilder.builder()
                .defaultMetadata()
                .name("stored-source")
                .mappings(searchAutoEmbedMappings())
                .storedSource(StoredSourceDefinition.createIncludeAll())
                .build(),
            createSearchIndexWithAutoEmbedAndField(
                "_autoEmbed",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build()));

    for (SearchIndexDefinition index : violatingIndexes) {
      assertThrows(
          InvalidIndexDefinitionException.class, () -> AutoEmbeddingIndexValidator.validate(index));
    }
  }

  private SearchIndexDefinition createSearchIndexWithAutoEmbed(String modelName) {
    return createSearchIndexWithAutoEmbedFields(new SearchAutoEmbedFieldDefinition(modelName));
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
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL))
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

  private static FieldDefinition autoEmbedField() {
    return FieldDefinitionBuilder.builder()
        .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL))
        .build();
  }

  private static DocumentFieldDefinition searchAutoEmbedMappings() {
    return DocumentFieldDefinitionBuilder.builder()
        .dynamic(false)
        .field("embedding", autoEmbedField())
        .build();
  }

  private static SearchIndexDefinition searchIndexWithMappings(
      String name, DocumentFieldDefinition mappings) {
    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .name(name)
        .mappings(mappings)
        .build();
  }

  private static void assertStoredSourceRejected(
      StoredSourceDefinition storedSource, String indexName) {
    SearchIndexDefinition searchIndex =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .name(indexName)
            .mappings(searchAutoEmbedMappings())
            .storedSource(storedSource)
            .build();

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex));

    assertThat(exception.getMessage()).contains("cannot use storedSource");
    assertThat(exception.getMessage()).contains(indexName);
    assertThat(exception.getMessage()).contains("remove storedSource or the autoEmbed field");
  }
}
