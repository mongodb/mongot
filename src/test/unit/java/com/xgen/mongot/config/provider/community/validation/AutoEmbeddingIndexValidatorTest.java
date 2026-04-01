package com.xgen.mongot.config.provider.community.validation;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.config.provider.community.embedding.AutoEmbeddingIndexValidator;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingProvider;
import com.xgen.mongot.index.definition.InvalidIndexDefinitionException;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.VectorTextFieldDefinition;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.KnnVectorFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
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
    EmbeddingModelCatalog.registerModelConfig(REGISTERED_MODEL, testConfig);
  }

  @Test
  public void validateIndexWithRegisteredModel_success() throws InvalidIndexDefinitionException {
    VectorIndexDefinition indexWithRegisteredModel = createVectorIndexWithModel(REGISTERED_MODEL);

    AutoEmbeddingIndexValidator.validate(indexWithRegisteredModel, Optional.empty());
  }

  @Test
  public void validateIndexWithUnregisteredModel_throwsException() {
    VectorIndexDefinition indexWithUnregisteredModel =
        createVectorIndexWithModel(UNREGISTERED_MODEL);

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () ->
                AutoEmbeddingIndexValidator.validate(indexWithUnregisteredModel, Optional.empty()));

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
            () -> AutoEmbeddingIndexValidator.validate(indexWithModel, Optional.empty()));

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

    AutoEmbeddingIndexValidator.validate(indexWithSameModel, Optional.empty());
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
    AutoEmbeddingIndexValidator.validate(indexWithMultipleModels, Optional.empty());
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
            () -> AutoEmbeddingIndexValidator.validate(indexWithMixedTypes, Optional.empty()));

    assertTrue(exception.getMessage().contains("cannot mix regular vector fields"));
  }

  @Test
  public void validateNoAutoEmbeddingFieldChanges_noChanges_succeeds()
      throws InvalidIndexDefinitionException {
    VectorAutoEmbedFieldDefinition field =
        new VectorAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("autoEmbedField"));
    VectorIndexDefinition oldIndex = createVectorIndexWithFields(List.of(field));
    VectorIndexDefinition newIndex = createVectorIndexWithFields(List.of(field));

    // Should not throw
    AutoEmbeddingIndexValidator.validateNoAutoEmbeddingFieldChanges(oldIndex, newIndex);
  }

  @Test
  public void validateNoAutoEmbeddingFieldChanges_modelChanged_throwsException() {
    // Register a second model for this test
    EmbeddingModelConfig secondModelConfig =
        EmbeddingModelConfig.create(
            "another-model",
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
    EmbeddingModelCatalog.registerModelConfig("another-model", secondModelConfig);

    VectorIndexDefinition oldIndex = createVectorIndexWithModel(REGISTERED_MODEL);
    VectorIndexDefinition newIndex = createVectorIndexWithModel("another-model");

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () ->
                AutoEmbeddingIndexValidator.validateNoAutoEmbeddingFieldChanges(
                    oldIndex, newIndex));

    assertThat(exception.getMessage()).contains("Updates to auto-embedding fields are not allowed");
  }

  @Test
  public void validateNoAutoEmbeddingFieldChanges_pathChanged_throwsException() {
    VectorAutoEmbedFieldDefinition field1 =
        new VectorAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("field1"));
    VectorAutoEmbedFieldDefinition field2 =
        new VectorAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("field2"));

    VectorIndexDefinition oldIndex = createVectorIndexWithFields(List.of(field1));
    VectorIndexDefinition newIndex = createVectorIndexWithFields(List.of(field2));

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () ->
                AutoEmbeddingIndexValidator.validateNoAutoEmbeddingFieldChanges(
                    oldIndex, newIndex));

    assertThat(exception.getMessage()).contains("Updates to auto-embedding fields are not allowed");
  }

  @Test
  public void validateNoAutoEmbeddingFieldChanges_oldNotAutoEmbedding_throwsException() {
    // Old index is not auto-embedding (only has vector field)
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

    // Should throw - method requires both indexes to be auto-embedding
    assertThrows(
        IllegalArgumentException.class,
        () -> AutoEmbeddingIndexValidator.validateNoAutoEmbeddingFieldChanges(oldIndex, newIndex));
  }

  @Test
  public void validateNoAutoEmbeddingFieldChanges_newNotAutoEmbedding_throwsException() {
    // Old index is auto-embedding
    VectorAutoEmbedFieldDefinition autoEmbedField =
        new VectorAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("autoEmbedField"));
    VectorIndexDefinition oldIndex = createVectorIndexWithFields(List.of(autoEmbedField));

    // New index is not auto-embedding (only has vector field)
    VectorDataFieldDefinition vectorField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(FieldPath.parse("embedding"))
            .numDimensions(3)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    VectorIndexDefinition newIndex = createVectorIndexWithFields(List.of(vectorField));

    // Should throw - method requires both indexes to be auto-embedding
    assertThrows(
        IllegalArgumentException.class,
        () -> AutoEmbeddingIndexValidator.validateNoAutoEmbeddingFieldChanges(oldIndex, newIndex));
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

  @Test
  public void validateNoHnswOptionsInAutoEmbedFields_definitionWithoutFields_doesNotThrow()
      throws InvalidIndexDefinitionException {
    BsonDocument definition = new BsonDocument();
    AutoEmbeddingIndexValidator.validateNoHnswOptionsInAutoEmbedFields(definition);
  }

  @Test
  public void validateNoHnswOptionsInAutoEmbedFields_autoEmbedFieldWithoutHnswOptions_doesNotThrow()
      throws InvalidIndexDefinitionException {
    BsonDocument autoEmbedField =
        new BsonDocument()
            .append("type", new BsonString("autoEmbed"))
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-4-lite"))
            .append("modality", new BsonString("text"));
    BsonArray fields = new BsonArray();
    fields.add(autoEmbedField);
    BsonDocument definition = new BsonDocument("fields", fields);
    AutoEmbeddingIndexValidator.validateNoHnswOptionsInAutoEmbedFields(definition);
  }

  @Test
  public void
      validateNoHnswOptionsInAutoEmbedFields_autoEmbedFieldWithHnswOptions_throwsException() {
    BsonDocument hnswOptions =
        new BsonDocument()
            .append("maxEdges", new BsonInt32(32))
            .append("numEdgeCandidates", new BsonInt32(200));
    BsonDocument autoEmbedField =
        new BsonDocument()
            .append("type", new BsonString("autoEmbed"))
            .append("path", new BsonString("desc"))
            .append("model", new BsonString("voyage-4-lite"))
            .append("modality", new BsonString("text"))
            .append("hnswOptions", hnswOptions);
    BsonArray fields = new BsonArray();
    fields.add(autoEmbedField);
    BsonDocument definition = new BsonDocument("fields", fields);

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validateNoHnswOptionsInAutoEmbedFields(definition));

    assertThat(exception.getMessage()).contains("hnswOptions is not supported");
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
        Optional.empty());
  }

  // ---- Search index tests ----

  @Test
  public void validateSearchIndexWithRegisteredModel_succeeds()
      throws InvalidIndexDefinitionException {
    SearchIndexDefinition searchIndex = createSearchIndexWithAutoEmbed(REGISTERED_MODEL);
    AutoEmbeddingIndexValidator.validate(searchIndex, Optional.empty());
  }

  @Test
  public void validateSearchIndexWithUnregisteredModel_throwsException() {
    SearchIndexDefinition searchIndex = createSearchIndexWithAutoEmbed(UNREGISTERED_MODEL);

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () -> AutoEmbeddingIndexValidator.validate(searchIndex, Optional.empty()));

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
            () -> AutoEmbeddingIndexValidator.validate(searchIndex, Optional.empty()));

    assertThat(exception.getMessage()).contains("cannot mix regular vector fields");
  }

  @Test
  public void validateNoSearchAutoEmbeddingFieldChanges_changedFields_throwsException() {
    SearchIndexDefinition oldIndex =
        createSearchIndexWithAutoEmbedFields(
            new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("content")));
    SearchIndexDefinition newIndex =
        createSearchIndexWithAutoEmbedFields(
            new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("description")));

    InvalidIndexDefinitionException exception =
        assertThrows(
            InvalidIndexDefinitionException.class,
            () ->
                AutoEmbeddingIndexValidator.validateNoAutoEmbeddingFieldChanges(
                    oldIndex, newIndex));

    assertThat(exception.getMessage()).contains("Updates to auto-embedding fields are not allowed");
  }

  @Test
  public void validateNoSearchAutoEmbeddingFieldChanges_identicalFields_succeeds()
      throws InvalidIndexDefinitionException {
    SearchAutoEmbedFieldDefinition field =
        new SearchAutoEmbedFieldDefinition(REGISTERED_MODEL, FieldPath.parse("content"));
    SearchIndexDefinition oldIndex = createSearchIndexWithAutoEmbedFields(field);
    SearchIndexDefinition newIndex = createSearchIndexWithAutoEmbedFields(field);

    AutoEmbeddingIndexValidator.validateNoAutoEmbeddingFieldChanges(oldIndex, newIndex);
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
}
