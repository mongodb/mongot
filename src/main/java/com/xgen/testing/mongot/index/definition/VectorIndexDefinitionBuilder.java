package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexNamespaceDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSearchEngine;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.VectorTextFieldDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.util.FieldPath;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.types.ObjectId;

public class VectorIndexDefinitionBuilder {
  private ObjectId indexId = new ObjectId("507f191e810c19729de860ea");
  private String name = "myVectorIndex";
  private String database = "myDatabase";
  private String lastObservedCollectionName = "myCollection";
  private Optional<ViewDefinition> view = Optional.empty();
  private int numPartitions = IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue();

  private UUID collectionUuid = UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa");
  private Optional<Long> definitionVersion = Optional.empty();
  private Optional<Instant> definitionVersionCreatedAt = Optional.empty();
  private Optional<ObjectId> definitionVersionId = Optional.empty();
  private List<VectorIndexFieldDefinition> fields = new ArrayList<>();
  private int indexFeatureVersion =
      VectorIndexDefinition.Fields.INDEX_FEATURE_VERSION.getDefaultValue();
  private Optional<StoredSourceDefinition> storedSource = Optional.empty();
  private Optional<FieldPath> nestedRoot = Optional.empty();
  private Optional<IndexNamespaceDefinition> indexNamespace = Optional.empty();
  private Optional<ObjectId> indexIdAtCreationTime = Optional.empty();
  private Optional<Long> autoEmbeddingDefinitionVersion = Optional.empty();
  private Optional<Long> materializedViewNameFormatVersion = Optional.empty();

  public static VectorIndexDefinitionBuilder builder() {
    return new VectorIndexDefinitionBuilder();
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public static VectorIndexDefinitionBuilder from(VectorIndexDefinition definition) {
    var builder =
        new VectorIndexDefinitionBuilder()
            .indexId(definition.getIndexId())
            .name(definition.getName())
            .database(definition.getDatabase())
            .collectionUuid(definition.getCollectionUuid())
            .lastObservedCollectionName(definition.getLastObservedCollectionName())
            .numPartitions(definition.getNumPartitions())
            .withDefinitionVersion(definition.getDefinitionVersion())
            .withDefinitionVersionCreatedAt(definition.getDefinitionVersionCreatedAt())
            .withDefinitionVersionId(definition.getDefinitionVersionId());

    definition
        .getFields()
        .forEach(
            field -> {
              if (field.isVectorField()) {
                builder.withVectorFieldDefaultOptions(
                    field.getPath().toString(),
                    field.asVectorField().specification().numDimensions(),
                    field.asVectorField().specification().similarity(),
                    field.asVectorField().specification().quantization());
              } else {
                builder.withFilterPath(field.getPath().toString());
              }
            });
    definition.getNestedRoot().ifPresent(builder::nestedRoot);

    return builder;
  }

  public VectorIndexDefinitionBuilder indexId(ObjectId indexId) {
    this.indexId = indexId;
    return this;
  }

  public VectorIndexDefinitionBuilder name(String name) {
    this.name = name;
    return this;
  }

  public VectorIndexDefinitionBuilder database(String database) {
    this.database = database;
    return this;
  }

  public VectorIndexDefinitionBuilder lastObservedCollectionName(String collection) {
    this.lastObservedCollectionName = collection;
    return this;
  }

  public VectorIndexDefinitionBuilder collectionUuid(UUID collectionUuid) {
    this.collectionUuid = collectionUuid;
    return this;
  }

  public VectorIndexDefinitionBuilder view(ViewDefinition view) {
    this.view = Optional.of(view);
    return this;
  }

  public VectorIndexDefinitionBuilder numPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
    return this;
  }

  public VectorIndexDefinitionBuilder withFilterPath(String path) {
    VectorIndexFilterFieldDefinition filter =
        new VectorIndexFilterFieldDefinition(FieldPath.parse(path));
    this.fields.add(filter);
    return this;
  }

  public VectorIndexDefinitionBuilder withScalarQuantizedCosineVectorField(
      String path, int dimensions) {
    return withVectorFieldDefaultOptions(
        path, dimensions, VectorSimilarity.COSINE, VectorQuantization.SCALAR);
  }

  public VectorIndexDefinitionBuilder withBinaryQuantizedCosineVectorField(
      String path, int dimensions) {
    return withVectorFieldDefaultOptions(
        path, dimensions, VectorSimilarity.COSINE, VectorQuantization.BINARY);
  }

  public VectorIndexDefinitionBuilder withCosineVectorField(String path, int dimensions) {
    return withVectorFieldDefaultOptions(
        path, dimensions, VectorSimilarity.COSINE, VectorQuantization.NONE);
  }

  public VectorIndexDefinitionBuilder withCustomEngineVectorField(String path, int dimensions) {
    return withVectorField(
        path,
        dimensions,
        VectorSimilarity.COSINE,
        VectorQuantization.NONE,
        new VectorIndexingAlgorithm.HnswIndexingAlgorithm(),
        VectorSearchEngine.CUSTOM);
  }

  public VectorIndexDefinitionBuilder withDotProductVectorField(String path, int dimensions) {
    return withVectorFieldDefaultOptions(
        path, dimensions, VectorSimilarity.DOT_PRODUCT, VectorQuantization.NONE);
  }

  public VectorIndexDefinitionBuilder withEuclideanVectorField(String path, int dimensions) {
    return withVectorFieldDefaultOptions(
        path, dimensions, VectorSimilarity.EUCLIDEAN, VectorQuantization.NONE);
  }

  public VectorIndexDefinitionBuilder setFields(List<VectorIndexFieldDefinition> fields) {
    this.fields = fields;
    return this;
  }

  public VectorIndexDefinitionBuilder withDefinitionVersion(Optional<Long> definitionVersion) {
    this.definitionVersion = definitionVersion;
    return this;
  }

  public VectorIndexDefinitionBuilder withDefinitionVersionCreatedAt(
      Optional<Instant> definitionVersionCreatedAt) {
    this.definitionVersionCreatedAt = definitionVersionCreatedAt;
    return this;
  }

  public VectorIndexDefinitionBuilder withDefinitionVersionId(
      Optional<ObjectId> definitionVersionId) {
    this.definitionVersionId = definitionVersionId;
    return this;
  }

  public VectorIndexDefinitionBuilder withVectorFieldDefaultOptions(
      String path, int dimensions, VectorSimilarity function, VectorQuantization quantization) {
    return withVectorField(
        path,
        dimensions,
        function,
        quantization,
        new VectorIndexingAlgorithm.HnswIndexingAlgorithm());
  }

  public VectorIndexDefinitionBuilder withVectorField(
      String path,
      int dimensions,
      VectorSimilarity function,
      VectorQuantization quantization,
      VectorIndexingAlgorithm indexingAlgorithm) {
    VectorDataFieldDefinition vector =
        new VectorDataFieldDefinition(
            FieldPath.parse(path),
            new VectorFieldSpecification(dimensions, function, quantization, indexingAlgorithm));
    this.fields.add(vector);
    return this;
  }

  public VectorIndexDefinitionBuilder withVectorField(
      String path,
      int dimensions,
      VectorSimilarity function,
      VectorQuantization quantization,
      VectorIndexingAlgorithm indexingAlgorithm,
      VectorSearchEngine engine) {
    VectorDataFieldDefinition vector =
        new VectorDataFieldDefinition(
            FieldPath.parse(path),
            new VectorFieldSpecification(
                dimensions, function, quantization, indexingAlgorithm, engine));
    this.fields.add(vector);
    return this;
  }

  public VectorIndexDefinitionBuilder indexFeatureVersion(int indexFeatureVersion) {
    this.indexFeatureVersion = indexFeatureVersion;
    return this;
  }

  public VectorIndexDefinitionBuilder storedSource(StoredSourceDefinition storedSource) {
    this.storedSource = Optional.ofNullable(storedSource);
    return this;
  }

  public VectorIndexDefinitionBuilder nestedRoot(String nestedRoot) {
    this.nestedRoot = Optional.of(FieldPath.parse(nestedRoot));
    return this;
  }

  public VectorIndexDefinitionBuilder nestedRoot(FieldPath nestedRoot) {
    this.nestedRoot = Optional.of(nestedRoot);
    return this;
  }

  public VectorIndexDefinitionBuilder indexNamespace(String path) {
    this.indexNamespace = Optional.of(new IndexNamespaceDefinition(FieldPath.parse(path)));
    return this;
  }

  public VectorIndexDefinitionBuilder withTextField(String path) {
    VectorTextFieldDefinition vectorText = new VectorTextFieldDefinition(FieldPath.parse(path));
    this.fields.add(vectorText);
    return this;
  }

  public VectorIndexDefinitionBuilder withTextField(String path, String modelName) {
    VectorTextFieldDefinition vectorText =
        new VectorTextFieldDefinition(modelName, FieldPath.parse(path));
    this.fields.add(vectorText);
    return this;
  }

  public VectorIndexDefinitionBuilder withTextField(
      String path, String modelName, VectorSimilarity similarity) {
    VectorTextFieldDefinition vectorText =
        new VectorTextFieldDefinition(modelName, FieldPath.parse(path), similarity);
    this.fields.add(vectorText);
    return this;
  }

  public VectorIndexDefinitionBuilder withTextField(
      String path, String modelName, VectorSimilarity similarity, VectorQuantization quantization) {
    VectorTextFieldDefinition vectorText =
        new VectorTextFieldDefinition(modelName, FieldPath.parse(path), similarity, quantization);
    this.fields.add(vectorText);
    return this;
  }

  public VectorIndexDefinitionBuilder withAutoEmbedField(String path) {
    VectorAutoEmbedFieldDefinition vectorText =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse(path));
    this.fields.add(vectorText);
    return this;
  }

  public VectorIndexDefinitionBuilder withAutoEmbedField(String path, String modelName) {
    VectorAutoEmbedFieldDefinition vectorText =
        new VectorAutoEmbedFieldDefinition(modelName, FieldPath.parse(path));
    this.fields.add(vectorText);
    return this;
  }

  public VectorIndexDefinitionBuilder withAutoEmbedField(
      String path,
      String modelName,
      int numDimensions,
      VectorAutoEmbedQuantization autoEmbedQuantization,
      VectorSimilarity similarity) {
    VectorAutoEmbedFieldDefinition field =
        new VectorAutoEmbedFieldDefinition(
            modelName,
            VectorAutoEmbedFieldSpecification.DEFAULT_MODALITY,
            FieldPath.parse(path),
            numDimensions,
            similarity,
            autoEmbedQuantization);
    this.fields.add(field);
    return this;
  }

  public VectorIndexDefinitionBuilder indexIdAtCreationTime(ObjectId indexIdAtCreationTime) {
    this.indexIdAtCreationTime = Optional.ofNullable(indexIdAtCreationTime);
    return this;
  }

  public VectorIndexDefinitionBuilder autoEmbeddingDefinitionVersion(
      Long autoEmbeddingDefinitionVersion) {
    this.autoEmbeddingDefinitionVersion = Optional.ofNullable(autoEmbeddingDefinitionVersion);
    return this;
  }

  public VectorIndexDefinitionBuilder materializedViewNameFormatVersion(
      Long materializedViewNameFormatVersion) {
    this.materializedViewNameFormatVersion = Optional.ofNullable(materializedViewNameFormatVersion);
    return this;
  }

  public VectorIndexDefinition build() {
    return new VectorIndexDefinition(
        this.indexId,
        this.name,
        this.database,
        this.lastObservedCollectionName,
        this.collectionUuid,
        this.view,
        this.numPartitions,
        this.fields,
        this.indexFeatureVersion,
        this.definitionVersion,
        this.definitionVersionCreatedAt,
        this.definitionVersionId,
        this.storedSource,
        this.nestedRoot,
        this.indexNamespace,
        this.indexIdAtCreationTime,
        this.autoEmbeddingDefinitionVersion,
        this.materializedViewNameFormatVersion);
  }
}
