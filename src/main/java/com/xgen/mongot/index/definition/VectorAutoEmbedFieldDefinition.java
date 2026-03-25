package com.xgen.mongot.index.definition;

import static com.xgen.mongot.index.definition.VectorIndexFieldDefinition.Type.AUTO_EMBED;
import static com.xgen.mongot.index.definition.VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_HNSW_OPTIONS;
import static com.xgen.mongot.index.definition.VectorTextFieldSpecification.DEFAULT_MODALITY;

import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;
import org.jetbrains.annotations.TestOnly;

/**
 * Part of auto embedding Vector Index definition that represents a field with embedding model
 * options.
 */
public class VectorAutoEmbedFieldDefinition extends VectorIndexVectorFieldDefinition {
  private static final VectorSimilarity DEFAULT_VECTOR_SIMILARITY = VectorSimilarity.DOT_PRODUCT;
  private static final VectorQuantization DEFAULT_VECTOR_QUANTIZATION = VectorQuantization.NONE;

  private static class Fields {
    // no need to include hnswOptions, compression fields & indexingMethod because those fields are
    // defined in VectorFieldSpecification
    static final Field.Required<String> MODEL = Field.builder("model").stringField().required();
    static final Field.Required<String> MODALITY =
        Field.builder("modality")
            .stringField()
            .validate(
                modality ->
                    modality.equalsIgnoreCase(DEFAULT_MODALITY)
                        ? java.util.Optional.empty()
                        : java.util.Optional.of("must be '" + DEFAULT_MODALITY + "'"))
            .required();
    private static final Field.WithDefault<VectorSimilarity> AUTO_EMBEDDING_SIMILARITY =
        Field.builder("similarity")
            .enumField(VectorSimilarity.class)
            .asCamelCase()
            .optional()
            .withDefault(DEFAULT_VECTOR_SIMILARITY);
  }

  private final VectorTextFieldSpecification specification;

  public VectorAutoEmbedFieldDefinition(
      String modelName,
      String modality,
      FieldPath textPath,
      VectorSimilarity similarity,
      VectorQuantization quantization,
      Optional<VectorFieldSpecification.HnswOptions> hnswOptions) {
    super(textPath);
    // TODO(CLOUDP-388224): proper handling of flat index for indexingMethod
    VectorIndexingAlgorithm.HnswIndexingAlgorithm indexingAlgorithm =
        hnswOptions
            .map(VectorIndexingAlgorithm.HnswIndexingAlgorithm::new)
            .orElseGet(VectorIndexingAlgorithm.HnswIndexingAlgorithm::new);
    this.specification =
        new VectorTextFieldSpecification(
            validateAndGet(modelName).collectionScan().modelConfig().getOutputDimensions(),
            similarity,
            quantization,
            indexingAlgorithm,
            modelName,
            modality);
  }

  public VectorAutoEmbedFieldDefinition(String modelName, String modality, FieldPath textPath) {
    this(
        modelName,
        modality,
        textPath,
        DEFAULT_VECTOR_SIMILARITY,
        DEFAULT_VECTOR_QUANTIZATION,
        Optional.empty());
  }

  @TestOnly
  public VectorAutoEmbedFieldDefinition(String modelName, FieldPath textPath) {
    this(
        modelName,
        DEFAULT_MODALITY,
        textPath,
        DEFAULT_VECTOR_SIMILARITY,
        DEFAULT_VECTOR_QUANTIZATION,
        Optional.empty());
  }

  @TestOnly
  public VectorAutoEmbedFieldDefinition(FieldPath textPath) {
    this("voyage-3-large", DEFAULT_MODALITY, textPath);
  }

  @Override
  public VectorTextFieldSpecification specification() {
    return this.specification;
  }

  @Override
  public BsonDocument toBson() {
    Optional<VectorFieldSpecification.HnswOptions> maybeHnswOptions =
        this.specification.indexingAlgorithm()
                instanceof VectorIndexingAlgorithm.HnswIndexingAlgorithm h
            ? Optional.of(h.options())
            : Optional.empty();
    return BsonDocumentBuilder.builder()
        .field(VectorIndexFieldDefinition.Fields.TYPE, AUTO_EMBED)
        .field(VectorIndexFieldDefinition.Fields.PATH, this.path)
        .field(Fields.MODEL, this.specification.modelName().toLowerCase())
        .field(Fields.MODALITY, this.specification.modality().toLowerCase())
        .field(VectorFieldSpecification.Fields.SIMILARITY, this.specification.similarity())
        .field(VectorFieldSpecification.Fields.QUANTIZATION, this.specification.quantization())
        .fieldOmitDefaultValue(
            VectorFieldSpecification.Fields.HNSW_OPTIONS, maybeHnswOptions, DEFAULT_HNSW_OPTIONS)
        .build();
  }

  public static VectorAutoEmbedFieldDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    FieldPath textPath = parser.getField(VectorIndexFieldDefinition.Fields.PATH).unwrap();
    String modelName = parser.getField(Fields.MODEL).unwrap().toLowerCase();
    String modality = parser.getField(Fields.MODALITY).unwrap().toLowerCase();
    VectorQuantization quantization =
        parser.getField(VectorFieldSpecification.Fields.QUANTIZATION).unwrap();
    VectorSimilarity similarity = parser.getField(Fields.AUTO_EMBEDDING_SIMILARITY).unwrap();
    Optional<VectorFieldSpecification.HnswOptions> maybeHnswOptions =
        parser.getField(VectorFieldSpecification.Fields.HNSW_OPTIONS).unwrap();
    return new VectorAutoEmbedFieldDefinition(
        modelName, modality, textPath, similarity, quantization, maybeHnswOptions);
  }

  private static EmbeddingModelConfig validateAndGet(String modelName) {
    return EmbeddingModelCatalog.resolveModelConfigOrDefault(modelName);
  }

  @Override
  public Type getType() {
    return AUTO_EMBED;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VectorAutoEmbedFieldDefinition that)) {
      return false;
    }
    return Objects.equals(this.path, that.path)
        && Objects.equals(this.specification, that.specification)
        && Objects.equals(this.specification.hashCode(), that.specification.hashCode());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.path, this.specification);
  }
}
