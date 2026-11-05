package com.xgen.mongot.index.definition;

import static com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification.DEFAULT_MODALITY;

import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Represents an auto-embed field definition within a search index.
 *
 * <p>This field type specifies that text at the field's mapping path should be automatically
 * embedded using the configured embedding model, then indexed for vector search.
 */
public final class SearchAutoEmbedFieldDefinition implements FieldTypeDefinition {

  private static class Fields {
    static final Field.Required<String> MODEL =
        Field.builder("model")
            .stringField()
            .validate(SearchAutoEmbedFieldDefinition::lowercaseViolation)
            .required();
    static final Field.Required<Modality> MODALITY =
        Field.builder("modality").enumField(Modality.class).asCamelCase().required();
  }

  private final VectorAutoEmbedFieldSpecification specification;
  private final Modality modality;

  public SearchAutoEmbedFieldDefinition(String modelName, String modality) {
    this(resolveSpecificationFromModelConfig(modelName, modality), Modality.fromValue(modality));
  }

  public SearchAutoEmbedFieldDefinition(String modelName) {
    this(modelName, DEFAULT_MODALITY);
  }

  /** Creates a Search auto-embed definition with an explicit, already-resolved specification. */
  public SearchAutoEmbedFieldDefinition(VectorAutoEmbedFieldSpecification specification) {
    this(
        validateResolvedSpecification(specification), Modality.fromValue(specification.modality()));
  }

  private SearchAutoEmbedFieldDefinition(
      VectorAutoEmbedFieldSpecification specification, Modality modality) {
    this.specification = specification;
    this.modality = modality;
  }

  /** Returns the parse error message when {@code modelName} is not lowercase, else empty. */
  private static Optional<String> lowercaseViolation(String modelName) {
    return modelName.chars().anyMatch(Character::isUpperCase)
        ? Optional.of("must be lowercase")
        : Optional.empty();
  }

  private static void requireLowercaseModel(String modelName) {
    if (lowercaseViolation(modelName).isPresent()) {
      throw new IllegalArgumentException(String.format("model '%s' must be lowercase", modelName));
    }
  }

  private static Optional<VectorFieldSpecification.HnswOptions> hnswOptions(
      VectorIndexingAlgorithm indexingAlgorithm) {
    return indexingAlgorithm
            instanceof
            VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                VectorFieldSpecification.HnswOptions options)
        ? Optional.of(options)
        : Optional.empty();
  }

  public String modelName() {
    return this.specification.modelName();
  }

  public VectorAutoEmbedFieldSpecification specification() {
    return this.specification;
  }

  @Override
  public Type getType() {
    return Type.AUTO_EMBED;
  }

  /**
   * Serializes the resolved tuning settings, matching {@link VectorAutoEmbedFieldSpecification}, so
   * re-parsing a persisted definition never depends on the model catalog being populated.
   */
  @Override
  public BsonDocument fieldTypeToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MODEL, this.specification.modelName())
        .field(Fields.MODALITY, this.modality)
        .field(
            VectorAutoEmbedFieldSpecification.Fields.NUM_DIMENSIONS,
            Optional.of(this.specification.numDimensions()))
        .field(
            VectorAutoEmbedFieldSpecification.Fields.SIMILARITY,
            Optional.of(this.specification.similarity()))
        .field(
            VectorAutoEmbedFieldSpecification.Fields.AUTO_EMBED_QUANTIZATION,
            Optional.of(this.specification.autoEmbedQuantization()))
        .fieldOmitDefaultValue(
            VectorFieldSpecification.Fields.INDEXING_ALGORITHM,
            this.specification.indexingAlgorithm().type())
        .fieldOmitDefaultValue(
            VectorFieldSpecification.Fields.HNSW_OPTIONS,
            hnswOptions(this.specification.indexingAlgorithm()),
            VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_HNSW_OPTIONS)
        .build();
  }

  static SearchAutoEmbedFieldDefinition fromBson(DocumentParser parser) throws BsonParseException {
    String modelName = parser.getField(Fields.MODEL).unwrap();
    String modality = parser.getField(Fields.MODALITY).unwrap().value();

    var indexingMethod = parser.getField(VectorFieldSpecification.Fields.INDEXING_ALGORITHM);
    Optional<VectorFieldSpecification.HnswOptions> hnswOptions =
        parser.getField(VectorFieldSpecification.Fields.HNSW_OPTIONS).unwrap();
    VectorIndexingAlgorithm indexingAlgorithm =
        VectorFieldSpecification.resolveIndexingAlgorithm(
            parser, indexingMethod.unwrap(), hnswOptions);

    Optional<Integer> numDimensions =
        parser.getField(VectorAutoEmbedFieldSpecification.Fields.NUM_DIMENSIONS).unwrap();
    Optional<VectorSimilarity> similarity =
        parser.getField(VectorAutoEmbedFieldSpecification.Fields.SIMILARITY).unwrap();
    Optional<VectorAutoEmbedQuantization> quantization =
        parser.getField(VectorAutoEmbedFieldSpecification.Fields.AUTO_EMBED_QUANTIZATION).unwrap();

    VectorAutoEmbedFieldSpecification.ResolvedAutoEmbedVectorParams resolved =
        VectorAutoEmbedFieldSpecification.resolveSearchAutoEmbedVectorParams(
            modelName, numDimensions, similarity, quantization);
    VectorAutoEmbedFieldSpecification specification =
        new VectorAutoEmbedFieldSpecification(
            resolved.numDimensions(),
            resolved.similarity(),
            resolved.providerQuantization(),
            indexingAlgorithm,
            modelName,
            modality);

    return new SearchAutoEmbedFieldDefinition(specification);
  }

  /** Rejects resolved settings that the vector field cannot index. */
  private static VectorAutoEmbedFieldSpecification validateResolvedSpecification(
      VectorAutoEmbedFieldSpecification specification) {
    requireLowercaseModel(specification.modelName());
    try {
      VectorAutoEmbedFieldSpecification.validateResolvedParams(
          specification.numDimensions(),
          specification.similarity(),
          specification.autoEmbedQuantization());
    } catch (BsonParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
    return specification;
  }

  /**
   * Resolves omitted tuning settings through the same model-catalog path used by BSON parsing.
   *
   * <p>The programmatic constructors use the model defaults with the default HNSW algorithm.
   */
  private static VectorAutoEmbedFieldSpecification resolveSpecificationFromModelConfig(
      String modelName, String modality) {
    requireLowercaseModel(modelName);
    try {
      VectorAutoEmbedFieldSpecification.ResolvedAutoEmbedVectorParams resolved =
          VectorAutoEmbedFieldSpecification.resolveSearchAutoEmbedVectorParams(
              modelName, Optional.empty(), Optional.empty(), Optional.empty());
      return new VectorAutoEmbedFieldSpecification(
          resolved.numDimensions(),
          resolved.similarity(),
          resolved.providerQuantization(),
          new VectorIndexingAlgorithm.HnswIndexingAlgorithm(),
          modelName,
          modality);
    } catch (BsonParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SearchAutoEmbedFieldDefinition that)) {
      return false;
    }
    return Objects.equals(this.specification, that.specification);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.specification);
  }
}
