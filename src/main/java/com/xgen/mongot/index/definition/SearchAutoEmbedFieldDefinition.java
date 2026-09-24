package com.xgen.mongot.index.definition;

import static com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification.DEFAULT_MODALITY;

import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Represents an auto-embed field definition within a search index.
 *
 * <p>This field type specifies that text from a field path should be automatically embedded using
 * the configured embedding model, then indexed for vector search.
 */
public final class SearchAutoEmbedFieldDefinition implements FieldTypeDefinition {

  private static class Fields {
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
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();
  }

  private final FieldPath path;
  private final VectorAutoEmbedFieldSpecification specification;

  public SearchAutoEmbedFieldDefinition(String modelName, String modality, FieldPath path) {
    this(path, resolveSpecificationFromModelConfig(modelName, modality));
  }

  public SearchAutoEmbedFieldDefinition(String modelName, FieldPath path) {
    this(modelName, DEFAULT_MODALITY, path);
  }

  /** Creates a Search auto-embed definition with an explicit, already-resolved specification. */
  public SearchAutoEmbedFieldDefinition(
      FieldPath path, VectorAutoEmbedFieldSpecification specification) {
    this.path = path;
    this.specification = validateResolvedSpecification(specification);
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

  public String modality() {
    return this.specification.modality();
  }

  public FieldPath path() {
    return this.path;
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
        .field(Fields.MODALITY, this.specification.modality())
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
        .field(Fields.PATH, this.path)
        .build();
  }

  static SearchAutoEmbedFieldDefinition fromBson(DocumentParser parser) throws BsonParseException {
    String modelName = parser.getField(Fields.MODEL).unwrap().toLowerCase(Locale.ROOT);
    String modality = parser.getField(Fields.MODALITY).unwrap().toLowerCase(Locale.ROOT);
    FieldPath path = parser.getField(Fields.PATH).unwrap();

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
        VectorAutoEmbedFieldSpecification.resolveAutoEmbedVectorParams(
            modelName, numDimensions, similarity, quantization);
    VectorAutoEmbedFieldSpecification specification =
        new VectorAutoEmbedFieldSpecification(
            resolved.numDimensions(),
            resolved.similarity(),
            resolved.providerQuantization(),
            indexingAlgorithm,
            modelName,
            modality);

    return new SearchAutoEmbedFieldDefinition(path, specification);
  }

  /** Rejects resolved settings that the vector field cannot index. */
  private static VectorAutoEmbedFieldSpecification validateResolvedSpecification(
      VectorAutoEmbedFieldSpecification specification) {
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
    String canonicalModelName = modelName.toLowerCase(Locale.ROOT);
    String canonicalModality = modality.toLowerCase(Locale.ROOT);
    try {
      VectorAutoEmbedFieldSpecification.ResolvedAutoEmbedVectorParams resolved =
          VectorAutoEmbedFieldSpecification.resolveAutoEmbedVectorParams(
              canonicalModelName, Optional.empty(), Optional.empty(), Optional.empty());
      return new VectorAutoEmbedFieldSpecification(
          resolved.numDimensions(),
          resolved.similarity(),
          resolved.providerQuantization(),
          new VectorIndexingAlgorithm.HnswIndexingAlgorithm(),
          canonicalModelName,
          canonicalModality);
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
    return Objects.equals(this.path, that.path)
        && Objects.equals(this.specification, that.specification);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.path, this.specification);
  }
}
