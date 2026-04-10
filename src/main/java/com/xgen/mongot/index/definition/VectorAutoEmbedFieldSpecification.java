package com.xgen.mongot.index.definition;

import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

/**
 * A vector field specification for autoEmbed fields that require embedding model information.
 *
 * <p>Extends {@link VectorFieldSpecification} to include the name of the embedding model used to
 * generate vectors from text. This is used for auto-embedding features where text queries need to
 * be converted to vectors.
 */
public final class VectorAutoEmbedFieldSpecification extends VectorFieldSpecification
    implements VectorFieldAutoEmbeddingSpecification {

  public static final String DEFAULT_MODALITY = "text";

  private final String modelName;
  private final String modality;
  private final VectorAutoEmbedQuantization autoEmbedQuantization;

  private static class Fields {
    static final Field.Required<String> MODEL = Field.builder("model").stringField().required();
    static final Field.Required<String> MODALITY =
        Field.builder("modality")
            .stringField()
            .validate(
                modality ->
                    modality.equalsIgnoreCase(DEFAULT_MODALITY)
                        ? Optional.empty()
                        : Optional.of("must be '" + DEFAULT_MODALITY + "'"))
            .required();

    /** Omitted in BSON → model {@code outputDimensions} → default NUM_DIMENSIONS of 1024. */
    static final Field.Optional<Integer> NUM_DIMENSIONS =
        Field.builder("numDimensions")
            .intField()
            .mustBeWithinBounds(Range.of(1, VectorFieldSpecification.MAX_DIMENSIONS))
            .optional()
            .noDefault();

    /**
     * Optional explicit {@code similarity} in the index definition BSON. When omitted, defaults
     * follow quantization-based mapping: {@code float}/{@code scalar} → dot product; {@code
     * binary}/{@code binaryNoRescore} → Euclidean.
     */
    private static final Field.Optional<VectorSimilarity> SIMILARITY =
        Field.builder("similarity")
            .enumField(VectorSimilarity.class)
            .asCamelCase()
            .optional()
            .noDefault();

    /** Omitted in BSON → model {@code quantization} → {@link VectorAutoEmbedQuantization#FLOAT}. */
    private static final Field.Optional<VectorAutoEmbedQuantization> AUTO_EMBED_QUANTIZATION =
        Field.builder("quantization")
            .enumField(VectorAutoEmbedQuantization.class)
            .asCamelCase()
            .optional()
            .noDefault();
  }

  public VectorAutoEmbedFieldSpecification(
      int numDimensions,
      VectorSimilarity similarity,
      VectorAutoEmbedQuantization autoEmbedQuantization,
      VectorIndexingAlgorithm indexingAlgorithm,
      String modelName,
      String modality) {
    super(
        numDimensions, similarity, autoEmbedQuantization.toLuceneQuantization(), indexingAlgorithm);
    this.modelName = modelName;
    this.modality = modality;
    this.autoEmbedQuantization = autoEmbedQuantization;
  }

  /**
   * Returns the name of the embedding model used to generate vectors from text.
   *
   * @return the embedding model name
   */
  public String modelName() {
    return this.modelName;
  }

  /**
   * Returns the modality ingested into the vector field for auto-embedding index.
   *
   * @return the modality
   */
  public String modality() {
    return this.modality;
  }

  /**
   * Returns the AutoEmbed quantization ingested into the vector field for auto-embedding index.
   *
   * @return the VectorAutoEmbedQuantization
   */
  public VectorAutoEmbedQuantization autoEmbedQuantization() {
    return this.autoEmbedQuantization;
  }

  @Override
  public BsonDocument toBson() {
    Optional<HnswOptions> maybeHnswOptions =
        this.indexingAlgorithm()
                instanceof VectorIndexingAlgorithm.HnswIndexingAlgorithm(HnswOptions options)
            ? Optional.of(options)
            : Optional.empty();

    return BsonDocumentBuilder.builder()
        .field(VectorFieldSpecification.Fields.NUM_DIMENSIONS, this.numDimensions())
        .field(VectorFieldSpecification.Fields.SIMILARITY, this.similarity())
        // Provider-side quantization for auto-embed index definition BSON.
        .field(Fields.AUTO_EMBED_QUANTIZATION, Optional.of(this.autoEmbedQuantization))
        .fieldOmitDefaultValue(
            VectorFieldSpecification.Fields.INDEXING_ALGORITHM, this.indexingAlgorithm().type())
        .fieldOmitDefaultValue(
            VectorFieldSpecification.Fields.HNSW_OPTIONS,
            maybeHnswOptions,
            VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_HNSW_OPTIONS)
        .fieldOmitDefaultValue(VectorFieldSpecification.Fields.ENGINE, this.engine())
        .field(Fields.MODEL, this.modelName)
        .field(Fields.MODALITY, this.modality)
        .build();
  }

  public static VectorAutoEmbedFieldSpecification fromBson(DocumentParser parser)
      throws BsonParseException {
    String modelName = parser.getField(Fields.MODEL).unwrap().toLowerCase();
    String modality = parser.getField(Fields.MODALITY).unwrap().toLowerCase();

    // resolve VectorIndexingAlgorithm
    VectorIndexingAlgorithm.AlgorithmType userAlgorithmType =
        parser.getField(VectorFieldSpecification.Fields.INDEXING_ALGORITHM).unwrap();
    Optional<HnswOptions> userHnswOptions =
        parser.getField(VectorFieldSpecification.Fields.HNSW_OPTIONS).unwrap();
    VectorIndexingAlgorithm indexingAlgorithm =
        resolveIndexingAlgorithm(parser, userAlgorithmType, userHnswOptions);

    // resolve numDimensions, VectorAutoEmbedQuantization, and VectorSimilarity
    Optional<Integer> userNumDimensions = parser.getField(Fields.NUM_DIMENSIONS).unwrap();
    Optional<VectorAutoEmbedQuantization> userProviderQuantization =
        parser.getField(Fields.AUTO_EMBED_QUANTIZATION).unwrap();
    Optional<VectorSimilarity> userSimilarity = parser.getField(Fields.SIMILARITY).unwrap();
    ResolvedAutoEmbedVectorParams resolved =
        resolveAutoEmbedVectorParams(
            modelName, userNumDimensions, userSimilarity, userProviderQuantization);

    if ((resolved.providerQuantization() == VectorAutoEmbedQuantization.BINARY_NO_RESCORE
            || resolved.providerQuantization() == VectorAutoEmbedQuantization.BINARY)
        && resolved.numDimensions() % 8 != 0) {
      throw new BsonParseException(
          "numDimensions must be a multiple of 8 for quantization type "
              + resolved.providerQuantization()
              + "; but got "
              + resolved.numDimensions(),
          Optional.empty());
    }

    return new VectorAutoEmbedFieldSpecification(
        resolved.numDimensions(),
        resolved.similarity(),
        resolved.providerQuantization(),
        indexingAlgorithm,
        modelName,
        modality);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    VectorAutoEmbedFieldSpecification that = (VectorAutoEmbedFieldSpecification) o;
    return Objects.equals(this.modelName, that.modelName)
        && Objects.equals(this.modality, that.modality)
        && this.autoEmbedQuantization == that.autoEmbedQuantization;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), this.modelName, this.modality, this.autoEmbedQuantization);
  }

  record ResolvedAutoEmbedVectorParams(
      int numDimensions,
      VectorSimilarity similarity,
      VectorAutoEmbedQuantization providerQuantization) {}

  /**
   * Resolution order: BSON (if present) → Voyage {@code modelConfig} (dimensions, quantization) →
   * static defaults. When BSON omits {@code similarity}, defaults match MMS bucket labels: {@code
   * float}/{@code scalar} → dot product; {@code binary}/{@code binaryNoRescore} → euclidean.
   *
   * <p>Package-private for unit tests in this package; not part of the public API.
   */
  static ResolvedAutoEmbedVectorParams resolveAutoEmbedVectorParams(
      String modelName,
      Optional<Integer> userNumDimensions,
      Optional<VectorSimilarity> userSimilarity,
      Optional<VectorAutoEmbedQuantization> userQuantization)
      throws BsonParseException {
    // Fast‑path: if the user provided all optional values, avoid touching the model catalog.
    if (userNumDimensions.isPresent() && userQuantization.isPresent()) {
      return new ResolvedAutoEmbedVectorParams(
          userNumDimensions.get(),
          userSimilarity.orElse(getDefaultSimilarity(userQuantization.get())),
          userQuantization.get());
    }

    // Resolve model config from catalog. When confcall response doesn't contain embedding model
    // configs, the model may not be registered yet. Wrap as BsonParseException so the confcall
    // handler marks the index as invalid instead of crashing confcall processing.
    EmbeddingModelConfig cfg;
    try {
      cfg = EmbeddingModelCatalog.getModelConfig(modelName);
    } catch (EmbeddingProviderNonTransientException e) {
      throw new BsonParseException(e.getMessage(), Optional.empty(), e);
    }
    EmbeddingServiceConfig.VoyageModelConfig modelConfig =
        (EmbeddingServiceConfig.VoyageModelConfig) cfg.collectionScan().modelConfig();

    // resolve numDimensions
    if (modelConfig.outputDimensions.isEmpty()) {
      throw new BsonParseException(
          "numDimensions cannot be resolved from model config", Optional.empty());
    }
    Integer resolvedNumDimensions = userNumDimensions.orElse(modelConfig.outputDimensions.get());

    // resolve VectorProviderQuantization
    if (modelConfig.quantization.isEmpty()) {
      throw new BsonParseException(
          "quantization cannot be resolved from model config", Optional.empty());
    }
    VectorAutoEmbedQuantization resolvedQuantization =
        userQuantization.orElse(modelConfig.quantization.get());

    // resolve VectorSimilarity: prefer explicit BSON value, otherwise map from quantization.
    VectorSimilarity similarity = userSimilarity.orElse(getDefaultSimilarity(resolvedQuantization));

    return new ResolvedAutoEmbedVectorParams(
        resolvedNumDimensions, similarity, resolvedQuantization);
  }

  private static VectorSimilarity getDefaultSimilarity(VectorAutoEmbedQuantization quantization) {
    return switch (quantization) {
      case BINARY, BINARY_NO_RESCORE -> VectorSimilarity.EUCLIDEAN;
      default -> VectorSimilarity.DOT_PRODUCT;
    };
  }
}
