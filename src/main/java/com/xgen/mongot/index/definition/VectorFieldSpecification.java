package com.xgen.mongot.index.definition;

import static com.xgen.mongot.index.definition.VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_HNSW_OPTIONS;
import static com.xgen.mongot.index.definition.VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_MAX_EDGES;
import static com.xgen.mongot.index.definition.VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_NUM_EDGE_CANDIDATES;
import static com.xgen.mongot.index.definition.VectorIndexingAlgorithm.HnswIndexingAlgorithm.MAXIMUM_MAX_EDGES;
import static com.xgen.mongot.index.definition.VectorIndexingAlgorithm.HnswIndexingAlgorithm.MAXIMUM_NUM_EDGE_CANDIDATES;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.ParsedField;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

/**
 * Encapsulates the parameters that define a vector field.
 *
 * <p>This specification is intended to be reusable across both vector field definitions and search
 * index definitions.
 */
public class VectorFieldSpecification implements DocumentEncodable {

  public static final int MAX_DIMENSIONS = 8192;

  public static class Fields {
    public static final Field.Required<Integer> NUM_DIMENSIONS =
        Field.builder("numDimensions")
            .intField()
            .mustBeWithinBounds(Range.of(1, MAX_DIMENSIONS))
            .required();
    public static final Field.Required<VectorSimilarity> SIMILARITY =
        Field.builder("similarity").enumField(VectorSimilarity.class).asCamelCase().required();

    public static final Field.WithDefault<VectorQuantization> QUANTIZATION =
        Field.builder("quantization")
            .enumField(VectorQuantization.class)
            .asCamelCase()
            .optional()
            .withDefault(VectorQuantization.NONE);

    public static final Field.WithDefault<VectorIndexingAlgorithm.AlgorithmType>
        INDEXING_ALGORITHM =
            Field.builder("indexingMethod")
                .enumField(VectorIndexingAlgorithm.AlgorithmType.class)
                .asCamelCase()
                .optional()
                .withDefault(VectorIndexingAlgorithm.AlgorithmType.HNSW);

    public static final Field.Optional<VectorFieldSpecification.HnswOptions> HNSW_OPTIONS =
        Field.builder("hnswOptions")
            .classField(VectorFieldSpecification.HnswOptions::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    /** Engine for vector storage and search. Defaults to {@link VectorSearchEngine#LUCENE}. */
    public static final Field.WithDefault<VectorSearchEngine> ENGINE =
        Field.builder("engine")
            .enumField(VectorSearchEngine.class)
            .asCamelCase()
            .optional()
            .withDefault(VectorSearchEngine.LUCENE);
  }

  public record HnswOptions(int maxEdges, int numEdgeCandidates) implements DocumentEncodable {
    private static class Fields {

      private static final Field.WithDefault<Integer> MAX_EDGES =
          Field.builder("maxEdges")
              .intField()
              .mustBeWithinBounds(Range.of(DEFAULT_MAX_EDGES, MAXIMUM_MAX_EDGES))
              .optional()
              .withDefault(DEFAULT_MAX_EDGES);
      private static final Field.WithDefault<Integer> NUM_EDGE_CANDIDATES =
          Field.builder("numEdgeCandidates")
              .intField()
              .mustBeWithinBounds(
                  Range.of(DEFAULT_NUM_EDGE_CANDIDATES, MAXIMUM_NUM_EDGE_CANDIDATES))
              .optional()
              .withDefault(DEFAULT_NUM_EDGE_CANDIDATES);
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .fieldOmitDefaultValue(Fields.MAX_EDGES, this.maxEdges)
          .fieldOmitDefaultValue(Fields.NUM_EDGE_CANDIDATES, this.numEdgeCandidates)
          .build();
    }

    public static VectorFieldSpecification.HnswOptions fromBson(DocumentParser parser)
        throws BsonParseException {
      return new VectorFieldSpecification.HnswOptions(
          parser.getField(Fields.MAX_EDGES).unwrap(),
          parser.getField(Fields.NUM_EDGE_CANDIDATES).unwrap());
    }
  }

  private final int numDimensions;
  private final VectorSimilarity similarity;
  private final VectorQuantization quantization;
  private final VectorIndexingAlgorithm indexingAlgorithm;
  private final VectorSearchEngine engine;

  public VectorFieldSpecification(
      int numDimensions,
      VectorSimilarity similarity,
      VectorQuantization quantization,
      VectorIndexingAlgorithm indexingAlgorithm) {
    this(numDimensions, similarity, quantization, indexingAlgorithm, VectorSearchEngine.LUCENE);
  }

  public VectorFieldSpecification(
      int numDimensions,
      VectorSimilarity similarity,
      VectorQuantization quantization,
      VectorIndexingAlgorithm indexingAlgorithm,
      VectorSearchEngine engine) {
    this.numDimensions = numDimensions;
    this.similarity = similarity;
    this.quantization = quantization;
    this.indexingAlgorithm = indexingAlgorithm;
    this.engine = engine;
  }

  public int numDimensions() {
    return this.numDimensions;
  }

  public VectorSimilarity similarity() {
    return this.similarity;
  }

  public VectorQuantization quantization() {
    return this.quantization;
  }

  public VectorIndexingAlgorithm indexingAlgorithm() {
    return this.indexingAlgorithm;
  }

  public VectorSearchEngine engine() {
    return this.engine;
  }

  public boolean isCustomVectorEngine() {
    return this.engine == VectorSearchEngine.CUSTOM;
  }

  @Override
  public BsonDocument toBson() {
    Optional<HnswOptions> maybeHnswOptions =
        this.indexingAlgorithm instanceof VectorIndexingAlgorithm.HnswIndexingAlgorithm
            ? Optional.of(
                ((VectorIndexingAlgorithm.HnswIndexingAlgorithm) this.indexingAlgorithm).options())
            : Optional.empty();

    return BsonDocumentBuilder.builder()
        .field(Fields.NUM_DIMENSIONS, this.numDimensions)
        .fieldOmitDefaultValue(Fields.INDEXING_ALGORITHM, this.indexingAlgorithm.type())
        .fieldOmitDefaultValue(Fields.HNSW_OPTIONS, maybeHnswOptions, DEFAULT_HNSW_OPTIONS)
        .field(Fields.SIMILARITY, this.similarity)
        // TODO(CLOUDP-307981): remove fieldOmitDefaultValue since scalar quantization has been
        //  introduced
        .fieldOmitDefaultValue(Fields.QUANTIZATION, this.quantization)
        .fieldOmitDefaultValue(Fields.ENGINE, this.engine)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VectorFieldSpecification that = (VectorFieldSpecification) o;
    return this.numDimensions == that.numDimensions
        && this.similarity == that.similarity
        && this.quantization == that.quantization
        && Objects.equals(this.indexingAlgorithm, that.indexingAlgorithm)
        && this.engine == that.engine;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.numDimensions, this.similarity,
        this.quantization, this.indexingAlgorithm, this.engine);
  }

  public static VectorFieldSpecification fromBson(DocumentParser parser) throws BsonParseException {
    validateBinaryQuantizationDimensionsMultipleOf8(
        parser.getContext(),
        parser.getField(Fields.NUM_DIMENSIONS),
        parser.getField(Fields.QUANTIZATION));

    Optional<HnswOptions> maybeHnswOptions = parser.getField(Fields.HNSW_OPTIONS).unwrap();
    VectorIndexingAlgorithm.AlgorithmType algorithmType =
        parser.getField(Fields.INDEXING_ALGORITHM).unwrap();

    VectorIndexingAlgorithm indexingAlgorithm =
        resolveIndexingAlgorithm(parser, algorithmType, maybeHnswOptions);
    return new VectorFieldSpecification(
        parser.getField(Fields.NUM_DIMENSIONS).unwrap(),
        parser.getField(Fields.SIMILARITY).unwrap(),
        parser.getField(Fields.QUANTIZATION).unwrap(),
        indexingAlgorithm,
        parser.getField(Fields.ENGINE).unwrap());
  }

  private static VectorIndexingAlgorithm resolveIndexingAlgorithm(
      DocumentParser parser,
      VectorIndexingAlgorithm.AlgorithmType algorithmType,
      Optional<HnswOptions> maybeHnswOptions)
      throws BsonParseException {
    if (VectorIndexingAlgorithm.AlgorithmType.HNSW != algorithmType
        && maybeHnswOptions.isPresent()) {
      parser
          .getContext()
          .handleSemanticError("hnswOptions is only supported with \"indexingMethod: hnsw\".");
    }

    return switch (algorithmType) {
      case FLAT -> new VectorIndexingAlgorithm.FlatIndexingAlgorithm();
      case HNSW ->
          maybeHnswOptions.isPresent()
              ? new VectorIndexingAlgorithm.HnswIndexingAlgorithm(maybeHnswOptions.get())
              : new VectorIndexingAlgorithm.HnswIndexingAlgorithm();
    };
  }

  private static void validateBinaryQuantizationDimensionsMultipleOf8(
      BsonParseContext context,
      ParsedField.Required<Integer> dimensions,
      ParsedField.WithDefault<VectorQuantization> quantization)
      throws BsonParseException {
    if (quantization.unwrap() == VectorQuantization.BINARY && dimensions.unwrap() % 8 != 0) {
      context.handleSemanticError(
          dimensions.getField()
              + " "
              + dimensions.unwrap().toString()
              + " must be a multiple of 8 for binary quantization");
      // NOTE(corecursion): I tested with non-multiple-of-8, and it worked fine at that time.
    }
  }
}
