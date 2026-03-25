package com.xgen.mongot.index.definition;

import static com.xgen.mongot.index.definition.VectorTextFieldSpecification.DEFAULT_MODALITY;

import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.Objects;
import org.bson.BsonDocument;

/**
 * Represents an auto-embed vector field definition within a search index.
 *
 * <p>This field type specifies that text from a source field should be automatically
 * embedded using the configured embedding model, then indexed for vector search.
 */
public final class SearchAutoEmbedFieldDefinition implements FieldTypeDefinition {

  private static final VectorSimilarity DEFAULT_VECTOR_SIMILARITY = VectorSimilarity.DOT_PRODUCT;
  private static final VectorQuantization DEFAULT_VECTOR_QUANTIZATION = VectorQuantization.NONE;

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
    static final Field.Required<FieldPath> SOURCE_FIELD =
        Field.builder("sourceField")
            .classField(FieldPathField::parse, FieldPathField::encode)
            .required();
  }

  private final String modelName;
  private final String modality;
  private final FieldPath sourceField;
  private final VectorFieldSpecification specification;

  public SearchAutoEmbedFieldDefinition(
      String modelName, String modality, FieldPath sourceField) {
    this.modelName = modelName;
    this.modality = modality;
    this.sourceField = sourceField;
    this.specification =
        new VectorFieldSpecification(
            resolveModelDimensions(modelName),
            DEFAULT_VECTOR_SIMILARITY,
            DEFAULT_VECTOR_QUANTIZATION,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm());
  }

  public SearchAutoEmbedFieldDefinition(String modelName, FieldPath sourceField) {
    this(modelName, DEFAULT_MODALITY, sourceField);
  }

  public String modelName() {
    return this.modelName;
  }

  public String modality() {
    return this.modality;
  }

  public FieldPath sourceField() {
    return this.sourceField;
  }

  public VectorFieldSpecification specification() {
    return this.specification;
  }

  @Override
  public Type getType() {
    return Type.AUTO_EMBED_VECTOR;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MODEL, this.modelName)
        .field(Fields.MODALITY, this.modality)
        .field(Fields.SOURCE_FIELD, this.sourceField)
        .build();
  }

  static SearchAutoEmbedFieldDefinition fromBson(DocumentParser parser) throws BsonParseException {
    String modelName = parser.getField(Fields.MODEL).unwrap();
    String modality = parser.getField(Fields.MODALITY).unwrap();
    FieldPath sourceField = parser.getField(Fields.SOURCE_FIELD).unwrap();
    return new SearchAutoEmbedFieldDefinition(modelName, modality, sourceField);
  }

  private static int resolveModelDimensions(String modelName) {
    return EmbeddingModelCatalog.resolveModelConfigOrDefault(modelName)
        .collectionScan()
        .modelConfig()
        .getOutputDimensions();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SearchAutoEmbedFieldDefinition that)) {
      return false;
    }
    // Intentionally excludes specification (dimensions, similarity, quantization) because these are
    // derived from the model. This matches VectorAutoEmbedFieldDefinition.equals() behavior.
    // TODO(CLOUDP-353553): dimensions, similarity, quantization overrides may be enabled soon
    //  in https://github.com/10gen/mongot/pull/5520 - update equals/hashCode accordingly.
    return Objects.equals(this.modelName, that.modelName)
        && Objects.equals(this.modality, that.modality)
        && Objects.equals(this.sourceField, that.sourceField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.modelName, this.modality, this.sourceField);
  }
}
