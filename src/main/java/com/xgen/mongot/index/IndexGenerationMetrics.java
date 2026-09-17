package com.xgen.mongot.index;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record IndexGenerationMetrics(
    IndexMetricsGenerationId indexMetricsGenerationId, IndexMetrics indexMetrics)
    implements DocumentEncodable {
  private static class Fields {
    private static final Field.Required<IndexMetricsGenerationId> INDEX_METRICS_GENERATION_ID =
        Field.builder("generationId")
            .classField(IndexMetricsGenerationId::fromBson)
            .disallowUnknownFields()
            .required();
  }

  @VisibleForTesting
  public IndexGenerationMetrics(GenerationId generationId, IndexMetrics indexMetrics) {
    this(IndexMetricsGenerationId.create(generationId), indexMetrics);
  }

  @Override
  public BsonDocument toBson() {
    BsonDocument indexGenerationStats =
        BsonDocumentBuilder.builder()
            .field(Fields.INDEX_METRICS_GENERATION_ID, this.indexMetricsGenerationId)
            .build();

    indexGenerationStats.putAll(this.indexMetrics.toBson());

    return indexGenerationStats;
  }

  public static IndexGenerationMetrics fromBson(DocumentParser parser) throws BsonParseException {
    return new IndexGenerationMetrics(
        parser.getField(Fields.INDEX_METRICS_GENERATION_ID).unwrap(),
        IndexMetrics.fromBson(parser));
  }

  public record IndexMetricsGenerationId(
      ObjectId indexId, int userIndexVersion, int indexFormatVersion, int attemptNumber)
      implements DocumentEncodable {
    static class Fields {
      private static final Field.Required<ObjectId> INDEX_ID =
          Field.builder("indexId").objectIdField().required();

      static final Field.Required<Integer> USER_VERSION =
          Field.builder("userVersion").intField().mustBeNonNegative().required();

      static final Field.Required<Integer> FORMAT_VERSION =
          Field.builder("formatVersion").intField().mustBePositive().required();

      static final Field.Required<Integer> ATTEMPT_NUMBER =
          Field.builder("attemptNumber").intField().required();
    }

    static IndexMetricsGenerationId create(GenerationId generationId) {
      return new IndexMetricsGenerationId(
          generationId.indexId,
          generationId.generation.userIndexVersion.versionNumber,
          generationId.generation.formatVersion.versionNumber(),
          generationId.generation.attemptNumber);
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.INDEX_ID, this.indexId)
          .field(Fields.USER_VERSION, this.userIndexVersion)
          .field(Fields.FORMAT_VERSION, this.indexFormatVersion)
          .field(Fields.ATTEMPT_NUMBER, this.attemptNumber)
          .build();
    }

    public static IndexMetricsGenerationId fromBson(DocumentParser parser)
        throws BsonParseException {
      return new IndexMetricsGenerationId(
          parser.getField(Fields.INDEX_ID).unwrap(),
          parser.getField(Fields.USER_VERSION).unwrap(),
          parser.getField(Fields.FORMAT_VERSION).unwrap(),
          parser.getField(Fields.ATTEMPT_NUMBER).unwrap());
    }
  }
}
