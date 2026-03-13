package com.xgen.mongot.config.provider.community.embedding;

import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Embedding configuration for community edition. Supports: - Overriding the provider endpoint URL -
 * Specifying file paths for Voyage API credentials - Configuring whether this instance is the
 * auto-embedding view writer (leader)
 */
public record EmbeddingConfig(
    Optional<String> providerEndpoint,
    Optional<Path> queryKeyFile,
    Optional<Path> indexingKeyFile,
    Optional<Integer> mvWriteRateLimitRps,
    boolean isAutoEmbeddingViewWriter)
    implements DocumentEncodable {

  private static class Fields {
    public static final Field.Optional<String> PROVIDER_ENDPOINT =
        Field.builder("providerEndpoint").stringField().optional().noDefault();

    public static final Field.Optional<Path> QUERY_KEY_FILE =
        Field.builder("queryKeyFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .optional()
            .noDefault();

    public static final Field.Optional<Path> INDEXING_KEY_FILE =
        Field.builder("indexingKeyFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .optional()
            .noDefault();

    public static final Field.Optional<Integer> MV_WRITE_RATE_LIMIT_RPS =
        Field.builder("mvWriteRateLimitRps").intField().mustBePositive().optional().noDefault();

    public static final Field.WithDefault<Boolean> IS_AUTO_EMBEDDING_VIEW_WRITER =
        Field.builder("isAutoEmbeddingViewWriter").booleanField().optional().withDefault(false);
  }

  public static EmbeddingConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new EmbeddingConfig(
        parser.getField(Fields.PROVIDER_ENDPOINT).unwrap(),
        parser.getField(Fields.QUERY_KEY_FILE).unwrap(),
        parser.getField(Fields.INDEXING_KEY_FILE).unwrap(),
        parser.getField(Fields.MV_WRITE_RATE_LIMIT_RPS).unwrap(),
        parser.getField(Fields.IS_AUTO_EMBEDDING_VIEW_WRITER).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PROVIDER_ENDPOINT, this.providerEndpoint)
        .field(Fields.QUERY_KEY_FILE, this.queryKeyFile)
        .field(Fields.INDEXING_KEY_FILE, this.indexingKeyFile)
        .field(Fields.MV_WRITE_RATE_LIMIT_RPS, this.mvWriteRateLimitRps)
        .field(Fields.IS_AUTO_EMBEDDING_VIEW_WRITER, this.isAutoEmbeddingViewWriter)
        .build();
  }
}

