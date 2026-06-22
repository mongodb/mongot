package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/** The {@code replication} block of the Community mongot config file. */
public record CommunityReplicationConfig(Optional<MongoDbConfig> mongoDbConfig)
    implements DocumentEncodable {

  static class Fields {
    public static final Field.Optional<MongoDbConfig> MONGODB_CONFIG =
        Field.builder("mongodb")
            .classField(MongoDbConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  public static CommunityReplicationConfig fromBson(DocumentParser parser)
      throws BsonParseException {
    return new CommunityReplicationConfig(parser.getField(Fields.MONGODB_CONFIG).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.MONGODB_CONFIG, this.mongoDbConfig).build();
  }

  public record MongoDbConfig(
      Optional<Integer> numConcurrentInitialSyncs,
      Optional<Integer> numConcurrentChangeStreams,
      Optional<Integer> numIndexingThreads,
      Optional<Integer> numConcurrentSynonymSyncs,
      Optional<Integer> changeStreamMaxTimeMs,
      Optional<Integer> changeStreamCursorMaxTimeSec,
      Optional<Integer> numChangeStreamDecodingThreads,
      Optional<Boolean> pauseAllInitialSyncs,
      Optional<List<ObjectId>> pauseInitialSyncOnIndexIds,
      Optional<List<String>> excludedChangestreamFields)
      implements DocumentEncodable {

    static class Fields {
      public static final Field.Optional<Integer> NUM_CONCURRENT_INITIAL_SYNCS =
          Field.builder("numConcurrentInitialSyncs")
              .intField()
              .mustBePositive()
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> NUM_CONCURRENT_CHANGE_STREAMS =
          Field.builder("numConcurrentChangeStreams")
              .intField()
              .mustBePositive()
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> NUM_INDEXING_THREADS =
          Field.builder("numIndexingThreads").intField().mustBePositive().optional().noDefault();

      public static final Field.Optional<Integer> NUM_CONCURRENT_SYNONYM_SYNCS =
          Field.builder("numConcurrentSynonymSyncs")
              .intField()
              .mustBePositive()
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> CHANGE_STREAM_MAX_TIME_MS =
          Field.builder("changeStreamMaxTimeMs").intField().mustBePositive().optional().noDefault();

      public static final Field.Optional<Integer> CHANGE_STREAM_CURSOR_MAX_TIME_SEC =
          Field.builder("changeStreamCursorMaxTimeSec")
              .intField()
              .mustBePositive()
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> NUM_CHANGE_STREAM_DECODING_THREADS =
          Field.builder("numChangeStreamDecodingThreads")
              .intField()
              .mustBePositive()
              .optional()
              .noDefault();

      public static final Field.Optional<Boolean> PAUSE_ALL_INITIAL_SYNCS =
          Field.builder("pauseAllInitialSyncs").booleanField().optional().noDefault();

      public static final Field.Optional<List<ObjectId>> PAUSE_INITIAL_SYNC_ON_INDEX_IDS =
          Field.builder("pauseInitialSyncOnIndexIds")
              .listOf(Value.builder().objectIdValue().required())
              .optional()
              .noDefault();

      public static final Field.Optional<List<String>> EXCLUDED_CHANGESTREAM_FIELDS =
          Field.builder("excludedChangestreamFields")
              .listOf(Value.builder().stringValue().required())
              .optional()
              .noDefault();
    }

    public static MongoDbConfig fromBson(DocumentParser parser) throws BsonParseException {
      return new MongoDbConfig(
          parser.getField(Fields.NUM_CONCURRENT_INITIAL_SYNCS).unwrap(),
          parser.getField(Fields.NUM_CONCURRENT_CHANGE_STREAMS).unwrap(),
          parser.getField(Fields.NUM_INDEXING_THREADS).unwrap(),
          parser.getField(Fields.NUM_CONCURRENT_SYNONYM_SYNCS).unwrap(),
          parser.getField(Fields.CHANGE_STREAM_MAX_TIME_MS).unwrap(),
          parser.getField(Fields.CHANGE_STREAM_CURSOR_MAX_TIME_SEC).unwrap(),
          parser.getField(Fields.NUM_CHANGE_STREAM_DECODING_THREADS).unwrap(),
          parser.getField(Fields.PAUSE_ALL_INITIAL_SYNCS).unwrap(),
          parser.getField(Fields.PAUSE_INITIAL_SYNC_ON_INDEX_IDS).unwrap(),
          parser.getField(Fields.EXCLUDED_CHANGESTREAM_FIELDS).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.NUM_CONCURRENT_INITIAL_SYNCS, this.numConcurrentInitialSyncs)
          .field(Fields.NUM_CONCURRENT_CHANGE_STREAMS, this.numConcurrentChangeStreams)
          .field(Fields.NUM_INDEXING_THREADS, this.numIndexingThreads)
          .field(Fields.NUM_CONCURRENT_SYNONYM_SYNCS, this.numConcurrentSynonymSyncs)
          .field(Fields.CHANGE_STREAM_MAX_TIME_MS, this.changeStreamMaxTimeMs)
          .field(Fields.CHANGE_STREAM_CURSOR_MAX_TIME_SEC, this.changeStreamCursorMaxTimeSec)
          .field(Fields.NUM_CHANGE_STREAM_DECODING_THREADS, this.numChangeStreamDecodingThreads)
          .field(Fields.PAUSE_ALL_INITIAL_SYNCS, this.pauseAllInitialSyncs)
          .field(Fields.PAUSE_INITIAL_SYNC_ON_INDEX_IDS, this.pauseInitialSyncOnIndexIds)
          .field(Fields.EXCLUDED_CHANGESTREAM_FIELDS, this.excludedChangestreamFields)
          .build();
    }
  }
}
