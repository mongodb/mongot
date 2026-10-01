package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.DeploymentEnvironment;
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
import picocli.CommandLine;

/** The {@code replication} block of the Community mongot config file. */
public record CommunityReplicationConfig(
    Optional<MongoDbConfig> mongoDbConfig, Optional<InitialSyncConfig> initialSyncConfig)
    implements DocumentEncodable {

  static class Fields {
    public static final Field.Optional<MongoDbConfig> MONGODB_CONFIG =
        Field.builder("mongodb")
            .classField(MongoDbConfig::fromBson)
            .disallowUnknownFields()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();

    public static final Field.Optional<InitialSyncConfig> INITIAL_SYNC_CONFIG =
        Field.builder("initialSync")
            .classField(InitialSyncConfig::fromBson)
            .disallowUnknownFields()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();
  }

  public static final class CommunityReplicationConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Mixin
    public MongoDbConfig.MongoDbConfigCliOptionRegistry mongoDbConfigCliOptionRegistry =
        new MongoDbConfig.MongoDbConfigCliOptionRegistry();

    public CommunityReplicationConfigCliOptionRegistry() {
      addChildRegistry(Fields.MONGODB_CONFIG, this.mongoDbConfigCliOptionRegistry);
    }
  }

  public static CommunityReplicationConfig fromBson(DocumentParser parser)
      throws BsonParseException {
    return new CommunityReplicationConfig(
        parser.getField(Fields.MONGODB_CONFIG).unwrap(),
        parser.getField(Fields.INITIAL_SYNC_CONFIG).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MONGODB_CONFIG, this.mongoDbConfig)
        .field(Fields.INITIAL_SYNC_CONFIG, this.initialSyncConfig)
        .build();
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
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> NUM_CONCURRENT_CHANGE_STREAMS =
          Field.builder("numConcurrentChangeStreams")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> NUM_INDEXING_THREADS =
          Field.builder("numIndexingThreads")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> NUM_CONCURRENT_SYNONYM_SYNCS =
          Field.builder("numConcurrentSynonymSyncs")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> CHANGE_STREAM_MAX_TIME_MS =
          Field.builder("changeStreamMaxTimeMs")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> CHANGE_STREAM_CURSOR_MAX_TIME_SEC =
          Field.builder("changeStreamCursorMaxTimeSec")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> NUM_CHANGE_STREAM_DECODING_THREADS =
          Field.builder("numChangeStreamDecodingThreads")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Boolean> PAUSE_ALL_INITIAL_SYNCS =
          Field.builder("pauseAllInitialSyncs")
              .booleanField()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<List<ObjectId>> PAUSE_INITIAL_SYNC_ON_INDEX_IDS =
          Field.builder("pauseInitialSyncOnIndexIds")
              .listOf(Value.builder().objectIdValue().required())
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<List<String>> EXCLUDED_CHANGESTREAM_FIELDS =
          Field.builder("excludedChangestreamFields")
              .listOf(Value.builder().stringValue().required())
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();
    }

    public static final class MongoDbConfigCliOptionRegistry extends CliOptionRegistry {
      @CommandLine.Option(
          names = "--replicationMongodbNumConcurrentInitialSyncs",
          description =
              "Maximum number of indexes that can run their initial sync at the same time")
      public Optional<Integer> numConcurrentInitialSyncs = Optional.empty();

      @CommandLine.Option(
          names = "--replicationMongodbNumConcurrentChangeStreams",
          description =
              "Maximum number of steady-state change streams that can have outstanding requests "
                  + "to the source mongod node at any one time")
      public Optional<Integer> numConcurrentChangeStreams = Optional.empty();

      @CommandLine.Option(
          names = "--replicationMongodbNumIndexingThreads",
          description =
              "Number of threads used to write replicated documents and changes into indexes")
      public Optional<Integer> numIndexingThreads = Optional.empty();

      @CommandLine.Option(
          names = "--replicationMongodbNumConcurrentSynonymSyncs",
          description = "Maximum number of synonym source collections that can sync concurrently")
      public Optional<Integer> numConcurrentSynonymSyncs = Optional.empty();

      public MongoDbConfigCliOptionRegistry() {
        addIntegerLeafValue(
            MongoDbConfig.Fields.NUM_CONCURRENT_INITIAL_SYNCS,
            () -> this.numConcurrentInitialSyncs);
        addIntegerLeafValue(
            MongoDbConfig.Fields.NUM_CONCURRENT_CHANGE_STREAMS,
            () -> this.numConcurrentChangeStreams);
        addIntegerLeafValue(
            MongoDbConfig.Fields.NUM_INDEXING_THREADS, () -> this.numIndexingThreads);
        addIntegerLeafValue(
            MongoDbConfig.Fields.NUM_CONCURRENT_SYNONYM_SYNCS,
            () -> this.numConcurrentSynonymSyncs);
      }
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

  public record InitialSyncConfig(Optional<BufferlessInitialSyncConfig> bufferlessConfig)
      implements DocumentEncodable {

    static class Fields {
      public static final Field.Optional<BufferlessInitialSyncConfig> BUFFERLESS_CONFIG =
          Field.builder("bufferless")
              .classField(BufferlessInitialSyncConfig::fromBson)
              .disallowUnknownFields()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();
    }

    public static InitialSyncConfig fromBson(DocumentParser parser) throws BsonParseException {
      return new InitialSyncConfig(parser.getField(Fields.BUFFERLESS_CONFIG).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.BUFFERLESS_CONFIG, this.bufferlessConfig)
          .build();
    }

    public record BufferlessInitialSyncConfig(Optional<Boolean> enableNaturalOrderScan)
        implements DocumentEncodable {

      static class Fields {
        // TODO(CLOUDP-416372) Remove the config once it is fully rolled out.
        public static final Field.Optional<Boolean> ENABLE_NATURAL_ORDER_SCAN =
            Field.builder("enableNaturalOrderScan")
                .booleanField()
                .edition(DeploymentEnvironment.COMMUNITY)
                .optional()
                .noDefault();
      }

      public static BufferlessInitialSyncConfig fromBson(DocumentParser parser)
          throws BsonParseException {
        return new BufferlessInitialSyncConfig(
            parser.getField(Fields.ENABLE_NATURAL_ORDER_SCAN).unwrap());
      }

      @Override
      public BsonDocument toBson() {
        return BsonDocumentBuilder.builder()
            .field(Fields.ENABLE_NATURAL_ORDER_SCAN, this.enableNaturalOrderScan)
            .build();
      }
    }
  }
}
