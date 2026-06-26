package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig.GlobalReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.util.Runtime;
import java.util.Optional;

/**
 * Maps the Community {@code replication.mongodb} config block onto the runtime {@link
 * MongoDbReplicationConfig}.
 */
public final class MongoDbReplicationConfigMapper {

  private MongoDbReplicationConfigMapper() {}

  /**
   * Builds {@link GlobalReplicationConfig} from the {@code replication.mongodb} block, falling back
   * to the defaults for anything not configured. The result is shared by both search replication
   * and auto-embedding materialized views.
   *
   * @param replicationConfig the parsed {@code replication} block, if present
   */
  public static GlobalReplicationConfig toGlobalReplicationConfig(
      Optional<CommunityReplicationConfig> replicationConfig) {
    var mongodb = replicationConfig.flatMap(CommunityReplicationConfig::mongoDbConfig);
    var defaults = CommonReplicationConfig.communityDefaultGlobalReplicationConfig();

    return new GlobalReplicationConfig(
        mongodb.flatMap(m -> m.pauseAllInitialSyncs()).orElse(defaults.pauseAllInitialSyncs()),
        mongodb
            .flatMap(m -> m.pauseInitialSyncOnIndexIds())
            .orElse(defaults.pauseInitialSyncOnIndexIds()),
        defaults.enableSplitLargeChangeStreamEvents(),
        defaults.splitLargeChangeStreamEventsForInitialSync(),
        mongodb
            .flatMap(m -> m.excludedChangestreamFields())
            .orElse(defaults.excludedChangestreamFields()),
        defaults.matchCollectionUuidForUpdateLookup());
  }

  /**
   * Builds a {@link MongoDbReplicationConfig} from the parsed Community {@code replication} block.
   *
   * @param globalReplicationConfig deployment-wide replication settings
   * @param replicationConfig the parsed {@code replication} block, if present
   * @param runtime runtime used to get numCpus
   */
  public static MongoDbReplicationConfig toMongoDbReplicationConfig(
      GlobalReplicationConfig globalReplicationConfig,
      Runtime runtime,
      Optional<CommunityReplicationConfig> replicationConfig) {
    var mongodb = replicationConfig.flatMap(CommunityReplicationConfig::mongoDbConfig);

    var numConcurrentInitialSyncs = mongodb.flatMap(m -> m.numConcurrentInitialSyncs());
    var numConcurrentChangeStreams = mongodb.flatMap(m -> m.numConcurrentChangeStreams());
    var numIndexingThreads =
        mongodb.flatMap(m -> m.numIndexingThreads()).or(() -> Optional.of(runtime.getNumCpus()));
    var numConcurrentSynonymSyncs = mongodb.flatMap(m -> m.numConcurrentSynonymSyncs());
    var changeStreamMaxTimeMs = mongodb.flatMap(m -> m.changeStreamMaxTimeMs());
    var changeStreamCursorMaxTimeSec = mongodb.flatMap(m -> m.changeStreamCursorMaxTimeSec());
    var numChangeStreamDecodingThreads = mongodb.flatMap(m -> m.numChangeStreamDecodingThreads());

    return MongoDbReplicationConfig.create(
        globalReplicationConfig,
        numConcurrentInitialSyncs,
        numConcurrentChangeStreams,
        numIndexingThreads,
        changeStreamMaxTimeMs,
        numConcurrentSynonymSyncs,
        changeStreamCursorMaxTimeSec,
        numChangeStreamDecodingThreads,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }
}
