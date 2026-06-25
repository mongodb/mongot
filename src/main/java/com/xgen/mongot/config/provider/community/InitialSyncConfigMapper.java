package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.community.CommunityReplicationConfig.InitialSyncConfig.BufferlessInitialSyncConfig;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import java.util.Optional;

/**
 * Maps the Community {@code replication.initialSync.bufferless} config block to the runtime {@link
 * InitialSyncConfig}.
 */
public final class InitialSyncConfigMapper {

  private InitialSyncConfigMapper() {}

  public static InitialSyncConfig toInitialSyncConfig(
      Optional<CommunityReplicationConfig> replicationConfig) {
    Optional<Boolean> enableNaturalOrderScan =
        replicationConfig
            .flatMap(CommunityReplicationConfig::initialSyncConfig)
            .flatMap(CommunityReplicationConfig.InitialSyncConfig::bufferlessConfig)
            .flatMap(BufferlessInitialSyncConfig::enableNaturalOrderScan);

    return new InitialSyncConfig(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        enableNaturalOrderScan,
        Optional.empty());
  }
}
