package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.community.CommunityAutoEmbeddingConfig.MaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig.GlobalReplicationConfig;
import java.util.Optional;

/**
 * Maps {@code autoEmbedding.materializedView} config block onto the runtime {@link
 * AutoEmbeddingMaterializedViewConfig}.
 */
public final class AutoEmbeddingMaterializedViewConfigMapper {

  private AutoEmbeddingMaterializedViewConfigMapper() {}

  /**
   * Builds an {@link AutoEmbeddingMaterializedViewConfig} from the parsed {@code autoEmbedding}
   * block.
   *
   * @param globalReplicationConfig global replication settings
   * @param config the community config
   */
  public static AutoEmbeddingMaterializedViewConfig toAutoEmbeddingMaterializedViewConfig(
      GlobalReplicationConfig globalReplicationConfig, CommunityConfig config) {
    Optional<CommunityAutoEmbeddingConfig> autoEmbeddingConfig =
        config.advancedConfigs().flatMap(AdvancedConfigs::autoEmbeddingConfig);
    var matView = autoEmbeddingConfig.flatMap(CommunityAutoEmbeddingConfig::materializedViewConfig);
    var allNumConcurrentInitialSyncs =
        config
            .advancedConfigs()
            .flatMap(AdvancedConfigs::replicationConfig)
            .flatMap(CommunityReplicationConfig::mongoDbConfig)
            .flatMap(CommunityReplicationConfig.MongoDbConfig::numConcurrentInitialSyncs);
    var maxConcurrentEmbeddingInitialSyncs =
        matView
            .flatMap(MaterializedViewConfig::numConcurrentInitialSyncs)
            .or(
                () ->
                    allNumConcurrentInitialSyncs.map(allInitSync -> Math.max(1, allInitSync / 2)));

    return AutoEmbeddingMaterializedViewConfig.create(
        globalReplicationConfig,
        matView.flatMap(MaterializedViewConfig::numConcurrentChangeStreams),
        matView.flatMap(MaterializedViewConfig::numIndexingThreads),
        matView.flatMap(MaterializedViewConfig::changeStreamMaxTimeMs),
        matView.flatMap(MaterializedViewConfig::changeStreamCursorMaxTimeSec),
        matView.flatMap(MaterializedViewConfig::numChangeStreamDecodingThreads),
        matView.flatMap(MaterializedViewConfig::requestRateLimitBackoffMs),
        maxConcurrentEmbeddingInitialSyncs,
        matView.flatMap(MaterializedViewConfig::maxInFlightEmbeddingGetMores),
        matView.flatMap(MaterializedViewConfig::matViewWriterMaxConnections),
        matView.flatMap(MaterializedViewConfig::numEmbeddingThreads),
        matView.flatMap(MaterializedViewConfig::embeddingGetMoreBatchSize),
        Optional.empty(),
        matView.flatMap(MaterializedViewConfig::mvWriteRateLimitRps),
        matView.flatMap(MaterializedViewConfig::embeddingProviderRpsLimit),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        matView.flatMap(MaterializedViewConfig::globalMemoryBudgetHeapPercent),
        matView.flatMap(MaterializedViewConfig::perBatchMemoryBudgetHeapPercent),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }
}
