package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.bson.types.ObjectId;
import org.junit.Test;

public class AutoEmbeddingMaterializedViewConfigMapperTest {

  private static final Path TUNING_CONFIG =
      Path.of("src/test/unit/resources/config/provider/community/communityConfigTuning.yaml");

  private static final Path MINIMAL_CONFIG =
      Path.of("src/test/unit/resources/config/provider/community/communityConfig.yaml");

  @Test
  public void toAutoEmbeddingMaterializedViewConfig_honorsConfiguredValues() throws Exception {
    CommunityConfig config = CommunityConfig.readFromFile(TUNING_CONFIG).config();

    AutoEmbeddingMaterializedViewConfig ae =
        AutoEmbeddingMaterializedViewConfigMapper.toAutoEmbeddingMaterializedViewConfig(
            MongoDbReplicationConfigMapper.toGlobalReplicationConfig(Optional.empty()), config);

    assertEquals(10, ae.numConcurrentChangeStreams);
    assertEquals(5, ae.numIndexingThreads);
    assertEquals(6, ae.numEmbeddingThreads);
    assertEquals(2, ae.maxConcurrentEmbeddingInitialSyncs);
    assertEquals(8, ae.matViewWriterMaxConnections);
    assertEquals(3, ae.maxInFlightEmbeddingGetMores);
    assertEquals(Optional.of(2000), ae.embeddingGetMoreBatchSize);
    assertEquals(700, ae.changeStreamMaxTimeMs);
    assertEquals(1200, ae.changeStreamCursorMaxTimeSec);
    assertEquals(3, ae.numChangeStreamDecodingThreads);
    assertEquals(250, ae.requestRateLimitBackoffMs);
    assertEquals(Optional.of(60), ae.getMvWriteRateLimitRps());
    assertEquals(Optional.of(30), ae.getEmbeddingProviderRpsLimit());
    assertEquals(80, ae.globalMemoryBudgetHeapPercent);
    assertEquals(40, ae.perBatchMemoryBudgetHeapPercent);
  }

  @Test
  public void toAutoEmbeddingMaterializedViewConfig_appliesDefaultsWhenUnset() throws Exception {
    CommunityConfig config = CommunityConfig.readFromFile(MINIMAL_CONFIG).config();

    AutoEmbeddingMaterializedViewConfig ae =
        AutoEmbeddingMaterializedViewConfigMapper.toAutoEmbeddingMaterializedViewConfig(
            MongoDbReplicationConfigMapper.toGlobalReplicationConfig(Optional.empty()), config);

    assertEquals(4, ae.matViewWriterMaxConnections);
    assertEquals(1000, ae.changeStreamMaxTimeMs);
    assertEquals(100, ae.requestRateLimitBackoffMs);
    assertEquals(1, ae.maxConcurrentEmbeddingInitialSyncs);
    assertEquals(Optional.empty(), ae.embeddingGetMoreBatchSize);
    assertEquals(Optional.empty(), ae.getMvWriteRateLimitRps());
    assertEquals(Optional.empty(), ae.getEmbeddingProviderRpsLimit());
    assertEquals(1, ae.defaultMaterializedViewNameFormatVersion);
  }

  @Test
  public void toAutoEmbeddingMaterializedViewConfig_derivesInitialSyncsFromReplication()
      throws Exception {
    CommunityConfig base = CommunityConfig.readFromFile(MINIMAL_CONFIG).config();
    var replicationConfig =
        new CommunityReplicationConfig(
            Optional.of(
                new CommunityReplicationConfig.MongoDbConfig(
                    Optional.of(8),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty())));
    CommunityConfig config = withReplicationConfig(base, replicationConfig);

    AutoEmbeddingMaterializedViewConfig ae =
        AutoEmbeddingMaterializedViewConfigMapper.toAutoEmbeddingMaterializedViewConfig(
            MongoDbReplicationConfigMapper.toGlobalReplicationConfig(Optional.empty()), config);

    assertEquals(4, ae.maxConcurrentEmbeddingInitialSyncs);
  }

  @Test
  public void toAutoEmbeddingMaterializedViewConfig_takesRateLimitsAndBudgetsFromMaterializedView()
      throws Exception {
    CommunityConfig base = CommunityConfig.readFromFile(MINIMAL_CONFIG).config();
    var autoEmbeddingConfig =
        new CommunityAutoEmbeddingConfig(
            Optional.of(
                new CommunityAutoEmbeddingConfig.MaterializedViewConfig(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(50),
                    Optional.of(25),
                    Optional.of(80),
                    Optional.of(40))));
    CommunityConfig config = withAutoEmbeddingConfig(base, autoEmbeddingConfig);

    AutoEmbeddingMaterializedViewConfig ae =
        AutoEmbeddingMaterializedViewConfigMapper.toAutoEmbeddingMaterializedViewConfig(
            MongoDbReplicationConfigMapper.toGlobalReplicationConfig(Optional.empty()), config);

    assertEquals(Optional.of(50), ae.getMvWriteRateLimitRps());
    assertEquals(Optional.of(25), ae.getEmbeddingProviderRpsLimit());
    assertEquals(80, ae.globalMemoryBudgetHeapPercent);
    assertEquals(40, ae.perBatchMemoryBudgetHeapPercent);
  }

  @Test
  public void toAutoEmbeddingMaterializedViewConfig_propagatesGlobalPauseKnobs() throws Exception {
    CommunityConfig config = CommunityConfig.readFromFile(TUNING_CONFIG).config();
    var global =
        MongoDbReplicationConfigMapper.toGlobalReplicationConfig(
            config.advancedConfigs().flatMap(AdvancedConfigs::replicationConfig));

    AutoEmbeddingMaterializedViewConfig ae =
        AutoEmbeddingMaterializedViewConfigMapper.toAutoEmbeddingMaterializedViewConfig(
            global, config);

    assertTrue(ae.getPauseAllInitialSyncs());
    assertEquals(
        List.of(new ObjectId("507f1f77bcf86cd799439011"), new ObjectId("507f1f77bcf86cd799439012")),
        ae.getPauseInitialSyncOnIndexIds());
  }

  private static CommunityConfig withReplicationConfig(
      CommunityConfig base, CommunityReplicationConfig replicationConfig) {
    return new CommunityConfig(
        base.syncSourceConfig(),
        base.storageConfig(),
        base.serverConfig(),
        base.metricsConfig(),
        base.healthCheckConfig(),
        base.loggingConfig(),
        base.embeddingConfig(),
        Optional.of(
            new AdvancedConfigs(
                Optional.empty(),
                Optional.empty(),
                Optional.of(replicationConfig),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty())));
  }

  private static CommunityConfig withAutoEmbeddingConfig(
      CommunityConfig base, CommunityAutoEmbeddingConfig autoEmbeddingConfig) {
    return new CommunityConfig(
        base.syncSourceConfig(),
        base.storageConfig(),
        base.serverConfig(),
        base.metricsConfig(),
        base.healthCheckConfig(),
        base.loggingConfig(),
        base.embeddingConfig(),
        Optional.of(
            new AdvancedConfigs(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(autoEmbeddingConfig),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty())));
  }
}
