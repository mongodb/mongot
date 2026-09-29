package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.Runtime;
import com.xgen.testing.util.MockRuntimeBuilder;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.bson.types.ObjectId;
import org.junit.Test;

public class MongoDbReplicationConfigMapperTest {

  private static final Path TUNING_CONFIG =
      Path.of("src/test/unit/resources/config/provider/community/communityConfigTuning.yaml");

  @Test
  public void toMongoDbReplicationConfig_appliesNumIndexingThreadsDefaultWhenUnset() {
    Runtime runtime =
        new MockRuntimeBuilder().withNumCpus(7).withMaxHeapSize(Bytes.ofMebi(512)).build();

    MongoDbReplicationConfig rc =
        MongoDbReplicationConfigMapper.toMongoDbReplicationConfig(
            MongoDbReplicationConfigMapper.toGlobalReplicationConfig(Optional.empty()),
            runtime,
            Optional.empty());
    assertEquals(7, rc.numIndexingThreads);
  }

  @Test
  public void toMongoDbReplicationConfig_honorsConfiguredValues() throws Exception {
    CommunityConfig config =
        CommunityConfig.readFromFile(TUNING_CONFIG, Optional.of(DeploymentEnvironment.COMMUNITY))
            .config();
    Runtime runtime =
        new MockRuntimeBuilder().withNumCpus(99).withMaxHeapSize(Bytes.ofMebi(512)).build();

    MongoDbReplicationConfig rc =
        MongoDbReplicationConfigMapper.toMongoDbReplicationConfig(
            MongoDbReplicationConfigMapper.toGlobalReplicationConfig(
                config.advancedConfigs().flatMap(AdvancedConfigs::replicationConfig)),
            runtime,
            config.advancedConfigs().flatMap(AdvancedConfigs::replicationConfig));

    assertEquals(3, rc.numConcurrentInitialSyncs);
    assertEquals(12, rc.numConcurrentChangeStreams);
    assertEquals(8, rc.numIndexingThreads);
    assertEquals(2, rc.numConcurrentSynonymSyncs);
    assertEquals(500, rc.changeStreamMaxTimeMs);
    assertEquals(900, rc.changeStreamCursorMaxTimeSec);
    assertEquals(4, rc.numChangeStreamDecodingThreads);
    assertTrue(rc.getPauseAllInitialSyncs());
    assertEquals(
        List.of(new ObjectId("507f1f77bcf86cd799439011"), new ObjectId("507f1f77bcf86cd799439012")),
        rc.getPauseInitialSyncOnIndexIds());
  }

  @Test
  public void toGlobalReplicationConfig_honorsConfiguredPauseKnobs() throws Exception {
    CommunityConfig config =
        CommunityConfig.readFromFile(TUNING_CONFIG, Optional.of(DeploymentEnvironment.COMMUNITY))
            .config();

    var global =
        MongoDbReplicationConfigMapper.toGlobalReplicationConfig(
            config.advancedConfigs().flatMap(AdvancedConfigs::replicationConfig));

    assertTrue(global.pauseAllInitialSyncs());
    assertEquals(
        List.of(new ObjectId("507f1f77bcf86cd799439011"), new ObjectId("507f1f77bcf86cd799439012")),
        global.pauseInitialSyncOnIndexIds());
    assertTrue(global.enableSplitLargeChangeStreamEvents());
    assertTrue(global.matchCollectionUuidForUpdateLookup());
    assertEquals(List.of("lsid", "txnNumber"), global.excludedChangestreamFields());
  }

  @Test
  public void toGlobalReplicationConfig_appliesCommunityDefaultsWhenUnset() {
    var global = MongoDbReplicationConfigMapper.toGlobalReplicationConfig(Optional.empty());

    assertFalse(global.pauseAllInitialSyncs());
    assertEquals(List.of(), global.pauseInitialSyncOnIndexIds());
    assertTrue(global.enableSplitLargeChangeStreamEvents());
    assertTrue(global.matchCollectionUuidForUpdateLookup());
    assertFalse(global.splitLargeChangeStreamEventsForInitialSync());
    assertEquals(List.of(), global.excludedChangestreamFields());
  }
}
