package com.xgen.mongot.config.provider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.common.base.Supplier;
import com.mongodb.ConnectionString;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.mongodb.DurabilityConfig;
import com.xgen.mongot.replication.mongodb.MongoDbNoOpReplicationManager;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Pattern;
import org.junit.Assert;
import org.junit.Test;

public class CommonUtilsTest {
  private static class Mocks {
    private final Path dataPath;
    private final MongoDbReplicationConfig replicationConfig;
    private final AutoEmbeddingMaterializedViewConfig autoEmbeddingMaterializedViewConfig;
    private final InitialSyncConfig initialSyncConfig;
    private final DurabilityConfig durabilityConfig;
    private final FeatureFlags featureFlags;
    private final MongotCursorManager mongotCursorManager;
    private final IndexCatalog indexCatalog;
    private final InitializedIndexCatalog initializedIndexCatalog;
    private final SimpleMeterRegistry meterRegistry;
    private final SimpleMeterRegistry ftdcRegistry;
    private final SyncSourceConfig syncSourceConfig;
    private final Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier;
    private final LeaseManager leaseManager;

    private Mocks() {
      this.dataPath = mock(Path.class);
      this.replicationConfig = MongoDbReplicationConfig.getDefault();
      this.autoEmbeddingMaterializedViewConfig = AutoEmbeddingMaterializedViewConfig.getDefault();
      this.initialSyncConfig = new InitialSyncConfig();
      this.durabilityConfig = mock(DurabilityConfig.class);
      this.featureFlags = mock(FeatureFlags.class);
      this.mongotCursorManager = mock(MongotCursorManager.class);
      this.indexCatalog = mock(IndexCatalog.class);
      this.initializedIndexCatalog = mock(InitializedIndexCatalog.class);
      this.ftdcRegistry = spy(new SimpleMeterRegistry());
      this.meterRegistry = spy(new SimpleMeterRegistry());
      this.syncSourceConfig =
          new SyncSourceConfig(
              new ConnectionString("mongodb://random/?serverselectiontimeoutms=100"),
              Optional.empty(),
              new ConnectionString("mongodb://random/?serverselectiontimeoutms=100"));
      this.embeddingServiceManagerSupplier = Optional.empty();
      this.leaseManager = mock(LeaseManager.class);
    }

    private static Mocks create() {
      return new Mocks();
    }
  }

  @Test
  public void testGetReplicationManager_disableMode() {
    var mocks = Mocks.create();
    var factory =
        CommonUtils.getReplicationManagerFactory(
            mocks.dataPath,
            mocks.replicationConfig,
            mocks.initialSyncConfig,
            mocks.durabilityConfig,
            mocks.featureFlags,
            mocks.mongotCursorManager,
            mocks.indexCatalog,
            mocks.initializedIndexCatalog,
            MeterAndFtdcRegistry.create(mocks.meterRegistry, mocks.ftdcRegistry),
            DefaultConfigManager.ReplicationMode.DISABLE,
            ReplicationStateMonitor.disabled(),
            mocks.embeddingServiceManagerSupplier);
    var noOpReplicationManager = factory.create(Optional.of(mocks.syncSourceConfig));
    Assert.assertTrue(noOpReplicationManager instanceof MongoDbNoOpReplicationManager);
  }

  @Test
  public void testGetReplicationManager_enableMode() {
    var mocks = Mocks.create();
    var factory =
        CommonUtils.getReplicationManagerFactory(
            mocks.dataPath,
            mocks.replicationConfig,
            mocks.initialSyncConfig,
            mocks.durabilityConfig,
            mocks.featureFlags,
            mocks.mongotCursorManager,
            mocks.indexCatalog,
            mocks.initializedIndexCatalog,
            MeterAndFtdcRegistry.create(mocks.meterRegistry, mocks.ftdcRegistry),
            DefaultConfigManager.ReplicationMode.ENABLE,
            ReplicationStateMonitor.enabled(),
            mocks.embeddingServiceManagerSupplier);

    // Verify that creation of a normal replication manager is attempted but fails due to mocked
    // configs
    Exception exception =
        Assert.assertThrows(
            Exception.class, () -> factory.create(Optional.of(mock(SyncSourceConfig.class))));
    String errorMessage = exception.getMessage();
    String expectedPattern = "Cannot invoke \".*\" because \"connectionString\" is null";
    // Assert that the message matches the regex pattern
    Assert.assertTrue(Pattern.compile(expectedPattern).matcher(errorMessage).find());
  }

  @Test
  public void testGetReplicationManager_diskBasedMode() {
    var mocks = Mocks.create();
    var replicationGate = ToggleGate.closed();
    var replicationStateMonitor =
        ReplicationStateMonitor.builder()
            .setReplicationGate(replicationGate)
            .setInitialSyncGate(ToggleGate.opened())
            .build();
    var factory =
        CommonUtils.getReplicationManagerFactory(
            mocks.dataPath,
            mocks.replicationConfig,
            mocks.initialSyncConfig,
            mocks.durabilityConfig,
            mocks.featureFlags,
            mocks.mongotCursorManager,
            mocks.indexCatalog,
            mocks.initializedIndexCatalog,
            MeterAndFtdcRegistry.create(mocks.meterRegistry, mocks.ftdcRegistry),
            DefaultConfigManager.ReplicationMode.DISK_UTILIZATION_BASED,
            replicationStateMonitor,
            mocks.embeddingServiceManagerSupplier);
    var noOpReplicationManager = factory.create(Optional.of(mocks.syncSourceConfig));
    Assert.assertTrue(noOpReplicationManager instanceof MongoDbNoOpReplicationManager);

    // Verify that creation of a normal replication manager is attempted but fails due to mocked
    // configs
    replicationGate.open();
    Exception exception =
        Assert.assertThrows(
            Exception.class, () -> factory.create(Optional.of(mock(SyncSourceConfig.class))));
    String errorMessage = exception.getMessage();
    String expectedPattern = "Cannot invoke \".*\" because \"connectionString\" is null";
    // Assert that the message matches the regex pattern
    Assert.assertTrue(Pattern.compile(expectedPattern).matcher(errorMessage).find());
  }

  @Test
  public void testGetMaterializedViewManager_leader_disableMode() {
    var mocks = Mocks.create();
    var factory =
        CommonUtils.getAutoEmbeddingMaterializedViewManagerFactory(
            mocks.dataPath,
            mocks.autoEmbeddingMaterializedViewConfig,
            mocks.initialSyncConfig,
            mocks.featureFlags,
            mocks.mongotCursorManager,
            MeterAndFtdcRegistry.create(mocks.meterRegistry, mocks.ftdcRegistry),
            DefaultConfigManager.ReplicationMode.DISABLE,
            mocks.embeddingServiceManagerSupplier,
            /* isLeader= */ true,
            mocks.leaseManager);
    var noOpManager = factory.create(Optional.of(mocks.syncSourceConfig));
    Assert.assertTrue(noOpManager.isEmpty());
  }

  @Test
  public void testGetMaterializedViewManager_leader_enableMode() {
    var mocks = Mocks.create();
    var factory =
        CommonUtils.getAutoEmbeddingMaterializedViewManagerFactory(
            mocks.dataPath,
            mocks.autoEmbeddingMaterializedViewConfig,
            mocks.initialSyncConfig,
            mocks.featureFlags,
            mocks.mongotCursorManager,
            MeterAndFtdcRegistry.create(mocks.meterRegistry, mocks.ftdcRegistry),
            DefaultConfigManager.ReplicationMode.ENABLE,
            mocks.embeddingServiceManagerSupplier,
            /* isLeader= */ true,
            mocks.leaseManager);

    // Leader mode with empty embeddingServiceManagerSupplier should
    // throw IllegalArgumentException.
    IllegalArgumentException exception =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> factory.create(Optional.of(mock(SyncSourceConfig.class))));
    String errorMessage = exception.getMessage();
    String expectedPattern = "EmbeddingServiceManagerSupplier must be provided";
    Assert.assertTrue(Pattern.compile(expectedPattern).matcher(errorMessage).find());
  }

  @Test
  public void testGetMaterializedViewManager_follower_disableMode() {
    var mocks = Mocks.create();
    var factory =
        CommonUtils.getAutoEmbeddingMaterializedViewManagerFactory(
            mocks.dataPath,
            mocks.autoEmbeddingMaterializedViewConfig,
            mocks.initialSyncConfig,
            mocks.featureFlags,
            mocks.mongotCursorManager,
            MeterAndFtdcRegistry.create(mocks.meterRegistry, mocks.ftdcRegistry),
            DefaultConfigManager.ReplicationMode.DISABLE,
            mocks.embeddingServiceManagerSupplier,
            /* isLeader= */ false,
            mocks.leaseManager);
    var noOpManager = factory.create(Optional.of(mocks.syncSourceConfig));
    Assert.assertTrue(noOpManager.isEmpty());
  }

  @Test
  public void testGetMaterializedViewManager_follower_enableMode() {
    var mocks = Mocks.create();
    var factory =
        CommonUtils.getAutoEmbeddingMaterializedViewManagerFactory(
            mocks.dataPath,
            mocks.autoEmbeddingMaterializedViewConfig,
            mocks.initialSyncConfig,
            mocks.featureFlags,
            mocks.mongotCursorManager,
            MeterAndFtdcRegistry.create(mocks.meterRegistry, mocks.ftdcRegistry),
            DefaultConfigManager.ReplicationMode.ENABLE,
            mocks.embeddingServiceManagerSupplier,
            /* isLeader= */ false,
            mocks.leaseManager);

    // Follower mode should succeed without embeddingServiceManagerSupplier
    // (creates MaterializedViewFollowerManager).
    var result = factory.create(Optional.of(mock(SyncSourceConfig.class)));
    Assert.assertTrue("Follower mode should return a manager", result.isPresent());
  }

  @Test
  public void testGetMaterializedViewManager_diskBasedMode() {
    var mocks = Mocks.create();
    var factory =
        CommonUtils.getAutoEmbeddingMaterializedViewManagerFactory(
            mocks.dataPath,
            mocks.autoEmbeddingMaterializedViewConfig,
            mocks.initialSyncConfig,
            mocks.featureFlags,
            mocks.mongotCursorManager,
            MeterAndFtdcRegistry.create(mocks.meterRegistry, mocks.ftdcRegistry),
            DefaultConfigManager.ReplicationMode.DISK_UTILIZATION_BASED,
            mocks.embeddingServiceManagerSupplier,
            /* isLeader= */ true,
            mocks.leaseManager);
    var noOpManager = factory.create(Optional.empty());
    Assert.assertTrue(noOpManager.isEmpty());

    // Verify that DISK_UTILIZATION_BASED is not supported yet.
    Exception exception =
        Assert.assertThrows(
            Exception.class, () -> factory.create(Optional.of(mock(SyncSourceConfig.class))));
    String errorMessage = exception.getMessage();
    String expectedPattern =
        "Materialized View Manager doesn't support disk utilization based processing";
    // Assert that the message matches the regex pattern
    Assert.assertTrue(Pattern.compile(expectedPattern).matcher(errorMessage).find());
  }
}
