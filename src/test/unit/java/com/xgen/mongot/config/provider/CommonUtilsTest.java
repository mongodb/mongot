package com.xgen.mongot.config.provider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.RateLimiter;
import com.mongodb.ConnectionString;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.mongodb.MaterializedViewCollectionResolver;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexFactory;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.mongodb.DurabilityConfig;
import com.xgen.mongot.replication.mongodb.MongoDbNoOpReplicationManager;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
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
    private final MaterializedViewCollectionMetadataCatalog mvMetadataCatalog;

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
      this.mvMetadataCatalog = mock(MaterializedViewCollectionMetadataCatalog.class);
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
  public void testGetMaterializedViewManager_disableMode() {
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
            mocks.leaseManager,
            mocks.mvMetadataCatalog);
    var noOpManager = factory.create(Optional.of(mocks.syncSourceConfig));
    Assert.assertTrue(noOpManager.isEmpty());
  }

  @Test
  public void testGetMaterializedViewManager_enableMode_missingEmbeddingServiceManagerSupplier() {
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
            mocks.leaseManager,
            mocks.mvMetadataCatalog);

    // With empty embeddingServiceManagerSupplier should throw IllegalArgumentException.
    IllegalArgumentException exception =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> factory.create(Optional.of(mock(SyncSourceConfig.class))));
    String errorMessage = exception.getMessage();
    String expectedPattern = "EmbeddingServiceManagerSupplier must be provided";
    Assert.assertTrue(Pattern.compile(expectedPattern).matcher(errorMessage).find());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetMaterializedViewIndexFactory_wiresRateLimitFromConfig() throws Exception {
    var mocks = Mocks.create();
    var collectionResolver = mock(MaterializedViewCollectionResolver.class);

    AutoEmbeddingMaterializedViewConfig configWithRateLimit =
        AutoEmbeddingMaterializedViewConfig.create(
            CommonReplicationConfig.defaultGlobalReplicationConfig(),
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
            Optional.of(100));

    MaterializedViewIndexFactory factory =
        CommonUtils.getMaterializedViewIndexFactory(
            mocks.syncSourceConfig,
            mocks.featureFlags,
            MeterAndFtdcRegistry.create(mocks.meterRegistry, mocks.ftdcRegistry),
            mocks.leaseManager,
            collectionResolver,
            configWithRateLimit);

    Field writerFactoryField =
        MaterializedViewIndexFactory.class.getDeclaredField("materializedViewWriterFactory");
    writerFactoryField.setAccessible(true);
    var writerFactory =
        (MaterializedViewWriter.Factory) writerFactoryField.get(factory);
    Field rateLimiterField =
        MaterializedViewWriter.Factory.class.getDeclaredField("rateLimiter");
    rateLimiterField.setAccessible(true);
    Optional<RateLimiter> rateLimiter =
        (Optional<RateLimiter>) rateLimiterField.get(writerFactory);
    Assert.assertTrue(
        "Factory should have a rate limiter when configured", rateLimiter.isPresent());

    factory.close();

    AutoEmbeddingMaterializedViewConfig configWithoutRateLimit =
        AutoEmbeddingMaterializedViewConfig.getDefault();
    MaterializedViewIndexFactory factoryNoLimit =
        CommonUtils.getMaterializedViewIndexFactory(
            mocks.syncSourceConfig,
            mocks.featureFlags,
            MeterAndFtdcRegistry.create(mocks.meterRegistry, mocks.ftdcRegistry),
            mocks.leaseManager,
            collectionResolver,
            configWithoutRateLimit);

    var writerFactory2 =
        (MaterializedViewWriter.Factory) writerFactoryField.get(factoryNoLimit);
    Optional<RateLimiter> rateLimiter2 =
        (Optional<RateLimiter>) rateLimiterField.get(writerFactory2);
    Assert.assertFalse(
        "Factory should not have a rate limiter when not configured", rateLimiter2.isPresent());

    factoryNoLimit.close();
  }

  @Test
  public void testMongotConfigsGetDefault_withAndWithoutMvConfig() {
    Path dataPath = Path.of("test-data");
    MongotConfigs defaults = MongotConfigs.getDefault(dataPath);
    Assert.assertEquals(
        "Default config should have empty mvWriteRateLimitRps",
        Optional.empty(),
        defaults.autoEmbeddingMaterializedViewConfig.getMvWriteRateLimitRps());

    AutoEmbeddingMaterializedViewConfig customMvConfig =
        AutoEmbeddingMaterializedViewConfig.create(
            CommonReplicationConfig.defaultGlobalReplicationConfig(),
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
            Optional.of(100));

    MongotConfigs withOverride = MongotConfigs.getDefault(dataPath, customMvConfig);
    Assert.assertEquals(
        "Overridden config should have mvWriteRateLimitRps=100",
        Optional.of(100),
        withOverride.autoEmbeddingMaterializedViewConfig.getMvWriteRateLimitRps());

    Assert.assertSame(
        "mvConfig should be the exact instance passed in",
        customMvConfig,
        withOverride.autoEmbeddingMaterializedViewConfig);
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
            mocks.leaseManager,
            mocks.mvMetadataCatalog);
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
