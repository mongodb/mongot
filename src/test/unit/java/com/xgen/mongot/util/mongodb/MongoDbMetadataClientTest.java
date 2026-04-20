package com.xgen.mongot.util.mongodb;

import com.mongodb.client.internal.MongoClientImpl;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class MongoDbMetadataClientTest {

  @Test
  public void testMongodUriUsedWhenMongosUriIsNotPresent() throws Exception {
    var syncSource =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@atlas-mongod:27017/"))
            .mongodClusterReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@atlas-mongod:27017/"))
            .mongodClusterReadWriteUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@atlas-mongod:27017/"))
            .build();
    try (var client =
        MongoClientBuilder.buildNonReplicationPreferringMongos(
            syncSource, this.getClass().getSimpleName(), new SimpleMeterRegistry())) {
      String configuredHost =
          ((MongoClientImpl) client).getSettings().getClusterSettings().getHosts().get(0).getHost();
      Assert.assertTrue(configuredHost.contains("atlas-mongod"));
    }
  }

  @Test
  public void testMongosUriUsedWhenPresent() throws Exception {
    var syncSource =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@atlas-mongod:27017/"))
            .mongodClusterReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@atlas-mongod:27017/"))
            .mongodClusterReadWriteUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@atlas-mongod:27017/"))
            .mongosSingleHostReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@atlas-mongos:27017/"))
            .mongosClusterReadWriteUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@atlas-mongos:27017/"))
            .isSharded(true)
            .build();
    try (var client =
        MongoClientBuilder.buildNonReplicationPreferringMongos(
            syncSource, this.getClass().getSimpleName(), new SimpleMeterRegistry())) {
      String configuredHost =
          ((MongoClientImpl) client).getSettings().getClusterSettings().getHosts().get(0).getHost();
      Assert.assertTrue(configuredHost.contains("atlas-mongos"));
    }
  }

  @Test
  public void testDefaultSocketTimeout() throws Exception {
    var syncSource =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@atlas-mongod:27017/"))
            .mongodClusterReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@atlas-mongod:27017/"))
            .mongodClusterReadWriteUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@atlas-mongod:27017/"))
            .build();
    try (var client =
        MongoClientBuilder.buildNonReplicationPreferringMongos(
            syncSource, this.getClass().getSimpleName(), new SimpleMeterRegistry())) {
      Assert.assertEquals(
          10,
          ((MongoClientImpl) client)
              .getSettings()
              .getSocketSettings()
              .getReadTimeout(TimeUnit.SECONDS));
    }
  }

  @Test
  public void testSocketTimeoutIsNotOverriddenWhenSpecifiedInConnectionString() throws Exception {
    var syncSource =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfo(
                    // kingfisher:ignore
                    "mongodb://user:pass@atlas-mongod:27017/?socketTimeoutMS=30000"))
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfo(
                    // kingfisher:ignore
                    "mongodb://user:pass@atlas-mongod:27017/?socketTimeoutMS=30000"))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfo(
                    // kingfisher:ignore
                    "mongodb://user:pass@atlas-mongod:27017/?socketTimeoutMS=30000"))
            .build();
    try (var client =
        MongoClientBuilder.buildNonReplicationPreferringMongos(
            syncSource, this.getClass().getSimpleName(), new SimpleMeterRegistry())) {
      Assert.assertEquals(
          30,
          ((MongoClientImpl) client)
              .getSettings()
              .getSocketSettings()
              .getReadTimeout(TimeUnit.SECONDS));
    }
  }

  @Test
  public void constructor_mongodUriAbsent_doesNotCreateClients() throws Exception {
    // Regression: before the guard, buildNonReplicationPreferringMongos would throw
    // NoSuchElementException because it called mongodSingleHostReplicationUri.get() eagerly.
    var config =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@mongod:27017/"))
            .mongodClusterReadWriteUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@mongod:27017/"))
            .build();
    try (var client = new MongoDbMetadataClient(Optional.of(config), new SimpleMeterRegistry())) {
      // Clients were not created, so resolveCollectionInfos should throw.
      Assert.assertThrows(
          CheckedMongoException.class, () -> client.resolveCollectionInfos(Set.of("test")));
    }
  }

  @Test
  public void maybeUpdateSyncSource_mongodUriAbsent_closesExistingClientsAndDoesNotCreateNew()
      throws Exception {
    var validConfig =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@mongod:27017/"))
            .mongodClusterReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@mongod:27017/"))
            .mongodClusterReadWriteUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@mongod:27017/"))
            .build();
    var configWithoutMongodUri =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@mongod:27017/"))
            .mongodClusterReadWriteUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@mongod:27017/"))
            .build();

    try (var client =
        new MongoDbMetadataClient(Optional.of(validConfig), new SimpleMeterRegistry())) {
      client.maybeUpdateSyncSource(configWithoutMongodUri);
      // Existing clients were closed and no new ones were created.
      Assert.assertThrows(
          CheckedMongoException.class, () -> client.resolveCollectionInfos(Set.of("test")));
    }
  }

  @Test
  public void maybeUpdateSyncSource_uriBecomesAvailable_recreatesClients() throws Exception {
    var validConfig =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@mongod:27017/"))
            .mongodClusterReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@mongod:27017/"))
            .mongodClusterReadWriteUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@mongod:27017/"))
            .build();
    var configWithoutMongodUri =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@mongod:27017/"))
            .mongodClusterReadWriteUri(
                // kingfisher:ignore
                ConnectionStringUtil.toConnectionInfo("mongodb://user:pass@mongod:27017/"))
            .build();

    try (var client =
        new MongoDbMetadataClient(Optional.of(validConfig), new SimpleMeterRegistry())) {
      // Initial state: clients are present. Querying an empty db set succeeds without connecting.
      client.resolveCollectionInfos(Set.of());

      // Remove URI: clients are closed.
      client.maybeUpdateSyncSource(configWithoutMongodUri);
      Assert.assertThrows(
          CheckedMongoException.class, () -> client.resolveCollectionInfos(Set.of()));

      // URI becomes available: clients are recreated.
      client.maybeUpdateSyncSource(validConfig);
      // resolveCollectionInfos no longer throws "unavailable" — clients are present again.
      client.resolveCollectionInfos(Set.of());
    }
  }

  @Test
  public void testUpdateMongoDbVersionMetric() throws Exception {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MongoDbMetadataClient client = new MongoDbMetadataClient(Optional.empty(), meterRegistry);
    Method updateMethod =
        MongoDbMetadataClient.class.getDeclaredMethod("updateMongoDbVersionMetric");
    updateMethod.setAccessible(true);
    Field serverInfoField = MongoDbMetadataClient.class.getDeclaredField("mongoDbServerInfo");
    serverInfoField.setAccessible(true);

    // Test 1: Initial version (7.0.0)
    MongoDbVersion version700 = new MongoDbVersion(7, 0, 0);
    String version700String = version700.toString();
    serverInfoField.set(client, new MongoDbServerInfo(Optional.of(version700), Optional.empty()));
    updateMethod.invoke(client);

    Gauge gauge700 =
        meterRegistry.find("mongodb.version").tags("version", version700String).gauge();
    Assert.assertNotNull("Gauge for version 7.0.0 should exist", gauge700);
    Assert.assertEquals(1.0, gauge700.value(), 0.0);

    // Test 2: Version change (7.0.0 -> 8.1.0) - previous should be set to 0
    MongoDbVersion version810 = new MongoDbVersion(8, 1, 0);
    String version810String = version810.toString();
    serverInfoField.set(client, new MongoDbServerInfo(Optional.of(version810), Optional.empty()));
    updateMethod.invoke(client);

    Gauge gauge810 =
        meterRegistry.find("mongodb.version").tags("version", version810String).gauge();
    Assert.assertNotNull("Gauge for version 8.1.0 should exist", gauge810);
    Assert.assertEquals(1.0, gauge810.value(), 0.0);

    // Previous version (7.0.0) should now be 0
    Gauge previousGauge700 =
        meterRegistry.find("mongodb.version").tags("version", version700String).gauge();
    Assert.assertNotNull("Previous gauge for version 7.0.0 should still exist", previousGauge700);
    Assert.assertEquals(0.0, previousGauge700.value(), 0.0);

    // Test 3: Unknown version
    serverInfoField.set(client, new MongoDbServerInfo(Optional.empty(), Optional.empty()));
    updateMethod.invoke(client);

    Gauge gaugeUnknown = meterRegistry.find("mongodb.version").tags("version", "unknown").gauge();
    Assert.assertNotNull("Gauge for unknown version should exist", gaugeUnknown);
    Assert.assertEquals(1.0, gaugeUnknown.value(), 0.0);

    // Previous version (8.1.0) should now be 0
    Gauge previousGauge810 =
        meterRegistry.find("mongodb.version").tags("version", version810String).gauge();
    Assert.assertNotNull("Previous gauge for version 8.1.0 should still exist", previousGauge810);
    Assert.assertEquals(0.0, previousGauge810.value(), 0.0);

    client.close();
  }
}
