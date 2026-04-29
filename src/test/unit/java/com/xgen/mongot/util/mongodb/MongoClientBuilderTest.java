package com.xgen.mongot.util.mongodb;

import static com.xgen.mongot.util.mongodb.MongoClientBuilder.TCNATIVE_PLATFORMS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.connection.ClusterConnectionMode;
import com.xgen.mongot.metrics.ServerStatusDataExtractor;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

public class MongoClientBuilderTest {

  private static final Tags replicationTags =
      Tags.of(ServerStatusDataExtractor.Scope.REPLICATION.getTag());

  @Test
  public void testConnectionStringDoesNotSupersedeParametersImplicitlySetByTheBuilder() {
    var connectionString =
        new ConnectionString(
            "mongodb://localhost:11111/?retryWrites=true&readConcernLevel=available"
                + "&serverSelectionTimeoutMS=12345678");
    var settings =
        MongoClientBuilder.builder(connectionString, new SimpleMeterRegistry())
            .buildSettings(ReadConcern.MAJORITY);

    Assert.assertFalse(settings.getRetryWrites());
    Assert.assertEquals(ReadConcern.MAJORITY, settings.getReadConcern());
    Assert.assertEquals(
        10_000, settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testBuilderDoesNotOverrideRetryWritesForClusterConnectionString() {
    List.of(
            new ConnectionString(
                "mongodb://localhost:11111,localhost2:11111/?retryWrites=true"
                    + "&readConcernLevel=available"
                    + "&readPreference=secondary"
                    + "&serverSelectionTimeoutMS=12345678"),
            new ConnectionString(
                "mongodb://localhost:11111,localhost2:11111/?readConcernLevel=available"
                    + "&readPreference=secondary"
                    + "&serverSelectionTimeoutMS=12345678"),
            new ConnectionString(
                "mongodb://localhost:11111/?retryWrites=true"
                    + "&readConcernLevel=available"
                    + "&readPreference=secondary"
                    + "&serverSelectionTimeoutMS=12345678"),
            new ConnectionString(
                "mongodb://localhost:11111/?readConcernLevel=available"
                    + "&readPreference=secondary"
                    + "&serverSelectionTimeoutMS=12345678"))
        .forEach(
            connectionString -> {
              var settings =
                  MongoClientBuilder.builder(connectionString, new SimpleMeterRegistry())
                      .buildSettings(ReadConcern.MAJORITY);

              Assert.assertTrue(settings.getRetryWrites());
              Assert.assertEquals(ReadConcern.MAJORITY, settings.getReadConcern());
              Assert.assertEquals(
                  10_000,
                  settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS));
              if (connectionString.getHosts().size() == 1
                  && Objects.equals(connectionString.getHosts().get(0), "localhost:11111")) {
                Assert.assertEquals(
                    ClusterConnectionMode.SINGLE, settings.getClusterSettings().getMode());
              } else {
                Assert.assertEquals(
                    ClusterConnectionMode.MULTIPLE, settings.getClusterSettings().getMode());
              }

              Assert.assertNotNull(settings.getReadPreference());
              Assert.assertEquals(ReadPreference.secondary(), settings.getReadPreference());
            });
  }

  @Test
  public void testBuilderDoesNotOverrideCompressorsForClusterConnectionString() {
    var connectionString =
        new ConnectionString(
            "mongodb://localhost:11111,localhost2:11111/?"
                + "readConcernLevel=available"
                + "&readPreference=secondary"
                + "&compressors=snappy,zstd");

    var settings =
        MongoClientBuilder.builder(connectionString, new SimpleMeterRegistry())
            .buildSettings(ReadConcern.MAJORITY);

    Assert.assertEquals(
        List.of(MongoCompressor.createSnappyCompressor(), MongoCompressor.createZstdCompressor()),
        settings.getCompressorList());
  }

  @Test
  public void testConnectionStringDoesNotSupersedeExplicitlySetParameters() {
    var connectionString =
        new ConnectionString("mongodb://localhost:11111/?appName=mongodb&maxPoolSize=123456");
    var settings =
        MongoClientBuilder.builder(connectionString, new SimpleMeterRegistry())
            .maxConnections(5)
            .description("initial sync")
            .buildSettings(ReadConcern.MAJORITY);

    Assert.assertEquals(5, settings.getConnectionPoolSettings().getMaxSize());
    Assert.assertTrue(settings.getApplicationName().contains("initial sync"));
  }

  @Test
  public void testConnectionStringParametersPreservedWhenNotOverriddenImplicitlyOrExplicitly() {
    var connectionString =
        new ConnectionString("mongodb://localhost:11111/?appName=myAppName&maxPoolSize=123456");

    var settings =
        MongoClientBuilder.builder(connectionString, new SimpleMeterRegistry())
            .buildSettings(ReadConcern.MAJORITY);

    Assert.assertEquals(123456, settings.getConnectionPoolSettings().getMaxSize());
    Assert.assertEquals("myAppName", settings.getApplicationName());
  }

  @Test
  public void testDynamicLinking() throws SslDynamicLinkingException {
    Assume.assumeTrue(TCNATIVE_PLATFORMS.contains(System.getProperty("os.arch")));
    var mock = Mockito.mock(SslContextFactory.class);
    var externalSslContextMock = Mockito.mock(io.netty.handler.ssl.SslContext.class);
    var meterRegistry = new SimpleMeterRegistry();
    doReturn(true).when(externalSslContextMock).isClient();
    doReturn(externalSslContextMock).when(mock).get();
    var connectionString = new ConnectionString("mongodb://localhost:11111/?tls=true");
    var builder =
        MongoClientBuilder.builder(connectionString, meterRegistry).sslContextFactory(mock);
    builder.buildSyncClient();
    Assert.assertEquals(
        1.0,
        meterRegistry
            .counter("mongoClientBuilder.successfulOpenSSLDynamicLinking", replicationTags)
            .count(),
        0);
    Assert.assertEquals(
        0.0,
        meterRegistry
            .counter("mongoClientBuilder.failedOpenSSLDynamicLinking", replicationTags)
            .count(),
        0);
    Assert.assertNotNull(builder.buildSettings(ReadConcern.MAJORITY).getStreamFactoryFactory());
  }

  @Test
  public void testFallbackHappensWithoutErrorsWhenDynamicLinkingFailed()
      throws SslDynamicLinkingException {
    Assume.assumeTrue(TCNATIVE_PLATFORMS.contains(System.getProperty("os.arch")));
    var mock = Mockito.mock(SslContextFactory.class);
    var meterRegistry = new SimpleMeterRegistry();
    doThrow(new SslDynamicLinkingException()).when(mock).get();
    var connectionString = new ConnectionString("mongodb://localhost:11111/?tls=true");
    var builder =
        MongoClientBuilder.builder(connectionString, meterRegistry).sslContextFactory(mock);
    builder.buildSyncClient();
    Assert.assertEquals(
        0.0,
        meterRegistry
            .counter("mongoClientBuilder.successfulOpenSSLDynamicLinking", replicationTags)
            .count(),
        0);
    Assert.assertEquals(
        1.0,
        meterRegistry
            .counter("mongoClientBuilder.failedOpenSSLDynamicLinking", replicationTags)
            .count(),
        0);
    Assert.assertNull(builder.buildSettings(ReadConcern.MAJORITY).getStreamFactoryFactory());
  }

  @Test
  public void testBuilderCreatesConnectionPoolMetrics() {
    var connectionString = new ConnectionString("mongodb://localhost:11111/");
    var meterRegistry = new SimpleMeterRegistry();

    MongoClientBuilder.builder(connectionString, meterRegistry).buildSettings(false);

    assertTrue(
        meterRegistry
            .getMetersAsString()
            .contains(InstrumentedConnectionPoolListenerFactory.METRIC_NAMESPACE));
  }

  @Test
  public void testBuildNonReplicationWithDefaults() {
    String uri = "mongodb://localhost:11111";
    var connectionInfo = new ConnectionInfo(new ConnectionString(uri));
    String applicationName = "testApp";
    MongoClient client =
        MongoClientBuilder.buildNonReplicationWithDefaults(
            connectionInfo, applicationName, new SimpleMeterRegistry());

    MongoClientSettings settings = ((MongoClientImpl) client).getSettings();

    assertEquals(String.format("mongot %s", applicationName), settings.getApplicationName());
    assertEquals(
        MongoClientBuilder.Defaults.SOCKET_TIMEOUT_MS,
        settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
    assertEquals(
        MongoClientBuilder.Defaults.SINGLE_THREAD_MAX_CONNECTIONS,
        settings.getConnectionPoolSettings().getMaxSize());
    assertEquals(10, settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.SECONDS));

    client.close();
  }

  @Test
  public void testBuildNonReplicationWithDefaultsWithCustomMaxConnections() {
    String uri = "mongodb://localhost:11111";
    ConnectionString connectionString = new ConnectionString(uri);
    String applicationName = "testApp";
    int maxConnections = 10;
    MongoClient client =
        MongoClientBuilder.buildNonReplicationWithDefaults(
            connectionString,
            applicationName,
            maxConnections,
            Optional.empty(),
            new SimpleMeterRegistry());

    MongoClientSettings settings = ((MongoClientImpl) client).getSettings();

    assertEquals(String.format("mongot %s", applicationName), settings.getApplicationName());
    assertEquals(maxConnections, settings.getConnectionPoolSettings().getMaxSize());

    client.close();
  }

  @Test
  public void testServerSelectionTimeoutDefaultsTo10Seconds() {
    var connectionString =
        new ConnectionString("mongodb://localhost:11111/?serverSelectionTimeoutMS=12345678");
    var settings =
        MongoClientBuilder.builder(connectionString, new SimpleMeterRegistry())
            .buildSettings(false);

    Assert.assertEquals(
        10_000, settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testServerSelectionTimeoutIsConfigurable() {
    var connectionString = new ConnectionString("mongodb://localhost:11111/");
    var settings =
        MongoClientBuilder.builder(connectionString, new SimpleMeterRegistry())
            .serverSelectionTimeoutMs(5_000)
            .buildSettings(false);

    Assert.assertEquals(
        5_000, settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testHeartbeatFrequencySettings() {
    var connectionString = new ConnectionString("mongodb://localhost:11111/");
    var settings =
        MongoClientBuilder.builder(connectionString, new SimpleMeterRegistry())
            .heartbeatFrequencyMs(5_000)
            .minHeartbeatFrequencyMs(500)
            .buildSettings(false);

    Assert.assertEquals(
        5_000, settings.getServerSettings().getHeartbeatFrequency(TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        500, settings.getServerSettings().getMinHeartbeatFrequency(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testLocalThresholdSetting() {
    var connectionString = new ConnectionString("mongodb://localhost:11111/");
    var settings =
        MongoClientBuilder.builder(connectionString, new SimpleMeterRegistry())
            .localThresholdMs(100)
            .buildSettings(false);

    Assert.assertEquals(
        100, settings.getClusterSettings().getLocalThreshold(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testBuildNonReplicationPreferringMongos() throws Exception {
    var syncSource =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfo("mongodb://mongod:11111"))
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfo("mongodb://mongod:11111"))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfo("mongodb://mongod:11111"))
            .mongosSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfo("mongodb://mongos:11111"))
            .mongosClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfo("mongodb://mongos:11111"))
            .isSharded(true)
            .build();
    String applicationName = "testApp";
    MongoClient client =
        MongoClientBuilder.buildNonReplicationPreferringMongos(
            syncSource, applicationName, new SimpleMeterRegistry());

    MongoClientSettings settings = ((MongoClientImpl) client).getSettings();

    assertTrue(
        settings.getClusterSettings().getHosts().stream()
            .anyMatch(server -> server.getHost().equals("mongos")));
    assertEquals(String.format("mongot %s", applicationName), settings.getApplicationName());

    client.close();
  }
}
