package com.xgen.mongot.util.mongodb;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.netty.NettyStreamFactoryFactory;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor.ReplicationMeterData.MongodbClientMeterData;
import com.xgen.mongot.metrics.ServerStatusDataExtractor.Scope;
import com.xgen.mongot.util.Check;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.netty.handler.ssl.OpenSsl;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoClientBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(MongoClientBuilder.class);
  private static final Tags METRIC_TAGS = Tags.of(Scope.REPLICATION.getTag());
  public static final Set<String> TCNATIVE_PLATFORMS = Set.of("amd64", "x86_64");

  private final ConnectionString connectionString;
  private final Counter successfulDynamicLinkingCounter;
  private final Counter failedDynamicLinkingCounter;
  private final InstrumentedConnectionPoolListenerFactory connectionPoolListenerFactory;
  private Optional<SslContextFactory> sslContextFactory;
  private Optional<String> applicationName;
  private Optional<Integer> maxConnections;
  private Optional<Integer> socketTimeoutMs;
  private Optional<SSLContext> sslContext;
  private Optional<ReadConcern> readConcern;
  private Optional<Long> heartbeatFrequencyMs;
  private Optional<Long> minHeartbeatFrequencyMs;
  private Optional<Long> localThresholdMs;
  private Optional<Long> serverSelectionTimeoutMs;

  private MongoClientBuilder(
      ConnectionString connectionString,
      String metricsNamespacePrefix,
      MeterRegistry meterRegistry) {
    this.connectionString = connectionString;
    MetricsFactory metricsFactory = new MetricsFactory("mongoClientBuilder", meterRegistry);
    this.successfulDynamicLinkingCounter =
        metricsFactory.counter(MongodbClientMeterData.SUCCESSFUL_DYNAMIC_LINKING, METRIC_TAGS);
    this.failedDynamicLinkingCounter =
        metricsFactory.counter(MongodbClientMeterData.FAILED_DYNAMIC_LINKING, METRIC_TAGS);
    this.connectionPoolListenerFactory =
        new InstrumentedConnectionPoolListenerFactory(metricsNamespacePrefix, meterRegistry);
    this.sslContextFactory = Optional.empty();
    this.applicationName = Optional.empty();
    this.maxConnections = Optional.empty();
    this.socketTimeoutMs = Optional.empty();
    this.sslContext = Optional.empty();
    this.readConcern = Optional.empty();
    this.heartbeatFrequencyMs = Optional.empty();
    this.minHeartbeatFrequencyMs = Optional.empty();
    this.localThresholdMs = Optional.empty();
    this.serverSelectionTimeoutMs = Optional.empty();
  }

  public static MongoClientBuilder builder(
      ConnectionString connectionString, MeterRegistry meterRegistry) {
    return new MongoClientBuilder(connectionString, "", meterRegistry);
  }

  public static MongoClientBuilder builder(
      ConnectionString connectionString,
      String metricsNamespacePrefix,
      MeterRegistry meterRegistry) {
    return new MongoClientBuilder(connectionString, metricsNamespacePrefix, meterRegistry);
  }

  /**
   * Builds a non-replication {@link MongoClient} for cluster-wide read/write operations (e.g.
   * index-management commands, auto-embedding, metadata service).
   *
   * <p>In sharded deployments uses {@link SyncSourceConfig#mongosClusterReadWriteUri} ({@code
   * directConnection=false} to all mongos instances) so the driver can route to any available
   * mongos. In non-sharded deployments falls back to {@link
   * SyncSourceConfig#mongodClusterReadWriteUri} so the driver can discover the primary via
   * replica-set discovery.
   *
   * <p>Prefer this over {@link #buildNonReplicationPreferringMongos} for any operation that must
   * reach the correct primary or needs full cluster routing rather than a single direct host.
   */
  public static MongoClient buildClusterReadWriteClient(
      SyncSourceConfig syncSourceConfig, String applicationName, MeterRegistry meterRegistry) {
    var syncSource =
        syncSourceConfig.mongosClusterReadWriteUri.orElse(
            syncSourceConfig.mongodClusterReadWriteUri);
    return buildNonReplicationWithDefaults(syncSource, applicationName, meterRegistry);
  }

  /**
   * Builds a non-replication {@link MongoClient} that routes through a single mongos when in a
   * sharded deployment, otherwise connects directly to a single mongod (e.g. for server-info
   * resolution, collection metadata reads).
   *
   * <p>Uses {@link SyncSourceConfig#mongosSingleHostReplicationUri} ({@code directConnection=true}
   * to the selected mongos) when present, otherwise falls back to {@link
   * SyncSourceConfig#mongodSingleHostReplicationUri} ({@code directConnection=true} to the selected
   * mongod).
   *
   * <p>Not suitable for writes or operations that require reaching the primary; use {@link
   * #buildClusterReadWriteClient} for those.
   */
  public static MongoClient buildNonReplicationPreferringMongos(
      SyncSourceConfig syncSourceConfig, String applicationName, MeterRegistry meterRegistry) {
    syncSourceConfig.validateReplicationUrisAvailable();

    var syncSource =
        syncSourceConfig
            .mongosSingleHostReplicationUri
            .or(() -> syncSourceConfig.mongodSingleHostReplicationUri)
            .orElseThrow();
    return buildNonReplicationWithDefaults(syncSource, applicationName, meterRegistry);
  }

  /**
   * Convenience method to create a non-replication MongoClient with sensible defaults for {@code
   * maxConnections} and {@code socketTimeoutMs}.
   */
  public static MongoClient buildNonReplicationWithDefaults(
      ConnectionInfo connectionInfo, String applicationName, MeterRegistry meterRegistry) {
    return buildNonReplicationWithDefaults(
        connectionInfo.uri(),
        applicationName,
        Defaults.SINGLE_THREAD_MAX_CONNECTIONS,
        connectionInfo.sslContext(),
        meterRegistry);
  }

  /**
   * Convenience method to create a non-replication MongoClient with sensible defaults for {@code
   * socketTimeoutMs}
   */
  public static MongoClient buildNonReplicationWithDefaults(
      ConnectionString connectionString,
      String applicationName,
      int maxConnections,
      Optional<SSLContext> sslContext,
      MeterRegistry meterRegistry) {

    return builder(connectionString, meterRegistry)
        .sslContext(sslContext)
        .description(applicationName)
        .maxConnections(maxConnections)
        .socketTimeoutMs(Defaults.SOCKET_TIMEOUT_MS)
        .buildNonReplicationClient();
  }

  public MongoClientBuilder sslContext(Optional<SSLContext> sslContext) {
    this.sslContext = sslContext;
    return this;
  }

  public MongoClientBuilder description(String description) {
    this.applicationName = Optional.of(String.format("mongot %s", description));
    return this;
  }

  public MongoClientBuilder maxConnections(int maxConnections) {
    this.maxConnections = Optional.of(maxConnections);
    return this;
  }

  public MongoClientBuilder socketTimeoutMs(int timeout) {
    this.socketTimeoutMs = Optional.of(timeout);
    return this;
  }

  public MongoClientBuilder sslContextFactory(SslContextFactory sslContextFactory) {
    this.sslContextFactory = Optional.of(sslContextFactory);
    return this;
  }

  public MongoClientBuilder readConcern(ReadConcern readConcern) {
    this.readConcern = Optional.of(readConcern);
    return this;
  }

  public MongoClientBuilder heartbeatFrequencyMs(long ms) {
    this.heartbeatFrequencyMs = Optional.of(ms);
    return this;
  }

  public MongoClientBuilder minHeartbeatFrequencyMs(long ms) {
    this.minHeartbeatFrequencyMs = Optional.of(ms);
    return this;
  }

  public MongoClientBuilder localThresholdMs(long ms) {
    this.localThresholdMs = Optional.of(ms);
    return this;
  }

  public MongoClientBuilder serverSelectionTimeoutMs(long ms) {
    this.serverSelectionTimeoutMs = Optional.of(ms);
    return this;
  }

  public MongoClient buildSyncClientForTesting(ReadConcern readConcern) {
    return MongoClients.create(buildSettings(readConcern));
  }

  public MongoClient buildSyncClient() {
    return MongoClients.create(buildSettings(ReadConcern.MAJORITY));
  }

  public BatchMongoClient buildSyncBatchClient() {
    return new BatchMongoClient(buildSettings(ReadConcern.MAJORITY));
  }

  // Non-replication builder.
  public MongoClient buildNonReplicationClient() {
    return MongoClients.create(buildSettings(false));
  }

  @VisibleForTesting
  MongoClientSettings buildSettings(ReadConcern readConcern) {
    this.readConcern(readConcern);
    return buildSettings(true);
  }

  @VisibleForTesting
  MongoClientSettings buildSettings(boolean replicationClient) {
    var settings = MongoClientSettings.builder().applyConnectionString(this.connectionString);

    settings.applyToClusterSettings(
        builder -> {
          builder.serverSelectionTimeout(
              // Default to 10 s (down from the 30 s driver default) so commands fail fast
              // when mongod/mongos is unavailable at call time.
              this.serverSelectionTimeoutMs.orElse(10_000L), TimeUnit.MILLISECONDS);
          this.localThresholdMs.ifPresent(ms -> builder.localThreshold(ms, TimeUnit.MILLISECONDS));
        });

    if (this.heartbeatFrequencyMs.isPresent() || this.minHeartbeatFrequencyMs.isPresent()) {
      settings.applyToServerSettings(
          b -> {
            this.heartbeatFrequencyMs.ifPresent(
                ms -> b.heartbeatFrequency(ms, TimeUnit.MILLISECONDS));
            this.minHeartbeatFrequencyMs.ifPresent(
                ms -> b.minHeartbeatFrequency(ms, TimeUnit.MILLISECONDS));
          });
    }

    if (replicationClient) {
      // Settings for replication clients.
      boolean isDirectConnectionOrSingleHost = validateConnectionString(this.connectionString);
      settings.applyToClusterSettings(
          builder -> {
            if (isDirectConnectionOrSingleHost) {
              // Connect directly to the server rather than trying to connect as a replica
              // set.
              builder.mode(ClusterConnectionMode.SINGLE);
            }
          });

      if (this.connectionString.getReadPreference() == null) {
        // Can't retry writes on the local database, so set this to false. Only override connection
        // string value for direct connection.
        settings.retryWrites(false);
      }
    }

    if (this.socketTimeoutMs.isPresent()) {
      settings.applyToSocketSettings(
          builder -> {
            // do not override the timeout if it was specified in the connection string
            var timeout =
                Optional.ofNullable(this.connectionString.getSocketTimeout())
                    .orElse(this.socketTimeoutMs.get());
            builder.readTimeout(timeout, TimeUnit.MILLISECONDS);
          });
    }

    if (Boolean.TRUE.equals(this.connectionString.getSslEnabled())) {
      attemptOpenSslDynamicLinking(settings);
    }

    if (this.sslContext.isPresent()) {
      settings.applyToSslSettings(
          builder -> {
            builder.enabled(true);
            builder.context(this.sslContext.get());
          });
    }

    this.readConcern.ifPresent(settings::readConcern);
    this.applicationName.ifPresent(settings::applicationName);
    this.maxConnections.ifPresent(
        size -> settings.applyToConnectionPoolSettings(builder -> builder.maxSize(size)));

    settings.applyToConnectionPoolSettings(
        builder ->
            builder.addConnectionPoolListener(
                this.connectionPoolListenerFactory.createFor(
                    this.applicationName.orElse("unnamed"))));

    return settings.build();
  }

  private void attemptOpenSslDynamicLinking(MongoClientSettings.Builder settings) {
    var platform = System.getProperty("os.arch");
    if (!TCNATIVE_PLATFORMS.contains(platform)) {
      LOG.atInfo()
          .addKeyValue("platform", platform)
          .addKeyValue("applicationName", this.applicationName)
          .log("Skipping OpenSSL linking on an unsupported platform");
      return;
    }
    try {
      settings.streamFactoryFactory(
          NettyStreamFactoryFactory.builder()
              .sslContext(this.sslContextFactory.orElse(new SslContextFactory()).get())
              .build());
      this.successfulDynamicLinkingCounter.increment();
      LOG.atInfo()
          .addKeyValue("openSslVersion", OpenSsl.versionString())
          .addKeyValue("applicationName", this.applicationName)
          .log("Dynamically linked to OpenSSL version");
    } catch (SslDynamicLinkingException e) {
      this.failedDynamicLinkingCounter.increment();
      LOG.error("netty-tcnative dynamic linking failed: ", e);
    }
  }

  private boolean validateConnectionString(ConnectionString connectionString) {
    boolean isDirectConnection =
        Optional.ofNullable(connectionString.isDirectConnection()).orElse(false);
    boolean isSingleHost = connectionString.getHosts().size() == 1;
    boolean isSrvProtocol = connectionString.isSrvProtocol();

    if (connectionString.getReadPreference() == null) {
      Check.checkState(
          isDirectConnection || isSingleHost,
          "Connection string must be direct connection or for single host when there "
              + "is no readPreference");
    } else {
      checkArg(
          !isDirectConnection,
          "Connection string must not be direct connection when readPreference exists");
    }
    // Connection mode must be MULTIPLE if SRV host name provided
    return !isSrvProtocol && (isDirectConnection || isSingleHost);
  }

  @VisibleForTesting
  static class Defaults {
    /**
     * Each MongoClient <a
     * href="https://web.archive.org/web/20241210082513/https://www.mongodb.com/docs/drivers/java/sync/current/faq/#how-does-connection-pooling-work-in-the-java-driver-">opens
     * 2 additional sockets per server</a>. At most, a 7-node replica set will have 14 connections
     * plus 1 connection for the single thread making the calls. This is under 20.
     */
    static final int SINGLE_THREAD_MAX_CONNECTIONS = 20;

    static final int SOCKET_TIMEOUT_MS = 10_000;
  }
}
