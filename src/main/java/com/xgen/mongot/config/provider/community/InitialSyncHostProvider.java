package com.xgen.mongot.config.provider.community;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.mongodb.ConnectionInfo;
import com.xgen.mongot.util.mongodb.MongoClientBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the logic for discovering healthy hosts in the replica-set and selecting an initial-sync
 * host based on the {@code replicationReader} preferences.
 *
 * <p>Uses the MongoDB driver's Server Discovery and Monitoring (SDAM) process via a pair of
 * long-lived {@link MongoClient} instances — one targeting the mongod replica-set, one targeting
 * the mongos cluster (if sharded) — to continuously track host health. When a host is requested the
 * provider selects from the set of currently healthy hosts matching the configured read preference.
 *
 * <p>The provider performs an initial round of topology discovery in a background thread before
 * exposing any hosts. While discovery is in progress, {@link #getMongodInitialSyncConnection()} and
 * {@link #getMongosInitialSyncConnection()} return {@link Optional#empty()}.
 */
public class InitialSyncHostProvider implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(InitialSyncHostProvider.class);

  // Read preference used to filter candidate mongod hosts.
  private final ReadPreference replicationReadPreference;

  // Raw community config for building direct-connect URIs with credentials.
  private final MongoConnectionConfig replicaSetConfig;
  private final Optional<RouterConfig> routerConfig;
  private final Optional<Path> caFile;

  // Long-lived SDAM clients that continuously heartbeat to mongod/mongos.
  private final MongoClient mongodClient;
  private final Optional<MongoClient> mongosClient;

  // Set to true once initial topology discovery has completed (or timed out).
  private final AtomicBoolean isReady = new AtomicBoolean(false);

  /**
   * Creates an {@code InitialSyncHostProvider} and starts background topology discovery.
   *
   * <p>Topology discovery runs asynchronously; callers may start using {@link
   * #getMongodInitialSyncConnection()} immediately, but it will return empty until {@link
   * #isReady()} returns {@code true}.
   *
   * @param communitySyncSourceConfig community config containing the replica-set and router
   *     settings used both to build direct-connect URIs and to create SDAM heartbeat clients
   */
  public InitialSyncHostProvider(
      SyncSourceConfig communitySyncSourceConfig, MeterRegistry meterRegistry) {
    this.replicaSetConfig = communitySyncSourceConfig.replicaSet();
    this.routerConfig = communitySyncSourceConfig.router();
    this.caFile = communitySyncSourceConfig.caFile();
    this.replicationReadPreference = communitySyncSourceConfig.getReplicationReaderReadPreference();

    this.mongodClient =
        createSdamClient(
            ConnectionInfoFactory.getClusterConnectionInfo(
                this.replicaSetConfig, this.replicationReadPreference, this.caFile),
            meterRegistry);
    this.mongosClient =
        this.routerConfig.map(
            router ->
                createSdamClient(
                    ConnectionInfoFactory.getClusterConnectionInfo(
                        router, this.replicationReadPreference, this.caFile),
                    meterRegistry));

    startDiscovery(meterRegistry);
  }

  @VisibleForTesting
  InitialSyncHostProvider(
      SyncSourceConfig config,
      MongoClient mongodClient,
      Optional<MongoClient> mongosClient,
      boolean ready) {
    this.replicaSetConfig = config.replicaSet();
    this.routerConfig = config.router();
    this.caFile = config.caFile();
    this.replicationReadPreference = config.getReplicationReaderReadPreference();
    this.mongodClient = mongodClient;
    this.mongosClient = mongosClient;
    this.isReady.set(ready);
  }

  /**
   * Returns a direct-connect {@link ConnectionInfo} targeting a healthy mongod selected from the
   * replica-set according to the configured read preference.
   *
   * @return the selected host, or empty if topology discovery has not yet completed or no healthy
   *     host matches the configured read preference
   */
  public Optional<ConnectionInfo> getMongodInitialSyncConnection() {
    return getInitialSyncConnection(this.mongodClient, this.replicaSetConfig, Optional.empty());
  }

  /**
   * Returns a direct-connect {@link ConnectionInfo} targeting a healthy mongos selected from the
   * mongos cluster, or empty in a non-sharded deployment.
   *
   * <p>The resulting connection string carries the replicationReader read preference so that mongos
   * can route requests to the appropriate shards.
   *
   * @return the selected host, or empty if topology discovery has not yet completed, no healthy
   *     mongos is available, or this is a non-sharded deployment
   */
  public Optional<ConnectionInfo> getMongosInitialSyncConnection() {
    return this.mongosClient.flatMap(
        client ->
            this.routerConfig.flatMap(
                config ->
                    getInitialSyncConnection(
                        client, config, Optional.of(this.replicationReadPreference))));
  }

  /**
   * Returns {@code true} if {@code selected} is still present in the set of healthy mongod hosts
   * matching the configured read preference.
   */
  public boolean isMongodHostStillValid(ServerAddress selected) {
    return isHostStillValid(selected, this.mongodClient, this.replicationReadPreference);
  }

  /**
   * Returns {@code true} if {@code selected} is still present in the set of healthy mongos hosts.
   * Returns {@code false} if this is a non-sharded deployment.
   */
  public boolean isMongosHostStillValid(ServerAddress selected) {
    return this.mongosClient
        .map(client -> isHostStillValid(selected, client, this.replicationReadPreference))
        .orElse(false);
  }

  /**
   * Returns {@code true} once the initial round of topology discovery has completed (or timed out)
   * for all configured clients — both the mongod replica-set client and, in a sharded deployment,
   * the mongos client. Host selection is blocked until this returns {@code true}.
   */
  public boolean isReady() {
    return this.isReady.get();
  }

  @Override
  public void close() {
    this.mongodClient.close();
    this.mongosClient.ifPresent(MongoClient::close);
  }

  /**
   * Selects a healthy host from {@code mongoClient}'s current topology view and returns a
   * direct-connect {@link ConnectionInfo} for it.
   *
   * <p>Returns {@link Optional#empty()} if topology discovery has not yet completed or if no
   * healthy host matches {@link #replicationReadPreference}.
   *
   * @param mongoClient the SDAM client whose topology view is used for host selection
   * @param config the connection config used to build the direct-connect URI
   * @param uriReadPref when present, embeds the read preference in the connection-string URI so
   *     that the router (mongos) can forward it to the underlying shards. Empty for direct mongod
   *     connections, where host selection has already happened and no routing is needed.
   * @return a direct-connect {@link ConnectionInfo} for the selected host, or empty if topology
   *     discovery has not yet completed or no healthy host matches the configured read preference
   */
  private Optional<ConnectionInfo> getInitialSyncConnection(
      MongoClient mongoClient, MongoConnectionConfig config, Optional<ReadPreference> uriReadPref) {
    if (!this.isReady.get()) {
      return Optional.empty();
    }
    List<ServerDescription> hosts =
        getHealthyHostsMatchingReadPref(mongoClient, this.replicationReadPreference);
    if (hosts.isEmpty()) {
      return Optional.empty();
    }
    ServerDescription selected = selectFromHosts(hosts);
    HostAndPort hostAndPort =
        HostAndPort.fromParts(selected.getAddress().getHost(), selected.getAddress().getPort());

    LOG.atInfo().addKeyValue("hostAndPort", hostAndPort).log("Selected initial sync host");

    // For mongos, embed the replicationReader read preference in the URI so mongos can route
    // requests to the correct shards. For direct mongod connections no URI read preference is
    // needed — we have already selected a specific host and connect to it directly.
    return Optional.of(
        uriReadPref.isPresent()
            ? ConnectionInfoFactory.getSingleHostConnectionInfo(
                config, hostAndPort, this.caFile, uriReadPref.get())
            : ConnectionInfoFactory.getSingleHostConnectionInfo(config, hostAndPort, this.caFile));
  }

  private boolean isHostStillValid(
      ServerAddress selected, MongoClient mongoClient, ReadPreference readPref) {
    List<ServerDescription> hosts = getHealthyHostsMatchingReadPref(mongoClient, readPref);
    return hosts.stream().anyMatch(sd -> sd.getAddress().equals(selected));
  }

  private List<ServerDescription> getHealthyHostsMatchingReadPref(
      MongoClient mongoClient, ReadPreference readPref) {
    ClusterDescription desc = mongoClient.getClusterDescription();
    return readPref.choose(desc);
  }

  /**
   * Selects a host from the list returned by {@link #getHealthyHostsMatchingReadPref}.
   *
   * <p>For {@code NEAREST}, returns the host with the lowest measured round-trip time. For all
   * other preferences, a uniformly random host is chosen to distribute initial-sync load across the
   * replica-set.
   */
  private ServerDescription selectFromHosts(List<ServerDescription> hosts) {
    if (this.replicationReadPreference
        .getName()
        .equalsIgnoreCase(ReadPreference.nearest().getName())) {
      return hosts.stream()
          .min(Comparator.comparingLong(ServerDescription::getRoundTripTimeNanos))
          .orElseThrow();
    }
    return hosts.get(ThreadLocalRandom.current().nextInt(hosts.size()));
  }

  /** Calls {@code awaitFullTopologyDiscovery} in an async threadpool and then closes the pool. */
  @VisibleForTesting
  void startDiscovery(MeterRegistry meterRegistry) {
    var discoveryExecutor =
        Executors.fixedSizeThreadPool("initial-sync-host-provider-discovery", 1, meterRegistry);
    discoveryExecutor.submit(this::awaitFullTopologyDiscovery);
    discoveryExecutor.shutdown();
  }

  /**
   * Waits in a background thread until all known hosts in both the mongod and (if sharded) mongos
   * topologies have completed their initial SDAM hello exchange, or until the 10-second deadline
   * expires.
   *
   * <p>This avoids a thundering-herd problem on startup where all mongot instances pick seed-list
   * hosts before the driver has discovered the full topology.
   *
   * <p>The {@link #isReady} flag is always set in the {@code finally} block so host selection can
   * proceed even if discovery times out or is interrupted.
   */
  @VisibleForTesting
  void awaitFullTopologyDiscovery() {
    try {
      Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
      while (Instant.now().isBefore(deadline)) {
        if (isMongodDiscoveryComplete() && isMongosDiscoveryComplete()) {
          LOG.atInfo()
              .addKeyValue(
                  "numMongodHosts",
                  this.mongodClient.getClusterDescription().getServerDescriptions().size())
              .log("Topology discovery complete");
          return;
        }
        Thread.sleep(100);
      }
      LOG.atWarn()
          .log("Timed out waiting for full topology discovery; proceeding with partial view");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.atWarn().log("Interrupted waiting for topology discovery");
    } catch (Exception e) {
      LOG.atWarn().addKeyValue("error", e.getMessage()).log("Error during topology discovery");
    } finally {
      this.isReady.set(true);
    }
  }

  private boolean isMongodDiscoveryComplete() {
    return isDiscoveryComplete(this.mongodClient);
  }

  private boolean isMongosDiscoveryComplete() {
    return this.mongosClient.map(InitialSyncHostProvider::isDiscoveryComplete).orElse(true);
  }

  private static boolean isDiscoveryComplete(MongoClient client) {
    ClusterDescription desc = client.getClusterDescription();
    return !desc.getServerDescriptions().isEmpty()
        && desc.getServerDescriptions().stream()
            .noneMatch(
                sd ->
                    sd.getType() == ServerType.UNKNOWN
                        || sd.getState() == ServerConnectionState.CONNECTING);
  }

  /**
   * Creates a lightweight {@link MongoClient} used solely for SDAM topology monitoring — it never
   * reads or writes application data.
   *
   * <p><b>Heartbeat frequency (5 s, down from the 10 s driver default):</b> halving the interval
   * means topology changes (host failure, elections) are reflected quickly.
   *
   * <p><b>Min-heartbeat frequency (500 ms):</b> matches the driver default but is set explicitly to
   * cap how quickly the driver re-heartbeats after a topology event, preventing a burst of
   * back-to-back hello commands when many hosts change state simultaneously.
   *
   * <p><b>Local threshold (100 ms, up from the 15 ms driver default):</b> widens the eligibility
   * window so that hosts with transiently elevated round-trip times are not unfairly evicted from
   * the candidate pool. Because initial-sync host selection happens once (not on every operation),
   * erring toward inclusion is safer than erring toward exclusion.
   *
   * <p><b>Socket read timeout (10 s):</b> ensures that heartbeats to a silently-unreachable host
   * (TCP blackhole) fail promptly rather than hanging indefinitely, keeping the topology view
   * accurate.
   *
   * <p><b>Max connections (1):</b> this client never issues application-level reads or writes, so a
   * single connection on the monitoring path is sufficient.
   */
  private static MongoClient createSdamClient(
      ConnectionInfo connectionInfo, MeterRegistry meterRegistry) {
    return MongoClientBuilder.builder(connectionInfo.uri(), meterRegistry)
        .description("initial sync host provider")
        .socketTimeoutMs(10_000)
        .heartbeatFrequencyMs(5_000)
        .minHeartbeatFrequencyMs(500)
        .localThresholdMs(100)
        .maxConnections(1)
        .sslContext(connectionInfo.sslContext())
        .buildNonReplicationClient();
  }
}
