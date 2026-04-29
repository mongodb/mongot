package com.xgen.mongot.config.provider.community;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.net.HostAndPort;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.xgen.mongot.util.mongodb.ConnectionInfo;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InitialSyncHostProviderTest {

  private static final ServerAddress RS_HOST_1 = new ServerAddress("rs1.local", 27017);
  private static final ServerAddress RS_HOST_2 = new ServerAddress("rs2.local", 27018);
  private static final ServerAddress MONGOS_HOST = new ServerAddress("mongos.local", 27019);

  @Mock private MongoClient mongodClient;
  @Mock private MongoClient mongosClient;

  private Path passwordFile;
  private ReplicaSetConfig replicaSetConfig;
  private RouterConfig routerConfig;

  @Before
  public void setUp() throws IOException {
    this.passwordFile = Files.createTempFile("test-mongot-pass", ".txt");
    Files.writeString(this.passwordFile, "s3cr3t");
    try {
      Files.setPosixFilePermissions(
          this.passwordFile, PosixFilePermissions.fromString("r--------"));
    } catch (UnsupportedOperationException ignored) {
      // do nothing
    }

    this.replicaSetConfig =
        new ReplicaSetConfig(
            List.of(
                HostAndPort.fromParts("rs1.local", 27017),
                HostAndPort.fromParts("rs2.local", 27018)),
            Optional.of("mongot"),
            Optional.of(this.passwordFile),
            "admin",
            false,
            Optional.of(MongoReadPreferenceName.SECONDARY_PREFERRED),
            Optional.empty());

    this.routerConfig =
        new RouterConfig(
            List.of(HostAndPort.fromParts("mongos.local", 27019)),
            Optional.of("mongot"),
            Optional.of(this.passwordFile),
            "admin",
            false,
            Optional.of(MongoReadPreferenceName.SECONDARY_PREFERRED),
            Optional.empty());
  }

  @After
  public void tearDown() throws IOException {
    Files.deleteIfExists(this.passwordFile);
  }

  private SyncSourceConfig nonShardedConfig() {
    return new SyncSourceConfig(
        this.replicaSetConfig, Optional.empty(), Optional.empty(), Optional.empty());
  }

  private SyncSourceConfig shardedConfig() {
    return new SyncSourceConfig(
        this.replicaSetConfig, Optional.of(this.routerConfig), Optional.empty(), Optional.empty());
  }

  private static ClusterDescription replicaSetCluster(List<ServerAddress> addresses) {
    List<ServerDescription> descs =
        addresses.stream()
            .<ServerDescription>map(
                addr ->
                    ServerDescription.builder()
                        .address(addr)
                        .type(ServerType.REPLICA_SET_SECONDARY)
                        .state(ServerConnectionState.CONNECTED)
                        .ok(true)
                        .build())
            .toList();
    return new ClusterDescription(ClusterConnectionMode.MULTIPLE, ClusterType.REPLICA_SET, descs);
  }

  private static ClusterDescription replicaSetClusterOf(List<ServerDescription> descs) {
    return new ClusterDescription(ClusterConnectionMode.MULTIPLE, ClusterType.REPLICA_SET, descs);
  }

  private static ServerDescription secondaryAt(ServerAddress addr, long rttMillis) {
    return ServerDescription.builder()
        .address(addr)
        .type(ServerType.REPLICA_SET_SECONDARY)
        .state(ServerConnectionState.CONNECTED)
        .ok(true)
        .roundTripTime(rttMillis, TimeUnit.MILLISECONDS)
        .build();
  }

  private static ClusterDescription shardedCluster(List<ServerAddress> addresses) {
    List<ServerDescription> descs =
        addresses.stream()
            .<ServerDescription>map(
                addr ->
                    ServerDescription.builder()
                        .address(addr)
                        .type(ServerType.SHARD_ROUTER)
                        .state(ServerConnectionState.CONNECTED)
                        .ok(true)
                        .build())
            .toList();
    return new ClusterDescription(ClusterConnectionMode.MULTIPLE, ClusterType.SHARDED, descs);
  }

  private SyncSourceConfig nearestConfig() {
    var nearestRsConfig =
        new ReplicaSetConfig(
            List.of(
                HostAndPort.fromParts("rs1.local", 27017),
                HostAndPort.fromParts("rs2.local", 27018)),
            Optional.of("mongot"),
            Optional.of(this.passwordFile),
            "admin",
            false,
            Optional.of(MongoReadPreferenceName.NEAREST),
            Optional.empty());
    return new SyncSourceConfig(
        nearestRsConfig, Optional.empty(), Optional.empty(), Optional.empty());
  }

  // ---- getMongodInitialSyncConnection ----

  @Test
  public void getMongodInitialSyncConnection_returnsEmptyWhenNotReady() {
    var provider =
        new InitialSyncHostProvider(nonShardedConfig(), this.mongodClient, Optional.empty(), false);
    assertThat(provider.getMongodInitialSyncConnection()).isEmpty();
  }

  @Test
  public void getMongodInitialSyncConnection_returnsEmptyWhenNoHealthyHosts() {
    when(this.mongodClient.getClusterDescription()).thenReturn(replicaSetCluster(List.of()));
    var provider =
        new InitialSyncHostProvider(nonShardedConfig(), this.mongodClient, Optional.empty(), true);
    assertThat(provider.getMongodInitialSyncConnection()).isEmpty();
  }

  @Test
  public void getMongodInitialSyncConnection_returnsDirectConnectionToSelectedHost() {
    when(this.mongodClient.getClusterDescription())
        .thenReturn(replicaSetCluster(List.of(RS_HOST_1)));
    var provider =
        new InitialSyncHostProvider(nonShardedConfig(), this.mongodClient, Optional.empty(), true);

    Optional<ConnectionInfo> result = provider.getMongodInitialSyncConnection();

    assertThat(result).isPresent();
    assertThat(result.get().uri().getHosts()).containsExactly("rs1.local:27017");
    assertThat(result.get().uri().isDirectConnection()).isTrue();
    // No read preference embedded: mongod host is already selected, routing is unnecessary.
    assertThat(result.get().uri().getReadPreference()).isNull();
  }

  @Test
  public void getMongodInitialSyncConnection_randomlySelectsFromMultipleHealthyHosts() {
    when(this.mongodClient.getClusterDescription())
        .thenReturn(replicaSetCluster(List.of(RS_HOST_1, RS_HOST_2)));
    var provider =
        new InitialSyncHostProvider(nonShardedConfig(), this.mongodClient, Optional.empty(), true);

    var selectedHosts = new HashSet<String>();
    for (int i = 0; i < 200; i++) {
      provider
          .getMongodInitialSyncConnection()
          .ifPresent(ci -> selectedHosts.addAll(ci.uri().getHosts()));
    }
    assertThat(selectedHosts).containsAtLeast("rs1.local:27017", "rs2.local:27018");
  }

  @Test
  public void getMongodInitialSyncConnection_nearestSelectsLowestRttHost() {
    // RS_HOST_2 has the lower RTT and should always be selected.
    when(this.mongodClient.getClusterDescription())
        .thenReturn(
            replicaSetClusterOf(List.of(secondaryAt(RS_HOST_1, 20), secondaryAt(RS_HOST_2, 5))));
    var provider =
        new InitialSyncHostProvider(nearestConfig(), this.mongodClient, Optional.empty(), true);

    var selectedHosts = new HashSet<String>();
    for (int i = 0; i < 50; i++) {
      provider
          .getMongodInitialSyncConnection()
          .ifPresent(ci -> selectedHosts.addAll(ci.uri().getHosts()));
    }
    assertThat(selectedHosts).containsExactly("rs2.local:27018");
  }

  // ---- getMongosInitialSyncConnection ----

  @Test
  public void getMongosInitialSyncConnection_returnsEmptyForNonShardedDeployment() {
    var provider =
        new InitialSyncHostProvider(nonShardedConfig(), this.mongodClient, Optional.empty(), true);
    assertThat(provider.getMongosInitialSyncConnection()).isEmpty();
  }

  @Test
  public void getMongosInitialSyncConnection_returnsEmptyWhenNotReady() {
    var provider =
        new InitialSyncHostProvider(
            shardedConfig(), this.mongodClient, Optional.of(this.mongosClient), false);
    assertThat(provider.getMongosInitialSyncConnection()).isEmpty();
  }

  @Test
  public void getMongosInitialSyncConnection_returnsEmptyWhenNoHealthyMongos() {
    when(this.mongosClient.getClusterDescription()).thenReturn(shardedCluster(List.of()));
    var provider =
        new InitialSyncHostProvider(
            shardedConfig(), this.mongodClient, Optional.of(this.mongosClient), true);
    assertThat(provider.getMongosInitialSyncConnection()).isEmpty();
  }

  @Test
  public void getMongosInitialSyncConnection_embedsReadPreferenceInUri() {
    when(this.mongosClient.getClusterDescription())
        .thenReturn(shardedCluster(List.of(MONGOS_HOST)));
    var provider =
        new InitialSyncHostProvider(
            shardedConfig(), this.mongodClient, Optional.of(this.mongosClient), true);

    Optional<ConnectionInfo> result = provider.getMongosInitialSyncConnection();

    assertThat(result).isPresent();
    assertThat(result.get().uri().getHosts()).containsExactly("mongos.local:27019");
    // directConnection is not set: the driver rejects a URI combining directConnection=true with a
    // read preference. A single-host URI causes SINGLE cluster mode without the flag.
    assertThat(result.get().uri().isDirectConnection()).isNull();
    // Read preference embedded so mongos can forward it to the underlying shard replica sets.
    assertThat(result.get().uri().getReadPreference().getName()).isEqualTo("secondaryPreferred");
  }

  // ---- isMongodHostStillValid ----

  @Test
  public void isMongodHostStillValid_returnsTrueWhenHostInHealthyList() {
    when(this.mongodClient.getClusterDescription())
        .thenReturn(replicaSetCluster(List.of(RS_HOST_1, RS_HOST_2)));
    var provider =
        new InitialSyncHostProvider(nonShardedConfig(), this.mongodClient, Optional.empty(), true);
    assertThat(provider.isMongodHostStillValid(RS_HOST_1)).isTrue();
  }

  @Test
  public void isMongodHostStillValid_returnsFalseWhenHostNotInHealthyList() {
    when(this.mongodClient.getClusterDescription())
        .thenReturn(replicaSetCluster(List.of(RS_HOST_2)));
    var provider =
        new InitialSyncHostProvider(nonShardedConfig(), this.mongodClient, Optional.empty(), true);
    assertThat(provider.isMongodHostStillValid(RS_HOST_1)).isFalse();
  }

  @Test
  public void isMongodHostStillValid_returnsFalseWhenNoHealthyHosts() {
    when(this.mongodClient.getClusterDescription()).thenReturn(replicaSetCluster(List.of()));
    var provider =
        new InitialSyncHostProvider(nonShardedConfig(), this.mongodClient, Optional.empty(), true);
    assertThat(provider.isMongodHostStillValid(RS_HOST_1)).isFalse();
  }

  // ---- isMongosHostStillValid ----

  @Test
  public void isMongosHostStillValid_returnsFalseForNonShardedDeployment() {
    var provider =
        new InitialSyncHostProvider(nonShardedConfig(), this.mongodClient, Optional.empty(), true);
    assertThat(provider.isMongosHostStillValid(MONGOS_HOST)).isFalse();
  }

  @Test
  public void isMongosHostStillValid_returnsTrueWhenHostInHealthyList() {
    when(this.mongosClient.getClusterDescription())
        .thenReturn(shardedCluster(List.of(MONGOS_HOST)));
    var provider =
        new InitialSyncHostProvider(
            shardedConfig(), this.mongodClient, Optional.of(this.mongosClient), true);
    assertThat(provider.isMongosHostStillValid(MONGOS_HOST)).isTrue();
  }

  @Test
  public void isMongosHostStillValid_returnsFalseWhenHostNotInList() {
    var other = new ServerAddress("other.local", 27019);
    when(this.mongosClient.getClusterDescription()).thenReturn(shardedCluster(List.of(other)));
    var provider =
        new InitialSyncHostProvider(
            shardedConfig(), this.mongodClient, Optional.of(this.mongosClient), true);
    assertThat(provider.isMongosHostStillValid(MONGOS_HOST)).isFalse();
  }

  // ---- close ----

  @Test
  public void close_closesMongodClient() {
    var provider =
        new InitialSyncHostProvider(nonShardedConfig(), this.mongodClient, Optional.empty(), true);
    provider.close();
    verify(this.mongodClient).close();
  }

  @Test
  public void close_closesMongosClientWhenPresent() {
    var provider =
        new InitialSyncHostProvider(
            shardedConfig(), this.mongodClient, Optional.of(this.mongosClient), true);
    provider.close();
    verify(this.mongodClient).close();
    verify(this.mongosClient).close();
  }

  // ---- awaitFullTopologyDiscovery ----

  @Test
  public void awaitFullTopologyDiscovery_setsReadyWhenAllServersConnected() {
    when(this.mongodClient.getClusterDescription())
        .thenReturn(replicaSetCluster(List.of(RS_HOST_1)));
    var provider =
        new InitialSyncHostProvider(nonShardedConfig(), this.mongodClient, Optional.empty(), false);
    assertThat(provider.isReady()).isFalse();

    provider.awaitFullTopologyDiscovery();

    assertThat(provider.isReady()).isTrue();
  }

  @Test
  public void awaitFullTopologyDiscovery_setsReadyWhenBothMongodAndMongosConnected() {
    when(this.mongodClient.getClusterDescription())
        .thenReturn(replicaSetCluster(List.of(RS_HOST_1)));
    when(this.mongosClient.getClusterDescription())
        .thenReturn(shardedCluster(List.of(MONGOS_HOST)));
    var provider =
        new InitialSyncHostProvider(
            shardedConfig(), this.mongodClient, Optional.of(this.mongosClient), false);
    assertThat(provider.isReady()).isFalse();

    provider.awaitFullTopologyDiscovery();

    assertThat(provider.isReady()).isTrue();
  }

  @Test
  public void awaitFullTopologyDiscovery_waitsUntilMongosConnects() {
    var connectingCluster =
        new ClusterDescription(
            ClusterConnectionMode.MULTIPLE,
            ClusterType.UNKNOWN,
            List.of(
                ServerDescription.builder()
                    .address(MONGOS_HOST)
                    .type(ServerType.UNKNOWN)
                    .state(ServerConnectionState.CONNECTING)
                    .ok(false)
                    .build()));
    when(this.mongodClient.getClusterDescription())
        .thenReturn(replicaSetCluster(List.of(RS_HOST_1)));
    when(this.mongosClient.getClusterDescription())
        .thenReturn(connectingCluster)
        .thenReturn(shardedCluster(List.of(MONGOS_HOST)));
    var provider =
        new InitialSyncHostProvider(
            shardedConfig(), this.mongodClient, Optional.of(this.mongosClient), false);

    provider.awaitFullTopologyDiscovery();

    assertThat(provider.isReady()).isTrue();
  }

  // ---- startDiscovery ----

  @Test
  public void startDiscovery_isNotReadyUntilDiscoveryCompletes() throws InterruptedException {
    // Return CONNECTING until 1s has elapsed, then switch to a healthy cluster.
    // This gives startDiscovery() time to return before the background thread can complete.
    Instant start = Instant.now();
    Instant switchAt = Instant.now().plus(Duration.ofSeconds(1));
    when(this.mongodClient.getClusterDescription())
        .thenAnswer(
            inv ->
                Instant.now().isAfter(switchAt)
                    ? replicaSetCluster(List.of(RS_HOST_1))
                    : connectingCluster());
    try (var provider =
        new InitialSyncHostProvider(
            nonShardedConfig(), this.mongodClient, Optional.empty(), false)) {
      provider.startDiscovery(new SimpleMeterRegistry());
      assertThat(provider.isReady()).isFalse();

      Instant deadline = Instant.now().plus(Duration.ofSeconds(5));
      while (!provider.isReady() && Instant.now().isBefore(deadline)) {
        Thread.sleep(10);
      }
      assertThat(provider.isReady()).isTrue();
      assertThat(Duration.between(start, Instant.now()).toMillis()).isAtLeast(1000L);
    }
  }

  private static ClusterDescription connectingCluster() {
    return new ClusterDescription(
        ClusterConnectionMode.MULTIPLE,
        ClusterType.UNKNOWN,
        List.of(
            ServerDescription.builder()
                .address(RS_HOST_1)
                .type(ServerType.UNKNOWN)
                .state(ServerConnectionState.CONNECTING)
                .ok(false)
                .build()));
  }
}
