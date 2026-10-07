package com.xgen.mongot.embedding.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoCredential.OidcCallback;
import com.mongodb.client.MongoClient;
import com.mongodb.client.internal.MongoClientImpl;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.util.mongodb.ConnectionInfo;
import com.xgen.mongot.util.mongodb.ConnectionStringUtil;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.junit.Test;

public class AutoEmbeddingMongoClientTest {

  @Test
  public void constructor_withClients_gettersReturnClients() {
    MongoClient resolverClient = mock(MongoClient.class);
    MongoClient leaseClient = mock(MongoClient.class);
    MongoClient writerClient = mock(MongoClient.class);

    var client =
        new AutoEmbeddingMongoClient(
            mock(SyncSourceConfig.class),
            resolverClient,
            leaseClient,
            writerClient,
            new SimpleMeterRegistry());

    assertThat(client.getMaterializedViewResolverMongoClient())
        .isEqualTo(Optional.of(resolverClient));
    assertThat(client.getLeaseManagerMongoClient()).isEqualTo(Optional.of(leaseClient));
    assertThat(client.getMaterializedViewWriterMongoClient()).isEqualTo(Optional.of(writerClient));
  }

  @Test
  public void constructor_noSyncSource_allClientsEmpty() {
    var client =
        new AutoEmbeddingMongoClient(
            Optional.empty(),
            new SimpleMeterRegistry(),
            AutoEmbeddingMaterializedViewConfig.getDefault());

    assertThat(client.getMaterializedViewResolverMongoClient()).isEmpty();
    assertThat(client.getLeaseManagerMongoClient()).isEmpty();
    assertThat(client.getMaterializedViewWriterMongoClient()).isEmpty();
  }

  @Test
  public void constructor_withSyncSource_storesSyncSourceConfig() {
    var uri = ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://localhost:27017/");
    var config =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(uri)
            .mongodClusterReadWriteUri(uri)
            .build();

    var client =
        new AutoEmbeddingMongoClient(
            Optional.of(config),
            new SimpleMeterRegistry(),
            AutoEmbeddingMaterializedViewConfig.getDefault());
    client.close();

    assertThat(client.getSyncSourceConfig()).isEqualTo(Optional.of(config));
  }

  @Test
  public void close_closesAllClients() {
    MongoClient resolverClient = mock(MongoClient.class);
    MongoClient leaseClient = mock(MongoClient.class);
    MongoClient writerClient = mock(MongoClient.class);

    var client =
        new AutoEmbeddingMongoClient(
            mock(SyncSourceConfig.class),
            resolverClient,
            leaseClient,
            writerClient,
            new SimpleMeterRegistry());

    client.close();

    verify(resolverClient).close();
    verify(leaseClient).close();
    verify(writerClient).close();
  }

  @Test
  public void updateSyncSource_sameClusterUri_doesNotCloseExistingClients() {
    MongoClient resolverClient = mock(MongoClient.class);
    MongoClient leaseClient = mock(MongoClient.class);
    MongoClient writerClient = mock(MongoClient.class);

    var clusterUri = ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://localhost:27017/");
    var initialConfig =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(clusterUri)
            .mongodClusterReadWriteUri(clusterUri)
            .build();

    var client =
        new AutoEmbeddingMongoClient(
            initialConfig, resolverClient, leaseClient, writerClient, new SimpleMeterRegistry());

    // Update with a different single-host URI but the same cluster URI.
    var updatedConfig =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(clusterUri)
            .mongodClusterReadWriteUri(clusterUri)
            .mongodSingleHostReplicationUri(clusterUri)
            .build();
    client.updateSyncSource(updatedConfig);

    verify(resolverClient, never()).close();
    verify(leaseClient, never()).close();
    verify(writerClient, never()).close();
    assertThat(client.getLeaseManagerMongoClient()).isEqualTo(Optional.of(leaseClient));
    // Config must be updated even though clients are preserved.
    assertThat(client.getSyncSourceConfig()).isEqualTo(Optional.of(updatedConfig));
  }

  @Test
  public void updateSyncSource_fromNoSyncSource_storesSyncSourceConfig() {
    var client =
        new AutoEmbeddingMongoClient(
            Optional.empty(),
            new SimpleMeterRegistry(),
            AutoEmbeddingMaterializedViewConfig.getDefault());

    var uri = ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://localhost:27017/");
    var config =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(uri)
            .mongodClusterReadWriteUri(uri)
            .build();
    client.updateSyncSource(config);
    client.close();

    assertThat(client.getSyncSourceConfig()).isEqualTo(Optional.of(config));
  }

  @Test
  public void updateSyncSource_differentClusterUri_closesOldClients() {
    MongoClient resolverClient = mock(MongoClient.class);
    MongoClient leaseClient = mock(MongoClient.class);
    MongoClient writerClient = mock(MongoClient.class);

    var initialUri = ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://localhost:27017/");
    var initialConfig =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(initialUri)
            .mongodClusterReadWriteUri(initialUri)
            .build();

    var client =
        new AutoEmbeddingMongoClient(
            initialConfig, resolverClient, leaseClient, writerClient, new SimpleMeterRegistry());

    var newUri = ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://localhost:27018/");
    var updatedConfig =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(newUri)
            .mongodClusterReadWriteUri(newUri)
            .build();
    try {
      client.updateSyncSource(updatedConfig);

      verify(resolverClient).close();
      verify(leaseClient).close();
      verify(writerClient).close();
    } finally {
      client.close();
    }
  }

  @Test
  public void close_noSyncSource_doesNotThrow() {
    var client =
        new AutoEmbeddingMongoClient(
            Optional.empty(),
            new SimpleMeterRegistry(),
            AutoEmbeddingMaterializedViewConfig.getDefault());
    client.close();
    assertThat(client.getMaterializedViewResolverMongoClient()).isEmpty();
  }

  // --- probe client tests ---

  @Test
  public void constructor_fiveArgTesting_probeClientEmpty() {
    // The 5-arg @VisibleForTesting constructor leaves the probe client null so existing test
    // callers are unaffected (no extra close() calls on shared mocks).
    MongoClient resolverClient = mock(MongoClient.class);
    MongoClient leaseClient = mock(MongoClient.class);
    MongoClient writerClient = mock(MongoClient.class);

    var client =
        new AutoEmbeddingMongoClient(
            mock(SyncSourceConfig.class),
            resolverClient,
            leaseClient,
            writerClient,
            new SimpleMeterRegistry());

    assertThat(client.getProbeMongoClient()).isEmpty();
  }

  @Test
  public void close_closesProbeClient() {
    MongoClient resolverClient = mock(MongoClient.class);
    MongoClient leaseClient = mock(MongoClient.class);
    MongoClient writerClient = mock(MongoClient.class);
    MongoClient probeClient = mock(MongoClient.class);

    var client =
        new AutoEmbeddingMongoClient(
            mock(SyncSourceConfig.class),
            resolverClient,
            leaseClient,
            writerClient,
            probeClient,
            new SimpleMeterRegistry());

    client.close();

    verify(probeClient).close();
  }

  @Test
  public void updateSyncSource_differentClusterUri_closesOldProbeClient() {
    MongoClient resolverClient = mock(MongoClient.class);
    MongoClient leaseClient = mock(MongoClient.class);
    MongoClient writerClient = mock(MongoClient.class);
    MongoClient probeClient = mock(MongoClient.class);

    var initialUri = ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://localhost:27017/");
    var initialConfig =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(initialUri)
            .mongodClusterReadWriteUri(initialUri)
            .build();

    var client =
        new AutoEmbeddingMongoClient(
            initialConfig,
            resolverClient,
            leaseClient,
            writerClient,
            probeClient,
            new SimpleMeterRegistry());

    var newUri = ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://localhost:27018/");
    var updatedConfig =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(newUri)
            .mongodClusterReadWriteUri(newUri)
            .build();
    try {
      client.updateSyncSource(updatedConfig);

      verify(probeClient).close();
    } finally {
      client.close();
    }
  }

  // --- OIDC credential wiring ---

  @Test
  public void initClients_oidcCallbackPresent_appliesOidcCredentialToAllClients() {
    OidcCallback callback = context -> null;
    var uri =
        new ConnectionInfo(
            new ConnectionString("mongodb://localhost:27017/"),
            Optional.empty(),
            Optional.of(
                MongoCredential.createOidcCredential(null)
                    .withMechanismProperty(MongoCredential.OIDC_CALLBACK_KEY, callback)));
    var config =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(uri)
            .mongodClusterReadWriteUri(uri)
            .build();

    var client =
        new AutoEmbeddingMongoClient(
            Optional.of(config),
            new SimpleMeterRegistry(),
            AutoEmbeddingMaterializedViewConfig.getDefault());
    try {
      assertThat(credentialMechanism(client.getMaterializedViewResolverMongoClient().get()))
          .isEqualTo(AuthenticationMechanism.MONGODB_OIDC);
      assertThat(credentialMechanism(client.getLeaseManagerMongoClient().get()))
          .isEqualTo(AuthenticationMechanism.MONGODB_OIDC);
      assertThat(credentialMechanism(client.getMaterializedViewWriterMongoClient().get()))
          .isEqualTo(AuthenticationMechanism.MONGODB_OIDC);
      assertThat(credentialMechanism(client.getProbeMongoClient().get()))
          .isEqualTo(AuthenticationMechanism.MONGODB_OIDC);
    } finally {
      client.close();
    }
  }

  @Test
  public void initClients_noOidcCallback_credentialAbsentFromAllClients() {
    var uri = ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://localhost:27017/");
    var config =
        SyncSourceConfig.builder()
            .mongodClusterReplicationUri(uri)
            .mongodClusterReadWriteUri(uri)
            .build();

    var client =
        new AutoEmbeddingMongoClient(
            Optional.of(config),
            new SimpleMeterRegistry(),
            AutoEmbeddingMaterializedViewConfig.getDefault());
    try {
      assertThat(settingsOf(client.getMaterializedViewResolverMongoClient().get()).getCredential())
          .isNull();
      assertThat(settingsOf(client.getLeaseManagerMongoClient().get()).getCredential()).isNull();
      assertThat(settingsOf(client.getMaterializedViewWriterMongoClient().get()).getCredential())
          .isNull();
      assertThat(settingsOf(client.getProbeMongoClient().get()).getCredential()).isNull();
    } finally {
      client.close();
    }
  }

  private static AuthenticationMechanism credentialMechanism(MongoClient client) {
    return settingsOf(client).getCredential().getAuthenticationMechanism();
  }

  private static MongoClientSettings settingsOf(MongoClient client) {
    return ((MongoClientImpl) client).getSettings();
  }
}
