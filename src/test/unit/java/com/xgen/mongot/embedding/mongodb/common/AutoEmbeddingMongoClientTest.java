package com.xgen.mongot.embedding.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.mongodb.client.MongoClient;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
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
    client.updateSyncSource(updatedConfig);

    verify(resolverClient).close();
    verify(leaseClient).close();
    verify(writerClient).close();
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
}
