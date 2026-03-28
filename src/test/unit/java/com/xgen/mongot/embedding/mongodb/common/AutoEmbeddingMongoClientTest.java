package com.xgen.mongot.embedding.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.mongodb.client.MongoClient;
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
    var client = new AutoEmbeddingMongoClient(Optional.empty(), new SimpleMeterRegistry());

    assertThat(client.getMaterializedViewResolverMongoClient()).isEmpty();
    assertThat(client.getLeaseManagerMongoClient()).isEmpty();
    assertThat(client.getMaterializedViewWriterMongoClient()).isEmpty();
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
  public void close_noSyncSource_doesNotThrow() {
    var client = new AutoEmbeddingMongoClient(Optional.empty(), new SimpleMeterRegistry());
    client.close();
    assertThat(client.getMaterializedViewResolverMongoClient()).isEmpty();
  }
}
