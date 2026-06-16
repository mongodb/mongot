package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.SessionRefresher.mockSessionRefresher;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.MongoNamespace;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.index.IndexMetricsUpdater.ReplicationMetricsUpdater.InitialSyncMetrics;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.NamespaceResolutionException;
import com.xgen.mongot.replication.mongodb.common.NamespaceResolver;
import com.xgen.mongot.replication.mongodb.common.SplitEventChangeStreamClient;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class DefaultInitialSyncMongoClientTest {

  @Test
  public void testEnsureCollectionNameUnchangedIsUnchanged() throws Exception {
    NamespaceResolver namespaceResolver = mock(NamespaceResolver.class);
    when(namespaceResolver.isCollectionNameChanged(any(), any())).thenReturn(false);

    DefaultInitialSyncMongoClient client =
        new DefaultInitialSyncMongoClient(
            mock(MongoClient.class),
            mockSessionRefresher(),
            new SimpleMeterRegistry(),
            namespaceResolver,
            "",
            false);

    client.ensureCollectionNameUnchanged(MOCK_INDEX_DEFINITION, "foo");
  }

  @Test
  public void testEnsureCollectionNameUnchangedIsChanged() throws Exception {
    NamespaceResolver namespaceResolver = mock(NamespaceResolver.class);
    when(namespaceResolver.isCollectionNameChanged(any(), any())).thenReturn(true);

    DefaultInitialSyncMongoClient client =
        new DefaultInitialSyncMongoClient(
            mock(MongoClient.class),
            mockSessionRefresher(),
            new SimpleMeterRegistry(),
            namespaceResolver,
            "",
            false);

    try {
      client.ensureCollectionNameUnchanged(MOCK_INDEX_DEFINITION, "foo");
      Assert.fail("did not throw InitialSyncException");
    } catch (InitialSyncException e) {
      Assert.assertTrue(e.isRequiresResync());
    }
  }

  @Test
  public void testGetChangeStreamMongoClientWithoutSplitLargeEvents() throws Exception {
    var mongoClient = mock(MongoClient.class);
    when(mongoClient.startSession()).thenReturn(mock(ClientSession.class));

    DefaultInitialSyncMongoClient client =
        new DefaultInitialSyncMongoClient(
            mongoClient,
            mockSessionRefresher(),
            new SimpleMeterRegistry(),
            mock(NamespaceResolver.class),
            "",
            false);

    var result =
        client.getChangeStreamMongoClient(
            new ChangeStreamAggregateCommand.Builder().build(),
            new MongoNamespace("db", "coll"),
            mock(InitialSyncMetrics.class),
            Optional.empty(),
            new GenerationId(new ObjectId(), Generation.CURRENT));

    Assert.assertFalse(result instanceof SplitEventChangeStreamClient);
  }

  @Test
  public void testGetChangeStreamMongoClientWithSplitLargeEvents() throws Exception {
    var mongoClient = mock(MongoClient.class);
    when(mongoClient.startSession()).thenReturn(mock(ClientSession.class));

    DefaultInitialSyncMongoClient client =
        new DefaultInitialSyncMongoClient(
            mongoClient,
            mockSessionRefresher(),
            new SimpleMeterRegistry(),
            mock(NamespaceResolver.class),
            "",
            true);

    var result =
        client.getChangeStreamMongoClient(
            new ChangeStreamAggregateCommand.Builder().build(),
            new MongoNamespace("db", "coll"),
            mock(InitialSyncMetrics.class),
            Optional.empty(),
            new GenerationId(new ObjectId(), Generation.CURRENT));

    Assert.assertTrue(result instanceof SplitEventChangeStreamClient);
  }

  @Test
  public void testEnsureCollectionNameUnchangedThrowsDoesNotExist() throws Exception {
    NamespaceResolver namespaceResolver = mock(NamespaceResolver.class);
    when(namespaceResolver.isCollectionNameChanged(any(), any()))
        .thenThrow(NamespaceResolutionException.create());

    DefaultInitialSyncMongoClient client =
        new DefaultInitialSyncMongoClient(
            mock(MongoClient.class),
            mockSessionRefresher(),
            new SimpleMeterRegistry(),
            namespaceResolver,
            "",
            false);

    try {
      client.ensureCollectionNameUnchanged(MOCK_INDEX_DEFINITION, "foo");
      Assert.fail("did not throw InitialSyncException");
    } catch (InitialSyncException e) {
      Assert.assertTrue(e.isDropped());
    }
  }
}
