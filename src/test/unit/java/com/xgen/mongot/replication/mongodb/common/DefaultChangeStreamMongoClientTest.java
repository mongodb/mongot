package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector.ChangeStreamMode;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.mongodb.MongoNamespace;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Test;

public class DefaultChangeStreamMongoClientTest {
  @Test
  public void testMissingClusterTimeThrowsTransientInitialSyncException() {
    var db = mock(MongoDatabase.class);
    // When exceptions are thrown from our proxies the driver wraps them with a
    // CodecConfigurationException
    doThrow(new CodecConfigurationException("boom"))
        .when(db)
        .runCommand(any(ClientSession.class), any(Bson.class), any(Class.class));
    doReturn(db).when(db).withCodecRegistry(any());
    doReturn(db).when(db).withWriteConcern(any());
    doReturn(db).when(db).withReadConcern(any());
    doReturn(db).when(db).withReadPreference(any());

    // mock a client to return the mock database
    var mongoClient = mock(MongoClient.class);
    doReturn(db).when(mongoClient).getDatabase(any());

    RefreshingClientSession refreshingSession = mock(RefreshingClientSession.class);
    doReturn(mock(ClientSession.class)).when(refreshingSession).getSession();

    DefaultChangeStreamMongoClient<InitialSyncException> client =
        new DefaultChangeStreamMongoClient<InitialSyncException>(
            new ChangeStreamAggregateCommandFactory(
                    MOCK_INDEX_DEFINITION, new MongoNamespace("test", "test"))
                .unpinned(MOCK_INDEX_DEFINITION.getIndexId(), ChangeStreamMode.getDefault()),
            refreshingSession,
            mongoClient,
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            new MongoNamespace("db", "test"),
            InitialSyncException::wrapIfThrowsChangeStream,
            Optional.empty(),
            Optional.empty());

    var e = Assert.assertThrows(InitialSyncException.class, client::getNext);
    Assert.assertSame(InitialSyncException.Type.REQUIRES_RESYNC, e.getType());
  }

  @Test
  public void createDatabaseLevelRejectsCollectionfulCommand() {
    // A collection-bound command would open a cursor whose namespace is not $cmd.aggregate, so
    // getMore/killCursors would address the wrong collection. The factory must reject it up front.
    ChangeStreamAggregateCommand collectionfulCommand =
        new ChangeStreamAggregateCommand.Builder().collection("things").build();
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            DefaultChangeStreamMongoClient.createDatabaseLevel(
                collectionfulCommand,
                mock(MongoClient.class),
                mock(SessionRefresher.class),
                "db",
                new MetricsFactory("factory", new SimpleMeterRegistry())));
  }

  @Test
  public void createDatabaseLevelAcceptsCollectionlessCommand() throws SteadyStateException {
    // A collection-less (aggregate: 1) command is the supported case; the factory builds the
    // <db>.$cmd.aggregate namespace itself so the caller cannot pass a mismatched one.
    ChangeStreamAggregateCommand collectionlessCommand =
        new ChangeStreamAggregateCommand.Builder().build();
    DefaultChangeStreamMongoClient<SteadyStateException> client =
        DefaultChangeStreamMongoClient.createDatabaseLevel(
            collectionlessCommand,
            mock(MongoClient.class),
            mock(SessionRefresher.class),
            "db",
            new MetricsFactory("factory", new SimpleMeterRegistry()));
    Assert.assertNotNull(client);
  }
}
