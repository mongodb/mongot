package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.TimeableChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamMongoClientFactory.TimedChangeStreamClientFactory;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class TimedChangeStreamClientTest {

  public static final GenerationId GENERATION_ID =
      new GenerationId(new ObjectId(), Generation.CURRENT);

  @Test
  public void getNext_initialClient_returnsItsBatchWithoutCallingFactory() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);
    var batch = batchWithToken("initial");

    when(timeableClient.getNext()).thenReturn(batch);
    when(timeableClient.getUptime()).thenReturn(Duration.ZERO);

    try (var restartClient =
        new TimedChangeStreamClient(
            timeableClient, clientFactory, GENERATION_ID, Duration.ofSeconds(60))) {

      assertEquals(batch, restartClient.getNext());
      verifyNoInteractions(clientFactory);
    }
  }

  @Test
  public void getNext_wrappedClientFails_propagatesFailure() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);

    when(timeableClient.getNext()).thenThrow(new RuntimeException("getMore() request failure"));

    try (var restartClient =
        new TimedChangeStreamClient(
            timeableClient, clientFactory, GENERATION_ID, Duration.ofSeconds(60))) {

      Exception ex = Assert.assertThrows(Exception.class, restartClient::getNext);
      assertEquals("getMore() request failure", ex.getMessage());
      verifyNoInteractions(clientFactory);
    }
  }

  @Test
  public void getNext_restartFails_propagatesFailure() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);

    when(timeableClient.getNext()).thenReturn(batchWithToken("next"));
    when(timeableClient.getUptime()).thenReturn(Duration.ofSeconds(80));
    when(clientFactory.resumeChangeStream(any(), any()))
        .thenThrow(new RuntimeException("restart failure test"));

    try (var restartClient =
        new TimedChangeStreamClient(
            timeableClient, clientFactory, GENERATION_ID, Duration.ofSeconds(60))) {

      Exception ex = Assert.assertThrows(Exception.class, restartClient::getNext);
      assertEquals("restart failure test", ex.getMessage());
    }
  }

  @Test
  public void getNext_ttlExceeded_resumesWithLatestFullTokenAndGeneration() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);
    var resumedClient = mock(TimeableChangeStreamMongoClient.class);
    var firstBatch = batchWithToken("first");
    var latestToken =
        new BsonDocument("_data", new BsonString("latest"))
            .append("_typeBits", new BsonBinary(new byte[] {0}));
    var latestBatch = new ChangeStreamBatch(List.of(), latestToken, Optional.empty());
    var resumedBatch = batchWithToken("resumed");

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(resumedClient);
    when(timeableClient.getNext()).thenReturn(firstBatch, latestBatch);
    when(timeableClient.getUptime()).thenReturn(Duration.ofSeconds(40), Duration.ofSeconds(80));
    when(resumedClient.getNext()).thenReturn(resumedBatch);
    when(resumedClient.getUptime()).thenReturn(Duration.ZERO);

    try (var restartClient =
        new TimedChangeStreamClient(
            timeableClient, clientFactory, GENERATION_ID, Duration.ofSeconds(60))) {

      assertEquals(firstBatch, restartClient.getNext());
      assertEquals(latestBatch, restartClient.getNext());
      assertEquals(resumedBatch, restartClient.getNext());

      verify(clientFactory).resumeChangeStream(GENERATION_ID, latestToken);
    }
  }

  @Test
  public void getNext_atUptimeLimit_doesNotRestart() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);
    var batch = batchWithToken("next");

    when(timeableClient.getNext()).thenReturn(batch);
    when(timeableClient.getUptime()).thenReturn(Duration.ofSeconds(60));

    try (var restartClient =
        new TimedChangeStreamClient(
            timeableClient, clientFactory, GENERATION_ID, Duration.ofSeconds(60))) {

      assertEquals(batch, restartClient.getNext());
      verifyNoInteractions(clientFactory);
    }
  }

  @Test
  public void getNext_underUptimeLimit_doesNotRestart() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);
    var batch = batchWithToken("next");

    when(timeableClient.getNext()).thenReturn(batch);
    when(timeableClient.getUptime()).thenReturn(Duration.ofSeconds(40));

    try (var restartClient =
        new TimedChangeStreamClient(
            timeableClient, clientFactory, GENERATION_ID, Duration.ofSeconds(60))) {

      assertEquals(batch, restartClient.getNext());
      verifyNoInteractions(clientFactory);
    }
  }

  @Test
  public void getNext_ttlExceeded_closesPreviousClient() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);
    var resumedClient = mock(TimeableChangeStreamMongoClient.class);
    var batch = batchWithToken("next");

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(resumedClient);
    when(timeableClient.getNext()).thenReturn(batch);
    when(timeableClient.getUptime()).thenReturn(Duration.ofSeconds(80));

    try (var restartClient =
        new TimedChangeStreamClient(
            timeableClient, clientFactory, GENERATION_ID, Duration.ofSeconds(60))) {

      assertEquals(batch, restartClient.getNext());
      verify(timeableClient).close();
    }

    verify(resumedClient).close();
  }

  @Test
  public void close_beforeRestart_closesInitialClient() {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);
    var restartClient =
        new TimedChangeStreamClient(
            timeableClient, clientFactory, GENERATION_ID, Duration.ofSeconds(60));

    restartClient.close();

    verify(timeableClient).close();
    verifyNoInteractions(clientFactory);
  }

  @Test
  public void close_duringRestart_closesNewClient() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var first = mock(TimeableChangeStreamMongoClient.class);
    var second = mock(TimeableChangeStreamMongoClient.class);
    var clientRef = new AtomicReference<TimedChangeStreamClient>();

    // close() lands while the restart is creating its new client.
    when(clientFactory.resumeChangeStream(any(), any()))
        .thenAnswer(
            invocation -> {
              clientRef.get().close();
              return second;
            });
    when(first.getNext()).thenReturn(batchWithToken("next"));

    var client =
        new TimedChangeStreamClient(first, clientFactory, GENERATION_ID, Duration.ofSeconds(60));
    clientRef.set(client);
    when(first.getUptime()).thenReturn(Duration.ofSeconds(80));

    client.getNext();

    verify(second).close();
  }

  private static ChangeStreamBatch batchWithToken(String token) {
    return new ChangeStreamBatch(
        List.of(), new BsonDocument("token", new BsonString(token)), Optional.empty());
  }
}
