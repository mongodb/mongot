package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.TimeableChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamMongoClientFactory.TimedChangeStreamClientFactory;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class TimedChangeStreamClientTest {

  public static final GenerationId GENERATION_ID =
      new GenerationId(new ObjectId(), Generation.CURRENT);

  @Test
  public void testTimedClientProxiesRequestAndResponseAsIs() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);
    var batch = mock(ChangeStreamBatch.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(timeableClient);
    when(timeableClient.getNext()).thenReturn(batch);

    try (var restartClient =
        new TimedChangeStreamClient(
            clientFactory, getResumeInfo(), GENERATION_ID, Duration.ofSeconds(60))) {

      assertEquals(batch, restartClient.getNext());
    }
  }

  @Test
  public void testTimedClientProxiesExceptionFromWrappedClient() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(timeableClient);
    when(timeableClient.getNext()).thenThrow(new RuntimeException("getMore() request failure"));

    try (var restartClient =
        new TimedChangeStreamClient(
            clientFactory, getResumeInfo(), GENERATION_ID, Duration.ofSeconds(60))) {

      Exception ex = Assert.assertThrows(Exception.class, restartClient::getNext);
      assertEquals("getMore() request failure", ex.getMessage());
    }
  }

  @Test
  public void testTimedClientFailureOnWrappedClientRestart() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(timeableClient);
    when(timeableClient.getNext()).thenReturn(mock(ChangeStreamBatch.class));

    try (var restartClient =
        new TimedChangeStreamClient(
            clientFactory, getResumeInfo(), GENERATION_ID, Duration.ofSeconds(60))) {

      when(timeableClient.getUptime()).thenReturn(Duration.ofSeconds(80));
      when(clientFactory.resumeChangeStream(any(), any()))
          .thenThrow(new RuntimeException("restart failure test"));

      verify(clientFactory).resumeChangeStream(eq(GENERATION_ID), any());

      // make sure the Future has failed
      Exception ex = Assert.assertThrows(Exception.class, restartClient::getNext);
      assertEquals("restart failure test", ex.getMessage());
    }
  }

  @Test
  public void testTimedClientResumesWithFetchedBatchPostToken() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);
    var batch = mock(ChangeStreamBatch.class);
    var nextResumeToken = getResumeToken("nextResumeToken");

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(timeableClient);
    when(timeableClient.getNext()).thenReturn(batch);
    when(batch.getPostBatchResumeToken()).thenReturn(nextResumeToken);

    try (var restartClient =
        new TimedChangeStreamClient(
            clientFactory, getResumeInfo(), GENERATION_ID, Duration.ofSeconds(60))) {

      when(timeableClient.getUptime()).thenReturn(Duration.ofSeconds(80));

      assertEquals(batch, restartClient.getNext());
      assertEquals(nextResumeToken, batch.getPostBatchResumeToken());

      verify(clientFactory)
          .resumeChangeStream(
              eq(GENERATION_ID), argThat(info -> info.getResumeToken().equals(nextResumeToken)));
    }
  }

  @Test
  public void testWrappedClientRestartWhenAtUptimeLimit() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);
    var batch = mock(ChangeStreamBatch.class);
    var nextResumeToken = getResumeToken("nextResumeToken");

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(timeableClient);
    when(timeableClient.getNext()).thenReturn(batch);
    when(batch.getPostBatchResumeToken()).thenReturn(nextResumeToken);

    try (var restartClient =
        new TimedChangeStreamClient(
            clientFactory, getResumeInfo(), GENERATION_ID, Duration.ofSeconds(60))) {

      when(timeableClient.getUptime()).thenReturn(Duration.ofSeconds(60));

      assertEquals(batch, restartClient.getNext());
      assertEquals(nextResumeToken, batch.getPostBatchResumeToken());

      verify(clientFactory, never())
          .resumeChangeStream(
              eq(GENERATION_ID), argThat(info -> info.getResumeToken().equals(nextResumeToken)));
    }
  }

  @Test
  public void testWrappedClientNotRestartedWhenUnderUptimeLimit() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);
    var batch = mock(ChangeStreamBatch.class);
    var nextResumeToken = getResumeToken("nextResumeToken");

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(timeableClient);
    when(timeableClient.getNext()).thenReturn(batch);
    when(batch.getPostBatchResumeToken()).thenReturn(nextResumeToken);

    try (var restartClient =
        new TimedChangeStreamClient(
            clientFactory, getResumeInfo(), GENERATION_ID, Duration.ofSeconds(60))) {

      when(timeableClient.getUptime()).thenReturn(Duration.ofSeconds(40));

      assertEquals(batch, restartClient.getNext());
      assertEquals(nextResumeToken, batch.getPostBatchResumeToken());
      verify(clientFactory, never())
          .resumeChangeStream(
              eq(GENERATION_ID), argThat(info -> info.getResumeToken().equals(nextResumeToken)));
    }
  }

  @Test
  public void testWrappedClientIsClosedOnRestart() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);
    var batch = mock(ChangeStreamBatch.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(timeableClient);
    when(timeableClient.getNext()).thenReturn(batch);

    try (var restartClient =
        new TimedChangeStreamClient(
            clientFactory, getResumeInfo(), GENERATION_ID, Duration.ofSeconds(60))) {

      when(timeableClient.getUptime()).thenReturn(Duration.ofSeconds(80));

      assertEquals(batch, restartClient.getNext());
      verify(timeableClient).close();
    }
  }

  @Test
  public void testWrappedClientIsClosedOnClientClose() {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var timeableClient = mock(TimeableChangeStreamMongoClient.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(timeableClient);

    try (var restartClient =
        new TimedChangeStreamClient(
            clientFactory, getResumeInfo(), GENERATION_ID, Duration.ofSeconds(60))) {

      restartClient.close();
      verify(timeableClient).close();
    }
  }

  @Test
  public void testClientCreatedWhileCloseIsRunningIsClosed() throws Exception {
    var clientFactory = mock(TimedChangeStreamClientFactory.class);
    var first = mock(TimeableChangeStreamMongoClient.class);
    var second = mock(TimeableChangeStreamMongoClient.class);
    var clientRef = new AtomicReference<TimedChangeStreamClient>();

    // close() lands while the restart is creating its new client.
    when(clientFactory.resumeChangeStream(any(), any()))
        .thenReturn(first)
        .thenAnswer(
            invocation -> {
              clientRef.get().close();
              return second;
            });
    when(first.getNext()).thenReturn(mock(ChangeStreamBatch.class));

    var client =
        new TimedChangeStreamClient(
            clientFactory, getResumeInfo(), GENERATION_ID, Duration.ofSeconds(60));
    clientRef.set(client);
    when(first.getUptime()).thenReturn(Duration.ofSeconds(80));

    client.getNext();

    verify(second).close();
  }

  private ChangeStreamResumeInfo getResumeInfo() {
    return ChangeStreamResumeInfo.create(
        new MongoNamespace("test", "test"), getResumeToken("initial"));
  }

  private BsonDocument getResumeToken(String token) {
    return new BsonDocument("token", new BsonString(token));
  }
}
