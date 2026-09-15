package com.xgen.mongot.replication.mongodb.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.MongoClientSettings;
import com.mongodb.internal.operation.AggregateResponseBatchCursor;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException.Type;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Test;

public class ChangeStreamMongoCursorClientTest {

  /**
   * In driver 5.x, change-stream {@code getMore} responses do not surface a top-level {@code
   * operationTime}, so {@code cursor.getOperationTime()} is null. That must no longer fail the
   * change stream; the replication-lag "now" reference is instead taken from the session's gossiped
   * {@code $clusterTime}.
   */
  @Test
  public void testNullCursorOperationTime_usesSessionClusterTime() throws SteadyStateException {
    BsonTimestamp clusterTime = new BsonTimestamp(1780382186, 1);
    var cursorClient =
        createBatchCursorMongoClient(
            new BsonDocument("clusterTime", clusterTime), new BsonDocument());

    ChangeStreamBatch batch = cursorClient.getNext();

    Assert.assertEquals(Optional.of(clusterTime), batch.getCommandOperationTime());
  }

  /**
   * Before the session has observed a cluster time, {@code getClusterTime()} is null. The change
   * stream must tolerate this (commandOperationTime is absent) rather than throwing.
   */
  @Test
  public void testNullClusterTime_commandOperationTimeIsEmpty() throws SteadyStateException {
    var cursorClient = createBatchCursorMongoClient(null, new BsonDocument());

    ChangeStreamBatch batch = cursorClient.getNext();

    Assert.assertTrue(batch.getCommandOperationTime().isEmpty());
  }

  /** A missing postBatchResumeToken is still a hard error: it is required to resume the stream. */
  @Test
  public void testInvalidBatchField_NullPostBatchResumeToken() {
    var cursorClient = createBatchCursorMongoClient(new BsonDocument(), null);

    SteadyStateException exception =
        Assert.assertThrows(SteadyStateException.class, cursorClient::getNext);

    Assert.assertEquals(Type.TRANSIENT, exception.getType());
    Assert.assertNotNull(exception.getCause());
    Assert.assertTrue(exception.getCause() instanceof ChangeStreamCursorClientException);

    Assert.assertNotNull(exception.getCause().getCause());
    Assert.assertTrue(exception.getCause().getCause() instanceof NullPointerException);

    Assert.assertEquals("getPostBatchResumeToken", exception.getCause().getCause().getMessage());
  }

  /**
   * If the gossiped {@code $clusterTime} document has a missing or non-timestamp {@code
   * clusterTime} field, that is an unexpected/malformed response shape: the change stream surfaces
   * it as a transient error ({@code SteadyStateException} type {@code TRANSIENT}) rather than
   * silently dropping the lag metric.
   */
  @Test
  public void testNonTimestampClusterTimeField_throwsTransient() {
    var cursorClient =
        createBatchCursorMongoClient(
            new BsonDocument("clusterTime", new org.bson.BsonString("not-a-timestamp")),
            new BsonDocument());

    SteadyStateException exception =
        Assert.assertThrows(SteadyStateException.class, cursorClient::getNext);

    Assert.assertEquals(Type.TRANSIENT, exception.getType());
    Assert.assertNotNull(exception.getCause());
    Assert.assertTrue(exception.getCause() instanceof ChangeStreamCursorClientException);
  }

  /**
   * Happy path: with a valid gossiped {@code $clusterTime}, the batch's operation time is sourced
   * from it (regardless of batch contents) and never throws.
   */
  @Test
  public void testValidClusterTime_commandOperationTimePresent() throws SteadyStateException {
    BsonTimestamp clusterTime = new BsonTimestamp(1780382186, 1);
    var cursorClient =
        createBatchCursorMongoClient(
            new BsonDocument("clusterTime", clusterTime), new BsonDocument());

    ChangeStreamBatch batch = cursorClient.getNext();

    Assert.assertEquals(Optional.of(clusterTime), batch.getCommandOperationTime());
  }

  private ChangeStreamMongoCursorClient<SteadyStateException> createBatchCursorMongoClient(
      BsonDocument clusterTime, BsonDocument resumeToken) {
    BatchMongoClient batchMongoClient = mock(BatchMongoClient.class);
    SessionRefresher sessionRefresher = mock(SessionRefresher.class);
    RefreshingClientSession refreshingClientSession = mock(RefreshingClientSession.class);
    AggregateResponseBatchCursor batchCursor = mock(AggregateResponseBatchCursor.class);

    com.mongodb.client.ClientSession clientSession = mock(com.mongodb.client.ClientSession.class);
    when(batchMongoClient.openSession(any())).thenReturn(clientSession);
    when(sessionRefresher.register(any())).thenReturn(refreshingClientSession);
    when(refreshingClientSession.getSession()).thenReturn(clientSession);
    when(clientSession.getClusterTime()).thenReturn(clusterTime);

    when(batchMongoClient.getSettings()).thenReturn(MongoClientSettings.builder().build());
    when(batchMongoClient.openCursor(any(), any(), any())).thenReturn(batchCursor);

    when(batchCursor.getPostBatchResumeToken()).thenReturn(resumeToken);

    return new ChangeStreamMongoCursorClient<>(
        mock(GenerationId.class),
        batchMongoClient,
        sessionRefresher,
        mock(AggregateOperationTemplate.class),
        new SimpleMeterRegistry(),
        SteadyStateException::wrapIfThrows,
        Optional.empty());
  }
}
