package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.ClientSession;
import com.mongodb.internal.operation.AggregateResponseBatchCursor;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException.Type;
import com.xgen.mongot.util.concurrent.Sleeper;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import com.xgen.mongot.util.mongodb.Errors;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Before;
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

  // Throttling retry: ChangeStreamThrottlingRetry applied inside getMore.

  private static final int RETRIES = ChangeStreamThrottlingRetry.MAX_THROTTLING_RETRIES;

  private BatchMongoClient batchMongoClient;
  private SessionRefresher sessionRefresher;
  private AggregateResponseBatchCursor<RawBsonDocument> batchCursor;
  private SimpleMeterRegistry meterRegistry;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    this.batchMongoClient = mock(BatchMongoClient.class);
    this.sessionRefresher = mock(SessionRefresher.class);
    this.batchCursor = mock(AggregateResponseBatchCursor.class);
    this.meterRegistry = new SimpleMeterRegistry();

    ClientSession clientSession = mock(ClientSession.class);
    RefreshingClientSession<ClientSession> refreshingSession = mock(RefreshingClientSession.class);
    when(this.batchMongoClient.openSession(any())).thenReturn(clientSession);
    doReturn(refreshingSession).when(this.sessionRefresher).register(any());
    when(refreshingSession.getSession()).thenReturn(clientSession);
    when(this.batchMongoClient.getSettings()).thenReturn(MongoClientSettings.builder().build());
    doReturn(this.batchCursor).when(this.batchMongoClient).openCursor(any(), any(), any());
    when(this.batchCursor.getPostBatchResumeToken()).thenReturn(new BsonDocument());
  }

  @Test
  public void getNext_throttledGetMore_retriedOnTheSameCursor() throws Exception {
    when(this.batchCursor.tryNext())
        .thenThrow(rejection(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code))
        .thenThrow(rejection(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code))
        .thenReturn(List.of());

    var batch = retryingClient(true, Sleeper.NO_OP).getNext();

    assertThat(batch.getRawEvents()).isEmpty();
    verify(this.batchCursor, times(3)).tryNext();
    // One cursor and one session: retrying a getMore never reopens the cursor.
    verify(this.batchMongoClient, times(1)).openCursor(any(), any(), any());
    verify(this.sessionRefresher, times(1)).register(any());
    assertThat(retries()).isEqualTo(2.0);
    assertThat(retriesExhausted()).isEqualTo(0.0);
    assertThat(rejections(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code)).isEqualTo(2.0);
  }

  @Test
  public void getNext_admissionQueueOverflow_retriedOnTheSameCursor() throws Exception {
    when(this.batchCursor.tryNext())
        .thenThrow(rejection(Errors.ADMISSION_QUEUE_OVERFLOW.code))
        .thenReturn(List.of());

    retryingClient(true, Sleeper.NO_OP).getNext();

    verify(this.batchCursor, times(2)).tryNext();
    assertThat(retries()).isEqualTo(1.0);
    assertThat(rejections(Errors.ADMISSION_QUEUE_OVERFLOW.code)).isEqualTo(1.0);
  }

  @Test
  public void getNext_persistentThrottling_failsAfterTheRetryBudget() {
    when(this.batchCursor.tryNext())
        .thenThrow(rejection(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code));

    var thrown =
        assertThrows(
            SteadyStateException.class, () -> retryingClient(true, Sleeper.NO_OP).getNext());

    assertThat(MongoExceptionUtils.isThrottling(thrown)).isTrue();
    verify(this.batchCursor, times(RETRIES + 1)).tryNext();
    assertThat(retries()).isEqualTo((double) RETRIES);
    assertThat(retriesExhausted()).isEqualTo(1.0);
    assertThat(rejections(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code))
        .isEqualTo((double) RETRIES + 1);
  }

  @Test
  public void getNext_retryDisabled_failsImmediatelyAndCountsTheRejection() {
    when(this.batchCursor.tryNext())
        .thenThrow(rejection(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code));

    assertThrows(SteadyStateException.class, () -> retryingClient(false, Sleeper.NO_OP).getNext());

    verify(this.batchCursor, times(1)).tryNext();
    assertThat(
            this.meterRegistry
                .find("indexing.steadyStateChangeStream.getMoreThrottlingRetries")
                .counter())
        .isNull();
    assertThat(rejections(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code)).isEqualTo(1.0);
  }

  @Test
  public void getNext_interruptedDueToOverload_countedButNotRetried() {
    // 473 interrupts a running operation and mongod deletes the cursor, so it cannot be retried
    // in place.
    when(this.batchCursor.tryNext()).thenThrow(rejection(Errors.INTERRUPTED_DUE_TO_OVERLOAD.code));

    assertThrows(SteadyStateException.class, () -> retryingClient(true, Sleeper.NO_OP).getNext());

    verify(this.batchCursor, times(1)).tryNext();
    assertThat(retries()).isEqualTo(0.0);
    assertThat(rejections(Errors.INTERRUPTED_DUE_TO_OVERLOAD.code)).isEqualTo(1.0);
  }

  @Test
  public void getNext_throttledOpen_notRetriedAndRegistersOneSession() {
    when(this.batchMongoClient.openCursor(any(), any(), any()))
        .thenThrow(rejection(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code));

    assertThrows(SteadyStateException.class, () -> retryingClient(true, Sleeper.NO_OP).getNext());

    // A retried open would register a new session on every attempt and leak all but the last.
    verify(this.batchMongoClient, times(1)).openCursor(any(), any(), any());
    verify(this.sessionRefresher, times(1)).register(any());
    assertThat(retries()).isEqualTo(0.0);
    assertThat(rejections(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code)).isEqualTo(1.0);
  }

  @Test
  public void getNext_stopConditionMetDuringBackoff_stopsRetrying() {
    when(this.batchCursor.tryNext())
        .thenThrow(rejection(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code));
    AtomicBoolean ownerShutDown = new AtomicBoolean(false);
    var client =
        new ChangeStreamMongoCursorClient<>(
            mock(GenerationId.class),
            this.batchMongoClient,
            this.sessionRefresher,
            mock(AggregateOperationTemplate.class),
            new SimpleMeterRegistry(),
            SteadyStateException::wrapIfThrows,
            Optional.empty(),
            Optional.empty(),
            Optional.of(
                OverloadRejectionMetrics.create(
                    this.meterRegistry, OverloadRejectionMetrics.Phase.STEADY_STATE)),
            Optional.of(
                ChangeStreamThrottlingRetry.create(
                        this.meterRegistry,
                        CommonReplicationConfig.Type.DEFAULT,
                        duration -> ownerShutDown.set(true))
                    .withStopCondition(ownerShutDown::get)));

    // Shutdown leaves the client open, so the owner's stop condition ends the retry.
    assertThrows(SteadyStateException.class, client::getNext);

    verify(this.batchCursor, times(1)).tryNext();
    assertThat(retriesExhausted()).isEqualTo(0.0);
  }

  private ChangeStreamMongoCursorClient<SteadyStateException> retryingClient(
      boolean retryEnabled, Sleeper sleeper) {
    return new ChangeStreamMongoCursorClient<>(
        mock(GenerationId.class),
        this.batchMongoClient,
        this.sessionRefresher,
        mock(AggregateOperationTemplate.class),
        new SimpleMeterRegistry(),
        SteadyStateException::wrapIfThrows,
        Optional.empty(),
        Optional.empty(),
        Optional.of(
            OverloadRejectionMetrics.create(
                this.meterRegistry, OverloadRejectionMetrics.Phase.STEADY_STATE)),
        retryEnabled
            ? Optional.of(
                ChangeStreamThrottlingRetry.create(
                    this.meterRegistry, CommonReplicationConfig.Type.DEFAULT, sleeper))
            : Optional.empty());
  }

  private double retries() {
    return counter("indexing.steadyStateChangeStream.getMoreThrottlingRetries");
  }

  private double retriesExhausted() {
    return counter("indexing.steadyStateChangeStream.getMoreThrottlingRetriesExhausted");
  }

  private double counter(String name) {
    return this.meterRegistry.get(name).tag("replicationType", "DEFAULT").counter().count();
  }

  private double rejections(int code) {
    return this.meterRegistry
        .get("replication.overloadRejections")
        .tag("phase", "steadyState")
        .tag("code", String.valueOf(code))
        .counter()
        .count();
  }

  private static MongoCommandException rejection(int code) {
    return new MongoCommandException(
        new BsonDocument("ok", new BsonInt32(0))
            .append("code", new BsonInt32(code))
            .append("errmsg", new BsonString("shed")),
        new ServerAddress());
  }
}
