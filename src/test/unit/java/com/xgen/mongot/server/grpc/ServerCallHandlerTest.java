package com.xgen.mongot.server.grpc;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorManagerImpl;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.QuarantinedIndexException;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.command.search.GetMoreCommand;
import com.xgen.mongot.server.command.search.KillCursorsCommand;
import com.xgen.mongot.server.executors.BulkheadCommandExecutor;
import com.xgen.mongot.server.executors.LoadSheddingRejectedException;
import com.xgen.mongot.server.executors.RegularBlockingRequestSettings;
import com.xgen.testing.mongot.index.query.OperatorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Covers {@link ServerCallHandler}'s choice between terminating a failed command with a retryable
 * gRPC status, so search envoy fails over to a peer, and replying with the in-band error body that
 * mongod surfaces to the user.
 */
public class ServerCallHandlerTest {

  private static final String PARSE_ERROR =
      "Index 'foo' is unavailable: its definition cannot be parsed (bad operator).";

  private static final RawBsonDocument REQUEST =
      new RawBsonDocument(new BsonDocument(), new BsonDocumentCodec());

  /**
   * A handler whose command execution always fails with {@code failure}, so a test can drive {@code
   * onNext}'s error dispatch without standing up a real command.
   */
  private static class FailingCallHandler extends BsonMessageCallHandler {

    private final Throwable failure;

    FailingCallHandler(
        BulkheadCommandExecutor commandExecutor,
        int envoyAttemptCount,
        int envoyMaxAttemptCount,
        boolean isShadow,
        boolean envoyInRequestPath,
        StreamObserver<RawBsonDocument> responseObserver,
        Throwable failure) {
      super(
          CommandRegistry.create(new SimpleMeterRegistry()),
          commandExecutor,
          mock(MongotCursorManager.class),
          SearchEnvoyMetadata.getDefaultInstance(),
          envoyAttemptCount,
          envoyMaxAttemptCount,
          isShadow,
          envoyInRequestPath,
          responseObserver);
      this.failure = failure;
    }

    @Override
    CompletableFuture<RawBsonDocument> handleMessage(
        HandlingContext handlingContext, RawBsonDocument request) {
      return CompletableFuture.failedFuture(this.failure);
    }
  }

  private static BulkheadCommandExecutor executor(boolean shouldRetryViaEnvoy) {
    return new BulkheadCommandExecutor(
        new SimpleMeterRegistry(),
        RegularBlockingRequestSettings.defaults(),
        FeatureFlags.getDefault(),
        DynamicFeatureFlagRegistry.empty(),
        Optional.empty(),
        shouldRetryViaEnvoy);
  }

  @SuppressWarnings("unchecked")
  private static StreamObserver<RawBsonDocument> mockObserver() {
    return mock(StreamObserver.class);
  }

  private static Status capturedStatus(StreamObserver<RawBsonDocument> observer) {
    ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
    verify(observer).onError(captor.capture());
    return ((StatusRuntimeException) captor.getValue()).getStatus();
  }

  private static RawBsonDocument capturedReply(StreamObserver<RawBsonDocument> observer) {
    ArgumentCaptor<RawBsonDocument> captor = ArgumentCaptor.forClass(RawBsonDocument.class);
    verify(observer).onNext(captor.capture());
    return captor.getValue();
  }

  private MongotCursorManagerImpl cursorManager;

  @Before
  public void setUpCursorManager() {
    this.cursorManager =
        MongotCursorManagerImpl.fromConfig(
            CursorConfig.getDefault(),
            new SimpleMeterRegistry(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog());
  }

  @After
  public void closeCursorManager() {
    this.cursorManager.close();
  }

  private BsonMessageCallHandler handlerWithOwnedCursors(
      List<Long> ownedCursorIds, StreamObserver<RawBsonDocument> observer) {
    var handler =
        new BsonMessageCallHandler(
            registryWithCursorCommands(this.cursorManager),
            executor(false),
            this.cursorManager,
            SearchEnvoyMetadata.getDefaultInstance(),
            1,
            3,
            false,
            false,
            observer);
    handler.createdCursorIds = List.copyOf(ownedCursorIds);
    return handler;
  }

  private static CommandRegistry registryWithCursorCommands(MongotCursorManagerImpl cursorManager) {
    return CommandRegistry.create(new SimpleMeterRegistry())
        .registerCommand(
            "getMore",
            new GetMoreCommand.Factory(cursorManager, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT),
            true)
        .registerCommand("killCursors", new KillCursorsCommand.Factory(cursorManager), true);
  }

  private long newCursor() throws Exception {
    return this.cursorManager.newCursor(
            "test",
            "collection",
            UUID.randomUUID(),
            Optional.empty(),
            query(),
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty())
        .cursorId;
  }

  private static Query query() {
    return OperatorQueryBuilder.builder()
        .index(Query.Fields.INDEX.getDefaultValue())
        .operator(OperatorBuilder.term().path("title").query("godfather").build())
        .returnStoredSource(false)
        .build();
  }

  private static RawBsonDocument request(BsonDocument command) {
    return new RawBsonDocument(command, new BsonDocumentCodec());
  }

  private static RawBsonDocument awaitReply(StreamObserver<RawBsonDocument> observer) {
    ArgumentCaptor<RawBsonDocument> captor = ArgumentCaptor.forClass(RawBsonDocument.class);
    verify(observer, timeout(5000)).onNext(captor.capture());
    return captor.getValue();
  }

  private static BsonDocument getMoreRequest(long cursorId) {
    return new BsonDocument("getMore", new BsonInt64(cursorId));
  }

  private static BsonDocument killCursorsRequest(long... cursorIds) {
    var cursors = new BsonArray();
    for (long cursorId : cursorIds) {
      cursors.add(new BsonInt64(cursorId));
    }
    return new BsonDocument()
        .append("killCursors", new BsonInt64(cursorIds[0]))
        .append("cursors", cursors);
  }

  // The strongest proof that a rejected command did not touch a cursor: the cursor still serves
  // its owner through its own stream.
  private void assertCursorAlive(long cursorId) throws Exception {
    var observer = mockObserver();
    var handler = handlerWithOwnedCursors(List.of(cursorId), observer);

    handler.onNext(request(getMoreRequest(cursorId)));

    assertThat(awaitReply(observer).getNumber("ok").intValue()).isEqualTo(1);
  }

  @Test
  public void onNext_quarantinedIndexBehindEnvoy_failsStreamWithUnavailable() {
    var observer = mockObserver();
    var handler =
        new FailingCallHandler(
            executor(/* shouldRetryViaEnvoy */ false),
            /* envoyAttemptCount */ 1,
            /* envoyMaxAttemptCount */ 3,
            /* isShadow */ false,
            /* envoyInRequestPath */ true,
            observer,
            new QuarantinedIndexException(PARSE_ERROR));

    handler.onNext(REQUEST);

    assertThat(capturedStatus(observer).getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    verify(observer, never()).onNext(any());
  }

  @Test
  public void onNext_quarantinedIndexWithNoEnvoy_repliesWithInBandError() {
    var observer = mockObserver();
    var handler =
        new FailingCallHandler(
            executor(/* shouldRetryViaEnvoy */ true),
            /* envoyAttemptCount */ 1,
            /* envoyMaxAttemptCount */ 3,
            /* isShadow */ false,
            /* envoyInRequestPath */ false,
            observer,
            new QuarantinedIndexException(PARSE_ERROR));

    handler.onNext(REQUEST);

    assertThat(capturedReply(observer).getInt32("ok").getValue()).isEqualTo(0);
    assertThat(capturedReply(observer).getString("errmsg").getValue()).isEqualTo(PARSE_ERROR);
    verify(observer, never()).onError(any());
  }

  @Test
  public void onNext_quarantinedIndexOnFinalEnvoyAttempt_repliesWithInBandError() {
    var observer = mockObserver();
    var handler =
        new FailingCallHandler(
            executor(/* shouldRetryViaEnvoy */ false),
            /* envoyAttemptCount */ 3,
            /* envoyMaxAttemptCount */ 3,
            /* isShadow */ false,
            /* envoyInRequestPath */ true,
            observer,
            new QuarantinedIndexException(PARSE_ERROR));

    handler.onNext(REQUEST);

    assertThat(capturedReply(observer).getString("errmsg").getValue()).isEqualTo(PARSE_ERROR);
    verify(observer, never()).onError(any());
  }

  @Test
  public void onNext_indexUnavailableForOtherReasons_repliesWithInBandError() {
    var observer = mockObserver();
    var handler =
        new FailingCallHandler(
            executor(/* shouldRetryViaEnvoy */ true),
            /* envoyAttemptCount */ 1,
            /* envoyMaxAttemptCount */ 3,
            /* isShadow */ false,
            /* envoyInRequestPath */ true,
            observer,
            new IndexUnavailableException("Index 'foo' is not ready to be queried"));

    handler.onNext(REQUEST);

    verify(observer).onNext(any());
    verify(observer, never()).onError(any());
  }

  @Test
  public void onNext_loadSheddingWithFlagOn_failsStreamWithResourceExhausted() {
    var observer = mockObserver();
    var handler =
        new FailingCallHandler(
            executor(/* shouldRetryViaEnvoy */ true),
            /* envoyAttemptCount */ 1,
            /* envoyMaxAttemptCount */ 3,
            /* isShadow */ false,
            /* envoyInRequestPath */ false,
            observer,
            new LoadSheddingRejectedException("Server is at capacity"));

    handler.onNext(REQUEST);

    assertThat(capturedStatus(observer).getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    verify(observer, never()).onNext(any());
  }

  @Test
  public void onNext_loadSheddingWithFlagOff_repliesWithInBandError() {
    var observer = mockObserver();
    var handler =
        new FailingCallHandler(
            executor(/* shouldRetryViaEnvoy */ false),
            /* envoyAttemptCount */ 1,
            /* envoyMaxAttemptCount */ 3,
            /* isShadow */ false,
            /* envoyInRequestPath */ true,
            observer,
            new LoadSheddingRejectedException("Server is at capacity"));

    handler.onNext(REQUEST);

    verify(observer).onNext(any());
    verify(observer, never()).onError(any());
  }

  @Test
  public void onNext_getMore_ownedCursor_returnsBatch() throws Exception {
    long cursorId = newCursor();
    var observer = mockObserver();
    var handler = handlerWithOwnedCursors(List.of(cursorId), observer);

    handler.onNext(request(getMoreRequest(cursorId)));

    var reply = awaitReply(observer);
    assertThat(reply.getNumber("ok").intValue()).isEqualTo(1);
    // The empty cursor is exhausted on its first batch, so the reply reports the exhausted marker.
    assertThat(reply.getDocument("cursor").getInt64("id").getValue()).isEqualTo(0L);
    assertThat(reply.getDocument("cursor").getString("ns").getValue()).isEqualTo("test.collection");
  }

  @Test
  public void onNext_getMore_foreignCursor_rejectsAndLeavesCursorIntact() throws Exception {
    long victimCursorId = newCursor();
    long attackersOwnCursorId = newCursor();
    var observer = mockObserver();
    var handler = handlerWithOwnedCursors(List.of(attackersOwnCursorId), observer);

    handler.onNext(request(getMoreRequest(victimCursorId)));

    var reply = awaitReply(observer);
    assertThat(reply.getNumber("ok").intValue()).isEqualTo(0);
    assertThat(reply.getString("errmsg").getValue())
        .isEqualTo("getMore addresses a cursor that was not created by this gRPC stream");

    // The guard rejects before the command runs; the victim's cursor still serves its own stream.
    assertCursorAlive(victimCursorId);
  }

  @Test
  public void onNext_killCursors_ownedCursor_killsCursor() throws Exception {
    long cursorId = newCursor();
    var observer = mockObserver();
    var handler = handlerWithOwnedCursors(List.of(cursorId), observer);

    handler.onNext(request(killCursorsRequest(cursorId)));

    assertThat(awaitReply(observer).getNumber("ok").intValue()).isEqualTo(1);
  }

  @Test
  public void onNext_killCursors_foreignCursor_rejectsWithoutKilling() throws Exception {
    long victimCursorId = newCursor();
    long attackersOwnCursorId = newCursor();
    var observer = mockObserver();
    var handler = handlerWithOwnedCursors(List.of(attackersOwnCursorId), observer);

    handler.onNext(request(killCursorsRequest(victimCursorId)));

    var reply = awaitReply(observer);
    assertThat(reply.getNumber("ok").intValue()).isEqualTo(0);
    assertThat(reply.getString("errmsg").getValue())
        .isEqualTo("killCursors addresses a cursor that was not created by this gRPC stream");

    // The rejection must not have destroyed the victim's cursor.
    assertCursorAlive(victimCursorId);
  }

  @Test
  public void onNext_killCursors_mixedOwnedAndForeignIds_rejectsAll() throws Exception {
    long victimCursorId = newCursor();
    long attackersOwnCursorId = newCursor();
    var observer = mockObserver();
    var handler = handlerWithOwnedCursors(List.of(attackersOwnCursorId), observer);

    handler.onNext(request(killCursorsRequest(victimCursorId, attackersOwnCursorId)));

    var reply = awaitReply(observer);
    assertThat(reply.getNumber("ok").intValue()).isEqualTo(0);
    assertThat(reply.getString("errmsg").getValue())
        .isEqualTo("killCursors addresses a cursor that was not created by this gRPC stream");

    // All-or-nothing: the rejected batch must have destroyed neither the foreign victim's cursor
    // nor the attacker's own cursor.
    assertCursorAlive(victimCursorId);
    assertCursorAlive(attackersOwnCursorId);
  }
}
