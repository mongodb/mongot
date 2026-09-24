package com.xgen.mongot.server.grpc;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.QuarantinedIndexException;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.executors.BulkheadCommandExecutor;
import com.xgen.mongot.server.executors.LoadSheddingRejectedException;
import com.xgen.mongot.server.executors.RegularBlockingRequestSettings;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
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
}
