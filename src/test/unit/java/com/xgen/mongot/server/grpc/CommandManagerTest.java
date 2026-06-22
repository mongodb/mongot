package com.xgen.mongot.server.grpc;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

/** Unit tests for {@link CommandManager}. */
public class CommandManagerTest {

  @Test
  public void isStreamCancelled_initiallyFalse() {
    CommandManager<String> manager = new CommandManager<>(noOpObserver());
    assertThat(manager.isStreamCancelled()).isFalse();
  }

  @Test
  public void isStreamCancelled_trueAfterStreamCancellation() {
    CommandManager<String> manager = new CommandManager<>(noOpObserver());
    manager.onStreamCancellation(() -> {});
    assertThat(manager.isStreamCancelled()).isTrue();
  }

  @Test
  public void isStreamCancelled_falseAfterHalfClose() {
    CommandManager<String> manager = new CommandManager<>(noOpObserver());
    manager.onHalfClosedByClient();
    assertThat(manager.isStreamCancelled()).isFalse();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void onCommandFailure_sendsErrorToObserver() {
    StreamObserver<String> observer = mock(StreamObserver.class);
    CommandManager<String> manager = new CommandManager<>(observer);
    manager.onCommandStart();

    manager.onCommandFailure(Status.RESOURCE_EXHAUSTED.withDescription("at capacity"));

    verify(observer)
        .onError(
            org.mockito.ArgumentMatchers.argThat(
                t -> {
                  assertThat(t).isInstanceOf(StatusRuntimeException.class);
                  StatusRuntimeException sre = (StatusRuntimeException) t;
                  assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                  return true;
                }));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void onCommandFailure_triggersCleanupWhenLastCommand() {
    StreamObserver<String> observer = mock(StreamObserver.class);
    CommandManager<String> manager = new CommandManager<>(observer);
    manager.onCommandStart();

    AtomicBoolean cleanupRan = new AtomicBoolean(false);
    manager.onStreamCancellation(() -> cleanupRan.set(true));

    manager.onCommandFailure(Status.RESOURCE_EXHAUSTED);

    assertThat(cleanupRan.get()).isTrue();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void onCommandFailure_decrementsCounterWithoutCleanupWhenMorePending() {
    StreamObserver<String> observer = mock(StreamObserver.class);
    CommandManager<String> manager = new CommandManager<>(observer);
    manager.onCommandStart();
    manager.onCommandStart();

    AtomicBoolean cleanupRan = new AtomicBoolean(false);
    manager.onStreamCancellation(() -> cleanupRan.set(true));

    manager.onCommandFailure(Status.RESOURCE_EXHAUSTED);

    assertThat(cleanupRan.get()).isFalse();
  }

  /**
   * Regression test for the gRPC contract violation where onCompleted was called after onError.
   *
   * <p>gRPC treats onError and onCompleted as mutually exclusive terminal operations. Calling
   * onCompleted after onError throws IllegalStateException from the underlying ServerCall.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void onCommandFailure_doesNotCallOnCompletedWhenCleanupTriggered() {
    StreamObserver<String> observer = mock(StreamObserver.class);
    CommandManager<String> manager = new CommandManager<>(observer);
    manager.onCommandStart();
    manager.onStreamCancellation(() -> {});

    manager.onCommandFailure(Status.RESOURCE_EXHAUSTED);

    verify(observer).onError(org.mockito.ArgumentMatchers.any());
    verify(observer, never()).onCompleted();
  }

  /**
   * Regression test: after onCommandFailure, a subsequent client half-close or cancellation must
   * not trigger onCompleted on the response observer either.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void onCommandFailure_thenHalfClose_doesNotCallOnCompleted() {
    StreamObserver<String> observer = mock(StreamObserver.class);
    CommandManager<String> manager = new CommandManager<>(observer);
    manager.onCommandStart();

    // RESOURCE_EXHAUSTED failure terminates the stream with onError.
    manager.onCommandFailure(Status.RESOURCE_EXHAUSTED);

    // Client subsequently half-closes; must not trigger onCompleted.
    manager.onHalfClosedByClient();

    verify(observer).onError(org.mockito.ArgumentMatchers.any());
    verify(observer, never()).onCompleted();
  }

  /**
   * Regression test: after onCommandFailure, if the cleanup path also fires via stream
   * cancellation, the cleanup callback must still run even though onCompleted is skipped.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void onCommandFailure_thenStreamCancellation_stillRunsCleanup() {
    StreamObserver<String> observer = mock(StreamObserver.class);
    CommandManager<String> manager = new CommandManager<>(observer);
    manager.onCommandStart();

    manager.onCommandFailure(Status.RESOURCE_EXHAUSTED);

    AtomicBoolean cleanupRan = new AtomicBoolean(false);
    manager.onStreamCancellation(() -> cleanupRan.set(true));

    assertThat(cleanupRan.get()).isTrue();
    verify(observer, never()).onCompleted();
  }

  private static StreamObserver<String> noOpObserver() {
    return new StreamObserver<>() {
      @Override
      public void onNext(String value) {}

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onCompleted() {}
    };
  }
}
