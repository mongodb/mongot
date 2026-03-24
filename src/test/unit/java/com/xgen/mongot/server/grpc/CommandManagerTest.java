package com.xgen.mongot.server.grpc;

import static com.google.common.truth.Truth.assertThat;

import io.grpc.stub.StreamObserver;
import org.junit.Test;

/** Unit tests for {@link CommandManager} stream cancellation tracking. */
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
