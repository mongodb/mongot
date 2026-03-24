package com.xgen.mongot.server.grpc;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used to manage the commands in a single gRPC stream.
 *
 * <p>Workflow:
 *
 * <ol>
 *   <li>When receiving a command from client, {@code onCommandStart} should be called.
 *   <li>When a command is completed, {@code onCommandComplete} should be called to send server
 *       response.
 *   <li>When receiving a half-close from client, {@code onHalfClosedByClient} should be called.
 *       Server will send half-close after all pending commands are replied.
 *   <li>When the stream is cancelled, {@code onStreamCancellation} should be called. Server will
 *       send half-close after all pending commands are replied. A cleanup callback will be run
 *       after server sending half-close.
 * </ol>
 *
 * <p>Concurrency and Order:
 *
 * <ol>
 *   <li>{@code onCommandStart}, {@code onHalfClosedByClient} and {@code onStreamCancellation}
 *       should not be called concurrently.
 *   <li>{@code onCommandComplete} can be called concurrently with other methods.
 *   <li>After {@code onHalfClosedByClient} is called, only {@code onCommandComplete} can be called.
 *   <li>After {@code onStreamCancellation} is called, only {@code onCommandComplete} can be called.
 * </ol>
 */
public class CommandManager<T> {
  private final StreamObserver<T> responseObserver;

  // Total number of pending `responseObserver.onNext` and `responseObserver.onCompleted` calls.
  // Server will send half-close after this number is reduced to 0.
  private final AtomicInteger numPendingResponseObserverCalls;

  // This callback will be executed after server sending half-close.
  // It can only be set in `onStreamCancellation`.
  private volatile Runnable cleanupCallback;

  private volatile boolean streamCancelled;

  CommandManager(StreamObserver<T> responseObserver) {
    this.responseObserver = responseObserver;
    // This is initialized to 1 because we need to call `responseObserver.onCompleted` after
    // receiving `onHalfClosedByClient` or `onStreamCancellation`.
    this.numPendingResponseObserverCalls = new AtomicInteger(1);
    this.streamCancelled = false;
    this.cleanupCallback =
        () -> {
          // Do nothing.
        };
  }

  void onCommandStart() {
    this.numPendingResponseObserverCalls.getAndIncrement();
  }

  // When the command fails, `replyMsg` will contain an error body.
  void onCommandComplete(T replyMsg, Runnable replySentCallback) {
    // Synchronization is required here because the `StreamObserver` is not thread-safe.
    synchronized (this) {
      try {
        this.responseObserver.onNext(replyMsg);
      } catch (StatusRuntimeException e) {
        // The RPC stream is already cancelled.
      }
    }
    replySentCallback.run();

    if (this.numPendingResponseObserverCalls.decrementAndGet() == 0) {
      sendSeverHalfCloseAndRunCleanupCallback();
    }
  }

  void onHalfClosedByClient() {
    if (this.numPendingResponseObserverCalls.decrementAndGet() == 0) {
      sendSeverHalfCloseAndRunCleanupCallback();
    }
  }

  boolean isStreamCancelled() {
    return this.streamCancelled;
  }

  void onStreamCancellation(Runnable cleanupCallback) {
    this.streamCancelled = true;
    // - If there are running commands, this callback will be triggerred by `onCommandComplete` of
    //   the last completed command.
    // - Otherwise, this callback will be triggerred in the current `onStreamCancellation` call.
    this.cleanupCallback = cleanupCallback;

    // Currently mongot doesn't implement any optimizations to cancel a running command.
    // So we still wait for the response of all pending commands before sending half-close.
    if (this.numPendingResponseObserverCalls.decrementAndGet() == 0) {
      sendSeverHalfCloseAndRunCleanupCallback();
    }
  }

  // This method is triggered exactly once:
  // - When receive half-close from client,
  //   - If there are running commands, this method will be triggerred by `onCommandComplete` of the
  //     last completed command.
  //   - Otherwise, this method will be triggerred by `onHalfClosedByClient`.
  // - When the stream is cancelled,
  //   - If there are running commands, this method will be triggerred by `onCommandComplete` of the
  //     last completed command.
  //   - Otherwise, this method will be triggerred by `onStreamCancellation`.
  private void sendSeverHalfCloseAndRunCleanupCallback() {
    // Synchronization is not necessary here because there should be no concurrent calls to the
    // responseObserver.
    try {
      this.responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      // The RPC stream is already cancelled.
    }

    this.cleanupCallback.run();
  }
}
