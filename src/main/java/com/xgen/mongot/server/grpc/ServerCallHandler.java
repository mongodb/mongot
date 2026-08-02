package com.xgen.mongot.server.grpc;

import static com.xgen.mongot.server.command.CommandFactoryMarker.Type.COMMAND_FACTORY;
import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.base.Stopwatch;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.ParsedCommand;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.executors.BulkheadCommandExecutor;
import com.xgen.mongot.server.executors.CancelledStreamSkipException;
import com.xgen.mongot.server.executors.LoadSheddingRejectedException;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.mongodb.Errors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ServerCallHandler<T> implements StreamObserver<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ServerCallHandler.class);

  /**
   * Error labels for load shedding rejection responses. These labels follow the MongoDB wire
   * protocol convention defined in error_labels.h, allowing clients to identify transient overload
   * conditions and retry appropriately.
   */
  private static final List<String> LOAD_SHEDDING_ERROR_LABELS =
      List.of("SystemOverloadedError", "RetryableError");

  private static final int MAX_ENVOY_RETRY_ATTEMPTS = 3;

  private final CommandRegistry commandRegistry;
  private final BulkheadCommandExecutor commandExecutor;
  private final MongotCursorManager cursorManager;
  private final CommandManager<T> commandManager;
  private final SearchEnvoyMetadata searchEnvoyMetadata;
  // Envoy's per-stream `x-envoy-attempt-count` header value, extracted once at stream creation
  // in GrpcStreamingServer. SearchEnvoyMetadataInterceptor always sets this context key (with a
  // fallback of 1 on missing/malformed headers), and tests that bypass the interceptor construct
  // handlers directly with an explicit value, so a non-null int is a safe invariant here.
  private final int envoyAttemptCount;
  private final FeatureFlags featureFlags;

  // Newly created cursors in this gRPC stream.
  // 1. This variable is at most written once. Each stream will have at most one $search command.
  // 2. This variable is at most read once during the cleanup callback in `onError`. When the
  //    cleanup callback is triggered, all commands in current gRPC stream has finished.
  volatile List<Long> createdCursorIds;

  ServerCallHandler(
      CommandRegistry commandRegistry,
      BulkheadCommandExecutor commandExecutor,
      MongotCursorManager cursorManager,
      SearchEnvoyMetadata searchEnvoyMetadata,
      int envoyAttemptCount,
      FeatureFlags featureFlags,
      StreamObserver<T> responseObserver) {
    this.commandRegistry = commandRegistry;
    this.commandExecutor = commandExecutor;
    this.cursorManager = cursorManager;
    this.searchEnvoyMetadata = searchEnvoyMetadata;
    this.envoyAttemptCount = envoyAttemptCount;
    this.featureFlags = featureFlags;
    this.commandManager = new CommandManager<T>(responseObserver);
    this.createdCursorIds = Collections.emptyList();
  }

  @Override
  public void onNext(T requestMsg) {
    this.commandManager.onCommandStart();
    Stopwatch totalTime = Stopwatch.createStarted();
    HandlingContext handlingContext = new HandlingContext();
    handleMessage(handlingContext, requestMsg)
        .whenComplete(
            (replyMsg, cause) -> {
              if (cause != null) {
                Throwable unwrapped = FutureUtils.unwrapCause(cause);
                if (shouldRetryViaEnvoy(unwrapped)) {
                  // Count this as a failure from the host's perspective so load-shedding
                  // alerting/dashboards remain accurate. Envoy may retry on another host, but
                  // this host did reject the command.
                  handlingContext.commandRegistration.ifPresent(
                      registration -> registration.recordFailure(unwrapped));
                  this.commandManager.onCommandFailure(
                      Status.RESOURCE_EXHAUSTED.withDescription("Search server at capacity"));
                } else {
                  this.commandManager.onCommandComplete(
                      getErrorMessage(requestMsg, cause),
                      () -> {
                        if (!(unwrapped instanceof InterruptedException)
                            && !(unwrapped instanceof CancelledStreamSkipException)) {
                          handlingContext.commandRegistration.ifPresent(
                              registration -> registration.recordFailure(unwrapped));
                        }
                      });
                }
              } else {
                Stopwatch serializationTime = Stopwatch.createStarted();
                this.commandManager.onCommandComplete(
                    replyMsg,
                    () -> {
                      // Update metrics after the message is sent.
                      handlingContext.commandRegistration.ifPresent(
                          commandRegistration -> {
                            commandRegistration.serializationTimer.ifPresent(
                                t -> t.record(serializationTime.elapsed()));
                            commandRegistration.totalTimer.record(totalTime.elapsed());
                          });
                    });
              }
            });
  }

  CompletableFuture<T> handleMessage(HandlingContext handlingContext, T request) {
    try {
      ParsedCommand parsedCommand = parseCommand(request);
      CommandRegistry.CommandRegistration registration =
          this.commandRegistry.getCommandRegistration(parsedCommand.name());
      handlingContext.commandRegistration = Optional.of(registration);

      // Session commands are supposed to be handled by the Envoy proxy instead of the gRPC
      // server.
      checkArg(
          registration.factory.getType() == COMMAND_FACTORY,
          "do not know how to work with the command factory of %s",
          parsedCommand.name());

      // We don't check registration.isSecure here because the gRPC server will leverage mTLS
      // instead.
      Command command = ((CommandFactory) registration.factory).create(parsedCommand.body());

      // If this command depends on cursors but no cursors are created, throws an error.
      if (command.dependOnCursors() && this.createdCursorIds.isEmpty()) {
        throw new IllegalStateException("gRPC stream is broken");
      }
      command.handleSearchEnvoyMetadata(this.searchEnvoyMetadata);

      return this.commandExecutor
          .execute(command, this.commandManager::isStreamCancelled)
          .thenApply(
              response -> {
                // If new cursors are created during command execution, track them.
                var createdCursorIds = command.getCreatedCursorIds();
                if (!createdCursorIds.isEmpty()) {
                  this.createdCursorIds = createdCursorIds;
                }
                return serializeResponse(request, response);
              });
    } catch (Throwable t) {
      return CompletableFuture.failedFuture(t);
    }
  }

  @Override
  public void onError(Throwable t) {
    this.commandManager.onStreamCancellation(
        () -> {
          // After sending half-close to the client, we will try to kill all the cursors that are
          // created in the gRPC stream.
          // Cursors may already be killed/exhausted. If a cursor is killed/exhausted,
          // `MongotCursorManager::killCursor` will be a no-op.
          this.createdCursorIds.forEach(
              cursorId -> {
                this.cursorManager.killCursor(cursorId);
              });
        });
  }

  @Override
  public void onCompleted() {
    this.commandManager.onHalfClosedByClient();
  }

  abstract ParsedCommand parseCommand(T message);

  abstract T serializeResponse(T request, BsonDocument response);

  private T getErrorMessage(T request, Throwable exception) {
    Throwable cause = FutureUtils.unwrapCause(exception);

    // Load shedding rejection should include error code and labels for client retry handling
    if (cause instanceof LoadSheddingRejectedException) {
      String message = cause.getMessage() == null ? "Server is at capacity" : cause.getMessage();
      BsonDocument error =
          MessageUtils.createErrorBodyWithLabels(
              message, LOAD_SHEDDING_ERROR_LABELS, Errors.SEARCH_REQUEST_REJECTED_DUE_TO_OVERLOAD);
      return serializeError(request, error);
    }

    if (!(cause instanceof InvalidQueryException)
        && !(cause instanceof CancelledStreamSkipException)) {
      LOG.warn("unexpected exception", cause);
    }

    BsonDocument error =
        MessageUtils.createErrorBody(
            cause.getMessage() == null ? "unknown error" : cause.getMessage());

    return serializeError(request, error);
  }

  abstract T serializeError(T request, BsonDocument error);

  private boolean shouldRetryViaEnvoy(Throwable cause) {
    if (!(cause instanceof LoadSheddingRejectedException)) {
      return false;
    }
    if (!this.featureFlags.isEnabled(Feature.OVERLOAD_RETRY_SIGNAL)) {
      return false;
    }
    return this.envoyAttemptCount < MAX_ENVOY_RETRY_ATTEMPTS;
  }

  static class HandlingContext {
    // After command execution, corresponding metrics in the following registration will be updated.
    Optional<CommandRegistry.CommandRegistration> commandRegistration = Optional.empty();
  }
}
