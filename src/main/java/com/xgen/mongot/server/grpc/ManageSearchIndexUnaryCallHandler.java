package com.xgen.mongot.server.grpc;

import static com.xgen.mongot.server.command.CommandFactoryMarker.Type.COMMAND_FACTORY;
import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.ParsedCommand;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.executors.BulkheadCommandExecutor;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FutureUtils;
import io.grpc.Status;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unary handler for {@code mongodb.BsonCommandService/manageSearchIndex}. Unlike the bidi streaming
 * {@code search}/{@code vectorSearch} methods, this is a plain single request/single response call
 * with no cursor or stream state to track.
 */
public class ManageSearchIndexUnaryCallHandler
    implements ServerCalls.UnaryMethod<RawBsonDocument, RawBsonDocument> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ManageSearchIndexUnaryCallHandler.class);

  private final CommandRegistry commandRegistry;
  private final BulkheadCommandExecutor commandExecutor;
  private final HealthManager healthManager;

  ManageSearchIndexUnaryCallHandler(
      CommandRegistry commandRegistry,
      BulkheadCommandExecutor commandExecutor,
      HealthManager healthManager) {
    this.commandRegistry = commandRegistry;
    this.commandExecutor = commandExecutor;
    this.healthManager = healthManager;
  }

  @Override
  public void invoke(RawBsonDocument request, StreamObserver<RawBsonDocument> responseObserver) {
    if (!this.healthManager.isHealthy()) {
      responseObserver.onError(Status.UNAVAILABLE.asRuntimeException());
      return;
    }

    try {
      // The first key is the command name, same convention as BsonMessageCallHandler.
      ParsedCommand parsedCommand = new ParsedCommand(request.getFirstKey(), request);
      CommandRegistry.CommandRegistration registration =
          this.commandRegistry.getCommandRegistration(parsedCommand.name());
      // manageSearchIndex must resolve to a plain command factory; session-command factories are
      // not valid on this path and have no meaning here.
      checkArg(
          registration.factory.getType() == COMMAND_FACTORY,
          "do not know how to work with the command factory of %s",
          parsedCommand.name());

      Command command = ((CommandFactory) registration.factory).create(parsedCommand.body());

      // isCancelled is used for cancelling search and vector search query, and not for search
      // index management commands. So use a false.
      this.commandExecutor
          .execute(command, () -> false)
          .whenComplete(
              (response, cause) -> {
                if (cause != null) {
                  respondWithError(responseObserver, cause);
                } else {
                  respond(responseObserver, response);
                }
              });
    } catch (Throwable t) {
      LOG.error("Got exception. ", t);
      respondWithError(responseObserver, t);
    }
  }

  private static void respond(
      StreamObserver<RawBsonDocument> responseObserver, BsonDocument response) {
    responseObserver.onNext(new RawBsonDocument(response, BsonUtils.BSON_DOCUMENT_CODEC));
    // A unary call sends exactly one response message, so onCompleted() must follow onNext()
    // to fully close the stream; without it, the client would hang indefinitely waiting for
    // more data.
    responseObserver.onCompleted();
  }

  private void respondWithError(StreamObserver<RawBsonDocument> responseObserver, Throwable cause) {
    Throwable unwrapped = FutureUtils.unwrapCause(cause);

    BsonDocument error =
        MessageUtils.createErrorBody(
            unwrapped.getMessage() == null ? "unknown error" : unwrapped.getMessage());
    respond(responseObserver, error);
  }
}
