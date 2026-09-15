package com.xgen.mongot.server.grpc;

import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.ParsedCommand;
import com.xgen.mongot.server.command.WireCommandParser;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.executors.BulkheadCommandExecutor;
import com.xgen.mongot.server.message.MessageMessage;
import io.grpc.stub.StreamObserver;
import org.bson.BsonDocument;

/**
 * A handler class to process {@link MessageMessage} for the gRPC bidirectional streaming service.
 */
public class WireMessageCallHandler extends ServerCallHandler<MessageMessage> {

  WireMessageCallHandler(
      CommandRegistry commandRegistry,
      BulkheadCommandExecutor commandExecutor,
      MongotCursorManager cursorManager,
      SearchEnvoyMetadata searchEnvoyMetadata,
      int envoyAttemptCount,
      int envoyMaxAttemptCount,
      StreamObserver<MessageMessage> responseObserver) {
    super(
        commandRegistry,
        commandExecutor,
        cursorManager,
        searchEnvoyMetadata,
        envoyAttemptCount,
        envoyMaxAttemptCount,
        responseObserver);
  }

  @Override
  ParsedCommand parseCommand(MessageMessage message) {
    return WireCommandParser.parse(message);
  }

  @Override
  MessageMessage serializeResponse(MessageMessage request, BsonDocument response) {
    return (MessageMessage) request.getOutboundMessage(response);
  }

  @Override
  MessageMessage serializeError(MessageMessage request, BsonDocument error) {
    return (MessageMessage) request.getOutboundMessage(error);
  }
}
