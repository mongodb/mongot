package com.xgen.mongot.server.command.builtin;

import com.mongodb.AuthenticationMechanism;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.message.MessageLimits;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;

public class HelloCommand implements Command {

  public static final String NAME = "hello";
  public static final CommandFactory FACTORY = (ignored) -> new HelloCommand();

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public BsonDocument run() {
    return new BsonDocument()
        // isWriteablePrimary is the equivalent of the ismaster field we currently set
        // in the isMaster command response
        .append("isWritablePrimary", BsonBoolean.TRUE)
        .append("maxBsonObjectSize", new BsonInt32(WireProtocolCapabilities.MAX_BSON_OBJECT_SIZE))
        .append("maxMessageSizeBytes", new BsonInt32(MessageLimits.MAX_MESSAGE_SIZE_BYTES))
        .append("maxWriteBatchSize", new BsonInt32(WireProtocolCapabilities.MAX_WRITE_BATCH_SIZE))
        .append("localTime", new BsonInt64(System.currentTimeMillis()))
        .append("minWireVersion", new BsonInt32(WireProtocolCapabilities.MIN_WIRE_VERSION))
        .append("maxWireVersion", new BsonInt32(WireProtocolCapabilities.MAX_WIRE_VERSION))
        .append("readOnly", BsonBoolean.FALSE)
        .append(
            "saslSupportedMechs",
            new BsonArray(
                WireProtocolCapabilities.SASL_SUPPORTED_MECHANISMS.stream()
                    .map(AuthenticationMechanism::getMechanismName)
                    .map(BsonString::new)
                    .collect(Collectors.toList())))
        .append("ok", new BsonInt32(1));
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.SYNC;
  }
}
