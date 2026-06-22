package com.xgen.mongot.server.command.search;

import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.search.definition.request.KillCursorsCommandDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.serialization.CodecRegistry;
import com.xgen.mongot.util.mongodb.serialization.KillCursorsResponseProxy;
import java.util.Collections;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KillCursorsCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(KillCursorsCommand.class);

  private final KillCursorsCommandDefinition definition;
  private final MongotCursorManager cursorManager;

  private KillCursorsCommand(
      KillCursorsCommandDefinition definition, MongotCursorManager cursorManager) {
    this.definition = definition;
    this.cursorManager = cursorManager;
  }

  @Override
  public String name() {
    return KillCursorsCommandDefinition.NAME;
  }

  @Override
  public BsonDocument run() {
    LOG.atTrace().addKeyValue("command", KillCursorsCommandDefinition.NAME).log("Received command");
    this.definition.cursorIds.forEach(this.cursorManager::killCursor);

    return new KillCursorsResponseProxy(
            1.0,
            this.definition.cursorIds,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList())
        .toBsonDocument(BsonDocument.class, CodecRegistry.PACKAGE_CODEC_REGISTRY);
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    // this command is mutually exclusive with getMores so it could be blocking.
    return ExecutionPolicy.ASYNC;
  }

  @Override
  public boolean dependOnCursors() {
    return true;
  }

  @Override
  public boolean maybeLoadShed() {
    return false;
  }

  public static class Factory implements CommandFactory {

    private final MongotCursorManager cursorManager;

    public Factory(MongotCursorManager cursorManager) {
      this.cursorManager = cursorManager;
    }

    @Override
    public Command create(BsonDocument args) {
      try (var parser = BsonDocumentParser.fromRoot(args).allowUnknownFields(true).build()) {
        KillCursorsCommandDefinition definition = KillCursorsCommandDefinition.fromBson(parser);
        return new KillCursorsCommand(definition, this.cursorManager);
      } catch (BsonParseException e) {
        // we have no way of throwing checked exceptions beyond this method
        // (called directly by opmsg)
        throw new IllegalArgumentException(e.getMessage());
      }
    }
  }
}
