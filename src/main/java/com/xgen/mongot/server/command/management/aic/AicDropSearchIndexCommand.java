package com.xgen.mongot.server.command.management.aic;

import static com.xgen.mongot.server.command.management.definition.common.CommonDefinitions.OK_RESPONSE;

import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.AuthoritativeIndexKey;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.management.definition.DropSearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserViewDefinition;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.mongodb.Errors;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AicDropSearchIndexCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(AicDropSearchIndexCommand.class);

  private final AuthoritativeIndexCatalog authoritativeIndexCatalog;

  private final String db;

  private final String collectionName;

  private final UUID collectionUuid;

  private final Optional<UserViewDefinition> view;

  private final DropSearchIndexCommandDefinition definition;

  AicDropSearchIndexCommand(
      AuthoritativeIndexCatalog authoritativeIndexCatalog,
      String db,
      UUID collectionUuid,
      String collectionName,
      Optional<UserViewDefinition> view,
      DropSearchIndexCommandDefinition definition) {
    this.authoritativeIndexCatalog = authoritativeIndexCatalog;
    this.db = db;
    this.collectionUuid = collectionUuid;
    this.collectionName = collectionName;
    this.view = view;
    this.definition = definition;
  }

  @Override
  public String name() {
    return DropSearchIndexCommandDefinition.NAME;
  }

  @Override
  public boolean maybeLoadShed() {
    return false;
  }

  @Override
  public BsonDocument run() {
    // This should be validated by command deserialization, but do a sanity check here.
    Check.exactlyOneOf(
        "Expected one of index name or ID", this.definition.name(), this.definition.id());
    String idType = this.definition.name().isPresent() ? "name" : "id";
    Object idValue =
        this.definition.name().map(Object.class::cast).or(this.definition::id).orElseThrow();

    // Only one of indexName and indexId will be added to the log attributes. Keys with empty
    // optionals as values will be omitted.
    LOG.atInfo()
        .addKeyValue("command", DropSearchIndexCommandDefinition.NAME)
        .addKeyValue("indexName", this.definition.name())
        .addKeyValue("indexId", this.definition.id())
        .log("Received command");

    try {
      List<IndexDefinition> indexesToDrop =
          this.authoritativeIndexCatalog.listIndexDefinitions(this.collectionUuid).stream()
              .filter(
                  index ->
                      this.definition.name().map(index.getName()::equals).orElse(true)
                          && this.definition.id().map(index.getIndexId()::equals).orElse(true))
              .toList();

      if (indexesToDrop.isEmpty()) {
        return MessageUtils.createError(
            Errors.INDEX_NOT_FOUND,
            String.format(
                "No index with %s %s exists in namespace %s.%s",
                idType,
                idValue,
                this.db,
                this.view.map(UserViewDefinition::name).orElse(this.collectionName)));
      }

      CheckedStream.from(indexesToDrop)
          .forEachChecked(
              idx ->
                  this.authoritativeIndexCatalog.deleteIndex(
                      new AuthoritativeIndexKey(this.collectionUuid, idx.getName())));
    } catch (MetadataServiceException e) {
      return MessageUtils.createError(Errors.COMMAND_FAILED, e.getMessage());
    } catch (Exception e) {
      String message = Objects.requireNonNullElse(e.getMessage(), "unknown error");
      return MessageUtils.createError(Errors.COMMAND_FAILED, message);
    }

    return OK_RESPONSE;
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.ASYNC;
  }
}
