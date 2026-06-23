package com.xgen.mongot.catalogservice;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MoreCollectors;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.catalogservice.schema.ServerStateEntry;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultServerState implements ServerState {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultServerState.class);

  @VisibleForTesting protected static final String COLLECTION_NAME = "serverState";

  private final MetadataClient<ServerStateEntry> client;

  private DefaultServerState(MetadataClient<ServerStateEntry> client) {
    this.client = client;
  }

  @Override
  public void insert(ServerStateEntry serverState) throws MetadataServiceException {
    this.client.insert(serverState);
  }

  @Override
  public void upsert(ServerStateEntry serverState) throws MetadataServiceException {
    this.client.replace(ServerStateEntry.serverIdFilter(serverState.serverId()), serverState, true);
  }

  @Override
  public boolean updateOne(ObjectId serverId, BsonDocument update) throws MetadataServiceException {
    return this.client
            .updateOne(ServerStateEntry.serverIdFilter(serverId), update)
            .getMatchedCount()
        == 1;
  }

  @Override
  public void delete(ObjectId serverId) throws MetadataServiceException {
    this.client.delete(ServerStateEntry.serverIdFilter(serverId));
  }

  @Override
  public List<ServerStateEntry> list(BsonDocument filter) throws MetadataServiceException {
    return this.client.list(filter).stream()
        .flatMap(
            d -> {
              try {
                return Stream.of(ServerStateEntry.fromBson(BsonDocumentParser.fromRoot(d).build()));
              } catch (BsonParseException e) {
                LOG.atWarn()
                    .addKeyValue("document", d)
                    .setCause(e)
                    .log("Ignoring unparsed document from Server Status Catalog");
                return Stream.empty();
              }
            })
        .toList();
  }

  @Override
  public List<ServerStateEntry> list() throws MetadataServiceException {
    return list(new BsonDocument());
  }

  @Override
  public Optional<ServerStateEntry> get(ObjectId serverId) throws MetadataServiceException {
    return list(ServerStateEntry.serverIdFilter(serverId)).stream()
        .collect(MoreCollectors.toOptional());
  }

  public static DefaultServerState create(MongoClient mongoClient) {
    return new DefaultServerState(new MetadataClient<>(mongoClient, COLLECTION_NAME));
  }

  @VisibleForTesting
  static DefaultServerState createForTesting(MongoClient client, String databaseName) {
    return new DefaultServerState(new MetadataClient<>(client, databaseName, COLLECTION_NAME));
  }
}
