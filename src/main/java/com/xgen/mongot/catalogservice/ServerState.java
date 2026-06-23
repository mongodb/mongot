package com.xgen.mongot.catalogservice;

import com.xgen.mongot.catalogservice.schema.ServerStateEntry;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public interface ServerState {

  /** Inserts a new server state entry into the metadata collection */
  void insert(ServerStateEntry serverState) throws MetadataServiceException;

  /**
   * Inserts a server state entry into the collection if it doesn't exist, otherwise replaces the
   * existing server state entry with the provided one.
   *
   * <p>Uses the serverId as the filter to find matches so if there is an entry with the same
   * serverId but different serverName it will replace the previous entry with the new one.
   *
   * @param serverState server state entry to upsert into the collection.
   */
  void upsert(ServerStateEntry serverState) throws MetadataServiceException;

  /**
   * Updates the given server's ServerStateEntry with the provided update command.
   *
   * @param serverId the server to update
   * @param update bson update expression
   * @return true if the filter matched at least one server id
   * @throws MetadataServiceException if there was an error updating the document.
   */
  boolean updateOne(ObjectId serverId, BsonDocument update) throws MetadataServiceException;

  /**
   * Deletes a server state entry from the metadata collection.
   *
   * @param serverId the id of the server to delete
   * @throws MetadataServiceException if there was an error deleting the entry
   */
  void delete(ObjectId serverId) throws MetadataServiceException;

  /**
   * Gets a single ServerStateEntry if it exists in metadata.
   *
   * @param serverId the server id for the server state entry
   * @return a server state entry if it exists
   * @throws MetadataServiceException if there was an error calling metadata
   */
  Optional<ServerStateEntry> get(ObjectId serverId) throws MetadataServiceException;

  /** Lists the server state entries from the metadata collection based on a filter */
  List<ServerStateEntry> list(BsonDocument filter) throws MetadataServiceException;

  /** Lists all server state entries from the metadata collection */
  List<ServerStateEntry> list() throws MetadataServiceException;
}
