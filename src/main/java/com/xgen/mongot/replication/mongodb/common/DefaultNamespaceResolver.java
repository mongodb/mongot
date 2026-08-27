package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkState;

import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.util.mongodb.CollectionInfo;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.bson.BsonDocument;

/**
 * DefaultNamespaceResolver implements NamespaceResolver by interfacing with a MongoDB deployment
 * via a MongoClient.
 */
public class DefaultNamespaceResolver implements NamespaceResolver {

  private final MongoClient mongoClient;

  public DefaultNamespaceResolver(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  @Override
  public MongoDbCollectionInfo resolveAndUpdateCollectionName(IndexDefinition indexDefinition)
      throws NamespaceResolutionException, InterruptedException, InitialSyncException {
    try {
      MongoDatabase database =
          this.mongoClient
              .getDatabase(indexDefinition.getDatabase())
              .withReadConcern(ReadConcern.MAJORITY);

      List<MongoDbCollectionInfo> collectionInfos =
          StreamSupport.stream(
                  database
                      .listCollections(BsonDocument.class)
                      .filter(CollectionInfo.uuidFilter(indexDefinition.getCollectionUuid()))
                      .spliterator(),
                  false)
              .map(MongoDbCollectionInfo::fromBsonDocument)
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toList());

      if (collectionInfos.isEmpty()) {
        throw NamespaceResolutionException.create();
      }

      checkState(
          collectionInfos.size() == 1,
          "found multiple collections in database %s with UUID %s",
          indexDefinition.getDatabase(),
          indexDefinition.getCollectionUuid());

      MongoDbCollectionInfo info = collectionInfos.get(0);
      indexDefinition.setLastObservedCollectionName(info.name());
      return info;
    } catch (MongoInterruptedException e) {
      throw new InterruptedException();
    } catch (MongoException e) {
      throw InitialSyncException.createResumableTransient(e);
    }
  }

  @Override
  public boolean isCollectionNameChanged(
      IndexDefinition indexDefinition, String expectedCollectionName)
      throws NamespaceResolutionException, InterruptedException, InitialSyncException {
    // TODO(CLOUDP-280897): is the following scenario possible? If so, is it avoidable?
    //       - want to open against UUID("1234-..."), collection name "original"
    //       - "original" renamed to "original-prime"
    //       - new collection named "original" created
    //       - open change stream against incorrect "original"
    //       - incorrect "original" renamed to "fake" (which the change stream is open against)
    //       - original "original" (now "original-prime") renamed to "original"
    //       - check to see that UUID("1234-...") still resolves to "original"
    //       We'll have to see an invalidation event at some point, but it's possible that prior
    //       to that invalidation event stopAfterOpTime() is called, and we stop buffering
    //       events.
    MongoDbCollectionInfo resolvedCollectionInfo =
        this.resolveAndUpdateCollectionName(indexDefinition);
    return !resolvedCollectionInfo.name().equals(expectedCollectionName);
  }
}
