package com.xgen.mongot.replication.mongodb.initialsync;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.IndexMetricsUpdater.ReplicationMetricsUpdater.InitialSyncMetrics;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import com.xgen.mongot.util.mongodb.CollectionScanAggregateCommand;
import com.xgen.mongot.util.mongodb.CollectionScanFindCommand;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

interface InitialSyncMongoClient {

  /**
   * Ensures that the collection name corresponding to the Index's defined collection UUID is still
   * the expected collection name, throwing an InitialSyncException if not.
   */
  void ensureCollectionNameUnchanged(IndexDefinition indexDefinition, String expectedCollectionName)
      throws InitialSyncException;

  String resolveAndUpdateCollectionName(IndexDefinition indexDefinition)
      throws InitialSyncException;

  /**
   * Retrieves the majority-committed operation time from the mongod. See mongod documentation at
   * https://docs.mongodb.com/manual/reference/command/replSetGetStatus/#mongodb-data-replSetGetStatus.optimes.readConcernMajorityOpTime
   * for more details.
   *
   * @throws InitialSyncException if unable to retrieve a valid optime.
   */
  BsonTimestamp getMaxValidMajorityReadOptime() throws InitialSyncException;

  BsonDocument fsync();

  /**
   * Returns a ChangeStreamMongoClient to use. Note that this must be called once and the returned
   * ChangeStreamMongoClient must be used throughout reading from the change stream, rather than
   * repeated calls to InitialSyncMongoClient::getChangeStreamMongoClient.
   */
  ChangeStreamMongoClient<InitialSyncException> getChangeStreamMongoClient(
      ChangeStreamAggregateCommand aggregateCommand,
      MongoNamespace namespace,
      InitialSyncMetrics initialSyncMetricsUpdater,
      Optional<Integer> batchSize,
      GenerationId generationId)
      throws InitialSyncException;

  /**
   * Returns a FindCommandMongoClient to use. Note that this must be called once and the returned
   * FindCommandMongoClient must be used across the collection scan, rather than repeated calls to
   * InitialSyncMongoClient::getFindCommandMongoClient.
   *
   * <p>This client will also verify that the collection name was unchanged.
   *
   * <p>Find and getMore use server default batching (no {@code batchSize} on the wire).
   */
  CollectionScanMongoClient<InitialSyncException> getCollectionFindCommandMongoClient(
      CollectionScanFindCommand findCommand,
      IndexDefinition indexDefinition,
      InitialSyncMetrics initialSyncMetricsUpdater)
      throws InitialSyncException;

  CollectionScanMongoClient<InitialSyncException> getCollectionAggregateCommandMongoClient(
      CollectionScanAggregateCommand findCommand,
      IndexDefinition indexDefinition,
      InitialSyncMetrics initialSyncMetricsUpdater,
      Optional<Integer> collectionScanGetMoreBatchSize)
      throws InitialSyncException;

  /**
   * Returns a find based collection scan client for auto-embedding resync. This client is expected
   * to point to the materialized view collection instead of the source collection.
   */
  CollectionScanMongoClient<InitialSyncException> getAutoEmbeddingResyncMongoClient(
      CollectionScanFindCommand findCommand,
      IndexDefinition indexDefinition,
      MongoNamespace namespace,
      InitialSyncMetrics initialSyncMetricsUpdater)
      throws InitialSyncException;

  String getSyncSourceHost();
}
