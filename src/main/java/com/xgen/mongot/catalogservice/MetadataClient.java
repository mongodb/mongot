package com.xgen.mongot.catalogservice;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

/**
 * The MetadataClient exposes all the functionality required to perform CRUD queries on any of the
 * internal mongod metadata collections.
 */
public class MetadataClient<T extends DocumentEncodable> {
  public static final String DATABASE_NAME = "__mdb_internal_search";

  private final MongoClient mongoClient;
  private final String databaseName;
  private final String collectionName;

  public MetadataClient(MongoClient mongoClient, String collectionName) {
    this(mongoClient, DATABASE_NAME, collectionName);
  }

  @VisibleForTesting
  MetadataClient(MongoClient mongoClient, String databaseName, String collectionName) {
    this.mongoClient = mongoClient;
    this.databaseName = databaseName;
    this.collectionName = collectionName;
  }

  /**
   * Inserts the document into the metadata collection.
   *
   * @param document the document to be inserted
   * @throws MetadataServiceException if there was an error persisting the entry to the collection,
   *     for example, if an index with the same ID already exists in the collection
   */
  public synchronized void insert(T document) throws MetadataServiceException {
    MetadataServiceException.wrapIfThrows(() -> this.getCollection().insertOne(document.toBson()));
  }

  /**
   * Replaces an existing index entry in the collection with a new value.
   *
   * @param filter filter for the document to replace
   * @param document the document to be inserted
   * @param upsert if this should be an upsert or a strict replacement
   * @throws MetadataServiceException if there was an error persisting the entry to the collection
   */
  public synchronized void replace(BsonDocument filter, T document, boolean upsert)
      throws MetadataServiceException {
    MetadataServiceException.wrapIfThrows(
        () ->
            this.getCollection()
                .replaceOne(filter, document.toBson(), new ReplaceOptions().upsert(upsert)));
  }

  /**
   * Updates a single document, updating specific fields as specified in the update document.
   *
   * @param filter filter to identify the single doc to update
   * @param update bson document describing the fields to update
   * @throws MetadataServiceException if there was an error updating the document
   */
  public synchronized UpdateResult updateOne(BsonDocument filter, BsonDocument update)
      throws MetadataServiceException {
    return MetadataServiceException.wrapIfThrows(
        () -> this.getCollection().updateOne(filter, update));
  }

  /**
   * Deletes a single item in the metadata collection that matches the given filter. If multiple
   * items match, it will delete the first item found based on the internal ordering of the
   * documents.
   *
   * @param filter filter for the document to delete
   * @throws MetadataServiceException if there was an error deleting the specified document
   */
  public synchronized void delete(BsonDocument filter) throws MetadataServiceException {
    MetadataServiceException.wrapIfThrows(() -> this.getCollection().deleteOne(filter));
  }

  /**
   * Deletes a multiple items in the metadata collection that match the given filter.
   *
   * @param filter filter for the documents to delete
   * @throws MetadataServiceException if there was an error deleting the specified document
   */
  public synchronized void deleteMany(BsonDocument filter) throws MetadataServiceException {
    MetadataServiceException.wrapIfThrows(() -> this.getCollection().deleteMany(filter));
  }

  /**
   * Applies a replaceOne for each filter/object combination in the provided map as a single
   * bulkWrite operation.
   *
   * @param replaceList map of filters and the object to replace the found object with
   * @param ordered if this should be an ordered or unordered bulkWrite
   * @param upsert if this should be an upsert or a strict replacement
   * @throws MetadataServiceException if there was an error applying any of the updates in the list
   */
  public synchronized void bulkReplace(
      Map<BsonDocument, T> replaceList, boolean ordered, boolean upsert)
      throws MetadataServiceException {
    List<ReplaceOneModel<BsonDocument>> replaceOneModels =
        replaceList.entrySet().stream()
            .map(
                e ->
                    new ReplaceOneModel<>(
                        e.getKey(), e.getValue().toBson(), new ReplaceOptions().upsert(upsert)))
            .toList();
    bulkWrite(replaceOneModels, ordered);
  }

  /**
   * Applies a deleteOne for each filter in the provided list as a single bulkWrite operation.
   *
   * @param deleteFilters list of filters for each delete operation
   * @param ordered if this should be an ordered or unordered bulkWrite
   * @throws MetadataServiceException if there was an error applying any of the deletes in the list
   */
  public synchronized void bulkDelete(List<BsonDocument> deleteFilters, boolean ordered)
      throws MetadataServiceException {
    List<DeleteOneModel<BsonDocument>> deleteOneModels =
        deleteFilters.stream().map(b -> new DeleteOneModel<BsonDocument>(b)).toList();
    bulkWrite(deleteOneModels, ordered);
  }

  /**
   * Applies a list of {@link WriteModel} operations using mongo's bulkWrite API.
   *
   * @param docs the list of WriteOperations to apply
   * @param ordered whether this should be an ordered or unordered bulkWrite
   * @throws MetadataServiceException if there was an error persisting the entries into the
   *     collection. Depending on if ordered is set to true or false it will either error after the
   *     first failure or after the entire batch of writes complete.
   */
  private synchronized void bulkWrite(
      List<? extends WriteModel<BsonDocument>> docs, boolean ordered)
      throws MetadataServiceException {
    if (docs.isEmpty()) {
      return;
    }
    MetadataServiceException.wrapIfThrows(
        () -> this.getCollection().bulkWrite(docs, new BulkWriteOptions().ordered(ordered)));
  }

  /**
   * Returns all documents in the metadata collection matching the given filter.
   *
   * @param filter filter to apply to the list command
   * @throws MetadataServiceException if there was an error reading from mongod
   */
  public synchronized List<BsonDocument> list(BsonDocument filter) throws MetadataServiceException {

    return MetadataServiceException.wrapIfThrows(
        () ->
            StreamSupport.stream(this.getCollection().find(filter).spliterator(), false).toList());
  }

  /**
   * Creates a new index on the collection if the index does not yet exist. This command is
   * idempotent and does nothing if the index already exits.
   *
   * @param keys the collection keys to index
   * @param indexOptions options on how to bild the index
   * @throws MetadataServiceException any error that may be thrown by the mongodb client
   */
  public synchronized void createIndex(Bson keys, IndexOptions indexOptions)
      throws MetadataServiceException {
    MetadataServiceException.wrapIfThrows(
        () -> this.getCollection().createIndex(keys, indexOptions));
  }

  private MongoCollection<BsonDocument> getCollection() {
    return this.mongoClient
        .getDatabase(this.databaseName)
        .getCollection(this.collectionName, BsonDocument.class)
        .withWriteConcern(WriteConcern.MAJORITY)
        .withReadPreference(ReadPreference.primary());
  }
}
