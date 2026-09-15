package com.xgen.testing.mongot.replication.mongodb;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.util.BsonUtils;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Hex;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;

public class ChangeStreamUtils {

  public static final BsonDocument POST_BATCH_RESUME_TOKEN = resumeToken(new BsonTimestamp(1, 0));

  public static final BsonDocument PRE_BATCH_RESUME_TOKEN = resumeToken(new BsonTimestamp(2, 0));

  public static final MongoNamespace NAMESPACE = new MongoNamespace("database", "my_collection");
  public static final MongoNamespace OTHER_NAMESPACE = new MongoNamespace("database.collection2");

  public static final ChangeStreamResumeInfo PRE_BATCH_RESUME_INFO =
      ChangeStreamResumeInfo.create(NAMESPACE, PRE_BATCH_RESUME_TOKEN);

  public static final ChangeStreamResumeInfo POST_BATCH_RESUME_INFO =
      ChangeStreamResumeInfo.create(NAMESPACE, POST_BATCH_RESUME_TOKEN);

  /** Returns a resumeToken representing the given opTime. */
  public static BsonDocument resumeToken(BsonTimestamp opTime) {
    return new BsonDocument(
        "_data",
        new BsonString(
            // A resume token is a hex-encoded WT KeyString with several required fields, and a
            // couple optional ones. The KeyString type prefix bytes are defined in mongo:
            // https://github.com/mongodb/mongo/blob/r5.0.0/src/mongo/db/storage/key_string.cpp#L65-L117
            // Notably, non-timestamp integers are encoded with as few bytes as possible.
            //
            // The specific resume token KeyString fields are defined also in mongo:
            // https://github.com/mongodb/mongo/blob/r5.0.0/src/mongo/db/pipeline/resume_token.cpp#L139-L224
            Hex.encodeHexString(
                ByteBuffer.allocate(14)
                    .put(0, (byte) -126) // type: Timestamp
                    .putLong(1, opTime.getValue()) // clusterTime
                    .put(9, (byte) 43) // type: 1-byte positive int
                    .put(10, (byte) 2) // version (1 << 1)
                    .put(11, (byte) 41) // type: zero (high water mark resume token)
                    .put(12, (byte) 41) // type: zero (txnOpIndex)
                    .put(13, (byte) 110) // type: boolean False (fromInvalidate)
                )));
  }

  public static ChangeStreamDocument<RawBsonDocument> insertEvent(
      int index, IndexDefinition indexDefinition) {
    return dataEvent(OperationType.INSERT, index, null, indexDefinition);
  }

  public static ChangeStreamDocument<RawBsonDocument> updateEvent(
      int index, UpdateDescription updateDescription, IndexDefinition indexDefinition) {
    return dataEvent(OperationType.UPDATE, index, updateDescription, indexDefinition);
  }

  public static ChangeStreamDocument<RawBsonDocument> updateNoDataEvent(
      int index, UpdateDescription updateDescription) {
    return noDataEvent(OperationType.UPDATE, index, updateDescription);
  }

  // use case when updateLookup happened after the doc is deleted, so we expect
  // metadata to be present (internal $addFields stage adds it) but be empty
  public static ChangeStreamDocument<RawBsonDocument> updateEventWithEmptyMetadata(
      int index, UpdateDescription updateDescription, IndexDefinition indexDefinition) {
    return emptyMetadataEvent(OperationType.UPDATE, index, updateDescription, indexDefinition);
  }

  public static ChangeStreamDocument<RawBsonDocument> updateEventWithNoMetadata(
      int index, UpdateDescription updateDescription) {
    return noMetadataEvent(OperationType.UPDATE, index, updateDescription);
  }

  public static ChangeStreamDocument<RawBsonDocument> insertEventWithNoMetadata(int index) {
    return noMetadataEvent(OperationType.INSERT, index);
  }

  public static ChangeStreamDocument<RawBsonDocument> replaceEvent(
      int index, IndexDefinition indexDefinition) {
    return dataEvent(OperationType.REPLACE, index, null, indexDefinition);
  }

  public static ChangeStreamDocument<RawBsonDocument> deleteEvent(
      int index, IndexDefinition indexDefinition) {
    return dataEvent(OperationType.DELETE, index, null, indexDefinition);
  }

  public static ChangeStreamDocument<RawBsonDocument> invalidateEvent(int index) {
    return collectionEvent(OperationType.INVALIDATE, index);
  }

  public static ChangeStreamDocument<RawBsonDocument> otherEvent(int index) {
    return collectionEvent(OperationType.OTHER, index);
  }

  /** Constructs a new RENAME event. */
  public static ChangeStreamDocument<RawBsonDocument> renameEvent(int index) {
    return new ChangeStreamDocument<>(
        OperationType.RENAME.getValue(),
        indexResumeToken(index),
        null,
        null,
        new BsonDocument()
            .append("db", new BsonString("database"))
            .append("coll", new BsonString("collection2")),
        null,
        null,
        null,
        new BsonTimestamp(0, index),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  /** Creates a rename event with specified namespaces. */
  public static ChangeStreamDocument<RawBsonDocument> renameEvent(
      int index, MongoNamespace ns, MongoNamespace destinationNs) {
    BsonDocument destinationNsDoc =
        (destinationNs != null)
            ? new BsonDocument()
                .append("db", new BsonString(destinationNs.getDatabaseName()))
                .append("coll", new BsonString(destinationNs.getCollectionName()))
            : null;
    return new ChangeStreamDocument<>(
        OperationType.RENAME.getValue(),
        indexResumeToken(index),
        new BsonDocument()
            .append("db", new BsonString(ns.getDatabaseName()))
            .append("coll", new BsonString(ns.getCollectionName())),
        null,
        destinationNsDoc,
        null,
        null,
        null,
        new BsonTimestamp(0, index),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public static ChangeStreamDocument<RawBsonDocument> dropEvent(int index) {
    return collectionEvent(OperationType.DROP, index);
  }

  public static ChangeStreamDocument<RawBsonDocument> dropDatabaseEvent(int index) {
    return collectionEvent(OperationType.DROP_DATABASE, index);
  }

  /** Converts a list of {@link ChangeStreamDocument} into a list of {@link RawBsonDocument}. */
  public static List<RawBsonDocument> toRawBsonDocuments(
      List<ChangeStreamDocument<RawBsonDocument>> changeStreamDocuments) {
    return changeStreamDocuments.stream()
        .map(ChangeStreamDocumentUtils::changeStreamDocumentToBsonDocument)
        .collect(Collectors.toList());
  }

  /** Constructs a data event with a full document containing metadata for the given index. */
  public static ChangeStreamDocument<RawBsonDocument> dataEvent(
      OperationType operationType,
      int index,
      UpdateDescription updateDescription,
      IndexDefinition indexDefinition) {
    return new ChangeStreamDocument<>(
        operationType.getValue(),
        indexResumeToken(index),
        null,
        null,
        null,
        BsonUtils.documentToRaw(
            new BsonDocument()
                .append("_id", new BsonInt32(index))
                .append("foo", new BsonString("bar"))
                .append(
                    indexDefinition.getIndexId().toString(), // metadata namespace
                    new BsonDocument("_id", new BsonInt32(index)))),
        null,
        new BsonDocument().append("_id", new BsonInt32(index)),
        new BsonTimestamp(0, index),
        updateDescription,
        null,
        null,
        null,
        null,
        null);
  }

  public static ChangeStreamDocument<RawBsonDocument> dataEvent(
      OperationType operationType,
      int index,
      UpdateDescription updateDescription,
      BsonDocument fullDocument) {
    return new ChangeStreamDocument<>(
        operationType.getValue(),
        indexResumeToken(index),
        null,
        null,
        null,
        BsonUtils.documentToRaw(fullDocument),
        null,
        new BsonDocument().append("_id", new BsonInt32(index)),
        new BsonTimestamp(0, index),
        updateDescription,
        null,
        null,
        null,
        null,
        null);
  }

  public static ChangeStreamDocument<RawBsonDocument> noMetadataEvent(
      OperationType operationType, int index, UpdateDescription updateDescription) {
    return new ChangeStreamDocument<>(
        operationType.getValue(),
        indexResumeToken(index),
        null,
        null,
        null,
        BsonUtils.documentToRaw(new BsonDocument().append("foo", new BsonString("bar"))),
        null,
        new BsonDocument().append("_id", new BsonInt32(index)),
        new BsonTimestamp(0, index),
        updateDescription,
        null,
        null,
        null,
        null,
        null);
  }

  public static ChangeStreamDocument<RawBsonDocument> noMetadataEvent(
      OperationType operationType, int index) {
    return new ChangeStreamDocument<>(
        operationType.getValue(),
        indexResumeToken(index),
        null,
        null,
        null,
        BsonUtils.documentToRaw(
            new BsonDocument()
                .append("_id", new BsonInt32(index))
                .append("foo", new BsonString("bar"))),
        null,
        new BsonDocument().append("_id", new BsonInt32(index)),
        new BsonTimestamp(0, index),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static ChangeStreamDocument<RawBsonDocument> noDataEvent(
      OperationType operationType, int index, UpdateDescription updateDescription) {
    return new ChangeStreamDocument<>(
        operationType.getValue(),
        indexResumeToken(index),
        null,
        null,
        null,
        null,
        null,
        new BsonDocument().append("_id", new BsonInt32(index)),
        new BsonTimestamp(0, index),
        updateDescription,
        null,
        null,
        null,
        null,
        null);
  }

  private static ChangeStreamDocument<RawBsonDocument> emptyMetadataEvent(
      OperationType operationType,
      int index,
      UpdateDescription updateDescription,
      IndexDefinition indexDefinition) {
    return new ChangeStreamDocument<>(
        operationType.getValue(),
        indexResumeToken(index),
        null,
        null,
        null,
        BsonUtils.documentToRaw(
            // fullDocument contains only empty metadata namespace
            new BsonDocument().append(indexDefinition.getIndexId().toString(), new BsonDocument())),
        null,
        new BsonDocument().append("_id", new BsonInt32(index)),
        new BsonTimestamp(0, index),
        updateDescription,
        null,
        null,
        null,
        null,
        null);
  }

  private static ChangeStreamDocument<RawBsonDocument> collectionEvent(
      OperationType operationType, int index) {
    return new ChangeStreamDocument<>(
        operationType.getValue(),
        indexResumeToken(index),
        null,
        null,
        null,
        null,
        null,
        null,
        new BsonTimestamp(0, index),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static BsonDocument indexResumeToken(int index) {
    return resumeToken(new BsonTimestamp(1, index));
  }
}
