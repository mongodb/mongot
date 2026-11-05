package com.xgen.testing.mongot.replication.mongodb;

import static com.xgen.mongot.util.Check.checkArg;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.util.BsonUtils;
import java.io.ByteArrayOutputStream;
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

  // These constants are used to construct resume tokens but only used for testing and not used in
  // the production code.
  private static final byte TIMESTAMP_IDENTIFIER = (byte) 0x82;
  private static final int KEY_STRING_ZERO = 0x29;
  private static final int KEY_STRING_POSITIVE_ONE_BYTE_INT = 0x2B;
  private static final int KEY_STRING_FALSE = 0x6E;
  private static final int KEY_STRING_TRUE = 0x6F;
  private static final int HIGH_WATER_MARK_TOKEN = 0;
  private static final int EVENT_TOKEN = 128;

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
    return resumeTokenPrefix(opTime, 1, HIGH_WATER_MARK_TOKEN, 0, false);
  }

  /** Returns an event token prefix with the invalidation flag set. */
  public static BsonDocument invalidateResumeToken(BsonTimestamp opTime) {
    return invalidateResumeToken(opTime, 1, 0);
  }

  /** Returns a version 1 or 2 event token prefix with the invalidation flag set. */
  public static BsonDocument invalidateResumeToken(
      BsonTimestamp opTime, int version, int txnOpIndex) {
    return eventResumeTokenPrefix(opTime, version, txnOpIndex, true);
  }

  /** Returns an ordinary event token prefix with the invalidation flag set to false. */
  public static BsonDocument eventResumeToken(BsonTimestamp opTime) {
    return eventResumeToken(opTime, 1, 0);
  }

  /** Returns a version 1 or 2 ordinary event token prefix. */
  public static BsonDocument eventResumeToken(BsonTimestamp opTime, int version, int txnOpIndex) {
    return eventResumeTokenPrefix(opTime, version, txnOpIndex, false);
  }

  private static BsonDocument eventResumeTokenPrefix(
      BsonTimestamp opTime, int version, int txnOpIndex, boolean fromInvalidate) {
    checkArg(version == 1 || version == 2, "Expected resume token version 1 or 2");
    return resumeTokenPrefix(opTime, version, EVENT_TOKEN, txnOpIndex, fromInvalidate);
  }

  private static BsonDocument resumeTokenPrefix(
      BsonTimestamp opTime, int version, int tokenType, int txnOpIndex, boolean fromInvalidate) {
    // Append each field in server order instead of assuming fixed byte offsets or prefix size.
    // These prefixes are test fixtures; server resume requests use complete MongoDB tokens.
    var data = new ByteArrayOutputStream();
    data.write(TIMESTAMP_IDENTIFIER);
    data.writeBytes(ByteBuffer.allocate(Long.BYTES).putLong(opTime.getValue()).array());
    writeNonnegativeInteger(data, version);
    writeNonnegativeInteger(data, tokenType);
    writeNonnegativeInteger(data, txnOpIndex);
    data.write(fromInvalidate ? KEY_STRING_TRUE : KEY_STRING_FALSE);
    return new BsonDocument("_data", new BsonString(Hex.encodeHexString(data.toByteArray())));
  }

  private static void writeNonnegativeInteger(ByteArrayOutputStream data, int value) {
    checkArg(value >= 0, "Expected nonnegative integer");
    long encoded = ((long) value) << 1;
    int size = (Long.SIZE - Long.numberOfLeadingZeros(encoded) + Byte.SIZE - 1) / Byte.SIZE;
    data.write(size == 0 ? KEY_STRING_ZERO : KEY_STRING_POSITIVE_ONE_BYTE_INT + size - 1);
    for (int i = size - 1; i >= 0; i--) {
      data.write((byte) (encoded >>> (i * Byte.SIZE)));
    }
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
