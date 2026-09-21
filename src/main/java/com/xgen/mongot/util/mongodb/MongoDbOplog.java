package com.xgen.mongot.util.mongodb;

import com.mongodb.client.MongoClient;
import com.xgen.mongot.util.Check;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

/**
 * Reads the oldest entry in the sync source's capped oplog.
 *
 * <p>{@code $natural: 1} on {@code local.oplog.rs} is a single-document read from the front of the
 * collection, which is the truncated tail used as the fall-off cliff for change-stream resume
 * tokens.
 */
public final class MongoDbOplog {

  private static final String OPLOG_COLLECTION_NAME = "oplog.rs";
  private static final String TIMESTAMP_FIELD = "ts";

  private MongoDbOplog() {}

  /**
   * Returns the timestamp of the oldest oplog entry on the connected mongod.
   *
   * @param mongoClient the direct mongod client (mongos cannot serve {@code local.oplog.rs})
   * @return the oldest oplog {@code ts}
   */
  public static BsonTimestamp getOldestTimestamp(MongoClient mongoClient) {
    Check.argNotNull(mongoClient, "mongoClient");
    BsonDocument oldest =
        mongoClient
            .getDatabase(Databases.LOCAL)
            .getCollection(OPLOG_COLLECTION_NAME, BsonDocument.class)
            .find()
            .projection(new BsonDocument(TIMESTAMP_FIELD, new BsonInt32(1)))
            .sort(new BsonDocument("$natural", new BsonInt32(1)))
            .limit(1)
            .first();
    if (oldest == null) {
      throw new IllegalStateException("local.oplog.rs is empty");
    }
    BsonValue tsValue = oldest.get(TIMESTAMP_FIELD);
    if (tsValue == null) {
      throw new IllegalStateException("oldest oplog entry did not include ts field");
    }
    if (!tsValue.isTimestamp()) {
      throw new IllegalStateException("oldest oplog entry ts field was not a timestamp");
    }
    return tsValue.asTimestamp();
  }
}
