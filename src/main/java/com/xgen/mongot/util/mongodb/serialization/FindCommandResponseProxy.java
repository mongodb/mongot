package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.bson.BsonSerialization;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

/**
 * FindCommandResponseProxy is a proxy for the response to a <em>find</em> command
 *
 * <p>See
 * https://github.com/mongodb/mongo/blob/72788deb99186ac2628604505147e68ac53cffc5/src/mongo/db/query/cursor_response.cpp
 *
 * <p>Note that the MongoDB driver handles unsuccessful commands by catching them and emitting a
 * MongoCommandException, so this proxy only represents a successful result.
 */
public class FindCommandResponseProxy {

  private static final String OK_FIELD = "ok";
  private static final String OPERATION_TIME_FIELD = "operationTime";
  private static final String CLUSTER_TIME_FIELD = "$clusterTime";
  private static final String CURSOR_FIELD = "cursor";

  private final double ok;
  private final BsonTimestamp operationTime;
  private final BsonDocument clusterTime;
  private final CursorProxy cursor;

  /** Constructs a new FindCommandResponseProxy. */
  @BsonCreator
  public FindCommandResponseProxy(
      @BsonProperty(OK_FIELD) Double ok,
      @BsonProperty(OPERATION_TIME_FIELD) BsonTimestamp operationTime,
      @BsonProperty(CLUSTER_TIME_FIELD) BsonDocument clusterTime,
      @BsonProperty(CURSOR_FIELD) CursorProxy cursor) {
    BsonSerialization.throwIfMissingProperty(ok, "ok");
    BsonSerialization.throwIfMissingProperty(operationTime, "operationTime");
    BsonSerialization.throwIfMissingProperty(clusterTime, "$clusterTime");
    BsonSerialization.throwIfMissingProperty(cursor, "cursor");

    this.ok = ok;
    this.operationTime = operationTime;
    this.clusterTime = clusterTime;
    this.cursor = cursor;
  }

  public static class CursorProxy {

    private static final String NS_FIELD = "ns";
    private static final String ID_FIELD = "id";
    private static final String FIRST_BATCH_FIELD = "firstBatch";
    private static final String POST_BATCH_RESUME_TOKEN_FIELD = "postBatchResumeToken";

    private final String ns;
    private final long id;
    private final List<RawBsonDocument> firstBatch;
    private final Optional<BsonDocument> postBatchResumeToken;

    /** Constructs a new CursorProxy. */
    @BsonCreator
    public CursorProxy(
        @BsonProperty(NS_FIELD) String ns,
        @BsonProperty(ID_FIELD) Long id,
        @BsonProperty(FIRST_BATCH_FIELD) List<RawBsonDocument> firstBatch,
        @BsonProperty(POST_BATCH_RESUME_TOKEN_FIELD) BsonDocument postBatchResumeToken) {
      BsonSerialization.throwIfMissingProperty(ns, "ns");
      BsonSerialization.throwIfMissingProperty(id, "id");
      BsonSerialization.throwIfMissingProperty(firstBatch, "firstBatch");

      this.ns = ns;
      this.id = id;
      this.firstBatch = firstBatch;
      // Only sent when the command set $_requestResumeToken, so absent is normal.
      this.postBatchResumeToken = Optional.ofNullable(postBatchResumeToken);
    }

    public String getNs() {
      return this.ns;
    }

    public long getId() {
      return this.id;
    }

    public List<RawBsonDocument> getFirstBatch() {
      return this.firstBatch;
    }

    public Optional<BsonDocument> getPostResumeToken() {
      return this.postBatchResumeToken;
    }
  }

  public double getOk() {
    return this.ok;
  }

  public BsonTimestamp getOperationTime() {
    return this.operationTime;
  }

  public BsonDocument getClusterTime() {
    return this.clusterTime;
  }

  public CursorProxy getCursor() {
    return this.cursor;
  }
}
