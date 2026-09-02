package com.xgen.mongot.util.mongodb;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.util.mongodb.serialization.FindCommandResponseProxy;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;

/** FindCommandResponse represents the response from a find command. */
public class FindCommandResponse {

  private final long id;
  private final List<RawBsonDocument> batch;
  private final BsonTimestamp operationTime;
  private final Optional<BsonDocument> postBatchResumeToken;

  FindCommandResponse(
      long id,
      List<RawBsonDocument> batch,
      BsonTimestamp operationTime,
      Optional<BsonDocument> postBatchResumeToken) {
    this.id = id;
    this.batch = batch;
    this.operationTime = operationTime;
    this.postBatchResumeToken = postBatchResumeToken;
  }

  /** Constructs a new FindCommandResponse from the given FindCommandResponseProxy. */
  public static FindCommandResponse fromProxy(FindCommandResponseProxy proxy) {
    checkState(
        proxy.getOk() == 1.0,
        "FindCommandResponseProxy had unexpected ok field value: %s",
        proxy.getOk());

    FindCommandResponseProxy.CursorProxy cursor = proxy.getCursor();
    return new FindCommandResponse(
        cursor.getId(),
        cursor.getFirstBatch(),
        proxy.getOperationTime(),
        cursor.getPostResumeToken());
  }

  public long getId() {
    return this.id;
  }

  public List<RawBsonDocument> getBatch() {
    return this.batch;
  }

  public BsonTimestamp getOperationTime() {
    return this.operationTime;
  }

  /**
   * Returns the cursor's {@code postBatchResumeToken}, present only when the command set {@code
   * $_requestResumeToken}.
   */
  public Optional<BsonDocument> getPostBatchResumeToken() {
    return this.postBatchResumeToken;
  }
}
