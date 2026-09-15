package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.util.mongodb.ChangeStreamResponse;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;

/**
 * ChangeStreamBatch contains the minimal amount of data that the {@code ChangeStreamBufferManager}
 * and {@code ChangeStreamIndexManager} need for a given batch of change stream events.
 */
public class ChangeStreamBatch {

  private final List<RawBsonDocument> events;
  private final BsonDocument postBatchResumeToken;
  private final Optional<BsonTimestamp> commandOperationTime;

  public ChangeStreamBatch(
      List<RawBsonDocument> events,
      BsonDocument postBatchResumeToken,
      Optional<BsonTimestamp> commandOperationTime) {
    this.events = events;
    this.postBatchResumeToken = postBatchResumeToken;
    this.commandOperationTime = commandOperationTime;
  }

  public static ChangeStreamBatch fromResponse(ChangeStreamResponse response) {
    return new ChangeStreamBatch(
        response.getBatch(),
        response.getPostBatchResumeToken(),
        Optional.ofNullable(response.getOperationTime()));
  }

  public List<RawBsonDocument> getRawEvents() {
    return this.events;
  }

  public BsonDocument getPostBatchResumeToken() {
    return this.postBatchResumeToken;
  }

  /**
   * Gets the server "now" timestamp observed when this batch was fetched, used as the upper bound
   * when computing replication lag.
   *
   * <p>This is the cluster time most recently gossiped by the server on the change-stream session
   * (driver 5.x change-stream {@code getMore} responses do not surface a top-level {@code
   * operationTime}). It is not related to the operation time of the contained change stream events.
   *
   * <p>Empty if the session has not yet observed a cluster time; callers must tolerate absence.
   */
  public Optional<BsonTimestamp> getCommandOperationTime() {
    return this.commandOperationTime;
  }
}
