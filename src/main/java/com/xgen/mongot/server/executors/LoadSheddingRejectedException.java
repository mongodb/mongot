package com.xgen.mongot.server.executors;

import java.util.concurrent.RejectedExecutionException;

/**
 * Exception thrown when a command is rejected due to load shedding (executor capacity limits).
 *
 * <p>This exception is specifically for cases where the server is at capacity and cannot accept
 * more requests. Error responses from this exception should include the "SystemOverloadedError" and
 * "RetryableError" labels as defined in MongoDB's error_labels.h, allowing clients to appropriately
 * handle and retry these transient overload conditions.
 */
public class LoadSheddingRejectedException extends RejectedExecutionException {

  static final String SERVER_AT_CAPACITY_MESSAGE =
      "Query rejected: search server is currently at capacity. Please try again later.";

  public LoadSheddingRejectedException(String message) {
    super(message);
  }

  /** Returns a capacity rejection with a consistent message across query limits. */
  public static LoadSheddingRejectedException serverAtCapacity() {
    return new LoadSheddingRejectedException(SERVER_AT_CAPACITY_MESSAGE);
  }
}
