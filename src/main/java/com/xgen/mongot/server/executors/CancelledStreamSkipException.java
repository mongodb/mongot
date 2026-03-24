package com.xgen.mongot.server.executors;

/**
 * Thrown when a dequeued command is skipped because its originating stream was cancelled or closed
 * before execution. This is not a failure — it indicates the command was proactively discarded to
 * avoid wasting thread pool capacity on stale work.
 */
public class CancelledStreamSkipException extends RuntimeException {
  public CancelledStreamSkipException(String message) {
    super(message);
  }
}
