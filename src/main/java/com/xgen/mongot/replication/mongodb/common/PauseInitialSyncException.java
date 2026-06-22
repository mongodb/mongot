package com.xgen.mongot.replication.mongodb.common;

/** Exception thrown when disk usage is high and initial syncs should be paused. */
public class PauseInitialSyncException extends Exception {

  public PauseInitialSyncException(String message) {
    super(message);
  }
}
