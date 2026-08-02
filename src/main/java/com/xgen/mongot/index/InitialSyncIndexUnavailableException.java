package com.xgen.mongot.index;

/** Thrown when a query targets an index that is still in initial sync. */
public class InitialSyncIndexUnavailableException extends IndexUnavailableException {

  public InitialSyncIndexUnavailableException(String message) {
    super(message);
  }
}
