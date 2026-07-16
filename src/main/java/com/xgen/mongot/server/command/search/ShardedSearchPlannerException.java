package com.xgen.mongot.server.command.search;

public class ShardedSearchPlannerException extends Exception {
  ShardedSearchPlannerException(String message) {
    super(message);
  }

  ShardedSearchPlannerException(String message, Throwable cause) {
    super(message, cause);
  }
}
