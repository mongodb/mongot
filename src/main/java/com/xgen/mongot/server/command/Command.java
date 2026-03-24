package com.xgen.mongot.server.command;

import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import java.util.Collections;
import java.util.List;
import org.bson.BsonDocument;

public interface Command {
  String name();

  BsonDocument run();

  ExecutionPolicy getExecutionPolicy();

  enum ExecutionPolicy {

    /**
     * Policy used for commands which can block for extended periods of time (e.g. any I/O call),
     * designed to be executed on isolated and relatively large thread pool.
     */
    ASYNC,

    /**
     * Policy for response-time sensitive commands, which eliminates extra overhead of the thread
     * switch by executing a command directly on the caller thread. Should be used with
     * understanding of the caller context: this policy is not applicable for blocking commands if
     * executed from the Netty event loop.
     */
    SYNC
  }

  /**
   * Get IDs of newly created cursors during {@code Command::run}.
   *
   * <p>This method should be called after {@code Command::run}.
   */
  default List<Long> getCreatedCursorIds() {
    return Collections.emptyList();
  }

  /**
   * Indicates whether this command depends on cursors.
   *
   * <p>This is currently used by the gRPC server. If no cursors are created in the current gRPC
   * stream, this command won't run.
   */
  default boolean dependOnCursors() {
    return false;
  }

  /**
   * Handle {@code SearchEnvoyMetadata} from the gRPC header.
   *
   * <p>This method is called before {@code Command::run}.
   */
  default void handleSearchEnvoyMetadata(SearchEnvoyMetadata searchEnvoyMetadata) {}

  /**
   * Indicates whether this command may be load shed under high load conditions.
   *
   * <p>Commands that return {@code false} are executed on a dedicated unbounded thread pool to
   * ensure they are never rejected due to load shedding. This is useful for critical commands that
   * must always succeed, such as killCursors, and for background management commands (e.g., AIC
   * index create/update) whose bursty load should not compete with user-facing queries.
   *
   * @return true if this command may be rejected due to load shedding, false if it must always
   *     execute
   */
  default boolean maybeLoadShed() {
    return true;
  }
}
