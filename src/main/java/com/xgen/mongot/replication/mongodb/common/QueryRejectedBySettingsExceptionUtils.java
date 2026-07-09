package com.xgen.mongot.replication.mongodb.common;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoQueryException;
import com.xgen.mongot.index.status.RecoveringStatusReason;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.Errors;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Helpers for identifying and reporting replication errors caused by mongod rejecting a replication
 * command because it matches an operation rejection filter ({@code setQuerySettings} with {@code
 * reject: true}).
 *
 * <p>When mongod rejects a replication command (e.g. the collection scan or change stream
 * aggregate) it returns error {@code 411} ({@link Errors#QUERY_REJECTED_BY_SETTINGS}). Resyncing
 * will keep hitting the same rejection until the rejection filter is removed, so we surface a
 * descriptive message on the index status while the index keeps retrying.
 */
public class QueryRejectedBySettingsExceptionUtils {

  private QueryRejectedBySettingsExceptionUtils() {}

  /**
   * Returns true if the cause is a mongod {@code QueryRejectedBySettings} error, indicating a
   * replication command was blocked by an operation rejection filter. See {@link
   * Errors#QUERY_REJECTED_BY_SETTINGS}.
   */
  public static boolean isQueryRejectedBySettingsError(Optional<Throwable> cause) {
    return cause
        .filter(c -> c instanceof MongoException)
        .map(c -> ((MongoException) c).getCode())
        .map(code -> code == Errors.QUERY_REJECTED_BY_SETTINGS.code)
        .orElse(false);
  }

  /** Builds a user-facing status message describing the query-settings rejection. */
  public static String getQueryRejectedBySettingsErrorMessage(Throwable cause) {
    Check.argNotNull(cause, "cause");
    String errorMessage =
        StringUtils.defaultIfBlank(extractDriverErrorMessage(cause), "unknown error");

    return RecoveringStatusReason.QUERY_REJECTED_BY_SETTINGS.formatMessage(errorMessage);
  }

  @Nullable
  private static String extractDriverErrorMessage(Throwable cause) {
    if (cause instanceof MongoCommandException commandException) {
      return commandException.getErrorMessage();
    } else if (cause instanceof MongoQueryException queryException) {
      return queryException.getErrorMessage();
    } else if (cause instanceof MongoException mongoException) {
      return mongoException.getMessage();
    }

    return cause.getMessage();
  }
}
