package com.xgen.mongot.replication.mongodb.common;

import com.google.errorprone.annotations.Var;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.xgen.mongot.util.mongodb.Errors;
import java.util.OptionalInt;

public class MongoExceptionUtils {

  /** Maximum number of causes {@link #isThrottling(Throwable)} walks to prevent loops. */
  private static final int MAX_CAUSE_DEPTH = 16;

  /**
   * Returns true if the given MongoException is a retryable exception, as documented by
   * https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.rst#retryable-error
   *
   * <p>We don't intend to actually retry the operation inline, since application-level retry logic
   * is tricky to implement correctly
   *
   * <p>See also:
   * https://github.com/mongodb/mongo-java-driver/blob/master/driver-core/src/main/com/mongodb/internal/operation/CommandOperationHelper.java#L89-L100
   */
  public static boolean isRetryable(MongoException e) {
    if (e instanceof MongoSocketException
        || e instanceof MongoNotPrimaryException
        || e instanceof MongoNodeIsRecoveringException
        || e instanceof MongoCursorNotFoundException
        || e instanceof MongoTimeoutException) {
      return true;
    }

    return Errors.RETRYABLE_ERROR_CODES.contains(e.getCode());
  }

  /** Returns true if the given MongoException is a retryable MongoClientException. */
  public static boolean isRetryableClientException(MongoException e) {
    return e instanceof MongoConfigurationException
        || e instanceof MongoConnectionPoolClearedException
        || e instanceof MongoSecurityException
        || e instanceof MongoServerUnavailableException;
  }

  /** Returns true if throttled by mongod. */
  public static boolean isThrottling(MongoException e) {
    return Errors.THROTTLING_ERROR_CODES.contains(e.getCode());
  }

  /** Returns true if throttled by mongod, examining the exception's cause chain. */
  public static boolean isThrottling(Throwable throwable) {
    @Var Throwable cause = throwable;
    for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
      if (cause instanceof MongoException mongoException && isThrottling(mongoException)) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }

  /**
   * Returns the overload error code carried by {@code throwable} or one of its causes, or empty if
   * it is not an overload rejection. See {@link Errors#OVERLOAD_REJECTION_ERROR_CODES}.
   *
   * <p>Walks the cause chain because mongod's error reaches us wrapped — typically a {@link
   * MongoException} inside a {@code SteadyStateException} or {@code InitialSyncException}.
   */
  public static OptionalInt overloadErrorCode(Throwable throwable) {
    @Var Throwable cause = throwable;
    for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
      if (cause instanceof MongoException mongoException
          && Errors.OVERLOAD_REJECTION_ERROR_CODES.contains(mongoException.getCode())) {
        return OptionalInt.of(mongoException.getCode());
      }
      cause = cause.getCause();
    }
    return OptionalInt.empty();
  }

  /**
   * Returns true if the given MongoException is a non-invalidating resync-able exception. See
   * {@link Errors#NON_INVALIDATING_ERROR_CODES} for exact list of errors.
   */
  public static boolean isNonInvalidatingResyncable(MongoException e) {
    return Errors.NON_INVALIDATING_ERROR_CODES.contains(e.getCode());
  }
}
