package com.xgen.mongot.replication.mongodb.common;

import com.google.common.collect.Sets;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.synonym.SynonymMappingException;
import com.xgen.mongot.util.LoggableException;
import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.mongot.util.regex.RegexInterruptedException;
import com.xgen.mongot.util.regex.RegexLimitExceededException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.bson.BsonTimestamp;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Related to syncing with a synonym source collection or with building synonym artifacts. */
public class SynonymSyncException extends LoggableException {

  private static final Logger LOG = LoggerFactory.getLogger(SynonymSyncException.class);

  /**
   * Mongo client error codes that indicate that a collection was dropped or does not exist. For
   * synonyms sync purposes, these states are equivalent.
   */
  private static final Set<Integer> COLLECTION_DROPPED_ERROR_CODES =
      Set.of(Errors.NAMESPACE_NOT_FOUND.code);

  /**
   * Mongo client error codes that do not invalidate a synonym mapping, but do require a collection
   * scan as the next action.
   */
  private static final Set<Integer> NON_INVALIDATING_MONGO_ERROR_CODES_REQUIRING_SCAN =
      Sets.union(
          Errors.RETRYABLE_ERROR_CODES,
          Set.of(
              Errors.CURSOR_NOT_FOUND.code,
              Errors.CAPPED_POSITION_LOST.code,
              Errors.CHANGE_STREAM_FATAL_ERROR.code,
              Errors.CHANGE_STREAM_HISTORY_LOST.code,
              Errors.QUERY_PLAN_KILLED.code,
              Errors.SHARD_NOT_FOUND.code));

  public enum Type {
    FIELD_EXCEEDED,
    INVALID,
    SHUTDOWN,
    TRANSIENT,
    DROPPED,
    FAILED
  }

  private final Type type;
  private final Optional<BsonTimestamp> operationTime;

  /** True when this exception was caused by mongod shedding load. */
  private final boolean throttled;

  private SynonymSyncException(
      @Nullable String message,
      @Nullable Throwable cause,
      Type type,
      Optional<BsonTimestamp> operationTime) {
    this(message, cause, type, operationTime, false);
  }

  private SynonymSyncException(
      @Nullable String message,
      @Nullable Throwable cause,
      Type type,
      Optional<BsonTimestamp> operationTime,
      boolean throttled) {
    super(String.valueOf(message), cause);

    this.type = type;
    this.operationTime = operationTime;
    this.throttled = throttled;
  }

  private SynonymSyncException(String message, Type type, Optional<BsonTimestamp> operationTime) {
    super(message);

    this.type = type;
    this.operationTime = operationTime;
    this.throttled = false;
  }

  public static SynonymSyncException withOpTime(
      SynonymSyncException e, Optional<BsonTimestamp> operationTime) {
    return new SynonymSyncException(
        e.getMessage(), e.getCause(), e.getType(), operationTime, e.throttled);
  }

  public Type getType() {
    return this.type;
  }

  public Optional<BsonTimestamp> getOperationTime() {
    return this.operationTime;
  }

  public static SynonymSyncException createShutDown() {
    return new SynonymSyncException("shutting down", Type.SHUTDOWN, Optional.empty());
  }

  public static SynonymSyncException createDropped() {
    return createDropped(Optional.empty());
  }

  public static SynonymSyncException createDropped(Optional<BsonTimestamp> operationTime) {
    return new SynonymSyncException("collection dropped", Type.DROPPED, operationTime);
  }

  /**
   * Gets the result of the given future, wrapping any exception in the proper SynonymSyncException.
   *
   * <p>Propagates Errors as is.
   */
  public static <T> T getOrWrapThrowable(CompletableFuture<T> future) throws SynonymSyncException {
    return getOrWrapThrowable(future, Optional.empty());
  }

  /**
   * Gets the result of the given future, wrapping any exception in the proper SynonymSyncException,
   * optionally paired with an operationTime.
   *
   * <p>Propagates Errors as is.
   */
  public static <T> T getOrWrapThrowable(
      CompletableFuture<T> future, Optional<BsonTimestamp> operationTime)
      throws SynonymSyncException {
    return wrapIfThrows(
        () -> {
          try {
            return future.get();
          } catch (ExecutionException e) {
            if (e.getCause() instanceof Error) {
              throw (Error) e.getCause();
            }

            if (!(e.getCause() instanceof Exception)) {
              throw new AssertionError(
                  "threw a throwable that is not an exception nor an error", e.getCause());
            }
            throw (Exception) e.getCause();
          }
        },
        operationTime);
  }

  /**
   * Runs the supplied Callable, wrapping well-known exception types into SynonymSyncExceptions if
   * they are caught.
   *
   * <p>Errors are propagated as is.
   */
  public static <T> T wrapIfThrows(Callable<T> callable) throws SynonymSyncException {
    return wrapIfThrows(callable, Optional.empty());
  }

  /**
   * Runs the supplied Callable, wrapping well-known exception types into SynonymSyncExceptions if
   * they are caught. Includes the provided optional operationTime in a wrapped
   * SynonymSyncException.
   *
   * <p>Errors are propagated as is.
   */
  public static <T> T wrapIfThrows(Callable<T> callable, Optional<BsonTimestamp> operationTime)
      throws SynonymSyncException {
    try {
      return callable.call();
    } catch (MongoInterruptedException | InterruptedException | RegexInterruptedException e) {
      throw createShutDown();
    } catch (RegexLimitExceededException e) {
      // INVALID may repeatedly rescan the same document when no high-water mark is available.
      throw new SynonymSyncException(e.getMessage(), e, Type.FAILED, operationTime);
    } catch (MongoException e) {
      int code = e.getCode();
      if (COLLECTION_DROPPED_ERROR_CODES.contains(code)) {
        // Collection was dropped.
        throw createDropped(operationTime);
      } else if (MongoExceptionUtils.isThrottling(e)) {
        // mongod shed this request; retry the scan after a short backoff.
        throw createThrottled(e, operationTime);
      } else if (NON_INVALIDATING_MONGO_ERROR_CODES_REQUIRING_SCAN.contains(code)) {
        // These errors do not invalidate a synonym mapping, but do require a collection scan as the
        // next sync action to take.
        throw createTransient(e, operationTime);
      } else {
        // For now, log unknown error codes - but treat them the same as transient codes explicitly
        // included above.
        LOG.atWarn()
            .addKeyValue("code", e.getCode())
            .addKeyValue("labels", e.getErrorLabels())
            .log("unknown exception from mongo client");
        throw createTransient(e, operationTime);
      }
    } catch (FieldExceededLimitsException e) {
      throw createFieldExceeded(e, operationTime);
    } catch (SynonymMappingException e) {
      throw switch (e.type) {
        case INVALID_DOCUMENT -> createInvalid(e, operationTime);
        case BUILD_ERROR -> createTransient(e, operationTime);
        case UNKNOWN_MAPPING -> createFailed(e.getMessage());
        case MAPPING_NOT_READY -> createTransient("Mapping not ready");
      };
      // Parsed invalid document during synonym collection scan
    } catch (SynonymSyncException e) {
      throw e.getOperationTime().isPresent() ? e : withOpTime(e, operationTime);
    } catch (CodecConfigurationException | NoShardFoundException e) {
      // See CLOUDP-141235 and SERVER-43086.
      // mongos may not deserialize change stream results correctly when first starting
      throw createTransient(e, operationTime);
    } catch (Exception e) {
      if (e instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }

      throw new AssertionError("threw unexpected checked exception: " + e, e);
    }
  }

  /**
   * Runs the supplied Runnable, wrapping well-known exception types into SynonymSyncExceptions if
   * they are caught.
   */
  public static void wrapIfThrows(CheckedRunnable<Exception> runnable) throws SynonymSyncException {
    wrapIfThrows(
        () -> {
          runnable.run();
          return null;
        });
  }

  public static boolean isShutDown(Throwable throwable) {
    return throwable instanceof SynonymSyncException synonymSyncException
        && synonymSyncException.isShutDown();
  }

  public boolean isShutDown() {
    return this.type == Type.SHUTDOWN;
  }

  public static SynonymSyncException createTransient(String msg) {
    return new SynonymSyncException(msg, Type.TRANSIENT, Optional.empty());
  }

  public static SynonymSyncException createTransient(Throwable cause) {
    return createTransient(cause, Optional.empty());
  }

  public static SynonymSyncException createTransient(
      Throwable cause, Optional<BsonTimestamp> operationTime) {
    return new SynonymSyncException(
        cause.getMessage(), cause.getCause(), Type.TRANSIENT, operationTime);
  }

  /**
   * Creates a TRANSIENT exception flagged as throttled, so that {@link
   * com.xgen.mongot.replication.mongodb.synonyms.SynonymMappingManager} retries on a short backoff.
   */
  public static SynonymSyncException createThrottled(
      Throwable cause, Optional<BsonTimestamp> operationTime) {
    return new SynonymSyncException(
        cause.getMessage(), cause.getCause(), Type.TRANSIENT, operationTime, true);
  }

  /** Returns true if this exception was caused by mongod shedding load. */
  public boolean isThrottled() {
    return this.throttled;
  }

  public static SynonymSyncException createFieldExceeded(Throwable cause) {
    return createFieldExceeded(cause, Optional.empty());
  }

  public static SynonymSyncException createFieldExceeded(
      Throwable cause, Optional<BsonTimestamp> operationTime) {
    return new SynonymSyncException(
        cause.getMessage(), cause.getCause(), Type.FIELD_EXCEEDED, operationTime);
  }

  public static SynonymSyncException createInvalid(Throwable cause) {
    return createInvalid(cause, Optional.empty());
  }

  public static SynonymSyncException createInvalid(
      Throwable cause, Optional<BsonTimestamp> opTime) {
    return new SynonymSyncException(cause.getMessage(), cause.getCause(), Type.INVALID, opTime);
  }

  public static SynonymSyncException createFailed(String message) {
    return new SynonymSyncException(message, Type.FAILED, Optional.empty());
  }
}
