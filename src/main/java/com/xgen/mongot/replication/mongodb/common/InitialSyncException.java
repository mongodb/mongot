package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkState;
import static com.xgen.mongot.util.mongodb.Errors.isMatchCollectionUuidUnsupportedException;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.xgen.mongot.index.DocsExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.mongot.util.mongodb.Errors;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.jetbrains.annotations.Nullable;

/**
 * InitialSyncException is a checked exception used throughout the initialsync package. Its purpose
 * is to wrap any expected exceptions or failure modes and classify the type of error.
 */
public class InitialSyncException extends Exception {

  public static final String BSON_TOO_LARGE_MESSAGE =
      "Unrecoverable replication error due to %s payload exceeding 16MB BSON limit. Rescheduling "
          + "new initial sync. To avoid the issue see: "
          + "https://www.mongodb.com/docs/atlas/atlas-search/manage-indexes/#disk--memory--and-resource-usage.";

  public static final String NOTABLE_SCAN_ERROR_MESSAGE =
      "Initial sync failed because collection scan is not allowed when notablescan is enabled. "
          + "Disable notablescan and recreate the search index to proceed.";

  public static final String MISSING_FIELD_MESSAGE = "Missing field";
  private final Type type;
  private final Optional<InitialSyncResumeInfo> resumeInfo;

  private InitialSyncException(Type type) {
    super();

    this.type = type;
    this.resumeInfo = Optional.empty();
  }

  private InitialSyncException(Type type, InitialSyncResumeInfo resumeInfo) {
    super();

    this.type = type;
    this.resumeInfo = Optional.of(resumeInfo);
  }

  private InitialSyncException(Type type, @Nullable Throwable cause) {
    super(cause);

    this.type = type;
    this.resumeInfo = Optional.empty();
  }

  private InitialSyncException(Type type, String message) {
    super(message);

    this.type = type;
    this.resumeInfo = Optional.empty();
  }

  private InitialSyncException(Type type, @Nullable Throwable cause, @Nullable String message) {
    super(message, cause);

    this.type = type;
    this.resumeInfo = Optional.empty();
  }

  public static InitialSyncException createRequiresResync(@Nullable Throwable cause) {
    return new InitialSyncException(Type.REQUIRES_RESYNC, cause);
  }

  public static InitialSyncException createRequiresResync(String message) {
    return new InitialSyncException(Type.REQUIRES_RESYNC, message);
  }

  public static InitialSyncException createRequiresResync(Throwable cause, String message) {
    return new InitialSyncException(Type.REQUIRES_RESYNC, cause, message);
  }

  public static InitialSyncException createResumableTransient(Throwable cause) {
    return new InitialSyncException(Type.RESUMABLE_TRANSIENT, cause);
  }

  public static InitialSyncException createDoesNotExist(String message) {
    return new InitialSyncException(Type.DOES_NOT_EXIST, message);
  }

  public static InitialSyncException createFailed(Throwable cause) {
    return new InitialSyncException(Type.FAILED, cause);
  }

  public static InitialSyncException createFailed(Throwable cause, String message) {
    return new InitialSyncException(Type.FAILED, cause, message);
  }

  public static InitialSyncException createFieldLimitExceeded(String reason) {
    return new InitialSyncException(Type.FIELD_EXCEEDED, reason);
  }

  public static InitialSyncException createDocsLimitExceeded(Throwable reason) {
    return new InitialSyncException(Type.DOCS_EXCEEDED, reason, reason.getMessage());
  }

  public static InitialSyncException createInvalidated(InitialSyncResumeInfo resumeInfo) {
    return new InitialSyncException(Type.INVALIDATED, resumeInfo);
  }

  @VisibleForTesting
  public static InitialSyncException createDropped() {
    return new InitialSyncException(Type.DROPPED);
  }

  public static InitialSyncException createDropped(String message) {
    return new InitialSyncException(Type.DROPPED, message);
  }

  public static InitialSyncException createShutDown() {
    return new InitialSyncException(Type.SHUT_DOWN);
  }

  public static boolean isShutDown(Throwable throwable) {
    return throwable instanceof InitialSyncException
        && ((InitialSyncException) throwable).isShutdown();
  }

  /** Converts a NamespaceResolutionException into an InitialSyncException. */
  static InitialSyncException fromNamespaceResolutionException(NamespaceResolutionException e) {
    return createDropped(String.format("from NamespaceResolutionException (%s)", e.getMessage()));
  }

  /**
   * Gets the result of the given future, wrapping any exception in the proper InitialSyncException.
   *
   * <p>Propagates Errors as is.
   */
  public static <T> T getOrWrapThrowable(CompletableFuture<T> future, Phase phase)
      throws InitialSyncException {
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
        phase);
  }

  /**
   * Runs the supplied Callable, wrapping well-known exception types into InitialSyncExceptions if
   * they are caught.
   *
   * <p>Errors are propagated as is.
   */
  public static <T> T wrapIfThrows(Callable<T> callable, Phase phase) throws InitialSyncException {
    try {
      return callable.call();
    } catch (MongoInterruptedException | InterruptedException e) {
      throw createShutDown();
    } catch (MongoException e) {
      if (MongoExceptionUtils.isRetryable(e)) {
        throw createResumableTransient(e);
      } else if (MongoExceptionUtils.isRetryableClientException(e)) {
        throw createResumableTransient(e);
      } else if (isMatchCollectionUuidUnsupportedException(e)) {
        throw createResumableTransient(e);
      } else if (e.getCode() == Errors.BSON_OBJECT_TOO_LARGE.code) {
        throw phase == Phase.MAIN
            ? createRequiresResync(e)
            : createRequiresResync(e, String.format(BSON_TOO_LARGE_MESSAGE, phase));
      } else if (isNotablescanError(e)) {
        throw createFailed(e, NOTABLE_SCAN_ERROR_MESSAGE);
      } else {
        throw createRequiresResync(e);
      }
    } catch (NamespaceResolutionException e) {
      throw fromNamespaceResolutionException(e);
    } catch (FieldExceededLimitsException e) {
      throw createFieldLimitExceeded(e.getMessage());
    } catch (DocsExceededLimitsException e) {
      throw createDocsLimitExceeded(e);
    } catch (CodecConfigurationException e) {
      // Sometimes mongod does not include $clusterTime or operationTime in the response,
      // which causes our deserialization to fail.
      // See SERVER-43086.
      throw createRequiresResync(e, "presumed transient issue decoding response from mongod");
    } catch (InitialSyncException e) {
      throw e;
    } catch (FragmentProcessingException e) {
      throw createRequiresResync(e);
    } catch (Exception e) {
      if (e instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }

      throw new AssertionError("threw unexpected checked exception: " + e, e);
    }
  }

  /**
   * Runs the supplied Runnable, wrapping well-known exception types into InitialSyncExceptions if
   * they are caught.
   */
  public static void wrapIfThrows(CheckedRunnable<Exception> runnable) throws InitialSyncException {
    wrapIfThrows(
        () -> {
          runnable.run();
          return null;
        },
        Phase.MAIN);
  }

  public static <T> T wrapIfThrowsCollectionScan(Callable<T> callable) throws InitialSyncException {
    return wrapIfThrows(callable, Phase.COLLECTION_SCAN);
  }

  public static <T> T wrapIfThrowsChangeStream(Callable<T> callable) throws InitialSyncException {
    return wrapIfThrows(callable, Phase.CHANGE_STREAM);
  }

  /**
   * Returns the resume info associated with the InitialSyncException if it is a INVALIDATED
   * exception.
   *
   * @return resume info associated with the INVALIDATED event
   * @throws IllegalStateException if the exception is not a INVALIDATED
   */
  public InitialSyncResumeInfo getResumeInfo() {
    checkState(
        this.type.equals(InitialSyncException.Type.INVALIDATED),
        "can only call getResumeInfo() on INVALIDATED InitialSyncException");
    checkState(
        this.resumeInfo.isPresent(), "resumeInfo not present for INVALIDATED InitialSyncException");

    return this.resumeInfo.get();
  }

  public Type getType() {
    return this.type;
  }

  public boolean isRequiresResync() {
    return this.type.equals(Type.REQUIRES_RESYNC);
  }

  public boolean isResumableTransient() {
    return this.type.equals(Type.RESUMABLE_TRANSIENT);
  }

  public boolean isDoesNotExist() {
    return this.type.equals(Type.DOES_NOT_EXIST);
  }

  public boolean isFailed() {
    return this.type.equals(Type.FAILED);
  }

  public boolean isInvalidated() {
    return this.type.equals(Type.INVALIDATED);
  }

  public boolean isDropped() {
    return this.type.equals(Type.DROPPED);
  }

  public boolean isShutdown() {
    return this.type.equals(Type.SHUT_DOWN);
  }

  public static boolean isNotablescanError(@Nullable Throwable cause) {
    if (cause instanceof MongoException mongoException) {
      return mongoException.getCode() == Errors.NO_QUERY_EXECUTION_PLANS.code
          && mongoException.getMessage() != null
          && mongoException.getMessage().contains("notablescan");
    }
    return false;
  }

  public static boolean isNoQueryExecutionPlansError(@Nullable Throwable cause) {
    if (cause instanceof MongoException mongoException) {
      return mongoException.getCode() == Errors.NO_QUERY_EXECUTION_PLANS.code;
    }
    return false;
  }

  public static boolean isInitialSyncIdMismatched(@Nullable Throwable cause) {
    return cause instanceof MongoCommandException mongoCommandException
        && mongoCommandException.getErrorCode() == Errors.INITIAL_SYNC_ID_MISMATCH.code;
  }

  public static boolean isBsonTooLargeError(@Nullable Throwable cause) {
    return cause instanceof MongoException mongoException
        && mongoException.getCode() == Errors.BSON_OBJECT_TOO_LARGE.code;
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public static boolean isNaturalOrderScanFailed(@Nullable Throwable cause) {
    if (cause == null) {
      return false;
    }

    if (cause instanceof UnsupportedMongoDbVersionException) {
      return true;
    }

    if (cause instanceof MongoCommandException mongoCommandException) {
      if (mongoCommandException.getErrorCode() == Errors.IDL_FAILED_TO_PARSE.code
          && mongoCommandException.getErrorMessage().contains(MISSING_FIELD_MESSAGE)) {
        return true;
      }
    }

    String unknownFieldMessage1 = "BSON field 'aggregate.$_startAt' is an unknown field";
    String unknownFieldMessage2 = "BSON field 'aggregate.$_requestResumeToken' is an unknown field";
    return cause instanceof MongoCommandException mongoCommandException
        && mongoCommandException.getErrorCode() == Errors.IDL_UNKNOWN_FIELD.code
        && Stream.of(unknownFieldMessage1, unknownFieldMessage2)
            .anyMatch(msg -> mongoCommandException.getErrorMessage().contains(msg));
  }

  public enum Phase {
    COLLECTION_SCAN,
    CHANGE_STREAM,
    MAIN
  }

  public enum Type {
    /** A non-resumable error that may resolve itself if we resync. */
    REQUIRES_RESYNC,

    /** A transient error from which we can resume the partially completed initial sync. */
    RESUMABLE_TRANSIENT,

    /**
     * The collection being replicated does not exist locally but exists on a different shard, so
     * try again since it may exist in the future.
     */
    DOES_NOT_EXIST,

    /** An error that will likely not resolve itself if we try again. */
    FAILED,

    /** An error requiring a new change stream from which to resume initial sync. */
    INVALIDATED,

    /** Index has exceeded a usage field limit. */
    FIELD_EXCEEDED,

    /** Index has exceeded docs limit */
    DOCS_EXCEEDED,

    /** The collection that is being replicated has been dropped or never existed. */
    DROPPED,

    /** The initial sync itself was cooperatively shut down. */
    SHUT_DOWN,
  }
}
