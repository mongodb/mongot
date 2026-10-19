package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkState;

import com.mongodb.MongoException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.mongot.util.regex.RegexInterruptedException;
import com.xgen.mongot.util.regex.RegexLimitExceededException;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.jetbrains.annotations.Nullable;

public class SteadyStateException extends Exception {

  public static final String BSON_TOO_LARGE_MESSAGE =
      "Unrecoverable replication error due to change stream payload exceeding 16MB BSON limit."
          + " Scheduling index for rebuild, existing index may be stale but will remain queryable"
          + " until rebuild completes. To avoid the issue see: "
          + "https://www.mongodb.com/docs/atlas/atlas-search/manage-indexes/#disk--memory--and-resource-usage.";

  private final Type type;
  private final Optional<ChangeStreamResumeInfo> resumeInfo;

  private SteadyStateException(Type type) {
    super();

    this.type = type;
    this.resumeInfo = Optional.empty();
  }

  private SteadyStateException(ChangeStreamResumeInfo resumeInfo, Type type) {
    super();

    this.type = type;
    this.resumeInfo = Optional.of(resumeInfo);
  }

  private SteadyStateException(Type type, @Nullable Throwable cause) {
    super(cause);

    this.type = type;
    this.resumeInfo = Optional.empty();
  }

  private SteadyStateException(Type type, String message) {
    super(message);

    this.type = type;
    this.resumeInfo = Optional.empty();
  }

  private SteadyStateException(Type type, @Nullable Throwable cause, String message) {
    super(message, cause);

    this.type = type;
    this.resumeInfo = Optional.empty();
  }

  public static SteadyStateException createTransient(Throwable cause) {
    return new SteadyStateException(Type.TRANSIENT, cause);
  }

  public static SteadyStateException createTransient(Throwable cause, String message) {
    return new SteadyStateException(Type.TRANSIENT, cause, message);
  }

  public static SteadyStateException createNonInvalidatingResync(Throwable cause) {
    return new SteadyStateException(Type.NON_INVALIDATING_RESYNC, cause);
  }

  public static SteadyStateException createNonInvalidatingResync(Throwable cause, String message) {
    return new SteadyStateException(Type.NON_INVALIDATING_RESYNC, cause, message);
  }

  public static SteadyStateException createRequiresResync(String message) {
    return new SteadyStateException(Type.REQUIRES_RESYNC, message);
  }

  public static SteadyStateException createRequiresResync(Throwable cause) {
    return new SteadyStateException(Type.REQUIRES_RESYNC, cause);
  }

  public static SteadyStateException createFieldExceeded(String message) {
    return new SteadyStateException(Type.FIELD_EXCEEDED, message);
  }

  public static SteadyStateException createAnalysisLimitExceeded(String message) {
    return new SteadyStateException(Type.ANALYSIS_LIMIT_EXCEEDED, message);
  }

  public static SteadyStateException createDocsExceeded(Throwable cause) {
    return new SteadyStateException(Type.DOCS_EXCEEDED, cause);
  }

  public static SteadyStateException createRenamed(ChangeStreamResumeInfo resumeInfo) {
    return new SteadyStateException(resumeInfo, Type.RENAMED);
  }

  public static SteadyStateException createInvalidated(ChangeStreamResumeInfo resumeInfo) {
    return new SteadyStateException(resumeInfo, Type.INVALIDATED);
  }

  public static SteadyStateException createDropped() {
    return new SteadyStateException(Type.DROPPED);
  }

  public static SteadyStateException createDropped(String message) {
    return new SteadyStateException(Type.DROPPED, message);
  }

  public static SteadyStateException createDropped(Throwable cause, String message) {
    return new SteadyStateException(Type.DROPPED, cause, message);
  }

  public static SteadyStateException createShutDown() {
    return new SteadyStateException(Type.SHUT_DOWN);
  }

  public static boolean isShutDown(Throwable throwable) {
    return throwable instanceof SteadyStateException
        && ((SteadyStateException) throwable).isShutdown();
  }

  /**
   * Runs the supplied Callable, wrapping well-known exception types into SteadyStateException if
   * they are caught. Exception translation to error types matches the one defined in {@link
   * InitialSyncException}, except for {@link CodecConfigurationException} which results with a
   * transient error.
   *
   * <p>Unexpected errors in replication logic or driver's internal implementation, in the case of
   * synchronous change stream, are propagated as is. Such exception will cause the index to enter
   * to FAILED state.
   */
  public static <T> T wrapIfThrows(Callable<T> callable) throws SteadyStateException {
    try {
      return callable.call();
    } catch (InterruptedException | RegexInterruptedException e) {
      throw createShutDown();
    } catch (RegexLimitExceededException e) {
      throw createAnalysisLimitExceeded(e.getMessage());
    } catch (MongoException e) {
      throw wrapMongoException(e);
    } catch (ChangeStreamCursorClientException e) {
      throw createTransient(e);
    } catch (NamespaceResolutionException e) {
      throw fromNamespaceResolutionException(e);
    } catch (FieldExceededLimitsException e) {
      throw createFieldExceeded(e.getMessage());
    } catch (CodecConfigurationException e) {
      // Sometimes mongod does not include $clusterTime or operationTime in the response,
      // which causes our deserialization to fail. See SERVER-43086 and CLOUDP-57263.
      throw createTransient(e, "presumed transient issue decoding response from mongod");
    } catch (FragmentProcessingException e) {
      throw createNonInvalidatingResync(e);
    } catch (SteadyStateException e) {
      throw e;
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new AssertionError("threw unexpected checked exception: " + e, e);
    }
  }

  public static SteadyStateException wrapMongoException(MongoException e) {
    if (e.getCode() == Errors.BSON_OBJECT_TOO_LARGE.code) {
      // Add details about the root cause of BSON_OBJECT_TOO_LARGE and hide customer information
      return createNonInvalidatingResync(e, BSON_TOO_LARGE_MESSAGE);
    }

    if (MongoExceptionUtils.isNonInvalidatingResyncable(e)) {
      return createNonInvalidatingResync(e);
    }

    return createTransient(e);
  }

  /** Converts a NamespaceResolutionException into an InitialSyncException. */
  private static SteadyStateException fromNamespaceResolutionException(
      NamespaceResolutionException e) {
    return createDropped(
        e, String.format("from NamespaceResolutionException (%s)", e.getMessage()));
  }

  public Type getType() {
    return this.type;
  }

  public boolean isTransient() {
    return this.type.equals(Type.TRANSIENT);
  }

  public boolean isRequiresResync() {
    return this.type.equals(Type.REQUIRES_RESYNC);
  }

  public boolean isNonInvalidatingResync() {
    return this.type.equals(Type.NON_INVALIDATING_RESYNC);
  }

  public boolean isRenamed() {
    return this.type.equals(Type.RENAMED);
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

  /**
   * Returns the resume info associated with the SteadyStateException if it is a RENAMED or
   * INVALIDATED exception.
   *
   * @return resume info associated with the RENAME or INVALIDATED event
   * @throws IllegalStateException if the exception is not a RENAMED or INVALIDATED
   */
  public ChangeStreamResumeInfo getResumeInfo() {
    checkState(
        this.type.equals(Type.RENAMED) || this.type.equals(Type.INVALIDATED),
        "can only call getResumeInfo() on RENAMED or INVALIDATED SteadyStateException");
    checkState(
        this.resumeInfo.isPresent(),
        "resumeInfo not present for RENAMED or INVALIDATED SteadyStateException");

    return this.resumeInfo.get();
  }

  public enum Type {
    /** A transient error from which we can resume steady state replication. */
    TRANSIENT,

    /** A non-resumable error that may resolve itself if we resync. */
    REQUIRES_RESYNC,

    /** An error requiring a resync, during which we maintain the current index. */
    NON_INVALIDATING_RESYNC,

    /** Index has exceeded a usage limit of fields. */
    FIELD_EXCEEDED,

    /** Custom analyzer regex matching exhausted its resource budget. */
    ANALYSIS_LIMIT_EXCEEDED,

    /** Index has reached the limit of documents it is able to index. */
    DOCS_EXCEEDED,

    /** The collection that is being replicated has been renamed. */
    RENAMED,

    /** An error requiring a new change stream from which to resume steady state replication. */
    INVALIDATED,

    /** The collection that is being replicated has been dropped. */
    DROPPED,

    /** Steady state replication was cooperatively shut down. */
    SHUT_DOWN,
  }
}
