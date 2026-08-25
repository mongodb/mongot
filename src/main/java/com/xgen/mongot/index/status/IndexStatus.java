package com.xgen.mongot.index.status;

import com.google.common.base.Objects;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.bson.BsonTimestamp;

public class IndexStatus {
  public static final String INVALID_DEFINITION_MESSAGE_PREFIX = "Invalid definition: ";

  private static final Map<Reason, Predicate<StatusCode>> reasonValidationMap =
      new EnumMap<>(Reason.class);

  static {
    reasonValidationMap.put(
        Reason.INITIALIZATION_FAILED, statusCode -> statusCode == StatusCode.FAILED);
    reasonValidationMap.put(
        Reason.AUTO_EMBEDDING_RESOLUTION_FAILED, statusCode -> statusCode == StatusCode.FAILED);
    reasonValidationMap.put(
        Reason.AUTO_EMBEDDING_RESOLUTION_RETRY, statusCode -> statusCode == StatusCode.FAILED);
    reasonValidationMap.put(
        Reason.INITIAL_SYNC_REPLICATION_FAILED, statusCode -> statusCode == StatusCode.FAILED);
    reasonValidationMap.put(
        Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY,
        statusCode -> statusCode == StatusCode.FAILED);
    reasonValidationMap.put(
        Reason.STEADY_STATE_REPLICATION_FAILED, statusCode -> statusCode == StatusCode.FAILED);
    reasonValidationMap.put(Reason.EXCEED_MAX_LIMIT, statusCode -> statusCode == StatusCode.FAILED);
    reasonValidationMap.put(
        Reason.COLLECTION_NOT_FOUND, statusCode -> statusCode == StatusCode.DOES_NOT_EXIST);
    reasonValidationMap.put(
        Reason.INDEX_DROPPED, statusCode -> statusCode == StatusCode.DOES_NOT_EXIST);
  }

  private static final Set<StatusCode> QUERIES_SERVICEABLE_STATUS_CODES =
      Set.of(
          StatusCode.STEADY,
          StatusCode.RECOVERING_TRANSIENT,
          StatusCode.RECOVERING_NON_TRANSIENT,
          StatusCode.STALE);

  public static IndexStatus doesNotExist(Reason reason) {
    return new IndexStatus(
        StatusCode.DOES_NOT_EXIST, Optional.empty(), Optional.empty(), Optional.of(reason));
  }

  private final StatusCode statusCode;
  private final Optional<String> message;
  private final Optional<BsonTimestamp> optime;
  private final Optional<Reason> reason;

  public IndexStatus(StatusCode statusCode) {
    this(statusCode, Optional.empty());
  }

  private IndexStatus(StatusCode statusCode, Optional<String> message) {
    this(statusCode, message, Optional.empty(), Optional.empty());
  }

  private IndexStatus(
      StatusCode statusCode,
      Optional<String> message,
      Optional<BsonTimestamp> optime,
      Optional<Reason> reason) {
    validateReasonForStatus(statusCode, reason);
    this.statusCode = statusCode;
    this.message = message;
    this.optime = optime;
    this.reason = reason;
  }

  private void validateReasonForStatus(StatusCode statusCode, Optional<Reason> reason) {
    reason.ifPresent(
        r -> {
          Predicate<StatusCode> validator = reasonValidationMap.get(r);
          if (validator != null && !validator.test(statusCode)) {
            throw new IllegalArgumentException(
                String.format("Reason %s is not valid for status %s", r, statusCode));
          }
        });
  }

  /**
   * Note: Because we are persisting the names of this enum in IndexCommitUserData extra care must
   * be taken when adding, removing, changing any of these values
   */
  public enum StatusCode {
    /**
     * The {@link com.xgen.mongot.index.Index} class is created but the status is not decided yet.
     */
    UNKNOWN,

    /** The source that is being indexed does not exist or the index is already dropped */
    DOES_NOT_EXIST,

    /** The index requires initial sync or blobstore download. */
    NOT_STARTED,

    /** The index is syncing from the source. */
    INITIAL_SYNC,

    /**
     * The index has become stale, is not attempting to recover, and is being kept around to service
     * queries.
     */
    STALE,

    /**
     * The index has encountered a transient error during steady state replication and has become
     * stale. It is attempting to resume from the same point without enqueuing a new initial sync.
     * If the resume token will expire before a successful recovery, the index will transition to
     * RECOVERING_NON_TRANSIENT.
     */
    RECOVERING_TRANSIENT,

    /**
     * The index has encountered a non-transient error during steady state and has become stale. It
     * is being kept around to service queries while the replacement is in initial sync.
     */
    RECOVERING_NON_TRANSIENT,

    /** The index has been synced and is replicating, ready to service queries. */
    STEADY,

    /** The index has failed. */
    FAILED;
  }

  /** This enum defines various reasons for the status of an index. */
  public enum Reason {
    /** The index has failed during the initialization phase. */
    INITIALIZATION_FAILED,
    INITIAL_SYNC_REPLICATION_FAILED,
    STEADY_STATE_REPLICATION_FAILED,
    USER_ERROR,
    EXCEED_MAX_LIMIT,
    COLLECTION_NOT_FOUND,
    INDEX_DROPPED,
    /** Fails to resolve derived definition of an auto embedding index. */
    AUTO_EMBEDDING_RESOLUTION_FAILED,
    /** Fails to resolve derived definition of an auto embedding index but recoverable. */
    AUTO_EMBEDDING_RESOLUTION_RETRY,
    /** Failed to replicate during initial sync but recoverable. */
    INITIAL_SYNC_REPLICATION_FAILED_RETRY
  }

  public static IndexStatus unknown() {
    return new IndexStatus(StatusCode.UNKNOWN);
  }

  public static IndexStatus notStarted() {
    return new IndexStatus(StatusCode.NOT_STARTED);
  }

  public boolean isIndexDropped() {
    return this.getStatusCode() == IndexStatus.StatusCode.DOES_NOT_EXIST
        && this.getReason().map(reason -> reason == IndexStatus.Reason.INDEX_DROPPED).orElse(true);
  }

  public boolean isCollectionNotFound() {
    return this.getStatusCode() == IndexStatus.StatusCode.DOES_NOT_EXIST
        && this.getReason().map(reason -> reason == Reason.COLLECTION_NOT_FOUND).orElse(false);
  }

  public boolean isUnknown() {
    return this.getStatusCode() == StatusCode.UNKNOWN;
  }

  public static IndexStatus initialSync() {
    return new IndexStatus(StatusCode.INITIAL_SYNC);
  }

  public static IndexStatus initialSync(String message) {
    return new IndexStatus(
        StatusCode.INITIAL_SYNC, Optional.of(message), Optional.empty(), Optional.empty());
  }

  public static IndexStatus stale(String reason, BsonTimestamp optime) {
    return new IndexStatus(
        StatusCode.STALE, Optional.of(reason), Optional.of(optime), Optional.empty());
  }

  public static IndexStatus recoveringNonTransient(BsonTimestamp optime) {
    return new IndexStatus(
        StatusCode.RECOVERING_NON_TRANSIENT,
        Optional.of(RecoveringStatusReason.RECOVERING.formatMessage()),
        Optional.of(optime),
        Optional.empty());
  }

  public static IndexStatus recoveringTransient(String message) {
    return new IndexStatus(
        StatusCode.RECOVERING_TRANSIENT, Optional.of(message), Optional.empty(), Optional.empty());
  }

  public static IndexStatus steady() {
    return new IndexStatus(StatusCode.STEADY);
  }

  public static IndexStatus failed(String message) {
    return new IndexStatus(StatusCode.FAILED, Optional.of(message));
  }

  public static IndexStatus failed(String message, Reason reason) {
    return new IndexStatus(
        StatusCode.FAILED, Optional.of(message), Optional.empty(), Optional.of(reason));
  }

  public static IndexStatus fromIndexStateInfo(StatusCode statusCode, Reason reason) {
    return new IndexStatus(statusCode, Optional.empty(), Optional.empty(), Optional.of(reason));
  }

  /**
   * Reconstructs an in-memory {@link IndexStatus} from fields read out of persisted state (see
   * {@link com.xgen.mongot.catalogservice.schema.IndexStatusP}). This is a distinct factory so any
   * future exemption from reason/statusCode validation for persisted reads has a clear, single
   * place to be made.
   */
  public static IndexStatus fromPersisted(
      StatusCode statusCode,
      Optional<String> message,
      Optional<BsonTimestamp> optime,
      Optional<Reason> reason) {
    return new IndexStatus(statusCode, message, optime, reason);
  }

  public StatusCode getStatusCode() {
    return this.statusCode;
  }

  public Optional<String> getMessage() {
    return this.message;
  }

  public Optional<BsonTimestamp> getOptime() {
    return this.optime;
  }

  public boolean canServiceQueries() {
    return canServiceQueries(this.statusCode);
  }

  /**
   * Same as {@link #canServiceQueries()} but callable from a bare {@link StatusCode}, without
   * needing a fully-constructed (and validated) {@link IndexStatus}.
   */
  public static boolean canServiceQueries(StatusCode statusCode) {
    return QUERIES_SERVICEABLE_STATUS_CODES.contains(statusCode);
  }

  public boolean canBeRecovered() {
    return this.reason
        .map(
            reason ->
                reason == Reason.INITIALIZATION_FAILED
                    || reason == Reason.AUTO_EMBEDDING_RESOLUTION_RETRY)
        .orElse(false);
  }

  public Optional<Reason> getReason() {
    return this.reason;
  }

  @Override
  public String toString() {
    return String.format(
        "IndexStatus{statusCode=%s, message=%s, reason=%s}",
        this.statusCode, this.message, this.reason);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexStatus that = (IndexStatus) o;
    return this.statusCode == that.statusCode
        && Objects.equal(this.message, that.message)
        && Objects.equal(this.optime, that.optime)
        && Objects.equal(this.reason, that.reason);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.statusCode, this.message, this.optime, this.reason);
  }
}
