package com.xgen.mongot.catalogservice;

import com.xgen.mongot.catalogservice.schema.IndexStatusP;
import com.xgen.mongot.index.status.IndexStatus;

/**
 * Converts between the in-memory {@link IndexStatus} model and its persisted schema representation
 * {@link IndexStatusP}. Serialization lives on {@link IndexStatusP}; this mapper is the sole bridge
 * between the persisted record and the internal model.
 *
 * <p>{@link IndexStatusP} mirrors {@link IndexStatus.StatusCode} and {@link IndexStatus.Reason}
 * with its own enums; the conversions here are exact 1:1 mappings between the two value sets.
 */
public class IndexStatusMapper {

  private IndexStatusMapper() {}

  /** Maps an in-memory {@link IndexStatus} to its persisted form for writing. */
  public static IndexStatusP toPersisted(IndexStatus status) {
    return new IndexStatusP(
        toPersisted(status.getStatusCode()),
        status.getMessage(),
        status.getOptime(),
        status.getReason().map(IndexStatusMapper::toPersisted));
  }

  public static IndexStatusP.StatusCode toPersisted(IndexStatus.StatusCode statusCode) {
    return switch (statusCode) {
      case UNKNOWN -> IndexStatusP.StatusCode.UNKNOWN;
      case DOES_NOT_EXIST -> IndexStatusP.StatusCode.DOES_NOT_EXIST;
      case NOT_STARTED -> IndexStatusP.StatusCode.NOT_STARTED;
      case INITIAL_SYNC -> IndexStatusP.StatusCode.INITIAL_SYNC;
      case STALE -> IndexStatusP.StatusCode.STALE;
      case RECOVERING_TRANSIENT -> IndexStatusP.StatusCode.RECOVERING_TRANSIENT;
      case RECOVERING_NON_TRANSIENT -> IndexStatusP.StatusCode.RECOVERING_NON_TRANSIENT;
      case STEADY -> IndexStatusP.StatusCode.STEADY;
      case FAILED -> IndexStatusP.StatusCode.FAILED;
    };
  }

  public static IndexStatusP.Reason toPersisted(IndexStatus.Reason reason) {
    return switch (reason) {
      case INITIALIZATION_FAILED -> IndexStatusP.Reason.INITIALIZATION_FAILED;
      case INITIAL_SYNC_REPLICATION_FAILED -> IndexStatusP.Reason.INITIAL_SYNC_REPLICATION_FAILED;
      case STEADY_STATE_REPLICATION_FAILED -> IndexStatusP.Reason.STEADY_STATE_REPLICATION_FAILED;
      case USER_ERROR -> IndexStatusP.Reason.USER_ERROR;
      case EXCEED_MAX_LIMIT -> IndexStatusP.Reason.EXCEED_MAX_LIMIT;
      case COLLECTION_NOT_FOUND -> IndexStatusP.Reason.COLLECTION_NOT_FOUND;
      case INDEX_DROPPED -> IndexStatusP.Reason.INDEX_DROPPED;
      case AUTO_EMBEDDING_RESOLUTION_FAILED -> IndexStatusP.Reason.AUTO_EMBEDDING_RESOLUTION_FAILED;
      case AUTO_EMBEDDING_RESOLUTION_RETRY -> IndexStatusP.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY;
      case INITIAL_SYNC_REPLICATION_FAILED_RETRY ->
          IndexStatusP.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY;
    };
  }

  /** Reconstructs an in-memory {@link IndexStatus} from a persisted record read back from state. */
  public static IndexStatus fromPersisted(IndexStatusP persisted) {
    return IndexStatus.fromPersisted(
        fromPersisted(persisted.statusCode()),
        persisted.message(),
        persisted.optime(),
        persisted.reason().map(IndexStatusMapper::fromPersisted));
  }

  public static IndexStatus.StatusCode fromPersisted(IndexStatusP.StatusCode statusCode) {
    return switch (statusCode) {
      case UNKNOWN -> IndexStatus.StatusCode.UNKNOWN;
      case DOES_NOT_EXIST -> IndexStatus.StatusCode.DOES_NOT_EXIST;
      case NOT_STARTED -> IndexStatus.StatusCode.NOT_STARTED;
      case INITIAL_SYNC -> IndexStatus.StatusCode.INITIAL_SYNC;
      case STALE -> IndexStatus.StatusCode.STALE;
      case RECOVERING_TRANSIENT -> IndexStatus.StatusCode.RECOVERING_TRANSIENT;
      case RECOVERING_NON_TRANSIENT -> IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT;
      case STEADY -> IndexStatus.StatusCode.STEADY;
      case FAILED -> IndexStatus.StatusCode.FAILED;
    };
  }

  public static IndexStatus.Reason fromPersisted(IndexStatusP.Reason reason) {
    return switch (reason) {
      case INITIALIZATION_FAILED -> IndexStatus.Reason.INITIALIZATION_FAILED;
      case INITIAL_SYNC_REPLICATION_FAILED -> IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED;
      case STEADY_STATE_REPLICATION_FAILED -> IndexStatus.Reason.STEADY_STATE_REPLICATION_FAILED;
      case USER_ERROR -> IndexStatus.Reason.USER_ERROR;
      case EXCEED_MAX_LIMIT -> IndexStatus.Reason.EXCEED_MAX_LIMIT;
      case COLLECTION_NOT_FOUND -> IndexStatus.Reason.COLLECTION_NOT_FOUND;
      case INDEX_DROPPED -> IndexStatus.Reason.INDEX_DROPPED;
      case AUTO_EMBEDDING_RESOLUTION_FAILED -> IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_FAILED;
      case AUTO_EMBEDDING_RESOLUTION_RETRY -> IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY;
      case INITIAL_SYNC_REPLICATION_FAILED_RETRY ->
          IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY;
    };
  }
}
