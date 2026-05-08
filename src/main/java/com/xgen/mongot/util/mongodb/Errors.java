package com.xgen.mongot.util.mongodb;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import java.util.Set;

/**
 * Errors contains MongoDB server errors.
 *
 * <p>Taken from https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
 */
public class Errors {

  public static class Error {
    public final int code;
    public final String name;

    private Error(int code, String name) {
      this.code = code;
      this.name = name;
    }
  }

  public static final Error BAD_VALUE = new Error(2, "BadValue");
  public static final Error UNAUTHORIZED = new Error(13, "Unauthorized");
  public static final Error AUTHENTICATION_FAILED = new Error(18, "AuthenticationFailed");
  public static final Error NAMESPACE_NOT_FOUND = new Error(26, "NamespaceNotFound");
  public static final Error INDEX_NOT_FOUND = new Error(27, "IndexNotFound");

  public static final Error CURSOR_NOT_FOUND = new Error(43, "CursorNotFound");

  public static final Error INDEX_ALREADY_EXISTS = new Error(68, "IndexAlreadyExists");
  public static final Error AUTH_SCHEMA_INCOMPATIBLE = new Error(69, "AuthSchemaIncompatible");
  public static final Error SHARD_NOT_FOUND = new Error(70, "ShardNotFound");

  public static final Error COMMAND_FAILED = new Error(125, "CommandFailed");

  public static final Error CAPPED_POSITION_LOST = new Error(136, "CappedPositionLost");

  public static final Error QUERY_PLAN_KILLED = new Error(175, "QueryPlanKilled");

  public static final Error KEY_NOT_FOUND = new Error(211, "KeyNotFound");

  public static final Error AUTHENTICATION_RESTRICTION_UNMET =
      new Error(214, "AuthenticationRestrictionUnmet");

  public static final Error CHANGE_STREAM_FATAL_ERROR = new Error(280, "ChangeStreamFatalError");

  public static final Error CHANGE_STREAM_HISTORY_LOST = new Error(286, "ChangeStreamHistoryLost");
  public static final Error INVALID_INDEX_SPECIFICATION_OPTION =
      new Error(197, "InvalidIndexSpecificationOption");
  public static final Error INDEX_INFORMATION_TOO_LARGE =
      new Error(396, "IndexInformationTooLarge");

  public static final Error INGRESS_REQUEST_RATE_LIMIT_EXCEEDED =
      new Error(462, "IngressRequestRateLimitExceeded");

  // Reserved for future load-shedding specific rejection code.
  // Keep unused for now until mongo-side code is finalized and rolled out.
  public static final Error SEARCH_REQUEST_REJECTED_DUE_TO_OVERLOAD =
      new Error(489, "SearchRequestRejectedDueToOverload");

  public static final Error USER_WRITES_BLOCKED = new Error(371, "UserWritesBlocked");

  // SystemOverloadedError category — server under resource pressure.
  public static final Error ADMISSION_QUEUE_OVERFLOW = new Error(433, "AdmissionQueueOverflow");
  public static final Error RATE_LIMIT_EXCEEDED = new Error(449, "RateLimitExceeded");
  public static final Error POOLED_CONNECTION_ACQUISITION_REJECTED =
      new Error(450, "PooledConnectionAcquisitionRejected");
  public static final Error INTERRUPTED_DUE_TO_OVERLOAD =
      new Error(473, "InterruptedDueToOverload");

  public static final Error EXCEEDED_DISK_LIMIT = new Error(14031, "ExceededDiskLimit");

  public static final Error BSON_OBJECT_TOO_LARGE = new Error(10334, "BSONObjectTooLarge");

  public static final Error IDL_UNKNOWN_FIELD = new Error(40415, "IDLUnknownField");

  public static final Error IDL_FAILED_TO_PARSE = new Error(40414, "IDLFailedToParse");
  public static final Error INITIAL_SYNC_ID_MISMATCH = new Error(8132701, "InitialSyncIdMismatch");
  public static final Error NO_QUERY_EXECUTION_PLANS = new Error(291, "NoQueryExecutionPlans");

  // “489” is reserved for future load-shedding specific rejection code.
  public static final Set<Integer> RETRYABLE_ERROR_CODES =
      Set.of(
          6, 7, 63, 89, 91, 133, 150, 189, 234, 262, 462, 489, 9001, 10107, 11600, 11602, 13388,
          13435, 13436);

  // SystemOverloadedError codes that are not already in RETRYABLE_ERROR_CODES (462 and 489 are).
  // These indicate the server is under resource pressure and writes should be retried.
  public static final Set<Integer> SYSTEM_OVERLOADED_ERROR_CODES =
      Set.of(
          Errors.ADMISSION_QUEUE_OVERFLOW.code,
          Errors.RATE_LIMIT_EXCEEDED.code,
          Errors.POOLED_CONNECTION_ACQUISITION_REJECTED.code,
          Errors.INTERRUPTED_DUE_TO_OVERLOAD.code);

  public static final Set<Integer> NON_INVALIDATING_ERROR_CODES =
      Set.of(
          Errors.CAPPED_POSITION_LOST.code,
          Errors.CHANGE_STREAM_FATAL_ERROR.code,
          Errors.CHANGE_STREAM_HISTORY_LOST.code,
          Errors.BSON_OBJECT_TOO_LARGE.code);

  public static final Set<Integer> INVALID_AUTHENTICATION_ERROR_CODES =
      Set.of(
          Errors.UNAUTHORIZED.code,
          Errors.AUTHENTICATION_FAILED.code,
          Errors.AUTH_SCHEMA_INCOMPATIBLE.code,
          Errors.AUTHENTICATION_RESTRICTION_UNMET.code);

  public static boolean isMatchCollectionUuidUnsupportedException(Throwable cause) {
    String unknownFieldMessage =
        "BSON field '$changeStream.matchCollectionUUIDForUpdateLookup' is an unknown field.";
    return cause instanceof MongoCommandException mongoCommandException
        && mongoCommandException.getErrorCode() == IDL_UNKNOWN_FIELD.code
        && mongoCommandException.getErrorMessage().contains(unknownFieldMessage);
  }

  public static boolean isIngressRequestRateLimitError(Throwable cause) {
    return cause instanceof MongoException mongoException
        && mongoException.getCode() == INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code;
  }
}
