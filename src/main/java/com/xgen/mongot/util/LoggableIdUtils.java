package com.xgen.mongot.util;

import com.google.common.flogger.FluentLogger;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonType;
import org.bson.BsonValue;

/**
 * Utility class for creating loggable document IDs that avoid PII leakage.
 *
 * <p>UUIDs and ObjectIds are returned as-is without hashing. Other ID types cannot be logged and
 * return "unloggable".
 *
 * <p>This class uses a global feature flag to control whether document IDs are logged. The feature
 * must be initialized via {@link #initialize(boolean)} before use. If not initialized, the default
 * behavior is to NOT log document IDs (returns "unknown").
 */
public final class LoggableIdUtils {

  /** The string returned when a document ID cannot be logged to avoid PII leakage. */
  public static final String UNKNOWN_LOGGABLE_ID = "unknown";

  /** The string returned when an ID type cannot be logged. */
  public static final String UNLOGGABLE_ID_TYPE = "unloggable";

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  /**
   * Global flag to control whether document IDs are logged. Defaults to false (disabled) for
   * safety. Must be initialized via {@link #initialize(boolean)} at application startup.
   */
  private static volatile boolean loggableDocumentIdEnabled = false;

  /**
   * Initializes the loggable document ID feature flag.
   *
   * <p>This method should be called once at application startup with the value from {@code
   * FeatureFlags.isEnabled(Feature.LOGGABLE_DOCUMENT_ID)}.
   *
   * @param enabled whether the loggable document ID feature is enabled
   */
  public static void initialize(boolean enabled) {
    loggableDocumentIdEnabled = enabled;
  }

  /**
   * Returns whether the loggable document ID feature is enabled.
   *
   * @return true if document IDs should be logged, false otherwise
   */
  public static boolean isEnabled() {
    return loggableDocumentIdEnabled;
  }

  private LoggableIdUtils() {
    // Utility class, prevent instantiation
  }

  /**
   * Converts a BsonBinary with UUID_STANDARD subtype to a UUID string.
   *
   * <p>This method safely extracts the UUID from the binary data without relying on
   * BsonBinary.asUuid(), which has inconsistent internal error checking.
   *
   * @param binary the BsonBinary value
   * @return Optional containing the UUID string if the binary is a valid standard UUID, empty
   *     otherwise
   */
  public static Optional<String> binaryToUuidString(BsonBinary binary) {
    if (binary == null) {
      return Optional.empty();
    }

    byte subType = binary.getType();
    if (subType != BsonBinarySubType.UUID_STANDARD.getValue()) {
      return Optional.empty();
    }

    byte[] data = binary.getData();
    if (data.length != 16) {
      return Optional.empty();
    }

    ByteBuffer bb = ByteBuffer.wrap(data);
    return Optional.of(new UUID(bb.getLong(), bb.getLong()).toString());
  }

  /**
   * Returns a loggable ID string from a BsonValue document ID.
   *
   * <p>If the loggable document ID feature is disabled (via {@link #initialize(boolean)}), returns
   * {@link #UNKNOWN_LOGGABLE_ID} immediately without processing the ID.
   *
   * <p>If the feature is enabled and the ID is a UUID or ObjectId, it is returned as-is without
   * hashing. Other ID types cannot be logged and return "unloggable".
   *
   * <p>This method performs BsonValue type checking and potentially UUID byte conversion. It should
   * ONLY be called in:
   *
   * <ul>
   *   <li>Rate-limited logging contexts (e.g., wrapped in {@code atMostEvery()} or {@code lazy()})
   *   <li>Exception handling paths (low-frequency error scenarios)
   * </ul>
   *
   * <p>Do NOT call this method in hot paths or tight loops.
   *
   * @param id BsonValue document ID, may be null
   * @return a loggable ID string - original value if it's a UUID or ObjectId and feature is
   *     enabled, otherwise "unloggable" or "unknown"
   */
  public static String getLoggableId(BsonValue id) {
    if (!isEnabled()) {
      return UNKNOWN_LOGGABLE_ID;
    }

    if (id == null) {
      return UNKNOWN_LOGGABLE_ID;
    }

    try {
      BsonType bsonType = id.getBsonType();

      // Return ObjectId hex string as-is
      if (bsonType == BsonType.OBJECT_ID) {
        return id.asObjectId().getValue().toHexString();
      }

      // Handle Binary types (UUIDs)
      if (bsonType == BsonType.BINARY) {
        byte subType = id.asBinary().getType();

        // Standard UUID (subtype 4) - safe to convert
        if (subType == BsonBinarySubType.UUID_STANDARD.getValue()) {
          return binaryToUuidString(id.asBinary()).orElse(UNLOGGABLE_ID_TYPE);
        }

        // Legacy UUID (subtype 3) - byte order is ambiguous, cannot safely convert to UUID string
        // Return "unloggable" to avoid incorrect UUID representation
        if (subType == BsonBinarySubType.UUID_LEGACY.getValue()) {
          GlobalMetricFactory.incrementUnloggableIdType("BINARY_UUID_LEGACY");
          return UNLOGGABLE_ID_TYPE;
        }
      }

      // Other types cannot be logged
      GlobalMetricFactory.incrementUnloggableIdType(bsonType.name());
      return UNLOGGABLE_ID_TYPE;
    } catch (Exception e) {
      // Rate-limited logging to avoid log spam if this somehow gets called frequently
      FLOGGER.atSevere().atMostEvery(1, TimeUnit.HOURS).withCause(e).log(
          "Unexpected error getting loggable ID from BsonValue");
      return UNKNOWN_LOGGABLE_ID;
    }
  }
}
