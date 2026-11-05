package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.argNotNull;
import static com.xgen.mongot.util.Check.checkArg;

import com.google.errorprone.annotations.Var;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * Utility class for dealing with change stream resume tokens: reading the raw {@code _data}
 * encoding, pulling the opTime out of it, and ordering tokens by stream position.
 */
public class ResumeTokenUtils {

  // First byte of _data, identifying the rest as a timestamp.
  private static final byte TIMESTAMP_IDENTIFIER = (byte) -126;

  // Type byte plus an eight byte clusterTime.
  private static final int MIN_DATA_BYTES = 9;
  private static final int MIN_DATA_HEX_CHARS = 2 * MIN_DATA_BYTES;

  private static final Pattern VALID_HEX_CHARS = Pattern.compile("[0-9a-fA-F]+");

  private static final int KEY_STRING_ZERO = 0x29;
  private static final int KEY_STRING_POSITIVE_ONE_BYTE_INT = 0x2B;
  private static final int KEY_STRING_FALSE = 0x6E;
  private static final int KEY_STRING_TRUE = 0x6F;
  private static final int HIGH_WATER_MARK_TOKEN = 0;
  private static final int EVENT_TOKEN = 128;

  /**
   * Orders resume tokens by stream position, not just optime. {@code _data} is an order-preserving
   * KeyString, so comparing it directly also breaks ties between events that share a clusterTime —
   * something {@link #opTimeFromResumeToken} can't do.
   *
   * <p>Case insensitive, since the server emits uppercase hex but {@code ChangeStreamUtils} emits
   * lowercase. Throws {@link IllegalArgumentException} on a null or malformed token rather than
   * {@code NullPointerException}.
   */
  public static final Comparator<BsonDocument> BY_STREAM_POSITION =
      Comparator.comparing(
          ResumeTokenUtils::comparableDataFromResumeToken, String.CASE_INSENSITIVE_ORDER);

  /**
   * Returns the raw hex-encoded {@code _data} value of {@code resumeToken}.
   *
   * @throws IllegalArgumentException if {@code _data} is missing or not a string
   */
  public static String dataFromResumeToken(BsonDocument resumeToken) {
    argNotNull(resumeToken, "resumeToken");
    checkArg(resumeToken.isString("_data"), "resumeToken did not have a _data field.");
    return resumeToken.getString("_data").getValue();
  }

  // Functions below are adapted from the original MongoDB code.
  // Reference: https://github.com/10gen/mongo/blob/master/src/mongo/db/pipeline/resume_token.cpp

  /** Returns the decoded token prefix when its format is supported. */
  public static Optional<ResumeTokenPrefix> tryParseResumeTokenPrefix(BsonDocument resumeToken) {
    try {
      return Optional.of(parseResumeTokenPrefix(resumeToken));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  /** The prefix fields needed to select the correct initial cursor position. */
  public record ResumeTokenPrefix(BsonTimestamp operationTime, boolean fromInvalidate) {}

  private static ResumeTokenPrefix parseResumeTokenPrefix(BsonDocument resumeToken) {
    ByteBuffer data;
    try {
      data = ByteBuffer.wrap(Hex.decodeHex(dataFromResumeToken(resumeToken)));
    } catch (DecoderException e) {
      throw new IllegalArgumentException("Resume token _data was not valid hex", e);
    }
    checkArg(data.remaining() >= MIN_DATA_BYTES, "Resume token did not contain a timestamp");
    checkArg(data.get() == TIMESTAMP_IDENTIFIER, "Resume token did not start with a timestamp");
    BsonTimestamp operationTime = new BsonTimestamp(data.getLong(1));
    data.position(MIN_DATA_BYTES);

    // Follow MongoDB's ResumeToken::getData prefix decoding. The remaining fields stay opaque.
    // https://github.com/10gen/mongo/blob/master/src/mongo/db/pipeline/resume_token.cpp#L254
    int version = readNonnegativeInteger(data);
    checkArg(version <= 2, "Unsupported resume token version: %s", version);
    if (version == 0) {
      readNonnegativeInteger(data);
      return new ResumeTokenPrefix(operationTime, false);
    }

    int tokenType = readNonnegativeInteger(data);
    checkArg(
        tokenType == HIGH_WATER_MARK_TOKEN || tokenType == EVENT_TOKEN,
        "Unrecognized resume token type: %s",
        tokenType);
    int txnOpIndex = readNonnegativeInteger(data);
    checkArg(data.hasRemaining(), "Resume token did not contain fromInvalidate");
    int flag = Byte.toUnsignedInt(data.get());
    checkArg(
        flag == KEY_STRING_FALSE || flag == KEY_STRING_TRUE,
        "Resume token fromInvalidate was not a boolean");
    checkArg(
        tokenType != HIGH_WATER_MARK_TOKEN || (txnOpIndex == 0 && flag == KEY_STRING_FALSE),
        "Invalid high-water-mark resume token");
    return new ResumeTokenPrefix(operationTime, flag == KEY_STRING_TRUE);
  }

  private static int readNonnegativeInteger(ByteBuffer data) {
    checkArg(data.hasRemaining(), "Resume token integer was missing");
    int type = Byte.toUnsignedInt(data.get());
    if (type == KEY_STRING_ZERO) {
      return 0;
    }

    int size = type - KEY_STRING_POSITIVE_ONE_BYTE_INT + 1;
    checkArg(size >= 1 && size <= Integer.BYTES, "Resume token field was not a nonnegative int32");
    checkArg(data.remaining() >= size, "Resume token integer was truncated");
    checkArg(data.get(data.position()) != 0, "Resume token integer had a leading zero");

    @Var long encoded = 0;
    for (int i = 0; i < size; i++) {
      encoded = (encoded << 8) | Byte.toUnsignedInt(data.get());
    }
    checkArg((encoded & 1) == 0, "Resume token field was not an integer");
    return (int) (encoded >>> 1);
  }

  /** Unlike {@link #dataFromResumeToken}, validates the value up front since there's no decode. */
  private static String comparableDataFromResumeToken(BsonDocument resumeToken) {
    String data = dataFromResumeToken(resumeToken);
    checkArg(
        data.length() >= MIN_DATA_HEX_CHARS, "resumeToken _data field was too short: %s", data);
    checkArg(
        data.length() % 2 == 0, "resumeToken _data field had an odd number of hex chars: %s", data);
    checkArg(
        VALID_HEX_CHARS.matcher(data).matches(), "resumeToken _data field was not hex: %s", data);
    return data;
  }

  /**
   * Extracts and returns the optime from the given resume token.
   *
   * @throws IllegalArgumentException if {@code _data} is missing, not a string, too short to hold
   *     an optime, or does not start with the timestamp type byte
   * @throws DecoderException if {@code _data} is not valid hex
   */
  public static BsonTimestamp opTimeFromResumeToken(BsonDocument resumeToken)
      throws DecoderException {
    byte[] bytes = Hex.decodeHex(dataFromResumeToken(resumeToken));

    // Check length before bytes[0] — an empty _data would otherwise throw
    // ArrayIndexOutOfBoundsException, which callers don't expect.
    checkArg(bytes.length >= MIN_DATA_BYTES, "postBatchResumeToken did not contain enough data");
    checkArg(
        bytes[0] == TIMESTAMP_IDENTIFIER,
        "first value of the postBatchResumeToken was not a timestamp.");

    ByteBuffer wrapped = ByteBuffer.wrap(bytes);
    return new BsonTimestamp(wrapped.getLong(1));
  }
}
