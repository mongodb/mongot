package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.argNotNull;
import static com.xgen.mongot.util.Check.checkArg;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
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
   * Returns the earliest token by {@link #BY_STREAM_POSITION} — the low-water mark across
   * partitions, the one position every reader has passed.
   *
   * @throws IllegalArgumentException if {@code resumeTokens} is empty
   */
  public static BsonDocument lowWaterMark(Collection<BsonDocument> resumeTokens) {
    checkArg(
        !resumeTokens.isEmpty(),
        "Cannot compute the low-water mark: no resume tokens were provided");
    return resumeTokens.stream().min(BY_STREAM_POSITION).orElseThrow();
  }

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
