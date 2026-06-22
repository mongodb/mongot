package com.xgen.mongot.server.message;

import com.xgen.mongot.util.mongodb.Errors.Error;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.RawBsonDocument;

/** Run MessageUtilsBenchTest before merging changes to this class. */
public class MessageUtils {

  private MessageUtils() {}

  /**
   * Read a null-terminated string from `bytes`
   *
   * <p>Post-condition: {@code bytes.readerIndex()} points to the index immediately after the next
   * null byte
   *
   * @throws IndexOutOfBoundsException if {@code bytes} does not contain a null byte
   */
  static String readCString(ByteBuf bytes) {
    int pos = bytes.bytesBefore((byte) 0);
    String str = bytes.toString(bytes.readerIndex(), pos, StandardCharsets.UTF_8);
    bytes.skipBytes(pos + 1);
    return str;
  }

  /**
   * Read a bson document from `bytes`
   *
   * <p>Precondition: {@code bytes.readerIndex()} is positioned before a valid bson document (the
   * next 4 bytes are expected to be a lower endian encoding of that document's length).
   *
   * <p>Postcondition: {@code bytes.readerIndex()} points to the index after the bson document.
   */
  static RawBsonDocument rawBsonDocumentFromBytes(ByteBuf bytes) {
    int size = bytes.getIntLE(bytes.readerIndex());
    byte[] docBytes = new byte[size];
    bytes.readBytes(docBytes);
    return new RawBsonDocument(docBytes);
  }

  /** Create a generic error response with a message body. */
  public static BsonDocument createErrorBody(Exception e) {
    String text = Objects.requireNonNullElseGet(e.getMessage(), () -> e.getClass().getSimpleName());
    return createErrorBody(text);
  }

  /** Create a generic error response with a message body. */
  public static BsonDocument createErrorBody(@Nonnull String text) {
    return new BsonDocument().append("ok", new BsonInt32(0)).append("errmsg", new BsonString(text));
  }

  /** Create a response for a specific error with a message body. */
  public static BsonDocument createError(Error error, String text) {
    return new BsonDocument()
        .append("ok", new BsonInt32(0))
        .append("code", new BsonInt32(error.code))
        .append("codeName", new BsonString(error.name))
        .append("errmsg", new BsonString(text));
  }

  /**
   * Create a generic error response with a message body and error labels.
   *
   * <p>Error labels follow the MongoDB wire protocol convention defined in error_labels.h. Labels
   * like "SystemOverloadedError" and "RetryableError" allow clients to appropriately handle and
   * retry transient errors.
   *
   * @param text the error message
   * @param errorLabels list of error label strings to include in the response
   */
  public static BsonDocument createErrorBodyWithLabels(
      @Nonnull String text, @Nonnull List<String> errorLabels) {
    BsonArray labelsArray = new BsonArray();
    for (String label : errorLabels) {
      labelsArray.add(new BsonString(label));
    }
    return new BsonDocument()
        .append("ok", new BsonInt32(0))
        .append("errmsg", new BsonString(text))
        .append("errorLabels", labelsArray);
  }

  /**
   * Create an error response with a message body, error code, and error labels.
   *
   * <p>This method extends {@link #createErrorBodyWithLabels(String, List)} to include a specific
   * MongoDB error code and codeName, following the standard MongoDB wire protocol for error
   * responses.
   *
   * <p>Error labels follow the MongoDB wire protocol convention defined in error_labels.h. Labels
   * like "SystemOverloadedError" and "RetryableError" allow clients to appropriately handle and
   * retry transient errors.
   *
   * @param text the error message
   * @param errorLabels list of error label strings to include in the response
   * @param error the MongoDB error type containing code and codeName
   */
  public static BsonDocument createErrorBodyWithLabels(
      @Nonnull String text, @Nonnull List<String> errorLabels, @Nonnull Error error) {
    BsonArray labelsArray = new BsonArray();
    for (String label : errorLabels) {
      labelsArray.add(new BsonString(label));
    }
    return new BsonDocument()
        .append("ok", new BsonInt32(0))
        .append("code", new BsonInt32(error.code))
        .append("codeName", new BsonString(error.name))
        .append("errmsg", new BsonString(text))
        .append("errorLabels", labelsArray);
  }
}
