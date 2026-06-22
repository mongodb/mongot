package com.xgen.mongot.server.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.util.mongodb.Errors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Test;

public class MessageUtilsTest {

  @Test
  public void readCString() {
    ByteBuf buffer = Unpooled.buffer(16);
    buffer.writeByte(0); // Ensure we use relative reads and update index properly
    buffer.writeCharSequence("Hello World", StandardCharsets.UTF_8);
    buffer.writeByte(0);
    buffer.readerIndex(1);

    String result = MessageUtils.readCString(buffer);

    assertEquals("Hello World".length(), result.length());
    assertEquals("Hello World", result);
    assertEquals("Hello World".length() + 2, buffer.readerIndex());
  }

  @Test
  public void emptyCString() {
    ByteBuf buffer = Unpooled.buffer(16);
    buffer.writeByte(0);

    String result = MessageUtils.readCString(buffer);

    assertEquals("", result);
    assertEquals(1, buffer.readerIndex());
  }

  @Test
  public void readMissingCString() {
    ByteBuf buffer = Unpooled.buffer(16);
    buffer.writeCharSequence("A".repeat(16), StandardCharsets.UTF_8);

    assertThrows(IndexOutOfBoundsException.class, () -> MessageUtils.readCString(buffer));
  }

  @Test
  public void readRawBsonDocument() {
    RawBsonDocument first = RawBsonDocument.parse("{x: 1}");
    RawBsonDocument second = RawBsonDocument.parse("{x: 2}");
    ByteBuf buffer = Unpooled.buffer(16);
    buffer.writeBytes(first.getByteBuffer().asNIO());
    buffer.writeBytes(second.getByteBuffer().asNIO());

    RawBsonDocument a = MessageUtils.rawBsonDocumentFromBytes(buffer);
    RawBsonDocument b = MessageUtils.rawBsonDocumentFromBytes(buffer);

    assertEquals(first, a);
    assertEquals(second, b);
  }

  @Test
  public void readRawBsonDocumentTooShort() {
    RawBsonDocument first = RawBsonDocument.parse("{x: 1}");
    ByteBuf buffer = Unpooled.buffer(16);
    buffer.writeIntLE(1000);
    buffer.writeBytes(first.getByteBuffer().asNIO());

    assertThrows(
        IndexOutOfBoundsException.class, () -> MessageUtils.rawBsonDocumentFromBytes(buffer));
  }

  @Test
  public void createErrorBodyNullMessage() {
    BsonDocument expected = MessageUtils.createErrorBody("IllegalStateException");

    BsonDocument result = MessageUtils.createErrorBody(new IllegalStateException());

    assertEquals(expected, result);
  }

  @Test
  public void createErrorBodyEquivalentIfNotNull() {
    BsonDocument expected = MessageUtils.createErrorBody("message");

    BsonDocument result = MessageUtils.createErrorBody(new Exception("message"));

    assertEquals(expected, result);
  }

  @Test
  public void createErrorBodyWithLabels_containsAllLabels() {
    List<String> labels = List.of("SystemOverloadedError", "RetryableError");

    BsonDocument result = MessageUtils.createErrorBodyWithLabels("error message", labels);

    assertEquals(0, result.getInt32("ok").getValue());
    assertEquals("error message", result.getString("errmsg").getValue());
    assertTrue(result.containsKey("errorLabels"));

    BsonArray errorLabels = result.getArray("errorLabels");
    assertEquals(2, errorLabels.size());
    assertEquals("SystemOverloadedError", errorLabels.get(0).asString().getValue());
    assertEquals("RetryableError", errorLabels.get(1).asString().getValue());
  }

  @Test
  public void createErrorBodyWithLabels_emptyLabels() {
    List<String> labels = List.of();

    BsonDocument result = MessageUtils.createErrorBodyWithLabels("error message", labels);

    assertEquals(0, result.getInt32("ok").getValue());
    assertEquals("error message", result.getString("errmsg").getValue());
    assertTrue(result.containsKey("errorLabels"));

    BsonArray errorLabels = result.getArray("errorLabels");
    assertEquals(0, errorLabels.size());
  }

  @Test
  public void createErrorBodyWithLabelsAndError_containsCodeAndCodeName() {
    List<String> labels = List.of("SystemOverloadedError", "RetryableError");

    BsonDocument result =
        MessageUtils.createErrorBodyWithLabels(
            "server at capacity", labels, Errors.SEARCH_REQUEST_REJECTED_DUE_TO_OVERLOAD);

    assertEquals(0, result.getInt32("ok").getValue());
    assertEquals(
        Errors.SEARCH_REQUEST_REJECTED_DUE_TO_OVERLOAD.code, result.getInt32("code").getValue());
    assertEquals(
        Errors.SEARCH_REQUEST_REJECTED_DUE_TO_OVERLOAD.name,
        result.getString("codeName").getValue());
    assertEquals("server at capacity", result.getString("errmsg").getValue());
    assertTrue(result.containsKey("errorLabels"));

    BsonArray errorLabels = result.getArray("errorLabels");
    assertEquals(2, errorLabels.size());
    assertEquals("SystemOverloadedError", errorLabels.get(0).asString().getValue());
    assertEquals("RetryableError", errorLabels.get(1).asString().getValue());
  }
}
