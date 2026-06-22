package com.xgen.testing.util;

import static org.junit.Assert.assertEquals;

import org.bson.BsonType;
import org.bson.RawBsonDocument;
import org.junit.Test;

public class BsonDocumentUtilTest {

  @Test
  public void decodeValidDocument() {
    var raw = RawBsonDocument.parse("{foo: 5}");

    assertEquals(
        "RawBsonDocument(length=14: INT32 'foo'=BsonInt32{value=5}, )",
        BsonDocumentUtil.decodeCorruptDocument(raw));
  }

  @Test
  public void decodeInvalidDocument() {
    var raw = RawBsonDocument.parse("{foo: 5}");
    raw.getByteBuffer().put(1, (byte) 1); // Corrupt header
    raw.getByteBuffer().put(4, (byte) BsonType.NULL.getValue()); // Corrupt type
    raw.getByteBuffer().put(5, (byte) '?'); // Corrupt name

    assertEquals(
        "RawBsonDocument(length=14 but wrote 270!: NULL '?oo'=BsonNull, BINARY ''=-- "
            + "While decoding a BSON document 4 bytes were required, but only 3 remain",
        BsonDocumentUtil.decodeCorruptDocument(raw));
  }
}
