package com.xgen.mongot.server.grpc;

import java.io.IOException;
import java.io.InputStream;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.Assert;
import org.junit.Test;

public class RawBsonMarshallerTest {

  private static RawBsonDocument toRaw(BsonDocument doc) {
    return new RawBsonDocument(doc, new BsonDocumentCodec());
  }

  private static byte[] getBytes(RawBsonDocument doc) {
    var buf = doc.getByteBuffer().asNIO().duplicate();
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    return bytes;
  }

  @Test
  public void stream_producesBytesIdenticalToBackingBuffer() throws IOException {
    RawBsonDocument doc =
        toRaw(new BsonDocument("ok", new BsonInt32(1)).append("msg", new BsonString("hello")));

    RawBsonMarshaller marshaller = new RawBsonMarshaller();
    try (InputStream stream = marshaller.stream(doc)) {
      Assert.assertArrayEquals(getBytes(doc), stream.readAllBytes());
    }
  }

  @Test
  public void roundTrip_streamThenParse() throws IOException {
    RawBsonDocument original =
        toRaw(
            new BsonDocument("ok", new BsonInt32(1))
                .append("key", new BsonString("value"))
                .append("num", new BsonInt32(42)));

    RawBsonMarshaller marshaller = new RawBsonMarshaller();
    try (InputStream stream = marshaller.stream(original)) {
      RawBsonDocument parsed = marshaller.parse(stream);
      Assert.assertEquals(original, parsed);
    }
  }

  @Test
  public void stream_emptyDocument() throws IOException {
    RawBsonDocument doc = toRaw(new BsonDocument());

    RawBsonMarshaller marshaller = new RawBsonMarshaller();
    try (InputStream stream = marshaller.stream(doc)) {
      Assert.assertArrayEquals(getBytes(doc), stream.readAllBytes());
    }
  }
}
