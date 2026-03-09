package com.xgen.mongot.server.grpc;

import static org.mockito.Mockito.mock;

import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.executors.BulkheadCommandExecutor;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.Assert;
import org.junit.Test;

public class BsonMessageCallHandlerTest {

  @SuppressWarnings("unchecked")
  private static BsonMessageCallHandler createHandler() {
    return new BsonMessageCallHandler(
        CommandRegistry.create(new SimpleMeterRegistry()),
        new BulkheadCommandExecutor(new SimpleMeterRegistry()),
        mock(MongotCursorManager.class),
        SearchEnvoyMetadata.getDefaultInstance(),
        mock(StreamObserver.class));
  }

  private static RawBsonDocument toRaw(BsonDocument doc) {
    return new RawBsonDocument(doc, new BsonDocumentCodec());
  }

  @Test
  public void serializeResponse_flatDocument() {
    var handler = createHandler();
    BsonDocument response = new BsonDocument("ok", new BsonInt32(1));
    RawBsonDocument result = handler.serializeResponse(toRaw(new BsonDocument()), response);
    Assert.assertEquals(1, result.getInt32("ok").getValue());
  }

  @Test
  public void serializeResponse_nestedRawBsonDocument() {
    var handler = createHandler();
    RawBsonDocument nested = toRaw(new BsonDocument("field", new BsonString("value")));

    BsonDocument response = new BsonDocument("ok", new BsonInt32(1)).append("nested", nested);

    RawBsonDocument result = handler.serializeResponse(toRaw(new BsonDocument()), response);
    Assert.assertEquals(1, result.getInt32("ok").getValue());
    Assert.assertEquals("value", result.getDocument("nested").getString("field").getValue());
  }

  @Test
  public void serializeResponse_arrayOfRawBsonDocuments() {
    var handler = createHandler();

    BsonArray batch = new BsonArray();
    for (int i = 0; i < 5; i++) {
      batch.add(toRaw(new BsonDocument("score", new BsonInt32(i))));
    }

    BsonDocument response = new BsonDocument("ok", new BsonInt32(1)).append("batch", batch);
    RawBsonDocument result = handler.serializeResponse(toRaw(new BsonDocument()), response);

    BsonArray resultBatch = result.getArray("batch");
    Assert.assertEquals(5, resultBatch.size());
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(i, resultBatch.get(i).asDocument().getInt32("score").getValue());
    }
  }

  @Test
  public void serializeResponse_preservesByteIdentity() {
    var handler = createHandler();
    RawBsonDocument nested = toRaw(new BsonDocument("data", new BsonString("hello")));
    BsonDocument response = new BsonDocument("ok", new BsonInt32(1)).append("nested", nested);

    RawBsonDocument result = handler.serializeResponse(toRaw(new BsonDocument()), response);

    RawBsonDocument reference = toRaw(response);
    Assert.assertArrayEquals(
        getBytes(reference),
        getBytes(result));
  }

  @Test
  public void serializeError_simpleError() {
    var handler = createHandler();
    BsonDocument error =
        new BsonDocument("ok", new BsonInt32(0)).append("errmsg", new BsonString("test error"));
    RawBsonDocument result = handler.serializeError(toRaw(new BsonDocument()), error);
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertEquals("test error", result.getString("errmsg").getValue());
  }

  private static byte[] getBytes(RawBsonDocument doc) {
    var buf = doc.getByteBuffer().asNIO().duplicate();
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    return bytes;
  }
}
