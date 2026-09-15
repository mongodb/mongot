package com.xgen.mongot.util.mongodb;

import static com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.changeStreamDocumentToBsonDocument;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.xgen.mongot.util.mongodb.serialization.ChangeStreamAggregateResponseProxy;
import com.xgen.mongot.util.mongodb.serialization.ChangeStreamGetMoreResponseProxy;
import java.util.Collections;
import java.util.List;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class ChangeStreamResponseTest {

  private static final BsonDocument RESUME_TOKEN =
      new BsonDocument("_data", new BsonBinary("resume token data".getBytes()));
  private static final BsonTimestamp OP_TIME = new BsonTimestamp(0, 1);
  private static final long CURSOR_ID = 13;
  private static final List<RawBsonDocument> BATCH =
      Collections.singletonList(
          changeStreamDocumentToBsonDocument(
              new ChangeStreamDocument<>(
                  OperationType.INSERT.getValue(),
                  RESUME_TOKEN,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  OP_TIME,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null)));

  @Test
  public void testFromSuccessfulAggregateProxy() {
    String namespace = "ChangeStreamResponseTest.testFromSuccessfulAggregateProxy";
    ChangeStreamAggregateResponseProxy proxy =
        new ChangeStreamAggregateResponseProxy(
            1.0,
            new BsonTimestamp(),
            new BsonDocument(),
            new ChangeStreamAggregateResponseProxy.CursorProxy(
                namespace, CURSOR_ID, BATCH, RESUME_TOKEN));

    ChangeStreamResponse response = ChangeStreamResponse.fromProxy(proxy);
    assertCorrectResponse(response);
  }

  @Test
  public void testFromSuccessfulGetMoreProxy() {
    String namespace = "ChangeStreamResponseTest.testFromSuccessfulAggregateProxy";
    ChangeStreamGetMoreResponseProxy proxy =
        new ChangeStreamGetMoreResponseProxy(
            1.0,
            new BsonTimestamp(),
            new BsonDocument(),
            new ChangeStreamGetMoreResponseProxy.CursorProxy(
                namespace, CURSOR_ID, BATCH, RESUME_TOKEN));

    ChangeStreamResponse response = ChangeStreamResponse.fromProxy(proxy);
    assertCorrectResponse(response);
  }

  private static void assertCorrectResponse(ChangeStreamResponse response) {
    Assert.assertEquals(BATCH, response.getBatch());
    Assert.assertEquals(CURSOR_ID, response.getId());
    Assert.assertEquals(RESUME_TOKEN, response.getPostBatchResumeToken());
  }
}
