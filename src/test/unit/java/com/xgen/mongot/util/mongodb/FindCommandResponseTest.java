package com.xgen.mongot.util.mongodb;

import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.mongodb.serialization.FindCommandResponseProxy;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class FindCommandResponseTest {

  private static final BsonTimestamp OPERATION_TIME = new BsonTimestamp();
  private static final long CURSOR_ID = 13;
  private static final List<RawBsonDocument> BATCH =
      Collections.singletonList(BsonUtils.documentToRaw(new BsonDocument()));

  /** The shape mongod returns for a {@code $natural} scan that requested a resume token. */
  private static final BsonDocument POST_BATCH_RESUME_TOKEN =
      new BsonDocument("$recordId", new BsonInt64(42))
          .append(
              "$initialSyncId",
              new BsonBinary(UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee")));

  @Test
  public void testFromUnsuccessfulProxy() {
    String namespace = "FindCommandResponseTest.testFromUnsuccessfulProxy";
    FindCommandResponseProxy proxy =
        new FindCommandResponseProxy(
            0.0,
            OPERATION_TIME,
            new BsonDocument(),
            new FindCommandResponseProxy.CursorProxy(namespace, CURSOR_ID, BATCH, null));

    try {
      FindCommandResponse.fromProxy(proxy);
      Assert.fail("should throw when ok != 1");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testFromSuccessfulProxy() {
    String namespace = "FindCommandResponseTest.testFromSuccessfulProxy";
    FindCommandResponseProxy proxy =
        new FindCommandResponseProxy(
            1.0,
            OPERATION_TIME,
            new BsonDocument(),
            new FindCommandResponseProxy.CursorProxy(namespace, CURSOR_ID, BATCH, null));

    FindCommandResponse response = FindCommandResponse.fromProxy(proxy);
    assertCorrectResponse(response);
    Assert.assertEquals(Optional.empty(), response.getPostBatchResumeToken());
  }

  @Test
  public void testFromSuccessfulProxy_ResumeToken() {
    String namespace = "FindCommandResponseTest.testFromSuccessfulProxyWithResumeToken";
    FindCommandResponseProxy proxy =
        new FindCommandResponseProxy(
            1.0,
            OPERATION_TIME,
            new BsonDocument(),
            new FindCommandResponseProxy.CursorProxy(
                namespace, CURSOR_ID, BATCH, POST_BATCH_RESUME_TOKEN));

    FindCommandResponse response = FindCommandResponse.fromProxy(proxy);

    assertCorrectResponse(response);
    Assert.assertEquals(POST_BATCH_RESUME_TOKEN, response.getPostBatchResumeToken().get());
  }

  private static void assertCorrectResponse(FindCommandResponse response) {
    Assert.assertEquals(OPERATION_TIME, response.getOperationTime());
    Assert.assertEquals(BATCH, response.getBatch());
    Assert.assertEquals(CURSOR_ID, response.getId());
  }
}
