package com.xgen.mongot.replication.mongodb.common;

import static org.junit.Assert.fail;

import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerId;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException.Type;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.mongot.util.regex.RegexInterruptedException;
import com.xgen.mongot.util.regex.RegexLimitExceededException;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class SteadyStateExceptionTest {

  @Test
  public void wrapIfThrows_regexLimit_isPersistentAnalysisFailure() {
    var failure = new RegexLimitExceededException();

    var translated =
        Assert.assertThrows(
            SteadyStateException.class,
            () ->
                SteadyStateException.wrapIfThrows(
                    () -> {
                      throw failure;
                    }));

    Assert.assertEquals(SteadyStateException.Type.ANALYSIS_LIMIT_EXCEEDED, translated.getType());
    Assert.assertEquals(failure.getMessage(), translated.getMessage());
  }

  @Test
  public void wrapIfThrows_regexInterrupted_isShutdown() {
    var translated =
        Assert.assertThrows(
            SteadyStateException.class,
            () ->
                SteadyStateException.wrapIfThrows(
                    () -> {
                      throw new RegexInterruptedException();
                    }));

    Assert.assertTrue(translated.isShutdown());
  }

  @Test
  public void testTransient() {
    SteadyStateException e1 = SteadyStateException.createTransient(new Exception("foo"));
    Assert.assertTrue(e1.isTransient());
    Assert.assertFalse(e1.isRequiresResync());
    Assert.assertFalse(e1.isRenamed());
    Assert.assertFalse(e1.isInvalidated());
    Assert.assertFalse(e1.isDropped());
    Assert.assertFalse(e1.isShutdown());
    Assert.assertFalse(e1.isNonInvalidatingResync());
    Assert.assertEquals(SteadyStateException.Type.TRANSIENT, e1.getType());
    Assert.assertThrows(IllegalStateException.class, e1::getResumeInfo);

    SteadyStateException e2 = SteadyStateException.createTransient(new RuntimeException());
    Assert.assertTrue(e2.isTransient());
    Assert.assertFalse(e2.isRequiresResync());
    Assert.assertFalse(e2.isRenamed());
    Assert.assertFalse(e2.isInvalidated());
    Assert.assertFalse(e2.isDropped());
    Assert.assertFalse(e2.isShutdown());
    Assert.assertFalse(e2.isNonInvalidatingResync());
    Assert.assertEquals(SteadyStateException.Type.TRANSIENT, e2.getType());
    Assert.assertThrows(IllegalStateException.class, e2::getResumeInfo);
  }

  @Test
  public void testRequiresResync() {
    SteadyStateException e = SteadyStateException.createRequiresResync("foo");
    Assert.assertTrue(e.isRequiresResync());
    Assert.assertFalse(e.isTransient());
    Assert.assertFalse(e.isRenamed());
    Assert.assertFalse(e.isInvalidated());
    Assert.assertFalse(e.isDropped());
    Assert.assertFalse(e.isShutdown());
    Assert.assertFalse(e.isNonInvalidatingResync());
    Assert.assertEquals(SteadyStateException.Type.REQUIRES_RESYNC, e.getType());
    Assert.assertThrows(IllegalStateException.class, e::getResumeInfo);
  }

  @Test
  public void testStale() {
    SteadyStateException e =
        SteadyStateException.createNonInvalidatingResync(new RuntimeException());
    Assert.assertTrue(e.isNonInvalidatingResync());
    Assert.assertFalse(e.isRequiresResync());
    Assert.assertFalse(e.isTransient());
    Assert.assertFalse(e.isRenamed());
    Assert.assertFalse(e.isInvalidated());
    Assert.assertFalse(e.isDropped());
    Assert.assertFalse(e.isShutdown());
    Assert.assertEquals(SteadyStateException.Type.NON_INVALIDATING_RESYNC, e.getType());
    Assert.assertThrows(IllegalStateException.class, e::getResumeInfo);
  }

  @Test
  public void testRenamed() {
    ChangeStreamResumeInfo resumeInfo =
        ChangeStreamResumeInfo.create(
            new MongoNamespace("database", "collection"),
            new BsonDocument("resumeToken", new BsonBoolean(true)));

    SteadyStateException e = SteadyStateException.createRenamed(resumeInfo);
    Assert.assertTrue(e.isRenamed());
    Assert.assertFalse(e.isTransient());
    Assert.assertFalse(e.isRequiresResync());
    Assert.assertFalse(e.isInvalidated());
    Assert.assertFalse(e.isDropped());
    Assert.assertFalse(e.isShutdown());
    Assert.assertFalse(e.isNonInvalidatingResync());
    Assert.assertEquals(SteadyStateException.Type.RENAMED, e.getType());

    Assert.assertEquals(resumeInfo, e.getResumeInfo());
  }

  @Test
  public void testInvalidated() {
    ChangeStreamResumeInfo resumeInfo =
        ChangeStreamResumeInfo.create(
            new MongoNamespace("database", "collection"),
            new BsonDocument("resumeToken", new BsonBoolean(true)));

    SteadyStateException e = SteadyStateException.createInvalidated(resumeInfo);
    Assert.assertTrue(e.isInvalidated());
    Assert.assertFalse(e.isTransient());
    Assert.assertFalse(e.isRequiresResync());
    Assert.assertFalse(e.isRenamed());
    Assert.assertFalse(e.isDropped());
    Assert.assertFalse(e.isShutdown());
    Assert.assertFalse(e.isNonInvalidatingResync());
    Assert.assertEquals(SteadyStateException.Type.INVALIDATED, e.getType());

    Assert.assertEquals(resumeInfo, e.getResumeInfo());
  }

  @Test
  public void testDropped() {
    SteadyStateException e = SteadyStateException.createDropped();
    Assert.assertTrue(e.isDropped());
    Assert.assertFalse(e.isTransient());
    Assert.assertFalse(e.isRequiresResync());
    Assert.assertFalse(e.isRenamed());
    Assert.assertFalse(e.isInvalidated());
    Assert.assertFalse(e.isShutdown());
    Assert.assertFalse(e.isNonInvalidatingResync());
    Assert.assertEquals(SteadyStateException.Type.DROPPED, e.getType());
    Assert.assertThrows(IllegalStateException.class, e::getResumeInfo);
  }

  @Test
  public void testShutDown() {
    SteadyStateException e = SteadyStateException.createShutDown();
    Assert.assertTrue(e.isShutdown());
    Assert.assertFalse(e.isTransient());
    Assert.assertFalse(e.isRequiresResync());
    Assert.assertFalse(e.isRenamed());
    Assert.assertFalse(e.isInvalidated());
    Assert.assertFalse(e.isDropped());
    Assert.assertFalse(e.isNonInvalidatingResync());
    Assert.assertEquals(SteadyStateException.Type.SHUT_DOWN, e.getType());
    Assert.assertThrows(IllegalStateException.class, e::getResumeInfo);
  }

  @Test
  public void testWrapIfThrowsShutdown() {
    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new InterruptedException();
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(Type.SHUT_DOWN, e.getType());
    }
  }

  @Test
  public void testWrapIfThrowsTransient() {
    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoInterruptedException("interrupted", new Exception());
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.TRANSIENT, e.getType());
    }
    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoSocketException("exception", new ServerAddress());
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.TRANSIENT, e.getType());
    }

    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoException(89 /* NetworkTimeout */, "Transient");
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.TRANSIENT, e.getType());
    }

    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoConnectionPoolClearedException(
                new ServerId(new ClusterId(), new ServerAddress()), null);
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.TRANSIENT, e.getType());
    }

    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoCursorNotFoundException(0L, new BsonDocument(), new ServerAddress());
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.TRANSIENT, e.getType());
    }
    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoException(Errors.SHARD_NOT_FOUND.code, "Exception");
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.TRANSIENT, e.getType());
    }
    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoException("exception");
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.TRANSIENT, e.getType());
    }
    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoException(55 /* InvalidDBRef */, "exception");
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.TRANSIENT, e.getType());
    }
    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoException(
                462 /* IngressRequestRateLimitExceeded */, "server overloaded exception");
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.TRANSIENT, e.getType());
    }
  }

  @Test
  public void testWrapIfThrowsNonInvalidatingResync() {
    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoException(Errors.CAPPED_POSITION_LOST.code, "CAPPED_POSITION_LOST");
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.NON_INVALIDATING_RESYNC, e.getType());
    }

    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoException(Errors.BSON_OBJECT_TOO_LARGE.code, "BSON_OBJECT_TOO_LARGE");
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.BSON_TOO_LARGE_MESSAGE, e.getMessage());
      Assert.assertEquals(SteadyStateException.Type.NON_INVALIDATING_RESYNC, e.getType());
    }
    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoException(
                Errors.CHANGE_STREAM_HISTORY_LOST.code, "CHANGE_STREAM_HISTORY_LOST");
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.NON_INVALIDATING_RESYNC, e.getType());
    }

    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new MongoException(
                Errors.CHANGE_STREAM_FATAL_ERROR.code, "CHANGE_STREAM_FATAL_ERROR");
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.NON_INVALIDATING_RESYNC, e.getType());
    }

    try {
      SteadyStateException.wrapIfThrows(
          () -> {
            throw new FragmentProcessingException("Test fragment error");
          });
      fail();
    } catch (SteadyStateException e) {
      Assert.assertEquals(SteadyStateException.Type.NON_INVALIDATING_RESYNC, e.getType());
      Assert.assertTrue(e.getCause() instanceof FragmentProcessingException);
      Assert.assertEquals("Test fragment error", e.getCause().getMessage());
    }
  }
}
