package com.xgen.mongot.replication.mongodb.common;

import static org.mockito.Mockito.mock;

import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.util.mongodb.Errors;
import java.util.concurrent.CompletableFuture;
import org.junit.Assert;
import org.junit.Test;

public class InitialSyncExceptionTest {

  @Test
  public void testRequiresResync() {
    InitialSyncException e1 = InitialSyncException.createRequiresResync("foo");
    Assert.assertTrue(e1.isRequiresResync());
    Assert.assertFalse(e1.isFailed());
    Assert.assertFalse(e1.isDropped());
    Assert.assertFalse(e1.isShutdown());
    Assert.assertEquals(InitialSyncException.Type.REQUIRES_RESYNC, e1.getType());

    InitialSyncException e2 = InitialSyncException.createRequiresResync(new RuntimeException());
    Assert.assertTrue(e2.isRequiresResync());
    Assert.assertFalse(e2.isFailed());
    Assert.assertFalse(e2.isDropped());
    Assert.assertFalse(e2.isShutdown());
    Assert.assertEquals(InitialSyncException.Type.REQUIRES_RESYNC, e2.getType());
  }

  @Test
  public void testDoesNotExist() {
    InitialSyncException e = InitialSyncException.createDoesNotExist("foo");
    Assert.assertTrue(e.isDoesNotExist());
    Assert.assertFalse(e.isDropped());
    Assert.assertFalse(e.isFailed());
    Assert.assertFalse(e.isShutdown());
    Assert.assertEquals(InitialSyncException.Type.DOES_NOT_EXIST, e.getType());
  }

  @Test
  public void testFailed() {
    InitialSyncException e = InitialSyncException.createFailed(new RuntimeException());
    Assert.assertTrue(e.isFailed());
    Assert.assertFalse(e.isRequiresResync());
    Assert.assertFalse(e.isDropped());
    Assert.assertFalse(e.isShutdown());
    Assert.assertEquals(InitialSyncException.Type.FAILED, e.getType());
  }

  @Test
  public void testDropped() {
    InitialSyncException e = InitialSyncException.createDropped();
    Assert.assertTrue(e.isDropped());
    Assert.assertFalse(e.isRequiresResync());
    Assert.assertFalse(e.isFailed());
    Assert.assertFalse(e.isShutdown());
    Assert.assertEquals(InitialSyncException.Type.DROPPED, e.getType());
  }

  @Test
  public void testShutDown() {
    InitialSyncException e = InitialSyncException.createShutDown();
    Assert.assertTrue(e.isShutdown());
    Assert.assertFalse(e.isRequiresResync());
    Assert.assertFalse(e.isFailed());
    Assert.assertFalse(e.isDropped());
    Assert.assertEquals(InitialSyncException.Type.SHUT_DOWN, e.getType());
  }

  @Test
  public void testFromNamespaceResolutionException() {
    InitialSyncException e =
        InitialSyncException.fromNamespaceResolutionException(
            NamespaceResolutionException.create());
    Assert.assertTrue(e.isDropped());
  }

  @Test
  public void testWrapIfThrowsNoException() {
    try {
      int result = InitialSyncException.wrapIfThrows(() -> 5, InitialSyncException.Phase.MAIN);
      Assert.assertEquals(5, result);
    } catch (InitialSyncException e) {
      Assert.fail("threw exception when none was thrown");
    }
  }

  @Test
  public void testWrapIfThrowsInterrupted() {
    try {
      InitialSyncException.wrapIfThrows(
          () -> {
            throw new InterruptedException();
          }, InitialSyncException.Phase.MAIN);
    } catch (InitialSyncException e) {
      Assert.assertTrue(e.isShutdown());
    }
  }

  @Test
  public void testWrapIfThrowsMongoException() {
    try {
      InitialSyncException.wrapIfThrows(
          () -> {
            throw new MongoException("exception");
          }, InitialSyncException.Phase.MAIN);
      Assert.fail("did not throw exception when MongoException was thrown");
    } catch (InitialSyncException e) {
      Assert.assertTrue(e.isRequiresResync());
    }
  }

  @Test
  public void testWrapIfThrowsRetryableMongoException() {
    try {
      InitialSyncException.wrapIfThrows(
          () -> {
            throw mock(MongoSocketException.class);
          }, InitialSyncException.Phase.MAIN);
      Assert.fail("did not throw exception when MongoSocketException was thrown");
    } catch (InitialSyncException e) {
      Assert.assertTrue(e.isResumableTransient());
    }
  }

  @Test
  public void testWrapIfThrowsRetryableMongoExceptionByCode() {
    try {
      InitialSyncException.wrapIfThrows(
          () -> {
            throw new MongoException(9001, "socket exception");
          }, InitialSyncException.Phase.MAIN);
      Assert.fail("did not throw exception when MongoException was thrown");
    } catch (InitialSyncException e) {
      Assert.assertTrue(e.isResumableTransient());
    }
  }

  @Test
  public void testWrapIfThrowsBsonTooLargeMongoException() {
    try {
      InitialSyncException.wrapIfThrowsCollectionScan(
          () -> {
            throw new MongoException(Errors.BSON_OBJECT_TOO_LARGE.code,
                "BSONObjectTooLarge exception");
          });
      Assert.fail("did not throw exception when MongoException was thrown");
    } catch (InitialSyncException e) {
      var collectionScanMessage =
          String.format(InitialSyncException.BSON_TOO_LARGE_MESSAGE,
              InitialSyncException.Phase.COLLECTION_SCAN.name());
      Assert.assertEquals(collectionScanMessage, e.getMessage());
      Assert.assertTrue(e.isRequiresResync());
    }

    try {
      InitialSyncException.wrapIfThrowsChangeStream(
          () -> {
            throw new MongoException(Errors.BSON_OBJECT_TOO_LARGE.code,
                "BSONObjectTooLarge exception");
          });
      Assert.fail("did not throw exception when MongoException was thrown");
    } catch (InitialSyncException e) {
      var changeStreamMessage =
          String.format(InitialSyncException.BSON_TOO_LARGE_MESSAGE,
              InitialSyncException.Phase.CHANGE_STREAM.name());
      Assert.assertEquals(changeStreamMessage, e.getMessage());
      Assert.assertTrue(e.isRequiresResync());
    }

    var bsonObjectTooLargeException =
        new MongoException(Errors.BSON_OBJECT_TOO_LARGE.code, "BSONObjectTooLarge exception");
    try {
      InitialSyncException.wrapIfThrows(
          () -> {
            throw bsonObjectTooLargeException;
          }, InitialSyncException.Phase.MAIN);
      Assert.fail("did not throw exception when MongoException was thrown");
    } catch (InitialSyncException e) {
      Assert.assertEquals(bsonObjectTooLargeException.toString(), e.getMessage());
      Assert.assertTrue(e.isRequiresResync());
    }
  }

  @Test
  public void testWrapIfThrowsNamespaceResolutionException() {
    try {
      InitialSyncException.wrapIfThrows(
          () -> {
            throw NamespaceResolutionException.create();
          }, InitialSyncException.Phase.MAIN);
      Assert.fail("did not throw exception when NamespaceResolutionException was thrown");
    } catch (InitialSyncException e) {
      Assert.assertTrue(e.isDropped());
    }
  }

  @Test
  public void testWrapIfThrowsInitialSyncException() {
    try {
      InitialSyncException.wrapIfThrows(
          () -> {
            throw InitialSyncException.createDropped();
          }, InitialSyncException.Phase.MAIN);
      Assert.fail("did not throw exception when InitialSyncException was thrown");
    } catch (InitialSyncException e) {
      Assert.assertTrue(e.isDropped());
    }
  }

  @Test
  public void testWrapIfThrowsOtherException() throws Exception {
    try {
      InitialSyncException.wrapIfThrows(
          () -> {
            throw new IllegalStateException();
          }, InitialSyncException.Phase.MAIN);
    } catch (IllegalStateException e) {
      // Expected
      return;
    }

    Assert.fail("did not throw exception when IllegalStateException was thrown");
  }

  @Test
  public void testWrapIfThrowsPropagatesErrors() {
    Assert.assertThrows(
        OutOfMemoryError.class,
        () ->
            InitialSyncException.wrapIfThrows(
                () -> {
                  throw new OutOfMemoryError();
                }, InitialSyncException.Phase.MAIN));
  }

  @Test
  public void testWrapIfThrowsFieldLimitsExceeded() {
    var e =
        Assert.assertThrows(
            InitialSyncException.class,
            () ->
                InitialSyncException.wrapIfThrows(
                    () -> {
                      throw new FieldExceededLimitsException("reason");
                    }, InitialSyncException.Phase.MAIN));
    Assert.assertSame(InitialSyncException.Type.FIELD_EXCEEDED, e.getType());
    Assert.assertEquals("reason", e.getMessage());
  }

  @Test
  public void testWrapIfThrowsIngressRateLimitTransientError() {
    var e = Assert.assertThrows(
        InitialSyncException.class,
        () ->
            InitialSyncException.wrapIfThrows(
                () -> {
                  throw new MongoException(
                      462 /* IngressRequestRateLimitExceeded */, "server overloaded exception");
                },
                InitialSyncException.Phase.MAIN));

    Assert.assertEquals(InitialSyncException.Type.RESUMABLE_TRANSIENT, e.getType());
  }

  @Test
  public void testWrapIfThrowsFragmentProcessingException() {
    var e =
        Assert.assertThrows(
            InitialSyncException.class,
            () ->
                InitialSyncException.wrapIfThrows(
                    () -> {
                      throw new FragmentProcessingException("fragment error");
                    },
                    InitialSyncException.Phase.MAIN));
    Assert.assertTrue(e.isRequiresResync());
  }

  @Test
  public void testGetOrWrapThrowablePropagatesErrors() {
    var future = CompletableFuture.failedFuture(new OutOfMemoryError());
    Assert.assertThrows(
        OutOfMemoryError.class,
        () -> InitialSyncException.getOrWrapThrowable(future, InitialSyncException.Phase.MAIN));
  }

  @Test
  public void testGetOrWrapThrowableHandlesMongodbInterruptedException() {
    var future = CompletableFuture.failedFuture(mock(MongoInterruptedException.class));
    var e = Assert.assertThrows(
        InitialSyncException.class,
        () -> InitialSyncException.getOrWrapThrowable(future, InitialSyncException.Phase.MAIN));
    Assert.assertEquals(InitialSyncException.Type.SHUT_DOWN, e.getType());
  }
}
