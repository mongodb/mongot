package com.xgen.mongot.index;

import static java.lang.Math.max;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Test;

public class ReplicationOpTimeInfoTest {

  @Test
  public void testSetReplicationOpTimeInfoIncreasing() {
    ReplicationOpTimeInfo opTimeInfo = new ReplicationOpTimeInfo();

    var time1 = new BsonTimestamp(1, 0).getValue();
    var time2 = new BsonTimestamp(2, 0).getValue();
    var time3 = new BsonTimestamp(3, 0).getValue();

    // mongod optime increases

    opTimeInfo.update(time1, time1);
    opTimeInfo.update(time1, time2);
    assertTrue(opTimeInfo.snapshot().isPresent());
    Assert.assertEquals(
        ReplicationOpTimeInfo.Snapshot.create(time1, time2), opTimeInfo.snapshotOrThrow());

    // setReplicationOpTimeInfo receives non-increasing mongod optimes
    opTimeInfo.update(time1, time3);
    opTimeInfo.update(time1, time2);
    Assert.assertEquals(
        ReplicationOpTimeInfo.Snapshot.create(time1, time3), opTimeInfo.snapshotOrThrow());

    // setReplicationOpTimeInfo receives non-increasing replication optimes
    opTimeInfo.update(time2, time3);
    opTimeInfo.update(time1, time3);
    Assert.assertEquals(
        ReplicationOpTimeInfo.Snapshot.create(time1, time3), opTimeInfo.snapshotOrThrow());

    // Test single parameter setReplicationOpTimeInfo call
    opTimeInfo.unset(() -> true);
    opTimeInfo.update(time1, time1);
    opTimeInfo.update(time2);
    assertTrue(opTimeInfo.snapshot().isPresent());
    Assert.assertEquals(
        ReplicationOpTimeInfo.Snapshot.create(time1, time2), opTimeInfo.snapshotOrThrow());

    // Single parameter setReplicationOpTimeInfo call only accepts increasing updates
    opTimeInfo.update(time1);
    Assert.assertEquals(
        ReplicationOpTimeInfo.Snapshot.create(time1, time2), opTimeInfo.snapshotOrThrow());
  }

  @Test
  public void testSetReplicationOpTimeInfoConcurrent() {
    long min = 1L;
    long max = 10L;
    var replicationOpTime1 = min + (long) (Math.random() * (max - min));
    var maxPossibleReplicationOpTime1 =
        replicationOpTime1 + (long) (Math.random() * (max - replicationOpTime1));
    var replicationOpTime2 = min + (long) (Math.random() * (max - min));
    var maxPossibleReplicationOpTime2 =
        replicationOpTime2 + (long) (Math.random() * (max - replicationOpTime2));

    ReplicationOpTimeInfo opTimeInfo = new ReplicationOpTimeInfo();

    // Two threads call setReplicationOpTimeInfo
    var future1 =
        CompletableFuture.runAsync(
            () -> opTimeInfo.update(replicationOpTime1, maxPossibleReplicationOpTime1));
    var future2 =
        CompletableFuture.runAsync(
            () -> opTimeInfo.update(replicationOpTime2, maxPossibleReplicationOpTime2));

    CompletableFuture.allOf(future1, future2).join();

    assertTrue(opTimeInfo.snapshot().isPresent());

    // Verify that the optime info is updated to be the increasing optime info object
    var result = opTimeInfo.snapshotOrThrow();
    Assert.assertEquals(
        result.maxPossibleReplicationOpTime(),
        max(maxPossibleReplicationOpTime1, maxPossibleReplicationOpTime2));
  }

  @Test
  public void testUnsetConcurrent() throws InterruptedException {
    int numThreads = 20;
    CountDownLatch latch = new CountDownLatch(1);
    ReplicationOpTimeInfo opTimeInfo = new ReplicationOpTimeInfo();

    AtomicBoolean shouldUnset = new AtomicBoolean(true);
    BooleanSupplier shouldUnsetSupplier = shouldUnset::get;

    try (ExecutorService threadPool = Executors.newFixedThreadPool(numThreads)) {
      // 20 threads try to unset opTimeInfo concurrently
      for (int i = 0; i < numThreads; i++) {
        threadPool.submit(
            () -> {
              try {
                latch.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              opTimeInfo.unset(shouldUnsetSupplier);
            });
      }

      // simulate setting index to STEADY by acquiring the unset lock
      synchronized (opTimeInfo.getUnsetLock()) {
        opTimeInfo.update(10, 20);
        shouldUnset.set(false);

        // release the 20 existing threads
        latch.countDown();
      }

      // one new thread tries to unset opTimeInfo
      threadPool.submit(() -> opTimeInfo.unset(shouldUnsetSupplier));

      // wait for all threads to finish
      threadPool.shutdown();
      assertTrue(threadPool.awaitTermination(10, TimeUnit.SECONDS));

      // opTimeInfo should still be set
      assertTrue(opTimeInfo.snapshot().isPresent());

      // unsetting with true supplier should succeed
      opTimeInfo.unset(() -> true);
      assertTrue(opTimeInfo.snapshot().isEmpty());
    }
  }

  @Test
  public void testVisibleOpTimeEmptyBeforeUpdate() {
    ReplicationOpTimeInfo opTimeInfo = new ReplicationOpTimeInfo();
    assertTrue(opTimeInfo.visibleOpTime().isEmpty());
  }

  @Test
  public void testUpdateVisibleStoresOpTime() {
    ReplicationOpTimeInfo opTimeInfo = new ReplicationOpTimeInfo();
    long opTime = new BsonTimestamp(42, 0).getValue();

    opTimeInfo.updateVisible(opTime);

    Assert.assertTrue(opTimeInfo.visibleOpTime().isPresent());
    Assert.assertEquals(opTime, opTimeInfo.visibleOpTime().getAsLong());
  }

  @Test
  public void testUpdateVisibleIsIndependentOfWriteState() {
    ReplicationOpTimeInfo opTimeInfo = new ReplicationOpTimeInfo();
    var writeOpTime = new BsonTimestamp(10, 0).getValue();
    var maxOpTime = new BsonTimestamp(20, 0).getValue();
    var visibleOpTime = new BsonTimestamp(5, 0).getValue();

    opTimeInfo.update(writeOpTime, maxOpTime);
    opTimeInfo.updateVisible(visibleOpTime);

    Assert.assertEquals(
        ReplicationOpTimeInfo.Snapshot.create(writeOpTime, maxOpTime),
        opTimeInfo.snapshotOrThrow());
    Assert.assertEquals(visibleOpTime, opTimeInfo.visibleOpTime().getAsLong());
  }

  @Test
  public void testUpdateVisibleOverwritesPreviousValue() {
    ReplicationOpTimeInfo opTimeInfo = new ReplicationOpTimeInfo();
    var first = new BsonTimestamp(10, 0).getValue();
    var second = new BsonTimestamp(20, 0).getValue();

    opTimeInfo.updateVisible(first);
    opTimeInfo.updateVisible(second);

    Assert.assertEquals(second, opTimeInfo.visibleOpTime().getAsLong());
  }
}
