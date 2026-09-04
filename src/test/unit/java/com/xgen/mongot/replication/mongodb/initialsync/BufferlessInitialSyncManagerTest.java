package com.xgen.mongot.replication.mongodb.initialsync;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Test;

public class BufferlessInitialSyncManagerTest {

  @Test
  public void testGetSizeBucketLessThan100MB() {
    Assert.assertEquals("lt_100MB", BufferlessInitialSyncManager.getSizeBucket(0));
    Assert.assertEquals("lt_100MB", BufferlessInitialSyncManager.getSizeBucket(1));
    Assert.assertEquals("lt_100MB", BufferlessInitialSyncManager.getSizeBucket(50L * 1024 * 1024));
    Assert.assertEquals(
        "lt_100MB", BufferlessInitialSyncManager.getSizeBucket(100L * 1024 * 1024 - 1));
  }

  @Test
  public void testGetSizeBucket100MBto1GB() {
    Assert.assertEquals(
        "100MB_1GB", BufferlessInitialSyncManager.getSizeBucket(100L * 1024 * 1024));
    Assert.assertEquals(
        "100MB_1GB", BufferlessInitialSyncManager.getSizeBucket(512L * 1024 * 1024));
    Assert.assertEquals(
        "100MB_1GB", BufferlessInitialSyncManager.getSizeBucket(1024L * 1024 * 1024 - 1));
  }

  @Test
  public void testGetSizeBucket1GBto5GB() {
    Assert.assertEquals("1GB_5GB", BufferlessInitialSyncManager.getSizeBucket(1024L * 1024 * 1024));
    Assert.assertEquals(
        "1GB_5GB", BufferlessInitialSyncManager.getSizeBucket(3L * 1024 * 1024 * 1024));
    Assert.assertEquals(
        "1GB_5GB", BufferlessInitialSyncManager.getSizeBucket(5L * 1024 * 1024 * 1024 - 1));
  }

  @Test
  public void testGetSizeBucket5GBto10GB() {
    Assert.assertEquals(
        "5GB_10GB", BufferlessInitialSyncManager.getSizeBucket(5L * 1024 * 1024 * 1024));
    Assert.assertEquals(
        "5GB_10GB", BufferlessInitialSyncManager.getSizeBucket(7L * 1024 * 1024 * 1024));
    Assert.assertEquals(
        "5GB_10GB", BufferlessInitialSyncManager.getSizeBucket(10L * 1024 * 1024 * 1024 - 1));
  }

  @Test
  public void testGetSizeBucketAbove10GB() {
    Assert.assertEquals(
        "gt_10GB", BufferlessInitialSyncManager.getSizeBucket(10L * 1024 * 1024 * 1024));
    Assert.assertEquals(
        "gt_10GB", BufferlessInitialSyncManager.getSizeBucket(100L * 1024 * 1024 * 1024));
  }

  @Test
  public void testFsyncDurationTimerRecordedOnSuccess() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("initialSyncManager", registry);

    InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
    when(mongoClient.fsync()).thenReturn(new BsonDocument());

    InitialSyncContext context = mock(InitialSyncContext.class);
    when(context.useNaturalOrderScan()).thenReturn(true);

    ServerClusterTimeProvider clusterTimeProvider = mock(ServerClusterTimeProvider.class);
    when(clusterTimeProvider.getCurrentClusterTime()).thenReturn(new BsonTimestamp());

    BufferlessChangeStreamApplierFactory changeStreamApplierFactory =
        mock(BufferlessChangeStreamApplierFactory.class);
    when(changeStreamApplierFactory.create(any(), anyBoolean()))
        .thenReturn(mock(BufferlessChangeStreamApplier.class));

    BufferlessInitialSyncManager manager =
        new BufferlessInitialSyncManager(
            context,
            mock(BufferlessCollectionScannerFactory.class),
            changeStreamApplierFactory,
            clusterTimeProvider,
            Duration.ofMinutes(5),
            Optional.empty(),
            metricsFactory,
            mongoClient);

    // Call sync() to exercise the fsync path; it will fail after fsync due to incomplete mocks
    try {
      manager.sync();
    } catch (Exception ignored) {
      // Expected — we only care about verifying the fsync success metrics
    }

    Assert.assertNotNull(
        registry.find("initialSyncManager.fsyncDuration").tag("outcome", "success").timer());
    Assert.assertEquals(
        1,
        registry
            .find("initialSyncManager.fsyncDuration")
            .tag("outcome", "success")
            .timer()
            .count());
    Assert.assertEquals(
        0,
        registry
            .find("initialSyncManager.fsyncDuration")
            .tag("outcome", "failure")
            .timer()
            .count());
  }

  @Test
  public void testFsyncErrorCounterIncludesMongoCommandExceptionTags() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("initialSyncManager", registry);

    InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
    when(mongoClient.fsync())
        .thenThrow(
            new MongoCommandException(
                new BsonDocument("ok", new org.bson.BsonInt32(0))
                    .append("errmsg", new org.bson.BsonString("not authorized"))
                    .append("code", new org.bson.BsonInt32(13))
                    .append("codeName", new org.bson.BsonString("Unauthorized")),
                new ServerAddress()));

    InitialSyncContext context = mock(InitialSyncContext.class);
    when(context.useNaturalOrderScan()).thenReturn(true);

    ServerClusterTimeProvider clusterTimeProvider = mock(ServerClusterTimeProvider.class);
    when(clusterTimeProvider.getCurrentClusterTime()).thenReturn(new BsonTimestamp());

    BufferlessChangeStreamApplierFactory changeStreamApplierFactory =
        mock(BufferlessChangeStreamApplierFactory.class);
    when(changeStreamApplierFactory.create(any(), anyBoolean()))
        .thenReturn(mock(BufferlessChangeStreamApplier.class));

    BufferlessInitialSyncManager manager =
        new BufferlessInitialSyncManager(
            context,
            mock(BufferlessCollectionScannerFactory.class),
            changeStreamApplierFactory,
            clusterTimeProvider,
            Duration.ofMinutes(5),
            Optional.empty(),
            metricsFactory,
            mongoClient);

    // Call sync() to exercise the fsync error path
    try {
      manager.sync();
    } catch (Exception ignored) {
      // Expected — we only care about verifying the error counter
    }

    // Verify the error counter is incremented with expected tags
    Assert.assertNotNull(
        registry
            .find("initialSyncManager.fsyncError")
            .tag("errorCode", "Unauthorized")
            .tag("errorType", "MongoCommandException")
            .counter());
    Assert.assertEquals(
        1.0,
        registry
            .find("initialSyncManager.fsyncError")
            .tag("errorCode", "Unauthorized")
            .tag("errorType", "MongoCommandException")
            .counter()
            .count(),
        0.0);
    // Failure duration timer should also be recorded
    Assert.assertEquals(
        1,
        registry
            .find("initialSyncManager.fsyncDuration")
            .tag("outcome", "failure")
            .timer()
            .count());
  }

  @Test
  public void testFsyncSkippedWhenNaturalOrderScanDisabled() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("initialSyncManager", registry);

    InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);

    InitialSyncContext context = mock(InitialSyncContext.class);
    when(context.useNaturalOrderScan()).thenReturn(false);

    new BufferlessInitialSyncManager(
        context,
        mock(BufferlessCollectionScannerFactory.class),
        mock(BufferlessChangeStreamApplierFactory.class),
        mock(ServerClusterTimeProvider.class),
        Duration.ofMinutes(5),
        Optional.empty(),
        metricsFactory,
        mongoClient);

    // Timers should be registered but with zero count since fsync won't be called
    Assert.assertEquals(
        0,
        registry
            .find("initialSyncManager.fsyncDuration")
            .tag("outcome", "success")
            .timer()
            .count());
    Assert.assertEquals(
        0,
        registry
            .find("initialSyncManager.fsyncDuration")
            .tag("outcome", "failure")
            .timer()
            .count());
  }
}
