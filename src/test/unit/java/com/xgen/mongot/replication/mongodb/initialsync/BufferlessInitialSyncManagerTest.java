package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.uniqueMockGenerationDefinition;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.mongodb.common.BufferlessNaturalOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.BufferlessParallelNaturalOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.PartitionResumeInfo;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
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
  public void testParallelResumeInfoRequiresResync() throws Exception {
    InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
    BufferlessInitialSyncManager manager =
        managerResumingFrom(parallelResumeInfo(), /* useNaturalOrderScan= */ true, mongoClient);

    InitialSyncException thrown = Assert.assertThrows(InitialSyncException.class, manager::sync);

    // Without the guard this would be an UnsupportedOperationException from getResumeToken().
    Assert.assertEquals(InitialSyncException.Type.REQUIRES_RESYNC, thrown.getType());
    // The sync bails before doing any work on the sync source.
    verify(mongoClient, never()).fsync();
  }

  /**
   * InitialSyncQueue discards a sequential natural-order blob when the scan is disabled but has no
   * case for a parallel one, so this pairing does reach the manager. The guard cannot key off the
   * scan setting, or the blob would fall through to a resume token it does not have.
   */
  @Test
  public void testParallelResumeInfoRequiresResyncWithNaturalOrderScanDisabled() throws Exception {
    InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
    BufferlessInitialSyncManager manager =
        managerResumingFrom(parallelResumeInfo(), /* useNaturalOrderScan= */ false, mongoClient);

    InitialSyncException thrown = Assert.assertThrows(InitialSyncException.class, manager::sync);

    Assert.assertEquals(InitialSyncException.Type.REQUIRES_RESYNC, thrown.getType());
    verify(mongoClient, never()).fsync();
  }

  /**
   * The guard has to catch only the parallel blob. Widening it to every resume info would turn each
   * resume into a full rescan, which no other assertion here would notice. Reaching fsync is the
   * cheapest evidence that a sequential blob still gets through.
   */
  @Test
  public void testSequentialResumeInfoIsNotCaughtByTheParallelGuard() throws Exception {
    InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
    when(mongoClient.fsync()).thenReturn(new BsonDocument());

    BufferlessInitialSyncManager manager =
        managerResumingFrom(sequentialResumeInfo(), /* useNaturalOrderScan= */ true, mongoClient);

    // Gets past the guard and then fails on the incomplete mocks; only how far it got matters.
    try {
      manager.sync();
    } catch (Exception ignored) {
      // Expected.
    }

    verify(mongoClient).fsync();
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

  @Test
  public void translateMaterializedViewRejection_invalidLease_mapsToShutDown() {
    InitialSyncException translated =
        BufferlessInitialSyncManager.translateMaterializedViewRejection(
            new MaterializedViewNonTransientException(
                "ended", MaterializedViewNonTransientException.Reason.INVALID_LEASE));

    Assert.assertEquals(InitialSyncException.Type.SHUT_DOWN, translated.getType());
  }

  @Test
  public void translateMaterializedViewRejection_fencingRejection_mapsToFailed() {
    InitialSyncException translated =
        BufferlessInitialSyncManager.translateMaterializedViewRejection(
            new MaterializedViewNonTransientException(
                "fenced", MaterializedViewNonTransientException.Reason.FENCING_REJECTION));

    Assert.assertEquals(InitialSyncException.Type.FAILED, translated.getType());
  }

  @Test
  public void doFirstCommit_updateCommitUserDataRejectedWithInvalidLease_throwsShutDown()
      throws Exception {
    // An ended tenure discovered while staging the first commit's user data (before the async
    // commit task even starts) must be an ordinary demotion, not a JVM halt (CLOUDP-434414).
    DocumentIndexer indexer = mock(DocumentIndexer.class);
    doThrow(
            new MaterializedViewNonTransientException(
                "ended", MaterializedViewNonTransientException.Reason.INVALID_LEASE))
        .when(indexer)
        .updateCommitUserData(any());
    BufferlessInitialSyncManager manager = managerWithIndexer(indexer);

    InitialSyncException thrown =
        Assert.assertThrows(
            InitialSyncException.class, () -> manager.doFirstCommit(IndexCommitUserData.EMPTY));

    Assert.assertEquals(InitialSyncException.Type.SHUT_DOWN, thrown.getType());
  }

  @Test
  public void doFirstCommit_commitRejectedWithInvalidLease_throwsShutDown() throws Exception {
    // Same as above, but the tenure ends between staging the commit data and the commit itself
    // running (on its own thread) actually reaching the writer.
    DocumentIndexer indexer = mock(DocumentIndexer.class);
    doThrow(
            new MaterializedViewNonTransientException(
                "ended", MaterializedViewNonTransientException.Reason.INVALID_LEASE))
        .when(indexer)
        .commit();
    BufferlessInitialSyncManager manager = managerWithIndexer(indexer);

    InitialSyncException thrown =
        Assert.assertThrows(
            InitialSyncException.class, () -> manager.doFirstCommit(IndexCommitUserData.EMPTY));

    Assert.assertEquals(InitialSyncException.Type.SHUT_DOWN, thrown.getType());
  }

  @Test
  public void doFirstCommit_commitRejectedWithFencingRejection_throwsFailed() throws Exception {
    // A zombie leader discovering it's been fenced at the first commit after initial sync must be
    // routed to the manager-owned retire-and-replace handler, not halt the JVM (CLOUDP-434414).
    DocumentIndexer indexer = mock(DocumentIndexer.class);
    doThrow(
            new MaterializedViewNonTransientException(
                "fenced", MaterializedViewNonTransientException.Reason.FENCING_REJECTION))
        .when(indexer)
        .commit();
    BufferlessInitialSyncManager manager = managerWithIndexer(indexer);

    InitialSyncException thrown =
        Assert.assertThrows(
            InitialSyncException.class, () -> manager.doFirstCommit(IndexCommitUserData.EMPTY));

    Assert.assertEquals(InitialSyncException.Type.FAILED, thrown.getType());
  }

  private static BufferlessInitialSyncManager managerWithIndexer(DocumentIndexer indexer) {
    IndexDefinitionGeneration indexDefinitionGeneration = uniqueMockGenerationDefinition();
    InitialSyncContext context =
        InitialSyncContext.create(
            indexDefinitionGeneration,
            mock(IndexingWorkScheduler.class),
            indexer,
            mock(IndexMetricsUpdater.class),
            Optional.empty(),
            false,
            false,
            false,
            1,
            ToggleGate.opened());
    return new BufferlessInitialSyncManager(
        context,
        mock(BufferlessCollectionScannerFactory.class),
        mock(BufferlessChangeStreamApplierFactory.class),
        mock(ServerClusterTimeProvider.class),
        Duration.ofMinutes(5),
        Optional.empty(),
        new MetricsFactory("initialSyncManager", new SimpleMeterRegistry()),
        mock(InitialSyncMongoClient.class));
  }

  private static BufferlessInitialSyncManager managerResumingFrom(
      InitialSyncResumeInfo resumeInfo,
      boolean useNaturalOrderScan,
      InitialSyncMongoClient mongoClient)
      throws InitialSyncException {
    InitialSyncContext context = mock(InitialSyncContext.class);
    when(context.useNaturalOrderScan()).thenReturn(useNaturalOrderScan);

    BufferlessChangeStreamApplierFactory changeStreamApplierFactory =
        mock(BufferlessChangeStreamApplierFactory.class);
    when(changeStreamApplierFactory.create(any(), anyBoolean()))
        .thenReturn(mock(BufferlessChangeStreamApplier.class));

    return new BufferlessInitialSyncManager(
        context,
        mock(BufferlessCollectionScannerFactory.class),
        changeStreamApplierFactory,
        mock(ServerClusterTimeProvider.class),
        Duration.ofMinutes(5),
        Optional.of(resumeInfo),
        new MetricsFactory("initialSyncManager", new SimpleMeterRegistry()),
        mongoClient);
  }

  private static BufferlessNaturalOrderInitialSyncResumeInfo sequentialResumeInfo() {
    return new BufferlessNaturalOrderInitialSyncResumeInfo(
        new BsonTimestamp(1_000, 1),
        new BsonDocument("$recordId", new BsonInt64(250)),
        Optional.empty());
  }

  private static BufferlessParallelNaturalOrderInitialSyncResumeInfo parallelResumeInfo() {
    return new BufferlessParallelNaturalOrderInitialSyncResumeInfo(
        new BsonTimestamp(1_000, 1),
        UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee"),
        List.of(
            new PartitionResumeInfo(
                0, Optional.empty(), Optional.of(500L), Optional.of(250L), false),
            new PartitionResumeInfo(
                1, Optional.of(500L), Optional.empty(), Optional.empty(), false)),
        Optional.empty());
  }
}
