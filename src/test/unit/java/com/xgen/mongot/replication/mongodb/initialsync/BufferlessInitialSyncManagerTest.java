package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.uniqueMockGenerationDefinition;
import static com.xgen.testing.mongot.mock.index.SearchIndex.IGNORE_METRICS;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer.mockDocumentIndexer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
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
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

public class BufferlessInitialSyncManagerTest {

  private static final BsonTimestamp HIGH_WATER_MARK = new BsonTimestamp(1_700_000_000, 7);

  /** Stands in for what namespace resolution returns, which the planner reads its options from. */
  private static final MongoDbCollectionInfo.Collection COLLECTION_INFO =
      new MongoDbCollectionInfo.Collection(
          "mockCollection",
          new MongoDbCollectionInfo.Collection.Info(
              UUID.fromString("1d69f3f2-3ca4-4bb0-bd5a-7c3b1e36a9d2")));

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
    when(changeStreamApplierFactory.create(any(), anyBoolean(), any()))
        .thenReturn(mock(BufferlessChangeStreamApplier.class));

    BufferlessInitialSyncManager manager =
        new BufferlessInitialSyncManager(
            context,
            mock(BufferlessCollectionScannerFactory.class),
            changeStreamApplierFactory,
            mock(ParallelCollectionScanSessionFactory.class),
            mock(InitialSyncPartitionPlannerFactory.class),
            COLLECTION_INFO,
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
    when(changeStreamApplierFactory.create(any(), anyBoolean(), any()))
        .thenReturn(mock(BufferlessChangeStreamApplier.class));

    BufferlessInitialSyncManager manager =
        new BufferlessInitialSyncManager(
            context,
            mock(BufferlessCollectionScannerFactory.class),
            changeStreamApplierFactory,
            mock(ParallelCollectionScanSessionFactory.class),
            mock(InitialSyncPartitionPlannerFactory.class),
            COLLECTION_INFO,
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

  /**
   * A blob the feature already wrote is unreadable with either switch off: only a natural-order
   * parallel run can scan its partitions, and it holds no single token to fall back on.
   */
  @Test
  public void testParallelResumeInfoRequiresResyncWhenEitherSwitchIsOff() throws Exception {
    assertParallelResumeRequiresResync(
        /* useNaturalOrderScan= */ true, /* parallelScanEnabled= */ false);
    assertParallelResumeRequiresResync(
        /* useNaturalOrderScan= */ false, /* parallelScanEnabled= */ true);
  }

  /** The core of the branch: a partitioned plan runs its windows through the session. */
  @Test
  public void testPartitionedPlanDrivesScanWindowsInsteadOfTheSingleCursorScan() throws Exception {
    ParallelCollectionScanSession session = mock(ParallelCollectionScanSession.class);
    when(session.scanWindow()).thenReturn(true, false);
    BufferlessCollectionScannerFactory collectionScannerFactory =
        mock(BufferlessCollectionScannerFactory.class);

    BufferlessInitialSyncManager manager =
        parallelManager(
            /* parallelScanEnabled= */ true,
            plannerReturning(partitionedPlan()),
            resumeState -> session,
            collectionScannerFactory,
            applierFactory());

    syncIgnoringTheTail(manager);

    verify(session, times(2)).scanWindow();
    verify(session).close();
    verify(collectionScannerFactory, never()).create(any(), any());
  }

  /**
   * The applier and the session have to share one state object. On two copies, the windows would
   * never see the high water mark the applier advances.
   */
  @Test
  public void testPartitionedPlanHandsOneResumeStateToBothTheApplierAndTheSession()
      throws Exception {
    List<ParallelCollectionScanResumeState> sessionSaw = new ArrayList<>();
    BufferlessChangeStreamApplierFactory applierFactory = applierFactory();

    BufferlessInitialSyncManager manager =
        parallelManager(
            /* parallelScanEnabled= */ true,
            plannerReturning(partitionedPlan()),
            resumeState -> {
              sessionSaw.add(resumeState);
              return mock(ParallelCollectionScanSession.class);
            },
            mock(BufferlessCollectionScannerFactory.class),
            applierFactory);

    syncIgnoringTheTail(manager);

    ArgumentCaptor<Optional<ParallelCollectionScanResumeState>> applierSaw =
        ArgumentCaptor.captor();
    verify(applierFactory).create(any(), anyBoolean(), applierSaw.capture());

    Assert.assertEquals(1, sessionSaw.size());
    Assert.assertEquals(Optional.of(sessionSaw.getFirst()), applierSaw.getValue());
    Assert.assertEquals(2, sessionSaw.getFirst().getPartitions().size());
  }

  /**
   * The boundaries are committed before the first window, and after the session, which installs
   * what a commit reads.
   */
  @Test
  public void testPartitionedPlanCommitsTheBoundariesBeforeTheFirstWindow() throws Exception {
    DocumentIndexer documentIndexer = mockDocumentIndexer();
    ParallelCollectionScanSession session = mock(ParallelCollectionScanSession.class);

    BufferlessInitialSyncManager manager =
        parallelManager(
            /* parallelScanEnabled= */ true,
            plannerReturning(partitionedPlan()),
            resumeState -> session,
            mock(BufferlessCollectionScannerFactory.class),
            applierFactory(),
            documentIndexer);

    syncIgnoringTheTail(manager);

    InOrder ordered = inOrder(documentIndexer, session);
    ordered.verify(documentIndexer).commit();
    ordered.verify(session).scanWindow();
  }

  /** A commit that fails costs only the boundaries, so the scan has to carry on. */
  @Test
  public void testPartitionedPlanScansOnWhenCommittingTheBoundariesFails() throws Exception {
    DocumentIndexer documentIndexer = mockDocumentIndexer();
    doThrow(new IOException("no space left on device")).doNothing().when(documentIndexer).commit();
    ParallelCollectionScanSession session = mock(ParallelCollectionScanSession.class);

    BufferlessInitialSyncManager manager =
        parallelManager(
            /* parallelScanEnabled= */ true,
            plannerReturning(partitionedPlan()),
            resumeState -> session,
            mock(BufferlessCollectionScannerFactory.class),
            applierFactory(),
            documentIndexer);

    syncIgnoringTheTail(manager);

    verify(session).scanWindow();
  }

  /**
   * What the commit buys: a restart rebuilds the partitions from the blob and picks each one up
   * from the record id it reached, with no planning and nothing to write again.
   */
  @Test
  public void testParallelResumeRebuildsTheSessionFromTheCommittedPartitions() throws Exception {
    InitialSyncPartitionPlanner planner = mock(InitialSyncPartitionPlanner.class);
    List<ParallelCollectionScanResumeState> sessionSaw = new ArrayList<>();
    DocumentIndexer documentIndexer = mockDocumentIndexer();

    BufferlessInitialSyncManager manager =
        parallelManager(
            /* parallelScanEnabled= */ true,
            contextWithProgress -> planner,
            resumeState -> {
              sessionSaw.add(resumeState);
              return mock(ParallelCollectionScanSession.class);
            },
            mock(BufferlessCollectionScannerFactory.class),
            applierFactory(),
            documentIndexer,
            Optional.of(parallelResumeInfo()));

    syncIgnoringTheTail(manager);

    verify(planner, never()).plan(any());
    Assert.assertEquals(1, sessionSaw.size());
    ParallelCollectionScanResumeState resumed = sessionSaw.getFirst();
    Assert.assertEquals(parallelResumeInfo().getInitialSyncId(), resumed.getInitialSyncId());
    Assert.assertEquals(
        List.of(Optional.of(250L), Optional.empty()),
        resumed.getPartitions().stream()
            .map(ParallelCollectionScanPartitionState::getResumeRecordId)
            .toList());
    // The boundaries came off disk, so a resume must not pay to write them again.
    verify(documentIndexer, never()).commit();
  }

  /** A window that throws still has to give the partition pool back. */
  @Test
  public void testPartitionedPlanClosesTheSessionWhenAWindowFails() throws Exception {
    ParallelCollectionScanSession session = mock(ParallelCollectionScanSession.class);
    when(session.scanWindow()).thenThrow(InitialSyncException.createShutDown());

    BufferlessInitialSyncManager manager =
        parallelManager(
            /* parallelScanEnabled= */ true,
            plannerReturning(partitionedPlan()),
            resumeState -> session,
            mock(BufferlessCollectionScannerFactory.class),
            applierFactory());

    syncIgnoringTheTail(manager);

    verify(session).close();
  }

  /** Declining to partition has to leave the existing single-cursor scan untouched. */
  @Test
  public void testSequentialFallbackKeepsTheSingleCursorScan() throws Exception {
    ParallelCollectionScanSessionFactory sessionFactory =
        mock(ParallelCollectionScanSessionFactory.class);
    BufferlessCollectionScannerFactory collectionScannerFactory =
        mock(BufferlessCollectionScannerFactory.class);

    BufferlessInitialSyncManager manager =
        parallelManager(
            /* parallelScanEnabled= */ true,
            plannerReturning(
                new InitialSyncPartitionPlan.SequentialFallback(
                    InitialSyncPartitionPlan.ShortCircuitReason.TOO_FEW_DOCUMENTS)),
            sessionFactory,
            collectionScannerFactory,
            applierFactory());

    syncIgnoringTheTail(manager);

    verify(sessionFactory, never()).create(any());
    verify(collectionScannerFactory).create(any(), any());
  }

  /**
   * The planner needs the resolved collection and a context carrying the high water mark. Without
   * that progress it raises internally, turns the failure into a fallback, and quietly scans with
   * one cursor.
   */
  @Test
  public void testPartitionedPlanPlansTheResolvedCollectionAtTheHighWaterMark() throws Exception {
    InitialSyncPartitionPlanner planner = mock(InitialSyncPartitionPlanner.class);
    when(planner.plan(any())).thenReturn(partitionedPlan());
    List<InitialSyncContext> plannerSaw = new ArrayList<>();

    BufferlessInitialSyncManager manager =
        parallelManager(
            /* parallelScanEnabled= */ true,
            contextWithProgress -> {
              plannerSaw.add(contextWithProgress);
              return planner;
            },
            resumeState -> mock(ParallelCollectionScanSession.class),
            mock(BufferlessCollectionScannerFactory.class),
            applierFactory());

    syncIgnoringTheTail(manager);

    Assert.assertEquals(1, plannerSaw.size());
    Assert.assertEquals(
        HIGH_WATER_MARK, plannerSaw.getFirst().getChangeStreamResumeOperationTime());
    verify(planner).plan(COLLECTION_INFO);
  }

  /** Planning costs a round trip, so a disabled feature must not reach the planner at all. */
  @Test
  public void testParallelScanDisabledNeverConsultsThePlanner() throws Exception {
    InitialSyncPartitionPlanner planner = mock(InitialSyncPartitionPlanner.class);

    BufferlessInitialSyncManager manager =
        parallelManager(
            /* parallelScanEnabled= */ false,
            contextWithProgress -> planner,
            mock(ParallelCollectionScanSessionFactory.class),
            mock(BufferlessCollectionScannerFactory.class),
            applierFactory());

    syncIgnoringTheTail(manager);

    verify(planner, never()).plan(any());
  }

  /**
   * A sequential natural-order resume has already indexed its way to a token. Planning would draw
   * boundaries over the whole collection and re-scan every document behind that token, so the
   * resume has to keep its single cursor however large the collection is.
   */
  @Test
  public void testSequentialResumeScansOnInsteadOfPartitioning() throws Exception {
    // A planner that would happily partition, so the resume staying sequential is the guard's
    // doing and not a planner that had nothing to offer.
    InitialSyncPartitionPlanner planner = mock(InitialSyncPartitionPlanner.class);
    when(planner.plan(any())).thenReturn(partitionedPlan());
    ParallelCollectionScanSessionFactory sessionFactory =
        mock(ParallelCollectionScanSessionFactory.class);
    BufferlessChangeStreamApplierFactory applierFactory = applierFactory();

    BufferlessInitialSyncManager manager =
        parallelManager(
            /* parallelScanEnabled= */ true,
            contextWithProgress -> planner,
            sessionFactory,
            mock(BufferlessCollectionScannerFactory.class),
            applierFactory,
            mockDocumentIndexer(),
            Optional.of(sequentialNaturalOrderResumeInfo()));

    syncIgnoringTheTail(manager);

    // Asserted positively rather than through the never() calls alone, which would also hold if
    // sync() stopped reaching the resolve at all.
    ArgumentCaptor<Optional<ParallelCollectionScanResumeState>> applierSaw =
        ArgumentCaptor.captor();
    verify(applierFactory).create(any(), anyBoolean(), applierSaw.capture());
    Assert.assertEquals(Optional.empty(), applierSaw.getValue());

    verify(planner, never()).plan(any());
    verify(sessionFactory, never()).create(any());
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
        mock(ParallelCollectionScanSessionFactory.class),
        mock(InitialSyncPartitionPlannerFactory.class),
        COLLECTION_INFO,
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
        mock(ParallelCollectionScanSessionFactory.class),
        mock(InitialSyncPartitionPlannerFactory.class),
        COLLECTION_INFO,
        mock(ServerClusterTimeProvider.class),
        Duration.ofMinutes(5),
        Optional.empty(),
        new MetricsFactory("initialSyncManager", new SimpleMeterRegistry()),
        mock(InitialSyncMongoClient.class));
  }

  private static void assertParallelResumeRequiresResync(
      boolean useNaturalOrderScan, boolean parallelScanEnabled) throws Exception {
    InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
    BufferlessInitialSyncManager manager =
        managerResumingFrom(
            parallelResumeInfo(), useNaturalOrderScan, parallelScanEnabled, mongoClient);

    InitialSyncException thrown = Assert.assertThrows(InitialSyncException.class, manager::sync);

    // Without the guard this would be an UnsupportedOperationException from getResumeToken().
    Assert.assertEquals(InitialSyncException.Type.REQUIRES_RESYNC, thrown.getType());
    // The sync bails before doing any work on the sync source.
    verify(mongoClient, never()).fsync();
  }

  /**
   * Runs sync() through the scan loop and swallows what follows: these mocks cannot carry a sync to
   * its first commit, and only the branch the loop took is under test.
   */
  private static void syncIgnoringTheTail(BufferlessInitialSyncManager manager) {
    try {
      manager.sync();
    } catch (Exception ignored) {
      // Expected.
    }
  }

  private static BufferlessChangeStreamApplierFactory applierFactory() throws InitialSyncException {
    BufferlessChangeStreamApplierFactory factory = mock(BufferlessChangeStreamApplierFactory.class);
    when(factory.create(any(), anyBoolean(), any()))
        .thenReturn(mock(BufferlessChangeStreamApplier.class));
    return factory;
  }

  /** A planner factory whose planner answers with {@code plan}, whatever it is handed. */
  private static InitialSyncPartitionPlannerFactory plannerReturning(
      InitialSyncPartitionPlan plan) {
    InitialSyncPartitionPlanner planner = mock(InitialSyncPartitionPlanner.class);
    when(planner.plan(any())).thenReturn(plan);
    return contextWithProgress -> planner;
  }

  private static InitialSyncPartitionPlan.PartitionedResult partitionedPlan() {
    return new InitialSyncPartitionPlan.PartitionedResult(
        ImmutableList.of(
            new InitialSyncPartitionPlan.PartitionRange(Optional.empty(), Optional.of(500L)),
            new InitialSyncPartitionPlan.PartitionRange(Optional.of(500L), Optional.empty())),
        UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee"));
  }

  private static BufferlessInitialSyncManager parallelManager(
      boolean parallelScanEnabled,
      InitialSyncPartitionPlannerFactory partitionPlannerFactory,
      ParallelCollectionScanSessionFactory parallelSessionFactory,
      BufferlessCollectionScannerFactory collectionScannerFactory,
      BufferlessChangeStreamApplierFactory changeStreamApplierFactory)
      throws Exception {
    return parallelManager(
        parallelScanEnabled,
        partitionPlannerFactory,
        parallelSessionFactory,
        collectionScannerFactory,
        changeStreamApplierFactory,
        mockDocumentIndexer());
  }

  private static BufferlessInitialSyncManager parallelManager(
      boolean parallelScanEnabled,
      InitialSyncPartitionPlannerFactory partitionPlannerFactory,
      ParallelCollectionScanSessionFactory parallelSessionFactory,
      BufferlessCollectionScannerFactory collectionScannerFactory,
      BufferlessChangeStreamApplierFactory changeStreamApplierFactory,
      DocumentIndexer documentIndexer)
      throws Exception {
    return parallelManager(
        parallelScanEnabled,
        partitionPlannerFactory,
        parallelSessionFactory,
        collectionScannerFactory,
        changeStreamApplierFactory,
        documentIndexer,
        Optional.empty());
  }

  private static BufferlessInitialSyncManager parallelManager(
      boolean parallelScanEnabled,
      InitialSyncPartitionPlannerFactory partitionPlannerFactory,
      ParallelCollectionScanSessionFactory parallelSessionFactory,
      BufferlessCollectionScannerFactory collectionScannerFactory,
      BufferlessChangeStreamApplierFactory changeStreamApplierFactory,
      DocumentIndexer documentIndexer,
      Optional<InitialSyncResumeInfo> resumeInfo)
      throws Exception {
    // A real context rather than a mock: the scan paths read indexDefinitionGeneration, a public
    // field that a mock leaves null.
    InitialSyncContext context =
        InitialSyncContext.create(
            MOCK_INDEX_DEFINITION_GENERATION,
            mock(IndexingWorkScheduler.class),
            documentIndexer,
            IGNORE_METRICS,
            Optional.empty(),
            /* removeMatchCollectionUuid= */ false,
            /* useNaturalOrderScan= */ true,
            parallelScanEnabled,
            /* parallelPartitionCount= */ 2,
            ToggleGate.opened());

    InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
    when(mongoClient.fsync()).thenReturn(new BsonDocument());
    when(mongoClient.getSyncSourceHost()).thenReturn("testHost");

    ServerClusterTimeProvider clusterTimeProvider = mock(ServerClusterTimeProvider.class);
    when(clusterTimeProvider.getCurrentClusterTime()).thenReturn(HIGH_WATER_MARK);

    return new BufferlessInitialSyncManager(
        context,
        collectionScannerFactory,
        changeStreamApplierFactory,
        parallelSessionFactory,
        partitionPlannerFactory,
        COLLECTION_INFO,
        clusterTimeProvider,
        Duration.ofMinutes(5),
        resumeInfo,
        new MetricsFactory("initialSyncManager", new SimpleMeterRegistry()),
        mongoClient);
  }

  private static BufferlessInitialSyncManager managerResumingFrom(
      InitialSyncResumeInfo resumeInfo,
      boolean useNaturalOrderScan,
      boolean parallelScanEnabled,
      InitialSyncMongoClient mongoClient)
      throws InitialSyncException {
    InitialSyncContext context = mock(InitialSyncContext.class);
    when(context.useNaturalOrderScan()).thenReturn(useNaturalOrderScan);
    when(context.isParallelNaturalOrderScanEnabled()).thenReturn(parallelScanEnabled);

    BufferlessChangeStreamApplierFactory changeStreamApplierFactory =
        mock(BufferlessChangeStreamApplierFactory.class);
    when(changeStreamApplierFactory.create(any(), anyBoolean(), any()))
        .thenReturn(mock(BufferlessChangeStreamApplier.class));

    return new BufferlessInitialSyncManager(
        context,
        mock(BufferlessCollectionScannerFactory.class),
        changeStreamApplierFactory,
        mock(ParallelCollectionScanSessionFactory.class),
        mock(InitialSyncPartitionPlannerFactory.class),
        COLLECTION_INFO,
        mock(ServerClusterTimeProvider.class),
        Duration.ofMinutes(5),
        Optional.of(resumeInfo),
        new MetricsFactory("initialSyncManager", new SimpleMeterRegistry()),
        mongoClient);
  }

  /** A resume from a single-cursor natural-order scan that had reached a record id. */
  private static BufferlessNaturalOrderInitialSyncResumeInfo sequentialNaturalOrderResumeInfo() {
    return new BufferlessNaturalOrderInitialSyncResumeInfo(
        HIGH_WATER_MARK,
        new BsonDocument("$recordId", new BsonInt64(250)),
        Optional.of("testHost"));
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
