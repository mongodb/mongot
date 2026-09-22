package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.mongodb.MongoException;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.replication.mongodb.IndexManager;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.mongodb.Errors;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link MaterializedViewGenerator}'s role-switching functionality.
 *
 * <p>These tests verify the behavior of the leader/follower role management methods: {@link
 * MaterializedViewGenerator#isLeader()} and {@link MaterializedViewGenerator#becomeLeader()}.
 *
 * <p>All generators are created as followers. Call {@link MaterializedViewGenerator#becomeLeader()}
 * to activate leader mode. To transition back to follower mode, the generator must be shut down and
 * replaced with a new generator (see {@link MaterializedViewManager#transitionToFollower}).
 */
public class MaterializedViewGeneratorTest {

  private NamedExecutorService executorService;

  @Before
  public void setUp() {
    this.executorService =
        Executors.fixedSizeThreadPool("test-indexing", 1, new SimpleMeterRegistry());
  }

  @After
  public void tearDown() {
    if (this.executorService != null) {
      this.executorService.shutdown();
    }
  }

  @Test
  public void isLeader_newlyCreated_returnsFalse() {
    MaterializedViewGenerator generator = createGenerator();
    assertFalse(generator.isLeader());
  }

  @Test
  public void becomeLeader_whenFollower_setsIsLeaderTrue() {
    MaterializedViewGenerator generator = createGenerator();
    assertFalse(generator.isLeader());

    generator.becomeLeader();

    assertTrue(generator.isLeader());
  }

  @Test
  public void becomeLeader_whenAlreadyLeader_remainsLeader() {
    MaterializedViewGenerator generator = createGenerator();
    generator.becomeLeader();
    assertTrue(generator.isLeader());

    generator.becomeLeader();

    assertTrue(generator.isLeader());
  }

  @Test
  public void becomeLeader_callsSetLeaderModeTrue() {
    InitializedMaterializedViewIndex matViewIndex = mock(InitializedMaterializedViewIndex.class);
    MaterializedViewGenerator generator = createGeneratorWithIndex(matViewIndex);

    generator.becomeLeader();

    assertTrue(generator.isLeader());
    verify(matViewIndex).setLeaderMode(true);
  }

  @Test
  public void clearLeaderState_whenLeader_clearsFlagAndGauge() {
    InitializedMaterializedViewIndex matViewIndex = mock(InitializedMaterializedViewIndex.class);
    MaterializedViewGenerator generator = createGeneratorWithIndex(matViewIndex);
    generator.becomeLeader();

    generator.clearLeaderState();

    assertFalse(generator.isLeader());
    verify(matViewIndex).setLeaderMode(false);
  }

  @Test
  public void shutdown_callsSetLeaderModeFalse() throws Exception {
    InitializedMaterializedViewIndex matViewIndex = mock(InitializedMaterializedViewIndex.class);
    MaterializedViewGenerator generator = createGeneratorWithIndex(matViewIndex);
    generator.becomeLeader();

    generator.shutdown().get();

    // setLeaderMode(false) may be called more than once: once by initReplication()'s terminal
    // state guard (if init fails before shutdown completes) and once by shutdown() itself.
    verify(matViewIndex, atLeastOnce()).setLeaderMode(false);
  }

  @Test
  public void shutdown_clearsIsLeader() throws Exception {
    MaterializedViewGenerator generator = createGenerator();
    generator.becomeLeader();
    assertTrue("Should be leader after becomeLeader()", generator.isLeader());

    generator.shutdown().get();

    assertFalse("Should not be leader after shutdown()", generator.isLeader());
  }

  @Test
  public void shutdown_calledTwice_returnsSameFuture() throws Exception {
    MaterializedViewGenerator generator = createGenerator();
    generator.becomeLeader();

    CompletableFuture<Void> first = generator.shutdown();
    CompletableFuture<Void> second = generator.shutdown();

    assertSame("Second shutdown() must return the same future as the first", first, second);
  }

  @Test
  public void handleSteadyStateNonInvalidatingResync_oplogFalloff_incrementsCounter() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MaterializedViewGenerator generator = createGeneratorForMetricsTest(registry);

    MongoException changeStreamHistoryLost =
        new MongoException(Errors.CHANGE_STREAM_HISTORY_LOST.code, "oplog fell off");
    SteadyStateException exception =
        SteadyStateException.createNonInvalidatingResync(changeStreamHistoryLost);

    generator.handleSteadyStateNonInvalidatingResync(exception);

    assertEquals(
        1.0, registry.counter("materializedViewGenerator.oplogFalloffResyncEvents").count(), 0.0);
  }

  @Test
  public void handleSteadyStateNonInvalidatingResync_cappedPositionLost_incrementsCounter() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MaterializedViewGenerator generator = createGeneratorForMetricsTest(registry);

    MongoException cappedPositionLost =
        new MongoException(Errors.CAPPED_POSITION_LOST.code, "capped position lost");
    SteadyStateException exception =
        SteadyStateException.createNonInvalidatingResync(cappedPositionLost);

    generator.handleSteadyStateNonInvalidatingResync(exception);

    assertEquals(
        1.0, registry.counter("materializedViewGenerator.oplogFalloffResyncEvents").count(), 0.0);
  }

  @Test
  public void handleSteadyStateNonInvalidatingResync_nonOplogError_doesNotIncrementCounter() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MaterializedViewGenerator generator = createGeneratorForMetricsTest(registry);

    MongoException bsonTooLarge =
        new MongoException(Errors.BSON_OBJECT_TOO_LARGE.code, "bson too large");
    SteadyStateException exception = SteadyStateException.createNonInvalidatingResync(bsonTooLarge);

    generator.handleSteadyStateNonInvalidatingResync(exception);

    assertEquals(
        0.0, registry.counter("materializedViewGenerator.oplogFalloffResyncEvents").count(), 0.0);
  }

  @Test
  public void handleSteadyStateNonInvalidatingResync_nonMongoException_doesNotIncrementCounter() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MaterializedViewGenerator generator = createGeneratorForMetricsTest(registry);

    SteadyStateException exception =
        SteadyStateException.createNonInvalidatingResync(new RuntimeException("non-mongo error"));

    generator.handleSteadyStateNonInvalidatingResync(exception);

    assertEquals(
        0.0, registry.counter("materializedViewGenerator.oplogFalloffResyncEvents").count(), 0.0);
  }

  @Test
  public void handleSteadyStateNonInvalidatingResync_fencingRejection_notifiesManager() {
    MaterializedViewGenerator generator = createGeneratorForMetricsTest(new SimpleMeterRegistry());
    MaterializedViewGenerator.FencingRejectionHandler handler =
        mock(MaterializedViewGenerator.FencingRejectionHandler.class);
    generator.setFencingRejectionHandler(handler);

    generator.handleSteadyStateNonInvalidatingResync(
        SteadyStateException.createNonInvalidatingResync(
            new MaterializedViewNonTransientException(
                "fenced", MaterializedViewNonTransientException.Reason.FENCING_REJECTION)));

    verify(handler).handle(generator);
  }

  @Test
  public void handleInitialSyncFailed_fencingRejection_notifiesManager() {
    MaterializedViewGenerator generator = createGeneratorForMetricsTest(new SimpleMeterRegistry());
    MaterializedViewGenerator.FencingRejectionHandler handler =
        mock(MaterializedViewGenerator.FencingRejectionHandler.class);
    generator.setFencingRejectionHandler(handler);

    generator.handleInitialSyncFailed(
        InitialSyncException.createFailed(
            new MaterializedViewNonTransientException(
                "fenced", MaterializedViewNonTransientException.Reason.FENCING_REJECTION)));

    verify(handler).handle(generator);
  }

  @Test
  public void handleInitialSyncFailed_fencingRejection_noHandler_resyncsInPlace() {
    // No fencingRejectionHandler installed -- as for a static leader, since
    // createManagedGenerator() only wires one up for DynamicLeaderLeaseManager. Falling through
    // to the generic FAILED path would strand the generation on a reason
    // IndexStatus.canBeRecovered() doesn't cover, so this must resync in place instead, matching
    // what the steady-state path already does for the same case.
    AtomicBoolean resyncEnqueued = new AtomicBoolean(false);
    MaterializedViewIndexGeneration indexGeneration =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator generator =
        new MaterializedViewGenerator(
            this.executorService,
            mock(MongotCursorManager.class),
            mock(InitialSyncQueue.class),
            mock(SteadyStateManager.class),
            indexGeneration,
            mock(InitializedMaterializedViewIndex.class),
            mock(DocumentIndexer.class),
            mock(PeriodicIndexCommitter.class),
            new com.xgen.mongot.metrics.MetricsFactory(
                "materializedViewGenerator", new SimpleMeterRegistry()),
            mock(FeatureFlags.class),
            Duration.ofSeconds(30),
            Duration.ofSeconds(30),
            Duration.ofSeconds(1),
            false,
            () -> {}) {
          @Override
          protected synchronized void enqueueInitialSync(
              com.xgen.mongot.index.status.IndexStatus status) {
            resyncEnqueued.set(true);
          }
        };

    generator.handleInitialSyncFailed(
        InitialSyncException.createFailed(
            new MaterializedViewNonTransientException(
                "fenced", MaterializedViewNonTransientException.Reason.FENCING_REJECTION)));

    assertTrue("must resync in place instead of failing permanently", resyncEnqueued.get());
    assertNotEquals(IndexManager.State.FAILED, generator.getState());
  }

  @Test
  public void runIndexerOperation_invalidLeaseWhileShutDown_logsAndSwallows() throws Exception {
    // Reached from ReplicationIndexManager.commitIndexer()/clearIndexer() when this generator's
    // tenure has already ended (e.g. shutdownReplicationAndCommitStatus() or clearIndex() racing
    // shutdown()). An ordinary demotion must not halt the JVM over a rejection that is moot for a
    // generator that is already ending or has already ended.
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MaterializedViewGenerator generator = createGeneratorForMetricsTest(registry);
    generator.shutdown();

    generator.runIndexerOperation(
        () -> {
          throw new MaterializedViewNonTransientException(
              "fenced", MaterializedViewNonTransientException.Reason.INVALID_LEASE);
        });

    assertEquals(
        1.0,
        registry
            .counter(
                "materializedViewGenerator.skippedTerminalCommitRejections",
                "reason",
                "INVALID_LEASE")
            .count(),
        0.0);
  }

  @Test
  public void runIndexerOperation_fencingRejectionWhileShutDown_logsAndSwallows() throws Exception {
    // Reached from ReplicationIndexManager.commitIndexer() via shutdownReplicationAndCommitStatus()
    // or staleIndex(), both of which already transitioned to SHUT_DOWN for an unrelated reason
    // before this write. This generator is already ending regardless of the fencing rejection, so
    // it must not halt the JVM over it.
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MaterializedViewGenerator generator = createGeneratorForMetricsTest(registry);
    generator.shutdown();

    generator.runIndexerOperation(
        () -> {
          throw new MaterializedViewNonTransientException(
              "fenced", MaterializedViewNonTransientException.Reason.FENCING_REJECTION);
        });

    assertEquals(
        1.0,
        registry
            .counter(
                "materializedViewGenerator.skippedTerminalCommitRejections",
                "reason",
                "FENCING_REJECTION")
            .count(),
        0.0);
  }

  @Test
  public void runIndexerOperation_notYetShutDown_rethrows() {
    // If this generator hasn't actually transitioned to SHUT_DOWN, a rejection reaching here is
    // not the terminal-transition race this seam exists for -- it's a genuine unexpected failure
    // and must still halt rather than being silently swallowed.
    MaterializedViewGenerator generator = createGeneratorForMetricsTest(new SimpleMeterRegistry());

    assertThrows(
        MaterializedViewNonTransientException.class,
        () ->
            generator.runIndexerOperation(
                () -> {
                  throw new MaterializedViewNonTransientException(
                      "fenced", MaterializedViewNonTransientException.Reason.INVALID_LEASE);
                }));
  }

  @Test
  public void maybeForceMergeAfterInitialSync_isNoOp() throws Exception {
    // Force-merge is a query-perf knob for the queryable auto-embed index, which is built by a
    // separate, non-MV ReplicationIndexManager reading from this generator's MV collection --
    // not this generator. Must not call commit() here even when the perf flag is set: unlike the
    // other call sites, nothing detects a fencing rejection reaching this specific one.
    System.setProperty("mongot.perf.forceMergeMaxNumSegments", "1");
    try {
      DocumentIndexer documentIndexer = mock(DocumentIndexer.class);
      MaterializedViewGenerator generator =
          MaterializedViewGenerator.create(
              this.executorService,
              mock(MongotCursorManager.class),
              mock(InitialSyncQueue.class),
              mock(SteadyStateManager.class),
              mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId())),
              mock(InitializedMaterializedViewIndex.class),
              documentIndexer,
              mock(PeriodicIndexCommitter.class),
              Duration.ofSeconds(30),
              Duration.ofSeconds(30),
              Duration.ofSeconds(1),
              new SimpleMeterRegistry(),
              mock(FeatureFlags.class),
              false,
              () -> {});

      generator.maybeForceMergeAfterInitialSync();

      verify(documentIndexer, never()).commit();
    } finally {
      System.clearProperty("mongot.perf.forceMergeMaxNumSegments");
    }
  }

  @Test
  public void handleCollectionDropped_cleansUpSharedResourcesBeforeTerminalTransition() {
    java.util.concurrent.atomic.AtomicReference<IndexManager.State> stateAtCleanup =
        new java.util.concurrent.atomic.AtomicReference<>();
    MaterializedViewGenerator[] holder = new MaterializedViewGenerator[1];
    // The cleanup callback must run before the generator turns terminal: a terminal state is
    // what makes the heartbeat release the lease, and the cleanup is only safe while this
    // generator still holds it.
    holder[0] = createGeneratorWithDropCallback(() -> stateAtCleanup.set(holder[0].getState()));

    holder[0].handleCollectionDropped();

    assertNotNull("cleanup callback never ran", stateAtCleanup.get());
    assertNotEquals(IndexManager.State.SHUT_DOWN, stateAtCleanup.get());
    assertEquals(IndexManager.State.SHUT_DOWN, holder[0].getState());
  }

  @Test
  public void handleCollectionDropped_callbackThrows_stillTearsDown() {
    MaterializedViewGenerator generator =
        createGeneratorWithDropCallback(
            () -> {
              throw new RuntimeException("cleanup failed");
            });

    generator.handleCollectionDropped();

    assertEquals(IndexManager.State.SHUT_DOWN, generator.getState());
  }

  private MaterializedViewGenerator createGeneratorWithDropCallback(
      Runnable onSourceCollectionDropped) {
    MaterializedViewIndexGeneration indexGeneration =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    return new MaterializedViewGenerator(
        this.executorService,
        mock(MongotCursorManager.class),
        mock(InitialSyncQueue.class),
        mock(SteadyStateManager.class),
        indexGeneration,
        mock(InitializedMaterializedViewIndex.class),
        mock(DocumentIndexer.class),
        mock(PeriodicIndexCommitter.class),
        new com.xgen.mongot.metrics.MetricsFactory(
            "materializedViewGenerator", new SimpleMeterRegistry()),
        mock(FeatureFlags.class),
        Duration.ofSeconds(30),
        Duration.ofSeconds(30),
        Duration.ofSeconds(1),
        false,
        onSourceCollectionDropped);
  }

  private MaterializedViewGenerator createGeneratorForMetricsTest(SimpleMeterRegistry registry) {
    MaterializedViewIndexGeneration indexGeneration =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    com.xgen.mongot.metrics.MetricsFactory metricsFactory =
        new com.xgen.mongot.metrics.MetricsFactory("materializedViewGenerator", registry);

    // Use a subclass that stubs enqueueInitialSync to prevent async NPEs on background threads.
    return new MaterializedViewGenerator(
        this.executorService,
        mock(MongotCursorManager.class),
        mock(InitialSyncQueue.class),
        mock(SteadyStateManager.class),
        indexGeneration,
        mock(InitializedMaterializedViewIndex.class),
        mock(DocumentIndexer.class),
        mock(PeriodicIndexCommitter.class),
        metricsFactory,
        mock(FeatureFlags.class),
        Duration.ofSeconds(30),
        Duration.ofSeconds(30),
        Duration.ofSeconds(1),
        false,
        () -> {}) {
      @Override
      protected synchronized void enqueueInitialSync(
          com.xgen.mongot.index.status.IndexStatus status) {
        // no-op: prevent async cascade in tests
      }
    };
  }

  private MaterializedViewGenerator createGenerator() {
    return createGeneratorWithIndex(
        mock(InitializedMaterializedViewIndex.class), new SimpleMeterRegistry());
  }

  private MaterializedViewGenerator createGeneratorWithIndex(
      InitializedMaterializedViewIndex matViewIndex) {
    return createGeneratorWithIndex(matViewIndex, new SimpleMeterRegistry());
  }

  private MaterializedViewGenerator createGeneratorWithIndex(
      InitializedMaterializedViewIndex matViewIndex, SimpleMeterRegistry registry) {
    MaterializedViewIndexGeneration indexGeneration =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));

    return MaterializedViewGenerator.create(
        this.executorService,
        mock(MongotCursorManager.class),
        mock(InitialSyncQueue.class),
        mock(SteadyStateManager.class),
        indexGeneration,
        matViewIndex,
        mock(DocumentIndexer.class),
        mock(PeriodicIndexCommitter.class),
        Duration.ofSeconds(30),
        Duration.ofSeconds(30),
        Duration.ofSeconds(1),
        registry,
        mock(FeatureFlags.class),
        false,
        () -> {});
  }
}
