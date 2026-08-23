package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.mongodb.MongoException;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.replication.mongodb.IndexManager;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
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
