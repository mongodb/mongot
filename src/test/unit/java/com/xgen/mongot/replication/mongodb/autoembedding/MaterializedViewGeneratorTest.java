package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.testing.mongot.metrics.SimpleMetricsFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link MaterializedViewGenerator}'s role-switching functionality.
 *
 * <p>These tests verify the behavior of the leader/follower role management methods: {@link
 * MaterializedViewGenerator#isLeader()}, {@link MaterializedViewGenerator#becomeLeader()}, and
 * {@link MaterializedViewGenerator#becomeFollower()}.
 *
 * <p>All generators are created as followers. Call {@link MaterializedViewGenerator#becomeLeader()}
 * to activate leader mode.
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
  public void becomeFollower_whenLeader_setsIsLeaderFalse() {
    MaterializedViewGenerator generator = createGenerator();
    generator.becomeLeader();
    assertTrue(generator.isLeader());

    generator.becomeFollower();

    assertFalse(generator.isLeader());
  }

  @Test
  public void becomeFollower_whenAlreadyFollower_remainsFollower() {
    MaterializedViewGenerator generator = createGenerator();
    assertFalse(generator.isLeader());

    generator.becomeFollower();

    assertFalse(generator.isLeader());
  }

  @Test
  public void roleTransition_leaderToFollowerToLeader_transitionsCorrectly() {
    MaterializedViewGenerator generator = createGenerator();
    generator.becomeLeader();
    assertTrue(generator.isLeader());

    generator.becomeFollower();
    assertFalse(generator.isLeader());

    generator.becomeLeader();
    assertTrue(generator.isLeader());
  }

  @Test
  public void roleTransition_followerToLeaderToFollower_transitionsCorrectly() {
    MaterializedViewGenerator generator = createGenerator();
    assertFalse(generator.isLeader());

    generator.becomeLeader();
    assertTrue(generator.isLeader());

    generator.becomeFollower();
    assertFalse(generator.isLeader());
  }

  private MaterializedViewGenerator createGenerator() {
    MaterializedViewIndexGeneration indexGeneration =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));

    return new MaterializedViewGenerator(
        this.executorService,
        mock(MongotCursorManager.class),
        mock(InitialSyncQueue.class),
        mock(SteadyStateManager.class),
        indexGeneration,
        mock(InitializedIndex.class),
        mock(DocumentIndexer.class),
        mock(PeriodicIndexCommitter.class),
        new SimpleMetricsFactory(),
        mock(FeatureFlags.class),
        Duration.ofSeconds(1),
        Duration.ofSeconds(1),
        Duration.ofSeconds(1),
        false);
  }
}

