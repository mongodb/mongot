package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.SessionRefresher.mockSessionRefresher;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.errorprone.annotations.Keep;
import com.mongodb.ConnectionString;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.replication.mongodb.common.ClientSessionRecord;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import com.xgen.testing.mongot.metrics.SimpleMetricsFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

public class MaterializedViewManagerTest {
  private static final ObjectId INDEX_ID = new ObjectId();
  private static final MaterializedViewIndexDefinitionGeneration MOCK_INDEX_DEFINITION_GENERATION =
      mockMatViewDefinitionGeneration(INDEX_ID);
  private static final MaterializedViewGenerationId MOCK_MAT_VIEW_GENERATION_ID =
      MOCK_INDEX_DEFINITION_GENERATION.getGenerationId();
  private static final GenerationId MOCK_GENERATION_ID =
      new GenerationId(INDEX_ID, Generation.CURRENT);

  @Test
  public void testAddIndex() throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(materializedViewindexGeneration);
    mocks.addIndexForReplication(materializedViewindexGeneration);

    verify(mocks.leaseManager).add(materializedViewindexGeneration);
  }

  @Test
  public void testUpdateExistingIndexNewDefinitionVersion() throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var materializedViewindexGeneration =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    MaterializedViewGenerator materializedViewGenerator =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration);
    mocks.addIndexForReplication(materializedViewindexGeneration);

    // Add a new version of the same index.
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var newIndexGeneration = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2, 1));
    mocks.mockMaterializedViewGenerator(newIndexGeneration);
    mocks.addIndexForReplication(newIndexGeneration);

    // Verify that the old MaterializedViewGenerator was shut down and the new one was added to the
    // lease manager.
    verify(materializedViewGenerator).shutdown();
    verify(mocks.leaseManager).add(materializedViewindexGeneration);
    verify(mocks.leaseManager).add(newIndexGeneration);
  }

  @Test
  public void testUpdateExistingIndexSameDefinitionVersion() throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var materializedViewindexGeneration =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    mocks.mockMaterializedViewGenerator(materializedViewindexGeneration);
    mocks.addIndexForReplication(materializedViewindexGeneration);
    verify(mocks.materializedViewGeneratorFactory).create(any());

    // Add a new version of the same index.
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var newIndexGeneration = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2));
    mocks.mockMaterializedViewGenerator(newIndexGeneration);
    Mockito.clearInvocations(mocks.materializedViewGeneratorFactory);
    mocks.addIndexForReplication(newIndexGeneration);

    // Verify that only one MaterializedViewGenerator was created.
    verify(mocks.materializedViewGeneratorFactory, never()).create(any());
    // Verify that both index generations point to the same index.
    assertEquals(materializedViewindexGeneration.getIndex(), newIndexGeneration.getIndex());
  }

  @Test
  public void testDropIndex() throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator materializedViewGenerator =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration);
    mocks.addIndexForReplication(materializedViewindexGeneration);

    // Drop it, ensure that MaterializedViewGenerator::drop was called.
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);
    verify(materializedViewGenerator).shutdown();
    Mockito.clearInvocations(materializedViewGenerator);
    // Drop again, verify does not get dropped again.
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);
    verify(materializedViewGenerator, never()).shutdown();
  }

  @Test
  public void testSameIndexCanNotBeAddedTwice() {
    Mocks mocks = Mocks.create();
    var index = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(index);
    mocks.addIndexForReplication(index);
    verify(mocks.materializedViewGeneratorFactory).create(any());
    Mockito.clearInvocations(mocks.materializedViewGeneratorFactory);
    mocks.addIndexForReplication(index);
    verify(mocks.materializedViewGeneratorFactory, never()).create(any());
  }

  @Test
  public void testCanNotInteractAfterShutDown() {
    Mocks mocks = Mocks.create();

    var index = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    mocks.mockMaterializedViewGenerator(index);

    mocks.addIndexForReplication(index);
    mocks.manager.shutdown();

    // can not add a new index or cancel existing
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            mocks.addIndexForReplication(
                mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION)));
    Assert.assertThrows(
        RuntimeException.class, () -> mocks.manager.dropIndex(index.getGenerationId()));
  }

  @Test
  public void testShutdownNoIndexes() throws Exception {
    Mocks mocks = Mocks.create();

    mocks.manager.shutdown().get(5, TimeUnit.SECONDS);

    verify(mocks.initialSyncQueue).shutdown();
    verify(mocks.initialSyncQueue).shutdown();
    verify(mocks.steadyStateManager).shutdown();
    verify(mocks.executorService).shutdown();
    verify(mocks.clientSessionRecordMap.get("test").sessionRefresher()).shutdown();
  }

  @Test
  public void testShutdownWithIndexes() throws Exception {
    Mocks mocks = Mocks.create();

    MaterializedViewIndexGeneration materializedViewindexGeneration1 =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    // Add two different indexes to the AutoEmbeddingMaterializedViewManager.
    MaterializedViewGenerator materializedViewGenerator1 =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration1);
    mocks.addIndexForReplication(materializedViewindexGeneration1);

    MaterializedViewIndexGeneration materializedViewindexGeneration2 =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator materializedViewGenerator2 =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration2);
    mocks.addIndexForReplication(materializedViewindexGeneration2);

    // Ensure that shutdown() shuts down both of the MaterializedViewGenerator.
    mocks.manager.shutdown().get(5, TimeUnit.SECONDS);
    verify(materializedViewGenerator1).shutdown();
    verify(materializedViewGenerator2).shutdown();
    verify(mocks.initialSyncQueue).shutdown();
    verify(mocks.steadyStateManager).shutdown();
    verify(mocks.commitExecutor).shutdown();
    verify(mocks.executorService).shutdown();
    verify(mocks.clientSessionRecordMap.get("test").sessionRefresher()).shutdown();
  }

  @Test
  public void testShutdownTwoIndexesWithSameIdShutsBothDown() throws Exception {
    Mocks mocks = Mocks.create();
    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var index1 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    var index2 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2, 1));
    assertEquals(gen1.indexId, gen2.indexId);

    // Add two different indexes to the AutoEmbeddingMaterializedViewManager.
    MaterializedViewGenerator materializedViewGenerator1 =
        mocks.mockMaterializedViewGenerator(index1);
    MaterializedViewGenerator materializedViewGenerator2 =
        mocks.mockMaterializedViewGenerator(index2);

    mocks.addIndexForReplication(index1);
    mocks.addIndexForReplication(index2);

    // Ensure that shutdown() shuts down both of the MaterializedViewGenerator.
    mocks.manager.shutdown().get(5, TimeUnit.SECONDS);
    verify(materializedViewGenerator1).shutdown();
    verify(materializedViewGenerator2).shutdown();
  }

  @Test
  // This test verifies that task shutdown and metrics deregistration work together correctly.
  //
  // Multiple indexes are actively replicating and each has its own manager with background tasks.
  // When mongot shuts down, all these components must shut down gracefully.
  public void testExecutorShutdown() throws Exception {
    Mocks mocks = Mocks.create();

    MaterializedViewIndexGeneration materializedViewindexGeneration1 =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator materializedViewGenerator1 =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration1);
    mocks.addIndexForReplication(materializedViewindexGeneration1);

    MaterializedViewIndexGeneration materializedViewindexGeneration2 =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator materializedViewGenerator2 =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration2);
    mocks.addIndexForReplication(materializedViewindexGeneration2);

    MaterializedViewIndexGeneration materializedViewindexGeneration3 =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator materializedViewGenerator3 =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration3);
    mocks.addIndexForReplication(materializedViewindexGeneration3);

    // Trigger shutdown. ExecutorService could shut down before the shutdown tasks complete,
    // causing metrics to be deregistered while tasks are still running. So, wait for the shutdown
    // future to complete and expect it to complete successfully, i.e., no exceptions.
    CompletableFuture<Void> shutdownFuture = mocks.manager.shutdown();
    shutdownFuture.get(5, TimeUnit.SECONDS);

    // Verify all index managers were properly shut down
    verify(materializedViewGenerator1).shutdown();
    for (MaterializedViewGenerator materializedViewGenerator :
        Arrays.asList(materializedViewGenerator2, materializedViewGenerator3)) {
      verify(materializedViewGenerator).shutdown();
    }

    // Verify all components were shut down
    verify(mocks.initialSyncQueue).shutdown();
    verify(mocks.steadyStateManager).shutdown();
    verify(mocks.commitExecutor).shutdown();
    verify(mocks.executorService).shutdown();
    verify(mocks.clientSessionRecordMap.get("test").sessionRefresher()).shutdown();
  }

  @Test
  public void testAutoEmbeddingManagerLaterInitialized() {
    Mocks mocks = Mocks.create();
    CompletableFuture<Void> future = new CompletableFuture<>();
    mocks.addMockMaterializedViewGenerator(future);
    assertFalse(mocks.manager.isInitialized());
    future.complete(null);
    assertTrue(mocks.manager.isInitialized());
  }

  @Test
  public void testGetAutoEmbeddingInitStateNoIndexes() {
    Mocks mocks = Mocks.create();
    assertTrue(mocks.manager.isInitialized());
  }

  @Test
  public void testGetAutoEmbeddingInitStateAllInitialized() {
    Mocks mocks = Mocks.create();
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    assertTrue(mocks.manager.isInitialized());
  }

  @Test
  public void testGetAutoEmbeddingInitStateSomeNotInitialized() {
    Mocks mocks = Mocks.create();
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    mocks.addMockMaterializedViewGenerator(new CompletableFuture<>());
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testGetAutoEmbeddingInitStateSomeNotInitialized2() {
    Mocks mocks = Mocks.create();
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    mocks.addMockMaterializedViewGenerator(new CompletableFuture<>());
    mocks.addMockMaterializedViewGenerator(new CompletableFuture<>());
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testGetAutoEmbeddingInitStateSomeCompleteSomeFailed() {
    Mocks mocks = Mocks.create();
    mocks.addMockMaterializedViewGenerator(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    mocks.addMockMaterializedViewGenerator(COMPLETED_FUTURE);
    mocks.addMockMaterializedViewGenerator(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testGetAutoEmbeddingInitStateAllFailed() {
    Mocks mocks = Mocks.create();
    mocks.addMockMaterializedViewGenerator(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    mocks.addMockMaterializedViewGenerator(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    mocks.addMockMaterializedViewGenerator(
        CompletableFuture.failedFuture(new RuntimeException("Init future failed")));
    assertFalse(mocks.manager.isInitialized());
  }

  @Test
  public void testHeartbeatLogIsEmitted() throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(MaterializedViewManager.class);
    List<ILoggingEvent> logEvents = TestUtils.getLogEvents(logger);

    Mocks mocks = Mocks.create();

    // Verify the startup log is emitted
    boolean foundStartupLog =
        new ArrayList<>(logEvents).stream()
            .anyMatch(
                event ->
                    event.getFormattedMessage().contains("Starting auto-embedding heartbeat"));
    assertTrue("Expected heartbeat startup log to be emitted", foundStartupLog);

    // Capture the scheduled heartbeat task and execute it to verify the actual heartbeat log
    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(
            runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

    // Execute the captured heartbeat task
    runnableCaptor.getValue().run();

    // Verify the actual heartbeat log is emitted
    boolean foundHeartbeatLog =
        new ArrayList<>(logEvents).stream()
            .anyMatch(
                event -> event.getFormattedMessage().equals("Auto-embedding leader heartbeat"));
    assertTrue("Expected heartbeat log to be emitted", foundHeartbeatLog);
  }

  @Test
  public void testAddIndex_emitsCreatingGeneratorLog_leaderMode() throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(MaterializedViewManager.class);
    List<ILoggingEvent> logEvents = TestUtils.getLogEvents(logger);

    Mocks mocks = Mocks.create();

    // Add an index in leader mode
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(materializedViewindexGeneration);
    mocks.addIndexForReplication(materializedViewindexGeneration);

    // Verify the log message is emitted with leader mode = true
    boolean foundLog =
        logEvents.stream()
            .anyMatch(
                event ->
                    event.getFormattedMessage().contains("Creating auto-embedding generator")
                        && event.getFormattedMessage().contains("leader mode = true"));
    assertTrue("Expected 'Creating auto-embedding generator (leader mode = true)' log", foundLog);
  }

  @Test
  public void testAddIndex_emitsCreatingGeneratorLog_followerMode() throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(MaterializedViewManager.class);
    List<ILoggingEvent> logEvents = TestUtils.getLogEvents(logger);

    Mocks mocks = Mocks.createFollower();

    // Add an index in follower mode
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(materializedViewindexGeneration);
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(materializedViewindexGeneration);
    when(autoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(materializedViewindexGeneration.getGenerationId());
    mocks.manager.add(autoEmbeddingIndexGeneration);

    // Verify the log message is emitted with leader mode = false
    boolean foundLog =
        logEvents.stream()
            .anyMatch(
                event ->
                    event.getFormattedMessage().contains("Creating auto-embedding generator")
                        && event.getFormattedMessage().contains("leader mode = false"));
    assertTrue("Expected 'Creating auto-embedding generator (leader mode = false)' log", foundLog);
  }

  // ==================== Follower Mode Tests ====================

  @Test
  public void followerMode_addIndex_tracksIndexGeneration() {
    Mocks mocks = Mocks.createFollower();
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(materializedViewindexGeneration);
    when(autoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(materializedViewindexGeneration.getGenerationId());
    mocks.manager.add(autoEmbeddingIndexGeneration);
    mocks.runnableCaptor.orElseThrow().getValue().run();
    verify(materializedViewindexGeneration.getIndex()).setStatus(any(IndexStatus.class));
    assertEquals(mocks.expectedStatus, materializedViewindexGeneration.getIndex().getStatus());
  }

  @Test
  public void followerMode_addIndexNewDefinitionVersion_replacesGenerator() {
    // In the unified implementation, when a new definition version is added,
    // the old generator is shut down and replaced with a new one.
    // Only the new generator's status is updated.
    Mocks mocks = Mocks.createFollower();
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(materializedViewindexGeneration);
    when(autoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(materializedViewindexGeneration.getGenerationId());
    mocks.manager.add(autoEmbeddingIndexGeneration);

    MaterializedViewIndexGeneration newMaterializedViewindexGeneration =
        mockMatViewIndexGeneration(
            mockMatViewDefinitionGeneration(materializedViewindexGeneration.getGenerationId(), 1));
    AutoEmbeddingIndexGeneration newAutoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(newAutoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(newMaterializedViewindexGeneration);
    when(newAutoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(
            newMaterializedViewindexGeneration
                .getDefinitionGeneration()
                .incrementUser()
                .getGenerationId());
    mocks.manager.add(newAutoEmbeddingIndexGeneration);

    mocks.runnableCaptor.orElseThrow().getValue().run();
    // Only the new generation's status is updated (old generator was replaced)
    verify(newMaterializedViewindexGeneration.getIndex()).setStatus(any(IndexStatus.class));
    assertEquals(mocks.expectedStatus, newMaterializedViewindexGeneration.getIndex().getStatus());
  }

  @Test
  public void followerMode_dropIndex_removesIndexGeneration() {
    Mocks mocks = Mocks.createFollower();
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(materializedViewindexGeneration);
    when(autoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(materializedViewindexGeneration.getGenerationId());
    mocks.manager.add(autoEmbeddingIndexGeneration);
    mocks.runnableCaptor.orElseThrow().getValue().run();
    verify(materializedViewindexGeneration.getIndex()).setStatus(any(IndexStatus.class));
    assertEquals(mocks.expectedStatus, materializedViewindexGeneration.getIndex().getStatus());

    mocks.manager.dropIndex(autoEmbeddingIndexGeneration.getGenerationId());
    // Verify no exception is thrown and the manager continues to work
    assertTrue(mocks.manager.isReplicationSupported());
  }

  @Test
  public void followerMode_dropIndexNewDefinitionVersion_removesCorrectGeneration() {
    // In the unified implementation, when a new definition version is added,
    // the old generator is shut down and replaced. Only the new generator's status is updated.
    Mocks mocks = Mocks.createFollower();
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(materializedViewindexGeneration);
    when(autoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(materializedViewindexGeneration.getGenerationId());
    mocks.manager.add(autoEmbeddingIndexGeneration);

    MaterializedViewIndexGeneration newMaterializedViewindexGeneration =
        mockMatViewIndexGeneration(
            mockMatViewDefinitionGeneration(materializedViewindexGeneration.getGenerationId(), 1));
    AutoEmbeddingIndexGeneration newAutoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(newAutoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(newMaterializedViewindexGeneration);
    when(newAutoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(
            newMaterializedViewindexGeneration
                .getDefinitionGeneration()
                .incrementUser()
                .getGenerationId());
    mocks.manager.add(newAutoEmbeddingIndexGeneration);

    mocks.runnableCaptor.orElseThrow().getValue().run();
    // Only the new generation's status is updated (old generator was replaced)
    verify(newMaterializedViewindexGeneration.getIndex()).setStatus(any(IndexStatus.class));
    assertEquals(mocks.expectedStatus, newMaterializedViewindexGeneration.getIndex().getStatus());

    mocks.manager.dropIndex(autoEmbeddingIndexGeneration.getGenerationId());
    mocks.manager.dropIndex(newAutoEmbeddingIndexGeneration.getGenerationId());
    // Verify no exception is thrown
    assertTrue(mocks.manager.isReplicationSupported());
  }

  @Test
  public void followerMode_isReplicationSupported_returnsTrue() {
    // With index-level leader election, every instance supports replication
    // (it can be a leader for some indexes)
    Mocks mocks = Mocks.createFollower();
    assertTrue(mocks.manager.isReplicationSupported());
  }

  @Test
  public void followerMode_isInitialized_returnsTrue() {
    Mocks mocks = Mocks.createFollower();
    assertTrue(mocks.manager.isInitialized());
  }

  static class Mocks {
    final NamedExecutorService executorService;
    @Keep final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;
    @Keep final MongotCursorManager cursorManager;
    final Map<String, ClientSessionRecord> clientSessionRecordMap;
    final InitialSyncQueue initialSyncQueue;
    final SteadyStateManager steadyStateManager;
    final MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory;
    final NamedScheduledExecutorService commitExecutor;
    final NamedScheduledExecutorService heartbeatExecutor;
    final Supplier<MaterializedViewManager> managerSupplier;
    final DecodingWorkScheduler decodingScheduler;
    final MeterRegistry meterRegistry;
    final LeaseManager leaseManager;
    final NamedScheduledExecutorService statusRefreshExecutor;
    final NamedScheduledExecutorService optimeUpdaterExecutor;
    final IndexStatus expectedStatus; // For follower mode tests
    final Optional<ArgumentCaptor<Runnable>> runnableCaptor; // For follower mode tests

    MaterializedViewManager manager;

    private Mocks(
        NamedExecutorService executorService,
        IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
        DecodingWorkScheduler decodingScheduler,
        MongotCursorManager cursorManager,
        Map<String, ClientSessionRecord> clientSessionRecordMap,
        InitialSyncQueue initialSyncQueue,
        SteadyStateManager steadyStateManager,
        BatchMongoClient syncBatchMongoClient,
        MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory,
        NamedScheduledExecutorService commitExecutor,
        NamedScheduledExecutorService heartbeatExecutor,
        NamedScheduledExecutorService statusRefreshExecutor,
        NamedScheduledExecutorService optimeUpdaterExecutor,
        LeaseManager leaseManager,
        IndexStatus expectedStatus,
        Optional<ArgumentCaptor<Runnable>> runnableCaptor) {
      this.executorService = executorService;
      this.indexingWorkSchedulerFactory = indexingWorkSchedulerFactory;
      this.cursorManager = cursorManager;
      this.clientSessionRecordMap = clientSessionRecordMap;
      this.initialSyncQueue = initialSyncQueue;
      this.steadyStateManager = steadyStateManager;
      this.materializedViewGeneratorFactory = materializedViewGeneratorFactory;
      this.commitExecutor = commitExecutor;
      this.heartbeatExecutor = heartbeatExecutor;
      this.statusRefreshExecutor = statusRefreshExecutor;
      this.optimeUpdaterExecutor = optimeUpdaterExecutor;
      this.decodingScheduler = decodingScheduler;
      this.meterRegistry = new SimpleMeterRegistry();
      this.expectedStatus = expectedStatus;
      this.runnableCaptor = runnableCaptor;
      this.leaseManager = leaseManager;

      SyncSourceConfig syncSourceConfig =
          new SyncSourceConfig(
              new ConnectionString("mongodb://newString"),
              Optional.empty(),
              new ConnectionString("mongodb://newString"));

      this.managerSupplier =
          () ->
              new MaterializedViewManager(
                  executorService,
                  indexingWorkSchedulerFactory,
                  clientSessionRecordMap,
                  syncSourceConfig,
                  initialSyncQueue,
                  steadyStateManager,
                  syncBatchMongoClient,
                  decodingScheduler,
                  materializedViewGeneratorFactory,
                  commitExecutor,
                  heartbeatExecutor,
                  statusRefreshExecutor,
                  optimeUpdaterExecutor,
                  this.meterRegistry,
                  leaseManager);
      this.manager = this.managerSupplier.get();
    }

    public void recreateManager() {
      this.manager = this.managerSupplier.get();
    }

    private static Mocks create() {
      NamedExecutorService executorService =
          spy(Executors.fixedSizeThreadPool("indexing", 1, new SimpleMeterRegistry()));

      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
          mock(IndexingWorkSchedulerFactory.class);

      DecodingWorkScheduler decodingWorkScheduler = mock(DecodingWorkScheduler.class);

      MongotCursorManager cursorManager = mock(MongotCursorManager.class);
      SessionRefresher sessionRefresher = mockSessionRefresher();

      InitialSyncQueue initialSyncQueue = mock(InitialSyncQueue.class);
      when(initialSyncQueue.shutdown()).thenReturn(COMPLETED_FUTURE);

      SteadyStateManager steadyStateManager = mock(SteadyStateManager.class);
      when(steadyStateManager.shutdown()).thenReturn(COMPLETED_FUTURE);

      com.mongodb.client.MongoClient syncMongoClient = mock(com.mongodb.client.MongoClient.class);

      BatchMongoClient syncBatchMongoClient = mock(BatchMongoClient.class);

      MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory =
          mock(MaterializedViewManager.MaterializedViewGeneratorFactory.class);

      NamedScheduledExecutorService commitExecutor =
          spy(
              Executors.fixedSizeThreadScheduledExecutor(
                  "index-commit", 1, new SimpleMeterRegistry()));

      NamedScheduledExecutorService heartbeatExecutor =
          spy(
              Executors.singleThreadScheduledExecutor(
                  "mat-view-leader-heartbeat", new SimpleMeterRegistry()));

      NamedScheduledExecutorService statusRefreshExecutor =
          spy(
              Executors.fixedSizeThreadScheduledExecutor(
                  "mat-view-status-refresh", 1, new SimpleMeterRegistry()));

      NamedScheduledExecutorService optimeUpdaterExecutor =
          spy(
              Executors.singleThreadScheduledExecutor(
                  "mat-view-optime-updater", new SimpleMeterRegistry()));

      InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
      when(initializedIndexCatalog.getIndex(any()))
          .thenReturn(Optional.of(mock(InitializedIndex.class)));

      LeaseManager leaseManager = mock(LeaseManager.class);
      when(leaseManager.drop(any())).thenReturn(COMPLETED_FUTURE);
      when(leaseManager.isLeader(any())).thenReturn(true);
      // Mock getLeaderGenerationIds to return a non-empty set for heartbeat test
      GenerationId leaderGenId = GenerationIdBuilder.create();
      when(leaseManager.getLeaderGenerationIds()).thenReturn(Set.of(leaderGenId));

      return new Mocks(
          executorService,
          indexingWorkSchedulerFactory,
          decodingWorkScheduler,
          cursorManager,
          Map.of("test", new ClientSessionRecord(syncMongoClient, sessionRefresher)),
          initialSyncQueue,
          steadyStateManager,
          syncBatchMongoClient,
          materializedViewGeneratorFactory,
          commitExecutor,
          heartbeatExecutor,
          statusRefreshExecutor,
          optimeUpdaterExecutor,
          leaseManager,
          IndexStatus.unknown(), // expectedStatus for leader mode
          Optional.empty());
    }

    private static Mocks createFollower() {
      // Separate mock schedulers for status refresh and optime updater
      NamedScheduledExecutorService statusRefreshScheduler =
          mock(NamedScheduledExecutorService.class);
      NamedScheduledExecutorService optimeUpdaterScheduler =
          mock(NamedScheduledExecutorService.class);
      ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
      LeaseManager mockLeaseManager = mock(LeaseManager.class);
      // Mock isLeader to return false for follower mode
      when(mockLeaseManager.isLeader(any())).thenReturn(false);
      // Track added generation IDs to return from getFollowerGenerationIds
      Set<GenerationId> addedGenerationIds = ConcurrentHashMap.newKeySet();
      doAnswer(
              invocation -> {
                IndexGeneration indexGen = invocation.getArgument(0);
                addedGenerationIds.add(indexGen.getGenerationId());
                return null;
              })
          .when(mockLeaseManager)
          .add(any());
      // Mock getFollowerGenerationIds to return all added generation IDs (since all are followers)
      when(mockLeaseManager.getFollowerGenerationIds())
              .thenAnswer(invocation -> addedGenerationIds);
      // Mock pollFollowerStatuses to return steady status for all added generation IDs
      when(mockLeaseManager.pollFollowerStatuses())
          .thenAnswer(
              invocation -> {
                Map<GenerationId, IndexStatus> result = new HashMap<>();
                for (GenerationId genId : addedGenerationIds) {
                  result.put(genId, IndexStatus.steady());
                }
                return result;
              });
      // Mock getLeaderGenerationIds to return empty set (no leaders in follower mode)
      when(mockLeaseManager.getLeaderGenerationIds()).thenReturn(Set.of());
      ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);

      // Capture the status refresh runnable
      doReturn(mockFuture)
          .when(statusRefreshScheduler)
          .scheduleWithFixedDelay(runnableCaptor.capture(), anyLong(), anyLong(), any());
      // Mock the optime updater scheduler (don't need to capture its runnable)
      doReturn(mockFuture)
          .when(optimeUpdaterScheduler)
          .scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());

      // Create a mock generator factory that returns a mock generator for any input
      MaterializedViewManager.MaterializedViewGeneratorFactory mockGeneratorFactory =
          mock(MaterializedViewManager.MaterializedViewGeneratorFactory.class);
      when(mockGeneratorFactory.create(any()))
          .thenAnswer(
              invocation -> {
                MaterializedViewIndexGeneration matViewIndexGen = invocation.getArgument(0);
                MaterializedViewGenerator mockGenerator = mock(MaterializedViewGenerator.class);
                when(mockGenerator.getIndexGeneration()).thenReturn(matViewIndexGen);
                when(mockGenerator.shutdown())
                    .thenReturn(CompletableFuture.completedFuture(null));
                return mockGenerator;
              });

      // Create minimal mocks for follower mode (many leader fields unused)
      return new Mocks(
          mock(NamedExecutorService.class),
          mock(IndexingWorkSchedulerFactory.class),
          mock(DecodingWorkScheduler.class),
          mock(MongotCursorManager.class),
          Map.of(),
          mock(InitialSyncQueue.class),
          mock(SteadyStateManager.class),
          mock(BatchMongoClient.class),
          mockGeneratorFactory,
          mock(NamedScheduledExecutorService.class),
          mock(NamedScheduledExecutorService.class),
          statusRefreshScheduler, // statusRefreshExecutor - captures runnable
          optimeUpdaterScheduler, // optimeUpdaterExecutor - separate mock
          mockLeaseManager,
          IndexStatus.steady(), // expectedStatus for follower mode
          Optional.of(runnableCaptor));
    }

    private MaterializedViewGenerator mockMaterializedViewGenerator(
        MaterializedViewIndexGeneration materializedViewIndexGeneration) {
      var materializedViewGenerator =
          spy(
              new MaterializedViewGenerator(
                  this.executorService,
                  this.cursorManager,
                  this.initialSyncQueue,
                  this.steadyStateManager,
                  materializedViewIndexGeneration,
                  mock(InitializedIndex.class),
                  mock(DocumentIndexer.class),
                  mock(PeriodicIndexCommitter.class),
                  new SimpleMetricsFactory(),
                  mock(FeatureFlags.class),
                  Duration.ofSeconds(1),
                  Duration.ofSeconds(1),
                  Duration.ofSeconds(1),
                  false));
      // Mock shutdown() to return a completed future so thenRun() callbacks execute immediately
      doReturn(CompletableFuture.completedFuture(null)).when(materializedViewGenerator).shutdown();
      when(this.materializedViewGeneratorFactory.create(eq(materializedViewIndexGeneration)))
          .thenReturn(materializedViewGenerator);
      return materializedViewGenerator;
    }

    private void addMockMaterializedViewGenerator(CompletableFuture<Void> initFuture) {
      var mockMatViewIndexGeneration =
          mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
      when(mockMaterializedViewGenerator(mockMatViewIndexGeneration).getInitFuture())
          .thenReturn(initFuture);
      addIndexForReplication(mockMatViewIndexGeneration);
    }

    private void addIndexForReplication(
        MaterializedViewIndexGeneration materializedViewindexGeneration) {
      AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
          mock(AutoEmbeddingIndexGeneration.class);
      when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
          .thenReturn(materializedViewindexGeneration);
      when(autoEmbeddingIndexGeneration.getGenerationId())
          .thenReturn(
              new GenerationId(
                  materializedViewindexGeneration.getGenerationId().indexId,
                  new Generation(
                      materializedViewindexGeneration.getGenerationId().generation.userIndexVersion,
                      IndexFormatVersion.CURRENT)));
      this.manager.add(autoEmbeddingIndexGeneration);
    }
  }
}
