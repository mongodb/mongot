package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog.canonicalKey;
import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.SessionRefresher.mockSessionRefresher;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.errorprone.annotations.Keep;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.leasing.DynamicLeaderLeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.StaticLeaderLeaseManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.replication.mongodb.ReplicationIndexManager;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.ClientSessionRecord;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.ConnectionStringUtil;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import com.xgen.testing.mongot.metrics.SimpleMetricsFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
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

    verify(mocks.leaseManager).add(materializedViewindexGeneration, false);
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
    // lease manager. New definition version (only) triggers resync (skipInitialSync=false).
    verify(materializedViewGenerator).shutdown();
    verify(mocks.leaseManager).add(materializedViewindexGeneration, false);
    verify(mocks.leaseManager).add(newIndexGeneration, false);
  }

  @Test
  public void testUpdateExistingIndexSameDefinitionVersion() throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var materializedViewindexGeneration =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    MaterializedViewGenerator oldGenerator =
        mocks.mockMaterializedViewGenerator(materializedViewindexGeneration);
    mocks.addIndexForReplication(materializedViewindexGeneration);
    verify(mocks.materializedViewGeneratorFactory).create(any());

    // Add a new generation of the same index with the same definition version.
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var newIndexGeneration = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2));
    mocks.mockMaterializedViewGenerator(newIndexGeneration);
    Mockito.clearInvocations(mocks.materializedViewGeneratorFactory);
    mocks.addIndexForReplication(newIndexGeneration);

    // Same definition version now triggers replaceGenerator: the old generator is shut down
    // and a new one is created. skipInitialSync=true preserves existing commit info so the
    // new generator resumes steady-state replication from the last checkpoint.
    verify(oldGenerator).shutdown();
    verify(mocks.materializedViewGeneratorFactory).create(any());
    verify(mocks.leaseManager).add(newIndexGeneration, true);
  }

  /**
   * When the existing generator is in FAILED state and a new index generation arrives with the same
   * definition version, we should create a new generator (the generator is always replaced
   * regardless of state or definition version).
   */
  @Test
  public void testUpdateExistingIndex_failedGenerator_sameVersion_replacesGenerator()
      throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var materializedViewIndexGeneration =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    MaterializedViewGenerator materializedViewGenerator =
        mocks.mockMaterializedViewGenerator(materializedViewIndexGeneration);
    mocks.addIndexForReplication(materializedViewIndexGeneration);

    // Simulate the generator entering FAILED state.
    doReturn(ReplicationIndexManager.State.FAILED).when(materializedViewGenerator).getState();

    // Add a new generation of the same index with the same definition version.
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var newIndexGeneration = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2));
    mocks.mockMaterializedViewGenerator(newIndexGeneration);
    mocks.addIndexForReplication(newIndexGeneration);

    // Verify that the old generator was shut down and a new one was created.
    verify(materializedViewGenerator).shutdown();
  }

  /**
   * When the existing generator is in SHUT_DOWN state and a new index generation arrives with the
   * same definition version, we should create a new generator (the generator is always replaced
   * regardless of state or definition version).
   */
  @Test
  public void testUpdateExistingIndex_shutDownGenerator_sameVersion_replacesGenerator()
      throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var materializedViewIndexGeneration =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    MaterializedViewGenerator materializedViewGenerator =
        mocks.mockMaterializedViewGenerator(materializedViewIndexGeneration);
    mocks.addIndexForReplication(materializedViewIndexGeneration);

    // Simulate the generator entering SHUT_DOWN state.
    doReturn(ReplicationIndexManager.State.SHUT_DOWN).when(materializedViewGenerator).getState();

    // Add a new generation of the same index with the same definition version.
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var newIndexGeneration = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2));
    mocks.mockMaterializedViewGenerator(newIndexGeneration);
    mocks.addIndexForReplication(newIndexGeneration);

    // Verify that the old generator was shut down and a new one was created.
    verify(materializedViewGenerator).shutdown();
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
  public void testDropIndex_removeMetadataCalledAfterLeaseManagerDrop() throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(materializedViewindexGeneration);
    mocks.addIndexForReplication(materializedViewindexGeneration);

    // Drop the index - this triggers onDrop -> leaseManager.drop -> then removeMetadata
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    // Verify ordering: leaseManager.drop() (which untracks the generationId) must complete
    // BEFORE removeMetadata() is called. This ensures the generationId is fully untracked
    // before the metadata is cleaned up.
    InOrder inOrder = Mockito.inOrder(mocks.leaseManager, mocks.metadataCatalog);
    inOrder.verify(mocks.leaseManager).drop(MOCK_MAT_VIEW_GENERATION_ID);
    inOrder
        .verify(mocks.metadataCatalog)
        .removeMetadata(materializedViewindexGeneration.getGenerationId());
  }

  @Test
  public void testSameIndexAddedTwice_replacesGenerator() {
    Mocks mocks = Mocks.create();
    var index = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator oldGenerator = mocks.mockMaterializedViewGenerator(index);
    mocks.addIndexForReplication(index);
    verify(mocks.materializedViewGeneratorFactory).create(any());
    Mockito.clearInvocations(mocks.materializedViewGeneratorFactory);
    mocks.addIndexForReplication(index);
    // Same index added again triggers replaceGenerator. Same generationId means
    // lease drop/add is skipped (already tracked correctly).
    verify(oldGenerator).shutdown();
    verify(mocks.materializedViewGeneratorFactory).create(any());
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

    verify(mocks.executorService).shutdown();
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
    verify(mocks.commitExecutor).shutdown();
    verify(mocks.executorService).shutdown();
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
    verify(mocks.commitExecutor).shutdown();
    verify(mocks.executorService).shutdown();
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
        new ArrayList<>(logEvents)
            .stream()
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
        new ArrayList<>(logEvents)
            .stream()
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
        new ArrayList<>(logEvents)
            .stream()
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
        new ArrayList<>(logEvents)
            .stream()
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
  public void followerMode_refreshStatus_skipsStatusUpdateForLeaderGenerator() {
    // When a generator has transitioned to leader mode, refreshStatus should NOT overwrite
    // its index status with the follower poll result.
    Mocks mocks = Mocks.createFollower();
    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(matViewIndexGen);
    when(autoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(matViewIndexGen.getGenerationId());
    mocks.manager.add(autoEmbeddingIndexGeneration);

    // Simulate the generator transitioning to leader mode.
    var generator = mocks.manager.getMatViewGenerator(matViewIndexGen.getGenerationId());
    assertTrue("Generator should exist", generator.isPresent());
    doReturn(true).when(generator.get()).isLeader();

    // Run the status refresh.
    mocks.runnableCaptor.orElseThrow().getValue().run();

    // setStatus should NOT be called because the generator is a leader.
    verify(matViewIndexGen.getIndex(), never()).setStatus(any(IndexStatus.class));
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

  // ==================== Sync Source Update Lifecycle Tests ====================

  @Test
  public void testAddWhenReplicationDisabled_buffersIndexGeneration() {
    Mocks mocks = Mocks.create();
    mocks.manager.setIsReplicationEnabled(false);

    MaterializedViewIndexGeneration matViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGeneration);
    mocks.addIndexForReplication(matViewIndexGeneration);

    // Generator should NOT be created when replication is disabled
    verify(mocks.materializedViewGeneratorFactory, never()).create(any());
    // LeaseManager.add() should NOT be called (it's inside createNewGenerator)
    verify(mocks.leaseManager, never()).add(any(IndexGeneration.class), anyBoolean());
  }

  @Test
  public void testDropIndex_whenReplicationDisabled_leaderDropsLeaseAndCollection()
      throws Exception {
    // Exercises the generator==null path in maybeDropCollectionAndLease:
    // When replication is disabled, add() records UUID refcounts and
    // latestMatViewIndexGenerationByCollection but does NOT create a generator.
    // On dropIndex, the new code path uses lastMaterializedViewIndexGeneration to drop
    // the lease and the MV collection.
    Mocks mocks = Mocks.create();
    mocks.manager.setIsReplicationEnabled(false);

    MaterializedViewIndexGeneration matViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGeneration);
    mocks.addIndexForReplication(matViewIndexGeneration);

    // Verify no generator was created
    verify(mocks.materializedViewGeneratorFactory, never()).create(any());

    // Verify refcounts are recorded
    UUID uuid = UUID.nameUUIDFromBytes(INDEX_ID.toHexString().getBytes(StandardCharsets.UTF_8));
    assertNotNull(mocks.manager.getActiveGenerationIdCatalog().genIdByMatViewCollection.get(uuid));

    // Construct the GenerationId that DefaultLifecycleManager would pass
    GenerationId dropGenId =
        new GenerationId(
            INDEX_ID,
            new Generation(Generation.CURRENT.userIndexVersion, IndexFormatVersion.CURRENT, 0));

    // Act — drop the index while replication is disabled
    mocks.manager.dropIndex(dropGenId).get(5, TimeUnit.SECONDS);

    // Assert — lease is dropped (generator==null path uses lastMaterializedViewIndexGeneration)
    verify(mocks.leaseManager).dropLease(any(String.class));
    // Assert — leader drops the MV collection via lastMaterializedViewIndexGeneration's writer
    verify(matViewIndexGeneration.getIndex().getWriter()).dropMaterializedViewCollection();
    // Assert — refcounts are cleaned
    assertNull(mocks.manager.getActiveGenerationIdCatalog().genIdByMatViewCollection.get(uuid));
  }

  @Test
  public void testDropIndex_whenReplicationDisabled_followerDropsLeaseOnly() throws Exception {
    // Follower path of the generator==null code path in maybeDropCollectionAndLease:
    // Follower drops the lease from memory but does NOT drop the MV collection.
    Mocks mocks = Mocks.createFollower();
    mocks.manager.setIsReplicationEnabled(false);

    MaterializedViewIndexGeneration matViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.addIndexForReplication(matViewIndexGeneration);

    // Verify no generator was created
    verify(mocks.materializedViewGeneratorFactory, never()).create(any());

    // Construct the GenerationId
    GenerationId dropGenId =
        new GenerationId(
            INDEX_ID,
            new Generation(Generation.CURRENT.userIndexVersion, IndexFormatVersion.CURRENT, 0));

    // Act
    mocks.manager.dropIndex(dropGenId).get(5, TimeUnit.SECONDS);

    // Assert — lease is dropped from memory
    verify(mocks.leaseManager).dropLease(any(String.class));
    // Assert — follower does NOT drop the MV collection (not leader)
    verify(matViewIndexGeneration.getIndex().getWriter(), never()).dropMaterializedViewCollection();
  }

  @Test
  public void testRestartReplication_createsGeneratorsFromBuffer() {
    Mocks mocks = Mocks.create();
    mocks.manager.setIsReplicationEnabled(false);

    MaterializedViewIndexGeneration matViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGeneration);
    mocks.addIndexForReplication(matViewIndexGeneration);

    // No generator created yet
    verify(mocks.materializedViewGeneratorFactory, never()).create(any());

    // Restart replication — generators should now be created from buffer
    mocks.manager.restartReplication();

    verify(mocks.materializedViewGeneratorFactory).create(matViewIndexGeneration);
    verify(mocks.leaseManager).add(matViewIndexGeneration, false);
    assertTrue(mocks.manager.isReplicationSupported());
  }

  @Test
  public void add_incrementsGeneratorCreationFailureCounter_whenCreateFails() {
    Mocks mocks = Mocks.create();

    MaterializedViewIndexGeneration matViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    when(mocks.materializedViewGeneratorFactory.create(any(MaterializedViewIndexGeneration.class)))
        .thenThrow(new RuntimeException("factory failure"));

    Assert.assertThrows(
        RuntimeException.class, () -> mocks.addIndexForReplication(matViewIndexGeneration));

    assertEquals(
        1.0,
        mocks
            .meterRegistry
            .counter(
                MaterializedViewManager.GENERATOR_CREATION_FAILURE_COUNTER_NAME,
                "errorType",
                "CREATE_NEW_GENERATOR",
                "reason",
                "NON_TRANSIENT")
            .count(),
        0.0);
  }

  @Test
  public void restartReplication_incrementsGeneratorCreationFailureCounter_whenCreateFails() {
    Mocks mocks = Mocks.create();
    mocks.manager.setIsReplicationEnabled(false);

    MaterializedViewIndexGeneration matViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGeneration);
    mocks.addIndexForReplication(matViewIndexGeneration);

    verify(mocks.materializedViewGeneratorFactory, never()).create(any());

    when(mocks.materializedViewGeneratorFactory.create(any(MaterializedViewIndexGeneration.class)))
        .thenThrow(new RuntimeException("factory failure"));

    Assert.assertThrows(RuntimeException.class, () -> mocks.manager.restartReplication());

    assertEquals(
        1.0,
        mocks
            .meterRegistry
            .counter(
                MaterializedViewManager.GENERATOR_CREATION_FAILURE_COUNTER_NAME,
                "errorType",
                "CREATE_NEW_GENERATOR",
                "reason",
                "NON_TRANSIENT")
            .count(),
        0.0);
  }

  // Transient failures during restartReplication must not crash mongot. The failing index is
  // skipped and marked STALE; other indexes still get their generators created.
  @Test
  public void restartReplication_skipsIndex_andContinues_whenTransientExceptionThrown() {
    Mocks mocks = Mocks.create();
    mocks.manager.setIsReplicationEnabled(false);

    MaterializedViewIndexDefinitionGeneration failingIndexDefinition =
        mockMatViewDefinitionGeneration(new ObjectId());
    MaterializedViewIndexDefinitionGeneration succeedingIndexDefinition =
        mockMatViewDefinitionGeneration(new ObjectId());
    MaterializedViewIndexGeneration failingGen = mockMatViewIndexGeneration(failingIndexDefinition);
    MaterializedViewIndexGeneration succeedingGen =
        mockMatViewIndexGeneration(succeedingIndexDefinition);

    // Pre-register both — mockMaterializedViewGenerator stubs factory.create() to return mocks.
    mocks.mockMaterializedViewGenerator(failingGen);
    MaterializedViewGenerator succeedingGenerator =
        mocks.mockMaterializedViewGenerator(succeedingGen);
    mocks.addIndexForReplication(failingGen);
    mocks.addIndexForReplication(succeedingGen);

    // Now reconfigure: factory throws transient for the failing index, still returns the mock for
    // the succeeding one. Use thenAnswer so per-arg routing is explicit.
    when(mocks.materializedViewGeneratorFactory.create(any(MaterializedViewIndexGeneration.class)))
        .thenAnswer(
            invocation -> {
              MaterializedViewIndexGeneration arg = invocation.getArgument(0);
              if (arg.getGenerationId().equals(failingGen.getGenerationId())) {
                throw new MaterializedViewTransientException(
                    "simulated 10107 NotPrimary",
                    MaterializedViewTransientException.Reason.READ_LEASE_FAILED);
              }
              return succeedingGenerator;
            });

    // Must NOT throw — restartReplication should swallow MaterializedViewTransientException.
    mocks.manager.restartReplication();

    // Failure recorded with the transient reason so alerts can target this specific case.
    assertEquals(
        1.0,
        mocks
            .meterRegistry
            .counter(
                MaterializedViewManager.GENERATOR_CREATION_FAILURE_COUNTER_NAME,
                "errorType",
                "CREATE_NEW_GENERATOR",
                "reason",
                MaterializedViewTransientException.Reason.READ_LEASE_FAILED.name())
            .count(),
        0.0);

    // The other index still got its generator created.
    verify(mocks.materializedViewGeneratorFactory).create(succeedingGen);
    assertTrue(
        mocks
            .manager
            .getMatViewGenerator(succeedingGen.getGenerationId())
            .isPresent());
    // The failing one is absent — caller (next conf push) is expected to retry.
    assertFalse(
        mocks.manager.getMatViewGenerator(failingGen.getGenerationId()).isPresent());

    // createNewGenerator's leaseManager.add() ran before the throw; the catch must roll it back
    // so refreshStatus() doesn't see a follower with no generator and log a warn every heartbeat.
    verify(mocks.leaseManager).drop(failingGen.getGenerationId());
    verify(mocks.leaseManager, never()).drop(succeedingGen.getGenerationId());
  }

  @Test
  public void testShutdownReplication_clearsGeneratorsAndDisablesReplication() throws Exception {
    Mocks mocks = Mocks.create();

    MaterializedViewIndexGeneration matViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator =
        mocks.mockMaterializedViewGenerator(matViewIndexGeneration);
    mocks.addIndexForReplication(matViewIndexGeneration);

    assertTrue(mocks.manager.isReplicationSupported());

    mocks.manager.shutdownReplication().get(5, TimeUnit.SECONDS);

    verify(generator).shutdown();
    verify(mocks.materializedViewGeneratorFactory).markUninitialized();
    assertFalse(mocks.manager.isReplicationSupported());
  }

  @Test
  public void testShutdownReplication_thenRestartReplication_recreatesAll() throws Exception {
    Mocks mocks = Mocks.create();

    MaterializedViewIndexGeneration matViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator oldGenerator =
        mocks.mockMaterializedViewGenerator(matViewIndexGeneration);
    mocks.addIndexForReplication(matViewIndexGeneration);

    // Shutdown replication
    mocks.manager.shutdownReplication().get(5, TimeUnit.SECONDS);
    verify(oldGenerator).shutdown();
    verify(mocks.materializedViewGeneratorFactory).markUninitialized();
    assertFalse(mocks.manager.isReplicationSupported());

    // Prepare a new generator for restart
    MaterializedViewGenerator newGenerator = mock(MaterializedViewGenerator.class);
    doReturn(CompletableFuture.completedFuture(null)).when(newGenerator).shutdown();
    when(newGenerator.getIndexGeneration()).thenReturn(matViewIndexGeneration);
    when(mocks.materializedViewGeneratorFactory.create(matViewIndexGeneration))
        .thenReturn(newGenerator);

    // Restart replication — generator should be recreated from buffer
    mocks.manager.restartReplication();

    assertTrue(mocks.manager.isReplicationSupported());
    // Factory.create() called once during add(), once during restartReplication()
    verify(mocks.materializedViewGeneratorFactory, times(2)).create(matViewIndexGeneration);
  }

  @Test
  public void testUpdateSyncSource_validUris_returnsTrueAndDelegatesToFactory() {
    Mocks mocks = Mocks.create();
    SyncSourceConfig validConfig =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://host"))
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://host"))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://host"))
            .build();

    assertTrue(mocks.manager.updateSyncSource(validConfig));
    verify(mocks.materializedViewGeneratorFactory).updateSyncSourceConfig(validConfig);
  }

  @Test
  public void testUpdateSyncSource_missingMongodUri_returnsFalseAndSkipsFactory() {
    Mocks mocks = Mocks.create();
    SyncSourceConfig missingUriConfig =
        SyncSourceConfig.builder()
            // mongodSingleHostReplicationUri intentionally left empty
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://host"))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://host"))
            .build();

    assertFalse(mocks.manager.updateSyncSource(missingUriConfig));
    verify(mocks.materializedViewGeneratorFactory, never()).updateSyncSourceConfig(any());
  }

  @Test
  public void testUpdateSyncSource_shardedMissingMongosUri_skipsFactory() {
    Mocks mocks = Mocks.create();
    SyncSourceConfig config =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://mongod"))
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://mongod"))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://mongod"))
            .mongosClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://mongos"))
            .isSharded(true)
            // mongosSingleHostReplicationUri intentionally left empty
            .build();

    mocks.manager.updateSyncSource(config);

    verify(mocks.materializedViewGeneratorFactory, never()).updateSyncSourceConfig(any());
  }

  @Test
  public void testFullSyncSourceUpdateCycle() throws Exception {
    Mocks mocks = Mocks.create();

    // Phase 1: Add index with replication enabled
    MaterializedViewIndexGeneration matViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator oldGenerator =
        mocks.mockMaterializedViewGenerator(matViewIndexGeneration);
    mocks.addIndexForReplication(matViewIndexGeneration);
    verify(mocks.materializedViewGeneratorFactory).create(matViewIndexGeneration);

    // Phase 2: Shutdown replication (simulating sync source change)
    mocks.manager.shutdownReplication().get(5, TimeUnit.SECONDS);
    verify(oldGenerator).shutdown();
    verify(mocks.materializedViewGeneratorFactory).markUninitialized();
    assertFalse(mocks.manager.isReplicationSupported());

    // Phase 3: Update sync source
    SyncSourceConfig newConfig =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newHost"))
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newHost"))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newHost"))
            .build();
    mocks.manager.updateSyncSource(newConfig);
    verify(mocks.materializedViewGeneratorFactory).updateSyncSourceConfig(newConfig);

    // Phase 4: Restart replication — generator recreated from buffer
    MaterializedViewGenerator newGenerator = mock(MaterializedViewGenerator.class);
    doReturn(CompletableFuture.completedFuture(null)).when(newGenerator).shutdown();
    when(newGenerator.getIndexGeneration()).thenReturn(matViewIndexGeneration);
    when(mocks.materializedViewGeneratorFactory.create(matViewIndexGeneration))
        .thenReturn(newGenerator);

    mocks.manager.restartReplication();
    assertTrue(mocks.manager.isReplicationSupported());
    verify(mocks.materializedViewGeneratorFactory, times(2)).create(matViewIndexGeneration);
  }

  /**
   * Verifies the end-to-end missing-URI lifecycle: shutdown → factory uninitialized → missing URI →
   * restart is no-op → URI available → factory updated → restart recreates generators.
   */
  @Test
  public void testMissingUri_shutdownThenRecovery_restartsCorrectly() throws Exception {
    Mocks mocks = Mocks.create();

    // Setup: add an index and start replication.
    MaterializedViewIndexGeneration matViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGeneration);
    mocks.addIndexForReplication(matViewIndexGeneration);
    assertTrue(mocks.manager.isReplicationSupported());

    // Step 1: shutdownReplication (simulates restart triggered by a sync source change).
    mocks.manager.shutdownReplication().get(5, TimeUnit.SECONDS);
    assertFalse(mocks.manager.isReplicationSupported());
    verify(mocks.materializedViewGeneratorFactory).markUninitialized();
    Mockito.clearInvocations(mocks.materializedViewGeneratorFactory);

    // Step 2: simulate factory becoming uninitialized after shutdown (closeClients() clears
    // initialSyncQueue in production).
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(false);

    // Step 3: updateSyncSource with missing mongod URI — factory update must be skipped.
    var configWithMissingUri =
        SyncSourceConfig.builder()
            // mongodSingleHostReplicationUri intentionally left empty
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://host"))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://host"))
            .build();
    mocks.manager.updateSyncSource(configWithMissingUri);
    verify(mocks.materializedViewGeneratorFactory, never()).updateSyncSourceConfig(any());

    // Step 4: restartReplication with uninitialized factory — must be a no-op.
    mocks.manager.restartReplication();
    verify(mocks.materializedViewGeneratorFactory, never()).create(any());
    assertFalse(mocks.manager.isReplicationSupported());

    // Step 5: URI becomes available — factory must be updated and becomes initialized.
    var validConfig =
        SyncSourceConfig.builder()
            .mongodSingleHostReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newHost"))
            .mongodClusterReplicationUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newHost"))
            .mongodClusterReadWriteUri(
                ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newHost"))
            .build();
    mocks.manager.updateSyncSource(validConfig);
    verify(mocks.materializedViewGeneratorFactory).updateSyncSourceConfig(validConfig);
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);

    // Step 6: restartReplication — generators recreated from the buffer.
    MaterializedViewGenerator newGenerator = mock(MaterializedViewGenerator.class);
    doReturn(CompletableFuture.completedFuture(null)).when(newGenerator).shutdown();
    when(newGenerator.getIndexGeneration()).thenReturn(matViewIndexGeneration);
    when(mocks.materializedViewGeneratorFactory.create(matViewIndexGeneration))
        .thenReturn(newGenerator);
    mocks.manager.restartReplication();

    verify(mocks.materializedViewGeneratorFactory).create(matViewIndexGeneration);
    assertTrue(mocks.manager.isReplicationSupported());
  }

  @Test
  public void testRestartReplication_withUninitializedFactory_skipsGeneratorCreation() {
    Mocks mocks = Mocks.create();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(false);
    mocks.manager.setIsReplicationEnabled(false);

    MaterializedViewIndexGeneration matViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.addIndexForReplication(matViewIndexGeneration);

    // Factory not yet initialized (no replication URIs) — restart should be a no-op.
    mocks.manager.restartReplication();

    verify(mocks.materializedViewGeneratorFactory, never()).create(any());
    assertFalse(mocks.manager.isReplicationSupported());

    // Simulate URIs becoming available: factory initialized, then restart.
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);
    mocks.mockMaterializedViewGenerator(matViewIndexGeneration);
    mocks.manager.restartReplication();

    verify(mocks.materializedViewGeneratorFactory).create(matViewIndexGeneration);
    assertTrue(mocks.manager.isReplicationSupported());
  }

  @Test
  public void testAddDuringDisabledReplication_higherVersionReplacesInBuffer() {
    Mocks mocks = Mocks.create();
    mocks.manager.setIsReplicationEnabled(false);

    // Add version 0
    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var matViewIndexGen1 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    mocks.mockMaterializedViewGenerator(matViewIndexGen1);
    mocks.addIndexForReplication(matViewIndexGen1);

    // Add version 1 of same index (higher)
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var matViewIndexGen2 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2, 1));
    mocks.mockMaterializedViewGenerator(matViewIndexGen2);
    mocks.addIndexForReplication(matViewIndexGen2);

    // No generators created yet
    verify(mocks.materializedViewGeneratorFactory, never()).create(any());

    // Restart — only version 1 should be used
    mocks.manager.restartReplication();
    verify(mocks.materializedViewGeneratorFactory).create(matViewIndexGen2);
  }

  @Test
  public void testAddDuringDisabledReplication_sameVersion_latestWins() {
    Mocks mocks = Mocks.create();
    mocks.manager.setIsReplicationEnabled(false);

    // Add version 0
    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var matViewIndexGen1 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    mocks.mockMaterializedViewGenerator(matViewIndexGen1);
    mocks.addIndexForReplication(matViewIndexGen1);

    // Add same version 0 again (different generation attempt)
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var matViewIndexGen2 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2));
    mocks.addIndexForReplication(matViewIndexGen2);

    // Same version: the latest matViewIndexGeneration wins (used by restartReplication).
    // Each generation has its own index since we always replace the generator.
    UUID uuid = UUID.nameUUIDFromBytes(INDEX_ID.toHexString().getBytes(StandardCharsets.UTF_8));
    assertEquals(
        matViewIndexGen2,
        mocks
            .manager
            .getActiveGenerationIdCatalog()
            .latestMatViewIndexGenerationByCollection
            .get(uuid));
  }

  @Test
  public void testDropDuringDisabledReplication_removesFromBuffer_restartCreatesNothing()
      throws Exception {
    // add() while disabled populates latestMatViewIndexGenerationByCollection.
    // dropIndex() while disabled removes from it.
    // restartReplication() should create zero generators.
    Mocks mocks = Mocks.create();
    mocks.manager.setIsReplicationEnabled(false);

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    // Verify buffer is populated
    UUID uuid = UUID.nameUUIDFromBytes(INDEX_ID.toHexString().getBytes(StandardCharsets.UTF_8));
    assertNotNull(
        mocks
            .manager
            .getActiveGenerationIdCatalog()
            .latestMatViewIndexGenerationByCollection
            .get(uuid));

    // Drop the index while disabled
    GenerationId dropGenId =
        new GenerationId(
            INDEX_ID,
            new Generation(Generation.CURRENT.userIndexVersion, IndexFormatVersion.CURRENT, 0));
    mocks.manager.dropIndex(dropGenId).get(5, TimeUnit.SECONDS);

    // Buffer should be empty
    assertNull(
        mocks
            .manager
            .getActiveGenerationIdCatalog()
            .latestMatViewIndexGenerationByCollection
            .get(uuid));

    // Restart replication — no generators should be created since buffer is empty
    mocks.manager.restartReplication();
    verify(mocks.materializedViewGeneratorFactory, never()).create(any());
  }

  @Test
  public void testDropOneDuringDisabledReplication_restartCreatesOnlyRemaining() throws Exception {
    // add() two different indexes while disabled, then dropIndex() one.
    // restartReplication() should create exactly one generator for the remaining index.
    Mocks mocks = Mocks.create();
    mocks.manager.setIsReplicationEnabled(false);

    // Add index 1
    MaterializedViewIndexGeneration matViewIndexGen1 =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGen1);
    mocks.addIndexForReplication(matViewIndexGen1);

    // Add index 2 (different indexId → different UUID)
    ObjectId indexId2 = new ObjectId();
    MaterializedViewIndexDefinitionGeneration defGen2 = mockMatViewDefinitionGeneration(indexId2);
    MaterializedViewIndexGeneration matViewIndexGen2 = mockMatViewIndexGeneration(defGen2);
    mocks.mockMaterializedViewGenerator(matViewIndexGen2);
    mocks.addIndexForReplication(matViewIndexGen2);

    // Both should be buffered
    UUID uuid1 = UUID.nameUUIDFromBytes(INDEX_ID.toHexString().getBytes(StandardCharsets.UTF_8));
    UUID uuid2 = UUID.nameUUIDFromBytes(indexId2.toHexString().getBytes(StandardCharsets.UTF_8));
    assertNotNull(
        mocks
            .manager
            .getActiveGenerationIdCatalog()
            .latestMatViewIndexGenerationByCollection
            .get(uuid1));
    assertNotNull(
        mocks
            .manager
            .getActiveGenerationIdCatalog()
            .latestMatViewIndexGenerationByCollection
            .get(uuid2));

    // Drop index 1
    GenerationId dropGenId =
        new GenerationId(
            INDEX_ID,
            new Generation(Generation.CURRENT.userIndexVersion, IndexFormatVersion.CURRENT, 0));
    mocks.manager.dropIndex(dropGenId).get(5, TimeUnit.SECONDS);

    // Only index 2 should remain in buffer
    assertNull(
        mocks
            .manager
            .getActiveGenerationIdCatalog()
            .latestMatViewIndexGenerationByCollection
            .get(uuid1));
    assertNotNull(
        mocks
            .manager
            .getActiveGenerationIdCatalog()
            .latestMatViewIndexGenerationByCollection
            .get(uuid2));

    // Restart — only index 2's generator should be created
    mocks.manager.restartReplication();
    verify(mocks.materializedViewGeneratorFactory, never()).create(matViewIndexGen1);
    verify(mocks.materializedViewGeneratorFactory).create(matViewIndexGen2);
  }

  @Test
  public void testDropHigherVersionDuringDisabledReplication_bufferCleared() throws Exception {
    // add() v0 then v1 (higher version replaces in buffer), then dropIndex() v1.
    // Buffer should be cleared because v1 was the last refcount for that UUID.
    // restartReplication() should create nothing.
    Mocks mocks = Mocks.create();
    mocks.manager.setIsReplicationEnabled(false);

    // Add version 0
    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var matViewIndexGen1 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    mocks.mockMaterializedViewGenerator(matViewIndexGen1);
    mocks.addIndexForReplication(matViewIndexGen1);

    // Add version 1 (higher, replaces v0 in buffer)
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var matViewIndexGen2 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2, 1));
    mocks.mockMaterializedViewGenerator(matViewIndexGen2);
    mocks.addIndexForReplication(matViewIndexGen2);

    UUID uuid = UUID.nameUUIDFromBytes(INDEX_ID.toHexString().getBytes(StandardCharsets.UTF_8));
    // Buffer should have v1 (the latest)
    assertEquals(
        matViewIndexGen2,
        mocks
            .manager
            .getActiveGenerationIdCatalog()
            .latestMatViewIndexGenerationByCollection
            .get(uuid));

    // Drop both attempts (v0 first, then v1)
    GenerationId dropGenId0 =
        new GenerationId(
            INDEX_ID,
            new Generation(Generation.CURRENT.userIndexVersion, IndexFormatVersion.CURRENT, 0));
    mocks.manager.dropIndex(dropGenId0).get(5, TimeUnit.SECONDS);
    // Buffer should still have v1 (v0 was one of two refcounts)
    assertNotNull(
        mocks
            .manager
            .getActiveGenerationIdCatalog()
            .latestMatViewIndexGenerationByCollection
            .get(uuid));

    UserIndexVersion u1Version =
        new UserIndexVersion(Generation.CURRENT.userIndexVersion.versionNumber + 1);
    GenerationId dropGenId1 =
        new GenerationId(INDEX_ID, new Generation(u1Version, IndexFormatVersion.CURRENT, 0));
    mocks.manager.dropIndex(dropGenId1).get(5, TimeUnit.SECONDS);
    // Buffer should be cleared — all refcounts gone
    assertNull(
        mocks
            .manager
            .getActiveGenerationIdCatalog()
            .latestMatViewIndexGenerationByCollection
            .get(uuid));

    // Restart — nothing to create
    mocks.manager.restartReplication();
    verify(mocks.materializedViewGeneratorFactory, never()).create(any());
  }

  @Test
  public void testIsReplicationSupported_lifecycle() {
    Mocks mocks = Mocks.create();
    assertTrue(mocks.manager.isReplicationSupported());

    mocks.manager.setIsReplicationEnabled(false);
    assertFalse(mocks.manager.isReplicationSupported());

    mocks.manager.setIsReplicationEnabled(true);
    assertTrue(mocks.manager.isReplicationSupported());
  }

  @Test
  public void testPeriodicTasksNoOpWhenReplicationDisabled() {
    // Use follower mocks because they capture the status refresh runnable
    Mocks mocks = Mocks.createFollower();
    mocks.manager.setIsReplicationEnabled(false);

    // Run the captured status refresh runnable
    mocks.runnableCaptor.orElseThrow().getValue().run();

    // LeaseManager should NOT be polled when replication is disabled
    verify(mocks.leaseManager, never()).pollFollowerStatuses();
  }

  // ==================== Retry / Attempt Ref Counting Tests ====================

  @Test
  public void testDropIndex_nextAttemptRefCounting_leaseSurvivesUntilLastAttemptDropped()
      throws Exception {
    // Exercises the key scenario: DefaultLifecycleManager can pass
    // GenerationId("indexID-f6-u1-a0") and GenerationId("indexID-f6-u1-a1") for different
    // attempts. MaterializedViewManager must correctly reference-count them under the same
    // canonicalKey so that the lease is only cleaned up when the last attempt is dropped.
    Mocks mocks = Mocks.create();

    // Create a single matview definition generation and index generation (shared across attempts)
    MaterializedViewIndexDefinitionGeneration defGen = mockMatViewDefinitionGeneration(INDEX_ID);
    MaterializedViewIndexGeneration matViewIndexGen = mockMatViewIndexGeneration(defGen);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);

    // add(a0): first attempt
    mocks.addIndexForReplicationWithAttempt(matViewIndexGen, 0);

    // add(a1): second attempt — same canonicalKey, different GenerationId.
    // With always-replace, this triggers replaceGenerator (shutdown + re-create).
    mocks.addIndexForReplicationWithAttempt(matViewIndexGen, 1);

    // Clear invocations from the add phase (replaceGenerator calls leaseManager.drop/add and
    // generator.shutdown) so subsequent verifications only check the dropIndex behavior.
    Mockito.clearInvocations(generator, mocks.leaseManager, mocks.metadataCatalog);

    // Both attempts should share the same canonicalKey in genIdByMatViewGenId
    var catalog = mocks.manager.getActiveGenerationIdCatalog();
    String canonicalKey = canonicalKey(new GenerationId(INDEX_ID, Generation.CURRENT));
    Set<GenerationId> genIds = catalog.genIdByMatViewGenId.get(canonicalKey);
    assertEquals("Both attempts should be tracked under the same canonicalKey", 2, genIds.size());

    // Construct the GenerationId for attempt 0
    GenerationId genIdA0 =
        new GenerationId(
            INDEX_ID,
            new Generation(Generation.CURRENT.userIndexVersion, IndexFormatVersion.CURRENT, 0));

    // dropIndex(a0): first attempt dropped — lease should survive
    mocks.manager.dropIndex(genIdA0).get(5, TimeUnit.SECONDS);

    // Verify lease was NOT cleaned up (leaseManager.drop and dropLease not called yet)
    verify(mocks.leaseManager, never()).drop(any());
    verify(mocks.leaseManager, never()).dropLease(any());
    // Generator should NOT be shut down (other attempt still active, same UUID)
    verify(generator, never()).shutdown();

    // Construct the GenerationId for attempt 1
    GenerationId genIdA1 =
        new GenerationId(
            INDEX_ID,
            new Generation(Generation.CURRENT.userIndexVersion, IndexFormatVersion.CURRENT, 1));

    // dropIndex(a1): last attempt dropped — lease should now be cleaned up
    mocks.manager.dropIndex(genIdA1).get(5, TimeUnit.SECONDS);

    // Now the generator should be shut down, generationId untracked, and lease dropped.
    // leaseManager.drop() is called twice: once eagerly in cleanUpMatViewResources() to stop
    // heartbeats immediately, and once in cleanUpGenerationIdStates() for reference-counting
    // cleanup. Both calls are idempotent.
    verify(generator).shutdown();
    verify(mocks.leaseManager, times(2)).drop(defGen.getGenerationId());
    verify(mocks.leaseManager).dropLease(any(String.class));
    verify(mocks.metadataCatalog).removeMetadata(defGen.getGenerationId());
  }

  @Test
  public void testDropIndex_multiVersionMultiAttemptRefCounting() throws Exception {
    // Comprehensive reference counting test:
    //   add(indexID-f6-u0-a0) → matview-indexID-u0 (createNewGenerator)
    //   add(indexID-f6-u0-a1) → matview-indexID-u0 (replaceGenerator, same definitionVersion)
    //   add(indexID-f6-u0-a2) → matview-indexID-u0 (replaceGenerator, same definitionVersion)
    //   add(indexID-f6-u1-a0) → matview-indexID-u1 (replaceGenerator, higher definitionVersion)
    //   add(indexID-f6-u1-a1) → matview-indexID-u1 (replaceGenerator, same definitionVersion)
    //
    // Two-dimensional ref counting:
    //   genIdByMatViewCollection[uuid] → {u0-a0, u0-a1, u0-a2, u1-a0, u1-a1} (keyed by UUID)
    //   genIdByMatViewGen["matview-indexID-u0"] → {u0-a0, u0-a1, u0-a2}
    //   genIdByMatViewGen["matview-indexID-u1"] → {u1-a0, u1-a1}
    //
    // Drop order: u0-a0, u0-a1, u0-a2, u1-a0, u1-a1
    Mocks mocks = Mocks.create();
    ObjectId indexId = INDEX_ID;

    // === Phase 1: Add u0 with 3 attempts ===
    MaterializedViewGenerationId matViewGenIdU0 =
        new MaterializedViewGenerationId(
            indexId, new MaterializedViewGeneration(Generation.CURRENT));
    MaterializedViewIndexDefinitionGeneration defGenU0 =
        mockMatViewDefinitionGeneration(matViewGenIdU0, 0);
    MaterializedViewIndexGeneration matViewIndexGenU0 = mockMatViewIndexGeneration(defGenU0);
    MaterializedViewGenerator generatorU0 = mocks.mockMaterializedViewGenerator(matViewIndexGenU0);

    mocks.addIndexForReplicationWithAttempt(matViewIndexGenU0, 0);
    mocks.addIndexForReplicationWithAttempt(matViewIndexGenU0, 1);
    mocks.addIndexForReplicationWithAttempt(matViewIndexGenU0, 2);

    // === Phase 2: Add u1 with 2 attempts (replaces u0's generator) ===
    MaterializedViewGenerationId matViewGenIdU1 =
        new MaterializedViewGenerationId(indexId, matViewGenIdU0.generation.incrementUser());
    MaterializedViewIndexDefinitionGeneration defGenU1 =
        mockMatViewDefinitionGeneration(
            matViewGenIdU1, 1); // higher definitionVersion triggers replace
    MaterializedViewIndexGeneration matViewIndexGenU1 = mockMatViewIndexGeneration(defGenU1);
    MaterializedViewGenerator generatorU1 = mocks.mockMaterializedViewGenerator(matViewIndexGenU1);

    mocks.addIndexForReplicationWithAttempt(matViewIndexGenU1, 0);
    mocks.addIndexForReplicationWithAttempt(matViewIndexGenU1, 1);

    // Verify u0's generator was shut down (replaced by same-version attempts and then by u1).
    // With replaceGenerator for all cases, shutdown is called for each replacement.
    verify(generatorU0, atLeast(1)).shutdown();
    Mockito.clearInvocations(generatorU0, generatorU1, mocks.leaseManager, mocks.metadataCatalog);

    // === Verify initial catalog state ===
    var catalog = mocks.manager.getActiveGenerationIdCatalog();
    String canonicalKeyU0 = canonicalKey(new GenerationId(indexId, Generation.CURRENT));
    UserIndexVersion u1Version =
        new UserIndexVersion(Generation.CURRENT.userIndexVersion.versionNumber + 1);
    String canonicalKeyU1 =
        canonicalKey(
            new GenerationId(indexId, new Generation(u1Version, IndexFormatVersion.CURRENT)));

    assertEquals(
        "u0 should have 3 attempts tracked",
        3,
        catalog.genIdByMatViewGenId.get(canonicalKeyU0).size());
    assertEquals(
        "u1 should have 2 attempts tracked",
        2,
        catalog.genIdByMatViewGenId.get(canonicalKeyU1).size());

    // All 5 generations share the same UUID
    UUID uuid = UUID.nameUUIDFromBytes(indexId.toHexString().getBytes(StandardCharsets.UTF_8));
    assertEquals(
        "All 5 generations should share same UUID in genIdByMatViewCollection",
        5,
        catalog.genIdByMatViewCollection.get(uuid).size());

    // === Drop u0-a0: uuid set=4, generatorId["u0"]=2 → no cleanup ===
    GenerationId genIdU0A0 =
        new GenerationId(
            indexId,
            new Generation(Generation.CURRENT.userIndexVersion, IndexFormatVersion.CURRENT, 0));
    mocks.manager.dropIndex(genIdU0A0).get(5, TimeUnit.SECONDS);

    verify(mocks.leaseManager, never()).drop(any());
    verify(mocks.leaseManager, never()).dropLease(any());
    verify(generatorU1, never()).shutdown();
    verify(mocks.metadataCatalog, never()).removeMetadata(any());
    assertEquals(4, catalog.genIdByMatViewCollection.get(uuid).size());
    assertEquals(2, catalog.genIdByMatViewGenId.get(canonicalKeyU0).size());

    // === Drop u0-a1: uuid set=3, generatorId["u0"]=1 → no cleanup ===
    GenerationId genIdU0A1 =
        new GenerationId(
            indexId,
            new Generation(Generation.CURRENT.userIndexVersion, IndexFormatVersion.CURRENT, 1));
    mocks.manager.dropIndex(genIdU0A1).get(5, TimeUnit.SECONDS);

    verify(mocks.leaseManager, never()).drop(any());
    verify(mocks.metadataCatalog, never()).removeMetadata(any());
    assertEquals(3, catalog.genIdByMatViewCollection.get(uuid).size());
    assertEquals(1, catalog.genIdByMatViewGenId.get(canonicalKeyU0).size());

    // === Drop u0-a2: uuid set=2, generatorId["u0"]=0 → removeMetadata(u0) ===
    GenerationId genIdU0A2 =
        new GenerationId(
            indexId,
            new Generation(Generation.CURRENT.userIndexVersion, IndexFormatVersion.CURRENT, 2));
    mocks.manager.dropIndex(genIdU0A2).get(5, TimeUnit.SECONDS);

    verify(mocks.leaseManager).drop(any());
    verify(mocks.leaseManager, never()).dropLease(any());
    verify(generatorU1, never()).shutdown();
    // u0's metadata cleaned up because genIdByMatViewGenId["u0"] is empty
    verify(mocks.metadataCatalog).removeMetadata(defGenU0.getGenerationId());
    assertEquals(2, catalog.genIdByMatViewCollection.get(uuid).size());
    assertNull("u0 generatorId should be cleaned", catalog.genIdByMatViewGenId.get(canonicalKeyU0));
    // u1 still alive
    assertEquals(2, catalog.genIdByMatViewGenId.get(canonicalKeyU1).size());
    Mockito.clearInvocations(mocks.metadataCatalog, mocks.leaseManager);

    // === Drop u1-a0: uuid set=1, generatorId["u1"]=1 → no cleanup ===
    GenerationId genIdU1A0 =
        new GenerationId(indexId, new Generation(u1Version, IndexFormatVersion.CURRENT, 0));
    mocks.manager.dropIndex(genIdU1A0).get(5, TimeUnit.SECONDS);

    verify(mocks.leaseManager, never()).drop(any());
    verify(mocks.leaseManager, never()).dropLease(any());
    verify(generatorU1, never()).shutdown();
    verify(mocks.metadataCatalog, never()).removeMetadata(any());
    assertEquals(1, catalog.genIdByMatViewCollection.get(uuid).size());
    assertEquals(1, catalog.genIdByMatViewGenId.get(canonicalKeyU1).size());

    // === Drop u1-a1: uuid set=0 → shutdown + drop + dropLease. generatorId["u1"]=0 →
    // removeMetadata(u1) ===
    GenerationId genIdU1A1 =
        new GenerationId(indexId, new Generation(u1Version, IndexFormatVersion.CURRENT, 1));
    mocks.manager.dropIndex(genIdU1A1).get(5, TimeUnit.SECONDS);

    // Generator shutdown + lease cleanup (leader path).
    // leaseManager.drop() is called twice: once eagerly in cleanUpMatViewResources() to stop
    // heartbeats immediately, and once in cleanUpGenerationIdStates() for reference-counting
    // cleanup. Both calls are idempotent.
    verify(generatorU1).shutdown();
    verify(mocks.leaseManager, times(2)).drop(defGenU1.getGenerationId());
    verify(mocks.leaseManager).dropLease(any(String.class));
    // Metadata cleanup
    verify(mocks.metadataCatalog).removeMetadata(defGenU1.getGenerationId());
    // Both maps fully cleaned
    assertNull("uuid should be fully cleaned", catalog.genIdByMatViewCollection.get(uuid));
    assertNull("u1 generatorId should be cleaned", catalog.genIdByMatViewGenId.get(canonicalKeyU1));
  }

  // ==================== Dynamic Leader Lease Acquisition Tests ====================

  @Test
  public void dynamicLeader_acquireLease_skipsGeneratorInFailedState() {
    // When a generator is in FAILED state and the lease becomes acquirable,
    // refreshStatus should skip leadership acquisition for that generator.
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    // Simulate the generator entering FAILED state.
    doReturn(ReplicationIndexManager.State.FAILED).when(generator).getState();

    // Configure pollFollowerStatuses to return this generation as acquirable.
    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(matViewGenId, IndexStatus.unknown()), Set.of(matViewGenId)));

    // Run the status refresh (which calls refreshStatus -> acquirableLeases loop).
    mocks.runnableCaptor.orElseThrow().getValue().run();

    // tryAcquireLeadership should NOT be called because the generator is in FAILED state.
    verify(mocks.leaseManager, never()).tryAcquireLeadership(any());
    verify(generator, never()).becomeLeader();
  }

  @Test
  public void dynamicLeader_acquireLease_skipsGeneratorInShutDownState() {
    // When a generator is in SHUT_DOWN state and the lease becomes acquirable,
    // refreshStatus should skip leadership acquisition for that generator.
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    // Simulate the generator entering SHUT_DOWN state.
    doReturn(ReplicationIndexManager.State.SHUT_DOWN).when(generator).getState();

    // Configure pollFollowerStatuses to return this generation as acquirable.
    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(matViewGenId, IndexStatus.unknown()), Set.of(matViewGenId)));

    // Run the status refresh.
    mocks.runnableCaptor.orElseThrow().getValue().run();

    // tryAcquireLeadership should NOT be called because the generator is in SHUT_DOWN state.
    verify(mocks.leaseManager, never()).tryAcquireLeadership(any());
    verify(generator, never()).becomeLeader();
  }

  @Test
  public void dynamicLeader_acquireLease_skipsGeneratorInFailedExceededState() {
    // refreshStatus must skip lease acquisition for FAILED_EXCEEDED, just like FAILED and
    // SHUT_DOWN. Both acquirable-lease guards gate on TERMINAL_STATES.
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    // Simulate the generator entering FAILED_EXCEEDED state (e.g., exceededIndex via
    // FIELD_EXCEEDED in initial sync or steady state, or DOCS_EXCEEDED in initial sync).
    doReturn(ReplicationIndexManager.State.FAILED_EXCEEDED).when(generator).getState();

    // Configure pollFollowerStatuses to return this generation as acquirable.
    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(matViewGenId, IndexStatus.unknown()), Set.of(matViewGenId)));

    // Run the status refresh.
    mocks.runnableCaptor.orElseThrow().getValue().run();

    // tryAcquireLeadership should NOT be called because FAILED_EXCEEDED is terminal.
    verify(mocks.leaseManager, never()).tryAcquireLeadership(any());
    verify(generator, never()).becomeLeader();
  }

  @Test
  public void dynamicLeader_acquireLease_succeedsForHealthyGenerator() {
    // When a generator is in INITIAL_SYNC state (healthy) and the lease becomes acquirable,
    // refreshStatus should attempt and succeed in acquiring leadership.
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    // Generator is in a healthy state.
    doReturn(ReplicationIndexManager.State.INITIAL_SYNC).when(generator).getState();

    // Configure pollFollowerStatuses to return this generation as acquirable.
    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(matViewGenId, IndexStatus.unknown()), Set.of(matViewGenId)));
    when(mocks.leaseManager.tryAcquireLeadership(matViewGenId)).thenReturn(true);

    // Run the status refresh.
    mocks.runnableCaptor.orElseThrow().getValue().run();

    // tryAcquireLeadership should be called and becomeLeader() invoked.
    verify(mocks.leaseManager).tryAcquireLeadership(matViewGenId);
    verify(generator).becomeLeader();
  }

  @Test
  public void dynamicLeader_acquireLease_skipsWhenPendingShutdownAndQueueDirty() {
    // During leadership oscillation, refreshStatus should skip leadership acquisition
    // when pendingShutdowns contains the generationId AND the queue still has a stale entry.
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    doReturn(ReplicationIndexManager.State.INITIAL_SYNC).when(generator).getState();

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(matViewGenId, IndexStatus.unknown()), Set.of(matViewGenId)));

    // Simulate a pending shutdown with stale queue entry.
    mocks.manager.pendingShutdowns.put(matViewGenId, COMPLETED_FUTURE);
    when(mocks.materializedViewGeneratorFactory.hasQueueEntry(matViewGenId)).thenReturn(true);

    // Run the status refresh.
    mocks.runnableCaptor.orElseThrow().getValue().run();

    // tryAcquireLeadership should NOT be called because queue is still dirty.
    verify(mocks.leaseManager, never()).tryAcquireLeadership(any());
    verify(generator, never()).becomeLeader();
    assertTrue(
        "pendingShutdowns should still contain the id",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));
  }

  @Test
  public void dynamicLeader_acquireLease_skipsWhenShutdownFutureNotDone() {
    // Even if the queue is clean, keep the guard while the shutdown future is still running.
    // This prevents concurrent writers when the old generator is in STEADY_STATE shutdown.
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    doReturn(ReplicationIndexManager.State.INITIAL_SYNC).when(generator).getState();

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(matViewGenId, IndexStatus.unknown()), Set.of(matViewGenId)));

    // Shutdown future still running, queue is clean.
    mocks.manager.pendingShutdowns.put(matViewGenId, new CompletableFuture<>());
    when(mocks.materializedViewGeneratorFactory.hasQueueEntry(matViewGenId)).thenReturn(false);

    mocks.runnableCaptor.orElseThrow().getValue().run();

    // Guard should stay — shutdown not done yet.
    verify(mocks.leaseManager, never()).tryAcquireLeadership(any());
    assertTrue(
        "pendingShutdowns should be retained while shutdown future is running",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));
  }

  @Test
  public void dynamicLeader_acquireLease_resumesWhenQueueClean() {
    // When pendingShutdowns contains the generationId but the queue is clean,
    // refreshStatus should remove from pendingShutdowns and acquire leadership.
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    doReturn(ReplicationIndexManager.State.INITIAL_SYNC).when(generator).getState();

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(matViewGenId, IndexStatus.unknown()), Set.of(matViewGenId)));
    when(mocks.leaseManager.tryAcquireLeadership(matViewGenId)).thenReturn(true);

    // Simulate a pending shutdown where the queue has already cleaned up.
    mocks.manager.pendingShutdowns.put(matViewGenId, COMPLETED_FUTURE);
    when(mocks.materializedViewGeneratorFactory.hasQueueEntry(matViewGenId)).thenReturn(false);

    // Run the status refresh.
    mocks.runnableCaptor.orElseThrow().getValue().run();

    // pendingShutdowns should be cleared and leadership acquired.
    assertFalse(
        "pendingShutdowns should be cleared",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));
    verify(mocks.leaseManager).tryAcquireLeadership(matViewGenId);
    verify(generator).becomeLeader();
  }

  @Test
  public void dynamicLeader_transitionToFollower_skipsWhenPendingShutdown() {
    // When two heartbeat threads snapshot the same leader generator before either enters
    // the synchronized transitionToFollower, the second call should skip because
    // pendingShutdowns already contains the generationId.
    // Uses DynamicLeaderLeaseManager mock so the instanceof check in emitHeartbeat passes.
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    // Generator was activated as leader during addIndexForReplication.
    verify(generator).becomeLeader();
    assertTrue(generator.isLeader());

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();

    // Simulate a pending shutdown — first transitionToFollower already in progress.
    mocks.manager.pendingShutdowns.put(matViewGenId, COMPLETED_FUTURE);

    // Simulate leadership loss so heartbeat would normally call transitionToFollower.
    when(mocks.leaseManager.isLeader(matViewGenId)).thenReturn(false);

    // Capture and run the heartbeat task.
    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();

    // The generator factory should NOT have been called again to create a new follower,
    // because transitionToFollower skipped due to pending shutdown.
    verify(mocks.materializedViewGeneratorFactory, times(1)).create(matViewIndexGen);
  }

  @Test
  public void dynamicLeader_transitionToFollower_addsPendingShutdownBeforeFactoryCreate() {
    // Verify that pendingShutdowns.put() happens BEFORE factory.create() in
    // transitionToFollower. This prevents Race 1 where a slow factory.create()
    // allows refreshStatus to check pendingShutdowns before it's set.
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    verify(generator).becomeLeader();
    assertTrue(generator.isLeader());

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();

    // Track when pendingShutdowns.put() is called relative to factory.create().
    // Use doAnswer on factory.create() to assert pendingShutdowns already contains the id.
    when(mocks.materializedViewGeneratorFactory.create(matViewIndexGen))
        .thenAnswer(
            invocation -> {
              // At this point in transitionToFollower, pendingShutdowns.put() should have
              // already been called (Race 1 fix).
              assertTrue(
                  "pendingShutdowns should contain generationId before factory.create()",
                  mocks.manager.pendingShutdowns.containsKey(matViewGenId));
              MaterializedViewGenerator newGen = mock(MaterializedViewGenerator.class);
              when(newGen.getIndexGeneration()).thenReturn(matViewIndexGen);
              when(newGen.shutdown()).thenReturn(CompletableFuture.completedFuture(null));
              return newGen;
            });

    // Simulate leadership loss so heartbeat calls transitionToFollower.
    when(mocks.leaseManager.isLeader(matViewGenId)).thenReturn(false);

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();

    // Verify transitionToFollower ran: factory.create() was called for the new follower generator.
    verify(mocks.materializedViewGeneratorFactory, times(2)).create(matViewIndexGen);
  }

  @Test
  public void dynamicLeader_heartbeatTransitionsToFollower_whenRenewalLosesLeadership() {
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    verify(generator).becomeLeader();
    assertTrue(generator.isLeader());

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    AtomicBoolean leaseManagerIsLeader = new AtomicBoolean(true);
    when(mocks.leaseManager.isLeader(matViewGenId))
        .thenAnswer(invocation -> leaseManagerIsLeader.get());
    doAnswer(
            invocation -> {
              leaseManagerIsLeader.set(false);
              return null;
            })
        .when(mocks.leaseManager)
        .heartbeat();

    Mockito.clearInvocations(mocks.leaseManager, mocks.materializedViewGeneratorFactory);

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();

    InOrder inOrder = Mockito.inOrder(mocks.leaseManager, mocks.materializedViewGeneratorFactory);
    inOrder.verify(mocks.leaseManager).heartbeat();
    inOrder.verify(mocks.materializedViewGeneratorFactory).create(matViewIndexGen);
    verify(generator).shutdown();
  }

  @Test
  public void dynamicLeader_transitionToFollower_rollsPendingShutdownOnFactoryFailure() {
    // If factory.create() throws, we still install the ex-leader shutdown future in
    // pendingShutdowns (same as the success path) so refreshStatus can wait for shutdown + queue
    // cleanup before unblocking leadership acquisition. The heartbeat runnable must not throw.
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    verify(generator).becomeLeader();

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();

    // Make factory.create() throw on the second call (transitionToFollower).
    when(mocks.materializedViewGeneratorFactory.create(matViewIndexGen))
        .thenThrow(new RuntimeException("factory failure"));

    // Simulate leadership loss.
    when(mocks.leaseManager.isLeader(matViewGenId)).thenReturn(false);

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());

    heartbeatCaptor.getValue().run();

    assertTrue(
        "pendingShutdowns should record ex-leader shutdown after failed follower create",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));
    assertTrue(mocks.manager.pendingShutdowns.get(matViewGenId).isDone());
    verify(generator).shutdown();
    assertEquals(
        0.0,
        mocks
            .meterRegistry
            .counter(MaterializedViewManager.GENERATOR_SHUTDOWN_FAILURE_COUNTER_NAME)
            .count(),
        0.0);
  }

  @Test
  public void
      dynamicLeader_transitionToFollower_incrementsShutdownFailureCounterWhenShutdownThrows() {
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();

    when(mocks.materializedViewGeneratorFactory.create(matViewIndexGen))
        .thenThrow(new RuntimeException("factory failure"));
    when(mocks.leaseManager.isLeader(matViewGenId)).thenReturn(false);

    when(generator.shutdown())
        .thenThrow(new RuntimeException("sync shutdown after create failure"));

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());

    heartbeatCaptor.getValue().run();

    assertFalse(
        "pendingShutdowns removed when shutdown throws in recovery path",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));
    assertEquals(
        1.0,
        mocks
            .meterRegistry
            .counter(MaterializedViewManager.GENERATOR_SHUTDOWN_FAILURE_COUNTER_NAME)
            .count(),
        0.0);
    assertEquals(
        1.0,
        mocks
            .meterRegistry
            .counter(
                MaterializedViewManager.GENERATOR_CREATION_FAILURE_COUNTER_NAME,
                "errorType",
                "FACTORY_CREATE_AFTER_LEADERSHIP_LOSS",
                "reason",
                "NON_TRANSIENT")
            .count(),
        0.0);
  }

  // ==================== Stale generator guard in refreshStatus ====================

  @Test
  public void dynamicLeader_acquireLease_releasesLeaseWhenGeneratorTerminalAfterAcquisition() {
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    doReturn(ReplicationIndexManager.State.INITIAL_SYNC).when(generator).getState();

    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(matViewGenId, IndexStatus.unknown()), Set.of(matViewGenId)));

    // When tryAcquireLeadership is called, simulate transitionToFollower running concurrently
    // by changing the generator to a terminal state.
    when(mocks.leaseManager.tryAcquireLeadership(matViewGenId))
        .thenAnswer(
            invocation -> {
              doReturn(ReplicationIndexManager.State.SHUT_DOWN).when(generator).getState();
              return true;
            });

    mocks.runnableCaptor.orElseThrow().getValue().run();

    verify(generator, never()).becomeLeader();
    verify(mocks.leaseManager).releaseLeadership(matViewGenId);
  }

  // ==================== Zombie lease detection in emitHeartbeat ====================

  @Test
  public void dynamicLeader_heartbeat_releasesZombieLease() {
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    verify(generator).becomeLeader();
    assertTrue(generator.isLeader());

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();

    // Simulate the zombie state: generator failed with isLeader=false,
    // but leaseManager still has it as leader
    doReturn(ReplicationIndexManager.State.FAILED).when(generator).getState();
    doReturn(false).when(generator).isLeader();
    assertTrue(
        "leaseManager should still consider it a leader",
        mocks.leaseManager.isLeader(matViewGenId));

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();

    verify(mocks.leaseManager).releaseLeadership(matViewGenId);
  }

  // ==================== Zombie lease detection — state-based =====================================
  //
  // On the async failAndDropIndex path, MaterializedViewGenerator.isLeader stays true even
  // though state has transitioned to FAILED. Heartbeat must release the lease based on state
  // alone, independent of the cached isLeader flag.

  @Test
  public void dynamicLeader_heartbeat_releasesLease_whenStateFailedAndIsLeaderStuckTrue() {
    // Production-realistic: state=FAILED, cached isLeader=true. Heartbeat releases anyway.
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    verify(generator).becomeLeader();
    assertTrue(generator.isLeader());

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    doReturn(ReplicationIndexManager.State.FAILED).when(generator).getState();

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();

    verify(mocks.leaseManager).releaseLeadership(matViewGenId);
    // Gauge must drop with the lease, even on terminal paths that never close the index.
    verify(generator).clearLeaderState();
  }

  @Test
  public void dynamicLeader_heartbeat_releasesLease_whenStateShutDown() {
    // Direct-to-SHUT_DOWN paths (dropIndex no-arg, shutdownReplicationOnStaleIndex,
    // handleInitialSyncException case SHUT_DOWN) bypass failAndDropIndex / failAndCloseIndex,
    // so any per-method override approach would miss them.
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    doReturn(ReplicationIndexManager.State.SHUT_DOWN).when(generator).getState();
    doReturn(true).when(generator).isLeader();

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();

    verify(mocks.leaseManager).releaseLeadership(matViewGenId);
  }

  @Test
  public void dynamicLeader_heartbeat_releasesLease_whenStateFailedExceeded() {
    // exceededIndex transitions to FAILED_EXCEEDED. Reachable for matview generators via
    // initial-sync and steady-state FIELD_EXCEEDED. Heartbeat must treat it as terminal.
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    doReturn(ReplicationIndexManager.State.FAILED_EXCEEDED).when(generator).getState();
    doReturn(true).when(generator).isLeader();

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();

    verify(mocks.leaseManager).releaseLeadership(matViewGenId);
  }

  @Test
  public void dynamicLeader_heartbeat_doesNotReleaseLease_whenStateSteadyStateShutDown() {
    // STEADY_STATE_SHUT_DOWN is transient: the post-stop classifier can transition back to
    // STEADY_STATE or INITIAL_SYNC. Treating it as terminal would churn the leader on every
    // transient hiccup. Must NOT be in TERMINAL_STATES.
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    doReturn(ReplicationIndexManager.State.STEADY_STATE_SHUT_DOWN).when(generator).getState();
    doReturn(true).when(generator).isLeader();

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();

    verify(mocks.leaseManager, never()).releaseLeadership(matViewGenId);
  }

  @Test
  public void dynamicLeader_heartbeat_skipsTransitionToFollower_whenSnapshotGeneratorReplacedInMap()
      throws Exception {
    // Same-genId race on the leadership-loss branch: the heartbeat snapshot captured the
    // OLD generator, but a concurrent replaceGenerator swapped a NEW one at the same uuid.
    // Without the identity check, transitionToFollower would overwrite the new generator.
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator oldGenerator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    verify(oldGenerator).becomeLeader();
    assertTrue(oldGenerator.isLeader());
    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    UUID matViewUuid = mocks.manager.getCollectionUuid(matViewGenId);

    // Old generator is healthy (non-terminal) but with stale cached isLeader=true.
    doReturn(ReplicationIndexManager.State.STEADY_STATE).when(oldGenerator).getState();
    doReturn(true).when(oldGenerator).isLeader();

    // Lease manager says we no longer hold the lease — leadership-loss condition.
    when(mocks.leaseManager.isLeader(matViewGenId)).thenReturn(false);

    // Construct a fresh "new" healthy generator with the same generationId. This simulates
    // the post-state of a same-genId replaceGenerator (Lucene-only change / oplog falloff).
    MaterializedViewGenerator newGenerator = mock(MaterializedViewGenerator.class);
    when(newGenerator.getIndexGeneration()).thenReturn(matViewIndexGen);
    doReturn(false).when(newGenerator).isLeader();
    doReturn(ReplicationIndexManager.State.INITIALIZING).when(newGenerator).getState();

    // Use reflection to swap the live map entry during the leadership-loss pass,
    // simulating a concurrent same-genId replaceGenerator. emitHeartbeat() runs two passes
    // (state-based release, then leadership-loss after lease renewal); both call
    // generator.getIndexGeneration() at the top of each iteration. Gate the swap to the
    // 2nd call so:
    //   • Pass 1's snapshot still contains oldGenerator (its release-branch conditions
    //     don't match anyway since STEADY_STATE isn't terminal).
    //   • Pass 2's snapshot still contains oldGenerator (taken before the 2nd Answer fires).
    //   • Just after pass 2 enters the iteration body, the swap installs newGenerator in
    //     the live map.
    //   • When transitionToFollower (or the proposed identity check) calls
    //     getMatViewGenerator(genId), the live map returns newGenerator. Identity check
    //     against oldGenerator must reject the transition.
    java.lang.reflect.Field mapField =
        MaterializedViewManager.class.getDeclaredField("managedMaterializedViewGenerators");
    mapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<UUID, MaterializedViewGenerator> liveMap =
        (Map<UUID, MaterializedViewGenerator>) mapField.get(mocks.manager);

    java.util.concurrent.atomic.AtomicInteger getIndexGenCallCount =
        new java.util.concurrent.atomic.AtomicInteger(0);
    doAnswer(
            inv -> {
              if (getIndexGenCallCount.incrementAndGet() == 2) {
                // 2nd call = top of pass 2's iteration. Snapshot was already taken;
                // mutate the live map now so the synchronized identity check sees the
                // new generator.
                liveMap.put(matViewUuid, newGenerator);
              }
              return matViewIndexGen;
            })
        .when(oldGenerator)
        .getIndexGeneration();

    Mockito.clearInvocations(mocks.materializedViewGeneratorFactory);

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();

    // transitionToFollower must NOT have fired: identity check rejected because
    // getMatViewGenerator(genId) returned newGenerator, not oldGenerator.
    //
    // If it HAD fired, the factory would have been called to create a new follower
    // generator. Verify that didn't happen.
    verify(mocks.materializedViewGeneratorFactory, never()).create(matViewIndexGen);

    // The healthy newGenerator must still be the value in the live map; not displaced by
    // a fresh follower instance.
    assertSame(
        "Same-genId race: leadership-loss heartbeat must not overwrite the new generator "
            + "installed by a concurrent replaceGenerator (would happen via "
            + "transitionToFollower's unconditional managedMaterializedViewGenerators.put).",
        newGenerator,
        liveMap.get(matViewUuid));
  }

  @Test
  public void dynamicLeader_heartbeat_skipsTransitionToFollower_whenStateFlipsTerminalMidCheck() {
    // Race: state flips non-terminal → terminal between pass 2's outer guard (outside the
    // manager monitor) and the synchronized identity check. The inner state re-check inside
    // the synchronized block (atomic with the identity check) catches this and skips
    // transitionToFollower; pass 1 of the next tick will release the lease.
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    verify(generator).becomeLeader();
    assertTrue(generator.isLeader());
    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();

    doReturn(true).when(generator).isLeader();
    when(mocks.leaseManager.isLeader(matViewGenId)).thenReturn(false);

    // Stateful getState() simulates the race:
    //   calls 1-2: STEADY_STATE (pass 1 release check, pass 2 outer guard)
    //   calls 3+:  FAILED       (pass 2 inner re-check sees the flip)
    java.util.concurrent.atomic.AtomicInteger getStateCallCount =
        new java.util.concurrent.atomic.AtomicInteger(0);
    doAnswer(
            inv ->
                getStateCallCount.incrementAndGet() <= 2
                    ? ReplicationIndexManager.State.STEADY_STATE
                    : ReplicationIndexManager.State.FAILED)
        .when(generator)
        .getState();

    Mockito.clearInvocations(mocks.materializedViewGeneratorFactory);

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();

    // Inner state re-check skips transitionToFollower — no new follower this tick.
    verify(mocks.materializedViewGeneratorFactory, never()).create(matViewIndexGen);
  }

  @Test
  public void dynamicLeader_heartbeat_doesNotReleaseLease_whenStateInitialSyncBackoff() {
    // INITIAL_SYNC_BACKOFF is the deliberate retry-wait state set by scheduleResync
    // (RIM:1098). The lease MUST be retained — the same generator will retry the sync
    // shortly. Releasing it would orphan every transient initial-sync failure.
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    doReturn(ReplicationIndexManager.State.INITIAL_SYNC_BACKOFF).when(generator).getState();
    doReturn(true).when(generator).isLeader();

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();

    verify(mocks.leaseManager, never()).releaseLeadership(matViewGenId);
  }

  // ==================== replaceGenerator pendingShutdowns Tests ====================

  @Test
  public void replaceGenerator_sameGenerationId_addsPendingShutdownAndSkipsLeaseDropAdd() {
    // When the same index is added again (same definitionVersion → same MaterializedView-
    // GenerationId), replaceGenerator should add to pendingShutdowns and skip the
    // lease drop+add cycle since the lease tracking is already correct.
    Mocks mocks = Mocks.create();

    var matViewIndexGen = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);
    Mockito.clearInvocations(mocks.leaseManager);

    // Add same index again (same generationId) → triggers replaceGenerator.
    mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    // pendingShutdowns should contain the generationId.
    assertTrue(
        "pendingShutdowns should contain the generationId after replaceGenerator",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));
    // leaseManager.drop and add should NOT be called (same generationId optimization).
    verify(mocks.leaseManager, never()).drop(any());
    verify(mocks.leaseManager, never()).add(any(), anyBoolean());
  }

  @Test
  public void replaceGenerator_differentGenerationId_addsPendingShutdownAndCallsLeaseDropAdd() {
    // When a higher-version index is added, replaceGenerator should add to pendingShutdowns
    // and call leaseManager.drop(old) + leaseManager.add(new, skipInitialSync).
    Mocks mocks = Mocks.create();

    var gen1 = MOCK_MAT_VIEW_GENERATION_ID;
    var matViewIndexGen1 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1));
    mocks.mockMaterializedViewGenerator(matViewIndexGen1);
    mocks.addIndexForReplication(matViewIndexGen1);
    Mockito.clearInvocations(mocks.leaseManager);

    // Add a higher-version index (different generationId) → triggers replaceGenerator.
    var gen2 =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var matViewIndexGen2 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2, 1));
    mocks.mockMaterializedViewGenerator(matViewIndexGen2);
    mocks.addIndexForReplication(matViewIndexGen2);

    MaterializedViewGenerationId newGenId = matViewIndexGen2.getGenerationId();
    // pendingShutdowns should contain the new generationId.
    assertTrue(
        "pendingShutdowns should contain the new generationId",
        mocks.manager.pendingShutdowns.containsKey(newGenId));
    // leaseManager.drop(old) and add(new, skipInitialSync) should be called.
    verify(mocks.leaseManager).drop(matViewIndexGen1.getGenerationId());
    verify(mocks.leaseManager).add(matViewIndexGen2, false);
  }

  @Test
  public void replaceGenerator_closesOldMatViewIndexOnVersionRoll_keepsItOnSameVersion()
      throws Exception {
    // Different-version replacement closes the old InitializedMaterializedViewIndex so its
    // per-index gauges (notably leaderStatus) are unregistered for the now-defunct
    // materialized view. Same-version replacement must skip the close because the new
    // instance shares the same label set and would unregister a live gauge.
    {
      Mocks mocks = Mocks.create();
      var matViewIndexGen1 = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
      InitializedMaterializedViewIndex oldMatViewIndex = matViewIndexGen1.getIndex();
      mocks.mockMaterializedViewGenerator(matViewIndexGen1);
      mocks.addIndexForReplication(matViewIndexGen1);

      var gen2 =
          new MaterializedViewGenerationId(
              MOCK_MAT_VIEW_GENERATION_ID.indexId,
              MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
      var matViewIndexGen2 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2, 1));
      mocks.mockMaterializedViewGenerator(matViewIndexGen2);
      mocks.addIndexForReplication(matViewIndexGen2);

      // Confirm replaceGenerator actually fired before asserting on close().
      assertTrue(
          "pendingShutdowns should contain the new generationId after replaceGenerator",
          mocks.manager.pendingShutdowns.containsKey(matViewIndexGen2.getGenerationId()));
      verify(oldMatViewIndex).close();
    }
    {
      Mocks mocks = Mocks.create();
      var matViewIndexGen = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
      InitializedMaterializedViewIndex oldMatViewIndex = matViewIndexGen.getIndex();
      mocks.mockMaterializedViewGenerator(matViewIndexGen);
      mocks.addIndexForReplication(matViewIndexGen);

      // Same generationId triggers same-version replacement.
      mocks.mockMaterializedViewGenerator(matViewIndexGen);
      mocks.addIndexForReplication(matViewIndexGen);

      // Confirm replaceGenerator actually fired (same-genId branch populates pendingShutdowns
      // before deferring leadership to refreshStatus).
      assertTrue(
          "pendingShutdowns should contain the generationId after same-genId replaceGenerator",
          mocks.manager.pendingShutdowns.containsKey(matViewIndexGen.getGenerationId()));
      verify(oldMatViewIndex, never()).close();
    }
  }

  @Test
  public void replaceGenerator_pendingShutdownSetBeforeFactoryCreate() {
    // Verify that pendingShutdowns.put() happens BEFORE factory.create() in
    // replaceGenerator, matching the transitionToFollower pattern from CLOUDP-393734.
    Mocks mocks = Mocks.create();

    var matViewIndexGen = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();

    // On the second create() call (from replaceGenerator), verify pendingShutdowns is set.
    when(mocks.materializedViewGeneratorFactory.create(matViewIndexGen))
        .thenAnswer(
            invocation -> {
              assertTrue(
                  "pendingShutdowns should contain generationId before factory.create()",
                  mocks.manager.pendingShutdowns.containsKey(matViewGenId));
              MaterializedViewGenerator newGen = mock(MaterializedViewGenerator.class);
              when(newGen.getIndexGeneration()).thenReturn(matViewIndexGen);
              when(newGen.shutdown()).thenReturn(CompletableFuture.completedFuture(null));
              return newGen;
            });

    // Add same index again → replaceGenerator.
    mocks.addIndexForReplication(matViewIndexGen);

    // Verify factory.create() was called (for the replaceGenerator path).
    verify(mocks.materializedViewGeneratorFactory, atLeast(2)).create(matViewIndexGen);
  }

  @Test
  public void replaceGenerator_rollsPendingShutdownOnFactoryFailure() {
    // If factory.create() throws in replaceGenerator, pendingShutdowns must be cleaned up.
    Mocks mocks = Mocks.create();

    var matViewIndexGen = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();

    // Make factory.create() throw on the second call (from replaceGenerator).
    when(mocks.materializedViewGeneratorFactory.create(matViewIndexGen))
        .thenThrow(new RuntimeException("factory failure"));

    try {
      mocks.addIndexForReplication(matViewIndexGen);
    } catch (RuntimeException expected) {
      // Expected — replaceGenerator re-throws after rollback.
    }

    assertFalse(
        "pendingShutdowns should be rolled back on factory failure",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));
  }

  @Test
  public void refreshStatus_activatesDeferredLeadershipFromReplaceGenerator() {
    // After replaceGenerator, the new generator is in leaderGenerationIds (we own the lease)
    // but becomeLeader() was not called (deferred). When refreshStatus clears the
    // pendingShutdowns entry (shutdown done + queue clean), it should activate leadership.
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();

    // Simulate the state after replaceGenerator: pendingShutdowns has a completed future,
    // queue is clean, we own the lease, but the generator has not been activated.
    mocks.manager.pendingShutdowns.put(matViewGenId, COMPLETED_FUTURE);
    when(mocks.materializedViewGeneratorFactory.hasQueueEntry(matViewGenId)).thenReturn(false);
    when(mocks.leaseManager.isLeader(matViewGenId)).thenReturn(true);
    doReturn(false).when(generator).isLeader();

    // Return empty poll results — the gen is in leaderGenerationIds, not followerGenerationIds,
    // so it won't appear in acquirableLeases.
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(LeaseManager.FollowerPollResult.EMPTY);

    // Run refreshStatus.
    mocks.runnableCaptor.orElseThrow().getValue().run();

    // pendingShutdowns should be cleared and becomeLeader() called.
    assertFalse(
        "pendingShutdowns should be cleared",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));
    verify(generator).becomeLeader();
  }

  @Test
  public void refreshStatus_skipsDeferredLeadershipWhenQueueDirty() {
    // Same scenario as above, but the queue still has a stale entry from the old generator.
    // refreshStatus should NOT activate leadership and should keep the pendingShutdowns entry.
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();

    // Simulate: shutdown done, but queue still has stale entry.
    mocks.manager.pendingShutdowns.put(matViewGenId, COMPLETED_FUTURE);
    when(mocks.materializedViewGeneratorFactory.hasQueueEntry(matViewGenId)).thenReturn(true);
    when(mocks.leaseManager.isLeader(matViewGenId)).thenReturn(true);

    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(LeaseManager.FollowerPollResult.EMPTY);

    mocks.runnableCaptor.orElseThrow().getValue().run();

    // Should NOT activate — queue is dirty.
    assertTrue(
        "pendingShutdowns should be retained",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));
    verify(generator, never()).becomeLeader();
  }

  @Test
  public void replaceGenerator_dynamicLeader_doesNotCallBecomeLeaderInThenRun() {
    // Under dynamic leader election, replaceGenerator should NOT call becomeLeader()
    // directly. Leadership is deferred to refreshStatus() via pendingShutdowns.
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator gen1 = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    when(mocks.leaseManager.isLeader(matViewGenId)).thenReturn(true);

    // Add same index again → triggers replaceGenerator.
    MaterializedViewGenerator gen2 = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    Mockito.clearInvocations(gen1, gen2);
    mocks.addIndexForReplication(matViewIndexGen);

    // becomeLeader should NOT be called on either generator by replaceGenerator.
    // It should only be called later by refreshStatus.
    verify(gen1, never()).becomeLeader();
    verify(gen2, never()).becomeLeader();
    // pendingShutdowns should be populated.
    assertTrue(mocks.manager.pendingShutdowns.containsKey(matViewGenId));
  }

  @Test
  public void replaceGenerator_endToEnd_refreshStatusActivatesLeadership() {
    // End-to-end: add(a0) → add(a1 same version) → refreshStatus → becomeLeader on gen2.
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    // Simulate: we own the lease (leader for this generation).
    when(mocks.leaseManager.isLeader(matViewGenId)).thenReturn(true);

    // Add same index again (attempt 1) → replaceGenerator creates gen2.
    MaterializedViewGenerator gen2 = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplicationWithAttempt(matViewIndexGen, 1);

    // gen2 should not be leader yet (deferred).
    verify(gen2, never()).becomeLeader();
    assertTrue(mocks.manager.pendingShutdowns.containsKey(matViewGenId));

    // Simulate queue is clean.
    when(mocks.materializedViewGeneratorFactory.hasQueueEntry(matViewGenId)).thenReturn(false);
    // gen2 is in the map but not activated.
    doReturn(false).when(gen2).isLeader();

    // Empty poll results — gen is in leaderGenerationIds, not followerGenerationIds.
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(LeaseManager.FollowerPollResult.EMPTY);

    // Run refreshStatus — should clear pendingShutdowns and activate gen2.
    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertFalse(mocks.manager.pendingShutdowns.containsKey(matViewGenId));
    verify(gen2).becomeLeader();
  }

  @Test
  public void dropIndex_cleansPendingShutdownsFromReplaceGenerator() throws Exception {
    // After replaceGenerator populates pendingShutdowns, dropIndex should clean it up
    // via cleanUpGenerationIdStates.
    Mocks mocks = Mocks.create();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();

    // Add same index again → replaceGenerator populates pendingShutdowns.
    mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);
    assertTrue(
        "pendingShutdowns should be populated after replaceGenerator",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));

    // Drop the index — cleanUpGenerationIdStates should remove from pendingShutdowns.
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    assertFalse(
        "pendingShutdowns should be cleaned up after dropIndex",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));
  }

  // ==================== Async Lifecycle Coverage Tests ====================

  @Test
  public void replaceGenerator_sameGenId_staticLeader_refreshStatusActivatesLeadership() {
    Mocks mocks = Mocks.createStaticLeaderWithRefreshCapture();

    var matViewIndexGen = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator gen1 = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    // Replace with same index (same generationId) — defers leadership to refreshStatus.
    MaterializedViewGenerator gen2 = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    Mockito.clearInvocations(gen1, gen2);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    // Leadership should NOT be activated yet (deferred for same-genId).
    verify(gen2, never()).becomeLeader();
    assertTrue(mocks.manager.pendingShutdowns.containsKey(matViewGenId));

    // Simulate queue is clean (shutdown was instant via COMPLETED_FUTURE mock).
    when(mocks.materializedViewGeneratorFactory.hasQueueEntry(matViewGenId)).thenReturn(false);
    // gen2 is not yet activated as leader.
    doReturn(false).when(gen2).isLeader();

    // Run refreshStatus — should clear pendingShutdowns and activate leadership
    // via activateStaticLeadership -> isLeader check -> becomeLeader.
    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertFalse(
        "pendingShutdowns should be cleared",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));
    verify(gen2).becomeLeader();
  }

  @Test
  public void replaceGenerator_differentGenId_becomeLeaderThrowsInThenRun_refreshStatusCleans() {
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    // Add gen1.
    var gen1Id = MOCK_MAT_VIEW_GENERATION_ID;
    var matViewIndexGen1 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1Id));
    MaterializedViewGenerator gen1 = mocks.mockMaterializedViewGenerator(matViewIndexGen1);
    mocks.addIndexForReplication(matViewIndexGen1);

    // Make gen1's shutdown return a NOT-YET-COMPLETED future.
    CompletableFuture<Void> slowShutdownFuture = new CompletableFuture<>();
    doReturn(slowShutdownFuture).when(gen1).shutdown();

    // Add gen2 (higher version -> different genId -> replaceGenerator).
    var gen2Id =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var matViewIndexGen2 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2Id, 1));
    MaterializedViewGenerator gen2 = mocks.mockMaterializedViewGenerator(matViewIndexGen2);

    // Make gen2.becomeLeader() throw (simulating init failure in the thenRun callback).
    doThrow(new RuntimeException("becomeLeader failed")).when(gen2).becomeLeader();

    // Simulate that we were leader for the old generation.
    when(mocks.leaseManager.isLeader(gen1Id)).thenReturn(true);

    mocks.addIndexForReplication(matViewIndexGen2);

    MaterializedViewGenerationId newGenId = matViewIndexGen2.getGenerationId();
    // pendingShutdowns should have an incomplete future (shutdown not done yet).
    assertTrue(mocks.manager.pendingShutdowns.containsKey(newGenId));

    // Complete the old generator's shutdown — thenRun fires, becomeLeader throws.
    // The shutdownFuture completes exceptionally because thenRun's exception propagates.
    slowShutdownFuture.complete(null);

    // pendingShutdowns now has a completed (exceptionally) future.
    assertTrue(
        "pendingShutdowns future should be done",
        mocks.manager.pendingShutdowns.get(newGenId).isDone());

    // Run refreshStatus — two-phase cleanup should catch the exception and remove the entry.
    when(mocks.materializedViewGeneratorFactory.hasQueueEntry(newGenId)).thenReturn(false);
    when(mocks.leaseManager.isLeader(newGenId)).thenReturn(true);
    doReturn(false).when(gen2).isLeader();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(LeaseManager.FollowerPollResult.EMPTY);

    // refreshStatus's two-phase cleanup will try becomeLeader again.
    // Reset the mock to succeed this time.
    Mockito.reset(gen2);
    when(gen2.getIndexGeneration()).thenReturn(matViewIndexGen2);
    when(gen2.shutdown()).thenReturn(CompletableFuture.completedFuture(null));
    doReturn(false).when(gen2).isLeader();

    mocks.runnableCaptor.orElseThrow().getValue().run();

    // The entry should be removed from pendingShutdowns regardless.
    assertFalse(
        "pendingShutdowns should be cleared after refreshStatus",
        mocks.manager.pendingShutdowns.containsKey(newGenId));
    // becomeLeader should have been called by refreshStatus's deferred activation.
    verify(gen2).becomeLeader();
  }

  @Test
  public void replaceGenerator_refreshStatusDuringSlowFactoryCreate_skipsCleanup() {
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    var matViewIndexGen = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();

    // Simulate the state DURING factory.create(): pendingShutdowns has an incomplete
    // placeholder future (put at line 631 before factory.create() is called).
    // The new generator is NOT yet in the map.
    mocks.manager.pendingShutdowns.put(matViewGenId, new CompletableFuture<>());

    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(LeaseManager.FollowerPollResult.EMPTY);

    // Run refreshStatus while "factory.create() is in progress".
    mocks.runnableCaptor.orElseThrow().getValue().run();

    // The incomplete placeholder should still be in pendingShutdowns (not cleaned up).
    assertTrue(
        "pendingShutdowns should still contain placeholder",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));
  }

  @Test
  public void replaceGenerator_differentGenId_asFollower_doesNotCallBecomeLeader() {
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    // Add gen1 as follower (isLeader returns false).
    var gen1Id = MOCK_MAT_VIEW_GENERATION_ID;
    var matViewIndexGen1 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1Id));
    MaterializedViewGenerator gen1 = mocks.mockMaterializedViewGenerator(matViewIndexGen1);
    mocks.addIndexForReplication(matViewIndexGen1);

    // Ensure we are NOT the leader for this generation.
    when(mocks.leaseManager.isLeader(gen1Id)).thenReturn(false);

    // Add gen2 (higher version -> different genId -> replaceGenerator with wasLeader=false).
    var gen2Id =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var matViewIndexGen2 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2Id, 1));
    MaterializedViewGenerator gen2 = mocks.mockMaterializedViewGenerator(matViewIndexGen2);
    Mockito.clearInvocations(gen1, gen2);
    mocks.addIndexForReplication(matViewIndexGen2);

    // becomeLeader should NOT have been called on either generator —
    // we were not leader for the old generation, so leadership should not transfer.
    verify(gen1, never()).becomeLeader();
    verify(gen2, never()).becomeLeader();

    // But lease tracking should still be updated.
    verify(mocks.leaseManager).drop(gen1Id);
    verify(mocks.leaseManager).add(matViewIndexGen2, false);
  }

  @Test
  public void dropIndex_whileReplaceShutdownInFlight_thenRunFiresAfterDrop() throws Exception {
    Mocks mocks = Mocks.create();

    var matViewIndexGen = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator gen1 = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    // Make gen1's shutdown return a NOT-YET-COMPLETED future.
    CompletableFuture<Void> slowShutdownFuture = new CompletableFuture<>();
    doReturn(slowShutdownFuture).when(gen1).shutdown();

    // Replace with same index -> replaceGenerator (same genId, deferred leadership).
    mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    // pendingShutdowns has the shutdown future (which is incomplete).
    assertTrue(mocks.manager.pendingShutdowns.containsKey(matViewGenId));

    // Drop the index BEFORE the old generator's shutdown completes.
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    // pendingShutdowns should be cleaned up by dropIndex.
    assertFalse(
        "pendingShutdowns should be cleaned by dropIndex",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));

    // Now complete the old generator's shutdown — thenRun fires.
    // This should NOT throw or cause any bad state, even though the generation
    // has already been dropped. The lease operations are idempotent.
    slowShutdownFuture.complete(null);

    // Verify no exceptions — the test completes successfully if we get here.
    // The thenRun's same-genId path just logs "deferring leadership to refreshStatus",
    // which is harmless since the generation is already dropped.
  }

  @Test
  public void replaceGenerator_consecutiveSameGenId_secondReplaceOverwritesPendingShutdown() {
    Mocks mocks = Mocks.create();

    var matViewIndexGen = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator gen1 = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    // First replacement: gen1 -> gen2 (same genId).
    MaterializedViewGenerator gen2 = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    assertTrue(mocks.manager.pendingShutdowns.containsKey(matViewGenId));

    // Second replacement: gen2 -> gen3 (same genId again).
    mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    // pendingShutdowns should still have the genId (overwritten by second replace).
    assertTrue(
        "pendingShutdowns should still contain the genId",
        mocks.manager.pendingShutdowns.containsKey(matViewGenId));

    // Both gen1 and gen2 should have been shut down.
    verify(gen1).shutdown();
    verify(gen2).shutdown();

    // Only gen3 should be the active generator now.
    var activeGen = mocks.manager.getMatViewGenerator(matViewGenId);
    assertTrue("Active generator should exist", activeGen.isPresent());
    // Factory should have been called 3 times total (gen1 + gen2 + gen3).
    verify(mocks.materializedViewGeneratorFactory, times(3)).create(matViewIndexGen);
  }

  // ==================== getMatViewGenerator Tests ====================

  @Test
  public void getMatViewGenerator_returnsGenerator_whenMetadataAndGeneratorExist() {
    Mocks mocks = Mocks.create();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    Optional<MaterializedViewGenerator> result = mocks.manager.getMatViewGenerator(matViewGenId);
    assertTrue("Generator should be present", result.isPresent());
  }

  @Test
  public void getMatViewGenerator_returnsEmpty_whenNoMetadata() {
    Mocks mocks = Mocks.create();

    // Query for a generationId that was never added — metadata catalog returns empty.
    MaterializedViewGenerationId unknownGenId =
        new MaterializedViewGenerationId(
            new ObjectId(), new MaterializedViewGeneration(Generation.CURRENT));
    when(mocks.metadataCatalog.getMetadataIfPresent(unknownGenId)).thenReturn(Optional.empty());

    Optional<MaterializedViewGenerator> result = mocks.manager.getMatViewGenerator(unknownGenId);
    assertFalse("Generator should be empty when no metadata exists", result.isPresent());
  }

  // ==================== Dynamic Leader with Unexpired Lease Tests ====================

  @Test
  public void dynamicLeader_restartWithUnexpiredLease_activatesLeadershipImmediately() {
    // Tests the code path in createNewGenerator() where we already own an unexpired lease
    // after restart. The isLeader() check after add() should return true, and becomeLeader()
    // should be called immediately on the generator.
    Mocks mocks = Mocks.createDynamicLeaderWithUnexpiredLease();

    // Add an index - this simulates restart where we already own the lease
    MaterializedViewIndexGeneration materializedViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator materializedViewGenerator =
        mocks.mockMaterializedViewGenerator(materializedViewIndexGeneration);
    mocks.addIndexForReplication(materializedViewIndexGeneration);

    // Verify that leaseManager.add() was called
    verify(mocks.leaseManager).add(materializedViewIndexGeneration, false);
    // Verify that becomeLeader() was called on the generator because we own the lease
    verify(materializedViewGenerator).becomeLeader();
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
    final MaterializedViewCollectionMetadataCatalog metadataCatalog;

    MaterializedViewManager manager;

    private Mocks(
        NamedExecutorService executorService,
        IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
        DecodingWorkScheduler decodingScheduler,
        MongotCursorManager cursorManager,
        Map<String, ClientSessionRecord> clientSessionRecordMap,
        InitialSyncQueue initialSyncQueue,
        SteadyStateManager steadyStateManager,
        MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory,
        NamedScheduledExecutorService commitExecutor,
        NamedScheduledExecutorService heartbeatExecutor,
        NamedScheduledExecutorService statusRefreshExecutor,
        NamedScheduledExecutorService optimeUpdaterExecutor,
        LeaseManager leaseManager,
        IndexStatus expectedStatus,
        Optional<ArgumentCaptor<Runnable>> runnableCaptor,
        MaterializedViewCollectionMetadataCatalog metadataCatalog) {
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
      this.metadataCatalog = metadataCatalog;

      SyncSourceConfig syncSourceConfig =
          SyncSourceConfig.builder()
              .mongodSingleHostReplicationUri(
                  ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newString"))
              .mongodClusterReplicationUri(
                  ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newString"))
              .mongodClusterReadWriteUri(
                  ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newString"))
              .build();
      AutoEmbeddingMongoClient autoEmbeddingMongoClient =
          new AutoEmbeddingMongoClient(
              Optional.of(syncSourceConfig),
              new SimpleMeterRegistry(),
              AutoEmbeddingMaterializedViewConfig.getDefault());

      this.managerSupplier =
          () ->
              new MaterializedViewManager(
                  executorService,
                  indexingWorkSchedulerFactory,
                  autoEmbeddingMongoClient,
                  decodingScheduler,
                  materializedViewGeneratorFactory,
                  commitExecutor,
                  heartbeatExecutor,
                  statusRefreshExecutor,
                  optimeUpdaterExecutor,
                  this.meterRegistry,
                  leaseManager,
                  metadataCatalog,
                  AutoEmbeddingMaterializedViewConfig.getDefault());
      this.manager = this.managerSupplier.get();
      this.manager.setIsReplicationEnabled(true);
    }

    public void recreateManager() {
      this.manager = this.managerSupplier.get();
      this.manager.setIsReplicationEnabled(true);
    }

    private static Mocks create() {
      NamedExecutorService executorService = mock(NamedExecutorService.class);
      when(executorService.getName()).thenReturn("indexing");
      try {
        when(executorService.awaitTermination(anyLong(), any())).thenReturn(true);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

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

      MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory =
          mock(MaterializedViewManager.MaterializedViewGeneratorFactory.class);
      when(materializedViewGeneratorFactory.shutdown())
          .thenReturn(CompletableFuture.completedFuture(null));
      when(materializedViewGeneratorFactory.isInitialized()).thenReturn(true);
      when(indexingWorkSchedulerFactory.getIndexingWorkSchedulers()).thenReturn(Map.of());

      NamedScheduledExecutorService commitExecutor = mockScheduledExecutor("index-commit");

      NamedScheduledExecutorService heartbeatExecutor =
          mockScheduledExecutor("mat-view-leader-heartbeat");

      NamedScheduledExecutorService statusRefreshExecutor =
          mockScheduledExecutor("mat-view-status-refresh");

      NamedScheduledExecutorService optimeUpdaterExecutor =
          mockScheduledExecutor("mat-view-optime-updater");

      InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
      when(initializedIndexCatalog.getIndex(any()))
          .thenReturn(Optional.of(mock(InitializedSearchIndex.class)));

      // Use StaticLeaderLeaseManager mock so that activateStaticLeadership() is called
      // (it's guarded by instanceof StaticLeaderLeaseManager check)
      StaticLeaderLeaseManager leaseManager = mock(StaticLeaderLeaseManager.class);
      doNothing().when(leaseManager).drop(any());
      when(leaseManager.dropLease(any(String.class))).thenReturn(COMPLETED_FUTURE);
      when(leaseManager.isLeader(any())).thenReturn(true);
      // Mock getLeaderGenerationIds to return a non-empty set for heartbeat test
      MaterializedViewGenerationId leaderGenId =
          new MaterializedViewGenerationId(
              GenerationIdBuilder.create().indexId,
              new MaterializedViewGeneration(Generation.CURRENT));
      when(leaseManager.getLeaderGenerationIds()).thenReturn(Set.of(leaderGenId));

      return new Mocks(
          executorService,
          indexingWorkSchedulerFactory,
          decodingWorkScheduler,
          cursorManager,
          Map.of("test", new ClientSessionRecord(syncMongoClient, sessionRefresher)),
          initialSyncQueue,
          steadyStateManager,
          materializedViewGeneratorFactory,
          commitExecutor,
          heartbeatExecutor,
          statusRefreshExecutor,
          optimeUpdaterExecutor,
          leaseManager,
          IndexStatus.unknown(), // expectedStatus for leader mode
          Optional.empty(),
          createMockMetadataCatalog());
    }

    /**
     * Creates Mocks configured as a StaticLeaderLeaseManager with the status refresh runnable
     * captured. This is identical to {@link #create()} except the statusRefreshExecutor captures
     * the refresh runnable so tests can invoke refreshStatus() directly.
     */
    @SuppressWarnings("unchecked")
    private static Mocks createStaticLeaderWithRefreshCapture() {
      NamedExecutorService executorService = mock(NamedExecutorService.class);
      when(executorService.getName()).thenReturn("indexing");
      try {
        when(executorService.awaitTermination(anyLong(), any())).thenReturn(true);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

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

      MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory =
          mock(MaterializedViewManager.MaterializedViewGeneratorFactory.class);
      when(materializedViewGeneratorFactory.shutdown())
          .thenReturn(CompletableFuture.completedFuture(null));
      when(indexingWorkSchedulerFactory.getIndexingWorkSchedulers()).thenReturn(Map.of());

      NamedScheduledExecutorService commitExecutor = mockScheduledExecutor("index-commit");

      NamedScheduledExecutorService heartbeatExecutor =
          mockScheduledExecutor("mat-view-leader-heartbeat");

      // Status refresh executor — capture the refresh runnable
      NamedScheduledExecutorService statusRefreshExecutor =
          mock(NamedScheduledExecutorService.class);
      when(statusRefreshExecutor.getName()).thenReturn("mat-view-status-refresh");
      ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
      ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
      doReturn(mockFuture)
          .when(statusRefreshExecutor)
          .scheduleWithFixedDelay(runnableCaptor.capture(), anyLong(), anyLong(), any());
      try {
        when(statusRefreshExecutor.awaitTermination(anyLong(), any())).thenReturn(true);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      NamedScheduledExecutorService optimeUpdaterExecutor =
          mockScheduledExecutor("mat-view-optime-updater");

      InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
      when(initializedIndexCatalog.getIndex(any()))
          .thenReturn(Optional.of(mock(InitializedSearchIndex.class)));

      // Use StaticLeaderLeaseManager mock so that activateStaticLeadership() is called
      // (it's guarded by instanceof StaticLeaderLeaseManager check)
      StaticLeaderLeaseManager leaseManager = mock(StaticLeaderLeaseManager.class);
      doNothing().when(leaseManager).drop(any());
      when(leaseManager.dropLease(any(String.class))).thenReturn(COMPLETED_FUTURE);
      when(leaseManager.isLeader(any())).thenReturn(true);
      // Mock getLeaderGenerationIds to return a non-empty set for heartbeat test
      MaterializedViewGenerationId leaderGenId =
          new MaterializedViewGenerationId(
              GenerationIdBuilder.create().indexId,
              new MaterializedViewGeneration(Generation.CURRENT));
      when(leaseManager.getLeaderGenerationIds()).thenReturn(Set.of(leaderGenId));
      // Mock pollFollowerStatuses for refreshStatus (leaders return EMPTY)
      when(leaseManager.pollFollowerStatuses()).thenReturn(LeaseManager.FollowerPollResult.EMPTY);

      return new Mocks(
          executorService,
          indexingWorkSchedulerFactory,
          decodingWorkScheduler,
          cursorManager,
          Map.of("test", new ClientSessionRecord(syncMongoClient, sessionRefresher)),
          initialSyncQueue,
          steadyStateManager,
          materializedViewGeneratorFactory,
          commitExecutor,
          heartbeatExecutor,
          statusRefreshExecutor,
          optimeUpdaterExecutor,
          leaseManager,
          IndexStatus.unknown(), // expectedStatus for leader mode
          Optional.of(runnableCaptor),
          createMockMetadataCatalog());
    }

    private static Mocks createFollower() {
      // Separate mock schedulers for status refresh and optime updater
      NamedScheduledExecutorService statusRefreshScheduler =
          mock(NamedScheduledExecutorService.class);
      NamedScheduledExecutorService optimeUpdaterScheduler =
          mock(NamedScheduledExecutorService.class);
      ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
      // Use StaticLeaderLeaseManager mock so that activateStaticLeadership() is called
      // (it's guarded by instanceof StaticLeaderLeaseManager check)
      StaticLeaderLeaseManager mockLeaseManager = mock(StaticLeaderLeaseManager.class);
      // Mock isLeader to return false for follower mode
      when(mockLeaseManager.isLeader(any())).thenReturn(false);
      doNothing().when(mockLeaseManager).drop(any());
      when(mockLeaseManager.dropLease(any(String.class))).thenReturn(COMPLETED_FUTURE);
      // Track added generation IDs to return from getFollowerGenerationIds
      Set<MaterializedViewGenerationId> addedGenerationIds = ConcurrentHashMap.newKeySet();
      doAnswer(
              invocation -> {
                MaterializedViewIndexGeneration indexGen = invocation.getArgument(0);
                addedGenerationIds.add(indexGen.getGenerationId());
                return null;
              })
          .when(mockLeaseManager)
          .add(any(), anyBoolean());
      // Mock getFollowerGenerationIds to return all added generation IDs (since all are followers)
      when(mockLeaseManager.getFollowerGenerationIds())
          .thenAnswer(invocation -> addedGenerationIds);
      // Mock pollFollowerStatuses to return steady status for all added generation IDs
      when(mockLeaseManager.pollFollowerStatuses())
          .thenAnswer(
              invocation -> {
                Map<MaterializedViewGenerationId, IndexStatus> result = new HashMap<>();
                for (MaterializedViewGenerationId genId : addedGenerationIds) {
                  result.put(genId, IndexStatus.steady());
                }
                return new LeaseManager.FollowerPollResult(result, Set.of());
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
                when(mockGenerator.getState())
                    .thenReturn(ReplicationIndexManager.State.INITIAL_SYNC);
                when(mockGenerator.shutdown()).thenReturn(CompletableFuture.completedFuture(null));
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
          mockGeneratorFactory,
          mock(NamedScheduledExecutorService.class),
          mock(NamedScheduledExecutorService.class),
          statusRefreshScheduler, // statusRefreshExecutor - captures runnable
          optimeUpdaterScheduler, // optimeUpdaterExecutor - separate mock
          mockLeaseManager,
          IndexStatus.steady(), // expectedStatus for follower mode
          Optional.of(runnableCaptor),
          createMockMetadataCatalog());
    }

    /**
     * Creates Mocks configured for dynamic leader election where we already own an unexpired lease.
     * This simulates restart with an existing lease - when add() is called, isLeader() returns true
     * immediately, triggering the leadership activation path in createNewGenerator().
     */
    private static Mocks createDynamicLeaderWithUnexpiredLease() {
      // Use a plain LeaseManager mock (NOT StaticLeaderLeaseManager) so the dynamic
      // leader election path in createNewGenerator() is tested.
      return createDynamicLeader(LeaseManager.class);
    }

    /**
     * Creates Mocks with a DynamicLeaderLeaseManager mock so the instanceof check in
     * emitHeartbeat() passes, enabling tests for the transitionToFollower heartbeat path.
     */
    private static Mocks createDynamicLeaderWithDynamicLeaseManager() {
      return createDynamicLeader(DynamicLeaderLeaseManager.class);
    }

    /**
     * Shared setup for dynamic leader mocks, parameterized by lease manager type. Using a plain
     * LeaseManager mock tests the generic dynamic leader path; using DynamicLeaderLeaseManager
     * additionally enables the instanceof-guarded heartbeat and refreshStatus paths.
     */
    private static Mocks createDynamicLeader(Class<? extends LeaseManager> leaseManagerType) {
      NamedExecutorService executorService = mock(NamedExecutorService.class);
      when(executorService.getName()).thenReturn("indexing");
      try {
        when(executorService.awaitTermination(anyLong(), any())).thenReturn(true);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory =
          mock(MaterializedViewManager.MaterializedViewGeneratorFactory.class);

      NamedScheduledExecutorService commitExecutor = mockScheduledExecutor("index-commit");

      NamedScheduledExecutorService heartbeatExecutor =
          mockScheduledExecutor("mat-view-leader-heartbeat");

      NamedScheduledExecutorService statusRefreshExecutor =
          mockScheduledExecutor("mat-view-status-refresh");

      NamedScheduledExecutorService optimeUpdaterExecutor =
          mockScheduledExecutor("mat-view-optime-updater");

      InitialSyncQueue initialSyncQueue = mock(InitialSyncQueue.class);
      when(initialSyncQueue.shutdown()).thenReturn(COMPLETED_FUTURE);

      SteadyStateManager steadyStateManager = mock(SteadyStateManager.class);
      when(steadyStateManager.shutdown()).thenReturn(COMPLETED_FUTURE);

      LeaseManager mockLeaseManager = mock(leaseManagerType);
      doNothing().when(mockLeaseManager).drop(any());
      when(mockLeaseManager.dropLease(any(String.class))).thenReturn(COMPLETED_FUTURE);

      // Track added generation IDs and mark them as leaders (simulating unexpired lease)
      Set<GenerationId> leaderGenerationIds = ConcurrentHashMap.newKeySet();
      doAnswer(
              invocation -> {
                IndexGeneration indexGen = invocation.getArgument(0);
                leaderGenerationIds.add(indexGen.getGenerationId());
                return null;
              })
          .when(mockLeaseManager)
          .add(any(), anyBoolean());

      when(mockLeaseManager.isLeader(any()))
          .thenAnswer(inv -> leaderGenerationIds.contains(inv.getArgument(0)));
      when(mockLeaseManager.getLeaderGenerationIds()).thenAnswer(inv -> leaderGenerationIds);

      return new Mocks(
          executorService,
          mock(IndexingWorkSchedulerFactory.class),
          mock(DecodingWorkScheduler.class),
          mock(MongotCursorManager.class),
          Map.of(),
          initialSyncQueue,
          steadyStateManager,
          materializedViewGeneratorFactory,
          commitExecutor,
          heartbeatExecutor,
          statusRefreshExecutor,
          optimeUpdaterExecutor,
          mockLeaseManager,
          IndexStatus.unknown(),
          Optional.empty(),
          createMockMetadataCatalog());
    }

    /**
     * Creates Mocks configured for dynamic leader election as a follower with acquirable leases.
     * Uses a plain LeaseManager mock (not StaticLeaderLeaseManager) so the dynamic leader election
     * path in refreshStatus() is exercised. The status refresh runnable is captured so tests can
     * invoke it directly and verify acquirable lease handling.
     */
    private static Mocks createDynamicFollowerWithAcquirableLeases() {
      NamedExecutorService executorService = mock(NamedExecutorService.class);
      when(executorService.getName()).thenReturn("indexing");
      try {
        when(executorService.awaitTermination(anyLong(), any())).thenReturn(true);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      InitialSyncQueue initialSyncQueue = mock(InitialSyncQueue.class);
      when(initialSyncQueue.shutdown()).thenReturn(COMPLETED_FUTURE);

      SteadyStateManager steadyStateManager = mock(SteadyStateManager.class);
      when(steadyStateManager.shutdown()).thenReturn(COMPLETED_FUTURE);

      MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory =
          mock(MaterializedViewManager.MaterializedViewGeneratorFactory.class);
      when(materializedViewGeneratorFactory.shutdown())
          .thenReturn(CompletableFuture.completedFuture(null));
      when(materializedViewGeneratorFactory.isInitialized()).thenReturn(true);

      NamedScheduledExecutorService commitExecutor = mockScheduledExecutor("index-commit");
      NamedScheduledExecutorService heartbeatExecutor =
          mockScheduledExecutor("mat-view-leader-heartbeat");
      NamedScheduledExecutorService optimeUpdaterExecutor =
          mockScheduledExecutor("mat-view-optime-updater");

      // Status refresh executor — capture the refresh runnable
      NamedScheduledExecutorService statusRefreshExecutor =
          mock(NamedScheduledExecutorService.class);
      when(statusRefreshExecutor.getName()).thenReturn("mat-view-status-refresh");
      ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
      ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
      doReturn(mockFuture)
          .when(statusRefreshExecutor)
          .scheduleWithFixedDelay(runnableCaptor.capture(), anyLong(), anyLong(), any());
      try {
        when(statusRefreshExecutor.awaitTermination(anyLong(), any())).thenReturn(true);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      // Use a DynamicLeaderLeaseManager mock so the instanceof check in refreshStatus() passes
      // and the acquirable leases loop is exercised
      DynamicLeaderLeaseManager mockLeaseManager = mock(DynamicLeaderLeaseManager.class);
      doNothing().when(mockLeaseManager).drop(any());
      when(mockLeaseManager.dropLease(any(String.class))).thenReturn(COMPLETED_FUTURE);
      when(mockLeaseManager.isLeader(any())).thenReturn(false);
      when(mockLeaseManager.getLeaderGenerationIds()).thenReturn(Set.of());

      // Track added generation IDs to return from getFollowerGenerationIds
      Set<MaterializedViewGenerationId> addedGenerationIds = ConcurrentHashMap.newKeySet();
      doAnswer(
              invocation -> {
                MaterializedViewIndexGeneration indexGen = invocation.getArgument(0);
                addedGenerationIds.add(indexGen.getGenerationId());
                return null;
              })
          .when(mockLeaseManager)
          .add(any(), anyBoolean());
      when(mockLeaseManager.getFollowerGenerationIds())
          .thenAnswer(invocation -> addedGenerationIds);
      // Default pollFollowerStatuses — no acquirable leases. Tests override this per-case.
      when(mockLeaseManager.pollFollowerStatuses())
          .thenReturn(new LeaseManager.FollowerPollResult(Map.of(), Set.of()));

      return new Mocks(
          executorService,
          mock(IndexingWorkSchedulerFactory.class),
          mock(DecodingWorkScheduler.class),
          mock(MongotCursorManager.class),
          Map.of(),
          initialSyncQueue,
          steadyStateManager,
          materializedViewGeneratorFactory,
          commitExecutor,
          heartbeatExecutor,
          statusRefreshExecutor,
          optimeUpdaterExecutor,
          mockLeaseManager,
          IndexStatus.unknown(),
          Optional.of(runnableCaptor),
          createMockMetadataCatalog());
    }

    @SuppressWarnings("unchecked")
    private static NamedScheduledExecutorService mockScheduledExecutor(String name) {
      NamedScheduledExecutorService executor = mock(NamedScheduledExecutorService.class);
      when(executor.getName()).thenReturn(name);
      ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
      Mockito.doReturn(mockFuture)
          .when(executor)
          .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any());
      try {
        when(executor.awaitTermination(anyLong(), any())).thenReturn(true);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return executor;
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
                  mock(InitializedMaterializedViewIndex.class),
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
      // Mock becomeLeader() to set isLeader=true without starting real async tasks.
      // Using doAnswer so that isLeader() returns true after becomeLeader() is called,
      // matching the real behavior while preventing the replication loop from starting.
      doAnswer(
              invocation -> {
                doReturn(true).when(materializedViewGenerator).isLeader();
                return null;
              })
          .when(materializedViewGenerator)
          .becomeLeader();
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
      addIndexForReplicationWithAttempt(materializedViewindexGeneration, 0);
    }

    /**
     * Adds an index for replication with a specific attempt number. This enables testing the
     * retry/attempt ref counting scenario where multiple GenerationIds share the same canonicalKey
     * but differ only in attemptNumber.
     */
    private void addIndexForReplicationWithAttempt(
        MaterializedViewIndexGeneration materializedViewindexGeneration, int attemptNumber) {
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
                      IndexFormatVersion.CURRENT,
                      attemptNumber)));
      this.manager.add(autoEmbeddingIndexGeneration);
    }
  }

  /**
   * Returns a mock catalog that returns deterministic metadata for any GenerationId. UUID is
   * derived from indexId only so all generations of the same index map to the same collection.
   */
  private static MaterializedViewCollectionMetadataCatalog createMockMetadataCatalog() {
    MaterializedViewCollectionMetadataCatalog catalog =
        mock(MaterializedViewCollectionMetadataCatalog.class);
    when(catalog.getMetadata(any(GenerationId.class)))
        .thenAnswer(
            inv -> {
              GenerationId id = inv.getArgument(0);
              UUID uuid =
                  UUID.nameUUIDFromBytes(id.indexId.toHexString().getBytes(StandardCharsets.UTF_8));
              return new MaterializedViewCollectionMetadata(
                  new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
                      0, Map.of()),
                  uuid,
                  "test_" + id.indexId.toHexString());
            });
    when(catalog.getMetadataIfPresent(any(GenerationId.class)))
        .thenAnswer(
            inv -> {
              GenerationId id = inv.getArgument(0);
              UUID uuid =
                  UUID.nameUUIDFromBytes(id.indexId.toHexString().getBytes(StandardCharsets.UTF_8));
              return Optional.of(
                  new MaterializedViewCollectionMetadata(
                      new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
                          0, Map.of()),
                      uuid,
                      "test_" + id.indexId.toHexString()));
            });
    return catalog;
  }
}
