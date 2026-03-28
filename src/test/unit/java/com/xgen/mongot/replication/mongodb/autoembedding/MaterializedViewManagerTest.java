package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.SessionRefresher.mockSessionRefresher;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
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
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
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
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
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
  public void testDropIndex_removeMetadataCalledAfterLeaseManagerDrop() throws Exception {
    Mocks mocks = Mocks.create();

    // Add an index.
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(materializedViewindexGeneration);
    mocks.addIndexForReplication(materializedViewindexGeneration);

    // Drop the index - this triggers onDrop -> leaseManager.drop -> then removeMetadata
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    // Verify ordering: leaseManager.drop() must complete BEFORE removeMetadata() is called.
    // This is critical because leaseManager.drop() internally calls getLeaseKey(generationId),
    // which resolves the collection name via mvMetadataCatalog.getMetadata(generationId).
    // If removeMetadata ran first, getLeaseKey would fail.
    InOrder inOrder = Mockito.inOrder(mocks.leaseManager, mocks.metadataCatalog);
    inOrder.verify(mocks.leaseManager).drop(MOCK_GENERATION_ID);
    inOrder.verify(mocks.metadataCatalog).removeMetadata(MOCK_GENERATION_ID);
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
  public void testUpdateSyncSource_delegatesToFactory() {
    Mocks mocks = Mocks.create();
    SyncSourceConfig newConfig =
        new SyncSourceConfig(
            ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newHost"),
            ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newHost"),
            Optional.empty(),
            Optional.empty());

    mocks.manager.updateSyncSource(newConfig);

    verify(mocks.materializedViewGeneratorFactory).updateSyncSourceConfig(newConfig);
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
    assertFalse(mocks.manager.isReplicationSupported());

    // Phase 3: Update sync source
    SyncSourceConfig newConfig =
        new SyncSourceConfig(
            ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newHost"),
            ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newHost"),
            Optional.empty(),
            Optional.empty());
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
  public void testAddDuringDisabledReplication_sameVersion_swapsIndex() {
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

    // Same version: swapIndex should be called on the newer generation, existing wins
    assertEquals(matViewIndexGen1.getIndex(), matViewIndexGen2.getIndex());
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
          new SyncSourceConfig(
              ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newString"),
              ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newString"),
              Optional.empty(),
              Optional.empty());
      AutoEmbeddingMongoClient autoEmbeddingMongoClient =
          new AutoEmbeddingMongoClient(Optional.of(syncSourceConfig), new SimpleMeterRegistry());

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
                  metadataCatalog);
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
      when(indexingWorkSchedulerFactory.getIndexingWorkSchedulers()).thenReturn(Map.of());

      NamedScheduledExecutorService commitExecutor =
          mockScheduledExecutor("index-commit");

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
      // Track added generation IDs to return from getFollowerGenerationIds
      Set<GenerationId> addedGenerationIds = ConcurrentHashMap.newKeySet();
      doAnswer(
              invocation -> {
                IndexGeneration indexGen = invocation.getArgument(0);
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
                Map<GenerationId, IndexStatus> result = new HashMap<>();
                for (GenerationId genId : addedGenerationIds) {
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
      NamedExecutorService executorService = mock(NamedExecutorService.class);
      when(executorService.getName()).thenReturn("indexing");
      try {
        when(executorService.awaitTermination(anyLong(), any())).thenReturn(true);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory =
          mock(MaterializedViewManager.MaterializedViewGeneratorFactory.class);

      NamedScheduledExecutorService commitExecutor =
          mockScheduledExecutor("index-commit");

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

      // Use a plain LeaseManager mock (NOT StaticLeaderLeaseManager) so the dynamic
      // leader election path in createNewGenerator() is tested
      LeaseManager mockLeaseManager = mock(LeaseManager.class);
      when(mockLeaseManager.drop(any())).thenReturn(COMPLETED_FUTURE);

      // Track added generation IDs and mark them as leaders (simulating unexpired lease)
      Set<GenerationId> leaderGenerationIds = ConcurrentHashMap.newKeySet();
      doAnswer(
              invocation -> {
                IndexGeneration indexGen = invocation.getArgument(0);
                // Simulate owning an unexpired lease - add to leaders set
                leaderGenerationIds.add(indexGen.getGenerationId());
                return null;
              })
          .when(mockLeaseManager)
          .add(any(), anyBoolean());

      // isLeader returns true for generations in leaderGenerationIds (unexpired lease)
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
          IndexStatus.unknown(), // expectedStatus
          Optional.empty(),
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
      // Mock becomeLeader() to prevent real async tasks from being submitted to the lifecycle
      // executor, which would cause shutdownOrFail() to hang waiting for task termination.
      Mockito.doNothing().when(materializedViewGenerator).becomeLeader();
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
