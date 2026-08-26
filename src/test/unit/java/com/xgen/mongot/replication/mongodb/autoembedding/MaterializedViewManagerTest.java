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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.errorprone.annotations.Keep;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.cache.EmbeddingRetryCache;
import com.xgen.mongot.embedding.cache.EmbeddingRetryCache.ReleaseTrigger;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.leasing.DynamicLeaderLeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.Lease;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.StaticLeaderLeaseManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
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
import com.xgen.mongot.replication.mongodb.IndexManager;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
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
    doReturn(IndexManager.State.FAILED).when(materializedViewGenerator).getState();

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
    doReturn(IndexManager.State.SHUT_DOWN).when(materializedViewGenerator).getState();

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
    mocks.markIndexDeleted();
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    // Verify ordering: leaseManager.drop() (which untracks the generationId) must complete
    // BEFORE removeMetadata() is called. This ensures the generationId is fully untracked
    // before the metadata is cleaned up.
    InOrder inOrder = Mockito.inOrder(mocks.leaseManager, mocks.metadataCatalog);
    // drop() runs twice (eagerly in cleanUpMatViewResources, again in
    // cleanUpGenerationIdStates); calls() consumes both without requiring them to be adjacent,
    // since the detached deletion-cleanup chain may interleave other lease-manager calls.
    inOrder.verify(mocks.leaseManager, Mockito.calls(2)).drop(MOCK_MAT_VIEW_GENERATION_ID);
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

  // ==================== Drop discriminator (supersession vs deletion) ====================
  //
  // Last local user of an MV collection: the IndexCatalog decides — index still configured means
  // retirement (leave collection + lease for the stale-lease GC), index absent means deletion
  // (clean up the MV collections and leases now).

  @Test
  public void dropIndex_aevBumpSupersession_leavesMvCollectionAndLeaseForGc() throws Exception {
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);

    // Sibling generation for the SAME index after an ops autoEmbeddingDefinitionVersion bump:
    // the definition (and so definitionVersion, here 0 for both) is unchanged -- only the MV
    // collection differs. The conf push adds the new generation before the old one is dropped.
    MaterializedViewGenerationId siblingGenId =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    registerSiblingCollection(
        mocks.metadataCatalog, siblingGenId, "test_" + INDEX_ID.toHexString() + "_aev1");
    MaterializedViewIndexGeneration siblingGen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(siblingGenId));
    mocks.mockMaterializedViewGenerator(siblingGen);
    mocks.addIndexForReplication(siblingGen);

    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    // Local cleanup still happens: generator shut down, and this mongot's heartbeat stopped.
    verify(generator).shutdown();
    verify(mocks.leaseManager, atLeast(1)).drop(MOCK_MAT_VIEW_GENERATION_ID);
    // The shared MV collection + lease are left for the stale-lease GC, not dropped here.
    verify(gen.getIndex().getWriter(), never()).dropMaterializedViewCollection();
    verify(mocks.leaseManager, never()).dropLease(any(String.class));
    // The cached lease copy is evicted so it cannot outlive the document once the GC deletes
    // it: a later re-initialization of the same definition must re-read the database (a
    // recreated MV collection keeps its name but gets a new UUID).
    verify(mocks.leaseManager).evictCachedLease("test_" + INDEX_ID.toHexString());
  }

  @Test
  public void dropIndex_indexDeletion_dropsMvCollectionAndLease() throws Exception {
    Mocks mocks = Mocks.create(); // leaseManager.isLeader() returns true by default
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);

    // The index is gone from the IndexCatalog -> genuine deletion.
    mocks.markIndexDeleted();
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    verify(generator).shutdown();
    // The cleanup chain is detached from the future dropIndex returns, so sync on its last
    // action (the lease delete) before the plain verifies.
    verify(mocks.leaseManager, timeout(5000))
        .cleanupLeaseForDeletedIndex("test_" + INDEX_ID.toHexString());
    verify(gen.getIndex().getWriter()).dropMaterializedViewCollection();
    // No manager-level eviction on the deletion branch: the by-id delete performs its own local
    // cleanup.
    verify(mocks.leaseManager, never()).evictCachedLease(any(String.class));
    // matViewCollectionDrop{scenario=indexDeletion, outcome=success} is recorded for the drop.
    assertEquals(
        1.0,
        collectionDropCount(mocks, MaterializedViewManager.DROP_SCENARIO_INDEX_DELETION, "success"),
        0.0);
  }

  @Test
  public void dropIndex_indexStillConfigured_noSibling_leavesSharedResourcesForGc()
      throws Exception {
    // The index is still in the IndexCatalog (a retirement drop, e.g. an initial-sync retry) and
    // no sibling generation is registered locally. The old sibling-presence heuristic read this
    // as deletion and dropped the shared MV + lease out from under the index; the catalog says
    // the index lives on, so they must be left for the successor generation / the GC.
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);

    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    verify(generator).shutdown();
    verify(gen.getIndex().getWriter(), never()).dropMaterializedViewCollection();
    verify(mocks.leaseManager, never()).dropLease(any(String.class));
    verify(mocks.leaseManager, never()).cleanupLeaseForDeletedIndex(any(String.class));
  }

  @Test
  public void dropIndex_indexDeletion_dropFails_leavesLeaseForGc() throws Exception {
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    // The leader's MV-collection drop fails. Deleting the lease anyway would strand the surviving
    // collection forever (the GC scanner is lease-driven), so the lease must be left in place —
    // the GC finds it on its own, and with the deleted-index safeguard fix it marks and deletes
    // it after the graces.
    when(gen.getIndex().getWriter().dropMaterializedViewCollection())
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("drop failed")));

    mocks.markIndexDeleted();
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    // The detached chain sweeps leftovers even after a failed MV drop; sync on that.
    verify(mocks.leaseManager, timeout(5000))
        .findLeasesForDeletedIndex(eq(INDEX_ID), any(String.class));
    verify(mocks.leaseManager, never()).cleanupLeaseForDeletedIndex(any(String.class));
    assertEquals(
        1.0,
        collectionDropCount(mocks, MaterializedViewManager.DROP_SCENARIO_INDEX_DELETION, "failure"),
        0.0);
    assertEquals(
        0.0,
        collectionDropCount(mocks, MaterializedViewManager.DROP_SCENARIO_INDEX_DELETION, "success"),
        0.0);
  }

  @Test
  public void dropIndex_indexDeletion_leaseDeleteFails_completesBestEffort() throws Exception {
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    when(mocks.leaseManager.cleanupLeaseForDeletedIndex(any(String.class))).thenReturn(false);

    mocks.markIndexDeleted();
    // MV drop succeeded, lease delete failed: best effort — the drop completes normally and the
    // lease stays where the GC scanner finds it.
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    verify(mocks.leaseManager, timeout(5000))
        .cleanupLeaseForDeletedIndex("test_" + INDEX_ID.toHexString());
    verify(gen.getIndex().getWriter()).dropMaterializedViewCollection();
  }

  @Test
  public void sourceCollectionDrop_leaseHolder_dropsMvThenDeletesLease() {
    Mocks mocks = Mocks.create(); // leaseManager.isLeader() returns true by default
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);

    mocks.onSourceCollectionDropped().accept(gen);

    // The cleanup chain is detached; sync on its last action (the lease delete).
    verify(mocks.leaseManager, timeout(5000))
        .cleanupLeaseForDeletedIndex("test_" + INDEX_ID.toHexString());
    verify(gen.getIndex().getWriter()).dropMaterializedViewCollection();
    assertEquals(
        1.0,
        collectionDropCount(
            mocks, MaterializedViewManager.DROP_SCENARIO_SOURCE_COLLECTION_DROP, "success"),
        0.0);
  }

  /**
   * The buffer {@code retryFailedGeneratorCreations} and {@code restartReplication} rebuild from.
   */
  private static Map<UUID, MaterializedViewIndexGeneration> bufferedGenerations(Mocks mocks) {
    return mocks.manager.getActiveGenerationIdCatalog().latestMatViewIndexGenerationByCollection;
  }

  /** The key the catalog buffered {@code generation} under, without assuming how it derived it. */
  private static UUID bufferedKeyOf(Mocks mocks, MaterializedViewIndexGeneration generation) {
    return bufferedGenerations(mocks).entrySet().stream()
        .filter(entry -> entry.getValue() == generation)
        .map(Map.Entry::getKey)
        .findFirst()
        .orElseThrow(() -> new AssertionError("generation is not buffered for generator rebuilds"));
  }

  @Test
  public void sourceCollectionDrop_deregistersGenerationSoRefreshCannotRebuildIt() {
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    assertTrue(
        "precondition: the generation is buffered for generator rebuilds",
        bufferedGenerations(mocks).containsValue(gen));

    mocks.onSourceCollectionDropped().accept(gen);

    // The teardown that follows this callback shuts the generator down and closes its index, which
    // is precisely what retryFailedGeneratorCreations rebuilds from. Leaving the generation
    // buffered lets it rebuild a generator on the closed index.
    assertFalse(
        "the dropped generation must not be left for the status refresh to rebuild",
        bufferedGenerations(mocks).containsValue(gen));
  }

  @Test
  public void sourceCollectionDrop_notLeaseHolder_stillDeregistersGeneration() {
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    when(mocks.leaseManager.isLeader(any())).thenReturn(false);

    mocks.onSourceCollectionDropped().accept(gen);

    // Leadership governs who cleans up the shared MV and lease, not whether this node keeps
    // trying to replicate a collection that no longer exists.
    assertFalse(bufferedGenerations(mocks).containsValue(gen));
  }

  @Test
  public void sourceCollectionDrop_newerGenerationBuffered_leavesItRegistered() {
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration dropped =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(dropped);
    mocks.addIndexForReplication(dropped);
    UUID matViewCollectionUuid = bufferedKeyOf(mocks, dropped);
    // A definition update racing the drop: a newer generation already owns the buffer entry, and
    // dropping the older one must not evict it.
    MaterializedViewIndexGeneration newer =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    bufferedGenerations(mocks).put(matViewCollectionUuid, newer);

    mocks.onSourceCollectionDropped().accept(dropped);

    assertEquals(newer, bufferedGenerations(mocks).get(matViewCollectionUuid));
  }

  @Test
  public void sourceCollectionDrop_notLeaseHolder_leavesEverythingForGc() {
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    when(mocks.leaseManager.isLeader(any())).thenReturn(false);

    mocks.onSourceCollectionDropped().accept(gen);

    verify(gen.getIndex().getWriter(), never()).dropMaterializedViewCollection();
    verify(mocks.leaseManager, never()).cleanupLeaseForDeletedIndex(any(String.class));
  }

  @Test
  public void sourceCollectionDrop_dropFails_leavesLeaseForGc() {
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    // Deleting the lease after a failed drop would strand the surviving MV (the GC scanner is
    // lease-driven), so the lease must stay in place.
    when(gen.getIndex().getWriter().dropMaterializedViewCollection())
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("drop failed")));

    mocks.onSourceCollectionDropped().accept(gen);

    verify(mocks.leaseManager, never()).cleanupLeaseForDeletedIndex(any(String.class));
    assertEquals(
        1.0,
        collectionDropCount(
            mocks, MaterializedViewManager.DROP_SCENARIO_SOURCE_COLLECTION_DROP, "failure"),
        0.0);
  }

  @Test
  public void sourceCollectionDrop_thenIndexDeletion_stillUnwindsTheGeneration() throws Exception {
    // Deregistering at drop time removes the generation from the rebuild buffer but deliberately
    // leaves the rest of its bookkeeping, so the config-driven deletion that follows still finds
    // its generator and unwinds normally -- releasing leadership rather than returning early.
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);

    mocks.onSourceCollectionDropped().accept(gen);
    assertFalse(bufferedGenerations(mocks).containsValue(gen));

    mocks.markIndexDeleted();
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    // Both only happen past the "already dropped" early return, so they distinguish a real unwind
    // from one that bailed out because the drop-time cleanup took too much bookkeeping with it.
    verify(generator).shutdown();
    verify(mocks.leaseManager, atLeastOnce())
        .cleanupLeaseForDeletedIndex("test_" + INDEX_ID.toHexString());
    assertTrue(mocks.manager.getMatViewGenerator(gen.getGenerationId()).isEmpty());
  }

  @Test
  public void dropIndex_indexDeletion_cleansUpLeftoverSiblingLease() throws Exception {
    // A supersession that completed before the deletion left its MV + lease behind for the GC.
    // Cleaning them up here spares the GC's graces (days in production).
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);

    String leftoverKey = "test_" + INDEX_ID.toHexString() + "_leftover";
    // Built with a random MV uuid, so no locally active generation matches and the cleanup
    // does not skip it as mid-flip.
    Lease leftover =
        leaseWithExpiration(leftoverKey, Instant.now(), Lease.CleanupState.NOT_ELIGIBLE);
    when(mocks.leaseManager.findLeasesForDeletedIndex(eq(INDEX_ID), any(String.class)))
        .thenReturn(
            List.of(
                new LeaseManager.DeletedIndexLease(
                    leftover, Optional.of(new MongoNamespace("testdb", leftoverKey)))));
    MongoClient writerClient = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> collection = mock(MongoCollection.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.of(writerClient));
    when(writerClient.getDatabase("testdb")).thenReturn(database);
    when(database.getCollection(leftoverKey)).thenReturn(collection);

    mocks.markIndexDeleted();
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    verify(mocks.leaseManager, timeout(5000))
        .cleanupLeaseForDeletedIndex("test_" + INDEX_ID.toHexString());
    // The leftover's MV collection is dropped via the writer client, then its lease deleted.
    verify(mocks.leaseManager).cleanupLeaseForDeletedIndex(leftoverKey);
    // The leftover scan must run before the dropped generation's own lease delete: that delete
    // evicts the leaseKey -> database mapping the scan resolves the lease table through.
    InOrder inOrder = Mockito.inOrder(mocks.leaseManager);
    inOrder.verify(mocks.leaseManager).findLeasesForDeletedIndex(eq(INDEX_ID), any(String.class));
    inOrder
        .verify(mocks.leaseManager)
        .cleanupLeaseForDeletedIndex("test_" + INDEX_ID.toHexString());
    // Both this generation's MV drop and the leftover's count as indexDeletion drops.
    assertEquals(
        2.0,
        collectionDropCount(mocks, MaterializedViewManager.DROP_SCENARIO_INDEX_DELETION, "success"),
        0.0);
  }

  @Test
  public void dropIndex_indexDeletion_leftoverLeaseWithoutNamespace_leavesItForGc()
      throws Exception {
    // The leftover lease exists but carries no usable MV namespace (missing or invalid
    // metadata): don't delete the lease blind — leave it and let the GC sort it out.
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);

    String leftoverKey = "test_" + INDEX_ID.toHexString() + "_leftover";
    Lease leftover =
        leaseWithExpiration(leftoverKey, Instant.now(), Lease.CleanupState.NOT_ELIGIBLE);
    when(mocks.leaseManager.findLeasesForDeletedIndex(eq(INDEX_ID), any(String.class)))
        .thenReturn(List.of(new LeaseManager.DeletedIndexLease(leftover, Optional.empty())));

    mocks.markIndexDeleted();
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    verify(mocks.leaseManager, timeout(5000))
        .cleanupLeaseForDeletedIndex("test_" + INDEX_ID.toHexString());
    verify(mocks.leaseManager, never()).cleanupLeaseForDeletedIndex(leftoverKey);
  }

  @Test
  public void dropIndex_indexDeletionMidFlip_dropsMvCollectionAndLease() throws Exception {
    // An index deletion landing mid-flip: a sibling generation for the same index is still
    // registered in the local catalog (its own MV collection), exactly the state the old
    // sibling-presence heuristic misread as supersession and skipped. The IndexCatalog is empty
    // for the index regardless of local siblings, so the dropped generation cleans up its MV
    // collection + lease instead of leaking them.
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);

    // Sibling generation for the SAME index on a different MV collection still registered.
    MaterializedViewGenerationId siblingGenId =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    registerSiblingCollection(
        mocks.metadataCatalog, siblingGenId, "test_" + INDEX_ID.toHexString() + "_aev1");
    MaterializedViewIndexGeneration siblingGen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(siblingGenId));
    mocks.mockMaterializedViewGenerator(siblingGen);
    mocks.addIndexForReplication(siblingGen);

    mocks.markIndexDeleted();
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    // Sibling presence no longer blocks the drop: the deleted generation's shared resources go.
    verify(generator).shutdown();
    verify(mocks.leaseManager, timeout(5000))
        .cleanupLeaseForDeletedIndex("test_" + INDEX_ID.toHexString());
    verify(gen.getIndex().getWriter()).dropMaterializedViewCollection();
  }

  @Test
  public void dropIndex_releasesEmbeddingRetryCacheClaims() throws Exception {
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);

    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    // cleanUpGenerationIdStates releases the dropped generation's claims so any embeddings it still
    // held are evicted. The key is the generator's canonical key; assert the DROP-triggered call.
    verify(mocks.embeddingRetryCache).releaseClaims(anyString(), eq(ReleaseTrigger.DROP));
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
    mocks.markIndexDeleted();
    mocks.manager.dropIndex(dropGenId).get(5, TimeUnit.SECONDS);

    // Assert — lease is deleted (generator==null path uses lastMaterializedViewIndexGeneration).
    verify(mocks.leaseManager, timeout(5000)).cleanupLeaseForDeletedIndex(any(String.class));
    // Assert — leader drops the MV collection via lastMaterializedViewIndexGeneration's writer
    verify(matViewIndexGeneration.getIndex().getWriter()).dropMaterializedViewCollection();
    // Assert — refcounts are cleaned
    assertNull(mocks.manager.getActiveGenerationIdCatalog().genIdByMatViewCollection.get(uuid));
  }

  @Test
  public void testDropIndex_whenReplicationDisabled_followerEvictsCachedLeaseOnly()
      throws Exception {
    // Follower path of the generator==null code path in maybeDropCollectionAndLease:
    // Follower evicts its cached lease copy but touches neither the lease document nor the MV
    // collection.
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
    mocks.markIndexDeleted();
    mocks.manager.dropIndex(dropGenId).get(5, TimeUnit.SECONDS);

    // Assert — only the cached lease copy is evicted (non-leader half of the deletion path).
    verify(mocks.leaseManager, timeout(5000)).evictCachedLease("test_" + INDEX_ID.toHexString());
    verify(mocks.leaseManager, never()).dropLease(any(String.class));
    verify(mocks.leaseManager, never()).cleanupLeaseForDeletedIndex(any(String.class));
    // Assert — follower does NOT drop the MV collection (not leader)
    verify(matViewIndexGeneration.getIndex().getWriter(), never()).dropMaterializedViewCollection();
  }

  @Test
  public void dropIndex_indexDeletion_leadershipGoneWithSibling_leavesLeaseDocumentForGc()
      throws Exception {
    // A definition revert makes two generations share one lease; the generation that acquired
    // leadership tears down first and takes the leadership state with it, so the last generation
    // out reaches the deletion branch as a non-leader on the very node that still owns the lease
    // document. Deleting the document here without having dropped the MV collection strands the
    // collection: a lease-less MV is invisible to the lease-driven GC. The document must survive
    // so the GC can clean the pair in drop-then-delete order.
    Mocks mocks = Mocks.create();
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    when(mocks.leaseManager.isLeader(any())).thenReturn(false);

    mocks.markIndexDeleted();
    mocks.manager.dropIndex(MOCK_GENERATION_ID).get(5, TimeUnit.SECONDS);

    verify(generator).shutdown();
    // The cached copy is evicted; the lease document and the MV collection are left untouched.
    verify(mocks.leaseManager, timeout(5000)).evictCachedLease("test_" + INDEX_ID.toHexString());
    verify(mocks.leaseManager, never()).dropLease(any(String.class));
    verify(mocks.leaseManager, never()).cleanupLeaseForDeletedIndex(any(String.class));
    verify(gen.getIndex().getWriter(), never()).dropMaterializedViewCollection();
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
    assertTrue(mocks.manager.getMatViewGenerator(succeedingGen.getGenerationId()).isPresent());
    // The failing one is absent — caller (next conf push) is expected to retry.
    assertFalse(mocks.manager.getMatViewGenerator(failingGen.getGenerationId()).isPresent());

    // createNewGenerator's leaseManager.add() ran before the throw; the catch must roll it back
    // so refreshStatus() doesn't see a follower with no generator and log a warn every heartbeat.
    verify(mocks.leaseManager).drop(failingGen.getGenerationId());
    verify(mocks.leaseManager, never()).drop(succeedingGen.getGenerationId());
  }

  // refreshStatus periodically recreates a generator a transient failure left missing. Covers the
  // guard no-op, a retry that fails transiently again (lease rolled back, re-attempted next tick),
  // eventual recovery, and the no-gap no-op.
  @Test
  public void refreshStatus_retriesMissingGenerator_afterTransientCreateFailure() {
    Mocks mocks = Mocks.createStaticLeaderWithRefreshCapture();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);
    mocks.manager.setIsReplicationEnabled(false);

    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen); // replication disabled: populates the catalog only

    // First two create() calls throw transient (restartReplication, then the first retry); the
    // third succeeds.
    AtomicInteger createCalls = new AtomicInteger();
    when(mocks.materializedViewGeneratorFactory.create(any(MaterializedViewIndexGeneration.class)))
        .thenAnswer(
            invocation -> {
              if (createCalls.getAndIncrement() < 2) {
                throw new MaterializedViewTransientException(
                    "simulated 10107 NotPrimary",
                    MaterializedViewTransientException.Reason.READ_LEASE_FAILED);
              }
              return generator;
            });

    // Transient failure during restartReplication leaves the generation active but with no
    // generator; the lease registration is rolled back. restartReplication is not a retry, so
    // neither retry-outcome counter moves.
    mocks.manager.restartReplication();
    assertFalse(mocks.manager.getMatViewGenerator(gen.getGenerationId()).isPresent());
    verify(mocks.materializedViewGeneratorFactory, times(1)).create(gen);
    verify(mocks.leaseManager, times(1)).drop(gen.getGenerationId());
    assertEquals(0.0, retryCount(mocks, "failure"), 0.0);

    // Guard: no retry while the factory is uninitialized.
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(false);
    mocks.runnableCaptor.orElseThrow().getValue().run();
    verify(mocks.materializedViewGeneratorFactory, times(1)).create(gen);
    assertEquals(0.0, retryCount(mocks, "failure"), 0.0);
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);

    // First retry fails transiently again: lease rolled back again, still no generator, counted as
    // a failed retry.
    mocks.runnableCaptor.orElseThrow().getValue().run();
    assertFalse(mocks.manager.getMatViewGenerator(gen.getGenerationId()).isPresent());
    verify(mocks.materializedViewGeneratorFactory, times(2)).create(gen);
    verify(mocks.leaseManager, times(2)).drop(gen.getGenerationId());
    assertEquals(1.0, retryCount(mocks, "failure"), 0.0);
    assertEquals(0.0, retryCount(mocks, "success"), 0.0);

    // Next tick succeeds: the missing generator is recreated and counted as a successful retry.
    mocks.runnableCaptor.orElseThrow().getValue().run();
    assertTrue(mocks.manager.getMatViewGenerator(gen.getGenerationId()).isPresent());
    verify(mocks.materializedViewGeneratorFactory, times(3)).create(gen);
    assertEquals(1.0, retryCount(mocks, "success"), 0.0);
    assertEquals(1.0, retryCount(mocks, "failure"), 0.0);

    // No gap remains: a further refresh does not recreate or re-count.
    mocks.runnableCaptor.orElseThrow().getValue().run();
    verify(mocks.materializedViewGeneratorFactory, times(3)).create(gen);
    assertEquals(1.0, retryCount(mocks, "success"), 0.0);
    assertEquals(1.0, retryCount(mocks, "failure"), 0.0);
  }

  // The drop-time deregistration and the status refresh run on different threads without a shared
  // lock, so a weakly consistent iteration can carry an entry that was deregistered in the
  // meantime -- modelled here by deregistering on the state read that decides the rebuild.
  // Rebuilding then would resurrect a generation whose index the drop teardown closed, which is
  // what crashed mongot; the rebuild path re-reads the buffer and refuses.
  @Test
  public void refreshStatus_doesNotRebuildGenerationDeregisteredMidIteration() {
    Mocks mocks = Mocks.createStaticLeaderWithRefreshCapture();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);

    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator shutDownGenerator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    UUID matViewCollectionUuid = bufferedKeyOf(mocks, gen);

    doAnswer(
            invocation -> {
              bufferedGenerations(mocks).remove(matViewCollectionUuid, gen);
              return IndexManager.State.SHUT_DOWN;
            })
        .when(shutDownGenerator)
        .getState();

    MaterializedViewGenerator rebuilt = mock(MaterializedViewGenerator.class);
    when(rebuilt.getIndexGeneration()).thenReturn(gen);
    when(mocks.materializedViewGeneratorFactory.create(gen)).thenReturn(rebuilt);
    Mockito.clearInvocations(mocks.materializedViewGeneratorFactory);

    mocks.runnableCaptor.orElseThrow().getValue().run();

    verify(mocks.materializedViewGeneratorFactory, never()).create(gen);
    assertSame(
        shutDownGenerator, mocks.manager.getMatViewGenerator(gen.getGenerationId()).orElseThrow());
    // A deliberate skip, not a retry outcome: neither counter moves.
    assertEquals(0.0, retryCount(mocks, "success"), 0.0);
    assertEquals(0.0, retryCount(mocks, "failure"), 0.0);
  }

  // A failed transition to follower leaves a SHUT_DOWN generator in the map. refreshStatus
  // recreates
  // it as a fresh follower — but not while a shutdown is still in flight for that generation.
  @Test
  public void refreshStatus_recreatesShutDownGenerator() {
    Mocks mocks = Mocks.createStaticLeaderWithRefreshCapture();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);

    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator shutDownGenerator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    doReturn(IndexManager.State.SHUT_DOWN).when(shutDownGenerator).getState();

    // The retry creates a fresh, healthy generator in its place.
    MaterializedViewGenerator healthyGenerator = mock(MaterializedViewGenerator.class);
    when(healthyGenerator.getIndexGeneration()).thenReturn(gen);
    when(mocks.materializedViewGeneratorFactory.create(gen)).thenReturn(healthyGenerator);

    // In-flight shutdown for this generation: the SHUT_DOWN generator must not be recreated.
    Mockito.clearInvocations(
        shutDownGenerator, healthyGenerator, mocks.materializedViewGeneratorFactory);
    mocks.manager.pendingShutdowns.put(gen.getGenerationId(), new CompletableFuture<>());
    mocks.runnableCaptor.orElseThrow().getValue().run();
    assertSame(
        shutDownGenerator, mocks.manager.getMatViewGenerator(gen.getGenerationId()).orElseThrow());
    verify(shutDownGenerator, never()).clearLeaderState();
    verify(mocks.materializedViewGeneratorFactory, never()).create(gen);
    verify(healthyGenerator, never()).becomeLeader();
    assertEquals(0.0, retryCount(mocks, "success"), 0.0);

    // Shutdown no longer in flight: the SHUT_DOWN generator is replaced by a fresh one.
    mocks.manager.pendingShutdowns.remove(gen.getGenerationId());
    Mockito.clearInvocations(
        shutDownGenerator, healthyGenerator, mocks.materializedViewGeneratorFactory);
    mocks.runnableCaptor.orElseThrow().getValue().run();
    assertSame(
        healthyGenerator, mocks.manager.getMatViewGenerator(gen.getGenerationId()).orElseThrow());
    InOrder replacementOrder =
        Mockito.inOrder(
            shutDownGenerator, mocks.materializedViewGeneratorFactory, healthyGenerator);
    replacementOrder.verify(shutDownGenerator).clearLeaderState();
    replacementOrder.verify(mocks.materializedViewGeneratorFactory).create(gen);
    replacementOrder.verify(healthyGenerator).becomeLeader();
    assertEquals(1.0, retryCount(mocks, "success"), 0.0);
  }

  @Test
  public void refreshStatus_dynamicShutDownLeaderStaysFollowerUntilLeaseReacquired() {
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);

    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerationId generationId = gen.getGenerationId();
    AtomicBoolean ownsLease = new AtomicBoolean(true);
    when(mocks.leaseManager.isLeader(generationId)).thenAnswer(ignored -> ownsLease.get());

    MaterializedViewGenerator shutDownGenerator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    verify(shutDownGenerator).becomeLeader();

    ownsLease.set(false);
    doReturn(IndexManager.State.SHUT_DOWN).when(shutDownGenerator).getState();
    MaterializedViewGenerator replacement = mocks.mockMaterializedViewGenerator(gen);
    doReturn(IndexManager.State.INITIAL_SYNC).when(replacement).getState();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(LeaseManager.FollowerPollResult.EMPTY);
    Mockito.clearInvocations(
        shutDownGenerator, replacement, mocks.materializedViewGeneratorFactory, mocks.leaseManager);

    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertSame(replacement, mocks.manager.getMatViewGenerator(generationId).orElseThrow());
    InOrder replacementOrder =
        Mockito.inOrder(shutDownGenerator, mocks.materializedViewGeneratorFactory);
    replacementOrder.verify(shutDownGenerator).clearLeaderState();
    replacementOrder.verify(mocks.materializedViewGeneratorFactory).create(gen);
    verify(replacement, never()).becomeLeader();
    verify(mocks.leaseManager, never()).tryAcquireLeadership(generationId);

    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(generationId, IndexStatus.unknown()), Set.of(generationId)));
    when(mocks.leaseManager.tryAcquireLeadership(generationId))
        .thenAnswer(
            ignored -> {
              ownsLease.set(true);
              return true;
            });

    mocks.runnableCaptor.orElseThrow().getValue().run();

    InOrder acquisitionOrder = Mockito.inOrder(mocks.leaseManager, replacement);
    acquisitionOrder.verify(mocks.leaseManager).tryAcquireLeadership(generationId);
    acquisitionOrder.verify(replacement).becomeLeader();
  }

  @Test
  public void refreshStatus_transientFailureRecreatingShutDownGenerator_retries() {
    Mocks mocks = Mocks.createStaticLeaderWithRefreshCapture();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);

    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator shutDownGenerator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    doReturn(IndexManager.State.SHUT_DOWN).when(shutDownGenerator).getState();

    MaterializedViewGenerator healthyGenerator = mock(MaterializedViewGenerator.class);
    when(healthyGenerator.getIndexGeneration()).thenReturn(gen);
    when(mocks.materializedViewGeneratorFactory.create(gen))
        .thenThrow(
            new MaterializedViewTransientException(
                "simulated lease read failure",
                MaterializedViewTransientException.Reason.READ_LEASE_FAILED))
        .thenReturn(healthyGenerator);
    Mockito.clearInvocations(
        shutDownGenerator,
        healthyGenerator,
        mocks.materializedViewGeneratorFactory,
        mocks.leaseManager);

    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertSame(
        shutDownGenerator, mocks.manager.getMatViewGenerator(gen.getGenerationId()).orElseThrow());
    verify(shutDownGenerator).clearLeaderState();
    verify(mocks.materializedViewGeneratorFactory).create(gen);
    verify(mocks.leaseManager).drop(gen.getGenerationId());
    verify(healthyGenerator, never()).becomeLeader();
    assertEquals(1.0, retryCount(mocks, "failure"), 0.0);
    assertEquals(0.0, retryCount(mocks, "success"), 0.0);

    Mockito.clearInvocations(
        shutDownGenerator,
        healthyGenerator,
        mocks.materializedViewGeneratorFactory,
        mocks.leaseManager);
    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertSame(
        healthyGenerator, mocks.manager.getMatViewGenerator(gen.getGenerationId()).orElseThrow());
    InOrder replacementOrder =
        Mockito.inOrder(
            shutDownGenerator, mocks.materializedViewGeneratorFactory, healthyGenerator);
    replacementOrder.verify(shutDownGenerator).clearLeaderState();
    replacementOrder.verify(mocks.materializedViewGeneratorFactory).create(gen);
    replacementOrder.verify(healthyGenerator).becomeLeader();
    verify(mocks.leaseManager, never()).drop(gen.getGenerationId());
    assertEquals(1.0, retryCount(mocks, "failure"), 0.0);
    assertEquals(1.0, retryCount(mocks, "success"), 0.0);
  }

  private static double retryCount(Mocks mocks, String outcome) {
    return mocks
        .meterRegistry
        .counter(MaterializedViewManager.GENERATOR_CREATION_RETRY_COUNTER_NAME, "outcome", outcome)
        .count();
  }

  // ================== Retired generations — MV collection missing (CLOUDP-437172)
  // ==================
  //
  // Recreating such a generator only restarts the flap, because it hands back the lease this host
  // still owns in the database and then shuts down again on the replayed COLLECTION_NOT_FOUND.

  private static double retiredCount(Mocks mocks) {
    return mocks
        .meterRegistry
        .counter(MaterializedViewManager.RETIRED_GENERATION_COUNTER_NAME)
        .count();
  }

  private static void runHeartbeatOnce(Mocks mocks) {
    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();
  }

  // Retired once on first observation, then never recreated and never re-counted — by later refresh
  // ticks, by restartReplication, or once the status the retirement was derived from is
  // overwritten.
  @Test
  public void refreshStatus_shutDownForCollectionNotFound_retiresGenerationAndStopsRecreating() {
    Mocks mocks = Mocks.createStaticLeaderWithRefreshCapture();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator deadGenerator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    doReturn(IndexManager.State.SHUT_DOWN).when(deadGenerator).getState();
    gen.getIndex().setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));

    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertTrue(
        "generation should be retired",
        mocks.manager.retiredGenerations.contains(gen.getGenerationId()));
    assertSame(
        "the dead generator must not be replaced",
        deadGenerator,
        mocks.manager.getMatViewGenerator(gen.getGenerationId()).orElseThrow());

    // refreshStatus overwrites this status from the lease on every tick a generator is not leader.
    gen.getIndex().setStatus(IndexStatus.steady());
    mocks.runnableCaptor.orElseThrow().getValue().run();
    mocks.runnableCaptor.orElseThrow().getValue().run();

    mocks.manager.restartReplication();

    // The one create is addIndexForReplication's; nothing recreated the generator since.
    verify(mocks.materializedViewGeneratorFactory, times(1)).create(gen);
    assertEquals("retirement is observed once", 1.0, retiredCount(mocks), 0.0);
    assertEquals(0.0, retryCount(mocks, "success"), 0.0);
    assertEquals("a retirement is not a retry failure", 0.0, retryCount(mocks, "failure"), 0.0);
  }

  // Retirement is keyed by generation id, so recreating the index — the only way this state
  // recovers — still gets a generator.
  @Test
  public void refreshStatus_newUserGenerationForRetiredIndex_isNotBlocked() {
    Mocks mocks = Mocks.createStaticLeaderWithRefreshCapture();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);
    MaterializedViewIndexGeneration deadGen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(MOCK_MAT_VIEW_GENERATION_ID));
    MaterializedViewGenerator deadGenerator = mocks.mockMaterializedViewGenerator(deadGen);
    mocks.addIndexForReplication(deadGen);
    doReturn(IndexManager.State.SHUT_DOWN).when(deadGenerator).getState();
    deadGen.getIndex().setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    mocks.runnableCaptor.orElseThrow().getValue().run();
    assertTrue(mocks.manager.retiredGenerations.contains(deadGen.getGenerationId()));

    // The index is recreated: same indexId, incremented user index version, new generation id.
    var newGenId =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    MaterializedViewIndexGeneration newGen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(newGenId, 1));
    MaterializedViewGenerator newGenerator = mocks.mockMaterializedViewGenerator(newGen);
    mocks.addIndexForReplication(newGen);

    assertFalse(
        "a new generation must not inherit the retirement",
        mocks.manager.retiredGenerations.contains(newGenId));
    assertSame(newGenerator, mocks.manager.getMatViewGenerator(newGenId).orElseThrow());
  }

  // Heartbeat pass 1 releases the lease for any terminal generator, but retires the generation only
  // when the collection is missing: a transient terminal must stay acquirable by a healthy node.
  @Test
  public void dynamicLeader_heartbeat_retiresPermanentTerminalsOnly() {
    Mocks collectionGone = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    MaterializedViewIndexGeneration goneGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator goneGenerator = collectionGone.mockMaterializedViewGenerator(goneGen);
    collectionGone.addIndexForReplication(goneGen);
    doReturn(IndexManager.State.SHUT_DOWN).when(goneGenerator).getState();
    goneGen.getIndex().setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));

    runHeartbeatOnce(collectionGone);

    verify(collectionGone.leaseManager).releaseLeadership(goneGen.getGenerationId());
    assertTrue(
        "a missing MV collection should retire the generation",
        collectionGone.manager.retiredGenerations.contains(goneGen.getGenerationId()));
    assertEquals(1.0, retiredCount(collectionGone), 0.0);

    Mocks transientTerminal = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    MaterializedViewIndexGeneration failedGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator failedGenerator =
        transientTerminal.mockMaterializedViewGenerator(failedGen);
    transientTerminal.addIndexForReplication(failedGen);
    doReturn(IndexManager.State.FAILED).when(failedGenerator).getState();

    runHeartbeatOnce(transientTerminal);

    verify(transientTerminal.leaseManager).releaseLeadership(failedGen.getGenerationId());
    assertFalse(
        "a transient terminal must not retire the generation",
        transientTerminal.manager.retiredGenerations.contains(failedGen.getGenerationId()));
    assertEquals(0.0, retiredCount(transientTerminal), 0.0);
  }

  // A recovery attempt bumps the search attempt but keeps the MV generation id, so it reaches
  // add() → computeGenerator() → replaceGenerator() rather than the recreation path.
  @Test
  public void add_sameGenerationAfterRetirement_doesNotReplaceGenerator() {
    Mocks mocks = Mocks.createStaticLeaderWithRefreshCapture();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(MOCK_MAT_VIEW_GENERATION_ID));
    MaterializedViewGenerator deadGenerator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    doReturn(IndexManager.State.SHUT_DOWN).when(deadGenerator).getState();
    gen.getIndex().setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    mocks.runnableCaptor.orElseThrow().getValue().run();

    // Same generation id, higher definition version: a retry attempt for the retired generation.
    MaterializedViewIndexGeneration retryGen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(MOCK_MAT_VIEW_GENERATION_ID, 1));
    MaterializedViewGenerator retryGenerator = mocks.mockMaterializedViewGenerator(retryGen);
    mocks.addIndexForReplication(retryGen);
    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertSame(
        "the retired generator must not be replaced",
        deadGenerator,
        mocks.manager.getMatViewGenerator(gen.getGenerationId()).orElseThrow());
    verify(retryGenerator, never()).becomeLeader();
  }

  // shutdownReplication clears the generator map, so a retired generation then looks like a missing
  // one on every refresh.
  @Test
  public void refreshStatus_afterShutdownClearsRetiredGenerator_recordsNoRetryFailures()
      throws Exception {
    Mocks mocks = Mocks.createStaticLeaderWithRefreshCapture();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator deadGenerator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    doReturn(IndexManager.State.SHUT_DOWN).when(deadGenerator).getState();
    gen.getIndex().setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    mocks.runnableCaptor.orElseThrow().getValue().run();

    // shutdownReplication clears the generator map; restartReplication skips the retired
    // generation, so every later refresh sees no generator for a still-active generation.
    mocks.manager.shutdownReplication().get(5, TimeUnit.SECONDS);
    mocks.manager.restartReplication();
    mocks.runnableCaptor.orElseThrow().getValue().run();
    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertEquals("a retirement is not a retry failure", 0.0, retryCount(mocks, "failure"), 0.0);
    assertEquals(0.0, retryCount(mocks, "success"), 0.0);
    verify(mocks.materializedViewGeneratorFactory, times(1)).create(gen);
  }

  // The retired generation's lease still carries the status this host published before the
  // collection went missing, so copying it back would report a missing collection as queryable.
  @Test
  public void refreshStatus_retiredGeneration_keepsCollectionNotFoundStatus() {
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator deadGenerator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    MaterializedViewGenerationId genId = gen.getGenerationId();
    doReturn(IndexManager.State.SHUT_DOWN).when(deadGenerator).getState();
    // Retirement clears leader state, which is what opens the follower status-update branch.
    doReturn(false).when(deadGenerator).isLeader();
    gen.getIndex().setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    // The lease still reports the healthy status published before the collection disappeared.
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(genId, IndexStatus.steady()), Set.of(genId)));

    mocks.runnableCaptor.orElseThrow().getValue().run();
    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertTrue(mocks.manager.retiredGenerations.contains(genId));
    assertTrue(
        "a retired generation must not be reported queryable",
        gen.getIndex().getStatus().isCollectionNotFound());
  }

  // This host keeps the retired generation's lease row, so the lease stays acquirable on every
  // poll and must be skipped rather than re-entering the election path each tick.
  @Test
  public void refreshStatus_retiredGenerationStillAcquirable_isNotReconsideredForElection() {
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator deadGenerator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    MaterializedViewGenerationId genId = gen.getGenerationId();
    doReturn(IndexManager.State.SHUT_DOWN).when(deadGenerator).getState();
    gen.getIndex().setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(genId, IndexStatus.unknown()), Set.of(genId)));

    mocks.runnableCaptor.orElseThrow().getValue().run();
    mocks.runnableCaptor.orElseThrow().getValue().run();
    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertTrue(mocks.manager.retiredGenerations.contains(genId));
    verify(mocks.leaseManager, never()).tryAcquireLeadership(genId);
    // The disqualification branch shuts the generator down; a retired generation must not re-enter
    // it on every tick.
    verify(deadGenerator, never()).shutdown();
  }

  // A shutdown between retiring and the next heartbeat would otherwise leave the lease manager
  // renewing a lease that no generator will ever claim.
  @Test
  public void refreshStatus_retiringGeneration_releasesLeadershipImmediately() {
    Mocks mocks = Mocks.createStaticLeaderWithRefreshCapture();
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);
    MaterializedViewIndexGeneration gen =
        mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(new ObjectId()));
    MaterializedViewGenerator deadGenerator = mocks.mockMaterializedViewGenerator(gen);
    mocks.addIndexForReplication(gen);
    doReturn(IndexManager.State.SHUT_DOWN).when(deadGenerator).getState();
    gen.getIndex().setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));

    mocks.runnableCaptor.orElseThrow().getValue().run();

    verify(mocks.leaseManager).releaseLeadership(gen.getGenerationId());
    verify(deadGenerator).clearLeaderState();
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
    mocks.markIndexDeleted();
    mocks.manager.dropIndex(genIdA0).get(5, TimeUnit.SECONDS);

    // Verify lease was NOT cleaned up (leaseManager.drop and the lease delete not called yet)
    verify(mocks.leaseManager, never()).drop(any());
    verify(mocks.leaseManager, never()).cleanupLeaseForDeletedIndex(any());
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
    verify(mocks.leaseManager, timeout(5000)).cleanupLeaseForDeletedIndex(any(String.class));
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
    mocks.markIndexDeleted();
    mocks.manager.dropIndex(genIdU0A0).get(5, TimeUnit.SECONDS);

    verify(mocks.leaseManager, never()).drop(any());
    verify(mocks.leaseManager, never()).cleanupLeaseForDeletedIndex(any());
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
    verify(mocks.leaseManager, never()).cleanupLeaseForDeletedIndex(any());
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
    verify(mocks.leaseManager, never()).cleanupLeaseForDeletedIndex(any());
    verify(generatorU1, never()).shutdown();
    verify(mocks.metadataCatalog, never()).removeMetadata(any());
    assertEquals(1, catalog.genIdByMatViewCollection.get(uuid).size());
    assertEquals(1, catalog.genIdByMatViewGenId.get(canonicalKeyU1).size());

    // === Drop u1-a1: uuid set=0 → shutdown + drop + lease delete. generatorId["u1"]=0 →
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
    verify(mocks.leaseManager, timeout(5000)).cleanupLeaseForDeletedIndex(any(String.class));
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
    doReturn(IndexManager.State.FAILED).when(generator).getState();

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
    doReturn(IndexManager.State.SHUT_DOWN).when(generator).getState();

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
    doReturn(IndexManager.State.FAILED_EXCEEDED).when(generator).getState();

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
    doReturn(IndexManager.State.INITIAL_SYNC).when(generator).getState();

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
    assertEquals(
        0.0,
        mocks
            .meterRegistry
            .counter(MaterializedViewManager.UNSAFE_LEADERSHIP_REACQUISITION_COUNTER_NAME)
            .count(),
        0.0);
  }

  @Test
  public void dynamicLeader_acquireLease_recordsUnsafeReacquisitionWithActiveLeader() {
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    MaterializedViewIndexGeneration matViewIndexGen =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerator generator = mocks.mockMaterializedViewGenerator(matViewIndexGen);
    mocks.addIndexForReplication(matViewIndexGen);
    doReturn(IndexManager.State.STEADY_STATE).when(generator).getState();
    doReturn(true).when(generator).isLeader();

    MaterializedViewGenerationId matViewGenId = matViewIndexGen.getGenerationId();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(matViewGenId, IndexStatus.unknown()), Set.of(matViewGenId)));
    when(mocks.leaseManager.tryAcquireLeadership(matViewGenId)).thenReturn(true);

    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertEquals(
        1.0,
        mocks
            .meterRegistry
            .counter(MaterializedViewManager.UNSAFE_LEADERSHIP_REACQUISITION_COUNTER_NAME)
            .count(),
        0.0);
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

    doReturn(IndexManager.State.INITIAL_SYNC).when(generator).getState();

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

    doReturn(IndexManager.State.INITIAL_SYNC).when(generator).getState();

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

    doReturn(IndexManager.State.INITIAL_SYNC).when(generator).getState();

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
    doReturn(IndexManager.State.INITIAL_SYNC).when(generator).getState();

    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(matViewGenId, IndexStatus.unknown()), Set.of(matViewGenId)));

    // When tryAcquireLeadership is called, simulate transitionToFollower running concurrently
    // by changing the generator to a terminal state.
    when(mocks.leaseManager.tryAcquireLeadership(matViewGenId))
        .thenAnswer(
            invocation -> {
              doReturn(IndexManager.State.SHUT_DOWN).when(generator).getState();
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
    doReturn(IndexManager.State.FAILED).when(generator).getState();
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
    doReturn(IndexManager.State.FAILED).when(generator).getState();

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
    doReturn(IndexManager.State.SHUT_DOWN).when(generator).getState();
    doReturn(true).when(generator).isLeader();

    ArgumentCaptor<Runnable> heartbeatCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mocks.heartbeatExecutor)
        .scheduleWithFixedDelay(heartbeatCaptor.capture(), anyLong(), anyLong(), any());
    heartbeatCaptor.getValue().run();

    InOrder demotionOrder = Mockito.inOrder(mocks.leaseManager, generator);
    demotionOrder.verify(mocks.leaseManager).releaseLeadership(matViewGenId);
    demotionOrder.verify(generator).clearLeaderState();
    demotionOrder.verify(mocks.leaseManager).heartbeat();
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
    doReturn(IndexManager.State.FAILED_EXCEEDED).when(generator).getState();
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
    doReturn(IndexManager.State.STEADY_STATE_SHUT_DOWN).when(generator).getState();
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
    doReturn(IndexManager.State.STEADY_STATE).when(oldGenerator).getState();
    doReturn(true).when(oldGenerator).isLeader();

    // Lease manager says we no longer hold the lease — leadership-loss condition.
    when(mocks.leaseManager.isLeader(matViewGenId)).thenReturn(false);

    // Construct a fresh "new" healthy generator with the same generationId. This simulates
    // the post-state of a same-genId replaceGenerator (Lucene-only change / oplog falloff).
    MaterializedViewGenerator newGenerator = mock(MaterializedViewGenerator.class);
    when(newGenerator.getIndexGeneration()).thenReturn(matViewIndexGen);
    doReturn(false).when(newGenerator).isLeader();
    doReturn(IndexManager.State.INITIALIZING).when(newGenerator).getState();

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
                    ? IndexManager.State.STEADY_STATE
                    : IndexManager.State.FAILED)
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
    doReturn(IndexManager.State.INITIAL_SYNC_BACKOFF).when(generator).getState();
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
    // Simulate add() retaining leadership for the new generation.
    when(mocks.leaseManager.isLeader(gen2Id)).thenReturn(true);

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
  public void replaceGenerator_differentGenId_losesLeadershipDuringShutdown_staysFollower() {
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();

    var gen1Id = MOCK_MAT_VIEW_GENERATION_ID;
    var matViewIndexGen1 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen1Id));
    when(mocks.leaseManager.isLeader(gen1Id)).thenReturn(true);
    MaterializedViewGenerator gen1 = mocks.mockMaterializedViewGenerator(matViewIndexGen1);
    mocks.addIndexForReplication(matViewIndexGen1);

    CompletableFuture<Void> slowShutdownFuture = new CompletableFuture<>();
    doReturn(slowShutdownFuture).when(gen1).shutdown();

    var gen2Id =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var matViewIndexGen2 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2Id, 1));
    MaterializedViewGenerator gen2 = mocks.mockMaterializedViewGenerator(matViewIndexGen2);
    when(mocks.leaseManager.isLeader(gen2Id)).thenReturn(false);

    mocks.addIndexForReplication(matViewIndexGen2);
    slowShutdownFuture.complete(null);

    verify(mocks.leaseManager).drop(gen1Id);
    verify(mocks.leaseManager).add(matViewIndexGen2, false);
    verify(gen2, never()).becomeLeader();
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

    // Add gen2 (higher version -> different genId) while the new generation remains a follower.
    var gen2Id =
        new MaterializedViewGenerationId(
            MOCK_MAT_VIEW_GENERATION_ID.indexId,
            MOCK_MAT_VIEW_GENERATION_ID.generation.incrementUser());
    var matViewIndexGen2 = mockMatViewIndexGeneration(mockMatViewDefinitionGeneration(gen2Id, 1));
    MaterializedViewGenerator gen2 = mocks.mockMaterializedViewGenerator(matViewIndexGen2);
    Mockito.clearInvocations(gen1, gen2);
    mocks.addIndexForReplication(matViewIndexGen2);

    // becomeLeader should NOT have been called on either generator because add() left the new
    // generation as a follower.
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

  // ==================== Dynamic Leader Restart Tests ====================

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

  @Test
  public void dynamicLeader_restartWithExpiredLease_doesNotActivateLeadership() throws Exception {
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    MaterializedViewIndexGeneration matViewIndexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    MaterializedViewGenerationId generationId = matViewIndexGeneration.getGenerationId();

    AtomicInteger registrations = new AtomicInteger();
    AtomicBoolean isLeader = new AtomicBoolean();
    doAnswer(
            invocation -> {
              isLeader.set(registrations.getAndIncrement() == 0);
              return null;
            })
        .when(mocks.leaseManager)
        .add(matViewIndexGeneration, false);
    when(mocks.leaseManager.isLeader(generationId)).thenAnswer(invocation -> isLeader.get());
    when(mocks.materializedViewGeneratorFactory.shutdown()).thenReturn(COMPLETED_FUTURE);
    when(mocks.materializedViewGeneratorFactory.isInitialized()).thenReturn(true);

    MaterializedViewGenerator oldGenerator =
        mocks.mockMaterializedViewGenerator(matViewIndexGeneration);
    mocks.addIndexForReplication(matViewIndexGeneration);
    verify(oldGenerator).becomeLeader();

    mocks.manager.shutdownReplication().get(5, TimeUnit.SECONDS);

    MaterializedViewGenerator restartedGenerator =
        mocks.mockMaterializedViewGenerator(matViewIndexGeneration);
    mocks.manager.restartReplication();

    verify(mocks.leaseManager, times(2)).add(matViewIndexGeneration, false);
    verify(mocks.materializedViewGeneratorFactory, times(2)).create(matViewIndexGeneration);
    assertSame(restartedGenerator, mocks.manager.getMatViewGenerator(generationId).orElseThrow());
    verify(restartedGenerator, never()).becomeLeader();
    assertFalse(restartedGenerator.isLeader());
  }

  // ==================== Stale-Lease GC Scanner Tests ====================

  // A lease expired this long ago is past the GC delete grace the manager uses. Derived from the
  // same config default the manager reads, so cleanup tests stay correct if that
  // default changes.
  private static final Duration PAST_DELETE_GRACE =
      Duration.ofMillis(
              AutoEmbeddingMaterializedViewConfig.getDefault().staleLeaseDeleteGracePeriodMs)
          .plus(Duration.ofHours(1));

  private static Lease leaseWithExpiration(
      String leaseKey, Instant leaseExpiration, Lease.CleanupState cleanupState) {
    return leaseWithExpiration(leaseKey, leaseExpiration, cleanupState, UUID.randomUUID());
  }

  private static Lease leaseWithExpiration(
      String leaseKey, Instant leaseExpiration, Lease.CleanupState cleanupState, UUID matViewUuid) {
    return leaseWithExpiration(leaseKey, leaseExpiration, cleanupState, matViewUuid, "0");
  }

  private static Lease leaseWithExpiration(
      String leaseKey,
      Instant leaseExpiration,
      Lease.CleanupState cleanupState,
      UUID matViewUuid,
      String latestIndexDefinitionVersion) {
    return new Lease(
        leaseKey,
        Lease.SCHEMA_VERSION,
        UUID.randomUUID().toString(),
        leaseKey,
        "some-other-mongot",
        leaseExpiration,
        1L,
        "",
        latestIndexDefinitionVersion,
        Map.of(),
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
            matViewUuid,
            leaseKey),
        null,
        cleanupState);
  }

  /**
   * A fresh, higher-version lease of the same index as {@code superseded}. The sweep derives the
   * never-mark-current set from the scan itself, so a stale lease is only markable if the scan also
   * holds a newer sibling of its index — without one, it is its index's current version and is
   * protected. The sibling carries a v1-format name ({@code "<indexId>-<hash>-1-<aeVersion>"}), so
   * {@link Lease#indexId()} groups it with the (v0-named, dash-free) superseded lease.
   */
  private static Lease currentVersionSiblingOf(Lease superseded) {
    return leaseWithExpiration(
        superseded.indexId() + "-h2-1-1",
        Instant.now().plus(Duration.ofMinutes(5)),
        Lease.CleanupState.NOT_ELIGIBLE,
        UUID.randomUUID(),
        "1");
  }

  @Test
  public void scanStaleLeases_threadAlwaysScheduled() {
    Mocks mocks = Mocks.create();

    // The GC scanner thread is always scheduled; the MV_GC_ENABLED dynamic flag gates the work per
    // tick (not the scheduling), so cleanup can be toggled at runtime without a restart.
    verify(mocks.staleLeaseScannerExecutor)
        .scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
  }

  @Test
  public void scanStaleLeases_flagDisabled_doesNothing() {
    Mocks mocks = Mocks.create();
    when(mocks.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
            DynamicFeatureFlags.MV_GC_ENABLED))
        .thenReturn(false);

    mocks.manager.scanStaleLeasesForCleanup();

    // Flag off: the sweep short-circuits before it ever queries for candidates.
    verify(mocks.leaseManager, never()).findGcCandidates();
  }

  @Test
  public void scanStaleLeases_flagEvaluationThrows_containedNotPropagated() {
    Mocks mocks = Mocks.create();
    when(mocks.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
            DynamicFeatureFlags.MV_GC_ENABLED))
        .thenThrow(new RuntimeException("transient registry error"));

    // Must not throw: a flag-evaluation failure has to be contained, otherwise VerboseRunnable
    // rethrows and scheduleWithFixedDelay cancels the scanner for the life of the process.
    mocks.manager.scanStaleLeasesForCleanup();

    // Contained before the sweep touched the lease manager; the task keeps ticking next interval.
    verify(mocks.leaseManager, never()).findGcCandidates();
  }

  @Test
  public void scanStaleLeases_deletionFlagDisabled_marksButDoesNotDelete() {
    Mocks mocks = Mocks.create();
    // GC on, deletion off: mark-only mode.
    when(mocks.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
            DynamicFeatureFlags.MV_GC_DELETION_ENABLED))
        .thenReturn(false);
    // A never-marked stale lease (the mark pass should flag it) and an already-marked overdue lease
    // (the cleanup pass must NOT delete it while deletion is disabled).
    Lease toMark =
        leaseWithExpiration(
            "toMark", Instant.now().minus(Duration.ofHours(1)), Lease.CleanupState.NOT_ELIGIBLE);
    Lease overdue =
        leaseWithExpiration(
            "overdue",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(toMark, currentVersionSiblingOf(toMark), overdue));
    when(mocks.leaseManager.markEligibleForCleanup(eq("toMark"), any())).thenReturn(true);

    mocks.manager.scanStaleLeasesForCleanup();

    // Mark pass still runs ...
    verify(mocks.leaseManager).markEligibleForCleanup(eq("toMark"), any());
    // ... but nothing is dropped or deleted.
    verify(mocks.leaseManager, never()).resolveStaleMatViewNamespace(any(), anyLong(), any());
    verify(mocks.leaseManager, never()).deleteStaleLease(any(), anyLong(), any());
    // Deletion deliberately off: the overdue gauge resets to 0 (nothing is blocked).
    assertEquals(0.0, overdueGaugeValue(mocks), 0.0);
  }

  @Test
  public void scanStaleLeases_scannerAliveGauge_tracksDynamicFlag() {
    Mocks mocks = Mocks.create();

    // Flag on (Mocks default): A is registered at construction and reads 1 (GC running) once the
    // sweep has run with the flag on.
    mocks.manager.scanStaleLeasesForCleanup();
    assertEquals(
        1.0,
        mocks
            .meterRegistry
            .get("replication.mongodb." + MaterializedViewManager.MAT_VIEW_GC_SCANNER_ALIVE_GAUGE)
            .gauge()
            .value(),
        0.0);

    // Control plane turns the flag off: the bound gauge re-evaluates the flag on scrape and drops
    // to 0 in lockstep — no restart, no re-registration, even though the scanner thread is still
    // scheduled.
    when(mocks.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
            DynamicFeatureFlags.MV_GC_ENABLED))
        .thenReturn(false);
    assertEquals(
        0.0,
        mocks
            .meterRegistry
            .get("replication.mongodb." + MaterializedViewManager.MAT_VIEW_GC_SCANNER_ALIVE_GAUGE)
            .gauge()
            .value(),
        0.0);
  }

  /** Current value of the single aggregate overdue gauge (matViewGcLeasesOverdue). */
  private static double overdueGaugeValue(Mocks mocks) {
    return mocks
        .meterRegistry
        .get("replication.mongodb." + MaterializedViewManager.MAT_VIEW_GC_LEASES_OVERDUE_GAUGE)
        .gauge()
        .value();
  }

  /** Count of the shared matViewCollectionDrop counter for one scenario+outcome tag combination. */
  private static double collectionDropCount(Mocks mocks, String scenario, String outcome) {
    return mocks
        .meterRegistry
        .counter(
            "replication.mongodb." + MaterializedViewManager.MAT_VIEW_COLLECTION_DROP_COUNTER_NAME,
            MaterializedViewManager.DROP_SCENARIO_TAG,
            scenario,
            MaterializedViewManager.DROP_OUTCOME_TAG,
            outcome)
        .count();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void scanStaleLeases_overdueGauge_countsBlockedNotWithinGrace() {
    Mocks mocks = Mocks.create();
    // "blocked": past the delete grace, but its MV drop fails, so the cleanup pass can't delete it.
    Lease blocked =
        leaseWithExpiration(
            "blocked",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    // "within-grace": marked but not yet past the delete grace — not overdue, so not counted.
    Lease withinGrace =
        leaseWithExpiration(
            "within-grace",
            Instant.now().minus(Duration.ofMinutes(35)),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(blocked, currentVersionSiblingOf(blocked), withinGrace));
    when(mocks.leaseManager.resolveStaleMatViewNamespace(eq("blocked"), anyLong(), any()))
        .thenReturn(Optional.of(new MongoNamespace("test-db", "mv-collection")));
    MongoClient writerClient = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> collection = mock(MongoCollection.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.of(writerClient));
    when(writerClient.getDatabase("test-db")).thenReturn(database);
    when(database.getCollection("mv-collection")).thenReturn(collection);
    doThrow(new RuntimeException("drop failed")).when(collection).drop();

    mocks.manager.scanStaleLeasesForCleanup();

    // Only the blocked overdue lease is counted; the within-grace one isn't overdue, and the count
    // is a single aggregate (no per-lease series).
    assertEquals(1.0, overdueGaugeValue(mocks), 0.0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void scanStaleLeases_overdueGauge_resetsWhenFlagDisabled() {
    Mocks mocks = Mocks.create();
    Lease blocked =
        leaseWithExpiration(
            "blocked",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(blocked, currentVersionSiblingOf(blocked)));
    when(mocks.leaseManager.resolveStaleMatViewNamespace(eq("blocked"), anyLong(), any()))
        .thenReturn(Optional.of(new MongoNamespace("test-db", "mv-collection")));
    MongoClient writerClient = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> collection = mock(MongoCollection.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.of(writerClient));
    when(writerClient.getDatabase("test-db")).thenReturn(database);
    when(database.getCollection("mv-collection")).thenReturn(collection);
    doThrow(new RuntimeException("drop failed")).when(collection).drop();

    mocks.manager.scanStaleLeasesForCleanup();
    assertEquals(1.0, overdueGaugeValue(mocks), 0.0);

    // Control plane disables GC: the sweep early-returns but resets the push-based gauge to 0,
    // rather than holding the stale 1 while GC is off.
    when(mocks.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
            DynamicFeatureFlags.MV_GC_ENABLED))
        .thenReturn(false);
    mocks.manager.scanStaleLeasesForCleanup();
    assertEquals(0.0, overdueGaugeValue(mocks), 0.0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void scanStaleLeases_overdueGauge_resetsWhenDeletionFlagDisabled() {
    Mocks mocks = Mocks.create();
    // Deletion on, one blocked overdue lease (its MV drop fails): the gauge reads 1.
    Lease blocked =
        leaseWithExpiration(
            "blocked",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(blocked, currentVersionSiblingOf(blocked)));
    when(mocks.leaseManager.resolveStaleMatViewNamespace(eq("blocked"), anyLong(), any()))
        .thenReturn(Optional.of(new MongoNamespace("test-db", "mv-collection")));
    MongoClient writerClient = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> collection = mock(MongoCollection.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.of(writerClient));
    when(writerClient.getDatabase("test-db")).thenReturn(database);
    when(database.getCollection("mv-collection")).thenReturn(collection);
    doThrow(new RuntimeException("drop failed")).when(collection).drop();

    mocks.manager.scanStaleLeasesForCleanup();
    assertEquals(1.0, overdueGaugeValue(mocks), 0.0);

    // Deletion turned off (GC stays on, mark-only): the next sweep resets the gauge to 0 even
    // though the blocked lease is still a candidate.
    when(mocks.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
            DynamicFeatureFlags.MV_GC_DELETION_ENABLED))
        .thenReturn(false);
    mocks.manager.scanStaleLeasesForCleanup();
    assertEquals(0.0, overdueGaugeValue(mocks), 0.0);
  }

  @Test
  public void scanStaleLeases_staleLease_marksEligible() {
    Mocks mocks = Mocks.create();
    Instant longExpired = Instant.now().minus(Duration.ofHours(1));
    Lease stale = leaseWithExpiration("staleLease", longExpired, Lease.CleanupState.NOT_ELIGIBLE);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(stale, currentVersionSiblingOf(stale)));
    when(mocks.leaseManager.markEligibleForCleanup(any(), any())).thenReturn(true);

    mocks.manager.scanStaleLeasesForCleanup();

    // The mark's OCC cutoff is now minus the grace period — the same cutoff that made the lease a
    // candidate in the sweep's client-side staleness filter.
    ArgumentCaptor<Instant> markCutoff = ArgumentCaptor.forClass(Instant.class);
    verify(mocks.leaseManager).markEligibleForCleanup(eq("staleLease"), markCutoff.capture());
    assertTrue(markCutoff.getValue().isBefore(Instant.now()));
    assertTrue(markCutoff.getValue().isAfter(longExpired));
  }

  @Test
  public void scanStaleLeases_expiredWithinMarkGrace_notACandidate() {
    Mocks mocks = Mocks.create();
    // Expired, but more recently than the mark grace — the sweep's staleness filter must leave it
    // alone. A fresh higher-version sibling is in the scan, so the grace filter (not the
    // never-mark-current safeguard) is what protects it.
    Lease recentlyExpired =
        leaseWithExpiration(
            "recentLease",
            Instant.now().minus(Duration.ofMinutes(10)),
            Lease.CleanupState.NOT_ELIGIBLE);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(recentlyExpired, currentVersionSiblingOf(recentlyExpired)));

    mocks.manager.scanStaleLeasesForCleanup();

    verify(mocks.leaseManager, never()).markEligibleForCleanup(any(), any());
    verify(mocks.leaseManager, never()).resolveStaleMatViewNamespace(any(), anyLong(), any());
  }

  @Test
  public void scanStaleLeases_alreadyEligible_skips() {
    Mocks mocks = Mocks.create();
    Lease alreadyMarked =
        leaseWithExpiration(
            "already-marked",
            Instant.now().minus(Duration.ofHours(2)),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates()).thenReturn(List.of(alreadyMarked));

    mocks.manager.scanStaleLeasesForCleanup();

    verify(mocks.leaseManager, never()).markEligibleForCleanup(any(), any());
  }

  @Test
  public void scanStaleLeases_mixedLeases_marksOnlyNotEligible() {
    Mocks mocks = Mocks.create();
    Lease stale =
        leaseWithExpiration(
            "staleLease",
            Instant.now().minus(Duration.ofHours(1)),
            Lease.CleanupState.NOT_ELIGIBLE);
    Lease alreadyMarked =
        leaseWithExpiration(
            "already-marked",
            Instant.now().minus(Duration.ofHours(2)),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(stale, currentVersionSiblingOf(stale), alreadyMarked));
    when(mocks.leaseManager.markEligibleForCleanup(any(), any())).thenReturn(true);

    mocks.manager.scanStaleLeasesForCleanup();

    verify(mocks.leaseManager).markEligibleForCleanup(eq("staleLease"), any());
    verify(mocks.leaseManager, never()).markEligibleForCleanup(eq("already-marked"), any());
  }

  @Test
  public void scanStaleLeases_markThrows_continuesToNextLease() {
    Mocks mocks = Mocks.create();
    Lease throwing =
        leaseWithExpiration(
            "throwingLease",
            Instant.now().minus(Duration.ofHours(1)),
            Lease.CleanupState.NOT_ELIGIBLE);
    Lease second =
        leaseWithExpiration(
            "secondLease",
            Instant.now().minus(Duration.ofHours(2)),
            Lease.CleanupState.NOT_ELIGIBLE);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(
            List.of(
                throwing, currentVersionSiblingOf(throwing),
                second, currentVersionSiblingOf(second)));
    when(mocks.leaseManager.markEligibleForCleanup(eq("throwingLease"), any()))
        .thenThrow(new RuntimeException("simulated mark failure"));
    when(mocks.leaseManager.markEligibleForCleanup(eq("secondLease"), any())).thenReturn(true);

    mocks.manager.scanStaleLeasesForCleanup();

    // The first lease's failure is contained per-iteration; the sweep still marks the second.
    verify(mocks.leaseManager).markEligibleForCleanup(eq("secondLease"), any());
  }

  @Test
  public void scanStaleLeases_scanThrows_isContained() {
    Mocks mocks = Mocks.create();
    when(mocks.leaseManager.findGcCandidates())
        .thenThrow(new RuntimeException("simulated scan failure"));

    // Must not propagate: VerboseRunnable rethrows and scheduleWithFixedDelay would cancel the
    // periodic task permanently.
    mocks.manager.scanStaleLeasesForCleanup();

    verify(mocks.leaseManager, never()).markEligibleForCleanup(any(), any());
    // matViewGcSweepFailures: the aborted tick is counted — otherwise a persistently failing
    // sweep is invisible in metrics (scannerAlive stays 1 and the overdue gauge just holds).
    assertEquals(
        1.0,
        mocks
            .meterRegistry
            .counter(MaterializedViewManager.MAT_VIEW_GC_SWEEP_FAILURE_COUNTER_NAME)
            .count(),
        0.0);
  }

  @Test
  public void scanStaleLeases_replicationDisabled_noOp() {
    Mocks mocks = Mocks.create();
    mocks.manager.setIsReplicationEnabled(false);

    // The scanner is scheduled unconditionally, so the isReplicationSupported() guard must keep it
    // from touching the lease store when replication is disabled.
    mocks.manager.scanStaleLeasesForCleanup();

    verify(mocks.leaseManager, never()).findGcCandidates();
    verify(mocks.leaseManager, never()).markEligibleForCleanup(any(), any());
  }

  // ==================== dropStaleMatViewCollection ====================

  @Test
  @SuppressWarnings("unchecked")
  public void dropStaleMatViewCollection_resolvedNamespace_dropsCollection() {
    Mocks mocks = Mocks.create();
    Instant cutoff = Instant.now();
    when(mocks.leaseManager.resolveStaleMatViewNamespace("lease-key", 1L, cutoff))
        .thenReturn(Optional.of(new MongoNamespace("test-db", "mv-collection")));
    MongoClient writerClient = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> collection = mock(MongoCollection.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.of(writerClient));
    when(writerClient.getDatabase("test-db")).thenReturn(database);
    when(database.getCollection("mv-collection")).thenReturn(collection);

    assertTrue(mocks.manager.dropStaleMatViewCollection("lease-key", 1L, cutoff));
    verify(collection).drop();
  }

  @Test
  public void dropStaleMatViewCollection_notResolved_returnsFalse() {
    Mocks mocks = Mocks.create();
    when(mocks.leaseManager.resolveStaleMatViewNamespace(any(), anyLong(), any()))
        .thenReturn(Optional.empty());

    assertFalse(mocks.manager.dropStaleMatViewCollection("lease-key", 1L, Instant.now()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void dropStaleMatViewCollection_dropThrows_returnsFalse() {
    Mocks mocks = Mocks.create();
    when(mocks.leaseManager.resolveStaleMatViewNamespace(any(), anyLong(), any()))
        .thenReturn(Optional.of(new MongoNamespace("test-db", "mv-collection")));
    MongoClient writerClient = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> collection = mock(MongoCollection.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.of(writerClient));
    when(writerClient.getDatabase("test-db")).thenReturn(database);
    when(database.getCollection("mv-collection")).thenReturn(collection);
    doThrow(new RuntimeException("drop failed")).when(collection).drop();

    assertFalse(mocks.manager.dropStaleMatViewCollection("lease-key", 1L, Instant.now()));
  }

  @Test
  public void dropStaleMatViewCollection_missingWriterClient_returnsFalse() {
    Mocks mocks = Mocks.create();
    when(mocks.leaseManager.resolveStaleMatViewNamespace(any(), anyLong(), any()))
        .thenReturn(Optional.of(new MongoNamespace("test-db", "mv-collection")));
    // Writer client absent: Check.isPresent throws AssertionError, swallowed as a false return.
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.empty());

    assertFalse(mocks.manager.dropStaleMatViewCollection("lease-key", 1L, Instant.now()));
  }

  // ==================== Cleanup pass (cleaner) ====================

  @Test
  @SuppressWarnings("unchecked")
  public void scanStaleLeases_markedPastDeleteGrace_dropsCollectionThenLease() {
    Mocks mocks = Mocks.create();
    // Marked in an earlier sweep, lapsed far past the delete grace.
    Lease marked =
        leaseWithExpiration(
            "cleanMe",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(marked, currentVersionSiblingOf(marked)));
    // Drive the real MaterializedViewManager.dropStaleMatViewCollection: resolve the namespace,
    // then drop the collection through the writer client.
    when(mocks.leaseManager.resolveStaleMatViewNamespace(eq("cleanMe"), anyLong(), any()))
        .thenReturn(Optional.of(new MongoNamespace("test-db", "mv-collection")));
    MongoClient writerClient = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> collection = mock(MongoCollection.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.of(writerClient));
    when(writerClient.getDatabase("test-db")).thenReturn(database);
    when(database.getCollection("mv-collection")).thenReturn(collection);
    when(mocks.leaseManager.deleteStaleLease(eq("cleanMe"), anyLong(), any())).thenReturn(true);

    mocks.manager.scanStaleLeasesForCleanup();

    // Collection first, lease second: the lease is the GC source of truth.
    InOrder inOrder = Mockito.inOrder(collection, mocks.leaseManager);
    inOrder.verify(collection).drop();
    inOrder.verify(mocks.leaseManager).deleteStaleLease(eq("cleanMe"), anyLong(), any());
    // Already-marked leases are not re-marked.
    verify(mocks.leaseManager, never()).markEligibleForCleanup(any(), any());
    // The namespace resolve (inside the drop) and the OCC delete receive the SAME cutoff, and it
    // is the delete grace (now − delete grace), not the mark grace — that cutoff drives the
    // pre-drop re-read veto and the OCC delete filter, the last defenses against cleaning up a
    // resurrected lease.
    ArgumentCaptor<Instant> resolveCutoff = ArgumentCaptor.forClass(Instant.class);
    ArgumentCaptor<Instant> deleteCutoff = ArgumentCaptor.forClass(Instant.class);
    verify(mocks.leaseManager)
        .resolveStaleMatViewNamespace(eq("cleanMe"), anyLong(), resolveCutoff.capture());
    verify(mocks.leaseManager).deleteStaleLease(eq("cleanMe"), anyLong(), deleteCutoff.capture());
    assertEquals(resolveCutoff.getValue(), deleteCutoff.getValue());
    Instant expectedDeleteCutoff =
        Instant.now()
            .minus(
                Duration.ofMillis(
                    AutoEmbeddingMaterializedViewConfig.getDefault()
                        .staleLeaseDeleteGracePeriodMs));
    assertTrue(
        "delete cutoff should be ~now − the delete grace, not the mark grace",
        Duration.between(deleteCutoff.getValue(), expectedDeleteCutoff).abs().toMinutes() < 5);
    // The overdue lease was cleaned this sweep, so it is not counted as blocked.
    assertEquals(0.0, overdueGaugeValue(mocks), 0.0);
  }

  @Test
  public void scanStaleLeases_markedThisSweep_notCleanedUpInSameSweep() {
    Mocks mocks = Mocks.create();
    // Ancient lease, but NOT_ELIGIBLE in this sweep's snapshot: the mark pass marks it, and the
    // cleanup pass must wait for a later sweep — mark and cleanup always span two sweeps.
    Lease stale =
        leaseWithExpiration(
            "justMarked", Instant.now().minus(Duration.ofDays(1)), Lease.CleanupState.NOT_ELIGIBLE);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(stale, currentVersionSiblingOf(stale)));
    when(mocks.leaseManager.markEligibleForCleanup(eq("justMarked"), any())).thenReturn(true);

    mocks.manager.scanStaleLeasesForCleanup();

    verify(mocks.leaseManager).markEligibleForCleanup(eq("justMarked"), any());
    // cleanUpStaleLease returns at the cleanupState check before touching the collection.
    verify(mocks.leaseManager, never()).resolveStaleMatViewNamespace(any(), anyLong(), any());
    verify(mocks.leaseManager, never()).deleteStaleLease(any(), anyLong(), any());
    // The two-sweep guarantee rests on a single scan per sweep: both passes act on the same
    // snapshot, so a lease the mark pass just flipped is still NOT_ELIGIBLE to the cleanup pass.
    // A re-scan here would re-read it as ELIGIBLE and clean it up same-sweep.
    verify(mocks.leaseManager, times(1)).findGcCandidates();
  }

  @Test
  public void scanStaleLeases_markedWithinDeleteGrace_notCleanedUp() {
    Mocks mocks = Mocks.create();
    // Past the 30-minute mark grace (so it is a candidate) but not the (much longer) delete grace.
    Lease marked =
        leaseWithExpiration(
            "too-fresh",
            Instant.now().minus(Duration.ofMinutes(35)),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates()).thenReturn(List.of(marked));

    mocks.manager.scanStaleLeasesForCleanup();

    // cleanUpStaleLease returns at the delete-grace check before touching the collection.
    verify(mocks.leaseManager, never()).resolveStaleMatViewNamespace(any(), anyLong(), any());
    verify(mocks.leaseManager, never()).deleteStaleLease(any(), anyLong(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void scanStaleLeases_collectionDropFails_leaseKept() {
    Mocks mocks = Mocks.create();
    Lease marked =
        leaseWithExpiration(
            "dropFails",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(marked, currentVersionSiblingOf(marked)));
    when(mocks.leaseManager.resolveStaleMatViewNamespace(eq("dropFails"), anyLong(), any()))
        .thenReturn(Optional.of(new MongoNamespace("test-db", "mv-collection")));
    MongoClient writerClient = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> collection = mock(MongoCollection.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.of(writerClient));
    when(writerClient.getDatabase("test-db")).thenReturn(database);
    when(database.getCollection("mv-collection")).thenReturn(collection);
    doThrow(new RuntimeException("drop failed")).when(collection).drop();

    mocks.manager.scanStaleLeasesForCleanup();

    // dropStaleMatViewCollection swallows the failure and returns false; the lease survives so the
    // next sweep can rediscover and retry.
    verify(mocks.leaseManager, never()).deleteStaleLease(any(), anyLong(), any());
    // matViewCollectionDrop{scenario=gc, outcome=failure}: the GC drop failure is counted (alert
    // source for GC blocked on MV deletion).
    assertEquals(
        1.0, collectionDropCount(mocks, MaterializedViewManager.DROP_SCENARIO_GC, "failure"), 0.0);
    assertEquals(
        0.0, collectionDropCount(mocks, MaterializedViewManager.DROP_SCENARIO_GC, "success"), 0.0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void scanStaleLeases_collectionDropSucceeds_countsGcSuccess() {
    Mocks mocks = Mocks.create();
    Lease marked =
        leaseWithExpiration(
            "dropOk",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(marked, currentVersionSiblingOf(marked)));
    when(mocks.leaseManager.resolveStaleMatViewNamespace(eq("dropOk"), anyLong(), any()))
        .thenReturn(Optional.of(new MongoNamespace("test-db", "mv-collection")));
    MongoClient writerClient = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> collection = mock(MongoCollection.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.of(writerClient));
    when(writerClient.getDatabase("test-db")).thenReturn(database);
    when(database.getCollection("mv-collection")).thenReturn(collection);
    when(mocks.leaseManager.deleteStaleLease(eq("dropOk"), anyLong(), any())).thenReturn(true);

    mocks.manager.scanStaleLeasesForCleanup();

    // The drop (a void no-op on the mock) succeeds, so the lease is deleted.
    verify(mocks.leaseManager).deleteStaleLease(eq("dropOk"), anyLong(), any());
    // matViewCollectionDrop{scenario=gc, outcome=success}, and no failure recorded.
    assertEquals(
        1.0, collectionDropCount(mocks, MaterializedViewManager.DROP_SCENARIO_GC, "success"), 0.0);
    assertEquals(
        0.0, collectionDropCount(mocks, MaterializedViewManager.DROP_SCENARIO_GC, "failure"), 0.0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void scanStaleLeases_cleanupThrows_continuesToNextLease() {
    Mocks mocks = Mocks.create();
    Lease throwing =
        leaseWithExpiration(
            "throwingCleanup",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    Lease second =
        leaseWithExpiration(
            "secondCleanup",
            Instant.now().minus(PAST_DELETE_GRACE.plusHours(1)),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(
            List.of(
                throwing, currentVersionSiblingOf(throwing),
                second, currentVersionSiblingOf(second)));
    // The first lease's cleanup blows up (resolve runs before the drop's try/catch, so it
    // propagates); the per-lease catch must contain it and move on.
    when(mocks.leaseManager.resolveStaleMatViewNamespace(eq("throwingCleanup"), anyLong(), any()))
        .thenThrow(new RuntimeException("simulated cleanup failure"));
    when(mocks.leaseManager.resolveStaleMatViewNamespace(eq("secondCleanup"), anyLong(), any()))
        .thenReturn(Optional.of(new MongoNamespace("test-db", "mv-collection")));
    MongoClient writerClient = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> collection = mock(MongoCollection.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.of(writerClient));
    when(writerClient.getDatabase("test-db")).thenReturn(database);
    when(database.getCollection("mv-collection")).thenReturn(collection);
    when(mocks.leaseManager.deleteStaleLease(eq("secondCleanup"), anyLong(), any()))
        .thenReturn(true);

    mocks.manager.scanStaleLeasesForCleanup();

    // The first lease's failure is contained per-iteration; the second is still cleaned up.
    verify(mocks.leaseManager).deleteStaleLease(eq("secondCleanup"), anyLong(), any());
  }

  @Test
  public void scanStaleLeases_matViewUsedByLocalGeneration_notMarked() {
    Mocks mocks = Mocks.create();
    UUID matViewUuid = UUID.randomUUID();
    Lease inUse =
        leaseWithExpiration(
            "inUse",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.NOT_ELIGIBLE,
            matViewUuid);
    // A fresh higher-version sibling keeps the never-mark-current safeguard out of the way; the
    // in-use guard is what must protect this lease.
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(inUse, currentVersionSiblingOf(inUse)));
    // A local generation still reads this collection, so this mongot still needs it — the mark-time
    // safeguard must skip it even though its heartbeat has gone stale.
    synchronized (mocks.manager) {
      mocks.manager.activeGenerationIdCatalog.genIdByMatViewCollection.put(matViewUuid, Set.of());
    }

    mocks.manager.scanStaleLeasesForCleanup();

    // Never marked, so it never reaches the OCC-guarded cleanup phase either.
    verify(mocks.leaseManager, never()).markEligibleForCleanup(eq("inUse"), any());
    verify(mocks.leaseManager, never()).resolveStaleMatViewNamespace(any(), anyLong(), any());
    verify(mocks.leaseManager, never()).deleteStaleLease(any(), anyLong(), any());
    // matViewGcLeaseInUseRefusals: the refusal is counted — a non-zero value warrants
    // investigation.
    assertEquals(
        1.0,
        mocks
            .meterRegistry
            .counter(MaterializedViewManager.MAT_VIEW_GC_LEASE_IN_USE_REFUSAL_COUNTER_NAME)
            .count(),
        0.0);
  }

  @Test
  public void scanStaleLeases_matViewUsedByLocalGenerationAtDeleteTime_notDeleted() {
    Mocks mocks = Mocks.create();
    UUID matViewUuid = UUID.randomUUID();
    // Already marked (by some other mongot) and past the delete grace, so it reaches the cleanup
    // pass — but a local generation on THIS mongot still reads its MV. GC is leaderless, so the
    // marking mongot and the deleting mongot can differ; the in-use guard must re-run at delete
    // time because a local reader never bumps the lease version for OCC to catch. The fresh
    // higher-version sibling keeps the never-touch-current safeguard out of the way.
    Lease inUse =
        leaseWithExpiration(
            "inUseAtDelete",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP,
            matViewUuid);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(inUse, currentVersionSiblingOf(inUse)));
    synchronized (mocks.manager) {
      mocks.manager.activeGenerationIdCatalog.genIdByMatViewCollection.put(matViewUuid, Set.of());
    }

    mocks.manager.scanStaleLeasesForCleanup();

    // The destructive drop/delete is refused before touching the collection or the lease.
    verify(mocks.leaseManager, never()).resolveStaleMatViewNamespace(any(), anyLong(), any());
    verify(mocks.leaseManager, never()).deleteStaleLease(any(), anyLong(), any());
    // matViewGcLeaseInUseRefusals: the delete-time refusal is counted, same as the mark-time one.
    assertEquals(
        1.0,
        mocks
            .meterRegistry
            .counter(MaterializedViewManager.MAT_VIEW_GC_LEASE_IN_USE_REFUSAL_COUNTER_NAME)
            .count(),
        0.0);
    // Overdue but refused, so it counts as blocked (not cleaned this sweep).
    assertEquals(1.0, overdueGaugeValue(mocks), 0.0);
  }

  @Test
  public void scanStaleLeases_markedCurrentVersionLease_notDeleted() {
    Mocks mocks = Mocks.create();
    // Marked and long past the delete grace, but it is its index's current (highest) version:
    // the mark didn't come from this code's mark pass (older build, manual edit), or its siblings
    // were deleted after it was marked. The cleanup pass must re-apply the never-touch-current
    // safeguard rather than trust the mark.
    Lease markedCurrent =
        leaseWithExpiration(
            "markedCurrent",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates()).thenReturn(List.of(markedCurrent));

    mocks.manager.scanStaleLeasesForCleanup();

    // Refused before touching the collection or the lease.
    verify(mocks.leaseManager, never()).resolveStaleMatViewNamespace(any(), anyLong(), any());
    verify(mocks.leaseManager, never()).deleteStaleLease(any(), anyLong(), any());
    // Overdue but refused: counted as blocked so the anomalous marked-current state is visible.
    assertEquals(1.0, overdueGaugeValue(mocks), 0.0);
    // matViewGcMarkedCurrentRefusals: the refusal is counted — recurring refusals mean some
    // writer violated the never-mark-current rule and are alertable.
    assertEquals(
        1.0,
        mocks
            .meterRegistry
            .counter(MaterializedViewManager.MAT_VIEW_GC_MARKED_CURRENT_REFUSAL_COUNTER_NAME)
            .count(),
        0.0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void scanStaleLeases_dropSucceedsButLeaseDeleteMisses_noErrorAndMovesOn() {
    Mocks mocks = Mocks.create();
    // Marked + past the delete grace, so the collection is dropped — but the OCC lease delete
    // misses: the lease was reused/renewed in the drop→delete window, so it no longer matches
    // (cleanupState == ELIGIBLE_FOR_CLEANUP && expired). deleteStaleLease returns false. The
    // cleaner must simply move on — no throw, no retry, no re-mark. A later sweep won't re-clean
    // it either: a reused lease's expiration is now in the future, so it is no longer a candidate.
    Lease marked =
        leaseWithExpiration(
            "deleteMisses",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(marked, currentVersionSiblingOf(marked)));
    when(mocks.leaseManager.resolveStaleMatViewNamespace(eq("deleteMisses"), anyLong(), any()))
        .thenReturn(Optional.of(new MongoNamespace("test-db", "mv-collection")));
    MongoClient writerClient = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> collection = mock(MongoCollection.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.of(writerClient));
    when(writerClient.getDatabase("test-db")).thenReturn(database);
    when(database.getCollection("mv-collection")).thenReturn(collection);
    // Collection drop succeeds; the OCC lease delete misses.
    when(mocks.leaseManager.deleteStaleLease(eq("deleteMisses"), anyLong(), any()))
        .thenReturn(false);

    // Must not throw.
    mocks.manager.scanStaleLeasesForCleanup();

    // The collection was dropped and the delete was attempted exactly once; the cleaner then moves
    // on without re-marking or retrying.
    verify(collection).drop();
    verify(mocks.leaseManager).deleteStaleLease(eq("deleteMisses"), anyLong(), any());
    verify(mocks.leaseManager, never()).markEligibleForCleanup(any(), any());
  }

  @Test
  public void scanStaleLeases_currentVersionLease_notMarked() {
    Mocks mocks = Mocks.create();
    // Both leases of index "idxA" are stale (e.g. a fleet-wide outage), but one holds the higher
    // definition version. The sweep derives the never-mark set from the scan itself, so it must
    // recognize the higher-version lease as current and mark only the superseded one.
    Instant longExpired = Instant.now().minus(Duration.ofHours(1));
    Lease currentVersion =
        leaseWithExpiration(
            "idxA-h2-1-1", longExpired, Lease.CleanupState.NOT_ELIGIBLE, UUID.randomUUID(), "1");
    Lease supersededVersion =
        leaseWithExpiration(
            "idxA-h1-1-0", longExpired, Lease.CleanupState.NOT_ELIGIBLE, UUID.randomUUID(), "0");
    when(mocks.leaseManager.findGcCandidates())
        .thenReturn(List.of(currentVersion, supersededVersion));
    when(mocks.leaseManager.markEligibleForCleanup(any(), any())).thenReturn(true);

    mocks.manager.scanStaleLeasesForCleanup();

    // The current version is protected; only the superseded lease is marked.
    verify(mocks.leaseManager, never()).markEligibleForCleanup(eq("idxA-h2-1-1"), any());
    verify(mocks.leaseManager).markEligibleForCleanup(eq("idxA-h1-1-0"), any());
  }

  @Test
  public void scanStaleLeases_soleLeaseOfIndex_neverMarked() {
    Mocks mocks = Mocks.create();
    // A stale lease with no higher-version sibling in the scan is its index's current version and
    // must never be marked. This is also what makes a partial scan (a failed database dropping
    // leases from the result) safe: marking requires a present higher-version sibling, so a
    // missing lease can only leave its siblings protected like this one, never expose them.
    Lease sole =
        leaseWithExpiration(
            "soleLease", Instant.now().minus(Duration.ofHours(1)), Lease.CleanupState.NOT_ELIGIBLE);
    when(mocks.leaseManager.findGcCandidates()).thenReturn(List.of(sole));

    mocks.manager.scanStaleLeasesForCleanup();

    verify(mocks.leaseManager, never()).markEligibleForCleanup(any(), any());
  }

  @Test
  public void scanStaleLeases_deletedIndex_soleLeaseMarked() {
    Mocks mocks = Mocks.create();
    // The index is gone from the config: its highest-version (here: sole) lease is no longer a
    // current version to protect, or nothing would ever clean it up.
    ObjectId deletedIndexId = new ObjectId();
    Lease lastLease =
        leaseWithExpiration(
            deletedIndexId.toHexString() + "-h1-1-0",
            Instant.now().minus(Duration.ofHours(1)),
            Lease.CleanupState.NOT_ELIGIBLE);
    when(mocks.leaseManager.findGcCandidates()).thenReturn(List.of(lastLease));
    when(mocks.indexCatalog.getIndexById(deletedIndexId)).thenReturn(Optional.empty());
    when(mocks.leaseManager.markEligibleForCleanup(any(), any())).thenReturn(true);

    mocks.manager.scanStaleLeasesForCleanup();

    verify(mocks.leaseManager).markEligibleForCleanup(eq(lastLease.id()), any());
    // matViewGcDeletedIndexLeases: the unprotected deleted-index lease is counted.
    assertEquals(
        1.0,
        mocks
            .meterRegistry
            .counter(MaterializedViewManager.MAT_VIEW_GC_DELETED_INDEX_LEASE_COUNTER_NAME)
            .count(),
        0.0);
  }

  @Test
  public void scanStaleLeases_configuredIndex_soleLeaseStillProtected() {
    Mocks mocks = Mocks.create();
    // Same shape, but the index is still in the config (the Mocks default): the safeguard holds
    // even though the lease's heartbeat has gone stale.
    Lease sole =
        leaseWithExpiration(
            new ObjectId().toHexString() + "-h1-1-0",
            Instant.now().minus(Duration.ofHours(1)),
            Lease.CleanupState.NOT_ELIGIBLE);
    when(mocks.leaseManager.findGcCandidates()).thenReturn(List.of(sole));

    mocks.manager.scanStaleLeasesForCleanup();

    verify(mocks.leaseManager, never()).markEligibleForCleanup(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void scanStaleLeases_deletedIndex_markedOverdueLeaseCleaned() {
    Mocks mocks = Mocks.create();
    // Already marked (e.g. by the deletion drop path after a failed delete) and far past the
    // delete grace, with no higher-version sibling: the delete-time safeguard re-check must also
    // recognize the deleted index, or the lease stays refused forever.
    ObjectId deletedIndexId = new ObjectId();
    Lease marked =
        leaseWithExpiration(
            deletedIndexId.toHexString() + "-h1-1-0",
            Instant.now().minus(PAST_DELETE_GRACE),
            Lease.CleanupState.ELIGIBLE_FOR_CLEANUP);
    when(mocks.leaseManager.findGcCandidates()).thenReturn(List.of(marked));
    when(mocks.indexCatalog.getIndexById(deletedIndexId)).thenReturn(Optional.empty());
    when(mocks.leaseManager.resolveStaleMatViewNamespace(eq(marked.id()), anyLong(), any()))
        .thenReturn(Optional.of(new MongoNamespace("test-db", "mv-collection")));
    MongoClient writerClient = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> collection = mock(MongoCollection.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
        .thenReturn(Optional.of(writerClient));
    when(writerClient.getDatabase("test-db")).thenReturn(database);
    when(database.getCollection("mv-collection")).thenReturn(collection);
    when(mocks.leaseManager.deleteStaleLease(eq(marked.id()), anyLong(), any())).thenReturn(true);

    mocks.manager.scanStaleLeasesForCleanup();

    verify(collection).drop();
    verify(mocks.leaseManager).deleteStaleLease(eq(marked.id()), anyLong(), any());
  }

  @Test
  public void firstScanDelay_isAtLeastMarkGracePeriod() {
    // The first GC sweep is deferred past a just-restarted mongot's
    // replication/lease-renewal resume. With defaults, mark grace (30m) > scan interval (5m), so
    // the initial delay is a full mark grace; the scan interval is the floor.
    AutoEmbeddingMaterializedViewConfig config = AutoEmbeddingMaterializedViewConfig.getDefault();
    assertEquals(
        config.staleLeaseMarkGracePeriodMs, MaterializedViewManager.firstScanDelayMs(config));
    assertTrue(MaterializedViewManager.firstScanDelayMs(config) >= config.staleLeaseScanIntervalMs);
  }

  @Test
  public void updateMaxReplicationOpTime_leadershipLossDuringUpdate_usesFollowerPollOptime()
      throws Exception {
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    var indexGeneration = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    var metricsUpdater = mock(IndexMetricsUpdater.class);
    var indexingMetricsUpdater = mock(IndexMetricsUpdater.IndexingMetricsUpdater.class);
    var opTimeInfo = new ReplicationOpTimeInfo();
    when(indexGeneration.getIndex().getMetricsUpdater()).thenReturn(metricsUpdater);
    when(metricsUpdater.getIndexingMetricsUpdater()).thenReturn(indexingMetricsUpdater);
    when(indexingMetricsUpdater.getReplicationOpTimeInfo()).thenReturn(opTimeInfo);

    var generator = mocks.mockMaterializedViewGenerator(indexGeneration);
    mocks.addIndexForReplication(indexGeneration);
    assertTrue(generator.isLeader());

    BsonTimestamp oldReplicationOpTime = new BsonTimestamp(100, 1);
    BsonTimestamp oldMaxOpTime = new BsonTimestamp(120, 1);
    BsonTimestamp followerReplicationOpTime = new BsonTimestamp(150, 1);
    BsonTimestamp newMaxOpTime = new BsonTimestamp(200, 1);
    opTimeInfo.update(oldReplicationOpTime.getValue(), oldMaxOpTime.getValue());

    MongoClient mongoClient = mock(MongoClient.class);
    MongoDatabase adminDatabase = mock(MongoDatabase.class);
    when(mocks.autoEmbeddingMongoClient.getMaterializedViewResolverMongoClient())
        .thenReturn(Optional.of(mongoClient));
    when(mongoClient.getDatabase("admin")).thenReturn(adminDatabase);
    CountDownLatch optimeReadStarted = new CountDownLatch(1);
    CountDownLatch finishOptimeRead = new CountDownLatch(1);
    when(adminDatabase.runCommand(any(Bson.class), eq(BsonDocument.class)))
        .thenAnswer(
            invocation -> {
              optimeReadStarted.countDown();
              assertTrue(finishOptimeRead.await(5, TimeUnit.SECONDS));
              return replSetStatus(newMaxOpTime);
            });

    CompletableFuture<Void> optimeUpdate =
        CompletableFuture.runAsync(mocks.manager::updateMaxReplicationOpTime);
    assertTrue(optimeReadStarted.await(5, TimeUnit.SECONDS));
    try {
      when(mocks.leaseManager.isLeader(indexGeneration.getGenerationId())).thenReturn(false);
      doReturn(false).when(generator).isLeader();
      when(mocks.leaseManager.pollFollowerStatuses())
          .thenReturn(
              new LeaseManager.FollowerPollResult(
                  Map.of(indexGeneration.getGenerationId(), IndexStatus.steady()),
                  Map.of(indexGeneration.getGenerationId(), followerReplicationOpTime),
                  Set.of()));

      mocks.runnableCaptor.orElseThrow().getValue().run();

      assertEquals(
          followerReplicationOpTime.getValue(), opTimeInfo.snapshotOrThrow().replicationOpTime());
    } finally {
      finishOptimeRead.countDown();
    }
    optimeUpdate.get(5, TimeUnit.SECONDS);

    assertEquals(
        followerReplicationOpTime.getValue(), opTimeInfo.snapshotOrThrow().replicationOpTime());
    assertEquals(
        newMaxOpTime.getValue(), opTimeInfo.snapshotOrThrow().maxPossibleReplicationOpTime());
  }

  @Test
  public void updateMaxReplicationOpTime_nonQueryableFollower_clearsReplicationOpTimeInfo() {
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();
    var indexGeneration = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    var opTimeInfo = ensureReplicationOpTimeInfo(indexGeneration);
    opTimeInfo.update(new BsonTimestamp(100, 1).getValue(), new BsonTimestamp(120, 1).getValue());
    mocks.mockMaterializedViewGenerator(indexGeneration);
    mocks.addIndexForReplication(indexGeneration);
    var generationId = indexGeneration.getGenerationId();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(generationId, IndexStatus.notStarted()), Map.of(), Set.of()));

    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertTrue(opTimeInfo.snapshot().isPresent());

    mocks.manager.updateMaxReplicationOpTime();

    assertTrue(opTimeInfo.snapshot().isEmpty());
  }

  @Test
  public void updateMaxReplicationOpTime_nonQueryableLeader_clearsReplicationOpTimeInfo() {
    Mocks mocks = Mocks.createDynamicLeaderWithDynamicLeaseManager();
    var indexGeneration = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    var opTimeInfo = ensureReplicationOpTimeInfo(indexGeneration);
    opTimeInfo.update(new BsonTimestamp(100, 1).getValue(), new BsonTimestamp(120, 1).getValue());
    var generator = mocks.mockMaterializedViewGenerator(indexGeneration);
    mocks.addIndexForReplication(indexGeneration);
    assertTrue(generator.isLeader());
    indexGeneration.getIndex().setStatus(IndexStatus.notStarted());

    mocks.manager.updateMaxReplicationOpTime();

    assertTrue(opTimeInfo.snapshot().isEmpty());
  }

  @Test
  public void refreshStatus_queryableFollowerWithoutCheckpoint_preservesReplicationOpTimeInfo() {
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();
    var indexGeneration = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    var opTimeInfo = ensureReplicationOpTimeInfo(indexGeneration);
    BsonTimestamp replicationOpTime = new BsonTimestamp(100, 1);
    BsonTimestamp maxOpTime = new BsonTimestamp(120, 1);
    opTimeInfo.update(replicationOpTime.getValue(), maxOpTime.getValue());
    mocks.mockMaterializedViewGenerator(indexGeneration);
    mocks.addIndexForReplication(indexGeneration);
    var generationId = indexGeneration.getGenerationId();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(generationId, IndexStatus.steady()), Map.of(), Set.of()));

    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertEquals(replicationOpTime.getValue(), opTimeInfo.snapshotOrThrow().replicationOpTime());
    assertEquals(maxOpTime.getValue(), opTimeInfo.snapshotOrThrow().maxPossibleReplicationOpTime());
  }

  @Test
  public void refreshStatus_queryableFollowerWithoutCheckpoint_leavesReplicationOpTimeInfoUnset() {
    Mocks mocks = Mocks.createDynamicFollowerWithAcquirableLeases();
    var indexGeneration = mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    var opTimeInfo = ensureReplicationOpTimeInfo(indexGeneration);
    mocks.mockMaterializedViewGenerator(indexGeneration);
    mocks.addIndexForReplication(indexGeneration);
    var generationId = indexGeneration.getGenerationId();
    when(mocks.leaseManager.pollFollowerStatuses())
        .thenReturn(
            new LeaseManager.FollowerPollResult(
                Map.of(generationId, IndexStatus.steady()), Map.of(), Set.of()));

    mocks.runnableCaptor.orElseThrow().getValue().run();

    assertTrue(opTimeInfo.snapshot().isEmpty());
  }

  private static ReplicationOpTimeInfo ensureReplicationOpTimeInfo(
      MaterializedViewIndexGeneration indexGeneration) {
    var matViewIndex = indexGeneration.getIndex();
    if (matViewIndex.getMetricsUpdater() != null) {
      return matViewIndex
          .getMetricsUpdater()
          .getIndexingMetricsUpdater()
          .getReplicationOpTimeInfo();
    }
    var metricsUpdater = mock(IndexMetricsUpdater.class);
    var indexingMetricsUpdater = mock(IndexMetricsUpdater.IndexingMetricsUpdater.class);
    var opTimeInfo = new ReplicationOpTimeInfo();
    when(matViewIndex.getMetricsUpdater()).thenReturn(metricsUpdater);
    when(metricsUpdater.getIndexingMetricsUpdater()).thenReturn(indexingMetricsUpdater);
    when(indexingMetricsUpdater.getReplicationOpTimeInfo()).thenReturn(opTimeInfo);
    return opTimeInfo;
  }

  private static BsonDocument replSetStatus(BsonTimestamp opTime) {
    BsonDocument opTimeDocument = new BsonDocument("ts", opTime).append("t", new BsonInt64(1L));
    return new BsonDocument(
        "optimes",
        new BsonDocument()
            .append("lastCommittedOpTime", opTimeDocument)
            .append("readConcernMajorityOpTime", opTimeDocument)
            .append("appliedOpTime", opTimeDocument)
            .append("durableOpTime", opTimeDocument));
  }

  static class Mocks {
    final NamedExecutorService executorService;
    @Keep final IndexingWorkSchedulerFactory indexingWorkSchedulerFactory;
    @Keep final MongotCursorManager cursorManager;
    final Map<String, ClientSessionRecord> clientSessionRecordMap;
    final InitialSyncQueue initialSyncQueue;
    final SteadyStateManager steadyStateManager;
    final MaterializedViewManager.MaterializedViewGeneratorFactory materializedViewGeneratorFactory;
    final NamedScheduledExecutorService heartbeatExecutor;
    final Supplier<MaterializedViewManager> managerSupplier;
    final DecodingWorkScheduler decodingScheduler;
    final MeterRegistry meterRegistry;
    final LeaseManager leaseManager;
    final AutoEmbeddingMongoClient autoEmbeddingMongoClient;
    final NamedScheduledExecutorService statusRefreshExecutor;
    final NamedScheduledExecutorService optimeUpdaterExecutor;
    final IndexStatus expectedStatus; // For follower mode tests
    final Optional<ArgumentCaptor<Runnable>> runnableCaptor; // For follower mode tests
    final MaterializedViewCollectionMetadataCatalog metadataCatalog;
    final NamedScheduledExecutorService staleLeaseScannerExecutor;
    final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;
    final EmbeddingRetryCache embeddingRetryCache;
    final IndexCatalog indexCatalog;

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
      this.heartbeatExecutor = heartbeatExecutor;
      this.statusRefreshExecutor = statusRefreshExecutor;
      this.optimeUpdaterExecutor = optimeUpdaterExecutor;
      this.decodingScheduler = decodingScheduler;
      this.meterRegistry = new SimpleMeterRegistry();
      this.expectedStatus = expectedStatus;
      this.runnableCaptor = runnableCaptor;
      this.leaseManager = leaseManager;
      this.metadataCatalog = metadataCatalog;
      this.staleLeaseScannerExecutor = mockScheduledExecutor("mat-view-stale-lease-scanner");
      // GC sweep + deletion both enabled by default for these tests; the flag-disabled paths are
      // covered separately.
      this.dynamicFeatureFlagRegistry = mock(DynamicFeatureFlagRegistry.class);
      when(this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
              DynamicFeatureFlags.MV_GC_ENABLED))
          .thenReturn(true);
      when(this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
              DynamicFeatureFlags.MV_GC_DELETION_ENABLED))
          .thenReturn(true);
      this.embeddingRetryCache = mock(EmbeddingRetryCache.class);
      // Every index is configured by default, so the GC current-version safeguard behaves as
      // before; deleted-index tests stub specific ids back to empty.
      this.indexCatalog = mock(IndexCatalog.class);
      when(this.indexCatalog.getIndexById(any()))
          .thenReturn(Optional.of(mock(IndexGeneration.class)));

      SyncSourceConfig syncSourceConfig =
          SyncSourceConfig.builder()
              .mongodSingleHostReplicationUri(
                  ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newString"))
              .mongodClusterReplicationUri(
                  ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newString"))
              .mongodClusterReadWriteUri(
                  ConnectionStringUtil.toConnectionInfoUnchecked("mongodb://newString"))
              .build();
      AutoEmbeddingMongoClient autoEmbeddingMongoClient = mock(AutoEmbeddingMongoClient.class);
      when(autoEmbeddingMongoClient.getSyncSourceConfig())
          .thenReturn(Optional.of(syncSourceConfig));
      when(autoEmbeddingMongoClient.getMaterializedViewResolverMongoClient())
          .thenReturn(Optional.empty());
      when(autoEmbeddingMongoClient.getMaterializedViewWriterMongoClient())
          .thenReturn(Optional.empty());
      this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;

      this.managerSupplier =
          () ->
              new MaterializedViewManager(
                  executorService,
                  indexingWorkSchedulerFactory,
                  autoEmbeddingMongoClient,
                  decodingScheduler,
                  materializedViewGeneratorFactory,
                  heartbeatExecutor,
                  statusRefreshExecutor,
                  optimeUpdaterExecutor,
                  this.staleLeaseScannerExecutor,
                  this.meterRegistry,
                  leaseManager,
                  this.indexCatalog,
                  metadataCatalog,
                  AutoEmbeddingMaterializedViewConfig.getDefault(),
                  this.dynamicFeatureFlagRegistry,
                  this.embeddingRetryCache);
      this.manager = this.managerSupplier.get();
      this.manager.setIsReplicationEnabled(true);
    }

    /**
     * Returns the source-collection-drop handler the manager wired into the generator factory, i.e.
     * what a generator's DROPPED handling invokes.
     */
    Consumer<MaterializedViewIndexGeneration> onSourceCollectionDropped() {
      @SuppressWarnings("unchecked")
      ArgumentCaptor<Consumer<MaterializedViewIndexGeneration>> captor =
          ArgumentCaptor.forClass(Consumer.class);
      verify(this.materializedViewGeneratorFactory, atLeast(1))
          .setOnSourceCollectionDropped(captor.capture());
      return captor.getValue();
    }

    public void recreateManager() {
      this.manager = this.managerSupplier.get();
      this.manager.setIsReplicationEnabled(true);
    }

    /** Removes every index from the mocked IndexCatalog, so drops read as index deletions. */
    void markIndexDeleted() {
      when(this.indexCatalog.getIndexById(any())).thenReturn(Optional.empty());
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
      when(leaseManager.cleanupLeaseForDeletedIndex(any(String.class))).thenReturn(true);
      when(leaseManager.findLeasesForDeletedIndex(any(), any(String.class))).thenReturn(List.of());
      when(leaseManager.isLeader(any())).thenReturn(true);
      // Default: an empty scan — no candidates, nothing protected. GC tests override this with
      // their own lease topology.
      when(leaseManager.findGcCandidates()).thenReturn(List.of());
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
      when(leaseManager.cleanupLeaseForDeletedIndex(any(String.class))).thenReturn(true);
      when(leaseManager.findLeasesForDeletedIndex(any(), any(String.class))).thenReturn(List.of());
      when(leaseManager.isLeader(any())).thenReturn(true);
      // Default: an empty scan — no candidates, nothing protected. GC tests override this with
      // their own lease topology.
      when(leaseManager.findGcCandidates()).thenReturn(List.of());
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
      when(mockLeaseManager.cleanupLeaseForDeletedIndex(any(String.class))).thenReturn(true);
      when(mockLeaseManager.findLeasesForDeletedIndex(any(), any(String.class)))
          .thenReturn(List.of());
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
                when(mockGenerator.getState()).thenReturn(IndexManager.State.INITIAL_SYNC);
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

      NamedScheduledExecutorService heartbeatExecutor =
          mockScheduledExecutor("mat-view-leader-heartbeat");

      NamedScheduledExecutorService statusRefreshExecutor =
          mockScheduledExecutor("mat-view-status-refresh");
      ArgumentCaptor<Runnable> statusRefreshCaptor = ArgumentCaptor.forClass(Runnable.class);
      doReturn(mock(ScheduledFuture.class))
          .when(statusRefreshExecutor)
          .scheduleWithFixedDelay(statusRefreshCaptor.capture(), anyLong(), anyLong(), any());

      NamedScheduledExecutorService optimeUpdaterExecutor =
          mockScheduledExecutor("mat-view-optime-updater");

      InitialSyncQueue initialSyncQueue = mock(InitialSyncQueue.class);
      when(initialSyncQueue.shutdown()).thenReturn(COMPLETED_FUTURE);

      SteadyStateManager steadyStateManager = mock(SteadyStateManager.class);
      when(steadyStateManager.shutdown()).thenReturn(COMPLETED_FUTURE);

      LeaseManager mockLeaseManager = mock(leaseManagerType);
      doNothing().when(mockLeaseManager).drop(any());
      when(mockLeaseManager.dropLease(any(String.class))).thenReturn(COMPLETED_FUTURE);
      when(mockLeaseManager.cleanupLeaseForDeletedIndex(any(String.class))).thenReturn(true);
      when(mockLeaseManager.findLeasesForDeletedIndex(any(), any(String.class)))
          .thenReturn(List.of());
      // Default: an empty scan — no candidates, nothing protected. GC tests override this with
      // their own lease topology.
      when(mockLeaseManager.findGcCandidates()).thenReturn(List.of());

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
          heartbeatExecutor,
          statusRefreshExecutor,
          optimeUpdaterExecutor,
          mockLeaseManager,
          IndexStatus.unknown(),
          Optional.of(statusRefreshCaptor),
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
      when(mockLeaseManager.cleanupLeaseForDeletedIndex(any(String.class))).thenReturn(true);
      when(mockLeaseManager.findLeasesForDeletedIndex(any(), any(String.class)))
          .thenReturn(List.of());
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
                  // Same index instance the generation exposes, as the production factory does.
                  materializedViewIndexGeneration.getIndex(),
                  mock(DocumentIndexer.class),
                  mock(PeriodicIndexCommitter.class),
                  new SimpleMetricsFactory(),
                  mock(FeatureFlags.class),
                  Duration.ofSeconds(1),
                  Duration.ofSeconds(1),
                  Duration.ofSeconds(1),
                  false,
                  () -> {}));
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
      ensureReplicationOpTimeInfo(materializedViewindexGeneration);
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

  /**
   * Stubs the mock metadata catalog so all generations sharing {@code matViewGenId}'s user index
   * version map to their own MV collection (distinct name and UUID), overriding the default
   * indexId-only mapping of {@link #createMockMetadataCatalog()}. This models an ops
   * autoEmbeddingDefinitionVersion bump, where the index moves to a new MV collection while the
   * definition is otherwise unchanged.
   */
  private static void registerSiblingCollection(
      MaterializedViewCollectionMetadataCatalog catalog,
      MaterializedViewGenerationId matViewGenId,
      String collectionName) {
    UUID uuid = UUID.nameUUIDFromBytes(collectionName.getBytes(StandardCharsets.UTF_8));
    MaterializedViewCollectionMetadata metadata =
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
            uuid,
            collectionName);
    ArgumentMatcher<GenerationId> sameUserVersion =
        id ->
            id != null
                && id.indexId.equals(matViewGenId.indexId)
                && id.generation.userIndexVersion == matViewGenId.generation.userIndexVersion;
    when(catalog.getMetadata(argThat(sameUserVersion))).thenReturn(metadata);
    when(catalog.getMetadataIfPresent(argThat(sameUserVersion))).thenReturn(Optional.of(metadata));
  }

  @Test
  public void summarizeReplicationLagMs_computesMaxAndAverageAcrossMaterializedViews() {
    assertEquals(
        250.0,
        MaterializedViewManager.summarizeReplicationLagMs(
            LongStream.of(100, 250, 40), stats -> (double) stats.getMax()),
        0.0);
    assertEquals(
        130.0,
        MaterializedViewManager.summarizeReplicationLagMs(
            LongStream.of(100, 250, 40), LongSummaryStatistics::getAverage),
        0.0);
  }

  @Test
  public void summarizeReplicationLagMs_withNoMaterializedViews_isNaN() {
    assertTrue(
        Double.isNaN(
            MaterializedViewManager.summarizeReplicationLagMs(
                LongStream.empty(), stats -> (double) stats.getMax())));
    assertTrue(
        Double.isNaN(
            MaterializedViewManager.summarizeReplicationLagMs(
                LongStream.empty(), LongSummaryStatistics::getAverage)));
  }
}
