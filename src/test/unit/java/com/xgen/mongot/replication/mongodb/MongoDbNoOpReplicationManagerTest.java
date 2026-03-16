package com.xgen.mongot.replication.mongodb;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Var;
import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.BufferlessIdOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexStateInfo;
import com.xgen.mongot.replication.mongodb.common.StaleStateInfo;
import com.xgen.mongot.util.Condition;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class MongoDbNoOpReplicationManagerTest {

  public static final BsonTimestamp STALE_OPTIME = new BsonTimestamp(39, 975);

  @Test
  public void testDropIndex() throws Exception {
    Mocks mocks = Mocks.create();

    // Drop it, ensure that cursors are killed. Physical close/drop is handled by IndexActions.
    mocks.manager.dropIndex(MOCK_INDEX_GENERATION_ID).get(5, TimeUnit.SECONDS);
    verify(mocks.cursorManager).killIndexCursors(MOCK_INDEX_GENERATION_ID);
    // Ensure that index drop and close are not invoked.
    verify(mocks.initializedIndex, never()).drop();
    verify(mocks.initializedIndex, never()).drop();
  }

  @Test
  public void testAddIndex_NoCommitUserData() {
    Mocks mocks = Mocks.create();
    IndexGeneration indexGeneration = mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);

    mocks.mockEncodedUserData(Mocks.CommitUserDataInfo.EMPTY);
    mocks.manager.add(indexGeneration);
    new Condition.Builder().atMost(Duration.ofSeconds(30)).until(mocks.manager::isInitialized);
    verify(mocks.initializedIndex).setStatus(any());
    Assert.assertEquals(
        IndexStatus.StatusCode.NOT_STARTED, mocks.initializedIndex.getStatus().getStatusCode());
  }

  @Test
  public void testAddIndex_ExceedLimit() {
    Mocks mocks = Mocks.create();
    IndexGeneration indexGeneration = mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);

    mocks.mockEncodedUserData(Mocks.CommitUserDataInfo.EXCEEDED_LIMIT);
    mocks.manager.add(indexGeneration);
    new Condition.Builder().atMost(Duration.ofSeconds(30)).until(mocks.manager::isInitialized);
    verify(mocks.initializedIndex).setStatus(any());
    Assert.assertEquals(
        IndexStatus.StatusCode.FAILED, mocks.initializedIndex.getStatus().getStatusCode());
  }

  @Test
  public void testAddIndex_StaleInfo() {
    Mocks mocks = Mocks.create();
    IndexGeneration indexGeneration = mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);

    mocks.mockEncodedUserData(Mocks.CommitUserDataInfo.STALE_INFO);
    mocks.manager.add(indexGeneration);
    new Condition.Builder().atMost(Duration.ofSeconds(30)).until(mocks.manager::isInitialized);
    verify(mocks.initializedIndex).setStatus(any());
    verify(
            mocks
                .initializedIndex
                .getMetricsUpdater()
                .getIndexingMetricsUpdater()
                .getReplicationOpTimeInfo())
        .update(STALE_OPTIME.getValue(), STALE_OPTIME.getValue());
    Assert.assertEquals(
        IndexStatus.StatusCode.STALE, mocks.initializedIndex.getStatus().getStatusCode());
  }

  @Test
  public void testAddIndex_CollectionNotFound() {
    var featureFlags =
        FeatureFlags.withDefaults()
            .enable(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)
            .build();
    Mocks mocks = Mocks.create(featureFlags);
    IndexGeneration indexGeneration = mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);

    mocks.mockEncodedUserData(Mocks.CommitUserDataInfo.DOES_NOT_EXIST);
    mocks.manager.add(indexGeneration);
    new Condition.Builder().atMost(Duration.ofSeconds(30)).until(mocks.manager::isInitialized);
    verify(mocks.initializedIndex).setStatus(any());
    Assert.assertEquals(
        IndexStatus.StatusCode.DOES_NOT_EXIST, mocks.initializedIndex.getStatus().getStatusCode());
    Assert.assertEquals(
        IndexStatus.Reason.COLLECTION_NOT_FOUND,
        mocks.initializedIndex.getStatus().getReason().get());
  }

  @Test
  public void testAddIndex_InitialSync() {
    Mocks mocks = Mocks.create();
    IndexGeneration indexGeneration = mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);

    mocks.mockEncodedUserData(Mocks.CommitUserDataInfo.INITIAL_SYNC);
    mocks.manager.add(indexGeneration);
    new Condition.Builder().atMost(Duration.ofSeconds(30)).until(mocks.manager::isInitialized);
    verify(mocks.initializedIndex).setStatus(any());
    Assert.assertEquals(
        IndexStatus.StatusCode.INITIAL_SYNC, mocks.initializedIndex.getStatus().getStatusCode());
  }

  @Test
  public void testAddIndex_Steady() {
    Mocks mocks = Mocks.create();
    IndexGeneration indexGeneration = mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);

    mocks.mockEncodedUserData(Mocks.CommitUserDataInfo.STEADY_STATE);
    mocks.manager.add(indexGeneration);
    new Condition.Builder().atMost(Duration.ofSeconds(30)).until(mocks.manager::isInitialized);
    verify(
            mocks
                .initializedIndex
                .getMetricsUpdater()
                .getIndexingMetricsUpdater()
                .getReplicationOpTimeInfo())
        .update(anyLong(), anyLong());
    verify(mocks.initializedIndex).setStatus(any());
    Assert.assertEquals(
        IndexStatus.StatusCode.STEADY, mocks.initializedIndex.getStatus().getStatusCode());
  }

  private static class Mocks {
    final MongotCursorManager cursorManager;
    final InitializedSearchIndex initializedIndex;
    final MongoDbNoOpReplicationManager manager;

    private Mocks(
        MongotCursorManager cursorManager,
        InitializedIndexCatalog initializedIndexCatalog,
        InitializedSearchIndex index,
        FeatureFlags featureFlags) {
      this.cursorManager = cursorManager;
      this.initializedIndex = index;
      this.manager =
          MongoDbNoOpReplicationManager.create(
              Optional.of(
                  new SyncSourceConfig(
                      new ConnectionString("mongodb://newString"),
                      Optional.empty(),
                      new ConnectionString("mongodb://newString"))),
              cursorManager,
              mock(IndexCatalog.class),
              initializedIndexCatalog,
              featureFlags,
              spy(new SimpleMeterRegistry()));
    }

    static Mocks create() {
      return create(FeatureFlags.getDefault());
    }

    static Mocks create(FeatureFlags featureFlags) {
      InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
      InitializedSearchIndex initializedIndex = mock(InitializedSearchIndex.class);
      IndexWriter indexWriter = mock(IndexWriter.class);
      when(indexWriter.getCommitUserData()).thenReturn(EncodedUserData.EMPTY);
      when(initializedIndex.getWriter()).thenReturn(indexWriter);
      IndexMetricsUpdater indexMetricsUpdater = mock(IndexMetricsUpdater.class);
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater =
          mock(IndexMetricsUpdater.IndexingMetricsUpdater.class);
      ReplicationOpTimeInfo replicationOpTimeInfo = mock(ReplicationOpTimeInfo.class);
      when(replicationOpTimeInfo.getUnsetLock()).thenReturn(new Object());
      when(indexingMetricsUpdater.getReplicationOpTimeInfo()).thenReturn(replicationOpTimeInfo);
      when(indexMetricsUpdater.getIndexingMetricsUpdater()).thenReturn(indexingMetricsUpdater);
      when(initializedIndex.getMetricsUpdater()).thenReturn(indexMetricsUpdater);
      when(initializedIndexCatalog.getIndex(any())).thenReturn(Optional.of(initializedIndex));
      AtomicReference<IndexStatus> statusRef = new AtomicReference<>(IndexStatus.unknown());
      doAnswer(
              (Answer<Void>)
                  invocation -> {
                    statusRef.set(invocation.getArgument(0));
                    return null;
                  })
          .when(initializedIndex)
          .setStatus(any());
      when(initializedIndex.getStatus())
          .thenAnswer((Answer<IndexStatus>) invocation -> statusRef.get());

      return new Mocks(
          mock(MongotCursorManager.class), initializedIndexCatalog, initializedIndex, featureFlags);
    }

    void mockEncodedUserData(CommitUserDataInfo info) {
      @Var var indexCommitUserData = IndexCommitUserData.EMPTY;
      switch (info) {
        case EXCEEDED_LIMIT:
          indexCommitUserData = IndexCommitUserData.createExceeded("test");
          break;
        case STALE_INFO:
          StaleStateInfo staleStateInfo =
              StaleStateInfo.create(STALE_OPTIME, StaleStatusReason.DOCS_EXCEEDED);
          indexCommitUserData = IndexCommitUserData.createStale(staleStateInfo);
          break;
        case DOES_NOT_EXIST:
          IndexStateInfo indexStateInfo =
              IndexStateInfo.create(
                  IndexStatus.StatusCode.DOES_NOT_EXIST, IndexStatus.Reason.COLLECTION_NOT_FOUND);
          indexCommitUserData = IndexCommitUserData.createFromIndexStateInfo(indexStateInfo);
          break;
        case INITIAL_SYNC:
          indexCommitUserData =
              IndexCommitUserData.createInitialSyncResume(
                  IndexFormatVersion.CURRENT,
                  new BufferlessIdOrderInitialSyncResumeInfo(
                      new BsonTimestamp(1234567890L), new BsonString("a")));
          break;
        case STEADY_STATE:
          indexCommitUserData =
              IndexCommitUserData.createChangeStreamResume(
                  ChangeStreamResumeInfo.create(
                      new MongoNamespace("database", "collection"),
                      new BsonDocument("_data", new BsonString("820000000000000000"))),
                  IndexFormatVersion.CURRENT);
          break;
        case EMPTY:
          break;
      }

      EncodedUserData encodedUserData = indexCommitUserData.toEncodedData();
      when(this.initializedIndex.getWriter().getCommitUserData()).thenReturn(encodedUserData);
    }

    enum CommitUserDataInfo {
      EXCEEDED_LIMIT,
      STALE_INFO,
      INITIAL_SYNC,
      STEADY_STATE,
      DOES_NOT_EXIST,
      EMPTY
    }
  }
}
