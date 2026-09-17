package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Optional;
import java.util.function.Supplier;
import org.bson.BsonTimestamp;
import org.junit.Test;

public class AutoEmbeddingCompositeIndexTest {

  @Test
  public void close_closesGenerationMetrics() throws Exception {
    InitializedMaterializedViewIndex materializedViewIndex =
        mock(InitializedMaterializedViewIndex.class);
    InitializedVectorIndex derivedIndex = mock(InitializedVectorIndex.class);
    AutoEmbeddingGenerationMetrics generationMetrics = mock(AutoEmbeddingGenerationMetrics.class);
    AutoEmbeddingCompositeIndex index =
        new AutoEmbeddingCompositeIndex(
            materializedViewIndex, derivedIndex, Optional::empty, () -> false, generationMetrics);

    index.close();

    verify(generationMetrics).close();
  }

  @Test
  public void close_attemptsBothIndexesWhenCloseFails() throws Exception {
    InitializedMaterializedViewIndex materializedViewIndex =
        mock(InitializedMaterializedViewIndex.class);
    InitializedVectorIndex derivedIndex = mock(InitializedVectorIndex.class);
    AutoEmbeddingGenerationMetrics generationMetrics = mock(AutoEmbeddingGenerationMetrics.class);
    IOException materializedViewFailure = new IOException("materialized view close failed");
    IOException derivedIndexFailure = new IOException("derived index close failed");
    doThrow(materializedViewFailure).when(materializedViewIndex).close();
    doThrow(derivedIndexFailure).when(derivedIndex).close();
    AutoEmbeddingCompositeIndex index =
        new AutoEmbeddingCompositeIndex(
            materializedViewIndex, derivedIndex, Optional::empty, () -> false, generationMetrics);

    IOException thrown = assertThrows(IOException.class, index::close);

    assertSame(materializedViewFailure, thrown);
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(derivedIndexFailure, thrown.getSuppressed()[0]);
    verify(materializedViewIndex).close();
    verify(derivedIndex).close();
    verify(generationMetrics).close();
  }

  @Test
  public void close_closesGenerationMetricsWhenIndexCloseThrowsUncheckedException()
      throws Exception {
    InitializedMaterializedViewIndex materializedViewIndex =
        mock(InitializedMaterializedViewIndex.class);
    InitializedVectorIndex derivedIndex = mock(InitializedVectorIndex.class);
    AutoEmbeddingGenerationMetrics generationMetrics = mock(AutoEmbeddingGenerationMetrics.class);
    RuntimeException materializedViewFailure =
        new RuntimeException("materialized view close failed");
    IOException derivedIndexFailure = new IOException("derived index close failed");
    doThrow(materializedViewFailure).when(materializedViewIndex).close();
    doThrow(derivedIndexFailure).when(derivedIndex).close();
    AutoEmbeddingCompositeIndex index =
        new AutoEmbeddingCompositeIndex(
            materializedViewIndex, derivedIndex, Optional::empty, () -> false, generationMetrics);

    RuntimeException thrown = assertThrows(RuntimeException.class, index::close);

    assertSame(materializedViewFailure, thrown);
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(derivedIndexFailure, thrown.getSuppressed()[0]);
    verify(materializedViewIndex).close();
    verify(derivedIndex).close();
    verify(generationMetrics).close();
  }

  @Test
  public void testConsolidateStatuses() {
    // MV DOES_NOT_EXIST takes precedence over everything (including FAILED)
    assertConsolidatedStatus(
        StatusCode.DOES_NOT_EXIST, StatusCode.STEADY, StatusCode.DOES_NOT_EXIST);
    assertConsolidatedStatus(
        StatusCode.DOES_NOT_EXIST, StatusCode.FAILED, StatusCode.DOES_NOT_EXIST);

    // FAILED propagates (MV DNE already handled above)
    assertConsolidatedStatus(StatusCode.FAILED, StatusCode.STEADY, StatusCode.FAILED);
    assertConsolidatedStatus(StatusCode.STEADY, StatusCode.FAILED, StatusCode.FAILED);
    assertConsolidatedStatus(StatusCode.INITIAL_SYNC, StatusCode.FAILED, StatusCode.FAILED);

    // STALE propagates
    assertConsolidatedStatus(StatusCode.STALE, StatusCode.STEADY, StatusCode.STALE);
    assertConsolidatedStatus(StatusCode.STEADY, StatusCode.STALE, StatusCode.STALE);
    assertConsolidatedStatus(StatusCode.STALE, StatusCode.INITIAL_SYNC, StatusCode.STALE);
    assertConsolidatedStatus(StatusCode.RECOVERING_TRANSIENT, StatusCode.STALE, StatusCode.STALE);

    // Lucene DOES_NOT_EXIST propagates
    assertConsolidatedStatus(
        StatusCode.STEADY, StatusCode.DOES_NOT_EXIST, StatusCode.DOES_NOT_EXIST);

    // INITIAL_SYNC propagates
    assertConsolidatedStatus(StatusCode.STEADY, StatusCode.INITIAL_SYNC, StatusCode.INITIAL_SYNC);
    assertConsolidatedStatus(StatusCode.INITIAL_SYNC, StatusCode.STEADY, StatusCode.INITIAL_SYNC);
    assertConsolidatedStatus(
        StatusCode.INITIAL_SYNC, StatusCode.NOT_STARTED, StatusCode.INITIAL_SYNC);
    assertConsolidatedStatus(
        StatusCode.NOT_STARTED, StatusCode.INITIAL_SYNC, StatusCode.INITIAL_SYNC);

    // UNKNOWN or NOT_STARTED propagates when Lucene is not STEADY
    assertConsolidatedStatus(StatusCode.STEADY, StatusCode.NOT_STARTED, StatusCode.NOT_STARTED);
    // MV UNKNOWN/NOT_STARTED + Lucene STEADY -> STEADY: allows MMS processReplicationStatus()
    // to apply the STEADY->STALE transition when replication is stopped (e.g. after a restart
    // with forceDisableReplication). Returning UNKNOWN here would cause isTransient()=true,
    // filtering the index before processReplicationStatus() can fire.
    assertConsolidatedStatus(StatusCode.UNKNOWN, StatusCode.STEADY, StatusCode.STEADY);
    assertConsolidatedStatus(StatusCode.NOT_STARTED, StatusCode.STEADY, StatusCode.STEADY);

    // RECOVERING_TRANSIENT or RECOVERING_NON_TRANSIENT propagates
    assertConsolidatedStatus(
        StatusCode.RECOVERING_TRANSIENT, StatusCode.STEADY, StatusCode.RECOVERING_TRANSIENT);
    assertConsolidatedStatus(
        StatusCode.STEADY, StatusCode.RECOVERING_TRANSIENT, StatusCode.RECOVERING_TRANSIENT);
    assertConsolidatedStatus(
        StatusCode.RECOVERING_NON_TRANSIENT,
        StatusCode.STEADY,
        StatusCode.RECOVERING_NON_TRANSIENT);

    // Both STEADY -> consolidated STEADY
    assertConsolidatedStatus(StatusCode.STEADY, StatusCode.STEADY, StatusCode.STEADY);
  }

  @Test
  public void getStatus_materializedViewFailed_reportsEmbeddingGenerationFailed() {
    AutoEmbeddingCompositeIndex index =
        compositeWith(new IndexStatus(StatusCode.FAILED), IndexStatus.steady());
    assertEquals(StatusCode.FAILED, index.getStatus().getStatusCode());
    assertEquals(
        Optional.of("Automated Embedding Index Failed: embedding generation failed"),
        index.getStatus().getMessage());
  }

  @Test
  public void getStatus_derivedIndexFailed_reportsIndexBuildFailed() {
    AutoEmbeddingCompositeIndex index =
        compositeWith(IndexStatus.steady(), new IndexStatus(StatusCode.FAILED));
    assertEquals(StatusCode.FAILED, index.getStatus().getStatusCode());
    assertEquals(
        Optional.of("Automated Embedding Index Failed: index build failed"),
        index.getStatus().getMessage());
  }

  @Test
  public void getStatus_bothComponentsFailed_reportsEmbeddingGenerationFailed() {
    AutoEmbeddingCompositeIndex index =
        compositeWith(new IndexStatus(StatusCode.FAILED), new IndexStatus(StatusCode.FAILED));
    assertEquals(
        Optional.of("Automated Embedding Index Failed: embedding generation failed"),
        index.getStatus().getMessage());
  }

  @Test
  public void testConsolidateStatusesHandlesAllStatusCodes() {
    for (StatusCode mvStatus : EnumSet.allOf(StatusCode.class)) {
      for (StatusCode luceneStatus : EnumSet.allOf(StatusCode.class)) {
        AutoEmbeddingCompositeIndex index = createIndexWithStatuses(mvStatus, luceneStatus);
        IndexStatus result = index.getStatus();

        String context = String.format("consolidateStatuses(%s, %s)", mvStatus, luceneStatus);
        assert result != null : context + " returned null";
        assert result.getStatusCode() != null : context + " returned null statusCode";

        // Guard against unhandled StatusCode values: if a new status is added
        // but not handled in consolidateStatuses(), it will fall through to STEADY.
        // Valid STEADY outputs: (STEADY,STEADY) or (UNKNOWN/NOT_STARTED,STEADY).
        if (result.getStatusCode() == StatusCode.STEADY) {
          boolean mvAllowsSteadyOutput =
              mvStatus == StatusCode.STEADY
                  || mvStatus == StatusCode.UNKNOWN
                  || mvStatus == StatusCode.NOT_STARTED;
          assertEquals(context + " unexpectedly returned STEADY", true, mvAllowsSteadyOutput);
          assertEquals(context + " unexpectedly returned STEADY", StatusCode.STEADY, luceneStatus);
        }
      }
    }
  }

  @Test
  public void testCalculateEffectiveLuceneStatus() {
    var steadyPosition = new BsonTimestamp(1000, 1);

    // Non-STEADY Lucene -> skip lag check, return as-is
    AutoEmbeddingCompositeIndex index =
        createIndexWithStatuses(StatusCode.STEADY, StatusCode.INITIAL_SYNC);
    assertEquals(StatusCode.INITIAL_SYNC, index.getStatus().getStatusCode());

    // MV in INITIAL_SYNC (no steadyAsOfOplogPosition)
    // Lucene STEADY -> consolidates to INITIAL_SYNC (MV INITIAL_SYNC takes precedence)
    AutoEmbeddingCompositeIndex indexMvNotSteady =
        createIndexWithPositions(
            StatusCode.INITIAL_SYNC, StatusCode.STEADY, Optional.empty(), Optional.empty());
    assertEquals(StatusCode.INITIAL_SYNC, indexMvNotSteady.getStatus().getStatusCode());

    // MV STEADY but no steadyAsOfOplogPosition (e.g. highWaterMark not yet set during fresh build)
    // Lucene STEADY -> can't determine lag, pass through Lucene's STEADY status
    AutoEmbeddingCompositeIndex indexMvSteadyNoPosition =
        createIndexWithPositions(
            StatusCode.STEADY, StatusCode.STEADY, Optional.empty(), Optional.empty());
    assertEquals(StatusCode.STEADY, indexMvSteadyNoPosition.getStatus().getStatusCode());

    // STEADY Lucene, MV has position, but Lucene position unavailable -> INITIAL_SYNC
    AutoEmbeddingCompositeIndex indexNoLucenePosition =
        createIndexWithPositions(
            StatusCode.STEADY, StatusCode.STEADY, Optional.of(steadyPosition), Optional.empty());
    assertEquals(StatusCode.INITIAL_SYNC, indexNoLucenePosition.getStatus().getStatusCode());

    // Lucene caught up (position == steadyPosition) -> STEADY
    var luceneCaughtUp = new BsonTimestamp(1000, 1);
    AutoEmbeddingCompositeIndex caughtUpIndex =
        createIndexWithPositions(
            StatusCode.STEADY,
            StatusCode.STEADY,
            Optional.of(steadyPosition),
            Optional.of(luceneCaughtUp));
    assertEquals(StatusCode.STEADY, caughtUpIndex.getStatus().getStatusCode());

    // Lucene ahead of steady position -> STEADY
    var luceneAhead = new BsonTimestamp(2000, 1);
    AutoEmbeddingCompositeIndex aheadIndex =
        createIndexWithPositions(
            StatusCode.STEADY,
            StatusCode.STEADY,
            Optional.of(steadyPosition),
            Optional.of(luceneAhead));
    assertEquals(StatusCode.STEADY, aheadIndex.getStatus().getStatusCode());

    // Lucene behind steady position -> INITIAL_SYNC
    var luceneBehind = new BsonTimestamp(500, 1);
    AutoEmbeddingCompositeIndex laggyIndex =
        createIndexWithPositions(
            StatusCode.STEADY,
            StatusCode.STEADY,
            Optional.of(steadyPosition),
            Optional.of(luceneBehind));
    assertEquals(StatusCode.INITIAL_SYNC, laggyIndex.getStatus().getStatusCode());
  }

  @Test
  public void testReturnsInitialSyncWhenSupplierReturnsEmpty() {
    var steadyPosition = new BsonTimestamp(1000, 1);

    // MV is STEADY with position, but supplier returns empty (Lucene not initialized yet)
    // Can't determine if Lucene caught up -> returns INITIAL_SYNC
    AutoEmbeddingCompositeIndex index =
        createIndexWithPositionsAndSupplier(
            StatusCode.STEADY,
            StatusCode.STEADY,
            Optional.of(steadyPosition),
            Optional.empty(),
            false);
    assertEquals(StatusCode.INITIAL_SYNC, index.getStatus().getStatusCode());
  }

  @Test
  public void testRestartInitializesRatchetFromLease() {
    var steadyPosition = new BsonTimestamp(1000, 1);
    var luceneBehind = new BsonTimestamp(500, 1);

    // Simulate a restart where the lease says the current version was previously queryable.
    // Even on the very first getStatus() call (before consolidateStatuses has run), the composite
    // should skip the lag check and remain STEADY rather than regressing to INITIAL_SYNC.
    AutoEmbeddingCompositeIndex index =
        createIndexWithPositionsAndSupplier(
            StatusCode.STEADY,
            StatusCode.STEADY,
            Optional.of(steadyPosition),
            Optional.of(luceneBehind),
            /* includeSupplier= */ true,
            /* wasQueryablePerLease= */ true);
    assertEquals(StatusCode.STEADY, index.getStatus().getStatusCode());
  }

  @Test
  public void testDefinitionChangeDoesNotRegressLiveCompositeToInitialSync() {
    var steadyPosition = new BsonTimestamp(1000, 1);
    var luceneCaughtUp = new BsonTimestamp(1000, 1);

    // Step 1: composite reaches STEADY (Lucene caught up to steadyPosition T1)
    AutoEmbeddingCompositeIndex index =
        createIndexWithPositions(
            StatusCode.STEADY,
            StatusCode.STEADY,
            Optional.of(steadyPosition),
            Optional.of(luceneCaughtUp));
    assertEquals(StatusCode.STEADY, index.getStatus().getStatusCode());

    // Step 2: new definition version created - steadyAsOfOplogPosition cleared to null.
    // Composite was previously queryable so it should remain STEADY, not regress to INITIAL_SYNC.
    when(index.matViewIndex.getSteadyAsOfOplogPosition()).thenReturn(Optional.empty());
    assertEquals(StatusCode.STEADY, index.getStatus().getStatusCode());

    // Step 3: new version reaches STEADY - steadyAsOfOplogPosition re-set to T2 > T1.
    // Lucene is still at T1. Composite was previously queryable so it should remain STEADY.
    var newSteadyPosition = new BsonTimestamp(2000, 1);
    when(index.matViewIndex.getSteadyAsOfOplogPosition())
        .thenReturn(Optional.of(newSteadyPosition));
    assertEquals(StatusCode.STEADY, index.getStatus().getStatusCode());
  }

  @Test
  public void testMvUnknownSteadyDoesNotLatchRatchet() {
    var luceneBehind = new BsonTimestamp(500, 1);
    var steadyPosition = new BsonTimestamp(1000, 1);

    // Step 1: MV UNKNOWN, Lucene STEADY, no steadyAsOfOplogPosition yet.
    // calculateEffectiveLuceneStatus passes through STEADY (position empty).
    // computeConsolidatedStatus(UNKNOWN, STEADY) returns STEADY via the fallthrough.
    // compositeWasQueryable must NOT be latched: mvStatus is not STEADY.
    AutoEmbeddingCompositeIndex index =
        createIndexWithPositionsAndSupplier(
            StatusCode.UNKNOWN,
            StatusCode.STEADY,
            Optional.empty(),
            Optional.of(luceneBehind),
            /* includeSupplier= */ true,
            /* wasQueryablePerLease= */ false);
    assertEquals(StatusCode.STEADY, index.getStatus().getStatusCode());

    // Step 2: steadyAsOfOplogPosition is now set to T1, ahead of Lucene's T0.5.
    // If compositeWasQueryable were latched in step 1, calculateEffectiveLuceneStatus would
    // bypass the lag check and return STEADY → consolidated STEADY (incorrect).
    // With the ratchet correctly un-latched, the lag check fires → INITIAL_SYNC.
    when(index.matViewIndex.getSteadyAsOfOplogPosition()).thenReturn(Optional.of(steadyPosition));
    assertEquals(StatusCode.INITIAL_SYNC, index.getStatus().getStatusCode());
  }

  @Test
  public void testMvUnknownWithSteadyLuceneReturnsSteady_replicationStoppedAfterRestart() {
    // When replication is stopped (e.g. forceDisableReplication) and mongot restarts, the
    // mat view is never initialized so its status stays UNKNOWN. The Lucene component is set
    // to STEADY by MongoDbNoOpReplicationManager reading local segment files. Without this
    // behaviour, computeConsolidatedStatus(UNKNOWN, STEADY) = UNKNOWN, which causes
    // isTransient()=true in MMS, filtering the index before processReplicationStatus() can
    // apply the STEADY->STALE transition.

    // Case 1: no steadyAsOfOplogPosition in lease (mat view never ran in this session)
    AutoEmbeddingCompositeIndex indexNoPosition =
        createIndexWithPositions(
            StatusCode.UNKNOWN, StatusCode.STEADY, Optional.empty(), Optional.empty());
    assertEquals(StatusCode.STEADY, indexNoPosition.getStatus().getStatusCode());

    // Case 2: steadyAsOfOplogPosition present from a previous run (synced from lease at startup),
    // and Lucene has caught up — the realistic forceDisableReplication+restart scenario.
    var steadyPosition = new BsonTimestamp(1000, 1);
    var luceneCaughtUp = new BsonTimestamp(1000, 1);
    AutoEmbeddingCompositeIndex indexCaughtUp =
        createIndexWithPositions(
            StatusCode.UNKNOWN,
            StatusCode.STEADY,
            Optional.of(steadyPosition),
            Optional.of(luceneCaughtUp));
    assertEquals(StatusCode.STEADY, indexCaughtUp.getStatus().getStatusCode());
  }

  @Test
  public void testMvUnknownWithSteadyLuceneReturnsUnknown_replicationNotStopped() {
    // The (UNKNOWN MV, STEADY Lucene) → synthetic STEADY fallthrough must only fire when
    // replication is actually stopped (cluster pause / disk-full / forceDisableReplication).
    // Without the gate this fallthrough also fires for followers at normal startup — their MV
    // is legitimately UNKNOWN because the leader owns it. Reporting STEADY there breaks
    // host-failover takeover by signalling "this index is already steady" before the follower
    // has done any MV work, causing the takeover path to skip creating a MongoDbReplicationManager
    // (CLOUDP-405298).

    // Case 1: no steadyAsOfOplogPosition (follower has never been a leader for this generation).
    AutoEmbeddingCompositeIndex followerNoPosition =
        createIndexWithPositionsAndSupplier(
            StatusCode.UNKNOWN,
            StatusCode.STEADY,
            Optional.empty(),
            Optional.empty(),
            /* includeSupplier= */ true,
            /* wasQueryablePerLease= */ false,
            /* isReplicationStopped= */ false);
    assertEquals(StatusCode.UNKNOWN, followerNoPosition.getStatus().getStatusCode());

    // Case 2: steadyAsOfOplogPosition present and Lucene caught up — the same shape as the
    // forceDisableReplication+restart test above, but without the replication-stopped signal.
    var steadyPosition = new BsonTimestamp(1000, 1);
    var luceneCaughtUp = new BsonTimestamp(1000, 1);
    AutoEmbeddingCompositeIndex followerCaughtUp =
        createIndexWithPositionsAndSupplier(
            StatusCode.UNKNOWN,
            StatusCode.STEADY,
            Optional.of(steadyPosition),
            Optional.of(luceneCaughtUp),
            /* includeSupplier= */ true,
            /* wasQueryablePerLease= */ false,
            /* isReplicationStopped= */ false);
    assertEquals(StatusCode.UNKNOWN, followerCaughtUp.getStatus().getStatusCode());

    // Same shape with NOT_STARTED for symmetry (production fallthrough covers both states).
    AutoEmbeddingCompositeIndex followerNotStarted =
        createIndexWithPositionsAndSupplier(
            StatusCode.NOT_STARTED,
            StatusCode.STEADY,
            Optional.empty(),
            Optional.empty(),
            /* includeSupplier= */ true,
            /* wasQueryablePerLease= */ false,
            /* isReplicationStopped= */ false);
    assertEquals(StatusCode.NOT_STARTED, followerNotStarted.getStatus().getStatusCode());
  }

  private void assertConsolidatedStatus(
      StatusCode mvStatus, StatusCode luceneStatus, StatusCode expected) {
    AutoEmbeddingCompositeIndex index = createIndexWithStatuses(mvStatus, luceneStatus);
    assertEquals(
        String.format("MV=%s, Lucene=%s should yield %s", mvStatus, luceneStatus, expected),
        expected,
        index.getStatus().getStatusCode());
  }

  /**
   * Builds a composite whose components return the given full statuses (with messages), for
   * asserting the consolidated FAILED message. A failing or non-STEADY Lucene status short-circuits
   * the lag check; a STEADY Lucene status passes through because no steady position is recorded.
   */
  private AutoEmbeddingCompositeIndex compositeWith(
      IndexStatus mvStatus, IndexStatus luceneStatus) {
    InitializedMaterializedViewIndex mockMvIndex = mock(InitializedMaterializedViewIndex.class);
    when(mockMvIndex.getStatus()).thenReturn(mvStatus);
    when(mockMvIndex.getSteadyAsOfOplogPosition()).thenReturn(Optional.empty());
    when(mockMvIndex.isCurrentVersionQueryablePerLease()).thenReturn(false);

    InitializedVectorIndex mockLuceneIndex = mock(InitializedVectorIndex.class);
    when(mockLuceneIndex.getStatus()).thenReturn(luceneStatus);

    return new AutoEmbeddingCompositeIndex(mockMvIndex, mockLuceneIndex, Optional::empty);
  }

  private AutoEmbeddingCompositeIndex createIndexWithStatuses(
      StatusCode mvStatus, StatusCode luceneStatus) {
    // For consolidation tests, make Lucene appear caught up by having matching positions
    var position = new BsonTimestamp(1000, 1);
    return createIndexWithPositions(
        mvStatus, luceneStatus, Optional.of(position), Optional.of(position));
  }

  private AutoEmbeddingCompositeIndex createIndexWithPositions(
      StatusCode mvStatus,
      StatusCode luceneStatus,
      Optional<BsonTimestamp> steadyAsOfPosition,
      Optional<BsonTimestamp> lucenePosition) {
    return createIndexWithPositionsAndSupplier(
        mvStatus, luceneStatus, steadyAsOfPosition, lucenePosition, true, false);
  }

  private AutoEmbeddingCompositeIndex createIndexWithPositionsAndSupplier(
      StatusCode mvStatus,
      StatusCode luceneStatus,
      Optional<BsonTimestamp> steadyAsOfPosition,
      Optional<BsonTimestamp> lucenePosition,
      boolean includeSupplier) {
    return createIndexWithPositionsAndSupplier(
        mvStatus, luceneStatus, steadyAsOfPosition, lucenePosition, includeSupplier, false, true);
  }

  private AutoEmbeddingCompositeIndex createIndexWithPositionsAndSupplier(
      StatusCode mvStatus,
      StatusCode luceneStatus,
      Optional<BsonTimestamp> steadyAsOfPosition,
      Optional<BsonTimestamp> lucenePosition,
      boolean includeSupplier,
      boolean wasQueryablePerLease) {
    return createIndexWithPositionsAndSupplier(
        mvStatus,
        luceneStatus,
        steadyAsOfPosition,
        lucenePosition,
        includeSupplier,
        wasQueryablePerLease,
        true);
  }

  private AutoEmbeddingCompositeIndex createIndexWithPositionsAndSupplier(
      StatusCode mvStatus,
      StatusCode luceneStatus,
      Optional<BsonTimestamp> steadyAsOfPosition,
      Optional<BsonTimestamp> lucenePosition,
      boolean includeSupplier,
      boolean wasQueryablePerLease,
      boolean isReplicationStopped) {

    InitializedMaterializedViewIndex mockMvIndex = mock(InitializedMaterializedViewIndex.class);
    when(mockMvIndex.getStatus()).thenReturn(new IndexStatus(mvStatus));
    when(mockMvIndex.getSteadyAsOfOplogPosition()).thenReturn(steadyAsOfPosition);
    when(mockMvIndex.getDefinition())
        .thenReturn(
            VectorIndexDefinitionBuilder.builder().withCosineVectorField("test", 128).build());
    when(mockMvIndex.isCurrentVersionQueryablePerLease()).thenReturn(wasQueryablePerLease);

    // Mock Lucene index with commit info for position extraction
    InitializedVectorIndex mockLuceneIndex = mock(InitializedVectorIndex.class);
    when(mockLuceneIndex.getStatus()).thenReturn(new IndexStatus(luceneStatus));

    IndexWriter mockWriter = mock(IndexWriter.class);
    when(mockLuceneIndex.getWriter()).thenReturn(mockWriter);

    // Create commit info with the Lucene position
    EncodedUserData commitData = createCommitInfoWithPosition(lucenePosition);
    when(mockWriter.getCommitUserData()).thenReturn(commitData);

    Supplier<Optional<InitializedIndex>> supplier =
        includeSupplier ? () -> Optional.of(mockLuceneIndex) : Optional::empty;
    return new AutoEmbeddingCompositeIndex(
        mockMvIndex, mockLuceneIndex, supplier, () -> isReplicationStopped);
  }

  private EncodedUserData createCommitInfoWithPosition(Optional<BsonTimestamp> position) {
    if (position.isEmpty()) {
      return EncodedUserData.EMPTY;
    }
    var resumeToken = ChangeStreamUtils.resumeToken(position.get());
    var indexCommitUserData =
        IndexCommitUserData.createChangeStreamResume(
            ChangeStreamResumeInfo.create(new MongoNamespace("test", "collection"), resumeToken),
            IndexFormatVersion.CURRENT.versionNumber);
    return indexCommitUserData.toEncodedData();
  }
}
