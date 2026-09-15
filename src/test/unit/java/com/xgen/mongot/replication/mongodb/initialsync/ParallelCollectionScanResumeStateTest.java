package com.xgen.mongot.replication.mongodb.initialsync;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockInitializedIndex;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.BufferlessParallelNaturalOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DefaultDocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.PartitionResumeInfo;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonTimestamp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class ParallelCollectionScanResumeStateTest {

  private static final UUID INITIAL_SYNC_ID =
      UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee");
  private static final Optional<String> SYNC_SOURCE_HOST = Optional.of("shard0-0:27017");

  @Test
  public void partitionState_toPersisted_mirrorsFields() {
    ParallelCollectionScanPartitionState state =
        new ParallelCollectionScanPartitionState(
            2, Optional.of(100L), Optional.of(200L), Optional.of(150L), false);

    assertThat(state.toPersisted())
        .isEqualTo(
            new PartitionResumeInfo(
                2, Optional.of(100L), Optional.of(200L), Optional.of(150L), false));
  }

  @Test
  public void partitionState_fromPersisted_roundTrips() {
    PartitionResumeInfo persisted =
        new PartitionResumeInfo(1, Optional.empty(), Optional.of(999L), Optional.of(500L), true);

    assertThat(ParallelCollectionScanPartitionState.fromPersisted(persisted).toPersisted())
        .isEqualTo(persisted);
  }

  @Test
  public void advanceResumeRecordId_recordsDurablePosition() {
    ParallelCollectionScanPartitionState state =
        new ParallelCollectionScanPartitionState(
            0, Optional.empty(), Optional.of(1_000L), Optional.empty(), false);

    assertThat(state.getResumeRecordId()).isEmpty();

    state.advanceResumeRecordId(10L);
    assertThat(state.getResumeRecordId()).hasValue(10L);

    state.advanceResumeRecordId(25L);
    assertThat(state.getResumeRecordId()).hasValue(25L);
  }

  @Test
  public void onCycleEnd_recordsWhetherPartitionFinished() {
    ParallelCollectionScanPartitionState state =
        new ParallelCollectionScanPartitionState(
            0, Optional.empty(), Optional.of(1_000L), Optional.empty(), false);

    state.onCycleEnd(false);
    assertThat(state.isDone()).isFalse();

    state.onCycleEnd(true);
    assertThat(state.isDone()).isTrue();
    assertThat(state.toPersisted().isDone()).isTrue();
  }

  @Test
  public void snapshot_assemblesResumeInfoFromLiveComponents() {
    ParallelCollectionScanPartitionState p0 =
        new ParallelCollectionScanPartitionState(
            0, Optional.empty(), Optional.of(100L), Optional.of(50L), false);
    ParallelCollectionScanPartitionState p1 =
        new ParallelCollectionScanPartitionState(
            1, Optional.of(100L), Optional.empty(), Optional.empty(), false);

    ParallelCollectionScanResumeState resumeState =
        new ParallelCollectionScanResumeState(
            IndexFormatVersion.CURRENT,
            INITIAL_SYNC_ID,
            SYNC_SOURCE_HOST,
            new BsonTimestamp(1_000, 1),
            List.of(p0, p1));

    IndexCommitUserData userData = resumeState.snapshot();

    assertThat(userData.getBackendIndexVersion()).hasValue(IndexFormatVersion.CURRENT);
    assertThat(userData.getInitialSyncResumeInfo())
        .hasValue(
            new BufferlessParallelNaturalOrderInitialSyncResumeInfo(
                new BsonTimestamp(1_000, 1),
                INITIAL_SYNC_ID,
                List.of(p0.toPersisted(), p1.toPersisted()),
                SYNC_SOURCE_HOST));
  }

  @Test
  public void resumeState_fromPlan_numbersPartitionsInScanOrderWithNothingScanned() {
    InitialSyncPartitionPlan.PartitionedResult plan =
        new InitialSyncPartitionPlan.PartitionedResult(
            ImmutableList.of(
                new InitialSyncPartitionPlan.PartitionRange(Optional.empty(), Optional.of(100L)),
                new InitialSyncPartitionPlan.PartitionRange(Optional.of(100L), Optional.of(200L)),
                new InitialSyncPartitionPlan.PartitionRange(Optional.of(200L), Optional.empty())),
            INITIAL_SYNC_ID);

    ParallelCollectionScanResumeState resumeState =
        ParallelCollectionScanResumeState.fromPlan(
            IndexFormatVersion.CURRENT, plan, new BsonTimestamp(1_000, 1), SYNC_SOURCE_HOST);

    assertThat(resumeState.getInitialSyncId()).isEqualTo(INITIAL_SYNC_ID);
    assertThat(resumeState.getHighWaterMark()).isEqualTo(new BsonTimestamp(1_000, 1));
    assertThat(resumeState.getSyncSourceHost()).isEqualTo(SYNC_SOURCE_HOST);
    assertThat(resumeState.getPartitions().stream().map(p -> p.toPersisted()).toList())
        .containsExactly(
            new PartitionResumeInfo(
                0, Optional.empty(), Optional.of(100L), Optional.empty(), false),
            new PartitionResumeInfo(
                1, Optional.of(100L), Optional.of(200L), Optional.empty(), false),
            new PartitionResumeInfo(
                2, Optional.of(200L), Optional.empty(), Optional.empty(), false))
        .inOrder();
  }

  @Test
  public void resumeState_fromPersisted_roundTripsThroughSnapshot() {
    BufferlessParallelNaturalOrderInitialSyncResumeInfo persisted =
        new BufferlessParallelNaturalOrderInitialSyncResumeInfo(
            new BsonTimestamp(1_000, 1),
            INITIAL_SYNC_ID,
            List.of(
                new PartitionResumeInfo(
                    0, Optional.empty(), Optional.of(100L), Optional.of(50L), false),
                new PartitionResumeInfo(
                    1, Optional.of(100L), Optional.empty(), Optional.of(100L), true)),
            SYNC_SOURCE_HOST);

    ParallelCollectionScanResumeState resumeState =
        ParallelCollectionScanResumeState.fromPersisted(IndexFormatVersion.CURRENT, persisted);

    assertThat(resumeState.snapshot().getInitialSyncResumeInfo()).hasValue(persisted);
    assertThat(resumeState.getPartitions().get(1).isDone()).isTrue();
  }

  @Test
  public void snapshot_reflectsAdvancedHighWaterMarkAndPartitionProgress() {
    ParallelCollectionScanPartitionState p0 =
        new ParallelCollectionScanPartitionState(
            0, Optional.empty(), Optional.empty(), Optional.empty(), false);

    ParallelCollectionScanResumeState resumeState =
        new ParallelCollectionScanResumeState(
            IndexFormatVersion.CURRENT,
            INITIAL_SYNC_ID,
            SYNC_SOURCE_HOST,
            new BsonTimestamp(1_000, 1),
            List.of(p0));

    p0.advanceResumeRecordId(42L);
    resumeState.advanceHighWaterMark(new BsonTimestamp(2_000, 3));

    BufferlessParallelNaturalOrderInitialSyncResumeInfo resumeInfo =
        (BufferlessParallelNaturalOrderInitialSyncResumeInfo)
            resumeState.snapshot().getInitialSyncResumeInfo().orElseThrow();

    assertThat(resumeInfo.getResumeOperationTime()).isEqualTo(new BsonTimestamp(2_000, 3));
    assertThat(resumeInfo.getPartitions()).hasSize(1);
    assertThat(resumeInfo.getPartitions().getFirst().getResumeRecordId()).hasValue(42L);
  }

  @Test
  public void snapshot_installedAsCommitSupplier_persistsPositionAsOfTheCommit() throws Exception {
    ParallelCollectionScanPartitionState p0 =
        new ParallelCollectionScanPartitionState(
            0, Optional.empty(), Optional.empty(), Optional.empty(), false);
    ParallelCollectionScanResumeState resumeState =
        new ParallelCollectionScanResumeState(
            IndexFormatVersion.CURRENT,
            INITIAL_SYNC_ID,
            SYNC_SOURCE_HOST,
            new BsonTimestamp(1_000, 1),
            List.of(p0));

    InitializedIndex index = mockInitializedIndex(MOCK_INDEX_DEFINITION_GENERATION);
    DefaultDocumentIndexer indexer = DefaultDocumentIndexer.create(index);
    indexer.updateCommitUserDataSupplier(resumeState::snapshot);

    p0.advanceResumeRecordId(42L);
    resumeState.advanceHighWaterMark(new BsonTimestamp(2_000, 3));
    indexer.commit();

    ArgumentCaptor<EncodedUserData> committed = ArgumentCaptor.forClass(EncodedUserData.class);
    verify(index.getWriter()).commit(committed.capture());
    BufferlessParallelNaturalOrderInitialSyncResumeInfo persisted =
        (BufferlessParallelNaturalOrderInitialSyncResumeInfo)
            IndexCommitUserData.fromEncodedData(committed.getValue(), Optional.empty())
                .getInitialSyncResumeInfo()
                .orElseThrow();

    assertThat(persisted.getResumeOperationTime()).isEqualTo(new BsonTimestamp(2_000, 3));
    assertThat(persisted.getPartitions().getFirst().getResumeRecordId()).hasValue(42L);
  }
}
