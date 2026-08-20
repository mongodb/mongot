package com.xgen.mongot.replication.mongodb.initialsync;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.PartitionRange;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.PartitionedResult;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.SequentialFallback;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.ShortCircuitReason;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;

public class InitialSyncPartitionPlanTest {

  private static final UUID INITIAL_SYNC_ID = UUID.randomUUID();
  private static final Optional<Long> OPEN = Optional.empty();

  private static Optional<Long> bound(long recordId) {
    return Optional.of(recordId);
  }

  @Test
  public void acceptsContiguousPartitions() {
    ImmutableList<PartitionRange> partitions =
        ImmutableList.of(
            new PartitionRange(OPEN, bound(95L)),
            new PartitionRange(bound(95L), bound(300L)),
            new PartitionRange(bound(300L), OPEN));

    PartitionedResult plan = new PartitionedResult(partitions, INITIAL_SYNC_ID);

    assertThat(plan.partitions()).hasSize(3);
    assertThat(plan.initialSyncId()).isEqualTo(INITIAL_SYNC_ID);
  }

  @Test
  public void rejectsFewerThanTwoPartitions() {
    ImmutableList<PartitionRange> single = ImmutableList.of(new PartitionRange(OPEN, OPEN));

    assertThrows(
        IllegalArgumentException.class,
        () -> new PartitionedResult(ImmutableList.of(), INITIAL_SYNC_ID));
    assertThrows(
        IllegalArgumentException.class, () -> new PartitionedResult(single, INITIAL_SYNC_ID));
  }

  @Test
  public void rejectsFirstPartitionWithLowerBound() {
    ImmutableList<PartitionRange> partitions =
        ImmutableList.of(
            new PartitionRange(bound(10L), bound(95L)), new PartitionRange(bound(95L), OPEN));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new PartitionedResult(partitions, INITIAL_SYNC_ID));

    assertThat(exception).hasMessageThat().contains("first partition must have no lower bound");
  }

  @Test
  public void rejectsLastPartitionWithUpperBound() {
    ImmutableList<PartitionRange> partitions =
        ImmutableList.of(
            new PartitionRange(OPEN, bound(95L)), new PartitionRange(bound(95L), bound(300L)));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new PartitionedResult(partitions, INITIAL_SYNC_ID));

    assertThat(exception).hasMessageThat().contains("last partition must have no upper bound");
  }

  @Test
  public void rejectsGapBetweenPartitions() {
    ImmutableList<PartitionRange> partitions =
        ImmutableList.of(
            new PartitionRange(OPEN, bound(95L)), new PartitionRange(bound(300L), OPEN));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new PartitionedResult(partitions, INITIAL_SYNC_ID));

    assertThat(exception).hasMessageThat().contains("contiguous");
  }

  @Test
  public void rejectsOverlappingPartitions() {
    ImmutableList<PartitionRange> partitions =
        ImmutableList.of(
            new PartitionRange(OPEN, bound(300L)), new PartitionRange(bound(95L), OPEN));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new PartitionedResult(partitions, INITIAL_SYNC_ID));

    assertThat(exception).hasMessageThat().contains("contiguous");
  }

  @Test
  public void rejectsPartitionWithLowerBoundNotBelowUpper() {
    assertThrows(IllegalArgumentException.class, () -> new PartitionRange(bound(300L), bound(95L)));
    assertThrows(IllegalArgumentException.class, () -> new PartitionRange(bound(95L), bound(95L)));
  }

  @Test
  public void rejectsNullInitialSyncId() {
    ImmutableList<PartitionRange> partitions =
        ImmutableList.of(
            new PartitionRange(OPEN, bound(95L)), new PartitionRange(bound(95L), OPEN));

    assertThrows(IllegalArgumentException.class, () -> new PartitionedResult(partitions, null));
  }

  @Test
  public void sequentialFallbackCarriesReason() {
    SequentialFallback fallback = new SequentialFallback(ShortCircuitReason.CLUSTERED_COLLECTION);

    assertThat(fallback.reason()).isEqualTo(ShortCircuitReason.CLUSTERED_COLLECTION);
  }

  @Test
  public void sequentialFallbackRejectsNullReason() {
    assertThrows(IllegalArgumentException.class, () -> new SequentialFallback(null));
  }
}
