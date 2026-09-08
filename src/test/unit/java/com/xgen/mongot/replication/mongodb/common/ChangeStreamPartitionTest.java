package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class ChangeStreamPartitionTest {

  @Test
  public void constructor_validPartition_preservesIdAndCount() {
    ChangeStreamPartition partition = new ChangeStreamPartition(2, 4);

    assertThat(partition.partitionId()).isEqualTo(2);
    assertThat(partition.partitionCount()).isEqualTo(4);
  }

  @Test
  public void constructor_zeroPartitionId_isAllowed() {
    ChangeStreamPartition partition = new ChangeStreamPartition(0, 1);

    assertThat(partition.partitionId()).isEqualTo(0);
    assertThat(partition.partitionCount()).isEqualTo(1);
  }

  @Test
  public void constructor_nonPositivePartitionCount_throws() {
    assertThrows(IllegalArgumentException.class, () -> new ChangeStreamPartition(0, 0));
    assertThrows(IllegalArgumentException.class, () -> new ChangeStreamPartition(0, -1));
  }

  @Test
  public void constructor_partitionIdOutOfRange_throws() {
    assertThrows(IllegalArgumentException.class, () -> new ChangeStreamPartition(-1, 2));
    assertThrows(IllegalArgumentException.class, () -> new ChangeStreamPartition(2, 2));
  }
}
