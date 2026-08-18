package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamIndexManager.BatchFullness;
import org.junit.Test;

public class BatchFullnessTest {

  @Test
  public void testEmptyBatchIgnoresBytes() {
    // Zero events is always EMPTY, even if a (spurious) non-zero byte count is passed.
    assertThat(BatchFullness.classify(0, 0)).isEqualTo(BatchFullness.EMPTY);
    assertThat(BatchFullness.classify(0, 1_000_000)).isEqualTo(BatchFullness.EMPTY);
  }

  @Test
  public void testBelowThresholdIsPartial() {
    assertThat(BatchFullness.classify(1, 1)).isEqualTo(BatchFullness.PARTIAL);
    assertThat(BatchFullness.classify(10, BatchFullness.FULL_BATCH_RAW_BYTES_THRESHOLD - 1))
        .isEqualTo(BatchFullness.PARTIAL);
  }

  @Test
  public void testAtOrAboveThresholdIsFull() {
    // The threshold itself counts as full (hit the ~16MB response cap => backlog).
    assertThat(BatchFullness.classify(5, BatchFullness.FULL_BATCH_RAW_BYTES_THRESHOLD))
        .isEqualTo(BatchFullness.FULL);
    assertThat(BatchFullness.classify(5, BatchFullness.FULL_BATCH_RAW_BYTES_THRESHOLD + 1))
        .isEqualTo(BatchFullness.FULL);
  }
}
