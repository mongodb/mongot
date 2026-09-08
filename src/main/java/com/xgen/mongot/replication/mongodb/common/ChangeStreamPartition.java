package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.util.Check;

/** Identifies one physical reader among the partitions of a logical change stream. */
public record ChangeStreamPartition(int partitionId, int partitionCount) {

  public ChangeStreamPartition {
    Check.argIsPositive(partitionCount, "partitionCount");
    Check.checkArg(
        partitionId >= 0 && partitionId < partitionCount,
        "partitionId must be in [0, %s) but was %s",
        partitionCount,
        partitionId);
  }
}
