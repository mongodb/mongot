package com.xgen.mongot.replication;

import java.time.Instant;

/**
 * The status of replica indexes.
 *
 * @param lastUpdateTime the last time the replication status was checked.
 * @param startTime when the initial status was created.
 * @param isReplicationStopped whether replication is currently stopped.
 * @param isAutoEmbeddingMaterializedViewStopped whether auto-embedding materialized-view
 *     replication is currently stopped.
 * @param initialSyncStatus the status of initial sync disk utilization.
 */
public record ReplicationStatus(
    Instant lastUpdateTime,
    Instant startTime,
    boolean isReplicationStopped,
    boolean isAutoEmbeddingMaterializedViewStopped,
    InitialSyncStatus initialSyncStatus) {}
