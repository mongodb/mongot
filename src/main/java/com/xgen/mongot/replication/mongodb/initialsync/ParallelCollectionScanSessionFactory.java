package com.xgen.mongot.replication.mongodb.initialsync;

/**
 * Builds the {@link ParallelCollectionScanSession} that owns a parallel scan's partition pool and
 * dispatcher, so the manager can be driven with a session it did not assemble.
 */
@FunctionalInterface
interface ParallelCollectionScanSessionFactory {

  ParallelCollectionScanSession create(ParallelCollectionScanResumeState resumeState);
}
