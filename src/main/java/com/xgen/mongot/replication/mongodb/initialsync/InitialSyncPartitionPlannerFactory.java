package com.xgen.mongot.replication.mongodb.initialsync;

/**
 * Builds the {@link InitialSyncPartitionPlanner} that decides whether a sync's collection scan can
 * be split, so the manager can be driven with a planner it did not assemble.
 */
@FunctionalInterface
interface InitialSyncPartitionPlannerFactory {

  /**
   * Builds this sync's planner.
   *
   * @param contextWithProgress a context carrying this sync's high water mark, which the planner
   *     reads the sync source as of
   */
  InitialSyncPartitionPlanner create(InitialSyncContext contextWithProgress);
}
