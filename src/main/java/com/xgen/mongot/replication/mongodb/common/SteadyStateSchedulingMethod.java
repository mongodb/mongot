package com.xgen.mongot.replication.mongodb.common;

/**
 * Selects which change-stream priority strategy variant, if any, the steady-state scheduler uses to
 * classify getMore tasks as high or regular priority.
 */
public enum SteadyStateSchedulingMethod {
  /** No priority classification; tasks are scheduled in a single FIFO queue. */
  FIFO,
  /** Prioritizes an index whose change-stream cursor is lagging the server optime. */
  OPTIME_LAG,
  /** Prioritizes the K worst-lagging indexes, ranked against the whole active population. */
  TOP_K_WORST_LAG
}
