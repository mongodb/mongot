package com.xgen.mongot.replication.mongodb.common;

/**
 * Selects which change-stream priority strategy variant, if any, the steady-state scheduler uses to
 * classify getMore tasks as high or regular priority.
 */
public enum SteadyStateSchedulingMethod {
  /** No priority classification; tasks are scheduled in a single FIFO queue. */
  FIFO,
  /** Prioritizes an index whose previous batch delivered any documents. */
  HAD_DOCUMENTS,
  /** Prioritizes an index whose previous batch met a byte-size threshold. */
  BATCH_DATA_SIZE,
  /** Prioritizes an index whose change-stream cursor is lagging the server optime. */
  OPTIME_LAG
}
