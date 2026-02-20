package com.xgen.mongot.index.status;

/**
 * The {@link SynonymStatus} of a SynonymMapping represents the readiness of that mapping to service
 * queries.
 *
 * <p>Synonym mapping has no effect on index status, and indexes can be READY even when one or more
 * synonym mappings are not in a READY or READY_UPDATING state. Vice versa is also true, although
 * index not being in a READY state effectively makes synonym mappings un-queryable.
 *
 * <p>Enum constants are listed here in order of increasing readiness: FAILED, INVALID,
 * SYNC_ENQUEUED, INITIAL_SYNC, READY_UPDATING, READY.
 */
public enum SynonymStatus {
  FAILED,
  INVALID,
  SYNC_ENQUEUED,
  INITIAL_SYNC,
  READY_UPDATING,
  READY;

  public boolean isReady() {
    return this == READY_UPDATING || this == READY;
  }

  public static External toExternal(SynonymStatus status) {
    return switch (status) {
      case SYNC_ENQUEUED, READY_UPDATING, INITIAL_SYNC -> External.BUILDING;
      case READY -> External.READY;
      case FAILED, INVALID -> External.FAILED;
    };
  }

  public enum External {
    BUILDING(1),
    READY(2),
    FAILED(3);

    private final int priority;

    External(int priority) {
      this.priority = priority;
    }

    public int getPriority() {
      return this.priority;
    }
  }
}
