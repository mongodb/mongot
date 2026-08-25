package com.xgen.mongot.config.backup;

/**
 * What to do with a config journal entry whose index definition does not parse.
 *
 * <p>Passed in rather than read from a feature flag so that the parsing layer takes no dependency
 * on the feature flag system and both behaviours are directly testable. Callers map the {@code
 * QUARANTINE_UNPARSEABLE_INDEX} feature flag onto this.
 */
public enum QuarantinePolicy {
  /** A single unparseable entry fails the whole journal read. */
  FAIL_FAST,
  /**
   * An unparseable entry is kept verbatim as a {@link JournalEntry.Quarantined} so the remaining
   * indexes can start.
   */
  QUARANTINE,
}
