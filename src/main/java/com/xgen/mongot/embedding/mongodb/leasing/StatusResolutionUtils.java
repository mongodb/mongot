package com.xgen.mongot.embedding.mongodb.leasing;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class containing methods for resolving the effective status of an auto-embedding related
 * entity based on the status of its constituent parts.
 */
public class StatusResolutionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(StatusResolutionUtils.class);

  /** The placeholder a lease carries for a version whose build has not started. */
  private static final Lease.IndexDefinitionVersionStatus UNKNOWN_VERSION_STATUS =
      new Lease.IndexDefinitionVersionStatus(false, StatusCode.UNKNOWN);

  /**
   * Resolves the effective MV status of {@code versionKey} from {@code lease}.
   *
   * <p>A version key the lease does not carry resolves to {@link StatusCode#UNKNOWN}. Version keys
   * are only ever added to a lease, never pruned, so an absent key means the leader has not
   * published this version yet — the follower created the generation inside the publication window.
   * Resolving such a key against the lease's latest version instead reports the <em>previous</em>
   * definition's status, typically STEADY, for a materialized view that is still empty. That
   * spurious STEADY latches the "was queryable" ratchet, which then masks the truthful INITIAL_SYNC
   * as RECOVERING_TRANSIENT, and a RECOVERING_TRANSIENT staged generation is promoted over the live
   * one that is still serving queries (HELP-97792).
   */
  public static IndexStatus getEffectiveMaterializedViewStatus(
      Lease lease, String versionKey, MaterializedViewGenerationId generationId) {
    Lease.IndexDefinitionVersionStatus requestedStatus =
        lease.indexDefinitionVersionStatusMap().get(versionKey);
    if (requestedStatus == null) {
      LOG.atWarn()
          .addKeyValue("versionKey", versionKey)
          .addKeyValue("generationId", generationId)
          .addKeyValue("leaseLatestVersion", lease.latestIndexDefinitionVersion())
          .log("Version key not published in lease yet, reporting materialized view as UNKNOWN");
      return new IndexStatus(StatusCode.UNKNOWN);
    }
    @Var
    Lease.IndexDefinitionVersionStatus latestStatus =
        lease.indexDefinitionVersionStatusMap().get(lease.latestIndexDefinitionVersion());
    if (latestStatus == null) {
      // A lease whose latestIndexDefinitionVersion is not a key in its own status map is malformed;
      // no write path produces one, and parsing does not reject it. Substitute the not-yet-built
      // placeholder rather than dereferencing null: the follower poll loop calls this without a
      // per-generation guard, and refreshStatus() runs as a VerboseRunnable under
      // scheduleWithFixedDelay, which rethrows — so an NPE here would cancel status refresh and
      // leadership acquisition for the whole process until restart.
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .addKeyValue("leaseLatestVersion", lease.latestIndexDefinitionVersion())
          .log("Lease latest version key absent from its own status map, treating it as UNKNOWN");
      latestStatus = UNKNOWN_VERSION_STATUS;
    }
    return getEffectiveMaterializedViewStatus(requestedStatus, latestStatus);
  }

  /**
   * Return effective status of the MV when there are multiple versions as indexes are being updated
   */
  public static IndexStatus getEffectiveMaterializedViewStatus(
      Lease.IndexDefinitionVersionStatus requestedIndexDefinitionVersionStatus,
      Lease.IndexDefinitionVersionStatus latestIndexDefinitionVersionStatus) {
    StatusCode requestedStatus = requestedIndexDefinitionVersionStatus.indexStatusCode();
    StatusCode latestStatus = latestIndexDefinitionVersionStatus.indexStatusCode();

    // Same Version: Pass through
    if (requestedIndexDefinitionVersionStatus.equals(latestIndexDefinitionVersionStatus)) {
      // If the index was previously queryable but is now rebuilding (e.g. fell off the oplog),
      // report RECOVERING_TRANSIENT (which gets reported externally as STALE) rather than
      // INITIAL_SYNC (BUILDING) so Atlas reflects "queryable but potentially out of date" while the
      // resync is in progress.
      if (requestedIndexDefinitionVersionStatus.isQueryable()
          && requestedStatus == StatusCode.INITIAL_SYNC) {
        return IndexStatus.recoveringTransient("Recovering via resync");
      }
      return new IndexStatus(requestedStatus);
    }

    // Terminal/DNE states in requested always take precedence
    if (requestedStatus == StatusCode.FAILED
        || requestedStatus == StatusCode.STALE
        || requestedStatus == StatusCode.DOES_NOT_EXIST) {
      return new IndexStatus(requestedStatus);
    }

    // Latest hasn't started building yet - indicate transition with RECOVERING_TRANSIENT
    if (latestStatus == StatusCode.UNKNOWN || latestStatus == StatusCode.NOT_STARTED) {
      return IndexStatus.recoveringTransient("New version pending build");
    }

    // Latest is actively building - use requested (old version serving)
    if (latestStatus == StatusCode.INITIAL_SYNC) {
      return new IndexStatus(requestedStatus);
    }

    // Latest has progressed past building - use its status
    return new IndexStatus(latestStatus);
  }
}
