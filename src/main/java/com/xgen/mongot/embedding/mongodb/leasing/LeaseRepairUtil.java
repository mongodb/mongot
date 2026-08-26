package com.xgen.mongot.embedding.mongodb.leasing;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for detecting and repairing auto-embedding leases that were authored on a flex
 * (multi-tenant) cluster and migrated onto a dedicated cluster via snapshot restore.
 *
 * <p>On a flex cluster Atlas stores a tenant's data under the physical database {@code
 * <tenantId>_<logicalDb>} and rewrites the index definition to the logical name on migration (MMS
 * {@code FTSIndexConfigSvc.mtmDatabaseToTenantDatabase}); a migrated lease's commitInfo still
 * carries the old physical namespace, and that disagreement is the artifact repaired here.
 *
 * <p>The tenant-prefix shape alone proves nothing, since database names are customer-controlled.
 * Detection compares against the live source database when known, and repair treats an unresolvable
 * stripped namespace as proof the shape match was a false positive.
 */
public final class LeaseRepairUtil {

  private static final Logger LOG = LoggerFactory.getLogger(LeaseRepairUtil.class);

  private LeaseRepairUtil() {}

  /** Tenant-prefix shape: 24 lowercase hex chars followed by an underscore. */
  private static final Pattern TENANT_PREFIX = Pattern.compile("^[a-f0-9]{24}_");

  /** Returns the {@code db.collection} namespace recorded in the lease's commitInfo, if any. */
  public static Optional<String> extractNamespace(Lease lease) {
    return parseCommitInfo(lease).getResumeInfo().map(info -> info.getNamespace().getFullName());
  }

  /** Whether a lease's commitInfo namespace is a flex→dedicated migration artifact. */
  public enum RepairNeed {
    /**
     * Flex cluster, no commitInfo namespace, no tenant prefix on it, or no live source database to
     * compare against.
     */
    NONE,
    /** Tenant prefix strips to exactly the live source database. */
    REQUIRED,
    /** Tenant-prefix shape matched but the live source database disagrees: customer-named db. */
    NOT_A_MIGRATION
  }

  /**
   * Classifies a lease for post-restore repair. This is a positive test: stripping the tenant
   * prefix from the lease's commitInfo database must yield exactly {@code liveSourceDatabase},
   * mirroring the rewrite MMS performs on migration.
   *
   * <p>An absent {@code liveSourceDatabase} yields {@code NONE} rather than falling back to the
   * prefix shape. The shape alone cannot distinguish a migrated {@code tenant_foo} from a
   * customer-named one that coexists with an unrelated {@code foo}; repairing on shape would
   * rewrite the lease to that unrelated source and clear the commitInfo that any later
   * reclassification depends on.
   *
   * <p>Empty commitInfo also returns {@code NONE}: that migration case is caught by {@code
   * DynamicLeaderLeaseManager.initializeLease} via the fresh MV UUID on {@code proposedMetadata}.
   */
  public static RepairNeed classifyRepairNeed(
      Lease lease, boolean isDedicatedCluster, Optional<String> liveSourceDatabase) {
    if (!isDedicatedCluster || liveSourceDatabase.isEmpty()) {
      return RepairNeed.NONE;
    }
    Optional<String> commitInfoDatabase =
        parseCommitInfo(lease).getResumeInfo().map(info -> info.getNamespace().getDatabaseName());
    if (commitInfoDatabase.isEmpty()) {
      return RepairNeed.NONE;
    }
    String prefixedDb = commitInfoDatabase.get();
    var matcher = TENANT_PREFIX.matcher(prefixedDb);
    if (!matcher.find()) {
      return RepairNeed.NONE;
    }
    return prefixedDb.substring(matcher.end()).equals(liveSourceDatabase.get())
        ? RepairNeed.REQUIRED
        : RepairNeed.NOT_A_MIGRATION;
  }

  private static IndexCommitUserData parseCommitInfo(Lease lease) {
    String commitInfo = lease.commitInfo();
    if (commitInfo == null || commitInfo.isEmpty()) {
      return IndexCommitUserData.EMPTY;
    }
    try {
      return IndexCommitUserData.fromEncodedData(
          EncodedUserData.fromString(commitInfo), Optional.empty());
    } catch (Throwable t) {
      // Unparseable non-empty commitInfo is unusual — log so it doesn't silently mask corruption.
      LOG.atWarn()
          .addKeyValue("leaseId", lease.id())
          .setCause(t)
          .log("Failed to parse lease commitInfo; treating as absent for repair detection");
      return IndexCommitUserData.EMPTY;
    }
  }

  /**
   * Full repair for a stale flex→dedicated migration lease (tenant-prefixed commitInfo case):
   * clears commitInfo, re-derives the source collection (name + UUID) from the commitInfo namespace
   * and the MV UUID via {@code uuidResolver}, nulls steadyAsOfOplogPosition. Reads the source
   * namespace before commitInfo is cleared. Returns the input lease unchanged via reference
   * equality if the prefix-regex hit was a false positive (name and both UUIDs already current).
   *
   * @param internalSearchDb the database where materialized-view collections live (the resolver's
   *     {@code resolveDefault()}).
   */
  public static Lease repairLeaseForPostRestoreMigration(
      Lease lease, CollectionUuidResolver uuidResolver, String internalSearchDb)
      throws LeaseRepairException {
    // The commitInfo namespace is the collection the change stream actually follows — the base
    // collection for a view-backed index, never the view leaf. Deriving both db and collection from
    // it canonicalizes the source identity even when collectionName holds a stale view leaf.
    var sourceNamespace =
        parseCommitInfo(lease)
            .getResumeInfo()
            .map(info -> info.getNamespace())
            .orElseThrow(
                () ->
                    new LeaseRepairException(
                        "lease "
                            + lease.id()
                            + " has no resume info to derive source namespace from"));
    String sourceDb = stripTenantPrefix(sourceNamespace.getDatabaseName());
    String sourceCollection = sourceNamespace.getCollectionName();
    Optional<UUID> resolvedSourceUuid = uuidResolver.resolve(sourceDb, sourceCollection);
    if (resolvedSourceUuid.isEmpty()) {
      // A genuine migration leaves the logical namespace in place, so an absent stripped namespace
      // means the prefix match was a false positive. Leave the lease alone.
      return lease;
    }
    UUID newSourceUuid = resolvedSourceUuid.get();
    MaterializedViewCollectionMetadata mvMeta = lease.materializedViewCollectionMetadata();
    // The MV lives in the internal database, which is never tenant-named, so its absence says
    // nothing about the prefix and stays a repair failure.
    UUID newMvUuid =
        uuidResolver
            .resolve(internalSearchDb, mvMeta.collectionName())
            .orElseThrow(
                () ->
                    new LeaseRepairException(
                        "materialized view collection "
                            + internalSearchDb
                            + "."
                            + mvMeta.collectionName()
                            + " not found while repairing lease "
                            + lease.id()));

    boolean sourceUuidStale = !newSourceUuid.toString().equals(lease.collectionUuid());
    boolean mvUuidStale = !newMvUuid.equals(mvMeta.collectionUuid());
    boolean sourceNameStale = !sourceCollection.equals(lease.collectionName());
    if (!sourceUuidStale && !mvUuidStale && !sourceNameStale) {
      // False positive on prefix regex — customer DB literally named with a 24-hex prefix.
      return lease;
    }
    return new Lease(
        lease.id(),
        Lease.SCHEMA_VERSION,
        newSourceUuid.toString(),
        sourceCollection,
        lease.leaseOwner(),
        lease.leaseExpiration(),
        lease.leaseVersion(),
        EncodedUserData.EMPTY.asString(),
        lease.latestIndexDefinitionVersion(),
        lease.indexDefinitionVersionStatusMap(),
        new MaterializedViewCollectionMetadata(
            mvMeta.schemaMetadata(), newMvUuid, mvMeta.collectionName()),
        null,
        Lease.CleanupState.NOT_ELIGIBLE);
  }

  /**
   * MV-only repair for the empty-commitInfo migration case (flex lease still in INITIAL_SYNC at
   * migration time): rewrites the materialized-view collection UUID to {@code freshMvUuid} and
   * preserves everything else on the lease. Called from {@code
   * DynamicLeaderLeaseManager.initializeLease} where the fresh MV UUID is already available via
   * {@code proposedMetadata} — no listCollections is required.
   */
  public static Lease repairMvUuidOnly(Lease lease, UUID freshMvUuid) {
    MaterializedViewCollectionMetadata mvMeta = lease.materializedViewCollectionMetadata();
    return new Lease(
        lease.id(),
        Lease.SCHEMA_VERSION,
        lease.collectionUuid(),
        lease.collectionName(),
        lease.leaseOwner(),
        lease.leaseExpiration(),
        lease.leaseVersion(),
        lease.commitInfo(),
        lease.latestIndexDefinitionVersion(),
        lease.indexDefinitionVersionStatusMap(),
        new MaterializedViewCollectionMetadata(
            mvMeta.schemaMetadata(), freshMvUuid, mvMeta.collectionName()),
        lease.steadyAsOfOplogPosition(),
        Lease.CleanupState.NOT_ELIGIBLE);
  }

  private static String stripTenantPrefix(String prefixedDb) throws LeaseRepairException {
    var matcher = TENANT_PREFIX.matcher(prefixedDb);
    if (!matcher.find()) {
      throw new LeaseRepairException(
          "expected tenant-prefixed database name but got: " + prefixedDb);
    }
    return prefixedDb.substring(matcher.end());
  }

  /**
   * Resolves a collection UUID by database + collection name. Empty means the namespace
   * definitively does not exist; {@link LeaseRepairException} means the lookup failed or resolved
   * to a non- collection. Callers depend on that split: an absent namespace is evidence, a failed
   * lookup isn't.
   */
  @FunctionalInterface
  public interface CollectionUuidResolver {
    Optional<UUID> resolve(String database, String collection) throws LeaseRepairException;
  }

  /** Thrown when post-restore repair cannot complete (e.g. UUID lookup failure, invalid input). */
  public static final class LeaseRepairException extends Exception {
    public LeaseRepairException(String message) {
      super(message);
    }

    public LeaseRepairException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
