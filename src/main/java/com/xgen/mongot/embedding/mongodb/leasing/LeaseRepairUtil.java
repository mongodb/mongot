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
 * <p>On a dedicated cluster, mongot's dbResolver never emits a tenant-prefixed namespace. A
 * tenant-prefixed namespace in a lease's commitInfo on a dedicated cluster is therefore an
 * invariant violation that uniquely identifies a flex→dedicated migration artifact.
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

  /**
   * Returns true if the lease was authored on a flex cluster and needs post-restore repair. Fires
   * when the commitInfo namespace carries a tenant prefix, which never occurs on dedicated
   * clusters. The empty-commitInfo migration case (flex lease in INITIAL_SYNC at migration time) is
   * caught separately in {@code DynamicLeaderLeaseManager.initializeLease} via the fresh MV UUID
   * passed in on {@code proposedMetadata}.
   */
  public static boolean requiresPostRestoreRepair(Lease lease, boolean isDedicatedCluster) {
    if (!isDedicatedCluster) {
      return false;
    }
    return parseCommitInfo(lease)
        .getResumeInfo()
        .map(info -> TENANT_PREFIX.matcher(info.getNamespace().getDatabaseName()).find())
        .orElse(false);
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
    UUID newSourceUuid = uuidResolver.resolve(sourceDb, sourceCollection);
    MaterializedViewCollectionMetadata mvMeta = lease.materializedViewCollectionMetadata();
    UUID newMvUuid = uuidResolver.resolve(internalSearchDb, mvMeta.collectionName());

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

  /** Resolves a collection UUID by database + collection name. */
  @FunctionalInterface
  public interface CollectionUuidResolver {
    UUID resolve(String database, String collection) throws LeaseRepairException;
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
