package com.xgen.mongot.embedding.mongodb.leasing;

import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseRepairUtil.RepairNeed;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonTimestamp;
import org.junit.Test;

/** Unit tests for {@link LeaseRepairUtil}. */
public class LeaseRepairUtilTest {

  private static final String TENANT_ID = "647343c92726405e854130e4"; // 24 hex chars
  private static final String COLLECTION_UUID = "550e8400-e29b-41d4-a716-446655440000";
  private static final String COLLECTION_NAME = "test-collection";
  private static final String MV_COLLECTION_NAME = "test-mv-collection";
  private static final String NESTED_LOGICAL_DB = "abcdef012345678901234567_mydb";

  @Test
  public void classifyRepairNeed_requiresAgreementWithLiveSourceDatabase() {
    Lease lease = leaseWithNamespaceDb(TENANT_ID + "_sample_mflix");

    assertEquals(RepairNeed.REQUIRED, classify(lease, "sample_mflix"));
    // Only the leading tenant segment is stripped, so a prefix-shaped logical name still repairs.
    assertEquals(
        RepairNeed.REQUIRED,
        classify(leaseWithNamespaceDb(TENANT_ID + "_" + NESTED_LOGICAL_DB), NESTED_LOGICAL_DB));

    // Live db disagrees with the stripped name: the customer named it that way.
    assertEquals(RepairNeed.NOT_A_MIGRATION, classify(lease, TENANT_ID + "_sample_mflix"));
    assertEquals(RepairNeed.NOT_A_MIGRATION, classify(lease, "other_db"));

    // No live db to compare against, and flex clusters: never repair.
    assertEquals(
        RepairNeed.NONE, LeaseRepairUtil.classifyRepairNeed(lease, true, Optional.empty()));
    assertEquals(
        RepairNeed.NONE,
        LeaseRepairUtil.classifyRepairNeed(lease, false, Optional.of("sample_mflix")));
  }

  @Test
  public void classifyRepairNeed_namespaceIsNotTenantShaped_returnsNone() {
    // Un-prefixed, non-tenant underscore, non-hex prefix, and no trailing underscore.
    assertEquals(RepairNeed.NONE, classify(leaseWithNamespaceDb("sample_mflix"), "sample_mflix"));
    assertEquals(RepairNeed.NONE, classify(leaseWithNamespaceDb("my_app_db"), "app_db"));
    assertEquals(
        RepairNeed.NONE, classify(leaseWithNamespaceDb("zzzzzzzzzzzzzzzzzzzzzzzz_db"), "db"));
    assertEquals(RepairNeed.NONE, classify(leaseWithNamespaceDb(TENANT_ID), TENANT_ID));

    // Empty commitInfo is handled by initializeLease via proposedMetadata's fresh MV UUID.
    assertEquals(
        RepairNeed.NONE,
        classify(leaseWithRawCommitInfo(EncodedUserData.EMPTY.asString()), "sample_mflix"));
  }

  private static RepairNeed classify(Lease lease, String liveSourceDatabase) {
    return LeaseRepairUtil.classifyRepairNeed(lease, true, Optional.of(liveSourceDatabase));
  }

  private static final String INTERNAL_SEARCH_DB = "__mdb_internal_search";

  @Test
  public void repairLeaseForPostRestoreMigration_rewritesStaleFieldsPreservesMv() throws Exception {
    UUID freshSourceUuid = UUID.randomUUID();
    UUID freshMvUuid = UUID.randomUUID();
    Lease stale = leaseWithNamespaceDb(TENANT_ID + "_sample_mflix", new BsonTimestamp(1000, 1));

    Lease repaired =
        LeaseRepairUtil.repairLeaseForPostRestoreMigration(
            stale,
            (db, coll) -> {
              if (db.equals("sample_mflix") && coll.equals(COLLECTION_NAME)) {
                return Optional.of(freshSourceUuid);
              }
              if (db.equals(INTERNAL_SEARCH_DB) && coll.equals(MV_COLLECTION_NAME)) {
                return Optional.of(freshMvUuid);
              }
              throw new AssertionError("unexpected resolver call: " + db + "." + coll);
            },
            INTERNAL_SEARCH_DB);

    // stale fields rewritten
    assertEquals(EncodedUserData.EMPTY.asString(), repaired.commitInfo());
    assertNull(repaired.steadyAsOfOplogPosition());
    assertEquals(freshSourceUuid.toString(), repaired.collectionUuid());
    assertEquals(freshMvUuid, repaired.materializedViewCollectionMetadata().collectionUuid());

    // preserved: identity + MV schema + ownership (these came from withNewTenure upstream)
    assertEquals(stale.id(), repaired.id());
    assertEquals(stale.collectionName(), repaired.collectionName());
    assertEquals(
        MV_COLLECTION_NAME, repaired.materializedViewCollectionMetadata().collectionName());
    assertEquals(
        stale.materializedViewCollectionMetadata().schemaMetadata(),
        repaired.materializedViewCollectionMetadata().schemaMetadata());
    assertEquals(stale.leaseOwner(), repaired.leaseOwner());
    assertEquals(stale.leaseVersion(), repaired.leaseVersion());
    // Metadata repair is not an ownership transfer, so epoch must be preserved.
    assertEquals(stale.epoch(), repaired.epoch());
  }

  @Test
  public void repairLeaseForPostRestoreMigration_falsePositive_returnsLeaseUnchanged()
      throws Exception {
    Lease lease = leaseWithNamespaceDb(TENANT_ID + "_sample_mflix");
    UUID leaseSourceUuid = UUID.fromString(lease.collectionUuid());
    UUID leaseMvUuid = lease.materializedViewCollectionMetadata().collectionUuid();

    // The stripped namespace does not exist, so the lease was never a migration artifact.
    assertSame(
        lease,
        LeaseRepairUtil.repairLeaseForPostRestoreMigration(
            lease,
            (db, coll) ->
                db.equals(INTERNAL_SEARCH_DB) ? Optional.of(leaseMvUuid) : Optional.empty(),
            INTERNAL_SEARCH_DB));

    // Stripped namespace exists but both UUIDs are already current — nothing to rewrite.
    assertSame(
        lease,
        LeaseRepairUtil.repairLeaseForPostRestoreMigration(
            lease,
            (db, coll) ->
                Optional.of(db.equals(INTERNAL_SEARCH_DB) ? leaseMvUuid : leaseSourceUuid),
            INTERNAL_SEARCH_DB));
  }

  @Test
  public void repairLeaseForPostRestoreMigration_viewLeafName_canonicalizesToBaseCollection()
      throws Exception {
    // Legacy view-backed lease: collectionName holds the view leaf, but commitInfo tracks the base
    // collection the change stream follows. Repair must resolve the base collection — never the
    // view leaf, which would resolve to a View — and rewrite collectionName to it.
    UUID freshRootUuid = UUID.randomUUID();
    UUID freshMvUuid = UUID.randomUUID();
    Lease stale =
        viewLeafLease(TENANT_ID + "_sample_mflix", "movies", "movies_after_1950", COLLECTION_UUID);

    Lease repaired =
        LeaseRepairUtil.repairLeaseForPostRestoreMigration(
            stale,
            (db, coll) -> {
              if (db.equals("sample_mflix") && coll.equals("movies")) {
                return Optional.of(freshRootUuid);
              }
              if (db.equals(INTERNAL_SEARCH_DB) && coll.equals(MV_COLLECTION_NAME)) {
                return Optional.of(freshMvUuid);
              }
              // Resolving the view leaf as a collection is exactly the bug under test.
              throw new AssertionError("unexpected resolver call: " + db + "." + coll);
            },
            INTERNAL_SEARCH_DB);

    assertEquals("movies", repaired.collectionName());
    assertEquals(freshRootUuid.toString(), repaired.collectionUuid());
    assertEquals(freshMvUuid, repaired.materializedViewCollectionMetadata().collectionUuid());
    assertEquals(EncodedUserData.EMPTY.asString(), repaired.commitInfo());
    assertNull(repaired.steadyAsOfOplogPosition());
  }

  @Test
  public void repairLeaseForPostRestoreMigration_viewLeafNameWithCurrentUuids_stillRewritesName()
      throws Exception {
    // Both UUIDs already current, but collectionName is a stale view leaf. Name staleness alone
    // must force the rebuild instead of an unchanged return, so the leaf gets canonicalized.
    Lease stale =
        viewLeafLease(TENANT_ID + "_sample_mflix", "movies", "movies_after_1950", COLLECTION_UUID);
    UUID currentSourceUuid = UUID.fromString(stale.collectionUuid());
    UUID currentMvUuid = stale.materializedViewCollectionMetadata().collectionUuid();

    Lease repaired =
        LeaseRepairUtil.repairLeaseForPostRestoreMigration(
            stale,
            (db, coll) ->
                Optional.of(db.equals(INTERNAL_SEARCH_DB) ? currentMvUuid : currentSourceUuid),
            INTERNAL_SEARCH_DB);

    assertNotSame(stale, repaired);
    assertEquals("movies", repaired.collectionName());
    assertEquals(stale.collectionUuid(), repaired.collectionUuid());
  }

  @Test
  public void repairLeaseForPostRestoreMigration_failureCases() {
    Lease stale = leaseWithNamespaceDb(TENANT_ID + "_sample_mflix");

    LeaseRepairUtil.CollectionUuidResolver anyUuid = (db, coll) -> Optional.of(UUID.randomUUID());

    // Lease with no resume info in commitInfo → LeaseRepairException (nothing to derive source db).
    // DEFINITIVE: the lease data itself cannot support repair, so a retry fails identically.
    Lease noResume = leaseWithRawCommitInfo(EncodedUserData.EMPTY.asString());
    var noResumeFailure =
        assertThrows(
            LeaseRepairUtil.LeaseRepairException.class,
            () ->
                LeaseRepairUtil.repairLeaseForPostRestoreMigration(
                    noResume, anyUuid, INTERNAL_SEARCH_DB));
    assertEquals(LeaseRepairUtil.LeaseRepairException.Kind.DEFINITIVE, noResumeFailure.kind());

    // Source-side lookup fails (called first) → LeaseRepairException propagates. Distinct from the
    // resolver returning empty, which means the namespace is absent and the lease is fine. The kind
    // the resolver chose is preserved, since only the resolver knows whether it got an answer.
    LeaseRepairUtil.CollectionUuidResolver sourceFailing =
        (db, coll) -> {
          if (db.equals(INTERNAL_SEARCH_DB)) {
            return Optional.of(UUID.randomUUID());
          }
          throw new LeaseRepairUtil.LeaseRepairException(
              LeaseRepairUtil.LeaseRepairException.Kind.LOOKUP_FAILED, "source lookup failed");
        };
    var sourceFailure =
        assertThrows(
            LeaseRepairUtil.LeaseRepairException.class,
            () ->
                LeaseRepairUtil.repairLeaseForPostRestoreMigration(
                    stale, sourceFailing, INTERNAL_SEARCH_DB));
    assertEquals(LeaseRepairUtil.LeaseRepairException.Kind.LOOKUP_FAILED, sourceFailure.kind());

    // MV collection absent → repair failure, not a false positive: the MV lives in the internal
    // database, which is never tenant-named, so its absence says nothing about the prefix.
    // DEFINITIVE, because an empty resolve is the resolver answering rather than failing. The
    // repair tool deletes on this and declines on LOOKUP_FAILED, so the two must not be merged.
    LeaseRepairUtil.CollectionUuidResolver mvAbsent =
        (db, coll) ->
            db.equals(INTERNAL_SEARCH_DB) ? Optional.empty() : Optional.of(UUID.randomUUID());
    var mvAbsentFailure =
        assertThrows(
            LeaseRepairUtil.LeaseRepairException.class,
            () ->
                LeaseRepairUtil.repairLeaseForPostRestoreMigration(
                    stale, mvAbsent, INTERNAL_SEARCH_DB));
    assertEquals(LeaseRepairUtil.LeaseRepairException.Kind.DEFINITIVE, mvAbsentFailure.kind());

    // Namespace on the lease doesn't actually have a tenant prefix → LeaseRepairException from
    // stripTenantPrefix. Reached only if a caller invokes repair without checking
    // requiresPostRestoreRepair first, so this documents the contract.
    Lease unprefixed = leaseWithNamespaceDb("sample_mflix");
    var unprefixedFailure =
        assertThrows(
            LeaseRepairUtil.LeaseRepairException.class,
            () ->
                LeaseRepairUtil.repairLeaseForPostRestoreMigration(
                    unprefixed, anyUuid, INTERNAL_SEARCH_DB));
    assertEquals(LeaseRepairUtil.LeaseRepairException.Kind.DEFINITIVE, unprefixedFailure.kind());
  }

  @Test
  public void repairMvUuidOnly_rewritesMvUuidPreservesEverythingElse() {
    UUID freshMvUuid = UUID.randomUUID();
    Lease stale = leaseWithNamespaceDb("sample_mflix", new BsonTimestamp(2000, 3));

    Lease repaired = LeaseRepairUtil.repairMvUuidOnly(stale, freshMvUuid);

    assertEquals(freshMvUuid, repaired.materializedViewCollectionMetadata().collectionUuid());
    assertEquals(stale.commitInfo(), repaired.commitInfo());
    assertEquals(stale.collectionUuid(), repaired.collectionUuid());
    assertEquals(stale.steadyAsOfOplogPosition(), repaired.steadyAsOfOplogPosition());
    assertEquals(stale.leaseOwner(), repaired.leaseOwner());
    assertEquals(stale.leaseVersion(), repaired.leaseVersion());
    assertEquals(stale.id(), repaired.id());
    assertEquals(
        stale.materializedViewCollectionMetadata().collectionName(),
        repaired.materializedViewCollectionMetadata().collectionName());
    assertEquals(
        stale.materializedViewCollectionMetadata().schemaMetadata(),
        repaired.materializedViewCollectionMetadata().schemaMetadata());
    // Metadata repair is not an ownership transfer, so epoch must be preserved.
    assertEquals(stale.epoch(), repaired.epoch());
  }

  private static Lease leaseWithNamespaceDb(String dbName) {
    return leaseWithNamespaceDb(dbName, null);
  }

  private static Lease leaseWithNamespaceDb(String dbName, BsonTimestamp steadyPosition) {
    var resumeInfo =
        ChangeStreamResumeInfo.create(
            new MongoNamespace(dbName, COLLECTION_NAME),
            ChangeStreamUtils.resumeToken(new BsonTimestamp(1L)));
    var commitInfo =
        IndexCommitUserData.createChangeStreamResume(
                resumeInfo, IndexFormatVersion.CURRENT.versionNumber)
            .toEncodedData()
            .asString();
    return leaseWithRawCommitInfo(commitInfo, steadyPosition);
  }

  private static Lease leaseWithRawCommitInfo(String commitInfo) {
    return leaseWithRawCommitInfo(commitInfo, null);
  }

  private static Lease leaseWithRawCommitInfo(String commitInfo, BsonTimestamp steadyPosition) {
    return new Lease(
        "test-lease-id",
        Lease.SCHEMA_VERSION,
        COLLECTION_UUID,
        COLLECTION_NAME,
        "localhost",
        Instant.now(),
        1L,
        commitInfo,
        "1",
        Map.of(
            "1",
            new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.INITIAL_SYNC)),
        new MaterializedViewCollectionMetadata(
            VERSION_ZERO, UUID.fromString(COLLECTION_UUID), MV_COLLECTION_NAME),
        steadyPosition,
        Lease.CleanupState.NOT_ELIGIBLE,
        5L);
  }

  // Lease whose commitInfo namespace is the base collection while collectionName holds the view
  // leaf — the legacy view-backed shape the repair must canonicalize.
  private static Lease viewLeafLease(
      String prefixedDb, String baseCollection, String viewLeaf, String sourceUuid) {
    var resumeInfo =
        ChangeStreamResumeInfo.create(
            new MongoNamespace(prefixedDb, baseCollection),
            ChangeStreamUtils.resumeToken(new BsonTimestamp(1L)));
    var commitInfo =
        IndexCommitUserData.createChangeStreamResume(
                resumeInfo, IndexFormatVersion.CURRENT.versionNumber)
            .toEncodedData()
            .asString();
    return new Lease(
        "test-lease-id",
        Lease.SCHEMA_VERSION,
        sourceUuid,
        viewLeaf,
        "localhost",
        Instant.now(),
        1L,
        commitInfo,
        "1",
        Map.of("1", new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.STEADY)),
        new MaterializedViewCollectionMetadata(
            VERSION_ZERO, UUID.fromString(COLLECTION_UUID), MV_COLLECTION_NAME),
        new BsonTimestamp(1000, 1),
        Lease.CleanupState.NOT_ELIGIBLE,
        5L);
  }
}
