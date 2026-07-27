package com.xgen.mongot.embedding.mongodb.leasing;

import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.bson.BsonTimestamp;
import org.junit.Test;

/** Unit tests for {@link LeaseRepairUtil}. */
public class LeaseRepairUtilTest {

  private static final String TENANT_ID = "647343c92726405e854130e4"; // 24 hex chars
  private static final String COLLECTION_UUID = "550e8400-e29b-41d4-a716-446655440000";
  private static final String COLLECTION_NAME = "test-collection";
  private static final String MV_COLLECTION_NAME = "test-mv-collection";

  @Test
  public void requiresPostRestoreRepair_detectsFlexOriginLeaseOnDedicatedClusterOnly() {
    // dedicated + tenant-prefixed namespace → repair (the migration artifact)
    Lease prefixedOnDedicated = leaseWithNamespaceDb(TENANT_ID + "_sample_mflix");
    assertTrue(LeaseRepairUtil.requiresPostRestoreRepair(prefixedOnDedicated, true));

    // dedicated + un-prefixed namespace → no repair
    assertFalse(
        LeaseRepairUtil.requiresPostRestoreRepair(leaseWithNamespaceDb("sample_mflix"), true));

    // flex cluster → never repair
    assertFalse(LeaseRepairUtil.requiresPostRestoreRepair(prefixedOnDedicated, false));

    // customer-named db with underscore that isn't tenant-shaped → no repair
    assertFalse(LeaseRepairUtil.requiresPostRestoreRepair(leaseWithNamespaceDb("my_app_db"), true));

    // 24 non-hex chars + underscore → no repair (prefix must be hex).
    assertFalse(
        LeaseRepairUtil.requiresPostRestoreRepair(
            leaseWithNamespaceDb("zzzzzzzzzzzzzzzzzzzzzzzz_db"), true));

    // 24 hex chars but no trailing underscore → no repair (regex requires the underscore)
    assertFalse(LeaseRepairUtil.requiresPostRestoreRepair(leaseWithNamespaceDb(TENANT_ID), true));

    // empty commitInfo → no repair from this helper. The empty-commitInfo migration case is
    // caught by DynamicLeaderLeaseManager.initializeLease instead (via proposedMetadata's fresh
    // MV UUID) — avoids the per-poll-tick listCollections overhead.
    assertFalse(
        LeaseRepairUtil.requiresPostRestoreRepair(
            leaseWithRawCommitInfo(EncodedUserData.EMPTY.asString()), true));
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
                return freshSourceUuid;
              }
              if (db.equals(INTERNAL_SEARCH_DB) && coll.equals(MV_COLLECTION_NAME)) {
                return freshMvUuid;
              }
              throw new AssertionError("unexpected resolver call: " + db + "." + coll);
            },
            INTERNAL_SEARCH_DB);

    // stale fields rewritten
    assertEquals(EncodedUserData.EMPTY.asString(), repaired.commitInfo());
    assertNull(repaired.steadyAsOfOplogPosition());
    assertEquals(freshSourceUuid.toString(), repaired.collectionUuid());
    assertEquals(freshMvUuid, repaired.materializedViewCollectionMetadata().collectionUuid());

    // preserved: identity + MV schema + ownership (these came from withRenewedOwnership upstream)
    assertEquals(stale.id(), repaired.id());
    assertEquals(stale.collectionName(), repaired.collectionName());
    assertEquals(
        MV_COLLECTION_NAME, repaired.materializedViewCollectionMetadata().collectionName());
    assertEquals(
        stale.materializedViewCollectionMetadata().schemaMetadata(),
        repaired.materializedViewCollectionMetadata().schemaMetadata());
    assertEquals(stale.leaseOwner(), repaired.leaseOwner());
    assertEquals(stale.leaseVersion(), repaired.leaseVersion());
  }

  @Test
  public void repairLeaseForPostRestoreMigration_allUuidsCurrent_returnsLeaseUnchanged()
      throws Exception {
    // Both source and MV UUIDs already match — prefix-regex hit was a false positive
    // (legitimately-named customer db). No-op via reference equality.
    Lease stale = leaseWithNamespaceDb(TENANT_ID + "_sample_mflix");
    UUID leaseSourceUuid = UUID.fromString(stale.collectionUuid());
    UUID leaseMvUuid = stale.materializedViewCollectionMetadata().collectionUuid();

    Lease result =
        LeaseRepairUtil.repairLeaseForPostRestoreMigration(
            stale,
            (db, coll) -> db.equals(INTERNAL_SEARCH_DB) ? leaseMvUuid : leaseSourceUuid,
            INTERNAL_SEARCH_DB);

    assertSame(stale, result);
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
                return freshRootUuid;
              }
              if (db.equals(INTERNAL_SEARCH_DB) && coll.equals(MV_COLLECTION_NAME)) {
                return freshMvUuid;
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
            (db, coll) -> db.equals(INTERNAL_SEARCH_DB) ? currentMvUuid : currentSourceUuid,
            INTERNAL_SEARCH_DB);

    assertNotSame(stale, repaired);
    assertEquals("movies", repaired.collectionName());
    assertEquals(stale.collectionUuid(), repaired.collectionUuid());
  }

  @Test
  public void repairLeaseForPostRestoreMigration_failureCases() {
    Lease stale = leaseWithNamespaceDb(TENANT_ID + "_sample_mflix");

    // Lease with no resume info in commitInfo → LeaseRepairException (nothing to derive source db)
    Lease noResume = leaseWithRawCommitInfo(EncodedUserData.EMPTY.asString());
    assertThrows(
        LeaseRepairUtil.LeaseRepairException.class,
        () ->
            LeaseRepairUtil.repairLeaseForPostRestoreMigration(
                noResume, (db, coll) -> UUID.randomUUID(), INTERNAL_SEARCH_DB));

    // Source-side lookup fails (called first) → LeaseRepairException propagates
    LeaseRepairUtil.CollectionUuidResolver sourceFailing =
        (db, coll) -> {
          if (db.equals(INTERNAL_SEARCH_DB)) {
            return UUID.randomUUID();
          }
          throw new LeaseRepairUtil.LeaseRepairException("source lookup failed");
        };
    assertThrows(
        LeaseRepairUtil.LeaseRepairException.class,
        () ->
            LeaseRepairUtil.repairLeaseForPostRestoreMigration(
                stale, sourceFailing, INTERNAL_SEARCH_DB));

    // Namespace on the lease doesn't actually have a tenant prefix → LeaseRepairException from
    // stripTenantPrefix. Reached only if a caller invokes repair without checking
    // requiresPostRestoreRepair first, so this documents the contract.
    Lease unprefixed = leaseWithNamespaceDb("sample_mflix");
    assertThrows(
        LeaseRepairUtil.LeaseRepairException.class,
        () ->
            LeaseRepairUtil.repairLeaseForPostRestoreMigration(
                unprefixed, (db, coll) -> UUID.randomUUID(), INTERNAL_SEARCH_DB));
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
        IndexCommitUserData.createChangeStreamResume(resumeInfo, IndexFormatVersion.CURRENT)
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
        Lease.CleanupState.NOT_ELIGIBLE);
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
        IndexCommitUserData.createChangeStreamResume(resumeInfo, IndexFormatVersion.CURRENT)
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
        Lease.CleanupState.NOT_ELIGIBLE);
  }
}
