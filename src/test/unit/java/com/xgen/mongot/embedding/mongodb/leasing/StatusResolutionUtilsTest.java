package com.xgen.mongot.embedding.mongodb.leasing;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Test;

public class StatusResolutionUtilsTest {

  private static final String COLLECTION_NAME = "collection-name";
  private static final MaterializedViewGenerationId GENERATION_ID =
      new MaterializedViewGenerationId(
          new ObjectId(), new MaterializedViewGeneration(Generation.CURRENT));

  @Test
  public void testGetEffectiveMaterializedViewStatus() {
    // Same version pass-through
    assertEffectiveStatus(StatusCode.STEADY, true, StatusCode.STEADY, true, StatusCode.STEADY);
    assertEffectiveStatus(
        StatusCode.INITIAL_SYNC, false, StatusCode.INITIAL_SYNC, false, StatusCode.INITIAL_SYNC);
    // Same version, INITIAL_SYNC but previously queryable (e.g. fell off the oplog) ->
    // RECOVERING_TRANSIENT
    assertEffectiveStatus(
        StatusCode.INITIAL_SYNC,
        true,
        StatusCode.INITIAL_SYNC,
        true,
        StatusCode.RECOVERING_TRANSIENT);

    // V1 terminal states always take precedence (regardless of V2)
    assertEffectiveStatus(StatusCode.FAILED, false, StatusCode.STEADY, true, StatusCode.FAILED);
    assertEffectiveStatus(
        StatusCode.FAILED, false, StatusCode.INITIAL_SYNC, false, StatusCode.FAILED);
    assertEffectiveStatus(StatusCode.STALE, true, StatusCode.STEADY, true, StatusCode.STALE);
    assertEffectiveStatus(StatusCode.STALE, true, StatusCode.INITIAL_SYNC, false, StatusCode.STALE);
    assertEffectiveStatus(
        StatusCode.DOES_NOT_EXIST, false, StatusCode.STEADY, true, StatusCode.DOES_NOT_EXIST);
    assertEffectiveStatus(
        StatusCode.DOES_NOT_EXIST,
        false,
        StatusCode.INITIAL_SYNC,
        false,
        StatusCode.DOES_NOT_EXIST);

    // V2 is STEADY -> switch to V2 (latest is ready)
    assertEffectiveStatus(
        StatusCode.INITIAL_SYNC, false, StatusCode.STEADY, true, StatusCode.STEADY);
    assertEffectiveStatus(
        StatusCode.NOT_STARTED, false, StatusCode.STEADY, true, StatusCode.STEADY);
    assertEffectiveStatus(StatusCode.UNKNOWN, false, StatusCode.STEADY, true, StatusCode.STEADY);
    assertEffectiveStatus(
        StatusCode.RECOVERING_TRANSIENT, true, StatusCode.STEADY, true, StatusCode.STEADY);

    // V2 is INITIAL_SYNC -> use V1 (old version serves while new one builds)
    assertEffectiveStatus(
        StatusCode.STEADY, true, StatusCode.INITIAL_SYNC, false, StatusCode.STEADY);

    // V2 hasn't started building (UNKNOWN, NOT_STARTED) -> RECOVERING_TRANSIENT
    assertEffectiveStatus(
        StatusCode.STEADY, true, StatusCode.UNKNOWN, false, StatusCode.RECOVERING_TRANSIENT);
    assertEffectiveStatus(
        StatusCode.STEADY, true, StatusCode.NOT_STARTED, false, StatusCode.RECOVERING_TRANSIENT);
    assertEffectiveStatus(
        StatusCode.INITIAL_SYNC, false, StatusCode.UNKNOWN, false, StatusCode.RECOVERING_TRANSIENT);

    // V2 has progressed past building -> use V2's status
    assertEffectiveStatus(StatusCode.STEADY, true, StatusCode.FAILED, false, StatusCode.FAILED);
    assertEffectiveStatus(StatusCode.STEADY, true, StatusCode.STALE, true, StatusCode.STALE);
    assertEffectiveStatus(
        StatusCode.STEADY,
        true,
        StatusCode.RECOVERING_TRANSIENT,
        true,
        StatusCode.RECOVERING_TRANSIENT);
    assertEffectiveStatus(
        StatusCode.INITIAL_SYNC, false, StatusCode.FAILED, false, StatusCode.FAILED);
  }

  /**
   * A version key the lease has not published yet must not inherit the previous version's status:
   * that reports STEADY for an empty materialized view (HELP-97792).
   */
  @Test
  public void getEffectiveMaterializedViewStatus_versionKeyAbsentFromLease_returnsUnknown() {
    Lease lease =
        createLease(
            "0", Map.of("0", new Lease.IndexDefinitionVersionStatus(true, StatusCode.STEADY)));

    IndexStatus result =
        StatusResolutionUtils.getEffectiveMaterializedViewStatus(lease, "1", GENERATION_ID);

    assertEquals(StatusCode.UNKNOWN, result.getStatusCode());
  }

  /**
   * Counterpart to the test above, pinning the case that is deliberately left alone: a version key
   * that <em>is</em> in the lease holding UNKNOWN still resolves to a STEADY latest version. That
   * is a completed definition update whose new version is live, not a lease-publication race, and
   * the two are only distinguishable by whether the key is present.
   */
  @Test
  public void getEffectiveMaterializedViewStatus_versionKeyPresentAsUnknown_returnsLatestSteady() {
    Lease lease =
        createLease(
            "1",
            Map.of(
                "0",
                new Lease.IndexDefinitionVersionStatus(false, StatusCode.UNKNOWN),
                "1",
                new Lease.IndexDefinitionVersionStatus(true, StatusCode.STEADY)));

    IndexStatus result =
        StatusResolutionUtils.getEffectiveMaterializedViewStatus(lease, "0", GENERATION_ID);

    assertEquals(StatusCode.STEADY, result.getStatusCode());
  }

  @Test
  public void getEffectiveMaterializedViewStatus_versionKeyPresentAsInitialSync_passesThrough() {
    Lease lease =
        createLease(
            "1",
            Map.of("1", new Lease.IndexDefinitionVersionStatus(false, StatusCode.INITIAL_SYNC)));

    IndexStatus result =
        StatusResolutionUtils.getEffectiveMaterializedViewStatus(lease, "1", GENERATION_ID);

    assertEquals(StatusCode.INITIAL_SYNC, result.getStatusCode());
  }

  /**
   * A lease whose {@code latestIndexDefinitionVersion} is not a key in its own status map must not
   * NPE. The follower poll loop has no per-generation guard and {@code refreshStatus} runs as a
   * {@code VerboseRunnable} under {@code scheduleWithFixedDelay}, which rethrows, so an exception
   * here would cancel status refresh and leadership acquisition for the whole process.
   */
  @Test
  public void getEffectiveMaterializedViewStatus_latestVersionKeyMissingFromMap_doesNotThrow() {
    Lease malformedLease =
        createLease(
            "2", Map.of("1", new Lease.IndexDefinitionVersionStatus(true, StatusCode.STEADY)));

    IndexStatus result =
        StatusResolutionUtils.getEffectiveMaterializedViewStatus(
            malformedLease, "1", GENERATION_ID);

    // Same answer as a lease whose latest version legitimately holds the not-yet-built placeholder:
    // the requested version is still queryable, the newer one has not started.
    assertEquals(StatusCode.RECOVERING_TRANSIENT, result.getStatusCode());
  }

  private static Lease createLease(
      String latestVersion, Map<String, Lease.IndexDefinitionVersionStatus> versionStatusMap) {
    return new Lease(
        COLLECTION_NAME,
        Lease.SCHEMA_VERSION,
        UUID.randomUUID().toString(),
        COLLECTION_NAME,
        "owner",
        Instant.now().plusSeconds(60),
        1L,
        "",
        latestVersion,
        versionStatusMap,
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
            UUID.randomUUID(),
            COLLECTION_NAME),
        null,
        Lease.CleanupState.NOT_ELIGIBLE);
  }

  private void assertEffectiveStatus(
      StatusCode v1Status,
      boolean v1Queryable,
      StatusCode v2Status,
      boolean v2Queryable,
      StatusCode expected) {
    Lease.IndexDefinitionVersionStatus v1 =
        new Lease.IndexDefinitionVersionStatus(v1Queryable, v1Status);
    Lease.IndexDefinitionVersionStatus v2 =
        new Lease.IndexDefinitionVersionStatus(v2Queryable, v2Status);

    IndexStatus result = StatusResolutionUtils.getEffectiveMaterializedViewStatus(v1, v2);

    assertEquals(
        String.format(
            "V1=%s(queryable=%s), V2=%s(queryable=%s) should yield %s",
            v1Status, v1Queryable, v2Status, v2Queryable, expected),
        expected,
        result.getStatusCode());
  }
}
