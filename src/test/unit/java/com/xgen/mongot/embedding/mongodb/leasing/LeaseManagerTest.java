package com.xgen.mongot.embedding.mongodb.leasing;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

/**
 * Unit tests for the shared {@link LeaseManager#currentVersionLeaseKeys(java.util.Collection)}
 * helper — the grouping/highest-version logic behind the GC safeguard. The per-implementation
 * database scan is exercised in {@code DynamicLeaderLeaseManagerTest} and {@code
 * StaticLeaderLeaseManagerTest}.
 */
public class LeaseManagerTest {

  /**
   * A lease whose id is {@code <indexId>-<rest>} (or just {@code <indexId>} for the v0 naming), so
   * {@link Lease#indexId()} groups it correctly, with the given latest index definition version.
   */
  private static Lease lease(String id, String latestIndexDefinitionVersion) {
    return new Lease(
        id,
        Lease.SCHEMA_VERSION,
        UUID.randomUUID().toString(),
        "source-collection",
        "owner",
        Instant.now(),
        1L,
        "",
        latestIndexDefinitionVersion,
        Map.of(),
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
            UUID.randomUUID(),
            id),
        null,
        Lease.CleanupState.NOT_ELIGIBLE);
  }

  @Test
  public void currentVersionLeaseKeys_empty_returnsEmpty() {
    assertThat(LeaseManager.currentVersionLeaseKeys(List.of())).isEmpty();
  }

  @Test
  public void currentVersionLeaseKeys_singleLease_returnsIt() {
    Lease only = lease("idxA-h1-1-0", "1");
    assertThat(LeaseManager.currentVersionLeaseKeys(List.of(only))).containsExactly("idxA-h1-1-0");
  }

  @Test
  public void currentVersionLeaseKeys_multipleVersionsSameIndex_returnsHighest() {
    // Numeric, not lexicographic: "10" > "9" even though "9" sorts after "10" as a string.
    Lease older = lease("idxA-h1-1-0", "9");
    Lease current = lease("idxA-h2-1-1", "10");

    assertThat(LeaseManager.currentVersionLeaseKeys(List.of(older, current)))
        .containsExactly("idxA-h2-1-1");
  }

  @Test
  public void currentVersionLeaseKeys_multipleIndexes_returnsHighestPerIndex() {
    Lease indexAOlder = lease("idxA-h1-1-0", "1");
    Lease indexACurrent = lease("idxA-h2-1-1", "2");
    Lease indexBOnly = lease("idxB-h1-1-0", "5");

    assertThat(
            LeaseManager.currentVersionLeaseKeys(List.of(indexAOlder, indexACurrent, indexBOnly)))
        .containsExactly("idxA-h2-1-1", "idxB-h1-1-0");
  }

  @Test
  public void currentVersionLeaseKeys_versionTie_returnsAllTied() {
    // Two leases of the same index at the same highest version: protect both (conservative).
    Lease tieA = lease("idxA-h1-1-0", "2");
    Lease tieB = lease("idxA-h2-1-1", "2");

    assertThat(LeaseManager.currentVersionLeaseKeys(List.of(tieA, tieB)))
        .containsExactly("idxA-h1-1-0", "idxA-h2-1-1");
  }

  @Test
  public void currentVersionLeaseKeys_unparseableVersion_protectsWholeIndex() {
    // A non-numeric version means the current version can't be determined; protect every lease of
    // that index rather than risk cleaning up the live one.
    Lease parseable = lease("idxA-h1-1-0", "1");
    Lease bogus = lease("idxA-h2-1-1", "not-a-number");

    assertThat(LeaseManager.currentVersionLeaseKeys(List.of(parseable, bogus)))
        .containsExactly("idxA-h1-1-0", "idxA-h2-1-1");
  }

  @Test
  public void currentVersionLeaseKeys_nullVersion_protectsWholeIndex() {
    // A null version is treated like an unparseable one: the current version can't be determined,
    // so protect every lease of that index rather than risk cleaning up the live one.
    Lease parseable = lease("idxA-h1-1-0", "1");
    Lease nullVersion = lease("idxA-h2-1-1", null);

    assertThat(LeaseManager.currentVersionLeaseKeys(List.of(parseable, nullVersion)))
        .containsExactly("idxA-h1-1-0", "idxA-h2-1-1");
  }

  @Test
  public void currentVersionLeaseKeys_v0NameWithoutDelimiter_isItsOwnIndex() {
    // v0 MV collection names are just the indexId (no '-'); each is its own index group.
    Lease v0 = lease("plainindexid", "1");
    Lease v1 = lease("idxB-h1-1-0", "1");

    Set<String> result = LeaseManager.currentVersionLeaseKeys(List.of(v0, v1));

    assertThat(result).containsExactly("plainindexid", "idxB-h1-1-0");
  }
}
