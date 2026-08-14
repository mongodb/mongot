package com.xgen.mongot.index.status;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import org.bson.BsonTimestamp;
import org.junit.Test;

/**
 * Unit tests for the in-memory behavior of {@link IndexStatus}: its factory methods, the
 * reason/statusCode validation performed in its constructors, and its derived accessors. BSON
 * serialization now lives on {@code IndexStatusP} and is covered by {@code IndexStatusPTest}.
 */
public class IndexStatusTest {

  @Test
  public void basicConstructor_setsStatusCodeAndLeavesRestEmpty() {
    IndexStatus status = new IndexStatus(IndexStatus.StatusCode.STEADY);

    assertEquals(IndexStatus.StatusCode.STEADY, status.getStatusCode());
    assertTrue(status.getMessage().isEmpty());
    assertTrue(status.getOptime().isEmpty());
    assertTrue(status.getReason().isEmpty());
  }

  @Test
  public void factories_populateExpectedFields() {
    assertTrue(IndexStatus.unknown().isUnknown());
    assertEquals(IndexStatus.StatusCode.NOT_STARTED, IndexStatus.notStarted().getStatusCode());
    assertEquals(IndexStatus.StatusCode.INITIAL_SYNC, IndexStatus.initialSync().getStatusCode());
    assertEquals(IndexStatus.StatusCode.STEADY, IndexStatus.steady().getStatusCode());

    IndexStatus initialSyncWithMessage = IndexStatus.initialSync("syncing");
    assertEquals(IndexStatus.StatusCode.INITIAL_SYNC, initialSyncWithMessage.getStatusCode());
    assertEquals(Optional.of("syncing"), initialSyncWithMessage.getMessage());

    BsonTimestamp optime = new BsonTimestamp(1000);
    IndexStatus stale = IndexStatus.stale("stale reason", optime);
    assertEquals(IndexStatus.StatusCode.STALE, stale.getStatusCode());
    assertEquals(Optional.of("stale reason"), stale.getMessage());
    assertEquals(Optional.of(optime), stale.getOptime());

    IndexStatus recoveringNonTransient = IndexStatus.recoveringNonTransient(optime);
    assertEquals(
        IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT, recoveringNonTransient.getStatusCode());
    assertEquals(Optional.of(optime), recoveringNonTransient.getOptime());

    IndexStatus recoveringTransient = IndexStatus.recoveringTransient("retrying");
    assertEquals(IndexStatus.StatusCode.RECOVERING_TRANSIENT, recoveringTransient.getStatusCode());
    assertEquals(Optional.of("retrying"), recoveringTransient.getMessage());
  }

  @Test
  public void failedFactories_setStatusAndReason() {
    IndexStatus failed = IndexStatus.failed("boom");
    assertEquals(IndexStatus.StatusCode.FAILED, failed.getStatusCode());
    assertEquals(Optional.of("boom"), failed.getMessage());
    assertTrue(failed.getReason().isEmpty());

    IndexStatus failedWithReason = IndexStatus.failed("boom", IndexStatus.Reason.EXCEED_MAX_LIMIT);
    assertEquals(IndexStatus.StatusCode.FAILED, failedWithReason.getStatusCode());
    assertEquals(Optional.of(IndexStatus.Reason.EXCEED_MAX_LIMIT), failedWithReason.getReason());
  }

  @Test
  public void doesNotExist_setsReasonAndFlags() {
    IndexStatus dropped = IndexStatus.doesNotExist(IndexStatus.Reason.INDEX_DROPPED);
    assertEquals(IndexStatus.StatusCode.DOES_NOT_EXIST, dropped.getStatusCode());
    assertEquals(Optional.of(IndexStatus.Reason.INDEX_DROPPED), dropped.getReason());
    assertTrue(dropped.isIndexDropped());
    assertFalse(dropped.isCollectionNotFound());

    IndexStatus collectionNotFound =
        IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND);
    assertTrue(collectionNotFound.isCollectionNotFound());
    assertFalse(collectionNotFound.isIndexDropped());
  }

  @Test
  public void isIndexDropped_isTrueForDoesNotExistWithoutReason() {
    IndexStatus status =
        IndexStatus.fromPersisted(
            IndexStatus.StatusCode.DOES_NOT_EXIST,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    assertTrue(status.isIndexDropped());
  }

  @Test
  public void canServiceQueries_trueOnlyForServiceableStatuses() {
    assertTrue(IndexStatus.steady().canServiceQueries());
    assertTrue(IndexStatus.stale("s", new BsonTimestamp(1)).canServiceQueries());
    assertTrue(IndexStatus.recoveringTransient("r").canServiceQueries());
    assertTrue(IndexStatus.recoveringNonTransient(new BsonTimestamp(1)).canServiceQueries());

    assertFalse(IndexStatus.unknown().canServiceQueries());
    assertFalse(IndexStatus.notStarted().canServiceQueries());
    assertFalse(IndexStatus.initialSync().canServiceQueries());
    assertFalse(IndexStatus.failed("f").canServiceQueries());
    assertFalse(IndexStatus.doesNotExist(IndexStatus.Reason.INDEX_DROPPED).canServiceQueries());
  }

  @Test
  public void canBeRecovered_trueOnlyForRecoverableReasons() {
    assertTrue(IndexStatus.failed("f", IndexStatus.Reason.INITIALIZATION_FAILED).canBeRecovered());
    assertTrue(
        IndexStatus.fromIndexStateInfo(
                IndexStatus.StatusCode.FAILED, IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY)
            .canBeRecovered());

    assertFalse(IndexStatus.failed("f", IndexStatus.Reason.EXCEED_MAX_LIMIT).canBeRecovered());
    assertFalse(IndexStatus.steady().canBeRecovered());
  }

  @Test
  public void constructor_acceptsValidReasonForStatus() {
    // FAILED-scoped reasons.
    IndexStatus.fromIndexStateInfo(
        IndexStatus.StatusCode.FAILED, IndexStatus.Reason.INITIALIZATION_FAILED);
    // DOES_NOT_EXIST-scoped reasons.
    IndexStatus.fromIndexStateInfo(
        IndexStatus.StatusCode.DOES_NOT_EXIST, IndexStatus.Reason.COLLECTION_NOT_FOUND);
    IndexStatus.fromIndexStateInfo(
        IndexStatus.StatusCode.DOES_NOT_EXIST, IndexStatus.Reason.INDEX_DROPPED);
    // USER_ERROR is unconstrained and valid for any status code.
    IndexStatus.fromIndexStateInfo(IndexStatus.StatusCode.STEADY, IndexStatus.Reason.USER_ERROR);
  }

  @Test
  public void constructor_rejectsReasonThatRequiresFailedStatus() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IndexStatus.fromIndexStateInfo(
                IndexStatus.StatusCode.STEADY, IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED));
  }

  @Test
  public void constructor_rejectsReasonThatRequiresDoesNotExistStatus() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IndexStatus.fromIndexStateInfo(
                IndexStatus.StatusCode.INITIAL_SYNC, IndexStatus.Reason.INDEX_DROPPED));
  }

  @Test
  public void failedFactory_rejectsReasonScopedToDoesNotExist() {
    assertThrows(
        IllegalArgumentException.class,
        () -> IndexStatus.failed("boom", IndexStatus.Reason.COLLECTION_NOT_FOUND));
  }

  @Test
  public void fromPersisted_populatesFieldsForValidCombination() {
    BsonTimestamp optime = new BsonTimestamp(1000);
    IndexStatus status =
        IndexStatus.fromPersisted(
            IndexStatus.StatusCode.FAILED,
            Optional.of("boom"),
            Optional.of(optime),
            Optional.of(IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED));

    assertEquals(IndexStatus.StatusCode.FAILED, status.getStatusCode());
    assertEquals(Optional.of("boom"), status.getMessage());
    assertEquals(Optional.of(optime), status.getOptime());
    assertEquals(
        Optional.of(IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED), status.getReason());
  }

  @Test
  public void fromPersisted_rejectsMismatchedReasonAndStatusCode() {
    // fromPersisted goes through the same validating constructor as the other factories, so a
    // mismatched (statusCode, reason) combination is rejected here too.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IndexStatus.fromPersisted(
                IndexStatus.StatusCode.UNKNOWN,
                Optional.empty(),
                Optional.empty(),
                Optional.of(IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED)));
  }

  @Test
  public void equalsAndHashCode_dependOnAllFields() {
    assertEquals(IndexStatus.steady(), IndexStatus.steady());
    assertEquals(IndexStatus.steady().hashCode(), IndexStatus.steady().hashCode());

    assertEquals(IndexStatus.failed("same"), IndexStatus.failed("same"));
    assertFalse(IndexStatus.failed("a").equals(IndexStatus.failed("b")));
    assertFalse(IndexStatus.steady().equals(IndexStatus.initialSync()));
  }
}
