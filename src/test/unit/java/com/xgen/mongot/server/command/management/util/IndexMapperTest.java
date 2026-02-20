package com.xgen.mongot.server.command.management.util;

import static com.xgen.mongot.index.status.IndexStatus.StatusCode;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class IndexMapperTest {

  @Test
  public void testCalculateStatus_mainIndexPending_returnsMainStatus() {
    // When main index is PENDING, return main status regardless of staged status
    assertEquals(
        StatusCode.NOT_STARTED,
        IndexMapper.calculateStatus(StatusCode.NOT_STARTED, StatusCode.STEADY, true));
    assertEquals(
        StatusCode.NOT_STARTED,
        IndexMapper.calculateStatus(StatusCode.NOT_STARTED, StatusCode.FAILED, false));
    assertEquals(
        StatusCode.UNKNOWN,
        IndexMapper.calculateStatus(StatusCode.UNKNOWN, StatusCode.INITIAL_SYNC, true));
  }

  @Test
  public void testCalculateStatus_mainIndexBuilding_returnsMainStatus() {
    // When main index is BUILDING, return main status regardless of staged status
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.INITIAL_SYNC, StatusCode.STEADY, true));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.INITIAL_SYNC, StatusCode.FAILED, false));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.INITIAL_SYNC, StatusCode.DOES_NOT_EXIST, true));
  }

  @Test
  public void testCalculateStatus_mainDoesNotExist_stagedReady_returnsInitialSync() {
    // When main doesn't exist and staged is READY, return INITIAL_SYNC (temporary state before
    // swap)
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.DOES_NOT_EXIST, StatusCode.STEADY, true));
  }

  @Test
  public void testCalculateStatus_mainDoesNotExist_stagedStale_returnsInitialSync() {
    // When main doesn't exist and staged is STALE, return INITIAL_SYNC
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.DOES_NOT_EXIST, StatusCode.STALE, false));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(
            StatusCode.DOES_NOT_EXIST, StatusCode.RECOVERING_TRANSIENT, true));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(
            StatusCode.DOES_NOT_EXIST, StatusCode.RECOVERING_NON_TRANSIENT, false));
  }

  @Test
  public void testCalculateStatus_mainDoesNotExist_stagedOther_returnsStagedStatus() {
    // When main doesn't exist and staged is not READY/STALE, return staged status
    assertEquals(
        StatusCode.FAILED,
        IndexMapper.calculateStatus(StatusCode.DOES_NOT_EXIST, StatusCode.FAILED, true));
    assertEquals(
        StatusCode.NOT_STARTED,
        IndexMapper.calculateStatus(StatusCode.DOES_NOT_EXIST, StatusCode.NOT_STARTED, false));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.DOES_NOT_EXIST, StatusCode.INITIAL_SYNC, true));
  }

  @Test
  public void testCalculateStatus_mainReady_isLatestVersion_returnsMainStatus() {
    // When main is READY and is latest version, return main status
    assertEquals(
        StatusCode.STEADY,
        IndexMapper.calculateStatus(StatusCode.STEADY, StatusCode.DOES_NOT_EXIST, true));
    assertEquals(
        StatusCode.STEADY,
        IndexMapper.calculateStatus(StatusCode.STEADY, StatusCode.FAILED, true));
    assertEquals(
        StatusCode.STEADY,
        IndexMapper.calculateStatus(StatusCode.STEADY, StatusCode.INITIAL_SYNC, true));
  }

  @Test
  public void testCalculateStatus_mainReady_notLatestVersion_stagedFailed_returnsFailed() {
    // When main is READY but not latest version and staged is FAILED, return FAILED
    assertEquals(
        StatusCode.FAILED,
        IndexMapper.calculateStatus(StatusCode.STEADY, StatusCode.FAILED, false));
  }

  @Test
  public void testCalculateStatus_mainReady_notLatestVersion_stagedNotFailed_returnsInitialSync() {
    // When main is READY but not latest version and staged is not FAILED, return INITIAL_SYNC
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.STEADY, StatusCode.INITIAL_SYNC, false));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.STEADY, StatusCode.NOT_STARTED, false));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.STEADY, StatusCode.DOES_NOT_EXIST, false));
  }

  @Test
  public void testCalculateStatus_mainFailed_isLatestVersion_returnsMainStatus() {
    // When main is FAILED and is latest version, return main status
    assertEquals(
        StatusCode.FAILED,
        IndexMapper.calculateStatus(StatusCode.FAILED, StatusCode.DOES_NOT_EXIST, true));
    assertEquals(
        StatusCode.FAILED,
        IndexMapper.calculateStatus(StatusCode.FAILED, StatusCode.INITIAL_SYNC, true));
  }

  @Test
  public void testCalculateStatus_mainFailed_notLatestVersion_stagedFailed_returnsFailed() {
    // When main is FAILED and not latest version and staged is FAILED, return FAILED
    assertEquals(
        StatusCode.FAILED,
        IndexMapper.calculateStatus(StatusCode.FAILED, StatusCode.FAILED, false));
  }

  @Test
  public void testCalculateStatus_mainFailed_notLatestVersion_stagedNotFailed_returnsInitialSync() {
    // When main is FAILED and not latest version and staged is not FAILED, return INITIAL_SYNC
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.FAILED, StatusCode.INITIAL_SYNC, false));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.FAILED, StatusCode.NOT_STARTED, false));
  }

  @Test
  public void
      testCalculateStatus_mainStale_isLatestVersion_stagedBuildingOrReady_returnsInitialSync() {
    // When main is STALE and is latest version and staged is BUILDING or READY, return INITIAL_SYNC
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.STALE, StatusCode.INITIAL_SYNC, true));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.STALE, StatusCode.STEADY, true));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(
            StatusCode.RECOVERING_TRANSIENT, StatusCode.INITIAL_SYNC, true));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(
            StatusCode.RECOVERING_NON_TRANSIENT, StatusCode.STEADY, true));
  }

  @Test
  public void testCalculateStatus_mainStale_isLatestVersion_stagedOther_returnsMainStatus() {
    // When main is STALE and is latest version and staged is not BUILDING/READY, return main status
    assertEquals(
        StatusCode.STALE,
        IndexMapper.calculateStatus(StatusCode.STALE, StatusCode.DOES_NOT_EXIST, true));
    assertEquals(
        StatusCode.STALE,
        IndexMapper.calculateStatus(StatusCode.STALE, StatusCode.FAILED, true));
    assertEquals(
        StatusCode.RECOVERING_TRANSIENT,
        IndexMapper.calculateStatus(
            StatusCode.RECOVERING_TRANSIENT, StatusCode.DOES_NOT_EXIST, true));
    assertEquals(
        StatusCode.RECOVERING_NON_TRANSIENT,
        IndexMapper.calculateStatus(StatusCode.RECOVERING_NON_TRANSIENT, StatusCode.FAILED, true));
  }

  @Test
  public void testCalculateStatus_mainStale_notLatestVersion_stagedFailed_returnsMainStatus() {
    // When main is STALE and not latest version and staged is FAILED, return main status
    assertEquals(
        StatusCode.STALE,
        IndexMapper.calculateStatus(StatusCode.STALE, StatusCode.FAILED, false));
    assertEquals(
        StatusCode.RECOVERING_TRANSIENT,
        IndexMapper.calculateStatus(StatusCode.RECOVERING_TRANSIENT, StatusCode.FAILED, false));
    assertEquals(
        StatusCode.RECOVERING_NON_TRANSIENT,
        IndexMapper.calculateStatus(StatusCode.RECOVERING_NON_TRANSIENT, StatusCode.FAILED, false));
  }

  @Test
  public void testCalculateStatus_mainStale_notLatestVersion_stagedNotFailed_returnsInitialSync() {
    // When main is STALE and not latest version and staged is not FAILED, return INITIAL_SYNC
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.STALE, StatusCode.INITIAL_SYNC, false));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.STALE, StatusCode.STEADY, false));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.STALE, StatusCode.DOES_NOT_EXIST, false));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(
            StatusCode.RECOVERING_TRANSIENT, StatusCode.NOT_STARTED, false));
    assertEquals(
        StatusCode.INITIAL_SYNC,
        IndexMapper.calculateStatus(StatusCode.RECOVERING_NON_TRANSIENT, StatusCode.STALE, false));
  }

  @Test
  public void testCalculateStatus_allStatusCodesHandled_noIllegalStateException() {
    // This test validates that all possible StatusCode combinations are handled
    // and that the IllegalStateException at the end of calculateStatus is unreachable.
    // We test all combinations of mainIndexStatusCode, stagedIndexStatusCode, and
    // isMainIndexLatestVersion to ensure complete coverage.

    StatusCode[] allStatusCodes = StatusCode.values();
    boolean[] latestVersionFlags = {true, false};

    // Test all combinations - none should throw IllegalStateException
    for (StatusCode mainStatus : allStatusCodes) {
      for (StatusCode stagedStatus : allStatusCodes) {
        for (boolean isLatest : latestVersionFlags) {
          // This should not throw IllegalStateException for any valid StatusCode combination
          StatusCode result = IndexMapper.calculateStatus(mainStatus, stagedStatus, isLatest);
          // Verify we got a valid result (not null)
          assertEquals(
              String.format(
                  "Expected non-null result for main=%s, staged=%s, isLatest=%s",
                  mainStatus, stagedStatus, isLatest),
              true,
              result != null);
        }
      }
    }
  }

  @Test
  public void testToIndexPriority_statusCodeMappings() {
    // Verify all StatusCode to ExternalStatus mappings and their priorities
    // DOES_NOT_EXIST (0)
    assertEquals(0, IndexMapper.toIndexPriority(StatusCode.DOES_NOT_EXIST));

    // READY (1)
    assertEquals(1, IndexMapper.toIndexPriority(StatusCode.STEADY));

    // BUILDING (2)
    assertEquals(2, IndexMapper.toIndexPriority(StatusCode.INITIAL_SYNC));

    // PENDING (3)
    assertEquals(3, IndexMapper.toIndexPriority(StatusCode.NOT_STARTED));
    assertEquals(3, IndexMapper.toIndexPriority(StatusCode.UNKNOWN));

    // STALE (4)
    assertEquals(4, IndexMapper.toIndexPriority(StatusCode.STALE));
    assertEquals(4, IndexMapper.toIndexPriority(StatusCode.RECOVERING_TRANSIENT));
    assertEquals(4, IndexMapper.toIndexPriority(StatusCode.RECOVERING_NON_TRANSIENT));

    // FAILED (5)
    assertEquals(5, IndexMapper.toIndexPriority(StatusCode.FAILED));

    // Verify priority ordering: DOES_NOT_EXIST < READY < BUILDING < PENDING < STALE < FAILED
    assertEquals(
        true,
        IndexMapper.toIndexPriority(StatusCode.DOES_NOT_EXIST)
            < IndexMapper.toIndexPriority(StatusCode.STEADY));
    assertEquals(
        true,
        IndexMapper.toIndexPriority(StatusCode.STEADY)
            < IndexMapper.toIndexPriority(StatusCode.INITIAL_SYNC));
    assertEquals(
        true,
        IndexMapper.toIndexPriority(StatusCode.INITIAL_SYNC)
            < IndexMapper.toIndexPriority(StatusCode.NOT_STARTED));
    assertEquals(
        true,
        IndexMapper.toIndexPriority(StatusCode.NOT_STARTED)
            < IndexMapper.toIndexPriority(StatusCode.STALE));
    assertEquals(
        true,
        IndexMapper.toIndexPriority(StatusCode.STALE)
            < IndexMapper.toIndexPriority(StatusCode.FAILED));
  }

  @Test
  public void testToExternalStatus_statusCodeMappings() {
    // Verify all StatusCode to ExternalStatus name mappings
    // DOES_NOT_EXIST
    assertEquals("DOES_NOT_EXIST", IndexMapper.toExternalStatus(StatusCode.DOES_NOT_EXIST));

    // READY
    assertEquals("READY", IndexMapper.toExternalStatus(StatusCode.STEADY));

    // BUILDING
    assertEquals("BUILDING", IndexMapper.toExternalStatus(StatusCode.INITIAL_SYNC));

    // PENDING
    assertEquals("PENDING", IndexMapper.toExternalStatus(StatusCode.NOT_STARTED));
    assertEquals("PENDING", IndexMapper.toExternalStatus(StatusCode.UNKNOWN));

    // STALE
    assertEquals("STALE", IndexMapper.toExternalStatus(StatusCode.STALE));
    assertEquals("STALE", IndexMapper.toExternalStatus(StatusCode.RECOVERING_TRANSIENT));
    assertEquals("STALE", IndexMapper.toExternalStatus(StatusCode.RECOVERING_NON_TRANSIENT));

    // FAILED
    assertEquals("FAILED", IndexMapper.toExternalStatus(StatusCode.FAILED));
  }
}
