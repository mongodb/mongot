package com.xgen.mongot.replication.mongodb.autoembedding;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.embedding.mongodb.leasing.Lease;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leaderless garbage collector for stale materialized-view leases. Each sweep marks leases whose
 * heartbeat lapsed past the mark grace {@link Lease.CleanupState#ELIGIBLE_FOR_CLEANUP}, then drops
 * the materialized view collection and lease document of those already marked and past the delete
 * grace. Idempotent and safe to run on every mongot concurrently.
 *
 * <p>The owning {@link MaterializedViewManager} schedules {@link #scanStaleLeasesForCleanup()} and
 * supplies the pieces that live on the manager: whether a sweep may run at all, whether a local
 * generation still reads a lease's materialized view (evaluated under the manager's lock), and the
 * collection-drop routine shared with the index-deletion path.
 */
class StaleLeaseGarbageCollector {

  private static final Logger LOG = LoggerFactory.getLogger(StaleLeaseGarbageCollector.class);

  static final String MAT_VIEW_GC_LEASES_OVERDUE_GAUGE = "matViewGcLeasesOverdue";
  static final String MAT_VIEW_GC_LEASE_IN_USE_REFUSAL_COUNTER_NAME = "matViewGcLeaseInUseRefusals";
  static final String MAT_VIEW_GC_SWEEP_FAILURE_COUNTER_NAME = "matViewGcSweepFailures";
  static final String MAT_VIEW_GC_MARKED_CURRENT_REFUSAL_COUNTER_NAME =
      "matViewGcMarkedCurrentRefusals";
  static final String MAT_VIEW_GC_DELETED_INDEX_LEASE_COUNTER_NAME = "matViewGcDeletedIndexLeases";

  private final LeaseManager leaseManager;

  // The scan interval and the mark/delete grace periods are configurable via
  // AutoEmbeddingMaterializedViewConfig (staleLeaseScanIntervalMs / staleLeaseMarkGracePeriodMs /
  // staleLeaseDeleteGracePeriodMs), set per cluster through the conf call.
  private final AutoEmbeddingMaterializedViewConfig materializedViewConfig;

  /**
   * Gates the sweep at runtime. The scanner thread is always scheduled; each tick evaluates {@link
   * DynamicFeatureFlags#MV_GC_ENABLED} (cluster-invariant) and does nothing while the flag is off,
   * so cleanup can be enabled or disabled per cluster without a restart.
   */
  private final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;

  private final IndexCatalog indexCatalog;
  private final MeterRegistry meterRegistry;

  /**
   * Single aggregate gauge: how many leases are <em>overdue</em> — marked {@link
   * Lease.CleanupState#ELIGIBLE_FOR_CLEANUP}, already past the delete grace, and the sweep failed
   * to clean them. 0 in healthy operation (an overdue lease is deleted the sweep it is found, even
   * in a churn burst), so a sustained non-zero value means cleanup is genuinely blocked for some
   * lease. Deliberately not per-lease: identifying which lease is a log lookup
   * (mark/drop-failure/cleanup all log {@code leaseKey}), keeping this a single low-cardinality
   * series with no per-lease lifecycle. Registered once at construction (held so updates stick);
   * set each sweep, reset to 0 when GC is off.
   */
  private final AtomicLong gcLeasesOverdueGauge;

  /** False while the owning manager is shut down or replication is unsupported. */
  private final BooleanSupplier sweepAllowed;

  /** True if a generation on this mongot still reads the lease's materialized view. */
  private final Predicate<Lease> usedByLocalGeneration;

  /**
   * Drops the materialized view collection at a namespace on behalf of a lease key, returning true
   * iff the collection is gone. Shared with the index-deletion drop path, so it lives on the owner.
   */
  private final BiPredicate<String, MongoNamespace> dropMatViewCollection;

  StaleLeaseGarbageCollector(
      LeaseManager leaseManager,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      IndexCatalog indexCatalog,
      MeterRegistry meterRegistry,
      MetricsFactory metricsFactory,
      BooleanSupplier sweepAllowed,
      Predicate<Lease> usedByLocalGeneration,
      BiPredicate<String, MongoNamespace> dropMatViewCollection) {
    this.leaseManager = leaseManager;
    this.materializedViewConfig = materializedViewConfig;
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
    this.indexCatalog = indexCatalog;
    this.meterRegistry = meterRegistry;
    // The overdue gauge (matViewGcLeasesOverdue) is registered once here so the series is always
    // present (0 when GC is off or healthy) and pushed updates stick.
    this.gcLeasesOverdueGauge = metricsFactory.numGauge(MAT_VIEW_GC_LEASES_OVERDUE_GAUGE);
    this.sweepAllowed = sweepAllowed;
    this.usedByLocalGeneration = usedByLocalGeneration;
    this.dropMatViewCollection = dropMatViewCollection;
  }

  /**
   * Initial delay before the first GC sweep, so a just-restarted mongot re-establishes replication
   * and lease renewal before GC trusts staleness. At least one mark grace, scan interval as floor.
   */
  @VisibleForTesting
  static long firstScanDelayMs(AutoEmbeddingMaterializedViewConfig config) {
    return Math.max(config.staleLeaseScanIntervalMs, config.staleLeaseMarkGracePeriodMs);
  }

  /**
   * Periodic stale-lease GC sweep. Pass 1 marks leases stale past the mark grace {@link
   * Lease.CleanupState#ELIGIBLE_FOR_CLEANUP}; pass 2 drops the collection and lease for those
   * already marked and stale past the delete grace, so mark and cleanup span at least two sweeps.
   * Leaderless and idempotent — safe to run on every mongot concurrently. Safeguards: never act on
   * an index's current (highest) version while the index is still configured — a deleted index has
   * no current version, so all of its leases are cleanup targets — nor on a lease whose MV a local
   * generation still uses; the deletion phase adds OCC re-reads on top.
   */
  @VisibleForTesting
  void scanStaleLeasesForCleanup() {
    if (!this.sweepAllowed.getAsBoolean()) {
      setOverdueGauge(0);
      return;
    }
    // Contain all failures to this tick: VerboseRunnable rethrows after logging, and
    // scheduleWithFixedDelay cancels the task permanently on a throw. The flag evaluation is inside
    // the try too, so a transient registry/config error can't escape and cancel the scanner for the
    // life of the process.
    try {
      // Runtime kill switch: the scanner thread is always scheduled, but the sweep does nothing
      // unless GC is enabled for this cluster. Evaluated per tick so the flag takes effect without
      // a
      // restart.
      if (!this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
          DynamicFeatureFlags.MV_GC_ENABLED)) {
        // GC disabled for this cluster — reset the overdue gauge so it doesn't hold a stale value
        // (it is push-based and would otherwise keep its last count while GC is off).
        setOverdueGauge(0);
        return;
      }
      // One sweep-start instant drives both graces, so the delete grace is measured from the same
      // moment as the mark grace rather than being shortened by the mark pass's own duration.
      Instant sweepStart = Instant.now();
      Instant markCutoff =
          sweepStart.minus(
              Duration.ofMillis(this.materializedViewConfig.staleLeaseMarkGracePeriodMs));
      // One scan drives both passes: candidates are the leases expired past the mark grace, and
      // the never-mark set comes from the same snapshot, so the two can never disagree.
      List<Lease> scannedLeases = this.leaseManager.findGcCandidates();
      List<Lease> candidates =
          scannedLeases.stream()
              .filter(lease -> lease.leaseExpiration().isBefore(markCutoff))
              .toList();
      // Safeguard: never mark or delete an index's current (highest-version) lease — it is the
      // live index, even if its heartbeat went stale (e.g. a fleet-wide outage). Partial scans are
      // safe: marking needs a scanned higher-version sibling, so a missing lease can only protect.
      // The protection lapses once the index is deleted: with no live version left, its remaining
      // leases — the highest one included — are orphans like any other, and nothing but this sweep
      // ever cleans them up. Config lag is covered by the grace periods: a live index's current
      // lease is continuously renewed and never becomes stale enough to be a candidate.
      Set<String> currentVersionLeaseKeys =
          keysOfConfiguredIndexes(
              LeaseManager.currentVersionLeaseKeys(scannedLeases), scannedLeases);
      markStaleLeases(candidates, currentVersionLeaseKeys, markCutoff);
      // Deletion kill switch: with GC on but this off, the sweep is mark-only and deletes nothing.
      // Gauge reset to 0 as above — with deletion deliberately off, nothing is "blocked".
      if (!this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
          DynamicFeatureFlags.MV_GC_DELETION_ENABLED)) {
        setOverdueGauge(0);
        return;
      }
      // Reclamation pass: only leases that were already marked when this sweep read them, so a
      // lease marked by the pass above is never cleaned up in the same sweep.
      Instant deleteCutoff =
          sweepStart.minus(
              Duration.ofMillis(this.materializedViewConfig.staleLeaseDeleteGracePeriodMs));
      @Var long overdueNotCleaned = 0;
      for (Lease lease : candidates) {
        boolean overdue = isOverdueForCleanup(lease, deleteCutoff);
        try {
          boolean cleaned = cleanUpStaleLease(lease, currentVersionLeaseKeys, deleteCutoff);
          if (overdue && !cleaned) {
            overdueNotCleaned++;
          }
        } catch (Exception e) {
          if (overdue) {
            overdueNotCleaned++;
          }
          LOG.atWarn()
              .addKeyValue("leaseKey", lease.id())
              .setCause(e)
              .log("Failed to clean up stale lease, continuing sweep");
        }
      }
      // matViewGcLeasesOverdue: overdue leases the cleanup pass could not delete this sweep. 0 in
      // healthy operation — each overdue lease is deleted the sweep it is found (churn bursts
      // included) — so a sustained non-zero value means cleanup is blocked for some lease (drop
      // failing, guard refusing, ...). Identify which from the logs.
      setOverdueGauge(overdueNotCleaned);
    } catch (Exception e) {
      // The one failure path without its own metric: an exception aborting the tick is otherwise
      // invisible (scannerAlive stays 1). Registered lazily, on first failure.
      this.meterRegistry.counter(MAT_VIEW_GC_SWEEP_FAILURE_COUNTER_NAME).increment();
      LOG.atWarn().setCause(e).log("Stale-lease GC sweep failed, will retry on next tick");
    }
  }

  /**
   * Marks each stale candidate {@link Lease.CleanupState#ELIGIBLE_FOR_CLEANUP} unless it is its
   * index's current (highest) version or its materialized view is still used by a local generation.
   */
  private void markStaleLeases(
      List<Lease> candidates, Set<String> currentVersionLeaseKeys, Instant markCutoff) {
    for (Lease lease : candidates) {
      // Contain per-lease failures too: one bad lease must not starve the rest of the sweep.
      try {
        if (lease.cleanupState() == Lease.CleanupState.ELIGIBLE_FOR_CLEANUP) {
          continue;
        }
        if (currentVersionLeaseKeys.contains(lease.id())) {
          continue;
        }
        if (refuseIfUsedByLocalGeneration(lease, "cleanup mark")) {
          continue;
        }
        if (this.leaseManager.markEligibleForCleanup(lease.id(), markCutoff)) {
          LOG.atInfo()
              .addKeyValue("leaseKey", lease.id())
              .addKeyValue("leaseExpiration", lease.leaseExpiration())
              .addKeyValue(
                  "gracePeriod",
                  Duration.ofMillis(this.materializedViewConfig.staleLeaseMarkGracePeriodMs))
              .log("Marked stale lease eligible for cleanup");
        }
      } catch (Exception e) {
        LOG.atWarn()
            .addKeyValue("leaseKey", lease.id())
            .setCause(e)
            .log("Failed to mark stale lease eligible for cleanup, continuing sweep");
      }
    }
  }

  /**
   * Filters the current-version safeguard set down to the leases whose index still exists in this
   * mongot's config. Deletion removes an index from the {@link IndexCatalog} and its versions are
   * never re-created, so a deleted index's leases all stay cleanup targets. A lease key whose index
   * id cannot be parsed keeps its protection.
   */
  private Set<String> keysOfConfiguredIndexes(Set<String> leaseKeys, List<Lease> scannedLeases) {
    Set<String> keys = new HashSet<>(leaseKeys);
    for (Lease lease : scannedLeases) {
      if (keys.contains(lease.id()) && !isIndexConfigured(lease.indexId())) {
        keys.remove(lease.id());
        // Re-counted on every sweep while the orphan exists, so the rate reflects how many
        // deleted-index leases the GC is currently working through.
        this.meterRegistry.counter(MAT_VIEW_GC_DELETED_INDEX_LEASE_COUNTER_NAME).increment();
        // Debug: this re-fires on every sweep for as long as the orphan exists (up to the delete
        // grace); the one-shot INFO signal for the outcome is the cleanup-mark log line.
        LOG.atDebug()
            .addKeyValue("leaseKey", lease.id())
            .addKeyValue("leaseExpiration", lease.leaseExpiration())
            .log(
                "Index absent from this mongot's config (deleted, or this node's config is "
                    + "lagging); its highest-version lease is not protected");
      }
    }
    return keys;
  }

  /**
   * Whether {@code indexId} is present in this mongot's config. An id that does not parse resolves
   * to configured — the conservative default, matching the grouping fallback in {@link
   * Lease#indexId()}.
   */
  private boolean isIndexConfigured(String indexId) {
    if (!ObjectId.isValid(indexId)) {
      // A lease key in no known layout — should not happen, so surface it while it keeps the
      // conservative protection.
      LOG.atWarn()
          .addKeyValue("indexId", indexId)
          .log("Lease key has no parseable index id; keeping its GC protection");
      return true;
    }
    return this.indexCatalog.getIndexById(new ObjectId(indexId)).isPresent();
  }

  /**
   * Sets the overdue gauge (matViewGcLeasesOverdue). Ticks that don't sweep — GC disabled, shutting
   * down, replication unsupported — call this with 0 so it doesn't report a stale value. (The
   * scanner-alive gauge is a scrape-time bound gauge and resets itself.)
   */
  private void setOverdueGauge(long count) {
    this.gcLeasesOverdueGauge.set(count);
  }

  /** Whether {@code lease} is past the delete grace (and so the cleanup pass should delete it). */
  private static boolean isOverdueForCleanup(Lease lease, Instant deleteCutoff) {
    return lease.cleanupState() == Lease.CleanupState.ELIGIBLE_FOR_CLEANUP
        && lease.leaseExpiration().isBefore(deleteCutoff);
  }

  /**
   * Cleans up one stale lease: drops the materialized view collection, then the lease document, so
   * a failure in between leaves a re-discoverable lease rather than an orphaned collection. Both
   * steps re-validate against the database. Returns true iff the lease was deleted this sweep.
   */
  private boolean cleanUpStaleLease(
      Lease lease, Set<String> currentVersionLeaseKeys, Instant deleteCutoff) {
    if (lease.cleanupState() != Lease.CleanupState.ELIGIBLE_FOR_CLEANUP) {
      // Not yet marked as of this sweep's snapshot — cleaned up by a later sweep at the earliest.
      return false;
    }
    if (!lease.leaseExpiration().isBefore(deleteCutoff)) {
      // Marked, but not yet lapsed past the delete grace.
      return false;
    }
    // The never-touch-current safeguard, re-checked at delete time: the mark may not have gone
    // through this code's mark pass (a build without the safeguard, a manual edit), or the lease
    // may have become its index's highest version since. Counted because recurring refusals mean
    // some writer violated the never-mark-current rule (alertable); transient ones self-clear via
    // the unmark on active-use writes.
    if (currentVersionLeaseKeys.contains(lease.id())) {
      this.meterRegistry.counter(MAT_VIEW_GC_MARKED_CURRENT_REFUSAL_COUNTER_NAME).increment();
      LOG.atWarn()
          .addKeyValue("leaseKey", lease.id())
          .log("Marked lease is its index's current version; refusing cleanup delete");
      return false;
    }
    // Re-checked at delete time: the marking mongot can differ from this one, and OCC cannot see
    // local readers (see refuseIfUsedByLocalGeneration).
    if (refuseIfUsedByLocalGeneration(lease, "cleanup delete")) {
      return false;
    }
    if (!dropStaleMatViewCollection(lease.id(), lease.leaseVersion(), deleteCutoff)) {
      // Not dropped (no longer eligible, or the drop failed) — the next sweep re-evaluates.
      return false;
    }
    if (this.leaseManager.deleteStaleLease(lease.id(), lease.leaseVersion(), deleteCutoff)) {
      LOG.atInfo()
          .addKeyValue("leaseKey", lease.id())
          .addKeyValue("leaseExpiration", lease.leaseExpiration())
          .log("Cleaned up stale lease and its materialized view collection");
      return true;
    }
    return false;
  }

  /**
   * The in-use safeguard, run at both mark and delete time: if a generation on this mongot still
   * reads {@code lease}'s materialized view, records the refusal (counter + log) and returns true.
   * GC is leaderless (marker and deleter can differ) and a local reader never bumps the lease
   * version, so OCC cannot catch this.
   *
   * @param skippedAction phrase naming what is being skipped, e.g. {@code "cleanup mark"}
   */
  private boolean refuseIfUsedByLocalGeneration(Lease lease, String skippedAction) {
    if (!this.usedByLocalGeneration.test(lease)) {
      return false;
    }
    // A follower reads an MV whose lease is not renewed; sustained non-zero is alertable.
    this.meterRegistry.counter(MAT_VIEW_GC_LEASE_IN_USE_REFUSAL_COUNTER_NAME).increment();
    LOG.atWarn()
        .addKeyValue("leaseKey", lease.id())
        .log(
            "Stale lease's mat view is still used by a local generation; skipping "
                + skippedAction);
    return true;
  }

  /**
   * Drops a stale lease's materialized view collection — the first cleanup step, before {@link
   * LeaseManager#deleteStaleLease}. The namespace resolves only while the lease is still eligible
   * and at {@code expectedLeaseVersion}, so a reused lease is left untouched. Best effort, never
   * throws, idempotent; true means the collection is gone and the lease delete may proceed.
   */
  @VisibleForTesting
  boolean dropStaleMatViewCollection(
      String leaseKey, long expectedLeaseVersion, Instant expirationCutoff) {
    Optional<MongoNamespace> namespace =
        this.leaseManager.resolveStaleMatViewNamespace(
            leaseKey, expectedLeaseVersion, expirationCutoff);
    if (namespace.isEmpty()) {
      return false;
    }
    // Best effort: on failure the next sweep re-evaluates this lease, and the counter
    // (scenario=gc, outcome=failure) flags, via a sustained non-zero value, GC blocked on MV
    // deletion failures.
    return this.dropMatViewCollection.test(leaseKey, namespace.get());
  }
}
