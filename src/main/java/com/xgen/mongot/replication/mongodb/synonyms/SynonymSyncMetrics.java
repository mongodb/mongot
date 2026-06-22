package com.xgen.mongot.replication.mongodb.synonyms;

import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

/**
 * Encapsulates all metrics for the synonym sync. This class provides access to various metrics that
 * can be used by different components of the synonym sync system.
 */
public class SynonymSyncMetrics {
  private final Timer syncDurationTimer;
  private final Timer collScanDurationTimer;
  private final Counter exceptionCounter;
  private final Counter collScansTriggeredByChangeStreamCounter;
  private final Counter collScansCounter;

  SynonymSyncMetrics(MetricsFactory metricsFactory) {
    this.syncDurationTimer = metricsFactory.timer("syncDurations");
    this.collScanDurationTimer = metricsFactory.timer("collScanDurations");
    this.exceptionCounter = metricsFactory.counter("exceptions");
    this.collScansTriggeredByChangeStreamCounter =
        metricsFactory.counter("collScansTriggeredByChangeStream");
    this.collScansCounter = metricsFactory.counter("collScans");
  }

  /**
   * Returns the timer for tracking total synonym sync durations. This measures the full lifecycle
   * of a synonym sync request, either a collection scan with indexing or change stream event
   * detection, plus any overhead from request processing and cleanup.
   */
  public Timer getSyncDurationTimer() {
    return this.syncDurationTimer;
  }

  /** Returns the timer for tracking synonym sync collection scan durations. */
  public Timer getCollScanDurationTimer() {
    return this.collScanDurationTimer;
  }

  /** Returns the counter for tracking exceptions during synonym syncs. */
  public Counter getExceptionCounter() {
    return this.exceptionCounter;
  }

  /**
   * Returns the counter for tracking collection scans triggered by change stream events. This
   * increments when a change stream detects changes and triggers a full collection scan to rebuild
   * synonyms.
   */
  public Counter getCollScansTriggeredByChangeStreamCounter() {
    return this.collScansTriggeredByChangeStreamCounter;
  }

  /**
   * Returns the counter for tracking all collection scans. This includes initial scans and scans
   * triggered by change stream events.
   */
  public Counter getCollScansCounter() {
    return this.collScansCounter;
  }
}
