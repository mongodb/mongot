package com.xgen.mongot.embedding.mongodb.leasing;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks, per generation, whether this host wrote materialized-view documents that may have no
 * checkpoint behind them. A rejected or skipped checkpoint right after a bulk write sets the flag;
 * an alert pages if it stays set. See {@link LeaseManager#clearCheckpointCommitRisk} for what
 * counts as recovery and why the bar is high.
 *
 * <p>All state is in memory on one host, so a restart loses it and one host cannot clear another's.
 * Thread-safe: recording runs on the indexing thread while clearing and expiry run on the
 * heartbeat.
 */
class CheckpointCommitRiskTracker {

  static final String PENDING_GAUGE_NAME = "checkpointCommitRiskPending";
  static final String RESOLVED_COUNTER_NAME = "checkpointCommitRiskResolved";
  static final String EXPIRED_COUNTER_NAME = "checkpointCommitRiskExpired";
  private static final List<String> METER_NAMES =
      List.of(PENDING_GAUGE_NAME, RESOLVED_COUNTER_NAME, EXPIRED_COUNTER_NAME);

  // Safety net for recoveries this host cannot see. The main one: another host takes over, writes
  // the checkpoint, and this host stays a follower — it never learns the risk is gone, so without
  // a timeout its flag would stay set forever. 15m is longer than the slowest case where the alert
  // still needs to fire (5m to fire, plus a 5m recovery lookback, so about 10m30s).
  @VisibleForTesting static final Duration TTL = Duration.ofMinutes(15);

  private static final Logger LOG = LoggerFactory.getLogger(CheckpointCommitRiskTracker.class);

  private final MetricsFactory metricsFactory;
  private final String hostname;
  private final Map<MaterializedViewGenerationId, GenerationRisk> riskByGeneration =
      new ConcurrentHashMap<>();

  CheckpointCommitRiskTracker(MetricsFactory metricsFactory, String hostname) {
    this.metricsFactory = metricsFactory;
    this.hostname = hostname;
  }

  /** Flags {@code generationId} as at risk. Does nothing unless a bulk write actually completed. */
  void record(MaterializedViewGenerationId generationId, boolean materializedViewWriteCompleted) {
    if (!materializedViewWriteCompleted) {
      return;
    }
    GenerationRisk risk = this.riskByGeneration.computeIfAbsent(generationId, this::createRisk);
    // Set the time before the flag, so expireStale() never sees a flag with no time on it. Setting
    // it again also restarts the TTL, so a risk that keeps happening stays flagged.
    risk.setAt().set(Instant.now());
    risk.pending().set(1);
  }

  /** Marks {@code generationId} recovered. Does nothing if it was not flagged. */
  void clear(MaterializedViewGenerationId generationId) {
    GenerationRisk risk = this.riskByGeneration.get(generationId);
    if (risk != null && risk.pending().getAndSet(0) == 1) {
      risk.resolved().increment();
    }
  }

  /**
   * Clears every generation of {@code indexId} and throws away their state and meters. Only for an
   * index that is gone for good, since nothing about it can come back and matter again. Without
   * this, each generation that ever hit risk keeps its entry and three timeseries until restart.
   */
  void clearForIndex(ObjectId indexId) {
    this.riskByGeneration.keySet().stream()
        .filter(generationId -> generationId.indexId.equals(indexId))
        .toList()
        .forEach(
            generationId -> {
              clear(generationId);
              retire(generationId);
            });
  }

  /**
   * Clears flags older than {@link #TTL}.
   *
   * <p>These count as expired, not resolved. Timing out only means we never saw a recovery, not
   * that one happened, and the alert reads the resolved counter to decide what to ignore. If the
   * expired counter keeps going up in production, some real recovery path is not being noticed and
   * we should handle it directly.
   */
  void expireStale(Instant now) {
    this.riskByGeneration.forEach(
        (generationId, risk) -> {
          Instant observedSetAt = risk.setAt().get();
          if (observedSetAt == null || now.isBefore(observedSetAt.plus(TTL))) {
            return;
          }
          if (risk.pending().getAndSet(0) == 1) {
            risk.expired().increment();
            LOG.atWarn()
                .addKeyValue("generationId", generationId)
                .addKeyValue("hostname", this.hostname)
                .addKeyValue("riskRecordedAt", observedSetAt)
                .log("Checkpoint commit risk expired without observed recovery");
          }
          // A new risk landed while we were clearing this one -- its time moved. Put it back.
          if (!observedSetAt.equals(risk.setAt().get())) {
            risk.pending().set(1);
          }
        });
  }

  private GenerationRisk createRisk(MaterializedViewGenerationId generationId) {
    Tags tags = PerIndexMetricsFactory.tagsFor(generationId);
    // Order matters. The alert uses increase() on the resolved counter, which needs to see it at
    // zero first. Registering the counters before the gauge means no scrape can see pending=1
    // without also seeing them.
    Counter resolved = this.metricsFactory.counter(RESOLVED_COUNTER_NAME, tags);
    Counter expired = this.metricsFactory.counter(EXPIRED_COUNTER_NAME, tags);
    AtomicLong pending = this.metricsFactory.numGauge(PENDING_GAUGE_NAME, tags);
    return new GenerationRisk(pending, resolved, expired, new AtomicReference<>());
  }

  private void retire(MaterializedViewGenerationId generationId) {
    if (this.riskByGeneration.remove(generationId) == null) {
      return;
    }
    Tags tags = PerIndexMetricsFactory.tagsFor(generationId);
    METER_NAMES.forEach(name -> this.metricsFactory.removeMeters(name, tags));
  }

  private record GenerationRisk(
      AtomicLong pending, Counter resolved, Counter expired, AtomicReference<Instant> setAt) {}
}
