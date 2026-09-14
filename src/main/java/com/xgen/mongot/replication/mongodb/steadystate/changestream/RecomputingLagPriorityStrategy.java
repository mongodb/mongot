package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for priority strategies that classify indexes by comparing lag across the whole active
 * population. Maintains a per-index lag registry and periodically recomputes the preferred set from
 * it; subclasses only implement {@link #computePreferredSet}.
 */
abstract class RecomputingLagPriorityStrategy implements ChangeStreamPriorityStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(RecomputingLagPriorityStrategy.class);

  private final ConcurrentHashMap<ObjectId, Integer> lagSecondsByIndex = new ConcurrentHashMap<>();

  /** How many live index generations share each key of {@link #lagSecondsByIndex}. */
  private final ConcurrentHashMap<ObjectId, Integer> activeGenerationsByIndex =
      new ConcurrentHashMap<>();

  private final AtomicReference<Set<ObjectId>> currentPreferredIndexIds =
      new AtomicReference<>(Set.of());
  private final NamedScheduledExecutorService scheduler;
  private final int healthyLagFloorSeconds;

  /**
   * Constructs the strategy and starts its background recompute loop.
   *
   * @param executorName names the single-threaded executor that runs the recompute loop.
   * @param healthyLagFloorSeconds minimum lag (seconds) for an index to be eligible for the
   *     preferred set. Must also be applied per candidate in {@link #computePreferredSet} — not
   *     just here — or healthy indexes get padded in just to fill a quota.
   */
  RecomputingLagPriorityStrategy(
      String executorName,
      int recomputeIntervalSeconds,
      int healthyLagFloorSeconds,
      MeterRegistry meterRegistry) {
    if (recomputeIntervalSeconds <= 0) {
      throw new IllegalArgumentException(
          "recomputeIntervalSeconds must be positive, got: " + recomputeIntervalSeconds);
    }
    if (healthyLagFloorSeconds < 0) {
      throw new IllegalArgumentException(
          "healthyLagFloorSeconds must be non-negative, got: " + healthyLagFloorSeconds);
    }
    this.healthyLagFloorSeconds = healthyLagFloorSeconds;
    this.scheduler = Executors.singleThreadScheduledExecutor(executorName, meterRegistry);
    this.scheduler.scheduleAtFixedRate(
        this::recomputeSafely,
        recomputeIntervalSeconds,
        recomputeIntervalSeconds,
        TimeUnit.SECONDS);
  }

  /**
   * Decides the current preferred set from a snapshot of every active index's latest known lag.
   * {@code lagSecondsSnapshot.size()} is the total population, used as the denominator for
   * K/threshold targets — but each selected candidate must individually clear {@code
   * healthyLagFloorSeconds}, or healthy indexes get padded in to fill the target count/value.
   *
   * @param previousPreferredSet the previous cycle's result, for subclasses implementing hysteresis
   */
  abstract Set<ObjectId> computePreferredSet(
      Map<ObjectId, Integer> lagSecondsSnapshot,
      Set<ObjectId> previousPreferredSet,
      int healthyLagFloorSeconds);

  @Override
  public final void addIndex(ObjectId indexId) {
    this.activeGenerationsByIndex.merge(indexId, 1, Integer::sum);
    this.lagSecondsByIndex.putIfAbsent(indexId, 0);
  }

  @Override
  public final boolean isHighPriority(ObjectId indexId, ChangeStreamBatch batch) {
    // Replace rather than put, because a batch can finish after its index was removed: the
    // dispatcher checks for shutdown before fetching a batch, not before reporting one, so a
    // getMore that was in flight when the index stopped still lands here.
    this.lagSecondsByIndex.replace(indexId, computeLagSeconds(batch));
    return this.currentPreferredIndexIds.get().contains(indexId);
  }

  @Override
  public final void removeIndex(ObjectId indexId) {
    // Drop the lag entry only once the last generation sharing it is gone.
    Integer remainingGenerations =
        this.activeGenerationsByIndex.computeIfPresent(
            indexId, (id, count) -> count == 1 ? null : count - 1);
    if (remainingGenerations == null) {
      this.lagSecondsByIndex.remove(indexId);
    }
  }

  private void recomputeSafely() {
    try {
      recompute();
    } catch (RuntimeException e) {
      LOG.warn("Failed to recompute preferred set for {}", getClass().getSimpleName(), e);
    }
  }

  /**
   * Synchronously runs one recompute cycle, bypassing the scheduled-interval wait. Tests use this
   * instead of sleeping past {@code recomputeIntervalSeconds} to observe classification changes
   * deterministically.
   */
  @TestOnly
  @VisibleForTesting
  void recomputeNowForTesting() {
    recompute();
  }

  private void recompute() {
    Map<ObjectId, Integer> snapshot = Map.copyOf(this.lagSecondsByIndex);
    if (snapshot.isEmpty()) {
      this.currentPreferredIndexIds.set(Set.of());
      return;
    }
    int worstLagSeconds = Collections.max(snapshot.values());
    if (worstLagSeconds < this.healthyLagFloorSeconds) {
      this.currentPreferredIndexIds.set(Set.of());
      return;
    }
    Set<ObjectId> previous = this.currentPreferredIndexIds.get();
    this.currentPreferredIndexIds.set(
        Set.copyOf(computePreferredSet(snapshot, previous, this.healthyLagFloorSeconds)));
  }

  /**
   * A resume token this cannot read yields {@link Integer#MAX_VALUE} so the index ranks worst and
   * clears any threshold, rather than the per-batch "default to high priority" {@link
   * OptimeLagPriorityStrategy} uses — there's no single-call equivalent in a cross-index registry.
   */
  private static int computeLagSeconds(ChangeStreamBatch batch) {
    BsonTimestamp resumeTokenTime;
    try {
      resumeTokenTime = ResumeTokenUtils.opTimeFromResumeToken(batch.getPostBatchResumeToken());
    } catch (DecoderException | RuntimeException e) {
      LOG.warn("Failed to read resume token for lag computation; treating as worst-case lag", e);
      return Integer.MAX_VALUE;
    }
    return Math.max(0, batch.getCommandOperationTime().getTime() - resumeTokenTime.getTime());
  }

  @Override
  public void shutdown() {
    this.scheduler.shutdown();
  }
}
