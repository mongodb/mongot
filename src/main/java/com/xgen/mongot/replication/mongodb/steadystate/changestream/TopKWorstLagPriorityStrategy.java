package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.types.ObjectId;

/**
 * Ranks all active indexes by current lag and classifies the K worst as preferred, where K is
 * derived from the drain ratio's safe share of the active index population.
 *
 * <p>Selects by position in the ranking, not by comparing against a value, so the preferred set is
 * bounded at K regardless of how many indexes share similar or tied lag values, including during a
 * correlated event where many indexes become lagged at once.
 */
final class TopKWorstLagPriorityStrategy extends RecomputingLagPriorityStrategy {

  static final int DEFAULT_RECOMPUTE_INTERVAL_SECONDS = 15;

  private final double targetPreferredFraction;

  /**
   * Constructs the strategy and starts its background recompute loop.
   *
   * @param drainRatioPreferredTasksPerRegularTask the queue-drain ratio; target preferred fraction
   *     (and hence K) is derived from it as ratio / (ratio + 1)
   */
  TopKWorstLagPriorityStrategy(
      int drainRatioPreferredTasksPerRegularTask,
      int recomputeIntervalSeconds,
      int healthyLagFloorSeconds,
      MeterRegistry meterRegistry) {
    super(
        "TopKWorstLagPriorityStrategy",
        recomputeIntervalSeconds,
        validate(drainRatioPreferredTasksPerRegularTask, healthyLagFloorSeconds),
        meterRegistry);
    this.targetPreferredFraction =
        drainRatioPreferredTasksPerRegularTask
            / ((double) drainRatioPreferredTasksPerRegularTask + 1);
  }

  private static int validate(
      int drainRatioPreferredTasksPerRegularTask, int healthyLagFloorSeconds) {
    if (drainRatioPreferredTasksPerRegularTask <= 0) {
      throw new IllegalArgumentException(
          "drainRatioPreferredTasksPerRegularTask must be positive, got: "
              + drainRatioPreferredTasksPerRegularTask);
    }
    return healthyLagFloorSeconds;
  }

  @Override
  Set<ObjectId> computePreferredSet(
      Map<ObjectId, Integer> lagSecondsSnapshot,
      Set<ObjectId> previousPreferredSet,
      int healthyLagFloorSeconds) {
    List<Map.Entry<ObjectId, Integer>> sortedDescending =
        lagSecondsSnapshot.entrySet().stream()
            .sorted(
                Comparator.<Map.Entry<ObjectId, Integer>>comparingInt(Map.Entry::getValue)
                    .reversed()
                    // Deterministic tie-break only
                    .thenComparing(entry -> entry.getKey().toHexString()))
            .toList();

    // K is sized off the TOTAL registered population, not just however many are currently lagging.
    int promoteRankCutoff =
        Math.max(1, (int) Math.round(this.targetPreferredFraction * sortedDescending.size()));

    Set<ObjectId> preferred = new HashSet<>();
    for (int i = 0; i < sortedDescending.size(); i++) {
      Map.Entry<ObjectId, Integer> entry = sortedDescending.get(i);
      int rank = i + 1; // 1-indexed
      // Ranking within the top K is necessary but not sufficient — an index must also individually
      // clear the healthy-lag floor, or healthy indexes get padded in just to fill the ceiling.
      boolean meetsFloor = entry.getValue() >= healthyLagFloorSeconds;
      if (meetsFloor && rank <= promoteRankCutoff) {
        preferred.add(entry.getKey());
      }
    }
    return preferred;
  }
}
