package com.xgen.mongot.util.concurrent;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;

public interface NamedExecutorService extends ExecutorService {

  // Based on the list of metrics here, for whatever version we're using:
  // This should only include Gauge, TimeGauge, FunctionCounter, and FunctionTimer
  // https://github.com/micrometer-metrics/micrometer/blob/311efdb2f2079103e5a0e4b0b87b12d91e405af8/micrometer-core/src/main/java/io/micrometer/core/instrument/binder/jvm/ExecutorServiceMetrics.java#L359
  Map<String, Meter.Type> METRICS =
      Map.ofEntries(
          Map.entry("executor.completed", Meter.Type.COUNTER),
          Map.entry("executor.active", Meter.Type.GAUGE),
          Map.entry("executor.queued", Meter.Type.GAUGE),
          Map.entry("executor.queue.remaining", Meter.Type.GAUGE),
          Map.entry("executor.pool.size", Meter.Type.GAUGE),
          Map.entry("executor.pool.core", Meter.Type.GAUGE),
          Map.entry("executor.pool.max", Meter.Type.GAUGE),
          Map.entry("executor", Meter.Type.TIMER),
          Map.entry("executor.idle", Meter.Type.TIMER));

  String getName();

  MeterRegistry getMeterRegistry();

  default void removeMetrics() {
    var meterRegistry = getMeterRegistry();
    for (Map.Entry<String, Meter.Type> entry : METRICS.entrySet()) {
      var gaugeName = entry.getKey();
      var gaugeType = entry.getValue();

      Meter.Id idToRemove =
          new Meter.Id(
              "%s.%s".formatted(getName(), gaugeName),
              // We don't currently use tags, but they need to be combined here if we add them
              Tags.of("name", "executorMetrics"),
              null,
              null,
              gaugeType);
      meterRegistry.remove(idToRemove);
    }
  }

  /**
   * Returns the approximate number of threads that are actively executing tasks.
   *
   * @return the number of active threads, or empty if not supported by the underlying executor
   */
  default OptionalInt getActiveCount() {
    return OptionalInt.empty();
  }

  /**
   * Returns the maximum allowed number of threads.
   *
   * @return the maximum number of threads, or empty if not supported by the underlying executor
   */
  default OptionalInt getMaxPoolSize() {
    return OptionalInt.empty();
  }

  /**
   * Returns the current number of tasks in the queue.
   *
   * @return the queue size, or empty if not supported by the underlying executor
   */
  default OptionalInt getQueueSize() {
    return OptionalInt.empty();
  }

  /**
   * Returns the registry of live thread ids owned by the underlying pool, if the pool's thread
   * factory tracks them.
   *
   * <p>Returns {@link Optional#empty()} for executors built from externally-supplied executors
   * whose threads we don't control.
   */
  default Optional<LiveThreadIdsRegistry> getLiveThreadIdsRegistry() {
    return Optional.empty();
  }
}
