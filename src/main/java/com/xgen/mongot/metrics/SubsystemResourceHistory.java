package com.xgen.mongot.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Var;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Tracks in-process rolling history of per-pool resource attribution (heap allocation bytes and
 * on-CPU nanoseconds). Allocation bytes measures cumulative new heap allocation excluding direct
 * buffers and native allocations.
 *
 * <p>Each attribution source obtains a {@link Recorder} via {@link #recorderFor} and pushes the
 * cumulative, monotonic values of heap allocation and CPU nanos.
 *
 * <p>History is appended by {@link #recordSample}, which must be driven by a periodic caller. Every
 * series is written at the same index on the same tick, so samples across pools and subsystems are
 * aligned in time and a subsystem total is the sum of its pools at a shared index.
 *
 * <p>All methods are safe for concurrent use.
 */
public final class SubsystemResourceHistory {

  /** The resource dimension attributed to a pool. */
  public enum Dimension {
    ALLOC_BYTES,
    CPU_NANOS
  }

  // Store 30 mins worth of samples, with a 5 second interval.
  private static final int DEFAULT_RETAINED_SAMPLES =
      (int) Duration.ofMinutes(30).dividedBy(Duration.ofSeconds(5));

  // A map of subsystems to pools to recorders. The recorder stores the cumulative values for a
  // pool.
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Recorder>> bySubsystem =
      new ConcurrentHashMap<>();

  // Represent a circular buffer of recorded samples, shared by every series. nextIndex is the
  // next write position to write the newest sample. The buffer wraps once nextIndex reaches the
  // end.
  // sampleCount tracks how many slots are populated with a maximum value of retainedSamples.
  private final long[] sampleTimestampMs;
  private int sampleCount;
  private int nextIndex;

  // Injectable clock for testing, defaults to the system timer.
  private final LongSupplier clockMs;

  public SubsystemResourceHistory() {
    this(DEFAULT_RETAINED_SAMPLES, () -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
  }

  @VisibleForTesting
  SubsystemResourceHistory(int retainedSamples, LongSupplier clockMs) {
    Preconditions.checkArgument(retainedSamples >= 2, "retainedSamples must be at least 2");
    this.sampleTimestampMs = new long[retainedSamples];
    this.clockMs = clockMs;
  }

  /**
   * Recorder for one pool's slots, created on first use and returned unchanged thereafter. Safe to
   * call again when a pool is rebuilt: the existing recorder is reused, so its series stays
   * continuous.
   */
  public Recorder recorderFor(String subsystem, String pool) {
    return this.bySubsystem
        .computeIfAbsent(subsystem, ignored -> new ConcurrentHashMap<>())
        .computeIfAbsent(pool, ignored -> new Recorder(this.sampleTimestampMs.length));
  }

  /** Appends one sample of every registered pool's slots. Intended for a periodic caller. */
  public synchronized void recordSample() {
    int writeIndex = this.nextIndex;
    this.sampleTimestampMs[writeIndex] = this.clockMs.getAsLong();

    for (ConcurrentHashMap<String, Recorder> pools : this.bySubsystem.values()) {
      for (Recorder recorder : pools.values()) {
        for (Dimension dimension : Dimension.values()) {
          recorder.series(dimension).append(writeIndex);
        }
      }
    }

    this.nextIndex = (writeIndex + 1) % this.sampleTimestampMs.length;
    if (this.sampleCount < this.sampleTimestampMs.length) {
      this.sampleCount++;
    }
  }

  /** Immutable snapshot of the subsystems known to this history. */
  public Set<String> subsystems() {
    return Set.copyOf(this.bySubsystem.keySet());
  }

  /** Immutable snapshot of the pools registered under a subsystem. Empty when it is unknown. */
  public Set<String> pools(String subsystem) {
    return Optional.ofNullable(this.bySubsystem.get(subsystem))
        .map(pools -> Set.copyOf(pools.keySet()))
        .orElseGet(Set::of);
  }

  /**
   * Increase across all of a subsystem's pools over {@code horizon}, normalized to that horizon
   * when the retained samples span a shorter interval.
   *
   * <p>Empty when the subsystem is unknown, fewer than two samples exist, or the retained interval
   * covers less than half of {@code horizon}.
   */
  public Optional<Long> increaseOver(String subsystem, Dimension dimension, Duration horizon) {
    return Optional.ofNullable(this.bySubsystem.get(subsystem))
        .flatMap(pools -> increaseOver(pools.values(), dimension, horizon));
  }

  /**
   * Increase for a single pool, with the semantics of {@link #increaseOver(String, Dimension,
   * Duration)}.
   */
  public Optional<Long> increaseOver(
      String subsystem, String pool, Dimension dimension, Duration horizon) {
    return Optional.ofNullable(this.bySubsystem.get(subsystem))
        .map(pools -> pools.get(pool))
        .flatMap(recorder -> increaseOver(List.of(recorder), dimension, horizon));
  }

  /**
   * Increase for a set of recorders, for the horizon duration. Returns empty when the stored
   * interval is too short.
   */
  private synchronized Optional<Long> increaseOver(
      Collection<Recorder> recorders, Dimension dimension, Duration horizon) {
    long horizonMs = horizon.toMillis();
    if (this.sampleCount < 2 || horizonMs <= 0) {
      return Optional.empty();
    }

    int capacity = this.sampleTimestampMs.length;
    int newestIndex = newestIndex();
    long newestMs = this.sampleTimestampMs[newestIndex];

    // Walk forward from the oldest retained sample to the first one at or after the start of the
    // horizon, so the returned duration <= horizon duration. When the
    // history does not go back that far this settles on the oldest sample held.
    int oldestIndex = (this.nextIndex - this.sampleCount + capacity) % capacity;
    @Var int startIndex = oldestIndex;
    for (int offset = 0; offset < this.sampleCount; offset++) {
      int index = (oldestIndex + offset) % capacity;
      if (this.sampleTimestampMs[index] >= newestMs - horizonMs) {
        startIndex = index;
        break;
      }
    }

    // A shorter interval than requested still measures a real rate, but extrapolating from a small
    // fraction of the horizon amplifies whatever noise the samples carry.
    long elapsedMs = newestMs - this.sampleTimestampMs[startIndex];
    if (elapsedMs * 2 < horizonMs) {
      return Optional.empty();
    }

    // The slots hold cumulative counters, so an increase is just the difference of the two
    // endpoints.
    @Var long delta = 0;
    for (Recorder recorder : recorders) {
      Series series = recorder.series(dimension);
      delta += series.values[newestIndex] - series.values[startIndex];
    }
    return Optional.of(Math.round((double) delta / elapsedMs * horizonMs));
  }

  /** Current cumulative total across a subsystem's pools. Empty when the subsystem is unknown. */
  public Optional<Long> currentTotal(String subsystem, Dimension dimension) {
    return Optional.ofNullable(this.bySubsystem.get(subsystem))
        .map(
            pools ->
                pools.values().stream()
                    .mapToLong(recorder -> recorder.series(dimension).slot.get())
                    .sum());
  }

  /** Time since {@link #recordSample} last ran. Empty before the first sample. */
  public synchronized Optional<Duration> sinceLastSample() {
    if (this.sampleCount == 0) {
      return Optional.empty();
    }
    return Optional.of(
        Duration.ofMillis(this.clockMs.getAsLong() - this.sampleTimestampMs[newestIndex()]));
  }

  @VisibleForTesting
  synchronized int sampleCount() {
    return this.sampleCount;
  }

  private int newestIndex() {
    int capacity = this.sampleTimestampMs.length;
    return (this.nextIndex - 1 + capacity) % capacity;
  }

  /** Hot-path handle for one pool. A push is a single slot write. */
  public static final class Recorder {
    private final Series allocBytes;
    private final Series cpuNanos;

    private Recorder(int retainedSamples) {
      this.allocBytes = new Series(retainedSamples);
      this.cpuNanos = new Series(retainedSamples);
    }

    /** Records this pool's cumulative heap-allocation bytes. */
    public void recordAllocBytes(long cumulativeBytes) {
      this.allocBytes.slot.set(cumulativeBytes);
    }

    /** Records this pool's cumulative on-CPU nanoseconds. */
    public void recordCpuNanos(long cumulativeNanos) {
      this.cpuNanos.slot.set(cumulativeNanos);
    }

    private Series series(Dimension dimension) {
      return dimension == Dimension.ALLOC_BYTES ? this.allocBytes : this.cpuNanos;
    }
  }

  /** History of values for a pool and dimension. */
  private static final class Series {
    // The current cumulative value.
    private final AtomicLong slot = new AtomicLong();
    // The history of dimensionvalues.
    private final long[] values;

    Series(int retainedSamples) {
      this.values = new long[retainedSamples];
    }

    /** Perist the current cumulative value to the history. */
    void append(int index) {
      this.values[index] = this.slot.get();
    }
  }
}
