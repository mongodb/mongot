package com.xgen.mongot.util.timers;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Ticker;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe implementation of an InvocationCountingTimer that uses sampling to reduce overhead.
 *
 * <p>Sampling strategy:
 *
 * <ul>
 *   <li>First 10 invocations: measure every time (100% sampling)
 *   <li>Invocations 11-100: measure every 10th (10% sampling)
 *   <li>Invocations 101+: measure every 100th (1% sampling)
 * </ul>
 *
 * <p>The elapsed time is extrapolated as: (measuredNanos * invocationCount) / measurementCount
 *
 * <p>Method calls to {@code getInvocationCount()} and {@code getElapsedNanos()} aren't guaranteed
 * to be consistent amongst themselves.
 */
public class SamplingInvocationCountingTimer implements InvocationCountingTimer {

  private static final SafeClosable NO_OP = () -> {};

  private final Supplier<Stopwatch> stopwatchSupplier;
  private final AtomicLong invocationCount = new AtomicLong();
  private final LongAdder measuredNanos = new LongAdder();
  private final LongAdder measurementCount = new LongAdder();
  private final InvocationCountingTimer.TimingDataCreator timingDataCreator;

  public SamplingInvocationCountingTimer(Ticker ticker) {
    this(ticker, TimingData.InvocationCountTimingData::new);
  }

  public SamplingInvocationCountingTimer(
      Ticker ticker, InvocationCountingTimer.TimingDataCreator timingDataCreator) {
    this.stopwatchSupplier = () -> Stopwatch.createStarted(ticker);
    this.timingDataCreator = timingDataCreator;
  }


  /** Initializes the timer with a custom TimingDataCreator and existing timing data. */
  public SamplingInvocationCountingTimer(
      Ticker ticker,
      InvocationCountingTimer.TimingDataCreator timingDataCreator,
      TimingData initialData) {
    this(ticker, timingDataCreator);
    this.invocationCount.set(initialData.invocationCount());
    // For initial data, we treat it as if all invocations were measured
    this.measuredNanos.add(initialData.elapsedNanos());
    this.measurementCount.add(initialData.invocationCount());

  }

  @Override
  public SafeClosable split() {
    long currentCount = this.invocationCount.getAndIncrement();

    if (shouldSample(currentCount)) {
      var stopwatch = this.stopwatchSupplier.get();
      return () -> {
        this.measuredNanos.add(stopwatch.stop().elapsed(TimeUnit.NANOSECONDS));
        this.measurementCount.increment();
      };
    }

    // Not sampling this invocation
    return NO_OP;
  }

  private static boolean shouldSample(long currentCount) {
    if (currentCount < 10) {
      return true;
    }
    if (currentCount < 100) {
      return currentCount % 10 == 0;
    }
    return currentCount % 100 == 0;
  }

  @Override
  public long getInvocationCount() {
    return this.invocationCount.get();
  }

  @Override
  public long getElapsedNanos() {
    long measurements = this.measurementCount.longValue();
    if (measurements == 0) {
      return 0L;
    }

    long totalNanos = this.measuredNanos.longValue();
    long invocations = this.invocationCount.get();

    // Multiply before divide to reduce rounding errors
    return (totalNanos * invocations) / measurements;
  }

  @Override
  public TimingData getTimingData() {
    return this.timingDataCreator.createTimingData(getInvocationCount(), getElapsedNanos());
  }
}
