package com.xgen.mongot.util.timers;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Ticker;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe implementation of an InvocationCountingTimer. Method calls to <code>
 * getInvocationCount()</code> <code>getElapsedNanos()</code> aren't guaranteed to be consistent
 * amongst themselves as you may observe an increase in one's return value compared to another.
 */
public class ThreadSafeInvocationCountingTimer implements InvocationCountingTimer {

  private final Supplier<Stopwatch> stopwatchSupplier;
  private final LongAdder elapsedNanos = new LongAdder();
  private final LongAdder invocationCount = new LongAdder();
  private final InvocationCountingTimer.TimingDataCreator timingDataCreator;

  /** Initializes the CountingTimer with elapsedNanos and invocationCount. */
  public ThreadSafeInvocationCountingTimer(Ticker ticker, long elapsedNanos, long invocationCount) {
    this(ticker);
    this.elapsedNanos.add(elapsedNanos);
    this.invocationCount.add(invocationCount);
  }

  public ThreadSafeInvocationCountingTimer(Ticker ticker) {
    this(ticker, TimingData.InvocationCountTimingData::new);
  }

  public ThreadSafeInvocationCountingTimer(
      Ticker ticker, InvocationCountingTimer.TimingDataCreator timingDataCreator) {
    this.stopwatchSupplier = () -> Stopwatch.createStarted(ticker);
    this.timingDataCreator = timingDataCreator;
  }

  /** Initializes the timer with a custom TimingDataCreator and existing timing data. */
  public ThreadSafeInvocationCountingTimer(
      Ticker ticker,
      InvocationCountingTimer.TimingDataCreator timingDataCreator,
      TimingData initialData) {
    this(ticker, timingDataCreator);
    this.elapsedNanos.add(initialData.elapsedNanos());
    this.invocationCount.add(initialData.invocationCount());
  }

  @Override
  public SafeClosable split() {
    var stopwatch = this.stopwatchSupplier.get();
    return () -> {
      this.elapsedNanos.add(stopwatch.stop().elapsed(TimeUnit.NANOSECONDS));
      this.invocationCount.increment();
    };
  }

  @Override
  public long getInvocationCount() {
    return this.invocationCount.longValue();
  }

  @Override
  public long getElapsedNanos() {
    return this.elapsedNanos.longValue();
  }

  @Override
  public TimingData getTimingData() {
    return this.timingDataCreator.createTimingData(getInvocationCount(), getElapsedNanos());
  }
}
