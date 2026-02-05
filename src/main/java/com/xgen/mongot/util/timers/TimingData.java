package com.xgen.mongot.util.timers;

public interface TimingData {

  TimingData EMPTY = new InvocationCountTimingData(0, 0);

  record InvocationCountTimingData(long invocationCount, long elapsedNanos) implements TimingData {
    public InvocationCountTimingData sum(TimingData another) {
      return new InvocationCountTimingData(
          this.invocationCount() + another.invocationCount(),
          this.elapsedNanos() + another.elapsedNanos());
    }
  }

  record NoInvocationCountTimingData(long elapsedNanos) implements TimingData {
    public NoInvocationCountTimingData(long ignored, long elapsedNanos) {
      this(elapsedNanos);
    }

    @Override
    public long invocationCount() {
      return 0;
    }

    @Override
    public TimingData sum(TimingData another) {
      return new NoInvocationCountTimingData(this.elapsedNanos() + another.elapsedNanos());
    }
  }

  long invocationCount();

  long elapsedNanos();

  TimingData sum(TimingData another);

  static boolean isEmpty(TimingData data) {
    return data.invocationCount() == 0L && data.elapsedNanos() == 0L;
  }
}
