package com.xgen.mongot.util.timers;

import java.util.Optional;

public interface InvocationCountingTimer {

  @FunctionalInterface
  interface TimingDataCreator {
    TimingData createTimingData(long invocationCount, long elapsedNanos);
  }

  record AutocloseableOptional<T extends InvocationCountingTimer.SafeClosable>(Optional<T> opt)
      implements SafeClosable {

    @Override
    public void close() {
      this.opt.ifPresent(T::close);
    }
  }

  interface SafeClosable extends AutoCloseable {
    @Override
    void close();
  }

  SafeClosable split();

  long getInvocationCount();

  long getElapsedNanos();

  TimingData getTimingData();
}
