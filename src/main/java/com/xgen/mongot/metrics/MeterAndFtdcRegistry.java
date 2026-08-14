package com.xgen.mongot.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Wraps meterRegistry and ftdcRegistry together to reduce the need to bubble down an additional
 * argument to various classes and methods using a separate MeterRegistry for FTDC metrics.
 *
 * <p>This class is required to be thread-safe.
 */
public record MeterAndFtdcRegistry(
    MeterRegistry meterRegistry,
    MeterRegistry ftdcRegistry,
    ThreadPoolResourceMetrics.Bindings threadPoolResourceBindings) {

  public static MeterAndFtdcRegistry createWithCompositeMeterRegistry() {
    return createWithMeterRegistry(new CompositeMeterRegistry());
  }

  public static MeterAndFtdcRegistry createWithSimpleRegistries() {
    return createWithMeterRegistry(new SimpleMeterRegistry());
  }

  public static MeterAndFtdcRegistry createWithMeterRegistry(MeterRegistry meterRegistry) {
    return create(meterRegistry, new SimpleMeterRegistry());
  }

  public static MeterAndFtdcRegistry create(
      MeterRegistry meterRegistry, MeterRegistry ftdcRegistry) {
    return new MeterAndFtdcRegistry(
        meterRegistry, ftdcRegistry, new ThreadPoolResourceMetrics.Bindings());
  }

  public CompositeMeterRegistry getCompositeMeterRegistry() {
    if (!(this.meterRegistry instanceof CompositeMeterRegistry)) {
      throw new IllegalStateException("The meter registry is not a CompositeMeterRegistry");
    }
    return (CompositeMeterRegistry) this.meterRegistry;
  }
}
