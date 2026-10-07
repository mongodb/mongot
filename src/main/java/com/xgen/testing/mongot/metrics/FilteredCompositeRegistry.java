package com.xgen.testing.mongot.metrics;

import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.stream.IntStream;

/**
 * Builds the Atlas registry layout in which MaaS children deny mongot meters at registration, so
 * the composite holds a no-op child for each of them.
 */
public class FilteredCompositeRegistry {

  /**
   * Composite reads pick an arbitrary child, so with this many no-op children a read that goes
   * through the composite returns 0 with probability 1000/1001.
   */
  private static final int DENIED_CHILDREN = 1000;

  /** Returns a composite with an unfiltered FTDC child plus children that deny every meter. */
  public static MeterAndFtdcRegistry create() {
    var composite = new CompositeMeterRegistry();
    var ftdc = new SimpleMeterRegistry();
    composite.add(ftdc);
    IntStream.range(0, DENIED_CHILDREN).forEach(i -> composite.add(denyAllRegistry()));
    return MeterAndFtdcRegistry.create(composite, ftdc);
  }

  private static SimpleMeterRegistry denyAllRegistry() {
    var registry = new SimpleMeterRegistry();
    registry.config().meterFilter(MeterFilter.denyUnless(id -> false));
    return registry;
  }
}
