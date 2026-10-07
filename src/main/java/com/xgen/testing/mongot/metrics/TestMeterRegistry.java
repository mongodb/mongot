package com.xgen.testing.mongot.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * A reusable {@link MeterRegistry} for tests that construct registry-taking objects but do not
 * assert on the resulting meters. Sharing one instance avoids threading a registry through every
 * call site.
 *
 * <p>Each test class runs in its own JVM, so in practice this is one registry per test class. Tests
 * that assert on meters should create their own registry instead of using this one.
 */
public final class TestMeterRegistry {

  private static final MeterRegistry INSTANCE = new SimpleMeterRegistry();

  /** Returns the shared registry. */
  public static MeterRegistry get() {
    return INSTANCE;
  }

  private TestMeterRegistry() {}
}
