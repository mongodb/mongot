package com.xgen.mongot.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GlobalMetricsFactoryTest {

  private SimpleMeterRegistry registry;

  @Before
  public void setUp() {
    this.registry = new SimpleMeterRegistry();
    GlobalMetricFactory.resetForTest();
  }

  @After
  public void tearDown() {
    GlobalMetricFactory.resetForTest();
  }

  @Test
  public void testInitialize() {
    GlobalMetricFactory.initialize(this.registry);
    Optional<?> maybeRegistry = GlobalMetricFactory.getRegistryForTest();
    assertTrue(maybeRegistry.isPresent());
    assertSame(this.registry, maybeRegistry.get());
  }

  @Test
  public void testBasicUnreachableCounter() {
    GlobalMetricFactory.initialize(this.registry);

    // No counter yet
    assertNull(
        this.registry.find("mongot.unreachable_code_paths").tags("reason", "TestError").counter());

    // Increment via factory
    GlobalMetricFactory.incrementUnreachable("TestError");

    // Now counter should exist
    Counter counter =
        this.registry.find("mongot.unreachable_code_paths").tags("reason", "TestError").counter();
    assertNotNull(counter);
    assertEquals(1.0, counter.count(), 0.0);

    // Increment again
    GlobalMetricFactory.incrementUnreachable("TestError");
    assertEquals(2.0, counter.count(), 0.0);
  }

  @Test
  public void testMultipleReasonsHaveIndependentCounters() {
    GlobalMetricFactory.initialize(this.registry);

    GlobalMetricFactory.incrementUnreachable("NullPointer");
    GlobalMetricFactory.incrementUnreachable("NullPointer");
    GlobalMetricFactory.incrementUnreachable("IndexOutOfBounds");

    Counter nullPointerCounter =
        this.registry.find("mongot.unreachable_code_paths").tags("reason", "NullPointer").counter();
    Counter indexOutOfBoundsCounter =
        this.registry
            .find("mongot.unreachable_code_paths")
            .tags("reason", "IndexOutOfBounds")
            .counter();
    Counter ioErrorCounter =
        this.registry.find("mongot.unreachable_code_paths").tags("reason", "IOError").counter();

    assertNotNull(nullPointerCounter);
    assertNotNull(indexOutOfBoundsCounter);
    assertNull(ioErrorCounter); // never incremented

    assertEquals(2.0, nullPointerCounter.count(), 0.0);
    assertEquals(1.0, indexOutOfBoundsCounter.count(), 0.0);
  }

  @Test
  public void testIncrementBeforeInitDoesNotThrow() {
    // Should not throw; just log a warning internally
    GlobalMetricFactory.incrementUnreachable("TestError");
    Optional<?> maybeRegistry = GlobalMetricFactory.getRegistryForTest();
    assertTrue("Registry should remain uninitialized", maybeRegistry.isEmpty());
  }

  @Test
  public void testInitializeTwiceLogsDebug() {
    GlobalMetricFactory.initialize(this.registry);
    GlobalMetricFactory.initialize(this.registry); // should not reinit
    Optional<?> maybeRegistry = GlobalMetricFactory.getRegistryForTest();
    assertTrue(maybeRegistry.isPresent());
    assertSame(this.registry, maybeRegistry.get());
  }

  @Test(expected = NullPointerException.class)
  public void testInitializeWithNullRegistry() {
    GlobalMetricFactory.initialize(null);
  }

  @Test
  public void testBloomFilterCountersRegisteredEagerlyAtZero() {
    GlobalMetricFactory.initialize(this.registry);

    // Present as 0 immediately after init, before any increment, so dashboards/alerts see a series
    // instead of "no data".
    assertBloomCounter("bloom_filter_id_lookup_skipped", 0.0);
    assertBloomCounter("bloom_filter_id_lookup_probed", 0.0);
    assertBloomCounter("bloom_filter_id_lookup_false_positive", 0.0);
    assertBloomCounter("bloom_filter_saturated_dropped", 0.0);
  }

  @Test
  public void testBloomFilterCountersEmitExpectedNames() {
    GlobalMetricFactory.initialize(this.registry);

    GlobalMetricFactory.incrementBloomFilterIdLookupSkipped();
    GlobalMetricFactory.incrementBloomFilterIdLookupProbed();
    GlobalMetricFactory.incrementBloomFilterIdLookupFalsePositive();
    GlobalMetricFactory.incrementBloomFilterSaturatedDropped(3);

    assertBloomCounter("bloom_filter_id_lookup_skipped", 1.0);
    assertBloomCounter("bloom_filter_id_lookup_probed", 1.0);
    assertBloomCounter("bloom_filter_id_lookup_false_positive", 1.0);
    assertBloomCounter("bloom_filter_saturated_dropped", 3.0);
  }

  private void assertBloomCounter(String name, double expected) {
    Counter counter = this.registry.find(name).counter();
    assertNotNull("Expected counter " + name + " to be registered", counter);
    assertEquals(expected, counter.count(), 0.0);
  }
}
