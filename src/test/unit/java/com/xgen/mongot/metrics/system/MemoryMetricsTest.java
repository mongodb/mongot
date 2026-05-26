package com.xgen.mongot.metrics.system;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;
import oshi.hardware.GlobalMemory;
import oshi.hardware.VirtualMemory;
import oshi.software.os.CgroupInfo;

public class MemoryMetricsTest {
  private static final long HOST_TOTAL = 16_000_000_000L; // 16 GB
  private static final long HOST_AVAILABLE = 12_000_000_000L; // 12 GB

  private static GlobalMemory mockGlobalMemory(long total, long available) {
    var memory = mock(GlobalMemory.class);
    when(memory.getTotal()).thenReturn(total);
    when(memory.getAvailable()).thenReturn(available);
    when(memory.getVirtualMemory()).thenReturn(mock(VirtualMemory.class));
    return memory;
  }

  private static CgroupInfo noCgroup() {
    var cgroup = mock(CgroupInfo.class);
    when(cgroup.getMemoryLimit()).thenReturn(CgroupInfo.UNLIMITED_MEMORY);
    return cgroup;
  }

  private static CgroupInfo cgroupWith(long limit, long usage) {
    var cgroup = mock(CgroupInfo.class);
    when(cgroup.getMemoryLimit()).thenReturn(limit);
    when(cgroup.getMemoryUsage()).thenReturn(usage);
    return cgroup;
  }

  @Test
  public void update_noCgroup_reportsHostValues() {
    var registry = new SimpleMeterRegistry();
    var memory = mockGlobalMemory(HOST_TOTAL, HOST_AVAILABLE);

    MemoryMetrics.create(memory, memory.getVirtualMemory(), noCgroup(), registry);

    assertEquals(HOST_TOTAL, (long) registry.get("system.memory.phys.total").gauge().value());
    assertEquals(
        HOST_AVAILABLE, (long) registry.get("system.memory.phys.available").gauge().value());
    assertEquals(
        HOST_TOTAL - HOST_AVAILABLE,
        (long) registry.get("system.memory.phys.inUse").gauge().value());
  }

  @Test
  public void update_cgroupLimitLargerThanHostTotal_reportsHostValues() {
    var registry = new SimpleMeterRegistry();
    var memory = mockGlobalMemory(HOST_TOTAL, HOST_AVAILABLE);
    // limit exceeds host RAM — not a binding constraint; usage is irrelevant
    var cgroup = cgroupWith(HOST_TOTAL * 2, 0);

    MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroup, registry);

    assertEquals(HOST_TOTAL, (long) registry.get("system.memory.phys.total").gauge().value());
    assertEquals(
        HOST_AVAILABLE, (long) registry.get("system.memory.phys.available").gauge().value());
    assertEquals(
        HOST_TOTAL - HOST_AVAILABLE,
        (long) registry.get("system.memory.phys.inUse").gauge().value());
  }

  @Test
  public void update_cgroupLimitEqualsHostTotal_reportsHostValues() {
    var registry = new SimpleMeterRegistry();
    var memory = mockGlobalMemory(HOST_TOTAL, HOST_AVAILABLE);
    // limit == hostTotal — not strictly constraining; usage is irrelevant
    var cgroup = cgroupWith(HOST_TOTAL, 0);

    MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroup, registry);

    assertEquals(HOST_TOTAL, (long) registry.get("system.memory.phys.total").gauge().value());
    assertEquals(
        HOST_AVAILABLE, (long) registry.get("system.memory.phys.available").gauge().value());
    assertEquals(
        HOST_TOTAL - HOST_AVAILABLE,
        (long) registry.get("system.memory.phys.inUse").gauge().value());
  }

  @Test
  public void update_cgroupLimitSmallerThanHostTotal_reportsCgroupValues() {
    long cgroupLimit = 4_000_000_000L; // 4 GB
    long cgroupUsage = 1_000_000_000L; // 1 GB
    var registry = new SimpleMeterRegistry();
    var memory = mockGlobalMemory(HOST_TOTAL, HOST_AVAILABLE);

    MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroupWith(cgroupLimit, cgroupUsage),
        registry);

    assertEquals(cgroupLimit, (long) registry.get("system.memory.phys.total").gauge().value());
    assertEquals(
        cgroupLimit - cgroupUsage,
        (long) registry.get("system.memory.phys.available").gauge().value());
    assertEquals(cgroupUsage, (long) registry.get("system.memory.phys.inUse").gauge().value());
  }

  @Test
  public void update_cgroupUsageExceedsLimit_availableIsZeroNotNegative() {
    long cgroupLimit = 2_000_000_000L;
    long cgroupUsage = 2_500_000_000L; // over limit (OOM imminent)
    var registry = new SimpleMeterRegistry();
    var memory = mockGlobalMemory(HOST_TOTAL, HOST_AVAILABLE);

    MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroupWith(cgroupLimit, cgroupUsage),
        registry);

    assertEquals(cgroupLimit, (long) registry.get("system.memory.phys.total").gauge().value());
    assertEquals(0L, (long) registry.get("system.memory.phys.available").gauge().value());
    assertEquals(cgroupLimit, (long) registry.get("system.memory.phys.inUse").gauge().value());
  }

  @Test
  public void update_cgroupUsageZero_availableEqualsLimit() {
    long cgroupLimit = 4_000_000_000L; // 4 GB
    var registry = new SimpleMeterRegistry();
    var memory = mockGlobalMemory(HOST_TOTAL, HOST_AVAILABLE);
    // OSHI returns 0 for usage when the cgroup usage file is unreadable
    var cgroup = cgroupWith(cgroupLimit, 0);

    MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroup, registry);

    assertEquals(cgroupLimit, (long) registry.get("system.memory.phys.total").gauge().value());
    assertEquals(cgroupLimit, (long) registry.get("system.memory.phys.available").gauge().value());
    assertEquals(0L, (long) registry.get("system.memory.phys.inUse").gauge().value());
  }

  @Test
  public void update_cgroupLimitLessThanHostAvailable_availableDerivedFromCgroupUsage() {
    long cgroupLimit = 500_000_000L; // 500 MB
    long cgroupUsage = 300_000_000L; // 300 MB
    var registry = new SimpleMeterRegistry();
    // HOST_AVAILABLE (12 GB) >> cgroupLimit (500 MB)
    var memory = mockGlobalMemory(HOST_TOTAL, HOST_AVAILABLE);

    MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroupWith(cgroupLimit, cgroupUsage),
        registry);

    assertEquals(cgroupLimit, (long) registry.get("system.memory.phys.total").gauge().value());
    assertEquals(
        cgroupLimit - cgroupUsage,
        (long) registry.get("system.memory.phys.available").gauge().value());
    assertEquals(cgroupUsage, (long) registry.get("system.memory.phys.inUse").gauge().value());
  }
}
