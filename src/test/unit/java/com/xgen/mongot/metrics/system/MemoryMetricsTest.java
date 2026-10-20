package com.xgen.mongot.metrics.system;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  /** A cgroup-v2 hierarchy the walk cannot find: the own-cgroup and host branches decide. */
  private CgroupV2Memory noCgroupV2() {
    Path missing = this.temp.getRoot().toPath().resolve("does-not-exist");
    return new CgroupV2Memory(missing, missing);
  }

  /** A fake hierarchy with this process at {@code ownPath}; levels are added by {@link #level}. */
  private CgroupV2Memory cgroupV2At(String ownPath, Path root) throws IOException {
    Path proc = this.temp.newFile("proc_self_cgroup").toPath();
    Files.writeString(proc, "0::" + ownPath + "\n");
    return new CgroupV2Memory(proc, root);
  }

  private static void level(Path root, String relPath, String max, String current)
      throws IOException {
    Path dir = root.resolve(relPath.replaceFirst("^/", ""));
    Files.createDirectories(dir);
    Files.writeString(dir.resolve("memory.max"), max);
    Files.writeString(dir.resolve("memory.current"), current);
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

    @SuppressWarnings("UnusedVariable")
    var metrics =
        MemoryMetrics.create(memory, memory.getVirtualMemory(), noCgroup(), noCgroupV2(), registry);

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

    @SuppressWarnings("UnusedVariable")
    var metrics =
        MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroup, noCgroupV2(), registry);

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

    @SuppressWarnings("UnusedVariable")
    var metrics =
        MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroup, noCgroupV2(), registry);

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
    var cgroup = cgroupWith(cgroupLimit, cgroupUsage);

    @SuppressWarnings("UnusedVariable")
    var metrics =
        MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroup, noCgroupV2(), registry);

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
    var cgroup = cgroupWith(cgroupLimit, cgroupUsage);

    @SuppressWarnings("UnusedVariable")
    var metrics =
        MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroup, noCgroupV2(), registry);

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

    @SuppressWarnings("UnusedVariable")
    var metrics =
        MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroup, noCgroupV2(), registry);

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
    var cgroup = cgroupWith(cgroupLimit, cgroupUsage);

    @SuppressWarnings("UnusedVariable")
    var metrics =
        MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroup, noCgroupV2(), registry);

    assertEquals(cgroupLimit, (long) registry.get("system.memory.phys.total").gauge().value());
    assertEquals(
        cgroupLimit - cgroupUsage,
        (long) registry.get("system.memory.phys.available").gauge().value());
    assertEquals(cgroupUsage, (long) registry.get("system.memory.phys.inUse").gauge().value());
  }

  @Test
  public void update_cgroupUsageChanges_gaugesReflectNewValues() {
    long cgroupLimit = 4_000_000_000L; // 4 GB
    long initialUsage = 1_000_000_000L; // 1 GB
    long updatedUsage = 3_000_000_000L; // 3 GB
    var registry = new SimpleMeterRegistry();
    var memory = mockGlobalMemory(HOST_TOTAL, HOST_AVAILABLE);
    var cgroup = mock(CgroupInfo.class);
    when(cgroup.getMemoryLimit()).thenReturn(cgroupLimit);
    when(cgroup.getMemoryUsage()).thenReturn(initialUsage, updatedUsage);

    var metrics =
        MemoryMetrics.create(memory, memory.getVirtualMemory(), cgroup, noCgroupV2(), registry);

    assertEquals(cgroupLimit, (long) registry.get("system.memory.phys.total").gauge().value());
    assertEquals(
        cgroupLimit - initialUsage,
        (long) registry.get("system.memory.phys.available").gauge().value());
    assertEquals(initialUsage, (long) registry.get("system.memory.phys.inUse").gauge().value());

    metrics.update();

    assertEquals(cgroupLimit, (long) registry.get("system.memory.phys.total").gauge().value());
    assertEquals(
        cgroupLimit - updatedUsage,
        (long) registry.get("system.memory.phys.available").gauge().value());
    assertEquals(updatedUsage, (long) registry.get("system.memory.phys.inUse").gauge().value());
  }

  @Test
  public void update_ownCgroupUnlimited_ancestorLimitWins() throws IOException {
    long podLimit = 4_000_000_000L;
    long podUsage = 3_000_000_000L;
    var registry = new SimpleMeterRegistry();
    var memory = mockGlobalMemory(HOST_TOTAL, HOST_AVAILABLE);
    Path root = this.temp.newFolder("cgroupfs_ancestor").toPath();
    level(root, "/kubepods/pod-x/mongot", "max", "0");
    level(root, "/kubepods/pod-x", Long.toString(podLimit), Long.toString(podUsage));

    @SuppressWarnings("UnusedVariable")
    var metrics =
        MemoryMetrics.create(
            memory,
            memory.getVirtualMemory(),
            noCgroup(),
            cgroupV2At("/kubepods/pod-x/mongot", root),
            registry);

    assertEquals(podLimit, (long) registry.get("system.memory.phys.total").gauge().value());
    assertEquals(
        podLimit - podUsage, (long) registry.get("system.memory.phys.available").gauge().value());
    assertEquals(podUsage, (long) registry.get("system.memory.phys.inUse").gauge().value());
  }

  @Test
  public void update_ownCgroupLooserThanAncestor_ancestorWins() throws IOException {
    long ownLimit = 8_000_000_000L;
    long parentLimit = 2_000_000_000L;
    long parentUsage = 1_500_000_000L;
    var registry = new SimpleMeterRegistry();
    var memory = mockGlobalMemory(HOST_TOTAL, HOST_AVAILABLE);
    Path root = this.temp.newFolder("cgroupfs_loose_own").toPath();
    level(root, "/pod/container", Long.toString(ownLimit), "1000");
    level(root, "/pod", Long.toString(parentLimit), Long.toString(parentUsage));

    @SuppressWarnings("UnusedVariable")
    var metrics =
        MemoryMetrics.create(
            memory,
            memory.getVirtualMemory(),
            cgroupWith(ownLimit, 1000), // oshi sees only the own level
            cgroupV2At("/pod/container", root),
            registry);

    assertEquals(parentLimit, (long) registry.get("system.memory.phys.total").gauge().value());
    assertEquals(parentUsage, (long) registry.get("system.memory.phys.inUse").gauge().value());
  }
}
