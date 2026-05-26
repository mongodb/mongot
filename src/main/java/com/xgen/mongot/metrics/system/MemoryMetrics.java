package com.xgen.mongot.metrics.system;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.BaseUnits;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;
import oshi.hardware.VirtualMemory;
import oshi.software.os.CgroupInfo;

// TODO(CLOUDP-285787): investigate moving metrics (especially gauges) to MetricsFactory
public class MemoryMetrics {
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();
  private final GlobalMemory globalMemory;
  private final VirtualMemory virtualMemory;
  private final CgroupInfo cgroupInfo;
  private final AtomicLong physTotal = new AtomicLong();
  private final AtomicLong physAvailable = new AtomicLong();
  private final AtomicLong physicalInUse = new AtomicLong();
  private final AtomicLong swapAvailable = new AtomicLong();
  private final AtomicLong memoryMappings = new AtomicLong();

  private MemoryMetrics(
      GlobalMemory globalMemory, VirtualMemory virtualMemory, CgroupInfo cgroupInfo) {
    this.globalMemory = globalMemory;
    this.virtualMemory = virtualMemory;
    this.cgroupInfo = cgroupInfo;
  }

  static MemoryMetrics create(SystemInfo systemInfo, MeterRegistry meterRegistry) {
    GlobalMemory globalMemory = systemInfo.getHardware().getMemory();
    VirtualMemory virtualMemory = globalMemory.getVirtualMemory();
    CgroupInfo cgroupInfo = systemInfo.getOperatingSystem().getCgroupInfo();
    return create(globalMemory, virtualMemory, cgroupInfo, meterRegistry);
  }

  @VisibleForTesting
  static MemoryMetrics create(
      GlobalMemory globalMemory,
      VirtualMemory virtualMemory,
      CgroupInfo cgroupInfo,
      MeterRegistry meterRegistry) {
    MemoryMetrics memoryMetrics = new MemoryMetrics(globalMemory, virtualMemory, cgroupInfo);

    Gauge.builder("system.memory.memoryMappings", memoryMetrics.memoryMappings, AtomicLong::get)
        .description("The number of memory mappings")
        .baseUnit(BaseUnits.OBJECTS)
        .register(meterRegistry);

    // Physical memory metrics
    Gauge.builder("system.memory.phys.total", memoryMetrics.physTotal, AtomicLong::get)
        .description("The amount of physical memory")
        .baseUnit(BaseUnits.BYTES)
        .register(meterRegistry);
    Gauge.builder("system.memory.phys.available", memoryMetrics.physAvailable, AtomicLong::get)
        .description("The amount of physical memory available")
        .baseUnit(BaseUnits.BYTES)
        .register(meterRegistry);
    Gauge.builder("system.memory.phys.inUse", memoryMetrics.physicalInUse, AtomicLong::get)
        .description("The amount of physical memory in use")
        .baseUnit(BaseUnits.BYTES)
        .register(meterRegistry);
    Gauge.builder("system.memory.pageSize", globalMemory, GlobalMemory::getPageSize)
        .description("The system memory page size")
        .baseUnit(BaseUnits.BYTES)
        .register(meterRegistry);

    // Virtual memory metrics
    Gauge.builder("system.memory.virt.inUse", virtualMemory, VirtualMemory::getVirtualInUse)
        .description("The total amount of physical and virtual memory in use")
        .baseUnit(BaseUnits.BYTES)
        .register(meterRegistry);
    Gauge.builder("system.memory.virt.max", virtualMemory, VirtualMemory::getVirtualMax)
        .description("The total combined physical and virtual memory capacity")
        .baseUnit(BaseUnits.BYTES)
        .register(meterRegistry);
    Gauge.builder("system.memory.virt.swap.total", virtualMemory, VirtualMemory::getSwapTotal)
        .description("The current swap file size")
        .baseUnit(BaseUnits.BYTES)
        .register(meterRegistry);
    Gauge.builder(
            "system.memory.virt.swap.available", memoryMetrics.swapAvailable, AtomicLong::get)
        .description("The amount of unallocated swap memory available")
        .baseUnit(BaseUnits.BYTES)
        .register(meterRegistry);
    Gauge.builder("system.memory.virt.swap.inUse", virtualMemory, VirtualMemory::getSwapUsed)
        .description("The amount of allocated memory in the swap file")
        .baseUnit(BaseUnits.BYTES)
        .register(meterRegistry);
    Gauge.builder("system.memory.virt.swap.pagesIn", virtualMemory, VirtualMemory::getSwapPagesIn)
        .description("The amount of memory committed to the swap file")
        .baseUnit(BaseUnits.OPERATIONS)
        .register(meterRegistry);
    Gauge.builder(
            "system.memory.virt.swap.pagesOut", virtualMemory, VirtualMemory::getSwapPagesOut)
        .description("The amount of memory committed to the swap file")
        .baseUnit(BaseUnits.OPERATIONS)
        .register(meterRegistry);

    memoryMetrics.update(); // ensure computed fields are populated
    return memoryMetrics;
  }

  private static Optional<Long> getMemoryMappings() {
    // self resolves to the current pid
    try {
      Path path = Paths.get("/proc/self/maps");
      // each line in /proc/pid/maps represents one memory mapping
      long lineCount;
      try (Stream<String> lines = Files.lines(path)) {
        lineCount = lines.count();
      }
      return Optional.of(lineCount);
    } catch (Throwable e) {
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.DAYS).log(
          "Exception raised during retrieval of memory mappings count");
    }
    return Optional.empty();
  }

  /** Refreshes all periodically-computed memory metrics. */
  public void update() {
    long hostTotal = this.globalMemory.getTotal();
    long hostAvailable = this.globalMemory.getAvailable();

    long cgroupLimit = this.cgroupInfo.getMemoryLimit();
    if (cgroupLimit < CgroupInfo.UNLIMITED_MEMORY && cgroupLimit < hostTotal) {
      long cgroupUsage = this.cgroupInfo.getMemoryUsage();
      long adjustedAvailable = Math.max(0L, cgroupLimit - cgroupUsage);

      this.physTotal.set(cgroupLimit);
      this.physAvailable.set(adjustedAvailable);
      this.physicalInUse.set(cgroupLimit - adjustedAvailable);
    } else {
      this.physTotal.set(hostTotal);
      this.physAvailable.set(hostAvailable);
      this.physicalInUse.set(hostTotal - hostAvailable);
    }

    this.swapAvailable.set(this.virtualMemory.getSwapTotal() - this.virtualMemory.getSwapUsed());

    // update memory mappings - only updated every 5 seconds
    var memoryMappings = getMemoryMappings();
    memoryMappings.ifPresent(this.memoryMappings::set);
  }
}
