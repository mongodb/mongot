package com.xgen.mongot.metrics.system;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;

public class SystemMetricsInstrumentation {
  private final MeterRegistry meterRegistry;
  private final DiskMetrics diskMetrics;
  private final NetstatMetrics netstatMetrics;
  private final ProcessMetrics processMetrics;
  private final MemoryMetrics memoryMetrics;

  @SuppressWarnings("unused") // held to prevent GC of the gauge's referenced object
  private final CpuInfoMetrics cpuInfoMetrics;

  @SuppressWarnings("unused") // held to prevent GC of the gauge's referenced object
  private final NumaInfoMetrics numaInfoMetrics;

  private final NamedScheduledExecutorService updater;
  private static final Logger LOG = LoggerFactory.getLogger(SystemMetricsInstrumentation.class);

  /** Instrument system metrics and bind them to the input registry. */
  public static SystemMetricsInstrumentation create(MeterRegistry meterRegistry, Path dataPath) {
    SystemInfo systemInfo = new SystemInfo();
    return new SystemMetricsInstrumentation(
        meterRegistry,
        DiskMetrics.create(systemInfo, meterRegistry, dataPath),
        NetstatMetrics.create(systemInfo, meterRegistry),
        ProcessMetrics.create(systemInfo, meterRegistry),
        MemoryMetrics.create(systemInfo, meterRegistry),
        CpuInfoMetrics.create(systemInfo, meterRegistry),
        NumaInfoMetrics.create(systemInfo, meterRegistry));
  }

  @VisibleForTesting
  SystemMetricsInstrumentation(
      MeterRegistry meterRegistry,
      DiskMetrics diskMetrics,
      NetstatMetrics netstatMetrics,
      ProcessMetrics processMetrics,
      MemoryMetrics memoryMetrics,
      CpuInfoMetrics cpuInfoMetrics,
      NumaInfoMetrics numaInfoMetrics) {
    this.meterRegistry = meterRegistry;
    this.diskMetrics = diskMetrics;
    this.netstatMetrics = netstatMetrics;
    this.processMetrics = processMetrics;
    this.memoryMetrics = memoryMetrics;
    this.cpuInfoMetrics = cpuInfoMetrics;
    this.numaInfoMetrics = numaInfoMetrics;
    this.updater =
        Executors.singleThreadScheduledExecutor("system-metrics-updater", this.meterRegistry);
  }

  /** Start updating system metrics at a fixed delay. */
  public void start(long delay, TimeUnit unit) {
    LOG.atInfo()
        .addKeyValue("interval", delay)
        .addKeyValue("timeUnit", unit.name().toLowerCase())
        .log("Starting periodic system metrics update");
    // scheduleWithFixedDelay cancels the task on an uncaught throwable, so one
    // failing updater must not take the others' gauges down with it.
    this.updater.scheduleWithFixedDelay(
        () -> {
          updateSafely("disk", this.diskMetrics::update);
          updateSafely("netstat", this.netstatMetrics::update);
          updateSafely("process", this.processMetrics::update);
          updateSafely("memory", this.memoryMetrics::update);
        },
        delay,
        delay,
        unit);
  }

  /** Runs one updater; a failure is logged and the next tick retries. */
  private static void updateSafely(String component, Runnable update) {
    try {
      update.run();
    } catch (RuntimeException e) {
      LOG.atWarn()
          .addKeyValue("component", component)
          .setCause(e)
          .log("system metrics updater failed");
    }
  }

  /** Stops periodic scheduling. */
  public void stop() {
    Executors.shutdownOrFail(this.updater);
  }
}
