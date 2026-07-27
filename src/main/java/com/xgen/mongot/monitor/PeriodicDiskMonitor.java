package com.xgen.mongot.monitor;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO(CLOUDP-346959): Use a builder to create PeriodicDiskMonitor objects.
public class PeriodicDiskMonitor implements DiskMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(PeriodicDiskMonitor.class);
  public static final Duration DEFAULT_DISK_UTILIZATION_UPDATE_PERIOD = Duration.ofSeconds(5);
  private final NamedScheduledExecutorService executorService;
  private final FileStore fileStore;
  private final AtomicLong diskMonitorGauge;

  // List of gates that needs updating every time disk utilization is polled. Any of these gates
  // being opened means disk write activity is being allowed.
  @GuardedBy("this")
  private final Set<Gate> gates;

  // anyGateWasOpened is set to true if any gate was ever open. It remains true even if all gates
  // are currently closed.
  @GuardedBy("this")
  private boolean anyGateWasOpened;

  private final double crashThreshold;

  @GuardedBy("this")
  private double diskUtilization;

  private PeriodicDiskMonitor(
      NamedScheduledExecutorService executorService,
      FileStore fileStore,
      double crashThreshold,
      MeterRegistry meterRegistry,
      double diskUtilization) {
    this.fileStore = fileStore;
    this.executorService = executorService;
    this.diskUtilization = diskUtilization;
    this.gates = new HashSet<>();
    this.anyGateWasOpened = false;
    this.crashThreshold = crashThreshold;

    MetricsFactory metricsFactory = new MetricsFactory("system.disk", meterRegistry);
    this.diskMonitorGauge = metricsFactory.numGauge("monitor");
    this.diskMonitorGauge.incrementAndGet();
  }

  public static DiskMonitor createAndStart(
      Path dataPath, double crashThreshold, MeterRegistry meterRegistry) {
    try {
      FileStore fileStore = Files.getFileStore(dataPath);
      PeriodicDiskMonitor diskMonitor = create(fileStore, crashThreshold, meterRegistry);
      diskMonitor.start(DEFAULT_DISK_UTILIZATION_UPDATE_PERIOD);
      return diskMonitor;
    } catch (IOException e) {
      LOG.atError()
          .addKeyValue("dataPath", dataPath)
          .setCause(e)
          .log("Error initializing disk monitor");
      return new NoOpDiskMonitor();
    }
  }

  @VisibleForTesting
  public static PeriodicDiskMonitor create(
      FileStore fileStore, double crashThreshold, MeterRegistry meterRegistry) throws IOException {
    return new PeriodicDiskMonitor(
        Executors.singleThreadScheduledExecutor("disk-monitor", Thread.MAX_PRIORITY, meterRegistry),
        fileStore,
        crashThreshold,
        meterRegistry,
        computeDiskUtilization(fileStore));
  }

  @Override
  @VisibleForTesting
  public void start(Duration diskMonitorDuration) {
    LOG.atInfo()
        .addKeyValue("interval", diskMonitorDuration)
        .log("Beginning periodic disk monitoring");
    this.executorService.scheduleWithFixedDelay(
        () -> Crash.because("failed to update disk metrics").ifThrows(this::update),
        diskMonitorDuration.toMillis(),
        diskMonitorDuration.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    LOG.info("Stopping periodic disk monitoring.");
    Executors.shutdownOrFail(this.executorService);
    this.diskMonitorGauge.decrementAndGet();
  }

  /**
   * Updates data related to disk utilization.
   *
   * @throws IOException if disk utilization files cannot be read.
   */
  @VisibleForTesting
  public synchronized void update() throws IOException {
    this.diskUtilization = computeDiskUtilization(this.fileStore);
    this.gates.forEach(gate -> gate.update(this.diskUtilization));
    if (this.gates.stream().anyMatch(Gate::isOpen)) {
      this.anyGateWasOpened = true;
    }
    if (this.anyGateWasOpened && this.diskUtilization >= this.crashThreshold) {
      this.crashNow();
    }
  }

  @VisibleForTesting
  public void crashNow() {
    Crash.because(
            "disk utilization has reached critical threshold, crashing to abort"
                + "pending merges and writes")
        .withCrashCategory(Crash.CrashCategory.DISK_FULL)
        .withThreadDump()
        .now();
  }

  @VisibleForTesting
  public synchronized double getDiskUtilization() {
    return this.diskUtilization;
  }

  private static double computeDiskUtilization(FileStore fileStore) throws IOException {
    long totalSpace = fileStore.getTotalSpace();
    long usableSpace = fileStore.getUsableSpace();
    return (double) (totalSpace - usableSpace) / totalSpace;
  }

  @Override
  public synchronized void register(Gate gate) {
    gate.update(this.diskUtilization);
    if (gate.isOpen()) {
      this.anyGateWasOpened = true;
    }
    this.gates.add(gate);
  }
}
