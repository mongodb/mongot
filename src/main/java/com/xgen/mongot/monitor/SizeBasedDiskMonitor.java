package com.xgen.mongot.monitor;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports how full mongot is against a ceiling on total index size, so write activity can be gated
 * by how much index a node is allowed to hold rather than by how much room is left on its data
 * volume.
 *
 * <p>When {@code diskSizeBytes} is not positive, the denominator falls back to {@code
 * maxIndexBytes}.
 *
 * <p>Unlike {@link PeriodicDiskMonitor} this monitor never crashes the process.
 */
public class SizeBasedDiskMonitor implements DiskMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(SizeBasedDiskMonitor.class);

  public static final Duration DEFAULT_INDEX_SIZE_UPDATE_PERIOD = Duration.ofMinutes(1);

  private static final long BYTES_PER_MB = 1024L * 1024L;

  private final NamedScheduledExecutorService executorService;
  private final CheckedSupplier<OptionalLong, IOException> sampler;
  private final LongSupplier diskSizeBytesSampler;
  private final long maxIndexBytes;
  private final AtomicLong capacityBytesGauge;
  private final AtomicLong totalIndexBytesGauge;
  private final Counter failedSampleCounter;

  @GuardedBy("this")
  private final Set<Gate> gates;

  @GuardedBy("this")
  private double indexSizeUtilization;

  private SizeBasedDiskMonitor(
      NamedScheduledExecutorService executorService,
      CheckedSupplier<OptionalLong, IOException> sampler,
      LongSupplier diskSizeBytesSampler,
      long maxIndexBytes,
      MeterRegistry meterRegistry) {
    this.executorService = executorService;
    this.sampler = sampler;
    this.diskSizeBytesSampler = diskSizeBytesSampler;
    this.maxIndexBytes = maxIndexBytes;
    this.gates = new HashSet<>();
    this.indexSizeUtilization = 1.0;

    MetricsFactory metricsFactory = new MetricsFactory("system.indexSize", meterRegistry);
    this.capacityBytesGauge = metricsFactory.numGauge("capacityBytes");
    this.totalIndexBytesGauge = metricsFactory.numGauge("totalBytes");
    this.failedSampleCounter = metricsFactory.counter("failedSamples");
    metricsFactory.objectValueGauge("maxBytes", this, monitor -> monitor.maxIndexBytes);
    metricsFactory.objectValueGauge(
        "utilization", this, SizeBasedDiskMonitor::getIndexSizeUtilization);
  }

  public static SizeBasedDiskMonitor createAndStart(
      CheckedSupplier<OptionalLong, IOException> sampler,
      LongSupplier diskSizeBytesSampler,
      long maxIndexMb,
      MeterRegistry meterRegistry) {
    SizeBasedDiskMonitor monitor = create(sampler, diskSizeBytesSampler, maxIndexMb, meterRegistry);
    monitor.start(DEFAULT_INDEX_SIZE_UPDATE_PERIOD);
    return monitor;
  }

  @VisibleForTesting
  public static SizeBasedDiskMonitor create(
      CheckedSupplier<OptionalLong, IOException> sampler,
      LongSupplier diskSizeBytesSampler,
      long maxIndexMb,
      MeterRegistry meterRegistry) {
    Check.checkArg(maxIndexMb > 0, "maxIndexMb must be positive but was %s", maxIndexMb);
    SizeBasedDiskMonitor monitor =
        new SizeBasedDiskMonitor(
            Executors.singleThreadScheduledExecutor("index-size-monitor", meterRegistry),
            sampler,
            diskSizeBytesSampler,
            Math.multiplyExact(maxIndexMb, BYTES_PER_MB),
            meterRegistry);
    monitor.update();
    return monitor;
  }

  @Override
  @VisibleForTesting
  public void start(Duration updatePeriod) {
    LOG.atInfo()
        .addKeyValue("interval", updatePeriod)
        .addKeyValue("maxIndexBytes", this.maxIndexBytes)
        .addKeyValue("capacityBytes", this.capacityBytesGauge.get())
        .log("Beginning periodic index size monitoring");
    this.executorService.scheduleWithFixedDelay(
        this::update, updatePeriod.toMillis(), updatePeriod.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    LOG.info("Stopping periodic index size monitoring.");
    Executors.shutdownOrFail(this.executorService);
  }

  @Override
  public synchronized void register(Gate gate) {
    gate.update(this.indexSizeUtilization);
    this.gates.add(gate);
  }

  @VisibleForTesting
  public void update() {
    try {
      long capacityBytes = sampleCapacityBytes();
      this.capacityBytesGauge.set(capacityBytes);

      OptionalLong sample = this.sampler.get();
      if (sample.isEmpty()) {
        keepLastSample("Could not sample total index size; keeping the last reading");
        return;
      }

      long totalIndexBytes = sample.getAsLong();
      double utilization = Math.min(1.0, (double) totalIndexBytes / capacityBytes);
      this.totalIndexBytesGauge.set(totalIndexBytes);
      synchronized (this) {
        this.indexSizeUtilization = utilization;
        this.gates.forEach(gate -> gate.update(this.indexSizeUtilization));
      }
    } catch (IOException | RuntimeException e) {
      keepLastSample("Could not sample total index size", e);
    }
  }

  private long sampleCapacityBytes() {
    long diskSizeBytes = this.diskSizeBytesSampler.getAsLong();
    return diskSizeBytes > 0 ? Math.min(this.maxIndexBytes, diskSizeBytes) : this.maxIndexBytes;
  }

  private void keepLastSample(String message) {
    this.failedSampleCounter.increment();
    LOG.atWarn().log(message);
  }

  private void keepLastSample(String message, Throwable cause) {
    this.failedSampleCounter.increment();
    LOG.atWarn().setCause(cause).log(message);
  }

  @VisibleForTesting
  public synchronized double getIndexSizeUtilization() {
    return this.indexSizeUtilization;
  }
}
