package com.xgen.mongot.metrics.system;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.BaseUnits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.software.os.OSProcess;

// TODO(CLOUDP-285787): investigate moving metrics (especially gauges) to MetricsFactory
public class ProcessMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessMetrics.class);

  private final OSProcess self;

  ProcessMetrics(OSProcess self) {
    this.self = self;
  }

  static ProcessMetrics create(SystemInfo systemInfo, MeterRegistry meterRegistry) {
    return create(systemInfo.getOperatingSystem().getCurrentProcess(), meterRegistry);
  }

  @VisibleForTesting
  static ProcessMetrics create(OSProcess self, MeterRegistry meterRegistry) {
    Gauge.builder("system.process.majorPageFaults", self, OSProcess::getMajorFaults)
        .description("Number of major page faults")
        .baseUnit(BaseUnits.OPERATIONS)
        .register(meterRegistry);
    Gauge.builder("system.process.minorPageFaults", self, OSProcess::getMinorFaults)
        .description("Number of minor page faults")
        .baseUnit(BaseUnits.OPERATIONS)
        .register(meterRegistry);
    long openFiles = self.getOpenFiles();
    // per javadoc, getOpenFiles returns -1 if the open file count
    // is unsupported or unknown
    if (openFiles >= 0) {
      Gauge.builder("system.process.openFileDescriptors", self, OSProcess::getOpenFiles)
          .description("Number of open file descriptors")
          .baseUnit(BaseUnits.OBJECTS)
          .register(meterRegistry);
    } else {
      LOG.warn(
          "Cannot report open file descriptors",
          openFiles);
    }
    return new ProcessMetrics(self);
  }

  public void update() {
    this.self.updateAttributes();
  }
}
