package com.xgen.mongot.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;

public class Instrumentation {
  private Instrumentation() {}

  /** Registers built-in micrometer metrics on the supplied meterRegistry. */
  public static void instrumentJvmMetrics(MeterRegistry meterRegistry) {
    Tags jvmTag = Tags.of(ServerStatusDataExtractor.Scope.JVM.getTag());

    new JvmGcMetrics(jvmTag).bindTo(meterRegistry);
    new JvmMemoryMetrics(jvmTag).bindTo(meterRegistry);
    new UptimeMetrics(jvmTag).bindTo(meterRegistry);

    // The following metrics are not reported to ServerStatus
    new ProcessorMetrics(jvmTag).bindTo(meterRegistry);
  }
}
