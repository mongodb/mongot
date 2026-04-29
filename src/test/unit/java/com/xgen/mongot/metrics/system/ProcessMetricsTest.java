package com.xgen.mongot.metrics.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import oshi.software.os.OSProcess;

public class ProcessMetricsTest {
  @Test
  public void update_processMetricValuesChange_updatesGaugeValue() {
    var meterRegistry = new SimpleMeterRegistry();
    var process = mock(OSProcess.class);
    var majorFaults = new AtomicLong(3);
    var minorFaults = new AtomicLong(11);
    var openFiles = new AtomicLong(7);
    when(process.getMajorFaults()).thenAnswer(invocation -> majorFaults.get());
    when(process.getMinorFaults()).thenAnswer(invocation -> minorFaults.get());
    when(process.getOpenFiles()).thenAnswer(invocation -> openFiles.get());

    // Gauges evaluate their supplier function on every read, so the values
    // reflect whatever the mock returns at the time of access.
    var processMetrics = ProcessMetrics.create(process, meterRegistry);
    assertEquals(3.0, meterRegistry.get("system.process.majorPageFaults").gauge().value(), 0.0);
    assertEquals(11.0, meterRegistry.get("system.process.minorPageFaults").gauge().value(), 0.0);
    assertEquals(
        7.0, meterRegistry.get("system.process.openFileDescriptors").gauge().value(), 0.0);

    // update must call updateAttributes which ensures the values are refreshed
    when(process.updateAttributes()).thenAnswer(i -> {
      majorFaults.set(30);
      minorFaults.set(110);
      openFiles.set(70);
      return true;
    });
    processMetrics.update();

    // The reads should reflect the updated values
    assertEquals(30.0, meterRegistry.get("system.process.majorPageFaults").gauge().value(), 0.0);
    assertEquals(110.0, meterRegistry.get("system.process.minorPageFaults").gauge().value(), 0.0);
    assertEquals(
        70.0, meterRegistry.get("system.process.openFileDescriptors").gauge().value(), 0.0);
  }

  @Test
  public void create_openFileDescriptorsUnsupported_doesNotRegisterGauge() {
    var meterRegistry = new SimpleMeterRegistry();
    var process = mock(OSProcess.class);
    when(process.getOpenFiles()).thenReturn(-1L);

    ProcessMetrics.create(process, meterRegistry);

    assertNull(meterRegistry.find("system.process.openFileDescriptors").gauge());
  }
}
