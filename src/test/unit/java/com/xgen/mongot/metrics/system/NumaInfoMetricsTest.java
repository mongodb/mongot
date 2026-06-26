package com.xgen.mongot.metrics.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.LogicalProcessor;

public class NumaInfoMetricsTest {
  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  // LogicalProcessor(processorNumber, physicalProcessorNumber, physicalPackageNumber, numaNode)
  private static LogicalProcessor lp(int proc, int pkg, int numaNode) {
    return new LogicalProcessor(proc, proc, pkg, numaNode);
  }

  private static CentralProcessor processorWith(List<LogicalProcessor> lps, int packages) {
    var processor = mock(CentralProcessor.class);
    when(processor.getLogicalProcessors()).thenReturn(lps);
    when(processor.getPhysicalPackageCount()).thenReturn(packages);
    return processor;
  }

  @Test
  public void create_registersGaugeWithValueOne() {
    var meterRegistry = new SimpleMeterRegistry();
    var processor = processorWith(List.of(lp(0, 0, 0), lp(1, 0, 0)), 1);

    // Micrometer holds the gauge state via WeakReference; keep a strong ref or GC returns NaN.
    var unused = NumaInfoMetrics.create(processor, meterRegistry, "0", "1");

    Gauge gauge = meterRegistry.get("system.numa.info").gauge();
    assertNotNull(gauge);
    assertEquals(1.0, gauge.value(), 0.0);
  }

  @Test
  public void create_singleNodeSingleSocket() {
    var meterRegistry = new SimpleMeterRegistry();
    var processor = processorWith(List.of(lp(0, 0, 0), lp(1, 0, 0), lp(2, 0, 0)), 1);

    var unused = NumaInfoMetrics.create(processor, meterRegistry, "0", "0");

    var meterId = meterRegistry.get("system.numa.info").gauge().getId();
    assertEquals("1", meterId.getTag("nodes"));
    assertEquals("1", meterId.getTag("sockets"));
    assertEquals("0", meterId.getTag("allowed_nodes"));
    assertEquals("0", meterId.getTag("numa_balancing"));
  }

  @Test
  public void create_multiNodeMultiSocket() {
    var meterRegistry = new SimpleMeterRegistry();
    // Two sockets, two NUMA nodes
    var lps = List.of(lp(0, 0, 0), lp(1, 0, 0), lp(2, 1, 1), lp(3, 1, 1));
    var processor = processorWith(lps, 2);

    var unused = NumaInfoMetrics.create(processor, meterRegistry, "0-1", "1");

    var meterId = meterRegistry.get("system.numa.info").gauge().getId();
    assertEquals("2", meterId.getTag("nodes"));
    assertEquals("2", meterId.getTag("sockets"));
    assertEquals("0-1", meterId.getTag("allowed_nodes"));
    assertEquals("1", meterId.getTag("numa_balancing"));
  }

  @Test
  public void readMemsAllowedList_missingFile_returnsUnknown() {
    Path missing = this.tmp.getRoot().toPath().resolve("does-not-exist");
    assertEquals("unknown", NumaInfoMetrics.readMemsAllowedList(missing));
  }

  @Test
  public void readMemsAllowedList_parsesValue() throws IOException {
    Path status = this.tmp.newFile("status").toPath();
    Files.writeString(
        status,
        String.join(
            "\n",
            "Name:\tjava",
            "State:\tR (running)",
            "Cpus_allowed_list:\t0-7",
            "Mems_allowed_list:\t0-1",
            ""));
    assertEquals("0-1", NumaInfoMetrics.readMemsAllowedList(status));
  }

  @Test
  public void readMemsAllowedList_missingLine_returnsUnknown() throws IOException {
    Path status = this.tmp.newFile("status").toPath();
    Files.writeString(status, "Name:\tjava\nState:\tR (running)\n");
    assertEquals("unknown", NumaInfoMetrics.readMemsAllowedList(status));
  }

  @Test
  public void readNumaBalancing_missingFile_returnsUnknown() {
    Path missing = this.tmp.getRoot().toPath().resolve("does-not-exist");
    assertEquals("unknown", NumaInfoMetrics.readNumaBalancing(missing));
  }

  @Test
  public void readNumaBalancing_parsesValue() throws IOException {
    Path balancing = this.tmp.newFile("numa_balancing").toPath();
    Files.writeString(balancing, "1\n");
    assertEquals("1", NumaInfoMetrics.readNumaBalancing(balancing));
  }

  @Test
  public void readNumaBalancing_emptyFile_returnsUnknown() throws IOException {
    Path balancing = this.tmp.newFile("numa_balancing").toPath();
    Files.writeString(balancing, "");
    assertEquals("unknown", NumaInfoMetrics.readNumaBalancing(balancing));
  }
}
